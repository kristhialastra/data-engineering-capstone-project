"""
Silver Enrichment Script
Nagba-basa ng movies mula sa Bronze na may missing budget/revenue/genres,
tinatawagan ang TMDB API para kunin ang tamang values,
at sine-save ang results sa silver.movies_enriched.

TMDB API rate limit: ~40 req/sec — gumagamit tayo ng 50ms delay = ~20 req/sec (conservative).
Para sa ~38K candidates, expect ~30 minutes runtime.
"""

import os
import sys
import time
import requests
import pandas as pd
from loguru import logger
from sqlalchemy import create_engine, text

# === Loguru Configuration ===
# Dalawang sinks: stdout para sa real-time monitoring, file para sa audit trail
logger.remove()
logger.add(sys.stdout, format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}")
logger.add("/logs/silver/silver.log", format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}", rotation="10 MB")

# === Constants ===
TMDB_BASE_URL = "https://api.themoviedb.org/3/movie"
REQUEST_DELAY = 0.05  # 50ms between requests — ~20 req/sec, conservative para hindi ma-429
REQUEST_TIMEOUT = 10  # 10 seconds timeout per request
PROGRESS_INTERVAL = 500  # Mag-log ng progress every 500 movies


def get_engine():
    """Gawa ng SQLAlchemy engine gamit ang environment variables."""
    db_url = (
        f"postgresql://{os.environ['DB_USER']}:{os.environ['DB_PASSWORD']}"
        f"@{os.environ['DB_HOST']}:{os.environ['DB_PORT']}/{os.environ['DB_NAME']}"
    )
    return create_engine(db_url)


def get_candidates(engine):
    """
    Kunin ang lahat ng movie IDs na kailangan i-enrich mula sa Bronze.
    Dalawang sources:
    1. movies_main — kung budget=0/NULL o revenue=0/NULL
    2. movie_extended — kung genres ay NULL o empty
    Pinagsasama (union) para isang API call lang per movie.
    """
    # Movies na may missing budget o revenue
    with engine.connect() as conn:
        result_financial = conn.execute(text(
            "SELECT id FROM bronze.movies_main "
            "WHERE budget IS NULL OR TRIM(budget) = '' OR TRIM(budget) = '0' "
            "   OR revenue IS NULL OR TRIM(revenue) = '' OR TRIM(revenue) = '0'"
        ))
        financial_ids = {row[0] for row in result_financial.fetchall()}

    logger.info(f"Movies na may missing budget/revenue: {len(financial_ids)}")

    # Movies na may missing genres
    with engine.connect() as conn:
        result_genres = conn.execute(text(
            "SELECT id FROM bronze.movie_extended "
            "WHERE genres IS NULL OR TRIM(genres) = ''"
        ))
        genre_ids = {row[0] for row in result_genres.fetchall()}

    logger.info(f"Movies na may missing genres: {len(genre_ids)}")

    # Union ng lahat ng IDs — isang API call lang per movie
    all_ids = financial_ids | genre_ids
    logger.info(f"Total unique candidates para sa TMDB enrichment: {len(all_ids)}")

    return all_ids


def call_tmdb_api(session, movie_id, api_key):
    """
    Tawagan ang TMDB API para sa isang movie.
    Returns: dict na may budget, revenue, genres kung successful, None kung failed.

    Error handling:
    - 404: movie not found — log warning, return None
    - 429: rate limited — wait for Retry-After, then retry
    - Iba pang errors: log warning, return None
    """
    url = f"{TMDB_BASE_URL}/{movie_id}"
    params = {"api_key": api_key}

    try:
        response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)

        # 429 — rate limited, wait then retry
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 10))
            logger.warning(f"Rate limited (429) sa movie {movie_id} — naghihintay ng {retry_after}s")
            time.sleep(retry_after)
            # Retry isang beses lang
            response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)

        # 404 — movie hindi nahanap sa TMDB
        if response.status_code == 404:
            logger.warning(f"Movie {movie_id} hindi nahanap sa TMDB (404) — skipping")
            return None

        # Iba pang HTTP errors
        if response.status_code != 200:
            logger.warning(f"HTTP {response.status_code} para sa movie {movie_id} — skipping")
            return None

        data = response.json()

        budget = data.get("budget", 0) or 0
        revenue = data.get("revenue", 0) or 0

        # Genres: list of dicts na may 'name' key → join sa comma-separated string
        genres_list = data.get("genres", []) or []
        genres_str = ", ".join(g["name"] for g in genres_list if "name" in g)

        return {
            "movie_id": int(movie_id),
            "budget": budget,
            "revenue": revenue,
            "genres": genres_str,
        }

    except requests.exceptions.RequestException as e:
        logger.warning(f"Connection error para sa movie {movie_id}: {e} — skipping")
        return None


def main():
    """
    Main function — i-enrich ang movies mula sa Bronze gamit ang TMDB API.
    Steps: kunin candidates, tawagan API, i-save sa silver.movies_enriched.
    """
    logger.info("=== Simula ng Silver Enrichment ===")

    try:
        engine = get_engine()
        logger.info("Database connection established")

        api_key = os.environ["TMDB_API_KEY"]
        logger.info("TMDB API key loaded")

        # Step 1: Kunin ang candidates
        candidate_ids = get_candidates(engine)
        total = len(candidate_ids)

        if total == 0:
            logger.info("Walang candidates para i-enrich — walang missing budget/revenue/genres")
            logger.info("=== Silver Enrichment TAPOS NA ===")
            return

        # Step 2: TRUNCATE silver.movies_enriched bago mag-insert (idempotent)
        with engine.connect() as conn:
            conn.execute(text("TRUNCATE TABLE silver.movies_enriched"))
            conn.commit()
        logger.info("Na-truncate ang silver.movies_enriched")

        # Step 3: Tawagan ang TMDB API para sa bawat candidate
        # Gamit ang requests.Session() para sa connection pooling
        session = requests.Session()
        enriched_results = []
        skipped = 0
        api_calls = 0

        for i, movie_id in enumerate(candidate_ids, 1):
            result = call_tmdb_api(session, movie_id, api_key)
            api_calls += 1

            if result is not None:
                # Skip kung walang useful data (budget=0 AND revenue=0 AND empty genres)
                if result["budget"] > 0 or result["revenue"] > 0 or result["genres"]:
                    enriched_results.append(result)
                else:
                    skipped += 1
            else:
                skipped += 1

            # Progress logging every 500 movies
            if i % PROGRESS_INTERVAL == 0:
                pct = (i / total) * 100
                logger.info(f"Progress: {i}/{total} movies ({pct:.1f}%)")

            # Rate limiting — 50ms delay between requests
            time.sleep(REQUEST_DELAY)

        session.close()

        # Step 4: I-save ang results sa silver.movies_enriched
        logger.info(f"API calls tapos na. Enriched: {len(enriched_results)}, Skipped: {skipped}")

        if enriched_results:
            df = pd.DataFrame(enriched_results)
            df.to_sql(
                name="movies_enriched",
                schema="silver",
                con=engine,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=5000,
            )
            logger.info(f"Na-insert ang {len(df)} enriched rows sa silver.movies_enriched")

        # Step 5: Verify row count
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM silver.movies_enriched"))
            db_count = result.scalar()

        logger.info(f"Verification: silver.movies_enriched = {db_count} rows")

        # Step 6: Assert — dapat may na-enrich na rows
        assert db_count > 0, (
            "Zero rows na-enrich mula sa TMDB API — "
            "baka may problema sa API key o network connectivity"
        )

        # Summary
        still_missing = total - len(enriched_results)
        logger.info("=== Silver Enrichment SUMMARY ===")
        logger.info(f"  Total candidates queried: {total}")
        logger.info(f"  Successfully enriched (written to table): {len(enriched_results)}")
        logger.info(f"  Skipped (404/error/no useful data): {skipped}")
        logger.info(f"  Still missing after enrichment: {still_missing}")
        logger.info(f"  API calls made: {api_calls}")
        logger.info("=== Silver Enrichment TAPOS NA ===")

    except Exception as e:
        logger.error(f"Error sa Silver Enrichment: {e}")
        raise


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # Non-zero exit code para ma-detect ng Airflow BashOperator ang failure
        logger.error(f"Unhandled exception: {e}")
        sys.exit(1)
