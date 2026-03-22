"""
Silver Transform Script
Nagta-transform ng Bronze raw data papunta sa clean Silver tables.
Cast types, dedup, parse mixed date formats, merge TMDB enrichment,
explode genres, production companies, producing countries, at spoken languages
sa separate tables.

Output tables:
- silver.movies — deduplicated, typed, enriched movies
- silver.movie_genres — one row per movie-genre pair
- silver.production_companies — one row per movie-company pair
- silver.producing_countries — one row per movie-country pair (English names via pycountry)
- silver.spoken_languages — one row per movie-language pair (English names via pycountry)
"""

import ast
import os
import sys
import pandas as pd
import pycountry
from loguru import logger
from sqlalchemy import create_engine, text

# === Loguru Configuration ===
logger.remove()
logger.add(sys.stdout, format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}")
logger.add("/logs/silver/silver.log", format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}", rotation="10 MB")

# === Output Tables ===
OUTPUT_TABLES = ["movies", "movie_genres", "production_companies", "producing_countries", "spoken_languages"]

# === Manual Fallback Maps ===
# Para sa ISO codes na wala sa pycountry (historic at non-standard)
COUNTRY_FALLBACK = {
    "SU": "Soviet Union",
    "XC": "Czechoslovakia",
    "XG": "East Germany",
    "YU": "Yugoslavia",
}
LANGUAGE_FALLBACK = {
    "cn": "Cantonese",
    "xx": "No Language",
}


def get_engine():
    """Gawa ng SQLAlchemy engine gamit ang environment variables."""
    db_url = (
        f"postgresql://{os.environ['DB_USER']}:{os.environ['DB_PASSWORD']}"
        f"@{os.environ['DB_HOST']}:{os.environ['DB_PORT']}/{os.environ['DB_NAME']}"
    )
    return create_engine(db_url)


def truncate_output_tables(engine):
    """TRUNCATE lahat ng output tables bago mag-insert — idempotent pattern."""
    for table_name in OUTPUT_TABLES:
        with engine.connect() as conn:
            conn.execute(text(f"TRUNCATE TABLE silver.{table_name}"))
            conn.commit()
        logger.info(f"Na-truncate ang silver.{table_name}")


def load_source_data(engine):
    """
    Basahin ang source data mula sa bronze at silver.movies_enriched.
    Returns: df_main, df_extended, df_enriched
    """
    with engine.connect() as conn:
        df_main = pd.read_sql(text("SELECT * FROM bronze.movies_main"), conn)
        logger.info(f"Loaded bronze.movies_main: {len(df_main)} rows")

        df_extended = pd.read_sql(text("SELECT * FROM bronze.movie_extended"), conn)
        logger.info(f"Loaded bronze.movie_extended: {len(df_extended)} rows")

        df_enriched = pd.read_sql(text("SELECT * FROM silver.movies_enriched"), conn)
        logger.info(f"Loaded silver.movies_enriched: {len(df_enriched)} rows")

    # Dedup enriched data sa movie_id — safety net kung nag-double run ang enrichment
    before = len(df_enriched)
    df_enriched = df_enriched.drop_duplicates(subset=["movie_id"], keep="first")
    after = len(df_enriched)
    if before != after:
        logger.warning(f"Na-dedup ang movies_enriched: {before} → {after} rows ({before - after} duplicates removed)")

    return df_main, df_extended, df_enriched


def parse_json_column(value):
    """
    Parse Python-style dict string mula sa bronze (single-quoted, not valid JSON).
    Gamitin ang ast.literal_eval — hindi json.loads.

    Edge cases na hina-handle:
    - NULL / None / empty string → []
    - "[]" → []
    - Garbage float values (e.g. "6.0") → isinstance check → []
    - Malformed string → try/except → []
    """
    if not value or not str(value).strip():
        return []
    try:
        result = ast.literal_eval(str(value))
        return result if isinstance(result, list) else []
    except Exception:
        return []


def get_country_name(iso_code):
    """
    I-lookup ang English country name mula sa ISO 3166-1 alpha-2 code.
    Gumagamit ng pycountry; fallback sa COUNTRY_FALLBACK para sa historic codes.
    """
    c = pycountry.countries.get(alpha_2=iso_code)
    if c:
        return c.name
    return COUNTRY_FALLBACK.get(iso_code, iso_code)


def get_language_name(iso_code):
    """
    I-lookup ang English language name mula sa ISO 639-1 code.
    Gumagamit ng pycountry; fallback sa LANGUAGE_FALLBACK para sa non-standard codes.
    """
    lang = pycountry.languages.get(alpha_2=iso_code)
    if lang:
        return lang.name
    return LANGUAGE_FALLBACK.get(iso_code, iso_code)


def transform_movies(engine, df_main, df_enriched):
    """
    Transform bronze.movies_main → silver.movies.
    Dedup, parse dates, cast types, merge enrichment, trim titles.
    """
    logger.info("--- Simula ng silver.movies transform ---")

    df_main["id"] = pd.to_numeric(df_main["id"], errors="coerce")
    df_main = df_main.dropna(subset=["id"])
    rows_before = len(df_main)
    df_main = df_main.drop_duplicates(subset=["id"], keep="first")
    rows_after = len(df_main)
    logger.info(f"Dedup: {rows_before} → {rows_after} rows ({rows_before - rows_after} duplicates removed)")

    date_fmt1 = pd.to_datetime(df_main["release_date"], format="%m/%d/%Y", errors="coerce")
    date_fmt2 = pd.to_datetime(df_main["release_date"], format="%Y-%m-%d", errors="coerce")
    date_fmt3 = pd.to_datetime(df_main["release_date"], format="%d-%m-%Y", errors="coerce")
    parsed_date = date_fmt1.fillna(date_fmt2).fillna(date_fmt3)
    logger.info(f"Date parsing: {parsed_date.notna().sum()} parsed, {parsed_date.isna().sum()} still NULL")
    df_main["release_date"] = pd.to_datetime(parsed_date, errors="coerce")

    df_main["budget"] = pd.to_numeric(df_main["budget"], errors="coerce").replace(0, pd.NA)
    df_main["revenue"] = pd.to_numeric(df_main["revenue"], errors="coerce").replace(0, pd.NA)
    df_main["id"] = df_main["id"].astype("Int64")

    df_merged = df_main.merge(
        df_enriched[["movie_id", "budget", "revenue"]],
        left_on="id",
        right_on="movie_id",
        how="left",
        suffixes=("_bronze", "_enriched"),
    )

    df_merged["budget"] = df_merged["budget_bronze"].fillna(df_merged["budget_enriched"])
    df_merged.loc[df_merged["budget"] == 0, "budget"] = pd.NA

    df_merged["revenue"] = df_merged["revenue_bronze"].fillna(df_merged["revenue_enriched"])
    df_merged.loc[df_merged["revenue"] == 0, "revenue"] = pd.NA

    df_merged["title"] = df_merged["title"].str.strip()

    df_movies = df_merged[["id", "title", "release_date", "budget", "revenue"]].copy()
    df_movies = df_movies.rename(columns={"id": "movie_id"})

    df_movies.to_sql(
        name="movies",
        schema="silver",
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=5000,
    )
    logger.info(f"Na-insert ang {len(df_movies)} rows sa silver.movies")
    assert len(df_movies) > 0, "Zero rows sa silver.movies — may problema sa transform"

    return df_movies


def transform_movie_genres(engine, df_extended, df_enriched):
    """
    Transform bronze.movie_extended genres → silver.movie_genres.
    Fill NULL genres mula sa enrichment, explode comma-separated → one row per genre.
    """
    logger.info("--- Simula ng silver.movie_genres transform ---")

    df_extended["id"] = pd.to_numeric(df_extended["id"], errors="coerce")
    df_extended = df_extended.dropna(subset=["id"])

    df_genres = df_extended[["id", "genres"]].copy()
    df_genres = df_genres.merge(
        df_enriched[["movie_id", "genres"]],
        left_on="id",
        right_on="movie_id",
        how="left",
        suffixes=("_bronze", "_enriched"),
    )

    bronze_genres = df_genres["genres_bronze"].fillna("").str.strip()
    enriched_genres = df_genres["genres_enriched"].fillna("").str.strip()
    df_genres["genres_final"] = bronze_genres.where(bronze_genres != "", enriched_genres)

    df_genres = df_genres[(df_genres["genres_final"] != "") & (df_genres["id"].notna())].copy()

    df_genres["genre"] = df_genres["genres_final"].str.split(",")
    df_exploded = df_genres[["id", "genre"]].explode("genre")
    df_exploded["genre"] = df_exploded["genre"].str.strip()
    df_exploded = df_exploded[df_exploded["genre"].notna() & (df_exploded["genre"] != "")].copy()

    df_exploded = df_exploded.rename(columns={"id": "movie_id"})
    df_exploded = df_exploded[["movie_id", "genre"]]

    df_exploded.to_sql(
        name="movie_genres",
        schema="silver",
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=5000,
    )
    logger.info(f"Na-insert ang {len(df_exploded)} rows sa silver.movie_genres")
    assert len(df_exploded) > 0, "Zero rows sa silver.movie_genres — may problema sa explode"

    return df_exploded


def transform_production_companies(engine, df_extended):
    """
    Transform bronze.movie_extended production_companies → silver.production_companies.
    Explode comma-separated → one row per company per movie.
    """
    logger.info("--- Simula ng silver.production_companies transform ---")

    df_companies = df_extended[["id", "production_companies"]].copy()
    df_companies["id"] = pd.to_numeric(df_companies["id"], errors="coerce")
    df_companies = df_companies.dropna(subset=["id"])

    df_companies = df_companies[
        df_companies["production_companies"].notna()
        & (df_companies["production_companies"].str.strip() != "")
    ].copy()

    df_companies["company_name"] = df_companies["production_companies"].str.split(",")
    df_exploded = df_companies[["id", "company_name"]].explode("company_name")
    df_exploded["company_name"] = df_exploded["company_name"].str.strip()
    df_exploded = df_exploded[df_exploded["company_name"].notna() & (df_exploded["company_name"] != "")].copy()

    df_exploded = df_exploded.rename(columns={"id": "movie_id"})
    df_exploded = df_exploded[["movie_id", "company_name"]]

    df_exploded.to_sql(
        name="production_companies",
        schema="silver",
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=5000,
    )
    logger.info(f"Na-insert ang {len(df_exploded)} rows sa silver.production_companies")
    assert len(df_exploded) > 0, "Zero rows sa silver.production_companies — may problema sa explode"

    return df_exploded


def transform_producing_countries(engine, df_extended, df_enriched):
    """
    Transform bronze.movie_extended production_countries → silver.producing_countries.
    Parse Python-style JSON, fill NULLs from enrichment, explode,
    look up English country names via pycountry (+ manual fallback for historic codes).
    One row per movie-country pair.
    """
    logger.info("--- Simula ng silver.producing_countries transform ---")

    df_countries = df_extended[["id", "production_countries"]].copy()
    df_countries["id"] = pd.to_numeric(df_countries["id"], errors="coerce")
    df_countries = df_countries.dropna(subset=["id"])

    # Merge with enrichment to fill empty arrays
    # enriched production_countries is pipe-delimited "ISO:name" pairs
    df_countries = df_countries.merge(
        df_enriched[["movie_id", "production_countries"]].rename(
            columns={"production_countries": "production_countries_enriched"}
        ),
        left_on="id",
        right_on="movie_id",
        how="left",
    )

    rows = []
    for _, row in df_countries.iterrows():
        movie_id = int(row["id"])

        # Try bronze first
        parsed = parse_json_column(row["production_countries"])

        # If bronze empty, try enrichment pipe-delimited string
        if not parsed:
            enriched_raw = row.get("production_countries_enriched", "")
            if enriched_raw and isinstance(enriched_raw, str) and enriched_raw.strip():
                for pair in enriched_raw.split("|"):
                    parts = pair.split(":", 1)
                    if len(parts) == 2 and parts[0].strip():
                        iso = parts[0].strip()
                        rows.append({
                            "movie_id": movie_id,
                            "iso_country_code": iso,
                            "country_name": get_country_name(iso),
                        })
            continue

        # Parse bronze JSON array
        for entry in parsed:
            iso = entry.get("iso_3166_1", "").strip()
            if not iso:
                continue
            rows.append({
                "movie_id": movie_id,
                "iso_country_code": iso,
                "country_name": get_country_name(iso),
            })

    df_result = pd.DataFrame(rows)

    if df_result.empty:
        raise AssertionError("Zero rows sa silver.producing_countries — may problema sa parse")

    # Drop any rows with empty iso or name (should not happen but safety net)
    df_result = df_result[
        df_result["iso_country_code"].notna() & (df_result["iso_country_code"] != "")
        & df_result["country_name"].notna() & (df_result["country_name"] != "")
    ].copy()

    df_result.to_sql(
        name="producing_countries",
        schema="silver",
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=5000,
    )
    logger.info(f"Na-insert ang {len(df_result)} rows sa silver.producing_countries")
    assert len(df_result) > 0, "Zero rows sa silver.producing_countries — may problema sa transform"

    return df_result


def transform_spoken_languages(engine, df_extended, df_enriched):
    """
    Transform bronze.movie_extended spoken_languages → silver.spoken_languages.
    Parse Python-style JSON, fill NULLs from enrichment, explode,
    look up English language names via pycountry (+ manual fallback for cn, xx).
    One row per movie-language pair.
    """
    logger.info("--- Simula ng silver.spoken_languages transform ---")

    df_langs = df_extended[["id", "spoken_languages"]].copy()
    df_langs["id"] = pd.to_numeric(df_langs["id"], errors="coerce")
    df_langs = df_langs.dropna(subset=["id"])

    # Merge with enrichment to fill empty arrays
    df_langs = df_langs.merge(
        df_enriched[["movie_id", "spoken_languages"]].rename(
            columns={"spoken_languages": "spoken_languages_enriched"}
        ),
        left_on="id",
        right_on="movie_id",
        how="left",
    )

    rows = []
    for _, row in df_langs.iterrows():
        movie_id = int(row["id"])

        # Try bronze first
        parsed = parse_json_column(row["spoken_languages"])

        # If bronze empty, try enrichment pipe-delimited string
        if not parsed:
            enriched_raw = row.get("spoken_languages_enriched", "")
            if enriched_raw and isinstance(enriched_raw, str) and enriched_raw.strip():
                for pair in enriched_raw.split("|"):
                    parts = pair.split(":", 1)
                    if len(parts) == 2 and parts[0].strip():
                        iso = parts[0].strip()
                        rows.append({
                            "movie_id": movie_id,
                            "iso_language_code": iso,
                            "language_name": get_language_name(iso),
                        })
            continue

        # Parse bronze JSON array
        for entry in parsed:
            iso = entry.get("iso_639_1", "").strip()
            if not iso:
                continue
            rows.append({
                "movie_id": movie_id,
                "iso_language_code": iso,
                "language_name": get_language_name(iso),
            })

    df_result = pd.DataFrame(rows)

    if df_result.empty:
        raise AssertionError("Zero rows sa silver.spoken_languages — may problema sa parse")

    df_result = df_result[
        df_result["iso_language_code"].notna() & (df_result["iso_language_code"] != "")
        & df_result["language_name"].notna() & (df_result["language_name"] != "")
    ].copy()

    df_result.to_sql(
        name="spoken_languages",
        schema="silver",
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=5000,
    )
    logger.info(f"Na-insert ang {len(df_result)} rows sa silver.spoken_languages")
    assert len(df_result) > 0, "Zero rows sa silver.spoken_languages — may problema sa transform"

    return df_result


def verify_counts(engine):
    """
    Final verification — i-count ang actual rows sa bawat output table.
    Assert lahat > 0 para siguradong may data.
    """
    logger.info("--- Final Verification ---")
    counts = {}

    for table_name in OUTPUT_TABLES:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM silver.{table_name}"))
            count = result.scalar()
        counts[table_name] = count
        logger.info(f"silver.{table_name}: {count} rows")
        assert count > 0, f"silver.{table_name} may 0 rows — may problema sa transform"

    return counts


def main():
    """
    Main function — i-transform ang Bronze data papunta sa Silver output tables.
    Steps: truncate, load, transform, write, verify.
    """
    logger.info("=== Simula ng Silver Transform ===")

    try:
        engine = get_engine()
        logger.info("Database connection established")

        truncate_output_tables(engine)
        df_main, df_extended, df_enriched = load_source_data(engine)

        transform_movies(engine, df_main, df_enriched)
        transform_movie_genres(engine, df_extended, df_enriched)
        transform_production_companies(engine, df_extended)
        transform_producing_countries(engine, df_extended, df_enriched)
        transform_spoken_languages(engine, df_extended, df_enriched)

        verify_counts(engine)

        engine.dispose()
        logger.info("=== Silver Transform TAPOS NA ===")

    except Exception as e:
        logger.error(f"Error sa Silver Transform: {e}")
        raise


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Unhandled exception: {e}")
        sys.exit(1)
