"""
Silver Validation Script
I-validate ang Silver layer bago mag-proceed sa Gold.
Limang checks: Pandera schema, row counts, null IDs, uniqueness, value ranges.
Kung may nag-fail na check, hindi pwedeng mag-continue ang pipeline.
"""

import os
import sys
import pandas as pd
import pandera as pa
from loguru import logger
from sqlalchemy import create_engine, text

# === Loguru Configuration ===
# Dalawang sinks: stdout para sa real-time monitoring, file para sa audit trail
logger.remove()
logger.add(sys.stdout, format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}")
logger.add("/logs/silver/silver.log", format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}", rotation="10 MB")

# === Pandera Schemas ===
# Isa-isang schema para sa bawat silver table
# strict=True — mag-fail kung may extra o kulang na columns
SCHEMAS = {
    "movies": pa.DataFrameSchema(
        columns={
            "movie_id": pa.Column(int, nullable=False),
            "title": pa.Column(object, nullable=True),
            "release_date": pa.Column("datetime64[ns]", nullable=True),
            "budget": pa.Column(float, nullable=True),
            "revenue": pa.Column(float, nullable=True),
        },
        strict=True,
    ),
    "movie_genres": pa.DataFrameSchema(
        columns={
            "movie_id": pa.Column(int, nullable=False),
            "genre": pa.Column(object, nullable=False),
        },
        strict=True,
    ),
    "production_companies": pa.DataFrameSchema(
        columns={
            "movie_id": pa.Column(int, nullable=False),
            "company_name": pa.Column(object, nullable=False),
        },
        strict=True,
    ),
    "movies_enriched": pa.DataFrameSchema(
        columns={
            "movie_id": pa.Column(int, nullable=False),
            "budget": pa.Column(float, nullable=True),
            "revenue": pa.Column(float, nullable=True),
            "genres": pa.Column(object, nullable=True),
        },
        strict=True,
    ),
}

# Lahat ng silver tables na iva-validate
SILVER_TABLES = ["movies", "movie_genres", "production_companies", "movies_enriched"]


def get_engine():
    """Gawa ng SQLAlchemy engine gamit ang environment variables."""
    db_url = (
        f"postgresql://{os.environ['DB_USER']}:{os.environ['DB_PASSWORD']}"
        f"@{os.environ['DB_HOST']}:{os.environ['DB_PORT']}/{os.environ['DB_NAME']}"
    )
    return create_engine(db_url)


def check_1_pandera_schema(engine):
    """
    Check 1: Pandera Schema Validation.
    Bine-verify na:
    - Lahat ng expected columns ay present (exact names)
    - Walang extra/unexpected columns (strict=True)
    - Bawat column ay may tamang dtype at nullability
    """
    logger.info("Nagsisimula ng Check 1: Pandera Schema Validation...")

    for table_name in SILVER_TABLES:
        # LIMIT 1000 — schema check lang, hindi kailangan i-load ang buong table
        with engine.connect() as conn:
            df = pd.read_sql(
                text(f"SELECT * FROM silver.{table_name} LIMIT 1000"),
                conn,
            )

        if df.empty:
            raise ValueError(
                f"silver.{table_name} ay walang data — hindi ma-validate ang schema. "
                f"I-run muna ang silver_transform.py."
            )

        # I-validate gamit ang Pandera schema
        SCHEMAS[table_name].validate(df)
        logger.info(
            f"  Check 1 PASSED para sa silver.{table_name} — "
            f"{len(df.columns)} columns, types correct"
        )

    logger.info("Check 1: Pandera Schema Validation — PASSED")


def check_2_row_counts(engine):
    """
    Check 2: Row Count > 0.
    Bawat silver table dapat may data — zero rows ibig sabihin nag-fail ang transform.
    Pipeline BLOCKER ito.
    """
    logger.info("Nagsisimula ng Check 2: Row Count > 0...")

    for table_name in SILVER_TABLES:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM silver.{table_name}"))
            count = result.scalar()

        assert count > 0, (
            f"silver.{table_name} may 0 rows — "
            f"baka nag-fail ang transform o enrich step"
        )
        logger.info(f"  Check 2 PASSED para sa silver.{table_name} — {count} rows")

    logger.info("Check 2: Row Count > 0 — PASSED")


def check_3_no_null_movie_ids(engine):
    """
    Check 3: No NULL movie_ids.
    Ang movie_id column ay dapat walang NULL values sa lahat ng silver tables.
    NULL movie_id = broken foreign key — pipeline BLOCKER ito.
    """
    logger.info("Nagsisimula ng Check 3: No NULL movie_ids...")

    for table_name in SILVER_TABLES:
        with engine.connect() as conn:
            result = conn.execute(
                text(f"SELECT COUNT(*) FROM silver.{table_name} WHERE movie_id IS NULL")
            )
            null_count = result.scalar()

        assert null_count == 0, (
            f"{null_count} NULL movie_id(s) found sa silver.{table_name} — "
            f"movie_id must have zero NULLs"
        )
        logger.info(f"  Check 3 PASSED para sa silver.{table_name} — 0 NULL movie_ids")

    logger.info("Check 3: No NULL movie_ids — PASSED")


def check_4_unique_movie_ids_in_movies(engine):
    """
    Check 4: Unique movie_ids sa silver.movies.
    Ang silver.movies ay deduplicated na — dapat walang duplicate movie_ids.
    Duplicate = dedup step nag-fail — pipeline BLOCKER.
    """
    logger.info("Nagsisimula ng Check 4: Unique movie_ids sa silver.movies...")

    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT movie_id, COUNT(*) FROM silver.movies "
            "GROUP BY movie_id HAVING COUNT(*) > 1"
        ))
        duplicates = result.fetchall()

    assert len(duplicates) == 0, (
        f"{len(duplicates)} duplicate movie_id(s) found sa silver.movies — "
        f"dedup step may problema"
    )
    logger.info(f"  Check 4 PASSED — 0 duplicate movie_ids sa silver.movies")

    logger.info("Check 4: Unique movie_ids — PASSED")


def check_5_value_ranges(engine):
    """
    Check 5: Value Ranges.
    5a. Budget at revenue sa silver.movies ay dapat >= 0 (walang negative)
    5b. Walang empty strings sa genre column
    5c. Walang empty strings sa company_name column
    Pipeline BLOCKER — negative financials o empty strings = transform logic error.
    """
    logger.info("Nagsisimula ng Check 5: Value Ranges...")

    # 5a: Budget >= 0
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT COUNT(*) FROM silver.movies WHERE budget < 0")
        )
        neg_budget = result.scalar()

    assert neg_budget == 0, (
        f"{neg_budget} negative budget(s) found sa silver.movies — "
        f"budget must be >= 0"
    )
    logger.info(f"  Check 5a PASSED — 0 negative budgets")

    # 5a: Revenue >= 0
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT COUNT(*) FROM silver.movies WHERE revenue < 0")
        )
        neg_revenue = result.scalar()

    assert neg_revenue == 0, (
        f"{neg_revenue} negative revenue(s) found sa silver.movies — "
        f"revenue must be >= 0"
    )
    logger.info(f"  Check 5a PASSED — 0 negative revenues")

    # 5b: No empty strings sa genre
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT COUNT(*) FROM silver.movie_genres WHERE TRIM(genre) = ''")
        )
        empty_genres = result.scalar()

    assert empty_genres == 0, (
        f"{empty_genres} empty genre string(s) found sa silver.movie_genres — "
        f"genre must not be empty"
    )
    logger.info(f"  Check 5b PASSED — 0 empty genre strings")

    # 5c: No empty strings sa company_name
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT COUNT(*) FROM silver.production_companies WHERE TRIM(company_name) = ''")
        )
        empty_companies = result.scalar()

    assert empty_companies == 0, (
        f"{empty_companies} empty company_name string(s) found sa silver.production_companies — "
        f"company_name must not be empty"
    )
    logger.info(f"  Check 5c PASSED — 0 empty company_name strings")

    logger.info("Check 5: Value Ranges — PASSED")


def main():
    """
    Main function — i-run ang lahat ng 5 validation checks.
    Bawat check ay may sariling try/except para mapatuloy ang iba kahit may mag-fail.
    Sa huli, kung may nag-fail, i-raise ang RuntimeError at exit ng non-zero code
    para ma-detect ng Airflow BashOperator.
    """
    logger.info("=== Simula ng Silver Validation ===")

    engine = get_engine()
    logger.info("Database connection established")

    # Kolektahin ang lahat ng failed checks — i-report lahat sa dulo
    failed_checks = []

    # --- Check 1: Pandera Schema Validation ---
    try:
        check_1_pandera_schema(engine)
    except Exception as e:
        logger.error(f"Check 1 FAILED: {e}")
        failed_checks.append(f"Check 1 (Pandera Schema): {e}")

    # --- Check 2: Row Count > 0 ---
    try:
        check_2_row_counts(engine)
    except Exception as e:
        logger.error(f"Check 2 FAILED: {e}")
        failed_checks.append(f"Check 2 (Row Counts): {e}")

    # --- Check 3: No NULL movie_ids ---
    try:
        check_3_no_null_movie_ids(engine)
    except Exception as e:
        logger.error(f"Check 3 FAILED: {e}")
        failed_checks.append(f"Check 3 (No NULL movie_ids): {e}")

    # --- Check 4: Unique movie_ids in silver.movies ---
    try:
        check_4_unique_movie_ids_in_movies(engine)
    except Exception as e:
        logger.error(f"Check 4 FAILED: {e}")
        failed_checks.append(f"Check 4 (Unique movie_ids): {e}")

    # --- Check 5: Value Ranges ---
    try:
        check_5_value_ranges(engine)
    except Exception as e:
        logger.error(f"Check 5 FAILED: {e}")
        failed_checks.append(f"Check 5 (Value Ranges): {e}")

    # I-dispose ang engine para linisin ang connections
    engine.dispose()

    # --- Final Summary ---
    logger.info("=== Silver Validation SUMMARY ===")

    if failed_checks:
        for fail in failed_checks:
            logger.error(f"  FAILED: {fail}")
        logger.error("Silver validation FAILED. Huwag mag-proceed sa Gold.")
        raise RuntimeError(f"Nabigong Silver checks: {failed_checks}")
    else:
        logger.info("  Check 1: Pandera Schema Validation — PASSED")
        logger.info("  Check 2: Row Count > 0 — PASSED")
        logger.info("  Check 3: No NULL movie_ids — PASSED")
        logger.info("  Check 4: Unique movie_ids — PASSED")
        logger.info("  Check 5: Value Ranges — PASSED")
        logger.info("Silver layer validated. Ligtas nang pumunta sa Gold.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # Non-zero exit code para ma-detect ng Airflow BashOperator ang failure
        logger.error(f"Unhandled exception: {e}")
        sys.exit(1)
