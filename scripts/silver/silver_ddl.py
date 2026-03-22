"""
Silver DDL Script
Gumagawa ng silver schema at typed tables sa Postgres.
Hindi na lahat TEXT — may INTEGER, DATE, NUMERIC na ang columns.
Bawat column may COMMENT para madaling maintindihan ng team.
"""

import os
import sys
from loguru import logger
from sqlalchemy import create_engine, text

# === Loguru Configuration ===
# Dalawang sinks: stdout para sa real-time monitoring, file para sa audit trail
logger.remove()
logger.add(sys.stdout, format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}")
logger.add("/logs/silver/silver.log", format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}", rotation="10 MB")

# === Table Definitions ===
# Typed columns na — hindi na lahat TEXT tulad ng Bronze
# Bawat table may column name, SQL type, at description para sa COMMENT
# Ang mga table names dito ay EXACT MATCH sa output ng silver_transform.py
TABLES = {
    "movies": {
        "drop_first": False,
        "columns": {
            "movie_id": {"type": "BIGINT NOT NULL", "comment": "Unique identifier ng movie, deduplicated mula sa bronze.movies_main"},
            "title": {"type": "VARCHAR(500)", "comment": "Opisyal na title ng movie, trimmed"},
            "release_date": {"type": "DATE", "comment": "Petsa ng release, na-parse mula sa mixed formats"},
            "budget": {"type": "NUMERIC(15,2)", "comment": "Budget sa USD — bronze value kung >0, else TMDB enriched value, else NULL"},
            "revenue": {"type": "NUMERIC(15,2)", "comment": "Revenue sa USD — bronze value kung >0, else TMDB enriched value, else NULL"},
        }
    },
    "movie_genres": {
        "drop_first": False,
        "columns": {
            "movie_id": {"type": "BIGINT NOT NULL", "comment": "TMDB movie ID, foreign key sa silver.movies"},
            "genre": {"type": "VARCHAR(100) NOT NULL", "comment": "Isang genre ng movie — one row per genre per movie"},
        }
    },
    "production_companies": {
        "drop_first": False,
        "columns": {
            "movie_id": {"type": "BIGINT NOT NULL", "comment": "TMDB movie ID, foreign key sa silver.movies"},
            "company_name": {"type": "VARCHAR(255) NOT NULL", "comment": "Pangalan ng isang production company — one row per company per movie"},
        }
    },
    # movies_enriched — DROP+CREATE para ma-add ang bagong columns (production_countries, spoken_languages)
    # Safe dahil ang table ay TRUNCATE at fully repopulated sa bawat enrich run
    "movies_enriched": {
        "drop_first": True,
        "columns": {
            "movie_id": {"type": "BIGINT NOT NULL", "comment": "TMDB movie ID, ginagamit para i-join sa silver.movies"},
            "budget": {"type": "NUMERIC(15,2)", "comment": "Budget mula sa TMDB API (puno ng missing values mula bronze)"},
            "revenue": {"type": "NUMERIC(15,2)", "comment": "Revenue mula sa TMDB API (puno ng missing values mula bronze)"},
            "genres": {"type": "TEXT", "comment": "Genres mula sa TMDB API (puno ng NULL genres mula bronze)"},
            "production_countries": {"type": "TEXT", "comment": "Pipe-delimited ISO:name pairs ng production countries mula sa TMDB API"},
            "spoken_languages": {"type": "TEXT", "comment": "Pipe-delimited ISO:name pairs ng spoken languages mula sa TMDB API"},
        }
    },
    "producing_countries": {
        "drop_first": False,
        "columns": {
            "movie_id": {"type": "BIGINT NOT NULL", "comment": "TMDB movie ID, foreign key sa silver.movies"},
            "iso_country_code": {"type": "VARCHAR(10) NOT NULL", "comment": "ISO 3166-1 alpha-2 country code — one row per country per movie"},
            "country_name": {"type": "VARCHAR(255) NOT NULL", "comment": "English country name mula sa pycountry (o manual fallback para sa historic countries)"},
        }
    },
    "spoken_languages": {
        "drop_first": False,
        "columns": {
            "movie_id": {"type": "BIGINT NOT NULL", "comment": "TMDB movie ID, foreign key sa silver.movies"},
            "iso_language_code": {"type": "VARCHAR(10) NOT NULL", "comment": "ISO 639-1 language code — one row per language per movie"},
            "language_name": {"type": "VARCHAR(255) NOT NULL", "comment": "English language name mula sa pycountry (o manual fallback para sa non-standard codes)"},
        }
    },
}


def get_engine():
    """Gawa ng SQLAlchemy engine gamit ang environment variables."""
    db_url = (
        f"postgresql://{os.environ['DB_USER']}:{os.environ['DB_PASSWORD']}"
        f"@{os.environ['DB_HOST']}:{os.environ['DB_PORT']}/{os.environ['DB_NAME']}"
    )
    return create_engine(db_url)


def create_schema(engine):
    """Gawa ng silver schema kung wala pa."""
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS silver"))
        conn.commit()
    logger.info("Schema 'silver' na-create na (o existing na)")


def create_table(engine, table_name, columns, drop_first=False):
    """
    Gawa ng table sa silver schema.
    Kung drop_first=True, i-drop ang existing table bago i-create (para sa schema changes).
    Kung drop_first=False, CREATE TABLE IF NOT EXISTS lang.
    """
    if drop_first:
        with engine.connect() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS silver.{table_name}"))
            conn.commit()
        logger.info(f"Dropped existing silver.{table_name} (drop_first=True)")

    col_defs = ", ".join(
        f'"{col}" {config["type"]}' for col, config in columns.items()
    )

    if drop_first:
        create_sql = f"CREATE TABLE silver.{table_name} ({col_defs})"
    else:
        create_sql = f"CREATE TABLE IF NOT EXISTS silver.{table_name} ({col_defs})"

    with engine.connect() as conn:
        conn.execute(text(create_sql))
        conn.commit()
    logger.info(f"Table 'silver.{table_name}' na-create na (drop_first={drop_first})")


def add_column_comments(engine, table_name, columns):
    """
    Mag-add ng COMMENT sa bawat column para madaling maintindihan
    ng ibang developers kung ano ang laman ng column.
    """
    with engine.connect() as conn:
        for col_name, config in columns.items():
            comment_sql = text(
                f'COMMENT ON COLUMN silver.{table_name}."{col_name}" IS :desc'
            )
            conn.execute(comment_sql, {"desc": config["comment"]})
            logger.info(f'Comment added sa silver.{table_name}."{col_name}": {config["comment"]}')
        conn.commit()


def main():
    """Main function — i-run ang buong Silver DDL process."""
    logger.info("=== Simula ng Silver DDL ===")

    try:
        engine = get_engine()
        logger.info("Database connection established")

        # Step 1: Gawa ng schema
        create_schema(engine)

        # Step 2: Gawa ng bawat table at i-add ang comments
        for table_name, config in TABLES.items():
            create_table(
                engine,
                table_name,
                config["columns"],
                drop_first=config.get("drop_first", False),
            )
            add_column_comments(engine, table_name, config["columns"])

        logger.info("=== Silver DDL TAPOS NA — lahat ng tables at comments na-create na ===")

    except Exception as e:
        logger.error(f"Error sa Silver DDL: {e}")
        raise


if __name__ == "__main__":
    main()
