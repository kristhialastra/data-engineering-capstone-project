"""
Movie Pipeline DAG — Medallion Architecture (Bronze → Silver → Gold)

Ito ang IISANG DAG para sa buong movie data pipeline.
Manually triggered — walang schedule, i-trigger lang kapag kailangan.

Layers:
- Bronze: Raw data mula sa GCS bucket, lahat TEXT, walang transforms
- Silver: Typed tables + TMDB API enrichment para sa missing budget/revenue/genres
- Gold: (to be added) Business-ready aggregations gamit dbt

Bawat layer ay naka-TaskGroup para organized sa Airflow UI.
Kung mag-fail ang isang task, hihinto ang buong pipeline — fail loudly.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

# === Default Args ===
# Walang retries — gusto nating makita ang totoong error agad-agad
# depends_on_past=False kasi manual trigger, walang historical dependency
default_args = {
    "owner": "capstone",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}

# === DAG Definition ===
# schedule_interval=None kasi manually triggered lang
# max_active_runs=1 para hindi magka-overlap ang runs (prevents data corruption)
# catchup=False kasi walang schedule na kailangan i-backfill
with DAG(
    dag_id="movie_pipeline",
    description="Medallion pipeline: Bronze → Silver → Gold para sa movie dataset",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["capstone", "medallion", "movies"],
    doc_md=__doc__,
) as dag:

    # =============================================================
    # BRONZE LAYER
    # Raw data mula sa GCS → Postgres bronze schema
    # Lahat ng columns TEXT, walang transformation
    # =============================================================
    with TaskGroup("bronze_tasks") as bronze_tasks:

        # Task 1: Gawa ng bronze schema at tables kung wala pa
        # Lahat ng columns ay TEXT, bawat column may COMMENT
        # IF NOT EXISTS — safe i-rerun kahit existing na ang tables
        ddl_bronze = BashOperator(
            task_id="ddl_bronze",
            bash_command="docker exec pandas-worker python /scripts/bronze/bronze_ddl.py",
            doc="Gawa ng bronze schema at tables. All columns TEXT, with COMMENTs.",
        )

        # Task 2: Basa ng CSVs mula GCS bucket, TRUNCATE + INSERT sa bronze tables
        # dtype=str para lahat ng values ay raw string — walang type inference
        # TRUNCATE muna bago INSERT para idempotent (safe i-rerun)
        load_bronze = BashOperator(
            task_id="load_bronze",
            bash_command="docker exec pandas-worker python /scripts/bronze/bronze_load.py",
            doc="Basa ng CSVs mula GCS bucket, TRUNCATE + INSERT sa bronze tables. "
                "Lahat ng columns naka-TEXT, walang transforms.",
        )

        # Task 3: Pandera validation, row count check vs GCS source, NULL id check
        # Kung may failure, mag-exit(1) ang script — Airflow catches non-zero exit
        # Pipeline stops dito kung hindi valid ang Bronze data
        validate_bronze = BashOperator(
            task_id="validate_bronze",
            bash_command="docker exec pandas-worker python /scripts/bronze/bronze_validate.py",
            doc="Pandera schema validation, row count check vs GCS source, NULL id check. "
                "Kung may failure, pipeline stops. Hindi tayo magpo-proceed sa Silver "
                "kung hindi validated ang Bronze.",
        )

        # Bronze task dependencies — linear, strict ordering
        # Walang parallelism — bawat step depends sa previous
        ddl_bronze >> load_bronze >> validate_bronze

    # =============================================================
    # SILVER LAYER
    # Typed tables, TMDB API enrichment, transform, at validation
    # =============================================================
    with TaskGroup("silver_tasks") as silver_tasks:

        # Task 1: Gawa ng silver schema at typed tables kung wala pa
        # INTEGER, DATE, NUMERIC columns — hindi na lahat TEXT tulad ng Bronze
        # IF NOT EXISTS — safe i-rerun kahit existing na ang tables
        ddl_silver = BashOperator(
            task_id="ddl_silver",
            bash_command="docker exec pandas-worker python /scripts/silver/silver_ddl.py",
            doc="Gawa ng silver schema at typed tables (movies, movie_genres, "
                "production_companies, movies_enriched). With COMMENTs.",
        )

        # Task 2: TMDB API enrichment para sa movies na may missing budget/revenue/genres
        # Binabasa ang bronze rows na may 0/NULL values, tinatawagan ang TMDB API
        # Results na-save sa silver.movies_enriched — supplement table, hindi replacement
        # ~38K candidates at 20 req/sec = ~30 minutes runtime — normal ito
        enrich_silver = BashOperator(
            task_id="enrich_silver",
            bash_command="docker exec pandas-worker python /scripts/silver/silver_enrich.py",
            doc="TMDB API enrichment: basahin ang bronze rows na may missing "
                "budget/revenue/genres, tawagin ang TMDB API, at i-save ang "
                "results sa silver.movies_enriched.",
        )

        # Task 3: Transform bronze data → silver output tables
        # Cast types, dedup IDs, parse mixed date formats, merge TMDB enrichment
        # Explode genres at production companies sa separate tables
        # Writes to silver.movies, silver.movie_genres, silver.production_companies
        transform_silver = BashOperator(
            task_id="transform_silver",
            bash_command="docker exec pandas-worker python /scripts/silver/silver_transform.py",
            doc="Transform bronze data: cast types, dedup, null handling, trim strings, "
                "parse nested fields. Writes to silver.movies, silver.movie_genres, "
                "silver.production_companies.",
        )

        # Task 4: Pandera schema validation ng lahat ng silver tables
        # Column types, nullability rules, value ranges, uniqueness check
        # Kung may failure, mag-exit(1) ang script — pipeline stops dito
        # Hindi tayo magpo-proceed sa Gold kung hindi validated ang Silver
        validate_silver = BashOperator(
            task_id="validate_silver",
            bash_command="docker exec pandas-worker python /scripts/silver/silver_validate.py",
            doc="Pandera schema validation ng lahat ng silver tables: column types, "
                "nullability rules, value ranges, uniqueness. SchemaError raised → "
                "pipeline stops. Hindi tayo magpo-proceed sa Gold kung hindi validated.",
        )

        # Silver task dependencies — linear, strict ordering
        # DDL → Enrich (TMDB API) → Transform (clean/dedup/explode) → Validate
        ddl_silver >> enrich_silver >> transform_silver >> validate_silver

    # =============================================================
    # GOLD LAYER
    # dbt models: staging views → intermediate views → mart tables
    # Runs inside dbt-model container at /usr/app/movies
    # =============================================================
    with TaskGroup("gold_tasks") as gold_tasks:

        # Task 1: dbt run — materialize all staging, intermediate, and mart models
        # Staging = views (always fresh), Intermediate = views, Marts = tables
        # Targets gold schema sa PostgreSQL
        dbt_run = BashOperator(
            task_id="dbt_run",
            bash_command="docker exec dbt-model dbt run --project-dir /usr/app/movies --profiles-dir /usr/app/movies",
            doc="dbt run: materialize staging views, intermediate views, and mart tables "
                "into the gold schema. Sources from silver layer via source() macro.",
        )

        # Task 2: dbt test — run generic tests (not_null, unique, accepted_values)
        # and singular tests (scope validation, orphan bridges, YoY math, etc.)
        # Kung may failure, mag-exit(1) — pipeline stops dito
        dbt_test = BashOperator(
            task_id="dbt_test",
            bash_command="docker exec dbt-model dbt test --project-dir /usr/app/movies --profiles-dir /usr/app/movies",
            doc="dbt test: run generic tests (not_null, unique, accepted_values) and "
                "singular tests (scope years, orphan bridges, genre pct logic, "
                "budget tier coverage, YoY delta math, service restriction regions).",
        )

        dbt_run >> dbt_test

    # =============================================================
    # CROSS-LAYER DEPENDENCIES
    # Bronze must pass validation bago mag-start ang Silver
    # Silver must pass validation bago mag-start ang Gold
    # =============================================================
    bronze_tasks >> silver_tasks >> gold_tasks
