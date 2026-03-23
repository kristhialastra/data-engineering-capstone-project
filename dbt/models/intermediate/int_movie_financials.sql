{{ config(materialized='view') }}

-- int_movie_financials
-- Per-movie financial metrics with LAG/LEAD ranked within release year.
-- Adds budget/revenue tier segmentation and rank-within-year for comparisons.
-- Used by fact_movies to enrich the core movie fact table.

WITH ranked AS (
    SELECT
        movie_id,
        movie_title,
        release_year,
        budget,
        revenue,

        -- Rank movies by revenue within their release year (for "top earner" analysis)
        RANK() OVER (
            PARTITION BY release_year
            ORDER BY revenue DESC NULLS LAST
        ) AS revenue_rank_in_year,

        -- Rank movies by budget within their release year
        RANK() OVER (
            PARTITION BY release_year
            ORDER BY budget DESC NULLS LAST
        ) AS budget_rank_in_year,

        -- LAG/LEAD to get neighboring budget values for anomaly context
        LAG(budget)  OVER (PARTITION BY release_year ORDER BY budget DESC NULLS LAST) AS higher_budget,
        LEAD(budget) OVER (PARTITION BY release_year ORDER BY budget DESC NULLS LAST) AS lower_budget

    FROM {{ ref('stg_movies') }}
),

tiered AS (
    SELECT
        *,
        {{ classify_budget('budget') }} AS budget_tier,
        {{ classify_budget('revenue') }} AS revenue_tier
    FROM ranked
)

SELECT * FROM tiered
