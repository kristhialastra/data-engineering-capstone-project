{{ config(materialized='view') }}

-- int_yearly_movie_trends
-- Year-over-year movie production trends with LAG/LEAD window functions.
-- Used by mart_yearly_trends for the YoY analysis visual in Power BI.
-- Intermediate layer: aggregates staging data before final mart presentation.

WITH yearly_agg AS (
    SELECT
        release_year,
        COUNT(DISTINCT movie_id)                                    AS movie_count,
        COUNT(DISTINCT movie_id) FILTER (WHERE budget IS NOT NULL)  AS movies_with_budget,
        COUNT(DISTINCT movie_id) FILTER (WHERE revenue IS NOT NULL) AS movies_with_revenue,
        ROUND(AVG(budget)::NUMERIC, 0)                              AS avg_budget,
        ROUND(AVG(revenue)::NUMERIC, 0)                             AS avg_revenue,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY budget)         AS median_budget,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY revenue)        AS median_revenue
    FROM {{ ref('stg_movies') }}
    GROUP BY release_year
)

SELECT
    release_year,
    movie_count,
    movies_with_budget,
    movies_with_revenue,
    avg_budget,
    avg_revenue,
    median_budget,
    median_revenue,

    -- LAG: previous year values for YoY delta calculation
    LAG(movie_count)  OVER (ORDER BY release_year) AS prev_year_movie_count,
    LAG(avg_budget)   OVER (ORDER BY release_year) AS prev_year_avg_budget,
    LAG(avg_revenue)  OVER (ORDER BY release_year) AS prev_year_avg_revenue,

    -- LEAD: next year values for forward-looking trend
    LEAD(movie_count) OVER (ORDER BY release_year) AS next_year_movie_count,

    -- YoY absolute change
    movie_count - LAG(movie_count) OVER (ORDER BY release_year) AS yoy_movie_count_delta,

    -- YoY % change (NULL for first year)
    ROUND(
        (movie_count - LAG(movie_count) OVER (ORDER BY release_year))::NUMERIC
        / NULLIF(LAG(movie_count) OVER (ORDER BY release_year), 0) * 100,
        2
    ) AS yoy_movie_count_pct,

    -- 3-year rolling average for smoothed trend line
    ROUND(
        AVG(movie_count) OVER (
            ORDER BY release_year
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        )::NUMERIC,
        1
    ) AS rolling_3yr_avg_movies

FROM yearly_agg
ORDER BY release_year
