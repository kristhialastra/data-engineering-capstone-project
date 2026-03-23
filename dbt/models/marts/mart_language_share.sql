{{ config(materialized='table') }}

-- mart_language_share
-- Language % share: what % of scoped movies use each spoken language.
-- Use % share, not raw counts, to avoid English dominance (68.8%) distorting comparisons.

WITH total AS (
    SELECT COUNT(DISTINCT movie_id) AS total_movies
    FROM {{ ref('stg_movie_languages') }}
)
SELECT
    l.iso_language_code,
    l.language_name,
    COUNT(DISTINCT l.movie_id)                                          AS movie_count,
    total.total_movies,
    {{ pct_of_total('COUNT(DISTINCT l.movie_id)', 'total.total_movies') }} AS pct_of_movies
FROM {{ ref('stg_movie_languages') }} l
CROSS JOIN total
GROUP BY l.iso_language_code, l.language_name, total.total_movies
ORDER BY movie_count DESC
