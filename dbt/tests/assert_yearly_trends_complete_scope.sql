-- assert_yearly_trends_complete_scope
-- mart_yearly_trends must have exactly one row per year for every year
-- from 1980 to 2016 — 37 years total, no gaps allowed.
-- Returns missing years — expect 0.

WITH expected_years AS (
    SELECT generate_series(1980, 2016) AS release_year
)
SELECT e.release_year
FROM expected_years e
LEFT JOIN {{ ref('mart_yearly_trends') }} t ON t.release_year = e.release_year
WHERE t.release_year IS NULL
