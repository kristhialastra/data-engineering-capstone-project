-- assert_yoy_delta_math
-- Verify LAG arithmetic: yoy_movie_count_delta must equal
-- movie_count - prev_year_movie_count for every year where prev is not NULL.
-- Returns rows where the calculation is inconsistent — expect 0.

SELECT
    release_year,
    movie_count,
    prev_year_movie_count,
    yoy_movie_count_delta,
    (movie_count - prev_year_movie_count) AS expected_delta
FROM {{ ref('mart_yearly_trends') }}
WHERE prev_year_movie_count IS NOT NULL
  AND yoy_movie_count_delta != (movie_count - prev_year_movie_count)
