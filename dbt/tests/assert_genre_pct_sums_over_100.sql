-- assert_genre_pct_sums_over_100
-- Genre % share uses DISTINCT movie_id per genre against total distinct movies.
-- Because one movie can have multiple genres, the sum of pct_of_movies > 100% is expected.
-- This test asserts it IS greater than 100 — if it's <= 100, something broke in the logic.
-- Returns a row (failure) if the sum is unexpectedly <= 100. Expect 0 rows (i.e. sum > 100).

SELECT SUM(pct_of_movies) AS total_pct
FROM {{ ref('mart_genre_share') }}
HAVING SUM(pct_of_movies) <= 100
