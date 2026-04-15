-- assert_no_duplicate_genres_per_movie
-- After the DISTINCT fix in stg_movie_genres, no (movie_id, genre) pair
-- should appear more than once in the bridge table.
-- Returns duplicate pairs — expect 0.

SELECT movie_id, genre, COUNT(*) AS occurrences
FROM {{ ref('bridge_movie_genres') }}
GROUP BY movie_id, genre
HAVING COUNT(*) > 1
