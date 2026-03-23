{{ config(materialized='table') }}

-- bridge_movie_genres
-- Many-to-many bridge: movie ↔ genre.
-- Used for genre multiselect filter and genre % share in Power BI.

SELECT
    movie_id,
    genre
FROM {{ ref('stg_movie_genres') }}
