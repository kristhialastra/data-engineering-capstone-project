{{ config(materialized='table') }}

-- bridge_movie_companies
-- Many-to-many bridge: movie ↔ production company.
-- Used for company count measure in Power BI.

SELECT
    movie_id,
    company_name
FROM {{ ref('stg_movie_companies') }}
