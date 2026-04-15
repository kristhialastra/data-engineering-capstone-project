{{ config(materialized='table') }}

-- mart_country_summary
-- Country-level movie count with region, subregion, and service restriction flag.
-- Supports the 3-part country filter in Power BI.
-- US dominates (40.3% of rows) — use region/subregion groupings for balanced views.

SELECT
    c.iso_country_code,
    c.country_name,
    c.country_region,
    c.country_subregion,
    c.is_service_restricted,
    COUNT(DISTINCT c.movie_id) AS movie_count
FROM {{ ref('stg_movie_countries') }} c
GROUP BY
    c.iso_country_code,
    c.country_name,
    c.country_region,
    c.country_subregion,
    c.is_service_restricted
ORDER BY movie_count DESC
