{{ config(
    materialized='table',
    schema='marts'
) }}

with intermediate_data as (
    select * from {{ ref('Int_earthquake_enriched') }}
)

select
    earthquake_id,  
    place_description,
    event_title,
    geometry_type,
    longitude,
    latitude,
    depth_km,
    depth_category,
    estimated_location_country_or_state
from intermediate_data