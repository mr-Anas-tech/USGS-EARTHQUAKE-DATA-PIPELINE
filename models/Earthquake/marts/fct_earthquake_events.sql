{{ config(
    materialized='table',
    schema='marts'
) }}

with intermediate_data as (
    select * from {{ ref('Int_earthquake_enriched') }}
)

select
    earthquake_id,  
    network_id,
    network_event_code,
    event_type,
    magnitude,
    magnitude_type,
    magnitude_severity_class,
    review_status,
    alert_level,
    has_tsunami_risk,
    risk_profile_flag,
    significance_score,
    total_felt_reports,
    max_reported_intensity,
    instrumental_intensity,
    total_stations_used,
    min_station_distance,
    rms_travel_time,
    azimuthal_gap,
    event_timestamp,
    updated_timestamp,
    event_hour,
    event_day_of_week,
    event_month_name,
    pipeline_reporting_delay_minutes
from intermediate_data