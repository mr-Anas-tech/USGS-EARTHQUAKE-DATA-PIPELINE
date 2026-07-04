{{
    config(
        materialized='view'
    )
}}

with raw_source as (
    select * from {{ source('Earthquake_raw', 'raw_usgs_data') }}
),

cleaned_eq as (
    select
        cast(id as string) as earthquake_id,
        cast(properties_type as string) as event_type,
        coalesce(cast(properties_mag as float64), 0) as magnitude,
        coalesce(cast(properties_magType as string), 'Unknown') as magnitude_type,
        
        cast(properties_place as string) as place_description,
        cast(properties_title as string) as event_title,
        cast(properties_status as string) as review_status,
        timestamp_millis(cast(properties_time as int64)) as event_timestamp,
        timestamp_millis(cast(properties_updated as int64)) as updated_timestamp,
        coalesce(cast(properties_felt as int64), 0) as total_felt_reports,
        cast(properties_cdi as float64) as max_reported_intensity,
        cast(properties_mmi as float64) as instrumental_intensity,
        cast(properties_alert as string) as alert_level,
        cast(properties_tsunami as int64) as has_tsunami_risk,
        cast(properties_sig as int64) as significance_score,
        cast(properties_net as string) as network_id,
        cast(properties_code as string) as network_event_code,
        coalesce(cast(properties_nst as int64), 0) as total_stations_used,
        coalesce(cast(properties_dmin as float64), 0) as min_station_distance,
        coalesce(cast(properties_rms as float64), 0) as rms_travel_time,
        coalesce(cast(properties_gap as float64), 0) as azimuthal_gap,
        cast(geometry_type as string) as geometry_type,
        geometry_coordinates as coordinates_array

    from raw_source
)

select * from cleaned_eq