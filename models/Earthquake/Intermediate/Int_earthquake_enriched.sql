with staging_data as (
    select * from {{ ref('eathquake') }} -- Aapke staging model ka ref
),

extracted_coordinates as (
    select
        *,
        cast(coordinates_array[safe_offset(0)] as float64) as longitude,
        cast(coordinates_array[safe_offset(1)] as float64) as latitude,
        cast(coordinates_array[safe_offset(2)] as float64) as depth_km
    from staging_data
),

insights as (
    select
        earthquake_id,
        event_type,
        magnitude,
        magnitude_type,
        place_description,
        event_title,
        review_status,
        event_timestamp,
        updated_timestamp,
        total_felt_reports,
        max_reported_intensity,
        instrumental_intensity,
        alert_level,
        has_tsunami_risk,
        significance_score,
        network_id,
        network_event_code,
        total_stations_used,
        min_station_distance,
        rms_travel_time,
        azimuthal_gap,
        geometry_type,
        longitude,
        latitude,
        depth_km,
        case 
            when magnitude < 3.0 then 'Micro/Minor (< 3.0)'
            when magnitude >= 3.0 and magnitude < 5.0 then 'Light/Moderate (3.0 - 4.9)'
            when magnitude >= 5.0 and magnitude < 7.0 then 'Strong/Strong (5.0 - 6.9)'
            else 'Major/Great (>= 7.0)'
        end as magnitude_severity_class,
        case 
            when depth_km <= 70 then 'Shallow (0-70 km)'
            when depth_km > 70 and depth_km <= 300 then 'Intermediate (70-300 km)'
            else 'Deep (> 300 km)'
        end as depth_category,
        trim(array_reverse(split(place_description, ','))[safe_offset(0)]) as estimated_location_country_or_state,
        extract(hour from event_timestamp) as event_hour,
        extract(dayofweek from event_timestamp) as event_day_of_week,
        format_timestamp('%B', event_timestamp) as event_month_name,
        timestamp_diff(updated_timestamp, event_timestamp, minute) as pipeline_reporting_delay_minutes,
        case 
            when has_tsunami_risk = 1 and magnitude >= 6.0 then 'Critical High Risk'
            when has_tsunami_risk = 1 or magnitude >= 5.0 then 'Elevated Risk'
            else 'Standard Monitoring'
        end as risk_profile_flag

    from extracted_coordinates
)

select * from insights