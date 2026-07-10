## 📥 Data Ingestion & Staging (API to MongoDB)

This section contains the Python script responsible for streaming real-time earthquake data from the USGS API into MongoDB. It includes built-in deduplication to ensure data integrity.

### 💻 Python Ingestion Script

```python
import requests
from pymongo import MongoClient
import time
import certifi

# Initialize MongoDB Client
client = MongoClient("mongodb://localhost:27017/")
db = client["earthquick_db"]
collection = db["raw_earthquick_data"]

# USGS API Endpoint (All Earthquakes from the Past Day)
url = "[https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson](https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson)"
print("Earthquake data is streaming...")

while True:
    try:
        response = requests.get(url)
        data = response.json()
        events = data.get("features", [])
        new_record_count = 0
        
        for event in events:
            unique_id = event.get("id")
            
            # Idempotency check: Check if the record already exists
            if not collection.find_one({"id": unique_id}):
                collection.insert_one(event)
                new_record_count += 1
                
        if new_record_count > 0:
            print(f"{new_record_count} new records loaded into MongoDB")
        else:
            print("Checking API... No new records found.")
            
        time.sleep(30)  # Poll the API every 30 seconds
        
    except Exception as e:
        print(f"Error occurred: {e}")
        time.sleep(10)  # Wait 10 seconds before retrying on failure
## ☁️ Data Warehouse Migration (MongoDB to Google BigQuery)

This phase extracts the raw JSON documents from MongoDB, flattens the nested structures into a tabular format using Pandas, and loads the clean data directly into **Google BigQuery** using Google Service Account credentials.

### ⚙️ Transformation Details:
* **BSON ObjectID Removal:** The MongoDB default `_id` field is removed to prevent schema compatibility issues in BigQuery.
* **JSON Flattening:** Utilizes `pd.json_normalize()` to flatten the highly nested GeoJSON features (like coordinates and properties) into relational columns.
* **Schema Standardization:** Replaces dots (`.`) with underscores (`_`) in column names (`df.columns.str.replace('.', '_')`) to make them compliant with BigQuery's column naming conventions.

### 💻 MongoDB to BigQuery Migration Script

```python
from pymongo import MongoClient
import pandas as pd
from google.oauth2 import service_account

# Connect to Local MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["earthquick_db"]
collection = db["raw_earthquick_data"]

# Fetch all raw records from Mongo
raw_events = list(collection.find())
print(f"Found raw records: {len(raw_events)}")

if len(raw_events) == 0:
    print("No records found to migrate.")
else:
    # Data Cleaning and Flattening
    for event in raw_events:
        if '_id' in event:
            del event['_id']  # Remove MongoDB internal BSON ID
            
    # Flatten nested GeoJSON structures into a DataFrame
    df = pd.json_normalize(raw_events)
    
    # Sanitize column names for BigQuery compatibility
    df.columns = df.columns.str.replace('.', '_', regex=False)
    
    # BigQuery Connection & Authentication Configuration
    KEY_PATH = "path/to/your/service_account_credentials.json"
    credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
    
    PROJECT_ID = "dbt-learning-439706"
    DATASET_ID = "Earthquake_raw"
    TABLE_ID = "raw_usgs_data"
    FULL_PATH = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    # Load DataFrame into BigQuery Warehouse
    print("Uploading data to Google BigQuery...")
    df.to_gbq(
        destination_table=FULL_PATH,
        credentials=credentials,
        if_exists="replace",  # Overwrite or update staging data
        progress_bar=True
    )
    print("Migration completed successfully!")
## 🏗️ Analytics Engineering (dbt Cloud Transformations)

This layer implements modular, version-controlled SQL data transformation using **dbt Cloud** on top of Google BigQuery. The architecture transitions data from a raw state into a structured **Star Schema** production-ready data model.

### 📈 Data Lineage & Modeling Flow
The pipeline moves data through a structured three-tier architecture:
1. **Staging Layer (`stg_earthquake`):** Casts raw semi-structured types into correct strict SQL types and cleans field names.
2. **Intermediate Layer (`int_earthquake_enriched`):** Performs advanced data enhancement, array indexing for spatial elements, feature engineering, and KPI calculations.
3. **Marts Layer:** Decouples entities into optimized Dimensional Models (**Dimension** and **Fact** tables) for downstream BI tools.

---

### 1️⃣ Staging Model (`earthquake.sql`)
* **Objective:** Base cleaning, schema casting (`cast` to `string`, `float64`, `int64`), handle nulls via `coalesce()`, and timestamp normalization via `timestamp_millis()`.

```sql
{{ config(materialized='view') }}

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

with staging_data as (
    select * from {{ ref('earthquake') }}
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
        *,
        -- Feature Engineering: Severity Classification
        case 
            when magnitude < 3.0 then 'Micro/Minor (< 3.0)'
            when magnitude >= 3.0 and magnitude < 5.0 then 'Light/Moderate (3.0 - 4.9)'
            when magnitude >= 5.0 and magnitude < 7.0 then 'Strong/Strong (5.0 - 6.9)'
            else 'Major/Great (>= 7.0)'
        end as magnitude_severity_class,
        
        -- Feature Engineering: Depth Categories
        case
            when depth_km <= 70 then 'Shallow (0-70 km)'
            when depth_km > 70 and depth_km <= 300 then 'Intermediate (70-300 km)'
            else 'Deep (> 300 km)'
        end as depth_category,
        
        -- Spatial Extract: Dynamic Location Split
        trim(array_reverse(split(place_description, ','))[safe_offset(0)]) as estimated_location_country_or_state,
        
        -- Time Extracts & Reporting KPIs
        extract(hour from event_timestamp) as event_hour,
        extract(dayofweek from event_timestamp) as event_day_of_week,
        format_timestamp('%B', event_timestamp) as event_month_name,
        timestamp_diff(updated_timestamp, event_timestamp, minute) as pipeline_reporting_delay_minutes,
        
        -- Composite Risk Profile
        case
            when has_tsunami_risk = 1 and magnitude >= 6.0 then 'Critical High Risk'
            when has_tsunami_risk = 1 or magnitude >= 5.0 then 'Elevated Risk'
            else 'Standard Monitoring'
        end as risk_profile_flag
    from extracted_coordinates
)

select * from insights

{{ config(materialized='table', schema='marts') }}

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
{{ config(materialized='table', schema='marts') }}

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


{{ config(materialized='table', schema='marts') }}

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
version: 2

models:
  - name: dim_earthquake_locations
    description: "Geographical dimension table containing exact locations, spatial coordinates, and depth categories."
    columns:
      - name: earthquake_id
        description: "Primary key for the location dimension."
        tests:
          - unique
          - not_null
      - name: estimated_location_country_or_state
        description: "Parsed regional state or country extracted dynamically via regex/array splits."
      - name: depth_category
        description: "Energy dispersion classification based on seismic depth."
        tests:
          - accepted_values:
              values: ['Shallow (0-70 km)', 'Intermediate (70-300 km)', 'Deep (> 300 km)']

  - name: fct_earthquake_events
    description: "Central facts table capturing numerical magnitudes, reporting delay KPIs, and critical structural flags."
    columns:
      - name: earthquake_id
        description: "Foreign key linking directly to the dim_earthquake_locations dimension table."
        tests:
          - unique
          - not_null

<img width="1392" height="760" alt="Screenshot 2026-07-10 170939" src="https://github.com/user-attachments/assets/d6da8b8f-7ea3-4139-a9c0-35b23d5925de" />
<img width="1515" height="542" alt="Screenshot 2026-07-10 170844" src="https://github.com/user-attachments/assets/3623b309-8234-432b-8ead-3acbf3260c64" />
<img width="1515" height="542" alt="Screenshot 2026-07-10 170844" src="https://github.com/user-attachments/assets/0d407a6e-791d-4a34-803f-78086ab9eafe" />
<img width="1791" height="667" alt="Screenshot 2026-07-10 170811" src="https://github.com/user-attachments/assets/7fed9344-decf-476d-b851-cc2326dbaebf" />
<img width="1327" height="712" alt="Screenshot 2026-07-10 165712" src="https://github.com/user-attachments/assets/8225e7ef-b7a3-4d72-a448-70f2b2571860" />
<img width="1846" height="772" alt="Screenshot 2026-07-10 165218" src="https://github.com/user-attachments/assets/e63cf0ee-945c-476a-adeb-0253188fbdf8" />
