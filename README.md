================================================================================
      DATA ENGINEERING ARCHITECTURE & PIPELINE SPECIFICATION
================================================================================

### 1. High-Level Data Flow Topology
[USGS API] ---> (Python Ingestion) ---> [MongoDB Atlas (Staging)]
                                              |
                                       (Batch Extraction)
                                              |
                                              v
[BigQuery Warehouse (Production/dbt)] <--- [Python ETL Script]
   |             ^
   |             | (Transformations & Models)
   v             v
[dbt (Cloud/Core)]
   |
   +---> [Python Environment (Exploratory Data Analysis / EDA)]
   |
   +---> [Power BI Desktop/Service (Executive Dashboards)]

---

### 2. Architectural Components & Deep Technical Breakdown

#### Phase A: Real-Time Landing & Staging Layer
* **Source System:** USGS (United States Geological Survey) Earthquake API exposing real-time GeoJSON data interfaces.
* **Ingestion Runtime:** Light-weight Python script executing on an automated cron schedule or cloud function framework.
* **Target Staging Store:** MongoDB (NoSQL Document Store).
* **Strategic Justification (The Cost Mitigation Logic):**
  > **Why MongoDB instead of directly hitting BigQuery continuously?** > Google BigQuery charges costs based on data scanning volume and active slot streaming usage. If your ingestion script frequently polls the USGS API and streams every small incremental batch directly into BigQuery multiple times an hour, it prevents efficient block caching, drastically inflates streaming insert fees, and spikes storage compute execution overhead. 
  > 
  > By placing MongoDB in front as an intermediate buffer layer:
  > 1. You can write highly unstructured, varying GeoJSON payloads into Mongo instantly with zero schema penalty.
  > 2. It acts as a zero-cost or fixed-cost landing zone that handles rapid API poll cycles without inflating BigQuery compute bills.

#### Phase B: Enterprise Data Warehousing & Transformation Layer
* **Data Transport Handler:** Dedicated Python ETL worker that reads clean delta loads from MongoDB, flattens the nested JSON documents into tabular schemas, and batches them into Google BigQuery using optimized append jobs.
* **Core Analytics Warehouse:** Google BigQuery (GCP). Serves as the centralized, enterprise-grade analytical storage layer containing raw historical tables (`raw_earthquakes`).
* **Transformation Engine:** dbt (Data Build Tool). 
  * dbt connects directly to BigQuery, orchestrating standard SQL transformations directly inside the warehouse environment via push-down compute.
  * **Processing Cycle:** dbt reads `raw_earthquakes` -> applies business logic, deduplication, time-zone alignment, and geographical categorization models -> writes the output back into production-ready analytical tables/views (`mart_seismic_summary`).

#### Phase C: Business Intelligence & Advanced Analytics Layer
* **Exploratory Data Analysis (EDA Pipeline):** A Python execution runtime (Jupyter/VS Code) reads the transformed, aggregated schemas directly from BigQuery via the `google-cloud-bigquery` library. This is where advanced statistical sampling, reliability classifications, and trend investigations are developed.
* **Visualization Engine:** Power BI. Connects to the final dbt-curated BigQuery datasets using native GCP connectors (DirectQuery or Scheduled Import Mode). Power BI consumes clean, high-performance structured datasets to build interactive geospatial and temporal tracking dashboards.




### 📥 Data Ingestion & Staging (API to MongoDB)

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










                                      INSIGHTS


### 1. 🚨 Critical Severity Signals & Regional Exposures
* **Venezuela High-Priority Emergency:** The system flags a maximum-severity crisis configuration consisting of 2 high-impact RED Alerts originating within Venezuela. 
* **Hour 22 Temporal Danger Zone:** These 2 critical RED Alerts are tightly bound to an Hour 22 (10:00 PM) execution block, marking late-night hours as a severe structural window.
* **Transnational Hot Zones:** 
  * **China:** Captures 2 critical Orange Alerts, 1 Yellow Alert, and 1 Green Alert, indicating intense environmental impact despite a smaller overall footprint.
  * **The Philippines:** Registers 1 highly critical Orange Alert alongside 16 Green baseline alerts.
* **Geographic Concentrators:** 
  * **Alaska:** Acts as the primary regional data volume driver, recording 6,017 events under 'Standard Monitoring' (avg magnitude 1.47) and 3 premium events flagged as 'Elevated Risk' due to a high average magnitude of 5.17.
  * **Afghanistan:** Exhibits deep-seated, high-intensity profiles, including 1 severe event at a depth of 207.01 km (5.20 magnitude) and 14 routine events averaging a high depth of 180.68 km.

### 2. ⏳ Macro Monthly Volumetric Trends & Non-Seismic Anomalies
* **The June Megacluster:** The database displays massive monthly variance, with June serving as the absolute peak tracking period, logging 10,925 earthquakes, 99 quarry blasts, and 76 explosions.
* **May Environmental Divergence Anomaly:** While May registers fewer total events (7,916), it contains 100% of the dataset's rarest non-seismic anomalies, including a recorded Meteorite impact, Sonic Boom, and Accidental Explosion.
* **July Stabilization Drop-off:** July exhibits a sharp data contraction down to only 847 events, highlighting either a partial month extract or rapid fault line stabilization.

### 3. 📉 Diurnal Patterns & The Public Engagement Overlap
* **Operational Peak & Trough:** Event frequency peaks sharply at Hour 8 (8:00 AM) with 941 logged incidents, and hits its systematic cyclical minimum at Hour 22 (10:00 PM) with 760 events.
* **The Hour 7 Public Panic Anomaly:** Citizen engagement patterns do not run parallel to event volume. While Hour 7 has a moderate baseline count (842 events), it generates an overwhelming surge of 7,986 total public reports—the absolute highest in the dataset.
* **The Hour 15 Reporting Spike:** Similarly, Hour 15 shows robust public sensitivity, generating 7,685 public reports across 813 tracked events, validating that localized felt intensities dictate public behavior far more than event frequency.

### 4. 🧠 Depth-Severity Cross-Tabulation & Subsurface Risks
* **Shallow Layer Dominance:** Seismic risk is heavily shallow-centric, with 92.3% of all tracked activity (18,492 out of 20,026 total events) occurring in the Shallow zone (0–70 km).
* **Systemic Energy Baseline:** 'Micro/Minor' magnitude events (< 3.0) represent the dominant structural profile with 17,311 occurrences, heavily concentrated in the shallow crust (16,497 events).
* **Catastrophic Shallow Concentrations:** All 3 recorded 'Major/Great' earthquakes (magnitude >= 7.0) occurred strictly within the Shallow zone (0–70 km), leaving intermediate and deep crustal boundaries entirely free of catastrophic events.
* **Deep Layer Stability:** Deep earthquakes (> 300 km) are rare (125 total events) and structurally bounded to lower energy classes, consisting of 118 Light/Moderate and 7 Strong events, with zero micro or major activity.

### 5. 🛠️ Sensor Reliability & Network Performance Telemetry
* **Network Integrity Breakdown:** 91.5% of the data pipeline is driven by high-reliability tracking (18,330 'Solid' events), while 1,696 events are flagged as 'Risky' due to insufficient tracking stations (< 12) or wide azimuthal gaps (> 180°).
* **Pipeline Latency Paradox:** Highly reliable 'Solid' networks paradoxically suffer from a significantly higher average pipeline reporting delay (~6,672 minutes) compared to low-reliability configurations (~2,566 minutes).
* **Wave Travel Efficiency:** The Root Mean Square (RMS) travel time is slightly higher for high-reliability networks (0.31 seconds) relative to risky sensor configurations (0.20 seconds).
* **Global Dashboard KPI Baselines:** The comprehensive tracking system establishes a final portfolio benchmark of 20K total earthquakes, an overall Average Magnitude of 1.67, an Average Depth of 21.40 km, and an Average Azimuthal Gap of 114.29.


<img width="1311" height="737" alt="Screenshot 2026-07-10 174432" src="https://github.com/user-attachments/assets/ec19628a-4457-4bc8-909b-0dbd1697b1bf" />
<img width="1094" height="373" alt="newplot (11)" src="https://github.com/user-attachments/assets/aa4a68ec-cf37-4dea-8bea-751f7e17315d" />
<img width="1094" height="373" alt="newplot (10)" src="https://github.com/user-attachments/assets/ec0e873c-22d9-4c49-99fa-4d4e7f9bb7b9" />
<img width="1094" height="373" alt="newplot (9)" src="https://github.com/user-attachments/assets/d7ecb723-7982-4755-bc75-a5d7fc9eae6f" />
<img width="1094" height="373" alt="newplot (8)" src="https://github.com/user-attachments/assets/e9d80e66-2cd0-4bb2-8453-c80eacb0a0fb" />
<img width="1094" height="385" alt="newplot (7)" src="https://github.com/user-attachments/assets/071e8204-3dd1-4ef2-a3ee-494a0e02b0d4" />



================================================================================
  COMPREHENSIVE EXECUTIVE SUMMARY (INTEGRATED FOR POWER BI / DOCUMENTATION)

"This analytical pipeline integrates regional indicators, critical early-warning alerts, public responsiveness dynamics, network engineering health, and deep subsurface cross-tabulations into a unified seismic portfolio framework. The global monitoring grid establishes a definitive baseline of 20K total events with an overall average magnitude of 1.67 and a depth profile of 21.40 km. Crucially, the system flags a high-priority emergency signature consisting of 2 maximum-severity Red Alerts localized entirely within Venezuela, executed during an Hour 22 late-night temporal window. This high-risk exposure is accompanied by critical secondary clusters, including 2 Orange Alerts in China, 1 in the Philippines, and massive geographic volume concentration in Alaska, which drives over 6,017 events.

Temporal analysis indicates that June acts as the primary data volume accelerator with 10,925 events, whereas May represents the highest environmental divergence, hosting 100% of the dataset's rare non-seismic anomalies (such as sonic booms and meteorite impacts). Diurnal logs show that event frequency peaks at Hour 8 (941 events) and drops to its minimum at Hour 22 (760 events). However, public panic criteria do not map directly to event volume; Hour 7 and Hour 15 stand out as massive citizen engagement anomalies, generating isolated spikes of 7,986 and 7,685 public reports respectively, driven by localized felt intensities rather than systemic frequency.


-----------------------------------------
DASHBOARD LINK:

https://drive.google.com/file/d/1FKWmC0R2uss1NbE0DWJhK-g1vA_uBUBa/view?usp=sharing
---------------------------------------

From an engineering perspective, 91.5% of the ingestion pipeline maintains 'High Reliability (Solid)' status, although these robust nodes demonstrate an increased data processing latency (~6,672 minutes) relative to smaller, low-reliability stations. Subsurface cross-tabulations validate that structural hazards are profoundly shallow-centric, with 92.3% of all activity occurring between 0–70 km. This shallow crustal boundary houses the entire 17,311 minor baseline events as well as the dataset's most catastrophic exposures—including all 3 recorded Major/Great (>= 7.0 magnitude) earthquakes—confirming that deeper tectonic shifts (> 70 km) remain structurally stable, predictable, and low-risk."
