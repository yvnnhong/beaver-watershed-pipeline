# Beaver Watershed Pipeline — Comprehensive Technical Documentation

**Version:** V6  
**Last Updated:** March 2026  
**Author:** Yvonne Hong  
**Live Dashboard:** https://beaver-watershed-pipeline.streamlit.app  
**GitHub:** https://github.com/yvnnhong/beaver-watershed-pipeline

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Research Question and Findings](#2-research-question-and-findings)
3. [Final Architecture](#3-final-architecture)
4. [Data Sources](#4-data-sources)
5. [AWS Services — What Each One Does](#5-aws-services--what-each-one-does)
6. [Lambda Functions — Detailed Breakdown](#6-lambda-functions--detailed-breakdown)
7. [Step Functions State Machine](#7-step-functions-state-machine)
8. [Database Schema](#8-database-schema)
9. [Spatial Join — How It Works](#9-spatial-join--how-it-works)
10. [Anomaly Detection — How It Works](#10-anomaly-detection--how-it-works)
11. [Streamlit Dashboard](#11-streamlit-dashboard)
12. [Data Quality and Validation](#13-data-quality-and-validation)
13. [Infrastructure Details](#13-infrastructure-details)
14. [Common Commands Reference](#14-common-commands-reference)
15. [Known Issues and Limitations](#15-known-issues-and-limitations)
16. [Design Decisions and Tradeoffs](#16-design-decisions-and-tradeoffs)

---

## 1. Project Overview

### What This Project Does

The Beaver Watershed Pipeline is a weekly-automated AWS data engineering pipeline that:

1. Ingests beaver occurrence records from the GBIF biodiversity database (every recorded beaver sighting in the US with GPS coordinates, date, and species)
2. Ingests water quality readings from USGS stream monitoring stations nationwide (dissolved oxygen, water temperature, pH, and turbidity)
3. Spatially joins the two datasets — pairing each beaver sighting with the nearest water quality monitoring station within 500km
4. Runs an Isolation Forest machine learning model to flag stations with anomalous water quality relative to their EPA climate region baseline
5. Surfaces all results via a live Streamlit dashboard with interactive maps, charts, and anomaly visualizations

### Why Beavers?

Beavers (Castor canadensis) are a keystone species — they engineer ecosystems by building dams that create wetlands. These wetlands filter water, reduce flooding, and increase biodiversity. There is active ecological research into using beaver reintroduction as a low-cost water quality improvement strategy. This pipeline asks whether the data supports that hypothesis at scale.

### Who This Is For

Environmental organizations evaluating beaver reintroduction programs, watershed managers monitoring water quality, and researchers studying the relationship between beaver activity and aquatic ecosystem health. The anomaly detection component provides actionable intelligence: specific geographic locations where water quality near beaver habitat is unusual and worth investigating.

### Portfolio Context

This is a Data Engineering portfolio project demonstrating:
- End-to-end AWS pipeline architecture (ingestion → processing → storage → visualization)
- REST API integration with two external data providers
- Serverless compute orchestration with AWS Step Functions
- Geospatial data processing (haversine spatial join)
- Unsupervised ML model deployment as a pipeline step
- Production data quality practices (sentinel value filtering, deduplication, distance capping)

---

## 2. Research Question and Findings

### The Question

Do areas with beaver activity correlate with healthy water quality in nearby US waterways?

### The Data

- **39,926** US beaver occurrence records spatially joined to USGS monitoring stations
- **569** unique USGS stream monitoring stations matched nationwide
- **Average distance** from beaver sighting to nearest matched station: 88.9 km
- Date range: 2020–2024 (5 years of daily USGS sensor readings per station)

### Water Quality Findings

| Parameter | Average | Coverage | Interpretation |
|---|---|---|---|
| Dissolved Oxygen | 9.69 mg/L | 100% | Well above 6.0 mg/L healthy threshold |
| Water Temperature | 14.1°C | 98% | Cool waterways consistent with beaver preference |
| pH | 7.69 | 74% | Neutral-slightly alkaline, ideal for aquatic ecosystems |
| Turbidity | 25.1 FNU | 57% | Moderately clear, right-skewed distribution |

### Key Statistics

- **99.4%** of beaver sightings are near water with dissolved oxygen above 6.0 mg/L (healthy threshold for aquatic life)
- **DO range** near beaver habitat: 2.0 – 14.2 mg/L
- **Correlation** between distance to station and dissolved oxygen: 0.275 (weak positive)
- **902 anomalous records** detected (2.3%) by Isolation Forest — flagged as potential pollution events or habitat degradation

### Interpretation

The weak distance-DO correlation (0.275) suggests beavers broadly associate with healthy water quality rather than specifically near monitoring infrastructure. The consistently high DO values (99.4% above threshold) support the hypothesis that beaver activity correlates with healthy waterways. The right-skewed turbidity distribution (most sightings near clear water, some near murkier conditions) is ecologically consistent — beaver dams trap upstream sediment, reducing downstream turbidity.

---

## 3. Final Architecture

### Complete Pipeline Flow

```
EventBridge Rule
  cron(0 6 ? * SUN *)
  Triggers every Sunday at 6am UTC
        |
        v
AWS Step Functions State Machine
  beaver-pipeline-state-machine
        |
        v
+---------------------------+
|  Lambda 1: data_fetcher   |
|  1024MB | 15min timeout   |
|                           |
|  1. POST to GBIF async    |
|     download API          |
|     -> receive downloadKey|
|     immediately           |
|                           |
|  2. Fetch USGS data for   |
|     all 50 states         |
|     (DO, temp, pH, turb)  |
|     2020-2024              |
|                           |
|  3. Save USGS JSON to S3  |
|     raw bucket             |
|                           |
|  Returns: {               |
|    downloadKey,           |
|    usgs_s3_key            |
|  }                        |
+---------------------------+
        |
        v
+---------------------------+
|  Wait State               |
|  300 seconds (5 minutes)  |
|  FREE — no Lambda running |
|  GBIF prepares zip file   |
|  in background            |
+---------------------------+
        |
        v
+---------------------------+
|  Lambda 2: check_status   |
|  128MB | 30sec timeout    |
|                           |
|  GET GBIF status endpoint |
|  with downloadKey         |
|                           |
|  Returns: {               |
|    status,                |
|    downloadUrl,           |
|    downloadKey,           |
|    usgs_s3_key            |
|  }                        |
+---------------------------+
        |
        v
+---------------------------+
|  Choice State             |
|  IsGBIFReady              |
|                           |
|  status == "SUCCEEDED"?   |
|  YES -> InvokeProcessor   |
|  NO  -> WaitForGBIF       |
|         (loop back)       |
+---------------------------+
        |
        v (SUCCEEDED)
+---------------------------+
|  Lambda 3: processor      |
|  1024MB | 15min timeout   |
|                           |
|  1. Download GBIF zip     |
|     from downloadUrl      |
|     (in memory, no disk)  |
|                           |
|  2. Load USGS JSON        |
|     from S3               |
|                           |
|  3. NumPy haversine       |
|     spatial join          |
|     (500km cap)           |
|                           |
|  4. Bulk INSERT into      |
|     RDS PostgreSQL        |
+---------------------------+
        |
        v
+---------------------------+
|  Lambda 4:                |
|  anomaly_detector         |
|  1024MB | 5min timeout    |
|                           |
|  1. Read all records      |
|     from RDS              |
|                           |
|  2. Z-score normalize     |
|     by EPA climate region |
|                           |
|  3. Fit Isolation Forest  |
|     contamination=5%      |
|                           |
|  4. Write anomaly_score   |
|     back to RDS           |
|     (1=normal, -1=anomaly)|
+---------------------------+
        |
        v
RDS PostgreSQL
beaver_water_joined table
        |
        v
Streamlit Dashboard
beaver-watershed-pipeline.streamlit.app
(auto-deploys on git push to main)
```

### State Machine Execution Time Breakdown

| Step | Duration | Cost |
|---|---|---|
| Lambda 1 (data_fetcher) | ~5-6 minutes | Billed Lambda runtime |
| Wait State | 5 minutes | FREE |
| Lambda 2 (check_status) | ~2 seconds | Billed Lambda runtime |
| Lambda 3 (processor) | ~15 seconds | Billed Lambda runtime |
| Lambda 4 (anomaly_detector) | ~15 seconds | Billed Lambda runtime |
| **Total end-to-end** | **~11 minutes** | |

---

## 4. Data Sources

### GBIF — Global Biodiversity Information Facility

**What it is:** An international network and data infrastructure that aggregates biodiversity occurrence data from institutions worldwide. Contains over 2 billion occurrence records for species globally.

**What we fetch:** All US beaver occurrence records for Castor canadensis (GBIF taxon key 2439838) with GPS coordinates.

**How we fetch it:** GBIF async download API. For datasets over 12,000 records, GBIF requires using their asynchronous download endpoint rather than paginated API calls. The process:
1. POST a download request with a predicate (species + country + has_coordinate filters) to `https://api.gbif.org/v1/occurrence/download/request`
2. Receive a `downloadKey` immediately — GBIF prepares the file in the background
3. Poll `https://api.gbif.org/v1/occurrence/download/{downloadKey}` until status is `SUCCEEDED`
4. Download the zip file from the provided URL, extract the CSV in memory

**Why async instead of paginated?** GBIF throttles paginated requests after ~10,200 records, slowing each request from 1.5 seconds to 50+ seconds per page. The async API bypasses this — one request, GBIF does all the work, we pick it up when ready.

**Data format:** SIMPLE_CSV format. Columns used: species, decimalLatitude, decimalLongitude, year, month, day, stateProvince, countryCode.

**Authentication:** GBIF username and password stored as Lambda environment variables.

### USGS Water Services API

**What it is:** The US Geological Survey operates a nationwide network of stream gauges and water quality monitoring stations. The Water Services API provides access to real-time and historical measurements.

**What we fetch:** Daily average values for 4 parameters across all 50 US states, 2020–2024:

| Parameter | USGS Code | Unit | Valid Range Used |
|---|---|---|---|
| Dissolved oxygen | 00300 | mg/L | 0–20 |
| Water temperature | 00010 | °C | 0–35 |
| pH | 00400 | dimensionless | 0–14 |
| Turbidity | 63680 | FNU | 0–2000 |

**How we fetch it:** One GET request per state to `https://waterservices.usgs.gov/nwis/dv/` with parameters: stateCd, parameterCd (all 4 at once), siteType=ST (streams only), date range, siteStatus=all. Response is JSON containing a timeSeries array — one entry per station per parameter.

**Data volume:** Each state request returns multiple years of daily readings. A large state like Oregon or Georgia can return thousands of individual daily readings across hundreds of stations.

**Sentinel values:** USGS uses -999999 to indicate missing or error readings. These must be filtered out before computing averages (see Data Quality section).

**Rate limiting:** 0.3 second sleep between state requests to avoid overwhelming the USGS API.

---

## 5. AWS Services — What Each One Does

### AWS Lambda

**What it is:** Serverless compute — you upload code, AWS runs it on demand without you managing any servers. You pay only for the milliseconds your code actually runs.

**How memory works:** Lambda memory allocation also controls CPU. 1024MB = more RAM AND faster CPU. This is why Lambda 1 and Lambda 3 are set to 1024MB — the USGS data processing and spatial join are CPU-intensive, not just memory-intensive.

**Timeout:** Maximum 15 minutes (900 seconds) per invocation. Lambda 1 uses almost all of this fetching 50 states of USGS data.

**Lambda layers:** Zip files containing Python packages that get attached to Lambda functions. Lambda's default Python runtime has almost no packages — pandas, numpy, psycopg2, scikit-learn all need to be provided via layers. Layers must be built on Linux (Lambda's runtime environment) — hence Docker on Windows.

**Cold starts:** First invocation after idle period takes slightly longer while AWS spins up the container. Not a concern for a weekly pipeline.

### AWS S3 (Simple Storage Service)

**What it is:** Object storage — store any file (called an object) in a bucket. Think of it as a hard drive in the cloud. Essentially unlimited storage, extremely durable (11 nines), cheap.

**How we use it:**
- `beaver-pipeline-raw`: Stores the USGS JSON data saved by Lambda 1. Lambda 3 reads this file to get the water quality data for the spatial join. S3 acts as the hand-off point between Lambda 1 and Lambda 3 — they can't pass large data directly through Step Functions (payload limit is 256KB).
- `beaver-pipeline-processed`: Available for processed output storage.
- `layers/`: Stores Lambda layer zip files for upload (direct Lambda upload fails for large layers over ~50MB).

**Key:** The S3 object key for USGS data is `usgs/usgs_dissolved_oxygen_all_states.json`. Lambda 1 writes it, Lambda 3 reads it.

### AWS RDS PostgreSQL (Relational Database Service)

**What it is:** Managed PostgreSQL database hosted by AWS. AWS handles backups, patching, and availability. You just connect and run SQL.

**How we use it:** Stores the final joined dataset in the `beaver_water_joined` table. Lambda 3 bulk inserts all records after the spatial join. Lambda 4 reads all records, scores them, and writes anomaly scores back. The Streamlit dashboard reads from RDS via psycopg2 on page load.

**Connection:** psycopg2 library. Host, user, and password stored as Lambda environment variables and Streamlit secrets (not committed to GitHub).

**Always running:** Unlike Lambda (pay per use), RDS runs 24/7 regardless of pipeline activity. This is the main ongoing AWS cost.

**Instance:** Single-AZ deployment sufficient for a portfolio project. Not highly available but much cheaper.

### AWS Step Functions

**What it is:** A serverless orchestration service that coordinates multiple AWS services into workflows called state machines. You define states (tasks, waits, choices, parallel) and transitions between them as JSON.

**Why we use it instead of one Lambda:** The pipeline has a natural wait in the middle — GBIF needs ~5 minutes to prepare the download file. If this wait happened inside a Lambda, we'd pay for 5 minutes of 1024MB Lambda memory doing nothing. Step Functions Wait States are free. Step Functions also handles the polling loop (check status → not ready → wait → check again) cleanly.

**State types used:**
- **Task state:** Invokes a Lambda function, waits for it to return, passes output to next state
- **Wait state:** Pauses execution for a fixed number of seconds. Completely free.
- **Choice state:** Routes execution based on a condition (like an if statement). Checks `$.status == "SUCCEEDED"` to decide whether to process or loop back.

**Data passing:** Step Functions passes a JSON payload between states. Each Lambda receives the previous state's output as its input. ResultSelector extracts specific fields from Lambda's response payload to pass forward.

**State transitions:** Each state entering, exiting, and transitioning counts as one state transition. First 4,000 per month are free. This pipeline uses ~20 per run, once per week = ~80/month. Well within free tier.

### AWS EventBridge

**What it is:** A serverless event bus that can trigger AWS services on a schedule (like cron) or in response to events.

**How we use it:** A single EventBridge rule triggers the Step Functions state machine every Sunday at 6am UTC. Cron expression: `cron(0 6 ? * SUN *)`. This means the pipeline is fully automated — no manual intervention needed for weekly data refreshes.

**Cost:** EventBridge scheduled rules are free.

### AWS IAM (Identity and Access Management)

**What it is:** AWS's permission system. Every AWS resource (Lambda, Step Functions, etc.) has an IAM role that defines what it's allowed to do.

**How we use it:**
- Each Lambda function has its own IAM role
- Lambda 1 role needs: S3 write access (to save USGS data), Lambda basic execution (CloudWatch logs)
- Lambda 2 role needs: Lambda basic execution only
- Lambda 3 role needs: S3 read access (to load USGS data), RDS access, Lambda basic execution
- Lambda 4 role needs: RDS access, Lambda basic execution
- Step Functions role needs: Lambda invoke access for all 4 Lambda functions
- IAM user `beaver-pipeline-user` has CLI access for deployments

**Common mistake:** When creating a new Lambda in the AWS console, AWS creates a new IAM role with minimal permissions. S3 and RDS access must be attached manually.

### AWS CloudWatch

**What it is:** AWS's logging and monitoring service. Lambda automatically sends all print() statements and errors to CloudWatch log groups.

**How we use it:** Debugging. When a Lambda fails or behaves unexpectedly, CloudWatch logs show exactly which state was being processed and what error occurred. Log group for Lambda 1: `/aws/lambda/beaver-data-fetcher`.

**Useful command:** `aws logs tail /aws/lambda/beaver-data-fetcher --since 30m`

---

## 6. Lambda Functions — Detailed Breakdown

### Lambda 1: beaver-data-fetcher

**File:** `lambda/data_fetcher/handler.py`  
**Memory:** 1024MB  
**Timeout:** 900 seconds (15 minutes)  
**Layer:** beaver-pipeline-layer v4 (pandas, numpy, psycopg2-binary, requests)  
**Environment variables:** GBIF_USERNAME, GBIF_PASSWORD

**What it does:**

Step 1 — GBIF async download request:
- Constructs a GBIF download predicate: taxon key 2439838 (Castor canadensis), country US, has_coordinate true
- POSTs to `https://api.gbif.org/v1/occurrence/download/request` with HTTP basic auth
- Receives a downloadKey string immediately (e.g. "0044930-260226173443078")
- GBIF starts preparing the zip file asynchronously in the background
- This step takes ~1-2 seconds

Step 2 — USGS data fetch for all 50 states:
- Loops through all 50 US state codes
- For each state: GET request to `https://waterservices.usgs.gov/nwis/dv/` with parameterCd=00300,00010,00400,63680
- Parses the timeSeries JSON response
- Deduplicates by station_id (same station appears once per parameter per time period)
- Applies valid range filters to exclude sentinel values
- Computes 5-year average per parameter per station
- Sleeps 0.3 seconds between state requests
- This step takes ~5-6 minutes

Step 3 — Save to S3:
- Serializes all station dicts to JSON
- PUT to `s3://beaver-pipeline-raw/usgs/usgs_dissolved_oxygen_all_states.json`

**Returns:** `{ statusCode: 200, downloadKey: "...", usgs_s3_key: "usgs/usgs_dissolved_oxygen_all_states.json" }`

**Known constraint:** Georgia (GA) and Oregon (OR) are the slowest states due to high station density. With date ranges beyond 2024, these states can push Lambda 1 over the 900-second limit. Current date range (2020-2024) reliably finishes in ~5-6 minutes.

### Lambda 2: beaver-check-status

**File:** `lambda/check_status/handler.py`  
**Memory:** 128MB  
**Timeout:** 30 seconds  
**Layer:** beaver-pipeline-layer v4  
**Environment variables:** GBIF_USERNAME, GBIF_PASSWORD

**What it does:**
- Receives `downloadKey` and `usgs_s3_key` from Step Functions
- GET request to `https://api.gbif.org/v1/occurrence/download/{downloadKey}`
- Parses response for status field: PREPARING, RUNNING, SUCCEEDED, FAILED, KILLED, SUSPENDED
- Returns status, downloadUrl (if SUCCEEDED), downloadKey, and usgs_s3_key

**Returns:** `{ statusCode: 200, status: "SUCCEEDED", downloadUrl: "https://...", downloadKey: "...", usgs_s3_key: "..." }`

**Why separate from Lambda 1?** Lambda 2 runs for ~2 seconds. If GBIF isn't ready after the first 5-minute wait, Step Functions loops back to wait another 5 minutes and calls Lambda 2 again. Each call is only 2 seconds of 128MB Lambda — essentially free. If this logic were in Lambda 1, we'd pay for 1024MB Lambda memory sitting idle during the loop.

### Lambda 3: beaver-processor

**File:** `lambda/processor/handler.py`  
**Memory:** 1024MB  
**Timeout:** 900 seconds (15 minutes)  
**Layer:** beaver-pipeline-layer v4  
**Environment variables:** DB_HOST, DB_USER, DB_PASSWORD

**What it does:**

Step 1 — Download GBIF zip:
- Streams the zip file from `downloadUrl` into memory (no disk writes — Lambda has limited /tmp storage)
- Extracts the CSV from the zip
- Parses beaver occurrence records: species, lat, lon, year, month, day, stateProvince, country

Step 2 — Load USGS data:
- GET `s3://beaver-pipeline-raw/usgs/usgs_dissolved_oxygen_all_states.json`
- Parses into list of station dicts with lat, lon, and 4 water quality averages

Step 3 — Haversine spatial join:
- For each beaver sighting: compute haversine distance to every USGS station
- Find nearest station
- If nearest distance > 500km: skip this sighting (low-confidence match)
- Otherwise: pair the sighting with the station's water quality data

Step 4 — Bulk insert into RDS:
- psycopg2 execute_values for efficient batch INSERT
- Truncates existing data first (full refresh on each run)
- Inserts all joined records into `beaver_water_joined` table

### Lambda 4: beaver-anomaly-detector

**File:** `lambda/anomaly_detector/handler.py`  
**Memory:** 1024MB  
**Timeout:** 300 seconds (5 minutes)  
**Layer:** beaver-anomaly-layer v1 (numpy, psycopg2-binary, scikit-learn — no pandas to stay under 262MB layer limit)  
**Environment variables:** DB_HOST, DB_USER, DB_PASSWORD

**What it does:**

Step 1 — Fetch records:
- SELECT all records from `beaver_water_joined` WHERE all 4 water quality parameters are non-null
- ~29,000 records have complete data (some stations only have DO, not all 4 parameters)

Step 2 — EPA climate region normalization:
- Map each record's state_province to one of 10 EPA climate regions
- For each region, compute mean and standard deviation for each of the 4 features
- Z-score transform: `z = (value - regional_mean) / regional_std`
- This ensures Florida's naturally low DO (warm water holds less oxygen) isn't flagged as anomalous compared to Oregon's higher baseline

Step 3 — Isolation Forest:
- Fit on normalized 4-feature matrix: [DO_z, temp_z, pH_z, turbidity_z]
- `contamination=0.05`: expects ~5% of records to be anomalous
- `random_state=42`: reproducible results
- `n_estimators=100`: 100 isolation trees
- `fit_predict()` returns 1 (normal) or -1 (anomaly) for each record

Step 4 — Write scores:
- UPDATE `beaver_water_joined` SET `anomaly_score = score` WHERE id = id
- executemany for batch updates

**Returns:** `{ statusCode: 200, recordsScored: N, anomaliesDetected: N }`

---

## 7. Step Functions State Machine

**Name:** beaver-pipeline-state-machine  
**Definition file:** `infrastructure/step_functions.json`  
**ARN:** `arn:aws:states:us-east-2:523902091271:stateMachine:beaver-pipeline-state-machine`

### State Diagram

```
StartAt: InvokeFetcher

InvokeFetcher (Task)
  -> WaitForGBIF

WaitForGBIF (Wait, 300 seconds)
  -> CheckGBIFStatus

CheckGBIFStatus (Task)
  -> IsGBIFReady

IsGBIFReady (Choice)
  $.status == "SUCCEEDED" -> InvokeProcessor
  Default -> WaitForGBIF

InvokeProcessor (Task)
  -> InvokeAnomalyDetector

InvokeAnomalyDetector (Task)
  -> End
```

### ResultSelector

Each Task state uses `ResultSelector` to extract specific fields from Lambda's response payload. Lambda returns `{ Payload: { statusCode, downloadKey, ... } }` — ResultSelector maps `$.Payload.downloadKey` to `$.downloadKey` so subsequent states receive clean data.

### How Data Flows Between States

```
InvokeFetcher returns:
  { downloadKey: "0044930-...", usgs_s3_key: "usgs/usgs_dissolved_oxygen_all_states.json" }

-> This becomes the input to WaitForGBIF (passed through unchanged)
-> This becomes the input to CheckGBIFStatus
-> CheckGBIFStatus adds: { status: "SUCCEEDED", downloadUrl: "https://..." }
-> This full object becomes input to InvokeProcessor
-> InvokeProcessor uses downloadUrl to fetch GBIF zip, usgs_s3_key to fetch USGS data from S3
```

---

## 8. Database Schema

### Table: beaver_water_joined

```sql
CREATE TABLE beaver_water_joined (
    id                    SERIAL PRIMARY KEY,
    species               VARCHAR(100),
    decimal_latitude      DECIMAL(10,6),
    decimal_longitude     DECIMAL(10,6),
    year                  INTEGER,
    month                 INTEGER,
    day                   INTEGER,
    state_province        VARCHAR(100),
    country               VARCHAR(50),
    nearest_station       VARCHAR(200),
    station_lat           DECIMAL(10,6),
    station_lon           DECIMAL(10,6),
    distance_km           DECIMAL(10,3),
    avg_dissolved_oxygen  DECIMAL(10,4),
    avg_water_temp        DECIMAL(10,4),
    avg_ph                DECIMAL(10,4),
    avg_turbidity         DECIMAL(10,4),
    anomaly_score         INTEGER DEFAULT 1
);
```

### Column Descriptions

| Column | Description |
|---|---|
| id | Auto-incrementing primary key |
| species | Always "Castor canadensis" (North American beaver) |
| decimal_latitude | Beaver sighting GPS latitude |
| decimal_longitude | Beaver sighting GPS longitude |
| year / month / day | Date of beaver sighting from GBIF record |
| state_province | US state from GBIF record (may be inconsistently formatted) |
| country | Always "US" |
| nearest_station | Name of the nearest USGS monitoring station |
| station_lat / station_lon | GPS coordinates of the matched USGS station |
| distance_km | Haversine distance from beaver sighting to matched station |
| avg_dissolved_oxygen | 5-year average DO at matched station (2020-2024), mg/L |
| avg_water_temp | 5-year average water temperature, °C |
| avg_ph | 5-year average pH (dimensionless) |
| avg_turbidity | 5-year average turbidity, FNU |
| anomaly_score | Isolation Forest score: 1=normal, -1=anomaly |

### Current Stats (V6)

- Total rows: ~39,926
- Unique stations: 569
- Avg distance: 88.7 km
- Avg DO: 9.69 mg/L
- % healthy DO (>6.0): 99.4%
- Anomalous records: 902 (2.3%)

---

## 9. Spatial Join — How It Works

### The Problem

We have two datasets with no shared key:
- GBIF: beaver sightings with lat/lon
- USGS: monitoring stations with lat/lon and water quality data

We need to pair each beaver sighting with its nearest water quality station.

### Haversine Distance

Euclidean distance on lat/lon coordinates is inaccurate because Earth is a sphere — a degree of longitude near the equator is ~111km but near the poles approaches zero. Haversine distance computes the great-circle distance between two points on a sphere:

```
a = sin²(Δlat/2) + cos(lat1) × cos(lat2) × sin²(Δlon2)
distance = 2R × arcsin(√a)
```

Where R = 6371 km (Earth's radius).

### NumPy Vectorization

Instead of nested Python loops (O(n×m) iterations, very slow), the spatial join uses NumPy array operations. For each beaver sighting, the haversine formula is applied to ALL station coordinates simultaneously as vectorized array math. This is orders of magnitude faster than Python loops because NumPy operations run in compiled C under the hood.

For 39,926 beaver sightings and 569 stations, this means ~22.7 million distance calculations, completed in seconds inside Lambda.

### 500km Distance Cap

Some US states have very sparse USGS monitoring coverage. Without a cap, a beaver sighting in a sparsely monitored state might be matched to a station hundreds of kilometers away — a meaningless match for water quality analysis. The 500km cap removes these low-confidence matches. Records where the nearest station is beyond 500km are excluded from the joined dataset. This is a deliberate data quality tradeoff: fewer records but higher-confidence spatial relationships.

---

## 10. Anomaly Detection — How It Works

### Why Isolation Forest

Isolation Forest is an unsupervised anomaly detection algorithm suitable for this dataset because:
- No labeled "pollution events" exist — supervised methods require labels
- The algorithm is interpretable and explainable
- It handles multi-dimensional feature spaces (4 water quality parameters simultaneously)
- Lightweight enough to run in a Lambda function

### How Isolation Forest Works

The algorithm builds an ensemble of isolation trees. Each tree randomly selects a feature and a random split value, partitioning the data recursively. The key insight: anomalous points (outliers) are far from the dense normal cluster and require fewer partitions to isolate. Normal points are embedded in dense regions and require many partitions. The anomaly score is based on the average path length across all trees — shorter average path = more anomalous.

With `contamination=0.05`, the model flags the 5% of records with the shortest average path length as anomalies.

### EPA Climate Region Normalization

Without normalization, the model would compare Florida water quality directly to Oregon water quality. Florida water is naturally warmer (subtropical climate), and warm water holds less dissolved oxygen — this is basic chemistry (Henry's Law: gas solubility decreases with temperature). A Florida station with 7.5 mg/L DO is normal; an Oregon station at 7.5 mg/L might be concerning.

Z-score normalization within EPA climate regions fixes this:

```
z = (station_value - regional_mean) / regional_std
```

Each station's readings are transformed to how many standard deviations they are from their regional mean. The Isolation Forest then flags records that are unusual relative to their own regional baseline.

### EPA Region Mapping

| Region | States |
|---|---|
| Region 1 | CT, ME, MA, NH, RI, VT |
| Region 2 | NJ, NY |
| Region 3 | DE, MD, PA, VA, WV |
| Region 4 | AL, FL, GA, KY, MS, NC, SC, TN |
| Region 5 | IL, IN, MI, MN, OH, WI |
| Region 6 | AR, LA, NM, OK, TX |
| Region 7 | IA, KS, MO, NE |
| Region 8 | CO, MT, ND, SD, UT, WY |
| Region 9 | AZ, CA, HI, NV |
| Region 10 | AK, ID, OR, WA |

### What Anomalies Mean

An anomalous record (-1) means that station's combination of DO, temperature, pH, and turbidity is unusual relative to other stations in the same EPA climate region. Possible causes:
- Industrial or agricultural pollution upstream
- Habitat degradation
- Unusual natural geology (e.g., naturally acidic bog watersheds)
- Equipment malfunction at the monitoring station
- Genuine ecological anomaly worth investigating

The anomaly flag is a signal, not a diagnosis. It identifies where to look, not what is wrong.

### Known Limitation

The model scores records at the beaver sighting level, not the station level. Because many beaver sightings share the same nearest station, a single anomalous station contributes many -1 records. The Top Anomalous Stations table in the dashboard aggregates by station to make this interpretable.

---

## 11. Streamlit Dashboard

**URL:** https://beaver-watershed-pipeline.streamlit.app  
**Deployment:** Streamlit Cloud (free tier), auto-deploys on git push to main  
**Secrets:** `.streamlit/secrets.toml` (not committed — set manually in Streamlit Cloud dashboard)  
**Cache:** `@st.cache_data(ttl=86400)` — 24 hour cache on RDS query

### Dashboard Sections (in order)

1. **KPI row:** Sightings count, Avg DO, Avg Distance (km), % Healthy DO, Stations count, Anomaly count
2. **Isolation Forest Anomaly Detection:** Full-width section showing anomaly map and top anomalous stations table
3. **Beaver Sightings Map:** pydeck ScatterplotLayer, points colored amber→neon green by DO level, dark Mapbox basemap
4. **Avg Dissolved Oxygen by State:** Horizontal bar chart, colored by DO value, with 6.0 mg/L threshold line
5. **Distance vs DO Scatter:** One point per sighting, colored by state
6. **Key Findings Table:** Summary statistics and interpretation
7. **Additional Water Quality Parameters:** Temperature by state, pH by state, turbidity histogram, water quality summary table
8. **Raw Data Expander:** Full filtered dataset

### Sidebar Filters

- State multiselect (all states selected by default)
- Dissolved Oxygen range slider (min to max)
- Max Distance to Station slider (0 to 499km)

All charts and maps respond to sidebar filter changes.

### Streamlit Free Tier Limitations

- App sleeps after 7 days of no traffic
- To wake: visit the URL directly
- To force cache refresh: temporarily change `ttl=86400` to `ttl=1`, push, refresh, change back

---

## 12. Data Quality and Validation

### Sentinel Value Filtering

USGS uses -999999 (and other large negative numbers) to indicate missing, erroneous, or unavailable readings. Without filtering, these values corrupt averages — Minnesota's average DO was computed as -4787 mg/L before this fix was applied.

Valid range filters applied per parameter:

| Parameter | Valid Range | Rationale |
|---|---|---|
| Dissolved oxygen | 0–20 mg/L | Physical maximum ~14.6 mg/L at 0°C; upper bound provides safety margin |
| Water temperature | 0–35°C | Below freezing and above 35°C are outside normal stream conditions |
| pH | 0–14 | Full pH scale |
| Turbidity | 0–2000 FNU | Very high but plausible; extreme flood conditions can reach 1000+ FNU |

Any reading outside these ranges is discarded before computing station averages.

### Station Deduplication

The USGS timeSeries API returns one timeSeries entry per station per parameter per time period. The same physical station appears multiple times in the response — once for each parameter and once for each time segment. Without deduplication, the pipeline would create thousands of duplicate station entries.

Fix: a `sites_dict` keyed by `station_id`. When the same station appears again (different parameter or time period), readings are accumulated into the existing entry. Final averages are computed after all timeSeries entries have been processed.

### Distance Cap

500km cap on spatial join matches. Matches beyond 500km indicate states with insufficient USGS monitoring coverage and are excluded from the joined dataset.

### State Name Normalization

GBIF records use inconsistent state name formatting — "Washington State (WA)", "Wa", "Washington", and "WA" are all used for the same state. A `STATE_NAME_MAP` dictionary in the processor normalizes known variants. Some non-standard variants remain (~66 distinct values instead of 50), but these represent a small fraction of records and are acceptable for analysis purposes.

---

## 13. Infrastructure Details

### Lambda Layers

**beaver-pipeline-layer v4** (used by Lambda 1, 2, 3):
- pandas, numpy, psycopg2-binary, requests
- Built on Linux via Docker: `public.ecr.aws/lambda/python:3.12`
- Compatible runtime: python3.12

**beaver-anomaly-layer v1** (used by Lambda 4):
- numpy, psycopg2-binary, scikit-learn (+ scipy as sklearn dependency)
- pandas excluded to stay under 262MB Lambda layer size limit
- Test files stripped from numpy, scipy, sklearn before zipping (reduces from ~254MB to ~201MB)
- Correct zip structure: packages must be inside a `python/` subdirectory inside the zip
- Stored in S3 before publishing (direct upload fails for large layers): `s3://beaver-pipeline-raw/layers/`

### Lambda IAM Roles

| Lambda | Role Name | Policies |
|---|---|---|
| beaver-data-fetcher | beaver-data-fetcher-role-o6u7vdvp | AmazonS3FullAccess, AWSLambdaBasicExecutionRole |
| beaver-check-status | beaver-check-status-role-ktsxd9l3 | AWSLambdaBasicExecutionRole |
| beaver-processor | beaver-processor-role-70l2jfu6 | AmazonS3FullAccess, AWSLambdaBasicExecutionRole, AmazonRDSFullAccess |
| beaver-anomaly-detector | beaver-anomaly-detector-role-i85a951t | AWSLambdaBasicExecutionRole, AmazonRDSFullAccess |

**Step Functions role:** StepFunctions-beaver-pipeline-state-machine-role-kuqi1x065  
Policies: AWSLambdaRole (allows invoking all Lambda functions)

### AWS Region

All resources: us-east-2 (Ohio)

### Cost Estimates (Monthly)

| Service | Usage | Estimated Cost |
|---|---|---|
| Lambda | ~4 runs/month, ~1,800 GB-seconds/run | Free (well under 400K GB-sec free tier) |
| Step Functions | ~80 state transitions/month | Free (under 4,000 free tier) |
| RDS | Running 24/7 | ~$15-20/month (main cost, covered by credits) |
| S3 | Small JSON files | <$0.01/month |
| EventBridge | 4 triggers/month | Free |

---

## 14. Common Commands Reference

### Deploy Lambda Code

```powershell
# Lambda 1
Compress-Archive -Path lambda/data_fetcher/handler.py -DestinationPath lambda/data_fetcher/handler.zip -Force
aws lambda update-function-code --function-name beaver-data-fetcher --zip-file fileb://lambda/data_fetcher/handler.zip

# Lambda 2
Compress-Archive -Path lambda/check_status/handler.py -DestinationPath lambda/check_status/handler.zip -Force
aws lambda update-function-code --function-name beaver-check-status --zip-file fileb://lambda/check_status/handler.zip

# Lambda 3
Compress-Archive -Path lambda/processor/handler.py -DestinationPath lambda/processor/handler.zip -Force
aws lambda update-function-code --function-name beaver-processor --zip-file fileb://lambda/processor/handler.zip

# Lambda 4
Compress-Archive -Path lambda/anomaly_detector/handler.py -DestinationPath lambda/anomaly_detector/handler.zip -Force
aws lambda update-function-code --function-name beaver-anomaly-detector --zip-file fileb://lambda/anomaly_detector/handler.zip
```

### Fix Lambda Handler (if AWS reset it)

```powershell
aws lambda update-function-configuration --function-name beaver-data-fetcher --handler handler.lambda_handler
aws lambda update-function-configuration --function-name beaver-check-status --handler handler.lambda_handler
aws lambda update-function-configuration --function-name beaver-processor --handler handler.lambda_handler
aws lambda update-function-configuration --function-name beaver-anomaly-detector --handler handler.lambda_handler
```

### Trigger Pipeline Manually

```powershell
aws stepfunctions start-execution --state-machine-arn arn:aws:states:us-east-2:523902091271:stateMachine:beaver-pipeline-state-machine --input "{}"
```

### Update Step Functions State Machine

```powershell
aws stepfunctions update-state-machine --state-machine-arn arn:aws:states:us-east-2:523902091271:stateMachine:beaver-pipeline-state-machine --definition file://infrastructure/step_functions.json
```

### RDS Queries

```powershell
# Connect
psql -h beaver-pipeline-db.cly8kak82dpk.us-east-2.rds.amazonaws.com -U postgres -d postgres

# Row count
psql -h beaver-pipeline-db.cly8kak82dpk.us-east-2.rds.amazonaws.com -U postgres -d postgres -c "SELECT COUNT(*) FROM beaver_water_joined;"

# Anomaly breakdown
psql -h beaver-pipeline-db.cly8kak82dpk.us-east-2.rds.amazonaws.com -U postgres -d postgres -c "SELECT anomaly_score, COUNT(*) FROM beaver_water_joined GROUP BY anomaly_score;"

# Stats check
psql -h beaver-pipeline-db.cly8kak82dpk.us-east-2.rds.amazonaws.com -U postgres -d postgres -c "SELECT COUNT(*), ROUND(AVG(distance_km)::numeric,1) as avg_dist, ROUND(AVG(avg_dissolved_oxygen)::numeric,2) as avg_do FROM beaver_water_joined;"

# Top states by sightings
psql -h beaver-pipeline-db.cly8kak82dpk.us-east-2.rds.amazonaws.com -U postgres -d postgres -c "SELECT state_province, COUNT(*), ROUND(AVG(avg_dissolved_oxygen)::numeric, 2) as avg_do FROM beaver_water_joined GROUP BY state_province ORDER BY COUNT(*) DESC LIMIT 10;"

# Truncate (wipe all rows, keep table structure)
psql -h beaver-pipeline-db.cly8kak82dpk.us-east-2.rds.amazonaws.com -U postgres -d postgres -c "TRUNCATE TABLE beaver_water_joined;"
```

### CloudWatch Logs

```powershell
aws logs tail /aws/lambda/beaver-data-fetcher --since 30m
aws logs tail /aws/lambda/beaver-check-status --since 30m
aws logs tail /aws/lambda/beaver-processor --since 30m
aws logs tail /aws/lambda/beaver-anomaly-detector --since 30m
```

### Rebuild Lambda Layer (if adding new packages)

```powershell
# Remove old layer folder
Remove-Item -Recurse -Force lambda-layer

# Build Linux-compatible layer with Docker
docker run -v ${PWD}/lambda-layer:/var/task/python --entrypoint pip public.ecr.aws/lambda/python:3.12 install numpy psycopg2-binary scikit-learn -t /var/task/python

# Strip test files to stay under 262MB
Get-ChildItem -Recurse lambda-layer -Filter tests -Directory | Remove-Item -Recurse -Force

# Create correct folder structure (python/ subfolder required by Lambda)
New-Item -ItemType Directory -Path lambda-layer-fixed/python -Force
Copy-Item -Recurse lambda-layer/* lambda-layer-fixed/python/

# Zip and upload via S3 (direct upload fails for large layers)
Compress-Archive -Path lambda-layer-fixed/* -DestinationPath lambda-layer-new.zip -Force
aws s3 cp lambda-layer-new.zip s3://beaver-pipeline-raw/layers/lambda-layer-new.zip
aws lambda publish-layer-version --layer-name beaver-anomaly-layer --content S3Bucket=beaver-pipeline-raw,S3Key=layers/lambda-layer-new.zip --compatible-runtimes python3.12
```

### Git Workflow

```powershell
# Always add specific files — never git add . with zips or layer folders around
git add lambda/data_fetcher/handler.py
git commit -m "description"
git push

# NEVER commit:
# lambda-layer/
# lambda-layer-fixed/
# *.zip
# .streamlit/secrets.toml
```

---

## 15. Known Issues and Limitations

| Issue | Status | Notes |
|---|---|---|
| USGS date range limited to 2020-2024 | Accepted | Extending to earlier years causes Lambda 1 timeout for large states (GA, OR). Fix would require per-state retry logic or narrower per-request time windows. |
| ~66 distinct state names instead of 50 | Accepted | GBIF data uses inconsistent state name formatting. STATE_NAME_MAP normalizes known variants; remaining non-standard formats represent a small fraction of records. |
| Streamlit sleeps after 7 days | Ongoing | Free tier limitation. Visit URL periodically before sending to recruiters. |
| No unique constraint on beaver_water_joined | Known | Pipeline truncates and reloads on each run so duplicates don't accumulate, but a proper unique constraint on (decimal_latitude, decimal_longitude, year, month, day) would be more robust. |
| Lambda Deploy button sometimes doesn't update | Known AWS bug | Always verify SHA256 hash changed after deploying. If not, upload fresh zip via Upload from > .zip file in console. |
| Anomaly scoring only on records with all 4 parameters | Accepted | ~29K of ~40K records have complete water quality data. Records missing any parameter receive default anomaly_score=1 (normal). |

---

## 16. Design Decisions and Tradeoffs

### Step Functions vs Single Lambda

**Decision:** Use Step Functions to orchestrate 4 Lambdas.  
**Why:** GBIF rate limits paginated requests after ~10,200 records. Their async download API avoids this but introduces a ~5 minute wait. A Wait State in Step Functions is free; a sleeping Lambda costs money. The polling loop (check → not ready → wait → check again) maps naturally to Step Functions Choice and Wait states.  
**Tradeoff:** More infrastructure complexity. 4 Lambda functions to maintain instead of 1.

### Haversine vs sklearn BallTree

**Decision:** NumPy vectorized haversine instead of sklearn BallTree.  
**Why:** BallTree provides O(log n) nearest neighbor search vs O(n) per query. However, adding sklearn to the data_fetcher/processor layer would push it over the 262MB Lambda layer size limit.  
**Tradeoff:** Slightly slower spatial join. Acceptable given dataset size (~40K sightings × 569 stations = 22.7M distance calculations completes in seconds with NumPy vectorization).

### Full Refresh vs Incremental Load

**Decision:** TRUNCATE and full reload on each pipeline run.  
**Why:** GBIF async downloads always return the full dataset, not just new records. Incremental loading would require deduplication logic and a unique key strategy.  
**Tradeoff:** Each run overwrites all data. Historical anomaly scores from previous runs are lost.

### 5 Years of USGS Data vs Longer History

**Decision:** 2020-2024 (5 years) instead of longer historical range.  
**Why:** USGS API for large states (GA, OR) is slow and occasionally times out for large date ranges. With the current Lambda 1 timeout of 900 seconds and 50 states, 2020-2024 reliably completes in ~5-6 minutes. Extending to 2010 caused consistent timeouts.  
**Tradeoff:** Less historical context for anomaly detection baselines. Fix would require chunking requests per state with retry logic, or a dedicated USGS Lambda.

### Separate Anomaly Layer vs Combined Layer

**Decision:** beaver-anomaly-layer (numpy + psycopg2 + sklearn) separate from beaver-pipeline-layer (pandas + numpy + psycopg2 + requests).  
**Why:** Adding sklearn + scipy to the existing layer pushed total size over 262MB Lambda limit.  
**Tradeoff:** Two layers to maintain. Lambda 4 doesn't have pandas or requests (not needed for anomaly detection).