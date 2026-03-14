# BEAVER WATERSHED PIPELINE
## V5 Handoff Document
### Claude Context Document — Read This First
### Last Updated: March 14, 2026

---

## 1. Who Is This Person

- **Name:** Yvonne Hong
- **Education:** UCSD B.S. Mathematics-Computer Science, 2025
- **Job hunting:** Data Engineering / MLOps roles in Los Angeles
- **Career goal:** Specialize in Data Engineering + MLOps intersection — building pipelines that produce ML-ready datasets and deploying models as part of production pipelines
- **Background:** ETL pipelines, REST APIs, Flask, PostgreSQL at Tristero internship. Previous projects: eBird 47GB Kafka/DuckDB pipeline, ML resume optimizer (PyTorch — removed from resume), anomaly detection project (separate, already on resume)
- **Goal:** 2-3 fully deployed, clickable DE/MLOps portfolio projects with environmental data theme. Beaver pipeline is project #2.
- **Setup:** Windows laptop, PowerShell, VS Code, Docker Desktop, Miniconda (Python 3.13 local, Python 3.12 for Lambda)
- **AWS:** ~$150 in credits remaining. Billing alert set at $1. Region: us-east-2 (Ohio)
- **Personality notes:** Very enthusiastic, pastes full terminal output when debugging, appreciates knowing WHY things work not just HOW, strong math background (connect DE concepts to math when possible), gets stressed when things break repeatedly so reassure her that progress is happening.

---

## 2. Project Overview

**Beaver & Watershed Health Pipeline:** An end-to-end data engineering pipeline that ingests GBIF beaver occurrence data and USGS water quality data (dissolved oxygen, temperature, pH, turbidity), spatially joins them, and surfaces insights via a deployed Streamlit dashboard.

**Core question:** Do areas with beaver activity correlate with healthy water quality in nearby US waterways?

- **Live Dashboard:** https://beaver-watershed-pipeline.streamlit.app
- **GitHub Repo:** https://github.com/yvnnhong/beaver-watershed-pipeline

---

## 3. Current Architecture (as of V5)

```
EventBridge (Sunday 6am UTC)
        ↓
  Step Functions State Machine (beaver-pipeline-state-machine)
        ↓
  ┌─ Lambda 1: beaver-data-fetcher ──────────────────────────┐
  │  • POST to GBIF async download API → get downloadKey     │
  │  • Fetch all 50 states USGS data (4 parameters)          │
  │  • Save USGS JSON to S3 raw bucket                       │
  │  • Returns: { downloadKey, usgs_s3_key }                 │
  └──────────────────────────────────────────────────────────┘
        ↓
  Wait State (5 minutes — free, no Lambda running)
        ↓
  ┌─ Lambda 2: beaver-check-status ──────────────────────────┐
  │  • GET GBIF status endpoint with downloadKey             │
  │  • Returns: { status, downloadUrl, downloadKey,          │
  │               usgs_s3_key }                              │
  └──────────────────────────────────────────────────────────┘
        ↓
  Choice State: status == "SUCCEEDED"?
     YES ↓              NO → loop back to Wait State
  ┌─ Lambda 3: beaver-processor ─────────────────────────────┐
  │  • Download GBIF zip from downloadUrl (in memory)        │
  │  • Load USGS JSON from S3                                │
  │  • Numpy haversine spatial join (500km cap)              │
  │  • Save joined data to S3 processed bucket               │
  │  • Bulk insert into RDS PostgreSQL                       │
  └──────────────────────────────────────────────────────────┘
        ↓
  RDS PostgreSQL → Streamlit Dashboard (live)
```

**Why Step Functions?**
The original single Lambda timed out because GBIF throttles paginated requests after ~10,200 records (slowing from 1.5s to 50s per batch). GBIF's own docs say to use their async download API for datasets over 12k records. Step Functions lets us: fire the request (Lambda 1), wait for free (Wait state), check status (Lambda 2), process (Lambda 3). Waiting is free in Step Functions but expensive in Lambda.

---

## 4. AWS Credentials & Configuration

**IMPORTANT: Never commit these to GitHub!**

| Setting | Value |
|---|---|
| AWS Region | us-east-2 (Ohio) |
| AWS Account ID | 523902091271 |
| IAM User | beaver-pipeline-user |
| IAM Access Key | AKIAXT6X7QADWKGPA4IS |
| RDS Endpoint | beaver-pipeline-db.cly8kak82dpk.us-east-2.rds.amazonaws.com |
| RDS Port | 5432 |
| RDS Database | postgres |
| RDS Username | postgres |
| RDS Password | Yvonne knows this |
| S3 Raw Bucket | beaver-pipeline-raw |
| S3 Processed Bucket | beaver-pipeline-processed |
| Lambda 1 | beaver-data-fetcher (1024MB, 15min, layer v4) |
| Lambda 2 | beaver-check-status (128MB, 30sec, layer v4) |
| Lambda 3 | beaver-processor (1024MB, 15min, layer v4) |
| Lambda Layer | beaver-pipeline-layer version 4 (pandas, numpy, psycopg2-binary, requests) |
| Step Functions | beaver-pipeline-state-machine |
| EventBridge | beaver-pipeline-weekly (every Sunday 6am UTC) |
| GBIF Username | yvonnehong2003 |
| GBIF Password | Yvonne knows this |

**Lambda Environment Variables:**
- `beaver-data-fetcher`: GBIF_USERNAME, GBIF_PASSWORD
- `beaver-check-status`: GBIF_USERNAME, GBIF_PASSWORD
- `beaver-processor`: DB_HOST, DB_USER, DB_PASSWORD

---

## 5. Database Schema

```sql
CREATE TABLE beaver_water_joined (
    id SERIAL PRIMARY KEY,
    species VARCHAR(100),
    decimal_latitude DECIMAL(10,6),
    decimal_longitude DECIMAL(10,6),
    year INTEGER,
    month INTEGER,
    day INTEGER,
    state_province VARCHAR(100),
    country VARCHAR(50),
    nearest_station VARCHAR(200),
    station_lat DECIMAL(10,6),
    station_lon DECIMAL(10,6),
    distance_km DECIMAL(10,3),
    avg_dissolved_oxygen DECIMAL(10,4),
    avg_water_temp DECIMAL(10,4),      -- added in V5
    avg_ph DECIMAL(10,4),              -- added in V5
    avg_turbidity DECIMAL(10,4)        -- added in V5
);
```

**Current stats (as of V5):**
- Total rows: ~39,931
- Stations: 569
- Avg distance: 88.9 km
- Avg DO: 9.69 mg/L
- Avg water temp: 14.1°C (98% coverage)
- Avg pH: 7.69 (74% coverage)
- Avg turbidity: 25.1 FNU (57% coverage)
- % healthy DO (>6.0 mg/L): 99.4%
- Top beaver state: Oregon (3,277 sightings)

---

## 6. USGS Parameters Fetched

| Parameter | Code | Unit | Valid Range Used |
|---|---|---|---|
| Dissolved oxygen | 00300 | mg/L | 0–20 |
| Water temperature | 00010 | °C | 0–35 |
| pH | 00400 | dimensionless | 0–14 |
| Turbidity | 63680 | FNU | 0–2000 |

Date range: 2020-01-01 to 2024-12-31 (to be extended to 2010 — see Next Steps)
All 50 US states fetched with 0.3s sleep between requests.

---

## 7. Project File Structure

```
beaver-watershed-pipeline/
├── README.md                          ← updated in V5
├── .gitignore                         ← excludes lambda-layer/, secrets.toml, zips
├── requirements.txt
├── streamlit_app.py                   ← Streamlit dashboard (root level)
├── gbif_test.json                     ← SHOULD BE DELETED (leftover test file)
├── .streamlit/
│   └── secrets.toml                   ← RDS credentials (NOT committed)
├── notebooks/
│   └── beaver_data_engineer.ipynb
├── data/
│   └── beaver_water_joined.csv
├── lambda/
│   ├── old_handler.py                 ← retired original Lambda, keep for reference
│   ├── data_fetcher/
│   │   └── handler.py                 ← Lambda 1
│   ├── check_status/
│   │   └── handler.py                 ← Lambda 2
│   └── processor/
│       └── handler.py                 ← Lambda 3
├── sql/
│   └── create_tables.sql
└── infrastructure/
    ├── step_functions.json            ← state machine definition
    └── setup_notes.md
```

---

## 8. Issues Encountered in V4→V5 and How They Were Fixed

This section documents every bug hit during the V4→V5 session so future Claude understands the history.

### Issue 1: GBIF URL Wrong
**Error:** `422 Client Error: Unprocessable Entity`  
**Cause:** Lambda 1 was posting to `https://api.gbif.org/v1/occurrence/download` instead of the correct endpoint `https://api.gbif.org/v1/occurrence/download/request`  
**Fix:** Added `/request` to `GBIF_DOWNLOAD_URL` constant in `data_fetcher/handler.py`

### Issue 2: Lambda IAM Roles Missing S3/RDS Permissions
**Error:** `AccessDenied when calling PutObject operation`  
**Cause:** When new Lambdas are created in AWS console, AWS auto-creates new IAM roles with minimal permissions. These new roles didn't have S3 or RDS access.  
**Fix:** Added the following policies to each role in IAM console:
- `beaver-data-fetcher-role-o6u7vdvp` → AmazonS3FullAccess, AWSLambdaBasicExecutionRole
- `beaver-check-status-role-ktsxd9l3` → AWSLambdaBasicExecutionRole
- `beaver-processor-role-70l2jfu6` → AmazonS3FullAccess, AWSLambdaBasicExecutionRole, AmazonRDSFullAccess

### Issue 3: Lambda Handler Name Wrong
**Error:** Lambda ran old default code instead of handler.py  
**Cause:** AWS defaults new Lambda functions to `lambda_function.lambda_handler` but our files are named `handler.py`  
**Fix:** Ran this for each Lambda:
```powershell
aws lambda update-function-configuration --function-name beaver-data-fetcher --handler handler.lambda_handler
aws lambda update-function-configuration --function-name beaver-check-status --handler handler.lambda_handler
aws lambda update-function-configuration --function-name beaver-processor --handler handler.lambda_handler
```

### Issue 4: beaver-check-status Missing Lambda Layer
**Error:** `Runtime.ImportModuleError: No module named 'requests'`  
**Cause:** We initially thought check-status didn't need the layer since it only uses requests. Wrong — requests is not in the default Lambda runtime.  
**Fix:** Added beaver-pipeline-layer v4 to beaver-check-status in Lambda console.

### Issue 5: Negative Dissolved Oxygen Values
**Error:** AVG(avg_dissolved_oxygen) returning -909.90 for California, -4787.31 for Minnesota  
**Cause:** USGS uses sentinel values like -999999 to indicate missing/error readings. Our pipeline was averaging these in with real readings.  
**Fix:** Added valid range filter in `fetch_usgs_state` in `data_fetcher/handler.py`:
```python
if 0 <= reading <= 20:
    do_readings.append(reading)
```
Same pattern applied to all 4 parameters with their respective valid ranges.

### Issue 6: Average Distance Ballooned to 552km
**Cause:** Old handler.py only fetched USGS data for California. With all 50 states, some states have very few USGS DO stations, so beavers in those states were being matched to stations thousands of km away.  
**Fix:** Added 500km distance cap in `spatial_join` in `processor/handler.py`:
```python
if nearest_distance > 500:
    continue
```

### Issue 7: Only 40 Unique Stations Despite 36,723 Records
**Error:** `COUNT(DISTINCT nearest_station) = 40`  
**Cause:** USGS timeSeries API returns one timeSeries entry per station per time period. The same station appeared hundreds of times in the response. We were appending a new dict for each timeSeries entry, creating 36,723 entries for only 40 unique stations.  
**Fix:** Rewrote `fetch_usgs_state` to deduplicate by station_id using a dict, extending readings lists when the same station appears again, then computing the average at the end:
```python
sites_dict: dict = {}
# ... build dict keyed by station_id ...
for s in sites_dict.values():
    s["avg_dissolved_oxygen"] = sum(s["do_readings"]) / len(s["do_readings"])
    del s["do_readings"]
    sites.append(s)
```

### Issue 8: INSERT Has More Expressions Than Target Columns
**Error:** `SyntaxError: INSERT has more expressions than target columns`  
**Cause:** When adding the 3 new columns (avg_water_temp, avg_ph, avg_turbidity), an old INSERT statement without the new columns was accidentally left in `load_to_rds`. Two execute_values calls existed — one with 16 columns, one with 13. The rows tuple had 16 values, so the 13-column INSERT failed.  
**Fix:** Deleted the duplicate old execute_values call. Only one INSERT should exist.

### Issue 9: Streamlit Cache Showing Stale Data
**Cause:** `@st.cache_data(ttl=86400)` caches RDS query for 24 hours. After pipeline reruns with new data/schema, dashboard still shows old cached data.  
**Fix:** Temporarily change ttl to 1, push to GitHub (forces Streamlit Cloud redeploy), wait 1-2 min, refresh dashboard, then change back to 86400 and push again.

### Issue 10: Messy State Names (72 states instead of 50)
**Cause:** GBIF records use inconsistent state name formatting — "Washington State (WA)", "Wa", "Washington" all treated as separate states.  
**Fix:** Added `STATE_NAME_MAP` dictionary in `processor/handler.py` that normalizes known variants. Still ~66 states after fix due to remaining unknown variants in GBIF data. Acceptable for now.

---

## 9. Key Commands Reference

### Deploy Lambda Code (PowerShell)
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
```

### Fix Lambda Handler Name (if needed)
```powershell
aws lambda update-function-configuration --function-name beaver-data-fetcher --handler handler.lambda_handler
aws lambda update-function-configuration --function-name beaver-check-status --handler handler.lambda_handler
aws lambda update-function-configuration --function-name beaver-processor --handler handler.lambda_handler
```

### RDS Queries
```powershell
# Connect
psql -h beaver-pipeline-db.cly8kak82dpk.us-east-2.rds.amazonaws.com -U postgres -d postgres

# Row count
psql -h beaver-pipeline-db.cly8kak82dpk.us-east-2.rds.amazonaws.com -U postgres -d postgres -c "SELECT COUNT(*) FROM beaver_water_joined;"

# Parameter coverage check
psql -h beaver-pipeline-db.cly8kak82dpk.us-east-2.rds.amazonaws.com -U postgres -d postgres -c "SELECT COUNT(*) as total, COUNT(avg_water_temp) as has_temp, COUNT(avg_ph) as has_ph, COUNT(avg_turbidity) as has_turbidity FROM beaver_water_joined;"

# Stats check
psql -h beaver-pipeline-db.cly8kak82dpk.us-east-2.rds.amazonaws.com -U postgres -d postgres -c "SELECT COUNT(*), ROUND(AVG(distance_km)::numeric,1) as avg_dist, ROUND(AVG(avg_dissolved_oxygen)::numeric,2) as avg_do FROM beaver_water_joined;"

# Top states
psql -h beaver-pipeline-db.cly8kak82dpk.us-east-2.rds.amazonaws.com -U postgres -d postgres -c "SELECT state_province, COUNT(*), ROUND(AVG(avg_dissolved_oxygen)::numeric, 2) as avg_do FROM beaver_water_joined GROUP BY state_province ORDER BY COUNT(*) DESC LIMIT 10;"

# Truncate (wipe all rows, keep table structure)
psql -h beaver-pipeline-db.cly8kak82dpk.us-east-2.rds.amazonaws.com -U postgres -d postgres -c "TRUNCATE TABLE beaver_water_joined;"

# Add new columns (example)
psql -h beaver-pipeline-db.cly8kak82dpk.us-east-2.rds.amazonaws.com -U postgres -d postgres -c "ALTER TABLE beaver_water_joined ADD COLUMN new_col DECIMAL(10,4);"
```

### Check USGS S3 Data
```powershell
aws s3 cp s3://beaver-pipeline-raw/usgs/usgs_dissolved_oxygen_all_states.json - | python -c "import sys,json; data=json.load(sys.stdin); print(f'Total stations: {len(data)}')"
```

### Test GBIF API Directly
```powershell
'{"creator":"yvonnehong2003","notificationAddresses":[],"sendNotification":false,"format":"SIMPLE_CSV","predicate":{"type":"and","predicates":[{"type":"equals","key":"TAXON_KEY","value":"2439838"},{"type":"equals","key":"COUNTRY","value":"US"},{"type":"equals","key":"HAS_COORDINATE","value":"true"}]}}' | Out-File -FilePath gbif_test.json -Encoding utf8

curl.exe -X POST "https://api.gbif.org/v1/occurrence/download/request" -u "yvonnehong2003:YOUR_PASSWORD" -H "Content-Type: application/json" -d "@gbif_test.json"
```

### Git Workflow
```powershell
git add <file>       # always add specific files, never git add . with zips around
git commit -m "message"
git push

# NEVER commit:
# lambda-layer/
# .streamlit/secrets.toml
# lambda/*/handler.zip
# gbif_test.json
```

### Run Streamlit Locally
```powershell
streamlit run streamlit_app.py
```

---

## 10. Dashboard Current State

**URL:** https://beaver-watershed-pipeline.streamlit.app  
**Deployed via:** Streamlit Cloud (auto-deploys on git push to main)  
**Secrets:** `.streamlit/secrets.toml` (not committed, manually set in Streamlit Cloud)

**Current charts:**
1. KPI row: Sightings, Avg DO, Avg Dist (km), % Healthy DO, Stations
2. Map: beaver sightings colored amber→neon green by dissolved oxygen (dark map)
3. Bar chart: Avg Dissolved Oxygen by State
4. Scatter: Distance to Station vs Dissolved Oxygen
5. Key Findings table + interpretation
6. Bar chart: Avg Water Temperature by State (NEW in V5)
7. Bar chart: Avg pH by State (NEW in V5)
8. Histogram: Turbidity Distribution (NEW in V5)
9. Water Quality Summary table (NEW in V5)
10. Raw data expander

**Cache:** `@st.cache_data(ttl=86400)` — 24 hour cache. To force refresh: temporarily set ttl=1, push, refresh, set back to 86400, push again.

**Known issues:**
- Streamlit Cloud free tier sleeps after 7 days no activity — visit URL to wake it
- ~66 distinct state values due to messy GBIF state name data (acceptable)
- gbif_test.json still in repo root — should be deleted

---

## 11. Interview Talking Points

**Why Step Functions?**
GBIF recommends async download for datasets over 12k records. Paginated requests caused Lambda to timeout at ~10,200 records. Step Functions coordinates a two-Lambda workflow: Lambda 1 fires the request and fetches USGS data, Step Functions waits for free while GBIF prepares the file, Lambda 3 processes once ready. Waiting is free in Step Functions but expensive in Lambda — this is standard DE orchestration pattern.

**Why 3 Lambdas?**
Separation of concerns + cost optimization. Each Lambda has exactly one job. Lambda 2 (check_status) runs for ~2 seconds per invocation vs Lambda 1 sitting idle for 10 minutes. Lambda memory = RAM + CPU allocation (AWS ties CPU to memory). More memory = faster execution, not just more RAM.

**Why haversine over Euclidean?**
Earth is curved. Euclidean distance on lat/lon coordinates becomes increasingly inaccurate at larger distances. Haversine computes great-circle distance on a sphere. Original design used sklearn BallTree for O(log n) nearest-neighbor, but this was removed to fit Lambda layer size limit (262MB). Current implementation is O(n) per beaver but numpy vectorization makes the constant factor tiny — acceptable for dataset size.

**Why USGS over EPA WQP?**
EPA WQP returned 500 errors for large date ranges. USGS Water Services is more stable. Always have a fallback data source.

**Why Docker for Lambda layers?**
Lambda runs on Linux. Building layers on Windows produces Windows binaries that won't work on Lambda. Docker lets us build Linux-compatible binaries on any OS.

**What does the data show?**
Dissolved oxygen averages 9.69 mg/L near beaver habitat — 99.4% of records above the 6.0 mg/L threshold for healthy aquatic life. Temperature averages 14.1°C (cool waterways consistent with beaver preference). pH of 7.69 is neutral-slightly alkaline, ideal for ecosystems. Turbidity of 25.1 FNU is moderately clear with a right-skewed distribution, suggesting most beaver sightings are near clear water but some are in murkier conditions. Weak correlation (0.275) between distance to station and DO suggests beavers broadly associate with healthy water rather than specifically proximity to monitoring infrastructure.

**On the 500km distance cap:**
Some states have sparse USGS monitoring coverage. Without a cap, a beaver in Alaska might be matched to a station in Washington state 800km away — meaningless for analysis. The 500km cap removes these low-confidence matches. "I implemented a distance threshold in the spatial join to filter out matches in states with sparse USGS monitoring coverage — data quality over quantity."

**On sentinel values:**
USGS uses -999999 to indicate missing readings. Without filtering, averaging these values in with real data produces impossible negative dissolved oxygen values. Real-world data pipelines always need validation logic for sentinel values. "I implemented per-parameter valid range filters — DO must be 0-20 mg/L, temperature 0-35°C, pH 0-14, turbidity 0-2000 FNU. Anything outside these ranges is a data quality issue, not a real reading."

---

## 12. Next Steps — In Priority Order

### IMMEDIATE CLEANUP (do first)
- [ ] Delete `gbif_test.json` from repo root (`git rm gbif_test.json`, commit, push)
- [ ] Update resume bullets: 39,931 records, 569 stations, Step Functions, 4 water quality parameters
- [ ] Delete old `beaver-pipeline-lambda` from AWS Lambda console (it's been replaced)

### STEP 1: Extend Date Range to 2010
**Why:** 14 years of water quality history (2010-2024) per station enables trend analysis and provides sufficient data for ML model training. The ecological value of longer history is significant — beaver populations and water quality both show multi-year trends.

**How:** In `lambda/data_fetcher/handler.py`, change one line:
```python
"startDT": "2010-01-01",  # was 2020-01-01
```

Then redeploy Lambda 1, truncate RDS, rerun Step Functions:
```powershell
Compress-Archive -Path lambda/data_fetcher/handler.py -DestinationPath lambda/data_fetcher/handler.zip -Force
aws lambda update-function-code --function-name beaver-data-fetcher --zip-file fileb://lambda/data_fetcher/handler.zip

psql -h beaver-pipeline-db.cly8kak82dpk.us-east-2.rds.amazonaws.com -U postgres -d postgres -c "TRUNCATE TABLE beaver_water_joined;"
```

**Watch out for:** Lambda 1 runtime may increase with 14 years of data. If it approaches 15 min timeout, consider fetching fewer parameters or narrowing the state list for the historical pull.

**After this:** Row count will increase significantly. Update resume/dashboard accordingly.

### STEP 2: Add MLOps — Isolation Forest Anomaly Detection

**Goal:** Add an anomaly detection model to the pipeline that flags water quality stations with abnormal readings. This turns the project from a pure DE project into a DE + MLOps project, directly supporting Yvonne's specialization goal.

**Why Isolation Forest:**
- Unsupervised — no labels needed, which is correct here since we don't have labeled "polluted" events
- Interpretable — you can explain exactly what it does in interviews
- Production-realistic — anomaly detection is actually used in real DE/MLOps pipelines
- Lightweight — fits easily in a Lambda layer
- sklearn, ~20 lines of code

**The Pitch:**
> "The pipeline includes an anomaly detection stage using Isolation Forest. After the spatial join, each station's water quality readings (DO, temperature, pH, turbidity) are scored against a model trained on historical baselines. Stations with anomaly score -1 are flagged as potential pollution events or habitat degradation — making the pipeline not just a data mover but an early warning system."

**Implementation Plan:**

**2a. Add anomaly_score column to RDS:**
```sql
ALTER TABLE beaver_water_joined ADD COLUMN anomaly_score INTEGER DEFAULT 1;
-- 1 = normal, -1 = anomaly (Isolation Forest convention)
```

**2b. Add a new Lambda 4: `anomaly_detector`**

Create `lambda/anomaly_detector/handler.py`:

```python
import os
import json
import boto3
import psycopg2
import numpy as np
from sklearn.ensemble import IsolationForest
from psycopg2.extras import execute_values

def lambda_handler(event: dict, context: object) -> dict:
    """
    Lambda 4: anomaly_detector
    Reads all records from RDS, fits Isolation Forest on water quality features,
    writes anomaly scores back to RDS.
    """
    conn = psycopg2.connect(
        host=os.environ["DB_HOST"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        dbname="postgres",
        port=5432
    )
    cursor = conn.cursor()

    # fetch all records with complete water quality data
    cursor.execute("""
        SELECT id, avg_dissolved_oxygen, avg_water_temp, avg_ph, avg_turbidity
        FROM beaver_water_joined
        WHERE avg_dissolved_oxygen IS NOT NULL
          AND avg_water_temp IS NOT NULL
          AND avg_ph IS NOT NULL
          AND avg_turbidity IS NOT NULL
    """)
    rows = cursor.fetchall()

    ids = [r[0] for r in rows]
    features = np.array([[r[1], r[2], r[3], r[4]] for r in rows])

    # fit Isolation Forest
    # contamination=0.05 means we expect ~5% of readings to be anomalies
    clf = IsolationForest(contamination=0.05, random_state=42, n_estimators=100)
    scores = clf.fit_predict(features)  # 1 = normal, -1 = anomaly

    # write scores back to RDS
    update_data = [(int(score), id_) for score, id_ in zip(scores, ids)]
    cursor.executemany(
        "UPDATE beaver_water_joined SET anomaly_score = %s WHERE id = %s",
        update_data
    )

    n_anomalies = sum(1 for s in scores if s == -1)
    conn.commit()
    cursor.close()
    conn.close()

    print(f"Scored {len(ids)} records. Anomalies detected: {n_anomalies} ({n_anomalies/len(ids)*100:.1f}%)")
    return {
        "statusCode": 200,
        "recordsScored": len(ids),
        "anomaliesDetected": n_anomalies
    }
```

**2c. Add Lambda 4 to Step Functions state machine**

Update `infrastructure/step_functions.json` to add a 5th state after InvokeProcessor:

```json
"InvokeAnomalyDetector": {
    "Type": "Task",
    "Resource": "arn:aws:states:::lambda:invoke",
    "Parameters": {
        "FunctionName": "beaver-anomaly-detector",
        "Payload.$": "$"
    },
    "ResultPath": "$",
    "End": true
}
```

Change `InvokeProcessor` to `"Next": "InvokeAnomalyDetector"` instead of `"End": true`.

**2d. Add anomaly visualization to Streamlit dashboard**

Add a new section showing:
- Count of anomalies detected
- Map with anomalous stations highlighted in red vs normal in green
- Table of top anomalous stations with their readings

**2e. Lambda Layer Note**

sklearn needs to be added to the Lambda layer. The current layer v4 has pandas, numpy, psycopg2-binary, requests. Need to rebuild the layer with sklearn added:
```bash
# In Docker (to build Linux-compatible layer):
docker run -v $(pwd):/var/task public.ecr.aws/lambda/python:3.12 pip install pandas numpy psycopg2-binary requests scikit-learn -t /var/task/python/
```

Then zip and upload as layer v5.

**2f. IAM Permissions**

The new `beaver-anomaly-detector` Lambda role needs:
- AmazonRDSFullAccess
- AWSLambdaBasicExecutionRole

### STEP 3: Add Year/Month Filtering to Dashboard

Once date range is extended to 2010, add sliders to the sidebar:
```python
year_range = st.sidebar.slider("Year Range", 2010, 2024, (2010, 2024))
```

Filter `df_raw` by year range before applying other filters. This lets users explore how water quality near beaver habitat has changed over time.

### STEP 4: Add Seasonal Trend Analysis to Dashboard

With 14 years of monthly data, add a time series chart:
```python
monthly_avg = df.groupby(["year", "month"])["avg_dissolved_oxygen"].mean().reset_index()
fig = px.line(monthly_avg, x="month", y="avg_dissolved_oxygen", color="year")
```

This shows seasonal cycles in DO near beaver habitat — a genuinely interesting ecological finding.

### STEP 5: Update Resume

After completing Steps 1-2, resume bullet should read:
```
Built end-to-end AWS data engineering + MLOps pipeline (GBIF + USGS APIs → 
AWS Step Functions → Lambda × 4 → S3 → RDS PostgreSQL → Streamlit) 
processing 50,000+ beaver occurrence records with 14 years of multi-parameter 
water quality data. Integrated Isolation Forest anomaly detection to 
automatically flag water quality degradation events across 569 monitoring 
stations nationwide.
```

---

## 13. Known Issues & Gotchas

| Issue | Status | Notes |
|---|---|---|
| gbif_test.json in repo root | NOT fixed | Delete with `git rm gbif_test.json` |
| ~66 state names instead of 50 | Acceptable | GBIF data quality issue, would need comprehensive cleaning dict |
| Streamlit sleeps after 7 days | Ongoing | Visit URL periodically, especially before sending to recruiters |
| Lambda Deploy button bug | Known AWS issue | Always verify SHA256 hash changed after deploying. If not, upload fresh .zip via Upload from > .zip file |
| ON CONFLICT DO NOTHING | No unique constraint defined | Pipeline won't double-insert on re-runs but worth adding a proper unique constraint on (decimal_latitude, decimal_longitude, year, month, day) |
| sklearn not in Lambda layer | Needs fixing for MLOps step | Must rebuild layer v5 with scikit-learn added |
| Date range only 2020-2024 | Next step | Extend to 2010 for ML readiness |

---

## 14. Cost Notes

As of V5, estimated AWS spend is well under $5 total from ~$160 in credits.

- **Step Functions:** First 4,000 state transitions/month free. You're using ~35/month.
- **Lambda:** First 400,000 GB-seconds/month free. Each full pipeline run uses ~1,800 GB-seconds.
- **RDS:** Running 24/7 regardless of pipeline runs. This is the main ongoing cost.
- **S3:** Tiny JSON files, fractions of a penny.

The pipeline runs once per week automatically. No manual intervention needed unless something breaks.

---

## 15. Ecology Context (for interviews)

**Why beavers?**
Beavers are a keystone species — they engineer ecosystems by building dams that create wetlands. These wetlands filter water, reduce flooding, and increase biodiversity. There's active research into using beaver reintroduction as a low-cost water quality improvement strategy. This project asks whether the data supports that hypothesis.

**What the data shows:**
- 99.4% of beaver sightings are near water with healthy DO (>6.0 mg/L)
- Average temperature of 14.1°C is consistent with cool, well-oxygenated water preferred by beaver habitat
- pH of 7.69 is ideal for aquatic ecosystems
- Turbidity right-skewed distribution suggests most beaver activity near clear water, with some in murkier conditions
- Weak distance correlation (0.275) suggests beavers associate with water quality broadly, not just near monitoring infrastructure

**On the ML extension:**
> "I kept 2020-2024 for the current pipeline, but the architecture is designed to extend backwards. With 14 years of historical data per station, you could train an anomaly detection model to flag stations where dissolved oxygen suddenly drops below historical baseline — which could indicate pollution events or habitat degradation upstream of beaver activity. As a data engineer my job is making sure that data is clean, consistent, and available for that kind of analysis."

---

*Beaver Watershed Pipeline — V5 Handoff — March 14, 2026*