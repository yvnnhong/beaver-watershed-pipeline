# Beaver Watershed Pipeline

Weekly-automated AWS data engineering pipeline that spatially joins GBIF beaver sighting records with USGS water quality data to identify anomalous monitoring stations near beaver habitat.

**Live Dashboard:** https://beaver-watershed-pipeline.streamlit.app  
**GitHub:** https://github.com/yvnnhong/beaver-watershed-pipeline

---

## The Question

Do areas with beaver activity correlate with healthy water quality in nearby US waterways?

---

## Architecture

```
EventBridge (weekly, Sunday 6am UTC)
        |
        v
Step Functions State Machine
        |
        v
Lambda 1: data_fetcher
  - POST to GBIF async download API -> receive downloadKey immediately
  - Fetch USGS water quality data for all 50 states (DO, temp, pH, turbidity)
  - Save USGS JSON to S3
        |
        v
Wait State (5 min free — GBIF prepares zip file)
        |
        v
Lambda 2: check_status
  - Poll GBIF status endpoint with downloadKey
  - Loop back to Wait State if not ready
        |
        v (SUCCEEDED)
Lambda 3: processor
  - Download GBIF zip in memory
  - Load USGS JSON from S3
  - NumPy haversine spatial join (500km cap)
  - Bulk insert into RDS PostgreSQL
        |
        v
Lambda 4: anomaly_detector
  - Read all records from RDS
  - Z-score normalize by EPA climate region
  - Fit Isolation Forest (contamination=5%)
  - Write anomaly_score back to RDS
        |
        v
RDS PostgreSQL -> Streamlit Dashboard
```

**Why Step Functions?**  
GBIF recommends their async download API for datasets over 12k records. Paginated requests caused Lambda to time out at ~10,200 records. Step Functions coordinates the workflow: Lambda 1 fires the GBIF request and fetches USGS data, Step Functions waits for free while GBIF prepares the file, Lambda 3 processes everything once ready. Waiting is free in Step Functions but would consume billable Lambda runtime.

**Why Isolation Forest?**  
Unsupervised — no labeled pollution events exist in this dataset. Interpretable — anomalies are points requiring fewer random partitions to isolate, indicating they are far from the normal cluster. EPA climate region z-score normalization ensures Florida stations are compared against other warm-climate stations, not against Oregon's naturally higher dissolved oxygen baseline.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Orchestration | AWS Step Functions |
| Compute | AWS Lambda (Python 3.12, x4 functions) |
| Storage | AWS S3 (raw + processed buckets) |
| Database | AWS RDS PostgreSQL |
| Scheduler | AWS EventBridge (every Sunday 6am UTC) |
| Spatial Join | NumPy haversine (vectorized) |
| Anomaly Detection | scikit-learn Isolation Forest, EPA region z-score normalization |
| Dashboard | Streamlit, pydeck, Plotly |
| Data Sources | GBIF Occurrence API, USGS Water Services API |
| Dependency Packaging | Docker (Linux-compatible Lambda layers) |

---

## Key Findings

- **39,926** US beaver occurrence records spatially joined to USGS monitoring stations
- **569** USGS stream monitoring stations matched (500km max distance cap)
- **Average distance** from beaver sighting to nearest station: 88.9 km
- **Dissolved oxygen** near beaver habitat: avg 9.69 mg/L — 99.4% above the 6.0 mg/L healthy threshold
- **Water temperature** avg 14.1°C (98% coverage) — consistent with cool waterway preference
- **pH** avg 7.69 (74% coverage) — neutral to slightly alkaline, healthy for aquatic ecosystems
- **Turbidity** avg 25.1 FNU (57% coverage) — moderately clear, right-skewed distribution
- **902 anomalous records** detected (2.3%) — flagged by Isolation Forest relative to EPA climate region baseline
- Weak correlation (0.275) between distance to station and dissolved oxygen

---

## Data Sources

| Source | Parameter | Code |
|---|---|---|
| GBIF | Beaver occurrences (Castor canadensis) | taxonKey 2439838 |
| USGS | Dissolved oxygen | 00300 |
| USGS | Water temperature | 00010 |
| USGS | pH | 00400 |
| USGS | Turbidity | 63680 |

Date range: 2020-2024. All 50 US states.

---

## AWS Infrastructure

| Service | Resource | Purpose |
|---|---|---|
| S3 | beaver-pipeline-raw | Raw JSON storage (GBIF + USGS) |
| S3 | beaver-pipeline-processed | Cleaned joined dataset |
| Lambda | beaver-data-fetcher | GBIF async request + USGS fetch (1024MB, 15min) |
| Lambda | beaver-check-status | GBIF status polling (128MB, 30sec) |
| Lambda | beaver-processor | Spatial join + RDS load (1024MB, 15min) |
| Lambda | beaver-anomaly-detector | Isolation Forest scoring (1024MB, 5min) |
| RDS | beaver-pipeline-db | PostgreSQL storing final joined dataset |
| Step Functions | beaver-pipeline-state-machine | Pipeline orchestration |
| EventBridge | beaver-pipeline-weekly | Weekly trigger (Sunday 6am UTC) |

---

## Project Structure

```
beaver-watershed-pipeline/
├── README.md
├── requirements.txt
├── streamlit_app.py                   # Streamlit dashboard
├── .streamlit/
│   └── secrets.toml                   # RDS credentials (not committed)
├── notebooks/
│   └── beaver_data_engineer.ipynb     # Colab prototype
├── data/
│   └── beaver_water_joined.csv        # Sample dataset
├── lambda/
│   ├── data_fetcher/
│   │   └── handler.py                 # Lambda 1: GBIF async request + USGS fetch
│   ├── check_status/
│   │   └── handler.py                 # Lambda 2: GBIF status polling
│   ├── processor/
│   │   └── handler.py                 # Lambda 3: spatial join + RDS load
│   └── anomaly_detector/
│       └── handler.py                 # Lambda 4: Isolation Forest scoring
├── sql/
│   └── create_tables.sql              # RDS table definitions
└── infrastructure/
    ├── step_functions.json            # Step Functions state machine definition
    └── setup_notes.md                 # AWS setup notes
```

---

## Running Locally

```bash
pip install -r requirements.txt

# Create .streamlit/secrets.toml with RDS credentials:
# [postgres]
# host = "your-rds-endpoint"
# port = 5432
# dbname = "postgres"
# user = "postgres"
# password = "your-password"

streamlit run streamlit_app.py
```