# Beaver & Watershed Health Pipeline

End-to-end AWS data engineering pipeline that ingests US beaver occurrence records and USGS water quality readings, spatially joins them, and surfaces findings via a deployed Streamlit dashboard.

**Live Dashboard:** https://beaver-watershed-pipeline.streamlit.app  
**GitHub:** https://github.com/yvnnhong/beaver-watershed-pipeline

---

## The Question

Do areas with beaver activity correlate with healthy water quality in nearby US waterways?

---

## Architecture
```
EventBridge (weekly) ──► Step Functions State Machine
                              │
                    ┌─────────▼─────────┐
                    │  Lambda 1          │
                    │  data_fetcher      │
                    │  - POST GBIF async │
                    │  - Fetch USGS x50  │
                    │  - Save to S3      │
                    └─────────┬─────────┘
                              │
                    ┌─────────▼─────────┐
                    │  Wait 5 min        │
                    │  (GBIF preparing   │
                    │   zip file)        │
                    └─────────┬─────────┘
                              │
                    ┌─────────▼─────────┐
                    │  Lambda 2          │
                    │  check_status      │
                    │  - Poll GBIF API   │
                    │  - Loop if not     │
                    │    ready           │
                    └─────────┬─────────┘
                              │ SUCCEEDED
                    ┌─────────▼─────────┐
                    │  Lambda 3          │
                    │  processor         │
                    │  - Download zip    │
                    │  - Spatial join    │
                    │  - Load to RDS     │
                    └─────────┬─────────┘
                              │
                    ┌─────────▼─────────┐
                    │  RDS PostgreSQL    │
                    │  ──► Streamlit     │
                    └───────────────────┘
```

**Why Step Functions?**  
GBIF recommends their async download API for datasets over 12k records. Paginated requests caused Lambda to time out at ~10,200 records. Step Functions coordinates the two-Lambda workflow: Lambda 1 fires the GBIF request and fetches USGS data, Step Functions waits for free while GBIF prepares the file, Lambda 3 processes everything once ready.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Orchestration | AWS Step Functions |
| Compute | AWS Lambda (Python 3.12, x3 functions) |
| Storage | AWS S3 (raw + processed buckets) |
| Database | AWS RDS PostgreSQL |
| Scheduler | AWS EventBridge (every Sunday 6am UTC) |
| Spatial Join | Numpy haversine (vectorized, O(n) per sighting) |
| Dashboard | Streamlit, pydeck, Plotly |
| Data Sources | GBIF Occurrence API, USGS Water Services API |
| Dependency Packaging | Docker (Linux-compatible Lambda layers) |
| Version Control | Git, GitHub |

---

## Key Findings

- **39,931** US beaver occurrence records across the continental US loaded into RDS
- **569** USGS stream monitoring stations matched via spatial join (500km max distance cap)
- **Average distance** from beaver sighting to nearest station: 88.9 km
- **Dissolved oxygen** near beaver habitat: 2.0 - 14.2 mg/L, avg 9.69 mg/L
- **99.4%** of records show healthy dissolved oxygen levels (>6.0 mg/L threshold)
- **Water temperature** avg 14.1°C (98% coverage) — consistent with cool waterway preference
- **pH** avg 7.69 (74% coverage) — neutral to slightly alkaline, healthy for aquatic ecosystems
- **Turbidity** avg 25.1 FNU (57% coverage) — moderately clear, right-skewed distribution
- Weak correlation (0.275) between distance to station and dissolved oxygen

---

## Data Sources

| Source | Parameter | USGS Code |
|---|---|---|
| GBIF | Beaver occurrences (Castor canadensis) | taxonKey 2439838 |
| USGS | Dissolved oxygen | 00300 |
| USGS | Water temperature | 00010 |
| USGS | pH | 00400 |
| USGS | Turbidity | 63680 |

Date range: 2020-2024. 50 US states.

---

## Project Structure
```
beaver-watershed-pipeline/
├── README.md
├── requirements.txt
├── streamlit_app.py                  # Streamlit dashboard
├── .streamlit/
│   └── secrets.toml                  # RDS credentials (not committed)
├── notebooks/
│   └── beaver_data_engineer.ipynb    # Colab prototype
├── data/
│   └── beaver_water_joined.csv       # Sample dataset
├── lambda/
│   ├── old_handler.py                # Original single-Lambda approach (retired)
│   ├── data_fetcher/
│   │   └── handler.py                # Lambda 1: GBIF async request + USGS fetch
│   ├── check_status/
│   │   └── handler.py                # Lambda 2: GBIF download status polling
│   └── processor/
│       └── handler.py                # Lambda 3: spatial join + RDS load
├── sql/
│   └── create_tables.sql             # RDS table definitions
└── infrastructure/
    ├── step_functions.json           # Step Functions state machine definition
    └── setup_notes.md                # AWS setup notes
```

---

## AWS Infrastructure

| Service | Resource | Purpose |
|---|---|---|
| S3 | beaver-pipeline-raw | Raw JSON storage (GBIF + USGS) |
| S3 | beaver-pipeline-processed | Cleaned joined dataset |
| Lambda | beaver-data-fetcher | GBIF async request + USGS fetch (1024MB, 15min) |
| Lambda | beaver-check-status | GBIF status polling (128MB, 30sec) |
| Lambda | beaver-processor | Spatial join + RDS load (1024MB, 15min) |
| RDS | beaver-pipeline-db | PostgreSQL storing final joined dataset |
| Step Functions | beaver-pipeline-state-machine | Pipeline orchestration |
| EventBridge | beaver-pipeline-weekly | Weekly trigger (Sunday 6am UTC) |

---

## ML Next Steps

The pipeline produces a clean, structured, multi-parameter dataset ready for downstream analysis. Planned extensions:

**1. Extend date range to 2010**  
Changing `startDT` in `data_fetcher/handler.py` from 2020 to 2010 gives 14 years of water quality history per station — sufficient for trend and anomaly analysis.

**2. Anomaly detection on water quality**  
With historical baselines established, an Isolation Forest model can flag stations where dissolved oxygen, temperature, or pH deviate significantly from their historical norm. This would automatically surface potential pollution events or habitat degradation upstream of beaver activity.
```python
from sklearn.ensemble import IsolationForest

features = ["avg_dissolved_oxygen", "avg_water_temp", "avg_ph", "avg_turbidity"]
clf = IsolationForest(contamination=0.05, random_state=42)
df["anomaly"] = clf.fit_predict(df[features].dropna())
# -1 = anomaly, 1 = normal
```

**3. Seasonal trend analysis**  
Year and month columns are already in the schema. With 14 years of data, seasonal decomposition (e.g. STL) can separate long-term trends from seasonal cycles in DO and temperature near beaver habitat.

**4. Predicting water quality from beaver density**  
Group beaver sightings by watershed, compute beaver density per km², and train a regression model predicting DO or turbidity from density. Tests the core ecological hypothesis directly.

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