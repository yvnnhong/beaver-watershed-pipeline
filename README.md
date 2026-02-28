# Beaver & Watershed Health Pipeline

An end-to-end data engineering pipeline that ingests US beaver occurrence records and USGS dissolved oxygen readings, spatially joins them, and surfaces findings via a deployed Streamlit dashboard.

**Live Dashboard:** https://beaver-watershed-pipeline.streamlit.app  
**GitHub:** https://github.com/yvnnhong/beaver-watershed-pipeline

---

## The Question

Do areas with beaver activity correlate with healthy dissolved oxygen levels in nearby US waterways?

---

## Architecture
# - EventBridge triggers Lambda every Sunday at 6am UTC for automated weekly refresh

```
GBIF API (beaver occurrences) ──┐
                                ├──► S3 Raw Bucket ──► Lambda (spatial join) ──► S3 Processed Bucket ──► RDS PostgreSQL ──► Streamlit Dashboard
USGS Water Services API ────────┘
```

**Data flow:**
1. GBIF API returns US beaver occurrence records (taxon key 2439838, *Castor canadensis*) with coordinates, date, and state
2. USGS Water Services API returns dissolved oxygen readings (parameter code 00300) from stream monitoring stations
3. AWS Lambda runs a numpy haversine spatial join, matching each beaver sighting to its nearest USGS station
4. Cleaned joined dataset is written to S3 processed bucket and loaded into RDS PostgreSQL
5. Streamlit dashboard reads from RDS and displays an interactive map, charts, and summary statistics

---

## Tech Stack

| Layer | Technology |
|---|---|
| Cloud Infrastructure | AWS Lambda, AWS S3, AWS RDS (PostgreSQL) |
| Pipeline | Python, pandas, numpy, psycopg2 |
| Spatial Join | Numpy haversine distance (O(n) per sighting) |
| Dashboard | Streamlit, pydeck, Plotly |
| Data Sources | GBIF Occurrence API, USGS Water Services API |
| Dependency Packaging | Docker (Linux-compatible Lambda layers) |
| Version Control | Git, GitHub |

---

## Key Findings

- **5,100+** US beaver occurrence records across 45 states loaded into RDS
- **13** USGS stream monitoring stations matched via spatial join
- **Average distance** from beaver sighting to nearest station: 49.3 km
- **Dissolved oxygen range** near beaver habitat: 7.3 - 11.3 mg/L (all above 6.0 mg/L healthy threshold)
- **98.6%** of records show healthy dissolved oxygen levels
- Weak correlation (0.191) between distance to station and dissolved oxygen, suggesting beavers broadly associate with healthy water rather than specifically proximity to monitoring infrastructure

---

## Project Structure

```
beaver-watershed-pipeline/
├── README.md
├── .gitignore
├── requirements.txt
├── streamlit_app.py              # Streamlit dashboard (deployed on Streamlit Cloud)
├── .streamlit/
│   └── secrets.toml              # RDS credentials (not committed)
├── notebooks/
│   └── beaver_data_engineer.ipynb   # Colab prototype and data exploration
├── data/
│   └── beaver_water_joined.csv      # Joined dataset sample
├── lambda/
│   ├── handler.py                   # Lambda function (full pipeline)
│   └── requirements.txt             # Lambda dependencies
├── sql/
│   └── create_tables.sql            # RDS table definitions
└── infrastructure/
    └── setup_notes.md               # AWS setup notes
```

---

## Running Locally

```bash
# Install dependencies
pip install -r requirements.txt

# Add RDS credentials
mkdir .streamlit
# Create .streamlit/secrets.toml with your RDS connection details (see secrets.toml.example)

# Run dashboard
streamlit run streamlit_app.py
```

---

## AWS Infrastructure

| Service | Resource | Purpose |
|---|---|---|
| S3 | beaver-pipeline-raw | Raw CSV storage (GBIF + USGS data) |
| S3 | beaver-pipeline-processed | Cleaned joined CSV output |
| Lambda | beaver-pipeline-lambda | Spatial join pipeline (Python 3.12, 1024MB, 5min timeout) |
| RDS | beaver-pipeline-db | PostgreSQL database storing final joined dataset |

---

## Data Sources

- [GBIF](https://www.gbif.org/) - Global Biodiversity Information Facility (beaver occurrences)
- [USGS Water Services](https://waterservices.usgs.gov/) - National Water Information System (dissolved oxygen)