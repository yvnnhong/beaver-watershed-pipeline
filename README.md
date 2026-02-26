# 🦫 Beaver & Watershed Health Pipeline

An end-to-end data engineering pipeline that ingests North American beaver occurrence data and USGS dissolved oxygen readings, spatially joins them, and surfaces insights via a deployed Streamlit dashboard.

**Live Dashboard:** [Add Streamlit URL here once deployed]

## The Question
Do areas with beaver activity correlate with healthy dissolved oxygen levels in nearby California waterways?

## Architecture
```
GBIF API (beaver sightings)  ──┐
                               ├──► S3 Raw Bucket ──► Lambda (spatial join) ──► S3 Processed ──► RDS PostgreSQL ──► Streamlit Dashboard
USGS API (dissolved oxygen)  ──┘
```

## Tech Stack
- **Cloud:** AWS S3, AWS Lambda, AWS RDS (PostgreSQL)
- **Pipeline:** Python, pandas, numpy, scikit-learn (BallTree)
- **Dashboard:** Streamlit (deployed on Streamlit Cloud)
- **Data Sources:** GBIF Occurrence API, USGS Water Services API

## Key Results
- 5,100+ US beaver occurrence records processed (43,000 in production)
- 41 California USGS stream monitoring stations matched
- 260 California beaver sightings spatially joined to nearest water station
- Average distance to nearest station: 49.3 km
- Dissolved oxygen range: 7.3 - 10.7 mg/L (all healthy, >6.0 threshold)
- Sacramento River watershed shows highest beaver density (80 sightings)

## Project Structure
```
beaver-watershed-pipeline/
├── README.md
├── .gitignore
├── requirements.txt
├── notebooks/
│   └── beaver_data_engineer.ipynb   # Colab prototype notebook
├── data/
│   └── beaver_water_joined.csv      # Final joined dataset (260 rows)
├── lambda/
│   ├── handler.py                   # Lambda function (full pipeline)
│   └── requirements.txt             # Lambda dependencies
├── streamlit/
│   └── app.py                       # Streamlit dashboard
├── sql/
│   └── create_tables.sql            # RDS table definitions
└── infrastructure/
    └── setup_notes.md               # AWS setup instructions
```

## Running Locally
```bash
pip install -r requirements.txt

# Run Streamlit dashboard (uses local CSV if no RDS connection)
streamlit run streamlit/app.py
```

## Data Sources
- [GBIF](https://www.gbif.org/) - Global Biodiversity Information Facility
- [USGS Water Services](https://waterservices.usgs.gov/) - National Water Information System
