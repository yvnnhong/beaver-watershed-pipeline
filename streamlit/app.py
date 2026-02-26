import streamlit as st
import pandas as pd
import numpy as np
import psycopg2
import os

# ---- PAGE CONFIG ----
st.set_page_config(
    page_title="Beaver Watershed Health",
    page_icon="🦫",
    layout="wide"
)

# ---- DB CONNECTION ----
# reads from environment variables - set these in Streamlit Cloud secrets
def get_connection():
    return psycopg2.connect(
        host=os.environ.get('DB_HOST'),
        database=os.environ.get('DB_NAME', 'beaver_pipeline'),
        user=os.environ.get('DB_USER'),
        password=os.environ.get('DB_PASSWORD'),
        port=os.environ.get('DB_PORT', '5432')
    )

# cache data so we don't re-query RDS on every interaction
@st.cache_data
def load_data():
    """Load joined beaver + water quality data from RDS PostgreSQL"""
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM beaver_water_joined", conn)
    conn.close()
    return df

# ---- LOAD DATA ----
# for local development without RDS, fall back to the CSV
try:
    df = load_data()
except Exception:
    st.warning("Could not connect to RDS - loading from local CSV for development")
    df = pd.read_csv('data/beaver_water_joined.csv')

# ---- HEADER ----
st.title("🦫 Beaver & Watershed Health")
st.markdown("Exploring the relationship between North American beaver activity and dissolved oxygen levels in California waterways.")

# ---- SUMMARY METRICS ROW ----
col1, col2, col3, col4 = st.columns(4)
col1.metric("Beaver Sightings", len(df))
col2.metric("Monitoring Stations", df['nearest_station'].nunique())
col3.metric("Avg Dissolved Oxygen", f"{df['avg_dissolved_oxygen'].mean():.2f} mg/L")
col4.metric("Avg Distance to Station", f"{df['distance_km'].mean():.1f} km")

st.divider()

# ---- MAP: BEAVER SIGHTINGS COLORED BY DISSOLVED OXYGEN ----
st.subheader("Beaver Sightings by Water Quality")
st.markdown("Each point is a beaver sighting. Color indicates dissolved oxygen at the nearest monitoring station.")

# rename columns for st.map compatibility
map_df = df[['decimalLatitude', 'decimalLongitude', 'avg_dissolved_oxygen']].rename(columns={
    'decimalLatitude': 'lat',
    'decimalLongitude': 'lon'
})

st.map(map_df)

st.divider()

# ---- BAR CHART: WATER QUALITY BY WATERSHED ----
st.subheader("Water Quality by Watershed")
st.markdown("Average dissolved oxygen at each monitoring station near beaver activity. Values above 6.0 mg/L indicate healthy water.")

station_summary = df.groupby('nearest_station').agg(
    beaver_count=('species', 'count'),
    avg_do=('avg_dissolved_oxygen', 'mean')
).reset_index().sort_values('beaver_count', ascending=False)

st.bar_chart(station_summary.set_index('nearest_station')['avg_do'])

st.divider()

# ---- SCATTER: DISTANCE VS DISSOLVED OXYGEN ----
st.subheader("Distance to Station vs Dissolved Oxygen")
st.markdown("Checking whether proximity to a monitoring station affects the dissolved oxygen reading.")

st.scatter_chart(
    df[['distance_km', 'avg_dissolved_oxygen']],
    x='distance_km',
    y='avg_dissolved_oxygen'
)

st.divider()

# ---- DATA TABLE ----
st.subheader("Raw Data")
st.markdown("Full joined dataset — each row is one beaver sighting matched to its nearest water quality station.")
st.dataframe(
    df[['species', 'decimalLatitude', 'decimalLongitude', 'stateProvince',
        'nearest_station', 'distance_km', 'avg_dissolved_oxygen', 'year', 'month']],
    use_container_width=True
)

# ---- FOOTER ----
st.divider()
st.caption("Data sources: GBIF (gbif.org) for beaver occurrences | USGS Water Services for dissolved oxygen readings | Built with AWS S3, Lambda, RDS PostgreSQL, and Streamlit")
