import io
import json
import os
import zipfile

import boto3
import numpy as np
import psycopg2
import requests
from psycopg2.extras import execute_values

# Lambda 3: processor
# Downloads the GBIF beaver data zip from the URL that GBIF prepared, reads USGS data from S3,
# runs a spatial join to match each beaver sighting to its nearest water quality station,
# and loads the joined results into RDS PostgreSQL.

# WHY THIS EXISTS:
# Once GBIF has finished preparing the zip file (confirmed by check_status Lambda),
# this Lambda does all the heavy lifting: downloading, joining, and loading.
# Separating this from data_fetcher means each Lambda has one clear job,
# and we only run this expensive step once we KNOW the data is ready.

S3_RAW_BUCKET = "beaver-pipeline-raw"
S3_PROCESSED_BUCKET = "beaver-pipeline-processed"

# Normalize messy GBIF state names to clean versions
STATE_NAME_MAP = {
    "Washington State (WA)": "Washington",
    "Virginia (VA)": "Virginia",
    "New Jersey (NJ)": "New Jersey",
    "North Dakota": "North Dakota",
    "South Dakota": "South Dakota",
    "Nv": "Nevada",
    "Ca": "California",
    "Tx": "Texas",
    "Or": "Oregon",
    "Wa": "Washington",
    "Mt": "Montana",
    "Id": "Idaho",
    "Co": "Colorado",
    "Mn": "Minnesota",
    "Wi": "Wisconsin",
    "Mi": "Michigan",
    "Il": "Illinois",
    "Oh": "Ohio",
    "Pa": "Pennsylvania",
    "Ny": "New York",
}


# ── GBIF ──────────────────────────────────────────────────────────────────────

def download_gbif_zip(download_url: str) -> list[dict]:
    """
    Download the GBIF zip file, unzip it in memory, parse the CSV.
    Returns a list of beaver record dicts.
    """
    print(f"Downloading GBIF zip from {download_url}...")
    response = requests.get(download_url, timeout=300, stream=True)
    response.raise_for_status()

    zip_bytes = io.BytesIO(response.content)
    beaver_records: list[dict] = []

    with zipfile.ZipFile(zip_bytes) as z:
        # GBIF zips contain one CSV file — find it
        csv_filename = [f for f in z.namelist() if f.endswith(".csv")][0]
        with z.open(csv_filename) as csvfile:
            import csv
            reader = csv.DictReader(io.TextIOWrapper(csvfile, encoding="utf-8"), delimiter="\t")
            for row in reader:
                try:
                    beaver_records.append({
                        "species": row.get("species", ""),
                        "decimal_latitude": float(row["decimalLatitude"]),
                        "decimal_longitude": float(row["decimalLongitude"]),
                        "year": int(row["year"]) if row.get("year") else None,
                        "month": int(row["month"]) if row.get("month") else None,
                        "day": int(row["day"]) if row.get("day") else None,
                        "state_province": STATE_NAME_MAP.get(row.get("stateProvince", ""), row.get("stateProvince", "")),
                        "country": row.get("countryCode", "")
                    })
                except (ValueError, KeyError):
                    continue  # skip malformed rows

    print(f"Parsed {len(beaver_records)} beaver records from GBIF zip")
    return beaver_records


# ── USGS ──────────────────────────────────────────────────────────────────────

def load_usgs_from_s3(usgs_s3_key: str) -> list[dict]:
    """
    Load USGS station data that Lambda 1 saved to S3.
    """
    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=S3_RAW_BUCKET, Key=usgs_s3_key)
    usgs_data: list[dict] = json.loads(response["Body"].read().decode("utf-8"))
    print(f"Loaded {len(usgs_data)} USGS stations from S3")
    return usgs_data


# ── SPATIAL JOIN ──────────────────────────────────────────────────────────────

def haversine_distances(beaver_lat: float, beaver_lon: float,
                        station_lats: np.ndarray, station_lons: np.ndarray) -> np.ndarray:
    """
    Vectorized haversine distance from one beaver point to all stations.
    Returns array of distances in km.

    Haversine formula computes great-circle distance between two points on a sphere —
    more accurate than Euclidean distance for lat/lon coordinates because the
    Earth is curved. For an interview: this is O(n) per beaver but numpy
    vectorization makes the constant factor tiny.
    """
    R = 6371.0  # Earth radius in km
    lat1 = np.radians(beaver_lat)
    lat2 = np.radians(station_lats)
    dlon = np.radians(station_lons - beaver_lon)
    dlat = lat2 - lat1

    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    return R * 2 * np.arcsin(np.sqrt(a))


def spatial_join(beaver_records: list[dict], usgs_stations: list[dict]) -> list[dict]:
    """
    For each beaver sighting, find the nearest USGS station and attach its
    dissolved oxygen reading. Returns joined records ready for RDS insertion.
    """
    # Pre-convert station coords to numpy arrays once — avoids re-converting inside the loop
    station_lats = np.array([s["station_lat"] for s in usgs_stations])
    station_lons = np.array([s["station_lon"] for s in usgs_stations])

    joined: list[dict] = []
    for beaver in beaver_records:
        distances = haversine_distances(
            beaver["decimal_latitude"],
            beaver["decimal_longitude"],
            station_lats,
            station_lons
        )
        nearest_idx: int = int(np.argmin(distances))
        nearest_station = usgs_stations[nearest_idx]

        nearest_distance = round(float(distances[nearest_idx]), 3)
        
        # skip beavers where nearest station is more than 500km away
        # these are likely data quality issues e.g. Alaska beaver matched to Florida station
        if nearest_distance > 500:
            continue

        joined.append({
            **beaver,
            "nearest_station": nearest_station["station_name"],
            "station_lat": nearest_station["station_lat"],
            "station_lon": nearest_station["station_lon"],
            "distance_km": nearest_distance,
            "avg_dissolved_oxygen": nearest_station["avg_dissolved_oxygen"],
            "avg_water_temp": nearest_station.get("avg_water_temp"),
            "avg_ph": nearest_station.get("avg_ph"),
            "avg_turbidity": nearest_station.get("avg_turbidity")
        })

    print(f"Spatial join complete: {len(joined)} records joined")
    return joined


# ── RDS ───────────────────────────────────────────────────────────────────────

def load_to_rds(joined_records: list[dict]) -> None:
    """
    Bulk insert joined records into RDS PostgreSQL.
    Uses ON CONFLICT DO NOTHING to safely handle re-runs without duplicates.
    """
    conn = psycopg2.connect(
        host=os.environ["DB_HOST"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        dbname="postgres",
        port=5432
    )
    cursor = conn.cursor()

    rows = [
        (
            r["species"], r["decimal_latitude"], r["decimal_longitude"],
            r["year"], r["month"], r["day"],
            r["state_province"], r["country"],
            r["nearest_station"], r["station_lat"], r["station_lon"],
            r["distance_km"], r["avg_dissolved_oxygen"],
            r.get("avg_water_temp"), r.get("avg_ph"), r.get("avg_turbidity")
        )
        for r in joined_records
    ]

    execute_values(cursor, """
        INSERT INTO beaver_water_joined (
            species, decimal_latitude, decimal_longitude,
            year, month, day,
            state_province, country,
            nearest_station, station_lat, station_lon,
            distance_km, avg_dissolved_oxygen,
            avg_water_temp, avg_ph, avg_turbidity
        ) VALUES %s
        ON CONFLICT DO NOTHING
    """, rows)

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Loaded {len(rows)} rows into RDS")


def save_processed_to_s3(joined_records: list[dict]) -> None:
    """Save final joined dataset to processed S3 bucket as JSON."""
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=S3_PROCESSED_BUCKET,
        Key="beaver_water_joined.json",
        Body=json.dumps(joined_records),
        ContentType="application/json"
    )
    print(f"Saved {len(joined_records)} joined records to s3://{S3_PROCESSED_BUCKET}/beaver_water_joined.json")


# ── LAMBDA HANDLER ────────────────────────────────────────────────────────────

def lambda_handler(event: dict, context: object) -> dict:
    """
    Lambda 3: processor
    Receives downloadUrl + usgs_s3_key from Step Functions.
    Downloads GBIF zip, loads USGS from S3, spatial joins, saves to S3 + RDS.
    """
    download_url: str = event["downloadUrl"]
    usgs_s3_key: str = event["usgs_s3_key"]

    # Step 1: download and parse GBIF zip
    beaver_records: list[dict] = download_gbif_zip(download_url)
    if len(beaver_records) == 0:
        raise Exception("GBIF zip contained no beaver records — aborting")

    # Step 2: load USGS data from S3
    usgs_stations: list[dict] = load_usgs_from_s3(usgs_s3_key)
    if len(usgs_stations) == 0:
        raise Exception("USGS data in S3 is empty — aborting")

    # Step 3: spatial join
    joined_records: list[dict] = spatial_join(beaver_records, usgs_stations)

    # Step 4: save to processed S3 bucket
    save_processed_to_s3(joined_records)

    # Step 5: load to RDS
    load_to_rds(joined_records)

    return {
        "statusCode": 200,
        "recordsProcessed": len(joined_records)
    }