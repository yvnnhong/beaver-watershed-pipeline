import json
import boto3
import pandas as pd
import numpy as np
import requests
import io
import os
import psycopg2
from psycopg2.extras import execute_values

# AWS clients
s3 = boto3.client('s3')

# S3 bucket names - set these as Lambda environment variables
RAW_BUCKET = os.environ.get('RAW_BUCKET', 'beaver-pipeline-raw')
PROCESSED_BUCKET = os.environ.get('PROCESSED_BUCKET', 'beaver-pipeline-processed')

# RDS connection - set these as Lambda environment variables
DB_HOST = os.environ.get('DB_HOST')
DB_NAME = os.environ.get('DB_NAME', 'beaver_pipeline')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_PORT = os.environ.get('DB_PORT', '5432')


def fetch_beaver_data(limit=43145):
    """
    Fetch all US beaver occurrence records from GBIF API.
    Uses pagination because GBIF max limit is 300 per request.
    In prototype we used 5000, in production we use the full ~43k.
    taxonKey=2439838 = Castor canadensis (North American Beaver) from gbif.org/species/2439838
    """
    url = "https://api.gbif.org/v1/occurrence/search"
    all_records = []

    for offset in range(0, limit, 300):
        response = requests.get(url, params={
            "taxonKey": 2439838,
            "country": "US",
            "limit": 300,
            "offset": offset,
            "hasCoordinate": True  # only records with lat/lon coordinates
        }, timeout=30)

        if response.status_code != 200:
            print(f"GBIF API error at offset {offset}: {response.status_code}")
            break

        batch = response.json()['results']
        if not batch:
            break  # no more records

        all_records.extend(batch)
        print(f"Fetched {len(all_records)} beaver records so far...")

    df = pd.DataFrame(all_records)
    df = df[['species', 'decimalLatitude', 'decimalLongitude', 'year', 'month', 'day', 'stateProvince', 'country']]
    df = df.dropna(subset=['decimalLatitude', 'decimalLongitude'])
    return df


def fetch_water_quality_data(state_cd='CA'):
    """
    Fetch dissolved oxygen readings from USGS Water Services API.
    parameterCd=00300 is the USGS code for dissolved oxygen.
    Note: We tried EPA WQP (waterqualitydata.us) first but it returned 500 errors for large queries.
    USGS Water Services is more reliable.
    In production, loop over multiple state codes to get nationwide data.
    """
    usgs_url = "https://waterservices.usgs.gov/nwis/iv/"

    response = requests.get(usgs_url, params={
        "format": "json",
        "stateCd": state_cd,
        "parameterCd": "00300",  # dissolved oxygen
        "siteType": "ST",        # streams only
        "period": "P365D"        # last 365 days
    }, timeout=60)

    if response.status_code != 200:
        raise Exception(f"USGS API error: {response.status_code}")

    time_series = response.json()['value']['timeSeries']

    records: list[dict] = []
    for station in time_series:
        site_info = station['sourceInfo']
        lat = site_info['geoLocation']['geogLocation']['latitude']
        lon = site_info['geoLocation']['geogLocation']['longitude']
        site_name = site_info['siteName']

        for reading in station['values'][0]['value']:
            records.append({
                'site_name': site_name,
                'latitude': lat,
                'longitude': lon,
                'datetime': reading['dateTime'],
                'dissolved_oxygen': reading['value']
            })

    df = pd.DataFrame(records)
    df['dissolved_oxygen'] = pd.to_numeric(df['dissolved_oxygen'], errors='coerce')
    return df


def spatial_join(df_beavers, df_water):
    """
    Match each beaver sighting to its nearest water quality station using haversine formula.
    Pure numpy implementation - same result as BallTree for this dataset size.
    """
    df_stations = df_water[['site_name', 'latitude', 'longitude']].drop_duplicates().reset_index(drop=True)

    df_beavers = df_beavers.reset_index(drop=True)

    nearest_indices = []
    nearest_distances = []

    lat2 = np.radians(df_stations['latitude'].values)
    lon2 = np.radians(df_stations['longitude'].values)

    for _, row in df_beavers.iterrows():
        lat1 = np.radians(row['decimalLatitude'])
        lon1 = np.radians(row['decimalLongitude'])

        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
        distances_km = 2 * 6371 * np.arcsin(np.sqrt(a))

        nearest_idx = np.argmin(distances_km)
        nearest_indices.append(nearest_idx)
        nearest_distances.append(distances_km[nearest_idx])

    nearest_stations = df_stations.iloc[nearest_indices].reset_index(drop=True)

    df_joined = pd.concat([
        df_beavers,
        nearest_stations.rename(columns={
            'site_name': 'nearest_station',
            'latitude': 'station_lat',
            'longitude': 'station_lon'
        }),
        pd.Series(nearest_distances, name='distance_km')
    ], axis=1)

    avg_do = df_water.groupby('site_name')['dissolved_oxygen'].mean().reset_index()
    avg_do.columns = ['nearest_station', 'avg_dissolved_oxygen']

    df_final = df_joined.merge(avg_do, on='nearest_station', how='left')

    return df_final


def load_to_rds(df):
    """
    Load the joined DataFrame into RDS PostgreSQL.
    Uses psycopg2 with execute_values for efficient bulk insert.
    """
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )

    cursor = conn.cursor()

    # insert rows - on conflict do nothing to avoid duplicates on re-runs
    insert_sql = """
        INSERT INTO beaver_water_joined (
            species, decimal_latitude, decimal_longitude,
            year, month, day, state_province, country,
            nearest_station, station_lat, station_lon,
            distance_km, avg_dissolved_oxygen
        ) VALUES %s
        ON CONFLICT DO NOTHING
    """

    rows = [
        (
            row['species'], row['decimalLatitude'], row['decimalLongitude'],
            row.get('year'), row.get('month'), row.get('day'),
            row.get('stateProvince'), row.get('country'),
            row['nearest_station'], row['station_lat'], row['station_lon'],
            row['distance_km'], row['avg_dissolved_oxygen']
        )
        for _, row in df.iterrows()
    ]

    execute_values(cursor, insert_sql, rows)
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Loaded {len(rows)} rows into RDS")


def lambda_handler(event, context):
    """
    Main Lambda entry point. Triggered by S3 event when new file lands in raw bucket.
    Full pipeline: fetch data -> spatial join -> save to S3 processed -> load to RDS
    """
    print("Lambda triggered - starting beaver watershed pipeline")

    try:
        # fetch data from APIs
        print("Fetching beaver data from GBIF...")
        df_beavers = fetch_beaver_data(limit=43145)  # full dataset in production
        print(f"Got {len(df_beavers)} beaver records")

        print("Fetching water quality data from USGS...")
        df_water = fetch_water_quality_data(state_cd='CA')
        print(f"Got {len(df_water)} water quality readings")

        # run spatial join
        print("Running spatial join...")
        df_final = spatial_join(df_beavers, df_water)
        print(f"Joined dataset: {df_final.shape}")

        # save processed CSV to S3
        csv_buffer = io.StringIO()
        df_final.to_csv(csv_buffer, index=False)
        s3.put_object(
            Bucket=PROCESSED_BUCKET,
            Key='beaver_water_joined.csv',
            Body=csv_buffer.getvalue()
        )
        print(f"Saved joined CSV to s3://{PROCESSED_BUCKET}/beaver_water_joined.csv")

        # load to RDS
        print("Loading to RDS PostgreSQL...")
        load_to_rds(df_final)

        return {
            'statusCode': 200,
            'body': json.dumps(f"Pipeline complete. {len(df_final)} records processed.")
        }

    except Exception as e:
        print(f"Pipeline failed: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Pipeline failed: {str(e)}")
        }
