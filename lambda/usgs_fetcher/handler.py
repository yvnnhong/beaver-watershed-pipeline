import json
import time
import os
import boto3
import requests

# Lambda 4: usgs_fetcher
# Fetches dissolved oxygen, temperature, pH, and turbidity data for all 50 US states from USGS.
# Runs in PARALLEL with Lambda 1 in Step Functions.
# Fetches in 3-year chunks per state to avoid USGS timeouts on large date ranges.
# Saves results to S3 raw bucket and returns the S3 key for Lambda 3 (processor) to use.

S3_RAW_BUCKET = "beaver-pipeline-raw"

ALL_STATE_CODES = [
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA",
    "HI","ID","IL","IN","IA","KS","KY","LA","ME","MD",
    "MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC",
    "SD","TN","TX","UT","VT","VA","WA","WV","WI","WY"
]

# 3-year chunks covering 2015-2025
DATE_CHUNKS = [
    ("2015-01-01", "2017-12-31"),
    ("2018-01-01", "2020-12-31"),
    ("2021-01-01", "2025-12-31"),
]

def fetch_usgs_state_chunk(state_cd: str, start_dt: str, end_dt: str) -> dict:
    """
    Fetch one 3-year chunk for one state from USGS.
    Returns a dict keyed by station_id with accumulated readings.
    """
    url = "https://waterservices.usgs.gov/nwis/dv/"
    params = {
        "format": "json",
        "stateCd": state_cd,
        "parameterCd": "00300,00010,00400,63680",
        "siteType": "ST",
        "startDT": start_dt,
        "endDT": end_dt,
        "siteStatus": "all"
    }
    sites_dict = {}
    try:
        response = requests.get(url, params=params, timeout=60)
        response.raise_for_status()
        data = response.json()

        time_series = data.get("value", {}).get("timeSeries", [])
        for ts in time_series:
            site_info = ts.get("sourceInfo", {})
            geo = site_info.get("geoLocation", {}).get("geogLocation", {})
            values = ts.get("values", [{}])[0].get("value", [])
            station_id = site_info.get("siteCode", [{}])[0].get("value", "")
            param_cd = ts.get("variable", {}).get("variableCode", [{}])[0].get("value", "")

            valid_ranges = {
                "00300": (0, 20),
                "00010": (0, 35),
                "00400": (0, 14),
                "63680": (0, 2000),
            }

            if param_cd not in valid_ranges:
                continue

            lo, hi = valid_ranges[param_cd]
            readings: list[float] = []
            for v in values:
                try:
                    reading = float(v["value"])
                    if lo <= reading <= hi:
                        readings.append(reading)
                except (ValueError, KeyError):
                    continue

            if readings and geo.get("latitude") and geo.get("longitude"):
                if station_id not in sites_dict:
                    sites_dict[station_id] = {
                        "station_id": station_id,
                        "station_name": site_info.get("siteName", ""),
                        "station_lat": float(geo["latitude"]),
                        "station_lon": float(geo["longitude"]),
                        "state_cd": state_cd,
                        "readings": {}
                    }
                if param_cd not in sites_dict[station_id]["readings"]:
                    sites_dict[station_id]["readings"][param_cd] = readings
                else:
                    sites_dict[station_id]["readings"][param_cd].extend(readings)

    except Exception as e:
        print(f"  {state_cd} {start_dt}–{end_dt}: FAILED — {e}")

    return sites_dict


def fetch_usgs_state(state_cd: str) -> list[dict]:
    """
    Fetch ALL chunks for one state and merge into a single list of station dicts.
    Each chunk's readings are accumulated so the final average spans all 15 years.
    """
    merged: dict = {}

    for start_dt, end_dt in DATE_CHUNKS:
        chunk = fetch_usgs_state_chunk(state_cd, start_dt, end_dt)
        for station_id, station in chunk.items():
            if station_id not in merged:
                # first time seeing this station — add it with its readings
                merged[station_id] = station
            else:
                # station already exists — just extend the readings lists
                for param_cd, readings in station["readings"].items():
                    if param_cd not in merged[station_id]["readings"]:
                        merged[station_id]["readings"][param_cd] = readings
                    else:
                        merged[station_id]["readings"][param_cd].extend(readings)
        time.sleep(0.3)  # be polite to USGS between chunks

    # compute averages across all 15 years of accumulated readings
    sites: list[dict] = []
    for s in merged.values():
        r = s.pop("readings")
        def avg(lst): return sum(lst) / len(lst) if lst else None
        s["avg_dissolved_oxygen"] = avg(r.get("00300", []))
        s["avg_water_temp"]       = avg(r.get("00010", []))
        s["avg_ph"]               = avg(r.get("00400", []))
        s["avg_turbidity"]        = avg(r.get("63680", []))
        if s["avg_dissolved_oxygen"] is not None:
            sites.append(s)

    print(f"  {state_cd}: {len(sites)} stations")
    return sites


def fetch_all_usgs_data() -> list[dict]:
    all_stations: list[dict] = []
    for state_cd in ALL_STATE_CODES:
        stations = fetch_usgs_state(state_cd)
        all_stations.extend(stations)
        time.sleep(0.3)  # be polite between states
    print(f"Total USGS stations fetched: {len(all_stations)}")
    return all_stations


def save_to_s3(data: list[dict], key: str) -> None:
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=S3_RAW_BUCKET,
        Key=key,
        Body=json.dumps(data),
        ContentType="application/json"
    )
    print(f"Saved to s3://{S3_RAW_BUCKET}/{key}")


def lambda_handler(event: dict, context: object) -> dict:
    print("Fetching USGS data for all 50 states (2010-2025, 3-year chunks)...")
    usgs_data: list[dict] = fetch_all_usgs_data()
    usgs_s3_key: str = "usgs/usgs_dissolved_oxygen_all_states.json"
    save_to_s3(usgs_data, usgs_s3_key)
    return {
        "statusCode": 200,
        "usgs_s3_key": usgs_s3_key
    }