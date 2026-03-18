import json
import time
import os
import boto3
import requests

# Lambda 1: data_fetcher
# Kicks off an async GBIF download request for all US beaver records (returns a downloadKey immediately, no waiting).
# While GBIF prepares the zip file in the background, fetches dissolved oxygen data for all 50 states from USGS and saves it to S3.
# Returns the downloadKey so Step Functions can pass it to the check_status Lambda to poll when the GBIF file is ready.

# WHY THIS EXISTS:
# The original handler.py tried to download all 43,145 beaver records from GBIF using hundreds of paginated requests,
# which caused GBIF to throttle us after ~10,200 records — slowing each request from 1.5s to 50s and timing out our 15-minute Lambda.
# GBIF's own docs say to use their async download API for datasets over 12k records: one POST request, GBIF prepares the file, we pick it up later.
# This Lambda is the "place the order" step — the actual downloading and processing happens in Lambda 3 once GBIF is ready.

S3_RAW_BUCKET = "beaver-pipeline-raw" #s3 raw bucket means the data in its original unprocessed format e.x. csv or json
GBIF_DOWNLOAD_URL = "https://api.gbif.org/v1/occurrence/download/request"

ALL_STATE_CODES = [
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA",
    "HI","ID","IL","IN","IA","KS","KY","LA","ME","MD",
    "MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC",
    "SD","TN","TX","UT","VT","VA","WA","WV","WI","WY"
]

def request_gbif_download(gbif_username: str, gbif_password: str) -> str:
    """
    POST to GBIF async download API.
    Returns downloadKey immediately — GBIF prepares the file in the background.
    This is the fix for the rate limiting problem: one API call instead of
    hundreds of paginated requests.
    """
    predicate = { #predicate = series of stuff that must evaluate to true for the condition to be true
        "creator": gbif_username,
        "notificationAddresses": [],
        "sendNotification": False,
        "format": "SIMPLE_CSV",
        "predicate": {
            "type": "and",
            "predicates": [
                {"type": "equals", "key": "TAXON_KEY", "value": "2439838"},  # Castor canadensis
                {"type": "equals", "key": "COUNTRY", "value": "US"},
                {"type": "equals", "key": "HAS_COORDINATE", "value": "true"}
            ]
        }
    }

    response = requests.post(
        GBIF_DOWNLOAD_URL,
        json=predicate,
        auth=(gbif_username, gbif_password),
        timeout=30
    )
    response.raise_for_status()

    download_key: str = response.text.strip().strip('"')
    print(f"GBIF download requested. Key: {download_key}")
    return download_key


def fetch_usgs_state(state_cd: str) -> list[dict]:
    """
    Fetch dissolved oxygen readings for ONE single state from USGS.
    """
    url = "https://waterservices.usgs.gov/nwis/dv/"
    params = {
        "format": "json",
        "stateCd": state_cd,
        "parameterCd": "00300,00010,00400,63680", # DO, temp, pH, turbidity
        "siteType": "ST",
        "startDT": "2020-01-01",
        "endDT": "2025-12-31",
        "siteStatus": "all"
    }
    try:
        response = requests.get(url, params=params, timeout=60)
        response.raise_for_status()
        data = response.json()

        sites_dict: dict = {}
        time_series = data.get("value", {}).get("timeSeries", [])
        sites_dict: dict = {}
        time_series = data.get("value", {}).get("timeSeries", [])
        for ts in time_series:
            site_info = ts.get("sourceInfo", {})
            geo = site_info.get("geoLocation", {}).get("geogLocation", {})
            values = ts.get("values", [{}])[0].get("value", [])
            station_id = site_info.get("siteCode", [{}])[0].get("value", "")

            # figure out which parameter this timeSeries is for
            param_cd = ts.get("variable", {}).get("variableCode", [{}])[0].get("value", "")

            # valid ranges per parameter
            valid_ranges = {
                "00300": (0, 20),    # dissolved oxygen mg/L
                "00010": (0, 35),    # water temp °C
                "00400": (0, 14),    # pH
                "63680": (0, 2000),  # turbidity FNU
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

        sites: list[dict] = []
        for s in sites_dict.values():
            r = s.pop("readings")
            def avg(lst): return sum(lst) / len(lst) if lst else None
            s["avg_dissolved_oxygen"] = avg(r.get("00300", []))
            s["avg_water_temp"]       = avg(r.get("00010", []))
            s["avg_ph"]               = avg(r.get("00400", []))
            s["avg_turbidity"]        = avg(r.get("63680", []))
            # only keep stations that have at least dissolved oxygen
            if s["avg_dissolved_oxygen"] is not None:
                sites.append(s)
        print(f"  {state_cd}: {len(sites)} stations")
        return sites

    except Exception as e:
        print(f"  {state_cd}: FAILED — {e}")
        return []
    
def fetch_all_usgs_data() -> list[dict]:
    """
    Loop all 50 states and collect USGS dissolved oxygen stations.
    Returns flat list of all station dicts.
    """
    all_stations: list[dict] = []
    for state_cd in ALL_STATE_CODES:
        stations = fetch_usgs_state(state_cd)
        all_stations.extend(stations)
        time.sleep(0.3)  # be polite to USGS API
    print(f"Total USGS stations fetched: {len(all_stations)}")
    return all_stations

def save_to_s3(data: list[dict], key: str) -> None:
    """Save a Python object as JSON to S3 raw bucket."""
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=S3_RAW_BUCKET,
        Key=key,
        Body=json.dumps(data),
        ContentType="application/json"
    )
    print(f"Saved to s3://{S3_RAW_BUCKET}/{key}")


def lambda_handler(event: dict, context: object) -> dict:
    """
    Lambda 1: data_fetcher

    1. POST to GBIF async download API → get downloadKey
    2. Fetch all 50 states of USGS dissolved oxygen data
    3. Save USGS data to S3
    4. Return downloadKey so Step Functions can pass it to the poller
    """
    gbif_username: str = os.environ["GBIF_USERNAME"]
    gbif_password: str = os.environ["GBIF_PASSWORD"]

    # Step 1: kick off GBIF async download (returns immediately)
    download_key: str = request_gbif_download(gbif_username, gbif_password)

    # Step 2: fetch all USGS data while GBIF prepares the file in background
    print("Fetching USGS data for all 50 states...")
    usgs_data: list[dict] = fetch_all_usgs_data()

    # Step 3: save USGS data to S3 so Lambda 2 can read it
    usgs_s3_key: str = "usgs/usgs_dissolved_oxygen_all_states.json"
    save_to_s3(usgs_data, usgs_s3_key)

    # Step 4: return downloadKey — Step Functions will pass this to check_status Lambda
    return {
        "statusCode": 200,
        "downloadKey": download_key,
        "usgs_s3_key": usgs_s3_key
    }

