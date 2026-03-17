import os
import requests

# Lambda 1: data_fetcher
# Kicks off an async GBIF download request for all US beaver records.
# Returns downloadKey immediately — GBIF prepares the zip in the background.
# USGS fetching now handled by Lambda 4 (beaver-usgs-fetcher) running in parallel.

GBIF_DOWNLOAD_URL = "https://api.gbif.org/v1/occurrence/download/request"

def request_gbif_download(gbif_username: str, gbif_password: str) -> str:
    predicate = {
        "creator": gbif_username,
        "notificationAddresses": [],
        "sendNotification": False,
        "format": "SIMPLE_CSV",
        "predicate": {
            "type": "and",
            "predicates": [
                {"type": "equals", "key": "TAXON_KEY", "value": "2439838"},
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

def lambda_handler(event: dict, context: object) -> dict:
    gbif_username: str = os.environ["GBIF_USERNAME"]
    gbif_password: str = os.environ["GBIF_PASSWORD"]
    download_key: str = request_gbif_download(gbif_username, gbif_password)
    return {
        "statusCode": 200,
        "downloadKey": download_key
    }