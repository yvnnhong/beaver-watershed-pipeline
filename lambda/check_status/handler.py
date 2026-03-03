import os
import requests

# Lambda 2: check_status
# Called repeatedly by Step Functions to check if GBIF has finished preparing the beaver data zip file.
# Takes the downloadKey from Lambda 1, hits the GBIF status endpoint, and returns the status.
# Step Functions will keep looping back to this Lambda every 5 minutes until status is SUCCEEDED.

# WHY THIS EXISTS:
# GBIF takes 5-15 minutes to prepare large async downloads — we can't just sit and wait in one Lambda.
# Instead Step Functions calls this tiny Lambda in a loop, each invocation only takes ~2 seconds,
# so we're not burning Lambda runtime while GBIF does its thing on their servers.

GBIF_DOWNLOAD_URL = "https://api.gbif.org/v1/occurrence/download"

def check_gbif_status(download_key: str, gbif_username: str, gbif_password: str) -> dict:
    """
    Poll GBIF for the status of an async download request.
    Returns status and download URL if ready.
    """
    response = requests.get(
        f"{GBIF_DOWNLOAD_URL}/{download_key}",
        auth=(gbif_username, gbif_password),
        timeout=30
    )
    response.raise_for_status()
    data = response.json()

    status: str = data.get("status", "UNKNOWN")
    download_url: str = data.get("downloadLink", "")

    print(f"GBIF download {download_key} status: {status}")
    return {
        "status": status,
        "downloadUrl": download_url
    }


def lambda_handler(event: dict, context: object) -> dict:
    """
    Lambda 2: check_status
    Receives downloadKey from Step Functions, polls GBIF, returns status.
    """
    gbif_username: str = os.environ["GBIF_USERNAME"]
    gbif_password: str = os.environ["GBIF_PASSWORD"]

    # Step Functions passes the output of Lambda 1 as this Lambda's event
    download_key: str = event["downloadKey"]
    usgs_s3_key: str = event["usgs_s3_key"]

    result: dict = check_gbif_status(download_key, gbif_username, gbif_password)

    # Pass everything forward so Step Functions can route to processor when ready
    return {
        "status": result["status"],
        "downloadUrl": result["downloadUrl"],
        "downloadKey": download_key,
        "usgs_s3_key": usgs_s3_key
    }