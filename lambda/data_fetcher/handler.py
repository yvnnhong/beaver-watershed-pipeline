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
GBIF_DOWNLOAD_URL = "https://api.gbif.org/v1/occurrence/download"

