# AWS Setup Notes

## Step 1: Create S3 Buckets
1. Go to AWS Console → S3 → Create bucket
2. Create `beaver-pipeline-raw` (store raw API data)
3. Create `beaver-pipeline-processed` (store joined CSV output)
4. Both buckets: keep default settings, block public access ON

## Step 2: Set Up RDS PostgreSQL
1. Go to AWS Console → RDS → Create database
2. Engine: PostgreSQL
3. Template: Free tier (db.t3.micro)
4. DB name: beaver_pipeline
5. Set username and password (save these!)
6. Enable public access: Yes (for connecting from local machine)
7. Once created, connect with psql or DBeaver and run sql/create_tables.sql

## Step 3: Deploy Lambda Function
1. Go to AWS Console → Lambda → Create function
2. Runtime: Python 3.12
3. Upload lambda/handler.py
4. Set environment variables:
   - RAW_BUCKET = beaver-pipeline-raw
   - PROCESSED_BUCKET = beaver-pipeline-processed
   - DB_HOST = your RDS endpoint
   - DB_NAME = beaver_pipeline
   - DB_USER = your RDS username
   - DB_PASSWORD = your RDS password
5. Add Lambda Layer with dependencies from lambda/requirements.txt
6. Set timeout to 15 minutes (maximum)
7. Set memory to 1024MB

## Step 4: Set Up S3 Event Trigger
1. Go to your Lambda function → Add trigger
2. Select S3
3. Bucket: beaver-pipeline-raw
4. Event type: PUT (fires when new file is uploaded)
5. This means every time you upload a new CSV to raw bucket, Lambda runs automatically

## Step 5: Deploy Streamlit Dashboard
1. Push code to GitHub
2. Go to share.streamlit.io
3. Connect your GitHub repo
4. Set main file path: streamlit/app.py
5. Add secrets (DB credentials) in Streamlit Cloud settings
6. Deploy - you get a free public URL!

## Connecting to RDS from Local Machine
```bash
psql -h YOUR_RDS_ENDPOINT -U YOUR_USERNAME -d beaver_pipeline
```
Then run the SQL from sql/create_tables.sql to create the tables.

## Testing Lambda Locally
```bash
cd lambda
pip install -r requirements.txt
python -c "from handler import lambda_handler; lambda_handler({}, {})"
```
