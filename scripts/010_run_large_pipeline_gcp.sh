#!/usr/bin/env bash

PROJECT_ID=$(gcloud config get-value project)
echo ${PROJECT_ID}

# Options, taken from context, but with default fallbacks
REGION=${REGION:-'europe-west1'}
BUCKET_NAME=${BUCKET_NAME:-"${PROJECT_ID}-dt-jd"}
SA_EMAIL=${SA_EMAIL:-"sa-json2bq@${PROJECT_ID}.iam.gserviceaccount.com"}
MAX_NUM_WORKERS=${MAX_NUM_WORKERS:-20}


. venv/bin/activate


WORKING_DIR=$(pwd)
export GOOGLE_APPLICATION_CREDENTIALS=${WORKING_DIR}/sa-key.json

cd src

python json2bq_main.py \
  --job_name=json2bq-job2 \
  --project=${PROJECT_ID} \
  --region=${REGION} \
  --max_num_workers=${MAX_NUM_WORKERS} \
  --service_account_email=${SA_EMAIL} \
  --runner=DataflowRunner \
  --temp_location="gs://${BUCKET_NAME}/temp" \
  --setup_script="${WORKING_DIR}/src/setup.py" \
  --input_pattern="gs://${BUCKET_NAME}/examples/large/*.jsonl" \
  --bq_dataset="testjson2bq" \
  --bq_table="large_dataset"
