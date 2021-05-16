#!/usr/bin/env bash

PROJECT_ID=$(gcloud config get-value project)
echo ${PROJECT_ID}

. venv/bin/activate

WORKING_DIR=$(pwd)
export GOOGLE_APPLICATION_CREDENTIALS=${WORKING_DIR}/sa-key.json

cd src

python json2bq_main.py \
  --input_pattern="${WORKING_DIR}/examples/test_all_types.jsonl" \
  --bq_temp_location="gs://${PROJECT_ID}-dt-jd/temp" \
  --project=${PROJECT_ID} \
  --bq_dataset="testjson2bq" \
  --bq_table="test_all_types_local"

# Uncomment the pipelines below to test schema updates

#python json2bq_main.py \
#  --input_pattern=${WORKING_DIR}/examples/base.jsonl \
#  --bq_temp_location="gs://${PROJECT_ID}-dt-jd/temp" \
#  --project=${PROJECT_ID} \
#  --bq_dataset="testjson2bq" \
#  --bq_table="base_local"
#
#python json2bq_main.py \
#  --input_pattern=${WORKING_DIR}/examples/modified.jsonl \
#  --bq_temp_location="gs://${PROJECT_ID}-dt-jd/temp" \
#  --project=${PROJECT_ID} \
#  --bq_dataset="testjson2bq" \
#  --bq_table="base_local"
