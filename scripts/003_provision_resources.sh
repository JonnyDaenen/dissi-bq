#!/usr/bin/env bash

PROJECT_ID=$(gcloud config get-value project)
echo ${PROJECT_ID}

# Create bucket
gsutil mb -p ${PROJECT_ID} -l EUROPE-WEST1 -b on gs://${PROJECT_ID}-dt-jd

# Upload files
gsutil cp ./examples/base.jsonl gs://${PROJECT_ID}-dt-jd/examples/input/base.jsonl
gsutil cp ./examples/modified.jsonl gs://${PROJECT_ID}-dt-jd/examples/input/modified.jsonl


# Create Service Account

gcloud iam service-accounts create sa-json2bq
gcloud iam service-accounts keys create sa-key.json --iam-account=sa-json2bq@${PROJECT_ID}.iam.gserviceaccount.com

# Bind the role to the JSON2BQ SA on a project level
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
    --member="serviceAccount:sa-json2bq@${PROJECT_ID}.iam.gserviceaccount.com" \
    --role="roles/owner"


bq --location=EU mk -d \
--description "Test dataset for JSON2BQ." \
testjson2bq
