#!/usr/bin/env bash

PROJECT_ID=$(gcloud config get-value project)
echo ${PROJECT_ID}

. venv/bin/activate

WORKING_DIR=$(pwd)
export GOOGLE_APPLICATION_CREDENTIALS=${WORKING_DIR}/sa-key.json

# generate data locally AND in bucket (remove parameter to restrict to local generation)
python scripts/007_gen_example.py ${PROJECT_ID}-dt-jd
