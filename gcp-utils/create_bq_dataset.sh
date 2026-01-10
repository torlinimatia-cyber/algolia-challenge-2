#!/bin/bash

set -e
source .env

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${GREEN}=== Creating BigQuery & GCS Resources ===${NC}\n"

# Get script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Load .env
if [ -f "$SCRIPT_DIR/.env" ]; then
    export $(cat "$SCRIPT_DIR/.env" | grep -v '^#' | grep -v '^$' | xargs)
else
    echo -e "${RED}Error: .env file not found${NC}"
    exit 1
fi

# Check required variables
if [ -z "$GCP_PROJECT_ID" ]; then
    echo -e "${RED}Error: GCP_PROJECT_ID not set in .env${NC}"
    exit 1
fi

BQ_DATASET=${BQ_DATASET:-etl_warehouse}
BQ_LOCATION=${BQ_LOCATION:-EU}
GCP_REGION=${GCP_REGION:-europe-west1}

if [ -z "$GCS_BUCKET" ]; then
    echo -e "${RED}Error: GCS_BUCKET not set in .env${NC}"
    exit 1
fi

echo -e "${GREEN}Configuration:${NC}"
echo "  Project: $GCP_PROJECT_ID"
echo "  Dataset: $BQ_DATASET (Location: $BQ_LOCATION)"
echo "  Bucket: $GCS_BUCKET (Region: $GCP_REGION)"
echo ""

# Get access token
echo -e "${GREEN}Getting access token...${NC}"
ACCESS_TOKEN=$(gcloud auth print-access-token)

# Create BigQuery Dataset
echo -e "\n${GREEN}Creating BigQuery dataset: $BQ_DATASET${NC}"

DATASET_PAYLOAD=$(cat <<EOF
{
  "datasetReference": {
    "projectId": "$GCP_PROJECT_ID",
    "datasetId": "$BQ_DATASET"
  },
  "location": "$BQ_LOCATION",
  "description": "ETL Pipeline Warehouse"
}
EOF
)

HTTP_CODE=$(curl -s -o /tmp/bq_response.json -w "%{http_code}" \
  -X POST \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  -d "$DATASET_PAYLOAD" \
  "https://bigquery.googleapis.com/bigquery/v2/projects/$GCP_PROJECT_ID/datasets")

if [ "$HTTP_CODE" = "200" ] || [ "$HTTP_CODE" = "201" ]; then
    echo -e "${GREEN}✓ Dataset created successfully${NC}"
elif [ "$HTTP_CODE" = "409" ]; then
    echo -e "${YELLOW}✓ Dataset already exists${NC}"
else
    echo -e "${RED}✗ Failed to create dataset (HTTP $HTTP_CODE)${NC}"
    cat /tmp/bq_response.json
    echo ""
fi


# Cleanup temp files
rm -f /tmp/bq_response.json /tmp/gcs_response.json

