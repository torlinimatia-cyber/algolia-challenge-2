#!/bin/bash

set -e
source .env

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${GREEN}=== Enable Billing & Create GCS Bucket ===${NC}\n"

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

if [ -z "$GCS_BUCKET" ]; then
    echo -e "${RED}Error: GCS_BUCKET not set in .env${NC}"
    exit 1
fi

GCP_REGION=${GCP_REGION:-europe-west1}

echo -e "${GREEN}Configuration:${NC}"
echo "  Project: $GCP_PROJECT_ID"
echo "  Bucket: $GCS_BUCKET"
echo "  Region: $GCP_REGION"
echo ""

# List available billing accounts
echo -e "${GREEN}Checking for billing accounts...${NC}"
BILLING_ACCOUNTS=$(gcloud billing accounts list --format="value(name)")

if [ -z "$BILLING_ACCOUNTS" ]; then
    echo -e "${RED}No billing accounts found!${NC}"
    echo -e "${YELLOW}You need to create a billing account first.${NC}"
    echo -e "Opening billing account creation page..."
    open "https://console.cloud.google.com/billing/create"
    echo ""
    echo "After creating a billing account, run this script again."
    exit 1
fi

# Display available billing accounts
echo -e "${GREEN}Available billing accounts:${NC}"
gcloud billing accounts list

# Check if project already has billing enabled
CURRENT_BILLING=$(gcloud billing projects describe "$GCP_PROJECT_ID" --format="value(billingAccountName)" 2>/dev/null || echo "")

if [ -n "$CURRENT_BILLING" ]; then
    echo -e "\n${GREEN}✓ Project already has billing enabled${NC}"
    echo "  Billing account: $CURRENT_BILLING"
else
    # Get first billing account
    BILLING_ACCOUNT=$(echo "$BILLING_ACCOUNTS" | head -n 1)
    
    if [ -z "$BILLING_ACCOUNT" ]; then
        echo -e "${RED}Error: Could not find a billing account${NC}"
        exit 1
    fi
    
    echo -e "\n${GREEN}Linking billing account to project...${NC}"
    echo "  Billing account: $BILLING_ACCOUNT"
    echo "  Project: $GCP_PROJECT_ID"
    
    gcloud billing projects link "$GCP_PROJECT_ID" --billing-account="$BILLING_ACCOUNT"
    
    echo -e "${GREEN}✓ Billing enabled successfully${NC}"
    
    # Wait a moment for billing to propagate
    echo "Waiting for billing to propagate..."
    sleep 5
fi

# Enable Cloud Storage API
echo -e "\n${GREEN}Enabling Cloud Storage API...${NC}"
gcloud services enable storage.googleapis.com --project="$GCP_PROJECT_ID"
echo -e "${GREEN}✓ Cloud Storage API enabled${NC}"

# Create GCS Bucket
echo -e "\n${GREEN}Creating GCS bucket: $GCS_BUCKET${NC}"

ACCESS_TOKEN=$(gcloud auth print-access-token)

BUCKET_PAYLOAD=$(cat <<EOF
{
  "name": "$GCS_BUCKET",
  "location": "$GCP_REGION",
  "storageClass": "STANDARD"
}
EOF
)

HTTP_CODE=$(curl -s -o /tmp/gcs_response.json -w "%{http_code}" \
  -X POST \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  -d "$BUCKET_PAYLOAD" \
  "https://storage.googleapis.com/storage/v1/b?project=$GCP_PROJECT_ID")

if [ "$HTTP_CODE" = "200" ] || [ "$HTTP_CODE" = "201" ]; then
    echo -e "${GREEN}✓ Bucket created successfully${NC}"
elif [ "$HTTP_CODE" = "409" ]; then
    echo -e "${YELLOW}✓ Bucket already exists${NC}"
else
    echo -e "${RED}✗ Failed to create bucket (HTTP $HTTP_CODE)${NC}"
    cat /tmp/gcs_response.json
    echo ""
    exit 1
fi

# Verify bucket
echo -e "\n${GREEN}Verifying bucket...${NC}"
curl -s -H "Authorization: Bearer $ACCESS_TOKEN" \
  "https://storage.googleapis.com/storage/v1/b/${GCS_BUCKET}" \
  | grep -q "name" && echo -e "${GREEN}✓ Bucket exists: $GCS_BUCKET${NC}" || echo -e "${RED}✗ Bucket not found${NC}"

# Cleanup
rm -f /tmp/gcs_response.json
