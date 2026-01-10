#!/bin/bash

set -e

source .env

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${GREEN}=== GCP Initialization & Authentication ===${NC}\n"

# Get script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Check if gcloud is installed
if ! command -v gcloud &> /dev/null; then
    echo -e "${RED}Error: gcloud CLI not found${NC}"
    echo -e "${YELLOW}Install it from: https://cloud.google.com/sdk/docs/install${NC}"
    exit 1
fi

echo -e "${BLUE}Current gcloud version:${NC}"
gcloud version | head -n 1
echo ""

# Check if GCP_PROJECT_ID is set
if [ -z "$GCP_PROJECT_ID" ]; then
    echo -e "${RED}Error: GCP_PROJECT_ID not set in .env${NC}"
    exit 1
fi

echo -e "${GREEN}Project ID from .env: ${YELLOW}$GCP_PROJECT_ID${NC}\n"

# Step 1: Authenticate user account
echo -e "${GREEN}Step 1: Authenticating your Google account${NC}"
echo -e "${YELLOW}This will open your browser for authentication...${NC}"
read -p "Press Enter to continue..."

# Run gcloud auth login and wait for it to complete
gcloud auth login

AUTH_STATUS=$?

if [ $AUTH_STATUS -eq 0 ]; then
    echo -e "${GREEN}✓ User authentication successful${NC}"
    
    # Verify authentication worked
    ACCOUNT=$(gcloud auth list --filter=status:ACTIVE --format="value(account)")
    if [ -n "$ACCOUNT" ]; then
        echo -e "${GREEN}  Authenticated as: $ACCOUNT${NC}\n"
    else
        echo -e "${RED}✗ No active account found${NC}"
        exit 1
    fi
else
    echo -e "${RED}✗ Authentication failed${NC}"
    exit 1
fi

# Step 2: Set the active project
echo -e "${GREEN}Step 2: Setting active project${NC}"
gcloud config set project "$GCP_PROJECT_ID"

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Active project set to: $GCP_PROJECT_ID${NC}\n"
else
    echo -e "${RED}✗ Failed to set project${NC}"
    exit 1
fi

# Step 3: Verify project access
echo -e "${GREEN}Step 3: Verifying project access${NC}"
PROJECT_INFO=$(gcloud projects describe "$GCP_PROJECT_ID" 2>&1)

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Project access verified${NC}"
    echo "$PROJECT_INFO" | grep "projectId\|name\|projectNumber"
    echo ""
else
    echo -e "${RED}✗ Cannot access project${NC}"
    echo "$PROJECT_INFO"
    echo ""
    echo -e "${YELLOW}Make sure:${NC}"
    echo "  1. The project ID '$GCP_PROJECT_ID' is correct"
    echo "  2. You have access to this project"
    echo "  3. The project exists in your GCP account"
    exit 1
fi

# Step 4: Set up Application Default Credentials
echo -e "${GREEN}Step 4: Setting up Application Default Credentials${NC}"
echo -e "${YELLOW}This is needed for Docker containers to authenticate${NC}"
echo -e "${YELLOW}Another browser window will open...${NC}"
read -p "Press Enter to continue..."

# Run gcloud auth application-default login and wait
gcloud auth application-default login

ADC_STATUS=$?

if [ $ADC_STATUS -eq 0 ]; then
    echo -e "${GREEN}✓ Application Default Credentials configured${NC}\n"
    
    # Wait a moment for file to be written
    sleep 2
    
    # Verify credentials file exists
    if [ -f "$HOME/.config/gcloud/application_default_credentials.json" ]; then
        echo -e "${GREEN}✓ Credentials file created: ~/.config/gcloud/application_default_credentials.json${NC}\n"
    else
        echo -e "${YELLOW}⚠ Waiting for credentials file...${NC}"
        sleep 3
        
        if [ -f "$HOME/.config/gcloud/application_default_credentials.json" ]; then
            echo -e "${GREEN}✓ Credentials file found${NC}\n"
        else
            echo -e "${RED}✗ Credentials file not found${NC}"
            echo -e "${YELLOW}Expected location: ~/.config/gcloud/application_default_credentials.json${NC}"
            exit 1
        fi
    fi
else
    echo -e "${RED}✗ Failed to configure ADC${NC}"
    exit 1
fi

# Step 5: Enable required APIs
echo -e "${GREEN}Step 5: Enabling required APIs${NC}"
echo "This may take a minute..."

APIS=(
    "bigquery.googleapis.com"
    "storage.googleapis.com"
)

for API in "${APIS[@]}"; do
    echo -n "  Enabling $API... "
    OUTPUT=$(gcloud services enable "$API" --project="$GCP_PROJECT_ID" 2>&1)
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓${NC}"
    else
        echo -e "${RED}✗${NC}"
        echo "    $OUTPUT"
    fi
done

echo ""

# Step 6: Display current configuration
echo -e "${GREEN}=== Current Configuration ===${NC}"
echo -e "${BLUE}Active account:${NC} $(gcloud config get-value account)"
echo -e "${BLUE}Active project:${NC} $(gcloud config get-value project)"
echo -e "${BLUE}Project ID:${NC} $GCP_PROJECT_ID"
echo ""

# Step 7: Test API access
echo -e "${GREEN}Step 6: Testing API access${NC}"
echo "Getting access token..."

# Get access token and verify it works
ACCESS_TOKEN=$(gcloud auth print-access-token 2>&1)
if [ $? -ne 0 ]; then
    echo -e "${RED}✗ Failed to get access token${NC}"
    echo "$ACCESS_TOKEN"
    exit 1
fi

echo -e "${GREEN}✓ Access token obtained${NC}"

echo -n "  Testing BigQuery API... "
BQ_TEST=$(curl -s -w "%{http_code}" -o /tmp/bq_test.json \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  "https://bigquery.googleapis.com/bigquery/v2/projects/$GCP_PROJECT_ID/datasets")

if [ "$BQ_TEST" = "200" ]; then
    echo -e "${GREEN}✓${NC}"
else
    echo -e "${RED}✗ (HTTP $BQ_TEST)${NC}"
fi

echo -n "  Testing Storage API... "
GCS_TEST=$(curl -s -w "%{http_code}" -o /tmp/gcs_test.json \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  "https://storage.googleapis.com/storage/v1/b?project=$GCP_PROJECT_ID")

if [ "$GCS_TEST" = "200" ]; then
    echo -e "${GREEN}✓${NC}"
else
    echo -e "${RED}✗ (HTTP $GCS_TEST)${NC}"
fi

# Cleanup temp files
rm -f /tmp/bq_test.json /tmp/gcs_test.json


