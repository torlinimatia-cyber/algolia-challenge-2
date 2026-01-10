
set -e 


source .env

ACCESS_TOKEN=$(gcloud auth print-access-token)

# Verify resources
echo -e "\n${GREEN}Verifying resources...${NC}"

# Check dataset
curl -s -H "Authorization: Bearer $ACCESS_TOKEN" \
  "https://bigquery.googleapis.com/bigquery/v2/projects/$GCP_PROJECT_ID/datasets" \
  | grep -q "$BQ_DATASET" && echo -e "${GREEN}✓ Dataset exists: $BQ_DATASET${NC}" || echo -e "${RED}✗ Dataset not found${NC}"

# Check bucket
curl -s -H "Authorization: Bearer $ACCESS_TOKEN" \
  "https://storage.googleapis.com/storage/v1/b?project=$GCP_PROJECT_ID" \
  | grep -q "$GCS_BUCKET" && echo -e "${GREEN}✓ Bucket exists: $GCS_BUCKET${NC}" || echo -e "${RED}✗ Bucket not found${NC}"
