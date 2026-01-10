#!/bin/bash

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${GREEN}=== BigQuery Query Tool ===${NC}\n"

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ -f "$SCRIPT_DIR/.env" ]; then
    export $(cat "$SCRIPT_DIR/.env" | grep -v '^#' | grep -v '^$' | xargs)
else
    echo -e "${RED}Error: .env file not found${NC}"
    exit 1
fi

if [ -z "$GCP_PROJECT_ID" ]; then
    echo -e "${RED}Error: GCP_PROJECT_ID not set in .env${NC}"
    exit 1
fi

BQ_DATASET=${BQ_DATASET:-etl_warehouse}

ACCESS_TOKEN=$(gcloud auth print-access-token)

run_query() {
    local query="$1"
    local query_name="$2"
    
    echo -e "\n${BLUE}Running: $query_name${NC}"
    echo -e "${YELLOW}Query: $query${NC}\n"
    
    QUERY_PAYLOAD=$(cat <<EOF
{
  "query": "$query",
  "useLegacySql": false,
  "location": "EU"
}
EOF
)
    
    curl -s -X POST \
      -H "Authorization: Bearer $ACCESS_TOKEN" \
      -H "Content-Type: application/json" \
      -d "$QUERY_PAYLOAD" \
      "https://bigquery.googleapis.com/bigquery/v2/projects/$GCP_PROJECT_ID/queries" \
      > /tmp/bq_result.json
    
    if grep -q '"jobComplete": true' /tmp/bq_result.json; then
        echo -e "${GREEN}✓ Query completed${NC}\n"
        
        if command -v jq &> /dev/null; then
            cat /tmp/bq_result.json | jq -r '.rows[]? | .f | map(.v) | @tsv'
        else
            grep -o '"v":"[^"]*"' /tmp/bq_result.json | sed 's/"v":"\([^"]*\)"/\1\t/g'
        fi
        echo ""
    else
        echo -e "${RED}✗ Query failed${NC}"
        cat /tmp/bq_result.json
        echo ""
    fi
}

echo -e "${GREEN}Available queries:${NC}"
echo "  1. List all tables in dataset"
echo "  2. Count rows in orders_fact"
echo "  3. Show sample orders (limit 10)"
echo "  4. Show orders by user"
echo "  5. Show orders summary"
echo "  6. Custom query"
echo "  7. Exit"
echo ""

read -p "Select option (1-7): " option

case $option in
    1)
        echo -e "\n${GREEN}Tables in $BQ_DATASET:${NC}"
        curl -s -H "Authorization: Bearer $ACCESS_TOKEN" \
          "https://bigquery.googleapis.com/bigquery/v2/projects/$GCP_PROJECT_ID/datasets/$BQ_DATASET/tables" \
          | grep -o '"tableId":"[^"]*"' | sed 's/"tableId":"\([^"]*\)"/\1/g'
        echo ""
        ;;
    
    2)
        QUERY="SELECT COUNT(*) as total_rows FROM \\\`$GCP_PROJECT_ID.$BQ_DATASET.orders_fact\\\`"
        run_query "$QUERY" "Count rows in orders_fact"
        ;;
    
    3)
        QUERY="SELECT order_id, user_id, product_name, quantity, price, line_total FROM \\\`$GCP_PROJECT_ID.$BQ_DATASET.orders_fact\\\` LIMIT 10"
        run_query "$QUERY" "Sample orders"
        ;;
    
    4)
        QUERY="SELECT user_id, COUNT(DISTINCT order_id) as num_orders, SUM(line_total) as total_spent FROM \\\`$GCP_PROJECT_ID.$BQ_DATASET.orders_fact\\\` GROUP BY user_id ORDER BY total_spent DESC"
        run_query "$QUERY" "Orders by user"
        ;;
    
    5)
        QUERY="SELECT * FROM \\\`$GCP_PROJECT_ID.$BQ_DATASET.orders_summary\\\` LIMIT 10"
        run_query "$QUERY" "Orders summary table"
        ;;
    
    6)
        echo -e "\n${YELLOW}Enter your SQL query (press Ctrl+D when done):${NC}"
        CUSTOM_QUERY=$(cat)
        ESCAPED_QUERY=$(echo "$CUSTOM_QUERY" | sed 's/`/\\`/g' | sed 's/"/\\"/g')
        run_query "$ESCAPED_QUERY" "Custom query"
        ;;
    
    7)
        echo "Exiting..."
        exit 0
        ;;
    
    *)
        echo -e "${RED}Invalid option${NC}"
        exit 1
        ;;
esac

rm -f /tmp/bq_result.json