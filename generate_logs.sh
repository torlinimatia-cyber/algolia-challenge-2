#!/bin/bash

set -e

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${GREEN}=== Container Logs Generator (Filtered) ===${NC}\n"

# Get current directory
CURRENT_DIR=$(pwd)
LOG_DIR="$CURRENT_DIR/logs"

# Load .env file
if [ -f "$CURRENT_DIR/.env" ]; then
    echo -e "${GREEN}Loading configuration from .env${NC}"
    export $(cat "$CURRENT_DIR/.env" | grep -v '^#' | grep -v '^$' | xargs)
else
    echo -e "${RED}Error: .env file not found${NC}"
    exit 1
fi

# Create logs directory if it doesn't exist
if [ ! -d "$LOG_DIR" ]; then
    echo -e "${GREEN}Creating logs directory: $LOG_DIR${NC}"
    mkdir -p "$LOG_DIR"
else
    echo -e "${YELLOW}Logs directory already exists: $LOG_DIR${NC}"
fi

# Get timestamp for log files
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

# Get container names from .env (only *_CONTAINER_NAME variables)
DEFINED_CONTAINERS=(
    "$MONGO_CONTAINER_NAME"
    "$CDC_CONTAINER_NAME"
    "$KAFKA_CONTAINER_NAME"
    "$ZOOKEEPER_CONTAINER_NAME"
    "$SPARK_PROCESSOR_CONTAINER_NAME"
    "$BQ_LOADER_CONTAINER_NAME"
)

# Remove empty values and duplicates
DEFINED_CONTAINERS=($(printf "%s\n" "${DEFINED_CONTAINERS[@]}" | grep -v '^$' | sort -u))

echo -e "${GREEN}Containers from .env: ${#DEFINED_CONTAINERS[@]}${NC}"
for container in "${DEFINED_CONTAINERS[@]}"; do
    echo -e "  - $container"
done
echo ""

# Check which containers are actually running
echo -e "${GREEN}Checking which containers are running...${NC}\n"

# Filter logs based on container type
filter_logs_by_container() {
    local container=$1
    local log_file=$2
    
    # Add header
    {
        echo "========================================"
        echo "Container: $container"
        echo "Generated: $(date)"
        echo "========================================"
        echo ""
    } > "$log_file"
    
    # Filter based on container name pattern
    case $container in
        *mongo*)
            echo "=== MongoDB Logs ===" >> "$log_file"
            docker logs "$container" 2>&1 | \
                grep -iE 'connection|replica|initiate|ready|waiting|error|fail|warning|listening' \
                >> "$log_file" 2>&1 || echo "No significant events" >> "$log_file"
            ;;
        
        *kafka*)
            echo "=== Kafka Logs ===" >> "$log_file"
            docker logs "$container" 2>&1 | \
                grep -iE 'started|broker|connect|topic|partition|leader|error|fail|warn|listening' | \
                grep -viE 'fetching|adding file|copied to|downloading' \
                >> "$log_file" 2>&1 || echo "No significant events" >> "$log_file"
            ;;
        
        *zookeeper*)
            echo "=== Zookeeper Logs ===" >> "$log_file"
            docker logs "$container" 2>&1 | \
                grep -iE 'binding|started|leader|follower|session|connect|error|fail|listening' \
                >> "$log_file" 2>&1 || echo "No significant events" >> "$log_file"
            ;;
        
        *cdc*)
            echo "=== CDC Consumer Logs ===" >> "$log_file"
            docker logs "$container" 2>&1 | \
                grep -iE 'connect|watching|publish|event|insert|update|delete|topic|partition|offset|error|fail|success|started' \
                >> "$log_file" 2>&1 || echo "No significant events" >> "$log_file"
            ;;
        
        *spark*)
            echo "=== Spark Processor Logs ===" >> "$log_file"
            docker logs "$container" 2>&1 | \
                grep -iE 'creating spark|session created|started|reading|writing|processing|batch|waiting for data|error|fail|warn|completed|fatal' | \
                grep -viE 'downloading|successful.*jar|fetching|copying|added jar|added file' \
                >> "$log_file" 2>&1 || echo "No significant events" >> "$log_file"
            ;;
        
        *bq*|*bigquery*)
            echo "=== BigQuery Loader Logs ===" >> "$log_file"
            docker logs "$container" 2>&1 | \
                grep -iE 'creating|uploading|loading|found|processing|table|bucket|success|complete|error|fail|warn|rows|started|monitoring' \
                >> "$log_file" 2>&1 || echo "No significant events" >> "$log_file"
            ;;
        
        *)
            echo "=== Application Logs ===" >> "$log_file"
            docker logs "$container" 2>&1 | \
                grep -iE 'error|fail|success|connect|disconnect|start|stop|ready|listen|publish|consume|insert|update|delete|load|upload|created|completed|process|exception|warning' | \
                grep -viE 'debug|trace|verbose|downloading|fetching file' \
                >> "$log_file" 2>&1 || echo "No significant events" >> "$log_file"
            ;;
    esac
}

# Process each defined container
SUCCESS_COUNT=0
FAIL_COUNT=0
SKIPPED_COUNT=0

for container in "${DEFINED_CONTAINERS[@]}"; do
    # Check if container is running
    if docker ps --format "{{.Names}}" | grep -q "^${container}$"; then
        echo -e "${BLUE}Processing: $container${NC}"
        
        LOG_FILE="$LOG_DIR/${container}_${TIMESTAMP}.log"
        
        # Filter and save logs
        filter_logs_by_container "$container" "$LOG_FILE"
        
        # Check if log file has content beyond header
        LINE_COUNT=$(wc -l < "$LOG_FILE")
        
        if [ "$LINE_COUNT" -gt 5 ]; then
            FILE_SIZE=$(ls -lh "$LOG_FILE" | awk '{print $5}')
            echo -e "${GREEN}  ✓ Saved: ${LOG_FILE##*/} (${FILE_SIZE}, $LINE_COUNT lines)${NC}"
            SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
        else
            echo -e "${YELLOW}  ⚠ No significant logs found${NC}"
            SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
        fi
    else
        echo -e "${YELLOW}⊘ Skipped: $container (not running)${NC}"
        SKIPPED_COUNT=$((SKIPPED_COUNT + 1))
    fi
done

echo -e "  Logs directory: ${BLUE}$LOG_DIR${NC}"
echo -e "  Processed: ${GREEN}$SUCCESS_COUNT${NC}"
echo -e "  Skipped (not running): ${YELLOW}$SKIPPED_COUNT${NC}"
echo -e "  Failed: ${RED}$FAIL_COUNT${NC}"
echo -e "  Total files created: $(ls -1 "$LOG_DIR"/*_${TIMESTAMP}.log 2>/dev/null | wc -l | tr -d ' ')"


