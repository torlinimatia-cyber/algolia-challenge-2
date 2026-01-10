#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

cd "$SCRIPT_DIR"

echo "Stopping CDC services..."
docker-compose -f docker-compose-cdc.yml down

echo "Stopping storage services..."
docker-compose -f docker-compose-storage.yml down

echo "Stopping processing services..."
docker-compose -f docker-compose-processing.yml down

echo "All services stopped"
echo "Cleanup complete"