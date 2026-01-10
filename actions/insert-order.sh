#!/bin/bash

set -e

if [ $# -lt 8 ]; then
    echo "Error: Missing required arguments"
    echo "Usage: ./insert-order.sh <order.json> <NETWORK_NAME> <MONGO_HOST> <MONGO_PORT> <MONGO_USERNAME> <MONGO_PASSWORD> <MONGO_DATABASE> <IMAGE_NAME>"
    exit 1
fi

ORDER_FILE=$1
NETWORK_NAME=$2
MONGO_HOST=$3
MONGO_PORT=$4
MONGO_USERNAME=$5
MONGO_PASSWORD=$6
MONGO_DATABASE=$7
IMAGE_NAME=$8

if [ ! -f "$ORDER_FILE" ]; then
    echo "Error: File not found: $ORDER_FILE"
    exit 1
fi

ORDER_DIR=$(dirname "$ORDER_FILE")
ORDER_FILENAME=$(basename "$ORDER_FILE")

docker run --rm \
  --network $NETWORK_NAME \
  -v "$(cd "$ORDER_DIR" && pwd):/orders" \
  -e MONGO_HOST=$MONGO_HOST \
  -e MONGO_PORT=$MONGO_PORT \
  -e MONGO_USERNAME=$MONGO_USERNAME \
  -e MONGO_PASSWORD=$MONGO_PASSWORD \
  -e MONGO_DATABASE=$MONGO_DATABASE \
  $IMAGE_NAME "/orders/$ORDER_FILENAME"

echo "Order insertion completed"