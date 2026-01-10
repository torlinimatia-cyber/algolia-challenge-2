#!/bin/bash

set -e
source .env

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

cd "$SCRIPT_DIR"
cd insert-order-service && bash setup.sh
cd "$SCRIPT_DIR"

docker-compose -f docker-compose-storage.yml up -d --build

sleep 10

echo "Initializing replica set..."
docker exec -it ${MONGO_CONTAINER_NAME} mongosh --eval "
try {
  rs.status();
  print('Replica set already initialized');
} catch(err) {
  rs.initiate({
    _id: 'rs0',
    members: [
      { _id: 0, host: '${MONGO_HOST}:${MONGO_PORT}' }
    ]
  });
  print('Replica set initialized successfully');
}
"
docker-compose -f docker-compose-cdc.yml up -d --build
sleep 15

source .env && ./actions/insert-order.sh data-samples/order-1.json $NETWORK_NAME $MONGO_HOST $MONGO_PORT $MONGO_USERNAME $MONGO_PASSWORD $MONGO_DATABASE $INSERT_ORDER_IMAGE_NAME 
sleep 5

docker-compose -f docker-compose-process.yml up -d --build

# cd gcp-utils
# bash init_and_authenticate.sh
# bash create_bq_dataset.sh
# bash create_gcp_bucket.sh
# bash verify_gcp_resources.sh
# cd "$SCRIPT_DIR"


echo "Setup complete"
# bash analysis_tool.sh

