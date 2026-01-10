source .env && ./actions/insert-order.sh data-samples/order-2.json $NETWORK_NAME $MONGO_HOST $MONGO_PORT $MONGO_USERNAME $MONGO_PASSWORD $MONGO_DATABASE $INSERT_ORDER_IMAGE_NAME

rm -rf $pwd/logs
bash generate_logs.sh