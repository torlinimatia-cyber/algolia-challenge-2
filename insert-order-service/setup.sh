#!/bin/bash

source ../.env

set -e

DIR=$(pwd)

docker build -t $INSERT_ORDER_IMAGE_NAME -f $DIR/Dockerfile $DIR