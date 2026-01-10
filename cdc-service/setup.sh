#!/bin/bash

source ../.env

set -e

DIR=$(pwd)

docker build -t $CDC_SERVICE_IMAGE_NAME -f $DIR/Dockerfile $DIR