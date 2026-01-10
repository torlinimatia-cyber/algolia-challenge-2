#!/bin/bash

source ../.env

set -e

DIR=$(pwd)

docker build -t $SPARK_PROCESSOR_IMAGE_NAME -f $DIR/Dockerfile $DIR