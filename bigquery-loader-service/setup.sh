#!/bin/bash

source ../.env

set -e

DIR=$(pwd)

docker build -t $BIG_QUERY_LOADER_IMAGE_NAME -f $DIR/Dockerfile $DIR