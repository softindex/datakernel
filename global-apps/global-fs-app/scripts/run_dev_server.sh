#!/bin/bash

#
# Builds the docker image and then runs it, stopping any already running instances beforehand
#

IMAGE_NAME=docker-registry.adkernel.com/global-cloud
CONTAINER_NAME=global-cloud

# Go to main project folder from anywhere
cd $(dirname "$(readlink $(test $(uname -s) = 'Linux' && echo "-f") "$0" || echo "$(echo "$0" | sed -e 's,\\,/,g')")")/..

# Stop running containers if any
docker ps --filter "name=$CONTAINER_NAME" -q | xargs -r docker container stop

# Run npm build only because of -v mount bind override for dev
cd src/main/resources/front
npm run-script build
cd ../../../..

# Run a new container
docker run --rm --net=host --name $CONTAINER_NAME -v $PWD/src/main/resources/front:/app/src/main/resources/build $IMAGE_NAME
