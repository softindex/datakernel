#!/bin/bash

#
# Builds the docker image and then runs it, stopping any already running instances beforehand
#

IMAGE_NAME=global-todo
CONTAINER_NAME=global-todo

# Go to main project folder from anywhere
cd $(dirname "$(readlink $(test $(uname -s) = 'Linux' && echo "-f") "$0" || echo "$(echo "$0" | sed -e 's,\\,/,g')")")/..

# Stop running containers if any
docker ps --filter "name=$CONTAINER_NAME" -q | xargs -r docker container stop

# Run npm build only because of -v mount bind override for dev
cd front
npm run-script build
cd ..

# Run a new container
docker run --rm -d --net=host --name $CONTAINER_NAME -v $PWD/front/build:/app/front/build $IMAGE_NAME
