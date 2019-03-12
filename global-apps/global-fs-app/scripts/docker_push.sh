#!/bin/bash

#
# Builds and then pushes the docker image to the registry
#

# Go to main project folder from anywhere
cd $(dirname "$(readlink $(test $(uname -s) = 'Linux' && echo "-f") "$0" || echo "$(echo "$0" | sed -e 's,\\,/,g')")")/..

# Build docker image
bash scripts/docker_build.sh || exit $?

# Push image to registry
docker push docker-registry.adkernel.com/global-cloud
