#!/bin/bash

#
# Builds the docker image
#

# Go to main project folder from anywhere
cd $(dirname "$(readlink $(test $(uname -s) = 'Linux' && echo "-f") "$0" || echo "$(echo "$0" | sed -e 's,\\,/,g')")")/..

# Build docker image
docker build . -t global-ot-chat --build-arg SSH_KEY="$(cat ~/.ssh/id_rsa)"
