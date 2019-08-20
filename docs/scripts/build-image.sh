#!/usr/bin/env bash

imageName=$1

# Go to main project folder from anywhere
cd $(dirname "$(readlink $(test $(uname -s) = 'Linux' && echo "-f") "$0" || echo "$(echo "$0" | sed -e 's,\\,/,g')")")/..

docker build -t ${imageName} -f Dockerfile ..
