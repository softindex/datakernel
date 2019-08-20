#!/usr/bin/env bash

region=$1
url=$2
imageName=$3
cluster=$4
taskDefinition=$5

# Go to main project folder from anywhere
cd $(dirname "$(readlink $(test $(uname -s) = 'Linux' && echo "-f") "$0" || echo "$(echo "$0" | sed -e 's,\\,/,g')")")/..

./scripts/build-image.sh ${imageName}
./scripts/push-image.sh ${region} ${url} ${imageName}
./scripts/restart-tasks.sh ${region} ${cluster} ${taskDefinition}

