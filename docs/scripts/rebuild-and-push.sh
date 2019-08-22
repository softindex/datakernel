#!/usr/bin/env bash

set -e

region=$1
url=$2
imageName=$3
cluster=$4
service=$5

cd $(dirname "$(readlink $(test $(uname -s) = 'Linux' && echo "-f") "$0" || echo "$(echo "$0" | sed -e 's,\\,/,g')")")/..
testResult=$(mvn test -Dtest=ValidationTest)
if [[ $testResult == *'BUILD SUCCESS'* ]]; then
    # Go to main project folder from anywhere

    ./scripts/build-image.sh ${imageName}
    ./scripts/push-image.sh ${region} ${url} ${imageName}
    ./scripts/restart-tasks.sh ${region} ${cluster} ${service}
    ./scripts/check-error-deploying.sh ${region} ${cluster}
else
    echo "Error test"
fi
