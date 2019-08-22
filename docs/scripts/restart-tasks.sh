#!/usr/bin/env bash

set -e

region=$1
cluster=$2
service=$3

echo force deploy
aws ecs update-service --cluster ${cluster} --service ${service} --force-new-deployment --region ${region} > /dev/null