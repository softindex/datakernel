#!/usr/bin/env bash

set -e

region=$1
url=$2
imageName=$3

# Login to aws
LOGIN_CMD=$(aws ecr get-login --no-include-email --region ${region})
${LOGIN_CMD}

# Delete redundant images
IMAGES_TO_DELETE=$( aws ecr list-images --region ${region} --repository-name ${imageName} --filter="tagStatus=UNTAGGED" --query 'imageIds[*]' --output json )
aws ecr batch-delete-image --region ${region} --repository-name ${imageName} --image-ids "$IMAGES_TO_DELETE" || true

# Push to ECR
docker tag ${imageName}:latest ${url}/${imageName}:latest
docker push ${url}/${imageName}:latest

