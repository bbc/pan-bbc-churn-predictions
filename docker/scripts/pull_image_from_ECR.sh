#!/usr/bin/env bash

export AWS_DEFAULT_REGION=eu-west-1
ENV=${ENV:-int}

ACCOUNT="639227811136"
IMAGE="pan-bbc-churn-prediction"
TAG="0.0.1"

ECR_PW=$(aws ecr get-login --region eu-west-1 --registry-ids ${ACCOUNT} | cut -d' ' -f6)
docker login https://${ACCOUNT}.dkr.ecr.eu-west-1.amazonaws.com -u AWS -p $ECR_PW
docker pull ${ACCOUNT}.dkr.ecr.eu-west-1.amazonaws.com/${IMAGE}:${TAG}
