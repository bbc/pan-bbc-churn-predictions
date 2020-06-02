#!/bin/bash
set -e

# Builds jenkins-docker locally, ready for use by run-stacks.sh
IMAGE="pan-bbc-churn-predictions"

docker build -t ${IMAGE} .

