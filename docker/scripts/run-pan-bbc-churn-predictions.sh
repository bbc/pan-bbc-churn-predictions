#!/usr/bin/env bash
export AWS_DEFAULT_REGION=eu-west-1
ENV=${ENV:-int}

mkdir /pan-bbc-churn-predictions
cd /pan-bbc-churn-predictions

# Copy code and data in from S3
aws s3 cp s3://${ENV}-insights-pan-bbc-churn-predictions/code/ /pan-bbc-churn-predictions/code --recursive
aws s3 cp s3://${ENV}-insights-pan-bbc-churn-predictions/data/input/training/ /pan-bbc-churn-predictions/data/input/training/ --recursive
aws s3 cp s3://${ENV}-insights-pan-bbc-churn-predictions/data/input/score/ /pan-bbc-churn-predictions/data/input/score/ --recursive

# Run Python code
mkdir -p /pan-bbc-churn-predictions/data/output/propensity_scores

cd /pan-bbc-churn-predictions/code

mkdir -p /pan-bbc-churn-predictions/code/charts/perf

python iplayer_churn_train.py
python iplayer_churn_score.py

# Copy models (pickles) back to S3 ( copy them into root, models)
aws s3 cp /pan-bbc-churn-predictions/code/pickles/ s3://${ENV}-insights-pan-bbc-churn-predictions/models/ --recursive

# Copy charts and logs back to S3 (data/output)
aws s3 cp /pan-bbc-churn-predictions/code/charts/ s3://${ENV}-insights-pan-bbc-churn-predictions/data/output/charts --recursive
aws s3 cp /pan-bbc-churn-predictions/code/logs/ s3://${ENV}-insights-pan-bbc-churn-predictions/data/output/logs --recursive

