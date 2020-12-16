export AWS_DEFAULT_REGION=eu-west-1
ENV=${ENV:-int}

current_date=`python get_current_week_start.py`
echo ${current_date}


# Copy propensity scores to S3 historical storage
aws s3 cp s3://${ENV}-insights-pan-bbc-churn-predictions/data/output/loyalty-propensity-scores/ s3://${ENV}-insights-pan-bbc-churn-predictions/historical-storage/${current_date}/loyalty-propensity-scores/   --recursive

# Copy models to historical storage
aws s3 cp s3://${ENV}-insights-pan-bbc-churn-predictions/models/ s3://${ENV}-insights-pan-bbc-churn-predictions/historical-storage/${current_date}/models/   --recursive

# Copy shap values to historical storage
aws s3 cp s3://${ENV}-insights-pan-bbc-churn-predictions/loyalty/iplayer/shap-values/ s3://${ENV}-insights-pan-bbc-churn-predictions/historical-storage/${current_date}/shap-values/  --recursive

# Copy logs and charts to historical storage
aws s3 cp s3://${ENV}-insights-pan-bbc-churn-predictions/data/output/logs/ s3://${ENV}-insights-pan-bbc-churn-predictions/historical-storage/${current_date}/logs/  --recursive
aws s3 cp s3://${ENV}-insights-pan-bbc-churn-predictions/data/output/charts/ s3://${ENV}-insights-pan-bbc-churn-predictions/historical-storage/${current_date}/charts/  --recursive

