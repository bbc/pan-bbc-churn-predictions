#!/bin/bash
set -e

IMAGE="r-news-segmentation"
ENV="LIVE"
ENV_LOWER=$(echo $ENV| tr '[:upper:]' '[:lower:]')

docker run -it -v ~/.aws:/root/.aws -v $PWD/../:/mnt \
	--env ENV="$ENV" \
	--env BATCH_FILE_S3_URL="s3://$ENV_LOWER-airflow-temp-for-redshift/insights_news_segmentation/code.zip" \
	--env U35_OUTPUT_S3_PATH="s3://$ENV_LOWER-insights-news-segmentation/data/output/news_segments/u35/news_segmentation_u35.csv" \
	--env CODE_S3_PATH="s3://$ENV_LOWER-airflow-temp-for-redshift/insights_news_segmentation/code.zip" \
	--env FEATURES_S3_PATH="s3://$ENV_LOWER-insights-news-segmentation/data/output/news_segmentation_features" \
	--env OVERALL_OUTPUT_S3_PATH="s3://$ENV_LOWER-insights-news-segmentation/data/output/news_segments/overall/news_segmentation_overall.csv" \
	--env BATCH_FILE_TYPE="zip" \
	"${IMAGE}" run_model_predictions.sh

