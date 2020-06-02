import boto3
import pandas as pd

from src import utils

# Fetch credentials from AWS
utils.aws_fetch_creds()
df = pd.read_csv('s3://central-insights/philpa03/loyalty/iplayer/shap-values/current/Behaviour.csv')

print(df.head(10))