import psycopg2
import boto3
import os
import json
import pickle
import pdb
import pandas as pd
import numpy as np
from sklearn.model_selection import StratifiedKFold
from sklearn import preprocessing

def pickler(obj, file):
    """
    Pickle a python object to a file
    """
    with open(file, 'wb') as output_file:
        pickle.dump(obj, output_file)

def unpickle(file):
    """
    Unpack a pickled object
    """
    with open(file, 'rb') as input_file:
        out = pickle.load(input_file)
    
    return out
    

# ETL UTILS
def aws_fetch_creds():
    """
    Fetch AWS credentials

    Requires script ~/aws-fetch.sh to be installed:

    >> aws-fetch.sh
    # activate venv
    source ~/rkk2/bin/activate

    # generate credentials
    $(vostok aws-credentials --account=657378245742)
    
    # construct JSON string to export environmental vars
    JSON_FMT='{"AWS_ACCESS_KEY_ID":"%s","AWS_SECRET_ACCESS_KEY":"%s", "AWS_DEFAULT_REGION":"eu-west-1","AWS_SECURITY_TOKEN":"%s"}'
    printf "$JSON_FMT" "$AWS_ACCESS_KEY_ID" "$AWS_SECRET_ACCESS_KEY" "$AWS_SECURITY_TOKEN"
    << script
    """
    stream = os.popen("~/aws-fetch-curl.sh")
    aws_json = stream.read()
    aws_creds =json.loads(aws_json)

    print('AWS access key ID: ', aws_creds['AWS_ACCESS_KEY_ID'])
    print('AWS secret access key: ', aws_creds['AWS_SECRET_ACCESS_KEY'])
    print('AWS default region: ', aws_creds['AWS_DEFAULT_REGION'])
    print('AWS session token: ', aws_creds['AWS_SECURITY_TOKEN'])

    os.environ['AWS_ACCESS_KEY_ID'] = aws_creds['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY'] = aws_creds['AWS_SECRET_ACCESS_KEY']
    os.environ['AWS_DEFAULT_REGION'] = aws_creds['AWS_DEFAULT_REGION']
    os.environ['AWS_SESSION_TOKEN'] = aws_creds['AWS_SECURITY_TOKEN']

    return aws_creds

def aws_fetch_secret(secret_name):
    """
    Use AWS credentials to retrieve Redshift credentials stored as a Secret in Amazon Secrets

    Pre-requisite: update AWS credentials in environment with aws_fetch_creds
    """
    # Create a Secrets Manager Client
    client = boto3.client(
        service_name = 'secretsmanager',
        region_name = 'eu-west-1'
    )

    # Get Secrets from AWS
    secret = client.get_secret_value(SecretId=secret_name)
    secret_dict = json.loads(secret['SecretString'])

    return secret_dict

def copy_to_s3(df, s3_obj, aws_creds, tmp='tmp.csv'):
    """
    Copy a dataframe to an S3 bucket
    """
    df.to_csv(tmp, index=False, header=False)
    shell = ("export AWS_ACCESS_KEY_ID={id}\n"+
        "export AWS_SECRET_ACCESS_KEY={key}\n"+
        "export AWS_SECURITY_TOKEN={token}\n"+
        "export AWS_DEFAULT_REGION=eu-west-1\n"+
        "\n"+
        "aws s3 cp {tmp} {s3_obj}"
    ).format(
        id=aws_creds['AWS_ACCESS_KEY_ID'],
        key=aws_creds['AWS_SECRET_ACCESS_KEY'],
        token=aws_creds['AWS_SECURITY_TOKEN'],
        tmp=tmp,
        s3_obj=s3_obj
    )
    os.system(shell)

    print('Data copied to S3')


def s3_to_Redshift(s3_obj, rs_table, aws_creds, secret_dict, truncate=True, csv=True, gzip=False):
    """
    Copy data from S3 bucket into Redshift
    """
    query_truncate = "TRUNCATE {rs_table}".format(rs_table=rs_table)
    query_rs_copy = ("COPY {rs_table} "+
        "FROM '{s3_obj}' "+
        "CREDENTIALS 'aws_access_key_id={id}"+
        ";aws_secret_access_key={key}"+
        ";token={token}'"
    ).format(
        rs_table=rs_table,
        s3_obj=s3_obj,
        id=aws_creds['AWS_ACCESS_KEY_ID'],
        key=aws_creds['AWS_SECRET_ACCESS_KEY'],
        token=aws_creds['AWS_SECURITY_TOKEN']
        )
    if csv: 
        query_rs_copy=query_rs_copy+' FORMAT AS CSV'
    if gzip:
        query_rs_copy=query_rs_copy+' GZIP'

    # Using psycopg2 to connect to RS as had issues with commiting with sqlengine
    conn = psycopg2.connect(dbname = 'redshiftdb', host='localhost', port=5439,
                            user=secret_dict['redshift_username'], password=secret_dict['redshift_password'])
    cur = conn.cursor()
    if truncate: 
        cur.execute(query_truncate)
    cur.execute(query_rs_copy)
    conn.commit()
    conn.close()

    print('Data copied to Redshift')

def rs_upload(df, s3_obj, rs_table, aws_creds, secret_dict,
              tmp='tmp.csv', truncate=True, csv=True, gzip=False):
    """
    Copy data from python to a Redshift table
    """
    copy_to_s3(df=df, s3_obj=s3_obj, aws_creds=aws_creds, tmp=tmp)
    s3_to_Redshift(s3_obj=s3_obj, rs_table=rs_table, aws_creds=aws_creds, secret_dict=secret_dict,
         truncate=truncate, csv=csv, gzip=gzip)

def push_to_s3(local_object, s3_loc, s3_bucket='central-insights'):
    """
    Use boto3 to push to an s3 bucket
    """
    # Create a Secrets Manager Client
    client = boto3.client(
        service_name = 'secretsmanager',
        region_name = 'eu-west-1'
    )
    client.put_object(Body=local_object, Bucket=s3_bucket, Key=s3_loc)

# DATA UTILS
def scale_my_data(X):
    scaler = preprocessing.MaxAbsScaler()
    X_scaled = scaler.fit_transform(X.values)
    X_scaled = pd.DataFrame(X_scaled, index=X.index, columns=X.columns)
    return X_scaled

class tidy_scaler(object):
    """
    An adaptation of sklearn MaxAbsScaler to work with pandas dataframes
    """
    def __init__(self):
        self.fitted=False
        self.maxabsscaler = preprocessing.MaxAbsScaler()

    def fit_transform(self, X):
        """
        Fitting a scaler that can handle pandas dataframes
        """
        maxabsscaler = self.maxabsscaler

        # maxabsscaler.fit(X.values)
        X_scaled = maxabsscaler.fit_transform(X.values)
        fitted_data = pd.DataFrame(X_scaled, index=X.index, columns = X.columns)

        self.maxabsscaler = maxabsscaler
        self.fitted = True

        return fitted_data

    def transform(self, X):
        """
        Transforming a pandas dataframe with a pre-fitted tidy scaler
        """
        X_scaled = self.maxabsscaler.transform(X.values)
        new_data = pd.DataFrame(X_scaled, index = X.index, columns = X.columns)

        return new_data

def my_StratifiedKFold(X, y, n_folds=10):
    """
    Stratifed K-Fold classification with re-usability. (The base form is evaluated and thrown away
    on first call)
    """
    np.random.seed(10)
    n_folds = 10

    skf_model = StratifiedKFold(n_splits = n_folds, random_state=0)
    splits = skf_model.split(X, y)

    # Saving down the enumerator and indices from the SKF split for consistent reuse
    my_skf = [[i, (train_idx, test_idx)] for i, (train_idx, test_idx) in enumerate(splits)]

    print('Creating train / test splits over', n_folds, 'folds')
    for i, (train_idx, test_idx) in my_skf:
        print(i,'= Train:', len(train_idx), 'Test:', len(test_idx))

    return my_skf