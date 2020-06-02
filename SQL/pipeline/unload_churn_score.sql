UNLOAD($$
select * from <params.SCHEMA>.tp_churn_iplayer_score_sample
    $$)
TO '<params.S3_OUTPUT_PATH>' -- ''
CREDENTIALS 'aws_access_key_id=<params.AWS_ACCESS_KEY_ID>;aws_secret_access_key=<params.AWS_SECRET_ACCESS_KEY>;token=<params.TOKEN>'
ALLOWOVERWRITE
PARALLEL OFF
BZIP2;
