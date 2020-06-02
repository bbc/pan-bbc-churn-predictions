UNLOAD($$
SELECT * FROM central_insights_sandbox.tp_churn_iplayer_training_sample
    $$)
TO '<params.S3_OUTPUT_PATH>' -- ''
CREDENTIALS 'aws_access_key_id=ASIAZJVHSKVAKUGBZU6S;aws_secret_access_key=FIhfErkNx3ytTwvveK9SMqRbGfcFhJ3xQ7AHLYKf;token=FwoGZXIvYXdzEJH//////////wEaDMwTciAV3NdYxGzF+yK8AbfPV/FzC9EfhUNiYKaiA/hv1poUk4ca0n+0Ldbu2wcc/xQlV41u0TM8ikp9M/s1HBluxNBwl1RQtx7tYzObK0DvNmta1UoeZcMXMVqtR+BP9LoSn1CTY0AwwX8mkORLO6eYBlec+e1z03nZHjsQ7T/XKDZcj1+O6ccxe4u8Jp4IT0CwnwS+9gProNC8u5fFcULLMF9EJE1dSRk6VWTCAkP3JGx2QeRYRByn8S3OmsvKu8tpaw+7VCxktdiwKMGmv/YFMi0q1xdSITIgD7eyd1v+0YKvyY1sl/40uK4DXOQDb3dyZya6Dvp+mBEWtKJx2YE='
ALLOWOVERWRITE
PARALLEL OFF
BZIP2;

/*
 DROP TABLE IF EXISTS central_insights_sandbox.tp_churn_iplayer_training_sample;
CREATE TABLE central_insights_sandbox.tp_churn_iplayer_training_sample AS
SELECT * FROM central_insights_sandbox.ap_churn_iplayer_training_sample limit 10;

TRUNCATE TABLE central_insights_sandbox.tp_churn_iplayer_training_sample;

INSERT INTO central_insights_sandbox.tp_churn_iplayer_training_sample
( SELECT * FROM central_insights_sandbox.ap_churn_iplayer_training_sample limit 10000)

 */