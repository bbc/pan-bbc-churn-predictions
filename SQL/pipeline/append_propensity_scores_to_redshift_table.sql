/*
=====================================================
COPY DATA FROM S3
=====================================================
*/


BEGIN; --Starts a transaction in redshift

-- Specify the default schema for this transaction
-- This allows us to change only this and test the script in another schema
SET search_path = '<params.SCHEMANAME>';

DROP TABLE IF EXISTS <params.TABLENAME>_STAGING_TEST;
CREATE TEMP TABLE <params.TABLENAME>_STAGING_TEST(
  bbc_hid3 varchar(250) distkey,
  target_week_start_date date,
  learner_type varchar(250),
  learner_name varchar(250),
  predicted_probability double precision,
  optimal_threshold double precision,
  optimal_classification int,
  score_datetime timestamp
  -- below columns present in BBC data file only, so we'll want to ignore them
);

COPY <params.TABLENAME>_STAGING_TEST
FROM '<params.S3_PATH>'
CREDENTIALS 'aws_access_key_id=<params.AWS_ACCESS_KEY_ID>;aws_secret_access_key=<params.AWS_SECRET_ACCESS_KEY>;token=<params.TOKEN>'
CSV
IGNOREHEADER AS 1
-- Below is for handling files with or without the extra columns only in the BBC file
EMPTYASNULL
FILLRECORD
TRUNCATECOLUMNS
;

CREATE TABLE IF NOT EXISTS <params.TABLENAME> (
 bbc_hid3 varchar(250) distkey,
  target_week_start_date date,
  learner_type varchar(250),
  learner_name varchar(250),
  predicted_probability double precision,
  optimal_threshold double precision,
  optimal_classification int,
  score_datetime timestamp
  -- below columns present in BBC data file only, so we'll want to ignore them
);



INSERT INTO <params.TABLENAME>
SELECT *  FROM <params.TABLENAME>_STAGING_TEST
;

GRANT SELECT ON <params.TABLENAME> TO GROUP central_insights;
COMMIT;

-- drop table central_insights_sandbox.tp_churn_iplayer_representative_user_scores ;
-- create table central_insights_sandbox.tp_churn_iplayer_representative_user_scores (
--   representative_user varchar(20),
--   learner_type varchar(20),
--   learner_name varchar(20),
--   predicted_probability float,
--   score_datetime timestamp
-- )


-- DROP TABLE IF EXISTS central_insights_sandbox.tp_churn_sounds_propensity_scores;
-- CREATE TABLE central_insights_sandbox.tp_churn_sounds_propensity_scores
-- (
--   bbc_hid3 varchar(250) distkey,
--   target_week_start_date date,
--   learner_type varchar(250),
--   learner_name varchar(250),
--   predicted_probability double precision,
--   optimal_threshold double precision,
--   optimal_classification int,
--   score_datetime timestamp
-- )
-- ;


