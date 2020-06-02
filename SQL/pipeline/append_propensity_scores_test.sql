/*
=====================================================
COPY DATA FROM S3
=====================================================
*/

-- Specify the default schema for this transaction
-- This allows us to change only this and test the script in another schema
SET search_path = 'central_insights_sandbox';

DROP TABLE IF EXISTS iplayer_churn_propensity_scores_STAGING_TEST;
CREATE TEMP TABLE iplayer_churn_propensity_scores_STAGING_TEST(
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

COPY iplayer_churn_propensity_scores_STAGING_TEST
FROM 's3://int-insights-pan-bbc-churn-predictions/data/output/loyalty-propensity-scores/model_scores.csv'
CREDENTIALS 'aws_access_key_id=ASIAZJVHSKVABUZS4CO7;aws_secret_access_key=E2ClCFlbb0/fu1Yys2EGOCdg6omHr/H2PMQV+bE4;token=FwoGZXIvYXdzEO7//////////wEaDG+D+FlvHS44xixN2iK8AcvVWCHCuhBCFyYxoj+f0H8xUFZCQSANNNydObjiyXgFFGEmgLJKJnpMxtgEkqdaTWnH6Cg2cVFjqBelM8GwcS0JvnxRi3ppzOJ1Edcgl/ySQdoHmONFV3zEE7Y3J7C7pvAT8m/irqhHsaFJeiOF7xgsEkMTuKwhnbfbh8juzOuAnAyLIncEWomhveRXhIv/Q453JIT7s3fCn9mynDIffeUy3jSL/pcPww7csa5mKuW3PMzcFRSmQFR/vdveKLb00/YFMi1yjvAbID3k5hK0RikFGLwSZUNZQTyCFgfTUGaCQ45XqydOC7y2qrt/RQsWuf8='
CSV
IGNOREHEADER AS 1
-- Below is for handling files with or without the extra columns only in the BBC file
EMPTYASNULL
FILLRECORD
TRUNCATECOLUMNS
;

CREATE TABLE IF NOT EXISTS iplayer_churn_propensity_scores(
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



INSERT INTO iplayer_churn_propensity_scores
SELECT *  FROM iplayer_churn_propensity_scores_STAGING_TEST;

GRANT SELECT ON iplayer_churn_propensity_scores TO GROUP central_insights;
COMMIT;


