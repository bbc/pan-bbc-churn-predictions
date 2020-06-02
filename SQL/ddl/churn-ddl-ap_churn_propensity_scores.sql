
DROP TABLE IF EXISTS central_insights_sandbox.ap_churn_iplayer_propensity_scores;
CREATE TABLE central_insights_sandbox.ap_churn_iplayer_propensity_scores
(
  bbc_hid3 varchar(250) distkey,
  target_week_start_date date,
  learner_type varchar(250),
  learner_name varchar(250),
  predicted_probability double precision,
  optimal_threshold double precision,
  optimal_classification int,
  score_datetime timestamp
)
;


DROP TABLE IF EXISTS central_insights_sandbox.ap_churn_sounds_propensity_scores;
CREATE TABLE central_insights_sandbox.ap_churn_sounds_propensity_scores
(
  bbc_hid3 varchar(250) distkey,
  target_week_start_date date,
  learner_type varchar(250),
  learner_name varchar(250),
  predicted_probability double precision,
  optimal_threshold double precision,
  optimal_classification int,
  score_datetime timestamp
)
;

drop table central_insights_sandbox.ap_churn_iplayer_representative_user_scores ;
create table central_insights_sandbox.ap_churn_iplayer_representative_user_scores (
  representative_user varchar(20),
  learner_type varchar(20),
  learner_name varchar(20),
  predicted_probability float,
  score_datetime timestamp
)
distkey (representative_user)
;