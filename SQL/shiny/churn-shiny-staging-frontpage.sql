DROP TABLE IF EXISTS central_insights_sandbox.ap_churn_shiny_historic_churn_sampled;
CREATE TABLE central_insights_sandbox.ap_churn_shiny_historic_churn_sampled AS
  SELECT
         target_week_start_date::date as week,
         target_churn_next_week,
         destination,
         count(*) as users
  FROM central_insights_sandbox.ap_churn_target
  WHERE fresh = 0
    and active_last_week = 1
  GROUP BY 1,2,3
;

DROP TABLE IF EXISTS central_insights_sandbox.ap_churn_shiny_iplayer_predicted_churn_sampled;
CREATE TABLE central_insights_sandbox.ap_churn_shiny_iplayer_predicted_churn_sampled AS
  SELECT
         target_week_start_date as week,
         'iplayer' as destination,
         learner_type,
         learner_name,
         sum(predicted_probability) as predicted_churn_raw,
         sum(optimal_classification) as predicted_churn_thresholded,
         count(*) as users
  FROM central_insights_sandbox.ap_churn_iplayer_propensity_scores
  GROUP BY 1,2,3,4
;

DROP TABLE IF EXISTS central_insights_sandbox.ap_churn_shiny_sounds_predicted_churn_sampled;
CREATE TABLE central_insights_sandbox.ap_churn_shiny_sounds_predicted_churn_sampled AS
  SELECT
         target_week_start_date as week,
         'sounds' as destination,
         learner_type,
         learner_name,
         sum(predicted_probability) as predicted_churn_raw,
         sum(optimal_classification) as predicted_churn_thresholded,
         count(*) as users
  FROM central_insights_sandbox.ap_churn_sounds_propensity_scores
  GROUP BY 1,2,3,4
;
