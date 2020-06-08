/*
CHURN - EXPLORATORY ANALYSIS

Title: churn_explore

Description:
Exploratory analysis into churn rates across the BBC portfolio, with an initial focus on iPlayer

Output Table(s):
TBA

Depends on:
TBA

*/

-- Set up local vars
BEGIN;
SET LOCAL search_path = 'central_insights_sandbox';
DROP TABLE IF EXISTS central_insights_sandbox.tp_churn_explore_vars;
CREATE TABLE central_insights_sandbox.tp_churn_explore_vars AS
  SELECT
         '2020-02-23'::DATE AS maxDate, --(last Sunday)
         dateadd('days',-((n_cohorts+17)*7),maxDate) as minDate,
         n_cohorts
  FROM (
       SELECT 6 as n_cohorts
         ) n_cohorts
;
COMMIT;
GRANT ALL ON central_insights_sandbox.tp_churn_explore_vars TO GROUP central_insights;

-- Collect active IDs by week
-- Potentially switch to redshift enriched, performance concerns
BEGIN;
DROP TABLE IF EXISTS central_insights_sandbox.tp_churn_weekly_active_ids;
CREATE TABLE central_insights_sandbox.tp_churn_weekly_active_ids AS
  SELECT
         audience_id as bbc_hid3,
--          destination,
         central_insights.udf_destination_prod(destination, app_name, page_name) as destination,
         --cast(ceil(random() * ((select n_cohorts from central_insights_sandbox.tp_churn_explore_vars)))-1 as int) as cohort,
         date_trunc('week', date_of_event) as week,
         count(*) as events,
         sum(playback_time_total)::float as streaming_time,
         count(distinct date_of_event)::float as distinct_days,
         sum(page_views_total)::float as page_views,
         sum(case when pips_genre_level_1_names like 'Comedy%' then playback_time_total else 0 end)::float as genre_st_comedy,
         sum(case when pips_genre_level_1_names like 'Drama%' then playback_time_total else 0 end)::float as genre_st_drama,
         sum(case when pips_genre_level_1_names like 'Entertainment%' then playback_time_total else 0 end)::float as genre_st_ents,
         sum(case when pips_genre_level_1_names like 'Children%' then playback_time_total else 0 end)::float as genre_st_childrens,
         sum(case when pips_genre_level_1_names like 'Factual%' then playback_time_total else 0 end)::float as genre_st_factual,
         sum(case when pips_genre_level_1_names like 'Learning%' then playback_time_total else 0 end)::float as genre_st_learning,
         sum(case when pips_genre_level_1_names like 'Music%' then playback_time_total else 0 end)::float as genre_st_music,
         sum(case when pips_genre_level_1_names like 'News%' then playback_time_total else 0 end)::float as genre_st_news,
         sum(case when pips_genre_level_1_names like 'Religion & Ethics%' then playback_time_total else 0 end)::float as genre_st_religion,
         sum(case when pips_genre_level_1_names like 'Sport%' then playback_time_total else 0 end)::float as genre_st_sport,
         sum(case when pips_genre_level_1_names like 'Weather%' then playback_time_total else 0 end)::float as genre_st_weather,
         streaming_time -
         (genre_st_comedy + genre_st_drama + genre_st_ents + genre_st_childrens + genre_st_factual
          + genre_st_learning + genre_st_music + genre_st_news + genre_st_religion + genre_st_sport + genre_st_weather) as genre_st_other
  FROM audience.audience_activity_daily_summary_enriched
  WHERE destination in ('PS_IPLAYER', 'PS_SOUNDS')
    AND date_of_event <= (select maxDate from central_insights_sandbox.tp_churn_explore_vars)
    AND date_of_event >= (select minDate from central_insights_sandbox.tp_churn_explore_vars)
    --AND cohort >= 0
  GROUP BY 1, 2, 3
;
COMMIT;
GRANT ALL ON central_insights_sandbox.tp_churn_weekly_active_ids TO GROUP central_insights;

BEGIN;
DROP TABLE IF EXISTS central_insights_sandbox.tp_churn_cohorts;
CREATE TABLE central_insights_sandbox.tp_churn_cohorts
  DISTKEY ( bbc_hid3 )  AS
  SELECT bbc_hid3,
         cohort,
         fresh,
--          dateadd('days', 7*cohort, minDate) as minDate,
--        dateadd('days', (7*cohort)-21, maxDate) as maxDate,
--        dateadd('days',(7*cohort)-35, maxDate) as maxFeatureDate,
         dateadd('days', -(7*(cohort+15))+1, maxDate) as minDate,
         dateadd('days', -(7*cohort), maxDate) as maxDate,
        dateadd('days', -((7*cohort)+14), maxDate) as maxFeatureDate,
       date_trunc('week', maxFeatureDate) as lastWeekStart
  FROM (
        SELECT bbc_hid3,
               cohort,
               0 as fresh
        FROM (
               SELECT bbc_hid3,
                      cast(ceil(random() * ((SELECT n_cohorts
                                             FROM central_insights_sandbox.tp_churn_explore_vars))) -
                           1 AS INT) AS cohort
               FROM (SELECT DISTINCT bbc_hid3
                     FROM central_insights_sandbox.tp_churn_weekly_active_ids) ids
               --          WHERE cohort >= 0
             ) a
        UNION
        SELECT DISTINCT bbc_hid3,
               -2 as cohort,
               1 as fresh
        FROM central_insights_sandbox.tp_churn_weekly_active_ids
       ) assign_cohort
  CROSS JOIN central_insights_sandbox.tp_churn_explore_vars vars
;
COMMIT;
GRANT ALL ON central_insights_sandbox.tp_churn_cohorts TO GROUP central_insights;

-- adding cohorts to weekly activity table
DROP TABLE IF EXISTS central_insights_sandbox.tp_churn_weekly_active_ids_deleteme;
ALTER TABLE central_insights_sandbox.tp_churn_weekly_active_ids RENAME TO tp_churn_weekly_active_ids_deleteme;
-- DROP TABLE IF EXISTS central_insights_sandbox.tp_churn_weekly_active_ids;
CREATE TABLE central_insights_sandbox.tp_churn_weekly_active_ids AS
  SELECT ids.*,
         coh.cohort,
         coh.fresh
  FROM central_insights_sandbox.tp_churn_weekly_active_ids_deleteme ids
    INNER JOIN central_insights_sandbox.tp_churn_cohorts coh
      ON ids.bbc_hid3 = coh.bbc_hid3
--   WHERE coh.cohort >= -2
;
GRANT ALL ON central_insights_sandbox.tp_churn_weekly_active_ids TO GROUP central_insights;


-- lookup of the feature date ranges for each cohort
DROP TABLE IF EXISTS central_insights_sandbox.tp_churn_cohort_dates;
CREATE TABLE central_insights_sandbox.tp_churn_cohort_dates as
SELECT DISTINCT
       cohort,
       minDate,
       maxDate,
       maxfeaturedate,
       date_trunc('week', maxDate) as maxWeek,
       lastweekstart
FROM
central_insights_sandbox.tp_churn_cohorts;
COMMIT;
GRANT ALL ON central_insights_sandbox.tp_churn_cohort_dates TO GROUP central_insights;


-- Add most recent week to variable set
BEGIN;
CREATE TABLE central_insights_sandbox.tp_churn_explore_vars_1 AS
  SELECT *,
         (select max(week) from central_insights_sandbox.tp_churn_weekly_active_ids WHERE cohort >= 0) as maxWeek
  FROM central_insights_sandbox.tp_churn_explore_vars
;
DROP TABLE IF EXISTS central_insights_sandbox.tp_churn_explore_vars;
ALTER TABLE central_insights_sandbox.tp_churn_explore_vars_1 RENAME TO tp_churn_explore_vars;
COMMIT;
GRANT ALL ON central_insights_sandbox.tp_churn_explore_vars TO GROUP central_insights;

-- Pivot to create 15 weeks of events as columns
BEGIN;
DROP TABLE IF EXISTS central_insights_sandbox.tp_churn_weekly_active_pivot;
CREATE TABLE central_insights_sandbox.tp_churn_weekly_active_pivot
  distkey (bbc_hid3)
  sortkey (destination)
  AS
  SELECT
    bbc_hid3,
    destination,
    cohort,
    fresh,
    -- Streaming time
--     nvl(sum(streaming_time),0) as streaming_time_13w,
    nvl(sum(case when weeks_out - cohort = 0 then streaming_time else null end),0) as stw_0,
    nvl(sum(case when weeks_out - cohort = 1 then streaming_time else null end),0) as stw_1,
    nvl(sum(case when weeks_out - cohort = 2 then streaming_time else null end),0) as stw_2,
    nvl(sum(case when weeks_out - cohort = 3 then streaming_time else null end),0) as stw_3,
    nvl(sum(case when weeks_out - cohort = 4 then streaming_time else null end),0) as stw_4,
    nvl(sum(case when weeks_out - cohort = 5 then streaming_time else null end),0) as stw_5,
    nvl(sum(case when weeks_out - cohort = 6 then streaming_time else null end),0) as stw_6,
    nvl(sum(case when weeks_out - cohort = 7 then streaming_time else null end),0) as stw_7,
    nvl(sum(case when weeks_out - cohort = 8 then streaming_time else null end),0) as stw_8,
    nvl(sum(case when weeks_out - cohort = 9 then streaming_time else null end),0) as stw_9,
    nvl(sum(case when weeks_out - cohort = 10 then streaming_time else null end),0) as stw_10,
    nvl(sum(case when weeks_out - cohort = 11 then streaming_time else null end),0) as stw_11,
    nvl(sum(case when weeks_out - cohort = 12 then streaming_time else null end),0) as stw_12,
    nvl(sum(case when weeks_out - cohort = 13 then streaming_time else null end),0) as stw_13,
    nvl(sum(case when weeks_out - cohort = 14 then streaming_time else null end),0) as stw_14,
    stw_2 + stw_3 + stw_4 + stw_5 + stw_6 + stw_7 + stw_8 + stw_9 + stw_10 + stw_11 +
     stw_12 + stw_13 + stw_14 as streaming_time_13w,
     -- Event Frequency
    nvl(sum(case when weeks_out - cohort = 0 then events else null end),0) as ew_0,
    nvl(sum(case when weeks_out - cohort = 1 then events else null end),0) as ew_1,
    nvl(sum(case when weeks_out - cohort = 2 then events else null end),0) as ew_2,
    nvl(sum(case when weeks_out - cohort = 3 then events else null end),0) as ew_3,
    nvl(sum(case when weeks_out - cohort = 4 then events else null end),0) as ew_4,
    nvl(sum(case when weeks_out - cohort = 5 then events else null end),0) as ew_5,
    nvl(sum(case when weeks_out - cohort = 6 then events else null end),0) as ew_6,
    nvl(sum(case when weeks_out - cohort = 7 then events else null end),0) as ew_7,
    nvl(sum(case when weeks_out - cohort = 8 then events else null end),0) as ew_8,
    nvl(sum(case when weeks_out - cohort = 9 then events else null end),0) as ew_9,
    nvl(sum(case when weeks_out - cohort = 10 then events else null end),0) as ew_10,
    nvl(sum(case when weeks_out - cohort = 11 then events else null end),0) as ew_11,
    nvl(sum(case when weeks_out - cohort = 12 then events else null end),0) as ew_12,
    nvl(sum(case when weeks_out - cohort = 13 then events else null end),0) as ew_13,
    nvl(sum(case when weeks_out - cohort = 14 then events else null end),0) as ew_14
  FROM (
         SELECT *,
                datediff(week, week, (select maxweek from central_insights_sandbox.tp_churn_explore_vars)) as weeks_out
         FROM central_insights_sandbox.tp_churn_weekly_active_ids
         where destination != 'radio' --removing iplayer radio from sounds
       ) A
  GROUP BY 1, 2, 3, 4
;
COMMIT;
GRANT ALL ON central_insights_sandbox.tp_churn_weekly_active_pivot TO GROUP central_insights;

-- Create a 1/0 flag table for activity
BEGIN;
DROP TABLE IF EXISTS central_insights_sandbox.tp_churn_weekly_active;
CREATE TABLE central_insights_sandbox.tp_churn_weekly_active AS
  SELECT
    *,
    case when ew_0 > 0 then 1 else 0 end as aw_0,
    case when ew_1 > 0 then 1 else 0 end as aw_1,
    case when ew_2 > 0 then 1 else 0 end as aw_2,
    case when ew_3 > 0 then 1 else 0 end as aw_3,
    case when ew_4 > 0 then 1 else 0 end as aw_4,
    case when ew_5 > 0 then 1 else 0 end as aw_5,
    case when ew_6 > 0 then 1 else 0 end as aw_6,
    case when ew_7 > 0 then 1 else 0 end as aw_7,
    case when ew_8 > 0 then 1 else 0 end as aw_8,
    case when ew_9 > 0 then 1 else 0 end as aw_9,
    case when ew_10 > 0 then 1 else 0 end as aw_10,
    case when ew_11 > 0 then 1 else 0 end as aw_11,
    case when ew_12 > 0 then 1 else 0 end as aw_12,
    case when ew_13 > 0 then 1 else 0 end as aw_13,
    case when ew_14 > 0 then 1 else 0 end as aw_14
  FROM central_insights_sandbox.tp_churn_weekly_active_pivot
;
COMMIT;
GRANT ALL ON central_insights_sandbox.tp_churn_weekly_active TO GROUP central_insights;

-- Calculate linear regression on 13 weeks, excluding current (scoring) week and next (prediction) week
BEGIN;
DROP TABLE IF EXISTS central_insights_sandbox.tp_churn_13week_lin_reg;
CREATE TABLE central_insights_sandbox.tp_churn_13week_lin_reg
  distkey (bbc_hid3)
  sortkey (destination)
  AS
  SELECT
         bbc_hid3,
         destination,
         cohort,
         fresh,
         case when ew_2 > 0 then 1 else 0 end as aw_2,
         /* Linear regression sums */
         -- Sum of X values
         91::float as sx,
         -- Sum of Y values
         (ew_2 + ew_3 + ew_4 + ew_5 + ew_6 + ew_7 + ew_8 + ew_9 + ew_10 + ew_11 + ew_12 + ew_13 + ew_14)::float as sy,
         -- Sum of X squares
         819::float as sxx,
         -- Sum of products
         ((ew_2*1) + (ew_3*2) + (ew_4*3) + (ew_5*4) + (ew_6*5) + (ew_7*6) + (ew_8*7) +
           (ew_9*8) + (ew_10*9) + (ew_11*10) + (ew_12*11) + (ew_13*12) + (ew_14*13))::float as sxy,
         -- Sum of Y squares
         ((ew_2*ew_2) + (ew_3*ew_3) + (ew_4*ew_4) + (ew_5*ew_5) + (ew_6*ew_6) + (ew_7*ew_7) + (ew_8*ew_8) +
           (ew_9*ew_9) + (ew_10*ew_10) + (ew_11*ew_11) + (ew_12*ew_12) + (ew_13*ew_13) + (ew_14*ew_14))::float as syy,

         /* Simple linear regression estimated coefficients */
         -- beta-hat
         ((13*sxy) - (sx*sy)) / ((13*sxx) - (sx*sx)) as beta_hat,
         -- alpha-hat
         (sy/13) - ((beta_hat * sx)/13) as alpha_hat,

         /* Standard errors */
         -- standard error of residuals
         ((13*syy) - (sy*sy) - ((beta_hat*beta_hat)*((13*sxx) - (sx*sx)))) / (13*11) as se2,
         -- standard error of slope
         (13*se2) / ((13*sxx) - sx*sx) as sb2,
         -- standard error of intercept
         sb2*sxx/13 as sa2,

         /* Confidence intervals */
         -- 11 degrees of freedom - t*11 is 2.201 for 0.975 confidence interval
         alpha_hat - (2.201 * sqrt(sa2)) as alpha_hat_lower,
         alpha_hat + (2.201 * sqrt(sa2)) as alpha_hat_upper,

         beta_hat - (2.201 * sqrt(sb2)) as beta_hat_lower,
         beta_hat + (2.201 * sqrt(sb2)) as beta_hat_upper,

         /* x intercept */
         case when beta_hat < 0 then -(alpha_hat/beta_hat) else null end as x_intercept

  FROM central_insights_sandbox.tp_churn_weekly_active_pivot
;
COMMIT;
GRANT ALL ON central_insights_sandbox.tp_churn_13week_lin_reg TO GROUP central_insights;


--"Pivot" over destination
BEGIN;
DROP TABLE IF EXISTS central_insights_sandbox.tp_churn_13week_lin_reg_pivot;
CREATE TABLE central_insights_sandbox.tp_churn_13week_lin_reg_pivot AS
SELECT
  bbc_hid3,
  fresh,
  SUM(case when destination = 'iplayer' then beta_hat else NULL end) as iplayer_lin_reg_coeff,
  SUM(case when destination = 'iplayer' then alpha_hat else NULL end) as iplayer_13w_yintercept,
  SUM(case when destination = 'iplayer' then x_intercept else NULL end) as iplayer_13w_xintercept,
  case when iplayer_13w_xintercept is not null then 1 else 0 end as iplayer_lin_reg_churn_flag,
  SUM(case when destination = 'sounds' then beta_hat else NULL end) as sounds_lin_reg_coeff,
  SUM(case when destination = 'sounds' then alpha_hat else NULL end) as sounds_13w_yintercept,
  SUM(case when destination = 'sounds' then x_intercept else NULL end) as sounds_13w_xintercept,
  case when sounds_13w_xintercept is not null then 1 else 0 end as sounds_lin_reg_churn_flag
  from central_insights_sandbox.tp_churn_13week_lin_reg
group by 1, 2
;
GRANT ALL ON central_insights_sandbox.tp_churn_13week_lin_reg_pivot TO GROUP central_insights;

-- Create a target set to train on
BEGIN;
DROP TABLE IF EXISTS central_insights_sandbox.tp_churn_target;
CREATE TABLE central_insights_sandbox.tp_churn_target
  distkey (bbc_hid3)
  sortkey (destination)
  AS
  SELECT
         bbc_hid3,
         destination,
         activity.cohort,
         date_trunc('week', coh.maxDate) as target_week_start_date,
         coh.maxDate as target_week_end_date,
         fresh,
         case when fresh = 1 then NULL else 1-aw_1 end as target_churn_this_week,
         case when fresh = 1 then NULL else 1-aw_0 end as target_churn_next_week,
         case when aw_2 = 1 and streaming_time_13w > 180 then 1 else 0 end as active_last_week,
         case when cast(random() * 5 as int) = 4 then 0 else 1 end as train
  FROM central_insights_sandbox.tp_churn_weekly_active activity
  LEFT JOIN
    central_insights_sandbox.tp_churn_cohort_dates coh
    on activity.cohort = coh.cohort
;
COMMIT;
GRANT ALL ON central_insights_sandbox.tp_churn_target TO GROUP central_insights;

DROP TABLE IF EXISTS central_insights_sandbox.tp_churn_weekly_active_ids_deleteme;