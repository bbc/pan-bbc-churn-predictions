DROP TABLE IF EXISTS central_insights_sandbox.ap_churn_newsletter_follows_reduced;
CREATE TABLE central_insights_sandbox.ap_churn_newsletter_follows_reduced
  distkey(bbc_hid3)
  AS
SELECT hashedidentity as bbc_hid3,
       cal_yyyymmdd::date as action_date,
       servertime,
       action
FROM prez.uasview
  WHERE lower(activitytype) = 'follows'
    AND resourcetype = 'newsletter'
    AND resourcedomain = 'profile'
    AND resourceid = 'pan_bbc'
;


DROP TABLE IF EXISTS central_insights_sandbox.ap_churn_mkt_optin;
CREATE TABLE central_insights_sandbox.ap_churn_mkt_optin
  AS
SELECT DISTINCT
    bbc_hid3,
    fresh,
    last_value(servertime) over
        (
        partition by bbc_hid3
        order by servertime
        rows between unbounded preceding and unbounded following
        ) as servertime,
    last_value(action) over
        (
        partition by bbc_hid3
        order by servertime
        rows between unbounded preceding and unbounded following
        ) as last_action
from
  (
  SELECT DISTINCT coh.bbc_hid3,
                  coh.fresh,
                  servertime, action
  FROM
  central_insights_sandbox.ap_churn_cohorts coh
  LEFT JOIN
  central_insights_sandbox.ap_churn_newsletter_follows_reduced uas
  ON uas.bbc_hid3 = coh.bbc_hid3
  WHERE action_date <= coh.maxFeatureDate
    ) follows
;




DROP TABLE IF EXISTS central_insights_sandbox.ap_churn_mkt_activity;
CREATE TABLE central_insights_sandbox.ap_churn_mkt_activity
  distkey (bbc_hid3)
  AS
SELECT nvl(opens.bbc_hid3, clicks.bbc_hid3) as bbc_hid3,
       nvl(opens.fresh, clicks.fresh) as fresh,
       nvl(opens.mkt_email_opens_lw,0) as mkt_email_opens_lw,
       nvl(opens.mkt_email_opens_13w,0) as mkt_email_opens_13w,
       nvl(clicks.mkt_email_clicks_lw,0) as mkt_email_clicks_lw,
       nvl(clicks.mkt_email_clicks_13w,0) as mkt_email_clicks_13w
FROM (
       SELECT coh.bbc_hid3,
              coh.fresh,
              count(distinct sendid)                                                   as mkt_email_opens_13w,
              count(distinct case when eventdatetime >= lastweekstart then sendid end) as mkt_email_opens_lw
       FROM research_measurement_data.rmd_salesforce_opens_src sf
              INNER JOIN central_insights_sandbox.ap_churn_cohorts coh
                         ON subscriberkey = coh.bbc_hid3
                           AND sf.eventdatetime >= coh.mindate
                           AND sf.eventdatetime <= coh.maxfeaturedate
       GROUP BY 1, 2
     ) opens
FULL OUTER JOIN
     (
       SELECT coh.bbc_hid3,
              coh.fresh,
              count(distinct sendid)                                                   as mkt_email_clicks_13w,
              count(distinct case when eventdatetime >= lastweekstart then sendid end) as mkt_email_clicks_lw
       FROM research_measurement_data.rmd_salesforce_clicks_src sf
              INNER JOIN central_insights_sandbox.ap_churn_cohorts coh
                         ON subscriberkey = coh.bbc_hid3
                           AND sf.eventdatetime >= coh.mindate
                           AND sf.eventdatetime <= coh.maxfeaturedate
       GROUP BY 1, 2
       ) clicks
ON opens.bbc_hid3 = clicks.bbc_hid3
AND opens.fresh = clicks.fresh
;
GRANT ALL ON central_insights_sandbox.ap_churn_mkt_activity TO GROUP central_insights;

-- DROP TABLE IF EXISTS central_insights_sandbox.ap_churn_newsletter_follows_reduced;

