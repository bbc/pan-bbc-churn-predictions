

DROP TABLE IF EXISTS central_insights_sandbox.ap_churn_demos;
CREATE TABLE central_insights_sandbox.ap_churn_demos
  distkey (bbc_hid3)
  AS
  SELECT prof.bbc_hid3,
         prof.enablepersonalisation,
         prof.mailverified,
         datediff('days', cal_createdate::date, date_trunc('week', coh.maxdate)) as acc_age_days,
         case when acc_age_days < 14 then 0 else 1 end as train_eligible, --really new accounts can't provide any useful training data
         cast(prof.age as integer) as age,
         ext.age_range,
         nvl(case when ext.age_range in ('16-19','20-24','25-29','30-34') then 1
                  when ext.age_range is not null then 0
                  else null end,
             case when ap.predicted_age = '16-34' then 1
                  else 0 end) as age_1634_enriched,
         case when prof.age is null then 1 else 0 end as age_missing_flag,
         case when ext.age_range is null and ap.predicted_age is null then 1 else 0 end as age_1634_enriched_missing_flag,
         prof.gender,
         nvl(prof.gender, gp.predicted_gender) as gender_enriched,
         case when prof.gender is null then 1 else 0 end as gender_missing_flag,
         case when prof.gender is null and gp.predicted_gender is null then 1 else 0 end as gender_enriched_missing_flag,
         ext.nation,
         case when lower(prof.location) = 'gb' then 1 else 0 end as uk_flag,
         ext.barb_region,
         ext.acorn_type_description,
         ext.acorn_group_description,
         ext.acorn_category_description
FROM prez.id_profile prof
  INNER JOIN central_insights_sandbox.ap_churn_cohorts coh
  ON prof.bbc_hid3 = coh.bbc_hid3
  LEFT JOIN prez.profile_extension ext
  ON prof.bbc_hid3 = ext.bbc_hid3
  LEFT JOIN central_insights_sandbox.gender_predictions gp
  ON prof.bbc_hid3 = gp.audience_id
  LEFT JOIN central_insights_sandbox.age1634_predictions ap
  ON prof.bbc_hid3 = ap.audience_id
  LEFT JOIN central_insights_sandbox.ap_churn_cohort_dates coh_dates
  ON coh.cohort = coh_dates.cohort
    --remove dupes
LEFT JOIN
     (
            SELECT bbc_hid3, count(*) AS n
            FROM prez.id_profile
            GROUP BY 1
            HAVING n > 1
     ) dupes
       ON prof.bbc_hid3 = dupes.bbc_hid3
WHERE dupes.bbc_hid3 is NULL
AND prof.status != 'deleted'
;
GRANT ALL ON central_insights_sandbox.ap_churn_demos TO GROUP central_insights;
