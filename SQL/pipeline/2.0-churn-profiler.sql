DROP TABLE IF EXISTS central_insights_sandbox.tp_churn_iplayer_score_sample_profiler;
CREATE TABLE central_insights_sandbox.tp_churn_iplayer_score_sample_profiler AS
  SELECT base.bbc_hid3,
         prof.age::int as age,
         case when age >= 16 and age <= 34 then 1 else 0 end as age_1634,
         prof.gender,
         ext.nation,
         ext.acorn_category_description as acorn_cat,
         ext.acorn_category::int as acorn_cat_num
FROM central_insights_sandbox.tp_churn_iplayer_score_sample base
  INNER JOIN prez.id_profile prof
    ON base.bbc_hid3 = prof.bbc_hid3
  LEFT JOIN prez.profile_extension ext
    ON prof.bbc_hid3 = ext.bbc_hid3
;

