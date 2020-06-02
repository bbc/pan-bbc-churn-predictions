DROP TABLE IF EXISTS central_insights_sandbox.tp_churn_sounds_feature_set;
CREATE TABLE central_insights_sandbox.tp_churn_sounds_feature_set AS
SELECT
  -- id_vars // churn-target
  targ.bbc_hid3,
  targ.fresh,
  targ.target_week_start_date,
--   targ.target_week_end_date,
  targ.destination,
  targ.target_churn_this_week,
  targ.target_churn_next_week,
  targ.active_last_week,
  targ.train,
  demos.train_eligible,
  targ.cohort,

  --demographics // churn-demos
  case when  demos.enablepersonalisation = 'true' then 1 else 0 end as profile_enablepersonalisation,
  case when demos.mailverified = 'true' then 1 else 0 end as profile_mailverified,
  demos.acc_age_days as profile_acc_age_days,
  case when demos.age < 0 or demos.age > 120 then NULL else demos.age end as profile_age,
  demos.age_1634_enriched as profile_age_1634_enriched,
--   demos.age_missing_flag as profile_age_missing_flag,
--   demos.age_1634_enriched_missing_flag as profile_age_1634_enriched_missing_flag,
--   demos.gender as profile_gender,
  demos.gender_enriched as profile_gender_enriched,
--   demos.gender_missing_flag as profile_gender_missing_flag,
--   demos.gender_enriched_missing_flag as profile_gender_enriched_missing_flag,
  nvl(demos.nation) as profile_nation,
  demos.uk_flag as profile_uk_flag,
  nvl(demos.barb_region) as profile_barb_region,
  nvl(demos.acorn_type_description) as profile_acorn_type_description,
  nvl(demos.acorn_group_description) as profile_acorn_group_description,
  nvl(demos.acorn_category_description) as profile_acorn_category_description,


  -- weekly streaming // churn-target
  events.streaming_time_13w,
  events.stw_0,
  events.stw_1,
  events.stw_2,
  events.stw_3,
  events.stw_4,
  events.stw_5,
  events.stw_6,
  events.stw_7,
  events.stw_8,
  events.stw_9,
  events.stw_10,
  events.stw_11,
  events.stw_12,
  events.stw_13,
  events.stw_14,
  events.stw_15,

  -- weekly events // churn-target
  events.ew_0,
  events.ew_1,
  events.ew_2,
  events.ew_3,
  events.ew_4,
  events.ew_5,
  events.ew_6,
  events.ew_7,
  events.ew_8,
  events.ew_9,
  events.ew_10,
  events.ew_11,
  events.ew_12,
  events.ew_13,
  events.ew_14,
  events.ew_15,

  -- iplayer linear regression // churn-target
  case when iplayer_linreg.bbc_hid3 is not null then 1 else 0 end as iplayer_user,
  case when iplayer_linreg.aw_2 is null then 0 else iplayer_linreg.aw_2 end as iplayer_active_last_week,
  iplayer_linreg.beta_hat as iplayer_lin_reg_coeff,
  iplayer_linreg.alpha_hat as iplayer_13w_yintercept,
  iplayer_linreg.x_intercept as iplayer_13w_xintercept,
  case when iplayer_linreg.x_intercept is not null then 1 else 0 end as iplayer_lin_reg_churn_flag,

  --sounds linear regression // churn-target
  case when sounds_linreg.bbc_hid3 is not null then 1 else 0 end as sounds_user,
  case when sounds_linreg.aw_2 is null then 0 else sounds_linreg.aw_2 end as sounds_active_last_week,
  sounds_linreg.beta_hat as sounds_lin_reg_coeff,
  sounds_linreg.alpha_hat as sounds_13w_yintercept,
  sounds_linreg.x_intercept as sounds_13w_xintercept,
  case when sounds_linreg.x_intercept is not null then 1 else 0 end as sounds_lin_reg_churn_flag,

    -- sounds // churn-sounds-only
  nvl(sounds_only.sounds_st_live_radio,0) as sounds_prod_st_live_radio,
  nvl(sounds_only.sounds_st_od_sounds_podcasts,0) as sounds_prod_st_od_sounds_podcasts,
  nvl(sounds_only.sounds_st_od_sounds_mixes,0) as sounds_prod_st_od_sounds_mixes,
  nvl(sounds_only.sounds_st_od_radio_podcasts,0) as sounds_prod_st_od_radio_podcasts,
  nvl(sounds_only.sounds_st_od_linear_mixes,0) as sounds_prod_st_od_linear_mixes,
  nvl(sounds_only.sounds_st_od_radio,0) as sounds_prod_st_od_radio,
  nvl(sounds_only.sounds_st_radio,0) as sounds_prod_st_radio,
  nvl(sounds_only.sounds_st_od_podcasts,0) as sounds_prod_st_od_podcasts,
  nvl(sounds_only.sounds_st_od_mixes,0) as sounds_prod_st_od_mixes,
  nvl(sounds_only.sounds_ev_live_radio,0) as sounds_prod_ev_live_radio,
  nvl(sounds_only.sounds_ev_od_sounds_podcasts,0) as sounds_prod_ev_od_sounds_podcasts,
  nvl(sounds_only.sounds_ev_od_sounds_mixes,0) as sounds_prod_ev_od_sounds_mixes,
  nvl(sounds_only.sounds_ev_od_radio_podcasts,0) as sounds_prod_ev_od_radio_podcasts,
  nvl(sounds_only.sounds_ev_od_linear_mixes,0) as sounds_prod_ev_od_linear_mixes,
  nvl(sounds_only.sounds_ev_od_radio,0) as sounds_prod_ev_od_radio,
  nvl(sounds_only.sounds_ev_radio,0) as sounds_prod_ev_radio,
  nvl(sounds_only.sounds_ev_od_podcasts,0) as sounds_prod_ev_od_podcasts,
  nvl(sounds_only.sounds_ev_od_mixes,0) as sounds_prod_ev_od_mixes,
  nvl(sounds_only.sounds_live_radio_st_perc,0)  as sounds_prod_live_radio_st_perc ,
  nvl(sounds_only.sounds_od_sounds_podcasts_st_perc,0) as sounds_prod_od_sounds_podcasts_st_perc,
  nvl(sounds_only.sounds_od_sounds_mixes_st_perc,0) as sounds_prod_od_sounds_mixes_st_perc,
  nvl(sounds_only.sounds_od_radio_podcasts_st_perc,0) as sounds_prod_od_radio_podcasts_st_perc,
  nvl(sounds_only.sounds_od_linear_mixes_st_perc,0)  as sounds_prod_od_linear_mixes_st_perc ,
  nvl(sounds_only.sounds_od_radio_st_perc,0) as sounds_prod_od_radio_st_perc,
  nvl(sounds_only.sounds_radio_st_perc,0) as sounds_prod_radio_st_perc,
  nvl(sounds_only.sounds_od_podcasts_st_perc,0) as sounds_prod_od_podcasts_st_perc,
  nvl(sounds_only.sounds_od_mixes_st_perc,0) as sounds_prod_od_mixes_st_perc,
  nvl(sounds_only.sounds_live_radio_ev_perc,0) as sounds_prod_live_radio_ev_perc,
  nvl(sounds_only.sounds_od_sounds_podcasts_ev_perc,0) as sounds_prod_od_sounds_podcasts_ev_perc,
  nvl(sounds_only.sounds_od_sounds_mixes_ev_perc,0) as sounds_prod_od_sounds_mixes_ev_perc,
  nvl(sounds_only.sounds_od_radio_podcasts_ev_perc,0) as sounds_prod_od_radio_podcasts_ev_perc,
  nvl(sounds_only.sounds_od_linear_mixes_ev_perc,0) as sounds_prod_od_linear_mixes_ev_perc,
  nvl(sounds_only.sounds_od_radio_ev_perc,0) as sounds_prod_od_radio_ev_perc,
  nvl(sounds_only.sounds_radio_ev_perc,0) as sounds_prod_radio_ev_perc,
  nvl(sounds_only.sounds_od_podcasts_ev_perc,0) as sounds_prod_od_podcasts_ev_perc,
  nvl(sounds_only.sounds_od_mixes_ev_perc,0) as sounds_prod_od_mixes_ev_perc,
  nvl(sounds_only.sounds_st_preferred_listening_detail, 'Unknown')  as sounds_prod_st_preferred_listening_detail ,
  nvl(sounds_only.sounds_st_preferred_listening, 'Unknown') as sounds_prod_st_preferred_listening,
  nvl(sounds_only.sounds_ev_preferred_listening_detail, 'Unknown') as sounds_prod_ev_preferred_listening_detail,
  nvl(sounds_only.sounds_ev_preferred_listening, 'Unknown') as sounds_prod_ev_preferred_listening,

  --genre affinity // churn-affinity
  genre_aff.genre_st_comedy,
  genre_aff.genre_st_drama,
  genre_aff.genre_st_ents,
  genre_aff.genre_st_childrens,
  genre_aff.genre_st_factual,
  genre_aff.genre_st_learning,
  genre_aff.genre_st_music,
  genre_aff.genre_st_news,
  genre_aff.genre_st_religion,
  genre_aff.genre_st_sport,
  genre_aff.genre_st_weather,

  genre_aff.genre_share_comedy,
  genre_aff.genre_share_drama,
  genre_aff.genre_share_ents,
  genre_aff.genre_share_childrens,
  genre_aff.genre_share_factual,
  genre_aff.genre_share_learning,
  genre_aff.genre_share_music,
  genre_aff.genre_share_news,
  genre_aff.genre_share_religion,
  genre_aff.genre_share_sport,
  genre_aff.genre_share_weather,

  genre_aff.releases_comedies,
  genre_aff.releases_dramas,
  genre_aff.releases_ents,
  genre_aff.releases_childrens,
  genre_aff.releases_factual,
  genre_aff.releases_learning,
  genre_aff.releases_music,
  genre_aff.releases_news,
  genre_aff.releases_religion,
  genre_aff.releases_sport,
  genre_aff.releases_weather,

  genre_aff.scaled_releases_comedies,
  genre_aff.scaled_releases_dramas,
  genre_aff.scaled_releases_ents,
  genre_aff.scaled_releases_childrens,
  genre_aff.scaled_releases_factual,
  genre_aff.scaled_releases_learning,
  genre_aff.scaled_releases_music,
  genre_aff.scaled_releases_news,
  genre_aff.scaled_releases_religion,
  genre_aff.scaled_releases_sport,
  genre_aff.scaled_releases_weather,

  genre_aff.sched_match_index_comedy,
  genre_aff.sched_match_index_drama,
  genre_aff.sched_match_index_ents,
  genre_aff.sched_match_index_childrens,
  genre_aff.sched_match_index_factual,
  genre_aff.sched_match_index_learning,
  genre_aff.sched_match_index_music,
  genre_aff.sched_match_index_news,
  genre_aff.sched_match_index_religion,
  genre_aff.sched_match_index_sport,
  genre_aff.sched_match_index_weather,

  genre_aff.sched_match_index,


  --last week // churn-lastweek
  case when lw.distinct_series is null then 0 else lw.distinct_series end as lw_distinct_series,
  case when lw.distinct_episodes is null then 0 else lw.distinct_episodes end as lw_distinct_episodes,
  case when lw.series_finales is null then 0 else lw.series_finales end as lw_series_finales,
  case when lw.watched_finale_flag is null then 0 else lw.watched_finale_flag end as lw_watched_finale_flag,
  case when lw.avg_episode_repeats is null then 0 else lw.avg_episode_repeats end as lw_avg_episode_repeats,

  --latest frequency segment // churn-freq-segs
  freq_seg_iplayer as freq_seg_latest_iplayer,
  freq_seg_sounds as freq_seg_latest_sounds,
  freq_seg_news as freq_seg_latest_news,
  freq_seg_sport as freq_seg_latest_sport,
  freq_seg_cbbc as freq_seg_latest_cbbc,
  freq_seg_cbeebies as freq_seg_latest_cbeebies,
  freq_seg_weather as freq_seg_latest_weather,
  freq_seg_panbbc as freq_seg_latest_panbbc,


  -- device usage // churn-freq-segs
  devices.sounds_st_dev_responsive as device_sounds_st_responsive,
	devices.sounds_st_dev_app as device_sounds_st_app,
	devices.sounds_st_dev as device_sounds_st,
	devices.sounds_ev_dev_responsive as device_sounds_ev_responsive,
	devices.sounds_ev_dev_app as device_sounds_ev_app,
	devices.sounds_ev_dev as device_sounds_ev,
  devices.sounds_st_dev_responsive_perc as device_sounds_st_responsive_perc,
	devices.sounds_st_dev_app_perc as device_sounds_st_app_perc,
	devices.sounds_ev_dev_responsive_perc as device_sounds_ev_responsive_perc,
	devices.sounds_ev_dev_app_perc as device_sounds_ev_app_perc,
-- 	devices.iplayer_st_dev_tv as device_iplayer_st_tv,
-- 	devices.iplayer_st_dev_tablet as device_iplayer_st_tablet,
-- 	devices.iplayer_st_dev_app as device_iplayer_st_app,
-- 	devices.iplayer_st_dev_console as device_iplayer_st_console,
-- 	devices.iplayer_st_dev_responsive as device_iplayer_st_responsive,
-- 	devices.iplayer_st_dev as device_iplayer_st,
-- 	devices.iplayer_ev_dev_tv as device_iplayer_ev_tv,
-- 	devices.iplayer_ev_dev_tablet as device_iplayer_ev_tablet,
-- 	devices.iplayer_ev_dev_app as device_iplayer_ev_app,
-- 	devices.iplayer_ev_dev_console as device_iplayer_ev_console,
-- 	devices.iplayer_ev_dev_responsive as device_iplayer_ev_responsive,
-- 	devices.iplayer_ev_dev as device_iplayer_ev,
-- 	devices.iplayer_st_dev_tv_perc as device_iplayer_st_tv_perc,
-- 	devices.iplayer_st_dev_tablet_perc as device_iplayer_st_tablet_perc,
-- 	devices.iplayer_st_dev_app_perc as device_iplayer_st_app_perc,
-- 	devices.iplayer_st_dev_console_perc as device_iplayer_st_console_perc,
-- 	devices.iplayer_st_dev_responsive_perc as device_iplayer_st_responsive_perc,
-- 	devices.iplayer_ev_dev_tv_perc as device_iplayer_ev_tv_perc,
-- 	devices.iplayer_ev_dev_tablet_perc as device_iplayer_ev_tablet_perc,
-- 	devices.iplayer_ev_dev_app_perc as device_iplayer_ev_app_perc,
-- 	devices.iplayer_ev_dev_console_perc as device_iplayer_ev_console_perc,
-- 	devices.iplayer_ev_dev_responsive_perc as device_iplayer_ev_responsive_perc,
	devices.sounds_st_preferred_device as device_sounds_st_preferred,
	devices.iplayer_st_preferred_device as device_iplayer_st_preferred,
	devices.sounds_ev_preferred_device as device_sounds_ev_preferred,
	devices.iplayer_ev_preferred_device as device_iplayer_ev_preferred,

  --activating and favourite content // churn-content
  sounds_acti.sounds_activ_f0,
  sounds_acti.sounds_activ_f1,
  sounds_acti.sounds_activ_f2,
  sounds_acti.sounds_activ_f3,
  sounds_acti.sounds_activ_f4,
  sounds_acti.sounds_activ_f5,
  sounds_acti.sounds_activ_f6,
  sounds_acti.sounds_activ_f7,
  sounds_acti.sounds_activ_f8,
  sounds_acti.sounds_activ_f9,
  sounds_acti.sounds_activ_f10,
  sounds_acti.sounds_activ_f11,
  sounds_acti.sounds_activ_f12,
  sounds_acti.sounds_activ_f13,
  sounds_acti.sounds_activ_f14,
  sounds_acti.sounds_activ_f15,
  sounds_acti.sounds_activ_f16,
  sounds_acti.sounds_activ_f17,
  sounds_acti.sounds_activ_f18,
  sounds_acti.sounds_activ_f19,
  sounds_acti.sounds_weeks_since_activation,
  sounds_acti.sounds_activating_genre,
  sounds_acti.sounds_activating_brand as sounds_activating_masterbrand,

              /* Removed as mostly missing for sounds users*/
--   iplayer_acti.iplayer_weeks_since_activation,
--   iplayer_acti.iplayer_activating_genre,
--   iplayer_acti.iplayer_activating_brand as iplayer_activating_masterbrand,

  sounds_fav.fav_content_genre as sounds_fav_content_genre,
  sounds_fav.fav_content_masterbrand as sounds_fav_content_masterbrand,
  sounds_fav.sounds_fav_f0,
  sounds_fav.sounds_fav_f1,
  sounds_fav.sounds_fav_f2,
  sounds_fav.sounds_fav_f3,
  sounds_fav.sounds_fav_f4,
  sounds_fav.sounds_fav_f5,
  sounds_fav.sounds_fav_f6,
  sounds_fav.sounds_fav_f7,
  sounds_fav.sounds_fav_f8,
  sounds_fav.sounds_fav_f9,
  sounds_fav.sounds_fav_f10,
  sounds_fav.sounds_fav_f11,
  sounds_fav.sounds_fav_f12,
  sounds_fav.sounds_fav_f13,
  sounds_fav.sounds_fav_f14,
  sounds_fav.sounds_fav_f15,
  sounds_fav.sounds_fav_f16,
  sounds_fav.sounds_fav_f17,
  sounds_fav.sounds_fav_f18,
  sounds_fav.sounds_fav_f19,


  iplayer_fav.fav_content_genre as iplayer_fav_content_genre,
  iplayer_fav.fav_content_masterbrand as iplayer_fav_content_masterbrand,

  case when mkt_optin.last_action = 'followed' then 1 else 0 end as mkt_opted_in,
  case when mkt_optin.last_action = 'followed' then datediff('days', servertime::timestamp, coh.maxfeaturedate) end as mkt_days_opted_in,
  case when mkt_optin.last_action = 'unfollowed' then datediff('days', servertime::timestamp, coh.maxfeaturedate) end as mkt_days_opted_out,

  nvl(mkt_activity.mkt_email_opens_lw, 0) as mkt_email_opens_lw,
  nvl(mkt_activity.mkt_email_opens_13w, 0) as mkt_email_opens_13w,
  nvl(mkt_activity.mkt_email_clicks_lw, 0) as mkt_email_clicks_lw,
  nvl(mkt_activity.mkt_email_clicks_13w, 0) as mkt_email_clicks_13,

  nvl(sounds_subscribes.sounds_subscribes_13w, 0) as sounds_subscribes_13w,
  nvl(sounds_subscribes.sounds_subscribes_lastweek, 0) as sounds_subscribes_lastweek,

  nvl(sounds_bookmarks.sounds_bookmarks_13w, 0) as sounds_bookmarks_13w,
  nvl(sounds_bookmarks.sounds_bookmarks_lastweek, 0) as sounds_bookmarks_lastweek


FROM central_insights_sandbox.tp_churn_target targ
       LEFT JOIN
     central_insights_sandbox.tp_churn_cohorts coh
     ON targ.bbc_hid3 = coh.bbc_hid3
     AND targ.fresh = coh.fresh
       LEFT JOIN
     central_insights_sandbox.tp_churn_weekly_active_pivot events
     ON targ.bbc_hid3 = events.bbc_hid3
     AND targ.fresh = events.fresh
       AND events.destination = 'sounds'
       LEFT JOIN
     central_insights_sandbox.tp_churn_13week_lin_reg iplayer_linreg
     ON targ.bbc_hid3 = iplayer_linreg.bbc_hid3
     AND targ.fresh = iplayer_linreg.fresh
       AND iplayer_linreg.destination = 'iplayer'
       LEFT JOIN
     central_insights_sandbox.tp_churn_13week_lin_reg sounds_linreg
     ON targ.bbc_hid3 = sounds_linreg.bbc_hid3
     AND targ.fresh = sounds_linreg.fresh
       AND sounds_linreg.destination = 'sounds'
       LEFT JOIN
     central_insights_sandbox.tp_churn_sounds_product_mix sounds_only
     ON targ.bbc_hid3 = sounds_only.bbc_hid3
     AND targ.fresh = sounds_only.fresh
       LEFT JOIN
     central_insights_sandbox.tp_churn_genre_features genre_aff
     ON targ.bbc_hid3 = genre_aff.bbc_hid3
     AND targ.fresh = genre_aff.fresh
       AND genre_aff.destination = 'sounds'
       LEFT JOIN
     central_insights_sandbox.tp_churn_lw_features lw
     ON targ.bbc_hid3 = lw.bbc_hid3
     AND targ.fresh = lw.fresh
       AND lw.destination = 'PS_SOUNDS'
       --we don't want anybody without an entry in the profile table
       INNER JOIN
     central_insights_sandbox.tp_churn_demos demos
     ON targ.bbc_hid3 = demos.bbc_hid3
       LEFT JOIN
     central_insights_sandbox.tp_churn_last_seg last_seg
     ON targ.bbc_hid3 = last_seg.bbc_hid3
     AND targ.fresh = last_seg.fresh
       LEFT JOIN
     central_insights_sandbox.tp_churn_devices devices
     ON targ.bbc_hid3 = devices.bbc_hid3
     AND targ.fresh = devices.fresh
       LEFT JOIN
     central_insights_sandbox.tp_churn_sounds_activating_users sounds_acti
     ON targ.bbc_hid3 = sounds_acti.bbc_hid3
     AND targ.fresh = sounds_acti.fresh
       LEFT JOIN
     central_insights_sandbox.tp_churn_iplayer_activating_users iplayer_acti
     ON targ.bbc_hid3 = iplayer_acti.bbc_hid3
     AND targ.fresh = iplayer_acti.fresh
       LEFT JOIN
     central_insights_sandbox.tp_churn_favourite_content_sounds sounds_fav
     ON targ.bbc_hid3 = sounds_fav.bbc_hid3
     AND targ.fresh = sounds_fav.fresh
       LEFT JOIN
     central_insights_sandbox.tp_churn_favourite_content_iplayer iplayer_fav
     ON targ.bbc_hid3 = iplayer_fav.bbc_hid3
     AND targ.fresh = iplayer_fav.fresh
       LEFT JOIN
     central_insights_sandbox.tp_churn_mkt_optin mkt_optin
     ON targ.bbc_hid3 = mkt_optin.bbc_hid3
     AND targ.fresh = mkt_optin.fresh
       LEFT JOIN
     central_insights_sandbox.tp_churn_mkt_activity mkt_activity
     ON targ.bbc_hid3 = mkt_activity.bbc_hid3
     AND targ.fresh = mkt_activity.fresh
       LEFT JOIN
     central_insights_sandbox.tp_churn_sounds_subscribes sounds_subscribes
     ON targ.bbc_hid3 = sounds_subscribes.bbc_hid3
     AND targ.fresh = sounds_subscribes.fresh
       LEFT JOIN
     central_insights_sandbox.tp_churn_sounds_bookmarks sounds_bookmarks
     ON targ.bbc_hid3 = sounds_bookmarks.bbc_hid3
     AND targ.fresh = sounds_bookmarks.fresh
WHERE targ.destination = 'sounds'
;
GRANT ALL ON central_insights_sandbox.tp_churn_sounds_feature_set TO GROUP central_insights;

DROP TABLE IF EXISTS central_insights_sandbox.tp_churn_sounds_training_sample;
CREATE TABLE central_insights_sandbox.tp_churn_sounds_training_sample AS
SELECT * FROM central_insights_sandbox.tp_churn_sounds_feature_set
WHERE fresh = 0
  and sounds_active_last_week = 1
ORDER BY random()
LIMIT 10000
;
GRANT ALL ON central_insights_sandbox.tp_churn_sounds_training_sample TO GROUP central_insights;

DROP TABLE IF EXISTS central_insights_sandbox.tp_churn_sounds_score_sample;
CREATE TABLE central_insights_sandbox.tp_churn_sounds_score_sample AS
SELECT * FROM central_insights_sandbox.tp_churn_sounds_feature_set
WHERE fresh = 1
  AND sounds_active_last_week = 1
ORDER BY random()
LIMIT 10000
;
GRANT ALL ON central_insights_sandbox.tp_churn_sounds_score_sample TO GROUP central_insights;

--duplicates
-- select count(*) from (
--                        select bbc_hid3, count(*) as count
--                        from central_insights_sandbox.tp_churn_sounds_feature_set
--                        group by 1
--                        having count > 1
--                      ) dupes;
--target: 0
--active: 0
--lin reg: 0
--sounds: 0
--genre: 0
--lw: 0
--demos: 45

