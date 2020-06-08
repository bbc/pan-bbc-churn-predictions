
-- Collect active IDs by week
-- Potentially switch to redshift enriched, performance concerns
--hids and last weeks for each cohort
DROP TABLE IF EXISTS central_insights_sandbox.tp_churn_sounds_13w_raw;
CREATE TABLE central_insights_sandbox.tp_churn_sounds_13w_raw AS
SELECT audience_id                   as bbc_hid3,
       coh.fresh,
       destination,
       version_id,
       CASE
         WHEN brand_id != 'N/A' AND brand_id != ''
           AND brand_id != 'null' AND brand_id IS NOT NULL THEN brand_id
         WHEN series_id != 'N/A' AND series_id != ''
           AND series_id != 'null' AND series_id IS NOT NULL THEN series_id
         WHEN episode_id != 'N/A' AND episode_id != ''
           AND episode_id != 'null' AND episode_id IS NOT NULL THEN episode_id
         --   WHEN presentation_id != 'N/A' AND presentation_id != ''
         -- AND presentation_id != 'null' AND presentation_id IS NOT NULL THEN presentation_id
         WHEN clip_id != 'N/A' AND clip_id != ''
           AND clip_id != 'null' AND clip_id IS NOT NULL THEN clip_id
         END                         AS tleo_id,
       broadcast_type,
       sum(playback_time_total)      as streaming_time,
       count(distinct case when playback_time_total >= 180 then date_of_event else null end) as events
FROM audience.audience_activity_daily_summary_enriched aud
       INNER JOIN central_insights_sandbox.tp_churn_cohorts coh
                  ON audience_id = coh.bbc_hid3
                    AND aud.date_of_event >= coh.mindate
                    AND aud.date_of_event <= coh.maxfeaturedate
WHERE destination in ('PS_SOUNDS')
  AND aud.playback_time_total > 0
GROUP BY 1, 2, 3, 4, 5, 6
;
GRANT ALL ON central_insights_sandbox.tp_churn_sounds_13w_raw TO GROUP central_insights;

/*
See Aileen's music/radio/podcasts snippet on github:
  https://github.com/bbc/sounds-analytics/blob/master/useful-snippets/music_radio_podcasts_definitions.SQL
*/
DROP TABLE IF EXISTS central_insights_sandbox.tp_churn_sounds_product_mix_tmp1;
CREATE TABLE central_insights_sandbox.tp_churn_sounds_product_mix_tmp1
  AS
SELECT bbc_hid3,
       fresh,
       destination,
       tleo_id,
       broadcast_type,

       --vmb fields
       vmb.format_names,
       vmb.episode_id,
       vmb.master_brand_id,
       vmb.master_brand_name,

       streaming_time::float as streaming_time,
       events::float as events
FROM central_insights_sandbox.tp_churn_sounds_13w_raw a
  LEFT JOIN prez.scv_vmb vmb
  ON a.version_id = vmb.version_id
;
GRANT ALL ON central_insights_sandbox.tp_churn_sounds_product_mix_tmp1 TO GROUP central_insights;

DROP TABLE IF EXISTS central_insights_sandbox.tp_churn_sounds_product_mix_tmp2;
CREATE TABLE central_insights_sandbox.tp_churn_sounds_product_mix_tmp2
  AS
SELECT bbc_hid3,
       fresh,
       destination,
       tleo_id,
       broadcast_type,

       --vmb fields
       format_names,
       episode_id,
       master_brand_id,
       master_brand_name,

       --derived fields
       CASE WHEN master_brand_id = 'bbc_sounds_podcasts' OR
          episode_id IN (
            SELECT DISTINCT episode_id
             FROM  prez.scv_vmb
             WHERE version_type = 'Podcast version') OR
          tleo_id IN (
            SELECT DISTINCT tleo_id
            FROM central_insights.sounds_podcasts_metadata_schedule_positions
             ) OR
      format_names LIKE '%Podcast%' THEN TRUE ELSE FALSE END as all_podcasts_bool,

      CASE WHEN master_brand_id = 'bbc_sounds_podcasts' OR
              tleo_id IN (
                          SELECT DISTINCT tleo_id
                          FROM central_insights.sounds_podcasts_metadata_schedule_positions
                          ) OR
              format_names LIKE '%Podcast%' THEN TRUE ELSE FALSE END as sounds_podcasts_bool,

      CASE WHEN format_names LIKE '%Mixes%' OR -- this should be enough really but I don't trust people to tag stuff so just incase, also pull through anything that's been curated in the mixes rail...
        episode_id IN (SELECT DISTINCT episode_id FROM central_insights.sounds_mixes_episodes_metadata_all) OR
        episode_id IN (SELECT DISTINCT episode_id FROM central_insights.sounds_mixes_metadata_schedule_positions)
        THEN TRUE ELSE FALSE END as all_mixes_bool,

      CASE WHEN master_brand_name = 'BBC Sounds Mixes' OR
    (master_brand_name = 'BBC Radio' AND format_names LIKE '%Mixes%') OR -- talent mixes are played out of local radio
    episode_id IN ('m00017xz','p0759pb5','p074xvs4','p075hcbw','p076kkqz','p073mfmf',
    'p0792nqg','m00057dk','m00057dh','m00057d6','m00057df','m00057dc','p07c69np','p07c65zj','p07dkblp','p07dnt5n',
    'p07dp1s6','p07dnw4s','p07dnqcw','p07dp36k','p07dpk7v','p07dh4c0','p07dzyqh','p07f1cth','p07f1c0j','p07f783g',
    'p07f78zy','p07k3zxc','p07khcm3','m00080lx','m00080lz','m00080m1','m00087k1','m00087k3','m00080mk','m00087k5',
    'm00080mm','m00087k7','m00080mf','m00080mc','m00080mh','m000810z','m0008111','m000810x','m0008113','p07kdz8k',
    'p07q0618','p07qjkpr','p07r964d','p07r8j2j','p07qsjg9'
    ) -- exceptions
    THEN TRUE ELSE FALSE END as sounds_mixes_bool,

    CASE WHEN broadcast_type = 'Live' THEN 'live_radio'
         WHEN sounds_podcasts_bool = TRUE THEN 'od_sounds_podcasts'
         WHEN sounds_mixes_bool = TRUE THEN 'od_sounds_mixes'
         WHEN all_podcasts_bool = TRUE THEN 'od_radio_podcasts'
         WHEN all_podcasts_bool = TRUE THEN 'od_linear_mixes'
         ELSE 'od_radio'
      END as listening_type,


       streaming_time,
       events
FROM central_insights_sandbox.tp_churn_sounds_product_mix_tmp1
;
GRANT ALL ON central_insights_sandbox.tp_churn_sounds_product_mix_tmp2 TO GROUP central_insights;


DROP TABLE IF EXISTS central_insights_sandbox.tp_churn_sounds_product_mix_tmp3;
CREATE TABLE central_insights_sandbox.tp_churn_sounds_product_mix_tmp3
  AS
SELECT bbc_hid3,
       fresh,
       destination,

       --totals
       nvl(sum(streaming_time),0)::float as streaming_time,
       nvl(sum(events),0)::float as events,

       --stream aggs
       sum(case when listening_type = 'live_radio' then streaming_time else 0 end)::float as sounds_st_live_radio,
       sum(case when listening_type = 'od_sounds_podcasts' then streaming_time else 0 end)::float as sounds_st_od_sounds_podcasts,
       sum(case when listening_type = 'od_sounds_mixes' then streaming_time else 0 end)::float as sounds_st_od_sounds_mixes,
       sum(case when listening_type = 'od_radio_podcasts' then streaming_time else 0 end)::float as sounds_st_od_radio_podcasts,
       sum(case when listening_type = 'od_linear_mixes' then streaming_time else 0 end)::float as sounds_st_od_linear_mixes,
       sum(case when listening_type = 'od_radio' then streaming_time else 0 end)::float as sounds_st_od_radio,

       sounds_st_live_radio + sounds_st_od_radio as sounds_st_radio,
       sounds_st_od_sounds_podcasts + sounds_st_od_radio_podcasts as sounds_st_od_podcasts,
       sounds_st_od_sounds_mixes + sounds_st_od_linear_mixes as sounds_st_od_mixes,

       --event aggs
       sum(case when listening_type = 'live_radio' then events else 0 end)::float as sounds_ev_live_radio,
       sum(case when listening_type = 'od_sounds_podcasts' then events else 0 end)::float as sounds_ev_od_sounds_podcasts,
       sum(case when listening_type = 'od_sounds_mixes' then events else 0 end)::float as sounds_ev_od_sounds_mixes,
       sum(case when listening_type = 'od_radio_podcasts' then events else 0 end)::float as sounds_ev_od_radio_podcasts,
       sum(case when listening_type = 'od_linear_mixes' then events else 0 end)::float as sounds_ev_od_linear_mixes,
       sum(case when listening_type = 'od_radio' then events else 0 end)::float as sounds_ev_od_radio,

       sounds_ev_live_radio + sounds_ev_od_radio as sounds_ev_radio,
       sounds_ev_od_sounds_podcasts + sounds_ev_od_radio_podcasts as sounds_ev_od_podcasts,
       sounds_ev_od_sounds_mixes + sounds_ev_od_linear_mixes as sounds_ev_od_mixes

FROM central_insights_sandbox.tp_churn_sounds_product_mix_tmp2
 GROUP BY 1, 2, 3
;
GRANT ALL ON central_insights_sandbox.tp_churn_sounds_product_mix_tmp3 TO GROUP central_insights;

DROP TABLE IF EXISTS central_insights_sandbox.tp_churn_sounds_product_mix;
CREATE TABLE central_insights_sandbox.tp_churn_sounds_product_mix
  distkey(bbc_hid3)
  AS 
SELECT *,
       
       sounds_st_live_radio / streaming_time as sounds_live_radio_st_perc,
       sounds_st_od_sounds_podcasts / streaming_time as sounds_od_sounds_podcasts_st_perc,
       sounds_st_od_sounds_mixes / streaming_time as sounds_od_sounds_mixes_st_perc,
       sounds_st_od_radio_podcasts / streaming_time as sounds_od_radio_podcasts_st_perc,
       sounds_st_od_linear_mixes / streaming_time as sounds_od_linear_mixes_st_perc,
       sounds_st_od_radio / streaming_time as sounds_od_radio_st_perc,

       sounds_st_radio / streaming_time as sounds_radio_st_perc,
       sounds_st_od_podcasts / streaming_time as sounds_od_podcasts_st_perc,
       sounds_st_od_mixes / streaming_time as sounds_od_mixes_st_perc,
       
       case when events = 0 then 0 else sounds_ev_live_radio / events end as sounds_live_radio_ev_perc,
       case when events = 0 then 0 else sounds_ev_od_sounds_podcasts / events end as sounds_od_sounds_podcasts_ev_perc,
       case when events = 0 then 0 else sounds_ev_od_sounds_mixes / events end as sounds_od_sounds_mixes_ev_perc,
       case when events = 0 then 0 else sounds_ev_od_radio_podcasts / events end as sounds_od_radio_podcasts_ev_perc,
       case when events = 0 then 0 else sounds_ev_od_linear_mixes / events end as sounds_od_linear_mixes_ev_perc,
       case when events = 0 then 0 else sounds_ev_od_radio / events end as sounds_od_radio_ev_perc,
       
       case when events = 0 then 0 else sounds_ev_radio / events end as sounds_radio_ev_perc,
       case when events = 0 then 0 else sounds_ev_od_podcasts / events end as sounds_od_podcasts_ev_perc,
       case when events = 0 then 0 else sounds_ev_od_mixes / events end as sounds_od_mixes_ev_perc,



       -- PREFERRED LISTENING ############

       ---- streaming-time
       case when sounds_live_radio_st_perc >= sounds_od_sounds_podcasts_st_perc
              and sounds_live_radio_st_perc >= sounds_od_sounds_mixes_st_perc
              and sounds_live_radio_st_perc >= sounds_od_radio_podcasts_st_perc
              and sounds_live_radio_st_perc >= sounds_od_linear_mixes_st_perc
              and sounds_live_radio_st_perc >= sounds_od_radio_st_perc
              then 'live-radio'
            when sounds_od_sounds_podcasts_st_perc >= sounds_od_sounds_mixes_st_perc
              and sounds_od_sounds_podcasts_st_perc >= sounds_od_radio_podcasts_st_perc
              and sounds_od_sounds_podcasts_st_perc >= sounds_od_linear_mixes_st_perc
              and sounds_od_sounds_podcasts_st_perc >= sounds_od_radio_st_perc
              then 'on-demand-sounds-podcasts'
            when sounds_od_sounds_mixes_st_perc >= sounds_od_radio_podcasts_st_perc
              and sounds_od_sounds_mixes_st_perc >= sounds_od_linear_mixes_st_perc
              and sounds_od_sounds_mixes_st_perc >= sounds_od_radio_st_perc
              then 'on-demand-sounds-mixes'
            when sounds_od_radio_podcasts_st_perc >= sounds_od_linear_mixes_st_perc
              and sounds_od_radio_podcasts_st_perc >= sounds_od_radio_st_perc
              then 'on-demand-radio-podcasts'
            when sounds_od_linear_mixes_st_perc >= sounds_od_radio_st_perc
              then 'on-demand-linear-mixes'
            else 'on-demand-radio'
         end as sounds_st_preferred_listening_detail,
       
       case when sounds_radio_st_perc >= sounds_od_podcasts_st_perc
              and sounds_radio_st_perc >= sounds_od_mixes_st_perc
              then 'radio'
            when sounds_od_podcasts_st_perc >= sounds_od_mixes_st_perc
              then 'podcasts'
            else 'mixes'
          end as sounds_st_preferred_listening,

       ---- events
       case when sounds_live_radio_ev_perc >= sounds_od_sounds_podcasts_ev_perc
              and sounds_live_radio_ev_perc >= sounds_od_sounds_mixes_ev_perc
              and sounds_live_radio_ev_perc >= sounds_od_radio_podcasts_ev_perc
              and sounds_live_radio_ev_perc >= sounds_od_linear_mixes_ev_perc
              and sounds_live_radio_ev_perc >= sounds_od_radio_ev_perc
              then 'live-radio'
            when sounds_od_sounds_podcasts_ev_perc >= sounds_od_sounds_mixes_ev_perc
              and sounds_od_sounds_podcasts_ev_perc >= sounds_od_radio_podcasts_ev_perc
              and sounds_od_sounds_podcasts_ev_perc >= sounds_od_linear_mixes_ev_perc
              and sounds_od_sounds_podcasts_ev_perc >= sounds_od_radio_ev_perc
              then 'on-demand-sounds-podcasts'
            when sounds_od_sounds_mixes_ev_perc >= sounds_od_radio_podcasts_ev_perc
              and sounds_od_sounds_mixes_ev_perc >= sounds_od_linear_mixes_ev_perc
              and sounds_od_sounds_mixes_ev_perc >= sounds_od_radio_ev_perc
              then 'on-demand-sounds-mixes'
            when sounds_od_radio_podcasts_ev_perc >= sounds_od_linear_mixes_ev_perc
              and sounds_od_radio_podcasts_ev_perc >= sounds_od_radio_ev_perc
              then 'on-demand-radio-podcasts'
            when sounds_od_linear_mixes_ev_perc >= sounds_od_radio_ev_perc
              then 'on-demand-linear-mixes'
            else 'on-demand-radio'
         end as sounds_ev_preferred_listening_detail,
       
       case when sounds_radio_ev_perc >= sounds_od_podcasts_ev_perc
              and sounds_radio_ev_perc >= sounds_od_mixes_ev_perc
              then 'radio'
            when sounds_od_podcasts_ev_perc >= sounds_od_mixes_ev_perc
              then 'podcasts'
            else 'mixes'
          end as sounds_ev_preferred_listening


FROM central_insights_sandbox.tp_churn_sounds_product_mix_tmp3
;
GRANT ALL ON central_insights_sandbox.tp_churn_sounds_product_mix TO GROUP central_insights;

DROP TABLE IF EXISTS central_insights_sandbox.tp_churn_sounds_13w_raw;
DROP TABLE IF EXISTS central_insights_sandbox.tp_churn_sounds_product_mix_tmp1;
DROP TABLE IF EXISTS central_insights_sandbox.tp_churn_sounds_product_mix_tmp2;
DROP TABLE IF EXISTS central_insights_sandbox.tp_churn_sounds_product_mix_tmp3;
