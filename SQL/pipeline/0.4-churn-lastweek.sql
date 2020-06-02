--hids and last weeks for each cohort
DROP TABLE IF EXISTS central_insights_sandbox.ap_churn_lastweek;
CREATE TABLE central_insights_sandbox.ap_churn_lastweek AS
  SELECT audience_id as bbc_hid3,
         coh.fresh,
         destination,
         central_insights.udf_destination_prod(destination, app_name, page_name) as destination_prod,
         version_id,
         count(distinct date_of_event) as events
  FROM audience.audience_activity_daily_summary_enriched aud
  INNER JOIN central_insights_sandbox.ap_churn_cohorts coh
    ON audience_id = coh.bbc_hid3
    AND aud.date_of_event >= coh.lastweekstart
    AND aud.date_of_event <= coh.maxfeaturedate
  WHERE destination in ('PS_IPLAYER', 'PS_SOUNDS')
    AND aud.playback_time_total >= 600
GROUP BY 1,2,3,4,5
;
GRANT ALL ON central_insights_sandbox.ap_churn_lastweek TO GROUP central_insights;

--series schedule to identify finales
DROP TABLE IF EXISTS central_insights_sandbox.ap_churn_schedule;
CREATE TABLE central_insights_sandbox.ap_churn_schedule AS
  select episode_id,
       version_id                                                                   as version_id_first_broadcast,
       series_id,
       programme_title,
       programme_duration,
       episode_title,
       genre,
       subgenre,
       case when tx_date = 'null' then null else to_date(tx_date, 'YYYY-MM-DD') end as tx_date,
       tx_start,
       row_number() over
         (partition by series_id
         order by decode(tx_date, 'null', to_date('2099-01-01', 'YYYY-MM-DD'), to_date(tx_date, 'YYYY-MM-DD')),
           decode(tx_start, 'null', '99:99:99', tx_start)
         )                                                                          as episode_number
from (select version_id,
             episode_id,
             series_id,
             programme_title,
             episode_title,
             programme_duration,
             split_part(genres_list_a_names,';',1) as genre,
             split_part(genres_list_a_names,';',2) as subgenre,
             tx_date,
             tx_start,
             row_number() over
               (
               partition by series_id, episode_id
               order by decode(tx_date, 'null', to_date('2099-01-01', 'YYYY-MM-DD'), to_date(tx_date, 'YYYY-MM-DD')),
                 decode(tx_start, 'null', '99:99:99', tx_start)
               ) as rank
      from prez.scv_vmb
      where bbc_st_pips = 'episode'
     ) subs
where rank = 1
;
GRANT ALL ON central_insights_sandbox.ap_churn_schedule TO GROUP central_insights;

DROP TABLE IF EXISTS central_insights_sandbox.ap_churn_series_finales;
CREATE TABLE central_insights_sandbox.ap_churn_series_finales AS
  SELECT
    episode_id,
         series_id,
         episode_number,
         max(episode_number) over (
           partition by series_id
           rows between unbounded preceding and unbounded following
           ) as eps_in_series,
         case when episode_number = 1 then 1 else 0 end as series_premiere,
         case when episode_number = eps_in_series then 1 else 0 end as series_finale
  from (
       select
          episode_id,
          series_id,
          episode_number
      from central_insights_sandbox.ap_churn_schedule
        group by 1, 2, 3
         ) sched
  ;
GRANT ALL ON central_insights_sandbox.ap_churn_series_finales TO GROUP central_insights;

DROP TABLE IF EXISTS central_insights_sandbox.ap_churn_series;
CREATE TABLE central_insights_sandbox.ap_churn_series AS
  SELECT bbc_hid3,
         fresh,
         lw.destination,
         count(distinct vmb.tleo) as distinct_series,
         count(distinct vmb.episode_id) as distinct_episodes,
         sum(sf.series_premiere) as series_premieres,
         sum(sf.series_finale) as series_finales
  FROM central_insights_sandbox.ap_churn_lastweek lw
    LEFT JOIN (SELECT version_id,
                      episode_id,
                      case
                  when vmb.brand_title != 'n/a'
                    and vmb.brand_title != ''
                    and vmb.brand_title != 'null'
                    and vmb.brand_title is not null
                    then vmb.brand_title
                  when vmb.series_title != 'n/a'
                    and vmb.series_title != ''
                    and vmb.series_title != 'null'
                    and vmb.series_title is not null
                    then vmb.series_title
                  when vmb.programme_title != 'n/a'
                    and vmb.programme_title != ''
                    and vmb.programme_title != 'null'
                    and vmb.programme_title is not null
                    then vmb.programme_title
                  when vmb.episode_title != 'n/a'
                    and vmb.episode_title != ''
                    and vmb.episode_title != 'null'
                    and vmb.episode_title is not null
                    then vmb.episode_title
                  when vmb.presentation_title != 'n/a'
                    and vmb.presentation_title != ''
                    and vmb.presentation_title != 'null'
                    and vmb.presentation_title is not null
                    then vmb.presentation_title
                  when vmb.clip_title != 'n/a'
                    and vmb.clip_title != ''
                    and vmb.clip_title != 'null'
                    and vmb.clip_title is not null
                    then vmb.clip_title
                  end as tleo
      FROM prez.scv_vmb vmb
      ) vmb
      ON lw.version_id = vmb.version_id
  LEFT JOIN central_insights_sandbox.ap_churn_series_finales sf
    ON vmb.episode_id = sf.episode_id
  WHERE lw.destination_prod != 'radio'
GROUP BY 1,2,3;
GRANT ALL ON central_insights_sandbox.ap_churn_series TO GROUP central_insights;


DROP TABLE IF EXISTS central_insights_sandbox.ap_churn_avg_repeats;
CREATE TABLE central_insights_sandbox.ap_churn_avg_repeats AS
  SELECT bbc_hid3,
         fresh,
         destination,
         avg(repeats) as avg_episode_repeats
FROM (
       SELECT bbc_hid3,
              fresh,
              destination,
              vmb.episode_id,
              sum(case when vmb.episode_id is not null then lw.events else 0 end)::float AS repeats
       FROM central_insights_sandbox.ap_churn_lastweek lw
              LEFT JOIN prez.scv_vmb vmb
                        ON lw.version_id = vmb.version_id
       GROUP BY 1, 2, 3, 4
     ) subs
GROUP BY 1,2,3
;
GRANT ALL ON central_insights_sandbox.ap_churn_avg_repeats TO GROUP central_insights;


DROP TABLE IF EXISTS central_insights_sandbox.ap_churn_lw_features;
CREATE TABLE central_insights_sandbox.ap_churn_lw_features
  distkey (bbc_hid3)
  sortkey (destination)
  AS
  SELECT series.bbc_hid3,
         series.destination,
         series.fresh,
         series.distinct_series,
         series.distinct_episodes,
         series.series_premieres,
         series.series_finales,
         case when series.series_finales > 0 then 1 else 0 end as watched_finale_flag,
         repeats.avg_episode_repeats
  FROM central_insights_sandbox.ap_churn_series series
  LEFT JOIN
    central_insights_sandbox.ap_churn_avg_repeats repeats
    ON series.bbc_hid3 = repeats.bbc_hid3
    and series.destination = repeats.destination
    and series.fresh = repeats.fresh
;
GRANT ALL ON central_insights_sandbox.ap_churn_lw_features TO GROUP central_insights;