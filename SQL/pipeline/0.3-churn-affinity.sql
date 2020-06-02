--AFFINITY - iPLAYER

--Too slow to do a date join, so doing a week agg and filtering

-- Time
BEGIN;
DROP TABLE IF EXISTS central_insights_sandbox.ap_churn_affinity_broad;
CREATE TABLE central_insights_sandbox.ap_churn_affinity_broad AS
SELECT
       bbc_hid3,
       destination,
       cohort,
       fresh,
       sum(events) as events,
       sum(streaming_time)::float as streaming_time,
       sum(distinct_days) as distinct_days,
       sum(page_views) as page_views,
       sum(genre_st_comedy)::float as genre_st_comedy,
       sum(genre_st_drama)::float as genre_st_drama,
       sum(genre_st_ents)::float as genre_st_ents,
       sum(genre_st_childrens)::float as genre_st_childrens,
       sum(genre_st_factual)::float as genre_st_factual,
       sum(genre_st_learning)::float as genre_st_learning,
       sum(genre_st_music)::float as genre_st_music,
       sum(genre_st_news)::float as genre_st_news,
       sum(genre_st_religion)::float as genre_st_religion,
       sum(genre_st_sport)::float as genre_st_sport,
       sum(genre_st_weather)::float as genre_st_weather,
       sum(genre_st_other)::float as genre_st_other
       FROM
  (
SELECT
       *,
       datediff(week, week, (select maxweek from central_insights_sandbox.ap_churn_explore_vars)) as weeks_out,
       weeks_out - cohort as ew
FROM central_insights_sandbox.ap_churn_weekly_active_ids ids
    where destination != 'radio'
  ) ids_ew
WHERE ew <= 14
GROUP BY 1, 2, 3, 4
;
COMMIT;
GRANT ALL ON central_insights_sandbox.ap_churn_affinity_broad TO GROUP central_insights;

DROP TABLE IF EXISTS central_insights_sandbox.ap_churn_genre_schedule;
CREATE TABLE central_insights_sandbox.ap_churn_genre_schedule AS
SELECT
      case when media_type = 'video' then 'iplayer'
           when media_type = 'audio' then 'sounds'
      end as destination,
      week,
      sum(case when pips_genre_level_1_names like 'Comedy%' then 1 else 0 end)::float as releases_comedies,
      sum(case when pips_genre_level_1_names like 'Drama%' then 1 else 0 end)::float as releases_dramas,
      sum(case when pips_genre_level_1_names like 'Entertainment%' then 1 else 0 end)::float as releases_ents,
      sum(case when pips_genre_level_1_names like 'Children%' then 1 else 0 end)::float as releases_childrens,
      sum(case when pips_genre_level_1_names like 'Factual%' then 1 else 0 end)::float as releases_factual,
      sum(case when pips_genre_level_1_names like 'Learning%' then 1 else 0 end)::float as releases_learning,
      sum(case when pips_genre_level_1_names like 'Music%' then 1 else 0 end)::float as releases_music,
      sum(case when pips_genre_level_1_names like 'News%' then 1 else 0 end)::float as releases_news,
      sum(case when pips_genre_level_1_names like 'Religion & Ethics%' then 1 else 0 end)::float as releases_religion,
      sum(case when pips_genre_level_1_names like 'Sport%' then 1 else 0 end)::float as releases_sport,
      sum(case when pips_genre_level_1_names like 'Weather%' then 1 else 0 end)::float as releases_weather,
      count(*) - (releases_comedies+releases_dramas+releases_ents+releases_childrens+releases_factual+
                  releases_learning+releases_music+releases_news+releases_religion+releases_sport+releases_weather) as releases_other
FROM
  (
  SELECT
    media_type,
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
                  end as tleo,
    date_trunc('week', case when tx_date = 'null' then null else to_date(tx_date, 'YYYY-MM-DD') end ) as week,
    pips_genre_level_1_names
    FROM
    prez.scv_vmb vmb
    WHERE tx_date >= '1970-01-01'
--       and media_type = 'video'
    GROUP BY media_type, tleo, 3, 4
    ) sched_week
GROUP BY 1, 2
;
GRANT ALL ON central_insights_sandbox.ap_churn_genre_schedule TO GROUP central_insights;

DROP TABLE IF EXISTS central_insights_sandbox.ap_churn_genre_schedule_scaled;
CREATE TABLE central_insights_sandbox.ap_churn_genre_schedule_scaled AS
SELECT
       destination,
       week,
       case when avg_releases_comedies = 0 then 1 else releases_comedies / avg_releases_comedies end as scaled_releases_comedies,
       case when avg_releases_dramas = 0 then 1 else releases_dramas / avg_releases_dramas end as scaled_releases_dramas,
       case when avg_releases_ents = 0 then 1 else releases_ents / avg_releases_ents end as scaled_releases_ents,
       case when avg_releases_children = 0 then 1 else releases_childrens / avg_releases_children end as scaled_releases_childrens,
       case when avg_releases_factual = 0 then 1 else releases_factual / avg_releases_factual end as scaled_releases_factual,
       case when avg_releases_learning = 0 then 1 else releases_learning / avg_releases_learning end as scaled_releases_learning,
       case when avg_releases_music = 0 then 1 else releases_music / avg_releases_music end as scaled_releases_music,
       case when avg_releases_news = 0 then 1 else releases_news / avg_releases_news end as scaled_releases_news,
       case when avg_releases_religion = 0 then 1 else releases_religion / avg_releases_religion end as scaled_releases_religion,
       case when avg_releases_sport = 0 then 1 else releases_sport / avg_releases_sport end as scaled_releases_sport,
       case when avg_releases_weather = 0 then 1 else releases_weather / avg_releases_weather end as scaled_releases_weather,
       case when avg_releases_other = 0 then 1 else releases_other / avg_releases_other end as scaled_releases_other
FROM (
       SELECT destination,
              week,
              releases_comedies,
              releases_dramas,
              releases_ents,
              releases_childrens,
              releases_factual,
              releases_learning,
              releases_music,
              releases_news,
              releases_religion,
              releases_sport,
              releases_weather,
              releases_other,
              avg(releases_comedies) over (partition by destination rows between 12 preceding and current row )  as avg_releases_comedies,
              avg(releases_dramas) over (partition by destination rows between 12 preceding and current row )    as avg_releases_dramas,
              avg(releases_ents) over (partition by destination rows between 12 preceding and current row )      as avg_releases_ents,
              avg(releases_childrens) over (partition by destination rows between 12 preceding and current row ) as avg_releases_children,
              avg(releases_factual) over (partition by destination rows between 12 preceding and current row )   as avg_releases_factual,
              avg(releases_learning) over (partition by destination rows between 12 preceding and current row )  as avg_releases_learning,
              avg(releases_music) over (partition by destination rows between 12 preceding and current row )     as avg_releases_music,
              avg(releases_news) over (partition by destination rows between 12 preceding and current row )      as avg_releases_news,
              avg(releases_religion) over (partition by destination rows between 12 preceding and current row )  as avg_releases_religion,
              avg(releases_sport) over (partition by destination rows between 12 preceding and current row )     as avg_releases_sport,
              avg(releases_weather) over (partition by destination rows between 12 preceding and current row )   as avg_releases_weather,
              avg(releases_other) over (partition by destination rows between 12 preceding and current row )     as avg_releases_other
       FROM central_insights_sandbox.ap_churn_genre_schedule
      WHERE week >= '2017-01-01'
     ) wind
;
GRANT ALL ON central_insights_sandbox.ap_churn_genre_schedule_scaled TO GROUP central_insights;

DROP TABLE IF EXISTS central_insights_sandbox.ap_churn_affinity_broad_share;
CREATE TABLE central_insights_sandbox.ap_churn_affinity_broad_share AS
SELECT broad.*,
       case when broad.streaming_time > 0 then genre_st_comedy / streaming_time else 0 end    as genre_share_comedy,
       case when broad.streaming_time > 0 then genre_st_drama / streaming_time else 0 end     as genre_share_drama,
       case when broad.streaming_time > 0 then genre_st_ents / streaming_time else 0 end      as genre_share_ents,
       case when broad.streaming_time > 0 then genre_st_childrens / streaming_time else 0 end as genre_share_childrens,
       case when broad.streaming_time > 0 then genre_st_factual / streaming_time else 0 end   as genre_share_factual,
       case when broad.streaming_time > 0 then genre_st_learning / streaming_time else 0 end  as genre_share_learning,
       case when broad.streaming_time > 0 then genre_st_music / streaming_time else 0 end     as genre_share_music,
       case when broad.streaming_time > 0 then genre_st_news / streaming_time else 0 end      as genre_share_news,
       case when broad.streaming_time > 0 then genre_st_religion / streaming_time else 0 end  as genre_share_religion,
       case when broad.streaming_time > 0 then genre_st_sport / streaming_time else 0 end     as genre_share_sport,
       case when broad.streaming_time > 0 then genre_st_weather / streaming_time else 0 end   as genre_share_weather,
       case when broad.streaming_time > 0 then genre_st_other / streaming_time else 0 end     as genre_share_other,

       --Binary flags
       case when genre_st_comedy > 0 then 1 else 0 end as genre_watch_comedy,
       case when genre_st_drama > 0 then 1 else 0 end as genre_watch_drama,
       case when genre_st_ents > 0 then 1 else 0 end as genre_watch_ents,
       case when genre_st_childrens > 0 then 1 else 0 end as genre_watch_childrens,
       case when genre_st_factual > 0 then 1 else 0 end as genre_watch_factual,
       case when genre_st_learning > 0 then 1 else 0 end as genre_watch_learning,
       case when genre_st_music > 0 then 1 else 0 end as genre_watch_music,
       case when genre_st_news > 0 then 1 else 0 end as genre_watch_news,
       case when genre_st_religion > 0 then 1 else 0 end as genre_watch_religion,
       case when genre_st_sport > 0 then 1 else 0 end as genre_watch_sport,
       case when genre_st_weather > 0 then 1 else 0 end as genre_watch_weather,
       case when genre_st_other > 0 then 1 else 0 end as genre_watch_other,

       genre_watch_comedy + genre_watch_drama + genre_watch_ents + genre_watch_childrens + genre_watch_factual +
       genre_watch_learning + genre_watch_music + genre_watch_news + genre_watch_religion +
       genre_watch_sport + genre_watch_weather + genre_watch_other as genre_distinct_count

FROM central_insights_sandbox.ap_churn_affinity_broad broad
;

GRANT ALL ON central_insights_sandbox.ap_churn_affinity_broad_share TO GROUP central_insights;

-- Attach schedule for range over the target week - AKA new shows coming up in the potential churn window
DROP TABLE IF EXISTS central_insights_sandbox.ap_churn_genre_features; --57291276
CREATE TABLE central_insights_sandbox.ap_churn_genre_features
  distkey (bbc_hid3)
  sortkey (destination)
  AS
SELECT
       broad.*,

       sched.releases_comedies,
       sched.releases_dramas,
       sched.releases_ents,
       sched.releases_childrens,
       sched.releases_factual,
       sched.releases_learning,
       sched.releases_music,
       sched.releases_news,
       sched.releases_religion,
       sched.releases_sport,
       sched.releases_weather,
       sched.releases_other,

       scaled.scaled_releases_comedies,
       scaled.scaled_releases_dramas,
       scaled.scaled_releases_ents,
       scaled.scaled_releases_childrens,
       scaled.scaled_releases_factual,
       scaled.scaled_releases_learning,
       scaled.scaled_releases_music,
       scaled.scaled_releases_news,
       scaled.scaled_releases_religion,
       scaled.scaled_releases_sport,
       scaled.scaled_releases_weather,
       scaled.scaled_releases_other,


       (scaled.scaled_releases_comedies - 1) * broad.genre_share_comedy as sched_match_index_comedy,
       (scaled.scaled_releases_dramas - 1) * broad.genre_share_drama as sched_match_index_drama,
       (scaled.scaled_releases_ents - 1) * broad.genre_share_ents as sched_match_index_ents,
       (scaled.scaled_releases_childrens - 1) * broad.genre_share_childrens as sched_match_index_childrens,
       (scaled.scaled_releases_factual - 1) * broad.genre_share_factual as sched_match_index_factual,
       (scaled.scaled_releases_learning - 1) * broad.genre_share_learning as sched_match_index_learning,
       (scaled.scaled_releases_music - 1) * broad.genre_share_music as sched_match_index_music,
       (scaled.scaled_releases_news - 1) * broad.genre_share_news as sched_match_index_news,
       (scaled.scaled_releases_religion - 1) * broad.genre_share_religion as sched_match_index_religion,
       (scaled.scaled_releases_sport - 1) * broad.genre_share_sport as sched_match_index_sport,
       (scaled.scaled_releases_weather - 1) * broad.genre_share_weather as sched_match_index_weather,

       sched_match_index_comedy + sched_match_index_drama + sched_match_index_ents + sched_match_index_childrens +
       sched_match_index_factual + sched_match_index_learning + sched_match_index_music + sched_match_index_news +
       sched_match_index_religion + sched_match_index_sport + sched_match_index_weather as sched_match_index

       FROM central_insights_sandbox.ap_churn_affinity_broad_share broad
        LEFT JOIN central_insights_sandbox.ap_churn_cohort_dates coh
          ON broad.cohort = coh.cohort
        LEFT JOIN central_insights_sandbox.ap_churn_genre_schedule sched
          ON broad.destination = sched.destination
          AND sched.week = date_trunc('week', maxDate)
        LEFT JOIN central_insights_sandbox.ap_churn_genre_schedule_scaled scaled
          ON broad.destination = scaled.destination
          AND scaled.week = date_trunc('week', maxDate)
;
GRANT ALL ON central_insights_sandbox.ap_churn_genre_features TO GROUP central_insights;

select * from central_insights_sandbox.ap_churn_affinity_broad
where bbc_hid3 = '1Iye6H20WXY5o9rJFkqyZbOCJBWqmiqeJqEQ3P5kxS8'
limit 100;