
DROP TABLE IF EXISTS central_insights_sandbox.tp_churn_timeandday_raw;
CREATE TABLE central_insights_sandbox.tp_churn_timeandday_raw AS
  SELECT audience_id,
         coh.fresh,
         destination,
         central_insights.udf_destination_prod(destination, app_name, page_name) as destination_prod,
         --Time of day
         sum(case when date_part('hour', event_datetime_min) in (6, 7, 8, 9) then playback_time_total else 0 end) as tod_breakfast,
         sum(case when date_part('hour', event_datetime_min) in (10, 11, 12, 13, 14, 15, 16) then playback_time_total else 0 end) as tod_daytime,
         sum(case when date_part('hour', event_datetime_min) in (17, 18, 19) then playback_time_total else 0 end) as tod_earlypeak,
         sum(case when date_part('hour', event_datetime_min) in (20, 21) then playback_time_total else 0 end) as tod_peak,
         sum(case when date_part('hour', event_datetime_min) in (22, 23) then playback_time_total else 0 end) as tod_late,
         sum(case when date_part('hour', event_datetime_min) in (0, 1, 2, 3, 4, 5) then playback_time_total else 0 end) as tod_overnight,

         --Day of week
         sum(case when date_part('dow', date_of_event) = 1 then playback_time_total else 0 end) as dow_monday,
         sum(case when date_part('dow', date_of_event) = 2 then playback_time_total else 0 end) as dow_tuesday,
         sum(case when date_part('dow', date_of_event) = 3 then playback_time_total else 0 end) as dow_wednesday,
         sum(case when date_part('dow', date_of_event) = 4 then playback_time_total else 0 end) as dow_thursday,
         sum(case when date_part('dow', date_of_event) = 5 then playback_time_total else 0 end) as dow_friday,
         sum(case when date_part('dow', date_of_event) = 6 then playback_time_total else 0 end) as dow_saturday,
         sum(case when date_part('dow', date_of_event) = 0 then playback_time_total else 0 end) as dow_sunday

  FROM audience.audience_activity_daily_summary_enriched aud
       INNER JOIN central_insights_sandbox.tp_churn_cohorts coh
                  ON audience_id = coh.bbc_hid3
                    AND aud.date_of_event >= coh.mindate
                    AND aud.date_of_event <= coh.maxfeaturedate
WHERE destination in ('PS_IPLAYER', 'PS_SOUNDS')
GROUP BY 1, 2, 3, 4
;

DROP TABLE IF EXISTS central_insights_sandbox.tp_churn_timeandday;
CREATE TABLE central_insights_sandbox.tp_churn_timeandday AS
  SELECT audience_id as bbc_hid3,
         fresh,
         destination,
         dow_monday + dow_tuesday + dow_wednesday + dow_thursday + dow_friday + dow_saturday + dow_sunday as playback_total,

         case when playback_total = 0 then 0 else tod_breakfast::float /playback_total end as tod_breakfast,
         case when playback_total = 0 then 0 else tod_daytime::float /playback_total end as tod_daytime,
         case when playback_total = 0 then 0 else tod_earlypeak::float /playback_total end as tod_earlypeak,
         case when playback_total = 0 then 0 else tod_peak::float /playback_total end as tod_peak,
         case when playback_total = 0 then 0 else tod_late::float /playback_total end as tod_late,
         case when playback_total = 0 then 0 else tod_overnight::float /playback_total end as tod_overnight,

         case when playback_total = 0 then 0 else dow_monday::float /playback_total end as dow_monday,
         case when playback_total = 0 then 0 else dow_tuesday::float /playback_total end as dow_tuesday,
         case when playback_total = 0 then 0 else dow_wednesday::float /playback_total end as dow_wednesday,
         case when playback_total = 0 then 0 else dow_thursday::float /playback_total end as dow_thursday,
         case when playback_total = 0 then 0 else dow_friday::float /playback_total end as dow_friday,
         case when playback_total = 0 then 0 else dow_saturday::float /playback_total end as dow_saturday,
         case when playback_total = 0 then 0 else dow_sunday::float /playback_total end as dow_sunday

FROM central_insights_sandbox.tp_churn_timeandday_raw
;
