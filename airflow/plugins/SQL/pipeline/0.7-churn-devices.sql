DROP TABLE IF EXISTS central_insights_sandbox.tp_churn_devices_13w_raw;
CREATE TABLE central_insights_sandbox.tp_churn_devices_13w_raw AS
SELECT audience_id                   as bbc_hid3,
       destination,
       fresh,
       central_insights.udf_destination_prod(destination, app_name, page_name) as destination_prod,
       device_type,
       CASE
         WHEN device_type = 'Desktop' THEN 'responsive'
         WHEN browser_brand = 'Applications' THEN 'mobile-app'
         ELSE 'responsive' END       as derived_app_type,
       sum(playback_time_total)     as streaming_time,
       count(distinct date_of_event) as events
FROM audience.audience_activity_daily_summary_enriched aud
       INNER JOIN central_insights_sandbox.tp_churn_cohorts coh
                  ON audience_id = coh.bbc_hid3
                    AND aud.date_of_event >= coh.mindate
                    AND aud.date_of_event <= coh.maxfeaturedate
WHERE destination in ('PS_IPLAYER', 'PS_SOUNDS')
  AND aud.playback_time_total >= 180
GROUP BY 1, 2, 3, 4,5 ,6
;
GRANT ALL ON central_insights_sandbox.tp_churn_devices_13w_raw TO GROUP central_insights;

DROP TABLE IF EXISTS central_insights_sandbox.tp_churn_devices_tmp1;
CREATE TABLE central_insights_sandbox.tp_churn_devices_tmp1
  AS
SELECT bbc_hid3,
       fresh,

       -- SOUNDS ###################
       ---- streaming time
       sum(
         case when destination = 'PS_SOUNDS' and derived_app_type = 'responsive' then streaming_time else 0 end
         ) as sounds_st_dev_responsive,
       sum(
         case when destination = 'PS_SOUNDS' and derived_app_type = 'mobile-app' then streaming_time else 0 end
         ) as sounds_st_dev_app,
       sounds_st_dev_responsive + sounds_st_dev_app as sounds_st_dev,

       ---- events
       sum(
         case when destination = 'PS_SOUNDS' and derived_app_type = 'responsive' then events else 0 end
         ) as sounds_ev_dev_responsive,
       sum(
         case when destination = 'PS_SOUNDS' and derived_app_type = 'mobile-app' then events else 0 end
         ) as sounds_ev_dev_app,
       sounds_ev_dev_responsive + sounds_ev_dev_app as sounds_ev_dev,
       -- ##################


       --iPlayer #################
       ---- streaming time
       sum(
         case when destination = 'PS_IPLAYER' and device_type = 'Smart TV' then streaming_time else 0 end
         ) as iplayer_st_dev_tv,
       sum(
         case when destination = 'PS_IPLAYER' and device_type = 'Tablet' then streaming_time else 0 end
         ) as iplayer_st_dev_tablet,
       sum(
         case when destination = 'PS_IPLAYER' and device_type = 'Smartphone' then streaming_time else 0 end
         ) as iplayer_st_dev_app,
       sum(
         case when destination = 'PS_IPLAYER' and device_type = 'Console' then streaming_time else 0 end
         ) as iplayer_st_dev_console,
       sum(
         case when destination = 'PS_IPLAYER' and device_type not in ('Smart TV', 'Tablet', 'Smartphone', 'Console') then streaming_time else 0 end
         ) as iplayer_st_dev_responsive,
       iplayer_st_dev_tv + iplayer_st_dev_tablet + iplayer_st_dev_app + iplayer_st_dev_console + iplayer_st_dev_responsive
         as iplayer_st_dev,
       
       ---- events
       sum(
         case when destination = 'PS_IPLAYER' and device_type = 'Smart TV' then events else 0 end
         ) as iplayer_ev_dev_tv,
       sum(
         case when destination = 'PS_IPLAYER' and device_type = 'Tablet' then events else 0 end
         ) as iplayer_ev_dev_tablet,
       sum(
         case when destination = 'PS_IPLAYER' and device_type = 'Smartphone' then events else 0 end
         ) as iplayer_ev_dev_app,
       sum(
         case when destination = 'PS_IPLAYER' and device_type = 'Console' then events else 0 end
         ) as iplayer_ev_dev_console,
       sum(
         case when destination = 'PS_IPLAYER' and device_type not in ('Smart TV', 'Tablet', 'Smartphone', 'Console') then events else 0 end
         ) as iplayer_ev_dev_responsive,
       iplayer_ev_dev_tv + iplayer_ev_dev_tablet + iplayer_ev_dev_app + iplayer_ev_dev_console + iplayer_ev_dev_responsive
         as iplayer_ev_dev

FROM central_insights_sandbox.tp_churn_devices_13w_raw
WHERE destination_prod != 'radio'
GROUP BY 1, 2
;
GRANT ALL ON central_insights_sandbox.tp_churn_devices_tmp1 TO GROUP central_insights;

DROP TABLE IF EXISTS central_insights_sandbox.tp_churn_devices;
CREATE TABLE central_insights_sandbox.tp_churn_devices
  distkey(bbc_hid3)
  AS
SELECT *,

      -- SOUNDS ################
      ---- streaming time
      case when sounds_st_dev = 0 then 0 else sounds_st_dev_responsive::float / sounds_st_dev end as sounds_st_dev_responsive_perc,
      case when sounds_st_dev = 0 then 0 else sounds_st_dev_app::float / sounds_st_dev end as sounds_st_dev_app_perc,
       
      ---- events
      case when sounds_ev_dev = 0 then 0 else sounds_ev_dev_responsive::float / sounds_ev_dev end as sounds_ev_dev_responsive_perc,
      case when sounds_ev_dev = 0 then 0 else sounds_ev_dev_app::float / sounds_ev_dev end as sounds_ev_dev_app_perc,



      -- IPLAYER ##############
      ---- streaming time
      case when iplayer_st_dev = 0 then 0 else iplayer_st_dev_tv::float / iplayer_st_dev end as iplayer_st_dev_tv_perc,
      case when iplayer_st_dev = 0 then 0 else iplayer_st_dev_tablet::float / iplayer_st_dev end as iplayer_st_dev_tablet_perc,
      case when iplayer_st_dev = 0 then 0 else iplayer_st_dev_app::float / iplayer_st_dev end as iplayer_st_dev_app_perc,
      case when iplayer_st_dev = 0 then 0 else iplayer_st_dev_console::float / iplayer_st_dev end as iplayer_st_dev_console_perc,
      case when iplayer_st_dev = 0 then 0 else iplayer_st_dev_responsive::float / iplayer_st_dev end as iplayer_st_dev_responsive_perc,
       
      ---- events
      case when iplayer_ev_dev = 0 then 0 else iplayer_ev_dev_tv::float / iplayer_ev_dev end as iplayer_ev_dev_tv_perc,
      case when iplayer_ev_dev = 0 then 0 else iplayer_ev_dev_tablet::float / iplayer_ev_dev end as iplayer_ev_dev_tablet_perc,
      case when iplayer_ev_dev = 0 then 0 else iplayer_ev_dev_app::float / iplayer_ev_dev end as iplayer_ev_dev_app_perc,
      case when iplayer_ev_dev = 0 then 0 else iplayer_ev_dev_console::float / iplayer_ev_dev end as iplayer_ev_dev_console_perc,
      case when iplayer_ev_dev = 0 then 0 else iplayer_ev_dev_responsive::float / iplayer_ev_dev end as iplayer_ev_dev_responsive_perc,


      -- PREFERRED DEVICES
      ---- streaming_time
      case when sounds_st_dev_app_perc >= .5 then 'mobile-app' else  'responsive' end as sounds_st_preferred_device,
      case when iplayer_st_dev_tv_perc >= iplayer_st_dev_tablet_perc
              and iplayer_st_dev_tv_perc >= iplayer_st_dev_app_perc
              and iplayer_st_dev_tv_perc >= iplayer_st_dev_console_perc
              and iplayer_st_dev_tv_perc >= iplayer_st_dev_responsive_perc
              then 'smart-tv'
           when iplayer_st_dev_tablet_perc >= iplayer_st_dev_app_perc
              and iplayer_st_dev_tablet_perc >= iplayer_st_dev_console_perc
              and iplayer_st_dev_tablet_perc >= iplayer_st_dev_responsive_perc
              then 'tablet'
           when iplayer_st_dev_app_perc >= iplayer_st_dev_console_perc
              and iplayer_st_dev_app_perc >= iplayer_st_dev_responsive_perc
              then 'app'
           when iplayer_st_dev_console_perc >= iplayer_st_dev_responsive_perc
              then 'console'
           else 'responsive'
      end as iplayer_st_preferred_device,

      ---- events
      case when sounds_ev_dev_app_perc >= .5 then 'mobile-app' else  'responsive' end as sounds_ev_preferred_device,
      case when iplayer_ev_dev_tv_perc >= iplayer_ev_dev_tablet_perc
              and iplayer_ev_dev_tv_perc >= iplayer_ev_dev_app_perc
              and iplayer_ev_dev_tv_perc >= iplayer_ev_dev_console_perc
              and iplayer_ev_dev_tv_perc >= iplayer_ev_dev_responsive_perc
              then 'smart-tv'
           when iplayer_ev_dev_tablet_perc >= iplayer_ev_dev_app_perc
              and iplayer_ev_dev_tablet_perc >= iplayer_ev_dev_console_perc
              and iplayer_ev_dev_tablet_perc >= iplayer_ev_dev_responsive_perc
              then 'tablet'
           when iplayer_ev_dev_app_perc >= iplayer_ev_dev_console_perc
              and iplayer_ev_dev_app_perc >= iplayer_ev_dev_responsive_perc
              then 'app'
           when iplayer_ev_dev_console_perc >= iplayer_ev_dev_responsive_perc
              then 'console'
           else 'responsive'
      end as iplayer_ev_preferred_device
FROM central_insights_sandbox.tp_churn_devices_tmp1
;
GRANT ALL ON central_insights_sandbox.tp_churn_devices TO GROUP central_insights;

DROP TABLE IF EXISTS  central_insights_sandbox.tp_churn_devices_raw;
DROP TABLE IF EXISTS central_insights_sandbox.tp_churn_devices_tmp1;


