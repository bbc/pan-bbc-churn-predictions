DROP TABLE IF EXISTS central_insights_sandbox.tp_churn_devices_13w_raw;
CREATE TABLE central_insights_sandbox.tp_churn_devices_13w_raw AS
SELECT audience_id                   as bbc_hid3,
       destination,
       fresh,
       central_insights.udf_destination_prod(destination, app_name, page_name) as destination_prod,
       device_type,
       CASE
         WHEN device_type = 'Desktop' THEN 'desktop-web'
         WHEN device_type = 'Smart TV' THEN 'smart-tv'
         WHEN device_type = 'Smartphone' and browser_brand = 'Applications' THEN 'mobile-app'
         WHEN device_type = 'Smartphone' and browser_brand != 'Applications' THEN 'mobile-web'
         WHEN device_type = 'Tablet' and browser_brand = 'Applications' THEN 'tablet-app'
         WHEN device_type = 'Tablet' and browser_brand != 'Applications' THEN 'tablet-web'
         ELSE 'other-web' END       as derived_app_type,
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

       count(distinct case when destination = 'PS_SOUNDS' then device_type else NULL end) as sounds_device_count,
       count(distinct case when destination = 'PS_SOUNDS' then device_type else NULL end) as iplayer_device_count,

       -- SOUNDS ###################
       ---- streaming time
       sum(
         case when destination = 'PS_SOUNDS' and derived_app_type = 'desktop-web' then streaming_time else 0 end
         ) as sounds_st_dev_desktop_web,
       sum(
         case when destination = 'PS_SOUNDS' and derived_app_type = 'mobile-app' then streaming_time else 0 end
         ) as sounds_st_dev_app,
       sum(
         case when destination = 'PS_SOUNDS' and derived_app_type = 'mobile-web' then streaming_time else 0 end
         ) as sounds_st_dev_mobile_web,
       sum(
         case when destination = 'PS_SOUNDS' and derived_app_type = 'smart-tv' then streaming_time else 0 end
         ) as sounds_st_dev_smart_tv,
       sum(
         case when destination = 'PS_SOUNDS' and derived_app_type = 'other-web' then streaming_time else 0 end
         ) as sounds_st_dev_other_web,
       sounds_st_dev_desktop_web + sounds_st_dev_app + sounds_st_dev_mobile_web
         + sounds_st_dev_smart_tv + sounds_st_dev_other_web as sounds_streaming_time,


       ---- events
              sum(
         case when destination = 'PS_SOUNDS' and derived_app_type = 'desktop-web' then events else 0 end
         ) as sounds_ev_dev_desktop_web,
       sum(
         case when destination = 'PS_SOUNDS' and derived_app_type = 'mobile-app' then events else 0 end
         ) as sounds_ev_dev_app,
       sum(
         case when destination = 'PS_SOUNDS' and derived_app_type = 'mobile-web' then events else 0 end
         ) as sounds_ev_dev_mobile_web,
       sum(
         case when destination = 'PS_SOUNDS' and derived_app_type = 'smart-tv' then events else 0 end
         ) as sounds_ev_dev_smart_tv,
       sum(
         case when destination = 'PS_SOUNDS' and derived_app_type = 'other-web' then events else 0 end
         ) as sounds_ev_dev_other_web,
       sounds_ev_dev_desktop_web + sounds_ev_dev_app + sounds_ev_dev_mobile_web
         + sounds_ev_dev_smart_tv + sounds_ev_dev_other_web as sounds_events,
       -- ##################


       -- IPLAYER ###################
       ---- streaming time
       sum(
         case when destination = 'PS_IPLAYER' and derived_app_type = 'desktop-web' then streaming_time else 0 end
         ) as iplayer_st_dev_desktop_web,
       sum(
         case when destination = 'PS_IPLAYER' and derived_app_type = 'mobile-app' then streaming_time else 0 end
         ) as iplayer_st_dev_app,
       sum(
         case when destination = 'PS_IPLAYER' and derived_app_type = 'mobile-web' then streaming_time else 0 end
         ) as iplayer_st_dev_mobile_web,
       sum(
         case when destination = 'PS_IPLAYER' and derived_app_type = 'smart-tv' then streaming_time else 0 end
         ) as iplayer_st_dev_smart_tv,
       sum(
         case when destination = 'PS_IPLAYER' and derived_app_type = 'other-web' then streaming_time else 0 end
         ) as iplayer_st_dev_other_web,
       iplayer_st_dev_desktop_web + iplayer_st_dev_app + iplayer_st_dev_mobile_web
         + iplayer_st_dev_smart_tv + iplayer_st_dev_other_web as iplayer_streaming_time,


       ---- events
              sum(
         case when destination = 'PS_IPLAYER' and derived_app_type = 'desktop-web' then events else 0 end
         ) as iplayer_ev_dev_desktop_web,
       sum(
         case when destination = 'PS_IPLAYER' and derived_app_type = 'mobile-app' then events else 0 end
         ) as iplayer_ev_dev_app,
       sum(
         case when destination = 'PS_IPLAYER' and derived_app_type = 'mobile-web' then events else 0 end
         ) as iplayer_ev_dev_mobile_web,
       sum(
         case when destination = 'PS_IPLAYER' and derived_app_type = 'smart-tv' then events else 0 end
         ) as iplayer_ev_dev_smart_tv,
       sum(
         case when destination = 'PS_IPLAYER' and derived_app_type = 'other-web' then events else 0 end
         ) as iplayer_ev_dev_other_web,
       iplayer_ev_dev_desktop_web + iplayer_ev_dev_app + iplayer_ev_dev_mobile_web
         + iplayer_ev_dev_smart_tv + iplayer_ev_dev_other_web as iplayer_events
       -- ##################

FROM central_insights_sandbox.tp_churn_devices_13w_raw
WHERE destination_prod != 'radio'
GROUP BY 1, 2
;
GRANT ALL ON central_insights_sandbox.tp_churn_devices_tmp1 TO GROUP central_insights;

DROP TABLE IF EXISTS central_insights_sandbox.tp_churn_devices;
CREATE TABLE central_insights_sandbox.tp_churn_devices
  distkey(bbc_hid3)
  AS
SELECT bbc_hid3,
       fresh,

       sounds_device_count,
       iplayer_device_count,

      -- SOUNDS ################
      ---- streaming time
      case when sounds_streaming_time = 0 then 0 else sounds_st_dev_desktop_web::float / sounds_streaming_time end as sounds_st_desktop_web_perc,
      case when sounds_streaming_time = 0 then 0 else sounds_st_dev_app::float / sounds_streaming_time end as sounds_st_app_perc,
      case when sounds_streaming_time = 0 then 0 else sounds_st_dev_mobile_web::float / sounds_streaming_time end as sounds_st_mobile_web_perc,
      case when sounds_streaming_time = 0 then 0 else sounds_st_dev_smart_tv::float / sounds_streaming_time end as sounds_st_smart_tv_perc,
      case when sounds_streaming_time = 0 then 0 else sounds_st_dev_other_web::float / sounds_streaming_time end as sounds_st_other_web_perc,

      ---- events
      case when sounds_events = 0 then 0 else sounds_ev_dev_desktop_web::float / sounds_events end as sounds_ev_desktop_web_perc,
      case when sounds_events = 0 then 0 else sounds_ev_dev_app::float / sounds_events end as sounds_ev_app_perc,
      case when sounds_events = 0 then 0 else sounds_ev_dev_mobile_web::float / sounds_events end as sounds_ev_mobile_web_perc,
      case when sounds_events = 0 then 0 else sounds_ev_dev_smart_tv::float / sounds_events end as sounds_ev_smart_tv_perc,
      case when sounds_events = 0 then 0 else sounds_ev_dev_other_web::float / sounds_events end as sounds_ev_other_web_perc,

      -- IPLAYER ################
      ---- streaming time
      case when iplayer_streaming_time = 0 then 0 else iplayer_st_dev_desktop_web::float / iplayer_streaming_time end as iplayer_st_desktop_web_perc,
      case when iplayer_streaming_time = 0 then 0 else iplayer_st_dev_app::float / iplayer_streaming_time end as iplayer_st_app_perc,
      case when iplayer_streaming_time = 0 then 0 else iplayer_st_dev_mobile_web::float / iplayer_streaming_time end as iplayer_st_mobile_web_perc,
      case when iplayer_streaming_time = 0 then 0 else iplayer_st_dev_smart_tv::float / iplayer_streaming_time end as iplayer_st_smart_tv_perc,
      case when iplayer_streaming_time = 0 then 0 else iplayer_st_dev_other_web::float / iplayer_streaming_time end as iplayer_st_other_web_perc,

      ---- events
      case when iplayer_events = 0 then 0 else iplayer_ev_dev_desktop_web::float / iplayer_events end as iplayer_ev_desktop_web_perc,
      case when iplayer_events = 0 then 0 else iplayer_ev_dev_app::float / iplayer_events end as iplayer_ev_app_perc,
      case when iplayer_events = 0 then 0 else iplayer_ev_dev_mobile_web::float / iplayer_events end as iplayer_ev_mobile_web_perc,
      case when iplayer_events = 0 then 0 else iplayer_ev_dev_smart_tv::float / iplayer_events end as iplayer_ev_smart_tv_perc,
      case when iplayer_events = 0 then 0 else iplayer_ev_dev_other_web::float / iplayer_events end as iplayer_ev_other_web_perc,


      -- PREFERRED DEVICES
      ---- streaming_time
      -- SOUNDS ##########
      case when sounds_st_desktop_web_perc >= sounds_st_app_perc
              and sounds_st_desktop_web_perc >= sounds_st_mobile_web_perc
              and sounds_st_desktop_web_perc >= sounds_st_smart_tv_perc
              and sounds_st_desktop_web_perc >= sounds_st_other_web_perc
              then 'desktop-web'
           when sounds_st_app_perc >= sounds_st_mobile_web_perc
              and sounds_st_app_perc >= sounds_st_smart_tv_perc
              and sounds_st_app_perc >= sounds_st_other_web_perc
              then 'mobile-app'
           when sounds_st_mobile_web_perc >= sounds_st_smart_tv_perc
              and sounds_st_mobile_web_perc >= sounds_st_other_web_perc
              then 'mobile-web'
           when sounds_st_smart_tv_perc >= sounds_st_other_web_perc
              then 'smart-tv'
           else 'other-web'
      end as sounds_st_preferred_device,
       
      case when iplayer_st_desktop_web_perc >= iplayer_st_app_perc
              and iplayer_st_desktop_web_perc >= iplayer_st_mobile_web_perc
              and iplayer_st_desktop_web_perc >= iplayer_st_smart_tv_perc
              and iplayer_st_desktop_web_perc >= iplayer_st_other_web_perc
              then 'desktop-web'
           when iplayer_st_app_perc >= iplayer_st_mobile_web_perc
              and iplayer_st_app_perc >= iplayer_st_smart_tv_perc
              and iplayer_st_app_perc >= iplayer_st_other_web_perc
              then 'mobile-app'
           when iplayer_st_mobile_web_perc >= iplayer_st_smart_tv_perc
              and iplayer_st_mobile_web_perc >= iplayer_st_other_web_perc
              then 'mobile-web'
           when iplayer_st_smart_tv_perc >= iplayer_st_other_web_perc
              then 'smart-tv'
           else 'other-web'
      end as iplayer_st_preferred_device,      
       
      case when sounds_ev_desktop_web_perc >= sounds_ev_app_perc
              and sounds_ev_desktop_web_perc >= sounds_ev_mobile_web_perc
              and sounds_ev_desktop_web_perc >= sounds_ev_smart_tv_perc
              and sounds_ev_desktop_web_perc >= sounds_ev_other_web_perc
              then 'desktop-web'
           when sounds_ev_app_perc >= sounds_ev_mobile_web_perc
              and sounds_ev_app_perc >= sounds_ev_smart_tv_perc
              and sounds_ev_app_perc >= sounds_ev_other_web_perc
              then 'mobile-app'
           when sounds_ev_mobile_web_perc >= sounds_ev_smart_tv_perc
              and sounds_ev_mobile_web_perc >= sounds_ev_other_web_perc
              then 'mobile-web'
           when sounds_ev_smart_tv_perc >= sounds_ev_other_web_perc
              then 'smart-tv'
           else 'other-web'
      end as sounds_ev_preferred_device,
      
      case when iplayer_ev_desktop_web_perc >= iplayer_ev_app_perc
              and iplayer_ev_desktop_web_perc >= iplayer_ev_mobile_web_perc
              and iplayer_ev_desktop_web_perc >= iplayer_ev_smart_tv_perc
              and iplayer_ev_desktop_web_perc >= iplayer_ev_other_web_perc
              then 'desktop-web'
           when iplayer_ev_app_perc >= iplayer_ev_mobile_web_perc
              and iplayer_ev_app_perc >= iplayer_ev_smart_tv_perc
              and iplayer_ev_app_perc >= iplayer_ev_other_web_perc
              then 'mobile-app'
           when iplayer_ev_mobile_web_perc >= iplayer_ev_smart_tv_perc
              and iplayer_ev_mobile_web_perc >= iplayer_ev_other_web_perc
              then 'mobile-web'
           when iplayer_ev_smart_tv_perc >= iplayer_ev_other_web_perc
              then 'smart-tv'
           else 'other-web'
      end as iplayer_ev_preferred_device
FROM central_insights_sandbox.tp_churn_devices_tmp1
;
GRANT ALL ON central_insights_sandbox.tp_churn_devices TO GROUP central_insights;

DROP TABLE IF EXISTS central_insights_sandbox.tp_churn_devices_raw;
DROP TABLE IF EXISTS central_insights_sandbox.tp_churn_devices_tmp1;


