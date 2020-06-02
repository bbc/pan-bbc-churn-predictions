--  DROP TABLE IF EXISTS central_insights_sandbox.ap_churn_freq_segs;
-- CREATE TABLE central_insights_sandbox.ap_churn_freq_segs
--     distkey (bbc_hid3)
--     sortkey (destination)
-- AS
--    SELECT hashedidentity as bbc_hid3,
--          segmentation as destination,
--          score
--   FROM central_insights.sfmc_sg10026_avg_days_between_visits_by_product
--   WHERE segmentation in ('sounds', 'iplayer', 'PanBBC')
-- ;

DROP TABLE IF EXISTS central_insights_sandbox.ap_churn_last_seg;
CREATE TABLE central_insights_sandbox.ap_churn_last_seg AS
SELECT coh.bbc_hid3,
       coh.fresh,
       nvl(seg_iplayer.segvalue, 'X. Inactive') as freq_seg_iplayer,
       nvl(seg_sounds.segvalue, 'X. Inactive') as freq_seg_sounds,
       nvl(seg_news.segvalue, 'X. Inactive') as freq_seg_news,
       nvl(seg_sport.segvalue, 'X. Inactive') as freq_seg_sport,
       nvl(seg_cbbc.segvalue, 'X. Inactive') as freq_seg_cbbc,
       nvl(seg_cbeebies.segvalue, 'X. Inactive') as freq_seg_cbeebies,
       nvl(seg_weather.segvalue, 'X. Inactive') as freq_seg_weather,
       nvl(seg_panbbc.segvalue, 'X. Inactive') as freq_seg_panbbc
FROM central_insights_sandbox.ap_churn_cohorts coh

  --iplayer
  LEFT JOIN central_insights.sg10026_info_individual_alltime seg_iplayer
  ON coh.bbc_hid3 = seg_iplayer.bbc_hid3
  AND lower(seg_iplayer.product) = 'iplayer'
  AND coh.lastweekstart = date_trunc('week', seg_iplayer.date_of_segmentation)

  --sounds
  LEFT JOIN central_insights.sg10026_info_individual_alltime seg_sounds
  ON coh.bbc_hid3 = seg_sounds.bbc_hid3
  AND lower(seg_sounds.product) = 'sounds'
  AND coh.lastweekstart = date_trunc('week', seg_sounds.date_of_segmentation)

  --news
  LEFT JOIN central_insights.sg10026_info_individual_alltime seg_news
  ON coh.bbc_hid3 = seg_news.bbc_hid3
  AND lower(seg_news.product) = 'news'
  AND coh.lastweekstart = date_trunc('week', seg_news.date_of_segmentation)

  --sport
  LEFT JOIN central_insights.sg10026_info_individual_alltime seg_sport
  ON coh.bbc_hid3 = seg_sport.bbc_hid3
  AND lower(seg_sport.product) = 'sport'
  AND coh.lastweekstart = date_trunc('week', seg_sport.date_of_segmentation)

  --cbbc
  LEFT JOIN central_insights.sg10026_info_individual_alltime seg_cbbc
  ON coh.bbc_hid3 = seg_cbbc.bbc_hid3
  AND lower(seg_cbbc.product) = 'cbbc'
  AND coh.lastweekstart = date_trunc('week', seg_cbbc.date_of_segmentation)

  --cbeebies
  LEFT JOIN central_insights.sg10026_info_individual_alltime seg_cbeebies
  ON coh.bbc_hid3 = seg_cbeebies.bbc_hid3
  AND lower(seg_cbeebies.product) = 'cbeebies'
  AND coh.lastweekstart = date_trunc('week', seg_cbeebies.date_of_segmentation)

  --weather
  LEFT JOIN central_insights.sg10026_info_individual_alltime seg_weather
  ON coh.bbc_hid3 = seg_weather.bbc_hid3
  AND lower(seg_weather.product) = 'weather'
  AND coh.lastweekstart = date_trunc('week', seg_weather.date_of_segmentation)

  --PanBBC
  LEFT JOIN central_insights.sg10026_info_individual_alltime seg_panbbc
  ON coh.bbc_hid3 = seg_panbbc.bbc_hid3
  AND lower(seg_panbbc.product) = 'panbbc'
  AND coh.lastweekstart = date_trunc('week', seg_panbbc.date_of_segmentation)
;
GRANT ALL ON central_insights_sandbox.ap_churn_last_seg TO GROUP central_insights;






