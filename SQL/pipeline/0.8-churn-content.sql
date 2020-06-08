--Metadata
/*
Need to unpick the mess that is episode-level metadata in SCV VMB. Creating a table to de-duplicate episode IDs for
genres and master brands.
*/

-- Sorting out content_ids first so we can match to Dan's itemised matrix
DROP TABLE IF EXISTS central_insights_sandbox.tp_churn_episode_content_ids;
CREATE TABLE central_insights_sandbox.tp_churn_episode_content_ids
  distkey (content_id)
  AS
SELECT DISTINCT episode_id,
                content_id
                FROM (
                SELECT episode_id,
                       case
                         when brand_id = 'null'
                           then series_id
                         else brand_id end as content_id
                FROM prez.scv_vmb
              ) map
where episode_id != 'null'
;
GRANT ALL ON central_insights_sandbox.tp_churn_episode_content_ids TO GROUP central_insights;

-- On to selecting genres and master brands based on frequency
DROP TABLE IF EXISTS central_insights_sandbox.tp_churn_content_genres_masterbrands;
CREATE TABLE central_insights_sandbox.tp_churn_content_genres_masterbrands
  AS
SELECT
--   g.episode_id,
  case when g.content_id = 'null' then NULL else g.content_id end as content_id,
  g.genre,
  mb.masterbrand,
  case when mb.masterbrand in
                        (
                        'BBC One',
                        'BBC Two',
                        'BBC Three',
                        'BBC Four',
                        'CBBC',
                        'CBeebies',
--                         'BBC Parliament',
--                         'BBC News Channel',
--                         'BBC Scotland',
                        'BBC World Service',
--                         'S4C',
--                         'BBC Asian Network',
                        'BBC Radio 1',
                        'BBC Radio 1Xtra',
                        'BBC Radio 2',
                        'BBC Radio 3',
                        'BBC Radio 4',
                        'BBC Radio 5 live',
                        'BBC Radio 5 live sports extra',
                        'BBC Radio 6 Music'
                ) then mb.masterbrand
              when lower(mb.masterbrand) like '%radio%' then 'Other Radio'
              else 'Other'
              end as master_brand_simplified
FROM
       (
    SELECT --episode_id,
           content_id,
           genre
    FROM (
         SELECT --episode_id,
                content_id,
                genre,
                ROW_NUMBER() OVER (
                  PARTITION BY content_id--episode_id
                  ORDER BY n DESC
                  ) AS rn
      FROM (
           SELECT vmb.episode_id,
                  content_id,
                  split_part(pips_genre_level_1_names, ';', 1) as genre,
                  count(*) as n
            FROM prez.scv_vmb vmb
           LEFT JOIN central_insights_sandbox.tp_churn_episode_content_ids c_ids
                ON vmb.episode_id = c_ids.episode_id
          GROUP BY 1,2,3
             ) g_n
           ) g_dedupe
    WHERE rn=1
    ) g
INNER JOIN
  (
    SELECT --episode_id,
           content_id,
           masterbrand
    FROM (
         SELECT --episode_id,
                content_id,
                masterbrand,
                ROW_NUMBER() OVER (
                  PARTITION BY content_id--episode_id
                  ORDER BY n DESC
                  ) AS rn
      FROM (
           SELECT vmb.episode_id,
                  content_id,
                  master_brand_name as masterbrand,
                  count(*) as n
            FROM prez.scv_vmb vmb
           LEFT JOIN central_insights_sandbox.tp_churn_episode_content_ids c_ids
                ON vmb.episode_id = c_ids.episode_id
            GROUP BY 1,2,3
             ) mb_n
           ) mb_dedupe
    WHERE rn=1
    ) mb
--ON g.episode_id = mb.episode_id
ON g.content_id = mb.content_id
-- LEFT JOIN central_insights_sandbox.tp_churn_episode_content_ids c_ids
-- ON g.episode_id = c_ids.episode_id
-- LEFT JOIN  central_insights_sandbox.dh_sounds_item_matrix_enriched item_matrix
-- on c_ids.content_id = item_matrix.content_id
;
GRANT ALL ON central_insights_sandbox.tp_churn_content_genres_masterbrands TO GROUP central_insights;

-- select count(*) from prez.scv_vmb;
--5.9m


/*
###########################
ACTIVATING USERS:

Looking back to the first piece of content that a user engaged with, following a 13week period of inactivity.

Activations are flagged using the Sounds and iPlayer activations tables provided by Aileen Wang and Josh Feldman
respectively.

Content is described with genre, master brands (aggregated to remove local radio) and also using the item matrices
provided by Dan Hill's recommender system to represent specific content in a numerical space.

###########################
*/


-- SOUNDS
DROP TABLE IF EXISTS central_insights_sandbox.tp_churn_sounds_activating_users;
CREATE TABLE central_insights_sandbox.tp_churn_sounds_activating_users
  distkey(bbc_hid3)
 AS
SELECT a.bbc_hid3,
       a.fresh,
       a.activation_week,
       a.activation_episode_id,
       c_ids.content_id as activation_content_id,
       item_matrix.f0 as sounds_activ_f0,
       item_matrix.f1 as sounds_activ_f1,
       item_matrix.f2 as sounds_activ_f2,
       item_matrix.f3 as sounds_activ_f3,
       item_matrix.f4 as sounds_activ_f4,
       item_matrix.f5 as sounds_activ_f5,
       item_matrix.f6 as sounds_activ_f6,
       item_matrix.f7 as sounds_activ_f7,
       item_matrix.f8 as sounds_activ_f8,
       item_matrix.f9 as sounds_activ_f9,
       item_matrix.f10 as sounds_activ_f10,
       item_matrix.f11 as sounds_activ_f11,
       item_matrix.f12 as sounds_activ_f12,
       item_matrix.f13 as sounds_activ_f13,
       item_matrix.f14 as sounds_activ_f14,
       item_matrix.f15 as sounds_activ_f15,
       item_matrix.f16 as sounds_activ_f16,
       item_matrix.f17 as sounds_activ_f17,
       item_matrix.f18 as sounds_activ_f18,
       item_matrix.f19 as sounds_activ_f19,
       datediff('week', a.activation_week, a.lastweekstart) + 1 as sounds_weeks_since_activation,
       b.genre as sounds_activating_genre,
       b.masterbrand,
       b.master_brand_simplified as sounds_activating_brand
FROM (
       SELECT DISTINCT coh.bbc_hid3,
                       coh.fresh,
                       --       coh.cohort,
                       --       coh.mindate,
                       --       coh.maxdate,
                       --       coh.maxfeaturedate,
                       coh.lastweekstart,
                       --       act.week_beginning as activation_week_,
                       first_value(act.week_beginning) over (
                         partition by coh.bbc_hid3
                         order by week_beginning desc
                         rows between unbounded preceding and unbounded following
                         ) as activation_week,
                       first_value(act.episode_id) over (
                         partition by coh.bbc_hid3 order by week_beginning desc
                         rows between unbounded preceding and unbounded following
                         ) as activation_episode_id
                       --       datediff('week', act.week_beginning, coh.lastweekstart) + 1 as weeks_since_activation
       FROM central_insights_sandbox.tp_churn_cohorts coh
              LEFT JOIN radio1_sandbox.sounds_activations_preagg act
                        ON coh.bbc_hid3 = act.bbc_hid3
                          AND act.week_beginning <= coh.lastweekstart
     ) a
LEFT JOIN central_insights_sandbox.tp_churn_episode_content_ids c_ids
  ON a.activation_episode_id = c_ids.episode_id
LEFT JOIN central_insights_sandbox.tp_churn_content_genres_masterbrands b
  ON c_ids.content_id = b.content_id
LEFT JOIN  central_insights_sandbox.dh_sounds_item_matrix_enriched item_matrix
  on b.content_id = item_matrix.content_id
;
GRANT ALL ON central_insights_sandbox.tp_churn_sounds_activating_users TO GROUP central_insights;
--25,155,402


DROP TABLE IF EXISTS central_insights_sandbox.tp_churn_iplayer_activating_users;
CREATE TABLE central_insights_sandbox.tp_churn_iplayer_activating_users
  distkey(bbc_hid3)
  AS
SELECT a.bbc_hid3,
       a.fresh,
       a.activation_week,
       vmb.episode_id as activation_episode_id,
       c_ids.content_id as activation_content_id,
       item_matrix.f0 as iplayer_activ_f0,
       item_matrix.f1 as iplayer_activ_f1,
       item_matrix.f2 as iplayer_activ_f2,
       item_matrix.f3 as iplayer_activ_f3,
       item_matrix.f4 as iplayer_activ_f4,
       item_matrix.f5 as iplayer_activ_f5,
       item_matrix.f6 as iplayer_activ_f6,
       item_matrix.f7 as iplayer_activ_f7,
       item_matrix.f8 as iplayer_activ_f8,
       item_matrix.f9 as iplayer_activ_f9,
       datediff('week', a.activation_week, a.lastweekstart) + 1 as iplayer_weeks_since_activation,
       b.genre as iplayer_activating_genre,
       b.masterbrand,
       b.master_brand_simplified as iplayer_activating_brand
FROM (
     SELECT DISTINCT coh.bbc_hid3,
                     coh.fresh,
                     coh.lastweekstart,
                     first_value(act.week_beginning) over (
                       partition by coh.bbc_hid3
                       order by week_beginning desc
                       rows between unbounded preceding and unbounded following
                       ) as activation_week,
                     first_value(act.first_version_id) over (
                       partition by coh.bbc_hid3 order by week_beginning desc
                       rows between unbounded preceding and unbounded following
                       ) as activation_version_id
      FROM central_insights_sandbox.tp_churn_cohorts coh
              LEFT JOIN central_insights_sandbox.iplayer_activations_postmigration_preagg act
                    ON coh.bbc_hid3 = act.audience_id
                    AND act.week_beginning <= coh.lastweekstart
       ) a
LEFT JOIN prez.scv_vmb vmb
  ON a.activation_version_id = vmb.version_id
LEFT JOIN central_insights_sandbox.tp_churn_episode_content_ids c_ids
  ON vmb.episode_id = c_ids.episode_id
LEFT JOIN central_insights_sandbox.tp_churn_content_genres_masterbrands b
  ON c_ids.content_id = b.content_id
LEFT JOIN  central_insights_sandbox.dh_iplayer_item_matrix_enriched item_matrix
  on b.content_id = item_matrix.content_id
;
GRANT ALL ON central_insights_sandbox.tp_churn_iplayer_activating_users TO GROUP central_insights;

/*
###########################
FAVOURITE CONTENT:

The most frequent content engaged with by a user over the last 13 weeks. Content here refers to a brand, or series if
no brand is specified, or episode if neither are specified in the VMB.

Content is described with genre, master brands (aggregated to remove local radio) and also using the item matrices
provided by Dan Hill's recommender system to represent specific content in a numerical space.

###########################
*/

-- User data

DROP TABLE IF EXISTS central_insights_sandbox.tp_churn_favourite_content_raw;
CREATE TABLE central_insights_sandbox.tp_churn_favourite_content_raw AS
SELECT audience_id                   as bbc_hid3,
       destination,
       coh.fresh,
       case when brand_id is not null and brand_id != 'null' then brand_id
            when series_id is not null and series_id != 'null' then series_id
            else episode_id end as content_id,
       case when brand_id is not null and brand_id != 'null' then 'brand_id'
            when series_id is not null and series_id != 'null' then 'series_id'
            else 'episode_id' end as content_id_category,
       sum(playback_time_total)     as streaming_time,
       count(distinct date_of_event) as events
FROM audience.audience_activity_daily_summary_enriched aud
       INNER JOIN central_insights_sandbox.tp_churn_cohorts coh
                  ON audience_id = coh.bbc_hid3
                    AND aud.date_of_event >= coh.mindate
                    AND aud.date_of_event <= coh.maxfeaturedate
WHERE destination in ('PS_IPLAYER', 'PS_SOUNDS')
  AND aud.playback_time_total >= 180
GROUP BY 1, 2, 3, 4, 5
;
GRANT ALL ON central_insights_sandbox.tp_churn_favourite_content_raw TO GROUP central_insights;


DROP TABLE IF EXISTS central_insights_sandbox.tp_churn_favourite_content_sounds;
CREATE TABLE central_insights_sandbox.tp_churn_favourite_content_sounds
  distkey(bbc_hid3)
  AS
SELECT
       fav.bbc_hid3,
       fav.fresh,
       -- fav.destination,
       fav.content_id as fav_content_id,
       fav.content_id_category as fav_content_id_category,
       cgmb.genre as fav_content_genre,
       cgmb.master_brand_simplified as fav_content_masterbrand,
       sounds_item_matrix.f0 as sounds_fav_f0,
       sounds_item_matrix.f1 as sounds_fav_f1,
       sounds_item_matrix.f2 as sounds_fav_f2,
       sounds_item_matrix.f3 as sounds_fav_f3,
       sounds_item_matrix.f4 as sounds_fav_f4,
       sounds_item_matrix.f5 as sounds_fav_f5,
       sounds_item_matrix.f6 as sounds_fav_f6,
       sounds_item_matrix.f7 as sounds_fav_f7,
       sounds_item_matrix.f8 as sounds_fav_f8,
       sounds_item_matrix.f9 as sounds_fav_f9,
       sounds_item_matrix.f10 as sounds_fav_f10,
       sounds_item_matrix.f11 as sounds_fav_f11,
       sounds_item_matrix.f12 as sounds_fav_f12,
       sounds_item_matrix.f13 as sounds_fav_f13,
       sounds_item_matrix.f14 as sounds_fav_f14,
       sounds_item_matrix.f15 as sounds_fav_f15,
       sounds_item_matrix.f16 as sounds_fav_f16,
       sounds_item_matrix.f17 as sounds_fav_f17,
       sounds_item_matrix.f18 as sounds_fav_f18,
       sounds_item_matrix.f19 as sounds_fav_f19
FROM
  (
    SELECT bbc_hid3,
           fresh,
           destination,
           content_id,
           content_id_category
    FROM (
           SELECT bbc_hid3,
                  fresh,
                  destination,
                  content_id,
                  content_id_category,
                  streaming_time,
                  events,
                  ROW_NUMBER() OVER (
                    PARTITION BY bbc_hid3
                    ORDER BY events desc
                    ) rn
           FROM central_insights_sandbox.tp_churn_favourite_content_raw
           WHERE destination = 'PS_SOUNDS'
         ) subs
    WHERE rn = 1
  ) fav
LEFT JOIN central_insights_sandbox.tp_churn_content_genres_masterbrands cgmb
  ON fav.content_id = cgmb.content_id
LEFT JOIN central_insights_sandbox.dh_sounds_item_matrix_enriched sounds_item_matrix
  ON fav.content_id = sounds_item_matrix.content_id
;
GRANT ALL ON central_insights_sandbox.tp_churn_favourite_content_sounds TO GROUP central_insights;


DROP TABLE IF EXISTS central_insights_sandbox.tp_churn_favourite_content_iplayer;
CREATE TABLE central_insights_sandbox.tp_churn_favourite_content_iplayer
  distkey(bbc_hid3)
  AS
SELECT
       fav.bbc_hid3,
       fav.fresh,
       -- fav.destination,
       fav.content_id as fav_content_id,
       fav.content_id_category as fav_content_id_category,
       cgmb.genre as fav_content_genre,
       cgmb.master_brand_simplified as fav_content_masterbrand,
       iplayer_item_matrix.f0 as iplayer_fav_f0,
       iplayer_item_matrix.f1 as iplayer_fav_f1,
       iplayer_item_matrix.f2 as iplayer_fav_f2,
       iplayer_item_matrix.f3 as iplayer_fav_f3,
       iplayer_item_matrix.f4 as iplayer_fav_f4,
       iplayer_item_matrix.f5 as iplayer_fav_f5,
       iplayer_item_matrix.f6 as iplayer_fav_f6,
       iplayer_item_matrix.f7 as iplayer_fav_f7,
       iplayer_item_matrix.f8 as iplayer_fav_f8,
       iplayer_item_matrix.f9 as iplayer_fav_f9
FROM
  (
    SELECT bbc_hid3,
           fresh,
           destination,
           content_id,
           content_id_category
    FROM (
           SELECT bbc_hid3,
                  fresh,
                  destination,
                  content_id,
                  content_id_category,
                  streaming_time,
                  events,
                  ROW_NUMBER() OVER (
                    PARTITION BY bbc_hid3
                    ORDER BY events desc
                    ) rn
           FROM central_insights_sandbox.tp_churn_favourite_content_raw
           WHERE destination = 'PS_IPLAYER'
                 and content_id is not null
         ) subs
    WHERE rn = 1
  ) fav
LEFT JOIN central_insights_sandbox.tp_churn_content_genres_masterbrands cgmb
  ON fav.content_id = cgmb.content_id
LEFT JOIN central_insights_sandbox.dh_iplayer_item_matrix_enriched iplayer_item_matrix
  ON fav.content_id = iplayer_item_matrix.content_id
;
GRANT ALL ON central_insights_sandbox.tp_churn_favourite_content_iplayer TO GROUP central_insights;

DROP TABLE IF EXISTS central_insights_sandbox.tp_churn_episode_content_ids;
DROP TABLE IF EXISTS central_insights_sandbox.tp_churn_content_genres_masterbrands;
DROP TABLE IF EXISTS central_insights_sandbox.tp_churn_favourite_content_raw;
