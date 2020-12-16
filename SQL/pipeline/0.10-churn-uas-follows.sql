/*
From https://confluence.dev.bbc.co.uk/pages/viewpage.action?pageId=194355766

Plays:

iPlayer send ‘Plays’ activities to UAS to monitor where a user has gotten to in their episode so that they can provide
the ‘Continue watching...’ functionality, as well as recommend the next episode to watch in the series. On the iPlayer
app there is a 'My Programmes' section which shows a user what they are currently 'Watching'.

Follows:

iPlayer send ‘Follows’ activities to UAS to determine what a user has 'Added’ to their 'My Programmes' –  although this
is available on individual episode pages it follows at the brand level. My Programmes always displays the latest episode
in the series in the ‘Added’ section with a link to ‘View all episodes’ which will show all series and episodes
currently on iPlayer.

Feedback:

iPlayer send a 'Feedback' activity to UAS when a user chooses to 'Remove' a programme from their ‘Added’ section.
 */

/*
###########
UAS FOLLOWS
###########
*/
DROP TABLE IF EXISTS central_insights_sandbox.tp_churn_uas_follows_reduced;
CREATE TABLE central_insights_sandbox.tp_churn_uas_follows_reduced
  distkey (bbc_hid3)
AS
SELECT
       hashedidentity as bbc_hid3,
       cal_yyyymmdd::date as follow_date,
       resourcetype,
       activitytype,
       resourcedomain
       FROM del.uas_profile_events uas
    WHERE uas.resourcetype in ('brand', 'genre', 'clip', 'artist', 'playlist', 'series', 'track', 'episode')
      and uas.activitytype in ('FOLLOWS', 'FAVOURITES')
      and uas.resourcedomain in ('tv', 'music', 'radio')
      and uas.cal_yyyymmdd::date >= (select min(mindate) from central_insights_sandbox.tp_churn_cohort_dates)
;
GRANT ALL ON central_insights_sandbox.tp_churn_uas_follows_reduced TO GROUP central_insights;


DROP TABLE IF EXISTS central_insights_sandbox.tp_churn_iplayer_my_progs;
CREATE TABLE central_insights_sandbox.tp_churn_iplayer_my_progs
  distkey (bbc_hid3)
AS
SELECT
       bbc_hid3,
       fresh,
       count(*) as iplayer_programme_follows_13w,
       sum(followed_last_week) as iplayer_programme_follows_lastweek
       FROM
  (
    SELECT uas.bbc_hid3,
           coh.fresh,
           follow_date,
           case when follow_date >= coh.lastweekstart then 1 else 0 end as followed_last_week
    FROM central_insights_sandbox.tp_churn_uas_follows_reduced uas
           INNER JOIN central_insights_sandbox.tp_churn_cohorts coh
                      ON uas.bbc_hid3 = coh.bbc_hid3
                        AND uas.follow_date >= coh.mindate
                        AND uas.follow_date <= coh.maxfeaturedate
    WHERE uas.resourcetype = 'brand'
      and uas.activitytype = 'FOLLOWS'
      and uas.resourcedomain = 'tv'
  ) preagg
GROUP BY 1, 2
;
GRANT ALL ON central_insights_sandbox.tp_churn_iplayer_my_progs TO GROUP central_insights;


DROP TABLE IF EXISTS central_insights_sandbox.tp_churn_sounds_subscribes;
CREATE TABLE central_insights_sandbox.tp_churn_sounds_subscribes
  distkey ( bbc_hid3 )
AS
SELECT
       bbc_hid3,
       fresh,
       count(*) as sounds_subscribes_13w,
       sum(followed_last_week) as sounds_subscribes_lastweek
       FROM
  (
    SELECT uas.bbc_hid3,
           coh.fresh,
           follow_date,
           case when follow_date >= coh.lastweekstart then 1 else 0 end as followed_last_week
    FROM central_insights_sandbox.tp_churn_uas_follows_reduced uas
           INNER JOIN central_insights_sandbox.tp_churn_cohorts coh
                      ON uas.bbc_hid3 = coh.bbc_hid3
                        AND uas.follow_date >= coh.mindate
                        AND uas.follow_date <= coh.maxfeaturedate
    WHERE uas.resourcetype in ('brand', 'genre', 'clip', 'artist', 'playlist', 'series')
      and uas.activitytype = 'FOLLOWS'
      and uas.resourcedomain in ('music', 'radio')
  ) preagg
GROUP BY 1, 2
;
GRANT ALL ON central_insights_sandbox.tp_churn_sounds_subscribes TO GROUP central_insights;



DROP TABLE IF EXISTS central_insights_sandbox.tp_churn_sounds_bookmarks;
CREATE TABLE central_insights_sandbox.tp_churn_sounds_bookmarks
  distkey ( bbc_hid3 )
AS
SELECT
       bbc_hid3,
       fresh,
       count(*) as sounds_bookmarks_13w,
       sum(followed_last_week) as sounds_bookmarks_lastweek
       FROM
  (
    SELECT uas.bbc_hid3,
           coh.fresh,
           follow_date,
           case when follow_date >= coh.lastweekstart then 1 else 0 end as followed_last_week
    FROM central_insights_sandbox.tp_churn_uas_follows_reduced uas
           INNER JOIN central_insights_sandbox.tp_churn_cohorts coh
                      ON uas.bbc_hid3 = coh.bbc_hid3
                        AND uas.follow_date >= coh.mindate
                        AND uas.follow_date <= coh.maxfeaturedate
    WHERE uas.resourcetype in ('track', 'clip', 'episode')
      and uas.activitytype = 'FAVOURITES'
      and uas.resourcedomain in ('music', 'radio')
  ) preagg
GROUP BY 1, 2
;
GRANT ALL ON central_insights_sandbox.tp_churn_sounds_bookmarks TO GROUP central_insights;

DROP TABLE IF EXISTS central_insights_sandbox.tp_churn_uas_follows_reduced;

/*
select count(*) from central_insights_sandbox.tp_churn_iplayer_my_progs;
--1,079,334
select count(*) from central_insights_sandbox.tp_churn_sounds_subscribes;
--873,980
select count(*) from central_insights_sandbox.tp_churn_sounds_bookmarks;
--608,899

select count(*) dupes from (
                             select bbc_hid3, count(*) n
                             from central_insights_sandbox.tp_churn_iplayer_my_progs
                             group by 1
                             having n > 1
                           ) duplicates
; --0

select count(*) dupes from (
                             select bbc_hid3, count(*) n
                             from central_insights_sandbox.tp_churn_sounds_subscribes
                             group by 1
                             having n > 1
                           ) duplicates
;--0

select count(*) dupes from (
                             select bbc_hid3, count(*) n
                             from central_insights_sandbox.tp_churn_sounds_bookmarks
                             group by 1
                             having n > 1
                           ) duplicates
;--0
*/