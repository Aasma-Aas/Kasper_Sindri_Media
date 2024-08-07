







CREATE DEFINER=`Site`@`%`  PROCEDURE `prod`.`Tactical_pageviews_card_month_dbt_14`()
BEGIN

with last_30_article_published as(
SELECT siteid as siteid,id   FROM prod.site_archive_post
where 
date  between DATE_SUB(NOW(), INTERVAL 30 DAY) and DATE_SUB(NOW(), INTERVAL 1 DAY) 

  AND  Categories <> 'Nyhedsoverblik' 

and siteid = 14
),
 last_30_article_published_Agg as
 (
select siteid as siteid,count(*) as agg from  last_30_article_published
where  siteid = 14
group by 1
),
agg_last_30 as (
select  e.siteid as siteid,sum(e.unique_pageviews)/agg as value from prod.pages e
join  last_30_article_published a on a.id=e.postid
join last_30_article_published_Agg agg on agg.siteid=a.siteid 
where date  between DATE_SUB(NOW(), INTERVAL 30 DAY) and DATE_SUB(NOW(), INTERVAL 1 DAY)
and  e.siteid = 14
group by 1,agg
),
last_30_before_article_published as(
SELECT siteid as siteid,id   FROM prod.site_archive_post
where 
date  between DATE_SUB(NOW(), INTERVAL 60 DAY) and DATE_SUB(NOW(), INTERVAL 30 DAY) 

  AND  Categories <> 'Nyhedsoverblik' 

and siteid = 14 

),
last_30_before_article_published_Agg as(
select siteid as siteid,count(*) as agg from  last_30_before_article_published
where  siteid = 14
group by 1
),
agg_last_30_before as(
select  e.siteid as siteid,sum(unique_pageviews)/agg as value from prod.pages e
join  last_30_before_article_published a on a.id=e.postid
join last_30_before_article_published_Agg agg on agg.siteid=a.siteid 
where date  between DATE_SUB(NOW(), INTERVAL 60 DAY) and DATE_SUB(NOW(), INTERVAL 30 DAY) and e.siteid = 14
group by 1,agg
),
json_data as( 
select al.siteid as siteid, 'Gns. sidevisninger pr artikel' as label, 'Gns. sidevisninger pr artikler publiceret seneste 30 dage ift. forrige 30 dage' as  hint, al.value as valu,((al.value-alb.value)/al.value)*100 as chang, '' as progressCurrent , '' as progresstotal
from agg_last_30 al
left join agg_last_30_before alb on al.siteid=alb.siteid
where  al.siteid = 14
)
select JSON_OBJECT('site',siteid,'data',JSON_OBJECT(
    'label', label,
    'hint', hint,
    'value',COALESCE(valu,0),'change',COALESCE(chang,0),'progressCurrent',progressCurrent, 'progressTotal',progressTotal
 )) AS json_data  from json_data;
 
 END