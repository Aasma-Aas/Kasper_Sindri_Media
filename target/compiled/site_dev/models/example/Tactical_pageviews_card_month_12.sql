






CREATE PROCEDURE `prod`.`Tactical_pageviews_card_month_12_dbt_test`()
BEGIN
with last_month_article_published as(
SELECT siteid as siteid,id   FROM prod.site_archive_post
where date between DATE_SUB(NOW(), INTERVAL 31 DAY) AND DATE_SUB(NOW(), INTERVAL 1 DAY) 
AND  categories <> "Nyhedsoverblik"
and siteid = 14
),
last_month_article_published_Agg as(
select siteid as siteid,count(*) as agg from  last_month_article_published
where  siteid = 14
group by 1
),
agg_last_month as (
select  e.siteid as siteid,sum(unique_pageviews)/agg as value from prod.pages e
join  last_month_article_published a on a.id=e.postid
join last_month_article_published_Agg agg on agg.siteid=a.siteid 
where e.date between DATE_SUB(NOW(), INTERVAL 31 DAY) AND DATE_SUB(NOW(), INTERVAL 1 DAY) and e.siteid = 14
group by 1,agg
),
last_month_before_article_published as(
SELECT siteid as siteid,id   FROM prod.site_archive_post
where DATE_SUB(NOW(), INTERVAL 61 DAY) AND DATE_SUB(NOW(), INTERVAL 31 DAY)  and siteid = 14
 AND  categories <> "Nyhedsoverblik"
),
last_month_before_article_published_Agg as(
select siteid as siteid,count(*) as agg from  last_month_before_article_published
where siteid = 14
group by 1
),
agg_last_month_before as(
select  e.siteid as siteid,sum(unique_pageviews)/agg as value from prod.pages e
join  last_month_before_article_published a on a.id=e.postid
join last_month_before_article_published_Agg agg on agg.siteid=a.siteid 
where DATE_SUB(NOW(), INTERVAL 61 DAY) AND DATE_SUB(NOW(), INTERVAL 31 DAY)  and e.siteid = 14
group by 1,agg
),
json_data as( 
select al.siteid as site, label_val label, hint_val  hint, al.value as valu,((al.value-alb.value)/al.value)*100 as chang, '' as progressCurrent , '' as progresstotal
from agg_last_month al
left join agg_last_month_before alb on al.siteid=alb.siteid
where al.siteid = 14
)
select 
    JSON_OBJECT(
            'site',14,
            'data',JSON_OBJECT(
            'label', 'Artikler',
            'hint', 'Artikler publiceret seneste 30 dage ift. forrige 30 dage',
            'value',COALESCE(valu,0),
            'change',COALESCE(chang,0),
            'progressCurrent',progressCurrent, 
            'progressTotal',progressTotal
 )) AS json_data  from json_data;
 
 END