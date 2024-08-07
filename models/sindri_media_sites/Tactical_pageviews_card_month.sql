{% set site = var('site') %}
{% set db_name = var('db_name_default') %}



CREATE {{db_name}}  PROCEDURE `prod`.`Tactical_pageviews_card_month_dbt_{{ site }}`()
BEGIN

with last_30_article_published as(
SELECT siteid as siteid,id   FROM prod.site_archive_post
where 
date  between DATE_SUB(NOW(), INTERVAL 30 DAY) and DATE_SUB(NOW(), INTERVAL 1 DAY) 
and siteid = {{site}}
),
 last_30_article_published_Agg as
 (
select siteid as siteid,count(*) as agg from  last_30_article_published
where  siteid = {{site}}
group by 1
),
agg_last_30 as (
select  e.siteid as siteid,sum(e.unique_pageviews)/agg as value from prod.pages e
join  last_30_article_published a on a.id=e.postid
join last_30_article_published_Agg agg on agg.siteid=a.siteid 
where date  between DATE_SUB(NOW(), INTERVAL 30 DAY) and DATE_SUB(NOW(), INTERVAL 1 DAY)
and  e.siteid = {{site}}
group by 1,agg
),
last_30_before_article_published as(
SELECT siteid as siteid,id   FROM prod.site_archive_post
where 
date  between DATE_SUB(NOW(), INTERVAL 60 DAY) and DATE_SUB(NOW(), INTERVAL 30 DAY) 
and siteid = {{site}} 

),
last_30_before_article_published_Agg as(
select siteid as siteid,count(*) as agg from  last_30_before_article_published
where  siteid = {{site}}
group by 1
),
agg_last_30_before as(
select  e.siteid as siteid,sum(unique_pageviews)/agg as value from prod.pages e
join  last_30_before_article_published a on a.id=e.postid
join last_30_before_article_published_Agg agg on agg.siteid=a.siteid 
where date  between DATE_SUB(NOW(), INTERVAL 60 DAY) and DATE_SUB(NOW(), INTERVAL 30 DAY) and e.siteid = {{site}}
group by 1,agg
),
json_data as( 
select al.siteid as siteid, 'Besøg' as label, 'Besøg år til dato ift sidste år til dato og ift målsætning' as  hint, al.value as valu,((al.value-alb.value)/al.value)*100 as chang, '' as progressCurrent , '' as progresstotal
from agg_last_30 al
left join agg_last_30_before alb on al.siteid=alb.siteid
where  al.siteid = {{site}}
)
select JSON_OBJECT('site',siteid,'data',JSON_OBJECT(
    'label', label,
    'hint', hint,
    'value',COALESCE(valu,0),'change',COALESCE(chang,0),'progressCurrent',progressCurrent, 'progressTotal',progressTotal
 )) AS json_data  from json_data;
 
 END



        







