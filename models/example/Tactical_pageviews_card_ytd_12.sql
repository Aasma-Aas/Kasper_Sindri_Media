{% set site = var('site') %}
{% set yearly = var('date_year') %}
{% set yearly_before = var('date_year_before') %}
{% set tactical_articles_card = var('tendency')['tactical_articles_card'] %}
{% set label_val = tactical_articles_card['label'] %}
{% set hint_val = tactical_articles_card['hint'] %}

CREATE PROCEDURE `prod`.`Tactical_pageviews_card_ytd_12_dbt_test`()
BEGIN
with last_ytd_article_published as(
SELECT siteid as siteid,id   FROM prod.site_archive_post
where date between {{yearly}} 
AND  categories <> "Nyhedsoverblik"
and siteid = {{site}}
),
last_ytd_article_published_Agg as(
select siteid as siteid,count(*) as agg from  last_ytd_article_published
where  siteid = {{site}}
group by 1
),
agg_last_ytd as (
select  e.siteid as siteid,sum(unique_pageviews)/agg as value from prod.pages e
join  last_ytd_article_published a on a.id=e.postid
join last_ytd_article_published_Agg agg on agg.siteid=a.siteid 
where e.date between {{yearly}} and e.siteid = {{site}}
group by 1,agg
),
last_ytd_before_article_published as(
SELECT siteid as siteid,id   FROM prod.site_archive_post
where {{yearly_before}} and siteid = {{site}}
 AND  categories <> "Nyhedsoverblik"
),
last_ytd_before_article_published_Agg as(
select siteid as siteid,count(*) as agg from  last_ytd_before_article_published
where siteid = {{site}}
group by 1
),
agg_last_ytd_before as(
select  e.siteid as siteid,sum(unique_pageviews)/agg as value from prod.pages e
join  last_ytd_before_article_published a on a.id=e.postid
join last_ytd_before_article_published_Agg agg on agg.siteid=a.siteid 
where {{yearly_before}} and e.siteid = {{site}}
group by 1,agg
),
json_data as( 
select al.siteid as site, label_val label, hint_val  hint, al.value as valu,((al.value-alb.value)/al.value)*100 as chang, '' as progressCurrent , '' as progresstotal
from agg_last_ytd al
left join agg_last_ytd_before alb on al.siteid=alb.siteid
where al.siteid = {{site}}
)
select 
    JSON_OBJECT(
            'site',{{site}},
            'data',JSON_OBJECT(
            'label', '{{ label_val }}',
            'hint', '{{ hint_val }}',
            'value',COALESCE(valu,0),
            'change',COALESCE(chang,0),
            'progressCurrent',progressCurrent, 
            'progressTotal',progressTotal
 )) AS json_data  from json_data;
 
 END