






CREATE DEFINER=`Site`@`%` PROCEDURE `prod`.`tactical_userneeds_clicks_chart_month_dbt_14`()
BEGIN
with last_ytd_article_published as(
SELECT siteid as siteid,id, date as fdate   FROM prod.site_archive_post
where 
date
between date_sub(current_date(),interval 1 month)  and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY)  

  AND  Categories <> 'Nyhedsoverblik' 

and siteid = 14
),
 last_ytd_article_published_Agg as(
select siteid as siteid,count(*) as agg from  last_ytd_article_published
where   siteid = 14
group by 1
),
cta_per_article as
(
select  e.siteid as siteid,e.postid,sum(hits) as val from prod.events e
left join  last_ytd_article_published a on a.id=e.postid
where Event_Action = 'Next Click'  and e.siteid = 14
group by 1,2
),
 agg_cta_per_article as(
select siteid as siteid,sum(val) as agg_sum from  cta_per_article
where   siteid = 14
group by 1
),
agg_data as(
select 
l.id,
a.val,min_cta,
percent_rank() over (ORDER BY case when val>=min_cta then val-min_cta else 0 end) as percentile
,case when coalesce (val,0)>=min_cta then val-min_cta else 0 end greater_than_target
,case when coalesce (val,0)<min_cta then 1 else 0 end as less_than_target
,percent_rank() over (ORDER BY case when coalesce (val,0)>=min_cta then coalesce (val,0) else 0 end) as percentile_click
,case when coalesce(val,0)>=min_cta then coalesce (val,0) else 0 end greater_than_target_click
,case when coalesce (val,0)<min_cta then coalesce (val,0) else 0 end as less_than_target_click
,l.siteid from last_ytd_article_published l
 left join cta_per_article a on l.siteid = a.siteid and l.id = a.postid
 left join prod.goals g on l.siteid = g.Site_ID and g.Date = l.fdate
 where g.date
between date_sub(current_date(),interval 1 month)  and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY)  and g.site_id = 14
),
data_out as
(
select 
a.siteid,
 'Artikler ift. next click mål' as label,
 'Artikler grupperet ift hvordan de har performet på next click.' as hint,
'"Under mål" , "Godkendt" , "Top 10%"' as categories,
(sum(less_than_target)/agg)*100 as less_than_target,
(sum(case when percentile<=1 and percentile>=0.9 then 1 else 0 end)/agg)*100 as top_10,
(sum(case when percentile>=0 and percentile<0.9 and less_than_target=0 then 1 else 0 end)/agg)*100 as approved
,(sum(less_than_target_click)/agg_sum)*100 as less_than_target_click
,(sum(case when percentile>=0 and percentile<0.9 and less_than_target=0 then greater_than_target_click else 0 end)/agg_sum)*100 as approved_clicks
,(sum(case when percentile<=1 and percentile>=0.9 then val else 0 end)/agg_sum)*100 as top_10_clicks
from agg_data a
join last_ytd_article_published_Agg ag on ag.siteid=a.siteid
join agg_cta_per_article ap on ap.siteid=a.siteid
where   a.siteid = 14
group by 1,2,3,4
),
json_data as
(
select siteid as siteid,label as lab ,hint as ht,categories as cat,CONCAT('{"name": "Andel artikler", "data": [',cast(less_than_target as char),',',cast(approved as char),',',cast(top_10 as char),']}'
,',{"name": "Gns. next click for artikler.", "data": [',cast(less_than_target_click as char),',',cast(approved_clicks as char),',',cast(top_10_clicks as char),']}') as series from data_out
)
    SELECT 
    CONCAT('{','"site":',siteid,',','"data":{"label":"', lab,
    '","hint":"', ht,
    '","categories":'
    ,'[',cat,'],',
    '"series":','[',series,']}}') as json_data
  FROM json_data;
          END