
  create view `site_dev`.`tactical_category_clicks_chart_ytd__dbt_tmp`
    
    
  as (
    





CREATE DEFINER=`Site`@`%` PROCEDURE `prod`.`tactical_category_clicks_chart_ytd_dbt_15`()
BEGIN

with last_ytd_days_article_published as(
SELECT siteid as siteid,id, date as fdate   FROM prod.site_archive_post
where 
date between MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1)  and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY) 

and siteid = 15
),
 last_ytd_days_article_published_Agg as(
select siteid as siteid,count(*) as agg from  last_ytd_days_article_published
where  siteid = 15
group by 1
),
cta_per_article as
(
select  a.siteid as siteid  ,a.id as postid ,coalesce(sum(e.unique_pageviews),0) as val from last_ytd_days_article_published a  
left join prod.pages e  on a.id=e.postid and a.siteid =  e.siteid
where e.siteid = 15
group by 1,2
),
 agg_cta_per_article as(
select siteid as siteid,sum(val) as agg_sum from  cta_per_article
where  siteid = 15
group by 1
),
agg_data as
(
select
postid,val,Min_pageviews,
percent_rank() over (ORDER BY case when coalesce (val,0)>=Min_pageviews then val-Min_pageviews else 0 end) as percentile
,case when coalesce (val,0)>=Min_pageviews then coalesce (val,0)-Min_pageviews else 0 end greater_than_target
,case when coalesce (val,0)<Min_pageviews then 1 else 0 end as less_than_target
,percent_rank() over (ORDER BY case when coalesce (val,0)>=Min_pageviews then coalesce (val,0) else 0 end) as percentile_click
,case when coalesce (val,0)>=Min_pageviews then coalesce (val,0) else 0 end greater_than_target_click
,case when coalesce (val,0)<Min_pageviews then coalesce (val,0) else 0 end as less_than_target_click
,l.siteid from last_ytd_days_article_published l
left join cta_per_article a on l.siteid = a.siteid and l.id = a.postid
left join prod.goals g on l.siteid = g.Site_ID and g.Date = l.fdate
where g.date  between MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1)  and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY)   and g.site_id =  15
),
data_out as
(
  select 
a.siteid as siteid,
'Artikler ift. sidevisningsmål' as label,
 'Artikler publiceret år til dato grupperet i tre kategorier: Artikler under sidevisningsmål | Artikler, der klarede mål | Top 10% bedste artikler ift. sidevisninger ,En søjle viser hvor stor andel af artiklerne, der falder i hver gruppe. En søjle viser hvor stor en andel af sidevisninger, der er skabt af den gruppe af artikler.' as hint,
'"Under mål" ,"Over mål" , "Top 10%"' as categories,
(sum(less_than_target)/agg)*100 as less_than_target,
(sum(case when percentile<=1 and percentile>=0.9 then 1 else 0 end)/agg)*100 as top_10,
(sum(case when percentile>=0 and percentile<0.9 and less_than_target=0 then 1 else 0 end)/agg)*100 as approved
,(sum(less_than_target_click)/agg_sum)*100 as less_than_target_click
,(sum(case when percentile>=0 and percentile<0.9 and less_than_target=0 then greater_than_target_click else 0 end)/agg_sum)*100 as approved_clicks
,(sum(case when percentile<=1 and percentile>=0.9 then val else 0 end)/agg_sum)*100 as top_10_clicks
from agg_data a
join last_ytd_days_article_published_Agg ag on ag.siteid=a.siteid
join agg_cta_per_article ap on ap.siteid=a.siteid
where  a.siteid = 15
group by 1,2,3,4
),
json_data as
(
select siteid, label as lab,hint as h,categories as cat,CONCAT('{"name": "Andel artikler", "data": [',cast(less_than_target as char),',',cast(approved as char),',',cast(top_10 as char),']},'
,'{"name": "Gns. next click for artikler." ,"data": [',cast(less_than_target_click as char),',',cast(approved_clicks as char),',',cast(top_10_clicks as char),']}') as series from data_out
)
  SELECT 
    CONCAT('{','"site":',siteid,',','"data":{"label":"', lab,
    '","hint":"', h,
    '","categories":','[',cat,'],',
    '"series":','[',series,']}}') as json_data
  FROM json_data;
  
END
  );