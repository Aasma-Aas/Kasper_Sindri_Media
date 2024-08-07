{% set site = var('site') %}
{% set event_name  = var('event_action') %}
{% set table_json  = var('json') %}
{% set db_name = var('db_name_default') %}
{% set tag_filter = var('tag_filter') %}
{% set status_ = var('tag_filter')['status']%}
{% set query_tag = var('tag_filter')['query_tag']%}

CREATE {{db_name}} PROCEDURE `prod`.`tactical_articles_table_ytd_dbt_{{ site }}`()
BEGIN

with last_ytd_days_article_published as(
SELECT siteid as siteid,id,title as article,userneeds as category,COALESCE(tags, '') AS tags,COALESCE(Categories, '') AS sektion,link as url, DATE(Modified) as  updated,
date as date  FROM prod.site_archive_post
where 
date between MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1)  and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY)
{% if status_ == 'yes' %}
  AND {{ query_tag }}
{% endif %}
and siteid = {{site}} 


 ),
uniquepages as
(
select  e.siteid as siteid,e.postid,sum(unique_pageviews) as uniq_val from prod.pages e
join  last_ytd_days_article_published a on a.id=e.postid
where  e.siteid = {{site}}
group by 1,2
),
cta_per_article as
(
select  e.siteid as siteid,e.postid,sum(hits) as val_hits from prod.events e
left join  last_ytd_days_article_published a on a.id=e.postid
where Event_Action = '{{event_name}}' and  e.siteid = {{site}}
group by 1,2
)

select 
CONCAT('{"site":',  l.siteid,',"data":{','"columns": {{table_json}} , ','"rows":',JSON_ARRAYAGG(
           JSON_OBJECT(
               'id', id,
               'article', article,
               'category', l.category,
	           'tags',l.tags,
			   'sektion',l.sektion,
               'date', date,
		       'updated', updated,
               'url', url,
               'brugerbehov',coalesce(uniq_val,0),
                'clicks',ROUND(coalesce((coalesce(val_hits,0)/coalesce(uniq_val,0)) * 100,0),1)
           ))
       ,'}}') AS json_data

from last_ytd_days_article_published l
left join uniquepages up
on l.siteid = up.siteid and l.id = up.postid
left join cta_per_article ca
on l.siteid = ca.siteid and l.id = ca.postid 
where  l.siteid = {{site}};
	END