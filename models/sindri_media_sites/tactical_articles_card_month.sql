{% set site = var('site') %}
{% set db_name = var('db_name_default') %}
{% set tag_filter = var('tag_filter') %}
{% set status_ = var('tag_filter')['status']%}
{% set query_tag = var('tag_filter')['query_tag']%}


CREATE {{db_name}} PROCEDURE `prod`.`tactical_articles_card_month_dbt_{{ site }}`()
BEGIN
with last_30_day as
(
SELECT siteId as siteId,count(date) as value FROM prod.site_archive_post
where 
date between DATE_SUB(NOW(), INTERVAL 31 DAY) and DATE_SUB(NOW(), INTERVAL 1 DAY) 
{% if status_ == 'yes' %}
  AND {{ query_tag }}
{% endif %}
and siteid = {{site}}
group by 1
),
last_30_days_before as 
(
SELECT siteId as siteId,count(*) value  FROM prod.site_archive_post
where 
date between DATE_SUB(NOW(), INTERVAL 60 DAY) and DATE_SUB(NOW(), INTERVAL 30 DAY) 
{% if status_ == 'yes' %}
  AND {{ query_tag }}
{% endif %}
and siteid = {{site}}
group by 1
)
select 
JSON_OBJECT(
'site',ld.siteId,'data',
JSON_OBJECT(
    'label', 'Artikler',
    'hint', 'Artikler publiceret seneste 30 dage ift. forrige 30 dage','value',
COALESCE(ld.value,0),
'change',COALESCE(round(((ld.value-lb.value)/lb.value)*100,2),0), 
'progressCurrent','','progressTotal',''))AS json_data
from last_30_day ld
left join last_30_days_before lb
on ld.siteId=lb.siteId
;

END