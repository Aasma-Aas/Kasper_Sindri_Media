CREATE DEFINER=`Site`@`%` PROCEDURE `tactical_articles_card_month`(
IN site INT, IN label_val VARCHAR(200), IN hint_val text
)
BEGIN
with last_30_day as
(
SELECT siteId as siteId,count(date) as value FROM prod.site_archive_post
where 
date between DATE_SUB(NOW(), INTERVAL 31 DAY) and DATE_SUB(NOW(), INTERVAL 1 DAY) and siteid =site
group by 1
),
last_30_days_before as 
(
SELECT siteId as siteId,count(*) value  FROM prod.site_archive_post
where 
date between DATE_SUB(NOW(), INTERVAL 60 DAY) and DATE_SUB(NOW(), INTERVAL 30 DAY) and siteid =site
group by 1
)
select 
JSON_OBJECT(
'site',ld.siteId,'data',
JSON_OBJECT('label', label_val, 'hint',hint_val,'value',
COALESCE(ld.value,0),
'change',COALESCE(round(((ld.value-lb.value)/lb.value)*100,2),0), 
'progressCurrent','','progressTotal',''))AS json_data
from last_30_day ld
left join last_30_days_before lb
on ld.siteId=lb.siteId
;

END