
CREATE DEFINER=`Site`@`%` PROCEDURE `tactical_articles_card_monthly`(
  IN site INT, IN label_val VARCHAR(200), IN hint_val text
)
BEGIN
  DECLARE sql_query TEXT; -- Declare variable to hold dynamic SQL query

  SET @sql_query := CONCAT(
    '(
    WITH last_30_day AS (
      SELECT siteId AS siteId, count(date) AS value FROM prod.site_archive_post
      WHERE date BETWEEN MAKEDATE(EXTRACT(YEAR FROM CURDATE()), 1) AND DATE_SUB(CURDATE(), INTERVAL 1 DAY)  AND siteid = 12 
      GROUP BY 1
    ),
    last_30_days_before AS (
      SELECT siteId AS siteId, count(*) value FROM prod.site_archive_post
     WHERE date BETWEEN MAKEDATE(EXTRACT(YEAR FROM CURDATE()), 1) AND DATE_SUB(CURDATE(), INTERVAL 1 DAY)  AND siteid = 12 
      GROUP BY 1
    )
    select 
JSON_OBJECT(
'site',ld.siteId,'data',
JSON_OBJECT('label', label data value , 'hint', hint data value, 
'value', COALESCE(ld.value,0),
'change',COALESCE(round(((ld.value-lb.value)/lb.value)*100,2),0), 
'progressCurrent','','progressTotal',''))AS json_data
from last_30_day ld
left join last_30_days_before lb
on ld.siteId=lb.siteId
    )');
     
CREATE DEFINER=`Site`@`%` PROCEDURE `tactical_articles_card_monthly`(
  IN site INT, IN label_val VARCHAR(200), IN hint_val text
)
BEGIN
  DECLARE sql_query TEXT; -- Declare variable to hold dynamic SQL query

  SET @sql_query := CONCAT(
    '(
    WITH last_30_day AS (
      SELECT siteId AS siteId, count(date) AS value FROM prod.site_archive_post
      WHERE date BETWEEN MAKEDATE(EXTRACT(YEAR FROM CURDATE()), 1) AND DATE_SUB(CURDATE(), INTERVAL 1 DAY)  AND siteid = 12 
      GROUP BY 1
    ),
    last_30_days_before AS (
      SELECT siteId AS siteId, count(*) value FROM prod.site_archive_post
     WHERE date BETWEEN MAKEDATE(EXTRACT(YEAR FROM CURDATE()), 1) AND DATE_SUB(CURDATE(), INTERVAL 1 DAY)  AND siteid = 12 
      GROUP BY 1
    )
    select 
JSON_OBJECT(
'site',ld.siteId,'data',
JSON_OBJECT('label', label data value , 'hint', hint data value, 
'value', COALESCE(ld.value,0),
'change',COALESCE(round(((ld.value-lb.value)/lb.value)*100,2),0), 
'progressCurrent','','progressTotal',''))AS json_data
from last_30_day ld
left join last_30_days_before lb
on ld.siteId=lb.siteId
    )');
     
  PREPARE stmt FROM @sql_query;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;
END;