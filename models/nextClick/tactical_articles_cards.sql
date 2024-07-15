{% for i in range(cards_count | length) %}
CREATE DEFINER=`Site`@`%` PROCEDURE `tactical_articles_card{{cards_count[0]}}`(
  IN site INT, IN label_val VARCHAR(200), IN hint_val text
)
BEGIN
  DECLARE sql_query TEXT; -- Declare variable to hold dynamic SQL query

  SET @sql_query := CONCAT(
    '(
    WITH last_30_day AS (
      SELECT siteId AS siteId, count(date) AS value FROM prod.site_archive_post
      WHERE {{ date_query[0] }}  AND siteid = {{ site_id }} 
      GROUP BY 1
    ),
    last_30_days_before AS (
      SELECT siteId AS siteId, count(*) value FROM prod.site_archive_post
     WHERE {{ date_query[0] }}  AND siteid = {{ site_id }} 
      GROUP BY 1
    )
    select 
JSON_OBJECT(
'site',ld.siteId,'data',
JSON_OBJECT('label', {{tendency_cards_label}} , 'hint', {{tendency_cards_hint}}, 
'value', COALESCE(ld.value,0),
'change',COALESCE(round(((ld.value-lb.value)/lb.value)*100,2),0), 
'progressCurrent','','progressTotal',''))AS json_data
from last_30_day ld
left join last_30_days_before lb
on ld.siteId=lb.siteId
    )');
     {% endfor %}
  PREPARE stmt FROM @sql_query;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;
END;
