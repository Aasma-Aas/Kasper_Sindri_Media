{% set site = var('site') %}
{% set event_name  = var('event_action') %}
{% set db_name = var('db_name_default') %}
{% set tag_filter = var('tag_filter') %}
{% set status_ = var('tag_filter')['status']%}
{% set query_tag = var('tag_filter')['query_tag']%}

CREATE {{db_name}}  PROCEDURE `prod`.`Tactical_clicks_ytd_dbt_{{ site }}`()
BEGIN

WITH 
curr_30_days_article_published AS (
    SELECT siteid, id
    FROM prod.site_archive_post
    WHERE date BETWEEN MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1)  and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY)
    
    {% if status_ == 'yes' %}
  AND {{ query_tag }}
{% endif %}
AND siteid = {{ site }}
),

agg_curr_30_days_events AS (
    SELECT 
        e.siteid AS siteid, 
        SUM(e.hits) AS next_clicks
    FROM prod.events e
    JOIN curr_30_days_article_published a ON a.id = e.postid
    WHERE e.date between MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1)  and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY)
    AND e.siteid = {{ site }}
    AND e.Event_Action = '{{event_name}}'
    GROUP BY 1
),

agg_curr_30_days_pages AS
(
	select
		p.siteid,
        sum(p.unique_pageviews) as pageview_sum
	from prod.pages p
    left join curr_30_days_article_published a ON a.id = p.postid
    where p.siteid = {{ site }}
    and p.date between MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1)  and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY)
),

value_curr_30_days AS
(
	Select
		e.siteid,
		round(e.next_clicks/p.pageview_sum * 100,2) as value
	from agg_curr_30_days_events e
	left join agg_curr_30_days_pages p on e.siteid = p.siteid
),

last_30_days_article_published AS (
    SELECT siteid, id
    FROM prod.site_archive_post
    WHERE date between DATE_SUB(MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1), INTERVAL 1 Year)  and  
					   DATE_SUB(DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY),INTERVAL 1 Year) 
    {% if status_ == 'yes' %}
  AND {{ query_tag }}
{% endif %}
AND siteid = {{ site }} 
),

agg_last_30_days_events AS (
    SELECT 
        e.siteid AS siteid, 
        SUM(e.hits) AS next_clicks_last
    FROM prod.events e
    JOIN last_30_days_article_published a ON a.id = e.postid
    WHERE e.date between DATE_SUB(MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1), INTERVAL 1 Year)  and  
						 DATE_SUB(DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY),INTERVAL 1 Year) 
    AND e.siteid = {{ site }}
    AND e.Event_Action = '{{event_name}}'
    GROUP BY 1
),

agg_last_30_days_pages AS
(
	select
		p.siteid,
        sum(p.unique_pageviews) as pageview_sum_last
	from prod.pages p
    left join last_30_days_article_published a ON a.id = p.postid
    where p.siteid = {{ site }}
    and p.date between DATE_SUB(MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1), INTERVAL 1 Year)  and  
					   DATE_SUB(DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY),INTERVAL 1 Year) 
),

value_last_30_days AS
(
	Select
		e.siteid,
		round(e.next_clicks_last/p.pageview_sum_last * 100,2) as value_last
        from agg_last_30_days_events e
        left join agg_last_30_days_pages p on e.siteid = p.siteid
)

SELECT 
    JSON_OBJECT(
        'site', al.siteid,
        'data', JSON_OBJECT(
        'label', 'Next click (%)',
        'hint', 'Gns. next click år til dato ift. sidste år',
        'value', COALESCE(value, 0),
        'change', COALESCE(value - value_last, 0),
        'progressCurrent', '',
        'progressTotal', ''
        )
    ) AS json_data
FROM value_curr_30_days al
LEFT JOIN value_last_30_days alb ON al.siteid = alb.siteid
WHERE al.siteid = {{ site }};

END


       







