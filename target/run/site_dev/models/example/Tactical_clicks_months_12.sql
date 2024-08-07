
  create view `site_dev`.`Tactical_clicks_months_12__dbt_tmp`
    
    
  as (
    





CREATE PROCEDURE `Tactical_clicks_months_12_dbt_test`()
BEGIN
    WITH curr_30_days_article_published AS (
        SELECT siteid, id
        FROM prod.site_archive_post
        WHERE date BETWEEN DATE_SUB(NOW(), INTERVAL 31 DAY) AND DATE_SUB(NOW(), INTERVAL 1 DAY)
        
            AND Categories <> 'Nyhedsoverblik'
        
        AND siteid = 14
    ),
    agg_curr_30_days_events AS (
        SELECT 
            e.siteid AS siteid, 
            SUM(e.hits) AS next_clicks
        FROM prod.events e
        JOIN curr_30_days_article_published a ON a.id = e.postid
        WHERE e.date BETWEEN DATE_SUB(NOW(), INTERVAL 31 DAY) AND DATE_SUB(NOW(), INTERVAL 1 DAY)
        AND e.siteid = 14
        AND e.Event_Action = 'Next Click'
        GROUP BY 1
    ),
    agg_curr_30_days_pages AS (
        SELECT
            p.siteid,
            SUM(p.unique_pageviews) AS pageview_sum
        FROM prod.pages p
        LEFT JOIN curr_30_days_article_published a ON a.id = p.postid
        WHERE p.siteid = 14
        AND p.date BETWEEN DATE_SUB(NOW(), INTERVAL 31 DAY) AND DATE_SUB(NOW(), INTERVAL 1 DAY)
        GROUP BY p.siteid
    ),
    value_curr_30_days AS (
        SELECT
            e.siteid,
            ROUND(e.next_clicks / p.pageview_sum * 100, 2) AS value
        FROM agg_curr_30_days_events e
        LEFT JOIN agg_curr_30_days_pages p ON e.siteid = p.siteid
    ),
    last_30_days_article_published AS (
        SELECT siteid, id
        FROM prod.site_archive_post
        WHERE date BETWEEN DATE_SUB(NOW(), INTERVAL 61 DAY) AND DATE_SUB(NOW(), INTERVAL 31 DAY)
        AND siteid = site
        AND categories <> "Nyhedsoverblik"
    ),
    agg_last_30_days_events AS (
        SELECT 
            e.siteid AS siteid, 
            SUM(e.hits) AS next_clicks_last
        FROM prod.events e
        JOIN last_30_days_article_published a ON a.id = e.postid
        WHERE e.date BETWEEN DATE_SUB(NOW(), INTERVAL 61 DAY) AND DATE_SUB(NOW(), INTERVAL 31 DAY)
        AND e.siteid = 14
        AND e.Event_Action = 'Next Click'
        GROUP BY 1
    ),
    agg_last_30_days_pages AS (
        SELECT
            p.siteid,
            SUM(p.unique_pageviews) AS pageview_sum_last
        FROM prod.pages p
        LEFT JOIN last_30_days_article_published a ON a.id = p.postid
        WHERE p.siteid = 14
        AND p.date BETWEEN DATE_SUB(NOW(), INTERVAL 61 DAY) AND DATE_SUB(NOW(), INTERVAL 31 DAY)
        GROUP BY p.siteid
    ),
    value_last_30_days AS (
        SELECT
            e.siteid,
            ROUND(e.next_clicks_last / p.pageview_sum_last * 100, 2) AS value_last
        FROM agg_last_30_days_events e
        LEFT JOIN agg_last_30_days_pages p ON e.siteid = p.siteid
    )
    SELECT 
        JSON_OBJECT(
            'site', al.siteid,
            'data', JSON_OBJECT(
            'label', 'Gns. next click (%)',
            'hint', 'Gns. next click på artikler publiceret seneste 30 dage ift. forrige 30 dage',
            'value', COALESCE(value, 0),
            'change', COALESCE(value - value_last, 0),
            'progressCurrent', '',
            'progressTotal', ''
            )
        ) AS json_data
    FROM value_curr_30_days al
    LEFT JOIN value_last_30_days alb ON al.siteid = alb.siteid
    WHERE al.siteid = 14;
END;
  );