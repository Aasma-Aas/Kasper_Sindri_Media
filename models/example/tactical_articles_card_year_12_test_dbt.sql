{% set site = var('site') %}

CREATE PROCEDURE `prod`.`tactical_articles_card_year_12_test_dbt`()
BEGIN
    WITH last_30_day AS (
        SELECT siteId as siteId, COUNT(date) as value 
        FROM prod.site_archive_post
        WHERE date between MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1)  and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY) AND categories <> "Nyhedsoverblik"
        AND siteid = {{ site }}
        GROUP BY 1
    ),
    last_30_days_before AS (
        SELECT siteId as siteId, COUNT(*) as value  
        FROM prod.site_archive_post
        WHERE date between DATE_SUB(MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1), INTERVAL 1 Year)  and  DATE_SUB(DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY),INTERVAL 1 Year) and categories <> "Nyhedsoverblik"
        AND siteid = {{ site }}
        GROUP BY 1
    )
    SELECT
        JSON_OBJECT(
            'site', ld.siteId,
            'data', JSON_OBJECT(
                 'label', 'Artikler',
                'hint', 'Artikler publiceret seneste 30 dage ift. forrige 30 dage',
                'value', COALESCE(ld.value, 0),
                'change', COALESCE(ROUND(((ld.value - lb.value) / lb.value) * 100, 2), 0),
                'progressCurrent', '',
                'progressTotal', ''
            )
        ) AS json_data
    FROM last_30_day ld
    LEFT JOIN last_30_days_before lb ON ld.siteId = lb.siteId;
END;
