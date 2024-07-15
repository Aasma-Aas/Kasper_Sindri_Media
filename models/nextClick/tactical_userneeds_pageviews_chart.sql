CREATE DEFINER=`Site`@`%` PROCEDURE `stage`.`tactical_userneeds_pageviews_chart_month_11_dbt_test`(
    IN site INT,
    IN label_val VARCHAR(200),
    IN hint_val TEXT,
    IN order_statement VARCHAR(2000),
    IN order_statement_second VARCHAR(2000),
    IN order_statement_third VARCHAR(2000),
    IN dtitle VARCHAR(200),
    IN title1 VARCHAR(200),
    IN title2 VARCHAR(200),
    IN event_action VARCHAR(255)
)
BEGIN
    DECLARE sql_query TEXT; -- Declare variable to hold dynamic SQL query

    -- Construct the dynamic SQL query
    SET @sql_query = CONCAT('
        WITH ')
         {% for i in range(count | length) %}
         'last_ytd_article_published{{ count[i] }} AS (
                SELECT
                    siteid AS siteid,
                    id,
                    date AS fdate,
                    CASE
                        {% for j in range(tendency_chart_var_2[i] | length) %}
                            WHEN {{ tendency_chart_var_1[i] }} LIKE "%{{ tendency_chart_var_2[i][j] }}%" THEN "{{ tendency_chart_var_3[i][j] }}"
                        {% endfor %}
                        ELSE "others"
                    END AS tags
                FROM prod.site_archive_post
                WHERE {{date_query[i]}}
                    AND siteid = {{site_id}}
            ),
            last_ytd_article_published_Agg{{ count[i] }} AS (
                SELECT
                    siteid AS siteid,
                    tags,
                    COUNT(*) AS agg
                FROM last_ytd_article_published{{ count[i] }}
                WHERE siteid = {{site_id}}
                GROUP BY siteid, tags
            ),
            agg_next_click_per_article{{ count[i] }} AS (
                SELECT
                    e.siteid AS siteid,
                    e.id AS postid,
                    e.tags,
                    COALESCE(SUM(hits), 0) AS val,
                    COALESCE(SUM(unique_pageviews), 0) AS page_view
                FROM last_ytd_article_published{{ count[i] }} e
                LEFT JOIN prod.pages p ON p.postid = e.id AND p.siteid = e.siteid
                LEFT JOIN prod.events a ON a.postid = e.id AND a.siteid = e.siteid AND a.event_action = {{event_action}}
                WHERE e.siteid = {{site_id}}
                GROUP BY e.siteid, e.id, e.tags
            ),
            cta_per_article{{ count[i] }} AS (
                SELECT
                    e.siteid AS siteid,
                    e.postid,
                    e.tags,
                    ROUND(COALESCE((COALESCE(val, 0) / COALESCE(page_view, 1)), 0.0) * 100.0, 2) AS val
                FROM agg_next_click_per_article{{ count[i] }} e
                WHERE e.siteid = {{site_id}}
                GROUP BY e.siteid, e.postid, e.tags
            ),
            agg_cta_per_article{{ count[i] }} AS (
                SELECT
                    siteid AS siteid,
                    tags,
                    SUM(val) AS agg_sum
                FROM cta_per_article{{ count[i] }}
                WHERE siteid = {{site_id}}
                GROUP BY siteid, tags
            ),
            less_tg_data{{ count[i] }} AS (
                SELECT
                    postid,
                    l.tags,
                    val,
                    min_cta,
                    a.siteid,
                    CASE WHEN val < min_cta THEN 1 ELSE 0 END AS less_than_target,
                    PERCENT_RANK() OVER (ORDER BY CASE WHEN val >= min_cta THEN val - min_cta ELSE 0 END) AS percentile
                FROM last_ytd_article_published{{ count[i] }} l
                JOIN cta_per_article{{ count[i] }} a ON l.siteid = a.siteid AND l.id = a.postid AND l.tags = a.tags
                JOIN prod.goals g ON a.siteid = g.site_id AND g.Date = l.fdate
                WHERE date BETWEEN DATE_SUB(NOW(), INTERVAL 31 DAY) AND DATE_SUB(NOW(), INTERVAL 1 DAY) AND g.site_id = {{site_id}}
            ),
            counts{{ count[i] }} AS (
                SELECT
                    siteid,
                    tags,
                    SUM(less_than_target) AS less_than_target_count,
                    SUM(CASE WHEN percentile <= 1 AND percentile >= 0.9 THEN 1 ELSE 0 END) AS top_10_count,
                    SUM(CASE WHEN percentile >= 0 AND percentile < 0.9 AND less_than_target = 0 THEN 1 ELSE 0 END) AS approved_count
                FROM less_tg_data{{ count[i] }}
                GROUP BY siteid, tags
            ),
            hits_by_tags{{ count[i] }} AS (
                SELECT
                    siteid AS siteid,
                    tags,
                    SUM(less_than_target) AS hits_tags,
                    SUM(CASE WHEN percentile <= 1 AND percentile >= 0.9 THEN val ELSE 0 END) AS sum_by_top_10,
                    SUM(CASE WHEN percentile >= 0 AND percentile < 0.9 AND less_than_target = 0 THEN val ELSE 0 END) AS total
                FROM less_tg_data{{ count[i] }}
                GROUP BY siteid, tags
            ),
            percent{{ count[i] }} AS (
                SELECT
                    counts{{ count[i] }}.siteid AS siteid,
                    tags,
                    ROUND(COALESCE(SUM(hits_tags) / SUM(total) * 100.0, 0.0), 2) AS pl_a
                FROM counts{{ count[i] }}
                JOIN hits_by_tags{{ count[i] }} USING (siteid, tags)
                GROUP BY siteid, tags
            ),
            pr_articles AS (
                SELECT
                    g.site_id AS site_id,
                    g.count AS count,
                    g.date AS date
                FROM prod.goals g
                WHERE g.site_id = {{site_id}}
            ),
            pr_statistics AS (
                SELECT
                    pr_a.articles,
                    g.site_id,
                    g.count,
                    g.date
                FROM pr_articles pr_a
                JOIN prod.goals g ON g.site_id = pr_a.site_id AND g.site_id = pr_a.articles
                WHERE g.site_id = {{site_id}}
                GROUP BY pr_a.articles, g.site_id, g.count, g.date
             ');
            
            SET @sql_query := CONCAT(@sql_query, '
            SELECT
                siteid,
                ', dtitle, ' AS dtitle,
                ', title1, ' AS title1,
                ', title2, ' AS title2,
                tags,
                COALESCE(SUM(agg), 0) AS agg,
                COALESCE(SUM(agg_sum), 0) AS agg_sum,
                COALESCE(SUM(pl_a), 0) AS pl_a,
                COALESCE(SUM(less_than_target_count), 0) AS less_than_target_count,
                COALESCE(SUM(top_10_count), 0) AS top_10_count,
                COALESCE(SUM(approved_count), 0) AS approved_count,
                COALESCE(SUM(hits_tags), 0) AS hits_tags,
                COALESCE(SUM(sum_by_top_10), 0) AS sum_by_top_10,
                COALESCE(SUM(total), 0) AS total
            FROM (
                SELECT * FROM (
                    SELECT 
                        siteid AS siteid, 
                        '', 
                        '', 
                        '', 
                        tags,
                        0 AS agg,
                        0 AS agg_sum,
                        0 AS pl_a,
                        0 AS less_than_target_count,
                        0 AS top_10_count,
                        0 AS approved_count,
                        0 AS hits_tags,
                        0 AS sum_by_top_10,
                        0 AS total
                    FROM prod.pages p
                    WHERE p.siteid = {{site_id}}
                    UNION ALL
                    SELECT * FROM (
                        SELECT * FROM (
                            SELECT 
                                e.siteid AS siteid, 
                                e.tags AS tags, 
                                COALESCE(SUM(hits), 0) AS hits_tags, 
                                COALESCE(SUM(unique_pageviews), 0) AS unique_pageviews 
                            FROM prod.pages p 
                            LEFT JOIN prod.events e ON e.postid = p.postid AND e.siteid = p.siteid
                            WHERE p.siteid = {{site_id}}
                            GROUP BY e.siteid, e.tags 
                        ) UNION ALL
                        SELECT * FROM (
                            SELECT 
                                e.siteid AS siteid, 
                                e.tags AS tags, 
                                COALESCE(SUM(hits), 0) AS hits_tags, 
                                COALESCE(SUM(unique_pageviews), 0) AS unique_pageviews 
                            FROM prod.pages p 
                            LEFT JOIN prod.events e ON e.postid = p.postid AND e.siteid = p.siteid
                            WHERE p.siteid = {{site_id}}
                            GROUP BY e.siteid, e.tags 
                        ) 
                    )
                )
            )
        ');

        SET @sql_query := CONCAT(@sql_query, '
            WHERE siteid = {{site_id}}
            GROUP BY siteid, tags
        ');


         {% endfor %}
    PREPARE stmt FROM @sql_query;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END;
