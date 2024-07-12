








CREATE DEFINER=`Site`@`%` PROCEDURE `prod`.`pageview_weeks_dummy_2_3__site__13_dbt_test`(
    IN site INT,
    IN label_val VARCHAR(200),
    IN hint_val TEXT,
    IN order_statement_first  VARCHAR(2000),
    IN domain VARCHAR(200),
    IN tag_statement_first VARCHAR(2000),
    IN tag_statement_second VARCHAR(2000),
    IN tag_statement_third VARCHAR(2000)
)
BEGIN
    SET SESSION group_concat_max_len = 1000000;
    SET @sql_query = CONCAT('
    with 
    
    
    referrers AS(
        ', 'SELECT DISTINCT realreferrer FROM pre_stage.ref_value
        WHERE realreferrer IN (', order_statement_first, ') and siteid = ', site ,'
        UNION ALL SELECT ''Others''
    )
    ,
    categories AS (
        ', 
        'SELECT DISTINCT Categories FROM pre_stage.ref_value 
        WHERE Categories IN (', tag_statement_first, ') and siteid = ', site ,'
    
        UNION ALL SELECT ''Others''
        ),
        combinations AS (
            SELECT r.realreferrer, c.categories AS tag
            FROM referrers r
            CROSS JOIN categories c
            ORDER BY FIELD(r.realreferrer, ',order_statement_first, ', "Others")
        ),
        
        pageview AS (
            SELECT 
                siteid,
                r.realreferrer,
                postid,
                date AS visit_date,
                COUNT(*) AS visit
            FROM prod.traffic_channels AS t
            LEFT JOIN referrers r ON t.realreferrer = r.realreferrer
            WHERE siteid = ', site, ' AND date BETWEEN DATE_SUB(NOW(), INTERVAL 8 DAY) AND DATE_SUB(NOW(), INTERVAL 1 DAY)
                AND r.realreferrer IN (',order_statement_first, ')
                and t.postid is not null
            GROUP BY siteid, r.realreferrer, postid, date
        ),
        main AS (
            SELECT 
                postid,
                siteid,
                event_name,
                SUM(hits) AS t_totals
            FROM prod.events
            WHERE siteid = ',site,' AND Event_Action = ''Frontpage'' 
            and postid is not null
            AND date BETWEEN DATE_SUB(NOW(), INTERVAL 8 DAY) AND DATE_SUB(NOW(), INTERVAL 1 DAY)
            GROUP BY postid, siteid, event_name
        ),
        transformed_data AS (
            SELECT
                postid,
                siteid,
                t_totals,
                CASE 
                    WHEN event_name LIKE ''://%'' THEN CONCAT(',domain,', SUBSTRING(event_name, 5))
                    ELSE event_name
                END AS modifyurl
            FROM main
        ),
        
        final_transformed AS (
            SELECT
                s.siteid AS siteid,
                p.id AS postid,
                ''Forside'' as realreferrer,
                s.t_totals as visits,
				CASE
                    WHEN userneeds IN (', tag_statement_first, ') THEN  userneeds
                    ELSE ''Others''
                END AS tag
            FROM transformed_data s
            LEFT JOIN prod.site_archive_post p ON s.siteid = p.siteid AND s.modifyurl = p.link
            WHERE p.id IS NOT NULL
            and s.siteid = ',site,' 
        ),
        site_archive AS (
            SELECT
                s.siteid AS siteid,
                s.id AS postid,
                p.realreferrer,
                p.visit AS visits,
                CASE
                    WHEN userneeds IN (',  tag_statement_first, ') THEN userneeds
                    ELSE ''Others''
                END AS tag
            FROM prod.site_archive_post s
            RIGHT JOIN pageview p ON s.id = p.postid AND s.siteid = p.siteid
            WHERE s.Siteid = ', site,'
        ),
        total_data As(
			select * from site_archive
            UNION
            select * from final_transformed
        ),
        summed_data AS (
            SELECT
                c.realreferrer AS realreferrer,
                c.tag AS tag,
                COALESCE(SUM(s.visits), 0) AS total_visits
            FROM combinations c
            LEFT JOIN total_data s ON c.realreferrer = s.realreferrer AND c.tag = s.tag
            GROUP BY c.realreferrer, c.tag
			ORDER BY FIELD(c.realreferrer, ', order_statement_first, ', "Others")
        ),
        percent AS (
            SELECT
                realreferrer,
                tag,
                total_visits,
                COALESCE((ROUND((total_visits * 100.0) / COALESCE(SUM(total_visits) OVER (PARTITION BY realreferrer), 0), 2)), 0) AS percentile,
                ROW_NUMBER() OVER (PARTITION BY realreferrer ORDER BY tag) AS tag_order
            FROM summed_data
        ),
        pivoted_data AS (
			SELECT
				realreferrer,
				CONCAT(''['', GROUP_CONCAT(DISTINCT CONCAT(''"'', tag, ''"'') ORDER BY FIELD(tag, ', tag_statement_first, ', "Others")), '']'') AS categories,
				GROUP_CONCAT(total_visits ORDER BY FIELD(tag, ',tag_statement_first, ', "Others")) AS total_visits,
				CONCAT(''['', GROUP_CONCAT((percentile) ORDER BY FIELD(tag, ', tag_statement_first, ', "Others")), '']'') AS percentile
			FROM percent
			GROUP BY realreferrer
             ORDER BY FIELD(realreferrer, ', order_statement_first, ', "Others")
		),
        final as(
			SELECT 	
				',label_val,' AS label,
                ',hint_val,' AS hint,
				categories AS cat, 
				CONCAT(''['', GROUP_CONCAT(''{"name": "'', realreferrer, ''","data":'', percentile, ''}''), '']'') AS series
			FROM pivoted_data
			group by label,cat,hint
		), 
    
    
    categories_second AS (
        ','SELECT DISTINCT Categories FROM pre_stage.ref_value 
        WHERE Categories IN (', tag_statement_second, ') and siteid = ', site ,' 
        UNION ALL SELECT ''Others''
        ),
        combinations_second AS (
            SELECT r.realreferrer, c.categories AS tag
            FROM referrers r
            CROSS JOIN categories_second c
            ORDER BY FIELD(r.realreferrer, ',order_statement_first, ', "Others")
        ),
        
        final_transformed_second AS (
            SELECT
                s.siteid AS siteid,
                p.id AS postid,
                ''Forside'' as realreferrer,
                s.t_totals as visits,
				CASE
                    WHEN Categories IN (', tag_statement_second, ') THEN  Categories
                    ELSE ''Others''
                END AS tag
            FROM transformed_data s
            LEFT JOIN prod.site_archive_post p ON s.siteid = p.siteid AND s.modifyurl = p.link
            WHERE p.id IS NOT NULL
            and s.siteid = ',site,' 
        ),
        site_archive_second AS (
            SELECT
                s.siteid AS siteid,
                s.id AS postid,
                p.realreferrer,
                p.visit AS visits,
                CASE
                    WHEN Categories IN (',  tag_statement_second, ') THEN Categories
                    ELSE ''Others''
                END AS tag
            FROM prod.site_archive_post s
            RIGHT JOIN pageview p ON s.id = p.postid AND s.siteid = p.siteid
            WHERE s.Siteid = ', site,'
        ),
        total_data_second As(
			select * from site_archive_second
            UNION
            select * from final_transformed_second
        ),
        summed_data_second AS (
            SELECT
                c.realreferrer AS realreferrer,
                c.tag AS tag,
                COALESCE(SUM(s.visits), 0) AS total_visits
            FROM combinations_second c
            LEFT JOIN total_data_second s ON c.realreferrer = s.realreferrer AND c.tag = s.tag
            GROUP BY c.realreferrer, c.tag
			ORDER BY FIELD(c.realreferrer, ', order_statement_first, ', "Others")
        ),
        percent_second AS (
            SELECT
                realreferrer,
                tag,
                total_visits,
                COALESCE((ROUND((total_visits * 100.0) / COALESCE(SUM(total_visits) OVER (PARTITION BY realreferrer), 0), 2)), 0) AS percentile,
                ROW_NUMBER() OVER (PARTITION BY realreferrer ORDER BY tag) AS tag_order
            FROM summed_data_second
        ),
        pivoted_data_second AS (
			SELECT
				realreferrer,
				CONCAT(''['', GROUP_CONCAT(DISTINCT CONCAT(''"'', tag, ''"'') ORDER BY FIELD(tag, ', tag_statement_second, ', "Others")), '']'') AS categories,
				GROUP_CONCAT(total_visits ORDER BY FIELD(tag, ',tag_statement_second, ', "Others")) AS total_visits,
				CONCAT(''['', GROUP_CONCAT((percentile) ORDER BY FIELD(tag, ', tag_statement_second, ', "Others")), '']'') AS percentile
			FROM percent_second
			GROUP BY realreferrer
             ORDER BY FIELD(realreferrer, ', order_statement_first, ', "Others")
		),
        final_second as(
			SELECT 	
				',label_val,' AS label,
                ',hint_val,' AS hint,
				categories AS cat, 
				CONCAT(''['', GROUP_CONCAT(''{"name": "'', realreferrer, ''","data":'', percentile, ''}''), '']'') AS series
			FROM pivoted_data_second
			group by label,cat,hint
		), 
    
    
    categories_third AS (
        ', 'SELECT DISTINCT Categories FROM pre_stage.ref_value 
        WHERE Categories IN (', tag_statement_third, ') and siteid = ', site,'
        UNION ALL SELECT ''Others''
        ),
        combinations_third AS (
            SELECT r.realreferrer, c.categories AS tag
            FROM referrers r
            CROSS JOIN categories_third c
            ORDER BY FIELD(r.realreferrer, ',order_statement_first, ', "Others")
        ),
        
        final_transformed_third AS (
            SELECT
                s.siteid AS siteid,
                p.id AS postid,
                ''Forside'' as realreferrer,
                s.t_totals as visits,
				CASE
                    WHEN Tags IN (', tag_statement_third, ') THEN  Tags
                    ELSE ''Others''
                END AS tag
            FROM transformed_data s
            LEFT JOIN prod.site_archive_post p ON s.siteid = p.siteid AND s.modifyurl = p.link
            WHERE p.id IS NOT NULL
            and s.siteid = ',site,' 
        ),
        site_archive_third AS (
            SELECT
                s.siteid AS siteid,
                s.id AS postid,
                p.realreferrer,
                p.visit AS visits,
                CASE
                    WHEN Tags IN (',  tag_statement_third, ') THEN Tags
                    ELSE ''Others''
                END AS tag
            FROM prod.site_archive_post s
            RIGHT JOIN pageview p ON s.id = p.postid AND s.siteid = p.siteid
            WHERE s.Siteid = ', site,'
        ),
        total_data_third As(
			select * from site_archive_third
            UNION
            select * from final_transformed_third
        ),
        summed_data_third AS (
            SELECT
                c.realreferrer AS realreferrer,
                c.tag AS tag,
                COALESCE(SUM(s.visits), 0) AS total_visits
            FROM combinations_third c
            LEFT JOIN total_data_third s ON c.realreferrer = s.realreferrer AND c.tag = s.tag
            GROUP BY c.realreferrer, c.tag
			ORDER BY FIELD(c.realreferrer, ', order_statement_first, ', "Others")
        ),
        percent_third AS (
            SELECT
                realreferrer,
                tag,
                total_visits,
                COALESCE((ROUND((total_visits * 100.0) / COALESCE(SUM(total_visits) OVER (PARTITION BY realreferrer), 0), 2)), 0) AS percentile,
                ROW_NUMBER() OVER (PARTITION BY realreferrer ORDER BY tag) AS tag_order
            FROM summed_data_third
        ),
        pivoted_data_third AS (
			SELECT
				realreferrer,
				CONCAT(''['', GROUP_CONCAT(DISTINCT CONCAT(''"'', tag, ''"'') ORDER BY FIELD(tag, ', tag_statement_third, ', "Others")), '']'') AS categories,
				GROUP_CONCAT(total_visits ORDER BY FIELD(tag, ',tag_statement_third, ', "Others")) AS total_visits,
				CONCAT(''['', GROUP_CONCAT((percentile) ORDER BY FIELD(tag, ', tag_statement_third, ', "Others")), '']'') AS percentile
			FROM percent_third
			GROUP BY realreferrer
             ORDER BY FIELD(realreferrer, ', order_statement_first, ', "Others")
		),
        final_third as(
			SELECT 	
				',label_val,' AS label,
                ',hint_val,' AS hint,
				categories AS cat, 
				CONCAT(''['', GROUP_CONCAT(''{"name": "'', realreferrer, ''","data":'', percentile, ''}''), '']'') AS series
			FROM pivoted_data_third
			group by label,cat,hint
		)
    