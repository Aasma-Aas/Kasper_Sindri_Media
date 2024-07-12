{{ config(materialized='table') }}
{% set count = ['', '_second', '_third']%}
{% set diction = {0: 'userneeds', 1: 'Categories', 2: 'Tags'}%}
{% set diction3 = {0: 'tag_statement_first', 1: 'tag_statement_second', 2: 'tag_statement_third'}%}
{%set dictionTable= {0:'site_archive_post',1:'site_archive_post',2:'split_tags_test'}%}

{% set dictionlist = {0: " 'SELECT DISTINCT realreferrer FROM pre_stage.ref_value
        WHERE realreferrer IN (', order_statement_first, ') and siteid = ', site ,'", 1: " 
        'SELECT DISTINCT Categories FROM pre_stage.ref_value 
        WHERE Categories IN (', tag_statement_first, ') and siteid = ', site ,'
    ", 2: "'SELECT DISTINCT Categories FROM pre_stage.ref_value 
        WHERE Categories IN (', tag_statement_second, ') and siteid = ', site ,' ",3:" 'SELECT DISTINCT Categories FROM pre_stage.ref_value 
        WHERE Categories IN (', tag_statement_third, ') and siteid = ', site,'"}%}



CREATE DEFINER=`Site`@`%` PROCEDURE `stage`.`pageview_ytd_13_dbt_test`(
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
    {% for i in range(count | length) %}
    {%if loop.first%}
    referrers{{count[i]}} AS(
        ',{{dictionlist[i]}}
        UNION ALL SELECT ''Others''
    )
    ,{% endif %}
    categories{{count[i]}} AS (
        ',{{dictionlist[i+1]}}
        UNION ALL SELECT ''Others''
        ),
        combinations{{count[i]}} AS (
            SELECT r.realreferrer, c.categories AS tag
            FROM referrers r
            CROSS JOIN categories{{count[i]}} c
            ORDER BY FIELD(r.realreferrer, ',order_statement_first, ', "Others")
        ),
        {%if loop.first%}
        pageview AS (
            SELECT 
                siteid,
                r.realreferrer,
                postid,
                date AS visit_date,
                COUNT(*) AS visit
            FROM prod.traffic_channels AS t
            LEFT JOIN referrers r ON t.realreferrer = r.realreferrer
            WHERE siteid = ', site, '  AND date BETWEEN  MAKEDATE(EXTRACT(YEAR FROM CURDATE()), 1) AND DATE_SUB(CAST(NOW() AS DATE), INTERVAL 1 DAY)
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
            AND date BETWEEN MAKEDATE(EXTRACT(YEAR FROM CURDATE()), 1) AND DATE_SUB(CAST(NOW() AS DATE), INTERVAL 1 DAY)
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
        {% endif %}
        final_transformed{{count[i]}} AS (
            SELECT
                s.siteid AS siteid,
                p.id AS postid,
                ''Forside'' as realreferrer,
                s.t_totals as visits,
				CASE
                    WHEN {{diction[i]}} IN (', {{diction3[i]}}, ') THEN  {{diction[i]}}
                    ELSE ''Others''
                END AS tag
            FROM transformed_data s
            LEFT JOIN prod.{{dictionTable[i]}} p ON s.siteid = p.siteid AND s.modifyurl = p.link
            WHERE p.id IS NOT NULL
            and s.siteid = ',site,' 
        ),
        site_archive{{count[i]}} AS (
            SELECT
                s.siteid AS siteid,
                s.id AS postid,
                p.realreferrer,
                p.visit AS visits,
                CASE
                    WHEN {{diction[i]}} IN (',  {{diction3[i]}}, ') THEN {{diction[i]}}
                    ELSE ''Others''
                END AS tag
            FROM prod.{{dictionTable[i]}} s
            RIGHT JOIN pageview p ON s.id = p.postid AND s.siteid = p.siteid
            WHERE s.Siteid = ', site,'
        ),
        total_data{{count[i]}} As(
			select * from site_archive{{count[i]}}
            UNION
            select * from final_transformed{{count[i]}}
        ),
        summed_data{{count[i]}} AS (
            SELECT
                c.realreferrer AS realreferrer,
                c.tag AS tag,
                COALESCE(SUM(s.visits), 0) AS total_visits
            FROM combinations{{count[i]}} c
            LEFT JOIN total_data{{count[i]}} s ON c.realreferrer = s.realreferrer AND c.tag = s.tag
            GROUP BY c.realreferrer, c.tag
			ORDER BY FIELD(c.realreferrer, ', order_statement_first, ', "Others")
        ),
        percent{{count[i]}} AS (
            SELECT
                realreferrer,
                tag,
                total_visits,
                COALESCE((ROUND((total_visits * 100.0) / COALESCE(SUM(total_visits) OVER (PARTITION BY realreferrer), 0), 2)), 0) AS percentile,
                ROW_NUMBER() OVER (PARTITION BY realreferrer ORDER BY tag) AS tag_order
            FROM summed_data{{count[i]}}
        ),
        pivoted_data{{count[i]}} AS (
			SELECT
				realreferrer,
				CONCAT(''['', GROUP_CONCAT(DISTINCT CONCAT(''"'', tag, ''"'') ORDER BY FIELD(tag, ', {{diction3[i]}}, ', "Others")), '']'') AS categories{{count[i]}},
				GROUP_CONCAT(total_visits ORDER BY FIELD(tag, ',{{diction3[i]}}, ', "Others")) AS total_visits,
				CONCAT(''['', GROUP_CONCAT((percentile) ORDER BY FIELD(tag, ', {{diction3[i]}}, ', "Others")), '']'') AS percentile{{count[i]}}
			FROM percent{{count[i]}}
			GROUP BY realreferrer
             ORDER BY FIELD(realreferrer, ', order_statement_first, ', "Others")
		),
        final{{count[i]}} as(
			SELECT 	
				',label_val,' AS label,
                ',hint_val,' AS hint,
				categories{{count[i]}} AS cat{{count[i]}}, 
				CONCAT(''['', GROUP_CONCAT(''{"name": "'', realreferrer, ''","data":'', percentile{{count[i]}}, ''}''), '']'') AS series{{count[i]}}
			FROM pivoted_data{{count[i]}}
			group by label,cat{{count[i]}},hint
		)
    {%- if not loop.last %}, {% endif%}
    {% endfor %}