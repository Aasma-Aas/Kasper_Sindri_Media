{{ config(materialized='table') }}
{% set count = ['', '_second', '_third']%}
{% set diction = {0: 'categories', 1: 'tags', 2: 'tags_r'}%}
{% set diction2 = {0: ['Fodsundhed og samfund', 'Forebyggelse', 'Forskning og viden', 'Praksis'], 1: ['Hjælp mig med at forstå', 'Inspirer mig', 'Giv mig en fordel'], 2: ['Long read', 'Kort og godt', 'Q&amp', 'Best Practice', 'Viden og forskning']} %}
{% set diction3 = {0: 'order_statement_first', 1: 'tag_statement_additional', 2: 'order_statement_third'}%}
{% set json_table_prefix = 'json_data'%}




CREATE DEFINER=`Site`@`%` PROCEDURE `prod`.`tactical_userneeds_pageviews_chart_month_dropdown`(
	IN site INT,
    IN label_val VARCHAR(200),
    IN hint_val text,
    IN order_statement_first  VARCHAR(2000),
	IN tag_statement_additional VARCHAR(2000),
    IN order_statement_third VARCHAR(2000),
	IN dtitle VARCHAR(200),
    IN title1 VARCHAR(200),
    IN title2 VARCHAR(200),
	IN event_action VARCHAR(200)
)
BEGIN
    SET @sql_query = CONCAT('
    with 
    {% for i in range(count | length) %}
    last_30_days_article_published{{ count[i] }} as
    (
		SELECT 
			siteid as siteid,
            id,
            date as fdate,
			CASE
                {% for j in range(diction2[i] | length)-%}
				WHEN {{ diction[i] }} like "%{{ diction2[i][j] }}%" then "{{ diction2[i][j] }}"
                {% endfor -%}
				ELSE ''others''
			END AS tags
        FROM prod.site_archive_post  
        WHERE date BETWEEN DATE_SUB(NOW(), INTERVAL 31 DAY) AND DATE_SUB(NOW(), INTERVAL 1 DAY) AND siteid = ', site, '
	),
    last_30_days_article_published_Agg{{ count[i] }} as
    (
        SELECT
            siteid as siteid,tags,count(*) as agg
        from
        last_30_days_article_published{{ count[i] }} 
		where siteid = ', site, '
		group by 1,2
    ),
    cta_per_article{{ count[i] }} as
    (
        select
            e.siteid as siteid,
            e.id as postid,
            coalesce(sum(hits),0) as val
        from last_30_days_article_published{{ count[i] }} e
        left join prod.events a on a.postid = e.id
        where e.siteid = ', site, ' and a.Event_Action = ',event_action,'
        group by 1,2
    ),
    agg_cta_per_article{{ count[i] }} as
      (
		select 
			siteid as siteid,
            sum(val) as agg_sum 
		from  cta_per_article{{ count[i] }}
		where siteid = ', site, '
		group by 1
	),
    less_tg_data{{count[i]}} as(
		select 
        
            l.id,
            l.tags ,
            a.val,
            min_cta,
            l.siteid,
			case when coalesce (val,0)<min_cta then 1 else 0 end as less_than_target,
            percent_rank() over ( order BY case when val>=min_cta then val-min_cta else 0 end) as percentile
		from last_30_days_article_published{{count[i]}} l
		left join cta_per_article{{count[i]}} a on l.siteid = a.siteid and l.id = a.postid
		left join prod.goals g on l.siteid = g.site_id and g.Date = l.fdate
		 where date  between DATE_SUB(NOW(), INTERVAL 31 DAY) and DATE_SUB(NOW(), INTERVAL 1 DAY) and g.site_id = ', site, '
	),
    hits_by_tags{{ count[i] }} as
    (
        select
            siteid as siteid,
            tags,
            sum(less_than_target) as hits_tags,
		sum(case when percentile<=1 and percentile>=0.9 then val else 0 end) sum_by_top_10,
		sum(case when percentile>=0 and percentile<0.9 and less_than_target=0 then val else 0 end) sum_by_approved
		  from less_tg_data{{ count[i] }}
		  where siteid = ', site, '
		group by 1,2
	),
    total_hits{{count[i]}} as
    (
        select 
            siteid as siteid ,
            sum(hits_tags) as total_tags_hit,
            sum(sum_by_top_10) as agg_by_top_10,
            sum(sum_by_approved) as agg_by_approved 
        from hits_by_tags{{count[i]}}
        where siteid = ', site, '
        group by 1
    ),
    agg_data{{count[i]}} as
    (
        select 
            h.siteid,
            h.tags,
            coalesce ((hits_tags/total_tags_hit),0)*100 as less_than_target,
            coalesce((sum_by_top_10/agg_by_top_10),0)*100 as top_10,coalesce((sum_by_approved/agg_by_approved),0)*100 approved
        from hits_by_tags{{count[i]}} h
        join total_hits{{count[i]}} t on h.siteid=t.siteid
        join last_30_days_article_published_Agg{{count[i]}} hbt on hbt.siteid=h.siteid and hbt.tags=h.tags
        where h.siteid = ', site, '
    ),
    categories_d{{count[i]}} as (
        select
            siteid as siteid ,
            ',label_val,' as label,'
            ,hint_val,' as hint,
            GROUP_CONCAT(tags ORDER BY FIELD(tags, ', {{diction3[i]}}, ', ''others'') SEPARATOR '','') AS cateogires,
            GROUP_CONCAT(COALESCE(less_than_target, 0) ORDER BY FIELD(tags, ', {{diction3[i]}}, ', ''others'') SEPARATOR '','') AS less_than_target,
            GROUP_CONCAT(approved ORDER BY FIELD(tags, ', {{diction3[i]}}, ', ''others'') SEPARATOR '','') AS approved,
            GROUP_CONCAT(top_10 ORDER BY FIELD(tags,', {{diction3[i]    }}, ', ''others'') SEPARATOR '','') AS top_10
        from agg_data{{count[i]}}
        group by 1,2,3
    ),
    categories{{count[i]}} as (
        select
            siteid as siteid,
            label as label,
            hint as hint,  
            CONCAT(''"'', REPLACE(cateogires, '','', ''","''), ''"'') AS  cateogires,
            less_than_target as less_than_target ,
            approved as approved ,
            top_10 as top_10 
        from  categories_d{{count[i]}}
    ),
    json_data{{count[i]}} as
    (
        SELECT 
            siteid,
            label AS lab, 
            hint AS h,
            cateogires AS cat,
            CONCAT(''{"name": "Under mål", "data": ['', CAST(less_than_target AS CHAR), '']}, '',
                    ''{"name": "Godkendt", "data": ['', CAST(approved AS CHAR), '']}, '',
                    ''{"name": "Top 10%", "data": ['', CAST(top_10 AS CHAR), '']} '') AS series
        FROM categories{{count[i]}}
    )
    {%- if not loop.last %}, {% endif%}
    {% endfor %}
    SELECT 
		CONCAT(
        ''{'',
            ''"site":'',jd.siteid,'','',
            ''"data": {'',
                ''"label": "'', jd.lab, ''",'',
                ''"categories": ['', jd.cat, ''],'',
                ''"series": ['', jd.series, '']'',
                '',"defaultTitle":"',dtitle,'"'',
                '',"additional":[ ''
                {% for i in range(1, (count | length)) %}
                ,''{'',
                ''"title":"',title{{ i }},'",'',
                ''"data": {'',
                ''"label": "'',{{json_table_prefix}}{{count[i]}}.lab, ''",'',
                ''"categories": ['', {{json_table_prefix}}{{count[i]}}.cat, ''],'',
                ''"series": ['', {{json_table_prefix}}{{count[i]}}.series, '']'',
                ''}''
                {% if not loop.last %}
                ,
                {% endif %}
                {% endfor %}
        '']}}''
    
			) as json_data
		  FROM json_data jd
          {% for i in range(1, count | length) %}
          CROSS join json_data{{count[i]}}
          {% endfor %};