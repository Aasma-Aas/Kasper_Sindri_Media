CREATE DEFINER=`Site`@`%` PROCEDURE `tactical_category_pageviews_chart_month_13`(
	IN site INT,
  IN label_val VARCHAR(200),
  IN hint_val text,
  IN order_statement_first  VARCHAR(2000),
	IN order_statement_second VARCHAR(2000),
  IN order_statement_third VARCHAR(2000),
	IN dtitle VARCHAR(200),
  IN title1 VARCHAR(200),
  IN title2 VARCHAR(200)
)
BEGIN
    SET @sql_query = CONCAT('
    with 
    {% for i in range(count | length) %}
    last_30_days_article_published{{ count[i] }} as(
        SELECT siteid as siteid,id,  date as fdate,
            CASE
              {%- for j in range(tendency_chart_value_category[i] | length)%}
			          WHEN {{ tendency_chart_col_category[i] }} like "%{{ tendency_chart_value_category[i][j] }}%" then "{{ tendency_chart_like_category[i][j] }}"
                {%- endfor %}
			    ELSE ''others''
			END AS tags
            FROM prod.site_archive_post  
        where 
        WHERE {{date_query[i]}} AND {{site_id}}),
    cta_per_article{{ count[i] }} as
    (
        select  a.siteid as siteid,a.id as postid,a.tags,coalesce(sum(unique_pageviews),0) as val from
        last_30_days_article_published{{ count[i] }} a 
        left join prod.pages e  on e.postid=a.id and e.siteid = a.siteid
        group by 1,2,3
    ),
        agg_cta_per_article{{ count[i] }} as(
        select siteid as siteid,tags, coalesce(sum(val), 0) as agg_sum  from  cta_per_article{{ count[i] }}
        where siteid= {{site_id}}
        group by 1,2
    ),
        last_30_days_article_published_Agg{{ count[i] }} as(
        select siteid as siteid,tags,count(*) as agg from  cta_per_article
        where siteid = {{site_id}}
        group by 1,2
    ),
    less_tg_data{{ count[i] }} as(
        select 
        postid,l.tags ,val,Min_pageviews
        ,a.siteid,
        case when val<Min_pageviews then 1 else 0 end as less_than_target
        ,percent_rank() over ( order BY case when val>=Min_pageviews then val-Min_pageviews else 0 end) as percentile
            from last_30_days_article_published{{ count[i] }} l
        join cta_per_article a on l.siteid = a.siteid and l.id = a.postid and l.tags=a.tags
        join prod.goals g on a.siteid = g.site_id and g.Date = l.fdate
        where {{date_query[i]}} AND {{site_id}}
    ),
counts{{ count[i] }} as (
        select 
            siteid,
            tags,
            sum(less_than_target) as less_than_target_count,
            sum(case when percentile <= 1 and percentile >= 0.9 then 1 else 0 end) as top_10_count,
            sum(case when percentile >= 0 and percentile < 0.9 and less_than_target = 0 then 1 else 0 end) as approved_count
        from less_tg_data{{ count[i] }}
        group by 1,2
    )
    ,
    hits_by_tags{{ count[i] }} as(
        select 
            siteid as siteid,
            tags,
            sum(less_than_target) as hits_tags,
            sum(case when percentile<=1 and percentile>=0.9 then val else 0 end) sum_by_top_10,
            sum(case when percentile>=0 and percentile<0.9 and less_than_target=0 then val else 0 end) sum_by_approved
        from less_tg_data{{ count[i] }}
        where siteid = {{site_id}}
        group by 1,2
    ),

    total_hits{{ count[i] }} as(
        select 
            siteid as siteid ,
            sum(less_than_target_count) as total_tags_hit,
            sum(top_10_count) as agg_by_top_10,
            sum(approved_count) as agg_by_approved 
        from counts{{ count[i] }}
        where  siteid = {{site_id}}
        group by 1
    ),
    agg_data{{ count[i] }} as(
        select 
            h.siteid as siteid ,
            h.tags,
            coalesce(less_than_target_count/total_tags_hit,0)*100 as less_than_target ,
            coalesce(top_10_count/agg_by_top_10,0)*100 as top_10,
            coalesce(approved_count/agg_by_approved,0)*100 as  approved
        from counts{{ count[i] }} h
        join total_hits{{ count[i] }} t on h.siteid=t.siteid
        join last_30_days_article_published_Agg{{ count[i] }} hbt on hbt.siteid=h.siteid and hbt.tags=h.tags
        where h.siteid  = {{site_id}}
    ),
    categories_d{{ count[i] }} as
    (
		select 
			siteid as siteid ,
      {{tendency_cards_label}} as label,
      {{tendency_cards_hint}} as hint,
			GROUP_CONCAT(tags ORDER BY FIELD(tags, ',{{tendency_chart_value_category[i]}},', ''others'') SEPARATOR '','') AS cateogires,
			GROUP_CONCAT(COALESCE(less_than_target, 0) ORDER BY FIELD(tags, ',{{tendency_chart_value_category[i]}},', ''others'') SEPARATOR '','') AS less_than_target,
			GROUP_CONCAT(approved ORDER BY FIELD(tags, ',{{tendency_chart_value_category[i]}},', ''others'') SEPARATOR '','') AS approved,
			GROUP_CONCAT(top_10 ORDER BY FIELD(tags,',{{tendency_chart_value_category[i]}},', ''others'') SEPARATOR '','') AS top_10
		from agg_data{{ count[i] }}
		group by 1,2,3
	),
    categories{{ count[i] }} as 
    (
		select 
			siteid as siteid ,
			label as label,
            hint as hint,  
			CONCAT(''"'', REPLACE(cateogires, '','', ''","''), ''"'') AS  cateogires,
			less_than_target as less_than_target ,
			approved as approved ,
			top_10 as top_10 
		from  categories_d{{ count[i] }}
	),
    json_data as
		(
		SELECT 
			siteid,
			label AS lab, 
			hint AS h,
			cateogires AS cat,
			CONCAT(''{"name": "Under mål", "data": ['', CAST(less_than_target AS CHAR), '']}, '',
				   ''{"name": "Over mål", "data": ['', CAST(approved AS CHAR), '']}, '',
				   ''{"name": "Top 10%", "data": ['', CAST(top_10 AS CHAR), '']} '') AS series
		FROM categories{{ count[i] }}
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
                '',"defaultTitle":"',,'"'',
                '',"additional":[ ''
                {% for i in range(1, (count | length)) %}
                ,''{'',
                ''"title":"',title{{ i }},'",'',
                ''"data": {'',
                ''"label": "'',json_data{{count[i]}}.lab, ''",'',
                ''"categories": ['', json_data{{count[i]}}.cat, ''],'',
                ''"series": ['', json_data{{count[i]}}.series, '']'',
                ''}}''
                {% if not loop.last %}
                , '',''
                {% endif %}
                {% endfor %}
        '']}}''
    
			) as json_data
		  FROM json_data jd
          {% for i in range(1, count | length) %}
          CROSS join json_data{{count[i]}}
          {% endfor %};
