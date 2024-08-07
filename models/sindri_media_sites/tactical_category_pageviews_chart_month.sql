{% set site = var('site') %}
{% set db_name = var('db_name_default') %}
{% set tag_filter = var('tag_filter') %}
{% set status_ = var('tag_filter')['status']%}
{% set query_tag = var('tag_filter')['query_tag']%}
{% set tendency = var('tendency')%}
{% set dtitle = tendency['dtitle'] %}
{% set count = tendency['dropdown_count']%}
{% set dropdown_menu = tendency['dropdown_menu']%}
{% set tendency_condition = tendency['tendency_condition']%}
{% set chart_count = tendency['tendency_chart_col_userneeds']%}
{% set chart_val = tendency['tendency_chart_value_userneeds']%}
{% set val_like = tendency['tendency_chart_like_userneeds']%}

CREATE {{db_name}} PROCEDURE `prod`.`tactical_category_pageviews_chart_month_dbt_{{site}}`()
BEGIN

    with 
	{% for i in range(count | length) %} 
    last_30_days_article_published{{ count[i] }} as(
        SELECT siteid as siteid,id,  date as fdate,
            CASE
			{% for j in range(chart_val[i] | length) %} WHEN {{ chart_count[i] }}  {{tendency_condition[i]}} {% if tendency_condition[i] == 'like' %}  "%{{ chart_val[i][j] }}%" {% else %} "{{ chart_val[i][j] }}" {% endif %} THEN "{{ val_like[i][j] }}" {% endfor %}
			ELSE 'others'
			END AS tags
            FROM prod.site_archive_post  
        where 
       date
		between DATE_SUB(NOW(), INTERVAL 31 DAY) and DATE_SUB(NOW(), INTERVAL 1 DAY)
        {% if status_ == 'yes' %}AND {{ query_tag }}{% endif %}
		AND siteid = {{site}}
		 ),
    cta_per_article{{ count[i] }} as
    (
        select  a.siteid as siteid,a.id as postid,a.tags,coalesce(sum(unique_pageviews),0) as val from
        last_30_days_article_published{{ count[i] }} a 
        left join prod.pages e  on e.postid=a.id and e.siteid = a.siteid
        group by 1,2,3
    ),
        agg_cta_per_article{{ count[i] }} as(
        select siteid as siteid,tags, coalesce(sum(val), 0) as agg_sum  from  cta_per_article{{ count[i] }}
        where siteid= {{site}} 
        group by 1,2
    ),
        last_30_days_article_published_Agg{{ count[i] }} as(
        select siteid as siteid,tags,count(*) as agg from  cta_per_article{{ count[i] }}
        where siteid = {{site}} 
        group by 1,2
    ),
    less_tg_data{{ count[i] }} as(
        select 
        postid,l.tags ,val,Min_pageviews
        ,a.siteid,
        case when val<Min_pageviews then 1 else 0 end as less_than_target
        ,percent_rank() over ( order BY case when val>=Min_pageviews then val-Min_pageviews else 0 end) as percentile
            from last_30_days_article_published{{ count[i] }} l
        join cta_per_article{{ count[i] }} a on l.siteid = a.siteid and l.id = a.postid and l.tags=a.tags
        join prod.goals g on a.siteid = g.site_id and g.Date = l.fdate
        where date
		between DATE_SUB(NOW(), INTERVAL 31 DAY) and DATE_SUB(NOW(), INTERVAL 1 DAY)  and a.siteid = {{site}} 
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
        from less_tg_data
        where siteid = {{site}}
        group by 1,2
    ),

    total_hits{{ count[i] }} as(
        select 
            siteid as siteid ,
            sum(less_than_target_count) as total_tags_hit,
            sum(top_10_count) as agg_by_top_10,
            sum(approved_count) as agg_by_approved 
        from counts{{ count[i] }}
        where  siteid ={{site}}
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
        where h.siteid  = {{site}}
    ),
    categories_d{{ count[i] }} as
    (
		select 
			siteid as siteid ,
			'Artikler ift. tags og sidevisninger' as label,
            'Artikler publiceret år til dato grupperet i tre kategorier: Artikler under sidevisningsmål | Artikler, der klarede mål | Top 10% bedste artikler ift. sidevisninger. For hver kategori vises hvor stor andel der er publiceret indenfor hvert brugerbehov, så det er muligt at se forskelle på tværs af kategorierne.' as hint,
			GROUP_CONCAT(tags ORDER BY FIELD(tags, '{% for j in range(chart_count[i] | length)%}{{val_like[i][j]}}{%- if not loop.last %}','{% endif %}{% endfor %}', 'others' ) SEPARATOR ',') 
			AS cateogires,
			GROUP_CONCAT(COALESCE(less_than_target, 0) ORDER BY FIELD(tags, '{% for j in range(chart_count[i] | length)%}{{val_like[i][j]}}{%- if not loop.last %}','{% endif %}{% endfor %}', 'others' ) SEPARATOR ',') 
			AS less_than_target,
			GROUP_CONCAT(approved ORDER BY FIELD(tags, '{% for j in range(chart_count[i] | length)%}{{val_like[i][j]}}{%- if not loop.last %}','{% endif %}{% endfor %}', 'others' ) SEPARATOR ',') 
			AS approved,
			GROUP_CONCAT(top_10 ORDER BY FIELD(tags, '{% for j in range(chart_count[i] | length)%}{{val_like[i][j]}}{%- if not loop.last %}','{% endif %}{% endfor %}', 'others' ) SEPARATOR ',') 
			AS top_10
		from agg_data{{ count[i] }}
		group by 1,2,3
	),
    categories{{ count[i] }} as 
    (
		select 
			siteid as siteid ,
			label as label,
            hint as hint,  
			CONCAT('"', REPLACE(cateogires, ',', '","'), '"') AS  cateogires,
			less_than_target as less_than_target ,
			approved as approved ,
			top_10 as top_10 
		from  categories_d{{ count[i] }}
	),
    json_data{{ count[i] }} as
		(
		select siteid,label as lab,hint as h,cateogires as cat,CONCAT(
		'{"name": "Under mål", "data": [',cast(less_than_target as char),']}'
		,',{"name": "Over mål" ,"data": [',cast(approved as char),']}',',
        {"name": "Top 10%" ,"data": [',cast(top_10 as char),']}') 
        as series from categories{{ count[i] }}
		)
		{%- if not loop.last %},{% endif%}{% endfor %}
        
SELECT 
			CONCAT
            (
'{',
            '"site":',jd.siteid,',',
            '"data": {',
                '"label": "', jd.lab, '",',
				'"hint": "', jd.h, '",',
                '"categories": [', jd.cat, '],',
                '"series": [', jd.series, '],',
				'"defaultTitle": "{{dtitle}}",',
				'"additional": [',
                    {% for i in range(1, dropdown_menu | length) %}
                    '{',
                        '"title": "{{dropdown_menu[i]}}",',
                        '"data": {',
                            '"label": "', json_data{{ count[i] }}.lab, '",',
                            '"categories": [', json_data{{ count[i] }}.cat, '],',
                            '"series": [', json_data{{ count[i] }}.series, ']',
                        '}',
                    '}'
                {% if not loop.last %}','{% endif %}
				{% endfor %}
        		']',
			'}',
		'}'
			) as json_data
		  FROM json_data jd
          {% for i in range(1, count | length) %}
		  CROSS join json_data{{count[i]}}
		  {% endfor %} ;


END
