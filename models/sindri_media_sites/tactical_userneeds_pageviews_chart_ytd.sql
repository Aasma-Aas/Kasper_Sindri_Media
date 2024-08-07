{% set site = var('site') %}
{% set db_name = var('db_name_default') %}
{% set event_name  = var('event_action') %}
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




CREATE PROCEDURE `prod`.`tactical_userneeds_pageviews_chart_ytd_dbt_{{site}}`()
BEGIN

    with 
	{% for i in range(count | length) %} 
	last_ytd_days_article_published{{ count[i] }} as(
		SELECT siteid as siteid,id,  date as fdate,
		CASE
			{% for j in range(chart_val[i] | length) %} WHEN {{ chart_count[i] }}  {{tendency_condition[i]}} {% if tendency_condition[i] == 'like' %}  "%{{ chart_val[i][j] }}%" {% else %} "{{ chart_val[i][j] }}" {% endif %} THEN "{{ val_like[i][j] }}" {% endfor %}
			ELSE 'others'
		END AS tags
        FROM prod.site_archive_post   
        WHERE 
		date between MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1)  and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY)  
		{% if status_ == 'yes' %}AND {{ query_tag }}{% endif %}
		AND siteid = {{site}}
        ),
	 last_ytd_days_article_published_Agg{{ count[i] }} as(
		select 
			siteid as siteid,
            tags,
            count(*) as agg 
		from  last_ytd_days_article_published{{ count[i] }}
		where siteid = {{site}}
		group by 1,2
		),
	  agg_next_click_per_article{{ count[i] }} as 
      (
		select  
			e.siteid as siteid,
            e.id as postid,
            e.tags,
            coalesce(sum(hits),0) as val
		from last_ytd_days_article_published{{ count[i] }}  e
		left join prod.events  a on a.postid=e.id and a.siteid = e.siteid 
		and a.event_action = '{{event_name}}'
		where    e.siteid = {{site}} 
		group by 1,2,3
		),
	agg_pages_per_article{{ count[i] }} as 
      (
		select  
			e.siteid as siteid,
            e.id as postid,
            e.tags,
			coalesce(sum(unique_pageviews),0) as page_view 
		from last_ytd_days_article_published{{ count[i] }}  e
		left join prod.pages p on p.postid=e.id and p.siteid =e.siteid
		where    e.siteid = {{site}}
		group by 1,2,3
		)  ,
	agg_total{{ count[i] }} as (
			select
			e.siteid as siteid,
			e.id as postid,
			e.tags as tags,
			p.page_view as page_view,
			a.val as val
			from last_ytd_days_article_published{{ count[i] }}  e
			left join agg_pages_per_article{{ count[i] }} p on p.postid=e.id and p.siteid =e.siteid
			left join agg_next_click_per_article{{ count[i] }}  a on a.postid=e.id and a.siteid = e.siteid
        ),
		cta_per_article{{ count[i] }} as
		( 
			select  e.siteid as siteid, e.postid, e.tags,
			round(coalesce((coalesce(val,0)/coalesce(page_view,1)),0.0)*100.0,2) as val from agg_total{{ count[i] }}  e
			where  e.siteid = {{site}}
			group by 1,2,3
			),
	agg_cta_per_article{{ count[i] }} as(
		select siteid as siteid,tags,sum(val) as agg_sum from  cta_per_article{{ count[i] }}
		where siteid = {{site}}
		group by 1,2
		),
	less_tg_data{{ count[i] }} as(
		select 
		postid,l.tags ,val,min_cta
		,a.siteid,
		case when val<min_cta then 1 else 0 end as less_than_target
		,percent_rank() over ( order BY case when val>=min_cta then val-min_cta else 0 end) as percentile
		 from last_ytd_days_article_published{{ count[i] }} l
		join cta_per_article{{ count[i] }} a on l.siteid = a.siteid and l.id = a.postid and l.tags=a.tags
		join prod.goals g on a.siteid = g.site_id and g.Date = l.fdate
		 where date between MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1)  and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY) and g.site_id = {{site}}
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
		),
		hits_by_tags{{ count[i] }} as(
			select 
				siteid as siteid,
				tags,
				sum(less_than_target) as hits_tags,
				sum(case when percentile<=1 and percentile>=0.9 then val else 0 end) sum_by_top_10,
				sum(case when percentile>=0 and percentile<0.9 and less_than_target=0 then val else 0 end) sum_by_approved
		   from less_tg_data{{ count[i] }}
		   where siteid = {{site}}
		   group by 1,2
		),
		total_hits{{ count[i] }} as(
			select 
				siteid as siteid ,
				sum(less_than_target_count) as total_tags_hit,
                sum(top_10_count) as agg_by_top_10,
                sum(approved_count) as agg_by_approved 
			from counts
			where  siteid = {{site}}
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
			join last_ytd_days_article_published_Agg{{ count[i] }} hbt on hbt.siteid=h.siteid and hbt.tags=h.tags
			where h.siteid  = {{site}}
		),
		categories_d{{ count[i] }} as (
			select 
			siteid as siteid ,
			'Artikler ift. tags og sidevisningsmål' as label,
            'Artikler publiceret seneste 30 dage grupperet i tre kategorier: Artikler under sidevisningsmål | Artikler, der klarede mål | Top 10% bedste artikler ift. sidevisninger. For hver kategori vises hvor stor andel der er publiceret indenfor hvert tag, så det er muligt at se forskelle på tværs af kategorierne.' as hint,
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
        categories{{ count[i] }} as (
		select siteid as siteid ,label as label, hint as hint,  
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
