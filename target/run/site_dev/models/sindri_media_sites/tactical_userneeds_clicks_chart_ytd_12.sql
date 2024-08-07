
  create view `site_dev`.`tactical_userneeds_clicks_chart_ytd_12__dbt_tmp`
    
    
  as (
    








CREATE DEFINER=`Site`@`%` PROCEDURE `prod`.`tactical_userneeds_clicks_chart_ytd_dbt_17`()
BEGIN

with last_30_days_article_published as
(
	SELECT 
		siteid as siteid,
		id, 
		date as fdate  
	FROM prod.site_archive_post
	where date between MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1)  and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY)
	
	and siteid = 17
)
,
last_30_days_article_published_Agg as
(
	select 
		siteid as siteid,
		count(*) as agg
	from  last_30_days_article_published
	where   siteid = 17
	group by 1
),
next_clicks_events as
(
	select  
		e.siteid as siteid,
		e.postid,
		sum(hits) as val 
	from prod.events e
	join  last_30_days_article_published a on a.id=e.postid
	where e.Event_Action = 'Next Click'  
	and e.siteid = 17
	and date between MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1)  and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY)
	group by 1,2
),
pages as
(
	select
		p.siteid,
		p.postid,
		sum(p.Unique_pageViews) as pageviews
	from prod.pages p
	left join last_30_days_article_published l on l.id = p.postid
	where p.date between
	MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1)  and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY)
	and p.siteid = 17
	group by 1,2
),
next_click_percentage as
(
	select 
		ncv.siteid,
		ncv.postid,
		ncv.val as value,
		p.pageviews,
		round((ncv.val/p.pageviews * 100),2) as val
	from next_clicks_events ncv
	left join pages p on p.postid = ncv.postid and  p.siteid = ncv.siteid
),
agg_data as
(
	select 
		l.siteid,
		postid,
		val,
		value,
		pageviews,
		min_cta,
		percent_rank() over (ORDER BY case when val>=min_cta then val-min_cta else 0 end) as percentile,
		case when coalesce (val,0)>=min_cta then val-min_cta else 0 end greater_than_target,
		case when coalesce (val,0)<min_cta then 1 else 0 end as less_than_target
		
	from last_30_days_article_published l
	left join next_click_percentage a on l.siteid = a.siteid and l.id = a.postid
	left join prod.goals g on  g.Date = l.fdate
	where g.date between MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1)  and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY)
	and g.site_id = 17
)
,
dark_blue as
(
	SELECT
    category,
    SUM(value) / SUM(pageviews) * 100 AS percentage
	FROM (
		SELECT
			postid,
			CASE
				WHEN percentile >= 0 AND percentile < 0.9 AND less_than_target = 0 THEN 'Approved'
				WHEN percentile <= 1 AND percentile >= 0.9 THEN 'Top 10%'
				WHEN less_than_target = 1 THEN 'Less_than_target'
			END AS category,
			value,
			pageviews
		FROM
			agg_data
	) AS categorized_data
	WHERE
		category IS NOT NULL  
	GROUP BY
		category
),
data_out as
(
	select 
		a.siteid,
		 'Artikler ift. next click mål' as label,
		 'Artikler grupperet ift hvordan de har performet på next click.' as hint,
		'"Under mål" , "Over mål" , "Top 10%"' as categories,
		COALESCE((sum(less_than_target)/agg)*100,0)  as less_than_target,
		COALESCE((sum(case when percentile<=1 and percentile>=0.9 then 1 else 0 end)/agg)*100,0) as top_10,
		COALESCE((sum(case when percentile>=0 and percentile<0.9 and less_than_target=0 then 1 else 0 end)/agg)*100,0) as approved,
        
        COALESCE((select percentage  from dark_blue where category = 'Less_than_target'),0) as less_than_target_click,
        COALESCE((select percentage  from dark_blue where category = 'Top 10%'),0) as top_10_clicks,
        COALESCE((select percentage  from dark_blue where category = 'Approved'),0) as approved_clicks
        
	from agg_data a
	join last_30_days_article_published_Agg ag on ag.siteid=a.siteid
	where   a.siteid = 17
	group by 1,2,3,4
),

json_data as
(
	select 
		siteid as siteid,
        label as lab,
        hint as ht,
        categories as cat,
        CONCAT
        (
        '{"name": "Andel artikler",
        "data": [',
        cast(less_than_target as char),',',
        cast(approved as char),',',
        cast(top_10 as char),']}'
	    ,',
        {"name": "Gns. next click for artikler.",
        "data": [',
        cast(less_than_target_click as char),',',
        cast(approved_clicks as char),',',
        cast(top_10_clicks as char),']}'
        ) as series 
	from data_out
)


SELECT 
    IFNULL(
        CONCAT(
            '{',
            '"site":', siteid, ',',
            '"data":{"label":"', lab, '","hint":"', ht, '","categories":', '[' , cat, '],',
            '"series":', '[', series, ']'
            ,'}}'
        ),
        '{"site": ' || 17 || ',
          "data":{"label":"' || lab || '","hint":"' || ht || '","categories":[' || cat || '],"series":[]}}'
    ) as json_data
FROM json_data;

END
  );