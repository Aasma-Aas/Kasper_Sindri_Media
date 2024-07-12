







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
    
    last_30_days_article_published as
    (
		SELECT 
			siteid as siteid,
            id,
            date as fdate,
			CASE
                WHEN categories like "%Fodsundhed og samfund%" then "Fodsundhed og samfund"
                WHEN categories like "%Forebyggelse%" then "Forebyggelse"
                WHEN categories like "%Forskning og viden%" then "Forskning og viden"
                WHEN categories like "%Praksis%" then "Praksis"
                ELSE ''others''
			END AS tags
        FROM prod.site_archive_post  
        WHERE date BETWEEN DATE_SUB(NOW(), INTERVAL 31 DAY) AND DATE_SUB(NOW(), INTERVAL 1 DAY) AND siteid = ', site, '
	),
    last_30_days_article_published_Agg as
    (
        SELECT
            siteid as siteid,tags,count(*) as agg
        from
        last_30_days_article_published 
		where siteid = ', site, '
		group by 1,2
    ),
    cta_per_article as
    (
        select
            e.siteid as siteid,
            e.id as postid,
            coalesce(sum(hits),0) as val
        from last_30_days_article_published e
        left join prod.events a on a.postid = e.id
        where e.siteid = ', site, ' and a.Event_Action = ',event_action,'
        group by 1,2
    ),
    agg_cta_per_article as
      (
		select 
			siteid as siteid,
            sum(val) as agg_sum 
		from  cta_per_article
		where siteid = ', site, '
		group by 1
	),
    less_tg_data as(
		select 
        
            l.id,
            l.tags ,
            a.val,
            min_cta,
            l.siteid,
			case when coalesce (val,0)<min_cta then 1 else 0 end as less_than_target,
            percent_rank() over ( order BY case when val>=min_cta then val-min_cta else 0 end) as percentile
		from last_30_days_article_published l
		left join cta_per_article a on l.siteid = a.siteid and l.id = a.postid
		left join prod.goals g on l.siteid = g.site_id and g.Date = l.fdate
		 where date  between DATE_SUB(NOW(), INTERVAL 31 DAY) and DATE_SUB(NOW(), INTERVAL 1 DAY) and g.site_id = ', site, '
	),
    hits_by_tags as
    (
        select
            siteid as siteid,
            tags,
            sum(less_than_target) as hits_tags,
		sum(case when percentile<=1 and percentile>=0.9 then val else 0 end) sum_by_top_10,
		sum(case when percentile>=0 and percentile<0.9 and less_than_target=0 then val else 0 end) sum_by_approved
		  from less_tg_data
		  where siteid = ', site, '
		group by 1,2
	),
    total_hits as
    (
        select 
            siteid as siteid ,
            sum(hits_tags) as total_tags_hit,
            sum(sum_by_top_10) as agg_by_top_10,
            sum(sum_by_approved) as agg_by_approved 
        from hits_by_tags
        where siteid = ', site, '
        group by 1
    ),
    agg_data as
    (
        select 
            h.siteid,
            h.tags,
            coalesce ((hits_tags/total_tags_hit),0)*100 as less_than_target,
            coalesce((sum_by_top_10/agg_by_top_10),0)*100 as top_10,coalesce((sum_by_approved/agg_by_approved),0)*100 approved
        from hits_by_tags h
        join total_hits t on h.siteid=t.siteid
        join last_30_days_article_published_Agg hbt on hbt.siteid=h.siteid and hbt.tags=h.tags
        where h.siteid = ', site, '
    ),
    categories_d as (
        select
            siteid as siteid ,
            ',label_val,' as label,'
            ,hint_val,' as hint,
            GROUP_CONCAT(tags ORDER BY FIELD(tags, ', order_statement_first, ', ''others'') SEPARATOR '','') AS cateogires,
            GROUP_CONCAT(COALESCE(less_than_target, 0) ORDER BY FIELD(tags, ', order_statement_first, ', ''others'') SEPARATOR '','') AS less_than_target,
            GROUP_CONCAT(approved ORDER BY FIELD(tags, ', order_statement_first, ', ''others'') SEPARATOR '','') AS approved,
            GROUP_CONCAT(top_10 ORDER BY FIELD(tags,', order_statement_first, ', ''others'') SEPARATOR '','') AS top_10
        from agg_data
        group by 1,2,3
    ),
    categories as (
        select
            siteid as siteid,
            label as label,
            hint as hint,  
            CONCAT(''"'', REPLACE(cateogires, '','', ''","''), ''"'') AS  cateogires,
            less_than_target as less_than_target ,
            approved as approved ,
            top_10 as top_10 
        from  categories_d
    ),
    json_data as
    (
        SELECT 
            siteid,
            label AS lab, 
            hint AS h,
            cateogires AS cat,
            CONCAT(''{"name": "Under mål", "data": ['', CAST(less_than_target AS CHAR), '']}, '',
                    ''{"name": "Godkendt", "data": ['', CAST(approved AS CHAR), '']}, '',
                    ''{"name": "Top 10%", "data": ['', CAST(top_10 AS CHAR), '']} '') AS series
        FROM categories
    ), 
    
    last_30_days_article_published_second as
    (
		SELECT 
			siteid as siteid,
            id,
            date as fdate,
			CASE
                WHEN tags like "%Hjælp mig med at forstå%" then "Hjælp mig med at forstå"
                WHEN tags like "%Inspirer mig%" then "Inspirer mig"
                WHEN tags like "%Giv mig en fordel%" then "Giv mig en fordel"
                ELSE ''others''
			END AS tags
        FROM prod.site_archive_post  
        WHERE date BETWEEN DATE_SUB(NOW(), INTERVAL 31 DAY) AND DATE_SUB(NOW(), INTERVAL 1 DAY) AND siteid = ', site, '
	),
    last_30_days_article_published_Agg_second as
    (
        SELECT
            siteid as siteid,tags,count(*) as agg
        from
        last_30_days_article_published_second 
		where siteid = ', site, '
		group by 1,2
    ),
    cta_per_article_second as
    (
        select
            e.siteid as siteid,
            e.id as postid,
            coalesce(sum(hits),0) as val
        from last_30_days_article_published_second e
        left join prod.events a on a.postid = e.id
        where e.siteid = ', site, ' and a.Event_Action = ',event_action,'
        group by 1,2
    ),
    agg_cta_per_article_second as
      (
		select 
			siteid as siteid,
            sum(val) as agg_sum 
		from  cta_per_article_second
		where siteid = ', site, '
		group by 1
	),
    less_tg_data_second as(
		select 
        
            l.id,
            l.tags ,
            a.val,
            min_cta,
            l.siteid,
			case when coalesce (val,0)<min_cta then 1 else 0 end as less_than_target,
            percent_rank() over ( order BY case when val>=min_cta then val-min_cta else 0 end) as percentile
		from last_30_days_article_published_second l
		left join cta_per_article_second a on l.siteid = a.siteid and l.id = a.postid
		left join prod.goals g on l.siteid = g.site_id and g.Date = l.fdate
		 where date  between DATE_SUB(NOW(), INTERVAL 31 DAY) and DATE_SUB(NOW(), INTERVAL 1 DAY) and g.site_id = ', site, '
	),
    hits_by_tags_second as
    (
        select
            siteid as siteid,
            tags,
            sum(less_than_target) as hits_tags,
		sum(case when percentile<=1 and percentile>=0.9 then val else 0 end) sum_by_top_10,
		sum(case when percentile>=0 and percentile<0.9 and less_than_target=0 then val else 0 end) sum_by_approved
		  from less_tg_data_second
		  where siteid = ', site, '
		group by 1,2
	),
    total_hits_second as
    (
        select 
            siteid as siteid ,
            sum(hits_tags) as total_tags_hit,
            sum(sum_by_top_10) as agg_by_top_10,
            sum(sum_by_approved) as agg_by_approved 
        from hits_by_tags_second
        where siteid = ', site, '
        group by 1
    ),
    agg_data_second as
    (
        select 
            h.siteid,
            h.tags,
            coalesce ((hits_tags/total_tags_hit),0)*100 as less_than_target,
            coalesce((sum_by_top_10/agg_by_top_10),0)*100 as top_10,coalesce((sum_by_approved/agg_by_approved),0)*100 approved
        from hits_by_tags_second h
        join total_hits_second t on h.siteid=t.siteid
        join last_30_days_article_published_Agg_second hbt on hbt.siteid=h.siteid and hbt.tags=h.tags
        where h.siteid = ', site, '
    ),
    categories_d_second as (
        select
            siteid as siteid ,
            ',label_val,' as label,'
            ,hint_val,' as hint,
            GROUP_CONCAT(tags ORDER BY FIELD(tags, ', tag_statement_additional, ', ''others'') SEPARATOR '','') AS cateogires,
            GROUP_CONCAT(COALESCE(less_than_target, 0) ORDER BY FIELD(tags, ', tag_statement_additional, ', ''others'') SEPARATOR '','') AS less_than_target,
            GROUP_CONCAT(approved ORDER BY FIELD(tags, ', tag_statement_additional, ', ''others'') SEPARATOR '','') AS approved,
            GROUP_CONCAT(top_10 ORDER BY FIELD(tags,', tag_statement_additional, ', ''others'') SEPARATOR '','') AS top_10
        from agg_data_second
        group by 1,2,3
    ),
    categories_second as (
        select
            siteid as siteid,
            label as label,
            hint as hint,  
            CONCAT(''"'', REPLACE(cateogires, '','', ''","''), ''"'') AS  cateogires,
            less_than_target as less_than_target ,
            approved as approved ,
            top_10 as top_10 
        from  categories_d_second
    ),
    json_data_second as
    (
        SELECT 
            siteid,
            label AS lab, 
            hint AS h,
            cateogires AS cat,
            CONCAT(''{"name": "Under mål", "data": ['', CAST(less_than_target AS CHAR), '']}, '',
                    ''{"name": "Godkendt", "data": ['', CAST(approved AS CHAR), '']}, '',
                    ''{"name": "Top 10%", "data": ['', CAST(top_10 AS CHAR), '']} '') AS series
        FROM categories_second
    ), 
    
    last_30_days_article_published_third as
    (
		SELECT 
			siteid as siteid,
            id,
            date as fdate,
			CASE
                WHEN tags_r like "%Long read%" then "Long read"
                WHEN tags_r like "%Kort og godt%" then "Kort og godt"
                WHEN tags_r like "%Q&amp%" then "Q&amp"
                WHEN tags_r like "%Best Practice%" then "Best Practice"
                WHEN tags_r like "%Viden og forskning%" then "Viden og forskning"
                ELSE ''others''
			END AS tags
        FROM prod.site_archive_post  
        WHERE date BETWEEN DATE_SUB(NOW(), INTERVAL 31 DAY) AND DATE_SUB(NOW(), INTERVAL 1 DAY) AND siteid = ', site, '
	),
    last_30_days_article_published_Agg_third as
    (
        SELECT
            siteid as siteid,tags,count(*) as agg
        from
        last_30_days_article_published_third 
		where siteid = ', site, '
		group by 1,2
    ),
    cta_per_article_third as
    (
        select
            e.siteid as siteid,
            e.id as postid,
            coalesce(sum(hits),0) as val
        from last_30_days_article_published_third e
        left join prod.events a on a.postid = e.id
        where e.siteid = ', site, ' and a.Event_Action = ',event_action,'
        group by 1,2
    ),
    agg_cta_per_article_third as
      (
		select 
			siteid as siteid,
            sum(val) as agg_sum 
		from  cta_per_article_third
		where siteid = ', site, '
		group by 1
	),
    less_tg_data_third as(
		select 
        
            l.id,
            l.tags ,
            a.val,
            min_cta,
            l.siteid,
			case when coalesce (val,0)<min_cta then 1 else 0 end as less_than_target,
            percent_rank() over ( order BY case when val>=min_cta then val-min_cta else 0 end) as percentile
		from last_30_days_article_published_third l
		left join cta_per_article_third a on l.siteid = a.siteid and l.id = a.postid
		left join prod.goals g on l.siteid = g.site_id and g.Date = l.fdate
		 where date  between DATE_SUB(NOW(), INTERVAL 31 DAY) and DATE_SUB(NOW(), INTERVAL 1 DAY) and g.site_id = ', site, '
	),
    hits_by_tags_third as
    (
        select
            siteid as siteid,
            tags,
            sum(less_than_target) as hits_tags,
		sum(case when percentile<=1 and percentile>=0.9 then val else 0 end) sum_by_top_10,
		sum(case when percentile>=0 and percentile<0.9 and less_than_target=0 then val else 0 end) sum_by_approved
		  from less_tg_data_third
		  where siteid = ', site, '
		group by 1,2
	),
    total_hits_third as
    (
        select 
            siteid as siteid ,
            sum(hits_tags) as total_tags_hit,
            sum(sum_by_top_10) as agg_by_top_10,
            sum(sum_by_approved) as agg_by_approved 
        from hits_by_tags_third
        where siteid = ', site, '
        group by 1
    ),
    agg_data_third as
    (
        select 
            h.siteid,
            h.tags,
            coalesce ((hits_tags/total_tags_hit),0)*100 as less_than_target,
            coalesce((sum_by_top_10/agg_by_top_10),0)*100 as top_10,coalesce((sum_by_approved/agg_by_approved),0)*100 approved
        from hits_by_tags_third h
        join total_hits_third t on h.siteid=t.siteid
        join last_30_days_article_published_Agg_third hbt on hbt.siteid=h.siteid and hbt.tags=h.tags
        where h.siteid = ', site, '
    ),
    categories_d_third as (
        select
            siteid as siteid ,
            ',label_val,' as label,'
            ,hint_val,' as hint,
            GROUP_CONCAT(tags ORDER BY FIELD(tags, ', order_statement_third, ', ''others'') SEPARATOR '','') AS cateogires,
            GROUP_CONCAT(COALESCE(less_than_target, 0) ORDER BY FIELD(tags, ', order_statement_third, ', ''others'') SEPARATOR '','') AS less_than_target,
            GROUP_CONCAT(approved ORDER BY FIELD(tags, ', order_statement_third, ', ''others'') SEPARATOR '','') AS approved,
            GROUP_CONCAT(top_10 ORDER BY FIELD(tags,', order_statement_third, ', ''others'') SEPARATOR '','') AS top_10
        from agg_data_third
        group by 1,2,3
    ),
    categories_third as (
        select
            siteid as siteid,
            label as label,
            hint as hint,  
            CONCAT(''"'', REPLACE(cateogires, '','', ''","''), ''"'') AS  cateogires,
            less_than_target as less_than_target ,
            approved as approved ,
            top_10 as top_10 
        from  categories_d_third
    ),
    json_data_third as
    (
        SELECT 
            siteid,
            label AS lab, 
            hint AS h,
            cateogires AS cat,
            CONCAT(''{"name": "Under mål", "data": ['', CAST(less_than_target AS CHAR), '']}, '',
                    ''{"name": "Godkendt", "data": ['', CAST(approved AS CHAR), '']}, '',
                    ''{"name": "Top 10%", "data": ['', CAST(top_10 AS CHAR), '']} '') AS series
        FROM categories_third
    )
    
    SELECT 
		CONCAT(
        ''{'',''"site":'',jd.siteid,'','',
        ''"data": {'',
        ''"label": "'', jd.lab, ''",'',
        ''"categories": ['', jd.cat, ''],'',
        ''"series": ['', jd.series, '']'',
        '',"defaultTitle":"',dtitle,'"'',
        '',"additional":[ {'',
        ''"title":"',title1,'",'',

        ''"data": {'',
        ''"label": "'',jnd.lab, ''",'',
        ''"categories": ['', jnd.cat, ''],'',
        ''"series": ['', jnd.series, '']'',
        ''}},'',
        ''{"title":"',title2,'",'',
        
        ''"data": {'',
        ''"label": "'', jrd.lab, ''",'',
        ''"categories": ['', jrd.cat, ''],'',
        ''"series": ['', jrd.series, '']'',
        ''}}]}}''
    
			) as json_data
		  FROM json_data jd
          CROSS join json_data_second jnd
          CROSS join json_data_third jrd;