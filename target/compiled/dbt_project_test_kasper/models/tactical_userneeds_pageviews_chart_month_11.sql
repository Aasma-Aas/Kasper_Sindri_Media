







CREATE DEFINER=`Site`@`%` PROCEDURE `stage`.`tactical_userneeds_pageviews_chart_month_11_dbt_test`(
	IN site INT,
    IN label_val VARCHAR(200),
    IN hint_val text,
    IN order_statement  VARCHAR(2000),
	IN order_statement_second VARCHAR(2000),
    IN order_statement_third VARCHAR(2000),
	IN dtitle VARCHAR(200),
    IN title1 VARCHAR(200),
    IN title2 VARCHAR(200),
    IN event_action VARCHAR(255)
)
BEGIN
    SET @sql_query = CONCAT('
    with 
    
    last_ytd_article_published as
    (
        SELECT
            siteid as siteid,
            id,
            date as fdate,
		    CASE
			    WHEN userneeds like "%Opdater mig%" then "Opdater mig"
			    WHEN userneeds like "%Forbind mig%" then "Forbind mig"
			    WHEN userneeds like "%Help mig med at forsta%" then "Hjælp mig med at forstå"
			    WHEN userneeds like "%Giv mig en fordel%" then "Giv mig en fordel"
			    WHEN userneeds like "%Underhold mig%" then "Underhold mig"
			    WHEN userneeds like "%Inspirer migg%" then "Inspirer mig"
			    ELSE ''others''
		    END AS tags
        FROM prod.site_archive_post   
        WHERE date between MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1)  and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY)
        
        AND siteid = ', site, '
AND (
    (tags NOT LIKE "%billedkunst%" 
    AND tags NOT LIKE "%børnehaveklassen%" 
    AND tags NOT LIKE "%dsa%" 
    AND tags NOT LIKE "%engelsk%" 
    AND tags NOT LIKE "%håndværk og design%" 
    AND tags NOT LIKE "%idræt%" 
    AND tags NOT LIKE "%kulturfag%" 
    AND tags NOT LIKE "%lærersenior%" 
    AND tags NOT LIKE "%lærerstuderende%" 
    AND tags NOT LIKE "%madkundskab%" 
    AND tags NOT LIKE "%musik%" 
    AND tags   NOT LIKE "%naturfag%" 
    AND tags   NOT LIKE "%plc%" 
    AND tags   NOT LIKE "%ppr%" 
    AND tags  NOT LIKE "%praktik%" 
    AND tags  NOT LIKE "%sosu%" 
    AND tags  NOT LIKE "%tyskfransk%" 
    AND tags NOT LIKE "%uu%")
    OR 
    (tags LIKE "%dansk%" 
    OR tags LIKE "%it,%" 
    OR tags LIKE ",%it,%" 
    OR tags LIKE "%,it%" 
    OR tags LIKE "%matematik%" 
    OR tags LIKE "%specialpædagogik%" 
    OR tags LIKE "%Skolepolitik%" 
    OR tags LIKE "%DLF%" 
    OR tags LIKE "%Skoleledelse%" 
    OR tags LIKE "%Psykisk arbejdsmiljø%" 
    OR tags LIKE "%Forskning%"
    )
    OR
    (tags NOT LIKE "%billedkunst%" 
    AND tags NOT LIKE "%børnehaveklassen%" 
    AND tags NOT LIKE "%dsa%" 
    AND tags NOT LIKE "%engelsk%" 
    AND tags NOT LIKE "%håndværk og design%" 
    AND tags NOT LIKE "%idræt%" 
    AND tags NOT LIKE "%kulturfag%" 
    AND tags NOT LIKE "%lærersenior%" 
    AND tags NOT LIKE "%lærerstuderende%" 
    AND tags NOT LIKE "%madkundskab%" 
    AND tags NOT LIKE "%musik%" 
    AND tags NOT LIKE "%naturfag%" 
    AND tags NOT LIKE "%plc%" 
    AND tags NOT LIKE "%ppr%" 
    AND tags NOT LIKE "%praktik%" 
    AND tags NOT LIKE "%sosu%" 
    AND tags NOT LIKE "%tyskfransk%" 
    AND tags NOT LIKE "%uu%")
    AND 
    (tags NOT LIKE "%dansk%" 
    AND tags NOT LIKE "%it,%" 
    AND tags NOT LIKE ",%it,%" 
    AND tags NOT LIKE "%,it%" 
    AND tags NOT LIKE "%matematik%" 
    AND tags NOT LIKE "%specialpædagogik%" 
    AND tags NOT LIKE "%Skolepolitik%" 
    AND tags NOT LIKE "%DLF%" 
    AND tags NOT LIKE "%Skoleledelse%" 
    AND tags NOT LIKE "%Psykisk arbejdsmiljø%" 
    AND tags NOT LIKE "%Forskning%")
)
),
    last_ytd_article_published_Agg as
    (
        SELECT
            siteid as siteid,tags,count(*) as agg
        from
        last_ytd_article_published 
		where siteid = ', site, '
		group by 1,2
    ),
    agg_next_click_per_article as 
      (
		SELECT  
			e.siteid as siteid,
            e.id as postid,
            e.tags,
            coalesce(sum(hits),0) as val,
			coalesce(sum(unique_pageviews),0) as page_view 
		from last_ytd_article_published  e
		left join prod.pages p on p.postid=e.id and p.siteid =e.siteid
		left join prod.events  a on a.postid=e.id and a.siteid = e.siteid and a.event_action = ',event_action,'
		where    e.siteid = ', site, '  
		group by 1,2,3
		),
        cta_per_article as
		( 
			SELECT
              e.siteid as siteid,
              e.postid, e.tags,
			round(coalesce((coalesce(val,0)/coalesce(page_view,1)),0.0)*100.0,2) as val 
            from agg_next_click_per_article  e
			where  e.siteid = ', site, '
			group by 1,2,3

		),
        agg_cta_per_article as(
		select siteid as siteid,tags,sum(val) as agg_sum from
        cta_per_article
		where siteid = ', site, '
		group by 1,2
		),
        less_tg_data as(
		select 
		postid,l.tags ,val,min_cta
		,a.siteid,
		case when val<min_cta then 1 else 0 end as less_than_target
		,percent_rank() over ( order BY case when val>=min_cta then val-min_cta else 0 end) as percentile
		 from last_ytd_article_published l
		join cta_per_article a on l.siteid = a.siteid and l.id = a.postid and l.tags=a.tags
		join prod.goals g on a.siteid = g.site_id and g.Date = l.fdate
		 where date  between DATE_SUB(NOW(), INTERVAL 31 DAY) and DATE_SUB(NOW(), INTERVAL 1 DAY) and g.site_id = ', site, '
		),
        counts as (
			select 
				siteid,
				tags,
				sum(less_than_target) as less_than_target_count,
				sum(case when percentile <= 1 and percentile >= 0.9 then 1 else 0 end) as top_10_count,
				sum(case when percentile >= 0 and percentile < 0.9 and less_than_target = 0 then 1 else 0 end) as approved_count
			from less_tg_data
			group by 1,2
		),
        hits_by_tags as(
			select 
				siteid as siteid,
				tags,
				sum(less_than_target) as hits_tags,
				sum(case when percentile<=1 and percentile>=0.9 then val else 0 end) sum_by_top_10,
				sum(case when percentile>=0 and percentile<0.9 and less_than_target=0 then val else 0 end) sum_by_approved
		   from less_tg_data
		   where siteid = ',site,'
		   group by 1,2)
           ,
           total_hits as(
			select 
				siteid as siteid ,
				sum(less_than_target_count) as total_tags_hit,
                sum(top_10_count) as agg_by_top_10,
                sum(approved_count) as agg_by_approved 
			from counts
			where  siteid = ',site,'
			group by 1)
            ,
            agg_data as(
			select 
				h.siteid as siteid ,
				h.tags,
				coalesce(less_than_target_count/total_tags_hit,0)*100 as less_than_target ,
				coalesce(top_10_count/agg_by_top_10,0)*100 as top_10,
				coalesce(approved_count/agg_by_approved,0)*100 as  approved
			from counts h
			join total_hits t on h.siteid=t.siteid
			join last_ytd_article_published_Agg hbt on hbt.siteid=h.siteid and hbt.tags=h.tags
			where h.siteid  = ',site,'
		),
        categories_d as (
			select 
				siteid as siteid ,
				 ',label_val,' as label,
                ',hint_val,' as hint,
				GROUP_CONCAT(tags ORDER BY FIELD(tags, ',order_statement,', ''others'' ) SEPARATOR '','') AS cateogires,
				GROUP_CONCAT(COALESCE(less_than_target, 0) ORDER BY FIELD(tags, ',order_statement,', ''others'' ) SEPARATOR '','') AS less_than_target,
			GROUP_CONCAT(approved ORDER BY FIELD(tags, ',order_statement,', ''others'' ) SEPARATOR '','') AS approved,
			GROUP_CONCAT(top_10 ORDER BY FIELD(tags, ',order_statement,', ''others'' ) SEPARATOR '','') AS top_10
		from agg_data
		group by 1,2,3
		),
        categories as (
		select siteid as siteid ,label as label, hint as hint,  
		 CONCAT(''"'', REPLACE(cateogires, '','', ''","''), ''"'') AS  cateogires,
		less_than_target as less_than_target ,
		approved as approved ,
		 top_10 as top_10 
		 from  categories_d
		),
        json_data as
		(
		select siteid,label as lab,hint as h,cateogires as cat,CONCAT(
		''{"name": "Under mål", "data": ['',cast(less_than_target as char),'']}''
		,'',{"name": "Over mål" ,"data": ['',cast(approved as char),'']}'','',
        {"name": "Top 10%" ,"data": ['',cast(top_10 as char),'']}'') 
        as series from categories
		), 
    
    last_ytd_article_published_second as
    (
        SELECT
            siteid as siteid,
            id,
            date as fdate,
		    CASE
			    WHEN Categories like "%Nyhed%" then "Nyhed"
			    WHEN Categories like "%Medlemsinfo%" then "Medlemsinfo"
			    WHEN Categories like "%Reportage%" then "Reportage"
			    WHEN Categories like "%Artikel%" then "Artikel"
			    WHEN Categories like "%Det ku ske for dig%" then "Det ku ske for dig"
			    WHEN Categories like "%Jeg mener%" then "Jeg mener"
			    WHEN Categories like "%Videoartikel%" then "Videoartikel"
			    ELSE ''others''
		    END AS tags
        FROM prod.site_archive_post   
        WHERE date between MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1)  and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY)
        
        AND siteid = ', site, '
AND (
    (tags NOT LIKE "%billedkunst%" 
    AND tags NOT LIKE "%børnehaveklassen%" 
    AND tags NOT LIKE "%dsa%" 
    AND tags NOT LIKE "%engelsk%" 
    AND tags NOT LIKE "%håndværk og design%" 
    AND tags NOT LIKE "%idræt%" 
    AND tags NOT LIKE "%kulturfag%" 
    AND tags NOT LIKE "%lærersenior%" 
    AND tags NOT LIKE "%lærerstuderende%" 
    AND tags NOT LIKE "%madkundskab%" 
    AND tags NOT LIKE "%musik%" 
    AND tags   NOT LIKE "%naturfag%" 
    AND tags   NOT LIKE "%plc%" 
    AND tags   NOT LIKE "%ppr%" 
    AND tags  NOT LIKE "%praktik%" 
    AND tags  NOT LIKE "%sosu%" 
    AND tags  NOT LIKE "%tyskfransk%" 
    AND tags NOT LIKE "%uu%")
    OR 
    (tags LIKE "%dansk%" 
    OR tags LIKE "%it,%" 
    OR tags LIKE ",%it,%" 
    OR tags LIKE "%,it%" 
    OR tags LIKE "%matematik%" 
    OR tags LIKE "%specialpædagogik%" 
    OR tags LIKE "%Skolepolitik%" 
    OR tags LIKE "%DLF%" 
    OR tags LIKE "%Skoleledelse%" 
    OR tags LIKE "%Psykisk arbejdsmiljø%" 
    OR tags LIKE "%Forskning%"
    )
    OR
    (tags NOT LIKE "%billedkunst%" 
    AND tags NOT LIKE "%børnehaveklassen%" 
    AND tags NOT LIKE "%dsa%" 
    AND tags NOT LIKE "%engelsk%" 
    AND tags NOT LIKE "%håndværk og design%" 
    AND tags NOT LIKE "%idræt%" 
    AND tags NOT LIKE "%kulturfag%" 
    AND tags NOT LIKE "%lærersenior%" 
    AND tags NOT LIKE "%lærerstuderende%" 
    AND tags NOT LIKE "%madkundskab%" 
    AND tags NOT LIKE "%musik%" 
    AND tags NOT LIKE "%naturfag%" 
    AND tags NOT LIKE "%plc%" 
    AND tags NOT LIKE "%ppr%" 
    AND tags NOT LIKE "%praktik%" 
    AND tags NOT LIKE "%sosu%" 
    AND tags NOT LIKE "%tyskfransk%" 
    AND tags NOT LIKE "%uu%")
    AND 
    (tags NOT LIKE "%dansk%" 
    AND tags NOT LIKE "%it,%" 
    AND tags NOT LIKE ",%it,%" 
    AND tags NOT LIKE "%,it%" 
    AND tags NOT LIKE "%matematik%" 
    AND tags NOT LIKE "%specialpædagogik%" 
    AND tags NOT LIKE "%Skolepolitik%" 
    AND tags NOT LIKE "%DLF%" 
    AND tags NOT LIKE "%Skoleledelse%" 
    AND tags NOT LIKE "%Psykisk arbejdsmiljø%" 
    AND tags NOT LIKE "%Forskning%")
)
),
    last_ytd_article_published_Agg_second as
    (
        SELECT
            siteid as siteid,tags,count(*) as agg
        from
        last_ytd_article_published_second 
		where siteid = ', site, '
		group by 1,2
    ),
    agg_next_click_per_article_second as 
      (
		SELECT  
			e.siteid as siteid,
            e.id as postid,
            e.tags,
            coalesce(sum(hits),0) as val,
			coalesce(sum(unique_pageviews),0) as page_view 
		from last_ytd_article_published_second  e
		left join prod.pages p on p.postid=e.id and p.siteid =e.siteid
		left join prod.events  a on a.postid=e.id and a.siteid = e.siteid and a.event_action = ',event_action,'
		where    e.siteid = ', site, '  
		group by 1,2,3
		),
        cta_per_article_second as
		( 
			SELECT
              e.siteid as siteid,
              e.postid, e.tags,
			round(coalesce((coalesce(val,0)/coalesce(page_view,1)),0.0)*100.0,2) as val 
            from agg_next_click_per_article_second  e
			where  e.siteid = ', site, '
			group by 1,2,3

		),
        agg_cta_per_article_second as(
		select siteid as siteid,tags,sum(val) as agg_sum from
        cta_per_article_second
		where siteid = ', site, '
		group by 1,2
		),
        less_tg_data_second as(
		select 
		postid,l.tags ,val,min_cta
		,a.siteid,
		case when val<min_cta then 1 else 0 end as less_than_target
		,percent_rank() over ( order BY case when val>=min_cta then val-min_cta else 0 end) as percentile
		 from last_ytd_article_published_second l
		join cta_per_article_second a on l.siteid = a.siteid and l.id = a.postid and l.tags=a.tags
		join prod.goals g on a.siteid = g.site_id and g.Date = l.fdate
		 where date  between DATE_SUB(NOW(), INTERVAL 31 DAY) and DATE_SUB(NOW(), INTERVAL 1 DAY) and g.site_id = ', site, '
		),
        counts_second as (
			select 
				siteid,
				tags,
				sum(less_than_target) as less_than_target_count,
				sum(case when percentile <= 1 and percentile >= 0.9 then 1 else 0 end) as top_10_count,
				sum(case when percentile >= 0 and percentile < 0.9 and less_than_target = 0 then 1 else 0 end) as approved_count
			from less_tg_data_second
			group by 1,2
		),
        hits_by_tags_second as(
			select 
				siteid as siteid,
				tags,
				sum(less_than_target) as hits_tags,
				sum(case when percentile<=1 and percentile>=0.9 then val else 0 end) sum_by_top_10,
				sum(case when percentile>=0 and percentile<0.9 and less_than_target=0 then val else 0 end) sum_by_approved
		   from less_tg_data_second
		   where siteid = ',site,'
		   group by 1,2)
           ,
           total_hits_second as(
			select 
				siteid as siteid ,
				sum(less_than_target_count) as total_tags_hit,
                sum(top_10_count) as agg_by_top_10,
                sum(approved_count) as agg_by_approved 
			from counts_second
			where  siteid = ',site,'
			group by 1)
            ,
            agg_data_second as(
			select 
				h.siteid as siteid ,
				h.tags,
				coalesce(less_than_target_count/total_tags_hit,0)*100 as less_than_target ,
				coalesce(top_10_count/agg_by_top_10,0)*100 as top_10,
				coalesce(approved_count/agg_by_approved,0)*100 as  approved
			from counts_second h
			join total_hits_second t on h.siteid=t.siteid
			join last_ytd_article_published_Agg_second hbt on hbt.siteid=h.siteid and hbt.tags=h.tags
			where h.siteid  = ',site,'
		),
        categories_d_second as (
			select 
				siteid as siteid ,
				 ',label_val,' as label,
                ',hint_val,' as hint,
				GROUP_CONCAT(tags ORDER BY FIELD(tags, ',order_statement_second,', ''others'' ) SEPARATOR '','') AS cateogires,
				GROUP_CONCAT(COALESCE(less_than_target, 0) ORDER BY FIELD(tags, ',order_statement_second,', ''others'' ) SEPARATOR '','') AS less_than_target,
			GROUP_CONCAT(approved ORDER BY FIELD(tags, ',order_statement_second,', ''others'' ) SEPARATOR '','') AS approved,
			GROUP_CONCAT(top_10 ORDER BY FIELD(tags, ',order_statement_second,', ''others'' ) SEPARATOR '','') AS top_10
		from agg_data_second
		group by 1,2,3
		),
        categories_second as (
		select siteid as siteid ,label as label, hint as hint,  
		 CONCAT(''"'', REPLACE(cateogires, '','', ''","''), ''"'') AS  cateogires,
		less_than_target as less_than_target ,
		approved as approved ,
		 top_10 as top_10 
		 from  categories_d_second
		),
        json_data_second as
		(
		select siteid,label as lab,hint as h,cateogires as cat,CONCAT(
		''{"name": "Under mål", "data": ['',cast(less_than_target as char),'']}''
		,'',{"name": "Over mål" ,"data": ['',cast(approved as char),'']}'','',
        {"name": "Top 10%" ,"data": ['',cast(top_10 as char),'']}'') 
        as series from categories_second
		), 
    
    last_ytd_article_published_third as
    (
        SELECT
            siteid as siteid,
            id,
            date as fdate,
		    CASE
			    WHEN Tags like "%Slagterindustri%" then "Slagterindustri"
			    WHEN Tags like "%Fødevareindustri%" then "Fødevareindustri"
			    WHEN Tags like "%Mejeri%" then "Mejeri"
			    WHEN Tags like "%Butik%" then "Butik"
			    WHEN Tags like "%Alle brancher%" then "Alle brancher"
			    ELSE ''others''
		    END AS tags
        FROM prod.site_archive_post   
        WHERE date between MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1)  and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY)
        
        AND siteid = ', site, '
AND (
    (tags NOT LIKE "%billedkunst%" 
    AND tags NOT LIKE "%børnehaveklassen%" 
    AND tags NOT LIKE "%dsa%" 
    AND tags NOT LIKE "%engelsk%" 
    AND tags NOT LIKE "%håndværk og design%" 
    AND tags NOT LIKE "%idræt%" 
    AND tags NOT LIKE "%kulturfag%" 
    AND tags NOT LIKE "%lærersenior%" 
    AND tags NOT LIKE "%lærerstuderende%" 
    AND tags NOT LIKE "%madkundskab%" 
    AND tags NOT LIKE "%musik%" 
    AND tags   NOT LIKE "%naturfag%" 
    AND tags   NOT LIKE "%plc%" 
    AND tags   NOT LIKE "%ppr%" 
    AND tags  NOT LIKE "%praktik%" 
    AND tags  NOT LIKE "%sosu%" 
    AND tags  NOT LIKE "%tyskfransk%" 
    AND tags NOT LIKE "%uu%")
    OR 
    (tags LIKE "%dansk%" 
    OR tags LIKE "%it,%" 
    OR tags LIKE ",%it,%" 
    OR tags LIKE "%,it%" 
    OR tags LIKE "%matematik%" 
    OR tags LIKE "%specialpædagogik%" 
    OR tags LIKE "%Skolepolitik%" 
    OR tags LIKE "%DLF%" 
    OR tags LIKE "%Skoleledelse%" 
    OR tags LIKE "%Psykisk arbejdsmiljø%" 
    OR tags LIKE "%Forskning%"
    )
    OR
    (tags NOT LIKE "%billedkunst%" 
    AND tags NOT LIKE "%børnehaveklassen%" 
    AND tags NOT LIKE "%dsa%" 
    AND tags NOT LIKE "%engelsk%" 
    AND tags NOT LIKE "%håndværk og design%" 
    AND tags NOT LIKE "%idræt%" 
    AND tags NOT LIKE "%kulturfag%" 
    AND tags NOT LIKE "%lærersenior%" 
    AND tags NOT LIKE "%lærerstuderende%" 
    AND tags NOT LIKE "%madkundskab%" 
    AND tags NOT LIKE "%musik%" 
    AND tags NOT LIKE "%naturfag%" 
    AND tags NOT LIKE "%plc%" 
    AND tags NOT LIKE "%ppr%" 
    AND tags NOT LIKE "%praktik%" 
    AND tags NOT LIKE "%sosu%" 
    AND tags NOT LIKE "%tyskfransk%" 
    AND tags NOT LIKE "%uu%")
    AND 
    (tags NOT LIKE "%dansk%" 
    AND tags NOT LIKE "%it,%" 
    AND tags NOT LIKE ",%it,%" 
    AND tags NOT LIKE "%,it%" 
    AND tags NOT LIKE "%matematik%" 
    AND tags NOT LIKE "%specialpædagogik%" 
    AND tags NOT LIKE "%Skolepolitik%" 
    AND tags NOT LIKE "%DLF%" 
    AND tags NOT LIKE "%Skoleledelse%" 
    AND tags NOT LIKE "%Psykisk arbejdsmiljø%" 
    AND tags NOT LIKE "%Forskning%")
)
),
    last_ytd_article_published_Agg_third as
    (
        SELECT
            siteid as siteid,tags,count(*) as agg
        from
        last_ytd_article_published_third 
		where siteid = ', site, '
		group by 1,2
    ),
    agg_next_click_per_article_third as 
      (
		SELECT  
			e.siteid as siteid,
            e.id as postid,
            e.tags,
            coalesce(sum(hits),0) as val,
			coalesce(sum(unique_pageviews),0) as page_view 
		from last_ytd_article_published_third  e
		left join prod.pages p on p.postid=e.id and p.siteid =e.siteid
		left join prod.events  a on a.postid=e.id and a.siteid = e.siteid and a.event_action = ',event_action,'
		where    e.siteid = ', site, '  
		group by 1,2,3
		),
        cta_per_article_third as
		( 
			SELECT
              e.siteid as siteid,
              e.postid, e.tags,
			round(coalesce((coalesce(val,0)/coalesce(page_view,1)),0.0)*100.0,2) as val 
            from agg_next_click_per_article_third  e
			where  e.siteid = ', site, '
			group by 1,2,3

		),
        agg_cta_per_article_third as(
		select siteid as siteid,tags,sum(val) as agg_sum from
        cta_per_article_third
		where siteid = ', site, '
		group by 1,2
		),
        less_tg_data_third as(
		select 
		postid,l.tags ,val,min_cta
		,a.siteid,
		case when val<min_cta then 1 else 0 end as less_than_target
		,percent_rank() over ( order BY case when val>=min_cta then val-min_cta else 0 end) as percentile
		 from last_ytd_article_published_third l
		join cta_per_article_third a on l.siteid = a.siteid and l.id = a.postid and l.tags=a.tags
		join prod.goals g on a.siteid = g.site_id and g.Date = l.fdate
		 where date  between DATE_SUB(NOW(), INTERVAL 31 DAY) and DATE_SUB(NOW(), INTERVAL 1 DAY) and g.site_id = ', site, '
		),
        counts_third as (
			select 
				siteid,
				tags,
				sum(less_than_target) as less_than_target_count,
				sum(case when percentile <= 1 and percentile >= 0.9 then 1 else 0 end) as top_10_count,
				sum(case when percentile >= 0 and percentile < 0.9 and less_than_target = 0 then 1 else 0 end) as approved_count
			from less_tg_data_third
			group by 1,2
		),
        hits_by_tags_third as(
			select 
				siteid as siteid,
				tags,
				sum(less_than_target) as hits_tags,
				sum(case when percentile<=1 and percentile>=0.9 then val else 0 end) sum_by_top_10,
				sum(case when percentile>=0 and percentile<0.9 and less_than_target=0 then val else 0 end) sum_by_approved
		   from less_tg_data_third
		   where siteid = ',site,'
		   group by 1,2)
           ,
           total_hits_third as(
			select 
				siteid as siteid ,
				sum(less_than_target_count) as total_tags_hit,
                sum(top_10_count) as agg_by_top_10,
                sum(approved_count) as agg_by_approved 
			from counts_third
			where  siteid = ',site,'
			group by 1)
            ,
            agg_data_third as(
			select 
				h.siteid as siteid ,
				h.tags,
				coalesce(less_than_target_count/total_tags_hit,0)*100 as less_than_target ,
				coalesce(top_10_count/agg_by_top_10,0)*100 as top_10,
				coalesce(approved_count/agg_by_approved,0)*100 as  approved
			from counts_third h
			join total_hits_third t on h.siteid=t.siteid
			join last_ytd_article_published_Agg_third hbt on hbt.siteid=h.siteid and hbt.tags=h.tags
			where h.siteid  = ',site,'
		),
        categories_d_third as (
			select 
				siteid as siteid ,
				 ',label_val,' as label,
                ',hint_val,' as hint,
				GROUP_CONCAT(tags ORDER BY FIELD(tags, ',order_statement_third,', ''others'' ) SEPARATOR '','') AS cateogires,
				GROUP_CONCAT(COALESCE(less_than_target, 0) ORDER BY FIELD(tags, ',order_statement_third,', ''others'' ) SEPARATOR '','') AS less_than_target,
			GROUP_CONCAT(approved ORDER BY FIELD(tags, ',order_statement_third,', ''others'' ) SEPARATOR '','') AS approved,
			GROUP_CONCAT(top_10 ORDER BY FIELD(tags, ',order_statement_third,', ''others'' ) SEPARATOR '','') AS top_10
		from agg_data_third
		group by 1,2,3
		),
        categories_third as (
		select siteid as siteid ,label as label, hint as hint,  
		 CONCAT(''"'', REPLACE(cateogires, '','', ''","''), ''"'') AS  cateogires,
		less_than_target as less_than_target ,
		approved as approved ,
		 top_10 as top_10 
		 from  categories_d_third
		),
        json_data_third as
		(
		select siteid,label as lab,hint as h,cateogires as cat,CONCAT(
		''{"name": "Under mål", "data": ['',cast(less_than_target as char),'']}''
		,'',{"name": "Over mål" ,"data": ['',cast(approved as char),'']}'','',
        {"name": "Top 10%" ,"data": ['',cast(top_10 as char),'']}'') 
        as series from categories_third
		)
    

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
                
                ,''{'',
                ''"title":"',title1,'",'',
                ''"data": {'',
                ''"label": "'',json_data_second.lab, ''",'',
                ''"categories": ['', json_data_second.cat, ''],'',
                ''"series": ['', json_data_second.series, '']'',
                ''}}''
                
                , '',''
                
                
                ,''{'',
                ''"title":"',title2,'",'',
                ''"data": {'',
                ''"label": "'',json_data_third.lab, ''",'',
                ''"categories": ['', json_data_third.cat, ''],'',
                ''"series": ['', json_data_third.series, '']'',
                ''}}''
                
                
        '']}}''
    
			) as json_data
		  FROM json_data jd
          
          CROSS join json_data_second
          
          CROSS join json_data_third
          ;