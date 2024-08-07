














CREATE DEFINER=`Site`@`%` PROCEDURE `prod`.`tactical_category_pageviews_chart_ytd_dbt_15`()
BEGIN

    with 
	 
    last_30_days_article_published as(
        SELECT siteid as siteid,id,  date as fdate,
            CASE
			 WHEN userneeds  like   "%Opdater mig%"  THEN "Opdater mig"  WHEN userneeds  like   "%Forbind mig%"  THEN "Forbind mig"  WHEN userneeds  like   "%Help mig med at forsta%"  THEN "Hjælp mig med at forstå"  WHEN userneeds  like   "%Hjælp mig med at forstå%"  THEN "Hjælp mig med at forstå"  WHEN userneeds  like   "%Giv mig en fordel%"  THEN "Giv mig en fordel"  WHEN userneeds  like   "%Underhold mig%"  THEN "Underhold mig"  WHEN userneeds  like   "%Inspirer mig%"  THEN "Inspirer mig" 
			ELSE 'others'
			END AS tags
            FROM prod.site_archive_post  
        where 
       date
between MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1)  and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY)
        
		AND siteid = 15
		 ),
    cta_per_article as
    (
        select  a.siteid as siteid,a.id as postid,a.tags,coalesce(sum(unique_pageviews),0) as val from
        last_30_days_article_published a 
        left join prod.pages e  on e.postid=a.id and e.siteid = a.siteid
        group by 1,2,3
    ),
        agg_cta_per_article as(
        select siteid as siteid,tags, coalesce(sum(val), 0) as agg_sum  from  cta_per_article
        where siteid= 15 
        group by 1,2
    ),
        last_30_days_article_published_Agg as(
        select siteid as siteid,tags,count(*) as agg from  cta_per_article
        where siteid = 15 
        group by 1,2
    ),
    less_tg_data as(
        select 
        postid,l.tags ,val,Min_pageviews
        ,a.siteid,
        case when val<Min_pageviews then 1 else 0 end as less_than_target
        ,percent_rank() over ( order BY case when val>=Min_pageviews then val-Min_pageviews else 0 end) as percentile
            from last_30_days_article_published l
        join cta_per_article a on l.siteid = a.siteid and l.id = a.postid and l.tags=a.tags
        join prod.goals g on a.siteid = g.site_id and g.Date = l.fdate
        where date
between MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1)  and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY)  and a.siteid = 15 
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
    )
    ,
    hits_by_tags as(
        select 
            siteid as siteid,
            tags,
            sum(less_than_target) as hits_tags,
            sum(case when percentile<=1 and percentile>=0.9 then val else 0 end) sum_by_top_10,
            sum(case when percentile>=0 and percentile<0.9 and less_than_target=0 then val else 0 end) sum_by_approved
        from less_tg_data
        where siteid = 15
        group by 1,2
    ),

    total_hits as(
        select 
            siteid as siteid ,
            sum(less_than_target_count) as total_tags_hit,
            sum(top_10_count) as agg_by_top_10,
            sum(approved_count) as agg_by_approved 
        from counts
        where  siteid =15
        group by 1
    ),
    agg_data as(
        select 
            h.siteid as siteid ,
            h.tags,
            coalesce(less_than_target_count/total_tags_hit,0)*100 as less_than_target ,
            coalesce(top_10_count/agg_by_top_10,0)*100 as top_10,
            coalesce(approved_count/agg_by_approved,0)*100 as  approved
        from counts h
        join total_hits t on h.siteid=t.siteid
        join last_30_days_article_published_Agg hbt on hbt.siteid=h.siteid and hbt.tags=h.tags
        where h.siteid  = 15
    ),
    categories_d as
    (
		select 
			siteid as siteid ,
			'Artikler ift. tags og sidevisninger' as label,
            'Artikler publiceret år til dato grupperet i tre kategorier: Artikler under sidevisningsmål | Artikler, der klarede mål | Top 10% bedste artikler ift. sidevisninger. For hver kategori vises hvor stor andel der er publiceret indenfor hvert brugerbehov, så det er muligt at se forskelle på tværs af kategorierne.' as hint,
			GROUP_CONCAT(tags ORDER BY FIELD(tags, 'Opdater mig','Forbind mig','Hjælp mig med at forstå','Hjælp mig med at forstå','Giv mig en fordel','Underhold mig','Inspirer mig','','', 'others' ) SEPARATOR ',') 
			AS cateogires,
			GROUP_CONCAT(COALESCE(less_than_target, 0) ORDER BY FIELD(tags, 'Opdater mig','Forbind mig','Hjælp mig med at forstå','Hjælp mig med at forstå','Giv mig en fordel','Underhold mig','Inspirer mig','','', 'others' ) SEPARATOR ',') 
			AS less_than_target,
			GROUP_CONCAT(approved ORDER BY FIELD(tags, 'Opdater mig','Forbind mig','Hjælp mig med at forstå','Hjælp mig med at forstå','Giv mig en fordel','Underhold mig','Inspirer mig','','', 'others' ) SEPARATOR ',') 
			AS approved,
			GROUP_CONCAT(top_10 ORDER BY FIELD(tags, 'Opdater mig','Forbind mig','Hjælp mig med at forstå','Hjælp mig med at forstå','Giv mig en fordel','Underhold mig','Inspirer mig','','', 'others' ) SEPARATOR ',') 
			AS top_10
		from agg_data
		group by 1,2,3
	),
    categories as 
    (
		select 
			siteid as siteid ,
			label as label,
            hint as hint,  
			CONCAT('"', REPLACE(cateogires, ',', '","'), '"') AS  cateogires,
			less_than_target as less_than_target ,
			approved as approved ,
			top_10 as top_10 
		from  categories_d
	),
    json_data as
		(
		select siteid,label as lab,hint as h,cateogires as cat,CONCAT(
		'{"name": "Under mål", "data": [',cast(less_than_target as char),']}'
		,',{"name": "Over mål" ,"data": [',cast(approved as char),']}',',
        {"name": "Top 10%" ,"data": [',cast(top_10 as char),']}') 
        as series from categories
		), 
    last_30_days_article_published_second as(
        SELECT siteid as siteid,id,  date as fdate,
            CASE
			 WHEN Categories  like   "%Leder%"  THEN "Leder"  WHEN Categories  like   "%Presse%"  THEN "Presse"  WHEN Categories  like   "%Privatansat%"  THEN "Privatansat"  WHEN Categories  like   "%Plejefamilie%"  THEN "Plejefamilie"  WHEN Categories  like   "%Tillidsrepræsentant%"  THEN "Tillidsrepræsentant"  WHEN Categories  like   "%Lærere%"  THEN "Lærere"  WHEN Categories  like   "%Arbejdsmiljørepræsentant%"  THEN "Arbejdsmiljørepræsentant"  WHEN Categories  like   "%TRIO%"  THEN "TRIO"  WHEN Categories  like   "%Senior%"  THEN "Senior" 
			ELSE 'others'
			END AS tags
            FROM prod.site_archive_post  
        where 
       date
between MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1)  and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY)
        
		AND siteid = 15
		 ),
    cta_per_article_second as
    (
        select  a.siteid as siteid,a.id as postid,a.tags,coalesce(sum(unique_pageviews),0) as val from
        last_30_days_article_published_second a 
        left join prod.pages e  on e.postid=a.id and e.siteid = a.siteid
        group by 1,2,3
    ),
        agg_cta_per_article_second as(
        select siteid as siteid,tags, coalesce(sum(val), 0) as agg_sum  from  cta_per_article_second
        where siteid= 15 
        group by 1,2
    ),
        last_30_days_article_published_Agg_second as(
        select siteid as siteid,tags,count(*) as agg from  cta_per_article_second
        where siteid = 15 
        group by 1,2
    ),
    less_tg_data_second as(
        select 
        postid,l.tags ,val,Min_pageviews
        ,a.siteid,
        case when val<Min_pageviews then 1 else 0 end as less_than_target
        ,percent_rank() over ( order BY case when val>=Min_pageviews then val-Min_pageviews else 0 end) as percentile
            from last_30_days_article_published_second l
        join cta_per_article_second a on l.siteid = a.siteid and l.id = a.postid and l.tags=a.tags
        join prod.goals g on a.siteid = g.site_id and g.Date = l.fdate
        where date
between MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1)  and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY)  and a.siteid = 15 
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
    )
    ,
    hits_by_tags_second as(
        select 
            siteid as siteid,
            tags,
            sum(less_than_target) as hits_tags,
            sum(case when percentile<=1 and percentile>=0.9 then val else 0 end) sum_by_top_10,
            sum(case when percentile>=0 and percentile<0.9 and less_than_target=0 then val else 0 end) sum_by_approved
        from less_tg_data
        where siteid = 15
        group by 1,2
    ),

    total_hits_second as(
        select 
            siteid as siteid ,
            sum(less_than_target_count) as total_tags_hit,
            sum(top_10_count) as agg_by_top_10,
            sum(approved_count) as agg_by_approved 
        from counts_second
        where  siteid =15
        group by 1
    ),
    agg_data_second as(
        select 
            h.siteid as siteid ,
            h.tags,
            coalesce(less_than_target_count/total_tags_hit,0)*100 as less_than_target ,
            coalesce(top_10_count/agg_by_top_10,0)*100 as top_10,
            coalesce(approved_count/agg_by_approved,0)*100 as  approved
        from counts_second h
        join total_hits_second t on h.siteid=t.siteid
        join last_30_days_article_published_Agg_second hbt on hbt.siteid=h.siteid and hbt.tags=h.tags
        where h.siteid  = 15
    ),
    categories_d_second as
    (
		select 
			siteid as siteid ,
			'Artikler ift. tags og sidevisninger' as label,
            'Artikler publiceret år til dato grupperet i tre kategorier: Artikler under sidevisningsmål | Artikler, der klarede mål | Top 10% bedste artikler ift. sidevisninger. For hver kategori vises hvor stor andel der er publiceret indenfor hvert brugerbehov, så det er muligt at se forskelle på tværs af kategorierne.' as hint,
			GROUP_CONCAT(tags ORDER BY FIELD(tags, 'Leder','Presse','Privatansat','Plejefamilie','Tillidsrepræsentant','Lærere','Arbejdsmiljørepræsentant','TRIO','Senior','', 'others' ) SEPARATOR ',') 
			AS cateogires,
			GROUP_CONCAT(COALESCE(less_than_target, 0) ORDER BY FIELD(tags, 'Leder','Presse','Privatansat','Plejefamilie','Tillidsrepræsentant','Lærere','Arbejdsmiljørepræsentant','TRIO','Senior','', 'others' ) SEPARATOR ',') 
			AS less_than_target,
			GROUP_CONCAT(approved ORDER BY FIELD(tags, 'Leder','Presse','Privatansat','Plejefamilie','Tillidsrepræsentant','Lærere','Arbejdsmiljørepræsentant','TRIO','Senior','', 'others' ) SEPARATOR ',') 
			AS approved,
			GROUP_CONCAT(top_10 ORDER BY FIELD(tags, 'Leder','Presse','Privatansat','Plejefamilie','Tillidsrepræsentant','Lærere','Arbejdsmiljørepræsentant','TRIO','Senior','', 'others' ) SEPARATOR ',') 
			AS top_10
		from agg_data_second
		group by 1,2,3
	),
    categories_second as 
    (
		select 
			siteid as siteid ,
			label as label,
            hint as hint,  
			CONCAT('"', REPLACE(cateogires, ',', '","'), '"') AS  cateogires,
			less_than_target as less_than_target ,
			approved as approved ,
			top_10 as top_10 
		from  categories_d_second
	),
    json_data_second as
		(
		select siteid,label as lab,hint as h,cateogires as cat,CONCAT(
		'{"name": "Under mål", "data": [',cast(less_than_target as char),']}'
		,',{"name": "Over mål" ,"data": [',cast(approved as char),']}',',
        {"name": "Top 10%" ,"data": [',cast(top_10 as char),']}') 
        as series from categories_second
		), 
    last_30_days_article_published_third as(
        SELECT siteid as siteid,id,  date as fdate,
            CASE
			 WHEN Tags  REGEXP  "Børn og unge|Anbringelse|Sikrede institutioner"  THEN "Børn og unge"  WHEN Tags  REGEXP  "Socialpolitik|Nyheder|Forbund og a-kasse|Coronavirus"  THEN "Andet"  WHEN Tags  REGEXP  "Autisme|Udviklingshandicap|Botilbud|Magtanvendelse|Seksualitet|Domfældte"  THEN "Handicap"  WHEN Tags  REGEXP  "Psykiatri|Misbrug|Væresteder|Hjemløse|Herberg og forsorgshjem|Marginaliserede"  THEN "Psykiatri og udsathed"  WHEN Tags  REGEXP  "Socialpædagogisk faglighed|Socialpædagogisk praksis|Metoder og tilgange|Etik"  THEN "Socialpædagogisk faglighed"  WHEN Tags  REGEXP  "Ansættelsesvilkår|Opsigelse|Overenskomst|Arbejdstid|Løn|Ferie|Barsel|Senior |Ligestilling|Job og karriere|Uddannelse|Meritpædagoguddannelse|Kompetenceudvikling |Efteruddannelse|Arbejdsmiljø|Stress|Sygdom|Vold og trusler|Arbejdsskade|PTSD|A-kasse|Lønforsikring|Ledighed|Efterløn|Dagpenge|Arbejdsløshed"  THEN "Arbejdsliv og vilkår" 
			ELSE 'others'
			END AS tags
            FROM prod.site_archive_post  
        where 
       date
between MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1)  and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY)
        
		AND siteid = 15
		 ),
    cta_per_article_third as
    (
        select  a.siteid as siteid,a.id as postid,a.tags,coalesce(sum(unique_pageviews),0) as val from
        last_30_days_article_published_third a 
        left join prod.pages e  on e.postid=a.id and e.siteid = a.siteid
        group by 1,2,3
    ),
        agg_cta_per_article_third as(
        select siteid as siteid,tags, coalesce(sum(val), 0) as agg_sum  from  cta_per_article_third
        where siteid= 15 
        group by 1,2
    ),
        last_30_days_article_published_Agg_third as(
        select siteid as siteid,tags,count(*) as agg from  cta_per_article_third
        where siteid = 15 
        group by 1,2
    ),
    less_tg_data_third as(
        select 
        postid,l.tags ,val,Min_pageviews
        ,a.siteid,
        case when val<Min_pageviews then 1 else 0 end as less_than_target
        ,percent_rank() over ( order BY case when val>=Min_pageviews then val-Min_pageviews else 0 end) as percentile
            from last_30_days_article_published_third l
        join cta_per_article_third a on l.siteid = a.siteid and l.id = a.postid and l.tags=a.tags
        join prod.goals g on a.siteid = g.site_id and g.Date = l.fdate
        where date
between MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1)  and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY)  and a.siteid = 15 
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
    )
    ,
    hits_by_tags_third as(
        select 
            siteid as siteid,
            tags,
            sum(less_than_target) as hits_tags,
            sum(case when percentile<=1 and percentile>=0.9 then val else 0 end) sum_by_top_10,
            sum(case when percentile>=0 and percentile<0.9 and less_than_target=0 then val else 0 end) sum_by_approved
        from less_tg_data
        where siteid = 15
        group by 1,2
    ),

    total_hits_third as(
        select 
            siteid as siteid ,
            sum(less_than_target_count) as total_tags_hit,
            sum(top_10_count) as agg_by_top_10,
            sum(approved_count) as agg_by_approved 
        from counts_third
        where  siteid =15
        group by 1
    ),
    agg_data_third as(
        select 
            h.siteid as siteid ,
            h.tags,
            coalesce(less_than_target_count/total_tags_hit,0)*100 as less_than_target ,
            coalesce(top_10_count/agg_by_top_10,0)*100 as top_10,
            coalesce(approved_count/agg_by_approved,0)*100 as  approved
        from counts_third h
        join total_hits_third t on h.siteid=t.siteid
        join last_30_days_article_published_Agg_third hbt on hbt.siteid=h.siteid and hbt.tags=h.tags
        where h.siteid  = 15
    ),
    categories_d_third as
    (
		select 
			siteid as siteid ,
			'Artikler ift. tags og sidevisninger' as label,
            'Artikler publiceret år til dato grupperet i tre kategorier: Artikler under sidevisningsmål | Artikler, der klarede mål | Top 10% bedste artikler ift. sidevisninger. For hver kategori vises hvor stor andel der er publiceret indenfor hvert brugerbehov, så det er muligt at se forskelle på tværs af kategorierne.' as hint,
			GROUP_CONCAT(tags ORDER BY FIELD(tags, 'Børn og unge','Andet','Handicap','Psykiatri og udsathed', 'others' ) SEPARATOR ',') 
			AS cateogires,
			GROUP_CONCAT(COALESCE(less_than_target, 0) ORDER BY FIELD(tags, 'Børn og unge','Andet','Handicap','Psykiatri og udsathed', 'others' ) SEPARATOR ',') 
			AS less_than_target,
			GROUP_CONCAT(approved ORDER BY FIELD(tags, 'Børn og unge','Andet','Handicap','Psykiatri og udsathed', 'others' ) SEPARATOR ',') 
			AS approved,
			GROUP_CONCAT(top_10 ORDER BY FIELD(tags, 'Børn og unge','Andet','Handicap','Psykiatri og udsathed', 'others' ) SEPARATOR ',') 
			AS top_10
		from agg_data_third
		group by 1,2,3
	),
    categories_third as 
    (
		select 
			siteid as siteid ,
			label as label,
            hint as hint,  
			CONCAT('"', REPLACE(cateogires, ',', '","'), '"') AS  cateogires,
			less_than_target as less_than_target ,
			approved as approved ,
			top_10 as top_10 
		from  categories_d_third
	),
    json_data_third as
		(
		select siteid,label as lab,hint as h,cateogires as cat,CONCAT(
		'{"name": "Under mål", "data": [',cast(less_than_target as char),']}'
		,',{"name": "Over mål" ,"data": [',cast(approved as char),']}',',
        {"name": "Top 10%" ,"data": [',cast(top_10 as char),']}') 
        as series from categories_third
		)
        
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
				'"defaultTitle": "Brugerbehov",',
				'"additional": [',
                    
                    '{',
                        '"title": "Målgruppe",',
                        '"data": {',
                            '"label": "', json_data_second.lab, '",',
                            '"categories": [', json_data_second.cat, '],',
                            '"series": [', json_data_second.series, ']',
                        '}',
                    '}'
                ','
				
                    '{',
                        '"title": "Emner",',
                        '"data": {',
                            '"label": "', json_data_third.lab, '",',
                            '"categories": [', json_data_third.cat, '],',
                            '"series": [', json_data_third.series, ']',
                        '}',
                    '}'
                
				
        		']',
			'}',
		'}'
			) as json_data
		  FROM json_data jd
          
		  CROSS join json_data_second
		  
		  CROSS join json_data_third
		   ;


END