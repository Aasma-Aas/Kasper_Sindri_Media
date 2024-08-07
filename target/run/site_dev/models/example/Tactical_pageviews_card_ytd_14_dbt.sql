
  create view `site_dev`.`Tactical_pageviews_card_ytd_14_dbt__dbt_tmp`
    
    
  as (
    




CREATE PROCEDURE `prod`.`Tactical_pageviews_card_ytd_14_dbt`()
BEGIN
with last_ytd_article_published as(
SELECT siteid as siteid,id   FROM prod.site_archive_post
where 
date
between MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1)  and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY) 
 
            AND Categories <> 'Nyhedsoverblik'
        
and siteid = 14
),
 last_ytd_article_published_Agg as(
select siteid as siteid,count(*) as agg from  last_ytd_article_published
where  siteid = 14
group by 1
),
agg_last_ytd as (
select  e.siteid as siteid,sum(unique_pageviews)/agg as value from prod.pages e
join  last_ytd_article_published a on a.id=e.postid
join last_ytd_article_published_Agg agg on agg.siteid=a.siteid 
where e.date
between MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1)  and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY) and e.siteid = 14
group by 1,agg
),
last_ytd_before_article_published as(
SELECT siteid as siteid,id   FROM prod.site_archive_post
where 
DATE_SUB(MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1), INTERVAL 1 Year) 
 and  DATE_SUB(DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY),INTERVAL 1 Year) 
  
            AND Categories <> 'Nyhedsoverblik'
        
        and siteid = 14

),
last_ytd_before_article_published_Agg as(
select siteid as siteid,count(*) as agg from  last_ytd_before_article_published
where siteid = 14
group by 1
),
agg_last_ytd_before as(
select  e.siteid as siteid,sum(unique_pageviews)/agg as value from prod.pages e
join  last_ytd_before_article_published a on a.id=e.postid
join last_ytd_before_article_published_Agg agg on agg.siteid=a.siteid 
where DATE_SUB(MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1), INTERVAL 1 Year)  and  DATE_SUB(DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY),INTERVAL 1 Year) and e.siteid = 14
group by 1,agg
),
json_data as( 
select al.siteid as site, label_val label, hint_val  hint, al.value as valu,((al.value-alb.value)/al.value)*100 as chang, '' as progressCurrent , '' as progresstotal
from agg_last_ytd al
left join agg_last_ytd_before alb on al.siteid=alb.siteid
where al.siteid = 14
)
select JSON_OBJECT('site',site,'data',JSON_OBJECT(
    'label', 'Besøg',
    'hint', 'Besøg år til dato ift sidste år til dato og ift målsætning',
    'value',COALESCE(valu,0),
    'change',COALESCE(chang,0),
    'progressCurrent',progressCurrent, 
    'progressTotal',progressTotal
 )) AS json_data  from json_data;
 
 END
  );