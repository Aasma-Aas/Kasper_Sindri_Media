
  create view `site_dev`.`tactical_articles_card_ytd__dbt_tmp`
    
    
  as (
    







CREATE DEFINER=`Site`@`%` PROCEDURE `prod`.`tactical_articles_card_ytd_dbt_17`()
BEGIN

with curr_ytd as
(
SELECT siteId as siteId,count(date) as value FROM prod.site_archive_post
where 
date 
between MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1)  and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY) 

and siteid = 17
group by 1
),
last_ytd as 
(
SELECT  siteId as siteId,count(date) value  FROM prod.site_archive_post
where 
date 
between DATE_SUB(MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1), INTERVAL 1 Year)  and  DATE_SUB(DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY),INTERVAL 1 Year) 

and siteid = 17
group by 1
),
json_data as( 
select ld.siteid as siteid,'Artikler' as label,'Artikler publiceret år til dato ift. sidste år til dato' as hint,ld.value as valu,COALESCE(((ld.value-lb.value)/lb.value)*100,0)  as chang,'' as progressCurrent
,'' as progressTotal from curr_ytd ld
left join last_ytd lb
on ld.siteid=lb.siteid
)
select  JSON_OBJECT('site',siteid,'data',JSON_OBJECT(
    'label', label,
    'hint', hint,
    'value',COALESCE(valu,0),'change',chang,'progressCurrent',progressCurrent, 'progressTotal',progressTotal
 )) AS json_data  from json_data
;

  END
  );