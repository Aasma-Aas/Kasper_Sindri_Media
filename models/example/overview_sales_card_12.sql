{%set site =var('site')%}
{%set date =var('date_week')%}
{%set event_action =var('event_action')%}
{%set json =var('json')%}

CREATE PROCEDURE `prod`.`overview_sales_card_12`(
)
BEGIN
with curr_year as (
SELECT
    e.SiteID as SiteID,
    e.date as date,
    SUM(e.hits) AS event_value
FROM
    prod.events e
WHERE
 e.siteid = {{site}} and
    e.date BETWEEN {{date}}
    AND e.event_action = '{{event_action}}'
GROUP BY SiteId,2
) ,
last_year as (
SELECT
e.SiteID as SiteID,
e.date as date,
SUM(e.hits) as event_value_last_year
FROM
    prod.events e
WHERE
 DATE_FORMAT(STR_TO_DATE(e.date, '%d-%m-%Y'),'%Y-%m-%d')
 between DATE_SUB(MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1), INTERVAL 1 Year)  and  DATE_SUB(DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY),INTERVAL 1 Year)
  and e.siteid = {{site}}
  AND e.event_action = 'Receipt'
 group by SiteId,2
),
curr_year_page as (
SELECT
    p.SiteID as SiteID,
    p.date,
    SUM(p.unique_pageviews) AS page_value
FROM
    prod.pages p
WHERE
 p.siteid = {{site}} and
    p.date BETWEEN {{date}}
GROUP BY SiteId,2
),
last_year_page as (
SELECT
    p.SiteID as SiteID,
    p.date,
    SUM(p.unique_pageviews)  as page_value_last_year
FROM
    prod.pages
WHERE
 DATE_FORMAT(STR_TO_DATE(p.date, '%d-%m-%Y'),'%Y-%m-%d')
 between DATE_SUB(MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1), INTERVAL 1 Year)  and  DATE_SUB(DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY),INTERVAL 1 Year)
  and p.siteid = {{site}}
 group by SiteId,2
),
goal as (
SELECT
Site_ID,
date,
sum(cta_per_day) as progressTotal
FROM
    prod.goals
  where site_id = {{site}} and date
between (SELECT MIN(date) AS MinimumDate
    FROM prod.pages
    WHERE siteid = {{site}} and YEAR(date) = YEAR(CURRENT_DATE()))  and  (SELECT MAX(date) AS MaximumDate
                  FROM prod.pages
                  WHERE siteid ={{site}} and YEAR(date) = YEAR(CURRENT_DATE()))
                  
 group by Site_Id,2
) ,
final as
(
select p.siteid as siteid, c.event_value/p.page_value as v
 from curr_year_page   p
left join  curr_year  c on c.siteid=p.siteid and c.date=p.date
),
res as
(
select  siteid, ROUND((sum(v)/count(*) * 100),3) as value_f
from final
group by siteid
)
    goal_percentage AS (
      SELECT
        Site_ID,
        SUM(cta_per_day) AS progressTotal,
        COUNT(*) AS days_passed
    FROM
        prod.goals
    WHERE
        site_id = {{site}} AND 
        date between {{date}}
    GROUP BY Site_ID

)
 SELECT
    JSON_OBJECT(
        'site', cy.SiteID,
        'data', {{json}}
    ) AS json_data
FROM res cy
LEFT JOIN last_year ly ON cy.siteid = ly.siteid
LEFT JOIN goal_percentage g ON g.Site_ID = cy.SiteID
GROUP BY cy.SiteID, g.progressTotal, g.days_passed; 
    
END