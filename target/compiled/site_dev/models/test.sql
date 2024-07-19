CREATE DEFINER=`Site`@`%` PROCEDURE `stage`.`tactical_userneeds_pageviews_chart_month_13_dbt_test`(
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
                
        '']}}''
    
			) as json_data
		  FROM json_data jd
          ;