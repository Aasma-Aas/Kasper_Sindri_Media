
  
    

  create  table "dbt_project_test_kasper"."public"."pageview_month_13__dbt_tmp"
  
  
    as
  
  (
    
CREATE DEFINER=`Site`@`%` PROCEDURE `stage`.`pageview_month_13`(
    IN site INT,
    IN label_val VARCHAR(200),
    IN hint_val TEXT,
    IN order_statement_first  VARCHAR(2000),
    IN domain VARCHAR(200),
    IN tag_statement_first VARCHAR(2000),
    IN tag_statement_second VARCHAR(2000),
    IN tag_statement_third VARCHAR(2000)
)
BEGIN
    SET SESSION group_concat_max_len = 1000000;
    SET @sql_query = CONCAT('
    with 
    
  );
  