CREATE DEFINER=`Site`@`%` PROCEDURE `stage`.`tactical_userneeds_pageviews_chart_month_11_dbt_test`(
    IN site INT,
    IN label_val VARCHAR(200),
    IN hint_val TEXT,
    IN order_statement VARCHAR(2000),
    IN order_statement_second VARCHAR(2000),
    IN order_statement_third VARCHAR(2000),
    IN dtitle VARCHAR(200),
    IN title1 VARCHAR(200),
    IN title2 VARCHAR(200),
    IN event_action VARCHAR(255)
)
BEGIN
    DECLARE sql_query TEXT; -- Declare variable to hold dynamic SQL query

    -- Construct the dynamic SQL query
    SET @sql_query = CONCAT('
        WITH ')
         
    PREPARE stmt FROM @sql_query;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END;