-- template.sql

CREATE PROCEDURE `example_procedure`(
    IN site_id INT,
    IN event_action VARCHAR(255)
)
BEGIN
    SET @sql_query = '
    SELECT *
    FROM events
    WHERE site_id = 123
    AND event_action = "click"';
    
    PREPARE stmt FROM @sql_query;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END;