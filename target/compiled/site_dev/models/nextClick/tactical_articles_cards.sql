
  PREPARE stmt FROM @sql_query;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;
END;