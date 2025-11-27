insert into  USR_CLEAR_LOG_PARAMS ( T_TABLE_NAME,
                                   T_QUERY) values ( upper('dscsumconfexp_dbt'),
 'BEGIN 
  DELETE FROM dscsumconfexp_dbt  
   WHERE T_SYSDATE < trunc(SYSDATE) - 5;
END;') 
