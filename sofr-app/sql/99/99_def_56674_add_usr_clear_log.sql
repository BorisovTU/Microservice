-- Добавление таблиц для исторической очистки
begin
insert into usr_clear_log_params (t_table_name,
                                  t_query) values 
                                  ('DQUEUE_EXCHANGE_DBT'
,'DECLARE 
  max_id INTEGER := 0; 
  min_id INTEGER := 0; 
BEGIN 
  SELECT  max(t_id), min(t_id) INTO max_id, min_id FROM DQUEUE_EXCHANGE_DBT WHERE T_CREATEDATE < (sysdate - NUMTODSINTERVAL(60, ''DAY'')); 

  WHILE max_id >= min_id 
  LOOP 
    DELETE FROM DQUEUE_EXCHANGE_DBT DQ WHERE DQ.T_ID BETWEEN (max_id - 10000) AND max_id; 
    COMMIT; 

    max_id := max_id - 10000; 
  END LOOP; 
END;');
insert into usr_clear_log_params (t_table_name,
                                  t_query) values 
                                  ('USR_EXCHNG_LOG_DB' 
,'DECLARE 
  max_id INTEGER := 0; 
  min_id INTEGER := 0; 
BEGIN 
  SELECT max(t_id), min(t_id) INTO max_id, min_id FROM USR_EXCHNG_LOG_DBT WHERE T_REQ_DTTM < (sysdate - NUMTODSINTERVAL(365, ''DAY'')); 

  WHILE max_id >= min_id 
  LOOP 
    DELETE FROM USR_EXCHNG_LOG_DBT DQ WHERE DQ.T_ID BETWEEN (max_id - 10000) AND max_id; 
    COMMIT; 

    max_id := max_id - 10000; 
  END LOOP; 
END;');
commit;
end;