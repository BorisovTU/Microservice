-- Добавление новой записи в dllvalues_dbt 
DECLARE
  logID VARCHAR2(50) := 'BOSS-194-1 Add LlValues';
  v_cnt NUMBER;
BEGIN
  SELECT COUNT(1) INTO v_cnt
    FROM dLlValues_dbt
   WHERE t_list = 1143
     AND t_element = 509;
  
  IF v_cnt = 0 THEN
    INSERT INTO dLlValues_dbt
      (t_list,
       t_element, 
       t_code, 
       t_name, 
       t_flag, 
       t_note,
       t_reserve)
    VALUES 
      (1143,
       509,
       '509',
       'Уведомление об отказе в признании лица КИ',
       509,
       'Уведомление об отказе в признании лица квалифицированным инвестором',
       CHR(1));

     COMMIT;
  END IF;
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
END;
/