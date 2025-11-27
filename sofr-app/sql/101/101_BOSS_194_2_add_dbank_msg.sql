-- Добавление новой записи в dbank_msg
DECLARE
  logID VARCHAR2(50) := 'BOSS-194-4 Add dbank_msg';
  v_cnt NUMBER;
BEGIN
  SELECT COUNT(1) INTO v_cnt
    FROM dBank_msg
   WHERE t_number = 21724;
  
  IF v_cnt = 0 THEN
     INSERT INTO dbank_msg 
       (t_number, t_page, t_contents)
     VALUES 
       (21724, 0, 'Для данной роли функция недоступна.');

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