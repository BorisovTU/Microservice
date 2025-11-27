-- Добавление новой записи в dllvalues_dbt 
DECLARE
  logID VARCHAR2(50) := 'BOSS-194-4 Add NameAlg';
  v_cnt NUMBER;
BEGIN
  SELECT COUNT(1) INTO v_cnt
    FROM dNameAlg_dbt
   WHERE t_iTypeAlg = 3146
     AND t_iNumberAlg = 2;
  
  IF v_cnt = 0 THEN
    INSERT INTO dNameAlg_dbt
      (t_iTypeAlg,
       t_iNumberAlg, 
       t_szNameAlg, 
       t_iLenName, 
       t_iQuantAlg, 
       t_reserve)
    VALUES 
      (3146,
       2,
       'Пролонгировать',
       0,
       0,
       CHR(1));
       
     UPDATE dNameAlg_dbt 
        SET t_iLenName = 14,
            t_iQuantAlg = 3
      WHERE t_iTypeAlg = 3146
        AND t_iNumberAlg = 0;

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