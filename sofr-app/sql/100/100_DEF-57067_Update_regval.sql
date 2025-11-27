-- Добавление настроек банка в regparm, regval
DECLARE
  logID VARCHAR2(50) := 'DEF-57067 Update RegVal';
  v_keyID NUMBER;
BEGIN
   SELECT t_keyID INTO v_keyID
     FROM dRegParm_dbt 
    WHERE t_parentID = (SELECT t_keyID FROM DREGPARM_DBT WHERE LOWER(T_NAME) = LOWER('КВАЛИФИКАЦИЯ'))
      AND LOWER(t_name) = LOWER('ДИРЕКТОРИЯ');

   IF (v_keyID <> 0) THEN
     UPDATE dRegVal_dbt 
        SET t_fmtBlobData_xxxx = to_blob(utl_raw.cast_to_raw('\\sgo-fc01-r03.go.rshbank.ru\sofr_for_etl\inbox\export\QINV\'))
      WHERE t_keyID = v_keyID;
   END IF;
   
   COMMIT;
        
EXCEPTION WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
    -- select * from itt_log where msg like 'DEF-57067%'
END;
/