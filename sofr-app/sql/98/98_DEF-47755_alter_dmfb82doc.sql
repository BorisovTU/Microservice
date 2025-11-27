-- Изменение размерности поля t_reportdesc до 500
DECLARE
  logID VARCHAR2(9) := 'DEF-47755';
  reportDescLength NUMBER;
BEGIN
  SELECT data_length 
    INTO reportDescLength
    FROM user_tab_columns 
   WHERE UPPER(table_name) = 'DMFB82DOC_DBT'
     AND UPPER(column_name) = 'T_REPORTDESC';
   
   IF reportDescLength < 500 THEN
     EXECUTE IMMEDIATE 'ALTER TABLE dMfb82Doc_dbt MODIFY t_reportDesc VARCHAR2(500)';
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