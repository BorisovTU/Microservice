-- Изменения по DEF-63633 
-- В справочнике Параметры лимитов QUIK (таблица DDL_LIMITPRM_DBT) по всем строкам, кроме строки по срочному рынку) 
-- в поле "Код участника торгов" установить значение MC0134700000
DECLARE
  logID VARCHAR2(32) := 'DEF-63633';
  x_Cnt NUMBER;
  x_Stat NUMBER := 0;

  -- Записать сообщение в itt_log
  PROCEDURE LogIt(p_message IN VARCHAR2)
  AS
  BEGIN
    it_log.log(p_msg => logID||': '||p_message);
  END;
  -- Изменения в ddl_limitprm_dbt
  PROCEDURE updateDlLimitPrm
  IS
    x_DefFirmID VARCHAR2(32) := 'MC0134700000';
  BEGIN
    LogIt('Изменения в таблице ddl_limitprm_dbt');
    EXECUTE IMMEDIATE 
       'UPDATE ddl_limitprm_dbt r SET r.t_firmcode = :x_DefFirmID WHERE r.t_marketkind <> 2'
       USING x_DefFirmID
    ;
    EXECUTE IMMEDIATE 'COMMIT';
    LogIt('Произведены изменения в таблице ddl_limitprm_dbt');
  EXCEPTION
   WHEN OTHERS THEN 
      LogIt('Ошибка при изменениях в таблице ddl_limitprm_dbt');
      EXECUTE IMMEDIATE 'ROLLBACK';
  END;
BEGIN
  updateDlLimitPrm();           	-- Изменения в ddl_limitprm_dbt
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
END;
/
