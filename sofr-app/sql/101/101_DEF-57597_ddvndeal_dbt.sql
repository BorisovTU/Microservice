-- Изменения по DEF-57597, изменение ddvndeal_dbt c логированием
DECLARE
  logID VARCHAR2(9) := 'DEF-57597';
  -- Записать сообщение в itt_log
  PROCEDURE LogIt( p_message VARCHAR2 )
  AS
  BEGIN
    it_log.log(p_msg => logID||': '||p_message);
  END;
  -- Добавление настройки
  PROCEDURE AlterDDVNDEAL_DBT
  AS
  BEGIN
    LogIt('Изменение в DDVNDEAL_DBT: добавление поля  T_METHODAPPLIC');
    EXECUTE IMMEDIATE 'ALTER TABLE DDVNDEAL_DBT ADD (T_METHODAPPLIC NUMBER(5) DEFAULT 0)';
    LogIt('Добавлено поле T_METHODAPPLIC в DDVNDEAL_DBT');
  EXCEPTION
    WHEN OTHERS THEN
       LogIt('Ошибка добавления поля T_METHODAPPLIC в DDVNDEAL_DBT');
  END;
BEGIN
  -- Добавление настройки
  AlterDDVNDEAL_DBT();
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
END;
/
