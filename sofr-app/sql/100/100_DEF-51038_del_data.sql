-- Изменения по DEF-51038, очистка таблицы dbrokacc_acc_dbt
DECLARE
  logID VARCHAR2(9) := 'DEF-51038';
  -- Записать сообщение в itt_log
  PROCEDURE LogIt(p_message VARCHAR2)
  AS
  BEGIN
    it_log.log(p_msg => logID||': '||p_message);
  END;
  -- Удаление данных в таблице
  PROCEDURE DelData(p_table VARCHAR2)
  AS
  BEGIN
    LogIt('Удаление данных в таблице '''||p_table||'''');
    execute immediate 'truncate table '||p_table;
    LogIt('Удалены данные в таблице '''||p_table||'''');
  END;
BEGIN
  -- Удаление данных в таблице
  DelData('dbrokacc_acc_dbt');
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
END;
/

