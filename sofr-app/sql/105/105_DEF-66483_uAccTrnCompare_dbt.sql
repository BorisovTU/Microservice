-- Изменения по DEF-66483
-- Модификация таблицы "uAccTrnCompare_dbt", 
-- добавление полей t_sync_db и t_sync_cr для сводных счетов по дебету и кредиту
DECLARE
  logID VARCHAR2(32) := 'DEF-66483';
  -- Записать сообщение в itt_log
  PROCEDURE LogIt( p_message VARCHAR2 )
  AS
  BEGIN
    it_log.log(p_msg => logID||': '||p_message);
  END;
  -- Модификация таблицы "uAccTrnCompare_dbt"
  PROCEDURE AlterUAccTrnCompare_dbt
  AS
  BEGIN
    LogIt('Модификация таблицы uacctrncompare_dbt');
    EXECUTE IMMEDIATE 'alter table uacctrncompare_dbt add (t_sync_db varchar2(25), t_sync_cr varchar2(25))';
    LogIt('Модифицирована таблица uacctrncompare_dbt');
  EXCEPTION
    WHEN OTHERS THEN
      LogIt('Ошибка модификации таблицы uacctrncompare_dbt');
  END;
BEGIN
  AlterUAccTrnCompare_dbt();			-- Модификация таблицы "uAccTrnCompare_dbt"
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
END;
/
