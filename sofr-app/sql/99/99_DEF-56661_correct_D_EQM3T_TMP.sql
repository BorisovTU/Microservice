-- Изменения по DEF-56661, для исправления блокера нужно откорректировать таблицу D_EQM3T_TMP (должно быть поле T_BOARDNAME)
DECLARE
  logID VARCHAR2(9) := 'DEF-56661';
  -- Записать сообщение в itt_log
  PROCEDURE LogIt(p_message VARCHAR2)
  AS
  BEGIN
    it_log.log(p_msg => logID||': '||p_message);
  END;
  -- Добавление индекса
  PROCEDURE CorrectIt
  AS
    x_Cnt number;
  BEGIN
    LogIt('Корректировка таблицы D_EQM3T_TMP');
    EXECUTE IMMEDIATE 'ALTER TABLE D_EQM3T_TMP ADD T_BOARDNAME VARCHAR2(30)';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN D_EQM3T_TMP.T_BOARDNAME IS ''Название режима торгов''';
    LogIt('Таблица D_EQM3T_TMP откорректирована');
  EXCEPTION
    WHEN OTHERS THEN 
       LogIt('Корректировка таблицы D_EQM3T_TMP не нужна');
  END;
BEGIN
  -- Корректировка таблицы
  CorrectIt();
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
END;
/
