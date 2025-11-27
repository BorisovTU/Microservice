-- Создание полей для таблиц, компиляция невалидных объектов

DECLARE
  -- Процедура поля t_commentar varchar2(20) в таблицу d_foordlog_tmp
  PROCEDURE addCommentar
  IS
    v_count NUMBER;
  BEGIN
    SELECT count(1) INTO v_count
      FROM all_tab_columns 
     WHERE UPPER(table_name) = 'D_FOORDLOG_TMP' 
       AND UPPER(column_name) = 'T_COMMENTAR';
       
    IF v_count = 0 THEN
      EXECUTE IMMEDIATE 'ALTER TABLE d_foordlog_tmp ADD t_commentar varchar2(20)';
    END IF;
  EXCEPTION WHEN others THEN 
    DBMS_OUTPUT.PUT_LINE('Ошибка: '||SQLCODE||' - '||SQLERRM); 
    it_error.put_error_in_stack;
    it_log.log(p_msg => 'DEF-31309 Release 100 alter d_foordlog_tmp',
               p_msg_type => it_log.C_MSG_TYPE__ERROR -- при указании такого типа, функция залогирует стек ошибки, собранный в it_error
              );
    it_error.clear_error_stack;
  END;
BEGIN
  addCommentar(); 	-- добавление поля t_commentar varchar2(20) в таблицу d_foordlog_tmp
END;
/

DECLARE
  logID VARCHAR2(100) := 'DEF-31309 Release 100 alter D_EQM3T_TMP';
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

DECLARE
  -- Процедура поля t_section varchar2(50) в таблицу d_fo07_tmp
  PROCEDURE addSection
  IS
    v_count NUMBER;
  BEGIN
    SELECT count(1) INTO v_count
      FROM all_tab_columns 
     WHERE UPPER(table_name) = 'D_FO07_TMP' 
       AND UPPER(column_name) = 'T_SECTION';
       
    IF v_count = 0 THEN
      EXECUTE IMMEDIATE 'ALTER TABLE d_fo07_tmp ADD t_section varchar2(50)';
    END IF;
  EXCEPTION WHEN others THEN 
    DBMS_OUTPUT.PUT_LINE('Ошибка: '||SQLCODE||' - '||SQLERRM); 
    it_error.put_error_in_stack;
    it_log.log(p_msg => 'DEF-31309 Release 100 alter d_fo07_tmp',
               p_msg_type => it_log.C_MSG_TYPE__ERROR -- при указании такого типа, функция залогирует стек ошибки, собранный в it_error
              );
    it_error.clear_error_stack;
  END;
BEGIN
  addSection(); 	-- добавление поля t_section varchar2(50) в таблицу d_fo07_tmp
END;
/

DECLARE
BEGIN
  EXECUTE IMMEDIATE 'ALTER PACKAGE RSB_GTIM_CSV COMPILE BODY';
  
  EXCEPTION WHEN others THEN 
    DBMS_OUTPUT.PUT_LINE('Ошибка: '||SQLCODE||' - '||SQLERRM); 
    it_error.put_error_in_stack;
    it_log.log(p_msg => 'DEF-31309 Release 100 compile RSB_GTIM_CSV',
               p_msg_type => it_log.C_MSG_TYPE__ERROR -- при указании такого типа, функция залогирует стек ошибки, собранный в it_error
              );
    it_error.clear_error_stack;
END;
/

DECLARE
BEGIN
  EXECUTE IMMEDIATE 'ALTER PACKAGE RSB_GTIM_MMVB COMPILE BODY';
  
  EXCEPTION WHEN others THEN 
    DBMS_OUTPUT.PUT_LINE('Ошибка: '||SQLCODE||' - '||SQLERRM); 
    it_error.put_error_in_stack;
    it_log.log(p_msg => 'DEF-31309 Release 100 compile RSB_GTIM_MMVB',
               p_msg_type => it_log.C_MSG_TYPE__ERROR -- при указании такого типа, функция залогирует стек ошибки, собранный в it_error
              );
    it_error.clear_error_stack;
END;
/