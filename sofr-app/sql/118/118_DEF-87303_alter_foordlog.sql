-- Добавление поля в таблицу d_foordlog_tmp
DECLARE
  -- Процедура добавления поля
  PROCEDURE addField
  IS
    v_count NUMBER;
  BEGIN
    SELECT count(1) INTO v_count
      FROM all_tab_columns 
     WHERE UPPER(table_name) = 'D_FOORDLOG_TMP' 
       AND UPPER(column_name) = 'T_ID_ORD';
       
    IF v_count = 0 THEN
      EXECUTE IMMEDIATE 'ALTER TABLE D_FOORDLOG_TMP ADD T_ID_ORD varchar2(32)';
    END IF;
  EXCEPTION WHEN others THEN 
    DBMS_OUTPUT.PUT_LINE('Ошибка: '||SQLCODE||' - '||SQLERRM); 
    it_error.put_error_in_stack;
    it_log.log(p_msg => 'DEF-87303 Release 118 alter d_foordlog_tmp',
               p_msg_type => it_log.C_MSG_TYPE__ERROR -- при указании такого типа, функция залогирует стек ошибки, собранный в it_error
              );
    it_error.clear_error_stack;
  END;
BEGIN
  addField(); -- добавление поля в таблицу d_foordlog_tmp
END;
/

--Компилирование пакета, который использует таблицу d_foordlog_tmp
DECLARE
BEGIN
  EXECUTE IMMEDIATE 'ALTER PACKAGE RSB_GTIM_CSV COMPILE BODY';
  
  EXCEPTION WHEN others THEN 
    DBMS_OUTPUT.PUT_LINE('Ошибка: '||SQLCODE||' - '||SQLERRM); 
    it_error.put_error_in_stack;
    it_log.log(p_msg => 'DEF-87303 Release 118 compile RSB_GTIM_CSV',
               p_msg_type => it_log.C_MSG_TYPE__ERROR -- при указании такого типа, функция залогирует стек ошибки, собранный в it_error
              );
    it_error.clear_error_stack;
END;
/