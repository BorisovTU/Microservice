-- Удаление невалидных объектов
BEGIN
  EXECUTE IMMEDIATE 'DROP PACKAGE SHCHERBININSV.RSHB_RSI_SCLIMIT';
EXCEPTION
  WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE('Ошибка: '||SQLCODE||' - '||SQLERRM); 
    it_error.put_error_in_stack;
    it_log.log(p_msg => 'DEF-31309',
               p_msg_type => it_log.C_MSG_TYPE__ERROR -- при указании такого типа, функция залогирует стек ошибки, собранный в it_error
              );
    it_error.clear_error_stack;
END;
/