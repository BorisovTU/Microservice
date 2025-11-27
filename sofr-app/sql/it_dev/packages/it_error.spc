create or replace package it_error as

 /**************************************************************************************************\
  Пакет для сбора стека ошибки
  **************************************************************************************************
  Изменения:
  --------------------------------------------------------------------------------------------------
  Дата        Автор            Jira                            Описание 
  ----------  ---------------  ---------------------------     -------------------------------------
  31.01.2022  Зотов Ю.Н.       BIQ-6664 CCBO-506               Создание
 \**************************************************************************************************/

  procedure put_error_in_stack;

  procedure clear_error_stack;

  function get_error_stack_clob(p_clear boolean default false) return clob;

  function get_error_stack(p_clear boolean default false) return varchar2;
  
/*
declare
  vDialog number;
begin
  zotov_test_exception.p1;
exception
  when others then
    it_error.put_error_in_stack;
    htools.message('Ошибка',it_error.get_error_stack);
    it_error.clear_error_stack;
end;

*/  
  
end;
/
