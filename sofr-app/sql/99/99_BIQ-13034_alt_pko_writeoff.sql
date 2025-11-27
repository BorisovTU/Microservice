--  Изменение поля 
declare
  column_already_exist EXCEPTION;
  pragma exception_init(column_already_exist, -1430); -- Ошибка ORA-001430
begin
   begin
      execute immediate 'alter table PKO_WRITEOFF add clienttype varchar2(10)';
   exception
      when column_already_exist
      then 
        dbms_output.put_line('column already exist');
        NULL;
   end;
   
   begin
     execute immediate 'alter table PKO_WRITEOFF modify Market VARCHAR2(30)';
   exception
      when others
      then 
        dbms_output.put_line('can not increase width of column');
        NULL;
   end;
    
end;
/
