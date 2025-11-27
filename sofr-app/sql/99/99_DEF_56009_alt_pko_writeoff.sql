--  Изменение поля 
declare
  n      number;
  t_name varchar2(100) := 'pko_writeoff';
  c_name varchar2(100) := 'custodyorderid';
begin
   select DATA_LENGTH  into n from user_tab_columns c where c.TABLE_NAME = upper(t_name) and c.COLUMN_NAME = upper(c_name);
   if n != 30 then
     execute immediate 'alter table PKO_WRITEOFF modify custodyorderid VARCHAR2(30)';
   end if;
end;
/