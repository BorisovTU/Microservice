-- Изменение поля
declare
  n      number;
  t_name varchar2(100) := 'PKO_WRITEOFF';
  c_name varchar2(100) := 'DealCode';
begin
   select DATA_LENGTH  into n from user_tab_columns c where c.TABLE_NAME = upper(t_name) and c.COLUMN_NAME = upper(c_name);
   if n != 128 then
     execute immediate 'alter table '||t_name||' modify '||c_name||' VARCHAR2(128)';
   end if;
end;
