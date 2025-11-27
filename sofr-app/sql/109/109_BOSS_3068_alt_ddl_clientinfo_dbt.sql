
declare
  n      number;
  t_name varchar2(100) := 'DDL_CLIENTINFO_DBT';
  c_name varchar2(100) := 't_errors_reason';
begin
   select count(*)  into n from user_tab_columns c where c.TABLE_NAME = upper(t_name) and c.COLUMN_NAME = upper(c_name);
   if n = 0 then
     execute immediate 'alter table DDL_CLIENTINFO_DBT add t_errors_reason varchar2(512)';
     execute immediate 'comment on column DDL_CLIENTINFO_DBT.t_errors_reason is ''Ошибки при обработке операций клиента''' ;
   end if;
end;

