
declare
  n      number;
  t_name varchar2(100) := 'DDL_CLIENTINFO_DBT';
  c_name varchar2(100) := 't_time306';
begin
   select count(*)  into n from user_tab_columns c where c.TABLE_NAME = upper(t_name) and c.COLUMN_NAME = upper(c_name);
   if n = 0 then
     execute immediate 'alter table DDL_CLIENTINFO_DBT add t_time306 date';
     execute immediate 'comment on column DDL_CLIENTINFO_DBT.t_time306 is ''Дата и время получения остатка 306 счета''' ;
   end if;
end;
/
