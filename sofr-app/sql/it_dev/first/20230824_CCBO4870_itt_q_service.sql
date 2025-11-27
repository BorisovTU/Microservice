--  Правка столбца
declare
  t_name   varchar2(100) := 'itt_q_service';
  c_name   varchar2(100) := 'service_proc';
  c_NULLABLE user_tab_columns.NULLABLE%type;
begin
  select c.NULLABLE
    into c_NULLABLE
    from user_tab_columns c
   where c.TABLE_NAME = upper(t_name)
     and c.COLUMN_NAME = upper(c_name);
  if c_NULLABLE != 'Y'
  then
       execute immediate 'alter table itt_q_service modify service_proc null';
  end if;
end;
