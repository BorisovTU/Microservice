declare
  n      number;
  t_name varchar2(100) := 'dclportsofr2dwh_tmp';
  c_name varchar2(100) ;
begin
  c_name := 't_dlcontrid';
  select count(*) into n from user_tab_columns c where c.TABLE_NAME = upper(t_name) and c.COLUMN_NAME = upper(c_name);
  if n = 0 then
    execute immediate 'alter table dclportsofr2dwh_tmp add t_dlcontrid number';
   end if;
end;
/