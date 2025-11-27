-- Добавление полей 
declare
  n      number;
  t_name varchar2(100) := 'itt_file';
  c_name varchar2(100) ;
begin
  c_name := 'part_no';
  select count(*) into n from user_tab_columns c where c.TABLE_NAME = upper(t_name) and c.COLUMN_NAME = upper(c_name);
  if n = 0 then
    execute immediate 'alter table ITT_FILE add part_no number';
  end if;
  c_name := 'sessionid';
  select count(*) into n from user_tab_columns c where c.TABLE_NAME = upper(t_name) and c.COLUMN_NAME = upper(c_name);
  if n = 0 then
    execute immediate 'alter table ITT_FILE add sessionid number default sys_context(''USERENV'',''SESSIONID'')';
  end if;

end;
