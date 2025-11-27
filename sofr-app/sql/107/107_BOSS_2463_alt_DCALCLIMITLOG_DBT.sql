declare
  n      number;
  t_name varchar2(100) := 'DCALCLIMITLOG_DBT';
begin
  select count(*)
    into n
    from user_tab_columns tc
   where tc.TABLE_NAME = upper(t_name)
     and tc.COLUMN_NAME = upper('t_sessionid');
  if n > 0
  then
    execute immediate 'alter table DCALCLIMITLOG_DBT drop column t_sessionid';
  end if;
  select count(*)
    into n
    from user_tab_columns tc
   where tc.TABLE_NAME = upper(t_name)
     and tc.COLUMN_NAME = upper('t_rootsessionid');
  if n > 0
  then
    execute immediate 'alter table DCALCLIMITLOG_DBT drop column t_rootsessionid';
  end if;
  select count(*)
    into n
    from user_tab_columns tc
   where tc.TABLE_NAME = upper(t_name)
     and tc.COLUMN_NAME = upper('t_mainsessionid');
  if n > 0
  then
    execute immediate 'alter table DCALCLIMITLOG_DBT drop column t_mainsessionid';
  end if;
  select count(*) into n from user_indexes i where i.INDEX_NAME = 'DCALCLIMITLOG_DBT_IDX0';
  if n = 0
  then
    execute immediate 'create index DCALCLIMITLOG_DBT_IDX0 on DCALCLIMITLOG_DBT(t_calc_direct) tablespace INDX';
  end if;
end;
/
