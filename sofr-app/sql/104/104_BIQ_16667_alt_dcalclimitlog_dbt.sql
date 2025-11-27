-- Изменение структуры
declare
  n      number;
  t_name varchar2(100) := 'DCALCLIMITLOG_DBT';
  c_name varchar2(100) ;
begin
  c_name := 't_calc_direct';
  select count(*) into n from user_tab_columns c where c.TABLE_NAME = upper(t_name) and c.COLUMN_NAME = upper(c_name);
  if n = 0 then
    execute immediate 'alter table DCALCLIMITLOG_DBT add t_calc_direct varchar2(128) default ''X'' not null';
    execute immediate 'drop index DCALCLIMITLOG_DBT_IDX0';
    execute immediate 'create index DCALCLIMITLOG_DBT_IDX0 on dcalclimitlog_dbt (t_calc_direct, t_mainsessionid, t_action) tablespace INDX ' ;
  end if;
end;
/