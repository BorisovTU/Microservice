declare
  n      number;
  t_name varchar2(100) := 'DDL_CLIENTINFO_DBT';
  c_name varchar2(100) ;
begin
  c_name := 't_calc_sid';
  select count(*) into n from user_tab_columns c where c.TABLE_NAME = upper(t_name) and c.COLUMN_NAME = upper(c_name);
  if n = 0 then
    execute immediate 'alter table DDL_CLIENTINFO_DBT add t_calc_sid varchar2(128) default ''X'' not null';
    execute immediate 'comment on column DDL_CLIENTINFO_DBT.t_calc_sid  is ''СИД расчета = rshb_rsi_sclimit.GC_CALC_SID_DEFAULT''';
    execute immediate 'alter table DDL_CLIENTINFO_DBT modify partition by list (t_calc_sid) (partition p99999999x values (''X''))';
    execute immediate 'drop index DDL_CLIENTINFO_DBT_IDX0';
    execute immediate 'create index DDL_CLIENTINFO_DBT_IDX0 on ddl_clientinfo_dbt (T_SFCONTRID) local' ;
    execute immediate 'drop index DDL_CLIENTINFO_DBT_IDX0';
    execute immediate 'create index DDL_CLIENTINFO_DBT_IDX0 on ddl_clientinfo_dbt (T_SFCONTRID) local' ;
    execute immediate 'drop index DDL_CLIENTINFO_DBT_IDX1';
    execute immediate 'create index DDL_CLIENTINFO_DBT_IDX1 on ddl_clientinfo_dbt (T_MARKETID,  T_SERVKIND, T_ISEDP ) local' ;
    execute immediate 'drop index DDL_CLIENTINFO_DBT_IDX2';
    execute immediate 'create index DDL_CLIENTINFO_DBT_IDX2 on ddl_clientinfo_dbt (T_DLCONTRID) local' ;
    execute immediate 'drop index DDL_CLIENTINFO_DBT_IDX3';
    execute immediate 'create index DDL_CLIENTINFO_DBT_IDX3 on ddl_clientinfo_dbt (T_CLIENT) local' ;
  end if;
end;
/