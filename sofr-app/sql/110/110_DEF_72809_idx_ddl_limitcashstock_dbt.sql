-- Перестроение индекса
declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME= 'DDL_LIMITCASHSTOCK_DBT_IDX3' ;
  if cnt =1 then
    execute immediate 'drop index DDL_LIMITCASHSTOCK_DBT_IDX3';
  end if;
  execute immediate 'create index DDL_LIMITCASHSTOCK_DBT_IDX3 on ddl_limitcashstock_dbt (t_internalaccount, t_date)  tablespace indx';
end;
/
