-- Построение индекса
declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME= 'DDL_LIMITCASHSTOCK_DBT_IDX3' ;
  if cnt =0 then
     execute immediate 'create index DDL_LIMITCASHSTOCK_DBT_IDX3 on ddl_limitcashstock_dbt (t_client, t_internalaccount, t_date) tablespace INDX';
  end if;
end;
/
