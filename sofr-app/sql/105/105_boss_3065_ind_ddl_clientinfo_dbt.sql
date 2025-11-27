declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME= 'DDL_CLIENTINFO_DBT_IDX1' ;
  if cnt =1 then
     execute immediate 'drop index DDL_CLIENTINFO_DBT_IDX1';
  end if;
  execute immediate 'create index DDL_CLIENTINFO_DBT_IDX1 on ddl_clientinfo_dbt (T_MARKETID, T_ISEDP, T_SERVKIND)  local';
end;
/
