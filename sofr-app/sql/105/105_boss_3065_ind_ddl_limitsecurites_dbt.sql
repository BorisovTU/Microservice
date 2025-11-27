declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME= 'DDL_LIMITSECURITES_DBT_IDX2' ;
  if cnt =0 then
     execute immediate 'create index DDL_LIMITSECURITES_DBT_IDX2 on ddl_limitsecurites_dbt (t_market_kind,t_market) tablespace INDX';
  end if;
end;
/
