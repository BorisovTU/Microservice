-- Построение индекса
declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME= 'DDL_CLIENTINFO_DBT_IDX0' ;
  if cnt =0 then
     execute immediate 'create index DDL_CLIENTINFO_DBT_IDX0 on ddl_clientinfo_dbt (T_SFCONTRID) local' ;
  end if;
end;
/
