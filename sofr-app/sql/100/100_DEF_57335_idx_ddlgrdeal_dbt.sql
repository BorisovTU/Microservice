-- Добавление индекса
declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where i.TABLE_NAME='DDLGRDEAL_DBT' and i.INDEX_NAME='DDLGRDEAL_DBT_IDX5' ;
  if cnt = 0 then
   execute immediate 'create index DDLGRDEAL_DBT_IDX5 on ddlgrdeal_dbt (t_plandate, t_dockind, t_fiid) tablespace indx  ' ;
  end if;
end;
/
