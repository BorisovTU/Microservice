declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME= 'DDLRQ_DBT_IDX6' ;
  if cnt =1 then
    execute immediate 'drop index DDLRQ_DBT_IDX6';
  end if;
  execute immediate 'create index DDLRQ_DBT_IDX6 on ddlrq_dbt (t_fiid, t_dockind, t_factdate)  tablespace indx';
end;
/
