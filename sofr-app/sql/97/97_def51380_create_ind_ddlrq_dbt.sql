-- Построение индекса
declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where  i.INDEX_NAME= 'DDLRQ_DBT_IDX6' ;
  if cnt =0 then
    execute immediate 'create index DDLRQ_DBT_IDX6 on ddlrq_dbt (t_fiid,T_DOCKIND) tablespace indx ';
  end if;
end;
/
