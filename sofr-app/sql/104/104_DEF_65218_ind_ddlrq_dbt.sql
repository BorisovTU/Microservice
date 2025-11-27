declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME= 'DDLRQ_DBT_USR2' ;
  if cnt =0 then
     execute immediate 'create index DDLRQ_DBT_USR2 on ddlrq_dbt (t_dockind, t_plandate) tablespace INDX';
  end if;
end;
/
