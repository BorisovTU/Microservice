declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where  i.INDEX_NAME= 'DMCACCDOC_DBT_USR6' ;
  if cnt =1 then
    execute immediate 'drop index DMCACCDOC_DBT_USR6';
  end if;
  execute immediate 'create index DMCACCDOC_DBT_USR6 on dmcaccdoc_dbt (t_catid, t_fiid)  tablespace indx';
end;
/
