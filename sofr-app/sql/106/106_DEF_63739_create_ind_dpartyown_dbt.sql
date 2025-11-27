declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where  i.INDEX_NAME= 'DPARTYOWN_DBT_USR1' ;
  if cnt =0 then
    execute immediate 'create index DPARTYOWN_DBT_USR1 on dpartyown_dbt (t_partykind, t_subkind)  tablespace indx';
  end if;
end;
/
