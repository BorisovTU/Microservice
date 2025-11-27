declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where  i.INDEX_NAME= 'DPARTYNAME_DBT_USR1' ;
  if cnt =0 then
    execute immediate 'create index DPARTYNAME_DBT_USR1 on dpartyname_dbt (t_name) tablespace indx';
  end if;
end;
/
