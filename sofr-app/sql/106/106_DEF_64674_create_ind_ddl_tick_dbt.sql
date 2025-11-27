declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where  i.INDEX_NAME= 'DDL_TICK_DBT_USR2' ;
  if cnt = 0 then
    execute immediate 'create index DDL_TICK_DBT_USR2 on ddl_tick_dbt (t_dealstatus,t_clientcontrid) tablespace indx';
  end if;
  select count(*) into cnt from user_indexes i where  i.INDEX_NAME= 'DDL_TICK_DBT_USR3' ;
  if cnt = 0 then
    execute immediate 'create index DDL_TICK_DBT_USR3 on DDL_TICK_DBT (t_dealstatus,t_partycontrid ) tablespace indx';
  end if;
end;
/
