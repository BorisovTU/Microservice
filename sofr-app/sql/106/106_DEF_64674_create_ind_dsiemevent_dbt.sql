declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where  i.INDEX_NAME= 'DSIEMEVENT_DBT_USR1' ;
  if cnt = 0 then
    execute immediate 'create index DSIEMEVENT_DBT_USR1 on dsiemevent_dbt (t_message) tablespace indx';
  end if;
end;
/
