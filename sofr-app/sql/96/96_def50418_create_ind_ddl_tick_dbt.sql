-- Построение индекса
declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where  i.INDEX_NAME= 'DDL_TICK_DBT_USR1' ;
  if cnt =0 then
    execute immediate 'create index DDL_TICK_DBT_USR1 on DDL_TICK_DBT (T_PARTYCONTRID, T_DEALDATE)  tablespace indx ';
  end if;
end;
/
