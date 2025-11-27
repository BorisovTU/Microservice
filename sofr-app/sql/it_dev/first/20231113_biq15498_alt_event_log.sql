-- Построение индекса
declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME= 'ITI_EVENT_SYSDATE' ;
  if cnt = 0 then
    execute immediate 'create index ITI_EVENT_sysdate on ITT_EVENT_LOG (create_sysdate) tablespace indx';
  end if;
  select count(*) into cnt from user_indexes i where i.INDEX_NAME= 'ITI_EVENT_LOG_ID' ;
  if cnt = 0 then
    execute immediate 'create unique index ITI_EVENT_LOG_ID on ITT_EVENT_LOG (LOG_ID) tablespace indx';
  end if;
end;
/