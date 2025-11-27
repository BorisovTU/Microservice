declare
  cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME = 'PKOWRITEOFF_IDX5';
  if cnt = 0
  then
    execute immediate 'create index PKOWRITEOFF_IDX5 on PKO_WRITEOFF (custodyorderid)  tablespace indx ';
  end if;
end;
