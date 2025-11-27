declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where  i.INDEX_NAME= 'PKOWRITEOFF_IDX4' ;
  if cnt =1 then
    execute immediate 'drop index PKOWRITEOFF_IDX4';
  end if;
  execute immediate 'create index PKOWRITEOFF_IDX4 on pko_writeoff (dealid)  tablespace indx';
end;
/
