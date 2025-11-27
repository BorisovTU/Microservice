declare
  cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME = 'DCDRECORDS_DBT_IDXARGN' ;
  if cnt = 1 then
     execute immediate 'drop index DCDRECORDS_DBT_IDXARGN';
  end if;
  execute immediate 'CREATE INDEX DCDRECORDS_DBT_IDXARGN ON DCDRECORDS_DBT (T_AGREEMENTNUMBER ASC)';
end;
/
