declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME= 'DOPRSTEP_DBT_IDXG' ;
  if cnt = 0 then
    EXECUTE IMMEDIATE 'CREATE INDEX DOPRSTEP_DBT_IDXG ON DOPRSTEP_DBT (T_ID_OPERATION, T_PREVIOUS_STEP, T_ID_STEP)';
  end if;
end;
/
