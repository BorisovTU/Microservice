declare
  n integer;
begin
  select count(*) into n from user_indexes i where i.INDEX_NAME = 'ITI_LOG__DATE_OBJECT_NAME';
  if n > 0
  then
    execute immediate 'drop index ITI_LOG__DATE_OBJECT_NAME';
  end if;
end;
/