declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME= 'PKOWRITEOFF_GUID' ;
  if cnt =0 then
     execute immediate 'create index PKOWRITEOFF_GUID on PKO_WRITEOFF (GUID)';
  else
     dbms_output.put_line('Индекс существует');
  end if;
end;