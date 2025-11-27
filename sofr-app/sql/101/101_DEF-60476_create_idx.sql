declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME= 'DSCUNIAVRSTOBJ_DBT_IDX0' ;
  if cnt =1 then
     execute immediate 'drop index DSCUNIAVRSTOBJ_DBT_IDX0';
  end if;
  execute immediate 'CREATE INDEX DSCUNIAVRSTOBJ_DBT_IDX0 ON DSCUNIAVRSTOBJ_DBT (T_ID_OPERATION, T_ISWRT, T_OBJKIND, T_OBJID, T_FIID)';
end;
/
