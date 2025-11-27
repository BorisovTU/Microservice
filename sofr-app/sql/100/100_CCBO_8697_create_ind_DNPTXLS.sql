declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME= 'DNPTXLS_DBT_IDX2' ;
  if cnt =0 then
     execute immediate 'create index DNPTXLS_DBT_IDX2 on DNPTXLS_DBT (t_parentid) tablespace INDX';
  end if;
end;
/
