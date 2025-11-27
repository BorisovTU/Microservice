-- Построение индекса
declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where  i.INDEX_NAME= 'DSFBASOBJ_DBT_IDX1' ;
  if cnt =0 then
     execute immediate 'create index DSFBASOBJ_DBT_IDX1 on DSFBASOBJ_DBT (t_baseobjectid, t_baseobjecttype) tablespace INDX ';
  end if;
end;
/
