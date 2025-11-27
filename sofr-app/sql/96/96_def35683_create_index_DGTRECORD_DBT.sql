-- Перестроение индекса
declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where i.TABLE_NAME='DGTRECORD_DBT' and i.INDEX_NAME='DGTRECORD_DBT_IDX3' ;
  if cnt =1 then
    execute immediate 'drop index DGTRECORD_DBT_IDX3';
  end if;
  execute immediate 'create index DGTRECORD_DBT_IDX3 on DGTRECORD_DBT (t_statusid, t_applicationid_from, t_sysdate) tablespace INDX';
end;
/
