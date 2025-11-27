-- Перестроение индекса
declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME= 'DNPTXOBJ_DBT_IDXA' ;
  if cnt =1 then
     execute immediate 'drop index DNPTXOBJ_DBT_IDXA';
  end if;
  execute immediate 'create index DNPTXOBJ_DBT_IDXA on DNPTXOBJ_DBT (t_client, t_date, t_kind) tablespace INDX';
end;
/
