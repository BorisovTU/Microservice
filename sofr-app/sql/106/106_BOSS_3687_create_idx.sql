declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME= 'ITI_LOG__OBJECT_NAME' ;
  if cnt =1 then
     execute immediate 'drop index ITI_LOG__OBJECT_NAME ';
  end if;
  execute immediate 'create index ITI_LOG__OBJECT_NAME on ITT_LOG (object_name, create_sysdate) tablespace INDX';
  select count(*) into cnt from user_indexes i where i.INDEX_NAME= 'ITI_LOG__DATE' ;
  if cnt = 0 then
     execute immediate 'create index ITI_LOG__DATE on ITT_LOG (create_sysdate) tablespace INDX';
  end if;
end;
/
