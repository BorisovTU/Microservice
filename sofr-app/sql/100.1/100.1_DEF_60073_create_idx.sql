declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME= 'D724CLIENT_DBT_IDX1' ;
  if cnt =1 then
     execute immediate 'drop index D724CLIENT_DBT_IDX1';
  end if;
  execute immediate 'create index D724CLIENT_DBT_IDX1 on D724CLIENT_DBT (t_sessionid, t_client_groupid, t_partyid) tablespace INDX';

  select count(*) into cnt from user_indexes i where i.INDEX_NAME= 'D724CONTR_DBT_IDX1' ;
  if cnt =1 then
     execute immediate 'drop index D724CONTR_DBT_IDX1';
  end if;
  execute immediate 'create index D724CONTR_DBT_IDX1 on D724CONTR_DBT (t_sessionid, t_contr_groupid, t_party) tablespace INDX';

end;
/
