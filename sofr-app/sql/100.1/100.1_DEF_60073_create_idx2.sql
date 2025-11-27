declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME= 'D724CONTR_DBT_IDX3' ;
  if cnt =0 then
      execute immediate 'create index D724CONTR_DBT_IDX3 on D724CONTR_DBT (T_SESSIONID, T_PARTY) tablespace INDX';
  end if;

  select count(*) into cnt from user_indexes i where i.INDEX_NAME= 'D724FIREST_DBT_IDX1' ;
  if cnt =0 then
      execute immediate 'create index D724FIREST_DBT_IDX1 on D724FIREST_DBT (T_SESSIONID, T_CLIENT_GROUPID) tablespace INDX';
  end if;

  select count(*) into cnt from user_indexes i where i.INDEX_NAME= 'D724ACCREST_DBT_IDX1' ;
  if cnt =0 then
      execute immediate 'create index D724ACCREST_DBT_IDX1 on D724ACCREST_DBT (T_SESSIONID, T_CLIENT_GROUPID) tablespace INDX';
  end if;

  select count(*) into cnt from user_indexes i where i.INDEX_NAME= 'D724ARREAR_DBT_IDX1' ;
  if cnt =0 then
      execute immediate 'create index D724ARREAR_DBT_IDX1 on D724ARREAR_DBT (T_SESSIONID, T_CLIENT_GROUPID) tablespace INDX';
  end if;

end;
/
