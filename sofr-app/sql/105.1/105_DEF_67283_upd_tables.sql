declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME= 'D724FIREST_DBT_IDX1' ;
  if cnt =1 then
     execute immediate 'drop index D724FIREST_DBT_IDX1';
  end if;
  execute immediate 'create index D724FIREST_DBT_IDX1 on D724FIREST_DBT (t_sessionid, t_client_groupid, t_partyid) tablespace INDX';

  select count(*) into cnt from user_indexes i where i.INDEX_NAME= 'D724ACCREST_DBT_IDX1' ;
  if cnt =1 then
     execute immediate 'drop index D724ACCREST_DBT_IDX1';
  end if;
  execute immediate 'create index D724ACCREST_DBT_IDX1 on D724ACCREST_DBT (t_sessionid, t_client_groupid, t_partyid) tablespace INDX';
  
  select count(*) into cnt from user_indexes i where i.INDEX_NAME= 'D724PFI_DBT_IDX1' ;
  if cnt =1 then
     execute immediate 'drop index D724PFI_DBT_IDX1';
  end if;
  execute immediate 'create index D724PFI_DBT_IDX1 on D724PFI_DBT (t_sessionid, t_client_groupid, t_partyid) tablespace INDX';
  
  select count(*) into cnt from user_indexes i where i.INDEX_NAME= 'D724ARREAR_DBT_IDX1' ;
  if cnt =1 then
     execute immediate 'drop index D724ARREAR_DBT_IDX1';
  end if;
  execute immediate 'create index D724ARREAR_DBT_IDX1 on D724ARREAR_DBT (t_sessionid, t_client_groupid, t_partyid) tablespace INDX';

end;
/

DECLARE
   e_exist_field EXCEPTION;

   PRAGMA EXCEPTION_INIT( e_exist_field, -1430);

BEGIN
   EXECUTE IMMEDIATE 'ALTER TABLE D724R3CLIENT_GROUP ADD T_ACCAMOUNT NUMBER(32,12)';
   EXCEPTION WHEN e_exist_field THEN NULL;
END;
/

DECLARE
   e_exist_field EXCEPTION;

   PRAGMA EXCEPTION_INIT( e_exist_field, -1430);

BEGIN
   EXECUTE IMMEDIATE 'ALTER TABLE D724R3CLIENT_GROUP ADD T_ACCAMOUNTIIS NUMBER(32,12)';
   EXCEPTION WHEN e_exist_field THEN NULL;
END;
/

DECLARE
   e_exist_field EXCEPTION;

   PRAGMA EXCEPTION_INIT( e_exist_field, -1430);

BEGIN
   EXECUTE IMMEDIATE 'ALTER TABLE D724R3CLIENT_GROUP ADD T_ACCKIND NUMBER(5)';
   EXCEPTION WHEN e_exist_field THEN NULL;
END;
/