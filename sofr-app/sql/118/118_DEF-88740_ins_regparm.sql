DECLARE
 v_ParentID NUMBER(10);
 v_KeyID NUMBER(10);

 e_object_exists EXCEPTION; 
 PRAGMA EXCEPTION_INIT(e_object_exists, -1);

BEGIN
  SELECT t_KeyID INTO v_ParentID
    FROM dregparm_dbt 
   WHERE t_Name = 'COMMON'
     AND t_ParentID = 0;

  SELECT t_KeyID INTO v_ParentID
    FROM dregparm_dbt 
   WHERE t_Name = 'ACTIVE DIRECTORY'
     AND t_ParentID = v_ParentID;

   INSERT INTO dregparm_dbt(T_KEYID, T_PARENTID, T_NAME, T_TYPE, T_GLOBAL,T_DESCRIPTION, T_SECURITY, T_ISBRANCH)
     VALUES(0,v_ParentID,'TEST_MODE',4,'X','Cинхронизация выполняется в тестовом режиме, выполняются все запросы на сервер Active Directory, но изменения не применяются в БД',chr(0),chr(0))
     RETURNING T_KEYID INTO v_KeyID;

    INSERT INTO dregval_dbt(T_KEYID, T_REGKIND, T_OBJECTID, T_BLOCKUSERVALUE, T_EXPDEP, T_LINTVALUE, T_LDOUBLEVALUE, T_FMTBLOBDATA_XXXX)
      VALUES(v_KeyID, 0, 0, chr(0), 0, 0, 0, '');


   INSERT INTO dregparm_dbt(T_KEYID, T_PARENTID, T_NAME, T_TYPE, T_GLOBAL,T_DESCRIPTION, T_SECURITY, T_ISBRANCH)
     VALUES(0,v_ParentID,'REPORT_MODE',0,'X','Режим формирования отчета',chr(0),chr(0))
     RETURNING T_KEYID INTO v_KeyID;

   INSERT INTO dregval_dbt(T_KEYID, T_REGKIND,T_OBJECTID,T_BLOCKUSERVALUE,T_EXPDEP,T_LINTVALUE,T_LDOUBLEVALUE,T_FMTBLOBDATA_XXXX)
        VALUES(v_KeyID, 0, 0, chr(0), 0, 0, 0, '');

EXCEPTION WHEN e_object_exists THEN NULL;

END;
/



