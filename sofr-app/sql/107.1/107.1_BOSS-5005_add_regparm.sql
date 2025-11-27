DECLARE
  v_ID       NUMBER := 0;
  v_ParentID NUMBER := 0;
BEGIN
  SELECT T_KEYID INTO v_ParentID FROM DREGPARM_DBT WHERE T_PARENTID = (SELECT T_KEYID FROM DREGPARM_DBT WHERE T_PARENTID = 0 AND LOWER(T_NAME) = LOWER('РСХБ')) AND LOWER(T_NAME) = LOWER('ИНТЕГРАЦИЯ');

  IF(v_ParentId <> 0) THEN 
    INSERT INTO DREGPARM_DBT (T_KEYID,T_PARENTID,T_NAME,T_TYPE,T_GLOBAL,T_DESCRIPTION,T_SECURITY,T_ISBRANCH,T_TEMPLATE)
                      VALUES (0,v_ParentID,'ПРОВЕРКА ОБРАБОТКИ СПБ',4,'X','Выполнение проверки обработки пунктов БА по СПБ при предоставлении остатков по сервису GetBrokerContractInfo',CHR(0),CHR(0),CHR(1)) returning T_KEYID INTO v_ID;
    
    INSERT INTO DREGVAL_DBT (T_KEYID,T_REGKIND,T_OBJECTID,T_BLOCKUSERVALUE,T_EXPDEP,T_LINTVALUE,T_LDOUBLEVALUE,T_FMTBLOBDATA_XXXX)
                     VALUES (v_ID,0,0,CHR(0),0,0,0,'');
  END IF;
END;
/