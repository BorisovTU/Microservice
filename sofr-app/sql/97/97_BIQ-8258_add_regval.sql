DECLARE
  v_ID       NUMBER := 0;
  v_ParentID NUMBER := 0;
BEGIN
  SELECT T_KEYID INTO v_ParentID FROM DREGPARM_DBT WHERE T_PARENTID = (SELECT T_KEYID FROM DREGPARM_DBT WHERE T_PARENTID = 0 AND LOWER(T_NAME) = LOWER('РСХБ')) AND LOWER(T_NAME) = LOWER('БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ');

  IF(v_ParentId <> 0) THEN 
    INSERT INTO DREGPARM_DBT (T_KEYID,T_PARENTID,T_NAME,T_TYPE,T_GLOBAL,T_DESCRIPTION,T_SECURITY,T_ISBRANCH,T_TEMPLATE)
                      VALUES (0,v_ParentID,'ИНФОРМИРОВАНИЕ ПО ИИС',0,CHR(0),CHR(1),CHR(0),CHR(0),CHR(1)) returning T_KEYID INTO v_ID;

    INSERT INTO DREGVAL_DBT (T_KEYID,T_REGKIND,T_OBJECTID,T_BLOCKUSERVALUE,T_EXPDEP,T_LINTVALUE,T_LDOUBLEVALUE,T_FMTBLOBDATA_XXXX)
                     VALUES (v_ID,0,0,CHR(0),0,0,0,'');

    v_ParentId := v_ID;
    IF(v_ParentId <> 0) THEN 
      INSERT INTO DREGPARM_DBT (T_KEYID,T_PARENTID,T_NAME,T_TYPE,T_GLOBAL,T_DESCRIPTION,T_SECURITY,T_ISBRANCH,T_TEMPLATE)
                        VALUES (0,v_ParentID,'ПЕРВОЕ СООБЩ. КЛИЕНТУ ИИС',0,'X','Количество рабочих дней, через которое клиенту отправляется первичное сообщение о необходимости предоставления документов о расторжении ИИС у другого ПУ',CHR(0),CHR(0),CHR(1)) returning T_KEYID INTO v_ID;
     
      INSERT INTO DREGVAL_DBT (T_KEYID,T_REGKIND,T_OBJECTID,T_BLOCKUSERVALUE,T_EXPDEP,T_LINTVALUE,T_LDOUBLEVALUE,T_FMTBLOBDATA_XXXX)
                       VALUES (v_ID,0,0,CHR(0),0,0,10,'');

      INSERT INTO DREGPARM_DBT (T_KEYID,T_PARENTID,T_NAME,T_TYPE,T_GLOBAL,T_DESCRIPTION,T_SECURITY,T_ISBRANCH,T_TEMPLATE)
                        VALUES (0,v_ParentID,'ПОВТОРНОЕ СООБЩ. КЛИЕНТУ ИИС',0,'X','Количество календарных дней, через которое клиенту отправляются повторные сообщения',CHR(0),CHR(0),CHR(1)) returning T_KEYID INTO v_ID;
     
      INSERT INTO DREGVAL_DBT (T_KEYID,T_REGKIND,T_OBJECTID,T_BLOCKUSERVALUE,T_EXPDEP,T_LINTVALUE,T_LDOUBLEVALUE,T_FMTBLOBDATA_XXXX)
                       VALUES (v_ID,0,0,CHR(0),0,0,7,'');

      INSERT INTO DREGPARM_DBT (T_KEYID,T_PARENTID,T_NAME,T_TYPE,T_GLOBAL,T_DESCRIPTION,T_SECURITY,T_ISBRANCH,T_TEMPLATE)
                        VALUES (0,v_ParentID,'ПЕРВЫЙ ОТЧЕТ ОД И ДРРК',0,'X','Количество календарных дней, оставшихся до даты расторжения договора ИИС (первичная отправка отчета)',CHR(0),CHR(0),CHR(1)) returning T_KEYID INTO v_ID;
     
      INSERT INTO DREGVAL_DBT (T_KEYID,T_REGKIND,T_OBJECTID,T_BLOCKUSERVALUE,T_EXPDEP,T_LINTVALUE,T_LDOUBLEVALUE,T_FMTBLOBDATA_XXXX)
                       VALUES (v_ID,0,0,CHR(0),0,0,5,'');

      INSERT INTO DREGPARM_DBT (T_KEYID,T_PARENTID,T_NAME,T_TYPE,T_GLOBAL,T_DESCRIPTION,T_SECURITY,T_ISBRANCH,T_TEMPLATE)
                        VALUES (0,v_ParentID,'ПОВТОРНЫЙ ОТЧЕТ ОД И ДРРК',0,'X','Количество календарных дней, оставшихся до даты расторжения договора ИИС (повторная отправка отчета)',CHR(0),CHR(0),CHR(1)) returning T_KEYID INTO v_ID;
     
      INSERT INTO DREGVAL_DBT (T_KEYID,T_REGKIND,T_OBJECTID,T_BLOCKUSERVALUE,T_EXPDEP,T_LINTVALUE,T_LDOUBLEVALUE,T_FMTBLOBDATA_XXXX)
                       VALUES (v_ID,0,0,CHR(0),0,0,1,'');
    END IF;
  END IF;

EXCEPTION 
  WHEN DUP_VAL_ON_INDEX THEN NULL;
END;
/

BEGIN
  EXECUTE IMMEDIATE 'COMMIT';
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/