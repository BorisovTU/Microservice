DECLARE
  v_ID       NUMBER := 0;
  v_ParentID NUMBER := 0;
BEGIN
  SELECT T_KEYID INTO v_ParentID FROM DREGPARM_DBT WHERE T_PARENTID = (SELECT T_KEYID FROM DREGPARM_DBT WHERE T_PARENTID = 0 AND LOWER(T_NAME) = LOWER('COMMON')) AND LOWER(T_NAME) = LOWER('КОМИССИИ');

  IF(v_ParentId <> 0) THEN 
    INSERT INTO DREGPARM_DBT (T_KEYID,T_PARENTID,T_NAME,T_TYPE,T_GLOBAL,T_DESCRIPTION,T_SECURITY,T_ISBRANCH,T_TEMPLATE)
                      VALUES (0,v_ParentID,'ОПЛАТА_БЕЗ_ПЕРЕСЧЕТА',0,'X','Значения: 0 - пересчитать начисления (работа по текущему варианту); 1 - не пересчитывать при оплате (работа по новым требованиям РСХБ)',CHR(0),CHR(0),CHR(1)) returning T_KEYID INTO v_ID;
    
    INSERT INTO DREGVAL_DBT (T_KEYID,T_REGKIND,T_OBJECTID,T_BLOCKUSERVALUE,T_EXPDEP,T_LINTVALUE,T_LDOUBLEVALUE,T_FMTBLOBDATA_XXXX)
                     VALUES (v_ID,0,0,CHR(0),0,1,0,'');
  END IF;
END;
/