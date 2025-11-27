-- Добавление настроек банка в regparm, regval
DECLARE
  logID VARCHAR2(50) := 'DEF-62575 Insert RegParm RegVal';
  v_ParentId NUMBER;
  v_cnt NUMBER := 0;
  v_ID  NUMBER := 0;
   
  FUNCTION AddRegParm(p_parentID IN NUMBER, p_name IN VARCHAR2, p_type IN NUMBER, p_description IN VARCHAR2) RETURN NUMBER
  IS
     v_parmid NUMBER := 0;
  BEGIN
     BEGIN
       SELECT t_KeyID INTO v_parmid FROM DREGPARM_DBT WHERE t_ParentId = p_parentID AND t_Name = p_name;
       EXCEPTION
         WHEN NO_DATA_FOUND THEN 
           v_parmid := 0;
     END;
     
     IF v_parmid = 0 THEN
       INSERT INTO DREGPARM_DBT 
            (T_KEYID, T_PARENTID, T_NAME, T_TYPE, T_GLOBAL, T_DESCRIPTION, T_SECURITY, T_ISBRANCH, T_TEMPLATE)
          VALUES 
            (0, p_parentID, p_name, p_type, CHR(0), p_description, CHR(0), CHR(0), CHR(1)) 
          RETURNING T_KEYID INTO v_parmid;
     END IF;
     
     RETURN v_parmid;
  END;
   
  PROCEDURE AddRegVal(p_KeyID IN NUMBER, p_intValue IN NUMBER, p_blobValue IN VARCHAR2)
  IS
  BEGIN
     INSERT INTO DREGVAL_DBT 
       (T_KEYID,T_REGKIND,T_OBJECTID,T_BLOCKUSERVALUE,T_EXPDEP,T_LINTVALUE,T_LDOUBLEVALUE,T_FMTBLOBDATA_XXXX)
     VALUES
       (p_KeyID, 0, 0, CHR(0), 0, p_intValue, 0, CASE WHEN p_blobValue = '' THEN null ELSE to_blob(utl_raw.cast_to_raw(p_blobValue)) END);
  END;
   
BEGIN
   SELECT t_KeyId INTO v_ParentId FROM DREGPARM_DBT 
   WHERE T_PARENTID = 
   ( SELECT t_KeyId FROM DREGPARM_DBT 
   WHERE T_PARENTID = 0 
   AND LOWER(T_NAME) = LOWER('РСХБ')) 
   AND LOWER(T_NAME) = LOWER('ДИРЕКТОРИИ') ;

   IF (v_ParentId <> 0) THEN
     v_ID := AddRegParm(v_ParentId, 'IMPORT_ORDERS', 
       2, 'сетевой ресурс для Excel-файлов в XLS_IMP468.MAC');
     AddRegVal(v_ID, 0, '\\sgo-fc01-r03.go.rshbank.ru\sofr_for_etl\inbox\message\');
   END IF;
   
   COMMIT;
        
EXCEPTION WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
END;
/
