DECLARE
   v_ParentId NUMBER;
   v_cnt NUMBER := 0;
   v_ID  NUMBER := 0;
   v_name VARCHAR2(100) := 'QUIK_ENROL_ORDER_TRANSFER';
BEGIN
   SELECT t_KeyId 
     INTO v_ParentId 
     FROM DREGPARM_DBT WHERE t_ParentId = 0 AND LOWER(T_NAME) = LOWER('SECUR');

   IF (v_ParentId <> 0) THEN 
      SELECT COUNT(*) INTO v_cnt FROM DREGPARM_DBT WHERE t_ParentId = v_ParentId AND t_Name = v_name;
      
      IF v_cnt = 0 THEN
          INSERT INTO DREGPARM_DBT 
            (T_KEYID, T_PARENTID, T_NAME, T_TYPE, T_GLOBAL, T_DESCRIPTION, T_SECURITY, T_ISBRANCH, T_TEMPLATE)
          VALUES 
            (0, v_ParentId, v_name, 4/*flag*/, CHR(0),'Передача поручений по зачислениям в QUIK', CHR(0), CHR(0), CHR(1)) RETURNING T_KEYID INTO v_ID;

         INSERT INTO DREGVAL_DBT 
           (T_KEYID,T_REGKIND,T_OBJECTID,T_BLOCKUSERVALUE,T_EXPDEP,T_LINTVALUE,T_LDOUBLEVALUE,T_FMTBLOBDATA_XXXX)
         VALUES
           (v_ID,0,0,CHR(0),0,0,0,'');
     END IF;
   END IF;
        
EXCEPTION
   WHEN OTHERS THEN NULL;
END;
