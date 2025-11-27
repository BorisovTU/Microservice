DECLARE
   v_ParentId NUMBER;
   v_cnt NUMBER := 0;
   v_ID  NUMBER := 0;
BEGIN
   SELECT t_KeyId INTO v_ParentId FROM DREGPARM_DBT WHERE LOWER(T_NAME) = LOWER('РСХБ') AND T_PARENTID = 0;

   IF (v_ParentId <> 0) THEN 
      SELECT COUNT(*) INTO v_cnt FROM DREGPARM_DBT WHERE t_ParentId = v_ParentId AND t_Name = 'ПАЧКА ДЛЯ СПФИ';
      
      IF v_cnt = 0 THEN
          INSERT INTO DREGPARM_DBT 
            (T_KEYID, T_PARENTID, T_NAME, T_TYPE, T_GLOBAL, T_DESCRIPTION, T_SECURITY, T_ISBRANCH, T_TEMPLATE)
          VALUES 
            (0, v_ParentId, 'ПАЧКА ДЛЯ СПФИ', 0, CHR(0),'Номер пачки, который проставляется в проводки по сделкам СПФИ', CHR(0), CHR(0), CHR(1)) RETURNING T_KEYID INTO v_ID;

         INSERT INTO DREGVAL_DBT 
           (T_KEYID,T_REGKIND,T_OBJECTID,T_BLOCKUSERVALUE,T_EXPDEP,T_LINTVALUE,T_LDOUBLEVALUE,T_FMTBLOBDATA_XXXX)
         VALUES
           (v_ID,0,0,CHR(0),0,30,0,'');
     END IF;
   END IF;

END;
/
