-- Добавление настройки USE_COUPON_REFUSE
DECLARE
   v_ParentId NUMBER;
   v_cnt NUMBER := 0;
   v_ID  NUMBER := 0;
BEGIN
   SELECT t_KeyId INTO v_ParentId FROM DREGPARM_DBT WHERE t_ParentId = (SELECT T_KEYID FROM DREGPARM_DBT WHERE T_PARENTID = 0 AND LOWER(T_NAME) = LOWER('COMMON')) AND LOWER(T_NAME) = LOWER('WORK_MODE');

   IF (v_ParentId <> 0) THEN 
      SELECT COUNT(*) INTO v_cnt FROM DREGPARM_DBT WHERE t_ParentId = v_ParentId AND t_Name = 'USE_COUPON_REFUSE';
      dbms_output.put_line('v_cnt = '||v_cnt);
      IF v_cnt = 0 THEN
        dbms_output.put_line('creating');
          INSERT INTO DREGPARM_DBT 
            (T_KEYID, T_PARENTID, T_NAME, T_TYPE, T_GLOBAL, T_DESCRIPTION, T_SECURITY, T_ISBRANCH, T_TEMPLATE)
          VALUES 
            (0, v_ParentId, 'USE_COUPON_REFUSE', 4, CHR(88),'Использовать функционал "Право отказа от выплаты купона"', CHR(0), CHR(0), CHR(1)) 
          RETURNING T_KEYID INTO v_ID;

         INSERT INTO DREGVAL_DBT 
           (T_KEYID,T_REGKIND,T_OBJECTID,T_BLOCKUSERVALUE,T_EXPDEP,T_LINTVALUE,T_LDOUBLEVALUE,T_FMTBLOBDATA_XXXX)
         VALUES
           (v_ID,0,0,CHR(0),0,0,0,'');
     END IF;
   END IF;
        
EXCEPTION
   WHEN OTHERS THEN NULL;
END;
/