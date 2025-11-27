-- ΅­®Ά«¥­¨¥ ­ αβΰ®©ª¨ ΅ ­ª  Ά regparm, regval
DECLARE
  logID VARCHAR2(50) := 'BOSS-2350 update RegParm RegVal';
  v_ParentId NUMBER;
  v_ID  NUMBER := 0;
    
  PROCEDURE UpdateRegVal(p_KeyID IN NUMBER)
  IS
  BEGIN
     UPDATE DREGVAL_DBT SET T_FMTBLOBDATA_XXXX = NULL WHERE T_KEYID = p_KeyID;
  END;
   
BEGIN
   SELECT t_KeyId INTO v_ParentId FROM DREGPARM_DBT WHERE t_ParentId = (SELECT T_KEYID FROM DREGPARM_DBT WHERE T_PARENTID = 0 AND LOWER(T_NAME) = LOWER('‘•')) AND LOWER(T_NAME) = LOWER('…‘… ‘‹“†‚€…');

   BEGIN
     SELECT t_KeyId INTO v_ParentId FROM DREGPARM_DBT WHERE t_ParentId = v_ParentId AND LOWER(T_NAME) = LOWER('“‚…„‹… ‹…’“');     
   END;

   BEGIN
     SELECT t_KeyId INTO v_ParentId FROM DREGPARM_DBT WHERE t_ParentId = v_ParentId AND LOWER(T_NAME) = LOWER('’. “‚…„.  ‡€›’… „');     
   END;

   IF (v_ParentId <> 0) THEN
     UpdateRegVal(v_ID);
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


