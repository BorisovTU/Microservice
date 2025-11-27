--настройка должна быть глобальной
DECLARE
   v_ParentId NUMBER;
BEGIN
   SELECT t_KeyId INTO v_ParentId FROM DREGPARM_DBT WHERE t_ParentId = (SELECT T_KEYID FROM DREGPARM_DBT WHERE T_PARENTID = 0 AND LOWER(T_NAME) = LOWER('РСХБ')) AND LOWER(T_NAME) = LOWER('ИНТЕГРАЦИЯ');

   IF (v_ParentId <> 0) THEN
       SELECT t_KeyId INTO v_ParentId FROM DREGPARM_DBT WHERE t_ParentId = v_ParentId AND LOWER(T_NAME) = LOWER('ЗАГРУЗКА_НЕСКОЛЬКИХ_ДБО');
       UPDATE dregparm_dbt SET T_GLOBAL=chr(88) WHERE T_KEYID=v_ParentId;
   END IF;
   
   COMMIT;
END;   
/
