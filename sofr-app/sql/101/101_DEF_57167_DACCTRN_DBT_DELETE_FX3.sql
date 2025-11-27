CREATE OR REPLACE TRIGGER "DACCTRN_DBT_DELETE" 
BEFORE DELETE OR  UPDATE ON DACCTRN_DBT 
REFERENCING NEW AS NEW OLD AS OLD
FOR EACH ROW
DECLARE
    OBJECT_TYPE          CONSTANT uTableProcessEvent_dbt.T_OBJECTTYPE%TYPE := 1; -- дистрибутивный вид объекта Проводка
    OBJECT_STATUS_WAIT   CONSTANT uTableProcessEvent_dbt.T_STATUS%TYPE := 11; -- статус: Ожидание ручной обработки
    OBJECT_STATUS_ERROR   CONSTANT uTableProcessEvent_dbt.T_STATUS%TYPE := 5;
    OBJECT_STATUS_READY  CONSTANT uTableProcessEvent_dbt.T_STATUS%TYPE := 1;
    OBJECT_STATUS_DEL    CONSTANT uTableProcessEvent_dbt.T_STATUS%TYPE := -45; 
    
    OBJECT_OPER_TYPE_CRE CONSTANT uTableProcessEvent_dbt.T_TYPE%TYPE := 1; -- тип: Создание
   
    v_ObjectId           uTableProcessEvent_dbt.T_OBJECTID%TYPE;
    v_RecId               uTableProcessEvent_dbt.T_RECID%TYPE;
    v_Status              uTableProcessEvent_dbt.T_STATUS%TYPE;
    v_Timestamp          uTableProcessEvent_dbt.T_TIMESTAMP%TYPE;
    v_Type               uTableProcessEvent_dbt.T_TYPE%TYPE;
    
BEGIN

   --найдём запись по объекту синхронизации в таблице синхронизации 
    BEGIN

        SELECT A.T_RECID, A.T_OBJECTID, A.T_STATUS, A.T_TIMESTAMP, A.T_TYPE
          INTO  v_RecId, v_ObjectId , v_Status, v_Timestamp, v_Type
          FROM uTableProcessEvent_dbt A
          WHERE A.T_OBJECTTYPE = OBJECT_TYPE
               AND A.T_OBJECTID = :OLD.T_ACCTRNID
               AND A.T_STATUS  NOT IN ( OBJECT_STATUS_WAIT,OBJECT_STATUS_ERROR,OBJECT_STATUS_DEL)
               AND A.T_RECID = (SELECT MAX(B.T_RECID) 
                              FROM uTableProcessEvent_dbt B
                             WHERE B.T_OBJECTTYPE = A.T_OBJECTTYPE
                               AND B.T_OBJECTID = A.T_OBJECTID
                               AND B.T_TYPE = OBJECT_OPER_TYPE_CRE);      
       
         IF  v_Status = OBJECT_STATUS_READY AND  (v_Timestamp >= (sysdate - NUMTODSINTERVAL(1, 'MINUTE')))
         AND v_type = OBJECT_OPER_TYPE_CRE
         AND (DELETING or ( UPDATING  AND :new.T_STATE = 4))
          THEN
              
            UPDATE uTableProcessEvent_dbt SET T_STATUS = OBJECT_STATUS_DEL, T_TIMESTAMP = SYSDATE  WHERE T_RECID = v_RecId;
            v_ObjectId := NULL; -- останавливаем дальнейшие проверки
         END IF;

    EXCEPTION WHEN NO_DATA_FOUND THEN
        v_ObjectId := NULL;
    END;

/* DELETE */

    IF DELETING AND  (:OLD.T_USERFIELD4 = CHR(1) OR :OLD.T_USERFIELD4 IS NULL)
                       AND v_ObjectId IS NOT NULL
    THEN
        RAISE_APPLICATION_ERROR (-20001,'Невозможно удалить проводку. Дождитесь ответа из Бисквит');
     END IF; 
 
/* UPDATE */

    IF UPDATING AND  (:OLD.T_USERFIELD4 = CHR(1) OR :OLD.T_USERFIELD4 IS NULL)
                        AND v_ObjectId IS NOT NULL
                        AND :NEW.T_STATE = 4
                        AND :NEW.T_STATE != :OLD.T_STATE
    THEN
         RAISE_APPLICATION_ERROR (-20001,'Невозможно удалить проводку. Дождитесь ответа из Бисквит');
    END IF; 

END;
/