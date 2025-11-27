CREATE OR REPLACE TRIGGER daccount_dbt_Synh_With_NA
AFTER /*DELETE OR*/ INSERT OR UPDATE ON daccount_dbt REFERENCING NEW AS NEW OLD AS OLD
FOR EACH ROW
DECLARE

 OBJECT_TYPE            CONSTANT uTableProcessEvent_dbt.T_OBJECTTYPE%TYPE := 4; --дистрибутивный вид объекта —чет
 OBJECT_STATUS          CONSTANT uTableProcessEvent_dbt.T_STATUS%TYPE := 1; --готов к обработке

 v_Status               uTableProcessEvent_dbt.T_STATUS%TYPE;
 v_ObjectId             uTableProcessEvent_dbt.T_OBJECTID%TYPE;
 v_RecId                uTableProcessEvent_dbt.T_RECID%TYPE;
 v_Type                 uTableProcessEvent_dbt.T_TYPE%TYPE;

BEGIN

 --найдЄм запись по объекту синхронизации в таблице синхронизации 
 BEGIN

   SELECT A.T_RECID, A.T_OBJECTID, A.T_STATUS, A.T_TYPE INTO v_RecId, v_ObjectId, v_Status, v_Type
   FROM uTableProcessEvent_dbt A
   WHERE A.T_OBJECTTYPE = OBJECT_TYPE
    AND A.T_OBJECTID = COALESCE( :new.T_ACCOUNTID, :old.T_ACCOUNTID )
    AND A.T_RECID = ( SELECT MAX(B.T_RECID) 
                      FROM uTableProcessEvent_dbt B
                      WHERE B.T_OBJECTTYPE = A.T_OBJECTTYPE
                       AND B.T_OBJECTID = A.T_OBJECTID);
   
 EXCEPTION WHEN NO_DATA_FOUND THEN

  v_ObjectId := NULL;
  v_Status := NULL;
  v_RecId := NULL;

 END;
    

 IF INSERTING THEN

  INSERT INTO uTableProcessEvent_dbt( T_TIMESTAMP, T_OBJECTTYPE, T_OBJECTID, T_TYPE, T_STATUS )
  VALUES( SYSDATE, OBJECT_TYPE, :new.T_ACCOUNTID, 1, OBJECT_STATUS);
 -- работает с :new
 END IF;
 

 IF UPDATING THEN

   --если в таблице синхронизации нет записи по счету
   IF v_Status IS NOT NULL AND v_Status = OBJECT_STATUS THEN

    UPDATE uTableProcessEvent_dbt
    SET T_TYPE = 2, T_TIMESTAMP = SYSDATE
    WHERE T_RECID = v_RecId;

   ELSE

    INSERT INTO uTableProcessEvent_dbt( T_TIMESTAMP, T_OBJECTTYPE, T_OBJECTID, T_TYPE, T_STATUS )
    VALUES( SYSDATE, OBJECT_TYPE, :new.T_ACCOUNTID, 2, OBJECT_STATUS);

   END IF;

 -- работает с :old и с :new
 END IF;


 IF DELETING THEN

   IF v_Status IS NOT NULL AND v_Status = OBJECT_STATUS THEN

    --переведем в статус јрхив
    UPDATE uTableProcessEvent_dbt
    SET T_STATUS = 3, T_TIMESTAMP = SYSDATE
    WHERE T_RECID = v_RecId;

   ELSE

    INSERT INTO uTableProcessEvent_dbt( T_TIMESTAMP, T_OBJECTTYPE, T_OBJECTID, T_TYPE, T_STATUS )
    VALUES( SYSDATE, OBJECT_TYPE, :old.T_ACCOUNTID, 3, OBJECT_STATUS);

   END IF;

   -- работает с :old
 END IF;

EXCEPTION

  WHEN OTHERS
  THEN DBMS_OUTPUT.PUT_LINE(sqlerrm);

END;



