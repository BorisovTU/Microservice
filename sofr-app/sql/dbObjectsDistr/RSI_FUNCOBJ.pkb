--�㭪樨
CREATE OR REPLACE PACKAGE BODY RSI_FuncObj
AS

 PROCEDURE ResetTaskState
   IS
   BEGIN

        UPDATE dfuncobj_dbt
        SET t_State = 0
        WHERE t_state = 20;


   EXCEPTION
      WHEN OTHERS
      THEN
         ROLLBACK;
         RAISE;
   END;

   PROCEDURE addTaskToFuncObj (objType    IN INTEGER,
                               objId      IN INTEGER,
                               funcId     IN INTEGER,
                               priority   IN INTEGER DEFAULT 0)
   IS
   BEGIN
      INSERT INTO dfuncobj_dbt (t_objecttype,
                                t_objectid,
                                t_funcid,
                                t_state,
                                t_priority)
           VALUES (objType,
                   objId,
                   funcId,
                   0,
                   priority);
   END;

   PROCEDURE addTaskToFuncObjWithParam (objType    IN INTEGER,
                                        objId      IN INTEGER,
                                        funcId     IN INTEGER,
                                        params     IN VARCHAR2,
                                        priority   IN INTEGER DEFAULT 0)
   IS
   BEGIN
      INSERT INTO dfuncobj_dbt (t_objecttype,
                                t_objectid,
                                t_funcid,
                                t_state,
                                t_param,
                                t_priority)
           VALUES (objType,
                   objId,
                   funcId,
                   0,
                   params,
                   priority);
   END;

   FUNCTION GetFmPtLstPtErrCount(ObjectID IN NUMBER, ObjectType IN NUMBER, FuncID IN NUMBER) RETURN NUMBER
   IS
      m_PartyCount NUMBER := 0;
      m_tmpCount NUMBER := 0;
   BEGIN
      FOR rec IN (select * from dfuncobj_dbt where 
         t_ObjectType = ObjectType and 
         t_ObjectID = ObjectID and 
         t_FuncID = FuncID and
         t_state not in (0, 20))
      LOOP
         EXECUTE IMMEDIATE 'select count(1) from (select regexp_substr(''' || rec.t_param || ''',''[^,]+'', 1, level) from dual connect by LEVEL <= REGEXP_COUNT(''' || rec.t_param || ''', '','') + 1)'
            INTO m_tmpCount;

         m_PartyCount := m_PartyCount + m_tmpCount;
      END LOOP;

      RETURN m_PartyCount;
   END;

END RSI_FuncObj;
/
