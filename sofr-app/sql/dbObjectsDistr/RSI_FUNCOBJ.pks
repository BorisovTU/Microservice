--�㭪樨
CREATE OR REPLACE PACKAGE RSI_FuncObj
AS

    PROCEDURE ResetTaskState;

   PROCEDURE addTaskToFuncObj (objType    IN INTEGER,
                               objId      IN INTEGER,
                               funcId     IN INTEGER,
                               priority   IN INTEGER DEFAULT 0);

   PROCEDURE addTaskToFuncObjWithParam (objType    IN INTEGER,
                               objId      IN INTEGER,
                               funcId     IN INTEGER,
                               params IN VARCHAR2,
                               priority   IN INTEGER DEFAULT 0);

    FUNCTION GetFmPtLstPtErrCount(ObjectID IN NUMBER, ObjectType IN NUMBER, FuncID IN NUMBER) RETURN NUMBER;
END RSI_FuncObj;
/
