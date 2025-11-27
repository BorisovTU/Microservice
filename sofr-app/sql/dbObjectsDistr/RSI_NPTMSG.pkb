

CREATE OR REPLACE PACKAGE BODY RSI_NPTMSG IS

   PROCEDURE PutMsgEx(pType IN NUMBER, pMessage IN VARCHAR2, pNum IN NUMBER)
   AS
     v_IsDebug NUMBER := 0; -- 1 - отладка, 0 - нет
   BEGIN
     if ((v_IsDebug = 0 and pType <> RSI_NPTXC.MES_DEBUG) or v_IsDebug = 1) then
        INSERT INTO DNPTXMES_TMP (T_NUM,
                                  T_TIME,
                                  T_TYPE,
                                  T_MESSAGE
                                 )
                          VALUES (pNum,
                                  TO_DATE('01.01.0001 '||TO_CHAR( SYSDATE, 'HH24:MI:SS'),'DD.MM.YYYY HH24:MI:SS'),
                                  pType,
                                  pMessage
                                 );
     end if;
   END;

   FUNCTION GetNextMsgNum RETURN NUMBER
   AS
     v_Num NUMBER := 0;
   BEGIN
     SELECT NVL(MAX(T_NUM), 0)+1 INTO v_Num FROM DNPTXMES_TMP;

     RETURN v_Num;
   END;
   
   
   -- Вывод сообщения об ошибке
   PROCEDURE PutMsg( pType IN NUMBER, pMessage IN VARCHAR2 )
   IS
   BEGIN
     PutMsgEx(pType, pMessage, GetNextMsgNum());
   END;
   
   -- Вывод сообщения об ошибке в автономной транзакции (для использования с RSI_NPTO.SetError)
   PROCEDURE PutMsgAutonom( pType IN NUMBER, pMessage IN VARCHAR2 )
   IS
      v_Num NUMBER := 0;
      
      PROCEDURE PutMsgA(pType IN NUMBER, pMessage IN VARCHAR2, pNum IN NUMBER)
      IS
        PRAGMA AUTONOMOUS_TRANSACTION;
      BEGIN

        PutMsgEx(pType, pMessage, pNum);
        COMMIT;
      END;
   BEGIN
      v_Num := GetNextMsgNum();

      PutMsgA(pType, pMessage, v_Num);
   END;


   -- Начинает запись в протокол для шага.
   PROCEDURE SaveOperLog( pDocID IN NUMBER, pAction IN NUMBER, pNotDel IN NUMBER DEFAULT 0, pType IN NUMBER DEFAULT -1 )
   IS
      TYPE   T_NPTXMES_TMP IS TABLE OF DNPTXMES_TMP%ROWTYPE;
      v_mes  T_NPTXMES_TMP;

      PROCEDURE SaveOperLogAutonom( pDocID IN NUMBER, pAction IN NUMBER, Mes IN T_NPTXMES_TMP, pNotDel IN NUMBER, pType IN NUMBER ) AS
         PRAGMA AUTONOMOUS_TRANSACTION;
      BEGIN

        IF pNotDel = 0 THEN
         DELETE FROM DNPTXMES_DBT
          WHERE T_DOCID = pDocID
            AND T_ACTION = pAction
            AND T_TYPE = (CASE WHEN pType >= 0 THEN pType ELSE T_TYPE END);
        END IF;

   -- 11G        FORALL i IN Mes.FIRST .. Mes.LAST
         FOR i IN 1 .. Mes.count LOOP
            INSERT INTO DNPTXMES_DBT (T_DOCID,
                                      T_ACTION,
                                      T_DATE,
                                      T_TIME,
                                      T_TYPE,
                                      T_MESSAGE
                                     )
                              VALUES( pDocID,
                                      pAction,
                                      SYSDATE,
                                      Mes(i).t_Time,
                                      Mes(i).t_Type,
                                      Mes(i).t_Message );
         END LOOP;

         COMMIT;      
      END;

   BEGIN

      SELECT * BULK COLLECT INTO v_mes FROM DNPTXMES_TMP WHERE T_TYPE = (CASE WHEN pType >= 0 THEN pType ELSE T_TYPE END) ORDER BY T_NUM;

      -- В автономной транзакции:
      SaveOperLogAutonom( pDocID, pAction, v_mes, pNotDel, pType );

      DELETE FROM DNPTXMES_TMP;

   END;

   -- Откатывает протокол для шага.
   PROCEDURE RecoilOperLog( pDocID IN NUMBER, pAction IN NUMBER, pType IN NUMBER DEFAULT -1 )
   IS
   BEGIN
      DELETE 
        FROM DNPTXMES_DBT 
       WHERE T_DOCID = pDocID
         AND T_ACTION = pAction
         AND T_TYPE = (CASE WHEN pType >= 0 THEN pType ELSE T_TYPE END);
   END;


   --Добавить сообщение для записи СНОБ во временную таблицу
   PROCEDURE AddTbMesTMP(p_TBID IN NUMBER, p_Type IN NUMBER, p_Message IN VARCHAR2)
   IS
      
   BEGIN
      INSERT INTO DNPTXTBMES_TMP (T_ID,
                                T_TBID,
                                T_DATE,
                                T_TIME,
                                T_TYPE,
                                T_MESSAGE
                               )
                        VALUES (0,
                                p_TBID,
                                SYSDATE,
                                TO_DATE('01.01.0001 '||TO_CHAR( SYSDATE, 'HH24:MI:SS'),'DD.MM.YYYY HH24:MI:SS'),
                                p_Type,
                                p_Message
                               );
   END;

   --Созранить сообщения записи СНОБ в постоянную таблицу из временной
   PROCEDURE SaveTbMes(p_ID_Operation IN NUMBER, p_ID_Step IN NUMBER)
   IS
      TYPE   T_NPTXTBMES_DBT IS TABLE OF DNPTXTBMES_DBT%ROWTYPE;
      v_mes  T_NPTXTBMES_DBT;
   BEGIN

      SELECT
/*T_ID          */ 0,
/*T_TBID        */ t_TBID,
/*T_ID_OPERATION*/ p_ID_Operation,
/*T_ID_STEP     */ p_ID_Step,
/*T_DATE        */ t_Date,
/*T_TIME        */ t_Time,
/*T_TYPE        */ t_Type,
/*T_MESSAGE     */ t_Message
      BULK COLLECT INTO v_mes 
      FROM DNPTXTBMES_TMP 
      ORDER BY T_DATE ASC, T_TIME ASC, T_ID ASC;

      IF v_mes.COUNT > 0 THEN
         FORALL indx IN v_mes.FIRST .. v_mes.LAST
            INSERT INTO dnptxtbmes_dbt
                 VALUES v_mes(indx);
      END IF;

      DELETE FROM DNPTXTBMES_TMP;
   END;

   --Откатывает сообщения записей СНОБ для шага.
   PROCEDURE RecoilTbMes(p_ID_Operation IN NUMBER, p_ID_Step IN NUMBER)
   IS
   BEGIN
      DELETE 
        FROM DNPTXTBMES_DBT 
       WHERE t_ID_Operation = p_ID_Operation
         AND t_ID_Step = p_ID_Step;

      DELETE FROM DNPTXTBMES_TMP;
   END;

END RSI_NPTMSG;
/
