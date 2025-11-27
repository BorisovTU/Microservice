CREATE OR REPLACE PACKAGE BODY RSI_RSB_GATE IS

   FUNCTION notUseGateForObjCode RETURN CHAR IS
   BEGIN
     RETURN m_notUseGateForObjCode;
   END;

   PROCEDURE addToObjectAndRecordIds(v_objectsCount IN NUMBER, v_recordsCount IN NUMBER, v_lastObjectId OUT NUMBER, v_lastRecordId OUT NUMBER)
   IS
     PRAGMA autonomous_transaction;
     v_currentObjectId    NUMBER(10);       -- Переменная, в которую читаем новое значение DGTOBJECT_DBT
     v_currentRecordId    NUMBER(10);       -- Переменна, в которую читаем новое значение DGTRECORD_DBT

   BEGIN

     LOCK TABLE DGTOBJECT_DBT IN EXCLUSIVE MODE;
     LOCK TABLE DGTRECORD_DBT IN EXCLUSIVE MODE;

     SELECT last_number INTO v_lastObjectId FROM USER_SEQUENCES WHERE sequence_name = UPPER ('dgtobject_dbt_SEQ');
     SELECT last_number INTO v_lastRecordId FROM USER_SEQUENCES WHERE sequence_name = UPPER ('dgtrecord_dbt_SEQ');

     v_currentObjectId := v_lastObjectId - 1;
     v_currentRecordId := v_lastRecordId - 1;

     LOOP
         EXIT WHEN v_currentObjectId >= (v_lastObjectId + v_objectsCount - 1);
         SELECT DGTOBJECT_DBT_SEQ.NEXTVAL INTO v_currentObjectId FROM DUAL;
     END LOOP;

     LOOP
         EXIT WHEN v_currentRecordId >= (v_lastRecordId + v_recordsCount - 1);
         SELECT DGTRECORD_DBT_SEQ.NEXTVAL INTO v_currentRecordId FROM DUAL;
     END LOOP;

     COMMIT;
   END;


   PROCEDURE OptimizeGTRecords(p_seanceid IN NUMBER, p_applicationid_to IN NUMBER)
   IS
     v_last_create_record NUMBER;
     v_first_create_record NUMBER;
     v_last_update_record NUMBER;
     v_first_update_record NUMBER;
     v_last_delete_record NUMBER;
     v_first_delete_record NUMBER;

     -- курсор по объектам, которые выбраны в рамках этой сессии.
     CURSOR c_OBJ IS (SELECT distinct rec.t_objectid t_objectid, ObjData.NRecs NRecs
                       FROM dgtrecord_dbt rec, dgtsncrec_dbt snc,
                            (SELECT count(1) NRecs, rec1.t_objectid objectid
                               FROM dgtrecord_dbt rec1, dgtsncrec_dbt snc1
                              WHERE rec1.t_statusid = 7
                                AND rec1.t_actionid <> 4
                                AND rec1.t_recordid = snc1.t_recordid
                                AND snc1.t_seanceid = p_seanceid
                              GROUP BY rec1.t_objectid
                            ) ObjData
                      WHERE rec.t_statusid = 7
                        AND rec.t_actionid <> 4
                        AND rec.t_recordid = snc.t_recordid
                        AND snc.t_seanceid = p_seanceid
                        AND ObjData.objectid = rec.t_objectid
                        AND ObjData.NRecs > 1
                    );


   BEGIN

     FOR OBJ IN c_OBJ LOOP

       SELECT NVL(MIN(gtr.t_recordid), 0), NVL(MAX(gtr.t_recordid), 0)
              INTO v_first_create_record, v_last_create_record
         FROM dgtrecord_dbt gtr, dgtsncrec_dbt gts
        WHERE     gtr.t_objectid = OBJ.t_objectid
              AND gtr.t_applicationid_from = 1
              AND gtr.t_statusid = 7 -- "готова к обработке"
              AND gtr.t_actionid = 1 -- "создать"
              AND gtr.t_recordid = gts.t_recordid
              AND gts.t_seanceid = p_seanceid;

       SELECT NVL(MIN(gtr.t_recordid), 0), NVL(MAX(gtr.t_recordid), 0)
              INTO v_first_update_record, v_last_update_record
         FROM dgtrecord_dbt gtr, dgtsncrec_dbt gts
        WHERE     gtr.t_objectid = OBJ.t_objectid
              AND gtr.t_applicationid_from = 1
              AND gtr.t_statusid = 7 -- "готова к обработке"
              AND gtr.t_actionid = 2 -- "обновить"
              AND gtr.t_recordid = gts.t_recordid
              AND gts.t_seanceid = p_seanceid;

       SELECT NVL(MIN(gtr.t_recordid), 0), NVL(MAX(gtr.t_recordid), 0)
              INTO v_first_delete_record, v_last_delete_record
         FROM dgtrecord_dbt gtr, dgtsncrec_dbt gts
        WHERE     gtr.t_objectid = OBJ.t_objectid
              AND gtr.t_applicationid_from = 1
              AND gtr.t_statusid = 7 -- "готова к обработке"
              AND gtr.t_actionid = 3 -- "удалить"
              AND gtr.t_recordid = gts.t_recordid
              AND gts.t_seanceid = p_seanceid;

       IF     v_first_create_record != 0
          AND v_last_delete_record != 0
          AND v_first_create_record < v_last_delete_record
          AND v_last_create_record < v_last_delete_record
       THEN
       -- Если объект в рамках сеанса в начале создается, а в конце удаляется,
       -- то все ЗР этого объекта получают статус "обработана"

         UPDATE dgtrecord_dbt
            SET t_statusid = 8
          WHERE t_statusid = 7
            AND t_objectid = OBJ.t_objectid
            AND t_recordid IN (SELECT t_recordid
                                 FROM dgtsncrec_dbt
                                WHERE t_seanceid = p_seanceid);
       ELSIF     v_first_create_record != 0
             AND v_last_update_record != 0
             AND v_first_create_record < v_last_update_record
       THEN
       -- Если объект в рамках сеанса в начале создается, а затем изменяется,
       -- то все ЗР этого объекта, кроме первой "создать", получают статус "обработана"
         UPDATE dgtrecord_dbt
            SET t_statusid = 8
          WHERE t_statusid = 7
            AND t_objectid = OBJ.t_objectid
            AND t_recordid != v_first_create_record
            AND t_recordid IN (SELECT t_recordid
                                 FROM dgtsncrec_dbt
                                WHERE t_seanceid = p_seanceid);

       ELSIF     v_first_delete_record != 0
             AND v_last_create_record != 0
             AND v_first_delete_record < v_last_create_record
       THEN
       -- Если объект в рамках сеанса в начале удаляется, а затем создается,
       -- то все ЗР этого объекта, кроме первой "удалить" и последней "создать", получают статус "обработана"
         UPDATE dgtrecord_dbt
            SET t_statusid = 8
          WHERE t_statusid = 7
            AND t_objectid = OBJ.t_objectid
            AND t_recordid != v_first_delete_record
            AND t_recordid != v_last_create_record
            AND t_recordid IN (SELECT t_recordid
                                 FROM dgtsncrec_dbt
                                WHERE t_seanceid = p_seanceid);

       ELSIF     v_first_delete_record != 0
             AND v_last_create_record = 0
       THEN
       -- Если объект в рамках сеанса в удаляется, и не создается,
       -- то все ЗР этого объекта, кроме "удалить", получают статус "обработана"
         UPDATE dgtrecord_dbt
            SET t_statusid = 8
          WHERE t_statusid = 7
            AND t_objectid = OBJ.t_objectid
            AND t_recordid != v_first_delete_record
            AND t_recordid IN (SELECT t_recordid
                                 FROM dgtsncrec_dbt
                                WHERE t_seanceid = p_seanceid);

       ELSIF     v_first_create_record != 0
             AND v_last_delete_record = 0
       THEN
       -- Если объект в рамках сеанса в создается, и не удаляется,
       -- то все ЗР этого объекта, кроме "создать", получают статус "обработана"
         UPDATE dgtrecord_dbt
            SET t_statusid = 8
          WHERE t_statusid = 7
            AND t_objectid = OBJ.t_objectid
            AND t_recordid != v_first_create_record
            AND t_recordid IN (SELECT t_recordid
                                 FROM dgtsncrec_dbt
                                WHERE t_seanceid = p_seanceid);

       ELSIF     v_first_update_record != 0
             AND v_last_create_record = 0
             AND v_last_delete_record = 0
       THEN
       -- Если объект в рамках сеанса в создается, и не удаляется,
       -- то все ЗР этого объекта, кроме "создать", получают статус "обработана"
         UPDATE dgtrecord_dbt
            SET t_statusid = 8
          WHERE t_statusid = 7
            AND t_objectid = OBJ.t_objectid
            AND t_recordid != v_first_update_record
            AND t_recordid IN (SELECT t_recordid
                                 FROM dgtsncrec_dbt
                                WHERE t_seanceid = p_seanceid);

       END IF;

     END LOOP;

   END;

  PROCEDURE Al_RegistryObject(och_cur IN ObjectChange_cur, IsOperStartup
            IN CHAR DEFAULT 'X', StartId IN NUMBER DEFAULT RSBSESSIONDATA.oper) IS

      v_OBJECTKIND      NUMBER(5);
      v_OBJECTCODE      VARCHAR2(100);
      v_OBJECTNAME      VARCHAR2(256);
      v_ACTIONID        NUMBER(5);
      v_ID_OPERATION    NUMBER(10);
      v_ID_STEP_NUMBER  NUMBER(5);

      v_objectsCount    NUMBER(10);
      v_recordsCount     NUMBER(10);

      v_SeanceID        NUMBER(10);

      v_temprecscount   NUMBER(10); -- кол-во записей с заданным T_ID_OPERATION, T_ID_STEP_NUMBER
      v_BoutCount       NUMBER(10);

      v_svObjectKind  DGTMASS_TMP.T_OBJECTKIND%TYPE;
      v_svObjectCode  DGTMASS_TMP.T_OBJECTCODE%TYPE;
      v_svObjectID    DGTMASS_TMP.T_OBJECTID%TYPE  ;


      bk_cursor         RSI_RSBOPERATION.BkoutData_cur;

      CURSOR c_Application  ( p_OBJECTKIND NUMBER ) IS
         SELECT T_APPLICATIONID_TO FROM DGTKOTO_DBT
           JOIN DGTAPP_DBT ON (DGTAPP_DBT.T_APPLICATIONID = DGTKOTO_DBT.T_APPLICATIONID_TO)
          WHERE (DGTAPP_DBT.T_ISBLOCKED <> 'X' OR DGTAPP_DBT.T_ISBLOCKED IS NULL)
            AND (DGTKOTO_DBT.T_OBJECTKIND = p_OBJECTKIND /*DGTMASS_TMP.T_OBJECTKIND*/);


      CURSOR c_GtMassID  IS
         SELECT T_OBJECTKIND, T_OBJECTCODE, T_OBJECTNAME, T_ACTIONID, T_ID_OPERATION, T_ID_STEP_NUMBER, T_APPID
           FROM DGTMASS_TMP
          WHERE T_ISNEWOBJECT = 'X' AND T_OBJECTID IS NULL
          ORDER BY T_OBJECTKIND, T_OBJECTCODE;

  BEGIN
    -- В комментах указаны пункты в проекте PRJ_000805_Проект на реализацию ?люза в RS_Bank V6 r20.doc,
    -- Ревизия 1.70, п.3.7.2. Описание процедуры Al_RegistryObject()
    --
    -- 1.
    IF (rsb_common.getregboolvalue('COMMON\WORK_MODE\ACTIV_RSGATE',  NULL)= false)
    THEN
        return;
    END IF;

    DELETE FROM DGTMASS_TMP;

    -- 2.
    LOOP
        FETCH och_cur INTO v_OBJECTKIND,v_OBJECTCODE,v_OBJECTNAME,v_ACTIONID,v_ID_OPERATION,v_ID_STEP_NUMBER;
        EXIT WHEN och_cur%NOTFOUND;

        FOR Application_rec in c_Application( v_OBJECTKIND ) LOOP

          INSERT INTO DGTMASS_TMP (T_OBJECTKIND, T_OBJECTCODE, T_OBJECTNAME, T_ACTIONID, T_ID_OPERATION, T_ID_STEP_NUMBER, T_APPID)
          VALUES(v_OBJECTKIND,v_OBJECTCODE,v_OBJECTNAME,v_ACTIONID,v_ID_OPERATION,v_ID_STEP_NUMBER,Application_rec.T_APPLICATIONID_TO);

        END LOOP;

    END LOOP;

    CLOSE och_cur;

    -- 3.
    UPDATE DGTMASS_TMP
        SET T_ISSYNCHIDS =
               (SELECT DGTKOTO_DBT.T_ISSYNCHIDS
                  FROM DGTKOTO_DBT JOIN DGTAPP_DBT
                       ON (DGTAPP_DBT.T_APPLICATIONID = DGTKOTO_DBT.T_APPLICATIONID_TO )
                 WHERE (   DGTAPP_DBT.T_ISBLOCKED <> 'X'
                        OR DGTAPP_DBT.T_ISBLOCKED IS NULL
                       )
                   AND (     DGTKOTO_DBT.T_OBJECTKIND       = DGTMASS_TMP.T_OBJECTKIND
                        AND  DGTKOTO_DBT.T_APPLICATIONID_TO = DGTMASS_TMP.T_APPID
                       )
               );


    -- 4.
    DELETE FROM DGTMASS_TMP
      WHERE (T_ISSYNCHIDS  = 'X' AND T_ACTIONID = CONST_ACTIONID_INS);

    -- 5.
    UPDATE DGTMASS_TMP
      SET T_OBJECTID = (SELECT DGTCODE_DBT.T_OBJECTID  FROM DGTCODE_DBT
            WHERE (DGTCODE_DBT.T_APPLICATIONID = CONST_APPID_RSBANK_SRC OR
                  DGTCODE_DBT.T_APPLICATIONID = CONST_APPID_RSBANK_TGT) AND
                  DGTCODE_DBT.T_OBJECTKIND = DGTMASS_TMP.T_OBJECTKIND AND
                  DGTCODE_DBT.T_OBJECTCODE = DGTMASS_TMP.T_OBJECTCODE),
          T_ISNEWOBJECT  = CHR(0);

    -- 6.
    UPDATE DGTMASS_TMP
    SET T_ISERROR = 'X'
    WHERE (T_OBJECTID = NULL AND T_ACTIONID = CONST_ACTIONID_DEL);

    -- 7.
    UPDATE DGTMASS_TMP SET T_ISNEWOBJECT = 'X'
    WHERE (T_ISERROR <> 'X' OR T_ISERROR IS NULL) AND T_OBJECTID IS NULL;

    -- 9.
-- UPDATE DGTMASS_TMP SET T_OBJECTID = dgtobject_dbt_seq.nextval
-- WHERE T_ISNEWOBJECT = 'X' AND T_OBJECTID IS NULL;

   FOR GtMassID_rec in c_GtMassID LOOP

      IF NOT (v_svObjectKind IS NOT NULL AND v_svObjectCode IS NOT NULL AND v_svObjectID IS NOT NULL AND
              v_svObjectKind = GtMassID_rec.T_OBJECTKIND AND v_svObjectCode = GtMassID_rec.T_OBJECTCODE )  THEN
        SELECT dgtobject_dbt_seq.nextval INTO v_svObjectID FROM DUAL;
      END IF;

      v_svObjectKind := GtMassID_rec.T_OBJECTKIND;
      v_svObjectCode := GtMassID_rec.T_OBJECTCODE;

      UPDATE DGTMASS_TMP SET T_OBJECTID = v_svObjectID
      WHERE T_ISNEWOBJECT = 'X'
      AND T_OBJECTID IS NULL
      AND T_OBJECTKIND       = GtMassID_rec.T_OBJECTKIND
      AND T_OBJECTCODE       = GtMassID_rec.T_OBJECTCODE
      AND T_APPID            = GtMassID_rec.T_APPID ;
--      AND T_ACTIONID         = GtMassID_rec.T_ACTIONID
--      AND T_ID_OPERATION     = GtMassID_rec.T_ID_OPERATION
--      AND T_ID_STEP_NUMBER   = GtMassID_rec.T_ID_STEP_NUMBER  мб NULL

   END LOOP;


    -- 8.
    INSERT INTO DGTOBJECT_DBT (T_OBJECTID, T_OBJECTKIND,T_NAME,T_SYSDATE,T_SYSTIME)
 SELECT DISTINCT T_OBJECTID, T_OBJECTKIND, T_OBJECTNAME, trunc(SysDate), TO_DATE('01.01.0001 ' || TO_CHAR(SysDate, 'HH24:MI:SS'), 'DD.MM.YYYY HH24:MI:SS') FROM DGTMASS_TMP
    WHERE T_ISNEWOBJECT = 'X';

    SELECT COUNT (T_OBJECTID) INTO v_objectsCount FROM DGTMASS_TMP WHERE T_ISNEWOBJECT = 'X';

    -- 10.
    INSERT INTO DGTCODE_DBT(T_OBJECTID,T_APPLICATIONID,T_OBJECTCODE,T_OBJECTKIND)
    SELECT DISTINCT DGTMASS_TMP.T_OBJECTID, CONST_APPID_RSBANK_SRC, DGTMASS_TMP.T_OBJECTCODE, DGTMASS_TMP.T_OBJECTKIND
    FROM DGTMASS_TMP
    WHERE DGTMASS_TMP.T_ISNEWOBJECT = 'X';

   -- 11.
    UPDATE DGTMASS_TMP
    SET T_ACTIONID = CONST_ACTIONID_INS
    WHERE T_ISNEWOBJECT = 'X' AND T_ACTIONID = CONST_ACTIONID_UPD;

    -- 14.
    UPDATE DGTMASS_TMP SET T_RECORDID = dgtrecord_dbt_seq.nextval
    WHERE T_RECORDID IS NULL AND (DGTMASS_TMP.T_ISERROR <> 'X' OR DGTMASS_TMP.T_ISERROR IS NULL);

    -- 13.
    INSERT INTO DGTRECORD_DBT (T_RECORDID,T_OBJECTID,T_APPLICATIONID_FROM,T_APPLICATIONID_TO,T_ACTIONID,T_STATUSID,T_SYSDATE,T_SYSTIME)
    SELECT T_RECORDID, T_OBJECTID, CONST_APPID_RSBANK_SRC, T_APPID,
          decode(T_ISSYNCHIDS,'X',CONST_ACTIONID_SYNC, T_ACTIONID),CONST_STATUS_RDYTOPROC, trunc(SysDate), TO_DATE('01.01.0001 ' || TO_CHAR(SysDate, 'HH24:MI:SS'), 'DD.MM.YYYY HH24:MI:SS')
    FROM DGTMASS_TMP WHERE T_ISERROR <> 'X' OR T_ISERROR IS NULL;

    SELECT COUNT (T_RECORDID) INTO v_recordsCount FROM DGTMASS_TMP WHERE T_ISERROR <> 'X' OR T_ISERROR IS NULL;

    -- 15.
    IF ((v_recordsCount > 0) OR (v_objectsCount > 0)) THEN
        -- 15.1. - 15.2.
        INSERT INTO DGTSEANCE_DBT(T_DIRECTION,T_KIND,T_APPLICATIONID,T_RECORDSTATUS,T_ISOPERSTARTUP,
                                  T_STARTID,T_SYSDATE,T_SYSTIME)
        VALUES(CONST_DIRECTION_EXP,CONST_KIND_DOWNLOAD,CONST_APPID_RSBANK_SRC,NULL,IsOperStartup,StartId, trunc(SysDate), TO_DATE('01.01.0001 ' || TO_CHAR(SysDate, 'HH24:MI:SS'), 'DD.MM.YYYY HH24:MI:SS'))
        RETURNING T_SEANCEID INTO v_seanceID;

        -- 15.3.
        INSERT INTO DGTSNCREC_DBT (T_SEANCEID,T_RECORDID,T_OBJECTID)
        SELECT v_SeanceID,DGTMASS_TMP.T_RECORDID,decode(DGTMASS_TMP.T_ISNEWOBJECT,'X',DGTMASS_TMP.T_OBJECTID,NULL)
        FROM DGTMASS_TMP WHERE T_RECORDID IS NOT NULL;

        -- 15.4.
        SELECT COUNT (T_ID_OPERATION) INTO v_temprecscount FROM DGTMASS_TMP WHERE T_ID_OPERATION IS NOT NULL AND T_ID_STEP_NUMBER IS NOT NULL;
        IF (v_temprecscount>0) THEN
            UPDATE DGTMASS_TMP SET T_BACKOUTSQLQUERY = 'DECLARE v_SeanceID NUMBER(10); v_RecordID NUMBER(10); BEGIN ' ||
                    'SELECT DGTSEANCE_DBT_SEQ.NEXTVAL INTO v_SeanceID FROM DUAL; ' ||
                    'SELECT DGTRECORD_DBT_SEQ.NEXTVAL INTO v_RecordID FROM DUAL; ' ||
                    'INSERT INTO DGTSEANCE_DBT ' ||
                    '(T_SEANCEID, T_DIRECTION,T_KIND,T_RECORDSTATUS,T_ISOPERSTARTUP,T_STARTID,T_SYSDATE,T_SYSTIME) ' ||
                    'VALUES(v_SeanceID,' || to_char(CONST_DIRECTION_EXP) || ',' || to_char(CONST_KIND_DOWNLOAD) ||
                    ',NULL,''X'',RSBSESSIONDATA.oper, trunc(SysDate), TO_DATE(''01.01.0001 '' || TO_CHAR(SysDate, ''HH24:MI:SS''), ''DD.MM.YYYY HH24:MI:SS'')); ' ||
                    'INSERT INTO DGTRECORD_DBT (T_RECORDID,T_OBJECTID,T_APPLICATIONID_FROM,T_APPLICATIONID_TO,T_ACTIONID,T_STATUSID,T_SYSDATE,T_SYSTIME) ' ||
                    'VALUES (v_RecordID,' || to_char(DGTMASS_TMP.T_OBJECTID) || ',' || to_char(CONST_APPID_RSBANK_SRC) ||
                    ',' || to_char(DGTMASS_TMP.T_APPID) || ', decode(' || decode(DGTMASS_TMP.T_ISSYNCHIDS, 'X', 'X', 'chr(0)') ||
                    ',''X'',' || to_char(CONST_ACTIONID_SYNC) || ',decode(' || to_char(DGTMASS_TMP.T_ACTIONID) ||
                    ',' || to_char(CONST_ACTIONID_INS) || ',' || to_char(CONST_ACTIONID_DEL) || ',' ||
                    to_char(CONST_ACTIONID_DEL) || ',' || to_char(CONST_ACTIONID_INS) || ',' ||
                    to_char(CONST_ACTIONID_UPD) || ')),' || to_char(CONST_STATUS_RDYTOPROC) || ', trunc(SysDate), TO_DATE(''01.01.0001 '' || TO_CHAR(SysDate, ''HH24:MI:SS''), ''DD.MM.YYYY HH24:MI:SS'')); ' ||
                    'INSERT INTO DGTSNCREC_DBT (T_SEANCEID,T_RECORDID,T_OBJECTID) VALUES(v_SeanceID,v_RecordID,NULL);' ||
                    'END;'WHERE T_ID_OPERATION IS NOT NULL AND T_ID_STEP_NUMBER IS NOT NULL AND T_RECORDID IS NOT NULL;
        END IF;
 -- Эти запросы для проверки п.21
        /*  DECLARE v_SeanceID1 NUMBER(10); v_RecordID1 NUMBER(10); BEGIN
        SELECT DGTSEANCE_DBT_SEQ.NEXTVAL INTO v_SeanceID1 FROM DUAL;
        SELECT DGTRECORD_DBT_SEQ.NEXTVAL INTO v_RecordID1 FROM DUAL;

        INSERT INTO DGTSEANCE_DBT (T_SEANCEID, T_DIRECTION,T_KIND,T_RECORDSTATUS,T_ISOPERSTARTUP,T_STARTID,T_SYSDATE,T_SYSTIME)
                VALUES(v_SeanceID1,CONST_DIRECTION_EXP,CONST_KIND_DOWNLOAD,NULL,'X',RSBSESSIONDATA.oper,SysDate,SysDate);

        INSERT INTO DGTRECORD_DBT (T_RECORDID,T_OBJECTID,T_APPLICATIONID_FROM,T_APPLICATIONID_TO,T_ACTIONID,T_STATUSID,T_SYSDATE,T_SYSTIME)
                VALUES (v_RecordID1,DGTMASS_TMP.T_OBJECTID, CONST_APPID_RSBANK_SRC,DGTMASS_TMP.T_APPID,
                        decode(DGTMASS_TMP.T_ISSYNCHIDS,'X',CONST_ACTIONID_SYNC,decode(DGTMASS_TMP.T_ACTIONID,CONST_ACTIONID_INS,
                       CONST_ACTIONID_DEL,CONST_ACTIONID_DEL,CONST_ACTIONID_INS,CONST_ACTIONID_UPD)),
                       CONST_STATUS_RDYTOPROC,SysDate,SysDate);

        INSERT INTO DGTSNCREC_DBT (T_SEANCEID,T_RECORDID,T_OBJECTID)
                VALUES(v_SeanceID1,v_RecordID1,NULL);END;*/

        SELECT COUNT (T_ID_OPERATION) INTO v_BoutCount FROM DGTMASS_TMP
        WHERE T_ID_OPERATION IS NOT NULL AND T_ID_STEP_NUMBER IS NOT NULL
              AND T_RECORDID IS NOT NULL AND T_BACKOUTSQLQUERY IS NOT NULL;

    END IF;

   -- 16.
   IF (v_BoutCount > 0) THEN
       OPEN bk_cursor
            FOR SELECT T_ID_OPERATION,T_ID_STEP_NUMBER,T_BACKOUTSQLQUERY
            FROM DGTMASS_TMP
            WHERE T_BACKOUTSQLQUERY IS NOT NULL;
       RSI_RSBOPERATION.SetBkoutDataForAll(bk_cursor);
   END IF;

  END;

  PROCEDURE Al_RegistryObjectEx
  IS

    v_ObjectChange ObjectChange_cur;

  BEGIN

    OPEN v_ObjectChange
    FOR SELECT T_OBJECTKIND, T_OBJECTCODE, T_OBJECTNAME, T_ACTIONID, NULL T_ID_OPERATION, NULL T_ID_STEP_NUMBER
        FROM DGTOBJECT_TMP;

    Al_RegistryObject( v_ObjectChange );

  END;

  FUNCTION CheckClientIDByStatus( p_ClientID IN NUMBER,
                                  p_StatusID IN NUMBER,
                                  p_ObjectKind IN NUMBER DEFAULT 0,
                                  p_SysDate IN DATE DEFAULT ZERO_DATE,
                                  p_AppFromExclude IN STRING DEFAULT NULL
                                )
  RETURN CHAR
  IS
    v_res         CHAR := CHR(0);
    v_Query       VARCHAR2(30000);
    v_Count       NUMBER(10) := 0;
  BEGIN

    v_Query := 'SELECT COUNT (1) FROM DGTRECORD_DBT r ';

    IF (p_ObjectKind = 0) THEN
      v_Query := v_Query || 'WHERE r.T_CLIENTID = :p_ClientID AND r.T_STATUSID != :p_StatusID ' ;
    ELSE
      v_Query := v_Query || 'JOIN DGTOBJECT_DBT o ON (o.T_OBJECTID = r.T_OBJECTID) WHERE r.T_CLIENTID = :p_ClientID AND r.T_STATUSID != :p_StatusID AND o.T_OBJECTKIND = ' || TO_CHAR(p_ObjectKind) ;
    END IF;

    IF (p_SysDate != ZERO_DATE) THEN
      v_Query := v_Query || ' AND r.T_SYSDATE = TO_DATE(' || TO_CHAR(p_SysDate, 'DDMMYYYY') || ', ''DDMMYYYY'')' ;
    END IF;

    IF (p_AppFromExclude IS NOT NULL) THEN
      v_Query := v_Query || ' AND r.T_APPLICATIONID_FROM NOT IN (' || p_AppFromExclude || ')' ;
    END IF;

    EXECUTE IMMEDIATE v_Query INTO v_Count USING p_ClientID, p_StatusID;

    IF (v_Count != 0) THEN
      v_res := 'X';
    END IF;

    RETURN v_res;
  END;
  
  FUNCTION CheckClientIDByRawRecords( p_ClientID IN NUMBER,
                                      p_ObjectKind IN NUMBER DEFAULT 0,
                                      p_SysDate IN DATE DEFAULT ZERO_DATE,
                                      p_AppFromExclude IN STRING DEFAULT NULL
                                    )
  RETURN CHAR  
  IS
    v_res         CHAR := CHR(0);
    v_Query       VARCHAR2(30000);
    v_Count       NUMBER(10) := 0;
  BEGIN

    v_Query := 'SELECT COUNT (1) FROM DGTRECORD_DBT r ';

    IF (p_ObjectKind = 0) THEN
      v_Query := v_Query || 'WHERE r.T_CLIENTID = :p_ClientID AND r.T_STATUSID IN (' || CONST_STATUS_IMP_READYTOPROC || ', ' || CONST_STATUS_IMP_REFUSEDTOPROC || ') ' ;
    ELSE
      v_Query := v_Query || 'JOIN DGTOBJECT_DBT o ON (o.T_OBJECTID = r.T_OBJECTID) WHERE r.T_STATUSID IN (' || CONST_STATUS_IMP_READYTOPROC || ', ' || CONST_STATUS_IMP_REFUSEDTOPROC || ') AND o.T_OBJECTKIND = ' || TO_CHAR(p_ObjectKind) ;
    END IF;

    IF (p_SysDate != ZERO_DATE) THEN
      v_Query := v_Query || ' AND r.T_SYSDATE = TO_DATE(' || TO_CHAR(p_SysDate, 'DDMMYYYY') || ', ''DDMMYYYY'')' ;
    END IF;

    IF (p_AppFromExclude IS NOT NULL) THEN
      v_Query := v_Query || ' AND r.T_APPLICATIONID_FROM NOT IN (' || p_AppFromExclude || ')' ;
    END IF;

    EXECUTE IMMEDIATE v_Query INTO v_Count USING p_ClientID;

    IF (v_Count != 0) THEN
      v_res := 'X';
    END IF;

    RETURN v_res;
  END;  

END RSI_RSB_GATE;
/