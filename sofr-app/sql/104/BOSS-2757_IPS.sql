CREATE OR REPLACE PROCEDURE rshb_Sofr_ServiceReceive_Message (
   ServiceName   IN     VARCHAR2,
   MESSAGE       IN     CLOB,
   Req_id           OUT NUMBER,
   ReceiptOut       OUT CLOB)
IS
   /* Процедура приема запросов в СОФР */
   v_temp_clob    CLOB;
   v_error_code   NUMBER;
   v_error_desc   VARCHAR2 (2000 CHAR);
   v_serviceId    NUMBER (5);
   v_direction    NUMBER (5);
   v_recId        NUMBER (10);
BEGIN
   SELECT T_ID, T_DIRECTION
     INTO v_serviceId, v_direction
     FROM SOFR_IPS_SETTINGS
    WHERE T_SERVICENAME = ServiceName;

   IF LOWER (ServiceName) = 'eprshb_to_sofr_verify_snob_result'
   THEN
      INSERT INTO SOFR_IPS_MESSAGE (T_ServiceID,
                                    T_Message,
                                    T_FileName,
                                    T_Direction,
                                    T_Status,
                                    T_UserField1,
                                    T_UserField2,
                                    T_Flag1,
                                    T_Flag2)
           VALUES (v_serviceId,
                   MESSAGE,
                   CHR (1),
                   1,
                   0,
                   CHR (1),
                   CHR (1),
                   CHR (0),
                   CHR (0))
        RETURNING t_id
             INTO v_recId;

      INSERT INTO SOFR_IPS_IDLNK (T_SERVICEID, T_INTID)
           VALUES (v_serviceId, v_recId)
        RETURNING T_EXTID
             INTO Req_id;

      SELECT XMLROOT (
                XMLType (
                   '<ErrorList><Error><ErrorCode>0</ErrorCode>
                                            <ErrorDesc></ErrorDesc></Error></ErrorList>'),
                VERSION '1.0').getClobVal ()
        INTO ReceiptOut
        FROM DUAL;
   ELSE
      raise_application_error (
         -20000,
         'Неизвестное имя сервиса');
   END IF;
EXCEPTION
   WHEN OTHERS
   THEN
      ROLLBACK;
      it_error.put_error_in_stack;
      it_log.
       LOG (
         p_msg_type   => it_log.C_MSG_TYPE__ERROR,
         p_msg        => 'Ошибка rshb_Sofr_ServiceReceive_Message: '
                        || SQLERRM);
      Req_id := 0;
      v_error_code := SQLCODE;
      v_error_desc := SQLERRM;

      SELECT XMLROOT (
                XMLType (
                      '<ErrorList><Error><ErrorCode>'
                   || v_error_code
                   || '</ErrorCode><ErrorDesc>'
                   || v_error_desc
                   || '</ErrorDesc></Error></ErrorList>'),
                VERSION '1.0').getClobVal ()
        INTO ReceiptOut
        FROM DUAL;
END;
/

CREATE OR REPLACE PROCEDURE rshb_Sofr_ServiceSend_Message (
   ServiceName   IN     VARCHAR2,
   Req_id           OUT NUMBER,
   MESSAGE          OUT CLOB,
   IsNext           OUT NUMBER)
IS
   /* Процедура формирования запроса по параметрам из СОФР  */
   v_serviceId   NUMBER (5);
   v_direction   NUMBER (5);
   v_msgId       NUMBER (10);
BEGIN
   IsNext := 0;
   Req_id := 0;

   SELECT T_ID, T_DIRECTION
     INTO v_serviceId, v_direction
     FROM SOFR_IPS_SETTINGS
    WHERE T_SERVICENAME = ServiceName;

   IF LOWER (ServiceName) = 'sofr_to_eprshb_verify_snob'
   THEN
      SELECT T_MESSAGE, T_ID
        INTO MESSAGE, v_msgId
        FROM SOFR_IPS_MESSAGE
       WHERE     T_SERVICEID = v_serviceId
             AND T_DIRECTION = v_direction
             AND T_STATUS = 0
             AND T_ID =
                    (SELECT MIN (t_id)
                       FROM SOFR_IPS_MESSAGE
                      WHERE     T_SERVICEID = v_serviceId
                            AND T_DIRECTION = v_direction
                            AND T_STATUS = 0)
      FOR UPDATE;

      MERGE INTO SOFR_IPS_IDLNK c
           USING (SELECT v_serviceId AS t_serviceId, v_msgId AS t_msgId
                    FROM DUAL) d
              ON (c.T_SERVICEID = d.t_serviceId AND c.T_INTID = d.t_msgId)
      WHEN NOT MATCHED
      THEN
         INSERT     (c.T_SERVICEID, c.T_INTID)
             VALUES (d.t_serviceId, d.t_msgId);

      SELECT T_EXTID
        INTO Req_id
        FROM SOFR_IPS_IDLNK
       WHERE T_SERVICEID = v_serviceId AND T_INTID = v_msgId;


      UPDATE SOFR_IPS_MESSAGE
         SET T_STATUS = 1
       WHERE T_ID = v_msgId;

      BEGIN
         SELECT 1
           INTO IsNext
           FROM DUAL
          WHERE EXISTS
                   (SELECT 1
                      FROM SOFR_IPS_MESSAGE
                     WHERE     T_SERVICEID = v_serviceId
                           AND T_DIRECTION = v_direction
                           AND T_STATUS = 0);
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            NULL;
      END;
   ELSE
      raise_application_error (
         -20000,
         'Неизвестное имя сервиса');
   END IF;
EXCEPTION
   WHEN OTHERS
   THEN
      it_error.put_error_in_stack;
      it_log.
       LOG (
         p_msg_type   => it_log.C_MSG_TYPE__ERROR,
         p_msg        => 'Ошибка rshb_Sofr_ServiceSend_Message: '
                        || SQLERRM);
      MESSAGE := 'ERROR: ' || SQLERRM;
END;
/

CREATE OR REPLACE PROCEDURE rshb_Sofr_ServiceStatus_Set (
   Req_id      IN NUMBER,
   ReceiptIn   IN CLOB)
IS
   /*Процедура приема квитанций в СОФР */
   v_HasError      NUMBER (1) := 0;
   v_serviceName   VARCHAR2 (100);
   v_intId         NUMBER (10);
BEGIN
   SELECT SETT.T_SERVICENAME, lnk.T_INTID
     INTO v_serviceName, v_intId
     FROM SOFR_IPS_IDLNK lnk, SOFR_IPS_SETTINGS sett
    WHERE SETT.T_ID = lnk.T_SERVICEID AND lnk.T_EXTID = Req_id;

   FOR error_list
      IN (   SELECT ErrorCode, NVL (ErrorDesc, 'OK!') ErrorDesc
               FROM XMLTABLE (
                       '/ErrorList/Error'
                       PASSING xmltype (ReceiptIn)
                       COLUMNS ErrorCode VARCHAR2 (200) PATH 'ErrorCode',
                               ErrorDesc VARCHAR2 (200) PATH 'ErrorDesc'))
   LOOP
      it_log.
       LOG (
         p_msg_type   => it_log.C_MSG_TYPE__DEBUG,
         p_msg        =>   'rshb_Sofr_ServiceStatus_Set. ReqId: '
                        || Req_id
                        || '; ServiceName: '
                        || v_serviceName
                        || '; ErrorCode: '
                        || error_list.ErrorCode
                        || '; ErrorDesc: '
                        || error_list.ErrorDesc);

      IF error_list.ErrorCode <> 0
      THEN
         v_HasError := 1;
      END IF;
   END LOOP;

   IF LOWER (v_serviceName) = 'sofr_to_eprshb_verify_snob'
   THEN
      UPDATE SOFR_IPS_MESSAGE
         SET T_STATUS = CASE WHEN v_HasError = 0 THEN 1 ELSE 2 END
       WHERE T_ID = v_intId;
   END IF;
EXCEPTION
   WHEN OTHERS
   THEN
      it_error.put_error_in_stack;
      it_log.
       LOG (
         p_msg_type   => it_log.C_MSG_TYPE__ERROR,
         p_msg        => 'Ошибка rshb_Sofr_ServiceStatus_Set: '
                        || SQLERRM);
END;
/
