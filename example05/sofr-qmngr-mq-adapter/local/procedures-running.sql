-- Создание пакета IT_INTEGRATION с описанием сигнатуры процедур, необходимых для локального тестирования
CREATE OR REPLACE PACKAGE IT_INTEGRATION AS
    PROCEDURE QMANAGER_LOAD_MSG(P_KAFKA_TOPIC IN VARCHAR2, P_GUID IN VARCHAR2, P_ESBDT IN TIMESTAMP, PCL_HEADER IN CLOB, PCL_MESSAGE IN CLOB, O_ERRORCODE OUT NUMBER, O_ERRORDESC OUT VARCHAR2);
    PROCEDURE QMANAGER_READ_MSG(P_WAIT_MSG IN NUMBER, O_KAFKA_TOPIC OUT VARCHAR2, O_MSGID OUT VARCHAR2, OCL_HEADER OUT CLOB, OCL_MESSAGE OUT CLOB, O_ERRORCODE OUT NUMBER, O_ERRORDESC OUT VARCHAR2);
    PROCEDURE QMANAGER_READ_MSG_ERROR(P_KAFKA_TOPIC IN VARCHAR2, P_MSGID IN VARCHAR2, P_ERRORCODE IN NUMBER, P_ERRORDESC IN VARCHAR2);
END IT_INTEGRATION;

-- Создание тела пакета IT_INTEGRATION с описанием процедур, необходимых для локального тестирования
CREATE OR REPLACE PACKAGE BODY IT_INTEGRATION AS
    PROCEDURE QMANAGER_LOAD_MSG(P_KAFKA_TOPIC IN VARCHAR2, P_GUID IN VARCHAR2, P_ESBDT IN TIMESTAMP, PCL_HEADER IN CLOB, PCL_MESSAGE IN CLOB, O_ERRORCODE OUT NUMBER, O_ERRORDESC OUT VARCHAR2) AS
    BEGIN
        IF P_KAFKA_TOPIC IS NULL THEN
            O_ERRORCODE := 10001;
            O_ERRORDESC := 'Не заполнено название топика';
        ELSIF P_GUID IS NULL THEN
            O_ERRORCODE := 10002;
            O_ERRORDESC := 'Не заполнен идентификатор сообщения';
        ELSIF P_ESBDT IS NULL THEN
            O_ERRORCODE := 10003;
            O_ERRORDESC := 'Не заполнена дата получения сообщения';
        ELSIF PCL_HEADER IS NULL THEN
            O_ERRORCODE := 10004;
            O_ERRORDESC := 'Не заполнены заголовки сообщения';
        ELSIF PCL_MESSAGE IS NULL THEN
            O_ERRORCODE := 10005;
            O_ERRORDESC := 'Не заполнено тело сообщения';
        ELSIF TRUNC(DBMS_RANDOM.VALUE(1, 5)) = 1
        THEN
            O_ERRORCODE := TRUNC(DBMS_RANDOM.VALUE(20000, 25000));
            O_ERRORDESC := 'Ошибка №' || O_ERRORCODE;
        ELSE
            O_ERRORCODE := 0;
            O_ERRORDESC := 'OK';
        END IF;
    END;
    PROCEDURE QMANAGER_READ_MSG(P_WAIT_MSG IN NUMBER, O_KAFKA_TOPIC OUT VARCHAR2, O_MSGID OUT VARCHAR2, OCL_HEADER OUT CLOB, OCL_MESSAGE OUT CLOB, O_ERRORCODE OUT NUMBER, O_ERRORDESC OUT VARCHAR2) AS
    BEGIN
        IF P_WAIT_MSG < 0
        THEN
            O_ERRORCODE := 10005;
            O_ERRORDESC := 'Установлен некорректный таймаут ожидания';
        ELSIF TRUNC(DBMS_RANDOM.VALUE(1, 5)) = 1
        THEN
            O_ERRORCODE := TRUNC(DBMS_RANDOM.VALUE(20000, 25000));
            O_ERRORDESC := 'Ошибка №' || O_ERRORCODE;
        ELSE
            O_KAFKA_TOPIC := 'sofr.diasoft.pko-info.resp';
            O_MSGID := SYS_GUID();
            OCL_HEADER := '{}';
            OCL_MESSAGE := '<xml><guid>' || O_MSGID || '</guid></xml>';
        END IF;
    END;
    PROCEDURE QMANAGER_READ_MSG_ERROR(P_KAFKA_TOPIC IN VARCHAR2, P_MSGID IN VARCHAR2, P_ERRORCODE IN NUMBER, P_ERRORDESC IN VARCHAR2) AS
    BEGIN
        IF P_KAFKA_TOPIC IS NULL OR P_MSGID IS NULL OR P_ERRORCODE IS NULL OR P_ERRORDESC IS NULL
        THEN
            NULL;
        END IF;
    END;
END IT_INTEGRATION;

-- Создание пакета RSB_BROKER_MONITORING с описанием сигнатуры процедур, необходимых для локального тестирования
CREATE OR REPLACE PACKAGE RSB_BROKER_MONITORING AS
    PROCEDURE getValuesMetric(P_OUT_JSON OUT VARCHAR2);
END RSB_BROKER_MONITORING;

-- Создание пакета RSB_BROKER_MONITORING с описанием процедур, необходимых для локального тестирования
CREATE OR REPLACE PACKAGE RSB_BROKER_MONITORING AS
    PROCEDURE getValuesMetric(P_OUT_JSON OUT VARCHAR2) AS
    BEGIN
        P_OUT_JSON:='{"metrics":[{"name":"sofr_change_fl_count","value":6,"timestamp":"2024-09-27T14:00:00",' ||
                    '"description":"Количество обновлений по текущим клиентам СОФР, ФЛ"},' ||
                    '{"name":"sofr_new_fl_count","value":3,"timestamp":"2024-09-27T14:00:00",' ||
                    '"description":"Количество новых клиентов СОФР, ФЛ"}]}';
    END;
END RSB_BROKER_MONITORING;
