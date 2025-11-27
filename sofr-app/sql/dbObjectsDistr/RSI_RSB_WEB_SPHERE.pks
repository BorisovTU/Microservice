CREATE OR REPLACE PACKAGE RSI_RSB_WEB_SPHERE
AS

  -- Вставить транспортный конверт в очередь - aq_outgoing_ws
  FUNCTION InsRequestToWS(p_QueueID IN NUMBER, p_ReqIDTrEn IN VARCHAR2, p_ReqIDMes IN VARCHAR2, p_JMSCorrelationID IN VARCHAR2, p_XMLTrEn IN CLOB, p_Expiration IN NUMBER DEFAULT -1) RETURN INTEGER;
  -- Вставить транспортный конверт в очередь - aq_incoming_ws
  FUNCTION InsRequestToIncWS(p_QueueID IN NUMBER, p_ReqIDTrEn IN VARCHAR2, p_ReqIDMes IN VARCHAR2, p_JMSCorrelationID IN VARCHAR2, p_XMLTrEn IN CLOB ) RETURN INTEGER;
  -- Вставить транспортный конверт в очередь для синхронных сообщений - aq_incoming_sync_ws
  FUNCTION InsRequestToIncSyncWS(p_QueueID IN NUMBER, p_ReqIDTrEn IN VARCHAR2, p_JMSMessageID IN VARCHAR2, p_JMSCorrelationID IN VARCHAR2, p_ParentReqIDTrEn IN VARCHAR2, p_XMLTrEn IN CLOB ) RETURN INTEGER;

  -- Функция получения сообщений из входящей очереди Oracle AQ синхронная
  FUNCTION GetAnswerSyncInAQ(p_Correlation IN VARCHAR2, p_Wait IN NUMBER, p_ReqIDTrEnAns IN VARCHAR2, p_QueueIDIn OUT NUMBER, p_JMSMessageIDAns OUT VARCHAR2, p_JMSCorrelationIDAns OUT VARCHAR2, p_XrLogID OUT NUMBER) RETURN INTEGER;
  
  -- Функция получения сообщения через таблицу логов
  FUNCTION TryGetSyncAnswer(p_Correlation IN VARCHAR2, p_ReqIDTrEnAns IN VARCHAR2, p_QueueIDIn OUT NUMBER, p_JMSMessageIDAns OUT VARCHAR2, p_JMSCorrelationIDAns OUT VARCHAR2, p_XrLogID OUT NUMBER) RETURN INTEGER;




END  RSI_RSB_WEB_SPHERE;