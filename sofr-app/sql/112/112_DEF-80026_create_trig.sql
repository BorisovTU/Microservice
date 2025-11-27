DECLARE 
    e_object_exists EXCEPTION;
    PRAGMA EXCEPTION_INIT(e_object_exists, -955); 
BEGIN
    EXECUTE IMMEDIATE 
        'CREATE TABLE DSS_HISTORYLOG_DBT' || 
        '(' || 
        'T_SERVICE NUMBER(10),' || 
        'T_DATE    DATE,' || 
        'T_LASTACTIONSTAMP  DATE,' || 
        'T_MODULE VARCHAR2(256),' || 
        'T_SQL VARCHAR2(1000)' || 
        ')';

  EXECUTE IMMEDIATE 'COMMENT ON TABLE DSS_HISTORYLOG_DBT IS ''Лог с ошибочным статусом для DSS_HISTORY_DBT''';

  EXECUTE IMMEDIATE 'COMMENT ON COLUMN DSS_HISTORYLOG_DBT.T_SERVICE IS ''SimpleService ID''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN DSS_HISTORYLOG_DBT.T_DATE IS ''Дата''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN DSS_HISTORYLOG_DBT.T_LASTACTIONSTAMP IS ''Дата/время завершения последнего действия с объектом''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN DSS_HISTORYLOG_DBT.T_MODULE IS ''Модуль''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN DSS_HISTORYLOG_DBT.T_SQL IS ''Текст запроса''';

EXCEPTION 
    WHEN e_object_exists THEN NULL; 
END;
/

CREATE OR REPLACE TRIGGER DSS_HISTORY_DBT_CORRECT_RECORD
   BEFORE INSERT OR UPDATE ON DSS_HISTORY_DBT
   FOR EACH ROW
DECLARE
BEGIN

   IF (:new.t_State = 2 AND (:new.t_Service = 10099 OR :new.t_Service = 10052 OR :new.t_Service = 10070 OR :new.t_Service = 10002 OR :new.t_Service = 10003 OR :new.t_Service = 10014 OR :new.t_Service = 10045) ) THEN
     :new.t_State := 3;

     INSERT INTO DSS_HISTORYLOG_DBT(T_SERVICE, T_DATE, T_LASTACTIONSTAMP,T_MODULE, T_SQL)
        VALUES(
                :new.t_Service,                 --T_SERVICE, 
                sysdate(),                      --T_DATE, 
                :new.t_LastActionStamp,         --T_LASTACTIONSTAMP,
                substr(SYS_CONTEXT('USERENV','MODULE'),0,256),       --T_MODULE, 
                substr(SYS_CONTEXT('USERENV','CURRENT_SQL'),0, 1000) --TSQL
        );
   END IF;

END;
/
