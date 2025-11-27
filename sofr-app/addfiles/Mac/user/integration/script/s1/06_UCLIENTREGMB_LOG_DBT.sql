--А.Киселев 07.09.2018 Загрузка в СОФР RS Securities ответного файла результатов регистрации клиентов на МБ
--Таблица логирования

DROP TABLE UCLIENTREGMB_LOG_DBT CASCADE CONSTRAINTS;

CREATE TABLE UCLIENTREGMB_LOG_DBT
(
  T_RECID     NUMBER(10),
  T_FILENAME  VARCHAR2(100 BYTE),
  T_LOADDATE  DATE                              DEFAULT TO_DATE(TO_CHAR(SYSDATE,'ddmmyyyy'), 'ddmmyyyy' ),
  T_LOADTIME  DATE                              DEFAULT TO_DATE( '01010001' || TO_CHAR(SYSDATE,'hh24miss'), 'ddmmyyyy hh24miss'),
  T_STATUS    INTEGER,
  T_OPER      NUMBER(5),
  T_ERRCODE   NUMBER(10),
  T_ERRTEXT   VARCHAR2(200 BYTE),
  T_XML_MESS  CLOB
)
TABLESPACE USERS
PCTUSED    0
PCTFREE    10
INITRANS   1
MAXTRANS   255
STORAGE    (
            INITIAL          64K
            NEXT             1M
            MINEXTENTS       1
            MAXEXTENTS       UNLIMITED
            PCTINCREASE      0
            BUFFER_POOL      DEFAULT
           )
LOGGING 
NOCOMPRESS 
LOB (T_XML_MESS) STORE AS 
      ( TABLESPACE  USERS 
        ENABLE      STORAGE IN ROW
        CHUNK       8192
        RETENTION
        NOCACHE
        INDEX       (
          TABLESPACE USERS
          STORAGE    (
                      INITIAL          64K
                      NEXT             1
                      MINEXTENTS       1
                      MAXEXTENTS       UNLIMITED
                      PCTINCREASE      0
                      BUFFER_POOL      DEFAULT
                     ))
        STORAGE    (
                    INITIAL          64K
                    NEXT             1M
                    MINEXTENTS       1
                    MAXEXTENTS       UNLIMITED
                    PCTINCREASE      0
                    BUFFER_POOL      DEFAULT
                   )
      )
NOCACHE
NOPARALLEL
MONITORING;

COMMENT ON TABLE UCLIENTREGMB_LOG_DBT IS 'Данные по результатам регистрации клиентов на МБ';

COMMENT ON COLUMN UCLIENTREGMB_LOG_DBT.T_RECID IS 'ИД сообщения';

COMMENT ON COLUMN UCLIENTREGMB_LOG_DBT.T_FILENAME IS 'Файл загрузки';

COMMENT ON COLUMN UCLIENTREGMB_LOG_DBT.T_LOADDATE IS 'Дата загрузки';

COMMENT ON COLUMN UCLIENTREGMB_LOG_DBT.T_LOADTIME IS 'Время загрузки';

COMMENT ON COLUMN UCLIENTREGMB_LOG_DBT.T_STATUS IS 'Статус загрузки';

COMMENT ON COLUMN UCLIENTREGMB_LOG_DBT.T_OPER IS 'Операционист';

COMMENT ON COLUMN UCLIENTREGMB_LOG_DBT.T_ERRCODE IS 'Код ошибки';

COMMENT ON COLUMN UCLIENTREGMB_LOG_DBT.T_ERRTEXT IS 'Текст ошибки';

COMMENT ON COLUMN UCLIENTREGMB_LOG_DBT.T_XML_MESS IS 'XML-сообщение';


CREATE UNIQUE INDEX UCLIENTREGMB_LOG_DBT_IDX0 ON UCLIENTREGMB_LOG_DBT
(T_RECID)
LOGGING
TABLESPACE USERS
PCTFREE    10
INITRANS   2
MAXTRANS   255
STORAGE    (
            INITIAL          64K
            NEXT             1M
            MINEXTENTS       1
            MAXEXTENTS       UNLIMITED
            PCTINCREASE      0
            BUFFER_POOL      DEFAULT
           )
NOPARALLEL;


CREATE OR REPLACE TRIGGER uclientRegMB_Log_dbt_t0_ainc
 BEFORE INSERT OR UPDATE OF T_RECID ON uclientRegMB_Log_dbt FOR EACH ROW
DECLARE
 v_id INTEGER;
BEGIN
 IF (:new.T_RECID = 0 OR :new.T_RECID IS NULL) THEN
  SELECT uclientRegMB_Log_dbt_SEQ_1.nextval INTO :new.T_RECID FROM dual;
 ELSE
  SELECT last_number INTO v_id FROM user_sequences WHERE sequence_name = UPPER('uclientRegMB_Log_dbt_SEQ_1');

  IF :new.T_RECID >= v_id THEN
   RAISE DUP_VAL_ON_INDEX;
  END IF;

 END IF;
END;
/

