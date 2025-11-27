-- Таблица "DDLCONTRFROB_DBT"

CREATE TABLE DDLCONTRFROB_DBT (
    T_FROBID NUMBER(10)
  , T_DLCONTRID NUMBER(10)
  , T_TAXPERIOD NUMBER(5)
  , T_INCOMECODE NUMBER(5)
  , T_INCOMESUM NUMBER(32,12)
  , T_DEDUCTIONCODE NUMBER(5)
  , T_DEDUCTIONSUM NUMBER(32,12)
  , T_OPER NUMBER(5)
  , T_CHANGEDATE DATE
  , T_CHANGETIME DATE
  , T_VERSION NUMBER(5)
)
/

COMMENT ON TABLE DDLCONTRFROB_DBT IS 'Финрез по ДБО, получ. от другого брокера'/
COMMENT ON COLUMN DDLCONTRFROB_DBT.T_FROBID IS 'Идетификатор записи'/
COMMENT ON COLUMN DDLCONTRFROB_DBT.T_DLCONTRID IS 'Идентификатор ДБО'/
COMMENT ON COLUMN DDLCONTRFROB_DBT.T_TAXPERIOD IS 'Налоговый период'/
COMMENT ON COLUMN DDLCONTRFROB_DBT.T_INCOMECODE IS 'Код дохода'/
COMMENT ON COLUMN DDLCONTRFROB_DBT.T_INCOMESUM IS 'Сумма дохода'/
COMMENT ON COLUMN DDLCONTRFROB_DBT.T_DEDUCTIONCODE IS 'Код вычета'/
COMMENT ON COLUMN DDLCONTRFROB_DBT.T_DEDUCTIONSUM IS 'Сумма вычета'/
COMMENT ON COLUMN DDLCONTRFROB_DBT.T_OPER IS 'Операционист, изменившый запись'/
COMMENT ON COLUMN DDLCONTRFROB_DBT.T_CHANGEDATE IS 'Дата изменения'/
COMMENT ON COLUMN DDLCONTRFROB_DBT.T_CHANGETIME IS 'Время изменения записи'/
COMMENT ON COLUMN DDLCONTRFROB_DBT.T_VERSION IS 'Номер изменения записи'/


CREATE UNIQUE INDEX DDLCONTRFROB_DBT_IDX0 ON DDLCONTRFROB_DBT (
   T_FROBID ASC
)
/

CREATE   INDEX DDLCONTRFROB_DBT_IDX1 ON DDLCONTRFROB_DBT (
   T_DLCONTRID ASC
  ,T_TAXPERIOD ASC
)
/

CREATE SEQUENCE DDLCONTRFROB_DBT_SEQ 
  START WITH 1
  MAXVALUE 999999999999999999999999999
  MINVALUE 1
  NOCYCLE
  NOCACHE
  NOORDER
/

CREATE OR REPLACE TRIGGER DDLCONTRFROB_DBT_T0_AINC
  BEFORE INSERT OR UPDATE OF T_FROBID ON DDLCONTRFROB_DBT FOR EACH ROW
DECLARE
  v_id INTEGER;
BEGIN
  IF (:NEW.T_FROBID = 0 OR :NEW.T_FROBID IS NULL) THEN
    SELECT DDLCONTRFROB_DBT_SEQ.NEXTVAL INTO :NEW.T_FROBID FROM DUAL;
  ELSE
    SELECT LAST_NUMBER INTO v_id FROM USER_SEQUENCES WHERE UPPER(SEQUENCE_NAME) = UPPER('DDLCONTRFROB_DBT_SEQ');
    IF :NEW.T_FROBID >= v_id THEN
      RAISE DUP_VAL_ON_INDEX;
    END IF;
  END IF;
END;
/

CREATE OR REPLACE TRIGGER DDLCONTRFROB_DBT_VERSION
  BEFORE INSERT OR UPDATE ON DDLCONTRFROB_DBT FOR EACH ROW
DECLARE
BEGIN
  IF :old.t_version is not null THEN
    :new.t_version := :old.t_version + 1;
  ELSE
    :new.t_version := 0;
  END IF;
END;
/

CREATE OR REPLACE TRIGGER DLCONTRFROB_DBT_TBIU
  BEFORE INSERT OR UPDATE 
  ON DDLCONTRFROB_DBT
  FOR EACH ROW
DECLARE
BEGIN

  IF :NEW.T_CHANGEDATE = TO_DATE('01.01.0001','DD.MM.YYYY') THEN
    :NEW.T_CHANGEDATE := TRUNC(SYSDATE);
    :NEW.T_CHANGETIME := TO_DATE('01.01.0001:' || TO_CHAR(SYSDATE, 'HH24:MI:SS'), 'DD.MM.YYYY:HH24:MI:SS'); 
  END IF;

END;
/

CREATE OR REPLACE TRIGGER DLCONTRFROB_DBT_TAIU
  AFTER INSERT OR UPDATE 
  ON DDLCONTRFROB_DBT
  FOR EACH ROW
DECLARE
BEGIN

  IF UPDATING THEN
    INSERT INTO DDLCONTRFROBCH_DBT (T_ID, 
                                    T_FROBID, 
                                    T_TAXPERIOD_OLD, 
                                    T_INCOMECODE_OLD, 
                                    T_INCOMESUM_OLD, 
                                    T_DEDUCTIONCODE_OLD, 
                                    T_DEDUCTIONSUM_OLD, 
                                    T_TAXPERIOD_NEW, 
                                    T_INCOMECODE_NEW, 
                                    T_INCOMESUM_NEW, 
                                    T_DEDUCTIONCODE_NEW, 
                                    T_DEDUCTIONSUM_NEW,
                                    T_OPER, 
                                    T_CHANGEDATE, 
                                    T_CHANGETIME
                                   )
                            VALUES(0, 
                                   :NEW.T_FROBID, 
                                   :OLD.T_TAXPERIOD, 
                                   :OLD.T_INCOMECODE, 
                                   :OLD.T_INCOMESUM,
                                   :OLD.T_DEDUCTIONCODE, 
                                   :OLD.T_DEDUCTIONSUM, 
                                   :NEW.T_TAXPERIOD, 
                                   :NEW.T_INCOMECODE, 
                                   :NEW.T_INCOMESUM,
                                   :NEW.T_DEDUCTIONCODE, 
                                   :NEW.T_DEDUCTIONSUM,  
                                   :NEW.T_OPER, 
                                   :NEW.T_CHANGEDATE,
                                   :NEW.T_CHANGETIME
                                  );
  ELSE
    INSERT INTO DDLCONTRFROBCH_DBT (T_ID, 
                                    T_FROBID, 
                                    T_TAXPERIOD_OLD, 
                                    T_INCOMECODE_OLD, 
                                    T_INCOMESUM_OLD, 
                                    T_DEDUCTIONCODE_OLD, 
                                    T_DEDUCTIONSUM_OLD, 
                                    T_TAXPERIOD_NEW, 
                                    T_INCOMECODE_NEW, 
                                    T_INCOMESUM_NEW, 
                                    T_DEDUCTIONCODE_NEW, 
                                    T_DEDUCTIONSUM_NEW,
                                    T_OPER, 
                                    T_CHANGEDATE, 
                                    T_CHANGETIME
                                   )
                            VALUES(0, 
                                   :NEW.T_FROBID, 
                                   0, 
                                   0, 
                                   0,
                                   0,
                                   0, 
                                   :NEW.T_TAXPERIOD, 
                                   :NEW.T_INCOMECODE, 
                                   :NEW.T_INCOMESUM,
                                   :NEW.T_DEDUCTIONCODE, 
                                   :NEW.T_DEDUCTIONSUM,  
                                   :NEW.T_OPER, 
                                   :NEW.T_CHANGEDATE,
                                   :NEW.T_CHANGETIME
                                  );
  END IF;

END;
/