-- Таблица "DNPTXBENEFITS_DBT"

CREATE TABLE DNPTXBENEFITS_DBT (
    T_BENEFITID NUMBER(10)
  , T_BENEFITTYPE VARCHAR2(20)
  , T_BEGDATEBENEFIT DATE
  , T_ENDDATEBENEFIT DATE
  , T_BENEFITPRIORITY NUMBER(10)
  , T_BENEFITGROUP NUMBER(10)
  , T_LAW VARCHAR2(2000)
  , T_STATUS NUMBER(5)
  , T_OPER NUMBER(5)
  , T_CHANGEDATE DATE
  , T_CHANGETIME DATE
)
/
COMMENT ON TABLE DNPTXBENEFITS_DBT IS 'Настроечная таблица налоговых льгот'
/
COMMENT ON COLUMN DNPTXBENEFITS_DBT.T_BENEFITID IS 'Идентификатор вида льготы'
/
COMMENT ON COLUMN DNPTXBENEFITS_DBT.T_BENEFITTYPE IS 'Вид льготы'
/
COMMENT ON COLUMN DNPTXBENEFITS_DBT.T_BEGDATEBENEFIT IS 'Дата начала применения льготы'
/
COMMENT ON COLUMN DNPTXBENEFITS_DBT.T_ENDDATEBENEFIT IS 'Дата окончания применения льготы'
/
COMMENT ON COLUMN DNPTXBENEFITS_DBT.T_BENEFITPRIORITY IS 'Приоритет применения льготы'
/
COMMENT ON COLUMN DNPTXBENEFITS_DBT.T_BENEFITGROUP IS 'Идентификатор группы льготируемых доходов, к которой относится вид льготы'
/
COMMENT ON COLUMN DNPTXBENEFITS_DBT.T_LAW IS 'Законы, регулирующие предоставление льготы'
/
COMMENT ON COLUMN DNPTXBENEFITS_DBT.T_STATUS IS 'Статус'
/
COMMENT ON COLUMN DNPTXBENEFITS_DBT.T_OPER IS 'Операционист, изменивший запись'
/
COMMENT ON COLUMN DNPTXBENEFITS_DBT.T_CHANGEDATE IS 'Дата изменения'
/
COMMENT ON COLUMN DNPTXBENEFITS_DBT.T_CHANGETIME IS 'Время изменения записи'
/

BEGIN
  EXECUTE IMMEDIATE 'DROP INDEX DNPTXBENEFITS_DBT_IDX0';
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/

CREATE UNIQUE INDEX DNPTXBENEFITS_DBT_IDX0 ON DNPTXBENEFITS_DBT (
   T_BENEFITID ASC
)
/

CREATE SEQUENCE DNPTXBENEFITS_DBT_SEQ 
  START WITH 1
  MAXVALUE 999999999999999999999999999
  MINVALUE 1
  NOCYCLE
  NOCACHE
  NOORDER
/

CREATE OR REPLACE TRIGGER DNPTXBENEFITS_DBT_T0_AINC
  BEFORE INSERT OR UPDATE OF T_BENEFITID ON DNPTXBENEFITS_DBT FOR EACH ROW
DECLARE
  v_id INTEGER;
BEGIN
  IF (:NEW.T_BENEFITID = 0 OR :NEW.T_BENEFITID IS NULL) THEN
    SELECT DNPTXBENEFITS_DBT_SEQ.NEXTVAL INTO :NEW.T_BENEFITID FROM DUAL;
  ELSE
    SELECT LAST_NUMBER INTO v_id FROM USER_SEQUENCES WHERE UPPER(SEQUENCE_NAME) = UPPER('DNPTXBENEFITS_DBT_SEQ');
    IF :NEW.T_BENEFITID >= v_id THEN
      RAISE DUP_VAL_ON_INDEX;
    END IF;
  END IF;
END;
/

CREATE OR REPLACE TRIGGER DNPTXBENEFITS_DBT_TBIU
  BEFORE INSERT OR UPDATE 
  ON DNPTXBENEFITS_DBT
  FOR EACH ROW
DECLARE
BEGIN

  IF (INSERTING AND :NEW.T_CHANGEDATE = TO_DATE('01.01.0001','DD.MM.YYYY')) OR UPDATING THEN
    :NEW.T_CHANGEDATE := TRUNC(SYSDATE);
    :NEW.T_CHANGETIME := TO_DATE('01.01.0001:' || TO_CHAR(SYSDATE, 'HH24:MI:SS'), 'DD.MM.YYYY:HH24:MI:SS'); 
  END IF;

END;
/

CREATE OR REPLACE TRIGGER DNPTXBENEFITS_DBT_TAU
  AFTER UPDATE 
  ON DNPTXBENEFITS_DBT
  FOR EACH ROW
DECLARE
BEGIN

  IF :NEW.T_STATUS > 1 THEN
    INSERT INTO DNPTXBENEFITSCH_DBT (  T_ID
                                     , T_BENEFITID
                                     , T_BENEFITTYPE_OLD
                                     , T_BENEFITTYPE_NEW
                                     , T_BEGDATEBENEFIT_OLD
                                     , T_BEGDATEBENEFIT_NEW
                                     , T_ENDDATEBENEFIT_OLD
                                     , T_ENDDATEBENEFIT_NEW
                                     , T_BENEFITPRIORITY_OLD
                                     , T_BENEFITPRIORITY_NEW
                                     , T_BENEFITGROUP_OLD
                                     , T_BENEFITGROUP_NEW
                                     , T_LAW_OLD
                                     , T_LAW_NEW
                                     , T_STATUS_OLD
                                     , T_STATUS_NEW
                                     , T_OPER
                                     , T_CHANGEDATE
                                     , T_CHANGETIME
                                 )
                          VALUES(0, 
                                 :NEW.T_BENEFITID,          
                                 :OLD.T_BENEFITTYPE,    
                                 :NEW.T_BENEFITTYPE,    
                                 :OLD.T_BEGDATEBENEFIT, 
                                 :NEW.T_BEGDATEBENEFIT, 
                                 :OLD.T_ENDDATEBENEFIT, 
                                 :NEW.T_ENDDATEBENEFIT, 
                                 :OLD.T_BENEFITPRIORITY,
                                 :NEW.T_BENEFITPRIORITY,
                                 :OLD.T_BENEFITGROUP,   
                                 :NEW.T_BENEFITGROUP,   
                                 :OLD.T_LAW,            
                                 :NEW.T_LAW,            
                                 :OLD.T_STATUS,         
                                 :NEW.T_STATUS,         
                                 :NEW.T_OPER, 
                                 :NEW.T_CHANGEDATE,
                                 :NEW.T_CHANGETIME         
                                );
  END IF;

END;
/
