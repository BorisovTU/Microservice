-- Таблица "DDLCONTRINFBROK_DBT"

CREATE TABLE DDLCONTRINFBROK_DBT (
    T_INFID NUMBER(10)
  , T_DLCONTRID NUMBER(10)
  , T_RECNUM NUMBER(5)
  , T_BROKERNAME VARCHAR2(60)
  , T_CONTRNUMBER VARCHAR2(30)
  , T_CONTRDATE DATE
  , T_NUMANDDATEREF VARCHAR2(60)
  , T_LISTCOUNT NUMBER(5)
  , T_OPER NUMBER(5)
  , T_CHANGEDATE DATE
  , T_CHANGETIME DATE
  , T_TYPE NUMBER(5)
  , T_VERSION NUMBER(5)
)
/
COMMENT ON TABLE DDLCONTRINFBROK_DBT IS 'ДБО. Сведения о других брокерах'
/
COMMENT ON COLUMN DDLCONTRINFBROK_DBT.T_INFID IS 'Идентификатор записи'
/
COMMENT ON COLUMN DDLCONTRINFBROK_DBT.T_DLCONTRID IS 'Идентификатор ДБО'
/
COMMENT ON COLUMN DDLCONTRINFBROK_DBT.T_RECNUM IS 'Номер записи'
/
COMMENT ON COLUMN DDLCONTRINFBROK_DBT.T_BROKERNAME IS 'Наименование первоначального брокера'
/
COMMENT ON COLUMN DDLCONTRINFBROK_DBT.T_CONTRNUMBER IS '№ договора ИИС у первоначального брокера'
/
COMMENT ON COLUMN DDLCONTRINFBROK_DBT.T_CONTRDATE IS 'Дата договора ИИС у первоначального брокера'
/
COMMENT ON COLUMN DDLCONTRINFBROK_DBT.T_NUMANDDATEREF IS 'Номер и дата Справки с информацией другого брокера'
/
COMMENT ON COLUMN DDLCONTRINFBROK_DBT.T_LISTCOUNT IS 'Количество листов в Справке от другого брокера'
/
COMMENT ON COLUMN DDLCONTRINFBROK_DBT.T_OPER IS 'Операционист, изменивший запись'
/
COMMENT ON COLUMN DDLCONTRINFBROK_DBT.T_CHANGEDATE IS 'Дата изменения'
/
COMMENT ON COLUMN DDLCONTRINFBROK_DBT.T_CHANGETIME IS 'Время изменения записи'
/
COMMENT ON COLUMN DDLCONTRINFBROK_DBT.T_TYPE IS 'Тип записи'
/
COMMENT ON COLUMN DDLCONTRINFBROK_DBT.T_VERSION IS 'Номер изменения записи'
/

BEGIN
  EXECUTE IMMEDIATE 'DROP INDEX DDLCONTRINFBROK_DBT_IDX0';
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/

CREATE UNIQUE INDEX DDLCONTRINFBROK_DBT_IDX0 ON DDLCONTRINFBROK_DBT (
   T_INFID ASC
)
/

BEGIN
  EXECUTE IMMEDIATE 'DROP INDEX DDLCONTRINFBROK_DBT_IDX1';
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/

CREATE   INDEX DDLCONTRINFBROK_DBT_IDX1 ON DDLCONTRINFBROK_DBT (
   T_DLCONTRID ASC
  ,T_RECNUM ASC
)
/

CREATE SEQUENCE DDLCONTRINFBROK_DBT_SEQ 
  START WITH 1
  MAXVALUE 999999999999999999999999999
  MINVALUE 1
  NOCYCLE
  NOCACHE
  NOORDER
/

CREATE OR REPLACE TRIGGER DDLCONTRINFBROK_DBT_T0_AINC
  BEFORE INSERT OR UPDATE OF T_INFID ON DDLCONTRINFBROK_DBT FOR EACH ROW
DECLARE
  v_id INTEGER;
BEGIN
  IF (:NEW.T_INFID = 0 OR :NEW.T_INFID IS NULL) THEN
    SELECT DDLCONTRINFBROK_DBT_SEQ.NEXTVAL INTO :NEW.T_INFID FROM DUAL;
  ELSE
    SELECT LAST_NUMBER INTO v_id FROM USER_SEQUENCES WHERE UPPER(SEQUENCE_NAME) = UPPER('DDLCONTRINFBROK_DBT_SEQ');
    IF :NEW.T_INFID >= v_id THEN
      RAISE DUP_VAL_ON_INDEX;
    END IF;
  END IF;
END;
/

CREATE OR REPLACE TRIGGER DLCONTRINFBROK_DBT_TBIU
  BEFORE INSERT OR UPDATE 
  ON DDLCONTRINFBROK_DBT
  FOR EACH ROW
DECLARE
BEGIN

  IF :NEW.T_CHANGEDATE = TO_DATE('01.01.0001','DD.MM.YYYY') THEN
    :NEW.T_CHANGEDATE := TRUNC(SYSDATE);
    :NEW.T_CHANGETIME := TO_DATE('01.01.0001:' || TO_CHAR(SYSDATE, 'HH24:MI:SS'), 'DD.MM.YYYY:HH24:MI:SS'); 
  END IF;

END;
/

CREATE OR REPLACE TRIGGER DLCONTRINFBROK_DBT_TAIU
  AFTER INSERT OR UPDATE 
  ON DDLCONTRINFBROK_DBT
  FOR EACH ROW
DECLARE
BEGIN

  IF UPDATING THEN
    INSERT INTO DDLCONTRINFBROKCH_DBT (T_ID 
                                     , T_INFID
                                     , T_RECNUM_OLD
                                     , T_RECNUM_NEW
                                     , T_BROKERNAME_OLD
                                     , T_BROKERNAME_NEW
                                     , T_CONTRNUMBER_OLD
                                     , T_CONTRNUMBER_NEW
                                     , T_CONTRDATE_OLD
                                     , T_CONTRDATE_NEW
                                     , T_NUMANDDATEREF_OLD
                                     , T_NUMANDDATEREF_NEW
                                     , T_LISTCOUNT_OLD
                                     , T_LISTCOUNT_NEW
                                     , T_OPER
                                     , T_CHANGEDATE
                                     , T_CHANGETIME
                                   )
                            VALUES(0, 
                                   :NEW.T_INFID, 
                                   :OLD.T_RECNUM,       
                                   :NEW.T_RECNUM,       
                                   :OLD.T_BROKERNAME,   
                                   :NEW.T_BROKERNAME,   
                                   :OLD.T_CONTRNUMBER,  
                                   :NEW.T_CONTRNUMBER,  
                                   :OLD.T_CONTRDATE,     
                                   :NEW.T_CONTRDATE,    
                                   :OLD.T_NUMANDDATEREF,
                                   :NEW.T_NUMANDDATEREF,
                                   :OLD.T_LISTCOUNT,    
                                   :NEW.T_LISTCOUNT,     
                                   :NEW.T_OPER, 
                                   :NEW.T_CHANGEDATE,
                                   :NEW.T_CHANGETIME
                                  );
  ELSE
    INSERT INTO DDLCONTRINFBROKCH_DBT (T_ID 
                                     , T_INFID
                                     , T_RECNUM_OLD
                                     , T_RECNUM_NEW
                                     , T_BROKERNAME_OLD
                                     , T_BROKERNAME_NEW
                                     , T_CONTRNUMBER_OLD
                                     , T_CONTRNUMBER_NEW
                                     , T_CONTRDATE_OLD
                                     , T_CONTRDATE_NEW
                                     , T_NUMANDDATEREF_OLD
                                     , T_NUMANDDATEREF_NEW
                                     , T_LISTCOUNT_OLD
                                     , T_LISTCOUNT_NEW
                                     , T_OPER
                                     , T_CHANGEDATE
                                     , T_CHANGETIME
                                   )
                            VALUES(0, 
                                   :NEW.T_INFID, 
                                   0,       
                                   :NEW.T_RECNUM,       
                                   CHR(1),   
                                   :NEW.T_BROKERNAME,   
                                   CHR(1),  
                                   :NEW.T_CONTRNUMBER,  
                                   TO_DATE('01.01.0001','DD.MM.YYYY'),     
                                   :NEW.T_CONTRDATE,    
                                   CHR(1),
                                   :NEW.T_NUMANDDATEREF,
                                   0,    
                                   :NEW.T_LISTCOUNT,
                                   :NEW.T_OPER, 
                                   :NEW.T_CHANGEDATE,
                                   :NEW.T_CHANGETIME
                                  );
  END IF;

END;
/