-- Таблица "DNPTXLINKSKINDSNOB_DBT"

CREATE TABLE DNPTXLINKSKINDSNOB_DBT (
    T_ID NUMBER(10)
  , T_TYPESNOB NUMBER(10)
  , T_TAXBASETYPE NUMBER(10)
  , T_BEGDATETYPESNOB DATE
  , T_ENDDATETYPESNOB DATE
  , T_USER NUMBER(5)
  , T_DREC DATE
)
/
COMMENT ON TABLE DNPTXLINKSKINDSNOB_DBT IS 'Настроечная табл. связи видов НОБ и СНОБ'/
COMMENT ON COLUMN DNPTXLINKSKINDSNOB_DBT.T_ID IS 'Идентификатор'/
COMMENT ON COLUMN DNPTXLINKSKINDSNOB_DBT.T_TYPESNOB IS 'Вид СНОБ'/
COMMENT ON COLUMN DNPTXLINKSKINDSNOB_DBT.T_TAXBASETYPE IS 'Вид НОБ для Хранилища'/
COMMENT ON COLUMN DNPTXLINKSKINDSNOB_DBT.T_BEGDATETYPESNOB IS 'Дата начала действия'/
COMMENT ON COLUMN DNPTXLINKSKINDSNOB_DBT.T_ENDDATETYPESNOB IS 'Дата окончания действия'/
COMMENT ON COLUMN DNPTXLINKSKINDSNOB_DBT.T_USER IS 'Пользователь'/
COMMENT ON COLUMN DNPTXLINKSKINDSNOB_DBT.T_DREC IS 'Дата записи'/

CREATE UNIQUE INDEX DNPTXLINKSKINDSNOB_DBT_IDX0 ON DNPTXLINKSKINDSNOB_DBT (
   T_ID ASC
)
/

CREATE UNIQUE INDEX DNPTXLINKSKINDSNOB_DBT_IDX1 ON DNPTXLINKSKINDSNOB_DBT (
   T_TYPESNOB ASC
  ,T_TAXBASETYPE ASC
  ,T_BEGDATETYPESNOB ASC
  ,T_ENDDATETYPESNOB ASC
)
/

CREATE SEQUENCE DNPTXLINKSKINDSNOB_DBT_SEQ 
  START WITH 1
  MAXVALUE 999999999999999999999999999
  MINVALUE 1
  NOCYCLE
  NOCACHE
  NOORDER
/

CREATE OR REPLACE TRIGGER DNPTXLINKSKINDSNOB_DBT_T0_AINC
  BEFORE INSERT OR UPDATE OF T_ID ON DNPTXLINKSKINDSNOB_DBT FOR EACH ROW
DECLARE
  v_id INTEGER;
BEGIN
  IF (:NEW.T_ID = 0 OR :NEW.T_ID IS NULL) THEN
    SELECT DNPTXLINKSKINDSNOB_DBT_SEQ.NEXTVAL INTO :NEW.T_ID FROM DUAL;
  ELSE
    SELECT LAST_NUMBER INTO v_id FROM USER_SEQUENCES WHERE UPPER(SEQUENCE_NAME) = UPPER('DNPTXLINKSKINDSNOB_DBT_SEQ');
    IF :NEW.T_ID >= v_id THEN
      RAISE DUP_VAL_ON_INDEX;
    END IF;
  END IF;
END;
/

CREATE OR REPLACE TRIGGER DNPTXLINKSKINDSNOB_DBT_TBIU
  BEFORE INSERT OR UPDATE 
  ON DNPTXLINKSKINDSNOB_DBT
  FOR EACH ROW
DECLARE
BEGIN

  :NEW.T_DREC := TRUNC(SYSDATE);

END;
/

CREATE OR REPLACE TRIGGER DNPTXLINKSKINDSNOB_DBT_TAIU
  AFTER INSERT OR UPDATE 
  ON DNPTXLINKSKINDSNOB_DBT
  FOR EACH ROW
DECLARE
BEGIN

  IF UPDATING THEN
    INSERT INTO DNPTXLINKSKINDSNOBCH_DBT (T_ID,
                                          T_LNKID,
                                          T_TYPESNOB_OLD,
                                          T_TAXBASETYPE_OLD,
                                          T_BEGDATETYPESNOB_OLD,
                                          T_ENDDATETYPESNOB_OLD,
                                          T_TYPESNOB_NEW,
                                          T_TAXBASETYPE_NEW,
                                          T_BEGDATETYPESNOB_NEW,
                                          T_ENDDATETYPESNOB_NEW,
                                          T_USER,
                                          T_DREC
                                         )
                                  VALUES(0, 
                                         :NEW.T_ID, 
                                         :OLD.T_TYPESNOB,       
                                         :OLD.T_TAXBASETYPE,    
                                         :OLD.T_BEGDATETYPESNOB,
                                         :OLD.T_ENDDATETYPESNOB, 
                                         :NEW.T_TYPESNOB,       
                                         :NEW.T_TAXBASETYPE,    
                                         :NEW.T_BEGDATETYPESNOB,
                                         :NEW.T_ENDDATETYPESNOB, 
                                         :NEW.T_USER, 
                                         :NEW.T_DREC
                                        );
  ELSE
    INSERT INTO DNPTXLINKSKINDSNOBCH_DBT (T_ID,
                                          T_LNKID,
                                          T_TYPESNOB_OLD,
                                          T_TAXBASETYPE_OLD,
                                          T_BEGDATETYPESNOB_OLD,
                                          T_ENDDATETYPESNOB_OLD,
                                          T_TYPESNOB_NEW,
                                          T_TAXBASETYPE_NEW,
                                          T_BEGDATETYPESNOB_NEW,
                                          T_ENDDATETYPESNOB_NEW,
                                          T_USER,
                                          T_DREC
                                         )
                                  VALUES(0, 
                                         :NEW.T_ID, 
                                         0, 
                                         0,
                                         TO_DATE('01.01.0001','DD.MM.YYYY'),
                                         TO_DATE('01.01.0001','DD.MM.YYYY'), 
                                         :NEW.T_TYPESNOB,       
                                         :NEW.T_TAXBASETYPE,    
                                         :NEW.T_BEGDATETYPESNOB,
                                         :NEW.T_ENDDATETYPESNOB, 
                                         :NEW.T_USER, 
                                         :NEW.T_DREC
                                        );
  END IF;

END;
/