-- Таблица "DNPTXSNOBLIMIT_DBT"

CREATE TABLE DNPTXSNOBLIMIT_DBT (
    T_ID NUMBER(10)
  , T_ISRESIDENT CHAR(1)
  , T_TYPESNOB NUMBER(10)
  , T_LIMITSNOB NUMBER(32,12)
  , T_TAXRATE NUMBER(5)
  , T_CODEKBK NUMBER(10)
  , T_BEGDATESCALE DATE
  , T_ENDDATESCALE DATE
  , T_USER NUMBER(5)
  , T_DREC DATE
)
/

COMMENT ON TABLE DNPTXSNOBLIMIT_DBT IS 'Настроечная табл. предельных знач. СНОБ'/
COMMENT ON COLUMN DNPTXSNOBLIMIT_DBT.T_ID IS 'Идентификатор'/
COMMENT ON COLUMN DNPTXSNOBLIMIT_DBT.T_ISRESIDENT IS 'Резидент'/
COMMENT ON COLUMN DNPTXSNOBLIMIT_DBT.T_TYPESNOB IS 'Вид СНОБ'/
COMMENT ON COLUMN DNPTXSNOBLIMIT_DBT.T_LIMITSNOB IS 'Предельное значение СНОБ (min)'/
COMMENT ON COLUMN DNPTXSNOBLIMIT_DBT.T_TAXRATE IS 'Ставка, %'/
COMMENT ON COLUMN DNPTXSNOBLIMIT_DBT.T_CODEKBK IS 'КБК по справочнику'/
COMMENT ON COLUMN DNPTXSNOBLIMIT_DBT.T_BEGDATESCALE IS 'Дата начала действия шкалы'/
COMMENT ON COLUMN DNPTXSNOBLIMIT_DBT.T_ENDDATESCALE IS 'Дата окончания действия шкалы'/
COMMENT ON COLUMN DNPTXSNOBLIMIT_DBT.T_USER IS 'Пользователь'/
COMMENT ON COLUMN DNPTXSNOBLIMIT_DBT.T_DREC IS 'Дата записи'/

CREATE UNIQUE INDEX DNPTXSNOBLIMIT_DBT_IDX0 ON DNPTXSNOBLIMIT_DBT (
   T_ID ASC
)/


CREATE UNIQUE INDEX DNPTXSNOBLIMIT_DBT_IDX1 ON DNPTXSNOBLIMIT_DBT (
   T_ISRESIDENT ASC
  ,T_TYPESNOB ASC
  ,T_TAXRATE ASC
  ,T_CODEKBK ASC
  ,T_BEGDATESCALE ASC
  ,T_ENDDATESCALE ASC
)
/

CREATE SEQUENCE DNPTXSNOBLIMIT_DBT_SEQ 
  START WITH 1
  MAXVALUE 999999999999999999999999999
  MINVALUE 1
  NOCYCLE
  NOCACHE
  NOORDER
/

CREATE OR REPLACE TRIGGER DNPTXSNOBLIMIT_DBT_T0_AINC
  BEFORE INSERT OR UPDATE OF T_ID ON DNPTXSNOBLIMIT_DBT FOR EACH ROW
DECLARE
  v_id INTEGER;
BEGIN
  IF (:NEW.T_ID = 0 OR :NEW.T_ID IS NULL) THEN
    SELECT DNPTXSNOBLIMIT_DBT_SEQ.NEXTVAL INTO :NEW.T_ID FROM DUAL;
  ELSE
    SELECT LAST_NUMBER INTO v_id FROM USER_SEQUENCES WHERE UPPER(SEQUENCE_NAME) = UPPER('DNPTXSNOBLIMIT_DBT_SEQ');
    IF :NEW.T_ID >= v_id THEN
      RAISE DUP_VAL_ON_INDEX;
    END IF;
  END IF;
END;
/

CREATE OR REPLACE TRIGGER DNPTXSNOBLIMIT_DBT_TBIU
  BEFORE INSERT OR UPDATE 
  ON DNPTXSNOBLIMIT_DBT
  FOR EACH ROW
DECLARE
BEGIN

  :NEW.T_DREC := TRUNC(SYSDATE);

END;
/

CREATE OR REPLACE TRIGGER DNPTXSNOBLIMIT_DBT_TAIU
  AFTER INSERT OR UPDATE 
  ON DNPTXSNOBLIMIT_DBT
  FOR EACH ROW
DECLARE
BEGIN

  IF UPDATING THEN
    INSERT INTO DNPTXSNOBLIMITCH_DBT (T_ID,
                                      T_LIMID,
                                      T_ISRESIDENT_OLD,
                                      T_TYPESNOB_OLD,
                                      T_LIMITSNOB_OLD,
                                      T_TAXRATE_OLD,
                                      T_CODEKBK_OLD,
                                      T_BEGDATESCALE_OLD,
                                      T_ENDDATESCALE_OLD,
                                      T_ISRESIDENT_NEW,
                                      T_TYPESNOB_NEW,
                                      T_LIMITSNOB_NEW,
                                      T_TAXRATE_NEW,
                                      T_CODEKBK_NEW,
                                      T_BEGDATESCALE_NEW,
                                      T_ENDDATESCALE_NEW,
                                      T_USER,
                                      T_DREC
                                     )
                              VALUES(0, 
                                     :NEW.T_ID, 
                                     :OLD.T_ISRESIDENT,       
                                     :OLD.T_TYPESNOB,
                                     :OLD.T_LIMITSNOB,    
                                     :OLD.T_TAXRATE,
                                     :OLD.T_CODEKBK,
                                     :OLD.T_BEGDATESCALE,
                                     :OLD.T_ENDDATESCALE, 
                                     :NEW.T_ISRESIDENT,       
                                     :NEW.T_TYPESNOB,
                                     :NEW.T_LIMITSNOB,    
                                     :NEW.T_TAXRATE,
                                     :NEW.T_CODEKBK,
                                     :NEW.T_BEGDATESCALE,
                                     :NEW.T_ENDDATESCALE, 
                                     :NEW.T_USER, 
                                     :NEW.T_DREC
                                    );
  ELSE
    INSERT INTO DNPTXSNOBLIMITCH_DBT (T_ID,
                                      T_LIMID,
                                      T_ISRESIDENT_OLD,
                                      T_TYPESNOB_OLD,
                                      T_LIMITSNOB_OLD,
                                      T_TAXRATE_OLD,
                                      T_CODEKBK_OLD,
                                      T_BEGDATESCALE_OLD,
                                      T_ENDDATESCALE_OLD,
                                      T_ISRESIDENT_NEW,
                                      T_TYPESNOB_NEW,
                                      T_LIMITSNOB_NEW,
                                      T_TAXRATE_NEW,
                                      T_CODEKBK_NEW,
                                      T_BEGDATESCALE_NEW,
                                      T_ENDDATESCALE_NEW,
                                      T_USER,
                                      T_DREC
                                     )
                              VALUES(0, 
                                     :NEW.T_ID,
                                     CHR(0), 
                                     0, 
                                     0,
                                     0,
                                     0,
                                     TO_DATE('01.01.0001','DD.MM.YYYY'),
                                     TO_DATE('01.01.0001','DD.MM.YYYY'), 
                                     :NEW.T_ISRESIDENT,       
                                     :NEW.T_TYPESNOB,
                                     :NEW.T_LIMITSNOB,    
                                     :NEW.T_TAXRATE,
                                     :NEW.T_CODEKBK,
                                     :NEW.T_BEGDATESCALE,
                                     :NEW.T_ENDDATESCALE, 
                                     :NEW.T_USER, 
                                     :NEW.T_DREC
                                    );
  END IF;

END;
/
