-- Таблица "DNPTXBENEFITSCONDITIONS_DBT"

CREATE TABLE DNPTXBENEFITSCONDITIONS_DBT (
    T_ID NUMBER(10)
  , T_BENEFITID NUMBER(10)
  , T_KINDDEALTYPE NUMBER(5)
  , T_DEALTYPE NUMBER(5)
  , T_DEALOBJECT NUMBER(5)
  , T_RESIDENTSTATUS NUMBER(5)
  , T_OWNERPERIODSIGN VARCHAR2(5)
  , T_OWNERPERIOD NUMBER(10)
  , T_ISEXCEPT CHAR(1)
  , T_ISSUERCOUNTRY VARCHAR2(3)
  , T_BENEFITSIGNONFI NUMBER(10)
  , T_CIRCULATE NUMBER(10)
  , T_BENEFITSIGNONISSUER NUMBER(10)
  , T_BENEFITSIGNONCLIENT NUMBER(10)
  , T_DEALDATESIGN VARCHAR2(5)
  , T_DEALDATE DATE
  , T_SPECIALCONDITION NUMBER(10)
  , T_VERSION NUMBER(5)
  , T_OPER NUMBER(5)
  , T_CHANGEDATE DATE
  , T_CHANGETIME DATE
)
/
COMMENT ON TABLE DNPTXBENEFITSCONDITIONS_DBT IS 'Условия для льгот'
/
COMMENT ON COLUMN DNPTXBENEFITSCONDITIONS_DBT.T_ID IS 'Уникальный идентификатор записи'
/
COMMENT ON COLUMN DNPTXBENEFITSCONDITIONS_DBT.T_BENEFITID IS 'Идентификатор вида льготы'
/
COMMENT ON COLUMN DNPTXBENEFITSCONDITIONS_DBT.T_KINDDEALTYPE IS 'Вид типа сделки (по виду связи НУ, по виду сделок)'
/
COMMENT ON COLUMN DNPTXBENEFITSCONDITIONS_DBT.T_DEALTYPE IS 'Тип сделки'
/
COMMENT ON COLUMN DNPTXBENEFITSCONDITIONS_DBT.T_DEALOBJECT IS 'Объект сделки (Идентификатор финансового инструмента)'
/
COMMENT ON COLUMN DNPTXBENEFITSCONDITIONS_DBT.T_RESIDENTSTATUS IS 'Налоговый статус ФЛ'
/
COMMENT ON COLUMN DNPTXBENEFITSCONDITIONS_DBT.T_OWNERPERIODSIGN IS 'Знак для срока владения (>, >= и тд)'
/
COMMENT ON COLUMN DNPTXBENEFITSCONDITIONS_DBT.T_OWNERPERIOD IS 'Срок владения'
/
COMMENT ON COLUMN DNPTXBENEFITSCONDITIONS_DBT.T_ISEXCEPT IS 'Доп. условие для выбора страны'
/
COMMENT ON COLUMN DNPTXBENEFITSCONDITIONS_DBT.T_ISSUERCOUNTRY IS 'Страна эмитента'
/
COMMENT ON COLUMN DNPTXBENEFITSCONDITIONS_DBT.T_BENEFITSIGNONFI IS 'Признак льготы на активе'
/
COMMENT ON COLUMN DNPTXBENEFITSCONDITIONS_DBT.T_CIRCULATE IS 'Обращаемость ФИ'
/
COMMENT ON COLUMN DNPTXBENEFITSCONDITIONS_DBT.T_BENEFITSIGNONISSUER IS 'Признак экономически значимой организации на эмитенте'
/
COMMENT ON COLUMN DNPTXBENEFITSCONDITIONS_DBT.T_BENEFITSIGNONCLIENT IS 'Признак льготы на клиенте'
/
COMMENT ON COLUMN DNPTXBENEFITSCONDITIONS_DBT.T_DEALDATESIGN IS 'Знак для даты заключения сделки (>, >= и тд)'
/
COMMENT ON COLUMN DNPTXBENEFITSCONDITIONS_DBT.T_DEALDATE IS 'Дата приобретения актива'
/
COMMENT ON COLUMN DNPTXBENEFITSCONDITIONS_DBT.T_SPECIALCONDITION IS 'Специальное условие'
/
COMMENT ON COLUMN DNPTXBENEFITSCONDITIONS_DBT.T_VERSION IS 'Номер изменения записи'
/
COMMENT ON COLUMN DNPTXBENEFITSCONDITIONS_DBT.T_OPER IS 'Операционист, изменивший запись'
/
COMMENT ON COLUMN DNPTXBENEFITSCONDITIONS_DBT.T_CHANGEDATE IS 'Дата изменения'
/
COMMENT ON COLUMN DNPTXBENEFITSCONDITIONS_DBT.T_CHANGETIME IS 'Время изменения записи'
/

BEGIN
  EXECUTE IMMEDIATE 'DROP INDEX DNPTXBENEFITSCOND_DBT_IDX0';
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/

CREATE UNIQUE INDEX DNPTXBENEFITSCOND_DBT_IDX0 ON DNPTXBENEFITSCONDITIONS_DBT (
   T_ID ASC
)
/

BEGIN
  EXECUTE IMMEDIATE 'DROP INDEX DNPTXBENEFITSCOND_DBT_IDX1';
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/

CREATE   INDEX DNPTXBENEFITSCOND_DBT_IDX1 ON DNPTXBENEFITSCONDITIONS_DBT (
   T_BENEFITID ASC
)
/

CREATE SEQUENCE DNPTXBENEFITSCONDITIONS_DBT_SEQ 
  START WITH 1
  MAXVALUE 999999999999999999999999999
  MINVALUE 1
  NOCYCLE
  NOCACHE
  NOORDER
/

CREATE OR REPLACE TRIGGER DNPTXBENEFITSCOND_DBT_T0_AINC
  BEFORE INSERT OR UPDATE OF T_ID ON DNPTXBENEFITSCONDITIONS_DBT FOR EACH ROW
DECLARE
  v_id INTEGER;
BEGIN
  IF (:NEW.T_ID = 0 OR :NEW.T_ID IS NULL) THEN
    SELECT DNPTXBENEFITSCONDITIONS_DBT_SEQ.NEXTVAL INTO :NEW.T_ID FROM DUAL;
  ELSE
    SELECT LAST_NUMBER INTO v_id FROM USER_SEQUENCES WHERE UPPER(SEQUENCE_NAME) = UPPER('DNPTXBENEFITSCONDITIONS_DBT_SEQ');
    IF :NEW.T_ID >= v_id THEN
      RAISE DUP_VAL_ON_INDEX;
    END IF;
  END IF;
END;
/

CREATE OR REPLACE TRIGGER DNPTXBENEFITSCOND_DBT_VERSION
  BEFORE INSERT OR UPDATE ON DNPTXBENEFITSCONDITIONS_DBT FOR EACH ROW
DECLARE
BEGIN
  IF :old.t_version is not null THEN
    :new.t_version := :old.t_version + 1;
  ELSE
    :new.t_version := 0;
  END IF;
END;
/

CREATE OR REPLACE TRIGGER DNPTXBENEFITSCOND_DBT_TBIU
  BEFORE INSERT OR UPDATE 
  ON DNPTXBENEFITSCONDITIONS_DBT
  FOR EACH ROW
DECLARE
BEGIN

  IF (INSERTING AND :NEW.T_CHANGEDATE = TO_DATE('01.01.0001','DD.MM.YYYY')) OR UPDATING THEN
    :NEW.T_CHANGEDATE := TRUNC(SYSDATE);
    :NEW.T_CHANGETIME := TO_DATE('01.01.0001:' || TO_CHAR(SYSDATE, 'HH24:MI:SS'), 'DD.MM.YYYY:HH24:MI:SS'); 
  END IF;

END;
/

CREATE OR REPLACE TRIGGER DNPTXBENEFITSCOND_DBT_TAIUD
  AFTER INSERT OR UPDATE OR DELETE
  ON DNPTXBENEFITSCONDITIONS_DBT
  FOR EACH ROW
DECLARE
  v_BenefitID NUMBER := 0;
  v_NeedLog NUMBER := 0;
BEGIN

  IF INSERTING OR UPDATING THEN
    v_BenefitID := :NEW.T_BENEFITID;
  ELSE
    v_BenefitID := :OLD.T_BENEFITID;
  END IF;

  SELECT Count(1)   
    INTO v_NeedLog
    FROM dnptxbenefits_dbt
   WHERE t_BenefitID = v_BenefitID
     AND t_Status > 1;

  IF v_NeedLog > 0 THEN

    IF UPDATING THEN
      INSERT INTO DNPTXBENEFITSCONDITIONSCH_DBT (  T_ID
                                                 , T_BENEFITID
                                                 , T_CONDID
                                                 , T_ACTION
                                                 , T_KINDDEALTYPE_OLD
                                                 , T_KINDDEALTYPE_NEW
                                                 , T_DEALTYPE_OLD
                                                 , T_DEALTYPE_NEW
                                                 , T_DEALOBJECT_OLD
                                                 , T_DEALOBJECT_NEW
                                                 , T_RESIDENTSTATUS_OLD
                                                 , T_RESIDENTSTATUS_NEW
                                                 , T_OWNERPERIODSIGN_OLD
                                                 , T_OWNERPERIODSIGN_NEW
                                                 , T_OWNERPERIOD_OLD
                                                 , T_OWNERPERIOD_NEW
                                                 , T_ISEXCEPT_OLD
                                                 , T_ISEXCEPT_NEW
                                                 , T_ISSUERCOUNTRY_OLD
                                                 , T_ISSUERCOUNTRY_NEW
                                                 , T_BENEFITSIGNONFI_OLD
                                                 , T_BENEFITSIGNONFI_NEW
                                                 , T_CIRCULATE_OLD
                                                 , T_CIRCULATE_NEW
                                                 , T_BENEFITSIGNONISSUER_OLD
                                                 , T_BENEFITSIGNONISSUER_NEW
                                                 , T_BENEFITSIGNONCLIENT_OLD
                                                 , T_BENEFITSIGNONCLIENT_NEW
                                                 , T_DEALDATESIGN_OLD
                                                 , T_DEALDATESIGN_NEW
                                                 , T_DEALDATE_OLD
                                                 , T_DEALDATE_NEW
                                                 , T_SPECIALCONDITION_OLD
                                                 , T_SPECIALCONDITION_NEW
                                                 , T_OPER
                                                 , T_CHANGEDATE
                                                 , T_CHANGETIME
                                     )
                              VALUES(0, 
                                     :NEW.T_BENEFITID,
                                     :NEW.T_ID, 
                                     2,
                                     :OLD.T_KINDDEALTYPE,           
                                     :NEW.T_KINDDEALTYPE,
                                     :OLD.T_DEALTYPE,           
                                     :NEW.T_DEALTYPE,           
                                     :OLD.T_DEALOBJECT,         
                                     :NEW.T_DEALOBJECT,         
                                     :OLD.T_RESIDENTSTATUS,         
                                     :NEW.T_RESIDENTSTATUS,         
                                     :OLD.T_OWNERPERIODSIGN,    
                                     :NEW.T_OWNERPERIODSIGN,    
                                     :OLD.T_OWNERPERIOD,        
                                     :NEW.T_OWNERPERIOD,        
                                     :OLD.T_ISEXCEPT,           
                                     :NEW.T_ISEXCEPT,           
                                     :OLD.T_ISSUERCOUNTRY,      
                                     :NEW.T_ISSUERCOUNTRY,      
                                     :OLD.T_BENEFITSIGNONFI,    
                                     :NEW.T_BENEFITSIGNONFI,    
                                     :OLD.T_CIRCULATE,          
                                     :NEW.T_CIRCULATE,
                                     :OLD.T_BENEFITSIGNONISSUER,
                                     :NEW.T_BENEFITSIGNONISSUER,          
                                     :OLD.T_BENEFITSIGNONCLIENT,
                                     :NEW.T_BENEFITSIGNONCLIENT,
                                     :OLD.T_DEALDATESIGN,       
                                     :NEW.T_DEALDATESIGN,       
                                     :OLD.T_DEALDATE,           
                                     :NEW.T_DEALDATE,           
                                     :OLD.T_SPECIALCONDITION,   
                                     :NEW.T_SPECIALCONDITION,   
                                     :NEW.T_OPER, 
                                     :NEW.T_CHANGEDATE,
                                     :NEW.T_CHANGETIME
                                    );
    ELSIF DELETING THEN
      INSERT INTO DNPTXBENEFITSCONDITIONSCH_DBT (  T_ID
                                                 , T_BENEFITID
                                                 , T_CONDID
                                                 , T_ACTION
                                                 , T_KINDDEALTYPE_OLD
                                                 , T_KINDDEALTYPE_NEW
                                                 , T_DEALTYPE_OLD
                                                 , T_DEALTYPE_NEW
                                                 , T_DEALOBJECT_OLD
                                                 , T_DEALOBJECT_NEW
                                                 , T_RESIDENTSTATUS_OLD
                                                 , T_RESIDENTSTATUS_NEW
                                                 , T_OWNERPERIODSIGN_OLD
                                                 , T_OWNERPERIODSIGN_NEW
                                                 , T_OWNERPERIOD_OLD
                                                 , T_OWNERPERIOD_NEW
                                                 , T_ISEXCEPT_OLD
                                                 , T_ISEXCEPT_NEW
                                                 , T_ISSUERCOUNTRY_OLD
                                                 , T_ISSUERCOUNTRY_NEW
                                                 , T_BENEFITSIGNONFI_OLD
                                                 , T_BENEFITSIGNONFI_NEW
                                                 , T_CIRCULATE_OLD
                                                 , T_CIRCULATE_NEW
                                                 , T_BENEFITSIGNONISSUER_OLD
                                                 , T_BENEFITSIGNONISSUER_NEW
                                                 , T_BENEFITSIGNONCLIENT_OLD
                                                 , T_BENEFITSIGNONCLIENT_NEW
                                                 , T_DEALDATESIGN_OLD
                                                 , T_DEALDATESIGN_NEW
                                                 , T_DEALDATE_OLD
                                                 , T_DEALDATE_NEW
                                                 , T_SPECIALCONDITION_OLD
                                                 , T_SPECIALCONDITION_NEW
                                                 , T_OPER
                                                 , T_CHANGEDATE
                                                 , T_CHANGETIME
                                     )
                              VALUES(0, 
                                     :OLD.T_BENEFITID,
                                     :OLD.T_ID,
                                     3,
                                     :OLD.T_KINDDEALTYPE,           
                                     0, 
                                     :OLD.T_DEALTYPE,           
                                     0,           
                                     :OLD.T_DEALOBJECT,         
                                     0,         
                                     :OLD.T_RESIDENTSTATUS,         
                                     0,         
                                     :OLD.T_OWNERPERIODSIGN,    
                                     CHR(1),    
                                     :OLD.T_OWNERPERIOD,        
                                     0,        
                                     :OLD.T_ISEXCEPT,           
                                     CHR(0),           
                                     :OLD.T_ISSUERCOUNTRY,      
                                     CHR(1),      
                                     :OLD.T_BENEFITSIGNONFI,    
                                     0,    
                                     :OLD.T_CIRCULATE,          
                                     0,
                                     :OLD.T_BENEFITSIGNONISSUER,
                                     0,     
                                     :OLD.T_BENEFITSIGNONCLIENT,
                                     0,
                                     :OLD.T_DEALDATESIGN,       
                                     CHR(1),       
                                     :OLD.T_DEALDATE,           
                                     TO_DATE('01.01.0001','DD.MM.YYYY'),           
                                     :OLD.T_SPECIALCONDITION,   
                                     0,   
                                     RsbSessionData.Oper, 
                                     TRUNC(SYSDATE),
                                     TO_DATE('01.01.0001:' || TO_CHAR(SYSDATE, 'HH24:MI:SS'), 'DD.MM.YYYY:HH24:MI:SS')
                                    );

    ELSE
     INSERT INTO DNPTXBENEFITSCONDITIONSCH_DBT (  T_ID
                                                , T_BENEFITID
                                                , T_CONDID
                                                , T_ACTION
                                                , T_KINDDEALTYPE_OLD
                                                , T_KINDDEALTYPE_NEW
                                                , T_DEALTYPE_OLD
                                                , T_DEALTYPE_NEW
                                                , T_DEALOBJECT_OLD
                                                , T_DEALOBJECT_NEW
                                                , T_RESIDENTSTATUS_OLD
                                                , T_RESIDENTSTATUS_NEW
                                                , T_OWNERPERIODSIGN_OLD
                                                , T_OWNERPERIODSIGN_NEW
                                                , T_OWNERPERIOD_OLD
                                                , T_OWNERPERIOD_NEW
                                                , T_ISEXCEPT_OLD
                                                , T_ISEXCEPT_NEW
                                                , T_ISSUERCOUNTRY_OLD
                                                , T_ISSUERCOUNTRY_NEW
                                                , T_BENEFITSIGNONFI_OLD
                                                , T_BENEFITSIGNONFI_NEW
                                                , T_CIRCULATE_OLD
                                                , T_CIRCULATE_NEW
                                                , T_BENEFITSIGNONISSUER_OLD
                                                , T_BENEFITSIGNONISSUER_NEW
                                                , T_BENEFITSIGNONCLIENT_OLD
                                                , T_BENEFITSIGNONCLIENT_NEW
                                                , T_DEALDATESIGN_OLD
                                                , T_DEALDATESIGN_NEW
                                                , T_DEALDATE_OLD
                                                , T_DEALDATE_NEW
                                                , T_SPECIALCONDITION_OLD
                                                , T_SPECIALCONDITION_NEW
                                                , T_OPER
                                                , T_CHANGEDATE
                                                , T_CHANGETIME
                                    )
                             VALUES(0, 
                                    :NEW.T_BENEFITID,
                                    :NEW.T_ID,
                                    1,
                                    0,           
                                    :NEW.T_KINDDEALTYPE, 
                                    0,           
                                    :NEW.T_DEALTYPE,           
                                    0,         
                                    :NEW.T_DEALOBJECT,         
                                    0,         
                                    :NEW.T_RESIDENTSTATUS,         
                                    CHR(1),    
                                    :NEW.T_OWNERPERIODSIGN,    
                                    0,        
                                    :NEW.T_OWNERPERIOD,        
                                    CHR(0),           
                                    :NEW.T_ISEXCEPT,           
                                    CHR(1),      
                                    :NEW.T_ISSUERCOUNTRY,      
                                    0,    
                                    :NEW.T_BENEFITSIGNONFI,    
                                    0,          
                                    :NEW.T_CIRCULATE,
                                    0,
                                    :NEW.T_BENEFITSIGNONISSUER,          
                                    0,
                                    :NEW.T_BENEFITSIGNONCLIENT,
                                    CHR(1),       
                                    :NEW.T_DEALDATESIGN,       
                                    TO_DATE('01.01.0001','DD.MM.YYYY'),           
                                    :NEW.T_DEALDATE,           
                                    0,   
                                    :NEW.T_SPECIALCONDITION,   
                                    :NEW.T_OPER, 
                                    :NEW.T_CHANGEDATE,
                                    :NEW.T_CHANGETIME
                                   ); 
      
    END IF;
  END IF;

END;
/