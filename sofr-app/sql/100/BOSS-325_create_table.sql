declare
    vcnt number;
begin
   select count(*) into vcnt from user_tables where upper(table_name) = 'DCDRECORDS_DBT';
   if vcnt = 0 then
         EXECUTE IMMEDIATE 'CREATE TABLE DCDRECORDS_DBT (
            T_ID                      NUMBER(20)
          , T_SHORTNAME               VARCHAR2(60 CHAR)
          , T_FULLNAME                VARCHAR2(320 CHAR)
          , T_AGREEMENTNUMBER         VARCHAR2(20 CHAR)
          , T_ISIIS                   CHAR(1 CHAR)
          , T_AGREEMENTOPENDATE       DATE
          , T_AGREEMENTCLOSEDATE       DATE
          , T_CORPORATEACTIONTYPE     VARCHAR2(10 CHAR)
          , T_PAYMENTTYPE             VARCHAR2(50 CHAR)
          , T_RECORDPAYMENTID         NUMBER(10)
          , T_OPERATIONSTATUS         VARCHAR2(50 CHAR)
          , T_PAYMENTDATE             DATE
          , T_CLIENTID_OBJECTID       VARCHAR2(50 CHAR)
          , T_CLIENTID_SYSTEMID       VARCHAR2(50 CHAR)
          , T_CLIENTID_SYSTEMNODEID   VARCHAR2(50 CHAR)
          , T_FINANCIALNAME           VARCHAR2(50 CHAR)
          , T_ISINREGISTRATIONNUMBER  VARCHAR2(25 CHAR)
          , T_RECORDSNOBID            VARCHAR2(50 CHAR)
          , T_TAXRATE                 NUMBER(10)
          , T_TAXBASE                 NUMBER(32,12)
          , T_ISSUERCURRENCY          VARCHAR2(20 CHAR)
          , T_ISSUERSUM               NUMBER(32,12)
          , T_CLIENTSUM               NUMBER(32,12)
          , T_CLIENTCURRENCY          VARCHAR2(20 CHAR)
          , T_KBK                     VARCHAR2(30 CHAR)
          , T_RETURNTAX               NUMBER(32,12)
          , T_INDIVIDUALTAX           NUMBER(32,12)
          , T_TAXREDUCTIONSUM         NUMBER(32,12)
          , T_SUMD1                   NUMBER(32,12)
          , T_SUMD2                   NUMBER(32,12)
          , T_OFFSETTAX               NUMBER(32,12)
          , T_ACCOUNTNUMBER           VARCHAR2(25 CHAR)
          , T_OPERATIONDATE           DATE
          , T_ISGETTAX                CHAR(1 CHAR)
          , T_COUPONNUMBER            NUMBER(10)
          , T_COUPONSTARTDATE         DATE
          , T_COUPONENDDATE           DATE
          , T_PROCRESULT              VARCHAR2(4000 CHAR)
     )';
     EXECUTE IMMEDIATE q'[COMMENT ON TABLE DCDRECORDS_DBT IS 'Данные из Депозитария']';
     EXECUTE IMMEDIATE q'[COMMENT ON COLUMN DCDRECORDS_DBT.T_SHORTNAME               IS 'Фамилия и инициалы клиента']';
     EXECUTE IMMEDIATE q'[COMMENT ON COLUMN DCDRECORDS_DBT.T_FULLNAME                IS 'ФИО клиента полностью']';
     EXECUTE IMMEDIATE q'[COMMENT ON COLUMN DCDRECORDS_DBT.T_AGREEMENTNUMBER         IS 'Номер ДБО']';
     EXECUTE IMMEDIATE q'[COMMENT ON COLUMN DCDRECORDS_DBT.T_ISIIS                   IS 'Признак ИИС']';
     EXECUTE IMMEDIATE q'[COMMENT ON COLUMN DCDRECORDS_DBT.T_AGREEMENTOPENDATE       IS 'Дата открытия ДБО']';
     EXECUTE IMMEDIATE q'[COMMENT ON COLUMN DCDRECORDS_DBT.T_AGREEMENTCLOSEDATE      IS 'Дата закрытия ДБО']';
     EXECUTE IMMEDIATE q'[COMMENT ON COLUMN DCDRECORDS_DBT.T_CORPORATEACTIONTYPE     IS 'Вид корпоративного действия в терминах Депозитария']';
     EXECUTE IMMEDIATE q'[COMMENT ON COLUMN DCDRECORDS_DBT.T_PAYMENTTYPE             IS 'Тип выплаты']';
     EXECUTE IMMEDIATE q'[COMMENT ON COLUMN DCDRECORDS_DBT.T_RECORDPAYMENTID         IS 'ID выплаты в Диасофт. Уникальный номер КД в Диасофт']';
     EXECUTE IMMEDIATE q'[COMMENT ON COLUMN DCDRECORDS_DBT.T_OPERATIONSTATUS         IS 'Статус операции']';
     EXECUTE IMMEDIATE q'[COMMENT ON COLUMN DCDRECORDS_DBT.T_PAYMENTDATE             IS 'Дата выплаты клиенту']';
     EXECUTE IMMEDIATE q'[COMMENT ON COLUMN DCDRECORDS_DBT.T_CLIENTID_OBJECTID       IS 'ID клиента в системе ЦФТ']';
     EXECUTE IMMEDIATE q'[COMMENT ON COLUMN DCDRECORDS_DBT.T_CLIENTID_SYSTEMID       IS 'ID клиента в системе ЦФТ']';
     EXECUTE IMMEDIATE q'[COMMENT ON COLUMN DCDRECORDS_DBT.T_CLIENTID_SYSTEMNODEID   IS 'ID клиента в системе ЦФТ']';
     EXECUTE IMMEDIATE q'[COMMENT ON COLUMN DCDRECORDS_DBT.T_FINANCIALNAME           IS 'Наименование фин.инструмента']';
     EXECUTE IMMEDIATE q'[COMMENT ON COLUMN DCDRECORDS_DBT.T_ISINREGISTRATIONNUMBER  IS 'ISIN или гос. регистрационный номер ЦБ в разрезе выпусков (если есть) или информация о ПФИ']';
     EXECUTE IMMEDIATE q'[COMMENT ON COLUMN DCDRECORDS_DBT.T_RECORDSNOBID            IS 'ID записи в СНОБ']';
     EXECUTE IMMEDIATE q'[COMMENT ON COLUMN DCDRECORDS_DBT.T_TAXRATE                 IS 'Процентная ставка НДФЛ, по которой был рассчитан НДФЛ']';
     EXECUTE IMMEDIATE q'[COMMENT ON COLUMN DCDRECORDS_DBT.T_TAXBASE                 IS 'Сумма выплаты, полученная от эмитента']';
     EXECUTE IMMEDIATE q'[COMMENT ON COLUMN DCDRECORDS_DBT.T_ISSUERCURRENCY          IS 'Валюта выплаты эмитентом']';
     EXECUTE IMMEDIATE q'[COMMENT ON COLUMN DCDRECORDS_DBT.T_ISSUERSUM               IS 'Сумма выплаты Эмитента в рублях по курсу в дату выплаты']';
     EXECUTE IMMEDIATE q'[COMMENT ON COLUMN DCDRECORDS_DBT.T_CLIENTSUM               IS 'Сумма выплаченная клиенту в валюте выплаты, за минусом удержанного НДФЛ']';
     EXECUTE IMMEDIATE q'[COMMENT ON COLUMN DCDRECORDS_DBT.T_CLIENTCURRENCY          IS 'Валюта выплаты клиенту']';
     EXECUTE IMMEDIATE q'[COMMENT ON COLUMN DCDRECORDS_DBT.T_KBK                     IS 'KBK (Код бюджетной классификации)']';
     EXECUTE IMMEDIATE q'[COMMENT ON COLUMN DCDRECORDS_DBT.T_RETURNTAX               IS 'Налог уплаченный (возвращенный)']';
     EXECUTE IMMEDIATE q'[COMMENT ON COLUMN DCDRECORDS_DBT.T_INDIVIDUALTAX           IS 'Налог исчисленный']';
     EXECUTE IMMEDIATE q'[COMMENT ON COLUMN DCDRECORDS_DBT.T_TAXREDUCTIONSUM         IS 'Сумма уменьшения налога']';
     EXECUTE IMMEDIATE q'[COMMENT ON COLUMN DCDRECORDS_DBT.T_SUMD1                   IS 'Значение Д1']';
     EXECUTE IMMEDIATE q'[COMMENT ON COLUMN DCDRECORDS_DBT.T_SUMD2                   IS 'Значение Д2']';
     EXECUTE IMMEDIATE q'[COMMENT ON COLUMN DCDRECORDS_DBT.T_OFFSETTAX               IS 'Сумма налога на прибыль организаций, подлежащая зачету (дивиденды ст. 214 НК РФ)']';
     EXECUTE IMMEDIATE q'[COMMENT ON COLUMN DCDRECORDS_DBT.T_ACCOUNTNUMBER           IS 'Счет для выплаты дохода в Платежной инструкции в Диасофт']';
     EXECUTE IMMEDIATE q'[COMMENT ON COLUMN DCDRECORDS_DBT.T_OPERATIONDATE           IS 'Дата выбора клиентом счета для выплаты дохода']';
     EXECUTE IMMEDIATE q'[COMMENT ON COLUMN DCDRECORDS_DBT.T_ISGETTAX                IS 'Удержание НДФЛ']';
     EXECUTE IMMEDIATE q'[COMMENT ON COLUMN DCDRECORDS_DBT.T_COUPONNUMBER            IS 'Номер купона']';
     EXECUTE IMMEDIATE q'[COMMENT ON COLUMN DCDRECORDS_DBT.T_COUPONSTARTDATE         IS 'Дата начала купона']';
     EXECUTE IMMEDIATE q'[COMMENT ON COLUMN DCDRECORDS_DBT.T_PROCRESULT              IS 'Результат обработки записи']';
   end if;
end;
/

declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where upper(i.TABLE_NAME)='DCDRECORDS_DBT' and upper(i.INDEX_NAME)='DCDRECORDS_DBT_IDX0' ;
  if cnt =1 then
    execute immediate 'drop index DCDRECORDS_DBT_IDX0';
  end if;
  execute immediate 'CREATE UNIQUE INDEX DCDRECORDS_DBT_IDX0 ON DCDRECORDS_DBT (T_ID)';
end;
/

declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where upper(i.TABLE_NAME)='DCDRECORDS_DBT' and upper(i.INDEX_NAME)='DCDRECORDS_DBT_IDX1' ;
  if cnt =1 then
    execute immediate 'drop index DCDRECORDS_DBT_IDX1';
  end if;
  execute immediate 'CREATE INDEX DCDRECORDS_DBT_IDX1 ON DCDRECORDS_DBT (T_RECORDPAYMENTID)';
end;
/

declare
 cnt number;
begin
  select count(*) into cnt from user_sequences i where (i.SEQUENCE_NAME)='DCDRECORDS_DBT_SEQ';
  if cnt =0 then
     execute immediate '
         CREATE SEQUENCE DCDRECORDS_DBT_SEQ 
           START WITH 1
           MAXVALUE 999999999999999999999999999
           MINVALUE 1
           NOCYCLE
           NOCACHE
           NOORDER
     ';
  end if;
end;
/

declare
 cnt number;
begin
     execute immediate q'[
       CREATE OR REPLACE TRIGGER DCDRECORDS_DBT_T1_AINC
         BEFORE INSERT OR UPDATE OF T_ID ON DCDRECORDS_DBT FOR EACH ROW
       DECLARE
         v_id INTEGER;
       BEGIN
         IF (:NEW.T_ID = 0 OR :NEW.T_ID IS NULL) THEN
           SELECT DCDRECORDS_DBT_SEQ.NEXTVAL INTO :NEW.T_ID FROM DUAL;
         ELSE
           SELECT LAST_NUMBER INTO v_id FROM USER_SEQUENCES WHERE UPPER(SEQUENCE_NAME) = UPPER('DCDRECORDS_DBT_SEQ');
           IF :NEW.T_ID >= v_id THEN
             RAISE DUP_VAL_ON_INDEX;
           END IF;
         END IF;
       END;
     ]';
end;
/


declare
    vcnt number;
begin
   select count(*) into vcnt from user_tables where upper(table_name) = 'DCDNPTXOBDC_DBT';
   if vcnt = 0 then
         EXECUTE IMMEDIATE 'CREATE TABLE DCDNPTXOBDC_DBT (
            T_ID                      NUMBER(10)
          , T_RECID                   NUMBER(10)
          , T_OBJID                   NUMBER(10)
     )';
   end if;
end;
/

declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where upper(i.TABLE_NAME)='DCDNPTXOBDC_DBT' and upper(i.INDEX_NAME)='DCDNPTXOBDC_DBT_IDX0' ;
  if cnt =1 then
    execute immediate 'drop index DCDNPTXOBDC_DBT_IDX0';
  end if;
  execute immediate 'CREATE UNIQUE INDEX DCDNPTXOBDC_DBT_IDX0 ON DCDNPTXOBDC_DBT (T_ID)';
end;
/

declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where upper(i.TABLE_NAME)='DCDNPTXOBDC_DBT' and upper(i.INDEX_NAME)='DCDNPTXOBDC_DBT_IDX1' ;
  if cnt =1 then
    execute immediate 'drop index DCDNPTXOBDC_DBT_IDX1';
  end if;
  execute immediate 'CREATE INDEX DCDNPTXOBDC_DBT_IDX1 ON DCDNPTXOBDC_DBT (T_RECID)';
end;
/

declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where upper(i.TABLE_NAME)='DCDNPTXOBDC_DBT' and upper(i.INDEX_NAME)='DCDNPTXOBDC_DBT_IDX2' ;
  if cnt =1 then
    execute immediate 'drop index DCDNPTXOBDC_DBT_IDX2';
  end if;
  execute immediate 'CREATE INDEX DCDNPTXOBDC_DBT_IDX2 ON DCDNPTXOBDC_DBT (T_OBJID)';
end;
/

declare
 cnt number;
begin
  select count(*) into cnt from user_sequences i where (i.SEQUENCE_NAME)='DCDNPTXOBDC_DBT_SEQ';
  if cnt =0 then
     execute immediate '
         CREATE SEQUENCE DCDNPTXOBDC_DBT_SEQ 
           START WITH 1
           MAXVALUE 999999999999999999999999999
           MINVALUE 1
           NOCYCLE
           NOCACHE
           NOORDER
     ';
  end if;
end;
/

declare
 cnt number;
begin
     execute immediate q'[
       CREATE OR REPLACE TRIGGER DCDNPTXOBDC_DBT_T1_AINC
         BEFORE INSERT OR UPDATE OF T_ID ON DCDNPTXOBDC_DBT FOR EACH ROW
       DECLARE
         v_id INTEGER;
       BEGIN
         IF (:NEW.T_ID = 0 OR :NEW.T_ID IS NULL) THEN
           SELECT DCDNPTXOBDC_DBT_SEQ.NEXTVAL INTO :NEW.T_ID FROM DUAL;
         ELSE
           SELECT LAST_NUMBER INTO v_id FROM USER_SEQUENCES WHERE UPPER(SEQUENCE_NAME) = UPPER('DCDNPTXOBDC_DBT_SEQ');
           IF :NEW.T_ID >= v_id THEN
             RAISE DUP_VAL_ON_INDEX;
           END IF;
         END IF;
       END;
     ]';
end;
/