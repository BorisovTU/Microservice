
BEGIN
 BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE uloadaccforcompare_dbt';
 EXCEPTION
  WHEN OTHERS THEN
  BEGIN

   /*RETURN*/ NULL;

  END;
 END;

END;
/

BEGIN
 BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE uloadentforcompare_dbt';
 EXCEPTION
  WHEN OTHERS THEN
  BEGIN

   /*RETURN*/ NULL;

  END;
 END;
END;
/

BEGIN
 BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE uclientRegMB_Log_dbt';
 EXCEPTION
  WHEN OTHERS THEN
  BEGIN

   /*RETURN*/ NULL;

  END;
 END;
END;
/

BEGIN
 BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE uclientRegMB_LogTmp_dbt';
 EXCEPTION
  WHEN OTHERS THEN
  BEGIN

   /*RETURN*/ NULL;

  END;
 END;
END;
/

BEGIN
 BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE udl_lmtcashstock_exch_dbt';
 EXCEPTION
  WHEN OTHERS THEN
  BEGIN

   /*RETURN*/ NULL;

  END;
 END;
END;
/

BEGIN
 BEGIN
  EXECUTE IMMEDIATE 'DROP SEQUENCE udl_lmtcashstock_exch_SEQ_1';
 EXCEPTION
  WHEN OTHERS THEN
  BEGIN

   /*RETURN*/ NULL;

  END;
 END;
END;
/

BEGIN
 BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE udl_lmtsecuritest_exch_dbt';
 EXCEPTION
  WHEN OTHERS THEN
  BEGIN

   /*RETURN*/ NULL;

  END;
 END;
END;
/

BEGIN
 BEGIN
  EXECUTE IMMEDIATE 'DROP SEQUENCE udl_lmtsecuritest_exch_SEQ_1';
 EXCEPTION
  WHEN OTHERS THEN
  BEGIN

   /*RETURN*/ NULL;

  END;
 END;
END;
/

BEGIN
 BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE udl_lmtfuturmark_exch_dbt';
 EXCEPTION
  WHEN OTHERS THEN
  BEGIN

   /*RETURN*/ NULL;

  END;
 END;
END;
/

BEGIN
 BEGIN
  EXECUTE IMMEDIATE 'DROP SEQUENCE udl_lmtfuturmark_exch_SEQ_1';
 EXCEPTION
  WHEN OTHERS THEN
  BEGIN

   /*RETURN*/ NULL;

  END;
 END;
END;
/

BEGIN
 BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE udl_lmt_cshstck_scrt_cmp_dbt';
 EXCEPTION
  WHEN OTHERS THEN
  BEGIN

   /*RETURN*/ NULL;

  END;
 END;
END;
/

BEGIN
 BEGIN
  EXECUTE IMMEDIATE 'DROP SEQUENCE udl_lmt_cshstck_scrt_cmp_SEQ_1';
 EXCEPTION
  WHEN OTHERS THEN
  BEGIN

   /*RETURN*/ NULL;

  END;
 END;
END;
/

BEGIN
 BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE udl_lmtfuturmark_cmp_dbt';
 EXCEPTION
  WHEN OTHERS THEN
  BEGIN

   /*RETURN*/ NULL;

  END;
 END;
END;
/

BEGIN
 BEGIN
  EXECUTE IMMEDIATE 'DROP SEQUENCE udl_lmtfuturmark_cmp_SEQ_1';
 EXCEPTION
  WHEN OTHERS THEN
  BEGIN

   /*RETURN*/ NULL;

  END;
 END;
END;
/

BEGIN
 BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE udl_dl_lmtadjust_exch_dbt';
 EXCEPTION
  WHEN OTHERS THEN
  BEGIN

   /*RETURN*/ NULL;

  END;
 END;
END;
/

BEGIN
 BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE ulob_txt_tmp';
 EXCEPTION
  WHEN OTHERS THEN
  BEGIN

   /*RETURN*/ NULL;

  END;
 END;
END;
/

BEGIN
 BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE uTableProcessIn_dbt';
 EXCEPTION
  WHEN OTHERS THEN
  BEGIN

   /*RETURN*/ NULL;

  END;
 END;
END;
/

BEGIN
 BEGIN
  EXECUTE IMMEDIATE 'DROP SEQUENCE uTableProcessIn_SEQ_1';
 EXCEPTION
  WHEN OTHERS THEN
  BEGIN

   /*RETURN*/ NULL;

  END;
 END;
END;
/

BEGIN
 BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE uTableProcessOut_dbt';
 EXCEPTION
  WHEN OTHERS THEN
  BEGIN

   /*RETURN*/ NULL;

  END;
 END;
END;
/

BEGIN
 BEGIN
  EXECUTE IMMEDIATE 'DROP SEQUENCE uTableProcessOut_SEQ_1';
 EXCEPTION
  WHEN OTHERS THEN
  BEGIN

   /*RETURN*/ NULL;

  END;
 END;
END;
/

BEGIN
 BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE uTableProcessEvent_dbt';
 EXCEPTION
  WHEN OTHERS THEN
  BEGIN

   /*RETURN*/ NULL;

  END;
 END;
END;
/

BEGIN
 BEGIN
  EXECUTE IMMEDIATE 'DROP SEQUENCE uTableProcessEvent_SEQ_1';
 EXCEPTION
  WHEN OTHERS THEN
  BEGIN

   /*RETURN*/ NULL;

  END;
 END;
END;
/




CREATE TABLE uloadaccforcompare_dbt
(                                                                                    
  T_REQID            VARCHAR(20),
  T_DATEREPORT       DATE,
  T_ACCOUNTNUMBER    VARCHAR(25),
  T_RESULT           INTEGER,
  T_CURRENCYCODE     VARCHAR(3),
  T_DATEOPEN         DATE,
  T_DATECLOSE        DATE,
  T_RESTIN           NUMBER(32,12),
  T_TURNDEBIT        NUMBER(32,12),
  T_TURNCREDIT       NUMBER(32,12),
  T_RESTOUT          NUMBER(32,12)
);



COMMENT ON TABLE uloadaccforcompare_dbt IS 'Данные по счетам для сверки';

COMMENT ON COLUMN uloadaccforcompare_dbt.T_REQID IS 'ИД запроса';        

COMMENT ON COLUMN uloadaccforcompare_dbt.T_DATEREPORT IS 'Дата отчета';

COMMENT ON COLUMN uloadaccforcompare_dbt.T_ACCOUNTNUMBER IS 'Номер счета';

COMMENT ON COLUMN uloadaccforcompare_dbt.T_RESULT IS 'Наличие в АБС';
                                                                                     
COMMENT ON COLUMN uloadaccforcompare_dbt.T_CURRENCYCODE IS 'Код валюты счета';

COMMENT ON COLUMN uloadaccforcompare_dbt.T_DATEOPEN IS 'Дата открытия';

COMMENT ON COLUMN uloadaccforcompare_dbt.T_DATECLOSE IS 'Дата закрытия';

COMMENT ON COLUMN uloadaccforcompare_dbt.T_RESTIN IS 'Входящий остаток';

COMMENT ON COLUMN uloadaccforcompare_dbt.T_TURNDEBIT IS 'Оборот в дебете';

COMMENT ON COLUMN uloadaccforcompare_dbt.T_TURNCREDIT IS 'Оборот в кредите';

COMMENT ON COLUMN uloadaccforcompare_dbt.T_RESTOUT IS 'Исходящий остаток';
/

                                                                                     


CREATE TABLE uloadentforcompare_dbt
(                                                                                    
  T_REQID            VARCHAR(20),
  T_DATEREPORT       DATE,
  T_REFERENCEID      VARCHAR(20),
  T_RESULT           INTEGER,
  T_ENTRYID          VARCHAR(20),
  T_OPERATIONALDAY   DATE,
  T_DOCDATE          DATE,
  T_DEBITACCOUNT     VARCHAR(25),
  T_CREDITACCOUNT    VARCHAR(25),
  T_DEBITAMOUNT      NUMBER(32,12),
  T_CREDITAMOUNT     NUMBER(32,12),
  T_ENTRYDETAILS     VARCHAR(600)
);



COMMENT ON TABLE uloadentforcompare_dbt IS 'Данные по проводкам для сверки';

COMMENT ON COLUMN uloadentforcompare_dbt.T_REQID IS 'ИД запроса';        

COMMENT ON COLUMN uloadentforcompare_dbt.T_DATEREPORT IS 'Дата отчета';

COMMENT ON COLUMN uloadentforcompare_dbt.T_REFERENCEID IS 'ИД проводки в СОФР';

COMMENT ON COLUMN uloadentforcompare_dbt.T_RESULT IS 'Наличие в АБС';
                                                                                     
COMMENT ON COLUMN uloadentforcompare_dbt.T_ENTRYID IS 'ИД проводки в АБС';

COMMENT ON COLUMN uloadentforcompare_dbt.T_OPERATIONALDAY IS 'Дата исполнения';

COMMENT ON COLUMN uloadentforcompare_dbt.T_DOCDATE IS 'Дата документа';

COMMENT ON COLUMN uloadentforcompare_dbt.T_DEBITACCOUNT IS 'Номер счета в дебете';

COMMENT ON COLUMN uloadentforcompare_dbt.T_CREDITACCOUNT IS 'Номер счета в кредите';

COMMENT ON COLUMN uloadentforcompare_dbt.T_DEBITAMOUNT IS 'Сумма в дебете';

COMMENT ON COLUMN uloadentforcompare_dbt.T_CREDITAMOUNT IS 'Сумма в кредите';

COMMENT ON COLUMN uloadentforcompare_dbt.T_ENTRYDETAILS IS 'Назначение платежа';
/

             


CREATE TABLE uclientRegMB_Log_dbt
(                                                                                    
  T_RECID            NUMBER(10),
  T_SESSIONID        NUMBER(10),
  T_FILENAME         VARCHAR(100),
  T_LOADDATE         DATE DEFAULT TO_DATE(TO_CHAR(SYSDATE,'ddmmyyyy'), 'ddmmyyyy' ),
  T_LOADTIME         DATE DEFAULT TO_DATE( '01010001' || TO_CHAR(SYSDATE,'hh24miss'), 'ddmmyyyy hh24miss'),
  T_STATUS           INTEGER,
  T_OPER             NUMBER(5),
  T_ERRCODE          NUMBER(10),
  T_ERRTEXT          VARCHAR(2000),
  T_XML_MESS         CLOB
);



COMMENT ON TABLE uclientRegMB_Log_dbt IS 'Данные по результатам регистрации клиентов на МБ';

COMMENT ON COLUMN uclientRegMB_Log_dbt.T_RECID IS 'ИД сообщения';

COMMENT ON COLUMN uclientRegMB_Log_dbt.T_SESSIONID IS 'Id сессии пользователя';

COMMENT ON COLUMN uclientRegMB_Log_dbt.T_FILENAME IS 'Файл загрузки';

COMMENT ON COLUMN uclientRegMB_Log_dbt.T_LOADDATE IS 'Дата загрузки';

COMMENT ON COLUMN uclientRegMB_Log_dbt.T_LOADTIME IS 'Время загрузки';

COMMENT ON COLUMN uclientRegMB_Log_dbt.T_STATUS IS 'Статус загрузки 0 - успешно не обработан, 1 - успешно обработан, 2 - ошибка';
                                                                                     
COMMENT ON COLUMN uclientRegMB_Log_dbt.T_OPER IS 'Операционист';

COMMENT ON COLUMN uclientRegMB_Log_dbt.T_ERRCODE IS 'Код ошибки';

COMMENT ON COLUMN uclientRegMB_Log_dbt.T_ERRTEXT IS 'Текст ошибки';

COMMENT ON COLUMN uclientRegMB_Log_dbt.T_XML_MESS IS 'XML-сообщение';



CREATE UNIQUE INDEX UCLIENTREGMB_LOG_DBT_IDX0 ON uclientRegMB_Log_dbt (T_RECID);

CREATE UNIQUE INDEX UCLIENTREGMB_LOG_DBT_IDX1 ON uclientRegMB_Log_dbt (T_SESSIONID, T_FILENAME);
/             

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

CREATE TABLE uclientRegMB_LogTmp_dbt
(                                                                                    
  T_SESSIONID        NUMBER(10),
  T_FILENAME         VARCHAR(100),
  T_OPER             NUMBER(5),
  T_ERRCODE          NUMBER(10),
  T_ERRTEXT          VARCHAR(2000),
  T_XML_MESS         CLOB
);


CREATE INDEX UCLIENTREGMB_LOGTMP_DBT_IDX0 ON uclientRegMB_LogTmp_dbt (T_SESSIONID);

COMMENT ON TABLE uclientRegMB_LogTmp_dbt IS 'Данные по результатам регистрации клиентов на МБ';

COMMENT ON COLUMN uclientRegMB_LogTmp_dbt.T_SESSIONID IS 'Id сессии пользователя';

COMMENT ON COLUMN uclientRegMB_LogTmp_dbt.T_FILENAME IS 'Файл загрузки';

COMMENT ON COLUMN uclientRegMB_LogTmp_dbt.T_OPER IS 'Операционист';

COMMENT ON COLUMN uclientRegMB_LogTmp_dbt.T_ERRCODE IS 'Код ошибки';

COMMENT ON COLUMN uclientRegMB_LogTmp_dbt.T_ERRTEXT IS 'Текст ошибки';

COMMENT ON COLUMN uclientRegMB_LogTmp_dbt.T_XML_MESS IS 'XML-сообщение';

/

------------------------------------------------------------------------------------
CREATE TABLE udl_lmtcashstock_exch_dbt
(                                                                                    
  T_RECID            NUMBER(10),  
  T_LIMIT_TYPE       VARCHAR2(10),    --'MONEY'     
  T_FIRM_ID          VARCHAR2(12),    --DDL_LIMITCASHSTOCK_DBT.T_FIRM_ID
  T_TAG              VARCHAR2(5),     --DDL_LIMITCASHSTOCK_DBT.T_TAG
  T_CURR_CODE        VARCHAR2(3),     --DDL_LIMITCASHSTOCK_DBT.T_CURR_CODE
  T_CLIENT_CODE      VARCHAR2(35),    --DDL_LIMITCASHSTOCK_DBT.T_CLIENT_CODE
  T_OPEN_BALANCE     NUMBER(32,12),   --DDL_LIMITCASHSTOCK_DBT.T_OPEN_BALANCE
  T_OPEN_LIMIT       NUMBER(32,12),   --DDL_LIMITCASHSTOCK_DBT.T_OPEN_LIMIT
  T_CURRENT_LIMIT    NUMBER(32,12),   --DDL_LIMITCASHSTOCK_DBT.T_CURRENT_LIMIT
  T_LEVERAGE         NUMBER(32,12),   --DDL_LIMITCASHSTOCK_DBT.T_LEVERAGE
  T_LIMIT_KIND       NUMBER(5)        --DDL_LIMITCASHSTOCK_DBT.T_LIMIT_KIND
);



COMMENT ON TABLE udl_lmtcashstock_exch_dbt IS 'буфер обмена для таблицы DDL_LIMITCASHSTOCK_DBT Лимиты денежных средств фондового рынка';

COMMENT ON COLUMN udl_lmtcashstock_exch_dbt.T_RECID IS 'ИД записи автоинкремент';        

COMMENT ON COLUMN udl_lmtcashstock_exch_dbt.T_LIMIT_TYPE IS 'Тип лимита';

COMMENT ON COLUMN udl_lmtcashstock_exch_dbt.T_FIRM_ID IS 'Код участника торгов';

COMMENT ON COLUMN udl_lmtcashstock_exch_dbt.T_TAG IS 'Группа';
                                                                                     
COMMENT ON COLUMN udl_lmtcashstock_exch_dbt.T_CURR_CODE IS 'Код валюты счета';

COMMENT ON COLUMN udl_lmtcashstock_exch_dbt.T_CLIENT_CODE IS 'Код клиента';

COMMENT ON COLUMN udl_lmtcashstock_exch_dbt.T_OPEN_BALANCE IS 'Входящий остаток';

COMMENT ON COLUMN udl_lmtcashstock_exch_dbt.T_OPEN_LIMIT IS 'Входящий лимит';

COMMENT ON COLUMN udl_lmtcashstock_exch_dbt.T_CURRENT_LIMIT IS 'Текущий лимит';

COMMENT ON COLUMN udl_lmtcashstock_exch_dbt.T_LEVERAGE IS 'Плечо';

COMMENT ON COLUMN udl_lmtcashstock_exch_dbt.T_LIMIT_KIND IS 'Вид лимита';


CREATE UNIQUE INDEX UDL_LMTCASHSTOCK_EXCH_IDX0 ON udl_lmtcashstock_exch_dbt (T_RECID);
/

CREATE SEQUENCE udl_lmtcashstock_exch_SEQ_1
  START WITH 1
  MAXVALUE 9999999999999999999999999999
  MINVALUE 1
  NOCYCLE;



CREATE OR REPLACE TRIGGER udl_lmtcashstock_exch_t0_ainc
 BEFORE INSERT OR UPDATE OF T_RECID ON udl_lmtcashstock_exch_dbt FOR EACH ROW
DECLARE
 v_id INTEGER;
BEGIN
 IF (:new.T_RECID = 0 OR :new.T_RECID IS NULL) THEN
  SELECT udl_lmtcashstock_exch_SEQ_1.nextval INTO :new.T_RECID FROM dual;
 ELSE
  SELECT last_number INTO v_id FROM user_sequences WHERE sequence_name = UPPER('udl_lmtcashstock_exch_SEQ_1');

  IF :new.T_RECID >= v_id THEN
   RAISE DUP_VAL_ON_INDEX;
  END IF;

 END IF;
END;
/
---------------------------------------------------------------------------------------

CREATE TABLE udl_lmtsecuritest_exch_dbt
(                                                                                    
  T_RECID               NUMBER(10),
  T_LIMIT_TYPE          VARCHAR2(10),    --'DEPO'
  T_FIRM_ID             VARCHAR2(12),    --DDL_LIMITSECURITES_DBT.T_FIRM_ID
  T_SECCODE             VARCHAR2(35),    --DDL_LIMITSECURITES_DBT.T_SECCODE
  T_CLIENT_CODE         VARCHAR2(35),    --DDL_LIMITSECURITES_DBT.T_CLIENT_CODE
  T_OPEN_BALANCE        NUMBER(32,12),   --DDL_LIMITSECURITES_DBT.T_OPEN_BALANCE
  T_OPEN_LIMIT          NUMBER(32,12),   --DDL_LIMITSECURITES_DBT.T_OPEN_LIMIT
  T_CURRENT_LIMIT       NUMBER(32,12),   --DDL_LIMITSECURITES_DBT.T_CURRENT_LIMIT
  T_TRDACCID            VARCHAR2(25),    --DDL_LIMITSECURITES_DBT.T_TRDACCID
  T_WA_POSITION_PRICE   NUMBER(32,12),   --DDL_LIMITSECURITES_DBT.T_WA_POSITION_PRICE
  T_LIMIT_KIND          NUMBER(5)        --DDL_LIMITSECURITES_DBT.T_LIMIT_KIND
);



COMMENT ON TABLE udl_lmtsecuritest_exch_dbt IS 'буфер обмена для таблицы DDL_LIMITSECURITES_DBT Лимиты ценных бумаг';

COMMENT ON COLUMN udl_lmtsecuritest_exch_dbt.T_RECID IS 'ИД записи автоинкремент';        

COMMENT ON COLUMN udl_lmtsecuritest_exch_dbt.T_LIMIT_TYPE IS 'Тип лимита';

COMMENT ON COLUMN udl_lmtsecuritest_exch_dbt.T_FIRM_ID IS 'Код участника торгов';

COMMENT ON COLUMN udl_lmtsecuritest_exch_dbt.T_SECCODE IS 'Код вида "Код на ММВБ" для ц/б';
                                                                                     
COMMENT ON COLUMN udl_lmtsecuritest_exch_dbt.T_CLIENT_CODE IS 'Код клиента';

COMMENT ON COLUMN udl_lmtsecuritest_exch_dbt.T_OPEN_BALANCE IS 'Входящий остаток';

COMMENT ON COLUMN udl_lmtsecuritest_exch_dbt.T_OPEN_LIMIT IS 'Входящий лимит';

COMMENT ON COLUMN udl_lmtsecuritest_exch_dbt.T_CURRENT_LIMIT IS 'Текущий лимит';

COMMENT ON COLUMN udl_lmtsecuritest_exch_dbt.T_TRDACCID IS 'Примечание субдоговора "Счет клиента на ММВБ Фондовый сектор"';

COMMENT ON COLUMN udl_lmtsecuritest_exch_dbt.T_WA_POSITION_PRICE IS 'Средняя цена приобретения';

COMMENT ON COLUMN udl_lmtsecuritest_exch_dbt.T_LIMIT_KIND IS 'Вид лимита';


CREATE UNIQUE INDEX UDL_LMTSECURITEST_EXCH_IDX0 ON udl_lmtsecuritest_exch_dbt (T_RECID);
/

CREATE SEQUENCE udl_lmtsecuritest_exch_SEQ_1
  START WITH 1
  MAXVALUE 9999999999999999999999999999
  MINVALUE 1
  NOCYCLE;



CREATE OR REPLACE TRIGGER udl_lmtsecuritest_exch_t0_ainc
 BEFORE INSERT OR UPDATE OF T_RECID ON udl_lmtsecuritest_exch_dbt FOR EACH ROW
DECLARE
 v_id INTEGER;
BEGIN
 IF (:new.T_RECID = 0 OR :new.T_RECID IS NULL) THEN
  SELECT udl_lmtcashstock_exch_SEQ_1.nextval INTO :new.T_RECID FROM dual;
 ELSE
  SELECT last_number INTO v_id FROM user_sequences WHERE sequence_name = UPPER('udl_lmtsecuritest_exch_SEQ_1');

  IF :new.T_RECID >= v_id THEN
   RAISE DUP_VAL_ON_INDEX;
  END IF;

 END IF;
END;
/
---------------------------------------------------------------------------------------

CREATE TABLE udl_lmtfuturmark_exch_dbt
(                                                                                    
  T_RECID               NUMBER(10),
  T_CLASS_CODE          VARCHAR2(10),    --DDL_LIMITFUTURMARK_DBT.T_CLASS_CODE
  T_ACCOUNT             VARCHAR2(25),    --DDL_LIMITFUTURMARK_DBT.T_ACCOUNT
  T_VOLUMEMN            NUMBER(32,12),   --DDL_LIMITFUTURMARK_DBT.T_VOLUMEMN
  T_VOLUMEPL            NUMBER(32,12),   --DDL_LIMITFUTURMARK_DBT.T_VOLUMEPL
  T_KFL                 FLOAT(53),       --DDL_LIMITFUTURMARK_DBT.T_KFL
  T_KGO                 FLOAT(53),       --DDL_LIMITFUTURMARK_DBT.T_KGO
  T_USE_KGO             VARCHAR2(3),     --DDL_LIMITFUTURMARK_DBT.T_USE_KGO
  T_FIRM_ID             VARCHAR2(12),    --DDL_LIMITFUTURMARK_DBT.T_FIRM_ID
  T_SECCODE             VARCHAR2(35)     --DDL_LIMITFUTURMARK_DBT.T_SECCODE
);



COMMENT ON TABLE udl_lmtfuturmark_exch_dbt IS 'буфер обмена для таблицы DDL_LIMITFUTURMARK_DBT Лимиты срочного рынка';

COMMENT ON COLUMN udl_lmtfuturmark_exch_dbt.T_RECID IS 'ИД записи автоинкремент';        

COMMENT ON COLUMN udl_lmtfuturmark_exch_dbt.T_CLASS_CODE IS 'Код класса инструмента';

COMMENT ON COLUMN udl_lmtfuturmark_exch_dbt.T_ACCOUNT IS 'Торговый счет. Примечание субдоговора вида "Счет клиента на ММВБ Срочный рынок"';

COMMENT ON COLUMN udl_lmtfuturmark_exch_dbt.T_VOLUMEMN IS 'Лимит открытых позиций для типа "Денежные средства" или "Всего"';

COMMENT ON COLUMN udl_lmtfuturmark_exch_dbt.T_VOLUMEPL IS 'Лимит открытых позиций для типа "Залоговые денежные средства"';

COMMENT ON COLUMN udl_lmtfuturmark_exch_dbt.T_KFL IS 'Коэффициент ликвидности';

COMMENT ON COLUMN udl_lmtfuturmark_exch_dbt.T_KGO IS 'Примечания субдоговора "Коэффициент гарантийного обеспечения"';

COMMENT ON COLUMN udl_lmtfuturmark_exch_dbt.T_USE_KGO IS 'Флаг загрузки KGO';

COMMENT ON COLUMN udl_lmtfuturmark_exch_dbt.T_FIRM_ID IS 'Код участника торгов';

COMMENT ON COLUMN udl_lmtfuturmark_exch_dbt.T_SECCODE IS 'Код вида "Код на ММВБ" для ц/б';
                                                                                     

CREATE UNIQUE INDEX UDL_LMTFUTURMARK_EXCH_IDX0 ON udl_lmtfuturmark_exch_dbt (T_RECID);
/

CREATE SEQUENCE udl_lmtfuturmark_exch_SEQ_1
  START WITH 1
  MAXVALUE 9999999999999999999999999999
  MINVALUE 1
  NOCYCLE;



CREATE OR REPLACE TRIGGER udl_lmtfuturmark_exch_t0_ainc
 BEFORE INSERT OR UPDATE OF T_RECID ON udl_lmtfuturmark_exch_dbt FOR EACH ROW
DECLARE
 v_id INTEGER;
BEGIN
 IF (:new.T_RECID = 0 OR :new.T_RECID IS NULL) THEN
  SELECT udl_lmtfuturmark_exch_SEQ_1.nextval INTO :new.T_RECID FROM dual;
 ELSE
  SELECT last_number INTO v_id FROM user_sequences WHERE sequence_name = UPPER('udl_lmtfuturmark_exch_SEQ_1');

  IF :new.T_RECID >= v_id THEN
   RAISE DUP_VAL_ON_INDEX;
  END IF;

 END IF;
END;
/
---------------------------------------------------------------------------------------
CREATE TABLE udl_lmt_cshstck_scrt_cmp_dbt
(                                                                                    
  T_RECID               NUMBER(10),
  T_LIMIT_TYPE          VARCHAR2(10),    --'MONEY' 'DEPO'
  T_FIRM_ID             VARCHAR2(12),    --DDL_LIMITCASHSTOCK_DBT.T_FIRM_ID            DDL_LIMITSECURITES_DBT.T_FIRM_ID
  T_TAG                 VARCHAR2(5),     --DDL_LIMITCASHSTOCK_DBT.T_TAG
  T_CURR_CODE           VARCHAR2(3),     --DDL_LIMITCASHSTOCK_DBT.T_CURR_CODE
  T_CLIENT_CODE         VARCHAR2(35),    --DDL_LIMITCASHSTOCK_DBT.T_CLIENT_CODE        DDL_LIMITSECURITES_DBT.T_CLIENT_CODE
  T_OPEN_BALANCE        NUMBER(32,12),   --DDL_LIMITCASHSTOCK_DBT.T_OPEN_BALANCE       DDL_LIMITSECURITES_DBT.T_OPEN_BALANCE
  T_OPEN_LIMIT          NUMBER(32,12),   --DDL_LIMITCASHSTOCK_DBT.T_OPEN_LIMIT         DDL_LIMITSECURITES_DBT.T_OPEN_LIMIT
  T_CURRENT_LIMIT       NUMBER(32,12),   --DDL_LIMITCASHSTOCK_DBT.T_CURRENT_LIMIT      DDL_LIMITSECURITES_DBT.T_CURRENT_LIMIT
  T_LEVERAGE            NUMBER(32,12),   --DDL_LIMITCASHSTOCK_DBT.T_LEVERAGE
  T_LIMIT_KIND          NUMBER(5),       --DDL_LIMITCASHSTOCK_DBT.T_LIMIT_KIND         DDL_LIMITSECURITES_DBT.T_LIMIT_KIND
  T_TRDACCID            VARCHAR2(25),    --DDL_LIMITSECURITES_DBT.T_TRDACCID
  T_WA_POSITION_PRICE   NUMBER(32,12)    --DDL_LIMITSECURITES_DBT.T_WA_POSITION_PRICE
);



COMMENT ON TABLE udl_lmt_cshstck_scrt_cmp_dbt IS 'Лимиты для сверки денежных средств фондового рынка и ценных бумаг';

COMMENT ON COLUMN udl_lmt_cshstck_scrt_cmp_dbt.T_RECID IS 'ИД записи автоинкремент';        

COMMENT ON COLUMN udl_lmt_cshstck_scrt_cmp_dbt.T_LIMIT_TYPE IS 'Тип лимита';

COMMENT ON COLUMN udl_lmt_cshstck_scrt_cmp_dbt.T_FIRM_ID IS 'Код участника торгов';

COMMENT ON COLUMN udl_lmt_cshstck_scrt_cmp_dbt.T_TAG IS 'Группа';
                                                                                     
COMMENT ON COLUMN udl_lmt_cshstck_scrt_cmp_dbt.T_CURR_CODE IS 'Код валюты счета';

COMMENT ON COLUMN udl_lmt_cshstck_scrt_cmp_dbt.T_CLIENT_CODE IS 'Код клиента';

COMMENT ON COLUMN udl_lmt_cshstck_scrt_cmp_dbt.T_OPEN_BALANCE IS 'Входящий остаток';

COMMENT ON COLUMN udl_lmt_cshstck_scrt_cmp_dbt.T_OPEN_LIMIT IS 'Входящий лимит';

COMMENT ON COLUMN udl_lmt_cshstck_scrt_cmp_dbt.T_CURRENT_LIMIT IS 'Текущий лимит';

COMMENT ON COLUMN udl_lmt_cshstck_scrt_cmp_dbt.T_LEVERAGE IS 'Плечо';

COMMENT ON COLUMN udl_lmt_cshstck_scrt_cmp_dbt.T_LIMIT_KIND IS 'Вид лимита';

COMMENT ON COLUMN udl_lmt_cshstck_scrt_cmp_dbt.T_TRDACCID IS 'Примечание субдоговора "Счет клиента на ММВБ Фондовый сектор"';

COMMENT ON COLUMN udl_lmt_cshstck_scrt_cmp_dbt.T_WA_POSITION_PRICE IS 'Средняя цена приобретения';


CREATE UNIQUE INDEX UDL_LMT_CSHSTCK_SCRT_CMP_IDX0 ON udl_lmt_cshstck_scrt_cmp_dbt (T_RECID);
/

CREATE SEQUENCE udl_lmt_cshstck_scrt_cmp_SEQ_1
  START WITH 1
  MAXVALUE 9999999999999999999999999999
  MINVALUE 1
  NOCYCLE;



CREATE OR REPLACE TRIGGER udl_lmtcshstck_scrtcmp_t0_ainc
 BEFORE INSERT OR UPDATE OF T_RECID ON udl_lmt_cshstck_scrt_cmp_dbt FOR EACH ROW
DECLARE
 v_id INTEGER;
BEGIN
 IF (:new.T_RECID = 0 OR :new.T_RECID IS NULL) THEN
  SELECT udl_lmt_cshstck_scrt_cmp_SEQ_1.nextval INTO :new.T_RECID FROM dual;
 ELSE
  SELECT last_number INTO v_id FROM user_sequences WHERE sequence_name = UPPER('udl_lmt_cshstck_scrt_cmp_SEQ_1');

  IF :new.T_RECID >= v_id THEN
   RAISE DUP_VAL_ON_INDEX;
  END IF;

 END IF;
END;
/

---------------------------------------------------------------------------------------
CREATE TABLE udl_lmtfuturmark_cmp_dbt
(                                                                                    
  T_RECID               NUMBER(10),
  T_CLASS_CODE          VARCHAR2(10),    --DDL_LIMITFUTURMARK_DBT.T_CLASS_CODE
  T_ACCOUNT             VARCHAR2(25),    --DDL_LIMITFUTURMARK_DBT.T_ACCOUNT
  T_VOLUMEMN            NUMBER(32,12),   --DDL_LIMITFUTURMARK_DBT.T_VOLUMEMN
  T_VOLUMEPL            NUMBER(32,12),   --DDL_LIMITFUTURMARK_DBT.T_VOLUMEPL
  T_KFL                 FLOAT(53),       --DDL_LIMITFUTURMARK_DBT.T_KFL
  T_KGO                 FLOAT(53),       --DDL_LIMITFUTURMARK_DBT.T_KGO
  T_USE_KGO             VARCHAR2(3),     --DDL_LIMITFUTURMARK_DBT.T_USE_KGO
  T_FIRM_ID             VARCHAR2(12),    --DDL_LIMITFUTURMARK_DBT.T_FIRM_ID
  T_SECCODE             VARCHAR2(35)     --DDL_LIMITFUTURMARK_DBT.T_SECCODE
);



COMMENT ON TABLE udl_lmtfuturmark_cmp_dbt IS 'буфер обмена для таблицы DDL_LIMITFUTURMARK_DBT Лимиты срочного рынка';

COMMENT ON COLUMN udl_lmtfuturmark_cmp_dbt.T_RECID IS 'ИД записи автоинкремент';        

COMMENT ON COLUMN udl_lmtfuturmark_cmp_dbt.T_CLASS_CODE IS 'Код класса инструмента';

COMMENT ON COLUMN udl_lmtfuturmark_cmp_dbt.T_ACCOUNT IS 'Торговый счет. Примечание субдоговора вида "Счет клиента на ММВБ Срочный рынок"';

COMMENT ON COLUMN udl_lmtfuturmark_cmp_dbt.T_VOLUMEMN IS 'Лимит открытых позиций для типа "Денежные средства" или "Всего"';

COMMENT ON COLUMN udl_lmtfuturmark_cmp_dbt.T_VOLUMEPL IS 'Лимит открытых позиций для типа "Залоговые денежные средства"';

COMMENT ON COLUMN udl_lmtfuturmark_cmp_dbt.T_KFL IS 'Коэффициент ликвидности';

COMMENT ON COLUMN udl_lmtfuturmark_cmp_dbt.T_KGO IS 'Примечания субдоговора "Коэффициент гарантийного обеспечения"';

COMMENT ON COLUMN udl_lmtfuturmark_cmp_dbt.T_USE_KGO IS 'Флаг загрузки KGO';

COMMENT ON COLUMN udl_lmtfuturmark_cmp_dbt.T_FIRM_ID IS 'Код участника торгов';

COMMENT ON COLUMN udl_lmtfuturmark_cmp_dbt.T_SECCODE IS 'Код вида "Код на ММВБ" для ц/б';
                                                                                     

CREATE UNIQUE INDEX UDL_LMTFUTURMARK_CMP_IDX0 ON udl_lmtfuturmark_cmp_dbt (T_RECID);
/

CREATE SEQUENCE udl_lmtfuturmark_cmp_SEQ_1
  START WITH 1
  MAXVALUE 9999999999999999999999999999
  MINVALUE 1
  NOCYCLE;



CREATE OR REPLACE TRIGGER udl_lmtfuturmark_cmp_t0_ainc
 BEFORE INSERT OR UPDATE OF T_RECID ON udl_lmtfuturmark_cmp_dbt FOR EACH ROW
DECLARE
 v_id INTEGER;
BEGIN
 IF (:new.T_RECID = 0 OR :new.T_RECID IS NULL) THEN
  SELECT udl_lmtfuturmark_cmp_SEQ_1.nextval INTO :new.T_RECID FROM dual;
 ELSE
  SELECT last_number INTO v_id FROM user_sequences WHERE sequence_name = UPPER('udl_lmtfuturmark_cmp_SEQ_1');

  IF :new.T_RECID >= v_id THEN
   RAISE DUP_VAL_ON_INDEX;
  END IF;

 END IF;
END;
/
---------------------------------------------------------------------------------------

CREATE TABLE udl_dl_lmtadjust_exch_dbt
(                                                                                    
  T_LIMIT_TYPE             VARCHAR2(10),     --'MONEY' 'DEPO' DDL_LIMITADJUST_DBT.T_LIMIT_TYPE
  T_LIMIT_ID               NUMBER(10),       --DDL_LIMITADJUST_DBT.T_LIMIT_ID
  T_FIRM_ID                VARCHAR2(12),     --DDL_LIMITADJUST_DBT.T_FIRM_ID
  T_CLIENT_CODE            VARCHAR2(35),     --DDL_LIMITADJUST_DBT.T_CLIENT_CODE
  T_OPEN_BALANCE           NUMBER(32,12),    --DDL_LIMITADJUST_DBT.T_OPEN_BALANCE
  T_OPEN_LIMIT             NUMBER(32,12),    --DDL_LIMITADJUST_DBT.T_OPEN_LIMIT
  T_CURRENT_BALANCE        NUMBER(32,12),    --???!!!Соответствие отсутствует
  T_CURRENT_LIMIT          NUMBER(32,12),    --DDL_LIMITADJUST_DBT.T_CURRENT_LIMIT
  T_LIMIT_OPERATION        VARCHAR2(15),     --DDL_LIMITADJUST_DBT.T_LIMIT_OPERATION
  T_TRDACCID               VARCHAR2(25),     --DDL_LIMITADJUST_DBT.T_TRDACCID
  T_SECCODE                VARCHAR2(35),     --DDL_LIMITADJUST_DBT.T_SECCODE
  T_TAG                    VARCHAR2(5),      --DDL_LIMITADJUST_DBT.T_TAG
  T_CURR_CODE              VARCHAR2(3),      --DDL_LIMITADJUST_DBT.T_CURR_CODE
  T_LIMIT_KIND             NUMBER(5),        --DDL_LIMITADJUST_DBT.T_LIMIT_KIND
  T_LEVERAGE               NUMBER(32,12),    --DDL_LIMITADJUST_DBT.T_LEVERAGE
  T_WA_POSITION_PRICE      NUMBER(32,12)     --???!!!Соответствие отсутствует
);



COMMENT ON TABLE udl_dl_lmtadjust_exch_dbt IS 'буфер обмена для таблицы DDL_LIMITADJUST_DBT Корректировка лимитов срочного рынка';

COMMENT ON COLUMN udl_dl_lmtadjust_exch_dbt.T_LIMIT_TYPE IS 'Тип лимита';        

COMMENT ON COLUMN udl_dl_lmtadjust_exch_dbt.T_LIMIT_ID IS 'Идентификатор из табл. лимита срочного рынка DDL_LIMITFUTURMARK_DBT';

COMMENT ON COLUMN udl_dl_lmtadjust_exch_dbt.T_FIRM_ID IS 'Код участника торгов';

COMMENT ON COLUMN udl_dl_lmtadjust_exch_dbt.T_CLIENT_CODE IS 'Код клиента';

COMMENT ON COLUMN udl_dl_lmtadjust_exch_dbt.T_OPEN_BALANCE IS 'Изменение Входящий остаток';

COMMENT ON COLUMN udl_dl_lmtadjust_exch_dbt.T_OPEN_LIMIT IS 'Изменение Входящий лимит';

COMMENT ON COLUMN udl_dl_lmtadjust_exch_dbt.T_CURRENT_BALANCE IS 'Изменение Открытый лимит';

COMMENT ON COLUMN udl_dl_lmtadjust_exch_dbt.T_CURRENT_LIMIT IS 'Изменение Текущий лимит';

COMMENT ON COLUMN udl_dl_lmtadjust_exch_dbt.T_LIMIT_OPERATION IS 'Способ коррекции лимита';

COMMENT ON COLUMN udl_dl_lmtadjust_exch_dbt.T_TRDACCID IS 'Номер счета депо';

COMMENT ON COLUMN udl_dl_lmtadjust_exch_dbt.T_SECCODE IS 'Код инструмента';

COMMENT ON COLUMN udl_dl_lmtadjust_exch_dbt.T_TAG IS 'Группа';

COMMENT ON COLUMN udl_dl_lmtadjust_exch_dbt.T_CURR_CODE IS 'Код валюты счета';

COMMENT ON COLUMN udl_dl_lmtadjust_exch_dbt.T_LIMIT_KIND IS 'Вид лимита';

COMMENT ON COLUMN udl_dl_lmtadjust_exch_dbt.T_LEVERAGE IS 'Плечо';

COMMENT ON COLUMN udl_dl_lmtadjust_exch_dbt.T_WA_POSITION_PRICE IS 'Средняя цена приобретения';
/                                                                                     

CREATE INDEX UDL_DL_LMTADJUST_EXCH_IDX0 ON udl_dl_lmtadjust_exch_dbt (T_LIMIT_ID);

---------------------------------------------------------------------------------------


CREATE GLOBAL TEMPORARY TABLE ulob_txt_tmp
(                                                                                    
 T_FILENAME   VARCHAR2(25),
 T_FILE       CLOB
)
ON COMMIT PRESERVE ROWS;


COMMENT ON TABLE ulob_txt_tmp IS 'файлы обмена .lci, .lco, .lim, .fli';
/
---------------------------------------------------------------------------------------


CREATE TABLE uTableProcessIn_dbt
(
  T_RECID           NUMBER(10)                    NOT NULL,
  T_OBJECTTYPE      NUMBER(10)                    NOT NULL,
  T_STATUS          NUMBER(10)                    NOT NULL,
  T_TIMESTAMP       DATE,
  T_RESULTCODE      VARCHAR2(50 CHAR),
  T_RESULTTEXT      VARCHAR2(1000 CHAR)
);

COMMENT ON TABLE uTableProcessIn_dbt IS 'Входящий поток СОФР';

COMMENT ON COLUMN uTableProcessIn_dbt.T_RECID IS 'ИД записи';

COMMENT ON COLUMN uTableProcessIn_dbt.T_OBJECTTYPE IS 'Тип потока данных';

COMMENT ON COLUMN uTableProcessIn_dbt.T_STATUS IS 'Текущий статуc: 1 = формируется 2 = готов к обработке 3 = обрабатывается 4 = архив';

COMMENT ON COLUMN uTableProcessIn_dbt.T_TIMESTAMP IS 'Дата, время  последнего изменения статуса';

COMMENT ON COLUMN uTableProcessIn_dbt.T_RESULTCODE IS 'Код результата';

COMMENT ON COLUMN uTableProcessIn_dbt.T_RESULTTEXT IS 'Поясняющий текст';

CREATE UNIQUE INDEX uTableProcessIn_IDX0 ON uTableProcessIn_dbt (T_RECID);
/

CREATE SEQUENCE uTableProcessIn_SEQ_1
  START WITH 1
  MAXVALUE 999999999999999999999999999
  MINVALUE 1
  NOCYCLE
  NOCACHE
  NOORDER;



CREATE OR REPLACE TRIGGER uTableProcessIn_T0_AINC
 BEFORE INSERT OR UPDATE OF T_RECID ON uTableProcessIn_dbt FOR EACH ROW
DECLARE
    v_id INTEGER;
BEGIN
    IF (:new.T_RECID = 0 OR :new.T_RECID IS NULL)
    THEN
        SELECT uTableProcessIn_SEQ_1.nextval INTO :new.T_RECID FROM dual;
    ELSE
        SELECT last_number INTO v_id FROM user_sequences WHERE sequence_name = UPPER ('uTableProcessIn_SEQ_1');
            IF :new.T_RECID >= v_id
            THEN
                RAISE DUP_VAL_ON_INDEX;
            END IF;
    END IF;
END;
/


CREATE TABLE uTableProcessOut_dbt
(
  T_RECID           NUMBER(10)                    NOT NULL,
  T_OBJECTTYPE      NUMBER(10)                    NOT NULL,
  T_STATUS          NUMBER(10)                    NOT NULL,
  T_TIMESTAMP       DATE,
  T_RESULTCODE      VARCHAR2(50 CHAR),
  T_RESULTTEXT      VARCHAR2(1000 CHAR)
);

COMMENT ON TABLE uTableProcessOut_dbt IS 'Исходящий поток СОФР';

COMMENT ON COLUMN uTableProcessOut_dbt.T_RECID IS 'ИД записи';

COMMENT ON COLUMN uTableProcessOut_dbt.T_OBJECTTYPE IS 'Тип потока данных';

COMMENT ON COLUMN uTableProcessOut_dbt.T_STATUS IS 'Текущий статуc: 1 = формируется 2 = готов к обработке 3 = обрабатывается 4 = архив';

COMMENT ON COLUMN uTableProcessOut_dbt.T_TIMESTAMP IS 'Дата, время  последнего изменения статуса';

COMMENT ON COLUMN uTableProcessOut_dbt.T_RESULTCODE IS 'Код результата';

COMMENT ON COLUMN uTableProcessOut_dbt.T_RESULTTEXT IS 'Поясняющий текст';

CREATE UNIQUE INDEX uTableProcessOut_IDX0 ON uTableProcessOut_dbt (T_RECID);
/

CREATE SEQUENCE uTableProcessOut_SEQ_1
  START WITH 1
  MAXVALUE 999999999999999999999999999
  MINVALUE 1
  NOCYCLE
  NOCACHE
  NOORDER;



CREATE OR REPLACE TRIGGER uTableProcessOut_T0_AINC
 BEFORE INSERT OR UPDATE OF T_RECID ON uTableProcessOut_dbt FOR EACH ROW
DECLARE
    v_id INTEGER;
BEGIN
    IF (:new.T_RECID = 0 OR :new.T_RECID IS NULL)
    THEN
        SELECT uTableProcessOut_SEQ_1.nextval INTO :new.T_RECID FROM dual;
    ELSE
        SELECT last_number INTO v_id FROM user_sequences WHERE sequence_name = UPPER ('uTableProcessOut_SEQ_1');
            IF :new.T_RECID >= v_id
            THEN
                RAISE DUP_VAL_ON_INDEX;
            END IF;
    END IF;
END;
/




CREATE TABLE uTableProcessEvent_dbt
(
  T_RECID           NUMBER(10)                    NOT NULL,
  T_TIMESTAMP       DATE,
  T_OBJECTTYPE      NUMBER(10)                    NOT NULL,
  T_OBJECTID        NUMBER(10)                    NOT NULL,
  T_TYPE            NUMBER(10),
  T_STATUS          NUMBER(10)                    NOT NULL,
  T_NOTE            VARCHAR2(1000),
  T_MESSAGEID       NUMBER(10),
  T_RESULTCODE      VARCHAR2(50 CHAR),
  T_RESULTTEXT      VARCHAR2(1000 CHAR)
);

COMMENT ON TABLE uTableProcessEvent_dbt IS 'События СОФР';

COMMENT ON COLUMN uTableProcessEvent_dbt.T_RECID IS 'ИД записи';

COMMENT ON COLUMN uTableProcessEvent_dbt.T_TIMESTAMP IS 'Дата, время фиксации События';

COMMENT ON COLUMN uTableProcessEvent_dbt.T_OBJECTTYPE IS 'Тип Объекта учета';

COMMENT ON COLUMN uTableProcessEvent_dbt.T_OBJECTID IS 'ИД Объекта';

COMMENT ON COLUMN uTableProcessEvent_dbt.T_TYPE IS 'Тип События';

COMMENT ON COLUMN uTableProcessEvent_dbt.T_STATUS IS 'Текущий статуc: 1 = готов к обработке 2 = обрабатывается 3 = архив';

COMMENT ON COLUMN uTableProcessEvent_dbt.T_NOTE IS 'Дополнительный параметр';

COMMENT ON COLUMN uTableProcessEvent_dbt.T_MESSAGEID IS 'ИД сообщения';

COMMENT ON COLUMN uTableProcessEvent_dbt.T_RESULTCODE IS 'Код результата';

COMMENT ON COLUMN uTableProcessEvent_dbt.T_RESULTTEXT IS 'Поясняющий текст';

CREATE UNIQUE INDEX uTableProcessEvent_IDX0 ON uTableProcessEvent_dbt (T_RECID);
/

CREATE SEQUENCE uTableProcessEvent_SEQ_1
  START WITH 1
  MAXVALUE 999999999999999999999999999
  MINVALUE 1
  NOCYCLE
  NOCACHE
  NOORDER;



CREATE OR REPLACE TRIGGER uTableProcessEvent_T0_AINC
 BEFORE INSERT OR UPDATE OF T_RECID ON uTableProcessEvent_dbt FOR EACH ROW
DECLARE
    v_id INTEGER;
BEGIN
    IF (:new.T_RECID = 0 OR :new.T_RECID IS NULL)
    THEN
        SELECT uTableProcessEvent_SEQ_1.nextval INTO :new.T_RECID FROM dual;
    ELSE
        SELECT last_number INTO v_id FROM user_sequences WHERE sequence_name = UPPER ('uTableProcessEvent_SEQ_1');
            IF :new.T_RECID >= v_id
            THEN
                RAISE DUP_VAL_ON_INDEX;
            END IF;
    END IF;
END;
/