-- Таблица "DCNVPRIOR_DBT"

declare
    vcnt number;
begin
   select count(*) into vcnt from user_tables where upper(table_name) = 'DIMPOPRDU_TMP';
   if vcnt = 0 then
      EXECUTE IMMEDIATE 'CREATE GLOBAL TEMPORARY TABLE DIMPOPRDU_TMP (
        T_ID NUMBER(10)
      , T_NAMEAVR VARCHAR2 (50)
      , T_ISIN VARCHAR2 (25)
      , T_DealDate DATE
      , T_TransferDate DATE
      , T_PFI NUMBER(10) DEFAULT -1
      , T_TransferNom NUMBER (32,12)
      , T_ExportDUNom NUMBER (32,12)
      , T_AmountDU NUMBER(10)
      , T_PriceNom NUMBER(32,12)
      , T_PricePortfolioTransfer NUMBER(32,12)
      , T_Sum NUMBER(32,12)
      , T_PricePortfolioExportDU NUMBER(32,12)
      , T_SumOther NUMBER(32,12)
      , T_NKD NUMBER(32,12)
      , T_FIID NUMBER(10) DEFAULT -1
      , T_DEALCODE VARCHAR2 (30) DEFAULT chr(0)
      , T_ISO VARCHAR2 (3)
      , T_DealID NUMBER(10) DEFAULT 0
      ) ON COMMIT PRESERVE ROWS';
    EXECUTE IMMEDIATE 'COMMENT ON TABLE DIMPOPRDU_TMP IS ''Данные для загрузки операций вывода из ДУ ''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN DIMPOPRDU_TMP.T_NAMEAVR IS ''Наименование ЦБ''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN DIMPOPRDU_TMP.T_ISIN IS ''ISIN''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN DIMPOPRDU_TMP.T_DealDate IS ''Дата заключения сделки на приобретение ЦБ''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN DIMPOPRDU_TMP.T_TransferDate IS ''Дата перехода права собственности при приобретении ЦБ''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN DIMPOPRDU_TMP.T_PFI IS ''Код валюты номинала''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN DIMPOPRDU_TMP.T_TransferNom IS ''Номинал на дату перехода права собственности при приобретении ЦБ''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN DIMPOPRDU_TMP.T_ExportDUNom IS ''Номинал на дату вывода ЦБ из ДУ''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN DIMPOPRDU_TMP.T_AmountDU IS ''Количество ЦБ, выведенных из ДУ, шт.''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN DIMPOPRDU_TMP.T_PriceNom IS ''Цена приобретения, % от номинала''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN DIMPOPRDU_TMP.T_PricePortfolioTransfer IS ''Стоимость портфеля ЦБ по налоговому учету - на дату перехода права соственности при приобретении ЦБ, в валюте номинала''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN DIMPOPRDU_TMP.T_Sum IS ''Сумма расходов, учтенная при амортизации долга на дату вывода ЦБ из ДУ, в валюте номинала''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN DIMPOPRDU_TMP.T_PricePortfolioExportDU IS ''Стоимость портфеля ЦБ по налоговому учету - на дату вывода ЦБ из ДУ, в валюте номинала''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN DIMPOPRDU_TMP.T_SumOther IS ''Сумма прочих расходов, уплаченных при приобретении ЦБ, не учтенная на дату вывода ЦБ из ДУ, руб.''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN DIMPOPRDU_TMP.T_NKD IS ''НКД на дату вывода ЦБ из ДУ, в валюте номинала''';
   end if;
end;
/

declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where upper(i.TABLE_NAME)='DIMPOPRDU_TMP' and upper(i.INDEX_NAME)='DIMPOPRDU_TMP_IDX0' ;
  if cnt =1 then
    execute immediate 'drop index DIMPOPRDU_TMP_IDX0';
  end if;
  execute immediate 'CREATE UNIQUE INDEX DIMPOPRDU_TMP_IDX0 ON DIMPOPRDU_TMP (T_ID)';
end;
/

declare
 cnt number;
begin
  select count(*) into cnt from user_sequences i where (i.SEQUENCE_NAME)='DIMPOPRDU_TMP_SEQ';
  if cnt =0 then
     execute immediate '
        CREATE SEQUENCE DIMPOPRDU_TMP_SEQ
          START WITH 1
          MAXVALUE 9999999999999999999999999999
          MINVALUE 1
          NOCYCLE
          NOCACHE
          NOORDER
          NOKEEP
          NOSCALE
          GLOBAL
     ';
  end if;
end;
/

declare
 cnt number;
begin
     execute immediate q'[
        CREATE OR REPLACE TRIGGER DIMPOPRDU_TMP_T0_AINC 
         BEFORE INSERT ON DIMPOPRDU_TMP FOR EACH ROW
        DECLARE
         v_id INTEGER;
        BEGIN
         IF (:new.t_id = 0 OR :new.t_id IS NULL) THEN
         SELECT DIMPOPRDU_TMP_seq.nextval INTO :new.t_id FROM dual;
         ELSE
         select last_number into v_id from user_sequences where sequence_name = upper ('DIMPOPRDU_TMP_SEQ');
         IF :new.t_id >= v_id THEN
         RAISE DUP_VAL_ON_INDEX;
         END IF;
         END IF;
        END;
     ]';
end;
/
