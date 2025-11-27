DECLARE
   ex_table_exists EXCEPTION;
   PRAGMA EXCEPTION_INIT(ex_table_exists, -955);
BEGIN
  EXECUTE IMMEDIATE 'CREATE TABLE DNPTXSLICEBASE_DBT
                            (
                              T_TBSID                NUMBER(10),
                              T_SLICEID              NUMBER(10),
                              T_TBID                 NUMBER(10),
                              T_CLIENTID             NUMBER(10),
                              T_TYPE                 NUMBER(10),
                              T_DESCRIPTION          VARCHAR2(100 BYTE),
                              T_INCREGIONDATE        DATE,
                              T_INCDATE              DATE,
                              T_INCTIME              DATE,
                              T_CONFIRMSTATE         NUMBER(5),
                              T_SOURCESYSTEM         NUMBER(5),
                              T_STORSTATE            NUMBER(5),
                              T_DOCKIND              NUMBER(5),
                              T_DOCID                NUMBER(10),
                              T_TAXPERIOD            NUMBER(5),
                              T_TAXBASEKIND          NUMBER(10),
                              T_TAXBASECURRPAY       NUMBER(32,12),
                              T_CALCPITAX            NUMBER(32,12),
                              T_RATECALCPITAX        NUMBER(5),
                              T_HOLDPITAX            NUMBER(32,12),
                              T_RATEHOLDPITAX        NUMBER(5),
                              T_BCCCALCPITAX         VARCHAR2(20 BYTE),
                              T_BCCHOLDPITAX         VARCHAR2(20 BYTE),
                              T_TAXPAYERSTATUS       NUMBER(5),
                              T_APPLSTAXBASEINCLUDE  NUMBER(32,12),
                              T_APPLSTAXBASEEXCLUDE  NUMBER(32,12),
                              T_RECSTAXBASE          NUMBER(32,12),
                              T_RECSTAXBASEDATE      DATE,
                              T_ORIGTBID             NUMBER(10)             DEFAULT 0,
                              T_STORID               VARCHAR2(50 BYTE)      DEFAULT 0,
                              T_INSTANCE             NUMBER(5)              DEFAULT 0,
                              T_ID_OPERATION         NUMBER(10)             DEFAULT 0,
                              T_ID_STEP              NUMBER(5)              DEFAULT 0,
                              T_NEEDRECALC           VARCHAR2(50), 
                              T_RECTAXBASEBYKIND     NUMBER(32,12), 
                              T_INITIAL_DOCKIND      NUMBER(5), 
                              T_INITIAL_DOCID        NUMBER(10)
                            )';
                                  
  EXECUTE IMMEDIATE 'COMMENT ON TABLE DNPTXSLICEBASE_DBT IS ''События СНОБ''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN DNPTXSLICEBASE_DBT.T_TBID IS ''Идентификатор записи''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN DNPTXSLICEBASE_DBT.T_TYPE IS ''Тип события''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN DNPTXSLICEBASE_DBT.T_DESCRIPTION IS ''Описание события''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN DNPTXSLICEBASE_DBT.T_INCREGIONDATE IS ''Дата получения дохода (регион)''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN DNPTXSLICEBASE_DBT.T_INCDATE IS ''Дата получения дохода''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN DNPTXSLICEBASE_DBT.T_INCTIME IS ''Время получения дохода''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN DNPTXSLICEBASE_DBT.T_CONFIRMSTATE IS ''Статус подтверждения записи в Хранилище''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN DNPTXSLICEBASE_DBT.T_SOURCESYSTEM IS ''СИ (система источник)''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN DNPTXSLICEBASE_DBT.T_STORSTATE IS ''Статус записи в Хранилище''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN DNPTXSLICEBASE_DBT.T_DOCKIND IS ''Вид первичного документа, породившего запись''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN DNPTXSLICEBASE_DBT.T_DOCID IS ''Идентификатор документа, породившего запись''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN DNPTXSLICEBASE_DBT.T_TAXPERIOD IS ''Налоговый период (год)''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN DNPTXSLICEBASE_DBT.T_TAXBASEKIND IS ''НОБ''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN DNPTXSLICEBASE_DBT.T_TAXBASECURRPAY IS ''НОБ по текущей выплате''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN DNPTXSLICEBASE_DBT.T_CALCPITAX IS ''НДФЛ исчисленный''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN DNPTXSLICEBASE_DBT.T_RATECALCPITAX IS ''Налоговая ставка НДФЛ исчисленный''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN DNPTXSLICEBASE_DBT.T_HOLDPITAX IS ''НДФЛ удержанный''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN DNPTXSLICEBASE_DBT.T_RATEHOLDPITAX IS ''Налоговая ставка  НДФЛ удержанный''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN DNPTXSLICEBASE_DBT.T_BCCCALCPITAX IS ''КБК исчисленного НДФЛ''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN DNPTXSLICEBASE_DBT.T_BCCHOLDPITAX IS ''КБК удержанного НДФЛ''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN DNPTXSLICEBASE_DBT.T_TAXPAYERSTATUS IS ''Статус налогоплательщика ФЛ''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN DNPTXSLICEBASE_DBT.T_APPLSTAXBASEINCLUDE IS ''Применена СНОБ с учетом НОБ текущей выплаты''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN DNPTXSLICEBASE_DBT.T_RECSTAXBASE IS ''Полученный СНОБ''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN DNPTXSLICEBASE_DBT.T_RECSTAXBASEDATE IS ''Дата получения СНОБ''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN DNPTXSLICEBASE_DBT.T_ORIGTBID IS ''Идентификатор исходного события''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN DNPTXSLICEBASE_DBT.T_STORID IS ''Идентификатор в хранилище''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN DNPTXSLICEBASE_DBT.T_INSTANCE IS ''Инстанс''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN DNPTXSLICEBASE_DBT.T_ID_OPERATION IS ''Идентификатор системной операции''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN DNPTXSLICEBASE_DBT.T_ID_STEP IS ''Идентификатор шага''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN DNPTXSLICEBASE_DBT.T_NEEDRECALC IS ''Сообщение необходимости пересчета''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN DNPTXSLICEBASE_DBT.T_RECTAXBASEBYKIND IS ''Полученный НОБ по виду дохода''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN DNPTXSLICEBASE_DBT.T_INITIAL_DOCKIND IS ''Вид родительского первичного документа, к которому относится запись''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN DNPTXSLICEBASE_DBT.T_INITIAL_DOCID IS ''Идентификатор родительского первичного документа, к которому относится запись''';

  BEGIN
    EXECUTE IMMEDIATE 'DROP INDEX DNPTXSLICEBASE_DBT_IDX0';
  EXCEPTION
    WHEN OTHERS THEN NULL;
  END;
  EXECUTE IMMEDIATE 'CREATE UNIQUE INDEX DNPTXSLICEBASE_DBT_IDX0 ON  DNPTXSLICEBASE_DBT (T_TBSID)';
  
  BEGIN
    EXECUTE IMMEDIATE 'DROP INDEX DNPTXSLICEBASE_DBT_IDX1';
  EXCEPTION
    WHEN OTHERS THEN NULL;
  END;
  EXECUTE IMMEDIATE 'CREATE INDEX DNPTXSLICEBASE_DBT_IDX1 ON  DNPTXSLICEBASE_DBT (T_SLICEID)';

  BEGIN
    EXECUTE IMMEDIATE 'DROP INDEX DNPTXSLICEBASE_DBT_IDX2';
  EXCEPTION
    WHEN OTHERS THEN NULL;
  END;
  EXECUTE IMMEDIATE 'CREATE INDEX DNPTXSLICEBASE_DBT_IDX2 ON  DNPTXSLICEBASE_DBT (T_INCDATE, T_TYPE, T_TAXPERIOD, T_STORSTATE, T_SLICEID)';

  BEGIN
    EXECUTE IMMEDIATE 'DROP INDEX DNPTXSLICEBASE_DBT_DBT_IDX3';
  EXCEPTION
    WHEN OTHERS THEN NULL;
  END;
  EXECUTE IMMEDIATE 'CREATE INDEX DNPTXSLICEBASE_DBT_DBT_IDX3 ON  DNPTXSLICEBASE_DBT (T_INCDATE)';

  BEGIN
    EXECUTE IMMEDIATE 'DROP SEQUENCE DNPTXSLICEBASE_DBT_SEQ';
  EXCEPTION
    WHEN OTHERS THEN NULL;
  END;
  EXECUTE IMMEDIATE 'CREATE SEQUENCE  DNPTXSLICEBASE_DBT_SEQ
          START WITH 1
          MAXVALUE 9999999999999999999999999999
          MINVALUE 1
          NOCYCLE
          CACHE 20
          NOORDER
          NOKEEP
          NOSCALE
          GLOBAL';

EXCEPTION
   WHEN ex_table_exists
   THEN NULL;
END;
/

CREATE OR REPLACE TRIGGER  DNPTXSLICEBASE_DBT_t0_ainc
 BEFORE INSERT OR UPDATE OF T_TBSID ON  DNPTXSLICEBASE_DBT FOR EACH ROW
DECLARE
 v_id INTEGER;
BEGIN
 IF (:new.T_TBSID = 0 OR :new.T_TBSID IS NULL) THEN
 SELECT DNPTXSLICEBASE_DBT_SEQ.nextval INTO :new.T_TBSID FROM dual;
 ELSE
 select last_number into v_id from user_sequences where sequence_name = upper ('DNPTXSLICEBASE_DBT_SEQ');
 IF :new.T_TBSID >= v_id THEN
 RAISE DUP_VAL_ON_INDEX;
 END IF;
 END IF;
END;
/
