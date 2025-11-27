-- Изменения по CCBO-11001 (партиционирование DOPROPER_DBT и DOPRDOCS_DBT)
DECLARE
  logID VARCHAR2(32) := 'CCBO-11001';
  x_Cnt NUMBER;
  x_Stat NUMBER := 0;

  -- Записать сообщение в itt_log
  PROCEDURE LogIt(p_message IN VARCHAR2)
  AS
  BEGIN
    it_log.log(p_msg => logID||': '||p_message);
  END;
  -- создание таблицы DTRANSFERCONTROL_DBT
  PROCEDURE createDTransferControlDbt ( p_Stat IN OUT number )
  IS
  BEGIN
    IF( p_Stat = 1 ) THEN
      RETURN;
    END IF;

    LogIt('Создание таблицы DTRANSFERCONTROL_DBT');
    SELECT count(*) INTO x_Cnt FROM user_tables WHERE table_name = upper('DTRANSFERCONTROL_DBT');
    IF (x_Cnt = 1) THEN
       LogIt('Существует таблица DTRANSFERCONTROL_DBT');
    ELSE
       EXECUTE IMMEDIATE 'CREATE TABLE DTRANSFERCONTROL_DBT
            (
               T_PARTITIONNAME VARCHAR2(64) NOT NULL ENABLE
               , T_HIGHVALUE DATE 
               , T_STATUS CHAR(1) DEFAULT ''R''
               , t_CompressFlag NUMBER DEFAULT 0
               , t_started DATE DEFAULT null
               , t_ended DATE DEFAULT null
            )'
       ;
       LogIt('Создана таблица DTRANSFERCONTROL_DBT');
    END IF;
  END;
  -- партиционирование таблицы DOPROPER_DBT (ее можно партиционировать ONLINE)
  PROCEDURE modifyDOPROPER_DBT ( p_Stat IN OUT number )
  IS
  BEGIN
    IF( p_Stat = 1 ) THEN
      RETURN;
    END IF;

    LogIt('партиционирование таблицы DOPROPER_DBT');
    SELECT count(*) INTO x_Cnt FROM USER_TAB_PARTITIONS WHERE TABLE_NAME = upper('DOPROPER_DBT');
    IF (x_Cnt > 0) THEN
       LogIt('Таблица DOPROPER_DBT уже партиционирована');
    ELSE
       EXECUTE IMMEDIATE 'ALTER TABLE doproper_dbt MODIFY
             PARTITION BY RANGE (T_END_DATE) INTERVAL (NUMTOYMINTERVAL(1, ''MONTH'' ))
            (
                PARTITION p2017_12 VALUES LESS THAN (TO_DATE( ''01-01-2018'', ''DD-MM-YYYY'' )) 
            )
            ONLINE
            ENABLE ROW MOVEMENT
       ';
       LogIt('Произведено партиционирование таблицы DOPROPER_DBT');
    END IF;
  END;
  -- заполнение таблицы DTRANSFERCONTROL_DBT
  PROCEDURE fillDTransferControlDbt ( p_Stat IN OUT number )
  IS
    x_HighValue DATE;
    x_TableName VARCHAR2(32) := 'DOPROPER_DBT';
    x_Rows NUMBER := 0;
  BEGIN
    IF( p_Stat = 1 ) THEN
      RETURN;
    END IF;

    LogIt('заполнение таблицы DTRANSFERCONTROL_DBT');
    EXECUTE IMMEDIATE 'DELETE FROM dtransfercontrol_dbt '; 
    FOR i IN (SELECT PARTITION_NAME, HIGH_VALUE FROM USER_TAB_PARTITIONS WHERE TABLE_NAME = x_TableName) LOOP
      EXECUTE IMMEDIATE 'BEGIN :ret := ' || i.HIGH_VALUE || '; END;' USING OUT x_HighValue;
      EXECUTE IMMEDIATE 'INSERT INTO dtransfercontrol_dbt (t_partitionname, t_highvalue) VALUES (:partition_name, :x_HighValue) ' 
        USING i.partition_name, x_HighValue;
      x_Rows := x_Rows + 1;
    END LOOP;
    LogIt('Произведено заполнение таблицы DTRANSFERCONTROL_DBT, x_Rows: '||x_Rows);
  END;
  -- создание таблицы-дублера для DOPRDOCS_DBT (партиционированного)
  PROCEDURE createDOPRDOCS1_DBT ( p_Stat IN OUT number )
  IS
  BEGIN
    IF( p_Stat = 1 ) THEN
      RETURN;
    END IF;

    LogIt('Создание таблицы DOPRDOCS1_DBT');
    SELECT count(*) INTO x_Cnt FROM user_tables WHERE table_name = upper('DOPRDOCS1_DBT');
    IF (x_Cnt = 1) THEN
       LogIt('Существует таблица DOPRDOCS1_DBT');
    ELSE
       EXECUTE IMMEDIATE 'CREATE TABLE DOPRDOCS1_DBT
            (
              T_DOCKIND NUMBER(5,0)
              , T_DOCUMENTID VARCHAR2(34) NOT NULL ENABLE
              , T_ID_OPERATION NUMBER(10,0) NOT NULL ENABLE
              , T_ID_STEP NUMBER(5,0)
              , T_PART NUMBER(5,0)
              , T_STATUS NUMBER(5,0)
              , T_ORIGIN NUMBER(5,0) NOT NULL ENABLE
              , T_SERVDOCKIND NUMBER(5,0)
              , T_SERVDOCID NUMBER(10,0)
              , T_AUTOKEY NUMBER(19,0) NOT NULL ENABLE
              , T_LAUNCHOPER CHAR(1)
              , T_ACCTRNID NUMBER(10,0)
              , T_FMTBLOBDATA_XXXX BLOB
              , CONSTRAINT "PK_OPRDOCS1" PRIMARY KEY ("T_AUTOKEY")            
            )  '
       ;
       EXECUTE IMMEDIATE q'[COMMENT ON TABLE DOPRDOCS1_DBT IS 'Данные для отката шага (партиционированные)']';
       EXECUTE IMMEDIATE q'[COMMENT ON COLUMN DOPRDOCS1_DBT.T_DOCKIND               	IS 'Вид первичного документа']';
       EXECUTE IMMEDIATE q'[COMMENT ON COLUMN DOPRDOCS1_DBT.T_DOCUMENTID            	IS 'Идентификатор документа']';
       EXECUTE IMMEDIATE q'[COMMENT ON COLUMN DOPRDOCS1_DBT.T_ID_OPERATION		IS 'Идентификатор операции']';
       EXECUTE IMMEDIATE q'[COMMENT ON COLUMN DOPRDOCS1_DBT.T_ID_STEP               	IS 'Идентификатор шага']';
       EXECUTE IMMEDIATE q'[COMMENT ON COLUMN DOPRDOCS1_DBT.T_PART               	IS 'Блок']';
       EXECUTE IMMEDIATE q'[COMMENT ON COLUMN DOPRDOCS1_DBT.T_STATUS               	IS 'Статус']';
       EXECUTE IMMEDIATE q'[COMMENT ON COLUMN DOPRDOCS1_DBT.T_ORIGIN               	IS 'Происхождение']';
       EXECUTE IMMEDIATE q'[COMMENT ON COLUMN DOPRDOCS1_DBT.T_SERVDOCKIND               IS 'Вид док сервисной операции']';
       EXECUTE IMMEDIATE q'[COMMENT ON COLUMN DOPRDOCS1_DBT.T_SERVDOCID               	IS 'Идентификатор документа сервисной операции']';
       EXECUTE IMMEDIATE q'[COMMENT ON COLUMN DOPRDOCS1_DBT.T_AUTOKEY               	IS 'Ключ']';
       EXECUTE IMMEDIATE q'[COMMENT ON COLUMN DOPRDOCS1_DBT.T_LAUNCHOPER               	IS 'Запустить операцию']';
       EXECUTE IMMEDIATE q'[COMMENT ON COLUMN DOPRDOCS1_DBT.T_FMTBLOBDATA_XXXX		IS 'Сохраненные данные']';
       EXECUTE IMMEDIATE 'ALTER TABLE doprdocs1_dbt ADD CONSTRAINT XFK_oprdocs1_ACCTRN FOREIGN KEY (T_ACCTRNID) REFERENCES dacctrn_dbt (T_ACCTRNID)';
       EXECUTE IMMEDIATE 'ALTER TABLE doprdocs1_dbt ADD CONSTRAINT XFK_oprdocs1_OPROPER FOREIGN KEY (t_ID_OPERATION) REFERENCES doproper_dbt (T_ID_OPERATION)';
       EXECUTE IMMEDIATE 'ALTER TABLE doprdocs1_dbt ADD CONSTRAINT XFK_oprdocs1_OPRSTEP FOREIGN KEY (t_ID_OPERATION, t_ID_step) REFERENCES doprstep_dbt (t_ID_OPERATION, t_ID_step)';
       EXECUTE IMMEDIATE 'ALTER TABLE doprdocs1_dbt MODIFY PARTITION BY REFERENCE (XFK_oprdocs1_OPROPER) ENABLE ROW MOVEMENT';
       LogIt('Создана таблица DOPRDOCS1_DBT');
    END IF;
  END;
  -- создание индекса
  PROCEDURE createIndex ( 
     p_Stat IN OUT number
     , p_Unique IN varchar2
     , p_TableName IN varchar2
     , p_IndexName IN varchar2
     , p_Fields IN varchar2 
     , p_TableSpace IN varchar2 DEFAULT 'USERS'  -- в INDX нет места
     , p_Local IN varchar2 DEFAULT ''  -- для партиционированной таблицы можно указать LOCAL
  )
  IS
  BEGIN
    IF( p_Stat = 1 ) THEN
      RETURN;
    END IF;

    LogIt('Проверка индекса '||p_IndexName);
    SELECT count(*) INTO x_Cnt FROM user_indexes i WHERE i.INDEX_NAME = p_IndexName ;
    IF (x_Cnt = 1) THEN
       LogIt('Индекс '||p_IndexName||' существует');
       LogIt('Удаление индекса '||p_IndexName);
       EXECUTE IMMEDIATE 'DROP INDEX '||p_IndexName;
       LogIt('Удален индекс: '||p_IndexName);
    END IF;
    LogIt('Создание индекса '||p_IndexName);
    EXECUTE IMMEDIATE 'CREATE '||p_Unique||' INDEX '||p_IndexName
       ||' ON '||p_TableName||' ('||p_Fields||') '
       ||p_Local||' TABLESPACE '||p_TableSpace
    ;
    LogIt('Создан индекс: '||p_IndexName);
  END;
  -- создание тригера
  PROCEDURE createTrigger ( 
     p_Stat IN OUT number
     , p_TableName IN varchar2
     , p_TrgName IN varchar2
     , p_ID IN varchar2
     , p_SeqName IN varchar2
  )
  IS
    x_Str VARCHAR2(32000);
    cr VARCHAR2(2) := CHR(10);
  BEGIN
    IF( p_Stat = 1 ) THEN
      RETURN;
    END IF;

    LogIt('Создание тригера '||p_TrgName);
    x_Str := 'CREATE OR REPLACE TRIGGER '||p_TrgName
         ||cr||'BEFORE INSERT OR UPDATE OF '||p_ID||' ON '||p_TableName||' FOR EACH ROW '
         ||cr||'DECLARE '
         ||cr||'v_id INTEGER; '
         ||cr||'BEGIN '
         ||cr||'IF (:NEW.'||p_ID||' = 0 OR :NEW.'||p_ID||' IS NULL) THEN '
         ||cr||'SELECT '||p_SeqName||'.NEXTVAL INTO :NEW.'||p_ID||' FROM DUAL; '
         ||cr||'ELSE '
         ||cr||'SELECT LAST_NUMBER INTO v_id FROM USER_SEQUENCES WHERE UPPER(SEQUENCE_NAME) = UPPER('''||p_SeqName||'''); '
         ||cr||'IF :NEW.'||p_ID||' >= v_id THEN '
         ||cr||'RAISE DUP_VAL_ON_INDEX; '
         ||cr||'END IF; '
         ||cr||'END IF; '
         ||cr||'END;';
    EXECUTE IMMEDIATE x_Str;
    LogIt('Создан тригер '||p_TrgName);
  EXCEPTION
   WHEN OTHERS THEN 
      LogIt('Ошибка создания тригера '||p_TrgName);
      p_Stat := 1;
  END;
  -- создание индексов партиционированного дублера DOPRDOCS_DBT
  PROCEDURE createDOPRDOCS1_IDX ( p_Stat IN OUT number )
  IS
  BEGIN
    IF( p_Stat = 1 ) THEN
      RETURN;
    END IF;

    createIndex ( p_Stat, 'UNIQUE', 'DOPRDOCS1_DBT', 'DOPRDOCS1_DBT_IDX1', 'T_ID_OPERATION, T_ID_STEP, T_AUTOKEY', 'USERS', '' );
    createIndex ( p_Stat, '', 'DOPRDOCS1_DBT', 'DOPRDOCS1_DBT_IDX2', 'T_SERVDOCKIND, T_SERVDOCID, T_ID_OPERATION, T_ID_STEP, T_AUTOKEY', 'USERS', '' );
    createIndex ( p_Stat, '', 'DOPRDOCS1_DBT', 'DOPRDOCS1_DBT_IDX5', 'T_DOCKIND, T_DOCUMENTID', 'USERS', '' );
  EXCEPTION
   WHEN OTHERS THEN 
      p_Stat := 1;
  END;
  -- создание триггера DOPRDOCS1_DBT_RSBT_001
  PROCEDURE createTrigger_DOPRDOCS1_DBT_RSBT_001 ( p_Stat IN OUT number )
  IS
    x_Str VARCHAR2(32000);
    x_TrgName VARCHAR2(32) := 'DOPRDOCS1_DBT_RSBT_001';
    cr VARCHAR2(2) := CHR(10);  -- перевод строки
    ct VARCHAR2(2) := CHR(9);	-- табуляция
  BEGIN
    IF( p_Stat = 1 ) THEN
      RETURN;
    END IF;

    LogIt('Создание тригера '||x_TrgName);
    x_Str := 'CREATE OR REPLACE TRIGGER '||x_TrgName
        ||cr||'BEFORE INSERT ON DOPRDOCS1_DBT '
	||cr||'FOR EACH ROW '
	||cr||'DECLARE '
	||cr||'BEGIN '
	||cr
  	||cr||ct||'IF (:new.t_ID_Step = 0) THEN '
    	||cr||ct||ct||':new.t_ID_Step := NULL; '
  	||cr||ct||'END IF;'
	||cr
  	||cr||ct||'IF (:new.t_AccTrnID = 0) THEN '
    	||cr||ct||ct||':new.t_AccTrnID := NULL; '
  	||cr||ct||'END IF;'
	||cr
        ||cr||'END '||x_TrgName||';';
    EXECUTE IMMEDIATE x_Str;
    LogIt('Создан тригер '||x_TrgName);
  END;
  -- создание триггеров партиционированного дублера DOPRDOCS_DBT
  PROCEDURE createDOPRDOCS1_TRIGGERS ( p_Stat IN OUT number )
  IS
  BEGIN
    IF( p_Stat = 1 ) THEN
      RETURN;
    END IF;

    createTrigger ( p_Stat, 'DOPRDOCS1', 'DOPRDOCS1_DBT_T3_AINC', 't_autokey', 'doprdocs_dbt_SEQ');
    createTrigger_DOPRDOCS1_DBT_RSBT_001( p_Stat );
  EXCEPTION
   WHEN OTHERS THEN 
      p_Stat := 1;
  END;
BEGIN

  createDTransferControlDbt ( x_Stat );		-- 1) создание таблицы DTRANSFERCONTROL_DBT
  modifyDOPROPER_DBT ( x_Stat ); 		-- 2) партиционирование таблицы DOPROPER_DBT
  fillDTransferControlDbt ( x_Stat );		-- 3) заполнение таблицы DTRANSFERCONTROL_DBT
  createDOPRDOCS1_DBT ( x_Stat );		-- 4) создание таблицы-дублера для DOPRDOCS_DBT (партиционированного)
  createDOPRDOCS1_IDX ( x_Stat );		-- 5) создание индексов партиционированного дублера DOPRDOCS_DBT
  createDOPRDOCS1_TRIGGERS ( x_Stat );		-- 6) создание триггеров партиционированного дублера DOPRDOCS_DBT
END;
/
