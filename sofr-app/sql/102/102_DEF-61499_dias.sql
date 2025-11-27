-- Изменения по DEF-61499 (удаление дублей из sofr_diasaccdepofull и sofr_diasdeporestfull)
DECLARE
  logID VARCHAR2(32) := 'DEF-61499';
  x_Cnt NUMBER;
  x_Stat NUMBER := 0;

  -- Записать сообщение в itt_log
  PROCEDURE LogIt(p_message IN VARCHAR2)
  AS
  BEGIN
    it_log.log(p_msg => logID||': '||p_message);
  END;
  -- создание таблицы DDIASACCDEPO_DBT
  PROCEDURE createDDiasAccDepo ( p_Stat IN OUT number )
  IS
  BEGIN
    IF( p_Stat = 1 ) THEN
      RETURN;
    END IF;

    LogIt('Создание таблицы DDIASACCDEPO_DBT');
    SELECT count(*) INTO x_Cnt FROM user_tables WHERE table_name = upper('DDIASACCDEPO_DBT');
    IF (x_Cnt = 1) THEN
       LogIt('Существует таблица DDIASACCDEPO_DBT');
    ELSE
       EXECUTE IMMEDIATE 'CREATE TABLE DDIASACCDEPO_DBT 
            (
               T_SOFRACCID NUMBER(*,0) NOT NULL ENABLE
               , T_DEPONUMBER VARCHAR2(32)
               , T_ACCDEPONUMBER VARCHAR2(16)
               , T_SECTIONCODE VARCHAR2(32) NOT NULL ENABLE
               , T_CONTRACTNUMBER VARCHAR2(32) NOT NULL ENABLE
               , T_TIMESTAMP DATE
               , T_LASTID NUMBER(*,0) NOT NULL ENABLE
            )'
       ;
       LogIt('Создана таблица DDIASACCDEPO_DBT');
    END IF;
  EXCEPTION
   WHEN OTHERS THEN 
      LogIt('Ошибка создания таблицы DDIASACCDEPO_DBT');
      EXECUTE IMMEDIATE 'ROLLBACK';
      p_Stat := 1;
  END;
  -- заполнение таблицы DDIASACCDEPO_DBT
  PROCEDURE fillDDiasAccDepo ( p_Stat IN OUT number )
  IS
  BEGIN
    IF( p_Stat = 1 ) THEN
      RETURN;
    END IF;

    LogIt('Первоначальное заполнение таблицы DDIASACCDEPO_DBT');
    EXECUTE IMMEDIATE 'INSERT INTO DDIASACCDEPO_DBT (
      t_sofraccid, t_sectioncode, t_contractnumber, t_timestamp, t_lastid
    )
      SELECT min(recid)
        , trim(sectioncode) AS t_sectioncode
        , trim(contractnumber) AS t_contractnumber
        , max(t_timestamp) AS t_timestamp
        , max(recid) AS t_lastid
      FROM 
        sofr_diasaccdepofull 
      GROUP BY
        trim(contractnumber), trim(sectioncode)'
    ;
    EXECUTE IMMEDIATE 'COMMIT';
    LogIt('Произведено первоначальное заполнение таблицы DDIASACCDEPO_DBT');
  EXCEPTION
   WHEN OTHERS THEN 
      LogIt('Ошибка первоначального заполнения таблицы DDIASACCDEPO_DBT');
      EXECUTE IMMEDIATE 'ROLLBACK';
      p_Stat := 1;
  END;
  -- заполнение deponumber и accdeponumber
  PROCEDURE fillAccDepoNumber ( p_Stat IN OUT number )
  IS
  BEGIN
    IF( p_Stat = 1 ) THEN
      RETURN;
    END IF;

    LogIt('заполнение deponumber и accdeponumber');
    EXECUTE IMMEDIATE 'UPDATE 
      DDIASACCDEPO_DBT r 
      SET (r.t_deponumber, r.t_accdeponumber) = (
         SELECT distinct trim(a.deponumber), trim(a.accdeponumber) FROM sofr_diasaccdepofull a WHERE a.recid = r.t_lastid 
         )';
    EXECUTE IMMEDIATE 'COMMIT';
    LogIt('Произведено заполнение deponumber и accdeponumber');
  EXCEPTION
   WHEN OTHERS THEN 
      LogIt('Ошибка заполнение deponumber и accdeponumber');
      EXECUTE IMMEDIATE 'ROLLBACK';
      p_Stat := 1;
  END;
  -- дозаполнение deponumber
  PROCEDURE fillDepoNumber ( p_Stat IN OUT number )
  IS
  BEGIN
    IF( p_Stat = 1 ) THEN
      RETURN;
    END IF;

    LogIt('дозаполнение deponumber');
    EXECUTE IMMEDIATE 'UPDATE 
      DDIASACCDEPO_DBT r 
      SET (r.t_deponumber) = (
         SELECT distinct trim(a.deponumber) FROM sofr_diasaccdepofull a WHERE a.recid = r.t_sofraccid 
         )
      WHERE r.t_deponumber IS null'
      ;
    EXECUTE IMMEDIATE 'COMMIT';
    LogIt('Произведено дозаполнение deponumber');
  EXCEPTION
   WHEN OTHERS THEN 
      LogIt('Ошибка дозаполнения deponumber');
      EXECUTE IMMEDIATE 'ROLLBACK';
      p_Stat := 1;
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
  EXCEPTION
   WHEN OTHERS THEN 
      LogIt('Ошибка создания индекса: '||p_IndexName);
      p_Stat := 1;
  END;
  -- создание последовательности
  PROCEDURE createSeq ( 
     p_Stat IN OUT number
     , p_SeqName IN varchar2
     , p_Start IN number DEFAULT 1
  )
  IS
  BEGIN
    IF( p_Stat = 1 ) THEN
      RETURN;
    END IF;

    LogIt('Создание последовательности '||p_SeqName);
    EXECUTE IMMEDIATE 'CREATE SEQUENCE '||p_SeqName
       ||' START WITH '||to_char(p_Start)
       ||' MAXVALUE 999999999999999999999999999 MINVALUE 1 NOCYCLE NOCACHE NOORDER'
    ;
    LogIt('Создана последовательность '||p_SeqName);
  EXCEPTION
   WHEN OTHERS THEN 
      LogIt('Ошибка создания последовательности '||p_SeqName);
      p_Stat := 1;
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
  -- создание индексов таблицы DDIASACCDEPO_DBT
  PROCEDURE createDDiasAccIdx ( p_Stat IN OUT number )
  IS
  BEGIN
    IF( p_Stat = 1 ) THEN
      RETURN;
    END IF;

    createIndex ( p_Stat, 'UNIQUE', 'DDIASACCDEPO_DBT', 'DDIASACCDEPO_IDX0', 't_sofraccid' );
    createIndex ( p_Stat, 'UNIQUE', 'DDIASACCDEPO_DBT', 'DDIASACCDEPO_IDX1', 't_contractnumber, t_sectioncode' );
  EXCEPTION
   WHEN OTHERS THEN 
      p_Stat := 1;
  END;
  -- создание таблицы DDIASACCMAP_DBT
  PROCEDURE createDDiasAccMap ( p_Stat IN OUT number )
  IS
  BEGIN
    IF( p_Stat = 1 ) THEN
      RETURN;
    END IF;

    LogIt('Создание таблицы DDIASACCMAP_DBT');
    SELECT count(*) INTO x_Cnt FROM user_tables WHERE table_name = upper('DDIASACCMAP_DBT');
    IF (x_Cnt = 1) THEN
       LogIt('Существует таблица DDIASACCMAP_DBT');
    ELSE
       EXECUTE IMMEDIATE 'CREATE TABLE DDIASACCMAP_DBT
            (
               T_SOFRACCID NUMBER(*,0) NOT NULL ENABLE
               , T_DIASACCID NUMBER(*,0) NOT NULL
            )'
       ;
       LogIt('Создана таблица DDIASACCMAP_DBT');
    END IF;
  EXCEPTION
   WHEN OTHERS THEN 
      LogIt('Ошибка создания таблицы DDIASACCMAP_DBT');
      p_Stat := 1;
  END;
  -- заполнение таблицы DDIASACCMAP_DBT (время работы -- 10 мин.)
  PROCEDURE fillDDiasAccMap ( p_Stat IN OUT number )
  IS
  BEGIN
    IF( p_Stat = 1 ) THEN
      RETURN;
    END IF;

    LogIt('Заполнение таблицы DDIASACCMAP_DBT');
    EXECUTE IMMEDIATE 'INSERT INTO DDIASACCMAP_DBT (
      t_sofraccid, t_diasaccid
    )
    SELECT b.t_sofraccid, r.T_diasaccID  
      FROM (
        SELECT r.recid AS t_diasaccID
          , trim(contractnumber) AS t_contractnumber
          , trim(sectioncode) AS t_sectioncode
        FROM sofr_diasaccdepofull r    
        GROUP BY r.recid, trim(contractnumber), trim(sectioncode)
        ) r    
       JOIN DDIASACCDEPO_DBT b ON (
          b.t_contractnumber = r.t_contractnumber AND b.t_sectioncode = r.t_sectioncode
        )';
    EXECUTE IMMEDIATE 'COMMIT';
    LogIt('Произведено заполнение таблицы DDIASACCMAP_DBT');
  EXCEPTION
   WHEN OTHERS THEN 
      LogIt('Ошибка заполнения таблицы DDIASACCMAP_DBT');
      EXECUTE IMMEDIATE 'ROLLBACK';
      p_Stat := 1;
  END;
  -- создание индексов таблицы DDIASACCMAP_DBT
  PROCEDURE createDDiasAccMapIdx ( p_Stat IN OUT number )
  IS
  BEGIN
    IF( p_Stat = 1 ) THEN
      RETURN;
    END IF;

    createIndex ( p_Stat, 'UNIQUE', 'DDIASACCMAP_DBT', 'DDIASACCMAP_DBT_IDX0', 't_diasaccid' );
    createIndex ( p_Stat, '', 'DDIASACCMAP_DBT', 'DDIASACCMAP_DBT_IDX1', 't_sofraccid' );
  EXCEPTION
   WHEN OTHERS THEN 
      p_Stat := 1;
  END;
  -- создание таблицы DDIASRESTDEPO_DBT (таблица остатков, будет партиционированной)
  PROCEDURE createDDiasRestDepo ( p_Stat IN OUT number )
  IS
  BEGIN
    IF( p_Stat = 1 ) THEN
      RETURN;
    END IF;

    LogIt('Создание таблицы DDIASRESTDEPO_DBT');
    SELECT count(*) INTO x_Cnt FROM user_tables WHERE table_name = upper('DDIASRESTDEPO_DBT');
    IF (x_Cnt = 1) THEN
       LogIt('Существует таблица DDIASRESTDEPO_DBT');
    ELSE
       EXECUTE IMMEDIATE 'CREATE TABLE DDIASRESTDEPO_DBT
            (
              RECID NUMBER(*,0) NOT NULL ENABLE
              , ACCDEPOID NUMBER(*,0) NOT NULL ENABLE
              , REPORTDATE DATE NOT NULL ENABLE
              , ISIN NUMBER(*,0) NOT NULL ENABLE
              , VALUE NUMBER(32,12) NOT NULL ENABLE
              , T_TIMESTAMP DATE
            ) 
             PARTITION BY RANGE (REPORTDATE) INTERVAL (NUMTOYMINTERVAL(1, ''MONTH'' ))
            (PARTITION p2021_12 VALUES LESS THAN (TO_DATE( ''01-01-2022'', ''DD-MM-YYYY'' )) 
            )'
       ;
       LogIt('Создана таблица DDIASRESTDEPO_DBT');
    END IF;
  EXCEPTION
   WHEN OTHERS THEN 
      LogIt('Ошибка создания таблицы DDIASRESTDEPO_DBT');
      p_Stat := 1;
  END;
  -- создание темперной таблицы DIAS_REST_TMP
  PROCEDURE createDDiasRestTmp ( p_Stat IN OUT number )
  IS
  BEGIN
    IF( p_Stat = 1 ) THEN
      RETURN;
    END IF;

    LogIt('Проверка таблицы DIAS_REST_TMP');
    SELECT count(*) INTO x_Cnt FROM user_tables WHERE table_name = upper('DIAS_REST_TMP');
    IF (x_Cnt = 1) THEN
       LogIt('Существует таблица DIAS_REST_TMP');
       LogIt('Удаление таблицы DIAS_REST_TMP');
       EXECUTE IMMEDIATE 'DROP TABLE DIAS_REST_TMP';
       LogIt('Удалена таблица DIAS_REST_TMP');
    END IF;

    LogIt('Создание таблицы DIAS_REST_TMP');
    EXECUTE IMMEDIATE 'CREATE GLOBAL TEMPORARY TABLE DIAS_REST_TMP
         ( 
           RECID NUMBER(*,0) NOT NULL ENABLE
           , ACCDEPOID NUMBER(*,0) NOT NULL ENABLE
           , REPORTDATE DATE NOT NULL ENABLE
           , ISIN VARCHAR2(50 CHAR) NOT NULL ENABLE
           , VALUE NUMBER(32,12) NOT NULL ENABLE
           , T_TIMESTAMP DATE
           , T_SOFRACCID NUMBER(*,0) NOT NULL ENABLE
         )
         ON COMMIT PRESERVE ROWS'
    ;
    LogIt('Создана таблица DIAS_REST_TMP');
  EXCEPTION
   WHEN OTHERS THEN 
      LogIt('Ошибка создания таблицы DIAS_REST_TMP');
      p_Stat := 1;
  END;
  -- создание индексов таблицы DDIASRESTDEPO_DBT
  PROCEDURE createDDiasAccRestIdx ( p_Stat IN OUT number )
  IS
  BEGIN
    IF( p_Stat = 1 ) THEN
      RETURN;
    END IF;

    createIndex ( p_Stat, '', 'DDIASRESTDEPO_DBT', 'DDIASRESTDEPO_DBT_IDX0', 'REPORTDATE', 'USERS', 'LOCAL' );
    createIndex ( p_Stat, '', 'DDIASRESTDEPO_DBT', 'DDIASRESTDEPO_DBT_IDX1', 'RECID', 'USERS', 'LOCAL' );
    createIndex ( p_Stat, '', 'DDIASRESTDEPO_DBT', 'DDIASRESTDEPO_DBT_IDX2', 'ACCDEPOID', 'USERS', 'LOCAL' );
    createIndex ( p_Stat, '', 'DDIASRESTDEPO_DBT', 'DDIASRESTDEPO_DBT_IDX3', 'ISIN', 'USERS', 'LOCAL' );
  EXCEPTION
   WHEN OTHERS THEN 
      p_Stat := 1;
  END;
  -- создание таблицы DDIASREPDATES_DBT
  PROCEDURE createDDiasDates ( p_Stat IN OUT number )
  IS
  BEGIN
    IF( p_Stat = 1 ) THEN
      RETURN;
    END IF;

    LogIt('Создание таблицы DDIASREPDATES_DBT');
    SELECT count(*) INTO x_Cnt FROM user_tables WHERE table_name = upper('DDIASREPDATES_DBT');
    IF (x_Cnt = 1) THEN
       LogIt('Существует таблица DDIASREPDATES_DBT');
    ELSE
       EXECUTE IMMEDIATE 'CREATE TABLE DDIASREPDATES_DBT
            (
               REPORTDATE DATE NOT NULL ENABLE
               , T_STATUS CHAR(1) DEFAULT ''R''
               , t_started DATE DEFAULT null
               , t_ended DATE DEFAULT null
            )'
       ;
       LogIt('Создана таблица DDIASREPDATES_DBT');
    END IF;
  EXCEPTION
   WHEN OTHERS THEN 
      LogIt('Ошибка создания таблицы DDIASREPDATES_DBT');
      p_Stat := 1;
  END;
  -- создание индексов таблицы DDIASREPDATES_DBT
  PROCEDURE createDDiasDatesIdx ( p_Stat IN OUT number )
  IS
  BEGIN
    IF( p_Stat = 1 ) THEN
      RETURN;
    END IF;

    createIndex ( p_Stat, 'UNIQUE', 'DDIASREPDATES_DBT', 'DDIASREPDATES_DBT_IDX0', 'REPORTDATE' );
    createIndex ( p_Stat, '', 'DDIASREPDATES_DBT', 'DDIASREPDATES_DBT_IDX1', 'T_STATUS' );
  EXCEPTION
   WHEN OTHERS THEN 
      p_Stat := 1;
  END;
  -- заполнение таблицы DDIASREPDATES_DBT
  PROCEDURE fillDDiasDates ( p_Stat IN OUT number )
  IS
  BEGIN
    IF( p_Stat = 1 ) THEN
      RETURN;
    END IF;

    LogIt('Заполнение таблицы DDIASREPDATES_DBT');
    EXECUTE IMMEDIATE 'INSERT INTO DDIASREPDATES_DBT 
       ( 
          reportdate 
       )
       SELECT r.reportdate 
       FROM sofr_diasdeporestfull r 
       GROUP BY r.reportdate
       ORDER BY r.reportdate DESC
       '
    ;
    EXECUTE IMMEDIATE 'COMMIT';
    LogIt('Произведено заполнение таблицы DDIASREPDATES_DBT');
  EXCEPTION
   WHEN OTHERS THEN 
      LogIt('Ошибка заполнения таблицы DDIASREPDATES_DBT');
      EXECUTE IMMEDIATE 'ROLLBACK';
      p_Stat := 1;
  END;
  -- создание таблицы DDIASISIN_DBT
  PROCEDURE createDDiasIsin ( p_Stat IN OUT number )
  IS
  BEGIN
    IF( p_Stat = 1 ) THEN
      RETURN;
    END IF;

    LogIt('Создание таблицы DDIASISIN_DBT');
    SELECT count(*) INTO x_Cnt FROM user_tables WHERE table_name = upper('DDIASISIN_DBT');
    IF (x_Cnt = 1) THEN
       LogIt('Существует таблица DDIASISIN_DBT');
    ELSE
       EXECUTE IMMEDIATE 'CREATE TABLE DDIASISIN_DBT
            (
               T_ID NUMBER(*,0) NOT NULL ENABLE
               , T_ISIN VARCHAR2(50 CHAR) NOT NULL ENABLE
            )'
       ;
       LogIt('Создана таблица DDIASISIN_DBT');
    END IF;
  EXCEPTION
   WHEN OTHERS THEN 
      LogIt('Ошибка создания таблицы DDIASISIN_DBT');
      p_Stat := 1;
  END;
  -- создание индексов таблицы DDIASISIN_DBT
  PROCEDURE createDDiasIsinIdx ( p_Stat IN OUT number )
  IS
  BEGIN
    IF( p_Stat = 1 ) THEN
      RETURN;
    END IF;

    createIndex ( p_Stat, 'UNIQUE', 'DDIASISIN_DBT', 'DDIASISIN_DBT_IDX0', 'T_ID' );
    createIndex ( p_Stat, 'UNIQUE', 'DDIASISIN_DBT', 'DDIASISIN_DBT_IDX1', 'T_ISIN' );
  EXCEPTION
   WHEN OTHERS THEN 
      p_Stat := 1;
  END;
  -- создание последовательности для T_ID таблицы DDIASISIN_DBT
  PROCEDURE createDDiasIsinSeq ( p_Stat IN OUT number )
  IS
  BEGIN
    IF( p_Stat = 1 ) THEN
      RETURN;
    END IF;

    createSeq ( p_Stat, 'DDIASISIN_DBT_SEQ' );
  EXCEPTION
   WHEN OTHERS THEN 
      p_Stat := 1;
  END;
  -- создание тригеров для таблицы DDIASISIN_DBT
  PROCEDURE createDDiasIsinTrigger ( p_Stat IN OUT number )
  IS
  BEGIN
    IF( p_Stat = 1 ) THEN
      RETURN;
    END IF;

    createTrigger ( p_Stat, 'DDIASISIN_DBT', 'DDIASISIN_DBT_T0_AINC', 'T_ID', 'DDIASISIN_DBT_SEQ');
  EXCEPTION
   WHEN OTHERS THEN 
      p_Stat := 1;
  END;
  -- заполнение таблицы DDIASISIN_DBT
  PROCEDURE fillDDiasIsin ( p_Stat IN OUT number )
  IS
  BEGIN
    IF( p_Stat = 1 ) THEN
      RETURN;
    END IF;

    LogIt('Заполнение таблицы DDIASISIN_DBT');
    EXECUTE IMMEDIATE 'INSERT INTO DDIASISIN_DBT
       ( 
          T_ISIN
       )
       SELECT r.isin 
       FROM sofr_diasdeporestfull r 
       GROUP BY r.isin
       '
    ;
    EXECUTE IMMEDIATE 'COMMIT';
    LogIt('Произведено заполнение таблицы DDIASISIN_DBT');
  EXCEPTION
   WHEN OTHERS THEN 
      LogIt('Ошибка заполнения таблицы DDIASISIN_DBT');
      EXECUTE IMMEDIATE 'ROLLBACK';
      p_Stat := 1;
  END;
  -- создание последовательности для T_SOFRACCID таблицы DDIASACCDEPO_DBT
  PROCEDURE createDDiasAccSeq ( p_Stat IN OUT number )
  IS
    x_SofrAccID NUMBER := 1;
  BEGIN
    IF( p_Stat = 1 ) THEN
      RETURN;
    END IF;

    -- последовательность начнется не с 1-цы, а с максимального значения для t_sofraccid
    BEGIN
       EXECUTE IMMEDIATE 'SELECT NVL(max(t_sofraccid),0)+1 FROM DDIASACCDEPO_DBT' INTO x_SofrAccID;
    EXCEPTION
       WHEN OTHERS THEN 
         NULL;
    END;

    createSeq ( p_Stat, 'DDIASACCDEPO_DBT_SEQ', x_SofrAccID );
  EXCEPTION
   WHEN OTHERS THEN 
      p_Stat := 1;
  END;
  -- создание тригера для таблицы DDIASACCDEPO_DBT
  PROCEDURE createDDiasAccTrigger ( p_Stat IN OUT number )
  IS
  BEGIN
    IF( p_Stat = 1 ) THEN
      RETURN;
    END IF;

    createTrigger ( p_Stat, 'DDIASACCDEPO_DBT', 'DDIASACCDEPO_DBT_T0_AINC', 'T_SOFRACCID', 'DDIASACCDEPO_DBT_SEQ');
  EXCEPTION
   WHEN OTHERS THEN 
      p_Stat := 1;
  END;
  -- изменение триггера SOFR_SVERKAACCDEPOIN_HIST_AIR
  PROCEDURE replaceTriggerAcc ( p_Stat IN OUT number )
  IS
    x_Str VARCHAR2(32000);
    x_TrgName VARCHAR2(32) := 'SOFR_SVERKAACCDEPOIN_HIST_AIR';
    cr VARCHAR2(2) := CHR(10);  -- перевод строки
    ct VARCHAR2(2) := CHR(9);	-- табуляция
  BEGIN
    IF( p_Stat = 1 ) THEN
      RETURN;
    END IF;

    LogIt('Создание тригера '||x_TrgName);
    x_Str := 'CREATE OR REPLACE TRIGGER '||x_TrgName
        ||cr||'AFTER INSERT ON SOFR_SVERKAACCDEPOIN '
	||cr||'REFERENCING NEW AS New OLD AS Old '
	||cr||'FOR EACH ROW '
	||cr||'DECLARE '
  	||cr||ct||'x_TimeStamp DATE; '
  	||cr||ct||'x_SofrAccID NUMBER; '
  	||cr||ct||'x_DiasAccID NUMBER := :new.RECID; '
  	||cr||ct||'x_DepoNumber DDIASACCDEPO_DBT.t_deponumber%type := trim(:new.DEPONUMBER); '
  	||cr||ct||'x_AccDpoNumber DDIASACCDEPO_DBT.t_accdeponumber%type := trim(:new.ACCDEPONUMBER); '
  	||cr||ct||'x_SectionCode DDIASACCDEPO_DBT.t_sectioncode%type := trim(:new.SECTIONCODE); '
  	||cr||ct||'x_ContractNumber DDIASACCDEPO_DBT.t_contractnumber%type := trim(:new.CONTRACTNUMBER); '
	||cr||'BEGIN '
  	||cr||ct||'SELECT SYSTIMESTAMP INTO x_TimeStamp FROM dual; '
  	||cr||ct||'-- пытаемся обновить timestamp таблицы счетов '
  	||cr||ct||'UPDATE DDIASACCDEPO_DBT r '
    	||cr||ct||ct||'SET r.t_timestamp = x_TimeStamp, r.t_lastid = x_DiasAccID '
    	||cr||ct||ct||'WHERE r.t_contractnumber = trim(x_ContractNumber) AND r.t_sectioncode = trim(x_SectionCode) '
    	||cr||ct||ct||'AND rownum = 1 '
    	||cr||ct||ct||'RETURNING r.t_sofraccid '
    	||cr||ct||ct||'INTO x_SofrAccID; '
  	||cr||ct||'-- если счета нет, добавляем '
  	||cr||ct||'IF( SQL%ROWCOUNT <> 1) THEN '
    	||cr||ct||ct||'INSERT INTO DDIASACCDEPO_DBT r ( '
      	||cr||ct||ct||ct||'r.t_deponumber, r.t_accdeponumber, r.t_sectioncode, r.t_contractnumber, r.t_timestamp, r.t_lastid '
    	||cr||ct||ct||') VALUES ( '
      	||cr||ct||ct||ct||'x_DepoNumber, x_AccDpoNumber, trim(x_SectionCode), trim(x_ContractNumber), x_TimeStamp, 0 '
    	||cr||ct||ct||') '
    	||cr||ct||ct||'RETURNING r.t_sofraccid '
    	||cr||ct||ct||'INTO x_SofrAccID; '
  	||cr||ct||'END IF; '
  	||cr||ct||'-- добавляем значение в таблицу маппинга '
  	||cr||ct||'INSERT INTO DDIASACCMAP_DBT r ( '
   	||cr||ct||ct||'r.t_sofraccid, r.t_diasaccid '
  	||cr||ct||') VALUES ( '
    	||cr||ct||ct||'x_SofrAccID, x_DiasAccID '
  	||cr||ct||'); '
	||cr||'EXCEPTION '
     	||cr||ct||'WHEN OTHERS THEN '
       	||cr||ct||ct||'it_error.put_error_in_stack; '
       	||cr||ct||ct||'it_log.log(p_msg => ''Error'', p_msg_type => it_log.c_msg_type__error); '
       	||cr||ct||ct||'it_error.clear_error_stack; '
       	||cr||ct||ct||'RAISE; '
        ||cr||'END;';
    EXECUTE IMMEDIATE x_Str;
    LogIt('Создан тригер '||x_TrgName);
  EXCEPTION
   WHEN OTHERS THEN 
      LogIt('Ошибка создания тригера '||x_TrgName);
      p_Stat := 1;
  END;
  -- изменение триггера SOFR_SVERKARESTDEPOIN_HIST_AIR
  PROCEDURE replaceTriggerRest ( p_Stat IN OUT number )
  IS
    x_Str VARCHAR2(32000);
    x_TrgName VARCHAR2(32) := 'SOFR_SVERKARESTDEPOIN_HIST_AIR';
    cr VARCHAR2(2) := CHR(10);  -- перевод строки
    ct VARCHAR2(2) := CHR(9);	-- табуляция
  BEGIN
    IF( p_Stat = 1 ) THEN
      RETURN;
    END IF;

    LogIt('Создание тригера '||x_TrgName);
    x_Str := 'CREATE OR REPLACE TRIGGER '||x_TrgName
        ||cr||'AFTER INSERT ON SOFR_SVERKARESTDEPOIN '
	||cr||'REFERENCING NEW AS New OLD AS Old '
	||cr||'FOR EACH ROW '
	||cr||'DECLARE '
  	||cr||ct||'x_TimeStamp DATE; '
  	||cr||ct||'x_IsinID DDIASISIN_DBT.t_id%type; '
  	||cr||ct||'x_Isin DDIASISIN_DBT.t_isin%type := trim(:new.ISIN); '
  	||cr||ct||'x_DiasAccID DDIASACCMAP_DBT.t_Diasaccid%type := :new.ACCDEPOID; '
  	||cr||ct||'x_SofrAccID DDIASACCMAP_DBT.T_SofrAccID%type := -1; '
  	||cr||ct||'x_RecID DDIASRESTDEPO_DBT.recID%type := :new.RECID; '
  	||cr||ct||'x_Value DDIASRESTDEPO_DBT.value%type := :new.VALUE; '
  	||cr||ct||'x_ReportDate DDIASRESTDEPO_DBT.reportdate%type := :new.REPORTDATE; '
	||cr||'BEGIN '
  	||cr||ct||'SELECT SYSTIMESTAMP INTO x_TimeStamp FROM dual; '
  	||cr||ct||'-- Анализируется ISIN. Полученное значение заменяется идентификатором. '
  	||cr||ct||'-- Если такого ISIN нет, производится добавление новой записи в таблицу. '
  	||cr||ct||'BEGIN '
    	||cr||ct||ct||'SELECT r.t_ID INTO x_IsinID FROM DDIASISIN_DBT r where r.t_isin = x_Isin; '
  	||cr||ct||'EXCEPTION '
     	||cr||ct||ct||'WHEN OTHERS THEN '
       	||cr||ct||ct||ct||'INSERT INTO DDIASISIN_DBT r ( t_isin ) VALUES ( x_Isin ) '
       	||cr||ct||ct||ct||'RETURNING t_ID INTO x_IsinID; '
  	||cr||ct||'END;'
  	||cr||ct||'-- Производится поиск ACCDEPOID в таблице маппинга. '
  	||cr||ct||'BEGIN '
    	||cr||ct||ct||'SELECT r.t_sofraccid INTO x_SofrAccID '
    	||cr||ct||ct||'FROM DDIASACCMAP_DBT r where r.t_diasaccid = x_DiasAccID; '
  	||cr||ct||'EXCEPTION '
     	||cr||ct||ct||'WHEN OTHERS THEN '
       	||cr||ct||ct||ct||'-- этого не может быть '
       	||cr||ct||ct||ct||'it_log.log( '
        ||cr||ct||ct||ct||ct||'p_msg => ''Ошибка маппинга, x_DiasAccID: '' || to_char(x_DiasAccID) '
        ||cr||ct||ct||ct||ct||', p_msg_type => it_log.c_msg_type__debug '
       	||cr||ct||ct||ct||'); '
  	||cr||ct||'END; '
  	||cr||ct||'IF( x_SofrAccID = -1 ) THEN '
    	||cr||ct||ct||'RETURN ; '
  	||cr||ct||'END IF; '
  	||cr||ct||'-- Производится попытка изменения данных в таблице DDIASRESTDEPO_DBT '
  	||cr||ct||'UPDATE DDIASRESTDEPO_DBT r '
    	||cr||ct||ct||'SET r.t_timestamp = x_TimeStamp, r.recID = x_RecID, r.value = x_Value '
    	||cr||ct||ct||'WHERE r.reportdate = x_ReportDate AND r.accdepoid = x_SofrAccID AND r.isin = x_IsinID '
    	||cr||ct||ct||'AND rownum = 1 '
  	||cr||ct||'; '
  	||cr||ct||'-- Неудачная попытка изменения данных означает то, что данные являются новыми (для счета, reportdate и ISIN), '
  	||cr||ct||'-- поэтому (при неудачном изменении) производится вставка записи в таблице остатков. '
  	||cr||ct||'-- если счета нет, добавляем '
  	||cr||ct||'IF( SQL%ROWCOUNT <> 1) THEN '
    	||cr||ct||ct||'INSERT INTO DDIASRESTDEPO_DBT r ( '
      	||cr||ct||ct||ct||'r.recid, r.accdepoid, r.reportdate, r.isin, r.value, r.t_timestamp '
    	||cr||ct||ct||') VALUES ( '
      	||cr||ct||ct||ct||'x_RecID, x_SofrAccID, x_ReportDate, x_IsinID, x_Value, x_TimeStamp '
    	||cr||ct||ct||'); '
  	||cr||ct||'END IF; '
	||cr||'EXCEPTION '
     	||cr||ct||'WHEN OTHERS THEN '
       	||cr||ct||ct||'it_error.put_error_in_stack; '
       	||cr||ct||ct||'it_log.log(p_msg => ''Error'', p_msg_type => it_log.c_msg_type__error); '
       	||cr||ct||ct||'it_error.clear_error_stack; '
       	||cr||ct||ct||'RAISE; '
        ||cr||'END;';
    EXECUTE IMMEDIATE x_Str;
    LogIt('Создан тригер '||x_TrgName);
  EXCEPTION
   WHEN OTHERS THEN 
      LogIt('Ошибка создания тригера '||x_TrgName);
      p_Stat := 1;
  END;
BEGIN

  createDDiasAccDepo( x_Stat );           	-- 1) создание таблицы DDIASACCDEPO_DBT
  fillDDiasAccDepo( x_Stat );           	-- 2) первоначальное заполнение таблицы DDIASACCDEPO_DBT
  fillAccDepoNumber( x_Stat );            	-- 3) заполнение deponumber и accdeponumber
  fillDepoNumber ( x_Stat );            	-- 4) дозаполнение deponumber
  createDDiasAccIdx( x_Stat );            	-- 5) создание индексов таблицы DDIASACCDEPO_DBT
  createDDiasAccMap( x_Stat );            	-- 6) создание таблицы DDIASACCMAP_DBT
  fillDDiasAccMap( x_Stat );            	-- 7) заполнение DDIASACCMAP_DBT (время работы 10 мин.)
  createDDiasAccMapIdx( x_Stat );            	-- 8) создание индексов таблицы DDIASACCMAP_DBT (время работы 2+3=5мин)
  createDDiasRestDepo ( x_Stat ); 		-- 9) создание таблицы DDIASRESTDEPO_DBT
  createDDiasRestTmp ( x_Stat ); 		-- 10) создание таблицы DIAS_REST_TMP (темперная, для копирования остатков за день)
  createDDiasAccRestIdx( x_Stat );            	-- 11) создание индексов таблицы DDIASRESTDEPO_DBT
  createDDiasDates ( x_Stat ); 		        -- 12) создание таблицы DDIASREPDATES_DBT (даты для копирования)
  createDDiasDatesIdx( x_Stat );            	-- 13) создание индексов таблицы DDIASREPDATES_DBT
  fillDDiasDates( x_Stat );           	     	-- 14) заполнение таблицы DDIASREPDATES_DBT (время работы 6 мин.)
  createDDiasIsin ( x_Stat ); 		        -- 15) создание таблицы DDIASISIN_DBT
  createDDiasIsinIdx( x_Stat );            	-- 16) создание индексов таблицы DDIASISIN_DBT
  createDDiasIsinSeq( x_Stat );            	-- 17) создание последовательности для T_ID таблицы DDIASISIN_DBT
  createDDiasIsinTrigger( x_Stat );            	-- 18) создание тригеров для таблицы DDIASISIN_DBT
  fillDDiasIsin ( x_Stat ); 		        -- 19) заполнение таблицы DDIASISIN_DBT
  createDDiasAccSeq( x_Stat );            	-- 20) создание последовательности для T_SOFRACCID таблицы DDIASACCDEPO_DBT
  createDDiasAccTrigger( x_Stat );            	-- 21) создание тригера для таблицы DDIASACCDEPO_DBT
  replaceTriggerAcc( x_Stat );                  -- 22) изменение триггера SOFR_SVERKAACCDEPOIN_HIST_AIR   
  replaceTriggerRest( x_Stat );                 -- 23) изменение триггера SOFR_SVERKAACCDEPOIN_HIST_AIR   
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
END;
/
