-- Изменения по BOSS-2609_BOSS-2893
-- Создание таблицы "D_OTCDEALTMP_DBT", для буферизации данных по загрузке сделок OTC в рамках Указа №844
DECLARE
  logID VARCHAR2(32) := 'BOSS-2609_BOSS-2893';
  x_Cnt NUMBER;
  x_Stat NUMBER := 0;
  -- Записать сообщение в itt_log
  PROCEDURE LogIt( p_message VARCHAR2 )
  AS
  BEGIN
    it_log.log(p_msg => logID||': '||p_message);
  END;
  -- Создание таблицы D_OTCDEALTMP_DBT
  PROCEDURE createTab_OTCDEALTMP ( p_Stat IN OUT number )
  AS
    x_Cnt number;
  BEGIN
    IF( p_Stat = 1 ) THEN
      RETURN;
    END IF;

    SELECT count(*) INTO x_Cnt FROM user_tables WHERE upper(table_name) = 'D_OTCDEALTMP_DBT';
    IF( x_Cnt = 1 ) THEN
      LogIt('Таблица D_OTCDEALTMP_DBT существует.');
      LogIt('Удаление таблицы D_OTCDEALTMP_DBT.');
      EXECUTE IMMEDIATE 'DROP TABLE D_OTCDEALTMP_DBT';
      LogIt('Удалена таблица D_OTCDEALTMP_DBT.');
    END IF;
    LogIt('Создание таблицы D_OTCDEALTMP_DBT');
    EXECUTE IMMEDIATE 'CREATE TABLE D_OTCDEALTMP_DBT (
      T_ID NUMBER(10,0) NOT NULL, T_CLIENT NUMBER(10,0) DEFAULT -1 NOT NULL, T_FIID NUMBER(10,0) DEFAULT -1 NOT NULL 
      , T_CLIENTCONTR NUMBER(10,0) DEFAULT -1 NOT NULL, T_REQID NUMBER(10,0) DEFAULT -1 NOT NULL 
      , T_DEALID NUMBER(10,0) DEFAULT -1 NOT NULL, T_FILEID NUMBER(10,0) DEFAULT -1 NOT NULL 
      , T_CREATED DATE DEFAULT sysdate NOT NULL
      , T_ERROR NUMBER(10,0) DEFAULT 0 NOT NULL
      , t_is_grouped_row number(1)
      , T_LAST_NAME VARCHAR2 (24), T_FIRST_NAME VARCHAR2 (24), T_MIDDLE_NAME VARCHAR2 (24), T_DOC_TYPE VARCHAR2 (2)
      , T_DOC_SERIES VARCHAR2 (13), T_DOC_NUMBER VARCHAR2 (26), T_DOC_DATE DATE
      , T_BIRTH_DATE DATE, T_INN VARCHAR2 (12) NOT NULL, T_ISIN VARCHAR2 (25) NOT NULL
      , T_FI_NDC_CODE VARCHAR2 (25), T_SECTION_CODE VARCHAR2 (25), T_DEPO_ACC_NUM VARCHAR2 (30)
      , T_DEPO_ACC_NUM_PERSON VARCHAR2 (40), T_QTY NUMBER, T_DEP_NAME VARCHAR2 (100)
      , T_DEP_CODE VARCHAR2 (30), T_PRICE NUMBER, T_VALUE NUMBER
      , T_TRUST_DATE VARCHAR2 (10), T_TRUST_NO VARCHAR2 (25), T_QTY_FACT NUMBER
      , T_PRICE_END NUMBER, T_VALUE_FACT NUMBER, T_SHAREHOLDING_FORMULA VARCHAR2 (30)
      , T_UTSTMP NUMBER, T_F0_FILE_NAME VARCHAR2 (40), T_F1_FILE_NAME VARCHAR2 (40)
    )';
    EXECUTE IMMEDIATE 'ALTER TABLE D_OTCDEALTMP_DBT ADD CONSTRAINT D_OTCDEALTMP_PK PRIMARY KEY (T_ID)';
    LogIt('Создана таблица D_OTCDEALTMP_DBT');
  EXCEPTION
    WHEN OTHERS THEN
      LogIt('Ошибка создания таблицы D_OTCDEALTMP_DBT');
      p_Stat := 1;
  END;
  PROCEDURE CreateColumnComments( p_TableName IN varchar2, p_ColumnName IN varchar2, p_Comment IN varchar2 )
  AS
  BEGIN
    execute immediate 'COMMENT ON COLUMN '||p_TableName||'.'||p_ColumnName||' IS '''||p_Comment||'''';
  END;
  -- Комментарии для таблицы D_OTCDEALTMP_DBT
  PROCEDURE CreateCmt_OTCDEALTMP ( p_Stat IN OUT number )
  AS
    x_Cnt number;
  BEGIN
    IF( p_Stat = 1 ) THEN
      RETURN;
    END IF;

    execute immediate 'comment on table D_OTCDEALTMP_DBT is ''Буферная таблица для загрузки сделок OTC в рамках Указа №844''';
    CreateColumnComments( 'D_OTCDEALTMP_DBT', 'T_ID', 'ID записи' );
    CreateColumnComments( 'D_OTCDEALTMP_DBT', 'T_CLIENT', 'ID субъекта' );
    CreateColumnComments( 'D_OTCDEALTMP_DBT', 'T_FIID', 'ID цб' );
    CreateColumnComments( 'D_OTCDEALTMP_DBT', 'T_CLIENTCONTR', 'ID субдоговора' );
    CreateColumnComments( 'D_OTCDEALTMP_DBT', 'T_REQID', 'ID заявки' );
    CreateColumnComments( 'D_OTCDEALTMP_DBT', 'T_DEALID', 'ID сделки' );
    CreateColumnComments( 'D_OTCDEALTMP_DBT', 'T_FILEID', 'ID файла' );
    CreateColumnComments( 'D_OTCDEALTMP_DBT', 'T_CREATED', 'Время создания' );
    CreateColumnComments( 'D_OTCDEALTMP_DBT', 'T_ERROR', 'Код ошибки' );
    CreateColumnComments( 'D_OTCDEALTMP_DBT', 'T_LAST_NAME', 'Фамилия владельца' );
    CreateColumnComments( 'D_OTCDEALTMP_DBT', 'T_FIRST_NAME', 'Имя владельца' );
    CreateColumnComments( 'D_OTCDEALTMP_DBT', 'T_MIDDLE_NAME', 'Отчество владельца' );
    CreateColumnComments( 'D_OTCDEALTMP_DBT', 'T_DOC_TYPE', 'Тип документа, удостоверяющего личность' );
    CreateColumnComments( 'D_OTCDEALTMP_DBT', 'T_DOC_SERIES', 'Серия документа, удостоверяющего личность' );
    CreateColumnComments( 'D_OTCDEALTMP_DBT', 'T_DOC_NUMBER', 'Номер документа, удостоверяющего личность' );
    CreateColumnComments( 'D_OTCDEALTMP_DBT', 'T_DOC_DATE', 'Дата документа, удостоверяющего личность' );
    CreateColumnComments( 'D_OTCDEALTMP_DBT', 'T_BIRTH_DATE', 'Дата рождения владельца' );
    CreateColumnComments( 'D_OTCDEALTMP_DBT', 'T_INN', 'ИНН владельца' );
    CreateColumnComments( 'D_OTCDEALTMP_DBT', 'T_ISIN', 'ISIN ценной бумаги' );
    CreateColumnComments( 'D_OTCDEALTMP_DBT', 'T_FI_NDC_CODE', 'Депозитарный код ценной бумаги' );
    CreateColumnComments( 'D_OTCDEALTMP_DBT', 'T_SECTION_CODE', 'Номер раздела счета в НКО АО НРД' );
    CreateColumnComments( 'D_OTCDEALTMP_DBT', 'T_DEPO_ACC_NUM', 'Номер счета в НКО АО НРД' );
    CreateColumnComments( 'D_OTCDEALTMP_DBT', 'T_DEPO_ACC_NUM_PERSON', 'Счет депо владельца в депозитарии' );
    CreateColumnComments( 'D_OTCDEALTMP_DBT', 'T_QTY', 'Количество иностранных ценных бумаг' );
    CreateColumnComments( 'D_OTCDEALTMP_DBT', 'T_DEP_NAME', 'Наименование депозитария владельца' );
    CreateColumnComments( 'D_OTCDEALTMP_DBT', 'T_DEP_CODE', 'Депозитарный код депонента в НКО АО НРД' );
    CreateColumnComments( 'D_OTCDEALTMP_DBT', 'T_PRICE', 'Начальная стоимость одной иностранной цб' );
    CreateColumnComments( 'D_OTCDEALTMP_DBT', 'T_VALUE', 'Стоимость иностранных цб' );
    CreateColumnComments( 'D_OTCDEALTMP_DBT', 'T_TRUST_DATE', 'Дата документа-основания действовать от имени пайщика' );
    CreateColumnComments( 'D_OTCDEALTMP_DBT', 'T_TRUST_NO', 'Номер документа-основания действовать от имени пайщика' );
    CreateColumnComments( 'D_OTCDEALTMP_DBT', 'T_QTY_FACT', 'Количество иностранных цб, Акцептованных Организатором торгов' );
    CreateColumnComments( 'D_OTCDEALTMP_DBT', 'T_PRICE_END', 'Цена приобретения цб' );
    CreateColumnComments( 'D_OTCDEALTMP_DBT', 'T_VALUE_FACT', 'Стоимость приобретенных цб' );
    CreateColumnComments( 'D_OTCDEALTMP_DBT', 'T_SHAREHOLDING_FORMULA', 'Формула расчета количества выкупа для ПИФ' );
    CreateColumnComments( 'D_OTCDEALTMP_DBT', 'T_UTSTMP', 'время формирования файла, Unix timestamp' );
    CreateColumnComments( 'D_OTCDEALTMP_DBT', 'T_F0_FILE_NAME', 'Наименование файла Формы 0' );
    CreateColumnComments( 'D_OTCDEALTMP_DBT', 'T_F1_FILE_NAME', 'Наименование файла Формы 1' );
  EXCEPTION
    WHEN OTHERS THEN
      LogIt('Ошибка при комментировании таблицы D_OTCDEALTMP_DBT');
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

    LogIt('Проверка последовательности '||p_SeqName);
    SELECT count(*) INTO x_Cnt FROM user_objects r WHERE r.OBJECT_NAME = p_SeqName and r.OBJECT_TYPE = 'SEQUENCE';
    IF (x_Cnt = 1) THEN
       LogIt('Последовательность '||p_SeqName||' существует');
       LogIt('Удаление последовательности '||p_SeqName);
       EXECUTE IMMEDIATE 'DROP SEQUENCE '||p_SeqName;
       LogIt('Удалена последовательность: '||p_SeqName);
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
  -- создание индексов для D_OTCDEALTMP_DBT
  PROCEDURE CreateIdx_OTCDEALTMP ( p_Stat IN OUT number )
  IS
  BEGIN
    IF( p_Stat = 1 ) THEN
      RETURN;
    END IF;

    createIndex ( p_Stat, '', 'D_OTCDEALTMP_DBT', 'D_OTCDEALTMP_IDX1', 'T_CLIENT', 'USERS' );
    createIndex ( p_Stat, '', 'D_OTCDEALTMP_DBT', 'D_OTCDEALTMP_IDX2', 'T_FIID', 'USERS' );
    createIndex ( p_Stat, '', 'D_OTCDEALTMP_DBT', 'D_OTCDEALTMP_IDX3', 'T_CLIENTCONTR', 'USERS' );
    createIndex ( p_Stat, '', 'D_OTCDEALTMP_DBT', 'D_OTCDEALTMP_IDX4', 'T_REQID', 'USERS' );
    createIndex ( p_Stat, '', 'D_OTCDEALTMP_DBT', 'D_OTCDEALTMP_IDX5', 'T_DEALID', 'USERS' );
    createIndex ( p_Stat, '', 'D_OTCDEALTMP_DBT', 'D_OTCDEALTMP_IDX6', 'T_FILEID', 'USERS' );
  EXCEPTION
   WHEN OTHERS THEN 
      p_Stat := 1;
  END;
  -- создание последовательности для T_ID таблицы D_OTCDEALTMP_DBT
  PROCEDURE CreateSeq_OTCDEALTMP ( p_Stat IN OUT number )
  IS
  BEGIN
    IF( p_Stat = 1 ) THEN
      RETURN;
    END IF;

    createSeq ( p_Stat, 'D_OTCDEALTMP_SEQ' );
  EXCEPTION
   WHEN OTHERS THEN 
      p_Stat := 1;
  END;
  -- создание тригеров для таблицы D_OTCDEALTMP_DBT
  PROCEDURE CreateTrg_OTCDEALTMP ( p_Stat IN OUT number )
  IS
  BEGIN
    IF( p_Stat = 1 ) THEN
      RETURN;
    END IF;

    createTrigger ( p_Stat, 'D_OTCDEALTMP_DBT', 'D_OTCDEALTMP_DBT_T0_AINC', 'T_ID', 'D_OTCDEALTMP_SEQ');
  EXCEPTION
   WHEN OTHERS THEN 
      p_Stat := 1;
  END;
BEGIN
  createTab_OTCDEALTMP( x_Stat );		-- 1) создание таблицы D_OTCDEALTMP_DBT
  CreateCmt_OTCDEALTMP( x_Stat );		-- 2) создание комментариев D_OTCDEALTMP_DBT
  CreateIdx_OTCDEALTMP( x_Stat );		-- 3) создание индексов для D_OTCDEALTMP_DBT
  CreateSeq_OTCDEALTMP( x_Stat );		-- 4) создание последовательности для T_ID таблицы D_OTCDEALTMP_DBT
  CreateTrg_OTCDEALTMP( x_Stat );		-- 5) создание тригеров для таблицы D_OTCDEALTMP_DBT
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
END;
/
