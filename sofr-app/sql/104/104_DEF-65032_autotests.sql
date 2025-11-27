-- Изменения по DEF-65032 (создание таблицы для авто-тестов)
DECLARE
  logID VARCHAR2(32) := 'DEF-65032';
  x_Cnt NUMBER;
  x_Stat NUMBER := 0;

  -- Записать сообщение в itt_log
  PROCEDURE LogIt(p_message IN VARCHAR2)
  AS
  BEGIN
    it_log.log(p_msg => logID||': '||p_message);
  END;
  -- создание таблицы DAUTOTESTS_DBT
  PROCEDURE createTab_AUTOTESTS ( p_Stat IN OUT number )
  IS
  BEGIN
    IF( p_Stat = 1 ) THEN
      RETURN;
    END IF;

    LogIt('Создание таблицы DAUTOTESTS_DBT');
    SELECT count(*) INTO x_Cnt FROM user_tables WHERE table_name = upper('DAUTOTEST_DBT');
    IF (x_Cnt = 1) THEN
       LogIt('Существует таблица DAUTOTESTS_DBT');
    ELSE
       EXECUTE IMMEDIATE 'CREATE TABLE DAUTOTESTS_DBT
            (
               T_ID NUMBER(*,0) NOT NULL ENABLE
               , T_MODULE VARCHAR2(32) 
               , T_PROCNAME VARCHAR2(32)
               , T_TESTNAME VARCHAR2(32) NOT NULL
               , T_DESCR VARCHAR2(128)
               , T_USED CHAR(1) DEFAULT ''X''
               , T_LEVEL NUMBER DEFAULT 100
            )'
       ;
       EXECUTE IMMEDIATE 'ALTER TABLE DAUTOTESTS_DBT ADD CONSTRAINT DAUTOTESTS_PK PRIMARY KEY (T_ID)';
       LogIt('Создана таблица DAUTOTESTS_DBT');
    END IF;
  EXCEPTION
   WHEN OTHERS THEN 
      LogIt('Ошибка создания таблицы DAUTOTESTS_DBT');
      EXECUTE IMMEDIATE 'ROLLBACK';
      p_Stat := 1;
  END;
  -- Создание комментария для колонки
  PROCEDURE CreateColumnComments( p_TableName IN varchar2, p_ColumnName IN varchar2, p_Comment IN varchar2 )
  AS
  BEGIN
    execute immediate 'COMMENT ON COLUMN '||p_TableName||'.'||p_ColumnName||' IS '''||p_Comment||'''';
  END;
  -- Комментарии для таблицы DAUTOTESTS_DBT
  PROCEDURE CreateCmt_AUTOTESTS ( p_Stat IN OUT number )
  AS
    x_Cnt number;
  BEGIN
    IF( p_Stat = 1 ) THEN
      RETURN;
    END IF;

    execute immediate 'comment on table DAUTOTESTS_DBT is ''Таблица со списком авто-тестов''';
    CreateColumnComments( 'DAUTOTESTS_DBT', 'T_ID', 'ID теста' );
    CreateColumnComments( 'DAUTOTESTS_DBT', 'T_MODULE', 'наименование модуля' );
    CreateColumnComments( 'DAUTOTESTS_DBT', 'T_PROCNAME', 'наименование процедуры' );
    CreateColumnComments( 'DAUTOTESTS_DBT', 'T_TESTNAME', 'наименование теста' );
    CreateColumnComments( 'DAUTOTESTS_DBT', 'T_DESCR', 'описание теста' );
    CreateColumnComments( 'DAUTOTESTS_DBT', 'T_USED', 'тест используется?' );
    CreateColumnComments( 'DAUTOTESTS_DBT', 'T_LEVEL', 'Уровень теста, тесты с меньшим значением выполняются раньше' );
  EXCEPTION
    WHEN OTHERS THEN
      LogIt('Ошибка при комментировании таблицы DAUTOTESTS_DBT');
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
  -- создание индексов для DAUTOTESTS_DBT
  PROCEDURE CreateIdx_AUTOTESTS ( p_Stat IN OUT number )
  IS
  BEGIN
    IF( p_Stat = 1 ) THEN
      RETURN;
    END IF;

    createIndex ( p_Stat, 'UNIQUE', 'DAUTOTESTS_DBT', 'DAUTOTESTS_IDX1', 'T_TESTNAME', 'USERS', '' );
  EXCEPTION
   WHEN OTHERS THEN 
      p_Stat := 1;
  END;
  -- создание последовательности для T_ID таблицы DAUTOTESTS_DBT
  PROCEDURE CreateSeq_AUTOTESTS ( p_Stat IN OUT number )
  IS
  BEGIN
    IF( p_Stat = 1 ) THEN
      RETURN;
    END IF;

    createSeq ( p_Stat, 'DAUTOTESTS_SEQ' );
  EXCEPTION
   WHEN OTHERS THEN 
      p_Stat := 1;
  END;
  -- создание тригеров для таблицы DAUTOTESTS_DBT
  PROCEDURE CreateTrg_AUTOTESTS ( p_Stat IN OUT number )
  IS
  BEGIN
    IF( p_Stat = 1 ) THEN
      RETURN;
    END IF;

    createTrigger ( p_Stat, 'DAUTOTESTS_DBT', 'DAUTOTESTS_DBT_T0_AINC', 'T_ID', 'DAUTOTESTS_SEQ');
  EXCEPTION
   WHEN OTHERS THEN 
      p_Stat := 1;
  END;
  -- добавление теста
  PROCEDURE addTest ( 
     p_Stat IN OUT number
     , p_module IN varchar2
     , p_procname IN varchar2
     , p_testname IN varchar2
     , p_descr IN varchar2
  )
  IS
    x_Str VARCHAR2(32000);
    cr VARCHAR2(2) := CHR(10);
  BEGIN
    IF( p_Stat = 1 ) THEN
      RETURN;
    END IF;

    LogIt('Добавление авто-теста '||p_testname);
    x_Str := 'INSERT INTO dautotests_dbt r (r.t_module, r.t_procname, r.t_testname, r.t_descr '
        ||') VALUES ( :p_module, :p_procname, :p_testname, :p_descr )'
    ;
    EXECUTE IMMEDIATE x_Str USING p_module, p_procname, p_testname, p_descr;
    EXECUTE IMMEDIATE 'COMMIT';
    LogIt('Добавлен авто-тест '||p_testname);
  EXCEPTION
   WHEN OTHERS THEN 
      LogIt('Ошибка добавления авто-теста '||p_testname);
      p_Stat := 1;
  END;
  -- начальное наполнение таблицы DAUTOTESTS_DBT
  PROCEDURE FillTab_AUTOTESTS ( p_Stat IN OUT number )
  IS
  BEGIN
    IF( p_Stat = 1 ) THEN
      RETURN;
    END IF;

    addTest ( p_Stat, 'cb_tests.mac', 'SimpleTest', 'SimpleTest', 'Простой тест');
    addTest ( p_Stat, 'dl_tests.mac', 'Test_DEF_65032', 'Test_DEF_65032', 'Тест для DEF-65032');
  EXCEPTION
   WHEN OTHERS THEN 
      p_Stat := 1;
  END;
BEGIN

  createTab_AUTOTESTS( x_Stat );			-- 1) создание таблицы DAUTOTESTS_DBT
  CreateCmt_AUTOTESTS( x_Stat );			-- 2) создание комментариев DAUTOTESTS_DBT
  CreateIdx_AUTOTESTS( x_Stat );			-- 3) создание индексов для DAUTOTESTS_DBT
  CreateSeq_AUTOTESTS( x_Stat );			-- 4) создание последовательности для T_ID таблицы DAUTOTESTS_DBT
  CreateTrg_AUTOTESTS( x_Stat );			-- 5) создание тригеров для таблицы DAUTOTESTS_DBT
  FillTab_AUTOTESTS( x_Stat );				-- 6) начальное наполнение таблицы DAUTOTESTS_DBT
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
END;
/
