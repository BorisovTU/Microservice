-- Изменения по DEF-65084 (создание таблицы для групп авто-тестов)
DECLARE
  logID VARCHAR2(32) := 'DEF-65084';
  x_Cnt NUMBER;
  x_Stat NUMBER := 0;

  -- Записать сообщение в itt_log
  PROCEDURE LogIt(p_message IN VARCHAR2)
  AS
  BEGIN
    it_log.log(p_msg => logID||': '||p_message);
  END;
  -- создание таблицы DAUTOTESTGROUPS_DBT
  PROCEDURE createTab_AUTOTESTGROUPS ( p_Stat IN OUT number )
  IS
  BEGIN
    IF( p_Stat = 1 ) THEN
      RETURN;
    END IF;

    LogIt('Создание таблицы DAUTOTESTGROUPS_DBT');
    SELECT count(*) INTO x_Cnt FROM user_tables WHERE table_name = upper('DAUTOTESTGROUPS_DBT');
    IF (x_Cnt = 1) THEN
       LogIt('Существует таблица DAUTOTESTGROUPS_DBT');
    ELSE
       EXECUTE IMMEDIATE 'CREATE TABLE DAUTOTESTGROUPS_DBT
            (
               T_ID NUMBER(*,0) NOT NULL ENABLE
               , T_MODULE VARCHAR2(32) 
               , T_SETUP VARCHAR2(32)
               , T_TEARDOWN VARCHAR2(32)
               , T_DESCR VARCHAR2(128)
            )'
       ;
       EXECUTE IMMEDIATE 'ALTER TABLE DAUTOTESTGROUPS_DBT ADD CONSTRAINT DAUTOTESTGROUPS_PK PRIMARY KEY (T_ID)';
       LogIt('Создана таблица DAUTOTESTGROUPS_DBT');
    END IF;
  EXCEPTION
   WHEN OTHERS THEN 
      LogIt('Ошибка создания таблицы DAUTOTESTGROUPS_DBT');
      EXECUTE IMMEDIATE 'ROLLBACK';
      p_Stat := 1;
  END;
  -- Создание комментария для колонки
  PROCEDURE CreateColumnComments( p_TableName IN varchar2, p_ColumnName IN varchar2, p_Comment IN varchar2 )
  AS
  BEGIN
    execute immediate 'COMMENT ON COLUMN '||p_TableName||'.'||p_ColumnName||' IS '''||p_Comment||'''';
  END;
  -- Комментарии для таблицы DAUTOTESTGROUPS_DBT
  PROCEDURE CreateCmt_AUTOTESTGROUPS ( p_Stat IN OUT number )
  AS
    x_Cnt number;
  BEGIN
    IF( p_Stat = 1 ) THEN
      RETURN;
    END IF;

    execute immediate 'comment on table DAUTOTESTGROUPS_DBT is ''Группы авто-тестов''';
    CreateColumnComments( 'DAUTOTESTGROUPS_DBT', 'T_ID', 'ID группы авто-тестов' );
    CreateColumnComments( 'DAUTOTESTGROUPS_DBT', 'T_MODULE', 'имя макро-файла для процедур инициализации и очистки группы' );
    CreateColumnComments( 'DAUTOTESTGROUPS_DBT', 'T_SETUP', 'процедура инициализации для группы авто-тестов' );
    CreateColumnComments( 'DAUTOTESTGROUPS_DBT', 'T_TEARDOWN', 'процедура очистки для группы авто-тестов' );
    CreateColumnComments( 'DAUTOTESTGROUPS_DBT', 'T_DESCR', 'описание группы авто-тестов' );
  EXCEPTION
    WHEN OTHERS THEN
      LogIt('Ошибка при комментировании таблицы DAUTOTESTGROUPS_DBT');
      p_Stat := 1;
  END;
  -- добавление группы тестов
  PROCEDURE addTestGroup ( 
     p_Stat IN OUT number
     , p_id IN number
     , p_descr IN varchar2
     , p_module IN varchar2 DEFAULT ''
     , p_setup IN varchar2 DEFAULT ''
     , p_teardown IN varchar2 DEFAULT ''
  )
  IS
    x_Str VARCHAR2(32000);
    cr VARCHAR2(2) := CHR(10);
  BEGIN
    IF( p_Stat = 1 ) THEN
      RETURN;
    END IF;

    LogIt('Добавление группы авто-тестов '||p_descr);
    x_Str := 'INSERT INTO dautotestgroups_dbt r ('
        ||'r.t_id, r.t_module, r.t_setup, r.t_teardown, r.t_descr '
        ||') VALUES ( '
        ||':p_id, :p_module, :p_setup, :p_teardown, :p_descr '
        ||')'
    ;
    EXECUTE IMMEDIATE x_Str USING p_id, p_module, p_setup, p_teardown, p_descr;
    EXECUTE IMMEDIATE 'COMMIT';
    LogIt('Добавлена группа авто-тестов '||p_descr);
  EXCEPTION
   WHEN OTHERS THEN 
      LogIt('Ошибка добавления группы авто-тестов '||p_descr);
      LogIt('SQLERRM '||SQLERRM);
      p_Stat := 1;
  END;
  -- начальное наполнение таблицы DAUTOTESTGROUPS_DBT
  PROCEDURE FillTab_AUTOTESTGROUPS ( p_Stat IN OUT number )
  IS
  BEGIN
    IF( p_Stat = 1 ) THEN
      RETURN;
    END IF;

    addTestGroup ( p_Stat, 0, 'Не определена');
    addTestGroup ( p_Stat, 1, 'Простые тесты');
    addTestGroup ( p_Stat, 2, 'Тесты rshb_rsi_sclimit.GetLimitPrm()');
    addTestGroup ( p_Stat, 3, 'Тесты отчета-сверки 24', 'uentcompare_tests.mac', 'SetUp', 'TearDown');
  EXCEPTION
   WHEN OTHERS THEN 
      p_Stat := 1;
  END;
  -- Добавление столбца t_Group в таблицу DAUTOTESTS_DBT
  PROCEDURE AlterTab_AUTOTESTS ( p_Stat IN OUT number )
  IS
  BEGIN
    IF( p_Stat = 1 ) THEN
      RETURN;
    END IF;

    LogIt('Добавление столбца t_Group в таблицу DAUTOTESTS_DBT');
    EXECUTE IMMEDIATE 'ALTER TABLE DAUTOTESTS_DBT ADD (T_GROUP NUMBER DEFAULT 0)';
    LogIt('Добавлен столбец t_Group в таблицу DAUTOTESTS_DBT');
  EXCEPTION
   WHEN OTHERS THEN 
      LogIt('Ошибка добавления столбца t_Group в таблицу DAUTOTESTS_DBT');
      EXECUTE IMMEDIATE 'ROLLBACK';
      p_Stat := 1;
  END;
  -- добавление теста
  PROCEDURE addTest ( 
     p_Stat IN OUT number
     , p_module IN varchar2
     , p_procname IN varchar2
     , p_testname IN varchar2
     , p_descr IN varchar2
     , p_group IN number
  )
  IS
    x_Str VARCHAR2(32000);
    cr VARCHAR2(2) := CHR(10);
  BEGIN
    IF( p_Stat = 1 ) THEN
      RETURN;
    END IF;

    LogIt('Добавление авто-теста '||p_testname);
    x_Str := 'INSERT INTO dautotests_dbt r ('
        ||'r.t_module, r.t_procname, r.t_testname, r.t_descr, r.t_group '
        ||') VALUES ( '
        ||':p_module, :p_procname, :p_testname, :p_descr, :p_group '
        ||')'
    ;
    EXECUTE IMMEDIATE x_Str USING p_module, p_procname, p_testname, p_descr, p_group;
    EXECUTE IMMEDIATE 'COMMIT';
    LogIt('Добавлен авто-тест '||p_testname);
  EXCEPTION
   WHEN OTHERS THEN 
      LogIt('Ошибка добавления авто-теста '||p_testname);
      p_Stat := 1;
  END;
  -- Изменение в таблице авто-тестов с учетом t_Group (и другое)
  PROCEDURE Refactor_AUTOTESTS ( p_Stat IN OUT number )
  IS
  BEGIN
    IF( p_Stat = 1 ) THEN
      RETURN;
    END IF;

    LogIt('Изменение группы для авто-теста t_id = 1');
    EXECUTE IMMEDIATE 'update DAUTOTESTS_DBT r set r.t_group = 1 where r.t_id = 1';
    LogIt('Изменение группы для авто-теста t_id = 2');
    EXECUTE IMMEDIATE q'[update DAUTOTESTS_DBT r 
      set r.t_group = 2
        , r.t_procname = 'NonEdpTagIsRTOD'
        , r.t_testname = 'NonEdpTagIsRTOD'
        , r.t_descr = 'Tag для суб-договора не-ЕДП должен быть RTOD'
      where r.t_id = 2]';
    commit;
    addTest ( p_Stat, 'dl_tests.mac', 'EdpTagIsEQTV', 'EdpTagIsEQTV', 'Tag для суб-договора ЕДП должен быть EQTV', 2);
    addTest ( p_Stat, 'uentcompare_tests.mac', 'NoCftIDOnErr3', 'NoCftIDOnErr3', 'если нет проводки в ЦФТ, не должно быть идентификатора', 3);
    addTest ( p_Stat, 'uentcompare_tests.mac', 'No306_306', 'No306_306', 'отсутствие проводок 306-306', 3);
    addTest ( p_Stat, 'uentcompare_tests.mac', 'NoPairs', 'NoPairs', 'отсутствие проводок по урегулированию парных счетов', 3);
  EXCEPTION
   WHEN OTHERS THEN 
      LogIt('Ошибка добавления столбца t_Group в таблицу DAUTOTESTS_DBT');
      LogIt('SQLERRM '||SQLERRM);
      EXECUTE IMMEDIATE 'ROLLBACK';
      p_Stat := 1;
  END;
BEGIN

  createTab_AUTOTESTGROUPS( x_Stat );			-- 1) создание таблицы DAUTOTESTGROUPS_DBT
  CreateCmt_AUTOTESTGROUPS( x_Stat );			-- 2) создание комментариев DAUTOTESTGROUPS_DBT
  FillTab_AUTOTESTGROUPS( x_Stat );			-- 3) начальное наполнение таблицы DAUTOTESTGROUPS_DBT
  AlterTab_AUTOTESTS( x_Stat );			        -- 4) Добавление столбца t_Group в таблицу DAUTOTESTS_DBT
  Refactor_AUTOTESTS( x_Stat );			        -- 5) Изменение в таблице авто-тестов с учетом t_Group (и другое)
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
END;
/
