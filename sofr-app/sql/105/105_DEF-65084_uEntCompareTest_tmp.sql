-- Изменения по DEF-65084
-- Создание таблицы "uEntCompareTest_tmp", для авто-тестирования отчета-сверке 24
DECLARE
  logID VARCHAR2(32) := 'DEF-65084';
  -- Записать сообщение в itt_log
  PROCEDURE LogIt( p_message VARCHAR2 )
  AS
  BEGIN
    it_log.log(p_msg => logID||': '||p_message);
  END;
  -- Создание таблицы uEntCompareTest_tmp
  PROCEDURE CreateUEntCompareTest_tmp
  AS
    x_Cnt number;
  BEGIN
    SELECT count(*) INTO x_Cnt FROM user_tables WHERE upper(table_name) = 'UENTCOMPARETEST_TMP';
    IF( x_Cnt = 1 ) THEN
      LogIt('Таблица UENTCOMPARETEST_TMP существует.');
      LogIt('Удаление таблицы UENTCOMPARETEST_TMP.');
      EXECUTE IMMEDIATE 'DROP TABLE UENTCOMPARETEST_TMP';
      LogIt('Удалена таблица UENTCOMPARETEST_TMP.');
    END IF;
    LogIt('Создание таблицы UENTCOMPARETEST_TMP');
    EXECUTE IMMEDIATE 'CREATE GLOBAL TEMPORARY TABLE UENTCOMPARETEST_TMP (
      T_IDBISCOTTO VARCHAR2(25), T_ACCTRNID NUMBER(10,0), T_DOCNUM VARCHAR2(15)
      , T_ACCT_DB VARCHAR2(25), T_ACCT_CR VARCHAR2(25), T_CURRENCY VARCHAR2(25)
      , T_AMTCUR NUMBER(32,12), T_AMTRUB NUMBER(32,12), T_OPDATE DATE
      , T_ERR NUMBER, T_DETAILS VARCHAR2(600), T_OPER NUMBER
    ) ON COMMIT PRESERVE ROWS';
    LogIt('Создана таблица UENTCOMPARETEST_TMP');
  EXCEPTION
    WHEN OTHERS THEN
      LogIt('Ошибка создания таблицы UENTCOMPARETEST_TMP');
  END;
  PROCEDURE CreateColumnComments( p_TableName IN varchar2, p_ColumnName IN varchar2, p_Comment IN varchar2 )
  AS
  BEGIN
    execute immediate 'COMMENT ON COLUMN '||p_TableName||'.'||p_ColumnName||' IS '''||p_Comment||'''';
  END;
  -- Комментарии для таблицы UENTCOMPARETEST_TMP
  PROCEDURE CreateComments
  AS
    x_Cnt number;
  BEGIN
    execute immediate 'comment on table UENTCOMPARETEST_TMP is ''Таблица для авто-тестов отчета-сверки 24''';
    CreateColumnComments( 'UENTCOMPARETEST_TMP', 'T_IDBISCOTTO', 'ID проводки ЦФТ' );
    CreateColumnComments( 'UENTCOMPARETEST_TMP', 'T_ACCTRNID', 'ID проводки СОФР' );
    CreateColumnComments( 'UENTCOMPARETEST_TMP', 'T_DOCNUM', 'Номер документа' );
    CreateColumnComments( 'UENTCOMPARETEST_TMP', 'T_ACCT_DB', 'Счет по дебету' );
    CreateColumnComments( 'UENTCOMPARETEST_TMP', 'T_ACCT_CR', 'Счет по кредиту' );
    CreateColumnComments( 'UENTCOMPARETEST_TMP', 'T_CURRENCY', 'Валюта' );
    CreateColumnComments( 'UENTCOMPARETEST_TMP', 'T_AMTCUR', 'Сумма в валюте проводки' );
    CreateColumnComments( 'UENTCOMPARETEST_TMP', 'T_AMTRUB', 'Сумма в рублях' );
    CreateColumnComments( 'UENTCOMPARETEST_TMP', 'T_OPDATE', 'Дата проводки' );
    CreateColumnComments( 'UENTCOMPARETEST_TMP', 'T_ERR', 'Код ошибки-расхождения' );
    CreateColumnComments( 'UENTCOMPARETEST_TMP', 'T_DETAILS', 'Наименование проводки' );
    CreateColumnComments( 'UENTCOMPARETEST_TMP', 'T_OPER', 'Операционист' );
  EXCEPTION
    WHEN OTHERS THEN
      LogIt('Ошибка добавления комментариев для таблицы UENTCOMPARETEST_TMP');
  END;
BEGIN
  -- Создание таблицы UENTCOMPARETEST_TMP
  CreateUEntCompareTest_tmp();
  CreateComments();
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
END;
/
