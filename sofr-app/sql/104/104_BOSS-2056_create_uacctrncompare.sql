-- Изменения по BOSS-2056_BOSS-2499
-- Создание таблицы "UACCTRNCOMPARE_DBT", для отчета сверки проводок между ЦФТ и СОФР
DECLARE
  logID VARCHAR2(32) := 'BOSS-2056_BOSS-2499';
  -- Записать сообщение в itt_log
  PROCEDURE LogIt( p_message VARCHAR2 )
  AS
  BEGIN
    it_log.log(p_msg => logID||': '||p_message);
  END;
  -- Создание таблицы UACCTRNCOMPARE_DBT
  PROCEDURE CreateTableUacctrncompare
  AS
    x_Cnt number;
  BEGIN
    SELECT count(*) INTO x_Cnt FROM user_tables WHERE upper(table_name) = 'UACCTRNCOMPARE_DBT';
    IF( x_Cnt = 1 ) THEN
      LogIt('Таблица UACCTRNCOMPARE_DBT существует.');
      LogIt('Удаление таблицы UACCTRNCOMPARE_DBT.');
      EXECUTE IMMEDIATE 'DROP TABLE UACCTRNCOMPARE_DBT';
      LogIt('Удалена таблица UACCTRNCOMPARE_DBT.');
    END IF;
    LogIt('Создание таблицы UACCTRNCOMPARE_DBT');
    EXECUTE IMMEDIATE 'CREATE TABLE UACCTRNCOMPARE_DBT (
      T_AUTOKEY NUMBER, T_ID NUMBER(10,0), T_DB VARCHAR2(25), T_CR VARCHAR2(25)
      , T_SUMCUR NUMBER(32,12), T_SUMRUB NUMBER(32,12), T_DESCR VARCHAR2(600)
      , T_DATE DATE, T_BISCID VARCHAR2(25), T_ERR NUMBER, T_REQID VARCHAR2(25)
    )';
    LogIt('Создана таблица UACCTRNCOMPARE_DBT');
  EXCEPTION
    WHEN OTHERS THEN
      LogIt('Ошибка создания таблицы UACCTRNCOMPARE_DBT');
  END;
  PROCEDURE CreateColumnComments( p_TableName IN varchar2, p_ColumnName IN varchar2, p_Comment IN varchar2 )
  AS
  BEGIN
    execute immediate 'COMMENT ON COLUMN '||p_TableName||'.'||p_ColumnName||' IS '''||p_Comment||'''';
  END;
  -- Создание таблицы UACCTRNCOMPARE_DBT
  PROCEDURE CreateComments
  AS
    x_Cnt number;
  BEGIN
    execute immediate 'comment on table UACCTRNCOMPARE_DBT is ''Таблица расхождений между проводками ЦФТ и СОФР''';
    CreateColumnComments( 'UACCTRNCOMPARE_DBT', 'T_AUTOKEY', 'ID записи из uloadentforcompare_dbt' );
    CreateColumnComments( 'UACCTRNCOMPARE_DBT', 'T_ID', 'ID проводки' );
    CreateColumnComments( 'UACCTRNCOMPARE_DBT', 'T_DB', 'счет дебета' );
    CreateColumnComments( 'UACCTRNCOMPARE_DBT', 'T_CR', 'счет кредита' );
    CreateColumnComments( 'UACCTRNCOMPARE_DBT', 'T_SUMCUR', 'сумма в валюте' );
    CreateColumnComments( 'UACCTRNCOMPARE_DBT', 'T_SUMRUB', 'сумма в рублях' );
    CreateColumnComments( 'UACCTRNCOMPARE_DBT', 'T_DESCR', 'описание проводки' );
    CreateColumnComments( 'UACCTRNCOMPARE_DBT', 'T_DATE', 'дата проводки' );
    CreateColumnComments( 'UACCTRNCOMPARE_DBT', 'T_BISCID', 'ID проводки ЦФТ' );
    CreateColumnComments( 'UACCTRNCOMPARE_DBT', 'T_ERR', 'тип ошибки' );
    CreateColumnComments( 'UACCTRNCOMPARE_DBT', 'T_REQID', 'номер выгрузки проводок' );
  EXCEPTION
    WHEN OTHERS THEN
      LogIt('Ошибка создания таблицы UACCTRNCOMPARE_DBT');
  END;
  -- Создание индекса UACCTRNCOMPARE_DBT
  PROCEDURE CreateIndex( p_TableName IN varchar2, p_IndexName IN varchar2, p_Columns IN varchar2 )
  AS
    x_Cnt number;
  BEGIN
    SELECT count(*) INTO x_Cnt FROM user_indexes i WHERE i.TABLE_NAME = p_TableName and i.INDEX_NAME = p_IndexName ;
    IF( x_Cnt = 1 ) THEN
      EXECUTE IMMEDIATE 'DROP INDEX '||p_IndexName;
    END IF;

    LogIt('Создание индекса '||p_IndexName);
    EXECUTE IMMEDIATE 'CREATE INDEX '||p_IndexName||' ON '||p_TableName||' ('||p_Columns||')';
    LogIt('Создан индекс '||p_IndexName);
  EXCEPTION
    WHEN OTHERS THEN
      LogIt('Ошибка создания таблицы UACCTRNCOMPARE_DBT');
  END;
BEGIN
  -- Создание таблицы UACCTRNCOMPARE_DBT
  CreateTableUacctrncompare();
  CreateComments();
  CreateIndex('UACCTRNCOMPARE_DBT', 'UACCTRNCOMPARE_IDX0', 'T_REQID');
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
END;
/
