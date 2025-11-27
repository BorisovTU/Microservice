-- Изменения по BOSS-2056_BOSS-2541
-- Создание таблицы "uEntCompareParam_tmp", для фильтрации данных в отчете-сверке проводок между ЦФТ и СОФР
DECLARE
  logID VARCHAR2(32) := 'BOSS-2056_BOSS-2541';
  -- Записать сообщение в itt_log
  PROCEDURE LogIt( p_message VARCHAR2 )
  AS
  BEGIN
    it_log.log(p_msg => logID||': '||p_message);
  END;
  -- Создание таблицы uEntCompareParam_tmp
  PROCEDURE CreateUEntCompareParam_tmp
  AS
    x_Cnt number;
  BEGIN
    SELECT count(*) INTO x_Cnt FROM user_tables WHERE upper(table_name) = 'UENTCOMPAREPARAM_TMP';
    IF( x_Cnt = 1 ) THEN
      LogIt('Таблица UENTCOMPAREPARAM_TMP существует.');
      LogIt('Удаление таблицы UENTCOMPAREPARAM_TMP.');
      EXECUTE IMMEDIATE 'DROP TABLE UENTCOMPAREPARAM_TMP';
      LogIt('Удалена таблица UENTCOMPAREPARAM_TMP.');
    END IF;
    LogIt('Создание таблицы UENTCOMPAREPARAM_TMP');
    EXECUTE IMMEDIATE 'CREATE GLOBAL TEMPORARY TABLE UENTCOMPAREPARAM_TMP (
      ID NUMBER, PERSN VARCHAR2(32), ACC_D VARCHAR2(25), ACC_C VARCHAR2(25)
    )';
    LogIt('Создана таблица UENTCOMPAREPARAM_TMP');
  EXCEPTION
    WHEN OTHERS THEN
      LogIt('Ошибка создания таблицы UENTCOMPAREPARAM_TMP');
  END;
  PROCEDURE CreateColumnComments( p_TableName IN varchar2, p_ColumnName IN varchar2, p_Comment IN varchar2 )
  AS
  BEGIN
    execute immediate 'COMMENT ON COLUMN '||p_TableName||'.'||p_ColumnName||' IS '''||p_Comment||'''';
  END;
  -- Комментарии для таблицы UENTCOMPAREPARAM_TMP
  PROCEDURE CreateComments
  AS
    x_Cnt number;
  BEGIN
    execute immediate 'comment on table UENTCOMPAREPARAM_TMP is ''Таблица для фильтрации данных при сверке проводок ЦФТ и СОФР''';
    CreateColumnComments( 'UENTCOMPAREPARAM_TMP', 'ID', 'ID' );
    CreateColumnComments( 'UENTCOMPAREPARAM_TMP', 'PERSN', 'номер пачки' );
    CreateColumnComments( 'UENTCOMPAREPARAM_TMP', 'ACC_D', 'счет дебета' );
    CreateColumnComments( 'UENTCOMPAREPARAM_TMP', 'ACC_C', 'счет кредита' );
  EXCEPTION
    WHEN OTHERS THEN
      LogIt('Ошибка добавления комментариев для таблицы UENTCOMPAREPARAM_TMP');
  END;
  -- Создание индекса UENTCOMPAREPARAM_TMP
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
      LogIt('Ошибка создания индекса '||p_IndexName);
  END;
BEGIN
  -- Создание таблицы UENTCOMPAREPARAM_TMP
  CreateUEntCompareParam_tmp();
  CreateComments();
  CreateIndex('UENTCOMPAREPARAM_TMP', 'UENTCOMPAREPARAM_IDX0', 'ID');
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
END;
/
