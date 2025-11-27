-- Изменения по BOSS-2056_BOSS-2998
-- Создание таблицы "UVALIDATE844DEALS_TMP", для валидации файла сделок по Указу 844
DECLARE
  logID VARCHAR2(32) := 'BOSS-2056_BOSS-2998';
  -- Записать сообщение в itt_log
  PROCEDURE LogIt( p_message VARCHAR2 )
  AS
  BEGIN
    it_log.log(p_msg => logID||': '||p_message);
  END;
  -- Создание таблицы uEntCompareParam_tmp
  PROCEDURE CreateUValidate844Deals_tmp
  AS
    x_Cnt number;
  BEGIN
    SELECT count(*) INTO x_Cnt FROM user_tables WHERE upper(table_name) = 'UVALIDATE844DEALS_TMP';
    IF( x_Cnt = 1 ) THEN
      LogIt('Таблица UVALIDATE844DEALS_TMP существует.');
      LogIt('Удаление таблицы UVALIDATE844DEALS_TMP.');
      EXECUTE IMMEDIATE 'DROP TABLE UVALIDATE844DEALS_TMP';
      LogIt('Удалена таблица UVALIDATE844DEALS_TMP.');
    END IF;
    LogIt('Создание таблицы UVALIDATE844DEALS_TMP');
    EXECUTE IMMEDIATE 'CREATE GLOBAL TEMPORARY TABLE UVALIDATE844DEALS_TMP (
      T_COL_NAME VARCHAR2(32), T_POSITION NUMBER DEFAULT -1
    ) ON COMMIT PRESERVE ROWS';
    LogIt('Создана таблица UVALIDATE844DEALS_TMP');
  EXCEPTION
    WHEN OTHERS THEN
      LogIt('Ошибка создания таблицы UVALIDATE844DEALS_TMP');
  END;
  PROCEDURE CreateColumnComments( p_TableName IN varchar2, p_ColumnName IN varchar2, p_Comment IN varchar2 )
  AS
  BEGIN
    execute immediate 'COMMENT ON COLUMN '||p_TableName||'.'||p_ColumnName||' IS '''||p_Comment||'''';
  END;
  -- Комментарии для таблицы UVALIDATE844DEALS_TMP
  PROCEDURE CreateComments
  AS
    x_Cnt number;
  BEGIN
    execute immediate 'comment on table UVALIDATE844DEALS_TMP is ''Таблица для валидации файла сделок по Указу 844''';
    CreateColumnComments( 'UVALIDATE844DEALS_TMP', 'T_COL_NAME', 'наименование колонки' );
    CreateColumnComments( 'UVALIDATE844DEALS_TMP', 'T_POSITION', 'индекс позиции' );
  EXCEPTION
    WHEN OTHERS THEN
      LogIt('Ошибка добавления комментариев для таблицы UVALIDATE844DEALS_TMP');
  END;
  -- Создание индекса UVALIDATE844DEALS_TMP
  PROCEDURE CreateIndex( p_TableName IN varchar2, p_Unique IN varchar2, p_IndexName IN varchar2, p_Columns IN varchar2 )
  AS
    x_Cnt number;
  BEGIN
    SELECT count(*) INTO x_Cnt FROM user_indexes i WHERE i.TABLE_NAME = p_TableName and i.INDEX_NAME = p_IndexName ;
    IF( x_Cnt = 1 ) THEN
      EXECUTE IMMEDIATE 'DROP INDEX '||p_IndexName;
    END IF;

    LogIt('Создание индекса '||p_IndexName);
    EXECUTE IMMEDIATE 'CREATE '||p_Unique||' INDEX '||p_IndexName||' ON '||p_TableName||' ('||p_Columns||')';
    LogIt('Создан индекс '||p_IndexName);
  EXCEPTION
    WHEN OTHERS THEN
      LogIt('Ошибка создания индекса '||p_IndexName);
  END;
BEGIN
  -- Создание таблицы UVALIDATE844DEALS_TMP
  CreateUValidate844Deals_tmp();
  CreateComments();
  CreateIndex('UVALIDATE844DEALS_TMP', 'UNIQUE', 'UVALIDATE844DEALS_IDX0', 'T_COL_NAME');
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
END;
/
