-- Изменения по BOSS-2056_BOSS-2838
-- Изменение таблицы "UACCTRNCOMPARE_DBT", добавление полей t_currency и t_docnum
DECLARE
  logID VARCHAR2(32) := 'BOSS-2056_BOSS-2838';
  -- Записать сообщение в itt_log
  PROCEDURE LogIt( p_message VARCHAR2 )
  AS
  BEGIN
    it_log.log(p_msg => logID||': '||p_message);
  END;
  -- Коментарий поля таблицы
  PROCEDURE CreateColumnComments( p_TableName IN varchar2, p_ColumnName IN varchar2, p_Comment IN varchar2 )
  AS
  BEGIN
    execute immediate 'COMMENT ON COLUMN '||p_TableName||'.'||p_ColumnName||' IS '''||p_Comment||'''';
  EXCEPTION WHEN OTHERS THEN
    NULL;
  END;
  -- Изменение таблицы
  PROCEDURE AlterTable( p_TableName IN varchar2, p_ColumnName IN varchar2, p_ColumnType IN varchar2, p_Comment IN varchar2 )
  AS
    x_Cnt number;
  BEGIN
    SELECT count(*) INTO x_Cnt FROM user_tables WHERE upper(table_name) = upper(p_TableName);
    IF( x_Cnt = 1 ) THEN
      BEGIN
        LogIt('Добавление поля '||p_TableName||'.'||p_ColumnName );
        EXECUTE IMMEDIATE 'ALTER TABLE '||p_TableName||' ADD ('||p_ColumnName||' '||p_ColumnType||')';
        LogIt('Добавлено поле '||p_TableName||'.'||p_ColumnName );
      EXCEPTION WHEN OTHERS THEN
        LogIt('Ошибка добавления поля '||p_TableName||'.'||p_ColumnName );
      END;
      CreateColumnComments( p_TableName, p_ColumnName, p_Comment );
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      LogIt('Ошибка изменения таблицы '||p_TableName);
  END;
BEGIN
  -- Создание таблицы UACCTRNCOMPARE_DBT
  AlterTable('UACCTRNCOMPARE_DBT', 'T_CURRENCY', 'VARCHAR2(25)', 'код валюты');
  AlterTable('UACCTRNCOMPARE_DBT', 'T_DOCNUM', 'VARCHAR2(15)', 'номер докум.');
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
END;
/
