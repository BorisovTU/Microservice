-- Изменения по DEF-66483
-- Создание таблицы "uEntSofr_tmp", для создания темперной таблицы для хранения данных СОФРа по отчету-сверке 24
DECLARE
  logID VARCHAR2(32) := 'DEF-66483';
  -- Записать сообщение в itt_log
  PROCEDURE LogIt( p_message VARCHAR2 )
  AS
  BEGIN
    it_log.log(p_msg => logID||': '||p_message);
  END;
  -- Создание таблицы uEntSofr_tmp
  PROCEDURE CreateUEntSofr_tmp
  AS
    x_Cnt number;
  BEGIN
    SELECT count(*) INTO x_Cnt FROM user_tables WHERE upper(table_name) = 'UENTSOFR_TMP';
    IF( x_Cnt = 1 ) THEN
      LogIt('Таблица UENTSOFR_TMP существует.');
      LogIt('Удаление таблицы UENTSOFR_TMP.');
      EXECUTE IMMEDIATE 'DROP TABLE UENTSOFR_TMP';
      LogIt('Удалена таблица UENTSOFR_TMP.');
    END IF;
    LogIt('Создание таблицы UENTSOFR_TMP');
    EXECUTE IMMEDIATE 'CREATE GLOBAL TEMPORARY TABLE UENTSOFR_TMP (
        T_ACCTRNID NUMBER(10,0) NOT NULL, T_ACCOUNT_PAYER VARCHAR2(25), T_FIID_PAYER NUMBER(10,0)
        , T_ACCOUNT_RECEIVER VARCHAR2(25), T_FIID_RECEIVER NUMBER(10,0), PAYER_FORM NUMBER, RECEIVER_FORM NUMBER
        , FUTURE_DB VARCHAR2(35), FUTURE_CR VARCHAR2(35), BAL_DB VARCHAR2(5), BAL_CR VARCHAR2(5)
        , VAL_DB VARCHAR2(3), VAL_CR VARCHAR2(3), T_IDBISCOTTO VARCHAR2(255)
        , T_SUM_PAYER NUMBER(32,12), T_SUM_NATCUR NUMBER(32,12), T_DATE_CARRY DATE
        , T_NUMB_DOCUMENT VARCHAR2(15), T_GROUND VARCHAR2(600), FROM_CFT NUMBER
        , FROM_SOFR_PAY NUMBER, FROM_SOFR_TRN NUMBER, T_CURRENCY VARCHAR2(7), T_DOCNUM VARCHAR2(15)
        , T_SYNC_DB VARCHAR2(25), T_SYNC_CR VARCHAR2(25)
    ) ON COMMIT PRESERVE ROWS';
    LogIt('Создана таблица UENTSOFR_TMP');
  EXCEPTION
    WHEN OTHERS THEN
      LogIt('Ошибка создания таблицы UENTSOFR_TMP');
  END;
  PROCEDURE CreateColumnComments( p_TableName IN varchar2, p_ColumnName IN varchar2, p_Comment IN varchar2 )
  AS
  BEGIN
    execute immediate 'COMMENT ON COLUMN '||p_TableName||'.'||p_ColumnName||' IS '''||p_Comment||'''';
  END;
  -- Комментарии для таблицы UENTSOFR_TMP
  PROCEDURE CreateComments
  AS
    x_Cnt number;
  BEGIN
    execute immediate 'comment on table UENTSOFR_TMP is ''Данные СОФРа по отчету-сверке 24''';
    CreateColumnComments( 'UENTSOFR_TMP', 'T_ACCTRNID', 'ID проводки СОФР' );
    CreateColumnComments( 'UENTSOFR_TMP', 'T_ACCOUNT_PAYER', 'Счет по дебету' );
    CreateColumnComments( 'UENTSOFR_TMP', 'T_FIID_PAYER', 'Валюта по дебету' );
    CreateColumnComments( 'UENTSOFR_TMP', 'T_ACCOUNT_RECEIVER', 'Счет по кредиту' );
    CreateColumnComments( 'UENTSOFR_TMP', 'T_FIID_RECEIVER', 'Валюта по кредиту' );
    CreateColumnComments( 'UENTSOFR_TMP', 'BAL_DB', 'Балансовый счет по дебету' );
    CreateColumnComments( 'UENTSOFR_TMP', 'BAL_CR', 'Балансовый счет по кредиту' );
    CreateColumnComments( 'UENTSOFR_TMP', 'VAL_DB', 'Код валюты по дебету' );
    CreateColumnComments( 'UENTSOFR_TMP', 'VAL_CR', 'Код валюты по кредиту' );
    CreateColumnComments( 'UENTSOFR_TMP', 'T_IDBISCOTTO', 'ID проводки ЦФТ' );
    CreateColumnComments( 'UENTSOFR_TMP', 'T_SUM_PAYER', 'Сумма по дебету' );
    CreateColumnComments( 'UENTSOFR_TMP', 'T_SUM_NATCUR', 'Сумма в рублях' );
    CreateColumnComments( 'UENTSOFR_TMP', 'T_DATE_CARRY', 'Дата проводки' );
    CreateColumnComments( 'UENTSOFR_TMP', 'T_NUMB_DOCUMENT', 'Номер документа' );
    CreateColumnComments( 'UENTSOFR_TMP', 'T_GROUND', 'Основание проводки' );
    CreateColumnComments( 'UENTSOFR_TMP', 'T_CURRENCY', 'Валюта проводки' );
    CreateColumnComments( 'UENTSOFR_TMP', 'T_SYNC_DB', 'Сводный счет ЦФТ по дебету' );
    CreateColumnComments( 'UENTSOFR_TMP', 'T_SYNC_CR', 'Сводный счет ЦФТ по кредиту' );
  EXCEPTION
    WHEN OTHERS THEN
      LogIt('Ошибка добавления комментариев для таблицы UENTSOFR_TMP');
  END;
  -- Создание индекса UENTSOFR_TMP
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
  -- CreateUEntSofr_idx
  PROCEDURE CreateUEntSofr_idx
  AS
    x_Cnt number;
  BEGIN
    CreateIndex('UENTSOFR_TMP', 'UNIQUE', 'UENTSOFR_IDX0', 'T_ACCTRNID');
    CreateIndex('UENTSOFR_TMP', '', 'UENTSOFR_IDX1', 'T_ACCOUNT_PAYER');
    CreateIndex('UENTSOFR_TMP', '', 'UENTSOFR_IDX2', 'T_ACCOUNT_RECEIVER');
  EXCEPTION
    WHEN OTHERS THEN
      LogIt('Ошибка добавления комментариев для таблицы UENTSOFR_TMP');
  END;
BEGIN
  CreateUEntSofr_tmp();				-- Создание таблицы uEntSofr_tmp
  CreateComments();				-- Коментарии
  CreateUEntSofr_idx();				-- Индексы таблицы uEntSofr_tmp
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
END;
/
