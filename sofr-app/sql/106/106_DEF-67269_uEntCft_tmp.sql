-- Изменения по DEF-67269
-- 1) Создание таблицы "uEntCft_tmp"
-- 2) Создание таблицы "uEntSofr_tmp" (уже была, изменяется, добавляются индексы)
DECLARE
  logID VARCHAR2(32) := 'DEF-67269';
  -- Записать сообщение в itt_log
  PROCEDURE LogIt( p_message VARCHAR2 )
  AS
  BEGIN
    it_log.log(p_msg => logID||': '||p_message);
  END;
  -- Создание комментария
  PROCEDURE CreateColumnComments( p_TableName IN varchar2, p_ColumnName IN varchar2, p_Comment IN varchar2 )
  AS
  BEGIN
    execute immediate 'COMMENT ON COLUMN '||p_TableName||'.'||p_ColumnName||' IS '''||p_Comment||'''';
  END;
  -- Создание индекса
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
  -- Создание таблицы UEntCft_tmp
  PROCEDURE CreateUEntCft_tmp
  AS
    x_Cnt number;
  BEGIN
    SELECT count(*) INTO x_Cnt FROM user_tables WHERE upper(table_name) = 'UENTCFT_TMP';
    IF( x_Cnt = 1 ) THEN
      LogIt('Таблица UENTCFT_TMP существует.');
      LogIt('Удаление таблицы UENTCFT_TMP.');
      EXECUTE IMMEDIATE 'DROP TABLE UENTCFT_TMP';
      LogIt('Удалена таблица UENTCFT_TMP.');
    END IF;
    LogIt('Создание таблицы UENTCFT_TMP');
    EXECUTE IMMEDIATE 'CREATE GLOBAL TEMPORARY TABLE UENTCFT_TMP (
      T_AUTOKEY NUMBER(10,0), T_COUNTER VARCHAR2(15), T_USERID VARCHAR2(15)
      , T_OPDATE DATE, BAL_DB VARCHAR2(5), BAL_CR VARCHAR2(5), VAL_DB VARCHAR2(3), VAL_CR VARCHAR2(3)
      , T_DOCDATE DATE, T_ACCT_DB VARCHAR2(25), T_ACCT_CR VARCHAR2(25), T_CURRENCY VARCHAR2(25)
      , T_AMTCUR NUMBER(32,12), T_AMTRUB NUMBER(32,12), T_DETAILS VARCHAR2(600), T_DOCNUM VARCHAR2(15)
      , T_ACCTRNID NUMBER(10,0), T_IDBISCOTTO VARCHAR2(20), T_REQID VARCHAR2(40), NOT_LOADED VARCHAR2(2)
      , matched_date char(1) default ''0''
    ) ON COMMIT PRESERVE ROWS';
    LogIt('Создана таблица UENTCFT_TMP');
    -- Индексы для таблицы UENTCFT_TMP
    LogIt('Создание индексов для UENTCFT_TMP');
    CreateIndex('UENTCFT_TMP', '', 'UENTCFT_IDX0', 'T_IDBISCOTTO');
    CreateIndex('UENTCFT_TMP', '', 'UENTCFT_IDX1', 'T_OPDATE');
    CreateIndex('UENTCFT_TMP', '', 'UENTCFT_IDX2', 'T_DOCDATE');
    CreateIndex('UENTCFT_TMP', '', 'UENTCFT_IDX3', 'MATCHED_DATE');
    LogIt('Созданы индексы для UENTCFT_TMP');
    -- Комментарии для таблицы UENTCFT_TMP
    LogIt('Создание комментариев для UENTCFT_TMP');
    execute immediate 'comment on table UENTCFT_TMP is ''Таблица для проводок из ЦФТ для отчета-сверки 24''';
    CreateColumnComments( 'UENTCFT_TMP', 'T_AUTOKEY', 'ключ из файла загрузки' );
    CreateColumnComments( 'UENTCFT_TMP', 'T_COUNTER', 'номер' );
    CreateColumnComments( 'UENTCFT_TMP', 'T_OPDATE', 'дата проводки' );
    CreateColumnComments( 'UENTCFT_TMP', 'BAL_DB', 'балансовый счет по дебету' );
    CreateColumnComments( 'UENTCFT_TMP', 'BAL_CR', 'балансовый счет по дебету' );
    CreateColumnComments( 'UENTCFT_TMP', 'VAL_DB', 'валюта по дебету' );
    CreateColumnComments( 'UENTCFT_TMP', 'VAL_CR', 'валюта по дебету' );
    CreateColumnComments( 'UENTCFT_TMP', 'T_DOCDATE', 'дата документа' );
    CreateColumnComments( 'UENTCFT_TMP', 'T_ACCT_DB', 'счет по дебету' );
    CreateColumnComments( 'UENTCFT_TMP', 'T_ACCT_CR', 'счет по дебету' );
    CreateColumnComments( 'UENTCFT_TMP', 'T_CURRENCY', 'валюта проводки' );
    CreateColumnComments( 'UENTCFT_TMP', 'T_AMTCUR', 'сумма в валюте проводки' );
    CreateColumnComments( 'UENTCFT_TMP', 'T_AMTRUB', 'сумма проводки в рублях' );
    CreateColumnComments( 'UENTCFT_TMP', 'T_DETAILS', 'основание проводки' );
    CreateColumnComments( 'UENTCFT_TMP', 'T_DOCNUM', 'номер документа' );
    CreateColumnComments( 'UENTCFT_TMP', 'T_ACCTRNID', 'ID проводки СОФР' );
    CreateColumnComments( 'UENTCFT_TMP', 'T_IDBISCOTTO', 'ID проводки ЦФТ' );
    CreateColumnComments( 'UENTCFT_TMP', 'T_REQID', 'номер выгрузки ЦФТ' );
    CreateColumnComments( 'UENTCFT_TMP', 'matched_date', 'если 1, то проводки соотетствуют датам из СОФР' );
    LogIt('Созданы комментарии для UENTCFT_TMP');
  EXCEPTION
    WHEN OTHERS THEN
      LogIt(SQLERRM);
      LogIt('Ошибка создания таблицы UENTCFT_TMP');
  END;
  -- Создание таблицы UEntSofr_tmp
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
        , matched_date char(1) default ''0''
    ) ON COMMIT PRESERVE ROWS';
    LogIt('Создана таблица UENTSOFR_TMP');
    -- Индексы для таблицы UENTSOFR_TMP
    LogIt('Создание индексов для UENTSOFR_TMP');
    CreateIndex('UENTSOFR_TMP', 'UNIQUE', 'UENTSOFR_IDX0', 'T_ACCTRNID');
    CreateIndex('UENTSOFR_TMP', '', 'UENTSOFR_IDX1', 'T_ACCOUNT_PAYER');
    CreateIndex('UENTSOFR_TMP', '', 'UENTSOFR_IDX2', 'T_ACCOUNT_RECEIVER');
    CreateIndex('UENTSOFR_TMP', '', 'UENTSOFR_IDX3', 'T_DATE_CARRY');
    CreateIndex('UENTSOFR_TMP', '', 'UENTSOFR_IDX4', 'MATCHED_DATE');
    LogIt('Созданы индексы для UENTSOFR_TMP');
    -- Комментарии для таблицы UENTSOFR_TMP
    LogIt('Создание комментариев для UENTSOFR_TMP');
    execute immediate 'comment on table UENTSOFR_TMP is ''Таблица для проводок СОФРа по отчету-сверке 24''';
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
    CreateColumnComments( 'UENTSOFR_TMP', 'matched_date', 'если 1, то проводки соотетствуют датам из ЦФТ' );
    LogIt('Созданы комментарии для UENTSOFR_TMP');
  EXCEPTION
    WHEN OTHERS THEN
      LogIt(SQLERRM);
      LogIt('Ошибка создания таблицы UENTSOFR_TMP');
  END;
BEGIN
  CreateUEntCft_tmp();					-- 1) Создание таблицы UENTCFT_TMP
  CreateUEntSofr_tmp();					-- 2) Создание таблицы UENTSOFR_TMP
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
END;
/
