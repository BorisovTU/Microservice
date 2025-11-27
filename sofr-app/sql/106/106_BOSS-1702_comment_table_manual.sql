declare
    vcnt number;
    vsql varchar2(2000);
begin
   /* с учетом последовательности скриптов таблица уже существует, проверять не будем */
      execute immediate 'COMMENT ON COLUMN UREFILL_MANUAL_DBT.T_ID IS ''Автоинкремент''';
      execute immediate 'COMMENT ON COLUMN UREFILL_MANUAL_DBT.T_TYPE_REFILL_NAME IS ''Тип подкрепления - наименование''';
      execute immediate 'COMMENT ON COLUMN UREFILL_MANUAL_DBT.T_CONTRACT IS ''Cубдоговор - ID''';
      execute immediate 'COMMENT ON COLUMN UREFILL_MANUAL_DBT.T_CONTRACT_NUMBER IS ''Cубдоговор - номер''';
      execute immediate 'COMMENT ON COLUMN UREFILL_MANUAL_DBT.T_STATUS_NAME IS ''Статус операции - наименование''';
      execute immediate 'COMMENT ON COLUMN UREFILL_MANUAL_DBT.T_OPER IS ''Операционист - номер''';
      execute immediate 'COMMENT ON COLUMN UREFILL_MANUAL_DBT.T_OPER_NAME IS ''Операционист - наименование''';
end;


