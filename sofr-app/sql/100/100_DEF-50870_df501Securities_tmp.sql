DECLARE
    e_table_does_not_exist EXCEPTION;
    PRAGMA EXCEPTION_INIT(e_table_does_not_exist, -942);
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE df501Securities_tmp';
EXCEPTION
    WHEN e_table_does_not_exist THEN NULL;
END;
/

DECLARE
    e_object_exists EXCEPTION;
    PRAGMA EXCEPTION_INIT(e_object_exists, -955); 
BEGIN
    EXECUTE IMMEDIATE 
        'CREATE GLOBAL TEMPORARY TABLE df501Securities_tmp' ||
'(' ||
  't_account                   VARCHAR2(25),' ||
  't_accountId                 NUMBER(10),' ||
  't_contragentId              NUMBER(10),' ||
  't_openDate                  DATE,' ||
  't_debtDate                  DATE,' ||
  't_rate                      FLOAT(53),' ||
  't_isDebtRepo                VARCHAR2(3),' ||
  't_isDebtCentralCounterParty VARCHAR2(3),' ||
  't_isSecurities              NUMBER(1)' ||
')' ||
'ON COMMIT PRESERVE ROWS';

  EXECUTE IMMEDIATE 'COMMENT ON TABLE df501Securities_tmp IS ''[RCB] Времменая таблица для формы 501. Данные по бэк-офису RS-Securities.''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN df501Securities_tmp.t_account                    IS ''Номер счета в подсистеме "Главная книга"''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN df501Securities_tmp.t_accountId                  IS ''Идентификатор счета в подсистеме "Главная книга"''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN df501Securities_tmp.t_contragentId               IS ''Идентификатор контрагента счета''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN df501Securities_tmp.t_openDate                   IS ''Дата заключения сделки RS-Securities''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN df501Securities_tmp.t_debtDate                   IS ''Дата обязательства''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN df501Securities_tmp.t_rate                       IS ''Процентная ставка''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN df501Securities_tmp.t_isDebtRepo                 IS ''Счет является открытым по сделке РЕПО''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN df501Securities_tmp.t_isDebtCentralCounterparty  IS ''Счет является открытым по сделке, заключенной на бирже с участием центрального контрагента''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN df501Securities_tmp.t_isSecurities               IS ''Счет является счетом бэк-офиса RS-Securities''';
EXCEPTION 
    WHEN e_object_exists THEN NULL; 
END;
/

declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where i.TABLE_NAME='df501Securities_tmp' and i.INDEX_NAME='df501Securities_tmp_IDX0' ;
  if cnt =1 then
    execute immediate 'drop index df501Securities_tmp_IDX0';
  end if;
  execute immediate 'CREATE UNIQUE INDEX df501Securities_tmp_IDX0 ON df501Securities_tmp (t_accountId)';
end;
/

