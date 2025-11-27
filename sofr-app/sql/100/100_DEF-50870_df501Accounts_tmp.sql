DECLARE
    e_table_does_not_exist EXCEPTION;
    PRAGMA EXCEPTION_INIT(e_table_does_not_exist, -942);
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE df501Accounts_tmp';
EXCEPTION
    WHEN e_table_does_not_exist THEN NULL;
END;
/

DECLARE
    e_object_exists EXCEPTION;
    PRAGMA EXCEPTION_INIT(e_object_exists, -955); 
BEGIN
    EXECUTE IMMEDIATE 
'CREATE GLOBAL TEMPORARY TABLE df501Accounts_tmp' ||
'(' ||
  't_chapter                 NUMBER(5),' ||
  't_isOpened                NUMBER(1),' ||
  't_account                 VARCHAR2(25),' ||
  't_accountId               NUMBER(10),' ||
  't_fiId                    NUMBER(10),' ||
  't_kind                    VARCHAR2(2),' ||
  't_balance                 VARCHAR2(25),' ||
  't_inCoverRest             NUMBER(32,12),' ||
  't_outCoverRest            NUMBER(32,12),' ||
  't_objectId                VARCHAR2(36),' ||
  't_operationDate           DATE,' ||
  't_daysToEnd               NUMBER(10),' ||
  't_contragentId            NUMBER(10),' ||
  't_part                    VARCHAR2(9),' ||
  't_debet                   NUMBER(32,12),' ||
  't_credit                  NUMBER(32,12),' ||
  't_isRepCategCC            NUMBER(1),' ||
  't_isDepositMargin         NUMBER(1),' ||
  't_isRepCategExcludeAcc    NUMBER(1),' ||
  't_isRepCategDiscount      NUMBER(1),' ||
  't_isRepCategBonus         NUMBER(1),' ||
  't_isDepositDealByCC       NUMBER(1),' ||
  't_isCategoryMarginSum     NUMBER(1),' ||
  't_isCategoryDiscount      NUMBER(1),' ||
  't_isCategoryBonus         NUMBER(1),' ||
  't_code                    VARCHAR2(35),' ||
  't_isBank                  NUMBER(1) ,' ||
  't_isCentralBank           NUMBER(1) ,' ||
  't_bankRegNumber           VARCHAR2(35),' ||
  't_isResident              NUMBER(1),' ||
  't_countryCode             VARCHAR2(3),' ||
  't_isSWIFT                 NUMBER(1),' ||
  't_name                    VARCHAR2(1023),' ||
  't_pseudonym               VARCHAR2(1023),' ||
  't_isNSPK                  NUMBER(1)      '  ||
')' ||
'ON COMMIT PRESERVE ROWS';

  
    EXECUTE IMMEDIATE  'COMMENT ON TABLE df501Accounts_tmp IS ''[RCB] Времменая таблица для формы 501. Данные по подсистеме "Главная книга"''';

    EXECUTE IMMEDIATE 'COMMENT ON COLUMN df501Accounts_tmp.t_chapter              IS ''Глава счета''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN df501Accounts_tmp.t_isOpened             IS ''Признак, открыт ли счет''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN df501Accounts_tmp.t_account              IS ''Номер счета''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN df501Accounts_tmp.t_accountId            IS ''Идентификатор лицевого счета''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN df501Accounts_tmp.t_fiId                 IS ''Идентификатор валюты счета''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN df501Accounts_tmp.t_kind                 IS ''Вид счета''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN df501Accounts_tmp.t_balance              IS ''Номер балансового счета''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN df501Accounts_tmp.t_inCoverRest          IS ''Входящий остаток в рублевом эквиваленте (на ДНОП)''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN df501Accounts_tmp.t_outCoverRest         IS ''Исходящий остаток в рублевом эквиваленте (за ДООП)''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN df501Accounts_tmp.t_objectId             IS ''Идентификатор счета, как объекта (не индексируемо)''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN df501Accounts_tmp.t_operationDate        IS ''Дата операции''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN df501Accounts_tmp.t_daysToEnd            IS ''Срок в днях до окончания операции''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN df501Accounts_tmp.t_contragentId         IS ''Идентификатор контрагента по счету''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN df501Accounts_tmp.t_part                 IS ''Раздел''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN df501Accounts_tmp.t_debet                IS ''Сумма дебетовых проводок с учетом исправительных оборотов''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN df501Accounts_tmp.t_credit               IS ''Сумма кредитных проводок с учетом исправительных оборотов''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN df501Accounts_tmp.t_isRepCategCC         IS ''На счете установлена категория "Категории для отчетности / Для формы 501" со значением "РЕПО ч/з ЦК"''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN df501Accounts_tmp.t_isRepCategCC         IS ''На счете установлена категория "Категории для отчетности / Для формы 501" со значением "Депозитная маржа"''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN df501Accounts_tmp.t_isRepCategExcludeAcc IS ''На счете установлена категория "Категории для отчетности / Для формы 501" со значением "Исключить из расчета"''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN df501Accounts_tmp.t_isRepCategDiscount   IS ''На счете установлена категория "Категории для отчетности / Приобретенные права требования / Счет по учету дисконта по приобретенным правам требования" со значением "Дисконт по ППТ"''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN df501Accounts_tmp.t_isRepCategBonus      IS ''На счете установлена категория "Категории для отчетности / Приобретенные права требования / Счет по учету премии по приобретенным правам требования" со значением "Премия по ППТ"''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN df501Accounts_tmp.t_isDepositDealByCC    IS ''На счете установлена категория "Категории для отчетности / Приобретенные права требования / Счет по учету премии по приобретенным правам требования" со значением "Депозитная сделка ч/з ЦК"''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN df501Accounts_tmp.t_isCategoryMarginSum  IS ''На счете установлена одна из категорий учета, или "-МС", или "+МС"''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN df501Accounts_tmp.t_isCategoryDiscount   IS ''На счете установлена категория учета "Дисконт_ПТ"''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN df501Accounts_tmp.t_isCategoryBonus      IS ''На счете установлена категория учета "Премия_ПТ"''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN df501Accounts_tmp.t_code                 IS ''Регистрационный номер''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN df501Accounts_tmp.t_isBank               IS ''Является банком''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN df501Accounts_tmp.t_isCentralBank        IS ''Является центральным банком''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN df501Accounts_tmp.t_bankRegNumber        IS ''Регистрационный номер КО''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN df501Accounts_tmp.t_isResident           IS ''Является резидентом''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN df501Accounts_tmp.t_countryCode          IS ''Страна''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN df501Accounts_tmp.t_isSWIFT              IS ''Является участником SWIFT''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN df501Accounts_tmp.t_name                 IS ''Наименование субъекта''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN df501Accounts_tmp.t_pseudonym            IS ''Псевдоним''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN df501Accounts_tmp.t_isNSPK               IS ''На контрагенте установлена принадлежность "Национальная система платежных карт"''';
EXCEPTION 
    WHEN e_object_exists THEN NULL; 
END;
/

declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where i.TABLE_NAME='df501Accounts_tmp' and i.INDEX_NAME='df501Accounts_tmp_IDX0' ;
  if cnt =1 then
    execute immediate 'drop index df501Accounts_tmp_IDX0';
  end if;
  execute immediate 'CREATE UNIQUE INDEX df501Accounts_tmp_IDX0 ON df501Accounts_tmp (t_account, t_part)';
end;
/

