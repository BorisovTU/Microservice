DECLARE
    e_table_does_not_exist EXCEPTION;
    PRAGMA EXCEPTION_INIT(e_table_does_not_exist, -942);
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE df501MmDeals_tmp';
EXCEPTION
    WHEN e_table_does_not_exist THEN NULL;
END;
/

DECLARE
    e_object_exists EXCEPTION;
    PRAGMA EXCEPTION_INIT(e_object_exists, -955); 
BEGIN
    EXECUTE IMMEDIATE 
'CREATE GLOBAL TEMPORARY TABLE df501MmDeals_tmp' ||
'(' ||
  't_id                NUMBER(10),' ||
  't_mainRestAccount   VARCHAR2(35),' ||
  't_delaydMainRestAcc VARCHAR2(35),' ||
  't_plannedFinishDate DATE,' ||
  't_isOverNight       NUMBER(1),' ||
  't_kind              VARCHAR2(11),' ||
  't_contragentId      NUMBER(10),' ||
  't_isProlongingDeal  VARCHAR2(3),' ||
  't_rate              FLOAT(53),' ||
  't_part              NUMBER(1)' ||
')' ||
'ON COMMIT PRESERVE ROWS';

  EXECUTE IMMEDIATE 'COMMENT ON TABLE df501MmDeals_tmp IS ''[RCB] Времменая таблица для формы 501. Данные по подсистеме "Межбанковские кредиты".''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN df501MmDeals_tmp.t_id                IS ''Id сделки''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN df501MmDeals_tmp.t_mainRestAccount   IS ''Счет ОД''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN df501MmDeals_tmp.t_delaydMainRestAcc IS ''Счет просроченного ОД''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN df501MmDeals_tmp.t_plannedFinishDate IS ''Планируемая дата погашения''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN df501MmDeals_tmp.t_isOverNight       IS ''Сделка овернайт''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN df501MmDeals_tmp.t_kind              IS ''Вид сделки: Размещение, Привлечение''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN df501MmDeals_tmp.t_contragentId      IS ''Контрагент по сделке''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN df501MmDeals_tmp.t_isProlongingDeal  IS ''Пролонгирующая сделка''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN df501MmDeals_tmp.t_part              IS ''Часть отчета к которой отнесен договор''';
EXCEPTION 
    WHEN e_object_exists THEN NULL; 
END;
/

declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where i.TABLE_NAME='df501MmDeals_tmp' and i.INDEX_NAME='df501MmDeals_tmp_IDX0' ;
  if cnt =1 then
    execute immediate 'drop index df501MmDeals_tmp_IDX0';
  end if;
  execute immediate 'CREATE UNIQUE INDEX df501MmDeals_tmp_IDX0 ON df501MmDeals_tmp (t_id)';
end;
/

declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where i.TABLE_NAME='df501MmDeals_tmp' and i.INDEX_NAME='df501MmDeals_tmp_IDX1' ;
  if cnt =1 then
    execute immediate 'drop index df501MmDeals_tmp_IDX1';
  end if;
  execute immediate 'CREATE UNIQUE INDEX df501MmDeals_tmp_IDX1 ON df501MmDeals_tmp (t_mainRestAccount, t_delaydMainRestAcc)';
end;
/
