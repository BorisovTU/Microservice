DECLARE
    e_table_does_not_exist EXCEPTION;
    PRAGMA EXCEPTION_INIT(e_table_does_not_exist, -942);
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE df501MmDeals_tmp';
EXCEPTION
    WHEN e_table_does_not_exist THEN NULL;
END;
/

CREATE GLOBAL TEMPORARY TABLE df501MmDeals_tmp
(
  t_id                NUMBER(10),
  t_mainRestAccount   VARCHAR2(35),
  t_delaydMainRestAcc VARCHAR2(35),
  t_plannedFinishDate DATE,
  t_isOverNight       NUMBER(1),
  t_kind              VARCHAR2(11),
  t_contragentId      NUMBER(10),
  t_isProlongingDeal  VARCHAR2(3),
  t_rate              FLOAT(53),
  t_part              NUMBER(1)
) ON COMMIT PRESERVE ROWS
/

CREATE UNIQUE INDEX df501MmDeals_tmp_IDX0 ON df501MmDeals_tmp(t_id)
/

CREATE INDEX df501MmDeals_tmp_IDX1 ON df501MmDeals_tmp(t_mainRestAccount, t_delaydMainRestAcc)
/

COMMENT ON TABLE df501MmDeals_tmp IS '[RCB] Времменая таблица для формы 501. Данные по подсистеме "Межбанковские кредиты".' /
COMMENT ON COLUMN df501MmDeals_tmp.t_id                IS 'Id сделки' /
COMMENT ON COLUMN df501MmDeals_tmp.t_mainRestAccount   IS 'Счет ОД' /
COMMENT ON COLUMN df501MmDeals_tmp.t_delaydMainRestAcc IS 'Счет просроченного ОД' /
COMMENT ON COLUMN df501MmDeals_tmp.t_plannedFinishDate IS 'Планируемая дата погашения' /
COMMENT ON COLUMN df501MmDeals_tmp.t_isOverNight       IS 'Сделка овернайт' /
COMMENT ON COLUMN df501MmDeals_tmp.t_kind              IS 'Вид сделки: Размещение, Привлечение' /
COMMENT ON COLUMN df501MmDeals_tmp.t_contragentId      IS 'Контрагент по сделке' /
COMMENT ON COLUMN df501MmDeals_tmp.t_isProlongingDeal  IS 'Пролонгирующая сделка' /
COMMENT ON COLUMN df501MmDeals_tmp.t_rate              IS 'Процентная ставка' /
COMMENT ON COLUMN df501MmDeals_tmp.t_part              IS 'Часть отчета к которой отнесен договор' /
