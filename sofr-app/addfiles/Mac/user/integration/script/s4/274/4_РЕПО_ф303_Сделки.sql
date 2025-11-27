drop TABLE tRSHB_F303_2CHD_D;
CREATE TABLE tRSHB_F303_2CHD_D
(
     DEALID          NUMBER(15),
  DEALIDSSYLKA    NUMBER(15),
  INSTRUMENTTYPE  INTEGER,
  TP              VARCHAR2(50 BYTE),
  DEALNUM         VARCHAR2(30 BYTE),
  DEALTYPE        INTEGER,
  CONTRACTFLAG    INTEGER,
  DEALTYPEBRIEF   VARCHAR2(79 BYTE),
  TRADINGSYSNUM   CHAR(30 BYTE),
  DEALDATE        DATE,
  VALDATEPLAN     Date,
  VALDATEFACT     Date,
  FUNDID          NUMBER(15),
  FUNDBRIEF       VARCHAR2(5 BYTE),
  FUNDRASCHID     NUMBER(15),
  FUNDRASCHBRIEF  VARCHAR2(5 BYTE),
  VIDSD           VARCHAR2(30 BYTE),
  VID2332         VARCHAR2(30 BYTE),
  ОКАТО           VARCHAR2(30 BYTE),
  KATKACHESTVA    VARCHAR2(30 BYTE),
  IDKONTRBIS      CHAR(10 BYTE),
  TYPEKONTR       VARCHAR2(20 BYTE),
  CONTRNAME       VARCHAR2(255 BYTE),
  SECURITYID      NUMBER(15),
  ISIN            VARCHAR2(30 BYTE),
  PRCREPO         FLOAT(126),
  TYPEPRCREPO     VARCHAR2(20 BYTE),
  TSS             NUMBER(28,10),
  DEALQTY         NUMBER(28,10)
);

COMMENT ON TABLE tRSHB_F303_2CHD_D IS 'Сделки РЕПО. Форма 303 - источник данных для ЦХД';
COMMENT ON COLUMN tRSHB_F303_2CHD_D.dealid          IS 'Уникальный ID части сделки РЕПО';
COMMENT ON COLUMN tRSHB_F303_2CHD_D.dealidSsylka    IS 'Уникальный ID связанной части сделки РЕПО';
COMMENT ON COLUMN tRSHB_F303_2CHD_D.instrumenttype  IS 'Биржа/Внебиржа (Торг. площадка)';
COMMENT ON COLUMN tRSHB_F303_2CHD_D.TP              IS 'Наименование ТП';
COMMENT ON COLUMN tRSHB_F303_2CHD_D.Dealnum         IS 'Номер части сделки в Диасофт';
COMMENT ON COLUMN tRSHB_F303_2CHD_D.Dealtype        IS 'Тип сделки';
COMMENT ON COLUMN tRSHB_F303_2CHD_D.contractflag    IS 'Признак части РЕПО';
COMMENT ON COLUMN tRSHB_F303_2CHD_D.DealTypeBrief   IS 'Наименование типа сделки';
COMMENT ON COLUMN tRSHB_F303_2CHD_D.tradingsysnum   IS 'Номер сделки в торговой системе';
COMMENT ON COLUMN tRSHB_F303_2CHD_D.dealdate        IS 'Дата заключения сделки';
COMMENT ON COLUMN tRSHB_F303_2CHD_D.valdatePlan     IS 'Плановая дата расчетов по части сделки ';
COMMENT ON COLUMN tRSHB_F303_2CHD_D.valdateFact     IS 'Фактическая дата расчетов по части сделки';
COMMENT ON COLUMN tRSHB_F303_2CHD_D.Fundid          IS 'ID валюты сделки';
COMMENT ON COLUMN tRSHB_F303_2CHD_D.FundBrief       IS 'Валюта сделки';
COMMENT ON COLUMN tRSHB_F303_2CHD_D.FundRaschid     IS 'ID валюты расчетов';
COMMENT ON COLUMN tRSHB_F303_2CHD_D.FundRaschBrief  IS 'Валюта расчетов';
COMMENT ON COLUMN tRSHB_F303_2CHD_D.vidSD           IS 'Вид сделки';
COMMENT ON COLUMN tRSHB_F303_2CHD_D.ViD2332         IS 'Вид сделки 2332';
COMMENT ON COLUMN tRSHB_F303_2CHD_D.ОКАТО           IS 'ОКАТО ГО';
COMMENT ON COLUMN tRSHB_F303_2CHD_D.KatKachestva    IS 'Категория качества ';
COMMENT ON COLUMN tRSHB_F303_2CHD_D.IDKontrBis      IS 'Внешний код Контрагетнта в BISquit';
COMMENT ON COLUMN tRSHB_F303_2CHD_D.typeKontr       IS 'Тип Контрагента ';
COMMENT ON COLUMN tRSHB_F303_2CHD_D.ContrName       IS 'Наименование контрагента';
COMMENT ON COLUMN tRSHB_F303_2CHD_D.securityid      IS 'ID ценной бамаги в Diasoft';
COMMENT ON COLUMN tRSHB_F303_2CHD_D.ISIN            IS 'Код ISIN Ценной бумаги';
COMMENT ON COLUMN tRSHB_F303_2CHD_D.prcRepo         IS '% ставка РЕПО';
COMMENT ON COLUMN tRSHB_F303_2CHD_D.TypePrcRepo     IS 'Тип % ставки РЕПО';
COMMENT ON COLUMN tRSHB_F303_2CHD_D.TSS             IS 'Справедливая стоимость части сделки';
COMMENT ON COLUMN tRSHB_F303_2CHD_D.dealqty         IS 'Сумма сделки';
