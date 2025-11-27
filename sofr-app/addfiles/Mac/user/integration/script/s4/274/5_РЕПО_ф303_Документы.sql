drop TABLE tRSHB_F303_2CHD_P;
CREATE TABLE tRSHB_F303_2CHD_P
(
     DEALID         NUMBER(15),
  OPERDATE       date,
  BATCH          VARCHAR2(30 BYTE),
  DEBRESOURCEID  NUMBER(15),
  DEBACC         VARCHAR2(30 BYTE),
  DEBFUNDID      NUMBER(15),
  DEBFUNDBRIEF   VARCHAR2(5 BYTE),
  DEBCOURSE      FLOAT(126),
  DEBQTY         NUMBER(28,10),
  DEBQTYRUB      NUMBER(28,10),
  CRERESOURCEID  NUMBER(15),
  CREACC         VARCHAR2(30 BYTE),
  CREFUNDID      NUMBER(15),
  CREFUNDBRIEF   VARCHAR2(5 BYTE),
  CRECOURSE      FLOAT(126),
  CREQTY         NUMBER(28,10),
  CREQTYRUB      NUMBER(28,10),
  KOMMENT        VARCHAR2(255 BYTE)/*надо Comment, но не дает создать с таким именем*/
);

COMMENT ON TABLE tRSHB_F303_2CHD_P IS 'Сделки РЕПО. Форма 303. Документы - источник данных для ЦХД';
COMMENT ON COLUMN tRSHB_F303_2CHD_P.dealid         IS 'Уникальный ID части сделки РЕПО';
COMMENT ON COLUMN tRSHB_F303_2CHD_P.operdate       IS 'Дата документа';
COMMENT ON COLUMN tRSHB_F303_2CHD_P.Batch          IS 'Пачка документа';
COMMENT ON COLUMN tRSHB_F303_2CHD_P.DebResourceid  IS 'ID счета по Дебету в Диасофт';
COMMENT ON COLUMN tRSHB_F303_2CHD_P.DebAcc         IS 'Номер счета по Дебету';
COMMENT ON COLUMN tRSHB_F303_2CHD_P.Debfundid      IS 'ID валюты по Дебету в Диасофт';
COMMENT ON COLUMN tRSHB_F303_2CHD_P.DebfundBrief   IS 'Валюта по дебету в Диасофт';
COMMENT ON COLUMN tRSHB_F303_2CHD_P.DebCourse      IS 'Курс валюты ';
COMMENT ON COLUMN tRSHB_F303_2CHD_P.DebQty         IS 'Cумма документа по дебету';
COMMENT ON COLUMN tRSHB_F303_2CHD_P.DebQtyRub      IS 'Cумма документа по дебету в Рублях РФ';
COMMENT ON COLUMN tRSHB_F303_2CHD_P.CreResourceid  IS 'ID счета по Кредиту в Диасофт';
COMMENT ON COLUMN tRSHB_F303_2CHD_P.CreAcc         IS 'Номер счета по Кредиту';
COMMENT ON COLUMN tRSHB_F303_2CHD_P.Crefundid      IS 'ID валюты по Кредиту в Диасофт';
COMMENT ON COLUMN tRSHB_F303_2CHD_P.CrefundBrief   IS 'Валюта по Кредиту в Диасофт';
COMMENT ON COLUMN tRSHB_F303_2CHD_P.CreCourse      IS 'Курс валюты ';
COMMENT ON COLUMN tRSHB_F303_2CHD_P.CreQty         IS 'Cумма документа по Кредиту';
COMMENT ON COLUMN tRSHB_F303_2CHD_P.CreQtyRub      IS 'Cумма документа по Кредиту в Рублях РФ';
COMMENT ON COLUMN tRSHB_F303_2CHD_P.Komment        IS 'Назначение документа';
