drop TABLE tRSHB_F303_2CHD_A;
CREATE TABLE tRSHB_F303_2CHD_A
(
     dealid       Numeric(15,0),
     resourceid   Numeric(15,0),
     AccType      varchar(30),
     AccBrief     varchar(30),
     AccName      varchar(255)
);

COMMENT ON TABLE tRSHB_F303_2CHD_A IS 'Сделки РЕПО. Форма 303. Счета - источник данных для ЦХД';
COMMENT ON COLUMN tRSHB_F303_2CHD_A.dealid      IS 'Уникальный ID части сделки РЕПО';
COMMENT ON COLUMN tRSHB_F303_2CHD_A.resourceid  IS 'ID счета в Диасофт';
COMMENT ON COLUMN tRSHB_F303_2CHD_A.AccType     IS 'Тип счета в Диасофт';
COMMENT ON COLUMN tRSHB_F303_2CHD_A.AccBrief    IS 'Номер счета ';
COMMENT ON COLUMN tRSHB_F303_2CHD_A.AccName     IS 'Наименование счета';
