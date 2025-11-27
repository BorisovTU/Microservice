drop TABLE tRSHB_PFI_PKL_2CHD;
CREATE TABLE tRSHB_PFI_PKL_2CHD
(
     REPORT_DT                     DATE,
     DEAL_NUMBER                   VARCHAR2(50),
     SUBJECT                       VARCHAR2(250),
     AGREEMENT_TYPE                VARCHAR2(20),
     SUBJECT_TYPE                  VARCHAR2(50),
     DEAL_TYPE                     VARCHAR2(15),
     COUNTRY                       VARCHAR2(25),
     COUNTRY_RATING                VARCHAR2(2),
     SUBJ_RATING                   VARCHAR2(4),
     SUBJ_RATING_3453U             VARCHAR2(4),
     FINSTR_NAME                   VARCHAR2(150),
     BASE_ACTIVE                   VARCHAR2(150),
     DEAL_SUPPLY                   VARCHAR2(25),
     DEAL_EXCHANGE                 VARCHAR2(25),
     DEAL_DIRECTION                VARCHAR2(25),
     BEGIN_DT                      DATE,
     END_DT                        DATE,
     ACCOUNT_DEMAND                VARCHAR2(20),
     CURRENCY_DEMAND               VARCHAR2(3),
     AMOUNT_DEMAND_VAL             NUMBER(24,2),
     AMOUNT_DEMAND_RUR             NUMBER(24,2),
     ACCOUNT_LIABILITY             VARCHAR2(20),
     CURRENCY_LIABILITY            VARCHAR2(3),
     AMOUNT_LIABILITY_VAL          NUMBER(24,2),
     AMOUNT_LIABILITY_RUR          NUMBER(24,2),
     ACCOUNT_SS                    VARCHAR2(20),
     AMOUNT_SS_ACTIVE              NUMBER(24,2),
     AMOUNT_SS_LIABILITY           NUMBER(24,2),
     IS_LIQ_NETTING                VARCHAR2(3),
     IS_CLC_NETTING                VARCHAR2(3),
     IS_REPOSITORY                 VARCHAR2(3),
     SWAP_PRC_CHANGE_DT            DATE,
     SWAP_BAL_ACC_FLOAT_RATE       VARCHAR2(5),
     SOURCE                        VARCHAR2(15)
);

COMMENT ON TABLE tRSHB_PFI_PKL_2CHD IS 'ПФИ - источник данных для ЦХД';
COMMENT ON COLUMN tRSHB_PFI_PKL_2CHD.REPORT_DT                      IS 'Дата отчета';
COMMENT ON COLUMN tRSHB_PFI_PKL_2CHD.DEAL_NUMBER                    IS 'Номер сделки';
COMMENT ON COLUMN tRSHB_PFI_PKL_2CHD.SUBJECT                        IS 'Контрагент';
COMMENT ON COLUMN tRSHB_PFI_PKL_2CHD.AGREEMENT_TYPE                 IS 'Соглашение на основание которого заключена сделка (RISDA/ISDA)';
COMMENT ON COLUMN tRSHB_PFI_PKL_2CHD.SUBJECT_TYPE                   IS 'Вид контрагента (КО/ЮЛ)';
COMMENT ON COLUMN tRSHB_PFI_PKL_2CHD.DEAL_TYPE                      IS 'Тип сделки (ПФИ/срочная)';
COMMENT ON COLUMN tRSHB_PFI_PKL_2CHD.COUNTRY                        IS 'Страна';
COMMENT ON COLUMN tRSHB_PFI_PKL_2CHD.COUNTRY_RATING                 IS 'Страновая оценка';
COMMENT ON COLUMN tRSHB_PFI_PKL_2CHD.SUBJ_RATING                    IS 'Рейтинг контрагента на отчетную дату ';
COMMENT ON COLUMN tRSHB_PFI_PKL_2CHD.SUBJ_RATING_3453U              IS 'Рейтинг контрагента в соответствии с  Указанием 3453-У';
COMMENT ON COLUMN tRSHB_PFI_PKL_2CHD.FINSTR_NAME                    IS 'Наименование инструмента ';
COMMENT ON COLUMN tRSHB_PFI_PKL_2CHD.BASE_ACTIVE                    IS 'Базовый актив';
COMMENT ON COLUMN tRSHB_PFI_PKL_2CHD.DEAL_SUPPLY                    IS 'Вид сделки (поставочная/ беспоставочная)';
COMMENT ON COLUMN tRSHB_PFI_PKL_2CHD.DEAL_EXCHANGE                  IS 'Вид сделки (биржевая/ внебиржевая)';
COMMENT ON COLUMN tRSHB_PFI_PKL_2CHD.DEAL_DIRECTION                 IS 'Направление сделки  (покупка/продажа)';
COMMENT ON COLUMN tRSHB_PFI_PKL_2CHD.BEGIN_DT                       IS 'Дата заключения сделки';
COMMENT ON COLUMN tRSHB_PFI_PKL_2CHD.END_DT                         IS 'Дата окончания сделки';
COMMENT ON COLUMN tRSHB_PFI_PKL_2CHD.ACCOUNT_DEMAND                 IS 'Лицевой счет требований ';
COMMENT ON COLUMN tRSHB_PFI_PKL_2CHD.CURRENCY_DEMAND                IS 'Валюта счета требований';
COMMENT ON COLUMN tRSHB_PFI_PKL_2CHD.AMOUNT_DEMAND_VAL              IS 'Сумма требований (в ин.валюте)';
COMMENT ON COLUMN tRSHB_PFI_PKL_2CHD.AMOUNT_DEMAND_RUR              IS 'Сумма требований (в руб.экв-те)';
COMMENT ON COLUMN tRSHB_PFI_PKL_2CHD.ACCOUNT_LIABILITY              IS 'Лицевой счет обязательств';
COMMENT ON COLUMN tRSHB_PFI_PKL_2CHD.CURRENCY_LIABILITY             IS 'Валюта счета обязательств';
COMMENT ON COLUMN tRSHB_PFI_PKL_2CHD.AMOUNT_LIABILITY_VAL           IS 'Сумма обязательств (в ин.валюте)';
COMMENT ON COLUMN tRSHB_PFI_PKL_2CHD.AMOUNT_LIABILITY_RUR           IS 'Сумма обязательств (в руб.эквиваленте)';
COMMENT ON COLUMN tRSHB_PFI_PKL_2CHD.ACCOUNT_SS                     IS 'Лицевой счет по учету справедливой стоимости ПФИ';
COMMENT ON COLUMN tRSHB_PFI_PKL_2CHD.AMOUNT_SS_ACTIVE               IS 'Справедливая стоимость актива';
COMMENT ON COLUMN tRSHB_PFI_PKL_2CHD.AMOUNT_SS_LIABILITY            IS 'Справедливая стоимость обязательства';
COMMENT ON COLUMN tRSHB_PFI_PKL_2CHD.IS_LIQ_NETTING                 IS 'Наличие в соглашении по сделке условия ликвидационного неттинга (ДА/НЕТ)';
COMMENT ON COLUMN tRSHB_PFI_PKL_2CHD.IS_CLC_NETTING                 IS 'Наличие в соглашении по сделке условия расчетного неттинга (ДА/НЕТ)';
COMMENT ON COLUMN tRSHB_PFI_PKL_2CHD.IS_REPOSITORY                  IS 'Сделка зарегистрирована в репозитарии (ДА/НЕТ)';
COMMENT ON COLUMN tRSHB_PFI_PKL_2CHD.SWAP_PRC_CHANGE_DT             IS 'Процентные свопы. Ближайшая дата пересмотра процентной ставки';
COMMENT ON COLUMN tRSHB_PFI_PKL_2CHD.SWAP_BAL_ACC_FLOAT_RATE        IS 'Процентные свопы. № счета 2-го порядка позиции с плавающей процентной ставкой.';
COMMENT ON COLUMN tRSHB_PFI_PKL_2CHD.SOURCE                         IS 'Наименование системы – источника (DIASOFT, FORTS, CFT и т.д.).';
