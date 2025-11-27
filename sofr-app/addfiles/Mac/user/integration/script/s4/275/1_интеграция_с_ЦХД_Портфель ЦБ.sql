drop TABLE tRSHB_Portfolio_PKL_2CHD;
CREATE TABLE tRSHB_Portfolio_PKL_2CHD
(
     REPORT_DT                     DATE,
     PORTFOLIO                     VARCHAR2(25),
     FI_GROUP                      VARCHAR2(60),
     SECURITY_NAME                 VARCHAR2(25),
     ACCOUNT_NUMBER_1              VARCHAR2(5),
     ACCOUNT_NUMBER_2              VARCHAR2(20),
     CURRENCY                      VARCHAR2(5),
     ISSUER                        VARCHAR2(30),
     IS_NFO                        VARCHAR2(3),
     ISSUER_COUNTRY                VARCHAR2(30),
     COUNTRY_RATING                VARCHAR2(2),
     IS_INTERNATIONAL_ORG          VARCHAR2(3),
     IS_OESR_EUROZONE              VARCHAR2(3),
     IS_STATE_BORROW               VARCHAR2(3),
     SECURITY_TYPE                 VARCHAR2(5),
     IS_SPV                        VARCHAR2(3),
     IS_HYPO_COVER                 VARCHAR2(3),
     ISSUER_OKVED                  VARCHAR2(10),
     ISSUER_RATING_SP              VARCHAR2(4),
     ISSUE_RATING_SP               VARCHAR2(4),
     ISSUER_RATING_3453U_SP        VARCHAR2(4),
     ISSUE_RATING_3453U_SP         VARCHAR2(4),
     ISSUER_RATING_MOODYS          VARCHAR2(4),
     ISSUE_RATING_MOODYS           VARCHAR2(4),
     ISSUER_RATING_3453U_MOODYS    VARCHAR2(4),
     ISSUE_RATING_3453U_MOODYS     VARCHAR2(4),
     ISSUER_RATING_FITCH           VARCHAR2(4),
     ISSUE_RATING_FITCH            VARCHAR2(4),
     ISSUER_RATING_3453U_FITCH     VARCHAR2(4),
     ISSUE_RATING_3453U_FITCH      VARCHAR2(4),
     REG_NUMBER                    VARCHAR2(20),
     ISIN                          VARCHAR2(20),
     ISSUE_START_DT                DATE,
     REPAY_DT                      DATE,
     COUPON_REPAY_DT               DATE,
     AMOUNT                        NUMBER(24,2),
     UNKD_WITH_COUPON              NUMBER(24,2),
     COUPON_AMOUNT                 NUMBER(24,2),
     DISCOUNT_AMOUNT               NUMBER(24,2),
     PREMIUM_AMOUNT                NUMBER(24,2),
     BALANCE_PRICE                 NUMBER(24,2),
     OVERVALUE                     NUMBER(24,2),
     TSS_AMOUNT                    NUMBER(24,2),
     RESERVE_AMOUNT                NUMBER(24,2),
     GUARANTORS                    VARCHAR2(180),
     SEC_MARKET_PRICE_VAL          NUMBER(24,2),
     SEC_MARKET_PRICE_RUR          NUMBER(24,2),
     IS_IN_MMWB_INDEX_50           VARCHAR2(3),
     IS_IN_RTS_INDEX_50            VARCHAR2(3),
     DEVALUATION                   NUMBER(24,2),
     REPORT_LINE                   VARCHAR2(10),
     PKL_VALUE                     NUMBER(24,2)
);

COMMENT ON TABLE tRSHB_Portfolio_PKL_2CHD IS 'Портфель ЦБ - источник данных для ЦХД';
COMMENT ON COLUMN tRSHB_Portfolio_PKL_2CHD.REPORT_DT                      IS 'Дата отчета';
COMMENT ON COLUMN tRSHB_Portfolio_PKL_2CHD.PORTFOLIO                      IS 'Наименование портфеля ФИ';
COMMENT ON COLUMN tRSHB_Portfolio_PKL_2CHD.FI_GROUP                       IS 'Группа финансового инструмента';
COMMENT ON COLUMN tRSHB_Portfolio_PKL_2CHD.SECURITY_NAME                  IS 'ЦБ';
COMMENT ON COLUMN tRSHB_Portfolio_PKL_2CHD.ACCOUNT_NUMBER_1               IS 'Балансовый счет';
COMMENT ON COLUMN tRSHB_Portfolio_PKL_2CHD.ACCOUNT_NUMBER_2               IS 'Лицевой счет вложений';
COMMENT ON COLUMN tRSHB_Portfolio_PKL_2CHD.CURRENCY                       IS 'Валюта';
COMMENT ON COLUMN tRSHB_Portfolio_PKL_2CHD.ISSUER                         IS 'Эмитент';
COMMENT ON COLUMN tRSHB_Portfolio_PKL_2CHD.IS_NFO                         IS 'ФО/НФО';
COMMENT ON COLUMN tRSHB_Portfolio_PKL_2CHD.ISSUER_COUNTRY                 IS 'Страна Эмитента';
COMMENT ON COLUMN tRSHB_Portfolio_PKL_2CHD.COUNTRY_RATING                 IS 'Страновая оценка';
COMMENT ON COLUMN tRSHB_Portfolio_PKL_2CHD.IS_INTERNATIONAL_ORG           IS 'Признак международной организации';
COMMENT ON COLUMN tRSHB_Portfolio_PKL_2CHD.IS_OESR_EUROZONE               IS 'Признак ОЭСР, Еврозона';
COMMENT ON COLUMN tRSHB_Portfolio_PKL_2CHD.IS_STATE_BORROW                IS 'Признак права заимствования от имени государства';
COMMENT ON COLUMN tRSHB_Portfolio_PKL_2CHD.SECURITY_TYPE                  IS 'Тип ценной бумаги';
COMMENT ON COLUMN tRSHB_Portfolio_PKL_2CHD.IS_SPV                         IS 'SPV (да/нет)';
COMMENT ON COLUMN tRSHB_Portfolio_PKL_2CHD.IS_HYPO_COVER                  IS 'Ипотечноепокрытие (да/нет) ';
COMMENT ON COLUMN tRSHB_Portfolio_PKL_2CHD.ISSUER_OKVED                   IS 'ОКВЭД эмитента';
COMMENT ON COLUMN tRSHB_Portfolio_PKL_2CHD.ISSUER_RATING_SP               IS 'Рейтинг Эмитента (Standart and Poors)';
COMMENT ON COLUMN tRSHB_Portfolio_PKL_2CHD.ISSUE_RATING_SP                IS 'Рейтинг эмиссии (Standart and Poors)';
COMMENT ON COLUMN tRSHB_Portfolio_PKL_2CHD.ISSUER_RATING_3453U_SP         IS 'Рейтинг Эмитента в соответствии с  Указанием 3453-У (Standart and Poors)';
COMMENT ON COLUMN tRSHB_Portfolio_PKL_2CHD.ISSUE_RATING_3453U_SP          IS 'Рейтинг эмиссии в соответствии с  Указанием 3453-У (Standart and Poors)';
COMMENT ON COLUMN tRSHB_Portfolio_PKL_2CHD.ISSUER_RATING_MOODYS           IS 'Рейтинг Эмитента (Moodys )';
COMMENT ON COLUMN tRSHB_Portfolio_PKL_2CHD.ISSUE_RATING_MOODYS            IS 'Рейтинг эмиссии (Moodys) ';
COMMENT ON COLUMN tRSHB_Portfolio_PKL_2CHD.ISSUER_RATING_3453U_MOODYS     IS 'Рейтинг Эмитента в соответствии с  Указанием 3453-У (Moodys )';
COMMENT ON COLUMN tRSHB_Portfolio_PKL_2CHD.ISSUE_RATING_3453U_MOODYS      IS 'Рейтинг эмиссии в соответствии с  Указанием 3453-У (Moodys )';
COMMENT ON COLUMN tRSHB_Portfolio_PKL_2CHD.ISSUER_RATING_FITCH            IS 'Рейтинг Эмитента (Fitch Ratings)';
COMMENT ON COLUMN tRSHB_Portfolio_PKL_2CHD.ISSUE_RATING_FITCH             IS 'Рейтинг эмиссии (Fitch Ratings)';
COMMENT ON COLUMN tRSHB_Portfolio_PKL_2CHD.ISSUER_RATING_3453U_FITCH      IS 'Рейтинг Эмитента в соответствии с  Указанием 3453-У (Fitch Ratings)';
COMMENT ON COLUMN tRSHB_Portfolio_PKL_2CHD.ISSUE_RATING_3453U_FITCH       IS 'Рейтинг эмиссии в соответствии с  Указанием 3453-У (Fitch Ratings)';
COMMENT ON COLUMN tRSHB_Portfolio_PKL_2CHD.REG_NUMBER                     IS ' N гос. регистрации';
COMMENT ON COLUMN tRSHB_Portfolio_PKL_2CHD.ISIN                           IS 'ISIN';
COMMENT ON COLUMN tRSHB_Portfolio_PKL_2CHD.ISSUE_START_DT                 IS 'Дата начала размещения';
COMMENT ON COLUMN tRSHB_Portfolio_PKL_2CHD.REPAY_DT                       IS 'Дата погашения ЦБ';
COMMENT ON COLUMN tRSHB_Portfolio_PKL_2CHD.COUPON_REPAY_DT                IS 'Дата погашения купона';
COMMENT ON COLUMN tRSHB_Portfolio_PKL_2CHD.AMOUNT                         IS 'Сумма';
COMMENT ON COLUMN tRSHB_Portfolio_PKL_2CHD.UNKD_WITH_COUPON               IS 'УНКД с учетом погашенных купонов';
COMMENT ON COLUMN tRSHB_Portfolio_PKL_2CHD.COUPON_AMOUNT                  IS 'Начисленный купон';
COMMENT ON COLUMN tRSHB_Portfolio_PKL_2CHD.DISCOUNT_AMOUNT                IS 'Начисленный дисконт';
COMMENT ON COLUMN tRSHB_Portfolio_PKL_2CHD.PREMIUM_AMOUNT                 IS 'Премия';
COMMENT ON COLUMN tRSHB_Portfolio_PKL_2CHD.BALANCE_PRICE                  IS 'Балансовая стоимость';
COMMENT ON COLUMN tRSHB_Portfolio_PKL_2CHD.OVERVALUE                      IS 'Переоценка';
COMMENT ON COLUMN tRSHB_Portfolio_PKL_2CHD.TSS_AMOUNT                     IS 'ТСС';
COMMENT ON COLUMN tRSHB_Portfolio_PKL_2CHD.RESERVE_AMOUNT                 IS 'Резерв';
COMMENT ON COLUMN tRSHB_Portfolio_PKL_2CHD.GUARANTORS                     IS 'Поручители';
COMMENT ON COLUMN tRSHB_Portfolio_PKL_2CHD.SEC_MARKET_PRICE_VAL           IS 'Рыночная стоимость ценной бумаги (в валюте бумаги)';
COMMENT ON COLUMN tRSHB_Portfolio_PKL_2CHD.SEC_MARKET_PRICE_RUR           IS 'Рыночная стоимость ценной бумаги (в рублевом эквиваленте)';
COMMENT ON COLUMN tRSHB_Portfolio_PKL_2CHD.IS_IN_MMWB_INDEX_50            IS 'Включение биржей в списки для расчета Индекса ММВБ 50 (да/нет)';
COMMENT ON COLUMN tRSHB_Portfolio_PKL_2CHD.IS_IN_RTS_INDEX_50             IS 'Включение биржей в списки для расчета Индекса РТС 50 (да/нет)';
COMMENT ON COLUMN tRSHB_Portfolio_PKL_2CHD.DEVALUATION                    IS 'Значение показателя обесценения';
COMMENT ON COLUMN tRSHB_Portfolio_PKL_2CHD.REPORT_LINE                    IS 'Строка отчета';
COMMENT ON COLUMN tRSHB_Portfolio_PKL_2CHD.PKL_VALUE                      IS 'Величина, включаемая в расчет ПКЛ';
