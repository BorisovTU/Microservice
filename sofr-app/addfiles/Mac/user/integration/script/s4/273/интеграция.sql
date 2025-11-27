DROP TABLE TRSHB_REPO_PKL_2CHD CASCADE CONSTRAINTS;

CREATE TABLE TRSHB_REPO_PKL_2CHD
(
  REPORT_DT                   DATE,
  DEAL_NUMBER                 VARCHAR2(50 BYTE),
  REPO_DIRECTION              VARCHAR2(25 BYTE),
  REPO_EXCHANGE               VARCHAR2(25 BYTE),
  REPO_IN_REPO                VARCHAR2(5 BYTE),
  SUBJECT                     VARCHAR2(250 BYTE),
  SUBJ_RATING                 VARCHAR2(15 BYTE),
  SUBJ_COUNTRY                VARCHAR2(25 BYTE),
  DEAL_DT                     DATE,
  PART_1_EX_DT                DATE,
  PART_2_EX_DT                DATE,
  IS_LIQ_NETTING              VARCHAR2(5 BYTE),
  IS_CLC_NETTING              VARCHAR2(5 BYTE),
  IS_REPOSITORY               VARCHAR2(5 BYTE),
  ACCOUNT_BOND                VARCHAR2(20 BYTE),
  CURRENCY_BOND               VARCHAR2(3 BYTE),
  ISSUE_NAME                  VARCHAR2(25 BYTE),
  ISSUER_RATING_SP            VARCHAR2(4 BYTE),
  ISSUE_RATING_SP             VARCHAR2(4 BYTE),
  ISSUER_RATING_3453U_SP      VARCHAR2(4 BYTE),
  ISSUE_RATING_3453U_SP       VARCHAR2(4 BYTE),
  ISSUER_RATING_MOODYS        VARCHAR2(4 BYTE),
  ISSUE_RATING_MOODYS         VARCHAR2(4 BYTE),
  ISSUER_RATING_3453U_MOODYS  VARCHAR2(4 BYTE),
  ISSUE_RATING_3453U_MOODYS   VARCHAR2(4 BYTE),
  ISSUER_RATING_FITCH         VARCHAR2(4 BYTE),
  ISSUE_RATING_FITCH          VARCHAR2(4 BYTE),
  ISSUER_RATING_3453U_FITCH   VARCHAR2(4 BYTE),
  ISSUE_RATING_3453U_FITCH    VARCHAR2(4 BYTE),
  DEPLOY_DT                   DATE,
  ISSUER                      VARCHAR2(60 BYTE),
  IS_NFO                      VARCHAR2(3 BYTE),
  ISSUER_COUNTRY              VARCHAR2(25 BYTE),
  COUNTRY_RATING              VARCHAR2(2 BYTE),
  IS_SPV                      VARCHAR2(3 BYTE),
  IS_HYPO_COVER               VARCHAR2(3 BYTE),
  ISIN                        VARCHAR2(20 BYTE),
  SECURITY_TYPE               VARCHAR2(5 BYTE),
  QUANTITY                    NUMBER(15),
  BALANCE_PRICE               NUMBER(24,2),
  OVERPRICE                   NUMBER(24,2),
  TSS_AMOUNT                  NUMBER(24,2),
  ACCOUNT_MONEY               VARCHAR2(20 BYTE),
  CURRENCY_MONEY              VARCHAR2(3 BYTE),
  AMOUNT_MONEY                NUMBER(24,2),
  ACCOUNT_RERC                VARCHAR2(20 BYTE),
  AMOUNT_PERC                 NUMBER(24,2),
  IS_MARG_PAYMENT             VARCHAR2(5 BYTE),
  IS_NO_SELL_LIMIT            VARCHAR2(5 BYTE),
  IS_NO_RETURN_IN_30_DAYS     VARCHAR2(5 BYTE),
  IS_SUBORD_BOND              VARCHAR2(5 BYTE),
  IS_USE_FOR_INDEX            VARCHAR2(5 BYTE),
  RATE                        NUMBER(5,2),
  DEVALUATION                 NUMBER(24,2),
  REPORT_LINE                 VARCHAR2(10 BYTE),
  PKL_VALUE                   NUMBER(24,2)
)
TABLESPACE USERS
PCTUSED    0
PCTFREE    10
INITRANS   1
MAXTRANS   255
STORAGE    (
            INITIAL          64K
            NEXT             1M
            MAXSIZE          UNLIMITED
            MINEXTENTS       1
            MAXEXTENTS       UNLIMITED
            PCTINCREASE      0
            BUFFER_POOL      DEFAULT
           )
LOGGING 
NOCOMPRESS 
NOCACHE
MONITORING;

COMMENT ON TABLE TRSHB_REPO_PKL_2CHD IS 'Сделки РЕПО - источник данных для ЦХД';

COMMENT ON COLUMN TRSHB_REPO_PKL_2CHD.REPORT_DT IS 'Дата отчета';

COMMENT ON COLUMN TRSHB_REPO_PKL_2CHD.DEAL_NUMBER IS 'Номер сделки';

COMMENT ON COLUMN TRSHB_REPO_PKL_2CHD.REPO_DIRECTION IS 'Тип сделки 1 (прямое РЕПО / обратное РЕПО)';

COMMENT ON COLUMN TRSHB_REPO_PKL_2CHD.REPO_EXCHANGE IS 'Тип сделки 2 (биржевая / внебиржевая)';

COMMENT ON COLUMN TRSHB_REPO_PKL_2CHD.REPO_IN_REPO IS 'Тип сделки 3 ("РЕПО в РЕПО": да/нет)';

COMMENT ON COLUMN TRSHB_REPO_PKL_2CHD.SUBJECT IS 'Контрагент по сделке';

COMMENT ON COLUMN TRSHB_REPO_PKL_2CHD.SUBJ_RATING IS 'Рейтинг контрагента (Выше "В" / нет)';

COMMENT ON COLUMN TRSHB_REPO_PKL_2CHD.SUBJ_COUNTRY IS 'Страна контрагента';

COMMENT ON COLUMN TRSHB_REPO_PKL_2CHD.DEAL_DT IS 'Дата заключения сделки';

COMMENT ON COLUMN TRSHB_REPO_PKL_2CHD.PART_1_EX_DT IS 'Дата исполнения по первой части сделки';

COMMENT ON COLUMN TRSHB_REPO_PKL_2CHD.PART_2_EX_DT IS 'Дата исполнения по второй части сделки';

COMMENT ON COLUMN TRSHB_REPO_PKL_2CHD.IS_LIQ_NETTING IS 'Наличие в соглашении по сделке условия ликвидационного неттинга (ДА/НЕТ)';

COMMENT ON COLUMN TRSHB_REPO_PKL_2CHD.IS_CLC_NETTING IS 'Наличие в соглашении по сделке условия расчетного неттинга (ДА/НЕТ)';

COMMENT ON COLUMN TRSHB_REPO_PKL_2CHD.IS_REPOSITORY IS 'Сделка зарегистрирована в репозитарии (ДА/НЕТ)';

COMMENT ON COLUMN TRSHB_REPO_PKL_2CHD.ACCOUNT_BOND IS 'Лицевой счет по учету ценных бумаг';

COMMENT ON COLUMN TRSHB_REPO_PKL_2CHD.CURRENCY_BOND IS 'Валюта лицевого счета по учету ЦБ';

COMMENT ON COLUMN TRSHB_REPO_PKL_2CHD.ISSUE_NAME IS 'Наименование выпуска ценных бумаг';

COMMENT ON COLUMN TRSHB_REPO_PKL_2CHD.ISSUER_RATING_SP IS 'Рейтинг Эмитента (Standart and Poors)';

COMMENT ON COLUMN TRSHB_REPO_PKL_2CHD.ISSUE_RATING_SP IS 'Рейтинг эмиссии (Standart and Poors)';

COMMENT ON COLUMN TRSHB_REPO_PKL_2CHD.ISSUER_RATING_3453U_SP IS 'Рейтинг Эмитента в соответствии с  Указанием 3453-У (Standart and Poors)';

COMMENT ON COLUMN TRSHB_REPO_PKL_2CHD.ISSUE_RATING_3453U_SP IS 'Рейтинг эмиссии в соответствии с  Указанием 3453-У (Standart and Poors)';

COMMENT ON COLUMN TRSHB_REPO_PKL_2CHD.ISSUER_RATING_MOODYS IS 'Рейтинг Эмитента (Moodys )';

COMMENT ON COLUMN TRSHB_REPO_PKL_2CHD.ISSUE_RATING_MOODYS IS 'Рейтинг эмиссии (Moodys) ';

COMMENT ON COLUMN TRSHB_REPO_PKL_2CHD.ISSUER_RATING_3453U_MOODYS IS 'Рейтинг Эмитента в соответствии с  Указанием 3453-У (Moodys )';

COMMENT ON COLUMN TRSHB_REPO_PKL_2CHD.ISSUE_RATING_3453U_MOODYS IS 'Рейтинг эмиссии в соответствии с  Указанием 3453-У (Moodys )';

COMMENT ON COLUMN TRSHB_REPO_PKL_2CHD.ISSUER_RATING_FITCH IS 'Рейтинг Эмитента (Fitch Ratings)';

COMMENT ON COLUMN TRSHB_REPO_PKL_2CHD.ISSUE_RATING_FITCH IS 'Рейтинг эмиссии (Fitch Ratings)';

COMMENT ON COLUMN TRSHB_REPO_PKL_2CHD.ISSUER_RATING_3453U_FITCH IS 'Рейтинг Эмитента в соответствии с  Указанием 3453-У (Fitch Ratings)';

COMMENT ON COLUMN TRSHB_REPO_PKL_2CHD.ISSUE_RATING_3453U_FITCH IS 'Рейтинг эмиссии в соответствии с  Указанием 3453-У (Fitch Ratings)';

COMMENT ON COLUMN TRSHB_REPO_PKL_2CHD.DEPLOY_DT IS 'Дата начала размещения ЦБ';

COMMENT ON COLUMN TRSHB_REPO_PKL_2CHD.ISSUER IS 'Эмитент';

COMMENT ON COLUMN TRSHB_REPO_PKL_2CHD.IS_NFO IS 'ФО/НФО';

COMMENT ON COLUMN TRSHB_REPO_PKL_2CHD.ISSUER_COUNTRY IS 'Страна эмитента ';

COMMENT ON COLUMN TRSHB_REPO_PKL_2CHD.COUNTRY_RATING IS 'Страновая оценка';

COMMENT ON COLUMN TRSHB_REPO_PKL_2CHD.IS_SPV IS 'SPV (да/нет)';

COMMENT ON COLUMN TRSHB_REPO_PKL_2CHD.IS_HYPO_COVER IS 'Ипотечное покрытие (да/нет) ';

COMMENT ON COLUMN TRSHB_REPO_PKL_2CHD.ISIN IS 'ISIN';

COMMENT ON COLUMN TRSHB_REPO_PKL_2CHD.SECURITY_TYPE IS 'Тип ценной бумаги';

COMMENT ON COLUMN TRSHB_REPO_PKL_2CHD.QUANTITY IS 'Количество ценных бумаг (шт.)';

COMMENT ON COLUMN TRSHB_REPO_PKL_2CHD.BALANCE_PRICE IS 'Балансовая стоимость ценных бумаг ';

COMMENT ON COLUMN TRSHB_REPO_PKL_2CHD.OVERPRICE IS 'Переоценка ценных бумаг ';

COMMENT ON COLUMN TRSHB_REPO_PKL_2CHD.TSS_AMOUNT IS 'ТСС';

COMMENT ON COLUMN TRSHB_REPO_PKL_2CHD.ACCOUNT_MONEY IS 'Лицевой счет по учету денежных средств';

COMMENT ON COLUMN TRSHB_REPO_PKL_2CHD.CURRENCY_MONEY IS 'Валюта лицевого счета по учету денежных средств';

COMMENT ON COLUMN TRSHB_REPO_PKL_2CHD.AMOUNT_MONEY IS 'Сумма денежных средств ';

COMMENT ON COLUMN TRSHB_REPO_PKL_2CHD.ACCOUNT_RERC IS 'Лицевой счет по учету начисленных процентов по размещенным (привлеченным) средствам';

COMMENT ON COLUMN TRSHB_REPO_PKL_2CHD.AMOUNT_PERC IS 'Сумма начисленных процентов по размещенным (привлеченным) средствам';

COMMENT ON COLUMN TRSHB_REPO_PKL_2CHD.IS_MARG_PAYMENT IS 'Наличие в соглашение по сделке условия перечисления контрагенту маржинального взноса в случае текущей справедливой стоимости ценных бумаг (ДА/НЕТ)';

COMMENT ON COLUMN TRSHB_REPO_PKL_2CHD.IS_NO_SELL_LIMIT IS 'Отсутствия ограничений прав банка по продаже и передаче ЦБ по договорам репо (да/нет)';

COMMENT ON COLUMN TRSHB_REPO_PKL_2CHD.IS_NO_RETURN_IN_30_DAYS IS 'Невозможность требования о досрочном возврате ЦБ в течение ближайших 30 календарных дней (да/нет)';

COMMENT ON COLUMN TRSHB_REPO_PKL_2CHD.IS_SUBORD_BOND IS 'Субординированные облигации';

COMMENT ON COLUMN TRSHB_REPO_PKL_2CHD.IS_USE_FOR_INDEX IS 'Включение биржей в списки для расчета индексов ММВБ 50, РТС 50 и других (да/нет)';

COMMENT ON COLUMN TRSHB_REPO_PKL_2CHD.RATE IS 'Процентная ставка';

COMMENT ON COLUMN TRSHB_REPO_PKL_2CHD.DEVALUATION IS 'Показатель обесценения';

COMMENT ON COLUMN TRSHB_REPO_PKL_2CHD.REPORT_LINE IS 'Строка отчета';

COMMENT ON COLUMN TRSHB_REPO_PKL_2CHD.PKL_VALUE IS 'Величина, включаемая в расчет ПКЛ';