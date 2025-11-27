-- Таблица "UENT306COMPARE_TMP"

CREATE GLOBAL TEMPORARY TABLE UENT306COMPARE_TMP (
  -- Данные ЦФТ
    t_cft_date_carry 		DATE			-- Дата проводки
  , t_cft_doc_number 		VARCHAR2(15)		-- Номер документа
  , t_cft_sum 			NUMBER(32,12)		-- Сумма в валюте счета
  , t_cft_dbt_acc 		VARCHAR2(25)		-- Счет дебета
  , t_cft_dbt_cur 		VARCHAR2(3)		-- Валюта счета дебета
  , t_cft_crd_acc 		VARCHAR2(25)		-- Счет кредита
  , t_cft_crd_cur 		VARCHAR2(3)		-- Валюта счета кредита
  , t_cft_payer_acc 		VARCHAR2(25)		-- Счет плательщика
  , t_cft_receiver_acc 		VARCHAR2(25)		-- Счет получателя
  , t_cft_payer_name 		VARCHAR2(320)		-- Наименование плательщика
  , t_cft_receiver_name 	VARCHAR2(320)		-- Наименование получателя
  , t_cft_pay_purpose 		VARCHAR2(600)		-- Назначение платежа
  -- Данные СОФР
  , t_sofr_date_carry 		DATE			-- Дата проводки
  , t_sofr_doc_number 		VARCHAR2(15)		-- Номер документа
  , t_sofr_sum 			NUMBER(32,12)		-- Сумма в валюте счета
  , t_sofr_dbt_acc 		VARCHAR2(25)		-- Счет дебета
  , t_sofr_dbt_cur 		VARCHAR2(3)		-- Валюта счета дебета
  , t_sofr_crd_acc 		VARCHAR2(25)		-- Счет кредита
  , t_sofr_crd_cur 		VARCHAR2(3)		-- Валюта счета кредита
  , t_sofr_payer_name 		VARCHAR2(320)		-- Наименование плательщика
  , t_sofr_receiver_name 	VARCHAR2(320)		-- Наименование получателя
  , t_sofr_ground 		VARCHAR2(600)		-- Основание
  -- Сверка
  , t_check_pay 		NUMBER(1)		-- Квитовка платежей
  , t_check_cur 		NUMBER(1)		-- Проверка соответствия валюты
  , t_check_name 		NUMBER(1)		-- Проверка соответствия ФИО в СОФР и ЦФТ
  , t_check_acc 		NUMBER(1)		-- Проверка корректности корреспонденции
  , t_check_all 		NUMBER(1)		-- Итог сверки
) ON COMMIT PRESERVE ROWS
/

COMMENT ON TABLE UENT306COMPARE_TMP IS 'Таблица отчета-сверки зачислений-списаний ДС СОФР-ЦФТ'/

COMMENT ON COLUMN UENT306COMPARE_TMP.t_cft_date_carry IS 'ЦФТ. Дата проводки'/
COMMENT ON COLUMN UENT306COMPARE_TMP.t_cft_doc_number IS 'ЦФТ. Номер документа'/
COMMENT ON COLUMN UENT306COMPARE_TMP.t_cft_sum IS 'ЦФТ. Сумма в валюте счета'/
COMMENT ON COLUMN UENT306COMPARE_TMP.t_cft_dbt_acc IS 'ЦФТ. Счет дебета'/
COMMENT ON COLUMN UENT306COMPARE_TMP.t_cft_dbt_cur IS 'ЦФТ. Валюта счета дебета'/
COMMENT ON COLUMN UENT306COMPARE_TMP.t_cft_crd_acc IS 'ЦФТ. Счет кредита'/
COMMENT ON COLUMN UENT306COMPARE_TMP.t_cft_crd_cur IS 'ЦФТ. Валюта счета кредита'/
COMMENT ON COLUMN UENT306COMPARE_TMP.t_cft_payer_acc IS 'ЦФТ. Счет плательщика'/
COMMENT ON COLUMN UENT306COMPARE_TMP.t_cft_receiver_acc IS 'ЦФТ. Счет получателя'/
COMMENT ON COLUMN UENT306COMPARE_TMP.t_cft_payer_name IS 'ЦФТ. Наименование плательщика'/
COMMENT ON COLUMN UENT306COMPARE_TMP.t_cft_receiver_name IS 'ЦФТ. Наименование получателя'/
COMMENT ON COLUMN UENT306COMPARE_TMP.t_cft_pay_purpose IS 'ЦФТ. Назначение платежа'/

COMMENT ON COLUMN UENT306COMPARE_TMP.t_sofr_date_carry IS 'СОФР. Дата проводки'/
COMMENT ON COLUMN UENT306COMPARE_TMP.t_sofr_doc_number IS 'СОФР. Номер документа'/
COMMENT ON COLUMN UENT306COMPARE_TMP.t_sofr_sum IS 'СОФР. Сумма в валюте счета'/
COMMENT ON COLUMN UENT306COMPARE_TMP.t_sofr_dbt_acc IS 'СОФР. Счет дебета'/
COMMENT ON COLUMN UENT306COMPARE_TMP.t_sofr_dbt_cur IS 'СОФР. Валюта счета дебета'/
COMMENT ON COLUMN UENT306COMPARE_TMP.t_sofr_crd_acc IS 'СОФР. Счет кредита'/
COMMENT ON COLUMN UENT306COMPARE_TMP.t_sofr_crd_cur IS 'СОФР. Валюта счета кредита'/
COMMENT ON COLUMN UENT306COMPARE_TMP.t_sofr_payer_name IS 'СОФР. Наименование плательщика'/
COMMENT ON COLUMN UENT306COMPARE_TMP.t_sofr_receiver_name IS 'СОФР. Наименование получателя'/
COMMENT ON COLUMN UENT306COMPARE_TMP.t_sofr_ground IS 'СОФР. Основание'/

COMMENT ON COLUMN UENT306COMPARE_TMP.t_check_pay IS 'Сверка. Квитовка платежей'/
COMMENT ON COLUMN UENT306COMPARE_TMP.t_check_cur IS 'Сверка. Проверка соответствия валюты'/
COMMENT ON COLUMN UENT306COMPARE_TMP.t_check_name IS 'Сверка. Проверка соответствия ФИО в СОФР и ЦФТ'/
COMMENT ON COLUMN UENT306COMPARE_TMP.t_check_acc IS 'Сверка. Проверка  корректности корреспонденции'/
COMMENT ON COLUMN UENT306COMPARE_TMP.t_check_all IS 'Сверка. Итог сверки'/

