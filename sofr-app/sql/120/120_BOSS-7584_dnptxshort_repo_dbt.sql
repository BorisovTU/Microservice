-- Таблица "DNPTXSHORT_REPO_DBT"

CREATE TABLE DNPTXSHORT_REPO_DBT (
      T_GUID         VARCHAR2(40)
    , T_ITOG         CHAR(1)
    , T_SORT         NUMBER(10)
    , T_SECCODE      VARCHAR2(15)  DEFAULT CHR(1) 
    , T_SECNAME      VARCHAR2(50)  DEFAULT CHR(1) 
    , T_CONTRACTS    VARCHAR2(64)  DEFAULT CHR(1) 
    , T_CODER        VARCHAR2(30)  DEFAULT CHR(1) 
    , T_DZ           VARCHAR2(40)  DEFAULT CHR(1) 
    , T_QNTY         VARCHAR2(45)  DEFAULT CHR(1) 
    , T_DIR1         VARCHAR2(45)  DEFAULT CHR(1) 
    , T_DD1          VARCHAR2(40)  DEFAULT CHR(1) 
    , T_DP1          VARCHAR2(40)  DEFAULT CHR(1) 
    , T_TOT1VP       VARCHAR2(45)  DEFAULT CHR(1) 
    , T_RATE         VARCHAR2(45)  DEFAULT CHR(1) 
    , T_DIR2         VARCHAR2(45)  DEFAULT CHR(1) 
    , T_DD2          VARCHAR2(40)  DEFAULT CHR(1) 
    , T_DP2          VARCHAR2(40)  DEFAULT CHR(1) 
    , T_DEAL2VP      VARCHAR2(45)  DEFAULT CHR(1) 
    , T_TOT2VP       VARCHAR2(45)  DEFAULT CHR(1) 
    , T_DOHRUB       VARCHAR2(45)  DEFAULT CHR(1) 
    , T_RASHRUB      VARCHAR2(45)  DEFAULT CHR(1) 
    , T_TAXPASHRUB   VARCHAR2(45)  DEFAULT CHR(1) 
    , T_GENRUB       VARCHAR2(45)  DEFAULT CHR(1) 
    , T_COM          VARCHAR2(45)  DEFAULT CHR(1) 
    , T_COUPCALCVN   VARCHAR2(45)  DEFAULT CHR(1) 
    , T_AMORTCALCVN  VARCHAR2(45)  DEFAULT CHR(1) 
    , T_SUMPRECISION NUMBER(5)     DEFAULT 0
)
/

COMMENT ON TABLE DNPTXSHORT_REPO_DBT IS 'Данные листа РЕПО краткой справки'
/
COMMENT ON COLUMN DNPTXSHORT_REPO_DBT.T_GUID         IS 'Идентификатор запуска'
/
COMMENT ON COLUMN DNPTXSHORT_REPO_DBT.T_ITOG         IS 'Итоговая строка'
/
COMMENT ON COLUMN DNPTXSHORT_REPO_DBT.T_SORT         IS 'Сортировка'
/
COMMENT ON COLUMN DNPTXSHORT_REPO_DBT.T_SECCODE      IS 'Код бумаги'
/
COMMENT ON COLUMN DNPTXSHORT_REPO_DBT.T_SECNAME      IS 'Выпуск ценной бумаги'
/
COMMENT ON COLUMN DNPTXSHORT_REPO_DBT.T_CONTRACTS    IS 'Номер договора, в рамках которого заключена сделка'
/
COMMENT ON COLUMN DNPTXSHORT_REPO_DBT.T_CODER        IS 'Номер сделки РЕПО'
/
COMMENT ON COLUMN DNPTXSHORT_REPO_DBT.T_DZ           IS 'Дата заключения сделки РЕПО'
/
COMMENT ON COLUMN DNPTXSHORT_REPO_DBT.T_QNTY         IS 'Кол-во ценных бумаг, штук'
/
COMMENT ON COLUMN DNPTXSHORT_REPO_DBT.T_DIR1         IS 'Направление 1 части'
/
COMMENT ON COLUMN DNPTXSHORT_REPO_DBT.T_DD1          IS 'Дата 1 части РЕПО (движение бумаг)'
/
COMMENT ON COLUMN DNPTXSHORT_REPO_DBT.T_DP1          IS 'Дата 1 части РЕПО   (движение денег)'
/
COMMENT ON COLUMN DNPTXSHORT_REPO_DBT.T_TOT1VP       IS 'Общая стоимость по 1 части РЕПО в валюте сделки'
/
COMMENT ON COLUMN DNPTXSHORT_REPO_DBT.T_RATE         IS 'Процент  РЕПО'
/
COMMENT ON COLUMN DNPTXSHORT_REPO_DBT.T_DIR2         IS 'Направление 2 части'
/
COMMENT ON COLUMN DNPTXSHORT_REPO_DBT.T_DD2          IS 'Дата 2 части РЕПО (движение бумаг)'
/
COMMENT ON COLUMN DNPTXSHORT_REPO_DBT.T_DP2          IS 'Дата 2 части РЕПО   (движение денег)'
/
COMMENT ON COLUMN DNPTXSHORT_REPO_DBT.T_DEAL2VP      IS 'Общая стоимость по 2 части РЕПО,  в валюте сделки'
/
COMMENT ON COLUMN DNPTXSHORT_REPO_DBT.T_TOT2VP       IS 'Стоимость 2 части РЕПО после учета промежуточных платежей,  в валюте сделки'
/
COMMENT ON COLUMN DNPTXSHORT_REPO_DBT.T_DOHRUB       IS 'Сумма % доходов в рублях'
/
COMMENT ON COLUMN DNPTXSHORT_REPO_DBT.T_RASHRUB      IS 'Фактические расходы, в рублях'
/
COMMENT ON COLUMN DNPTXSHORT_REPO_DBT.T_TAXPASHRUB   IS 'Расходы, принимаемые для целей налогового учета, руб.'
/
COMMENT ON COLUMN DNPTXSHORT_REPO_DBT.T_GENRUB       IS 'Сумма для налогооблажения'
/
COMMENT ON COLUMN DNPTXSHORT_REPO_DBT.T_COM          IS 'Комиссия по сделке РЕПО'
/
COMMENT ON COLUMN DNPTXSHORT_REPO_DBT.T_COUPCALCVN   IS 'Сумма купонов, подлежащих выплате эмитентом, в валюте номинала - за период с начала налогового периода по отчетную дату'
/
COMMENT ON COLUMN DNPTXSHORT_REPO_DBT.T_AMORTCALCVN  IS 'Сумма частичных погашений, подлежащих выплате от эмитента, в валюте номинала - за период с начала налогового периода по отчетную дату'
/
COMMENT ON COLUMN DNPTXSHORT_REPO_DBT.T_SUMPRECISION IS 'Точность количества'
/

BEGIN
  EXECUTE IMMEDIATE 'DROP INDEX DNPTXSHORT_REPO_DBT_IDX0';
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/

CREATE INDEX DNPTXSHORT_REPO_DBT_IDX0 ON DNPTXSHORT_REPO_DBT (
   T_GUID ASC, T_ITOG ASC, T_SORT ASC
)
/
