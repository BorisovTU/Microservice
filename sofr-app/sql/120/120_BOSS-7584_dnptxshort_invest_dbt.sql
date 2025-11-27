-- Таблица "DNPTXSHORT_INVEST_DBT"

CREATE TABLE DNPTXSHORT_INVEST_DBT (
      T_GUID         VARCHAR2(40)
    , T_ITOG         CHAR(1)
    , T_SORT         NUMBER(10)
    , T_SECCODE      VARCHAR2(15)  DEFAULT CHR(1)  
    , T_SECNAME      VARCHAR2(50)  DEFAULT CHR(1)  
    , T_QNTY         VARCHAR2(45)  DEFAULT CHR(1)  
    , T_CONTRACTS    VARCHAR2(64)  DEFAULT CHR(1)  
    , T_CODES        VARCHAR2(30)  DEFAULT CHR(1)  
    , T_DZS          VARCHAR2(40)  DEFAULT CHR(1)  
    , T_DPS          VARCHAR2(40)  DEFAULT CHR(1)  
    , T_PRSRUB       VARCHAR2(45)  DEFAULT CHR(1)  
    , T_ALLSRUB      VARCHAR2(45)  DEFAULT CHR(1)  
    , T_COMS         VARCHAR2(45)  DEFAULT CHR(1)  
    , T_CONTRACTB    VARCHAR2(64)  DEFAULT CHR(1)  
    , T_CODEB        VARCHAR2(30)  DEFAULT CHR(1)  
    , T_DZB          VARCHAR2(40)  DEFAULT CHR(1)  
    , T_DPB          VARCHAR2(40)  DEFAULT CHR(1)  
    , T_PRBRUB       VARCHAR2(45)  DEFAULT CHR(1)  
    , T_ALLBRUB      VARCHAR2(45)  DEFAULT CHR(1)  
    , T_COMB         VARCHAR2(45)  DEFAULT CHR(1)  
    , T_PLUSI        VARCHAR2(45)  DEFAULT CHR(1)  
    , T_FR_SEC_3Y    VARCHAR2(45)  DEFAULT CHR(1)  
    , T_TAXTAGSYEARS VARCHAR2(45)  DEFAULT CHR(1)  
)
/

COMMENT ON TABLE DNPTXSHORT_INVEST_DBT IS 'Данные листа РЕПО краткой справки'
/
COMMENT ON COLUMN DNPTXSHORT_INVEST_DBT.T_GUID         IS 'Идентификатор запуска'
/
COMMENT ON COLUMN DNPTXSHORT_INVEST_DBT.T_ITOG         IS 'Итоговая строка'
/
COMMENT ON COLUMN DNPTXSHORT_INVEST_DBT.T_SORT         IS 'Сортировка'
/
COMMENT ON COLUMN DNPTXSHORT_INVEST_DBT.T_SECCODE      IS 'Код ц/б'
/                                                      
COMMENT ON COLUMN DNPTXSHORT_INVEST_DBT.T_SECNAME      IS 'Название ц/б'                                                 
/
COMMENT ON COLUMN DNPTXSHORT_INVEST_DBT.T_QNTY         IS 'Количество, проданное в этой сделке продажи, из этой покупки' 
/
COMMENT ON COLUMN DNPTXSHORT_INVEST_DBT.T_CONTRACTS    IS 'Номер договора, в рамках которого заключена сделка'           
/
COMMENT ON COLUMN DNPTXSHORT_INVEST_DBT.T_CODES        IS 'Номер сделки'                                                 
/
COMMENT ON COLUMN DNPTXSHORT_INVEST_DBT.T_DZS          IS 'Дата заключения'                                              
/
COMMENT ON COLUMN DNPTXSHORT_INVEST_DBT.T_DPS          IS 'Дата оплаты'                                                  
/
COMMENT ON COLUMN DNPTXSHORT_INVEST_DBT.T_PRSRUB       IS 'Цена сделки, руб.'                                            
/
COMMENT ON COLUMN DNPTXSHORT_INVEST_DBT.T_ALLSRUB      IS 'Сумма сделки включая НКД, руб.'                               
/
COMMENT ON COLUMN DNPTXSHORT_INVEST_DBT.T_COMS         IS 'Всего комиссий, руб.'                                         
/
COMMENT ON COLUMN DNPTXSHORT_INVEST_DBT.T_CONTRACTB    IS 'Номер договора, в рамках которого заключена сделка'           
/
COMMENT ON COLUMN DNPTXSHORT_INVEST_DBT.T_CODEB        IS 'Номер сделки'                                                 
/
COMMENT ON COLUMN DNPTXSHORT_INVEST_DBT.T_DZB          IS 'Дата заключения'                                              
/
COMMENT ON COLUMN DNPTXSHORT_INVEST_DBT.T_DPB          IS 'Дата оплаты'                                                  
/
COMMENT ON COLUMN DNPTXSHORT_INVEST_DBT.T_PRBRUB       IS 'Цена сделки, руб.'                                            
/
COMMENT ON COLUMN DNPTXSHORT_INVEST_DBT.T_ALLBRUB      IS 'Сумма сделки включая НКД, руб.'                               
/
COMMENT ON COLUMN DNPTXSHORT_INVEST_DBT.T_COMB         IS 'Всего комиссий, руб.'                                         
/
COMMENT ON COLUMN DNPTXSHORT_INVEST_DBT.T_PLUSI        IS 'Доходы от реализации ц/б'                                     
/
COMMENT ON COLUMN DNPTXSHORT_INVEST_DBT.T_FR_SEC_3Y    IS 'Финансовый результат по ц/б'                                  
/
COMMENT ON COLUMN DNPTXSHORT_INVEST_DBT.T_TAXTAGSYEARS IS 'Срок нахождения ц\б в собственности налогоплательщика, года'
/

BEGIN
  EXECUTE IMMEDIATE 'DROP INDEX DNPTXSHORT_INVEST_DBT_IDX0';
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/

CREATE INDEX DNPTXSHORT_INVEST_DBT_IDX0 ON DNPTXSHORT_INVEST_DBT (
   T_GUID ASC, T_ITOG ASC, T_SORT ASC
)
/

