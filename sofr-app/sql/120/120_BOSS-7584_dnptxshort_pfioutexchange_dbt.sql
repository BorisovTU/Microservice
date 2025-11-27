-- Таблица "DNPTXSHORT_PFIOUTEXCHANGE_DBT"

CREATE TABLE DNPTXSHORT_PFIOUTEXCHANGE_DBT (
      T_GUID                 VARCHAR2(40)
    , T_ITOG                 CHAR(1)
    , T_SORT                 NUMBER(10)
    , T_CLIENT               VARCHAR2(100)  DEFAULT CHR(1)  
    , T_CLIENTSTATUS         VARCHAR2(30)   DEFAULT CHR(1)  
    , T_CONTRACTS            VARCHAR2(64)   DEFAULT CHR(1)  
    , T_DEALDATE             VARCHAR2(40)   DEFAULT CHR(1)  
    , T_CONTRACTN            VARCHAR2(64)   DEFAULT CHR(1)  
    , T_CONTRACTTYPE         VARCHAR2(64)   DEFAULT CHR(1)  
    , T_DIRECTION            VARCHAR2(45)   DEFAULT CHR(1)  
    , T_CONTRACT             VARCHAR2(64)   DEFAULT CHR(1)  
    , T_BASETYPE             VARCHAR2(64)   DEFAULT CHR(1)  
    , T_FITYPE               VARCHAR2(50)   DEFAULT CHR(1)  
    , T_EXPIRATIONDATE       VARCHAR2(40)   DEFAULT CHR(1)  
    , T_Q                    VARCHAR2(45)   DEFAULT CHR(1)  
    , T_CONTRAGENT           VARCHAR2(100)  DEFAULT CHR(1)  
    , T_EXPIRATIONPRICECUR   VARCHAR2(45)   DEFAULT CHR(1)  
    , T_PRICECURRENCY        VARCHAR2(45)   DEFAULT CHR(1)  
    , T_MARKETPRICE          VARCHAR2(45)   DEFAULT CHR(1)  
    , T_MATERIAL             VARCHAR2(45)   DEFAULT CHR(1)  
    , T_VARPOS               VARCHAR2(45)   DEFAULT CHR(1)  
    , T_VARNEG               VARCHAR2(45)   DEFAULT CHR(1)  
    , T_PREMPOS              VARCHAR2(45)   DEFAULT CHR(1)  
    , T_PREMNEG              VARCHAR2(45)   DEFAULT CHR(1)  
    , T_COM                  VARCHAR2(45)   DEFAULT CHR(1)  
    , T_FR                   VARCHAR2(45)   DEFAULT CHR(1)  
)
/

COMMENT ON TABLE DNPTXSHORT_PFIOUTEXCHANGE_DBT IS 'Данные листа РЕПО краткой справки'
/
COMMENT ON COLUMN DNPTXSHORT_PFIOUTEXCHANGE_DBT.T_GUID                 IS 'Идентификатор запуска'
/
COMMENT ON COLUMN DNPTXSHORT_PFIOUTEXCHANGE_DBT.T_ITOG                 IS 'Итоговая строка'
/
COMMENT ON COLUMN DNPTXSHORT_PFIOUTEXCHANGE_DBT.T_SORT                 IS 'Сортировка'
/
COMMENT ON COLUMN DNPTXSHORT_PFIOUTEXCHANGE_DBT.T_CLIENT               IS 'Инвестор'
/                                          
COMMENT ON COLUMN DNPTXSHORT_PFIOUTEXCHANGE_DBT.T_CLIENTSTATUS         IS 'Статус'
/                                            
COMMENT ON COLUMN DNPTXSHORT_PFIOUTEXCHANGE_DBT.T_CONTRACTS            IS 'Номер договора, в рамках которого заключена сделка'
/
COMMENT ON COLUMN DNPTXSHORT_PFIOUTEXCHANGE_DBT.T_DEALDATE             IS 'Дата  сделки'
/                                      
COMMENT ON COLUMN DNPTXSHORT_PFIOUTEXCHANGE_DBT.T_CONTRACTN            IS 'Номер сделки'
/                                      
COMMENT ON COLUMN DNPTXSHORT_PFIOUTEXCHANGE_DBT.T_CONTRACTTYPE         IS 'Вид контракта'
/                                     
COMMENT ON COLUMN DNPTXSHORT_PFIOUTEXCHANGE_DBT.T_DIRECTION            IS 'Вид сделки'
/                                        
COMMENT ON COLUMN DNPTXSHORT_PFIOUTEXCHANGE_DBT.T_CONTRACT             IS 'Поставочный/расчетный'
/                             
COMMENT ON COLUMN DNPTXSHORT_PFIOUTEXCHANGE_DBT.T_BASETYPE             IS 'Исходный актив - Вид'
/                              
COMMENT ON COLUMN DNPTXSHORT_PFIOUTEXCHANGE_DBT.T_FITYPE               IS 'Исходный актив - Код'
/                              
COMMENT ON COLUMN DNPTXSHORT_PFIOUTEXCHANGE_DBT.T_EXPIRATIONDATE       IS 'Дата исполнения'
/                                   
COMMENT ON COLUMN DNPTXSHORT_PFIOUTEXCHANGE_DBT.T_Q                    IS 'Кол-во исходного актива'
/                           
COMMENT ON COLUMN DNPTXSHORT_PFIOUTEXCHANGE_DBT.T_CONTRAGENT           IS 'Наименование контрагента'
/                          
COMMENT ON COLUMN DNPTXSHORT_PFIOUTEXCHANGE_DBT.T_EXPIRATIONPRICECUR   IS 'Цена исходного актива по контракту в валюте'
/       
COMMENT ON COLUMN DNPTXSHORT_PFIOUTEXCHANGE_DBT.T_PRICECURRENCY        IS 'Валюта цены'
/                                       
COMMENT ON COLUMN DNPTXSHORT_PFIOUTEXCHANGE_DBT.T_MARKETPRICE          IS 'Рыночная цена'
/                                     
COMMENT ON COLUMN DNPTXSHORT_PFIOUTEXCHANGE_DBT.T_MATERIAL             IS 'Материальная выгода'
/                               
COMMENT ON COLUMN DNPTXSHORT_PFIOUTEXCHANGE_DBT.T_VARPOS               IS 'Сумма полученной вариационной маржи, руб.'
/         
COMMENT ON COLUMN DNPTXSHORT_PFIOUTEXCHANGE_DBT.T_VARNEG               IS 'Сумма уплаченной вариационной маржи, руб.'
/         
COMMENT ON COLUMN DNPTXSHORT_PFIOUTEXCHANGE_DBT.T_PREMPOS              IS 'Сумма полученных премий, руб.'
/                     
COMMENT ON COLUMN DNPTXSHORT_PFIOUTEXCHANGE_DBT.T_PREMNEG              IS 'Сумма уплаченных премий, руб.'
/                     
COMMENT ON COLUMN DNPTXSHORT_PFIOUTEXCHANGE_DBT.T_COM                  IS 'Сумма уплаченных комиссий (в рублях)'
/              
COMMENT ON COLUMN DNPTXSHORT_PFIOUTEXCHANGE_DBT.T_FR                   IS 'Финансовый результат, руб.'
/

BEGIN
  EXECUTE IMMEDIATE 'DROP INDEX DNPTXSHORT_PFIOUTEXCHANGE_DBT_IDX0';
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/

CREATE INDEX DNPTXSHORT_PFIOUTEXCHANGE_DBT_IDX0 ON DNPTXSHORT_PFIOUTEXCHANGE_DBT (
   T_GUID ASC, T_ITOG ASC, T_SORT ASC
)
/

