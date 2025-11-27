-- Таблица "DNPTXSHORT_PFIEXCHANGE_DBT"

CREATE TABLE DNPTXSHORT_PFIEXCHANGE_DBT (
      T_GUID          VARCHAR2(40)
    , T_ITOG          CHAR(1)
    , T_SORT          NUMBER(10)
    , T_CLIENT        VARCHAR2(100)  DEFAULT CHR(1) 
    , T_CLIENTSTATUS  VARCHAR2(30)   DEFAULT CHR(1) 
    , T_SECCODE       VARCHAR2(15)   DEFAULT CHR(1) 
    , T_FICODE        VARCHAR2(15)   DEFAULT CHR(1) 
    , T_FINAME        VARCHAR2(50)   DEFAULT CHR(1) 
    , T_FITYPE        VARCHAR2(50)   DEFAULT CHR(1) 
    , T_BASETYPE      VARCHAR2(50)   DEFAULT CHR(1) 
    , T_BASECODE      VARCHAR2(50)   DEFAULT CHR(1) 
    , T_BASENAME      VARCHAR2(100)  DEFAULT CHR(1) 
    , T_TRADEPLACE    VARCHAR2(100)  DEFAULT CHR(1) 
    , T_CONTRACTS     VARCHAR2(64)   DEFAULT CHR(1) 
    , T_DPOS          VARCHAR2(45)   DEFAULT CHR(1) 
    , T_QBUY          VARCHAR2(45)   DEFAULT CHR(1) 
    , T_QSELL         VARCHAR2(45)   DEFAULT CHR(1) 
    , T_QLONG         VARCHAR2(45)   DEFAULT CHR(1) 
    , T_MATERIAL      VARCHAR2(45)   DEFAULT CHR(1) 
    , T_VARPOS        VARCHAR2(45)   DEFAULT CHR(1) 
    , T_VARNEG        VARCHAR2(45)   DEFAULT CHR(1) 
    , T_PREMPOS       VARCHAR2(45)   DEFAULT CHR(1) 
    , T_PREMNEG       VARCHAR2(45)   DEFAULT CHR(1) 
    , T_COM           VARCHAR2(45)   DEFAULT CHR(1) 
    , T_FR            VARCHAR2(45)   DEFAULT CHR(1) 
    , T_BMARKET       VARCHAR2(45)   DEFAULT CHR(1) 
    , T_TAXRATE       VARCHAR2(45)   DEFAULT CHR(1)  
)
/

COMMENT ON TABLE DNPTXSHORT_PFIEXCHANGE_DBT IS 'Данные листа РЕПО краткой справки'
/
COMMENT ON COLUMN DNPTXSHORT_PFIEXCHANGE_DBT.T_GUID         IS 'Идентификатор запуска'
/
COMMENT ON COLUMN DNPTXSHORT_PFIEXCHANGE_DBT.T_ITOG         IS 'Итоговая строка'
/
COMMENT ON COLUMN DNPTXSHORT_PFIEXCHANGE_DBT.T_SORT         IS 'Сортировка'
/
COMMENT ON COLUMN DNPTXSHORT_PFIEXCHANGE_DBT.T_CLIENT        IS 'Инвестор'                                              
/
COMMENT ON COLUMN DNPTXSHORT_PFIEXCHANGE_DBT.T_CLIENTSTATUS  IS 'Статус'                                                
/
COMMENT ON COLUMN DNPTXSHORT_PFIEXCHANGE_DBT.T_SECCODE       IS 'Код ПИ'                                                
/
COMMENT ON COLUMN DNPTXSHORT_PFIEXCHANGE_DBT.T_FICODE        IS 'Наименование ПИ'                                       
/
COMMENT ON COLUMN DNPTXSHORT_PFIEXCHANGE_DBT.T_FINAME        IS 'Вид ПИ'                                                
/
COMMENT ON COLUMN DNPTXSHORT_PFIEXCHANGE_DBT.T_FITYPE        IS 'Код базового инструмента'                              
/
COMMENT ON COLUMN DNPTXSHORT_PFIEXCHANGE_DBT.T_BASETYPE      IS 'Код базового инструмента'                              
/
COMMENT ON COLUMN DNPTXSHORT_PFIEXCHANGE_DBT.T_BASECODE      IS 'Наименование базового инструмента'                     
/
COMMENT ON COLUMN DNPTXSHORT_PFIEXCHANGE_DBT.T_BASENAME      IS 'Биржа'                                                 
/
COMMENT ON COLUMN DNPTXSHORT_PFIEXCHANGE_DBT.T_TRADEPLACE    IS 'Брокер'
/                                                
COMMENT ON COLUMN DNPTXSHORT_PFIEXCHANGE_DBT.T_CONTRACTS     IS 'Номер договора, в рамках которого заключена сделка'
/    
COMMENT ON COLUMN DNPTXSHORT_PFIEXCHANGE_DBT.T_DPOS          IS 'Дата'
/                                                  
COMMENT ON COLUMN DNPTXSHORT_PFIEXCHANGE_DBT.T_QBUY          IS 'Количество, купленное в этот день'
/                     
COMMENT ON COLUMN DNPTXSHORT_PFIEXCHANGE_DBT.T_QSELL         IS 'Количество, проданное в этот день'
/                     
COMMENT ON COLUMN DNPTXSHORT_PFIEXCHANGE_DBT.T_QLONG         IS 'Открытая позиция на конец дня (+ - покупка, - продажа)'
/
COMMENT ON COLUMN DNPTXSHORT_PFIEXCHANGE_DBT.T_MATERIAL      IS 'Материальная выгода, руб.'
/                             
COMMENT ON COLUMN DNPTXSHORT_PFIEXCHANGE_DBT.T_VARPOS        IS 'Сумма полученной вариационной маржи, руб.'             
/
COMMENT ON COLUMN DNPTXSHORT_PFIEXCHANGE_DBT.T_VARNEG        IS 'Сумма уплаченной вариационной маржи, руб.'             
/
COMMENT ON COLUMN DNPTXSHORT_PFIEXCHANGE_DBT.T_PREMPOS       IS 'Сумма полученных премий, руб.'                         
/
COMMENT ON COLUMN DNPTXSHORT_PFIEXCHANGE_DBT.T_PREMNEG       IS 'Сумма уплаченных премий, руб.'                         
/
COMMENT ON COLUMN DNPTXSHORT_PFIEXCHANGE_DBT.T_COM           IS 'Комиссия, руб.'                                       
/
COMMENT ON COLUMN DNPTXSHORT_PFIEXCHANGE_DBT.T_FR            IS 'Финансовый результат, руб.'                            
/
COMMENT ON COLUMN DNPTXSHORT_PFIEXCHANGE_DBT.T_BMARKET       IS 'Обращаемость (для ПИ, где базовый актив - ц/б)'        
/
COMMENT ON COLUMN DNPTXSHORT_PFIEXCHANGE_DBT.T_TAXRATE       IS 'Общая ставка налогообложения'
/

BEGIN
  EXECUTE IMMEDIATE 'DROP INDEX DNPTXSHORT_PFIEXCHANGE_DBT_IDX0';
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/

CREATE INDEX DNPTXSHORT_PFIEXCHANGE_DBT_IDX0 ON DNPTXSHORT_PFIEXCHANGE_DBT (
   T_GUID ASC, T_ITOG ASC, T_SORT ASC
)
/

