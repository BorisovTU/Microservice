-- Таблица "DNPTXSHORT_REALIZ_DBT"

CREATE TABLE DNPTXSHORT_REALIZ_DBT (
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
    , T_DIFS         VARCHAR2(45)  DEFAULT CHR(1)
    , T_COMBANKS     VARCHAR2(45)  DEFAULT CHR(1)
    , T_COMEXCS      VARCHAR2(45)  DEFAULT CHR(1)
    , T_COMS         VARCHAR2(45)  DEFAULT CHR(1)
    , T_CONTRACTB    VARCHAR2(64)  DEFAULT CHR(1)
    , T_CODEB        VARCHAR2(30)  DEFAULT CHR(1)
    , T_DZB          VARCHAR2(40)  DEFAULT CHR(1)
    , T_DPB          VARCHAR2(40)  DEFAULT CHR(1)
    , T_PRBRUB       VARCHAR2(45)  DEFAULT CHR(1)
    , T_ALLBRUB      VARCHAR2(45)  DEFAULT CHR(1)
    , T_MATERIALB    VARCHAR2(45)  DEFAULT CHR(1)
    , T_TAXMATERIALB VARCHAR2(45)  DEFAULT CHR(1)
    , T_DIFB         VARCHAR2(45)  DEFAULT CHR(1)
    , T_COMBANKB     VARCHAR2(45)  DEFAULT CHR(1)
    , T_COMEXCB      VARCHAR2(45)  DEFAULT CHR(1)
    , T_COMB         VARCHAR2(45)  DEFAULT CHR(1)
    , T_TAXFRLINK    VARCHAR2(45)  DEFAULT CHR(1)
    , T_SECTYPE      VARCHAR2(50)  DEFAULT CHR(1)
    , T_TAXTAGS      VARCHAR2(150) DEFAULT CHR(1)
    , T_AA           VARCHAR2(250) DEFAULT CHR(1)
    , T_SPOINT       NUMBER(5)     DEFAULT 0
    , T_BPOINT       NUMBER(5)     DEFAULT 0
)
/
COMMENT ON TABLE DNPTXSHORT_REALIZ_DBT IS 'Данные листа Реализаци краткой справки'
/
COMMENT ON COLUMN DNPTXSHORT_REALIZ_DBT.T_GUID         IS 'Идентификатор запуска'
/
COMMENT ON COLUMN DNPTXSHORT_REALIZ_DBT.T_ITOG         IS 'Итоговая строка'
/
COMMENT ON COLUMN DNPTXSHORT_REALIZ_DBT.T_SORT         IS 'Сортировка'
/
COMMENT ON COLUMN DNPTXSHORT_REALIZ_DBT.T_SECCODE      IS 'Код ц/б'
/                                                                                     
COMMENT ON COLUMN DNPTXSHORT_REALIZ_DBT.T_SECNAME      IS 'Название ц/б'
/                                                                                
COMMENT ON COLUMN DNPTXSHORT_REALIZ_DBT.T_QNTY         IS 'Количество, проданное в этой сделке продажи, из этой покупки'
/                                
COMMENT ON COLUMN DNPTXSHORT_REALIZ_DBT.T_CONTRACTS    IS 'Номер договора, в рамках которого заключена сделка'
/                                          
COMMENT ON COLUMN DNPTXSHORT_REALIZ_DBT.T_CODES        IS 'Номер сделки/Номер купона'
/                                                                   
COMMENT ON COLUMN DNPTXSHORT_REALIZ_DBT.T_DZS          IS 'Дата заключения'
/                                                                             
COMMENT ON COLUMN DNPTXSHORT_REALIZ_DBT.T_DPS          IS 'Дата оплаты'
/                                                                                 
COMMENT ON COLUMN DNPTXSHORT_REALIZ_DBT.T_PRSRUB       IS 'Цена сделки, руб.'
/                                                                           
COMMENT ON COLUMN DNPTXSHORT_REALIZ_DBT.T_ALLSRUB      IS 'Сумма сделки включая НКД, руб.'
/                                                              
COMMENT ON COLUMN DNPTXSHORT_REALIZ_DBT.T_DIFS         IS 'Сумма отклонения от рынка, руб.'
/                                                             
COMMENT ON COLUMN DNPTXSHORT_REALIZ_DBT.T_COMBANKS     IS 'Комиссия банка, руб.'
/                                                                        
COMMENT ON COLUMN DNPTXSHORT_REALIZ_DBT.T_COMEXCS      IS 'Комиссия биржи, руб.'
/                                                                        
COMMENT ON COLUMN DNPTXSHORT_REALIZ_DBT.T_COMS         IS 'Всего комиссий, руб.'
/                                                                        
COMMENT ON COLUMN DNPTXSHORT_REALIZ_DBT.T_CONTRACTB    IS 'Номер договора, в рамках которого заключена сделка'
/                                          
COMMENT ON COLUMN DNPTXSHORT_REALIZ_DBT.T_CODEB        IS 'Номер сделки'
/                                                                                
COMMENT ON COLUMN DNPTXSHORT_REALIZ_DBT.T_DZB          IS 'Дата заключения'
/                                                                             
COMMENT ON COLUMN DNPTXSHORT_REALIZ_DBT.T_DPB          IS 'Дата оплаты'
/                                                                                 
COMMENT ON COLUMN DNPTXSHORT_REALIZ_DBT.T_PRBRUB       IS 'Цена сделки, руб.'
/                                                                           
COMMENT ON COLUMN DNPTXSHORT_REALIZ_DBT.T_ALLBRUB      IS 'Сумма сделки включая НКД, руб. Для выплаты купона - Сумма НКД, учитываемого в расходы, руб.'
/ 
COMMENT ON COLUMN DNPTXSHORT_REALIZ_DBT.T_MATERIALB    IS 'Материальная выгода'
/                                                                         
COMMENT ON COLUMN DNPTXSHORT_REALIZ_DBT.T_TAXMATERIALB IS 'Налог на матвыгоду'
/                                                                          
COMMENT ON COLUMN DNPTXSHORT_REALIZ_DBT.T_DIFB         IS 'Сумма отклонения от рынка'
/                                                                   
COMMENT ON COLUMN DNPTXSHORT_REALIZ_DBT.T_COMBANKB     IS 'Комиссия банка, руб.'
/                                                                        
COMMENT ON COLUMN DNPTXSHORT_REALIZ_DBT.T_COMEXCB      IS 'Комиссия биржи, руб.'
/                                                                        
COMMENT ON COLUMN DNPTXSHORT_REALIZ_DBT.T_COMB         IS 'Всего комиссий, руб.'
/                                                                        
COMMENT ON COLUMN DNPTXSHORT_REALIZ_DBT.T_TAXFRLINK    IS 'Сумма для налогообложения'
/                                                                   
COMMENT ON COLUMN DNPTXSHORT_REALIZ_DBT.T_SECTYPE      IS 'Обращаемость ц/б'
/                                                                            
COMMENT ON COLUMN DNPTXSHORT_REALIZ_DBT.T_TAXTAGS      IS 'Особенности налогообложения'
/                                                                 
COMMENT ON COLUMN DNPTXSHORT_REALIZ_DBT.T_AA           IS 'Комментарий'
/
COMMENT ON COLUMN DNPTXSHORT_REALIZ_DBT.T_SPOINT       IS 'Точность цены продажи'
/              
COMMENT ON COLUMN DNPTXSHORT_REALIZ_DBT.T_BPOINT       IS 'Точность цены покупки'
/

BEGIN
  EXECUTE IMMEDIATE 'DROP INDEX DNPTXSHORT_REALIZ_DBT_IDX0';
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/

CREATE INDEX DNPTXSHORT_REALIZ_DBT_IDX0 ON DNPTXSHORT_REALIZ_DBT (
   T_GUID ASC, T_ITOG ASC, T_SORT ASC
)
/



                



        
