-- Таблица "DNPTXSHORT_LUCRE_DBT"

CREATE TABLE DNPTXSHORT_LUCRE_DBT (
      T_GUID         VARCHAR2(40)
    , T_ITOG         CHAR(1)
    , T_SORT         NUMBER(10)
    , T_SECCODE      VARCHAR2(15)  DEFAULT CHR(1)   
    , T_SECNAME      VARCHAR2(50)  DEFAULT CHR(1)   
    , T_VN           VARCHAR2(45)  DEFAULT CHR(1)   
    , T_CONTRACTB    VARCHAR2(64)  DEFAULT CHR(1)   
    , T_CODEB        VARCHAR2(30)  DEFAULT CHR(1)            
    , T_DZB          VARCHAR2(40)  DEFAULT CHR(1)   
    , T_DDB          VARCHAR2(40)  DEFAULT CHR(1)   
    , T_DPB          VARCHAR2(40)  DEFAULT CHR(1)   
    , T_QNTY         VARCHAR2(45)  DEFAULT CHR(1)    
    , T_PRBVP        VARCHAR2(45)  DEFAULT CHR(1)   
    , T_VALB         VARCHAR2(45)  DEFAULT CHR(1)   
    , T_VPB          VARCHAR2(45)  DEFAULT CHR(1)   
    , T_CRBD         VARCHAR2(45)  DEFAULT CHR(1)   
    , T_KZB          VARCHAR2(45)  DEFAULT CHR(1)   
    , T_MAINBVP      VARCHAR2(45)  DEFAULT CHR(1)   
    , T_MAINBRUB     VARCHAR2(45)  DEFAULT CHR(1)   
    , T_BMRKTB       VARCHAR2(45)  DEFAULT CHR(1)   
    , T_PRICEMRKTB   VARCHAR2(45)  DEFAULT CHR(1)   
    , T_MRKTB        VARCHAR2(45)  DEFAULT CHR(1)   
    , T_DMRKTB       VARCHAR2(45)  DEFAULT CHR(1)   
    , T_MATERIAL     VARCHAR2(45)  DEFAULT CHR(1)   
    , T_COMB         VARCHAR2(45)  DEFAULT CHR(1)   
    , T_NOMB         VARCHAR2(45)  DEFAULT CHR(1)   
    , T_CONTB        VARCHAR2(320) DEFAULT CHR(1)   
    , T_SECTYPE      VARCHAR2(50)  DEFAULT CHR(1)   
    , T_TAXRATE      VARCHAR2(45)  DEFAULT CHR(1)   
    , T_SUMPRECISION NUMBER(5)     DEFAULT 0
)
/
COMMENT ON TABLE DNPTXSHORT_LUCRE_DBT IS 'Данные листа МатВыгода краткой справки'
/
COMMENT ON COLUMN DNPTXSHORT_LUCRE_DBT.T_GUID         IS 'Идентификатор запуска'
/
COMMENT ON COLUMN DNPTXSHORT_LUCRE_DBT.T_ITOG         IS 'Итоговая строка'
/
COMMENT ON COLUMN DNPTXSHORT_LUCRE_DBT.T_SORT         IS 'Сортировка'
/
COMMENT ON COLUMN DNPTXSHORT_LUCRE_DBT.T_SECCODE      IS 'Код ц/б'
/                                                                      
COMMENT ON COLUMN DNPTXSHORT_LUCRE_DBT.T_SECNAME      IS 'Выпуск ценной бумаги'                                                          
/
COMMENT ON COLUMN DNPTXSHORT_LUCRE_DBT.T_VN           IS 'Код валюты номинала'                                                           
/
COMMENT ON COLUMN DNPTXSHORT_LUCRE_DBT.T_CONTRACTB    IS 'Номер договора, в рамках которого заключена сделка'                            
/
COMMENT ON COLUMN DNPTXSHORT_LUCRE_DBT.T_CODEB        IS 'Номер сделки приобретения'
/                                                             
COMMENT ON COLUMN DNPTXSHORT_LUCRE_DBT.T_DZB          IS 'Дата заключения сделки на приобретение ЦБ'
/                                     
COMMENT ON COLUMN DNPTXSHORT_LUCRE_DBT.T_DDB          IS 'Дата перехода права собств-ти при приобретении ЦБ'
/                             
COMMENT ON COLUMN DNPTXSHORT_LUCRE_DBT.T_DPB          IS 'Дата оплаты по сделке приоретения'
/                                             
COMMENT ON COLUMN DNPTXSHORT_LUCRE_DBT.T_QNTY         IS 'Количество бумаг, шт.'
/                                                         
COMMENT ON COLUMN DNPTXSHORT_LUCRE_DBT.T_PRBVP        IS 'Цена приобретения, в валюте цены сделки'
/                                       
COMMENT ON COLUMN DNPTXSHORT_LUCRE_DBT.T_VALB         IS 'Валюта цены сделки приобретения'
/                                               
COMMENT ON COLUMN DNPTXSHORT_LUCRE_DBT.T_VPB          IS 'Валюта расчетов сделки приобретения'
/                                           
COMMENT ON COLUMN DNPTXSHORT_LUCRE_DBT.T_CRBD         IS 'Курс ЦБ РФ  на дату поставки приобретения'
/                                     
COMMENT ON COLUMN DNPTXSHORT_LUCRE_DBT.T_KZB          IS 'Коэффициент пересчета из валюты цены в валюту номинала для сделки приобретения'
/
COMMENT ON COLUMN DNPTXSHORT_LUCRE_DBT.T_MAINBVP      IS 'Сумма в валюте платежа'
/                                                       
COMMENT ON COLUMN DNPTXSHORT_LUCRE_DBT.T_MAINBRUB     IS 'Сумма, руб.'
/                                                                  
COMMENT ON COLUMN DNPTXSHORT_LUCRE_DBT.T_BMRKTB       IS 'Бумага является обращающейся на дату приобретения'
/                             
COMMENT ON COLUMN DNPTXSHORT_LUCRE_DBT.T_PRICEMRKTB   IS 'Нижняя граница рыночной цены'
/                                                  
COMMENT ON COLUMN DNPTXSHORT_LUCRE_DBT.T_MRKTB        IS 'Источник информации'
/                                                          
COMMENT ON COLUMN DNPTXSHORT_LUCRE_DBT.T_DMRKTB       IS 'Дата котировки'
/                                                               
COMMENT ON COLUMN DNPTXSHORT_LUCRE_DBT.T_MATERIAL     IS 'Материальная выгода'
/                                                          
COMMENT ON COLUMN DNPTXSHORT_LUCRE_DBT.T_COMB         IS 'Комиссия, уплаченная при приобретении, руб.'
/                                   
COMMENT ON COLUMN DNPTXSHORT_LUCRE_DBT.T_NOMB         IS 'Номинал ценной бумаги на дату приобретения'
/                                    
COMMENT ON COLUMN DNPTXSHORT_LUCRE_DBT.T_CONTB        IS 'Контрагент по сделке приобретения'
/                                             
COMMENT ON COLUMN DNPTXSHORT_LUCRE_DBT.T_SECTYPE      IS 'Налоговая  группа'
/                                                            
COMMENT ON COLUMN DNPTXSHORT_LUCRE_DBT.T_TAXRATE      IS 'Общая ставка налогообложения'
/                                                  
COMMENT ON COLUMN DNPTXSHORT_LUCRE_DBT.T_SUMPRECISION IS 'Точность количества ц/б'
/


BEGIN
  EXECUTE IMMEDIATE 'DROP INDEX DNPTXSHORT_LUCRE_DBT_IDX0';
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/

CREATE INDEX DNPTXSHORT_LUCRE_DBT_IDX0 ON DNPTXSHORT_LUCRE_DBT (
   T_GUID ASC, T_ITOG ASC, T_SORT ASC
)
/
