CREATE OR REPLACE PACKAGE BODY RSI_NPTXSHORT2025
IS

  FUNCTION CorrectMoneyStr(p_ValStr IN VARCHAR2, p_FullNum IN NUMBER DEFAULT 0) RETURN VARCHAR2
  AS
    v_Point NUMBER := 2;
    v_Num NUMBER; 
  BEGIN

    IF p_ValStr = CHR(1) OR p_ValStr = '' THEN
      RETURN '';
    END IF;
    
     
     select to_number(
         case when regexp_like(p_ValStr, '[.,]')
              then replace(p_ValStr, ',', '.')
              else p_ValStr || '.0'
         end,
         '9999999999999999999990D999999999999',
         'NLS_NUMERIC_CHARACTERS = ''.,'''
       ) as num_value
       into v_Num
       from dual;

    IF p_FullNum > 0 THEN
      SELECT LENGTH(TO_CHAR(v_Num)) - LENGTH(TO_CHAR(ROUND(v_Num), 0)) INTO v_Point FROM DUAL;
      IF v_Point > 0 THEN
        v_Point := v_Point - 1;
      END IF;
    END IF;
   
    RETURN to_char(v_Num, 'FM99999999999999999990'||rpad('D',v_Point+1,'0'),'NLS_NUMERIC_CHARACTERS = '',.''');
  END;

  FUNCTION CorrectDateStr(p_ValStr IN VARCHAR2) RETURN VARCHAR2
  AS
  BEGIN
    IF p_ValStr = CHR(1) OR p_ValStr = '' THEN
      RETURN '';
    END IF;

    RETURN TO_CHAR(TO_DATE(p_ValStr, 'DD.MM.YYYY'), 'DD.MM.YYYY');
  END;

  FUNCTION CorrectNumPrecision(p_ValStr IN VARCHAR2, p_SumPrecision IN NUMBER DEFAULT 0) RETURN VARCHAR2
  AS
  v_Num NUMBER;
  BEGIN
     IF p_ValStr = CHR(1) OR p_ValStr = '' THEN
       RETURN '';
     END IF;
     
     select to_number(
         case when regexp_like(p_ValStr, '[.,]')
              then replace(p_ValStr, ',', '.')
              else p_ValStr || '.0'
         end,
         '9999999999999999999990D999999999999',
         'NLS_NUMERIC_CHARACTERS = ''.,'''
       ) as num_value
       into v_Num
       from dual;

     IF ROUND(v_Num, 0) = v_Num THEN
       RETURN to_char(v_Num, 'FM99999999999999999990'||rpad('D',0,'0'),'NLS_NUMERIC_CHARACTERS = '',.''');
     END IF;

     RETURN to_char(v_Num, 'FM99999999999999999990'||rpad('D',p_SumPrecision+1,'0'),'NLS_NUMERIC_CHARACTERS = '',.'''); 
  END;

  FUNCTION GetTotalCalcField(p_GUID IN VARCHAR2, p_FieldName VARCHAR2) RETURN VARCHAR2
  AS
    v_FieldValue DNPTXSHORT_TOTALCALC_DBT.T_FIELDVALUE%TYPE;
    v_FieldType DNPTXSHORT_TOTALCALC_DBT.T_FIELDTYPE%TYPE;
  BEGIN

    SELECT T_FIELDVALUE, T_FIELDTYPE
      INTO v_FieldValue, v_FieldType
      FROM DNPTXSHORT_TOTALCALC_DBT 
     WHERE T_GUID = p_GUID 
       AND T_FIELDNAME = LOWER(p_FieldName);

    IF v_FieldValue = CHR(1) THEN
      v_FieldValue := '';
    END IF;

    IF v_FieldType = 28 THEN
      v_FieldValue := CorrectMoneyStr(v_FieldValue);
    END IF;

    RETURN '"'||v_FieldValue||'"';

    EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RETURN NULL; 
  END;

  FUNCTION GetSNOBNode(p_GUID IN VARCHAR2) RETURN CLOB
  AS
    v_SNOBNode CLOB;

    v_RepParm DNPTXSHORT_REPPARM_DBT%ROWTYPE;
  BEGIN

    SELECT * INTO v_RepParm FROM DNPTXSHORT_REPPARM_DBT WHERE T_GUID = p_GUID;

    IF v_RepParm.t_EndDate <= TO_DATE('31.12.2024','DD.MM.YYYY') THEN
      SELECT JSON_ARRAY(JSON_OBJECT('SNOBType' IS GetTotalCalcField(p_GUID, 'SNOBType') FORMAT JSON,
                                    'SNOBAmount' IS GetTotalCalcField(p_GUID, 'SNOBAmount') FORMAT JSON
                                   ) FORMAT JSON
                       RETURNING CLOB
                       )
        INTO v_SNOBNode
        FROM DUAL;

    ELSE
      SELECT JSON_ARRAY(JSON_OBJECT('SNOBType' IS GetTotalCalcField(p_GUID, 'SNOBType1') FORMAT JSON,
                                    'SNOBAmount' IS GetTotalCalcField(p_GUID, 'SNOBAmount1') FORMAT JSON
                                   ) FORMAT JSON,
                        JSON_OBJECT('SNOBType' IS GetTotalCalcField(p_GUID, 'SNOBType2') FORMAT JSON,
                                    'SNOBAmount' IS GetTotalCalcField(p_GUID, 'SNOBAmount2') FORMAT JSON
                                   ) FORMAT JSON
                       RETURNING CLOB
                       ) 
        INTO v_SNOBNode
        FROM DUAL;
    END IF;


    RETURN v_SNOBNode;
  END;

  FUNCTION GetClientTaxInfoNode(p_GUID IN VARCHAR2) RETURN CLOB
  AS
    v_ClientTaxInfoNode CLOB;
  BEGIN

    SELECT    JSON_ARRAY(JSON_OBJECT('TaxType'            IS 'TotalFinResult',
                                     'ShortDescription'   IS 'Финансовый результат от купли - продажи и погашения ценных бумаг, в т.ч.',
                                     'FullDescription'    IS 'Финансовый результат от купли-продажи и погашения ценных бумаг - всего',
                                     'BrokerageData'      IS GetTotalCalcField(p_GUID, 'TaxFR_total') FORMAT JSON,
                                     'DepositoryData'     IS '0,00',
                                     'GeneralTaxBaseData' IS '0,00'
                                    ) FORMAT JSON,
                         JSON_OBJECT('TaxType'            IS 'MarketFinResult',
                                     'ShortDescription'   IS 'обращающихся',
                                     'FullDescription'    IS 'Финансовый результат от купли-продажи и погашения ценных бумаг - обращающихся',
                                     'BrokerageData'      IS GetTotalCalcField(p_GUID, 'TaxFR_market') FORMAT JSON,
                                     'DepositoryData'     IS '0,00',
                                     'GeneralTaxBaseData' IS '0,00'
                                    ) FORMAT JSON,
                         JSON_OBJECT('TaxType'            IS 'TotalNonTurnoverFinResult',       
                                     'ShortDescription'   IS 'необращающихся всего, из них',    
                                     'FullDescription'    IS 'Финансовый результат от купли-продажи и погашения ценных бумаг -  необращающихся всего',  
                                     'BrokerageData'      IS GetTotalCalcField(p_GUID, 'TaxFR_OTC') FORMAT JSON,     
                                     'DepositoryData'     IS '0,00',    
                                     'GeneralTaxBaseData' IS '0,00'
                                    ) FORMAT JSON,
                         JSON_OBJECT('TaxType'            IS 'TurnoverLostFinResult',   
                                     'ShortDescription'   IS 'потерявшие обращаемость', 
                                     'FullDescription'    IS 'Финансовый результат от купли-продажи и погашения ценных бумаг - потерявшие обращаемость',        
                                     'BrokerageData'      IS GetTotalCalcField(p_GUID, 'TaxFR_Lost') FORMAT JSON,    
                                     'DepositoryData'     IS '0,00',    
                                     'GeneralTaxBaseData' IS '0,00'
                                    ) FORMAT JSON,
                         JSON_OBJECT('TaxType'            IS 'ComDepo', 
                                     'ShortDescription'   IS 'Сумма расходов в виде комиссий, не относящихся к сделкам ',       
                                     'FullDescription'    IS 'Сумма расходов в виде комиссий, не относящихся к сделкам',        
                                     'BrokerageData'      IS GetTotalCalcField(p_GUID, 'ComDepo') FORMAT JSON,       
                                     'DepositoryData'     IS '0,00',    
                                     'GeneralTaxBaseData' IS '0,00'
                                    ) FORMAT JSON,
                         JSON_OBJECT('TaxType'            IS 'CoupRub', 
                                     'ShortDescription'   IS 'Суммы, полученные при погашении купонов',         
                                     'FullDescription'    IS 'Суммы, полученные при погашении купонов',         
                                     'BrokerageData'      IS GetTotalCalcField(p_GUID, 'CoupRub') FORMAT JSON,       
                                     'DepositoryData'     IS GetTotalCalcField(p_GUID, 'DepoCoupRub') FORMAT JSON,   
                                     'GeneralTaxBaseData' IS '0,00'
                                    ) FORMAT JSON,
                         JSON_OBJECT('TaxType'            IS 'RepoShortFinResult',      
                                     'ShortDescription'   IS 'Финансовый результат по коротким позициям в РЕПО',        
                                     'FullDescription'    IS 'Финансовый результат по коротким позициям в РЕПО',        
                                     'BrokerageData'      IS GetTotalCalcField(p_GUID, 'TaxFR_Open') FORMAT JSON,    
                                     'DepositoryData'     IS '0,00',    
                                     'GeneralTaxBaseData' IS '0,00'
                                    ) FORMAT JSON,
                         JSON_OBJECT('TaxType'            IS 'RepoFactFinResult',       
                                     'ShortDescription'   IS 'Финансовый результат по сделкам РЕПО/займа фактический',  
                                     'FullDescription'    IS 'Финансовый результат по сделкам РЕПО/займа фактический',  
                                     'BrokerageData'      IS GetTotalCalcField(p_GUID, 'RepoFactRub') FORMAT JSON,   
                                     'DepositoryData'     IS '0,00',    
                                     'GeneralTaxBaseData' IS '0,00'
                                    ) FORMAT JSON,
                         JSON_OBJECT('TaxType'            IS 'RepoTaxFinResult',        
                                     'ShortDescription'   IS 'Финансовый результат по сделкам РЕПО/займа для налогообложения',  
                                     'FullDescription'    IS 'Финансовый результат по сделкам РЕПО/займа для налогообложения',  
                                     'BrokerageData'      IS GetTotalCalcField(p_GUID, 'RepoTaxRub') FORMAT JSON,    
                                     'DepositoryData'     IS '0,00',    
                                     'GeneralTaxBaseData' IS '0,00'
                                    ) FORMAT JSON,
                         JSON_OBJECT('TaxType'            IS 'RepoLostAmount',  
                                     'ShortDescription'   IS 'Сумма убытка по  РЕПО и коротким позициям в уменьшение НОБ по ц/б',       
                                     'FullDescription'    IS 'Сумма убытка по РЕПО и коротким позициям в уменьшение НОБ по ц/б',        
                                     'BrokerageData'      IS GetTotalCalcField(p_GUID, 'RepoTaxRach') FORMAT JSON,   
                                     'DepositoryData'     IS '0,00',    
                                     'GeneralTaxBaseData' IS '0,00'
                                    ) FORMAT JSON,
                         JSON_OBJECT('TaxType'            IS 'DerivFinResult',  
                                     'ShortDescription'   IS 'Финансовый результат по сделкам с ПИ',    
                                     'FullDescription'    IS 'Финансовый результат по сделкам с ПИ',    
                                     'BrokerageData'      IS GetTotalCalcField(p_GUID, 'DerivTaxRub') FORMAT JSON,   
                                     'DepositoryData'     IS '0,00',    
                                     'GeneralTaxBaseData' IS '0,00'
                                    ) FORMAT JSON,
                         JSON_OBJECT('TaxType'            IS 'FinancialGain',   
                                     'ShortDescription'   IS 'Материальная выгода по покупкам', 
                                     'FullDescription'    IS 'Материальная выгода по покупкам', 
                                     'BrokerageData'      IS GetTotalCalcField(p_GUID, 'BaseMaterial') FORMAT JSON,  
                                     'DepositoryData'     IS '0,00',    
                                     'GeneralTaxBaseData' IS '0,00'
                                    ) FORMAT JSON,
                         JSON_OBJECT('TaxType'            IS 'BaseBill',        
                                     'ShortDescription'   IS 'Проценты (дисконт), полученные при оплате предъявленного к платежу векселя',      
                                     'FullDescription'    IS 'Проценты (дисконт), полученные при оплате предъявленного к платежу векселя',      
                                     'BrokerageData'      IS '0,00',    
                                     'DepositoryData'     IS '0,00',    
                                     'GeneralTaxBaseData' IS GetTotalCalcField(p_GUID, 'BaseBill') FORMAT JSON
                                    ) FORMAT JSON,
                         JSON_OBJECT('TaxType'            IS 'OtherIncome',     
                                     'ShortDescription'   IS 'Прочие доходы (компенсации и пр.)',       
                                     'FullDescription'    IS 'Прочие доходы (компенсации и пр.)',       
                                     'BrokerageData'      IS '0,00',    
                                     'DepositoryData'     IS '0,00',    
                                     'GeneralTaxBaseData' IS GetTotalCalcField(p_GUID, 'OtherIncome') FORMAT JSON
                                    ) FORMAT JSON,
                         JSON_OBJECT('TaxType'            IS 'NoDeductionTaxBase',      
                                     'ShortDescription'   IS 'Общая НОБ без учета инвестиционного вычета / налогового вычета на ДСГ',   
                                     'FullDescription'    IS 'Общая НОБ без учета инвестиционного вычета / налогового вычета на ДСГ (618)',     
                                     'BrokerageData'      IS GetTotalCalcField(p_GUID, 'TaxBase_618') FORMAT JSON,   
                                     'DepositoryData'     IS GetTotalCalcField(p_GUID, 'DepoTaxBase_618') FORMAT JSON,       
                                     'GeneralTaxBaseData' IS '0,00'
                                    ) FORMAT JSON,
                         JSON_OBJECT('TaxType'            IS 'LongTermBenefit', 
                                     'ShortDescription'   IS 'Учтенное в НОБ значение льготы долгосрочного владения (ЛДВ) (618)',       
                                     'FullDescription'    IS 'Учтенное в НОБ значение льготы долгосрочного владения (ЛДВ) (618)',       
                                     'BrokerageData'      IS GetTotalCalcField(p_GUID, 'MinusG_618') FORMAT JSON,    
                                     'DepositoryData'     IS '0,00',    
                                     'GeneralTaxBaseData' IS '0,00'
                                    ) FORMAT JSON,
                         JSON_OBJECT('TaxType'            IS 'InvestementDeduction',    
                                     'ShortDescription'   IS 'Инвестиционный налоговый вычет (619) (для ИИС)',  
                                     'FullDescription'    IS 'Инвестиционный налоговый вычет (619) (для ИИС)',  
                                     'BrokerageData'      IS GetTotalCalcField(p_GUID, 'MinusG_619') FORMAT JSON,    
                                     'DepositoryData'     IS '0,00',    
                                     'GeneralTaxBaseData' IS '0,00'
                                    ) FORMAT JSON,
                         JSON_OBJECT('TaxType'            IS 'SavingDeduction', 
                                     'ShortDescription'   IS 'Налоговый вычет (517) на долгосрочные сбережения граждан (ДСГ)',  
                                     'FullDescription'    IS 'Налоговый вычет (517) на долгосрочные сбережения граждан (ДСГ)',  
                                     'BrokerageData'      IS GetTotalCalcField(p_GUID, 'MinusG_517') FORMAT JSON,    
                                     'DepositoryData'     IS '0,00',    
                                     'GeneralTaxBaseData' IS '0,00'
                                    ) FORMAT JSON,
                         JSON_OBJECT('TaxType'            IS 'TotalTaxBase',    
                                     'ShortDescription'   IS 'Общая НОБ с учетом инвестиционного вычета / налогового вычета на ДСГ',    
                                     'FullDescription'    IS 'Общая НОБ с учетом инвестиционного вычета / налогового вычета на ДСГ - всего',    
                                     'BrokerageData'      IS GetTotalCalcField(p_GUID, 'TaxBaseAll') FORMAT JSON,   
                                     'DepositoryData'     IS GetTotalCalcField(p_GUID, 'DepoTaxBaseAll') FORMAT JSON,       
                                     'GeneralTaxBaseData' IS GetTotalCalcField(p_GUID, 'OnbTaxBaseAll') FORMAT JSON
                                    ) FORMAT JSON,
                         JSON_OBJECT('TaxType'            IS 'TaxBaseGeneralRate',              
                                     'ShortDescription'   IS 'по общей ставке (13 или 30)',     
                                     'FullDescription'    IS 'Общая НОБ с учетом инвестиционного вычета / налогового вычета на ДСГ - по общей ставке (13 или 30)',      
                                     'BrokerageData'      IS GetTotalCalcField(p_GUID, 'TaxBase_13') FORMAT JSON,    
                                     'DepositoryData'     IS GetTotalCalcField(p_GUID, 'DepoTaxBase_13') FORMAT JSON,        
                                     'GeneralTaxBaseData' IS GetTotalCalcField(p_GUID, 'OnbTaxBase_13') FORMAT JSON
                                    ) FORMAT JSON,
                         JSON_OBJECT('TaxType'            IS 'TaxBaseReducedRate',              
                                     'ShortDescription'   IS 'по ставке 15',    
                                     'FullDescription'    IS 'Общая НОБ с учетом инвестиционного вычета / налогового вычета на ДСГ - по ставке 15',     
                                     'BrokerageData'      IS GetTotalCalcField(p_GUID, 'TaxBase_15') FORMAT JSON,    
                                     'DepositoryData'     IS GetTotalCalcField(p_GUID, 'DepoTaxBase_15') FORMAT JSON,        
                                     'GeneralTaxBaseData' IS GetTotalCalcField(p_GUID, 'OnbTaxBase_15') FORMAT JSON
                                    ) FORMAT JSON,
                         JSON_OBJECT('TaxType'            IS 'TaxBaseEighteenRate',             
                                     'ShortDescription'   IS 'по ставке 18',   
                                     'FullDescription'    IS 'Общая НОБ с учетом инвестиционного вычета / налогового вычета на ДСГ - по ставке 18',     
                                     'BrokerageData'      IS '0,00',    
                                     'DepositoryData'     IS '0,00',    
                                     'GeneralTaxBaseData' IS GetTotalCalcField(p_GUID, 'OnbTaxBase_18') FORMAT JSON
                                    ) FORMAT JSON,
                         JSON_OBJECT('TaxType'            IS 'TaxBaseTwentyRate',               
                                     'ShortDescription'   IS 'по ставке 20',    
                                     'FullDescription'    IS 'Общая НОБ с учетом инвестиционного вычета / налогового вычета на ДСГ - по ставке 20',     
                                     'BrokerageData'      IS '0,00',    
                                     'DepositoryData'     IS '0,00',    
                                     'GeneralTaxBaseData' IS GetTotalCalcField(p_GUID, 'OnbTaxBase_20') FORMAT JSON
                                    ) FORMAT JSON,
                         JSON_OBJECT('TaxType'            IS 'TaxBaseTwentyTwoRate',            
                                     'ShortDescription'   IS 'по ставке 22',    
                                     'FullDescription'    IS 'Общая НОБ с учетом инвестиционного вычета / налогового вычета на ДСГ - по ставке 22',     
                                     'BrokerageData'      IS '0,00',    
                                     'DepositoryData'     IS '0,00',    
                                     'GeneralTaxBaseData' IS GetTotalCalcField(p_GUID, 'OnbTaxBase_22') FORMAT JSON
                                    ) FORMAT JSON,
                         JSON_OBJECT('TaxType'            IS 'TaxAmountDueYear',                
                                     'ShortDescription'   IS 'Сумма налога, исчисленная налоговым агентом с начала года, в том числе:', 
                                     'FullDescription'    IS 'Сумма налога, исчисленная налоговым агентом с начала года - всего',       
                                     'BrokerageData'      IS GetTotalCalcField(p_GUID, 'TaxDueYear') FORMAT JSON,    
                                     'DepositoryData'     IS GetTotalCalcField(p_GUID, 'DepoTaxDueYear') FORMAT JSON,        
                                     'GeneralTaxBaseData' IS GetTotalCalcField(p_GUID, 'OnbTaxDueYear') FORMAT JSON
                                    ) FORMAT JSON,
                         JSON_OBJECT('TaxType'            IS 'GeneralRateTaxAmount',            
                                     'ShortDescription'   IS 'по общей ставке (13 или 30)',     
                                     'FullDescription'    IS 'Сумма налога, исчисленная налоговым агентом с начала года - по общей ставке (13 или 30)', 
                                     'BrokerageData'      IS GetTotalCalcField(p_GUID, 'TaxDueYear_13') FORMAT JSON, 
                                     'DepositoryData'     IS GetTotalCalcField(p_GUID, 'DepoTaxDueYear_13') FORMAT JSON,     
                                     'GeneralTaxBaseData' IS GetTotalCalcField(p_GUID, 'OnbTaxDueYear_13') FORMAT JSON
                                    ) FORMAT JSON,
                         JSON_OBJECT('TaxType'            IS 'ReducedRateTaxAmount',            
                                     'ShortDescription'   IS 'по ставке 15',    
                                     'FullDescription'    IS 'Сумма налога, исчисленная налоговым агентом с начала года - по ставке 15',        
                                     'BrokerageData'      IS GetTotalCalcField(p_GUID, 'TaxDueYear_15') FORMAT JSON, 
                                     'DepositoryData'     IS GetTotalCalcField(p_GUID, 'DepoTaxDueYear_15') FORMAT JSON,     
                                     'GeneralTaxBaseData' IS GetTotalCalcField(p_GUID, 'OnbTaxDueYear_15') FORMAT JSON
                                    ) FORMAT JSON,
                         JSON_OBJECT('TaxType'            IS 'EighteenRateTaxAmount',           
                                     'ShortDescription'   IS 'по ставке 18',    
                                     'FullDescription'    IS 'Сумма налога, исчисленная налоговым агентом с начала года - по ставке 18',        
                                     'BrokerageData'      IS '0,00',    
                                     'DepositoryData'     IS '0,00',    
                                     'GeneralTaxBaseData' IS GetTotalCalcField(p_GUID, 'OnbTaxDueYear_18') FORMAT JSON
                                    ) FORMAT JSON,
                         JSON_OBJECT('TaxType'            IS 'TwentyRateTaxAmount',             
                                     'ShortDescription'   IS 'по ставке 20',    
                                     'FullDescription'    IS 'Сумма налога, исчисленная налоговым агентом с начала года - по ставке 20',        
                                     'BrokerageData'      IS '0,00',    
                                     'DepositoryData'     IS '0,00',    
                                     'GeneralTaxBaseData' IS GetTotalCalcField(p_GUID, 'OnbTaxDueYear_20') FORMAT JSON
                                    ) FORMAT JSON,
                         JSON_OBJECT('TaxType'            IS 'TwentyTwoRateTaxAmount',          
                                     'ShortDescription'   IS 'по ставке 22',    
                                     'FullDescription'    IS 'Сумма налога, исчисленная налоговым агентом с начала года - по ставке 22',        
                                     'BrokerageData'      IS '0,00',    
                                     'DepositoryData'     IS '0,00',    
                                     'GeneralTaxBaseData' IS GetTotalCalcField(p_GUID, 'OnbTaxDueYear_22') FORMAT JSON
                                    ) FORMAT JSON,
                         JSON_OBJECT('TaxType'            IS 'WithheldTotalTaxAmount',          
                                     'ShortDescription'   IS 'Сумма налога, удержанная налоговым агентом в течение года,  в том числе:',       
                                     'FullDescription'    IS 'Сумма налога, удержанная налоговым агентом в течение года - всего',       
                                     'BrokerageData'      IS GetTotalCalcField(p_GUID, 'TaxPaid_all') FORMAT JSON,   
                                     'DepositoryData'     IS GetTotalCalcField(p_GUID, 'DepoTaxPaid_all') FORMAT JSON,       
                                     'GeneralTaxBaseData' IS GetTotalCalcField(p_GUID, 'OnbTaxPaid_all') FORMAT JSON
                                    ) FORMAT JSON,
                         JSON_OBJECT('TaxType'            IS 'WithheldGeneralTaxAmount',        
                                     'ShortDescription'   IS 'по общей ставке (13 или 30)',     
                                     'FullDescription'    IS 'Сумма налога, удержанная налоговым агентом в течение года - по общей ставке (13 или 30)', 
                                     'BrokerageData'      IS GetTotalCalcField(p_GUID, 'TaxPaid') FORMAT JSON,       
                                     'DepositoryData'     IS GetTotalCalcField(p_GUID, 'DepoTaxPaid') FORMAT JSON,   
                                     'GeneralTaxBaseData' IS GetTotalCalcField(p_GUID, 'OnbTaxPaid') FORMAT JSON
                                    ) FORMAT JSON,
                         JSON_OBJECT('TaxType'            IS 'WithheldReducedTaxAmount',        
                                     'ShortDescription'   IS 'по ставке 15',    
                                     'FullDescription'    IS 'Сумма налога, удержанная налоговым агентом в течение года - по ставке 15',        
                                     'BrokerageData'      IS GetTotalCalcField(p_GUID, 'TaxPaid_15') FORMAT JSON,    
                                     'DepositoryData'     IS GetTotalCalcField(p_GUID, 'DepoTaxPaid_15') FORMAT JSON,        
                                     'GeneralTaxBaseData' IS GetTotalCalcField(p_GUID, 'OnbTaxPaid_15') FORMAT JSON
                                    ) FORMAT JSON,
                         JSON_OBJECT('TaxType'            IS 'WithheldEighteenTaxAmount',       
                                     'ShortDescription'   IS 'по ставке 18',    
                                     'FullDescription'    IS 'Сумма налога, удержанная налоговым агентом в течение года - по ставке 18',        
                                     'BrokerageData'      IS '0,00',    
                                     'DepositoryData'     IS '0,00',    
                                     'GeneralTaxBaseData' IS GetTotalCalcField(p_GUID, 'OnbTaxPaid_18') FORMAT JSON
                                    ) FORMAT JSON,
                         JSON_OBJECT('TaxType'            IS 'WithheldTwentyTaxAmount',         
                                     'ShortDescription'   IS 'по ставке 20',    
                                     'FullDescription'    IS 'Сумма налога, удержанная налоговым агентом в течение года - по ставке 20',        
                                     'BrokerageData'      IS '0,00',    
                                     'DepositoryData'     IS '0,00',    
                                     'GeneralTaxBaseData' IS GetTotalCalcField(p_GUID, 'OnbTaxPaid_20') FORMAT JSON
                                    ) FORMAT JSON,
                         JSON_OBJECT('TaxType'            IS 'WithheldTwentyTwoTaxAmount',      
                                     'ShortDescription'   IS 'по ставке 22',    
                                     'FullDescription'    IS 'Сумма налога, удержанная налоговым агентом в течение года - по ставке 22',        
                                     'BrokerageData'      IS '0,00',    
                                     'DepositoryData'     IS '0,00',    
                                     'GeneralTaxBaseData' IS GetTotalCalcField(p_GUID, 'OnbTaxPaid_22') FORMAT JSON
                                    ) FORMAT JSON,
                         JSON_OBJECT('TaxType'            IS 'PayOrRefundTaxAmount',            
                                     'ShortDescription'   IS 'Сумма налога, подлежащая удержанию(+) / возврату(-),  в том числе:',      
                                     'FullDescription'    IS 'Сумма налога, подлежащая удержанию/возврату - всего',     
                                     'BrokerageData'      IS GetTotalCalcField(p_GUID, 'TaxDueNow1_all') FORMAT JSON,        
                                     'DepositoryData'     IS GetTotalCalcField(p_GUID, 'DepoTaxDueNow1_all') FORMAT JSON,    
                                     'GeneralTaxBaseData' IS GetTotalCalcField(p_GUID, 'OnbTaxDueNow1_all') FORMAT JSON
                                    ) FORMAT JSON,
                         JSON_OBJECT('TaxType'            IS 'PayOrRefundGeneralTaxAmount',     
                                     'ShortDescription'   IS 'по общей ставке (13 или 30)',     
                                     'FullDescription'    IS 'Сумма налога, подлежащая удержанию/возврату - по общей ставке (13 или 30)',       
                                     'BrokerageData'      IS GetTotalCalcField(p_GUID, 'TaxDueNow1') FORMAT JSON,    
                                     'DepositoryData'     IS GetTotalCalcField(p_GUID, 'DepoTaxDueNow1') FORMAT JSON,        
                                     'GeneralTaxBaseData' IS GetTotalCalcField(p_GUID, 'OnbTaxDueNow1') FORMAT JSON
                                    ) FORMAT JSON,
                         JSON_OBJECT('TaxType'            IS 'PayOrRefundReducedTaxAmount',     
                                     'ShortDescription'   IS 'по ставке 15',    
                                     'FullDescription'    IS 'Сумма налога, подлежащая удержанию/возврату - по ставке 15',      
                                     'BrokerageData'      IS GetTotalCalcField(p_GUID, 'TaxDueNow1_15') FORMAT JSON, 
                                     'DepositoryData'     IS GetTotalCalcField(p_GUID, 'DepoTaxDueNow1_15') FORMAT JSON,     
                                     'GeneralTaxBaseData' IS GetTotalCalcField(p_GUID, 'OnbTaxDueNow1_15') FORMAT JSON
                                    ) FORMAT JSON,
                         JSON_OBJECT('TaxType'            IS 'PayOrRefundEighteenTaxAmount',    
                                     'ShortDescription'   IS 'по ставке 18',    
                                     'FullDescription'    IS 'Сумма налога, подлежащая удержанию/возврату - по ставке 18',      
                                     'BrokerageData'      IS '0,00',    
                                     'DepositoryData'     IS '0,00',    
                                     'GeneralTaxBaseData' IS GetTotalCalcField(p_GUID, 'OnbTaxDueNow1_18') FORMAT JSON
                                    ) FORMAT JSON,
                         JSON_OBJECT('TaxType'            IS 'PayOrRefundTwentyTaxAmount',      
                                     'ShortDescription'   IS 'по ставке 20',    
                                     'FullDescription'    IS 'Сумма налога, подлежащая удержанию/возврату - по ставке 20',      
                                     'BrokerageData'      IS '0,00',    
                                     'DepositoryData'     IS '0,00',    
                                     'GeneralTaxBaseData' IS GetTotalCalcField(p_GUID, 'OnbTaxDueNow1_20') FORMAT JSON
                                    ) FORMAT JSON,
                         JSON_OBJECT('TaxType'            IS 'PayOrRefundTwentyTwoTaxAmount',   
                                     'ShortDescription'   IS 'по ставке 22',    
                                     'FullDescription'    IS 'Сумма налога, подлежащая удержанию/возврату - по ставке 22',      
                                     'BrokerageData'      IS '0,00',    
                                     'DepositoryData'     IS '0,00',    
                                     'GeneralTaxBaseData' IS GetTotalCalcField(p_GUID, 'OnbTaxDueNow1_22') FORMAT JSON
                                    ) FORMAT JSON
                       RETURNING CLOB
                      ) 
      INTO v_ClientTaxInfoNode
      FROM DUAL;

    RETURN v_ClientTaxInfoNode;
  END;

  FUNCTION GetRealizationNode(p_GUID IN VARCHAR2) RETURN CLOB
  AS
    v_RealizationNode CLOB;
  BEGIN

    SELECT JSON_ARRAYAGG(JSON_OBJECT( 'SecCode'                IS T_SECCODE
                                     ,'SecName'                IS T_SECNAME
                                     ,'Quantity'               IS CorrectNumPrecision(T_QNTY)
                                     ,'ContractNumberSell'     IS T_CONTRACTS   
                                     ,'TransactionNumberSell'  IS T_CODES       
                                     ,'TransactionDateSell'    IS CorrectDateStr(T_DZS)         
                                     ,'PaymentDateSell'        IS CorrectDateStr(T_DPS)         
                                     ,'TransactionPriceSell'   IS CorrectNumPrecision(T_PRSRUB, T_SPOINT)      
                                     ,'TransactionSumNkdSell'  IS CorrectMoneyStr(T_ALLSRUB)     
                                     ,'DevitationSumSell'      IS CorrectMoneyStr(T_DIFS)        
                                     ,'BankCommissionSell'     IS CorrectMoneyStr(T_COMBANKS)    
                                     ,'ExchangeCommissionSell' IS CorrectMoneyStr(T_COMEXCS)     
                                     ,'CommissionSumSell'      IS CorrectMoneyStr(T_COMS)        
                                     ,'ContractNumberBuy'      IS T_CONTRACTB
                                     ,'TransactionNumberBuy'   IS T_CODEB
                                     ,'TransactionDateBuy'     IS CorrectDateStr(T_DZB)
                                     ,'PaymentDateBuy'         IS CorrectDateStr(T_DPB)
                                     ,'TransactionPriceBuy'    IS CorrectNumPrecision(T_PRBRUB, T_BPOINT)
                                     ,'TransactionSumNkdBuy'   IS CorrectMoneyStr(T_ALLBRUB)
                                     ,'FinancialGainBuy'       IS CorrectMoneyStr(T_MATERIALB)
                                     ,'FinancialGainTaxBuy'    IS CorrectMoneyStr(T_TAXMATERIALB)
                                     ,'DevitationSumBuy'       IS CorrectMoneyStr(T_DIFB)
                                     ,'BankCommissionBuy'      IS CorrectMoneyStr(T_COMBANKB)
                                     ,'ExchangeCommissionBuy'  IS CorrectMoneyStr(T_COMEXCB)
                                     ,'CommissionSumBuy'       IS CorrectMoneyStr(T_COMB)
                                     ,'TaxationAmount'         IS CorrectMoneyStr(T_TAXFRLINK)
                                     ,'SecMarketability'       IS T_SECTYPE
                                     ,'TaxationPeculiarities'  IS T_TAXTAGS
                                    ABSENT ON NULL)
                      ORDER BY T_GUID ASC, T_ITOG ASC, T_SORT ASC
                      RETURNING CLOB)
      INTO v_RealizationNode                                                   
      FROM DNPTXSHORT_REALIZ_DBT 
     WHERE T_GUID = p_GUID AND T_ITOG = CHR(0);

    RETURN v_RealizationNode;
  END;

  FUNCTION GetShortPositionNode(p_GUID IN VARCHAR2) RETURN CLOB
  AS
    v_ShortPositionNode CLOB;
  BEGIN

    SELECT JSON_ARRAYAGG(JSON_OBJECT( 'SecCode'                IS T_SECCODE     
                                     ,'SecName'                IS T_SECNAME     
                                     ,'Quantity'               IS CorrectNumPrecision(T_QNTY)
                                     ,'ContractNumberSell'     IS T_CONTRACTS   
                                     ,'TransactionNumberSell'  IS T_CODES       
                                     ,'TransactionDateSell'    IS CorrectDateStr(T_DZS)         
                                     ,'PaymentDateSell'        IS CorrectDateStr(T_DPS)         
                                     ,'TransactionPriceSell'   IS CorrectNumPrecision(T_PRSRUB, T_SPOINT)       
                                     ,'TransactionSumNkdSell'  IS CorrectMoneyStr(T_ALLSRUB)     
                                     ,'DevitationSumSell'      IS CorrectMoneyStr(T_DIFS)        
                                     ,'BankCommissionSell'     IS CorrectMoneyStr(T_COMBANKS)    
                                     ,'ExchangeCommissionSell' IS CorrectMoneyStr(T_COMEXCS)     
                                     ,'CommissionSumSell'      IS CorrectMoneyStr(T_COMS)        
                                     ,'ContractNumberBuy'      IS T_CONTRACTB   
                                     ,'TransactionNumberBuy'   IS T_CODEB       
                                     ,'TransactionDateBuy'     IS CorrectDateStr(T_DZB)         
                                     ,'PaymentDateBuy'         IS CorrectDateStr(T_DPB)         
                                     ,'TransactionPriceBuy'    IS CorrectNumPrecision(T_PRBRUB, T_BPOINT)      
                                     ,'TransactionSumNkdBuy'   IS CorrectMoneyStr(T_ALLBRUB)     
                                     ,'FinancialGainBuy'       IS CorrectMoneyStr(T_MATERIALB)   
                                     ,'FinancialGainTaxBuy'    IS CorrectMoneyStr(T_TAXMATERIALB)
                                     ,'DevitationSumBuy'       IS CorrectMoneyStr(T_DIFB)        
                                     ,'BankCommissionBuy'      IS CorrectMoneyStr(T_COMBANKB)    
                                     ,'ExchangeCommissionBuy'  IS CorrectMoneyStr(T_COMEXCB)     
                                     ,'CommissionSumBuy'       IS CorrectMoneyStr(T_COMB)        
                                     ,'TaxationAmount'         IS T_TAXFRLINK   
                                     ,'SecMarketability'       IS T_SECTYPE     
                                     ,'TaxationPeculiarities'  IS T_TAXTAGS     
                                    ABSENT ON NULL)
                      ORDER BY T_GUID ASC, T_ITOG ASC, T_SORT ASC
                      RETURNING CLOB)
      INTO v_ShortPositionNode                                                   
      FROM DNPTXSHORT_SHORTPOS_DBT 
     WHERE T_GUID = p_GUID AND T_ITOG = CHR(0);

    RETURN v_ShortPositionNode;
  END;

  FUNCTION GetRepoTransactionNode(p_GUID IN VARCHAR2) RETURN CLOB
  AS
    v_RepoTransactionNode CLOB;
  BEGIN

    SELECT JSON_ARRAYAGG(JSON_OBJECT( 'SecCode'             IS T_SECCODE    
                                     ,'SecName'             IS T_SECNAME    
                                     ,'ContractNumber'      IS T_CONTRACTS  
                                     ,'RepoNumber'          IS T_CODER      
                                     ,'RepoDate'            IS CorrectDateStr(T_DZ)         
                                     ,'SecQuantity'         IS CorrectNumPrecision(T_QNTY, T_SUMPRECISION)       
                                     ,'FirstPartDirection'  IS T_DIR1       
                                     ,'FirstPartSecDate'    IS CorrectDateStr(T_DD1)               
                                     ,'FirstPartFundsDate'  IS CorrectDateStr(T_DP1)        
                                     ,'FirstPartTotalCost'  IS CorrectMoneyStr(T_TOT1VP)     
                                     ,'RepoRPercent'        IS T_RATE       
                                     ,'SecondPartDirection' IS T_DIR2       
                                     ,'SecondPartSecDate'   IS CorrectDateStr(T_DD2)        
                                     ,'SecondPartFundsDate' IS CorrectDateStr(T_DP2)        
                                     ,'SecondPartTotalCost' IS CorrectMoneyStr(T_DEAL2VP)    
                                     ,'RepoSecondLegAmount' IS CorrectMoneyStr(T_TOT2VP)     
                                     ,'IncomeAmount'        IS CorrectMoneyStr(T_DOHRUB)     
                                     ,'ActualCosts'         IS CorrectMoneyStr(T_RASHRUB)          
                                     ,'TaxCosts'            IS CorrectMoneyStr(T_TAXPASHRUB) 
                                     ,'RepoCommission'      IS CorrectMoneyStr(T_COM)        
                                     ,'CouponSum'           IS CorrectMoneyStr(T_COUPCALCVN) 
                                     ,'PartialRepayment'    IS CorrectMoneyStr(T_AMORTCALCVN)
                                     ,'TaxationAmount'      IS CorrectMoneyStr(T_GENRUB)     
                                    ABSENT ON NULL)
                      ORDER BY T_GUID ASC, T_ITOG ASC, T_SORT ASC
                      RETURNING CLOB)
      INTO v_RepoTransactionNode                                                   
      FROM DNPTXSHORT_REPO_DBT 
     WHERE T_GUID = p_GUID AND T_ITOG = CHR(0);

    RETURN v_RepoTransactionNode;
  END;

  FUNCTION GetFinancialGainNode(p_GUID IN VARCHAR2) RETURN CLOB
  AS
    v_FinancialGainNode CLOB;
  BEGIN

    SELECT JSON_ARRAYAGG(JSON_OBJECT( 'SecCode'                            IS T_SECCODE   
                                     ,'SecName'                            IS T_SECNAME   
                                     ,'NominalCurrencyCode'                IS T_VN        
                                     ,'ContractNumber'                     IS T_CONTRACTB 
                                     ,'AcquisitionTransactionNumber'       IS T_CODEB     
                                     ,'AcquisitionTransactionDate'         IS CorrectDateStr(T_DZB)       
                                     ,'OwnershipTransferDate'              IS CorrectDateStr(T_DDB)       
                                     ,'AcquisitionPaymentDate'             IS CorrectDateStr(T_DPB)       
                                     ,'SecQuantity'                        IS CorrectNumPrecision(T_QNTY, T_SUMPRECISION)      
                                     ,'AcquisitionPrice'                   IS CorrectMoneyStr(T_PRBVP)     
                                     ,'AcquisitionPriceCurrency'           IS T_VALB      
                                     ,'AcquisitionPurchaseCurrency'        IS T_VPB       
                                     ,'CBExchangeRate'                     IS CorrectMoneyStr(T_CRBD)      
                                     ,'AcquisitionConversionRate'          IS CorrectMoneyStr(T_KZB, 1)       
                                     ,'PaymentAmountInCurrency'            IS CorrectMoneyStr(T_MAINBVP)   
                                     ,'Amount'                             IS CorrectMoneyStr(T_MAINBRUB)  
                                     ,'AcquisitionRedeemableSec'           IS T_BMRKTB    
                                     ,'MarketPriceLowerLimit'              IS CorrectMoneyStr(T_PRICEMRKTB, 1)
                                     ,'InformationSource'                  IS T_MRKTB     
                                     ,'QuotationDate'                      IS CorrectDateStr(T_DMRKTB)    
                                     ,'FinancialGain'                      IS CorrectMoneyStr(T_MATERIAL)  
                                     ,'AcquisitionPaidCommission'          IS CorrectMoneyStr(T_COMB)      
                                     ,'AcquisitionDateNominalValue'        IS CorrectMoneyStr(T_NOMB, 1)      
                                     ,'AcquisitionTransactionCounterparty' IS T_CONTB     
                                     ,'TaxGroup'                           IS T_SECTYPE   
                                     ,'TaxRate'                            IS T_TAXRATE     
                                    ABSENT ON NULL)
                      ORDER BY T_GUID ASC, T_ITOG ASC, T_SORT ASC
                      RETURNING CLOB)
      INTO v_FinancialGainNode                                                   
      FROM DNPTXSHORT_LUCRE_DBT 
     WHERE T_GUID = p_GUID AND T_ITOG = CHR(0);

    RETURN v_FinancialGainNode;
  END;

  FUNCTION GetInvestmentDeductionNode(p_GUID IN VARCHAR2) RETURN CLOB
  AS
    v_InvestmentDeductionNode CLOB;
  BEGIN

    SELECT JSON_ARRAYAGG(JSON_OBJECT( 'SecCode'                      IS T_SECCODE     
                                     ,'SecName'                      IS T_SECNAME     
                                     ,'SecQuantity'                  IS CorrectNumPrecision(T_QNTY)        
                                     ,'ContractNumberSell'           IS T_CONTRACTS   
                                     ,'TransactionNumberSell'        IS T_CODES       
                                     ,'TransactionDateSell'          IS CorrectDateStr(T_DZS)         
                                     ,'PaymentDateSell'              IS CorrectDateStr(T_DPS)         
                                     ,'TransactionPriceSell'         IS CorrectMoneyStr(T_PRSRUB)      
                                     ,'TransactionSumNkdSell'        IS CorrectMoneyStr(T_ALLSRUB)     
                                     ,'CommissionSumSell'            IS CorrectMoneyStr(T_COMS)
                                     ,'ContractNumberBuy'            IS T_CONTRACTB   
                                     ,'TransactionNumberBuy'         IS T_CODEB       
                                     ,'TransactionDateBuy'           IS CorrectDateStr(T_DZB)         
                                     ,'PaymentDateBuy'               IS CorrectDateStr(T_DPB)         
                                     ,'TransactionPriceBuy'          IS CorrectMoneyStr(T_PRBRUB)      
                                     ,'TransactionSumNkdBuy'         IS CorrectMoneyStr(T_ALLBRUB)     
                                     ,'CommissionSumBuy'             IS CorrectMoneyStr(T_COMB)        
                                     ,'SecSaleIncome'                IS CorrectMoneyStr(T_PLUSI)       
                                     ,'SecFinancialResult'           IS CorrectMoneyStr(T_FR_SEC_3Y)   
                                     ,'TaxpayerSecOwnershipDuration' IS CorrectMoneyStr(T_TAXTAGSYEARS)     
                                    ABSENT ON NULL)
                      ORDER BY T_GUID ASC, T_ITOG ASC, T_SORT ASC
                      RETURNING CLOB)
      INTO v_InvestmentDeductionNode                                                   
      FROM DNPTXSHORT_INVEST_DBT 
     WHERE T_GUID = p_GUID AND T_ITOG = CHR(0);

    RETURN v_InvestmentDeductionNode;
  END;

  FUNCTION GetExchangeTradedDerivativesNode(p_GUID IN VARCHAR2) RETURN CLOB
  AS
    v_ExchangeTradedDerivativesNode CLOB;
  BEGIN

    SELECT JSON_ARRAYAGG(JSON_OBJECT( 'Client'                        IS T_CLIENT      
                                     ,'ClientStatus'                  IS T_CLIENTSTATUS
                                     ,'SecCode'                       IS T_SECCODE     
                                     ,'DerivativeName'                IS T_FICODE      
                                     ,'DerivativeType'                IS T_FINAME      
                                     ,'BaseSecType'                   IS T_FITYPE      
                                     ,'BaseSecCode'                   IS T_BASETYPE    
                                     ,'BaseSecName'                   IS T_BASECODE    
                                     ,'ExchangeName'                  IS T_BASENAME    
                                     ,'TradePlace'                    IS T_TRADEPLACE  
                                     ,'ContractNumber'                IS T_CONTRACTS   
                                     ,'SecDate'                       IS CorrectDateStr(T_DPOS)        
                                     ,'SecPurchasedQuantityTheDay'    IS CorrectMoneyStr(T_QBUY, 1)        
                                     ,'SecSoldQuantityTheDay'         IS CorrectMoneyStr(T_QSELL, 1)       
                                     ,'DayEndOpenPosition'            IS CorrectMoneyStr(T_QLONG, 1)       
                                     ,'FinancialGain'                 IS CorrectMoneyStr(T_MATERIAL)        
                                     ,'ReceivedVariationMarginAmount' IS CorrectMoneyStr(T_VARPOS)      
                                     ,'PaidVariationMarginAmount'     IS CorrectMoneyStr(T_VARNEG)      
                                     ,'ReceivedPremiumAmount'         IS CorrectMoneyStr(T_PREMPOS)     
                                     ,'PaidPremiumAmount'             IS CorrectMoneyStr(T_PREMNEG)     
                                     ,'Commission'                    IS CorrectMoneyStr(T_COM)         
                                     ,'FinancialResult'               IS CorrectMoneyStr(T_FR)          
                                     ,'SecMarketability'              IS T_BMARKET     
                                     ,'TaxRate'                       IS CorrectMoneyStr(T_TAXRATE)     
                                    ABSENT ON NULL)
                      ORDER BY T_GUID ASC, T_ITOG ASC, T_SORT ASC
                      RETURNING CLOB)
      INTO v_ExchangeTradedDerivativesNode                                                   
      FROM DNPTXSHORT_PFIEXCHANGE_DBT 
     WHERE T_GUID = p_GUID AND T_ITOG = CHR(0);

    RETURN v_ExchangeTradedDerivativesNode;
  END;

  FUNCTION GetOTCDerivativesNode(p_GUID IN VARCHAR2) RETURN CLOB
  AS
    v_OTCDerivativesNode CLOB;
  BEGIN

    SELECT JSON_ARRAYAGG(JSON_OBJECT( 'Client'                        IS T_CLIENT            
                                     ,'ClientStatus'                  IS T_CLIENTSTATUS      
                                     ,'ContractNumber'                IS T_CONTRACTS         
                                     ,'TransactionDate'               IS CorrectDateStr(T_DEALDATE)          
                                     ,'TransactionNumber'             IS T_CONTRACTN         
                                     ,'ContractType'                  IS T_CONTRACTTYPE      
                                     ,'Direction'                     IS T_DIRECTION         
                                     ,'Contract'                      IS T_CONTRACT          
                                     ,'InitialAssetsType'             IS T_BASETYPE          
                                     ,'InitialAssetsCode'             IS T_FITYPE            
                                     ,'ExecutionDate'                 IS CorrectDateStr(T_EXPIRATIONDATE)    
                                     ,'InitialAssetsQuantity'         IS CorrectMoneyStr(T_Q, 1)                 
                                     ,'CounterpartyName'              IS T_CONTRAGENT          
                                     ,'InitialAssetsPriceCurrency'    IS CorrectMoneyStr(T_EXPIRATIONPRICECUR, 1)  
                                     ,'PriceCurrency'                 IS T_PRICECURRENCY       
                                     ,'MarketPrice'                   IS CorrectMoneyStr(T_MARKETPRICE)          
                                     ,'FinancialGain'                 IS CorrectMoneyStr(T_MATERIAL)          
                                     ,'ReceivedVariationMarginAmount' IS CorrectMoneyStr(T_VARPOS)            
                                     ,'PaidVariationMarginAmount'     IS CorrectMoneyStr(T_VARNEG)            
                                     ,'ReceivedPremiumAmount'         IS CorrectMoneyStr(T_PREMPOS)           
                                     ,'PaidPremiumAmount'             IS CorrectMoneyStr(T_PREMNEG)           
                                     ,'PaidCommissionAmount'          IS CorrectMoneyStr(T_COM)               
                                     ,'FinancialResult'               IS CorrectMoneyStr(T_FR)                
                                    ABSENT ON NULL)
                      ORDER BY T_GUID ASC, T_ITOG ASC, T_SORT ASC
                      RETURNING CLOB)
      INTO v_OTCDerivativesNode                                                   
      FROM DNPTXSHORT_PFIOUTEXCHANGE_DBT 
     WHERE T_GUID = p_GUID AND T_ITOG = CHR(0);

    RETURN v_OTCDerivativesNode;
  END;

  FUNCTION GetPromissoryNotesNode(p_GUID IN VARCHAR2) RETURN CLOB
  AS
    v_PromissoryNotesNode CLOB;
  BEGIN

    SELECT JSON_ARRAYAGG(JSON_OBJECT( 'DocNumber'             IS T_DOCNUMBER
                                     ,'OperationDate'         IS CorrectDateStr(T_DOPER)    
                                     ,'PromissoryNotesNumber' IS T_NBILL    
                                     ,'PromissoryNotesCost'   IS CorrectMoneyStr(T_COST)     
                                     ,'RepaymentAmount'       IS CorrectMoneyStr(T_REPAYMENT)
                                     ,'PercentRUB'            IS CorrectMoneyStr(T_INTEREST) 
                                     ,'DiscountRUB'           IS CorrectMoneyStr(T_DISCOUNT) 
                                     ,'SumNOBRUB'             IS CorrectMoneyStr(T_SUMNOB)   
                                    ABSENT ON NULL)
                      ORDER BY T_GUID ASC, T_ITOG ASC, T_SORT ASC
                      RETURNING CLOB)
      INTO v_PromissoryNotesNode                                                   
      FROM DNPTXSHORT_BILL_DBT 
     WHERE T_GUID = p_GUID AND T_ITOG = CHR(0);

    RETURN v_PromissoryNotesNode;
  END;

  FUNCTION GetProtocolNode(p_GUID IN VARCHAR2) RETURN CLOB
  AS
    v_ProtocolNode CLOB;
  BEGIN

    SELECT JSON_ARRAYAGG(JSON_OBJECT( 'IncomeNDR'   IS T_INCOMENDR  
                                     ,'IncomeCode'  IS T_INCOMECODE 
                                     ,'IncomeName'  IS T_INCOMENAME 
                                     ,'IncomeSum'   IS CorrectMoneyStr(T_INCOMESUM)  
                                     ,'ExpenseNDR'  IS T_EXPENSENDR 
                                     ,'ExpenseCode' IS T_EXPENSECODE
                                     ,'ExpenseName' IS T_EXPENSENAME
                                     ,'ExpenseSum'  IS CorrectMoneyStr(T_EXPENSESUM) 
                                    ABSENT ON NULL)
                      ORDER BY T_GUID ASC, T_ITOG ASC, T_SORT ASC
                      RETURNING CLOB)
      INTO v_ProtocolNode                                                   
      FROM DNPTXSHORT_PROTOCOL_DBT 
     WHERE T_GUID = p_GUID AND T_ITOG = CHR(0);

    RETURN v_ProtocolNode;
  END;

  FUNCTION CreateSendTaxReportReqJSON(p_GUID IN VARCHAR2) RETURN CLOB
  AS
    v_SendTaxReportReqJSON CLOB;

    v_RepParm DNPTXSHORT_REPPARM_DBT%ROWTYPE;

    v_RequestTimeStr VARCHAR2(30);
  BEGIN

    SELECT * INTO v_RepParm FROM DNPTXSHORT_REPPARM_DBT WHERE T_GUID = p_GUID;

    v_RequestTimeStr := it_xml.timestamp_to_char_iso8601(SYSDATE);

    SELECT JSON_OBJECT('GUID'                      IS p_GUID,
                       'GUIDReq'                   IS v_RepParm.t_ReqGUID,
                       'RequestTime'               IS v_RequestTimeStr,
                       'Dbeg'                      IS GetTotalCalcField(p_GUID, 'Dbeg') FORMAT JSON,
                       'Dend'                      IS GetTotalCalcField(p_GUID, 'Dend') FORMAT JSON,
                       'Dsys'                      IS GetTotalCalcField(p_GUID, 'Dsys') FORMAT JSON,
                       'Client'                    IS GetTotalCalcField(p_GUID, 'Client') FORMAT JSON,
                       'ClientStatus'              IS GetTotalCalcField(p_GUID, 'ClientStatus') FORMAT JSON,
                       'ReportType'                IS GetTotalCalcField(p_GUID, 'ReportType') FORMAT JSON,
                       'LastNOBDate'               IS GetTotalCalcField(p_GUID, 'DlastNOB') FORMAT JSON,
                       'InvestmentDeductionAmount' IS GetTotalCalcField(p_GUID, 'SumFR') FORMAT JSON,
                       'NOBInvestemntDeduction'    IS GetTotalCalcField(p_GUID, 'SumMinusG_618') FORMAT JSON,
                       'LimitDeductionRatio'       IS GetTotalCalcField(p_GUID, 'Index_618') FORMAT JSON,
                       'InvestmentDeductionLimit'  IS GetTotalCalcField(p_GUID, 'Limit_618') FORMAT JSON,
                       'SNOB'                      IS GetSNOBNode(p_GUID) FORMAT JSON,
                       'ClientTaxInfo'             IS GetClientTaxInfoNode(p_GUID) FORMAT JSON,
                       'Realization'               IS GetRealizationNode(p_GUID) FORMAT JSON,
                       'ShortPosition'             IS GetShortPositionNode(p_GUID) FORMAT JSON,
                       'RepoTransactions'          IS GetRepoTransactionNode(p_GUID) FORMAT JSON,
                       'FinancialGain'             IS GetFinancialGainNode(p_GUID) FORMAT JSON,
                       'InvestmentDeduction'       IS GetInvestmentDeductionNode(p_GUID) FORMAT JSON,
                       'ExchangeTradedDerivatives' IS GetExchangeTradedDerivativesNode(p_GUID) FORMAT JSON,
                       'OTCDerivatives'            IS GetOTCDerivativesNode(p_GUID) FORMAT JSON,
                       'PromissoryNotes'           IS GetPromissoryNotesNode(p_GUID) FORMAT JSON,
                       'Protocol'                  IS GetProtocolNode(p_GUID) FORMAT JSON
                       ABSENT ON NULL
                       RETURNING CLOB
                      )
                       
      INTO v_SendTaxReportReqJSON
      FROM DUAL;

    v_SendTaxReportReqJSON := REPLACE(v_SendTaxReportReqJSON, '\u0001', '');

    RETURN v_SendTaxReportReqJSON;
  END;


  PROCEDURE CreateJSON(p_ClientID IN NUMBER, p_TaxPeriod IN NUMBER, p_GUIDReq IN VARCHAR2, p_GUID OUT VARCHAR2, p_SendTaxReportReqJSON OUT CLOB, p_ErrorCode OUT NUMBER, p_ErrorDesc OUT VARCHAR2 ) 
  AS
    v_StartTime   NUMBER;
    v_EndTime     NUMBER;
    v_ElapsedTime NUMBER;
    v_MaxWaitingTime NUMBER; --Макисмальное время ожидания в минутах

    v_Status NUMBER;
    v_ErrorDesc VARCHAR2(512);

    v_cnt NUMBER := 0;
    v_EndDate DATE;

  BEGIN

    p_ErrorCode := 0;
    p_ErrorDesc := '';

    v_MaxWaitingTime := NVL(Rsb_Common.GetRegIntValue('COMMON\НДФЛ\MAX_ВРЕМЯ_ФОРМ_НДФЛ_SINV'), 120);

    SELECT CAST(SYS_GUID() AS VARCHAR2(32)) INTO p_GUID FROM dual;

    p_GUID := substr(p_GUID, 1, 8)||'-'||substr(p_GUID, 9, 4)||'-'||substr(p_GUID, 13, 4)||'-'||substr(p_GUID, 17, 4)||'-'||substr(p_GUID, 21);
    
    IF p_TaxPeriod = EXTRACT(YEAR FROM SYSDATE) THEN
        v_EndDate := TRUNC(SYSDATE); 
    ELSE
        v_EndDate := TO_DATE(p_TaxPeriod || '1231', 'YYYYMMDD');
    END IF;

    --Заполнем параметры для подготовки данных отчета
    INSERT INTO DNPTXSHORT_REPPARM_DBT (T_GUID, 
                                        T_REQGUID,
                                        T_STATUS,
                                        T_ENDDATE, 
                                        T_REPTYPE, 
                                        T_BYDEPO,  
                                        T_AVRKIND, 
                                        T_AVRFIID, 
                                        T_NEEDREALIZLIST, 
                                        T_NEEDSHORTPOSLIST, 
                                        T_NEEDREPOLIST, 
                                        T_NEEDLUCRELIST, 
                                        T_NEEDPFILIST, 
                                        T_NEEDBILLLIST,    
                                        T_NEEDPROTOCOLLIST, 
                                        T_INCLUDE_TECH, 
                                        T_BYIIS, 
                                        T_CLIENTID, 
                                        T_DLCONTRID 
                                        ) 
                               VALUES (p_GUID,     --T_GUID,            
                                       p_GUIDReq,  --T_REQGUID,         
                                       0,          --T_STATUS,          
                                       v_EndDate,  --T_ENDDATE,         
                                       10,         --T_REPTYPE, Окончание года         
                                       'X',        --T_BYDEPO,          
                                       0,          --T_AVRKIND,         
                                       -1,         --T_AVRFIID,         
                                       'X',        --T_NEEDREALIZLIST,
                                       'X',        --T_NEEDSHORTPOSLIST,
                                       'X',        --T_NEEDREPOLIST,
                                       'X',        --T_NEEDLUCRELIST,
                                       'X',        --T_NEEDPFILIST,
                                       'X',        --T_NEEDBILLLIST,
                                       'X',        --T_NEEDPROTOCOLLIST,
                                       CHR(0),     --T_INCLUDE_TECH,    
                                       CHR(0),     --T_BYIIS,           
                                       p_ClientID, --T_CLIENTID,        
                                       0           --T_DLCONTRID        
                                      );

    --Создаем задание FUNCOBJ
    INSERT INTO DFUNCOBJ_DBT  ( T_ID
                              , T_OBJECTTYPE
                              , T_OBJECTID
                              , T_FUNCID
                              , T_PRIORITY
                              , T_PARAM
                              ) VALUES
                              ( 0
                              , 0
                              , 0
                              , 5301
                              , 1
                              , p_GUID
                              );

    COMMIT;

    v_StartTime := DBMS_UTILITY.GET_TIME;
    --Ждём подготовку данных
    WHILE 1 = 1 LOOP
      DBMS_LOCK.SLEEP(20); --Ждём 20 секунд

      --Делаем проверку статуса подготовки данных
      SELECT T_STATUS, T_ERRORDESC INTO v_Status, v_ErrorDesc
        FROM DNPTXSHORT_REPPARM_DBT
       WHERE T_GUID = p_GUID;

      IF v_Status > 0 THEN
        IF v_Status = 2 THEN 
          p_ErrorCode := C_ERROR_CODE_GEN_JSON;
          p_ErrorDesc := 'Ошибка расчета данных: ' || v_ErrorDesc;
        END IF;
        
        EXIT;
      END IF;

      v_EndTime := DBMS_UTILITY.GET_TIME;

      v_ElapsedTime := (v_EndTime - v_StartTime) / 100 / 60; --Прошедшее время ожидания в минутах

      IF v_ElapsedTime >= v_MaxWaitingTime THEN
        p_ErrorCode := C_ERROR_CODE_GEN_JSON;
        p_ErrorDesc := 'Время формирования отчета превышает установленные значения. Для формирования отчета необходимо обратиться в тех.поддержку.';
        EXIT; --Если ожидание затянулось, то прерываем
      END IF;

    END LOOP;

    IF p_ErrorCode = 0 THEN
      SELECT COUNT(1) INTO v_Cnt
        FROM DNPTXSHORT_TOTALCALC_DBT
       WHERE T_GUID = p_GUID;

      IF v_Cnt = 0 THEN
        p_ErrorCode := C_ERROR_CODE_GEN_JSON;
        p_ErrorDesc := 'Данные за налоговый период отсутствуют';
      END IF;
      
      IF(p_ErrorCode = 0) THEN
        SELECT COUNT(1) INTO v_Cnt
          FROM DNPTXSHORT_TOTALCALC_DBT
         WHERE T_GUID = p_GUID
           AND T_FIELDTYPE = 28
           AND  TO_NUMBER(
             CASE WHEN regexp_like(T_FIELDVALUE, '[.,]')
                  THEN REPLACE(T_FIELDVALUE, ',', '.')
                  ELSE T_FIELDVALUE || '.0'
             END, '9999999999999999999990D999999999999', 'NLS_NUMERIC_CHARACTERS = ''.,''') <> 0;
             
        IF v_Cnt = 0 THEN
          p_ErrorCode := C_ERROR_CODE_GEN_JSON;
          p_ErrorDesc := 'Данные за налоговый период отсутствуют';
        END IF;
        
      END IF;
    END IF;

    IF p_ErrorCode = 0 THEN
      --Подготовка данных прошла успешно, формируем JSON
      p_SendTaxReportReqJSON := CreateSendTaxReportReqJSON(p_GUID);

    END IF;

  END;


END RSI_NPTXSHORT2025;
/
