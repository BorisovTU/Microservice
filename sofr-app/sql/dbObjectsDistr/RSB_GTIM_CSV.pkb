CREATE OR REPLACE PACKAGE BODY RSB_GTIM_CSV
IS
  BATCH_SIZE CONSTANT NUMBER(5) := 100; --Размер пачки для сохранения объектов записей репликаций
  CSV_ACCOUNT_OUR_BANK CONSTANT VARCHAR2(2) := 'CL';

  --Структура используемых параметров
  TYPE PRMREC_t IS RECORD (SeanceID           DGTSEANCE_DBT.T_SEANCEID%TYPE, --Идентификатор сеанса
                           Imp_Date           DATE, --Дата импорта
                           Source_Code        DGTAPP_DBT.T_CODE%TYPE, --Код источника
                           MMVB_Code          DOBJCODE_DBT.T_CODE%TYPE, --Код ММВБ
                           MarketReportNumber VARCHAR2(30), --Номер отчёта биржи
                           MarketReportID     NUMBER(10), --Идентификатор отчёта биржи
                           CalcReportID       NUMBER(10), --Идентификатор отчёта операции расчётов
                           isFutures          BOOLEAN, --Обработка фьючерсных контрактов
                           IsRSHB             NUMBER(5) --Признак реализации для РСХБ
                          );

  --Общая структура по данным сделок с ПИ
  TYPE DATADEALS_t IS RECORD (ID_FM_FO04     NUMBER(10),
                              ID_Deal        VARCHAR2(32),
                              ISIN           VARCHAR2(25),
                              Price          NUMBER(16,5),
                              Vol            NUMBER(10),
                              Kod            VARCHAR2(7),
                              BuySell        CHAR(1), 
                              Date1          DATE,
                              DealType       NUMBER(5),
                              DealCode       VARCHAR2(43),
                              ExtDealCode    VARCHAR2(32),
                              UniqCode       VARCHAR2(43),
                              Time1          DATE,
                              Var_Marg       NUMBER(16,2),
                              NoBS           VARCHAR2(32),
                              Fee_NS         NUMBER(16,2),
                              Price_RUR      NUMBER(16,5),
                              Date_CLR       DATE,
                              Fee_Ex         NUMBER(16,2),
                              Fee_CC         NUMBER(16,2),
                              Prem           NUMBER(16,2),
                              DealKind       NUMBER(10),
                              PositionType   NUMBER(5),
                              ExtSettleCode  VARCHAR2(4),
                              CalcCode       VARCHAR2(30),
                              ID_Mult        NUMBER(10),
                              ISIN2          VARCHAR2(25),
                              Price2         NUMBER(16,5),
                              Date2          DATE,
                              Price_RUR2     NUMBER(16,5),
                              Comm           VARCHAR2(20),
                              isExpir        NUMBER(5),
                              IsHDayTrade    CHAR(1),
                              FileDate       DATE  
                             );
                             
  --Общая структура итогов по позициям с ПИ
  TYPE DATAPOS_t IS RECORD (ID_FM_FOPOS    NUMBER(10),
                            Date_CLR       DATE,
                            ISIN           VARCHAR2(25),
                            Kod            VARCHAR2(7),
                            Var_Marg_P     NUMBER(16,2),
                            Var_Marg_D     NUMBER(16,2),
                            Go             NUMBER(16,2),
                            Pos_Exec       NUMBER(10),
                            Sbor_Exec      NUMBER(16,2),
                            ExtSettleCode  VARCHAR2(4), 
                            UniqCode       VARCHAR2(50), 
                            ClientID       NUMBER(10),
                            CalcCode       VARCHAR2(30)
                           );
                           
  --Общая структура заявок на сделки СР
  TYPE DATAREQ_t IS RECORD (ID_FM_FOORDLOG NUMBER(10),
                            Dir            NUMBER(5),
                            N              VARCHAR2(32),
                            ID_ORD         VARCHAR2(32),
                            ISIN           VARCHAR2(25),
                            Date1          DATE,
                            Time1          DATE,
                            Client_Code    VARCHAR2(7),
                            ID_Deal        VARCHAR2(19),
                            Amount         NUMBER(10),
                            Price          NUMBER(16,5),
                            DealKind       NUMBER(10),
                            UniqCode       VARCHAR2(43),
                            Commentar      VARCHAR2(20),
                            IsHDayTrade    CHAR(1),
                            FileDate       DATE 
                           );
                             
  --Структура информации по ногам спредов
  TYPE DATALEG_t IS RECORD   (ID_FM_FO04       NUMBER(10),
                              BuySell          CHAR(1), 
                              ISIN             VARCHAR2(25),
                              Price            NUMBER(16,5),
                              Date1            DATE,
                              Price_RUR        NUMBER(16,5)
                             );
                             
  --Общая структура по данным Производных Инструментов
  TYPE DATAPFI_t IS RECORD   (ID_FM_FO07     NUMBER(10),
                              Contract       VARCHAR2(25),
                              StartDate      DATE,
                              ExecutionDate  DATE,
                              Low            NUMBER(32,12),
                              vHigh          NUMBER(32,12),
                              vClose         NUMBER(32,12),
                              Settl          NUMBER(32,12),
                              Tick_Price     NUMBER(32,12),
                              Tick           NUMBER(32,12),
                              Base_Contr     VARCHAR2(25),
                              FI_Kind        NUMBER(5),
                              NameContr      VARCHAR2(25),
                              Is_Percent     NUMBER(5),
                              Settl_RUR      NUMBER(32,12),
                              Lot_Volume     NUMBER(10),
                              Type_Exec      NUMBER(5),
                              L_TradeDay     DATE,
                              Strike         NUMBER(32,12),
                              Put            CHAR(1), 
                              EvrOp          CHAR(1), 
                              TheorPrice     NUMBER(32,12),
                              Fut_Type       NUMBER(5),
                              UniqCodePFI    VARCHAR2(35),
                              UniqCodeCP     VARCHAR2(40),
                              UniqCodeCPG    VARCHAR2(40),
                              UniqCodeMinP   VARCHAR2(40),
                              UniqCodeMaxP   VARCHAR2(40),
                              UniqCodeTP     VARCHAR2(40),
                              UniqCodeMinSP  VARCHAR2(40),
                              UniqCodeCLP    VARCHAR2(40),
                              ObjectIDPFI    NUMBER(10),
                              ObjectIDCP     NUMBER(10),
                              ObjectIDCPG    NUMBER(10),
                              ObjectIDMinP   NUMBER(10),
                              ObjectIDMaxP   NUMBER(10),
                              ObjectIDTP     NUMBER(10),
                              ObjectIDMinSP  NUMBER(10),
                              ObjectIDCLP    NUMBER(10),
                              PutNum         NUMBER(5),
                              EvrOpNum       NUMBER(5),
                              Imp_Date       DATE,
                              Section        VARCHAR2(50)
                             );
                             
  --Получить ID отчёта биржи, если он загружен раньше
  PROCEDURE GetMarketReportID(p_Prm IN OUT NOCOPY PRMREC_t)
  IS
  BEGIN
  
     SELECT NVL(GTSNCREC.T_OBJECTID,0) INTO p_Prm.MarketReportID
                        FROM DGTSNCREC_DBT GTSNCREC, DGTOBJECT_DBT GTOBJECT 
                       WHERE GTSNCREC.T_SEANCEID   = p_Prm.SeanceID
                         AND GTOBJECT.T_OBJECTKIND = RSB_GTFN.RG_MARKETREPORT
                         AND GTOBJECT.T_OBJECTID   = GTSNCREC.T_OBJECTID;
     EXCEPTION
        WHEN NO_DATA_FOUND THEN p_Prm.MarketReportID := 0;

  END;
  
   --Получить ID отчёта операции расчётов, если он загружен раньше
  PROCEDURE GetCalcReportID(p_Prm IN OUT NOCOPY PRMREC_t)
  IS
  BEGIN
  
     SELECT NVL(GTCODE.T_OBJECTID,0) INTO p_Prm.CalcReportID
                        FROM DGTCODE_DBT GTCODE 
                       WHERE GTCODE.T_OBJECTKIND = RSB_GTFN.RG_DVCALCREP
                         AND GTCODE.T_APPLICATIONID IN (RSB_GTFN.GT_PAYMENTS_CSV, RSB_GTFN.GT_CSV)
                         AND GTCODE.T_OBJECTCODE   = p_Prm.MarketReportNumber;
     EXCEPTION
        WHEN NO_DATA_FOUND THEN p_Prm.CalcReportID := 0;

  END;

  --Инициализировать структуру параметров по передамаемым параметрам в строке
  PROCEDURE SetPrmRecByStr(p_Prm IN OUT NOCOPY PRMREC_t, p_PrmStr IN VARCHAR2)
  IS
    v_PrmStrMap RSB_GTFN.STRMAP_T;
    v_idx NUMBER(5);
  BEGIN
    v_PrmStrMap := RSB_GTFN.GetPrmMapByStr(p_PrmStr);

    BEGIN
      IF v_PrmStrMap.EXISTS('MMVB_Code') THEN
        p_Prm.MMVB_Code := NVL(v_PrmStrMap('MMVB_Code'), CHR(1));
      ELSE
        p_Prm.MMVB_Code := CHR(1);
      END IF;
    EXCEPTION WHEN OTHERS THEN p_Prm.MMVB_Code := CHR(1);
    END;
    
    BEGIN
      IF v_PrmStrMap.EXISTS('MarketReportNumber') THEN
        p_Prm.MarketReportNumber := NVL(v_PrmStrMap('MarketReportNumber'), CHR(1));
      ELSE
        p_Prm.MarketReportNumber := CHR(1);
      END IF;
    EXCEPTION WHEN OTHERS THEN p_Prm.MarketReportNumber := CHR(1);
    END;
    
    BEGIN
      IF v_PrmStrMap.EXISTS('IsRSHB') THEN
        p_Prm.IsRSHB := TO_NUMBER(v_PrmStrMap('IsRSHB'));
      ELSE
        p_Prm.IsRSHB := 0;
      END IF;
    EXCEPTION WHEN OTHERS THEN p_Prm.IsRSHB := 0;
    END;
  END;
  
  --Подготовить отчет биржи
  PROCEDURE PrepareMarketReport(p_Prm IN OUT NOCOPY PRMREC_t)
  IS
    v_stat           NUMBER(5);
    v_ErrMes         VARCHAR2(1000);
  BEGIN
  
    GetMarketReportID(p_Prm);
    IF(p_Prm.MarketReportID = 0) THEN
      v_stat := RSB_GTFN.WriteMarketReport(p_Prm.SeanceID, p_Prm.Source_Code, p_Prm.MarketReportID, v_ErrMes, p_Prm.MarketReportNumber, p_Prm.Imp_Date, p_Prm.MMVB_Code);

      IF v_ErrMes <> CHR(1) THEN
        if instr(v_errMes, 'Уже существует запись в DGTCODE_DBT с такими параметрами') = 1
        then
          RSB_GTLOG.DirectInsMsg(p_Prm.SeanceID, RSB_GTFN.RG_MARKETREPORT, RSB_GTLOG.ISSUE_WARNING, 'Ошибка при вставке документа-подтверждения "Отчет биржи"' || CHR(10) || v_ErrMes);
        else
          RSB_GTLOG.DirectInsMsg(p_Prm.SeanceID, RSB_GTFN.RG_MARKETREPORT, RSB_GTLOG.ISSUE_FAULT, 'Ошибка при вставке документа-подтверждения "Отчет биржи"' || CHR(10) || v_ErrMes);
        end if;
      END IF;
    END IF;
  END;
  
  --Создание отчета по операции расчетов
  FUNCTION WriteCalcReport(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_OutMarketReportID OUT NUMBER, p_ErrMsg OUT VARCHAR2,
                             p_ReportNumber IN VARCHAR2, p_ImpDate IN DATE, p_RgPartyObject IN VARCHAR2, p_Time IN DATE, p_CalcDate IN DATE)
    RETURN NUMBER
  IS
    v_stat NUMBER(5) := 0;
  BEGIN
    p_ErrMsg            := CHR(1);
    p_OutMarketReportID := 0;

    v_stat := RSI_GT.Init(RSI_GT.DIRECT_INSERT_YES, p_SeanceID, p_SourceCode, RSI_GT.GTCODE_RECEIVE, p_ErrMsg);

    IF v_stat = 0 THEN
      v_stat := RSI_GT.InitRec(RSB_GTFN.RG_DVCALCREP, 'Отчет по операции расчетов №' || p_ReportNumber, 1, p_ReportNumber, 0, p_ErrMsg);

      p_OutMarketReportID := RSI_GT.GetObjectID;
    END IF;

    IF v_stat = 0 THEN
      --добавляем параметры
      RSI_GT.SetParmByName('RGDVREP_DOCDATE', p_CalcDate);
      RSI_GT.SetParmByName('RGDVREP_DATE',   p_ImpDate);
      RSI_GT.SetParmByName('RGDVREP_TIME', p_Time);
      RSI_GT.SetParmByName('RGDVREP_DEPARTMENT', 1);
      RSI_GT.SetParmByName('RGDVREP_DOCPARTY', p_RgPartyObject);
      RSI_GT.SetParmByName('RGDVREP_DOCNUM', p_ReportNumber);
      RSI_GT.SetParmByName('RGDVREP_DOCKIND', 320); -- DOCKIND_DV_MARKETREPORT

      IF RSI_GT.GetLastError <> CHR(1) THEN
        v_stat := 1;
        p_ErrMsg := RSI_GT.GetLastError;
      END IF;
    END IF;

    IF v_stat = 0 THEN
      --сохранение объектов
      v_stat := RSI_GT.Save(p_ErrMsg);
    END IF;

    RETURN v_stat;
  END;
  
  --Подготовить отчет по операции расчетов
  PROCEDURE PrepareCalcReport(p_Prm IN OUT NOCOPY PRMREC_t, p_Time IN DATE, p_CalcDate IN DATE)
  IS
    v_stat           NUMBER(5);
    v_ErrMes         VARCHAR2(1000);
  BEGIN
  
    GetCalcReportID(p_Prm);
    IF(p_Prm.CalcReportID = 0) THEN
      v_stat := WriteCalcReport(p_Prm.SeanceID, p_Prm.Source_Code, p_Prm.CalcReportID, v_ErrMes, p_Prm.MarketReportNumber, p_Prm.Imp_Date, p_Prm.MMVB_Code, p_Time, p_CalcDate);

      IF v_ErrMes <> CHR(1) THEN
        RSB_GTLOG.DirectInsMsg(p_Prm.SeanceID, RSB_GTFN.RG_DVCALCREP, RSB_GTLOG.ISSUE_FAULT, 'Ошибка при вставке документа-подтверждения "Отчет биржи" по операции расчетов за дату '|| TO_CHAR(p_CalcDate, 'DD.MM.YYYY') || CHR(10) || v_ErrMes);
      END IF;
    END IF;
  END;
   
 -- ФУНКЦИИ ДЛЯ ОБРАБОТКИ ИНФОРМАЦИИ ПО СДЕЛКАМ
  
  --Очистка временной таблицы сделок F04 и O04
  FUNCTION DelFO04(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER
  IS
  BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE D_FO04_TMP';
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DVDEAL, RSB_GTLOG.ISSUE_FAULT,
                           'Внимание! Произошла непредвиденная ошибка в ф-и DelFO04(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DVDEAL) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;
  
  --Заполнение временной таблицы F04 на основании Payments
  FUNCTION FillFromF04(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2, p_Beg_date IN DATE, p_End_date IN DATE)
    RETURN NUMBER
  IS
  BEGIN 
    EXECUTE IMMEDIATE
      'INSERT INTO D_FO04_TMP(
        T_ID_FM_FO04, 
        T_ID_MB_REQUISITES, 
        T_ID_PROCESSING_LOG, 
        T_ID_DEAL, 
        T_ISIN, 
        T_PRICE, 
        T_VOL, 
        T_KOD, 
        T_BUYSELL, 
        T_DATE1, 
        T_TIME1, 
        T_DEALTYPE, 
        T_VAR_MARG, 
        T_NOBS, 
        T_FEE_NS, 
        T_PRICE_RUR, 
        T_DATE_CLR, 
        T_FEE_EX, 
        T_FEE_CC, 
        T_EXTSETTLECODE, 
        T_ID_MULT,                      
        T_ISIN2, 
        T_PRICE2, 
        T_DATE2, 
        T_PRICE_RUR2,
        T_COMM,
        T_ISHDAYTRADE,
        T_FILEDATE
     ) 
     (SELECT 
        NVL(F04.ID_FM_F04,0), 
        NVL(F04.ID_MB_REQUISITES,0), 
        NVL(F04.ID_PROCESSING_LOG,0), 
        NVL(F04.ID_DEAL,CHR(1)), 
        NVL(F04.ISIN,CHR(1)),  
        NVL(F04.PRICE,0),  
        NVL(F04.VOL,0),        
        NVL(F04.KOD_BUY,CHR(1)), 
        ''B'', 
        NVL(F04.DATE1,TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')), 
        NVL(F04.TIME,TO_DATE(''01.01.0001 00:00:00'', ''DD.MM.YYYY HH24:MI:SS'')), 
        NVL(F04.TYPE_BUY,0), 
        NVL(F04.VAR_MARG_B,0), 
        NVL(F04.NO_BUY,CHR(1)), 
        NVL(F04.FEE_NS_B,0), 
        NVL(F04.PRICE_RUR,0), 
        NVL(F04.DATE_CLR,TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')), 
        NVL(F04.FEE_EX_B,0),  
        NVL(F04.FEE_CC_B,0), 
        (SELECT NVL(SUBSTR(REQ.FILE_NAME,-8,4), CHR(1)) FROM '||p_Synonim||'.MB_REQUISITES REQ WHERE REQ.DOC_TYPE_ID = ''f04'' AND REQ.ID_MB_REQUISITES = F04.ID_MB_REQUISITES ),
        NVL(F04.ID_MULT,0), 
        CHR(1),  
        0,  
        TO_DATE(''01.01.0001'', ''DD.MM.YYYY''),  
        0,
        DECODE ( NVL(F04.COMM_SELL,CHR(1)), CHR(1), NVL(F04.COMM_BUY,CHR(1)), NVL(F04.COMM_SELL,CHR(1)) ),
        CASE WHEN EXISTS (SELECT 1 from RSHB_RSPAYM_MB.MB_REQUISITES MR WHERE MR.ID_MB_REQUISITES = F04.ID_MB_REQUISITES AND MR.FILE_NAME LIKE ''%trade%'') then chr(88) else chr(1) END,
        NVL((SELECT PrAc.TRADE_DATE from RSHB_RSPAYM_MB.PROCESSING_ACTUAL PrAc where PrAc.ID_MB_REQUISITES = F04.ID_MB_REQUISITES), TO_DATE(''01.01.0001'', ''DD.MM.YYYY''))        
      FROM '||p_Synonim||'.FM_F04 F04
        WHERE F04.ID_MB_REQUISITES IN (SELECT NVL(PA.ID_MB_REQUISITES, 0) FROM '||p_Synonim||'.PROCESSING_ACTUAL PA WHERE PA.FILE_TYPE = ''f04'' AND NVL(PA.TRADE_DATE,TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')) BETWEEN :Beg_date1 AND :End_date1 ) 
              AND NVL(F04.KOD_BUY,CHR(1)) <> CHR(1) 
              AND (NVL(F04.DATE_CLR,TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')) BETWEEN :Beg_date2 AND :End_date2 OR F04.DATE_CLR is NULL)    
              AND NVL(F04.TYPE_BUY,0) IN (0,1,2,3,4,11,14,15,24,25,26) AND NVL(F04.ZRID,0) = 0  
     UNION ALL SELECT           
        NVL(F04.ID_FM_F04,0),
        NVL(F04.ID_MB_REQUISITES,0), 
        NVL(F04.ID_PROCESSING_LOG,0), 
        NVL(F04.ID_DEAL,CHR(1)), 
        NVL(F04.ISIN,CHR(1)),  
        NVL(F04.PRICE,0),  
        NVL(F04.VOL,0),    
        NVL(F04.KOD_SELL,CHR(1)), 
        ''S'',      
        NVL(F04.DATE1,TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')), 
        NVL(F04.TIME,TO_DATE(''01.01.0001 00:00:00'', ''DD.MM.YYYY HH24:MI:SS'')),
        NVL(F04.TYPE_SELL,0), 
        NVL(F04.VAR_MARG_S,0), 
        NVL(F04.NO_SELL,CHR(1)), 
        NVL(F04.FEE_NS_S,0), 
        NVL(F04.PRICE_RUR,0), 
        NVL(F04.DATE_CLR,TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')), 
        NVL(F04.FEE_EX_S,0),  
        NVL(F04.FEE_CC_S,0),  
        (SELECT NVL(SUBSTR(REQ.FILE_NAME,-8,4), CHR(1)) FROM '||p_Synonim||'.MB_REQUISITES REQ WHERE REQ.DOC_TYPE_ID = ''f04'' AND REQ.ID_MB_REQUISITES = F04.ID_MB_REQUISITES ),
        NVL(F04.ID_MULT,0), 
        CHR(1),  
        0,  
        TO_DATE(''01.01.0001'', ''DD.MM.YYYY''),  
        0,
        DECODE ( NVL(F04.COMM_SELL,CHR(1)), CHR(1), NVL(F04.COMM_BUY,CHR(1)), NVL(F04.COMM_SELL,CHR(1)) ),
        CASE WHEN EXISTS (SELECT 1 from RSHB_RSPAYM_MB.MB_REQUISITES MR WHERE MR.ID_MB_REQUISITES = F04.ID_MB_REQUISITES AND MR.FILE_NAME LIKE ''%trade%'') then chr(88) else chr(1) END,
        NVL((SELECT PrAc.TRADE_DATE from RSHB_RSPAYM_MB.PROCESSING_ACTUAL PrAc where PrAc.ID_MB_REQUISITES = F04.ID_MB_REQUISITES), TO_DATE(''01.01.0001'', ''DD.MM.YYYY''))              
      FROM '||p_Synonim||'.FM_F04 F04
        WHERE F04.ID_MB_REQUISITES IN (SELECT NVL(PA.ID_MB_REQUISITES, 0) FROM '||p_Synonim||'.PROCESSING_ACTUAL PA WHERE PA.FILE_TYPE = ''f04'' AND NVL(PA.TRADE_DATE,TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')) BETWEEN :Beg_date3 AND :End_date3 ) 
              AND NVL(F04.KOD_SELL,CHR(1)) <> CHR(1)
              AND (NVL(F04.DATE_CLR,TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')) BETWEEN :Beg_date4 AND :End_date4 OR F04.DATE_CLR is NULL)   
              AND NVL(F04.TYPE_SELL,0) IN (0,1,2,3,4,11,14,15,24,25,26) AND NVL(F04.ZRID,0) = 0  
              ) '
    USING p_Beg_date, p_End_date, p_Beg_date, p_End_date, p_Beg_date, p_End_date, p_Beg_date, p_End_date;
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DVDEAL, RSB_GTLOG.ISSUE_FAULT,
                           'Внимание! Произошла непредвиденная ошибка в ф-и FillFromF04(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DVDEAL) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END; 
  
  --Заполнение временной таблицы сделок O04 на основании Payments
  FUNCTION FillFromO04(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2, p_Beg_date IN DATE, p_End_date IN DATE)
    RETURN NUMBER
  IS 
  BEGIN 
    EXECUTE IMMEDIATE
      'INSERT INTO D_FO04_TMP(
        T_ID_FM_FO04, 
        T_ID_MB_REQUISITES, 
        T_ID_PROCESSING_LOG, 
        T_ID_DEAL, 
        T_ISIN, 
        T_PRICE, 
        T_VOL, 
        T_KOD, 
        T_BUYSELL, 
        T_DATE1, 
        T_TIME1, 
        T_DEALTYPE, 
        T_VAR_MARG, 
        T_NOBS, 
        T_FEE_NS, 
        T_PRICE_RUR, 
        T_DATE_CLR, 
        T_FEE_EX, 
        T_FEE_CC, 
        T_PREM,
        T_EXTSETTLECODE,  
        T_ISIN2, 
        T_PRICE2, 
        T_DATE2, 
        T_PRICE_RUR2,
        T_COMM,
        T_ISHDAYTRADE,
        T_FILEDATE
     ) 
     (SELECT 
        NVL(o04.ID_FM_O04,0), 
        NVL(o04.ID_MB_REQUISITES,0), 
        NVL(o04.ID_PROCESSING_LOG,0), 
        NVL(o04.ID_DEAL,CHR(1)), 
        NVL(o04.ISIN,CHR(1)),  
        NVL(o04.PRICE,0),  
        NVL(o04.VOL,0),        
        NVL(o04.KOD_BUY,CHR(1)), 
        ''B'', 
        NVL(o04.DATE1,TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')), 
        NVL(o04.TIME,TO_DATE(''01.01.0001 00:00:00'', ''DD.MM.YYYY HH24:MI:SS'')), 
        NVL(o04.TYPE_BUY,0), 
        NVL(o04.VAR_MARG_B,0), 
        NVL(o04.NO_BUY,CHR(1)), 
        NVL(o04.FEE_NS_B,0), 
        NVL(o04.PRICE_RUR,0), 
        NVL(o04.DATE_CLR,TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')), 
        NVL(o04.FEE_EX_B,0),  
        NVL(o04.FEE_CC_B,0),
        NVL(o04.PREM_BUY,0),
        (SELECT NVL(SUBSTR(REQ.FILE_NAME,-8,4), CHR(1)) FROM '||p_Synonim||'.MB_REQUISITES REQ WHERE REQ.DOC_TYPE_ID = ''o04'' AND REQ.ID_MB_REQUISITES = o04.ID_MB_REQUISITES ),
        CHR(1),  
        0,  
        TO_DATE(''01.01.0001'', ''DD.MM.YYYY''),  
        0,
        DECODE ( NVL(o04.COMM_SELL,CHR(1)), CHR(1), NVL(o04.COMM_BUY,CHR(1)), NVL(o04.COMM_SELL,CHR(1)) ),
        CASE WHEN EXISTS (SELECT 1 from RSHB_RSPAYM_MB.MB_REQUISITES MR WHERE MR.ID_MB_REQUISITES = o04.ID_MB_REQUISITES AND MR.FILE_NAME LIKE ''%trade%'') then chr(88) else chr(1) END,
        NVL((SELECT PrAc.TRADE_DATE from RSHB_RSPAYM_MB.PROCESSING_ACTUAL PrAc where PrAc.ID_MB_REQUISITES = F04.ID_MB_REQUISITES), TO_DATE(''01.01.0001'', ''DD.MM.YYYY''))  
      FROM '||p_Synonim||'.FM_O04 o04
        WHERE o04.ID_MB_REQUISITES IN (SELECT NVL(PA.ID_MB_REQUISITES, 0) FROM '||p_Synonim||'.PROCESSING_ACTUAL PA WHERE PA.FILE_TYPE = ''o04'' AND NVL(PA.TRADE_DATE,TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')) BETWEEN :Beg_date1 AND :End_date1 ) 
              AND NVL(o04.KOD_BUY,CHR(1)) <> CHR(1) 
              AND (NVL(o04.DATE_CLR,TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')) BETWEEN :Beg_date2 AND :End_date2 OR o04.DATE_CLR is NULL)  
              AND NVL(o04.TYPE_BUY,0) IN (0,1,2,3,4,11,14,15) AND NVL(o04.ZRID,0) = 0  
     UNION ALL SELECT           
        NVL(o04.ID_FM_O04,0),
        NVL(o04.ID_MB_REQUISITES,0), 
        NVL(o04.ID_PROCESSING_LOG,0), 
        NVL(o04.ID_DEAL,CHR(1)), 
        NVL(o04.ISIN,CHR(1)),  
        NVL(o04.PRICE,0),  
        NVL(o04.VOL,0),    
        NVL(o04.KOD_SELL,CHR(1)), 
        ''S'',      
        NVL(o04.DATE1,TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')), 
        NVL(o04.TIME,TO_DATE(''01.01.0001 00:00:00'', ''DD.MM.YYYY HH24:MI:SS'')),
        NVL(o04.TYPE_SELL,0), 
        NVL(o04.VAR_MARG_S,0), 
        NVL(o04.NO_SELL,CHR(1)), 
        NVL(o04.FEE_NS_S,0), 
        NVL(o04.PRICE_RUR,0), 
        NVL(o04.DATE_CLR,TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')), 
        NVL(o04.FEE_EX_S,0),  
        NVL(o04.FEE_CC_S,0),
        NVL(o04.PREM_SELL,0),
        (SELECT NVL(SUBSTR(REQ.FILE_NAME,-8,4), CHR(1)) FROM '||p_Synonim||'.MB_REQUISITES REQ WHERE REQ.DOC_TYPE_ID = ''o04'' AND REQ.ID_MB_REQUISITES = o04.ID_MB_REQUISITES),
        CHR(1),  
        0,  
        TO_DATE(''01.01.0001'', ''DD.MM.YYYY''),  
        0,
        DECODE ( NVL(o04.COMM_SELL,CHR(1)), CHR(1), NVL(o04.COMM_BUY,CHR(1)), NVL(o04.COMM_SELL,CHR(1)) ),
        CASE WHEN EXISTS (SELECT 1 from RSHB_RSPAYM_MB.MB_REQUISITES MR WHERE MR.ID_MB_REQUISITES = o04.ID_MB_REQUISITES AND MR.FILE_NAME LIKE ''%trade%'') then chr(88) else chr(1) END,
        NVL((SELECT PrAc.TRADE_DATE from RSHB_RSPAYM_MB.PROCESSING_ACTUAL PrAc where PrAc.ID_MB_REQUISITES = F04.ID_MB_REQUISITES), TO_DATE(''01.01.0001'', ''DD.MM.YYYY''))                   
      FROM '||p_Synonim||'.FM_O04 o04
        WHERE o04.ID_MB_REQUISITES IN (SELECT NVL(PA.ID_MB_REQUISITES, 0) FROM '||p_Synonim||'.PROCESSING_ACTUAL PA WHERE PA.FILE_TYPE = ''o04'' AND NVL(PA.TRADE_DATE,TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')) BETWEEN :Beg_date3 AND :End_date3 ) 
              AND NVL(o04.KOD_SELL,CHR(1)) <> CHR(1)
              AND (NVL(o04.DATE_CLR,TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')) BETWEEN :Beg_date4 AND :End_date4 OR o04.DATE_CLR is NULL)   
              AND NVL(o04.TYPE_SELL,0) IN (0,1,2,3,4,11,14,15) AND NVL(o04.ZRID,0) = 0  
              ) '
    USING p_Beg_date, p_End_date, p_Beg_date, p_End_date, p_Beg_date, p_End_date, p_Beg_date, p_End_date;
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DVDEAL, RSB_GTLOG.ISSUE_FAULT,
                           'Внимание! Произошла непредвиденная ошибка в ф-и FillFromO04(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DVDEAL) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END; 
  
  --Обновление ZRID загруженных сделок из F04 в таблице Payments
  FUNCTION UpdateF04_PM(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2)
    RETURN NUMBER
  IS
    TYPE ObjID_t IS TABLE OF DGTSNCREC_DBT.T_RECORDID%TYPE; 
    TYPE PmID_t IS TABLE OF D_FO04_TMP.T_ID_FM_FO04%TYPE; 
    v_ObjID ObjID_t; 
    v_PmID   PmID_t; 
  BEGIN
    SELECT F04.T_ID_FM_FO04, GTSNCREC.T_RECORDID 
      BULK COLLECT INTO v_PmID, v_ObjID 
      FROM DGTSNCREC_DBT GTSNCREC, 
           DGTCODE_DBT GTCODE, 
           DGTOBJECT_DBT GTOBJECT, 
           D_FO04_TMP F04 
     WHERE GTSNCREC.T_SEANCEID = p_SeanceID
       AND GTCODE.T_OBJECTKIND = RSB_GTFN.RG_DVDEAL
       AND GTCODE.T_OBJECTID   = GTSNCREC.T_OBJECTID 
       AND GTOBJECT.T_OBJECTID = GTSNCREC.T_OBJECTID 
       AND GTCODE.T_OBJECTCODE = F04.T_UNIQCODE;

    IF v_PmID.COUNT <> 0 THEN
      FORALL i IN v_PmID.FIRST .. v_PmID.LAST 
        EXECUTE IMMEDIATE
          'UPDATE '||p_Synonim||'.FM_F04  
              SET ZRID = :v_ObjIDi 
            WHERE ID_FM_F04 = :v_PmIDi'
        USING IN v_ObjID(i), IN v_PmID(i);
        v_ObjID.Delete;
        v_PmID.Delete;
    END IF; 
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DVDEAL, RSB_GTLOG.ISSUE_FAULT,
                           'Внимание! Произошла непредвиденная ошибка в ф-и UpdateF04_PM(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DVDEAL) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;
  
  --Обновление ZRID загруженных сделок из O04 в таблице Payments
  FUNCTION UpdateO04_PM(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2)
    RETURN NUMBER
  IS
    TYPE ObjID_t IS TABLE OF DGTSNCREC_DBT.T_RECORDID%TYPE; 
    TYPE PmID_t IS TABLE OF D_FO04_TMP.T_ID_FM_FO04%TYPE; 
    v_ObjID ObjID_t; 
    v_PmID   PmID_t; 
  BEGIN
    SELECT F04.T_ID_FM_FO04, GTSNCREC.T_RECORDID 
      BULK COLLECT INTO v_PmID, v_ObjID 
      FROM DGTSNCREC_DBT GTSNCREC, 
           DGTCODE_DBT GTCODE, 
           DGTOBJECT_DBT GTOBJECT, 
           D_FO04_TMP F04 
     WHERE GTSNCREC.T_SEANCEID = p_SeanceID
       AND GTCODE.T_OBJECTKIND = RSB_GTFN.RG_DVDEAL
       AND GTCODE.T_OBJECTID   = GTSNCREC.T_OBJECTID 
       AND GTOBJECT.T_OBJECTID = GTSNCREC.T_OBJECTID 
       AND GTCODE.T_OBJECTCODE = F04.T_UNIQCODE;

    IF v_PmID.COUNT <> 0 THEN
      FORALL i IN v_PmID.FIRST .. v_PmID.LAST 
        EXECUTE IMMEDIATE
          'UPDATE '||p_Synonim||'.FM_O04 
              SET ZRID = :v_ObjIDi 
            WHERE ID_FM_O04 = :v_PmIDi'
        USING IN v_ObjID(i), IN v_PmID(i);
        v_ObjID.Delete;
        v_PmID.Delete;
    END IF; 
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DVDEAL, RSB_GTLOG.ISSUE_FAULT,
                           'Внимание! Произошла непредвиденная ошибка в ф-и UpdateO04_PM(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DVDEAL) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;
  
  --Генерация кодов, связанных со сделками ПИ
  PROCEDURE SetDealCodes(p_Prm IN OUT NOCOPY PRMREC_t, p_Deal IN OUT NOCOPY DATADEALS_t, p_OurBankCode IN VARCHAR2 )
  IS
    v_StringDate      VARCHAR(11);
    v_StringFileDate VARCHAR(11);
  BEGIN  
    v_StringDate := TO_CHAR(p_Deal.Date1, 'DD.MM.YYYY');
    v_StringFileDate := CASE WHEN SUBSTR(to_char(p_Deal.FileDate,'dd'), 1, 1) = '0' THEN ' ' ELSE '' END || TRIM(LEADING '0' FROM to_char(p_Deal.FileDate, 'dd.mm.yyyy'));
    IF( (p_Prm.isFutures AND p_Deal.DealType = 11) OR (not p_Prm.isFutures AND p_Deal.DealType IN (1,4)) ) THEN
      p_Deal.DealCode := SUBSTR(v_StringDate, 1, 2)||SUBSTR(v_StringDate, 4, 2)||SUBSTR(v_StringDate, 7, 4);  
      IF(p_Prm.isFutures) THEN
        p_Deal.DealCode := p_Deal.DealCode||'3'||p_Deal.ID_Deal; 
      ELSE
        p_Deal.DealCode := p_Deal.DealCode||'4'||p_Deal.ID_Deal; 
      END IF;
      IF( (p_Deal.ID_Deal = '0') OR (p_Deal.ID_Deal = CHR(1))) THEN
        p_Deal.UniqCode :=  p_Deal.DealCode;
        p_Deal.ExtDealCode :=  CHR(1);
      ELSE
        p_Deal.UniqCode := p_Deal.ID_Deal;
        p_Deal.ExtDealCode := p_Deal.ID_Deal;
      END IF;
    ELSE
      p_Deal.DealCode := p_Deal.BuySell||'/'||SUBSTR(v_StringDate, 1, 2)||SUBSTR(v_StringDate, 4, 2)||SUBSTR(v_StringDate, 7, 4)||'0'||p_Deal.ID_Deal; 
      IF( (p_Deal.ID_Deal = '0') OR (p_Deal.ID_Deal = CHR(1))) THEN
        p_Deal.UniqCode :=  p_Deal.DealCode;
        p_Deal.ExtDealCode :=  CHR(1);
      ELSE
        p_Deal.UniqCode := p_Deal.BuySell||'/'||p_Deal.ID_Deal;
        p_Deal.ExtDealCode := p_Deal.ID_Deal;
      END IF;
    END IF;

    v_StringDate := TO_CHAR(p_Deal.Date_CLR, 'DD.MM.YYYY');
    p_Deal.CalcCode  :=  p_Prm.MMVB_Code||SUBSTR(v_StringDate, 1, 2)||SUBSTR(v_StringDate, 4, 2)||SUBSTR(v_StringDate, 7, 4);
    IF ((p_Deal.Kod = CHR(1)) OR (UPPER(p_Deal.Kod) = p_OurBankCode) OR RSB_GTFN.CheckBankCode(UPPER(p_Deal.Kod)) <> 0) THEN
      p_Deal.CalcCode  := p_Deal.CalcCode||'/С'||'/'||RsbSessionData.OperDprt();
    ELSE
      p_Deal.CalcCode  := p_Deal.CalcCode||'/К'||'/'||RsbSessionData.OperDprt();
    END IF;
    p_Deal.UniqCode := p_Deal.UniqCode||'_'||v_StringFileDate;
  END;
  
 --Обновление информации по объектам шлюза и кодам сделок F04 и O04
  FUNCTION UpdateObjCodeFO04(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_DVKind IN NUMBER, p_PrmStr IN VARCHAR2)
    RETURN NUMBER
  IS
    v_Prm PRMREC_t;
    v_OurBankCode VARCHAR(35);
    v_Deal    DATADEALS_t;
    v_stat    NUMBER(5) := 0;
    TYPE DEALSARR_t IS TABLE OF DATADEALS_t;
    v_DealsArr DEALSARR_t := DEALSARR_t();
    TYPE ObjID_t IS TABLE OF DGTCODE_DBT.T_OBJECTID%TYPE; 
    TYPE Fo04ID_t IS TABLE OF D_FO04_TMP.t_ID_FM_FO04%TYPE; 
    TYPE Fo04BS_t IS TABLE OF D_FO04_TMP.T_BUYSELL%TYPE; 
    v_ObjIDs  ObjID_t; 
    v_Fo04IDs   Fo04ID_t;
    v_Fo04BSs Fo04BS_t;
  BEGIN 
    --инициализация используемых параметров
    SetPrmRecByStr(v_Prm, p_PrmStr);
    v_Prm.SeanceID    := p_SeanceID;
    v_Prm.Source_Code := p_SourceCode;
    IF(p_DVKind = RSB_GTFN.DERIVATIVE_FUTURES) THEN
      v_Prm.isFutures := true;
    ELSE
      v_Prm.isFutures := false;
    END IF;
    
    BEGIN
       SELECT NVL(t_Code, CHR(1)) INTO v_OurBankCode
         FROM dobjcode_dbt
        WHERE t_ObjectType = 3 -- субъект
          AND t_CodeKind   = 8 -- код на ММВБ
          AND t_State      = 0 -- действующий код
          AND t_ObjectID   = RsbSessionData.OurBank;
       EXCEPTION
        WHEN NO_DATA_FOUND THEN v_OurBankCode := CHR(1);
    END;

    UPDATE D_FO04_TMP
       SET T_DATE_CLR = RSI_RSBCALENDAR.GetDateAfterWorkDay(T_FILEDATE, 0, 401) --Следующий рабочий день по календарю СР
     WHERE T_DATE_CLR = RSI_GT.ZeroDate;

    
    FOR cData IN (SELECT NVL(fo04.t_ID_FM_FO04, 0) ID_FM_FO04,
                         NVL(fo04.T_ID_DEAL, CHR(1)) ID_Deal,
                         NVL(fo04.T_KOD, CHR(1)) Kod,
                         NVL(fo04.T_BUYSELL, CHR(1)) BuySell,
                         NVL(fo04.T_DEALTYPE, 0) DealType, 
                         NVL(fo04.T_DATE1, RSI_GT.ZeroDate) Date1,
                         NVL(fo04.T_DATE_CLR, RSI_GT.ZeroDate) Date_CLR,
                         NVL(fo04.T_FILEDATE, RSI_GT.ZeroDate) FileDate
                    FROM D_FO04_TMP fo04
                 )
    LOOP   
        v_Deal.ID_FM_FO04    := cData.ID_FM_FO04;
        v_Deal.ID_Deal       := cData.ID_Deal;
        v_Deal.Kod           := cData.Kod;
        v_Deal.DealType      := cData.DealType;
        v_Deal.BuySell       := cData.BuySell;
        v_Deal.Date1         := cData.Date1;
        v_Deal.Date_CLR      := cData.Date_CLR;
        v_Deal.FileDate      := cData.FileDate;
        SetDealCodes(v_Prm, v_Deal, v_OurBankCode);
        v_DealsArr.Extend();
        v_DealsArr(v_DealsArr.last) := v_Deal;
    END LOOP;

    IF v_DealsArr.COUNT > 0 THEN
     FORALL indx IN v_DealsArr.FIRST .. v_DealsArr.LAST
      UPDATE D_FO04_TMP FO04
       SET FO04.T_UNIQCODE     = v_DealsArr(indx).UniqCode,
           FO04.T_DEALCODE     = v_DealsArr(indx).DealCode,
           FO04.T_EXTDEALCODE  = v_DealsArr(indx).ExtDealCode,
           FO04.T_CALCCODE     = v_DealsArr(indx).CalcCode           
        WHERE FO04.T_BUYSELL   = v_DealsArr(indx).BuySell 
         AND  FO04.T_ID_FM_FO04 = v_DealsArr(indx).ID_FM_FO04;
      v_DealsArr.Delete;     
    END IF; 

    SELECT GTCODE.T_OBJECTID, FO04.T_ID_FM_FO04, FO04.T_BUYSELL 
      BULK COLLECT INTO v_ObjIDs, v_Fo04IDs, v_Fo04BSs 
      FROM DGTCODE_DBT GTCODE, D_FO04_TMP FO04 
     WHERE GTCODE.T_APPLICATIONID IN (RSB_GTFN.GT_PAYMENTS_CSV, RSB_GTFN.GT_CSV)
       AND GTCODE.T_OBJECTKIND = RSB_GTFN.RG_DVDEAL
       AND GTCODE.T_OBJECTCODE = FO04.T_UNIQCODE;
       
    IF v_ObjIDs.COUNT <> 0 THEN 
      FORALL i IN v_ObjIDs.FIRST .. v_ObjIDs.LAST 
        UPDATE D_FO04_TMP FO04 
           SET FO04.t_ObjectID   = v_ObjIDs(i) 
         WHERE FO04.T_ID_FM_FO04 = v_Fo04IDs(i)
           AND FO04.T_BUYSELL    = v_Fo04BSs(i); 
     END IF;
     
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(v_Prm.SeanceID, RSB_GTFN.RG_DVDEAL, RSB_GTLOG.ISSUE_FAULT,
                           'Внимание! Произошла непредвиденная ошибка при обработке данных в ф-и UpdateObjCodeFO04(), ' ||
                           'SeanceID=' || TO_CHAR(v_Prm.SeanceID) || ', Source_Code=' || v_Prm.Source_Code || 
                           ', SessionID=' || TO_CHAR(USERENV('sessionid')) || ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;
  
  --Установка параметров, которых нет в таблице Payments
  PROCEDURE SetDealInfo(p_Deal IN OUT NOCOPY DATADEALS_t, isFutures IN BOOLEAN)
  IS
  BEGIN
  
    IF( isFutures AND p_Deal.DealType IN (11,24,25)) THEN
      p_Deal.DealKind := RSB_GTFN.OPERKIND_FUTURESEXEC;
      IF(p_Deal.BuySell = 'B') THEN
         p_Deal.PositionType := 1; -- короткая позиция
      ELSE
        p_Deal.PositionType := 2; -- длинная позиция
      END IF;
    ELSIF (not isFutures AND p_Deal.DealType IN (1,4)) THEN
      IF(p_Deal.DealType = 1) THEN
        p_Deal.DealKind :=  RSB_GTFN.OPERKIND_OPTIONEXEC;
      ELSE
        p_Deal.DealKind :=  RSB_GTFN.OPERKIND_OPTIONEXP;
      END IF;
      IF(p_Deal.BuySell = 'B') THEN
         p_Deal.PositionType := 1; -- короткая позиция
      ELSE
        p_Deal.PositionType := 2; -- длинная позиция
      END IF;
    ELSE
      IF(isFutures) THEN
        IF(p_Deal.BuySell = 'B') THEN
          p_Deal.DealKind := RSB_GTFN.OPERKIND_FUTURESBUY;
        ELSE
          p_Deal.DealKind := RSB_GTFN.OPERKIND_FUTURESSELL;
        END IF;
      ELSE
        IF(p_Deal.BuySell = 'B') THEN
          p_Deal.DealKind := RSB_GTFN.OPERKIND_OPTIONBUY;
        ELSE
          p_Deal.DealKind := RSB_GTFN.OPERKIND_OPTIONSELL;
        END IF;      
      END IF;
      p_Deal.PositionType := 0;
    END IF;
  END;
  
  --Установка информации по спредам
  PROCEDURE UpdateDealLegs
  IS
    TYPE DATALEG_TABLE_t IS TABLE OF DATALEG_t;
    v_DATALEG_TABLE DATALEG_TABLE_t;
  BEGIN
    
    SELECT F04.T_ID_FM_FO04,
           F04.T_BuySell,
           LEG.T_ISIN,
           LEG.T_Price,
           LEG.T_Date1,
           LEG.T_Price_RUR
    BULK COLLECT INTO v_DATALEG_TABLE
      FROM D_FO04_TMP F04, D_FO04_TMP LEG
      WHERE F04.T_DEALTYPE IN (14, 15) --ноги спреда
        AND LEG.T_DEALTYPE IN (14, 15) --ноги спреда
        AND F04.T_DEALTYPE != LEG.T_DEALTYPE
        AND F04.T_BUYSELL  != LEG.T_BUYSELL
        AND F04.T_ID_MULT  =  LEG.T_ID_MULT 
        AND F04.T_KOD      =  LEG.T_KOD;

    IF v_DATALEG_TABLE.COUNT > 0 THEN
     FORALL indx IN v_DATALEG_TABLE.FIRST .. v_DATALEG_TABLE.LAST
      UPDATE D_FO04_TMP F04
       SET F04.T_ISIN2      = v_DATALEG_TABLE(indx).ISIN,
           F04.T_Price2     = v_DATALEG_TABLE(indx).Price,
           F04.T_Date2      = v_DATALEG_TABLE(indx).Date1,
           F04.T_Price_RUR2 = v_DATALEG_TABLE(indx).Price_RUR
        WHERE F04.T_BUYSELL = v_DATALEG_TABLE(indx).BuySell 
         AND  F04.T_ID_FM_FO04 = v_DATALEG_TABLE(indx).ID_FM_FO04;
      v_DATALEG_TABLE.Delete;   
    END IF;
  END;

  --Заполнение и сохранение параметров сделки
  FUNCTION PutDealInfoPM(p_Prm IN OUT NOCOPY PRMREC_t, p_Deal IN OUT NOCOPY DATADEALS_t, p_ErrMes IN OUT NOCOPY VARCHAR2, p_WarnMes IN OUT NOCOPY VARCHAR2) RETURN NUMBER
  IS
    v_stat         NUMBER(5) := 0;
  BEGIN

    v_stat := RSI_GT.InitRec(RSB_GTFN.RG_DVDEAL,
                            'Биржевая операция с ПИ № '||SUBSTR(p_Deal.UniqCode, 1, LENGTH(p_Deal.UniqCode) - 11)||' за '||CASE WHEN SUBSTR(to_char(p_Deal.FileDate,'dd'), 1, 1) = '0' THEN ' ' ELSE '' END || TRIM(LEADING '0' FROM to_char(p_Deal.FileDate, 'dd.mm.yyyy')), --Наименование объекта
                             0, --проверять существование объекта в системе по коду
                             p_Deal.UniqCode, --код объекта
                             0, --ID клиента
                             p_ErrMes);

    IF v_stat = 0 THEN 
      RSI_GT.SetParmByName('RGDVDL_MARKETREPORT_ID', p_Prm.MarketReportID);
      RSI_GT.SetParmByName('RGDVDL_KIND', p_Deal.DealKind);
      RSI_GT.SetParmByName('RGDVDL_CODE', p_Deal.DealCode);
      RSI_GT.SetParmByName('RGDVDL_EXTCODE', p_Deal.ExtDealCode);
      RSI_GT.SetParmByName('RGDVDL_DATE', p_Deal.Date1);
      RSI_GT.SetParmByName('RGDVDL_TIME', p_Deal.Time1);
      RSI_GT.SetParmByName('RGDVDL_HEDGE', CHR(1));
      RSI_GT.SetParmByName('RGDVDL_FI_CODE', p_Deal.ISIN);
      RSI_GT.SetParmByName('RGDVDL_AMOUNT', p_Deal.Vol);
      RSI_GT.SetParmByName('RGDVDL_POSITION', p_Deal.PositionType);
      RSI_GT.SetParmByName('RGDVDL_PRICE', p_Deal.Price);
      IF NOT p_Prm.isFutures THEN
        RSI_GT.SetParmByName('RGDVDL_BONUS', p_Deal.Prem);
      END IF;
      RSI_GT.SetParmByName('RGDVDL_CLIENT', p_Deal.Kod);
      RSI_GT.SetParmByName('RGDVDL_DEPARTMENT', RsbSessionData.OperDprt());
      RSI_GT.SetParmByName('RGDVDL_OPER', RsbSessionData.Oper());
      RSI_GT.SetParmByName('RGDVDL_CALCKIND', RSB_GTFN.OPERKIND_DVOPER);
      RSI_GT.SetParmByName('RGDVDL_CALCCODE', p_Deal.CalcCode);
      RSI_GT.SetParmByName('RGDVDL_MARGIN', p_Deal.Var_Marg);
      RSI_GT.SetParmByName('RGDVDL_AMOUNT_COMM', p_Deal.Fee_Ex);
      RSI_GT.SetParmByName('RGDVDL_CLRCOMM', p_Deal.Fee_CC);
      RSI_GT.SetParmByName('RGDVDL_OUTCOMM', p_Deal.Fee_NS);
      RSI_GT.SetParmByName('RGDVDL_PRICE_RUR', p_Deal.Price_RUR);
      RSI_GT.SetParmByName('RGDVDL_TYPE', p_Deal.DealType);
      RSI_GT.SetParmByName('RGDVDL_IDDEAL', p_Deal.ID_Deal);
      RSI_GT.SetParmByName('RGDVDL_NO', p_Deal.NoBS);
      IF (p_Deal.DealType IN (14,15)) THEN -- ноги спреда
        RSI_GT.SetParmByName('RGDVDL_FI_CODE_2', p_Deal.ISIN2);
        RSI_GT.SetParmByName('RGDVDL_IDMULT', p_Deal.ID_Mult);
        RSI_GT.SetParmByName('RGDVDL_PRICE_2', p_Deal.Price2);
        RSI_GT.SetParmByName('RGDVDL_PRICE_RUR_2', p_Deal.Price_RUR2);
        RSI_GT.SetParmByName('RGDVDL_DATE_2', p_Deal.Date2);
      END IF;
      RSI_GT.SetParmByName('RGDVDL_DATE_CLR', p_Deal.Date_CLR);
      RSI_GT.SetParmByName('RGDVDL_EXTSETTLECODE', p_Deal.ExtSettleCode);
      RSI_GT.SetParmByName('RGDVDL_TRADER_INFO', p_Deal.Comm);
      RSI_GT.SetParmByName('RGDVDL_IS_OPTIONEXP', p_Deal.isExpir);
      RSI_GT.SetParmByName('RGDVDL_ISHDAYTRADE', p_Deal.isHDayTrade);
      
      --проверка ошибок после добавления параметров
      IF RSI_GT.GetLastError <> CHR(1) THEN
        v_stat := 1;
        p_ErrMes := RSI_GT.GetLastError;
      END IF;
    END IF;

    RETURN v_stat;
  END;

  --Обработка записей по данным буферной таблицы FO04
  PROCEDURE ProcessReplRecByTmp_FO04(p_Prm IN OUT NOCOPY PRMREC_t)
  IS
    v_Deal    DATADEALS_t;
    v_stat    NUMBER(5) := 0;
    v_ErrMes  VARCHAR2(1000) := CHR(1);
    v_WarnMes VARCHAR2(1000) := CHR(1);
    TYPE DEALSARR_t IS TABLE OF DATADEALS_t;
    v_DealsArr DEALSARR_t := DEALSARR_t();
  BEGIN
    --инициализация глобального лога для накопления записей для вставки
    RSB_GTLOG.Init(BATCH_SIZE, p_Prm.SeanceID, RSB_GTFN.RG_DVDEAL);

    --инициализация глобального накопителя объектов ЗР для вставки
    v_stat := RSI_GT.Init(RSI_GT.DIRECT_INSERT_YES, p_Prm.SeanceID, p_Prm.Source_Code, RSI_GT.GTCODE_RECEIVE, v_ErrMes);
    IF v_stat <> 0 THEN
      RSB_GTLOG.Error('Внимание! '||v_ErrMes);
    END IF;
    UpdateDealLegs();
    IF v_stat = 0 THEN
      FOR cData IN (SELECT NVL(fo04.t_ID_FM_FO04, 0) ID_FM_FO04,
                           NVL(fo04.T_ID_DEAL, CHR(1)) ID_Deal,
                           NVL(fo04.T_ISIN, CHR(1)) ISIN,
                           NVL(fo04.T_PRICE, 0) Price,
                           NVL(fo04.T_VOL, 0) Vol,
                           NVL(fo04.T_KOD, CHR(1)) Kod,
                           NVL(fo04.T_BUYSELL, CHR(1)) BuySell,
                           NVL(fo04.T_DATE1, RSI_GT.ZeroDate) Date1,
                           NVL(fo04.T_DEALTYPE, 0) DealType, 
                           NVL(fo04.T_TIME1, RSI_GT.ZeroTime) Time1,
                           NVL(fo04.T_VAR_MARG, 0) Var_Marg,
                           NVL(fo04.T_NOBS, CHR(1)) NoBS,
                           NVL(fo04.T_FEE_NS, 0) Fee_NS,
                           NVL(fo04.T_PRICE_RUR, 0) Price_RUR,
                           NVL(fo04.T_DATE_CLR, RSI_GT.ZeroDate) Date_CLR,
                           NVL(fo04.T_FEE_EX, 0) Fee_Ex,
                           NVL(fo04.T_FEE_CC, 0) Fee_CC,
                           NVL(fo04.T_PREM, 0) Prem,
                           NVL(fo04.T_EXTSETTLECODE, 0) ExtSettleCode,
                           NVL(fo04.T_ID_MULT, 0) ID_Mult,
                           NVL(fo04.T_ISIN, CHR(1)) ISIN2,
                           NVL(fo04.T_PRICE, 0) Price2,
                           NVL(fo04.T_DATE1, RSI_GT.ZeroDate) Date2,
                           NVL(fo04.T_PRICE_RUR, 0) Price_RUR2,
                           NVL(fo04.T_COMM, CHR(1)) Comm,
                           NVL(fo04.T_OBJECTID, 0) ObjectID,
                           NVL(fo04.T_CALCCODE, CHR(1)) CalcCode,
                           NVL(fo04.T_UNIQCODE, CHR(1)) UniqCode,
                           NVL(fo04.T_DEALCODE, CHR(1)) DealCode,
                           NVL(fo04.T_EXTDEALCODE, CHR(1)) ExtDealCode,
                           NVL(fo04.T_ISHDAYTRADE, CHR(1)) isHDayTrade,
                           NVL(fo04.T_FILEDATE, RSI_GT.ZeroDate) FileDate
                      FROM D_FO04_TMP fo04
                   )
      LOOP
        v_ErrMes  := CHR(1);
        v_WarnMes := CHR(1);
        IF cData.ObjectID > 0 THEN
           v_ErrMes := 'Ошибка при загрузке биржевой операции с ПИ с кодом "' || cData.DealCode || '"' || CHR(10) || CHR(10) || 'Запись репликации с действием создать для объекта ' || TO_CHAR(cData.ObjectID) || ' уже передавалась в RS-Bank.' || CHR(10) || 'Повторная репликация невозможна.';  
        ELSE
           v_Deal.ID_FM_FO04    := cData.ID_FM_FO04;
           v_Deal.ID_Deal       := cData.ID_Deal;
           v_Deal.ISIN          := cData.ISIN;
           v_Deal.Price         := cData.Price;
           v_Deal.Vol           := cData.Vol; 
           v_Deal.Kod           := cData.Kod;
           v_Deal.Date1         := cData.Date1;
           v_Deal.DealType      := cData.DealType;
           v_Deal.BuySell       := cData.BuySell;
           v_Deal.Time1         := cData.Time1;
           v_Deal.Var_Marg      := cData.Var_Marg;
           v_Deal.NoBS          := cData.NoBS;
           v_Deal.Fee_NS        := cData.Fee_NS;
           v_Deal.Price_RUR     := cData.Price_RUR;
           v_Deal.Date_CLR      := cData.Date_CLR;
           v_Deal.Fee_Ex        := cData.Fee_Ex;
           v_Deal.Fee_CC        := cData.Fee_CC;
           v_Deal.ExtSettleCode := cData.ExtSettleCode;
           v_Deal.Comm          := cData.Comm;
           v_Deal.CalcCode      := cData.CalcCode;
           v_Deal.UniqCode      := cData.UniqCode;
           v_Deal.DealCode      := cData.DealCode;
           v_Deal.ExtDealCode   := cData.ExtDealCode;
           v_Deal.isHDayTrade   := cData.isHDayTrade;
           v_Deal.FileDate      := cData.FileDate;
        
           IF (v_Deal.DealType IN (14,15)) THEN -- ноги спреда
             v_Deal.ID_Mult    := cData.ID_Mult;
             v_Deal.ISIN2      := cData.ISIN2;
             v_Deal.Price2     := cData.Price2;
             v_Deal.Date2      := cData.Date2;
             v_Deal.Price_RUR2 := cData.Price_RUR2;
           END IF;
           
           IF(not p_Prm.isFutures) THEN
             v_Deal.Prem       := cData.Prem;
             IF (v_Deal.DealType = 4) THEN --Экспирация
               v_Deal.isExpir  := 1;
             ELSE
               v_Deal.isExpir  := 0;
             END IF;         
           END IF;
           SetDealInfo(v_Deal, p_Prm.isFutures);
        END IF;    
         
        IF v_ErrMes = CHR(1) THEN
          IF PutDealInfoPM(p_Prm, v_Deal, v_ErrMes, v_WarnMes) = 0 THEN
            v_stat := RSI_GT.SaveBatch(BATCH_SIZE, v_ErrMes); --сохранить по мере накопления
          END IF;
        END IF;

        IF v_ErrMes <> CHR(1) THEN
          RSB_GTLOG.Error('Внимание! '||v_ErrMes);
        END IF;

        IF v_WarnMes <> CHR(1) THEN
          RSB_GTLOG.Warning(v_WarnMes);
        END IF;
      END LOOP;
    END IF;

    IF v_stat = 0 THEN
      --сохранение накопленных объектов ЗР
      v_stat := RSI_GT.Save(v_ErrMes);
      IF v_stat <> 0 THEN
        RSB_GTLOG.Error('Внимание! '||v_ErrMes);
      END IF;
    END IF;
    
    --сохранение накопленных записей лога
    RSB_GTLOG.Save; 

  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_Prm.SeanceID, RSB_GTFN.RG_DVDEAL, RSB_GTLOG.ISSUE_FAULT,
                           'Внимание! Произошла непредвиденная ошибка при обработке данных в ф-и ProcessReplRecByTmp_FO04(), ' ||
                           'SeanceID=' || TO_CHAR(p_Prm.SeanceID) || ', Source_Code=' || p_Prm.Source_Code || 
                           ', SessionID=' || TO_CHAR(USERENV('sessionid')) || ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
  END;

  --Создание объектов записей репликаций по данным временной таблицы FO04
  FUNCTION CreateReplRecByTmp_FO04(p_SeanceID IN NUMBER, p_ImpDate IN DATE, p_SourceCode IN VARCHAR2, p_DVKind IN NUMBER, p_Time IN DATE, p_CalcDate IN DATE, p_PrmStr IN VARCHAR2)
    RETURN NUMBER
  IS
    v_Prm PRMREC_t;
  BEGIN
    --инициализация используемых параметров
    SetPrmRecByStr(v_Prm, p_PrmStr);
    v_Prm.SeanceID           := p_SeanceID;
    v_Prm.Imp_Date           := p_ImpDate;
    v_Prm.Source_Code        := p_SourceCode;
    IF(p_DVKind = RSB_GTFN.DERIVATIVE_FUTURES) THEN
      v_Prm.isFutures := true;
    ELSE
      v_Prm.isFutures := false;
    END IF;
    --подготовка отчета биржи
    PrepareMarketReport(v_Prm);
    --Подготовка отчёта по операции расчётов только в будние дни
    IF(RSI_RSBCALENDAR.GetDateAfterWorkDay(p_CalcDate, 0, 401) = p_CalcDate) THEN
      PrepareCalcReport(v_Prm, p_Time, p_CalcDate);
    END IF;
    --загрузка данных по сделкам
    ProcessReplRecByTmp_FO04(v_Prm);

    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DVDEAL, RSB_GTLOG.ISSUE_FAULT,
                           'Внимание! Произошла непредвиденная ошибка в ф-и CreateReplRecByTmp_FO04(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DVDEAL) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;
  
  -- ФУНКЦИИ ДЛЯ ОБРАБОТКИ ИТОГОВ ПО ПОЗИЦИЯМ 
 
  --Очистка временной таблицы итогово по позициям FPOS и OPOS
  FUNCTION DelFOPOS(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER
  IS
  BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE D_FOPOS_TMP';
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DVTRN, RSB_GTLOG.ISSUE_FAULT,
                           'Внимание! Произошла непредвиденная ошибка в ф-и DelFOPOS(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DVTRN) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;
  
  --Заполнение временной таблицы позиций FPOS на основании Payments
  FUNCTION FillFromFPOS(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2, p_Beg_date IN DATE, p_End_date IN DATE)
    RETURN NUMBER
  IS
  BEGIN 
    EXECUTE IMMEDIATE
      'INSERT INTO D_FOPOS_TMP(
        T_ID_FM_FOPOS, 
        T_ID_MB_REQUISITES, 
        T_ID_PROCESSING_LOG, 
        T_DATE_CLR,
        T_KOD,
        T_ISIN,
        T_VAR_MARG_P,
        T_VAR_MARG_D,
        T_GO,
        T_POS_EXEC,
        T_SBOR_EXEC,
        T_EXTSETTLECODE
     ) 
     (SELECT 
        NVL(FPOS.ID_FM_FPOS,0), 
        NVL(FPOS.ID_MB_REQUISITES,0), 
        NVL(FPOS.ID_PROCESSING_LOG,0), 
        NVL(FPOS.DATE_CLR,TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')),
        NVL(FPOS.KOD,CHR(1)),
        NVL(FPOS.ISIN,CHR(1)),
        NVL(FPOS.VAR_MARG_P,0), 
        NVL(FPOS.VAR_MARG_D,0), 
        NVL(FPOS.GO_BRUTTO,0), 
        NVL(FPOS.POS_EXEC,0), 
        NVL(FPOS.SBOR_EXEC,0),
        NVL(SUBSTR(FPOS.KOD,1,4), CHR(1))
      FROM '||p_Synonim||'.FM_FPOS FPOS
        WHERE FPOS.ID_MB_REQUISITES IN (SELECT NVL(PA.ID_MB_REQUISITES, 0) FROM '||p_Synonim||'.PROCESSING_ACTUAL PA WHERE PA.FILE_TYPE = ''fpos'' AND NVL(PA.TRADE_DATE,TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')) BETWEEN :Beg_date1 AND :End_date1 ) 
              AND NVL(FPOS.ACCOUNT,CHR(1)) = '''||CSV_ACCOUNT_OUR_BANK||'''
              AND NVL(FPOS.ISIN,CHR(1)) != ''EUR_CLT''
              AND NVL(FPOS.DATE_CLR,TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')) BETWEEN :Beg_date2 AND :End_date2 AND NVL(FPOS.ZRID,0) = 0 
              ) '
    USING p_Beg_date, p_End_date, p_Beg_date, p_End_date;
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DVTRN, RSB_GTLOG.ISSUE_FAULT,
                           'Внимание! Произошла непредвиденная ошибка в ф-и FillFromFPOS(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DVTRN) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END; 
  
  --Заполнение временной таблицы позиций OPOS на основании Payments
  FUNCTION FillFromOPOS(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2, p_Beg_date IN DATE, p_End_date IN DATE)
    RETURN NUMBER
  IS
  BEGIN 
    EXECUTE IMMEDIATE
      'INSERT INTO D_FOPOS_TMP(
        T_ID_FM_FOPOS, 
        T_ID_MB_REQUISITES, 
        T_ID_PROCESSING_LOG, 
        T_DATE_CLR,
        T_KOD,
        T_ISIN,
        T_VAR_MARG_P,
        T_VAR_MARG_D,
        T_GO,
        T_POS_EXEC,
        T_SBOR_EXEC,
        T_EXTSETTLECODE
     ) 
     (SELECT 
        NVL(OPOS.ID_FM_OPOS,0), 
        NVL(OPOS.ID_MB_REQUISITES,0), 
        NVL(OPOS.ID_PROCESSING_LOG,0), 
        NVL(OPOS.DATE_CLR,TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')),
        NVL(OPOS.KOD,CHR(1)),
        NVL(OPOS.ISIN,CHR(1)),
        NVL(OPOS.VAR_MARG_P,0), 
        NVL(OPOS.VAR_MARG_D,0), 
        NVL(OPOS.GO,0), 
        NVL(OPOS.POS_EXEC,0), 
        NVL(OPOS.SBOR_EXEC,0),
        NVL(SUBSTR(OPOS.KOD,1,4), CHR(1))
      FROM '||p_Synonim||'.FM_OPOS OPOS
        WHERE OPOS.ID_MB_REQUISITES IN (SELECT NVL(PA.ID_MB_REQUISITES, 0) FROM '||p_Synonim||'.PROCESSING_ACTUAL PA WHERE PA.FILE_TYPE = ''opos'' AND NVL(PA.TRADE_DATE,TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')) BETWEEN :Beg_date1 AND :End_date1 ) 
              AND NVL(OPOS.ACCOUNT,CHR(1)) = '''||CSV_ACCOUNT_OUR_BANK||'''
              AND NVL(OPOS.ISIN,CHR(1)) != ''EUR_CLT''
              AND NVL(OPOS.DATE_CLR,TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')) BETWEEN :Beg_date2 AND :End_date2 AND NVL(OPOS.ZRID,0) = 0    
              ) '
    USING p_Beg_date, p_End_date, p_Beg_date, p_End_date;
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DVTRN, RSB_GTLOG.ISSUE_FAULT,
                           'Внимание! Произошла непредвиденная ошибка в ф-и FillFromOPOS(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DVTRN) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END; 
  
  --Обновление ZRID загруженных позиций из FPOS в таблице Payments
  FUNCTION UpdateFPOS_PM(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2)
    RETURN NUMBER
  IS
    TYPE ObjID_t IS TABLE OF DGTSNCREC_DBT.T_RECORDID%TYPE; 
    TYPE PmID_t IS TABLE OF D_FOPOS_TMP.T_ID_FM_FOPOS%TYPE; 
    v_ObjID ObjID_t; 
    v_PmID   PmID_t; 
  BEGIN
    SELECT FPOS.T_ID_FM_FOPOS, GTSNCREC.T_RECORDID 
      BULK COLLECT INTO v_PmID, v_ObjID 
      FROM DGTSNCREC_DBT GTSNCREC, 
           DGTCODE_DBT GTCODE, 
           DGTOBJECT_DBT GTOBJECT, 
           D_FOPOS_TMP FPOS 
     WHERE GTSNCREC.T_SEANCEID = p_SeanceID
       AND GTCODE.T_OBJECTKIND = RSB_GTFN.RG_DVTRN
       AND GTCODE.T_OBJECTID   = GTSNCREC.T_OBJECTID 
       AND GTOBJECT.T_OBJECTID = GTSNCREC.T_OBJECTID 
       AND GTCODE.T_OBJECTCODE = FPOS.T_UNIQCODE;

    IF v_PmID.COUNT <> 0 THEN
      FORALL i IN v_PmID.FIRST .. v_PmID.LAST 
        EXECUTE IMMEDIATE
          'UPDATE '||p_Synonim||'.FM_FPOS  
              SET ZRID = :v_ObjIDi 
            WHERE ID_FM_FPOS = :v_PmIDi'
        USING IN v_ObjID(i), IN v_PmID(i);
        v_ObjID.Delete;
        v_PmID.Delete;
    END IF; 
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DVTRN, RSB_GTLOG.ISSUE_FAULT,
                           'Внимание! Произошла непредвиденная ошибка в ф-и UpdateFPOS_PM(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DVTRN) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;
  
  --Обновление ZRID загруженных позиций из OPOS в таблице Payments
  FUNCTION UpdateOPOS_PM(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2)
    RETURN NUMBER
  IS
    TYPE ObjID_t IS TABLE OF DGTSNCREC_DBT.T_RECORDID%TYPE; 
    TYPE PmID_t IS TABLE OF D_FOPOS_TMP.T_ID_FM_FOPOS%TYPE; 
    v_ObjID ObjID_t; 
    v_PmID  PmID_t; 
  BEGIN
    SELECT OPOS.T_ID_FM_FOPOS, GTSNCREC.T_RECORDID 
      BULK COLLECT INTO v_PmID, v_ObjID 
      FROM DGTSNCREC_DBT GTSNCREC, 
           DGTCODE_DBT GTCODE, 
           DGTOBJECT_DBT GTOBJECT, 
           D_FOPOS_TMP OPOS 
     WHERE GTSNCREC.T_SEANCEID = p_SeanceID
       AND GTCODE.T_OBJECTKIND = RSB_GTFN.RG_DVTRN
       AND GTCODE.T_OBJECTID   = GTSNCREC.T_OBJECTID 
       AND GTOBJECT.T_OBJECTID = GTSNCREC.T_OBJECTID 
       AND GTCODE.T_OBJECTCODE = OPOS.T_UNIQCODE;

    IF v_PmID.COUNT <> 0 THEN
      FORALL i IN v_PmID.FIRST .. v_PmID.LAST 
        EXECUTE IMMEDIATE
          'UPDATE '||p_Synonim||'.FM_OPOS  
              SET ZRID = :v_ObjIDi 
            WHERE ID_FM_OPOS = :v_PmIDi'
        USING IN v_ObjID(i), IN v_PmID(i);
        v_ObjID.Delete;
        v_PmID.Delete;
    END IF; 
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DVTRN, RSB_GTLOG.ISSUE_FAULT,
                           'Внимание! Произошла непредвиденная ошибка в ф-и UpdateOPOS_PM(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DVTRN) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;
  
  --Генерация кодов, связанных с позициями ПИ
  PROCEDURE SetPosCodes(p_Prm IN OUT NOCOPY PRMREC_t, p_Pos IN OUT NOCOPY DATAPOS_t, p_OurBankCode IN VARCHAR2 )
  IS
    v_StringDate      VARCHAR(11);
  BEGIN
  
    v_StringDate := TO_CHAR(p_Pos.Date_CLR, 'DD.MM.YYYY');
    p_Pos.UniqCode := p_Pos.ISIN||'/'||RsbSessionData.OperDprt()||'/'||-1||'/'||p_Pos.Kod||'/';
    p_Pos.UniqCode := p_Pos.UniqCode||SUBSTR(v_StringDate, 1, 2)||SUBSTR(v_StringDate, 4, 2)||SUBSTR(v_StringDate, 7, 4); 
   
    p_Pos.CalcCode := p_Prm.MMVB_Code||SUBSTR(v_StringDate, 1, 2)||SUBSTR(v_StringDate, 4, 2)||SUBSTR(v_StringDate, 7, 4);
    IF ((p_Pos.Kod = CHR(1)) OR (UPPER(p_Pos.Kod) = p_OurBankCode) OR RSB_GTFN.CheckBankCode(UPPER(p_Pos.Kod)) <> 0) THEN
      p_Pos.CalcCode  := p_Pos.CalcCode||'/С'||'/'||RsbSessionData.OperDprt();
    ELSE
      p_Pos.CalcCode  := p_Pos.CalcCode||'/К'||'/'||RsbSessionData.OperDprt();
    END IF;
  END;
  
  --Обновление информации по объектам шлюза и кодам итогов по позициям FPOS и OPOS
  FUNCTION UpdateObjCodeFOPOS(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_DVKind IN NUMBER, p_PrmStr IN VARCHAR2)
    RETURN NUMBER
  IS
    v_Prm PRMREC_t;
    v_OurBankCode VARCHAR(35);
    v_Pos    DATAPOS_t;
    v_stat    NUMBER(5) := 0;
    TYPE POSARR_t IS TABLE OF DATAPOS_t;
    v_PosArr POSARR_t := POSARR_t();
    TYPE ObjID_t IS TABLE OF DGTCODE_DBT.T_OBJECTID%TYPE; 
    TYPE FoPOSID_t IS TABLE OF D_FOPOS_TMP.T_ID_FM_FOPOS%TYPE; 
    v_ObjIDs  ObjID_t; 
    v_FoPOSIDs   FoPOSID_t;
  BEGIN 
    --инициализация используемых параметров
    SetPrmRecByStr(v_Prm, p_PrmStr);
    v_Prm.SeanceID    := p_SeanceID;
    v_Prm.Source_Code := p_SourceCode;
    IF(p_DVKind = RSB_GTFN.DERIVATIVE_FUTURES) THEN
      v_Prm.isFutures := true;
    ELSE
      v_Prm.isFutures := false;
    END IF;
    
    BEGIN
       SELECT NVL(t_Code, CHR(1)) INTO v_OurBankCode
         FROM dobjcode_dbt
        WHERE t_ObjectType = 3 -- субъект
          AND t_CodeKind   = 8 -- код на ММВБ
          AND t_State      = 0 -- действующий код
          AND t_ObjectID   = RsbSessionData.OurBank;
       EXCEPTION
        WHEN NO_DATA_FOUND THEN v_OurBankCode := CHR(1);
    END;
    
    FOR cData IN (SELECT NVL(fopos.t_ID_FM_FOPOS, 0) ID_FM_FOPOS,
                         NVL(fopos.T_ISIN, CHR(1)) ISIN,
                         NVL(fopos.T_KOD, CHR(1)) Kod,
                         NVL(fopos.T_DATE_CLR, RSI_GT.ZeroDate) Date_CLR
                    FROM D_FOPOS_TMP fopos
                 )
    LOOP   
        v_Pos.ID_FM_FOPOS := cData.ID_FM_FOPOS;
        v_Pos.ISIN        := cData.ISIN;
        v_Pos.Kod         := cData.Kod;
        v_Pos.Date_CLR    := cData.Date_CLR;
        SetPosCodes(v_Prm, v_Pos, v_OurBankCode); 
        v_PosArr.Extend();
        v_PosArr(v_PosArr.last) := v_Pos;
    END LOOP;

    IF v_PosArr.COUNT > 0 THEN
     FORALL indx IN v_PosArr.FIRST .. v_PosArr.LAST
      UPDATE D_FOPOS_TMP FOPOS
       SET FOPOS.T_UNIQCODE     = v_PosArr(indx).UniqCode,
           FOPOS.T_CALCCODE     = v_PosArr(indx).CalcCode           
        WHERE FOPOS.T_ID_FM_FOPOS = v_PosArr(indx).ID_FM_FOPOS;
      v_PosArr.Delete;     
    END IF; 

    SELECT GTCODE.T_OBJECTID, FOPOS.T_ID_FM_FOPOS
      BULK COLLECT INTO v_ObjIDs, v_FoPOSIDs
      FROM DGTCODE_DBT GTCODE, D_FOPOS_TMP FOPOS 
     WHERE GTCODE.T_APPLICATIONID IN (RSB_GTFN.GT_PAYMENTS_CSV, RSB_GTFN.GT_CSV)
       AND GTCODE.T_OBJECTKIND = RSB_GTFN.RG_DVTRN
       AND GTCODE.T_OBJECTCODE = FOPOS.T_UNIQCODE;
       
    IF v_ObjIDs.COUNT <> 0 THEN 
      FORALL i IN v_ObjIDs.FIRST .. v_ObjIDs.LAST 
        UPDATE D_FOPOS_TMP FOPOS 
           SET FOPOS.t_ObjectID   = v_ObjIDs(i) 
         WHERE FOPOS.T_ID_FM_FOPOS = v_FoPOSIDs(i);
     END IF;
     
    --Проверим повторения внутри пачки
    UPDATE D_FOPOS_TMP FOPOS 
       SET FOPOS.T_ISREPEAT = 1
     WHERE EXISTS (SELECT 1
                     FROM D_FOPOS_TMP FOPOS1
                    WHERE FOPOS.T_UNIQCODE = FOPOS1.T_UNIQCODE
                      AND FOPOS.T_ID_FM_FOPOS > FOPOS1.T_ID_FM_FOPOS); 
     
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(v_Prm.SeanceID, RSB_GTFN.RG_DVTRN, RSB_GTLOG.ISSUE_FAULT,
                           'Внимание! Произошла непредвиденная ошибка при обработке данных в ф-и UpdateObjCodeFOPOS(), ' ||
                           'SeanceID=' || TO_CHAR(v_Prm.SeanceID) || ', Source_Code=' || v_Prm.Source_Code || 
                           ', SessionID=' || TO_CHAR(USERENV('sessionid')) || ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;
  
  --Заполнение и сохранение параметров итогов позиции с ПИ
  FUNCTION PutPosInfoPM(p_Prm IN OUT NOCOPY PRMREC_t, p_Pos IN OUT NOCOPY DATAPOS_t, p_ErrMes IN OUT NOCOPY VARCHAR2, p_WarnMes IN OUT NOCOPY VARCHAR2) RETURN NUMBER
  IS
    v_stat         NUMBER(5) := 0;
  BEGIN

    v_stat := RSI_GT.InitRec(RSB_GTFN.RG_DVTRN,
                            'Итоги дня по позиции с ПИ № '||p_Pos.UniqCode, --Наименование объекта
                             0, --проверять существование объекта в системе по коду
                             p_Pos.UniqCode, --код объекта
                             p_Pos.ClientID, --ID клиента
                             p_ErrMes);

    IF v_stat = 0 THEN 
      RSI_GT.SetParmByName('RGDVTRN_DATE', p_Pos.Date_CLR);
      RSI_GT.SetParmByName('RGDVTRN_FI_CODE', p_Pos.ISIN);
      RSI_GT.SetParmByName('RGDVTRN_BROKER', -1);
      RSI_GT.SetParmByName('RGDVTRN_CLIENT', p_Pos.Kod);
      RSI_GT.SetParmByName('RGDVTRN_DEPARTMENT', RsbSessionData.OperDprt());
      RSI_GT.SetParmByName('RGDVTRN_MARGIN', p_Pos.Var_Marg_P + p_Pos.Var_Marg_D);
      RSI_GT.SetParmByName('RGDVTRN_MARGIN_DAY', p_Pos.Var_Marg_P);
      RSI_GT.SetParmByName('RGDVTRN_MARGIN_DEALS', p_Pos.Var_Marg_D);
      RSI_GT.SetParmByName('RGDVTRN_CONTRCOMM', p_Pos.Sbor_Exec);
      RSI_GT.SetParmByName('RGDVTRN_GUARANTY', p_Pos.Go);
      RSI_GT.SetParmByName('RGDVTRN_CALCKIND', RSB_GTFN.OPERKIND_DVOPER);
      RSI_GT.SetParmByName('RGDVTRN_CALCCODE', p_Pos.CalcCode);
      RSI_GT.SetParmByName('RGDVTRN_EXTSETTLECODE', p_Pos.ExtSettleCode);

      --проверка ошибок после добавления параметров
      IF RSI_GT.GetLastError <> CHR(1) THEN
        v_stat := 1;
        p_ErrMes := RSI_GT.GetLastError;
      END IF;
    END IF;

    RETURN v_stat;
  END;
  
  --Обработка записей по данным буферной таблицы FOPOS
  PROCEDURE ProcessReplRecByTmp_FOPOS(p_Prm IN OUT NOCOPY PRMREC_t)
  IS
    v_Pos     DATAPOS_t;
    v_stat    NUMBER(5) := 0;
    v_SfContrID    NUMBER(10) := 0;
    v_ClientID     NUMBER(10) := 0;
    v_ErrMes  VARCHAR2(1000) := CHR(1);
    v_WarnMes VARCHAR2(1000) := CHR(1);
    TYPE POSARR_t IS TABLE OF DATAPOS_t;
    v_PosArr POSARR_t := POSARR_t();
  BEGIN
    --инициализация глобального лога для накопления записей для вставки
    RSB_GTLOG.Init(BATCH_SIZE, p_Prm.SeanceID, RSB_GTFN.RG_DVTRN);

    --инициализация глобального накопителя объектов ЗР для вставки
    v_stat := RSI_GT.Init(RSI_GT.DIRECT_INSERT_YES, p_Prm.SeanceID, p_Prm.Source_Code, RSI_GT.GTCODE_RECEIVE, v_ErrMes);
    IF v_stat <> 0 THEN
      RSB_GTLOG.Error('Внимание! '||v_ErrMes);
    END IF;
    IF v_stat = 0 THEN
      FOR cData IN (SELECT NVL(fopos.t_ID_FM_FOPOS, 0) ID_FM_FOPOS,
                           NVL(fopos.T_DATE_CLR, RSI_GT.ZeroDate) Date_CLR,
                           NVL(fopos.T_KOD, CHR(1)) Kod,
                           NVL(fopos.T_ISIN, CHR(1)) ISIN,
                           NVL(fopos.T_VAR_MARG_P, 0) Var_Marg_P,
                           NVL(fopos.T_VAR_MARG_D, 0) Var_Marg_D,
                           NVL(fopos.T_GO, 0) Go,
                           NVL(fopos.T_POS_EXEC, 0) Pos_Exec,
                           NVL(fopos.T_SBOR_EXEC, 0) Sbor_Exec,
                           NVL(fopos.T_EXTSETTLECODE, CHR(1)) ExtSettleCode,
                           NVL(fopos.T_CALCCODE, CHR(1)) CalcCode,
                           NVL(fopos.T_UNIQCODE, CHR(1)) UniqCode,
                           NVL(fopos.T_OBJECTID, 0) ObjectID,
                           NVL(fopos.T_ISREPEAT, 0) IsRepeat
                      FROM D_FOPOS_TMP fopos
                   )
      LOOP
        v_ErrMes  := CHR(1);
        v_WarnMes := CHR(1);
        IF cData.ObjectID > 0 THEN
           RSB_GTLOG.Message('Итоги дня по позиции ПИ с кодом "'|| cData.UniqCode || '"' || ' уже загружены в шлюз'|| CHR(10)|| CHR(10)||'Запись репликации с действием создать для объекта ' || TO_CHAR(cData.ObjectID) || ' уже передавалась в RS-Bank.' || CHR(10) || 'Повторная репликация невозможна.', RSB_GTLOG.ISSUE_WARNING);
        ELSIF cData.IsRepeat > 0 THEN
           RSB_GTLOG.Message('Итоги дня по позиции ПИ с кодом "'|| cData.UniqCode || '"' || ' уже загружены в шлюз'|| CHR(10)|| CHR(10)||'Запись репликации с действием создать для данного объекта уже обработана в текущей сессии.' || CHR(10) || 'Повторная репликация невозможна.', RSB_GTLOG.ISSUE_WARNING);
        ELSE               
           v_Pos.ID_FM_FOPOS   := cData.ID_FM_FOPOS;
           v_Pos.Date_CLR      := cData.Date_CLR;
           v_Pos.Kod           := cData.Kod;
           v_Pos.ISIN          := cData.ISIN;
           v_Pos.Var_Marg_P    := cData.Var_Marg_P; 
           v_Pos.Var_Marg_D    := cData.Var_Marg_D;
           v_Pos.Go            := cData.Go;
           v_Pos.Pos_Exec      := cData.Pos_Exec;
           v_Pos.Sbor_Exec     := cData.Sbor_Exec;
           v_Pos.ExtSettleCode := cData.ExtSettleCode;    
           v_Pos.CalcCode := cData.CalcCode;  
           v_Pos.UniqCode := cData.UniqCode;  
           v_stat := RSB_GTFN.GetRealID(RSB_GTFN.RG_PARTY, v_Pos.Kod, CNST.PTCK_MICEX, v_Pos.ClientID, p_Prm.IsRSHB, v_WarnMes, true, RSB_GTFN.PTSK_DV, cData.Date_CLR, v_SfContrID);
           IF PutPosInfoPM(p_Prm, v_Pos, v_ErrMes, v_WarnMes) = 0 THEN
             v_stat := RSI_GT.SaveBatch(BATCH_SIZE, v_ErrMes); --сохранить по мере накопления
           END IF;
        END IF;

        IF v_ErrMes <> CHR(1) THEN
          RSB_GTLOG.Error('Внимание! '||v_ErrMes);
        END IF;

        IF v_WarnMes <> CHR(1) THEN
          RSB_GTLOG.Warning(v_WarnMes);
        END IF;
      END LOOP;
    END IF;

    IF v_stat = 0 THEN
      --сохранение накопленных объектов ЗР
      v_stat := RSI_GT.Save(v_ErrMes);
      IF v_stat <> 0 THEN
        RSB_GTLOG.Error('Внимание! '||v_ErrMes);
      END IF;
    END IF;
    --сохранение накопленных записей лога
    RSB_GTLOG.Save; 

  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_Prm.SeanceID, RSB_GTFN.RG_DVTRN, RSB_GTLOG.ISSUE_FAULT,
                           'Внимание! Произошла непредвиденная ошибка при обработке данных в ф-и ProcessReplRecByTmp_FOPOS(), ' ||
                           'SeanceID=' || TO_CHAR(p_Prm.SeanceID) || ', Source_Code=' || p_Prm.Source_Code || 
                           ', SessionID=' || TO_CHAR(USERENV('sessionid')) || ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
  END;

  --Создание объектов записей репликаций по данным временной таблицы FOPOS
  FUNCTION CreateReplRecByTmp_FOPOS(p_SeanceID IN NUMBER, p_ImpDate IN DATE, p_SourceCode IN VARCHAR2, p_DVKind IN NUMBER, p_PrmStr IN VARCHAR2)
    RETURN NUMBER
  IS
    v_Prm PRMREC_t;
  BEGIN
    --инициализация используемых параметров
    SetPrmRecByStr(v_Prm, p_PrmStr);
    v_Prm.SeanceID           := p_SeanceID;
    v_Prm.Imp_Date           := p_ImpDate;
    v_Prm.Source_Code        := p_SourceCode;
    IF(p_DVKind = RSB_GTFN.DERIVATIVE_FUTURES) THEN
      v_Prm.isFutures := true;
    ELSE
      v_Prm.isFutures := false;
    END IF;
    --загрузка данных по позициям
    ProcessReplRecByTmp_FOPOS(v_Prm);

    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DVTRN, RSB_GTLOG.ISSUE_FAULT,
                           'Внимание! Произошла непредвиденная ошибка в ф-и CreateReplRecByTmp_FOPOS(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DVTRN) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;
  
  -- ФУНКЦИИ ДЛЯ ОБРАБОТКИ ИНФОРМАЦИИ ПО ЗАЯВКАМ
  
  --Очистка временной таблицы заявок FORDLOG и OORDLOG
  FUNCTION DelFOORDLOG(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER
  IS
  BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE D_FOORDLOG_TMP';
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DVREQ, RSB_GTLOG.ISSUE_FAULT,
                           'Внимание! Произошла непредвиденная ошибка в ф-и DelFOORDLOG(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DVREQ) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;
  
  --Заполнение временной таблицы заявок FORDLOG на основании Payments
  FUNCTION FillFromFORDLOG(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2, p_Beg_date IN DATE, p_End_date IN DATE)
    RETURN NUMBER
  IS
  BEGIN 
    EXECUTE IMMEDIATE
      'INSERT INTO D_FOORDLOG_TMP(
        T_ID_FM_FOORDLOG, T_ID_MB_REQUISITES, T_ID_PROCESSING_LOG, T_DIR, T_N, T_ID_ORD
        , T_ISIN, T_DATE, T_TIME, T_CLIENT_CODE, T_ID_DEAL, T_AMOUNT, T_PRICE
        , T_COMMENTAR, T_ISHDAYTRADE, T_FILEDATE
     ) 
     (SELECT 
        NVL(FORD.ID_FM_FORDLOG,0), 
        NVL(FORD.ID_MB_REQUISITES,0), 
        NVL(FORD.ID_PROCESSING_LOG,0), 
        NVL(FORD.DIR,0),
        NVL(FORD.N,CHR(1)),
        NVL(FORD.ID_ORD,CHR(1)),
        NVL(FORD.ISIN,CHR(1)),
        NVL(TO_DATE(TO_CHAR(FORD.MOMENT, ''ddmmyyyy''), ''ddmmyyyy''),TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')),
        NVL(TO_DATE(''01010001'' ||TO_CHAR(FORD.MOMENT, ''hh24:mi:ss''), ''ddmmyyyyhh24:mi:ss''),TO_DATE(''01.01.0001 00:00:00'', ''DD.MM.YYYY HH24:MI:SS'')), 
        NVL(FORD.CLIENT_CODE,CHR(1)), 
        NVL(FORD.ID_DEAL,CHR(1)),   
        NVL(FORD.AMOUNT,0), 
        NVL(FORD.PRICE,0),
        NVL(FORD.COMENT,CHR(1)),
        CASE WHEN EXISTS (SELECT 1 from RSHB_RSPAYM_MB.MB_REQUISITES MR WHERE MR.ID_MB_REQUISITES = FORD.ID_MB_REQUISITES AND MR.FILE_NAME LIKE ''%trade%'') then chr(88) else chr(1) END,
        NVL((SELECT PrAc.TRADE_DATE from RSHB_RSPAYM_MB.PROCESSING_ACTUAL PrAc where PrAc.ID_MB_REQUISITES = FORD.ID_MB_REQUISITES), TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')) 
      FROM '||p_Synonim||'.FM_FORDLOG FORD
        WHERE FORD.ID_MB_REQUISITES IN (SELECT NVL(PA.ID_MB_REQUISITES, 0) FROM '||p_Synonim||'.PROCESSING_ACTUAL PA WHERE PA.FILE_TYPE = ''fordlog'' AND NVL(PA.TRADE_DATE,TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')) BETWEEN :Beg_date1 AND :End_date1 ) 
              AND NVL(FORD.ID_DEAL,CHR(1)) != ''0''
              AND NVL(FORD.ZRID,0) = 0 
              AND LOWER(NVL(FORD.COMENT,CHR(1))) NOT LIKE ''%mc%''
              ) '
    USING p_Beg_date, p_End_date;
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DVREQ, RSB_GTLOG.ISSUE_FAULT,
                           'Внимание! Произошла непредвиденная ошибка в ф-и FillFromFORDLOG(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DVREQ) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END; 
  
  --Заполнение временной таблицы заявок OORDLOG на основании Payments
  FUNCTION FillFromOORDLOG(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2, p_Beg_date IN DATE, p_End_date IN DATE)
    RETURN NUMBER
  IS
  BEGIN 
    EXECUTE IMMEDIATE
      'INSERT INTO D_FOORDLOG_TMP(
        T_ID_FM_FOORDLOG, T_ID_MB_REQUISITES, T_ID_PROCESSING_LOG, T_DIR, T_N, T_ID_ORD
        , T_ISIN, T_DATE, T_TIME, T_CLIENT_CODE, T_ID_DEAL, T_AMOUNT, T_PRICE
        , T_COMMENTAR, T_ISHDAYTRADE, T_FILEDATE
     ) 
     (SELECT 
        NVL(OORD.ID_FM_OORDLOG,0), 
        NVL(OORD.ID_MB_REQUISITES,0), 
        NVL(OORD.ID_PROCESSING_LOG,0), 
        NVL(OORD.DIR,0),
        NVL(OORD.N,CHR(1)),
        NVL(OORD.ID_ORD,CHR(1)),
        NVL(OORD.ISIN,CHR(1)),
        NVL(TO_DATE(TO_CHAR(OORD.MOMENT, ''ddmmyyyy''), ''ddmmyyyy''),TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')),
        NVL(TO_DATE(''01010001'' ||TO_CHAR(OORD.MOMENT, ''hh24:mi:ss''), ''ddmmyyyyhh24:mi:ss''),TO_DATE(''01.01.0001 00:00:00'', ''DD.MM.YYYY HH24:MI:SS'')), 
        NVL(OORD.CLIENT_CODE,CHR(1)), 
        NVL(OORD.ID_DEAL,CHR(1)),   
        NVL(OORD.AMOUNT,0), 
        NVL(OORD.PRICE,0),
        NVL(OORD.COMENT,CHR(1)),
        CASE WHEN EXISTS (SELECT 1 from RSHB_RSPAYM_MB.MB_REQUISITES MR WHERE MR.ID_MB_REQUISITES = OORD.ID_MB_REQUISITES AND MR.FILE_NAME LIKE ''%trade%'') then chr(88) else chr(1) END,
        NVL((SELECT PrAc.TRADE_DATE from RSHB_RSPAYM_MB.PROCESSING_ACTUAL PrAc where PrAc.ID_MB_REQUISITES = FORD.ID_MB_REQUISITES), TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')) 
      FROM '||p_Synonim||'.FM_OORDLOG OORD
        WHERE OORD.ID_MB_REQUISITES IN (SELECT NVL(PA.ID_MB_REQUISITES, 0) FROM '||p_Synonim||'.PROCESSING_ACTUAL PA WHERE PA.FILE_TYPE = ''oordlog'' AND NVL(PA.TRADE_DATE,TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')) BETWEEN :Beg_date1 AND :End_date1 ) 
              AND NVL(OORD.ID_DEAL,CHR(1)) != ''0''
              AND NVL(OORD.ZRID,0) = 0 
              AND LOWER(NVL(OORD.COMENT,CHR(1))) NOT LIKE ''%mc%''
              ) '
    USING p_Beg_date, p_End_date;
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DVREQ, RSB_GTLOG.ISSUE_FAULT,
                           'Внимание! Произошла непредвиденная ошибка в ф-и FillFromOORDLOG(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DVREQ) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;
  
    --Обновление ZRID загруженных позиций из FORDLOG в таблице Payments
  FUNCTION UpdateFORDLOG_PM(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2)
    RETURN NUMBER
  IS
    TYPE ObjID_t IS TABLE OF DGTSNCREC_DBT.T_RECORDID%TYPE; 
    TYPE PmID_t IS TABLE OF D_FOORDLOG_TMP.T_ID_FM_FOORDLOG%TYPE; 
    v_ObjID ObjID_t; 
    v_PmID  PmID_t; 
  BEGIN
    SELECT FORDLOG.T_ID_FM_FOORDLOG, GTSNCREC.T_RECORDID 
      BULK COLLECT INTO v_PmID, v_ObjID 
      FROM DGTSNCREC_DBT GTSNCREC, 
           DGTCODE_DBT GTCODE, 
           DGTOBJECT_DBT GTOBJECT, 
           D_FOORDLOG_TMP FORDLOG 
     WHERE GTSNCREC.T_SEANCEID = p_SeanceID
       AND GTCODE.T_OBJECTKIND = RSB_GTFN.RG_DVREQ
       AND GTCODE.T_OBJECTID   = GTSNCREC.T_OBJECTID 
       AND GTOBJECT.T_OBJECTID = GTSNCREC.T_OBJECTID 
       AND GTCODE.T_OBJECTCODE = FORDLOG.T_UNIQCODE;

    IF v_PmID.COUNT <> 0 THEN
      FORALL i IN v_PmID.FIRST .. v_PmID.LAST 
        EXECUTE IMMEDIATE
          'UPDATE '||p_Synonim||'.FM_FORDLOG  
              SET ZRID = :v_ObjIDi 
            WHERE ID_FM_FORDLOG = :v_PmIDi'
        USING IN v_ObjID(i), IN v_PmID(i);
        v_ObjID.Delete;
        v_PmID.Delete;
    END IF; 
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DVREQ, RSB_GTLOG.ISSUE_FAULT,
                           'Внимание! Произошла непредвиденная ошибка в ф-и UpdateFORDLOG_PM(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DVREQ) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;
  
  --Обновление ZRID загруженных позиций из OORDLOG в таблице Payments
  FUNCTION UpdateOORDLOG_PM(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2)
    RETURN NUMBER
  IS
    TYPE ObjID_t IS TABLE OF DGTSNCREC_DBT.T_RECORDID%TYPE; 
    TYPE PmID_t IS TABLE OF D_FOORDLOG_TMP.T_ID_FM_FOORDLOG%TYPE; 
    v_ObjID ObjID_t; 
    v_PmID  PmID_t; 
  BEGIN
    SELECT ORDLOG.T_ID_FM_FOORDLOG, GTSNCREC.T_RECORDID 
      BULK COLLECT INTO v_PmID, v_ObjID 
      FROM DGTSNCREC_DBT GTSNCREC, 
           DGTCODE_DBT GTCODE, 
           DGTOBJECT_DBT GTOBJECT, 
           D_FOORDLOG_TMP ORDLOG 
     WHERE GTSNCREC.T_SEANCEID = p_SeanceID
       AND GTCODE.T_OBJECTKIND = RSB_GTFN.RG_DVREQ
       AND GTCODE.T_OBJECTID   = GTSNCREC.T_OBJECTID 
       AND GTOBJECT.T_OBJECTID = GTSNCREC.T_OBJECTID 
       AND GTCODE.T_OBJECTCODE = ORDLOG.T_UNIQCODE;

    IF v_PmID.COUNT <> 0 THEN
      FORALL i IN v_PmID.FIRST .. v_PmID.LAST 
        EXECUTE IMMEDIATE
          'UPDATE '||p_Synonim||'.FM_OORDLOG  
              SET ZRID = :v_ObjIDi 
            WHERE ID_FM_OORDLOG = :v_PmIDi'
        USING IN v_ObjID(i), IN v_PmID(i);
        v_ObjID.Delete;
        v_PmID.Delete;
    END IF; 
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DVREQ, RSB_GTLOG.ISSUE_FAULT,
                           'Внимание! Произошла непредвиденная ошибка в ф-и UpdateOPOS_PM(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DVTRN) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;
  
  --Генерация внутреннего кода заявки СР
  FUNCTION SetReqCode(p_Prm IN PRMREC_t, p_Req IN DATAREQ_t)
    RETURN VARCHAR2
  IS
    v_StringDate      VARCHAR(11);
  BEGIN
  
    v_StringDate := TO_CHAR(p_Req.Date1, 'DD.MM.YYYY');
    RETURN SUBSTR(v_StringDate, 1, 2)||SUBSTR(v_StringDate, 4, 2)||SUBSTR(v_StringDate, 7, 4)||'0'||p_Req.ID_Ord; 
   
  END;
  
  --Обновление информации по объектам шлюза и кодам заявок FORDLOG и ORDLOG
  FUNCTION UpdateObjCodeFOORDLOG(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_DVKind IN NUMBER, p_PrmStr IN VARCHAR2)
    RETURN NUMBER
  IS
    v_Prm PRMREC_t;
    v_Req    DATAREQ_t;
    v_stat    NUMBER(5) := 0;
    TYPE REQARR_t IS TABLE OF DATAREQ_t;
    v_ReqArr REQARR_t := REQARR_t();
    TYPE ObjID_t IS TABLE OF DGTCODE_DBT.T_OBJECTID%TYPE; 
    TYPE FORDLOGID_t IS TABLE OF D_FOORDLOG_TMP.T_ID_FM_FOORDLOG%TYPE; 
    v_ObjIDs   ObjID_t; 
    v_FORDLOGIDs FORDLOGID_t;
    v_StringFileDate VARCHAR(11);
  BEGIN 
    --инициализация используемых параметров
    SetPrmRecByStr(v_Prm, p_PrmStr);
    v_Prm.SeanceID    := p_SeanceID;
    v_Prm.Source_Code := p_SourceCode;
    IF(p_DVKind = RSB_GTFN.DERIVATIVE_FUTURES) THEN
      v_Prm.isFutures := true;
    ELSE
      v_Prm.isFutures := false;
    END IF;
     
    FOR cData IN (SELECT NVL(foordlog.t_ID_FM_FOORDLOG, 0) ID_FM_FOORDLOG,
                         NVL(foordlog.t_N, CHR(1)) N,
                         NVL(foordlog.T_FILEDATE, RSI_GT.ZeroDate) FileDate                        
                    FROM D_FOORDLOG_TMP foordlog
                 )
    LOOP   
        v_Req.ID_FM_FOORDLOG := cData.ID_FM_FOORDLOG;
        v_StringFileDate     := CASE WHEN SUBSTR(to_char(cData.FileDate,'dd'), 1, 1) = '0' THEN ' ' ELSE '' END || TRIM(LEADING '0' FROM to_char(cData.FileDate, 'dd.mm.yyyy'));        
        v_Req.UniqCode       := cData.N||'_'||v_StringFileDate;
        v_ReqArr.Extend();
        v_ReqArr(v_ReqArr.last) := v_Req;
    END LOOP;

    IF v_ReqArr.COUNT > 0 THEN
     FORALL indx IN v_ReqArr.FIRST .. v_ReqArr.LAST
      UPDATE D_FOORDLOG_TMP FOORDLOG
       SET FOORDLOG.T_UNIQCODE     = v_ReqArr(indx).UniqCode         
        WHERE FOORDLOG.T_ID_FM_FOORDLOG = v_ReqArr(indx).ID_FM_FOORDLOG;
      v_ReqArr.Delete;     
    END IF; 

    SELECT GTCODE.T_OBJECTID, FOORDLOG.T_ID_FM_FOORDLOG
      BULK COLLECT INTO v_ObjIDs, v_FORDLOGIDs
      FROM DGTCODE_DBT GTCODE, D_FOORDLOG_TMP FOORDLOG 
     WHERE GTCODE.T_APPLICATIONID IN (RSB_GTFN.GT_PAYMENTS_CSV, RSB_GTFN.GT_CSV)
       AND GTCODE.T_OBJECTKIND = RSB_GTFN.RG_DVREQ
       AND GTCODE.T_OBJECTCODE = FOORDLOG.T_UNIQCODE;
       
    IF v_ObjIDs.COUNT <> 0 THEN 
      FORALL i IN v_ObjIDs.FIRST .. v_ObjIDs.LAST 
        UPDATE D_FOORDLOG_TMP FOORDLOG 
           SET FOORDLOG.t_ObjectID   = v_ObjIDs(i) 
         WHERE FOORDLOG.T_ID_FM_FOORDLOG = v_FORDLOGIDs(i);
     END IF;
     
    --Проверим повторения внутри пачки
    UPDATE D_FOORDLOG_TMP FOORDLOG 
       SET FOORDLOG.T_ISREPEAT = 1
     WHERE EXISTS (SELECT 1
                     FROM D_FOORDLOG_TMP FOORDLOG1
                    WHERE FOORDLOG.T_UNIQCODE = FOORDLOG1.T_UNIQCODE
                      AND FOORDLOG.T_ID_FM_FOORDLOG > FOORDLOG1.T_ID_FM_FOORDLOG); 
     
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(v_Prm.SeanceID, RSB_GTFN.RG_DVREQ, RSB_GTLOG.ISSUE_FAULT,
                           'Внимание! Произошла непредвиденная ошибка при обработке данных в ф-и UpdateObjCodeFOORDLOG(), ' ||
                           'SeanceID=' || TO_CHAR(v_Prm.SeanceID) || ', Source_Code=' || v_Prm.Source_Code || 
                           ', SessionID=' || TO_CHAR(USERENV('sessionid')) || ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;
  
  --Установка параметров, которых нет в таблице Payments
  PROCEDURE SetReqInfo(p_Req IN OUT NOCOPY DATAREQ_t, isFutures IN BOOLEAN)
  IS
  BEGIN
  
    IF(isFutures) THEN
       IF(p_Req.Dir = 1) THEN
          p_Req.DealKind := RSB_GTFN.OPERKIND_FUTURESBUY;       
       ELSE
          p_Req.DealKind := RSB_GTFN.OPERKIND_FUTURESSELL;   
       END IF;
    ELSE
       IF(p_Req.Dir = 1) THEN
          p_Req.DealKind := RSB_GTFN.OPERKIND_OPTIONBUY;       
       ELSE
          p_Req.DealKind := RSB_GTFN.OPERKIND_OPTIONSELL;   
       END IF;
    END IF;

  END;
  
  --Заполнение и сохранение параметров итогов позиции с ПИ
  FUNCTION PutReqInfoPM(p_Prm IN OUT NOCOPY PRMREC_t, p_Req IN OUT NOCOPY DATAREQ_t, p_ErrMes IN OUT NOCOPY VARCHAR2, p_WarnMes IN OUT NOCOPY VARCHAR2) RETURN NUMBER
  IS
    v_stat         NUMBER(5) := 0;
    v_TraderCode   p_Req.Commentar%TYPE;
  BEGIN

    v_stat := RSI_GT.InitRec(RSB_GTFN.RG_DVREQ,
                            'Клиентское поручение № '||SUBSTR(p_Req.UniqCode, 1, LENGTH(p_Req.UniqCode) - 11)||' за '||CASE WHEN SUBSTR(to_char(p_Req.FileDate,'dd'), 1, 1) = '0' THEN ' ' ELSE '' END || TRIM(LEADING '0' FROM to_char(p_Req.FileDate, 'dd.mm.yyyy')), --Наименование объекта
                             0, --проверять существование объекта в системе по коду
                             p_Req.UniqCode, --код объекта
                             0, --ID клиента
                             p_ErrMes);

    IF v_stat = 0 THEN 
      RSI_GT.SetParmByName('RGDVRQ_OPERKIND', p_Req.DealKind);
      RSI_GT.SetParmByName('RGDVRQ_FI_CODE', p_Req.ISIN);
      RSI_GT.SetParmByName('RGDVRQ_CODETS', p_Req.ID_Ord);
      RSI_GT.SetParmByName('RGDVRQ_CODE', SetReqCode(p_Prm, p_Req));
      RSI_GT.SetParmByName('RGDVRQ_DATE', p_Req.Date1);
      RSI_GT.SetParmByName('RGDVRQ_TIME', p_Req.Time1);
      RSI_GT.SetParmByName('RGDVRQ_CLIENT', p_Req.Client_Code);
      RSI_GT.SetParmByName('RGDVRQ_IDDEAL', p_Req.ID_Deal);
      RSI_GT.SetParmByName('RGDVRQ_PRICE', p_Req.Price);
      RSI_GT.SetParmByName('RGDVRQ_VOL', p_Req.Amount);
      RSI_GT.SetParmByName('RGDVRQ_ISHDAYTRADE', p_Req.IsHDayTrade);

      v_TraderCode := RSB_GTFN.GetRefNote(p_Req.Commentar, '/');
      RSI_GT.SetParmByName('RGDVRQ_TRADERCODE', v_TraderCode);

      --проверка ошибок после добавления параметров
      IF RSI_GT.GetLastError <> CHR(1) THEN
        v_stat := 1;
        p_ErrMes := RSI_GT.GetLastError;
      END IF;
    END IF;

    RETURN v_stat;
  END;
  
  --Обработка записей по данным буферной таблицы FOORDLOG
  PROCEDURE ProcessReplRecByTmp_FOORDLOG(p_Prm IN OUT NOCOPY PRMREC_t)
  IS
    v_Req     DATAREQ_t;
    v_stat    NUMBER(5) := 0;
    v_ErrMes  VARCHAR2(1000) := CHR(1);
    v_WarnMes VARCHAR2(1000) := CHR(1);
    TYPE REQARR_t IS TABLE OF DATAREQ_t;
    v_ReqArr REQARR_t := REQARR_t();
  BEGIN
    --инициализация глобального лога для накопления записей для вставки
    RSB_GTLOG.Init(BATCH_SIZE, p_Prm.SeanceID, RSB_GTFN.RG_DVREQ);

    --инициализация глобального накопителя объектов ЗР для вставки
    v_stat := RSI_GT.Init(RSI_GT.DIRECT_INSERT_YES, p_Prm.SeanceID, p_Prm.Source_Code, RSI_GT.GTCODE_RECEIVE, v_ErrMes);
    IF v_stat <> 0 THEN
      RSB_GTLOG.Error('Внимание! '||v_ErrMes);
    END IF;
    IF v_stat = 0 THEN
      FOR cData IN (SELECT NVL(foordlog.T_ID_FM_FOORDLOG, 0) ID_FM_FOORDLOG,
                           NVL(foordlog.T_DIR, 0) Dir,
                           NVL(foordlog.T_N, CHR(1)) N,
                           NVL(foordlog.T_ISIN, CHR(1)) ISIN,
                           NVL(foordlog.T_DATE, RSI_GT.ZeroDate) Date1,
                           NVL(foordlog.T_TIME, RSI_GT.ZeroTime) Time1,
                           NVL(foordlog.T_CLIENT_CODE, CHR(1)) Client_Code,
                           NVL(foordlog.T_ID_DEAL, CHR(1)) ID_Deal,
                           NVL(foordlog.T_ID_ORD, CHR(1)) ID_Ord,
                           NVL(foordlog.T_AMOUNT, 0) Amount,
                           NVL(foordlog.T_PRICE, 0) Price,
                           NVL(foordlog.T_UNIQCODE, CHR(1)) UniqCode,
                           NVL(foordlog.T_OBJECTID, 0) ObjectID,
                           NVL(foordlog.T_ISREPEAT, 0) IsRepeat,
                           NVL(foordlog.T_COMMENTAR, CHR(1)) Commentar,
                           NVL(foordlog.T_ISHDAYTRADE, CHR(1)) IsHDayTrade,
                           NVL(foordlog.T_DATE, RSI_GT.ZeroDate) FileDate
                      FROM D_FOORDLOG_TMP foordlog
                   )
      LOOP
        v_ErrMes  := CHR(1);
        v_WarnMes := CHR(1);
        IF cData.ObjectID > 0 THEN
           RSB_GTLOG.Message('Ошибка при загрузке клиентского поручения № "' || cData.UniqCode || '"' || CHR(10) || CHR(10) || 'Запись репликации с действием создать для объекта ' || TO_CHAR(cData.ObjectID) || ' уже передавалась в RS-Bank.' || CHR(10) || 'Повторная репликация невозможна.', RSB_GTLOG.ISSUE_WARNING);           
        ELSIF cData.IsRepeat > 0 THEN 
           RSB_GTLOG.Message('Ошибка при загрузке клиентского поручения № "' || cData.UniqCode || '"' || CHR(10) || CHR(10) || 'Запись репликации с действием создать для данного объекта уже обработана в текущей сессии.' || CHR(10) || 'Повторная репликация невозможна.', RSB_GTLOG.ISSUE_WARNING);    
        ELSE               
           v_Req.ID_FM_FOORDLOG := cData.ID_FM_FOORDLOG;
           v_Req.Dir            := cData.Dir;
           v_Req.N              := cData.N;
           v_Req.ISIN           := cData.ISIN;
           v_Req.Date1          := cData.Date1; 
           v_Req.Time1          := cData.Time1;
           v_Req.Client_Code    := cData.Client_Code;
           v_Req.ID_Deal        := cData.ID_Deal;
           v_Req.ID_Ord         := cData.ID_Ord;
           v_Req.Amount         := cData.Amount;
           v_Req.Price          := cData.Price;    
           v_Req.UniqCode       := cData.UniqCode;
           v_Req.Commentar      := cData.Commentar;
           v_Req.IsHDayTrade    := cData.IsHDayTrade;
           v_Req.FileDate       := cData.FileDate;
           SetReqInfo(v_Req, p_Prm.isFutures);
           IF PutReqInfoPM(p_Prm, v_Req, v_ErrMes, v_WarnMes) = 0 THEN
             v_stat := RSI_GT.SaveBatch(BATCH_SIZE, v_ErrMes); --сохранить по мере накопления
           END IF;
        END IF;

        IF v_ErrMes <> CHR(1) THEN
          RSB_GTLOG.Error('Внимание! '||v_ErrMes);
        END IF;

        IF v_WarnMes <> CHR(1) THEN
          RSB_GTLOG.Warning(v_WarnMes);
        END IF;
      END LOOP;
    END IF;

    IF v_stat = 0 THEN
      --сохранение накопленных объектов ЗР
      v_stat := RSI_GT.Save(v_ErrMes);
      IF v_stat <> 0 THEN
        RSB_GTLOG.Error('Внимание! '||v_ErrMes);
      END IF;
    END IF;
    --сохранение накопленных записей лога
    RSB_GTLOG.Save; 

  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_Prm.SeanceID, RSB_GTFN.RG_DVREQ, RSB_GTLOG.ISSUE_FAULT,
                           'Внимание! Произошла непредвиденная ошибка при обработке данных в ф-и ProcessReplRecByTmp_FOORDLOG(), ' ||
                           'SeanceID=' || TO_CHAR(p_Prm.SeanceID) || ', Source_Code=' || p_Prm.Source_Code || 
                           ', SessionID=' || TO_CHAR(USERENV('sessionid')) || ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
  END;

  --Создание объектов записей репликаций по данным временной таблицы FOORDLOG
  FUNCTION CreateReplRecByTmp_FOORDLOG(p_SeanceID IN NUMBER, p_ImpDate IN DATE, p_SourceCode IN VARCHAR2, p_DVKind IN NUMBER, p_PrmStr IN VARCHAR2)
    RETURN NUMBER
  IS
    v_Prm PRMREC_t;
  BEGIN
    --инициализация используемых параметров
    SetPrmRecByStr(v_Prm, p_PrmStr);
    v_Prm.SeanceID           := p_SeanceID;
    v_Prm.Imp_Date           := p_ImpDate;
    v_Prm.Source_Code        := p_SourceCode;
    IF(p_DVKind = RSB_GTFN.DERIVATIVE_FUTURES) THEN
      v_Prm.isFutures := true;
    ELSE
      v_Prm.isFutures := false;
    END IF;
    --загрузка данных по позициям
    ProcessReplRecByTmp_FOORDLOG(v_Prm);

    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DVREQ, RSB_GTLOG.ISSUE_FAULT,
                           'Внимание! Произошла непредвиденная ошибка в ф-и CreateReplRecByTmp_FOORDLOG(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DVREQ) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;
  
 -- ФУНКЦИИ ДЛЯ ОБРАБОТКИ ИНФОРМАЦИИ ПО ПРОИЗВОДНЫМ ИНСТРУМЕНТАМ 
  
  --Очистка временной таблицы f07 и o07
  FUNCTION DelFO07(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_IsPFI IN NUMBER)
    RETURN NUMBER
  IS
    vObjectType NUMBER(5) := RSB_GTFN.RG_PFI;
  BEGIN
    IF(p_IsPFI = 0) THEN
      vObjectType := RSB_GTFN.RG_RATE;
    END IF;
    EXECUTE IMMEDIATE 'TRUNCATE TABLE D_FO07_TMP';
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, vObjectType, RSB_GTLOG.ISSUE_FAULT,
                           'Внимание! Произошла непредвиденная ошибка в ф-и DelFO07(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', ObjectKind=' || TO_CHAR(vObjectType) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;
 
  --Заполнение временной таблицы по данным f07 на основании Payments
  FUNCTION FillFromF07(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2, p_Beg_date IN DATE, p_End_date IN DATE, p_IsPFI NUMBER)
    RETURN NUMBER
  IS
    vObjectType NUMBER(5) := RSB_GTFN.RG_PFI;
  BEGIN
    IF(p_IsPFI = 0) THEN -- загрузка курсов
       vObjectType := RSB_GTFN.RG_RATE;
       EXECUTE IMMEDIATE
       'INSERT INTO D_FO07_TMP(
        T_ID_FM_FO07, 
        T_ID_MB_REQUISITES, 
        T_ID_PROCESSING_LOG, 
        T_CONTRACT,
        T_STARTDATE,
        T_LOW,
        T_HIGH,
        T_CLOSE,
        T_SETTL,
        T_TICK_PRICE,
        T_BASE_CONTR,
        T_SETTL_RUR,
        T_THEORPRICE,
        T_FI_KIND
     ) 
     (SELECT 
        NVL(F07.ID_FM_F07,0), 
        NVL(F07.ID_MB_REQUISITES,0), 
        NVL(F07.ID_PROCESSING_LOG,0),
        NVL(F07.CONTRACT,CHR(1)),
        NVL(F07.DATE1,TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')),
        NVL(F07.LOW,0),
        NVL(F07.HIGH,0),
        NVL(F07.CLOSE,0),
        NVL(F07.SETTL,0), 
        NVL(F07.TICK_PRICE,0),
        NVL(F07.BASE_FUT,CHR(1)),
        NVL(F07.SETTL_RUR,0),
        0,
        0
        FROM '||p_Synonim||'.FM_F07 F07
          WHERE F07.ID_MB_REQUISITES IN (SELECT NVL(PA.ID_MB_REQUISITES, 0) FROM '||p_Synonim||'.PROCESSING_ACTUAL PA WHERE PA.FILE_TYPE = ''f07'' AND NVL(PA.TRADE_DATE,TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')) BETWEEN :Beg_date1 AND :End_date1 ) 
                AND F07.MULTILEG = 0 
                AND NVL(F07.IDZR2,chr(1)) = chr(1) AND NVL(F07.DATE1,TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')) BETWEEN :Beg_date1 AND :End_date1)  '
      USING p_Beg_date, p_End_date, p_Beg_date, p_End_date;
      RETURN 0;
    ELSE
       EXECUTE IMMEDIATE
       'INSERT INTO D_FO07_TMP(
        T_ID_FM_FO07, 
        T_ID_MB_REQUISITES, 
        T_ID_PROCESSING_LOG, 
        T_CONTRACT,
        T_STARTDATE,
        T_EXECUTIONDATE,
        T_LOW,
        T_HIGH,
        T_CLOSE,
        T_SETTL,
        T_TICK_PRICE,
        T_TICK,
        T_BASE_CONTR,
        T_NAMECONTR,
        T_IS_PERCENT,
        T_SETTL_RUR,
        T_LOT_VOLUME,
        T_TYPE_EXEC,
        T_L_TRADEDAY,
        T_STRIKE,
        T_PUT,
        T_EVROP,
        T_THEORPRICE,
        T_FUT_TYPE,
        T_IMP_DATE,
        T_SECTION
     ) 
     (SELECT 
        NVL(F07.ID_FM_F07,0), 
        NVL(F07.ID_MB_REQUISITES,0), 
        NVL(F07.ID_PROCESSING_LOG,0),
        NVL(F07.CONTRACT,CHR(1)),
        NVL(F07.DATE1,TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')),
        NVL(F07.EXECUTION,TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')),
        NVL(F07.LOW,0),
        NVL(F07.HIGH,0),
        NVL(F07.CLOSE,0),
        NVL(F07.SETTL,0), 
        NVL(F07.TICK_PRICE,0),
        NVL(F07.TICK,0),
        NVL(F07.BASE_FUT,CHR(1)),
        NVL(F07.NAME,CHR(1)),
        NVL(F07.IS_PERCENT,0),
        NVL(F07.SETTL_RUR,0),
        NVL(F07.LOT_VOLUME,0),
        NVL(F07.TYPE_EXEC,0),
        NVL(F07.L_TRADEDAY,TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')),
        0,
        CHR(1),
        CHR(1),
        0,
        0,
        (SELECT NVL(PRC.TRADE_DATE,TO_DATE(''01.01.0001'',''DD.MM.YYYY'')) FROM '||p_Synonim||'.PROCESSING_ACTUAL PRC WHERE PRC.ID_MB_REQUISITES = F07.ID_MB_REQUISITES),
        NVL(F07.SECTION,CHR(1))
        FROM '||p_Synonim||'.FM_F07 F07
          WHERE F07.ID_MB_REQUISITES IN (SELECT NVL(PA.ID_MB_REQUISITES, 0) FROM '||p_Synonim||'.PROCESSING_ACTUAL PA WHERE PA.FILE_TYPE = ''f07'' AND NVL(PA.TRADE_DATE,TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')) BETWEEN :Beg_date1 AND :End_date1 ) 
                AND F07.MULTILEG = 0 
                AND NVL(F07.ZRID,0) = 0 ) '
      USING p_Beg_date, p_End_date;
      RETURN 0;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, vObjectType, RSB_GTLOG.ISSUE_FAULT,
                           'Внимание! Произошла непредвиденная ошибка в ф-и FillFromF07(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', ObjectKind=' || TO_CHAR(vObjectType) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END; 
  
  --Заполнение временной таблицы по данным o07 на основании Payments
  FUNCTION FillFromO07(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2, p_Beg_date IN DATE, p_End_date IN DATE, p_IsPFI NUMBER)
    RETURN NUMBER
  IS
    vObjectType NUMBER(5) := RSB_GTFN.RG_PFI;
  BEGIN 
    IF(p_IsPFI = 0) THEN
      vObjectType := RSB_GTFN.RG_RATE;
      EXECUTE IMMEDIATE
      'INSERT INTO D_FO07_TMP(
        T_ID_FM_FO07, 
        T_ID_MB_REQUISITES, 
        T_ID_PROCESSING_LOG, 
        T_CONTRACT,
        T_STARTDATE,
        T_LOW,
        T_HIGH,
        T_CLOSE,
        T_SETTL,
        T_TICK_PRICE,
        T_BASE_CONTR,
        T_SETTL_RUR,
        T_THEORPRICE,
        T_FI_KIND
     ) 
     (SELECT 
        NVL(O07.ID_FM_O07,0), 
        NVL(O07.ID_MB_REQUISITES,0), 
        NVL(O07.ID_PROCESSING_LOG,0),
        NVL(O07.CONTRACT,CHR(1)),
        NVL(O07.DATE1,TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')),
        NVL(O07.LOW,0),
        NVL(O07.HIGH,0),
        NVL(O07.CLOSE,0),
        0, 
        NVL(O07.TICK_PRICE,0),
        NVL(O07.FUT_CONTR,CHR(1)),
        0,
        NVL(O07.THEORPRICE,0),
        0
      FROM '||p_Synonim||'.FM_O07 O07
        WHERE O07.ID_MB_REQUISITES IN (SELECT NVL(PA.ID_MB_REQUISITES, 0) FROM '||p_Synonim||'.PROCESSING_ACTUAL PA WHERE PA.FILE_TYPE = ''o07'' AND NVL(PA.TRADE_DATE,TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')) BETWEEN :Beg_date1 AND :End_date1 ) 
          AND NVL(O07.IDZR2,chr(1)) = chr(1) AND NVL(O07.DATE1,TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')) BETWEEN :Beg_date1 AND :End_date1)  '
      USING p_Beg_date, p_End_date,  p_Beg_date, p_End_date;
      RETURN 0;
    ELSE
      EXECUTE IMMEDIATE
      'INSERT INTO D_FO07_TMP(
        T_ID_FM_FO07, 
        T_ID_MB_REQUISITES, 
        T_ID_PROCESSING_LOG, 
        T_CONTRACT,
        T_STARTDATE,
        T_EXECUTIONDATE,
        T_LOW,
        T_HIGH,
        T_CLOSE,
        T_SETTL,
        T_TICK_PRICE,
        T_TICK,
        T_BASE_CONTR,
        T_NAMECONTR,
        T_IS_PERCENT,
        T_SETTL_RUR,
        T_LOT_VOLUME,
        T_TYPE_EXEC,
        T_L_TRADEDAY,
        T_STRIKE,
        T_PUT,
        T_EVROP,
        T_THEORPRICE,
        T_FUT_TYPE,
        T_IMP_DATE,
        T_SECTION
     ) 
     (SELECT 
        NVL(O07.ID_FM_O07,0), 
        NVL(O07.ID_MB_REQUISITES,0), 
        NVL(O07.ID_PROCESSING_LOG,0),
        NVL(O07.CONTRACT,CHR(1)),
        NVL(O07.DATE1,TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')),
        NVL(O07.EXECUTION,TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')),
        NVL(O07.LOW,0),
        NVL(O07.HIGH,0),
        NVL(O07.CLOSE,0),
        0, 
        NVL(O07.TICK_PRICE,0),
        NVL(O07.TICK,0),
        NVL(O07.FUT_CONTR,CHR(1)),
        NVL(O07.NAME,CHR(1)),
        0,
        0,
        0,
        0,
        TO_DATE(''01.01.0001'', ''DD.MM.YYYY''),
        NVL(O07.STRIKE,0),
        NVL(O07.PUT,CHR(1)),
        NVL(O07.EVROP,CHR(1)),
        NVL(O07.THEORPRICE,0),
        TO_NUMBER(NVL(O07.FUT_TYPE,0)),
        (SELECT NVL(PRC.TRADE_DATE,TO_DATE(''01.01.0001'',''DD.MM.YYYY'')) FROM '||p_Synonim||'.PROCESSING_ACTUAL PRC WHERE PRC.ID_MB_REQUISITES = O07.ID_MB_REQUISITES),
        CHR(1)
      FROM '||p_Synonim||'.FM_O07 O07
        WHERE O07.ID_MB_REQUISITES IN (SELECT NVL(PA.ID_MB_REQUISITES, 0) FROM '||p_Synonim||'.PROCESSING_ACTUAL PA WHERE PA.FILE_TYPE = ''o07'' AND NVL(PA.TRADE_DATE,TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')) BETWEEN :Beg_date1 AND :End_date1 ) 
                AND NVL(O07.ZRID,0) = 0 ) '
    USING p_Beg_date, p_End_date;
    RETURN 0;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, vObjectType, RSB_GTLOG.ISSUE_FAULT,
                           'Внимание! Произошла непредвиденная ошибка в ф-и FillFromO07(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', ObjectKind=' || TO_CHAR(vObjectType) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END; 
  
  --Обновление ZRID и IDZR2 загруженных итогов из f07 в таблице Payments
  FUNCTION UpdateF07_PM(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2, p_isPFI IN NUMBER)
    RETURN NUMBER
  IS
    TYPE ObjID_t IS TABLE OF DGTSNCREC_DBT.T_RECORDID%TYPE; 
    TYPE PmID_t IS TABLE OF D_FO07_TMP.T_ID_FM_FO07%TYPE; 
    v_ObjID ObjID_t; 
    v_PmID  PmID_t; 
    vObjectType NUMBER(5) := RSB_GTFN.RG_PFI;
  BEGIN     
    IF(p_IsPFI = 0) THEN
      vObjectType := RSB_GTFN.RG_RATE;
      SELECT F07.T_ID_FM_FO07
        BULK COLLECT INTO v_PmID 
        FROM DGTSNCREC_DBT GTSNCREC, 
             DGTCODE_DBT GTCODE, 
             DGTOBJECT_DBT GTOBJECT, 
             D_FO07_TMP F07 
       WHERE GTSNCREC.T_SEANCEID = p_SeanceID
         AND GTCODE.T_OBJECTKIND = vObjectType
         AND GTCODE.T_OBJECTID   = GTSNCREC.T_OBJECTID 
         AND GTOBJECT.T_OBJECTID = GTSNCREC.T_OBJECTID 
         AND GTCODE.T_OBJECTCODE IN (F07.T_UNIQCODECP,F07.T_UNIQCODECPG,F07.T_UNIQCODEMINP,F07.T_UNIQCODEMAXP,F07.T_UNIQCODEMINSP,F07.T_UNIQCODETP,F07.T_UNIQCODECLP);

      IF v_PmID.COUNT <> 0 THEN
        FORALL i IN v_PmID.FIRST .. v_PmID.LAST 
          EXECUTE IMMEDIATE
            'UPDATE '||p_Synonim||'.FM_F07  
                SET IDZR2 = CHR(88) 
              WHERE ID_FM_F07 = :v_PmIDi'
          USING IN v_PmID(i);
          v_PmID.Delete;
      END IF; 
    ELSE
      SELECT F07.T_ID_FM_FO07, GTSNCREC.T_RECORDID 
        BULK COLLECT INTO v_PmID, v_ObjID 
        FROM DGTSNCREC_DBT GTSNCREC, 
             DGTCODE_DBT GTCODE, 
             DGTOBJECT_DBT GTOBJECT, 
             D_FO07_TMP F07 
       WHERE GTSNCREC.T_SEANCEID = p_SeanceID
         AND GTCODE.T_OBJECTKIND = vObjectType
         AND GTCODE.T_OBJECTID   = GTSNCREC.T_OBJECTID 
         AND GTOBJECT.T_OBJECTID = GTSNCREC.T_OBJECTID 
         AND GTCODE.T_OBJECTCODE = F07.T_UNIQCODEPFI;

      IF v_PmID.COUNT <> 0 THEN
        FORALL i IN v_PmID.FIRST .. v_PmID.LAST 
          EXECUTE IMMEDIATE
            'UPDATE '||p_Synonim||'.FM_F07  
                SET ZRID = :v_ObjIDi 
              WHERE ID_FM_F07 = :v_PmIDi'
          USING IN v_ObjID(i), IN v_PmID(i);
          v_ObjID.Delete;
          v_PmID.Delete;
      END IF; 
    END IF;
    
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, vObjectType, RSB_GTLOG.ISSUE_FAULT,
                           'Внимание! Произошла непредвиденная ошибка в ф-и UpdateF07_PM(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', ObjectKind=' || TO_CHAR(vObjectType) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;
  
  -- Обновление ZRID и IDZR2 загруженных итогов из o07 в таблице Payments
  FUNCTION UpdateO07_PM(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2, p_isPFI IN NUMBER)
    RETURN NUMBER
  IS
    TYPE ObjID_t IS TABLE OF DGTSNCREC_DBT.T_RECORDID%TYPE; 
    TYPE PmID_t IS TABLE OF D_FO07_TMP.T_ID_FM_FO07%TYPE; 
    v_ObjID ObjID_t; 
    v_PmID  PmID_t;
    vObjectType NUMBER(5) := RSB_GTFN.RG_PFI;
  BEGIN
    IF(p_IsPFI = 0) THEN
      vObjectType := RSB_GTFN.RG_RATE;
      SELECT O07.T_ID_FM_FO07
        BULK COLLECT INTO v_PmID 
        FROM DGTSNCREC_DBT GTSNCREC, 
             DGTCODE_DBT GTCODE, 
             DGTOBJECT_DBT GTOBJECT, 
             D_FO07_TMP O07 
       WHERE GTSNCREC.T_SEANCEID = p_SeanceID
         AND GTCODE.T_OBJECTKIND = vObjectType
         AND GTCODE.T_OBJECTID   = GTSNCREC.T_OBJECTID 
         AND GTOBJECT.T_OBJECTID = GTSNCREC.T_OBJECTID 
         AND GTCODE.T_OBJECTCODE IN (O07.T_UNIQCODECP,O07.T_UNIQCODECPG,O07.T_UNIQCODEMINP,O07.T_UNIQCODEMAXP,O07.T_UNIQCODEMINSP,O07.T_UNIQCODETP,O07.T_UNIQCODECLP);

      IF v_PmID.COUNT <> 0 THEN
        FORALL i IN v_PmID.FIRST .. v_PmID.LAST 
          EXECUTE IMMEDIATE
            'UPDATE '||p_Synonim||'.FM_O07  
                SET IDZR2 = CHR(88) 
              WHERE ID_FM_O07 = :v_PmIDi'
          USING IN v_PmID(i);
          v_PmID.Delete;
      END IF; 
    ELSE
      SELECT O07.T_ID_FM_FO07, GTSNCREC.T_RECORDID 
        BULK COLLECT INTO v_PmID, v_ObjID 
        FROM DGTSNCREC_DBT GTSNCREC, 
             DGTCODE_DBT GTCODE, 
             DGTOBJECT_DBT GTOBJECT, 
             D_FO07_TMP O07 
       WHERE GTSNCREC.T_SEANCEID = p_SeanceID
         AND GTCODE.T_OBJECTKIND = vObjectType
         AND GTCODE.T_OBJECTID   = GTSNCREC.T_OBJECTID 
         AND GTOBJECT.T_OBJECTID = GTSNCREC.T_OBJECTID 
         AND GTCODE.T_OBJECTCODE = O07.T_UNIQCODEPFI;

      IF v_PmID.COUNT <> 0 THEN
        FORALL i IN v_PmID.FIRST .. v_PmID.LAST 
          EXECUTE IMMEDIATE
            'UPDATE '||p_Synonim||'.FM_O07  
                SET ZRID = :v_ObjIDi 
              WHERE ID_FM_O07 = :v_PmIDi'
          USING IN v_ObjID(i), IN v_PmID(i);
          v_ObjID.Delete;
          v_PmID.Delete;
      END IF;
    END IF;
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, vObjectType, RSB_GTLOG.ISSUE_FAULT,
                           'Внимание! Произошла непредвиденная ошибка в ф-и UpdateO07_PM(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', ObjectKind=' || TO_CHAR(vObjectType) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;
    
--Генерация кодов, связанных с ПИ
  PROCEDURE SetPFICodes(p_Prm IN OUT NOCOPY PRMREC_t, p_PFI IN OUT NOCOPY DATAPFI_t )
  IS
    v_StringDate      VARCHAR(11);
  BEGIN  
    v_StringDate := CASE WHEN SUBSTR(to_char(p_PFI.Imp_Date,'dd'), 1, 1) = '0' THEN ' ' ELSE '' END || TRIM(LEADING '0' FROM to_char(p_PFI.Imp_Date, 'dd.mm.yyyy'));
    p_PFI.UniqCodePFI := p_PFI.Contract||v_StringDate;
  END;
  
  --Обновление информации по объектам шлюза и кодам ПИ f07 и o07
  FUNCTION UpdateObjCodePFIFO07(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_DVKind IN NUMBER, p_PrmStr IN VARCHAR2)
    RETURN NUMBER
  IS
    v_Prm  PRMREC_t;
    v_PFI  DATAPFI_t;
    v_stat NUMBER(5) := 0;
    TYPE PFIARR_t IS TABLE OF DATAPFI_t;
    v_PFIArr PFIARR_t := PFIARR_t();
    TYPE ObjIDPFI_t IS TABLE OF DGTCODE_DBT.T_OBJECTID%TYPE; 
    TYPE FoPFIID_t IS TABLE OF D_FO07_TMP.T_ID_FM_FO07%TYPE; 
    v_ObjIDs  ObjIDPFI_t; 
    v_FoPFIIDs FoPFIID_t;
  BEGIN 
    --инициализация используемых параметров
    SetPrmRecByStr(v_Prm, p_PrmStr);
    v_Prm.SeanceID    := p_SeanceID;
    v_Prm.Source_Code := p_SourceCode;
    IF(p_DVKind = RSB_GTFN.DERIVATIVE_FUTURES) THEN
      v_Prm.isFutures := true;
    ELSE
      v_Prm.isFutures := false;
    END IF;
       
    FOR cData IN (SELECT NVL(fo07.t_ID_FM_FO07, 0) ID_FM_FO07,
                         NVL(fo07.T_CONTRACT, CHR(1)) Contract,
                         NVL(fo07.T_IMP_DATE, RSI_GT.ZeroDate) Imp_Date
                    FROM D_FO07_TMP fo07
                 )
    LOOP   
        v_PFI.ID_FM_FO07 := cData.ID_FM_FO07;
        v_PFI.Contract   := cData.Contract;
        v_PFI.Imp_Date  := cData.Imp_Date;
        SetPFICodes(v_Prm, v_PFI); 
        v_PFIArr.Extend();
        v_PFIArr(v_PFIArr.last) := v_PFI;
    END LOOP;

    IF v_PFIArr.COUNT > 0 THEN
     FORALL indx IN v_PFIArr.FIRST .. v_PFIArr.LAST
      UPDATE D_FO07_TMP FO07
       SET FO07.T_UNIQCODEPFI     = v_PFIArr(indx).UniqCodePFI         
        WHERE FO07.T_ID_FM_FO07 = v_PFIArr(indx).ID_FM_FO07;
      v_PFIArr.Delete;    
    END IF; 

    SELECT GTCODE.T_OBJECTID, FO07.T_ID_FM_FO07
      BULK COLLECT INTO v_ObjIDs, v_FoPFIIDs
      FROM DGTCODE_DBT GTCODE, D_FO07_TMP FO07 
     WHERE GTCODE.T_APPLICATIONID IN (RSB_GTFN.GT_PAYMENTS_CSV, RSB_GTFN.GT_CSV)
       AND GTCODE.T_OBJECTKIND = RSB_GTFN.RG_PFI
       AND GTCODE.T_OBJECTCODE = FO07.T_UNIQCODEPFI;
       
     IF v_ObjIDs.COUNT <> 0 THEN 
      FORALL i IN v_ObjIDs.FIRST .. v_ObjIDs.LAST 
        UPDATE D_FO07_TMP FO07 
           SET FO07.t_ObjectIDPFI   = v_ObjIDs(i) 
         WHERE FO07.T_ID_FM_FO07 = v_FoPFIIDs(i);
     END IF;
     
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(v_Prm.SeanceID, RSB_GTFN.RG_PFI, RSB_GTLOG.ISSUE_FAULT,
                           'Внимание! Произошла непредвиденная ошибка при обработке данных в ф-и UpdateObjCodePFIFO07(), ' ||
                           'SeanceID=' || TO_CHAR(v_Prm.SeanceID) || ', Source_Code=' || v_Prm.Source_Code || 
                           ', SessionID=' || TO_CHAR(USERENV('sessionid')) || ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;
  
  --Генерация кодов, связанных с курсами ПИ
  PROCEDURE SetRateCodes(p_Prm IN OUT NOCOPY PRMREC_t, p_PFI IN OUT NOCOPY DATAPFI_t )
  IS
    v_StringDate      VARCHAR(11);
  BEGIN
    v_StringDate := CASE WHEN SUBSTR(to_char(p_PFI.StartDate,'dd'), 1, 1) = '0' THEN ' ' ELSE '' END || TRIM(LEADING '0' FROM to_char(p_PFI.StartDate, 'dd.mm.yyyy'));
    p_PFI.UniqCodeMinP  := p_PFI.Contract||'_'||TO_CHAR(RSB_GTFN.RATETYPE_MIN_PRICE)||v_StringDate;                        
    p_PFI.UniqCodeMaxP  := p_PFI.Contract||'_'||TO_CHAR(RSB_GTFN.RATETYPE_MAX_PRICE)||v_StringDate; 
    p_PFI.UniqCodeCLP   := p_PFI.Contract||'_'||TO_CHAR(RSB_GTFN.RATETYPE_CLOSE_PRICE)||v_StringDate; 
    p_PFI.UniqCodeMinSP := p_PFI.Contract||'_'||TO_CHAR(RSB_GTFN.RATETYPE_MINSTEP_PRICE)||v_StringDate;
    IF (p_Prm.isFutures) THEN
       p_PFI.UniqCodeCP    := p_PFI.Contract||'_'||TO_CHAR(RSB_GTFN.RATETYPE_CALC_PRICE)||v_StringDate;                             
       p_PFI.UniqCodeCPG   := p_PFI.Contract||'_'||TO_CHAR(RSB_GTFN.RATETYPE_CALC_PRICE_G)||v_StringDate;  
    ELSE
       p_PFI.UniqCodeTP    := p_PFI.Contract||'_'||TO_CHAR(RSB_GTFN.RATETYPE_TEORETIC_COST)||v_StringDate;  
    END IF;
  END;
   
  --Обновление информации по объектам шлюза и кодам курсов ПИ f07 и o07
  FUNCTION UpdateObjCodeRateFO07(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_DVKind IN NUMBER, p_PrmStr IN VARCHAR2)
    RETURN NUMBER
  IS 
    v_Prm  PRMREC_t;
    v_PFI  DATAPFI_t;
    v_stat NUMBER(5) := 0;
    TYPE PFIARR_t IS TABLE OF DATAPFI_t;
    v_PFIArr PFIARR_t := PFIARR_t();
    TYPE ObjID_t IS TABLE OF DGTCODE_DBT.T_OBJECTID%TYPE;
    TYPE FoPFIID_t IS TABLE OF D_FO07_TMP.T_ID_FM_FO07%TYPE; 
    TYPE FI_Kind_t IS TABLE OF DFININSTR_DBT.T_FI_KIND%TYPE;
    v_ObjIDs  ObjID_t; 
    v_FoPFIIDs FoPFIID_t;
    v_FI_Kinds FI_Kind_t;    
  BEGIN 
    --инициализация используемых параметров
    SetPrmRecByStr(v_Prm, p_PrmStr);
    v_Prm.SeanceID    := p_SeanceID;
    v_Prm.Source_Code := p_SourceCode;
    IF(p_DVKind = RSB_GTFN.DERIVATIVE_FUTURES) THEN
      v_Prm.isFutures := true;
    ELSE
      v_Prm.isFutures := false;
    END IF;
       
    FOR cData IN (SELECT NVL(f07.t_ID_FM_FO07, 0) ID_FM_FO07,
                         NVL(f07.T_CONTRACT, CHR(1)) Contract,
                         NVL(f07.T_STARTDATE, RSI_GT.ZeroDate) StartDate
                    FROM D_FO07_TMP f07
                 )
    LOOP   
        v_PFI.ID_FM_FO07 := cData.ID_FM_FO07;
        v_PFI.Contract   := cData.Contract;
        v_PFI.StartDate  := cData.StartDate;
        SetRateCodes(v_Prm, v_PFI); 
        v_PFIArr.Extend();
        v_PFIArr(v_PFIArr.last) := v_PFI;
    END LOOP;
    
    IF(v_Prm.isFutures) THEN
      IF v_PFIArr.COUNT > 0 THEN
       FORALL indx IN v_PFIArr.FIRST .. v_PFIArr.LAST
        UPDATE D_FO07_TMP FO07
         SET FO07.T_UNIQCODECP     = v_PFIArr(indx).UniqCodeCP,
             FO07.T_UNIQCODECPG    = v_PFIArr(indx).UniqCodeCPG,
             FO07.T_UNIQCODEMINP   = v_PFIArr(indx).UniqCodeMinP,
             FO07.T_UNIQCODEMAXP   = v_PFIArr(indx).UniqCodeMaxP,
             FO07.T_UNIQCODEMINSP  = v_PFIArr(indx).UniqCodeMinSP,
             FO07.T_UNIQCODECLP    = v_PFIArr(indx).UniqCodeCLP
          WHERE FO07.T_ID_FM_FO07 = v_PFIArr(indx).ID_FM_FO07;
        v_PFIArr.Delete;    
      END IF; 
    ELSE
      IF v_PFIArr.COUNT > 0 THEN
       FORALL indx IN v_PFIArr.FIRST .. v_PFIArr.LAST
        UPDATE D_FO07_TMP FO07
         SET FO07.T_UNIQCODETP     = v_PFIArr(indx).UniqCodeTP,
             FO07.T_UNIQCODEMINP   = v_PFIArr(indx).UniqCodeMinP,
             FO07.T_UNIQCODEMAXP   = v_PFIArr(indx).UniqCodeMaxP,
             FO07.T_UNIQCODEMINSP  = v_PFIArr(indx).UniqCodeMinSP,
             FO07.T_UNIQCODECLP    = v_PFIArr(indx).UniqCodeCLP
          WHERE FO07.T_ID_FM_FO07 = v_PFIArr(indx).ID_FM_FO07;
        v_PFIArr.Delete;    
      END IF; 
    END IF;
    --Курс Минимальная Цена     
    SELECT GTCODE.T_OBJECTID, FO07.T_ID_FM_FO07       
     BULK COLLECT INTO v_ObjIDs, v_FoPFIIDs
     FROM DGTCODE_DBT GTCODE, D_FO07_TMP FO07 
    WHERE GTCODE.T_APPLICATIONID IN (RSB_GTFN.GT_PAYMENTS_CSV, RSB_GTFN.GT_CSV)
      AND GTCODE.T_OBJECTKIND = RSB_GTFN.RG_RATE
      AND GTCODE.T_OBJECTCODE = FO07.T_UNIQCODEMINP;  
      
    IF v_ObjIDs.COUNT <> 0 THEN 
     FORALL i IN v_ObjIDs.FIRST .. v_ObjIDs.LAST 
       UPDATE D_FO07_TMP FO07 
          SET FO07.t_ObjectIDMinP = v_ObjIDs(i) 
        WHERE FO07.T_ID_FM_FO07   = v_FoPFIIDs(i);
        v_FoPFIIDs.delete; v_ObjIDs.delete;
    END IF;    
    --Курс Максимальная Цена     
    SELECT GTCODE.T_OBJECTID, FO07.T_ID_FM_FO07       
     BULK COLLECT INTO v_ObjIDs, v_FoPFIIDs
     FROM DGTCODE_DBT GTCODE, D_FO07_TMP FO07 
    WHERE GTCODE.T_APPLICATIONID IN (RSB_GTFN.GT_PAYMENTS_CSV, RSB_GTFN.GT_CSV)
      AND GTCODE.T_OBJECTKIND = RSB_GTFN.RG_RATE
      AND GTCODE.T_OBJECTCODE = FO07.T_UNIQCODEMAXP;  
      
    IF v_ObjIDs.COUNT <> 0 THEN 
     FORALL i IN v_ObjIDs.FIRST .. v_ObjIDs.LAST 
       UPDATE D_FO07_TMP FO07 
          SET FO07.t_ObjectIDMaxP = v_ObjIDs(i) 
        WHERE FO07.T_ID_FM_FO07   = v_FoPFIIDs(i);
        v_FoPFIIDs.delete; v_ObjIDs.delete;
    END IF;  
    
    --Курс Цена закрытия    
    SELECT GTCODE.T_OBJECTID, FO07.T_ID_FM_FO07       
     BULK COLLECT INTO v_ObjIDs, v_FoPFIIDs
     FROM DGTCODE_DBT GTCODE, D_FO07_TMP FO07 
    WHERE GTCODE.T_APPLICATIONID IN (RSB_GTFN.GT_PAYMENTS_CSV, RSB_GTFN.GT_CSV)
      AND GTCODE.T_OBJECTKIND = RSB_GTFN.RG_RATE
      AND GTCODE.T_OBJECTCODE = FO07.T_UNIQCODECLP;  
      
    IF v_ObjIDs.COUNT <> 0 THEN 
     FORALL i IN v_ObjIDs.FIRST .. v_ObjIDs.LAST 
       UPDATE D_FO07_TMP FO07 
          SET FO07.t_ObjectIDCLP = v_ObjIDs(i) 
        WHERE FO07.T_ID_FM_FO07   = v_FoPFIIDs(i);
        v_FoPFIIDs.delete; v_ObjIDs.delete;
    END IF;  
      
    --Курс Стоимость Минимального Шага    
    SELECT GTCODE.T_OBJECTID, FO07.T_ID_FM_FO07       
     BULK COLLECT INTO v_ObjIDs, v_FoPFIIDs
     FROM DGTCODE_DBT GTCODE, D_FO07_TMP FO07 
    WHERE GTCODE.T_APPLICATIONID IN (RSB_GTFN.GT_PAYMENTS_CSV, RSB_GTFN.GT_CSV)
      AND GTCODE.T_OBJECTKIND = RSB_GTFN.RG_RATE
      AND GTCODE.T_OBJECTCODE = FO07.T_UNIQCODEMINSP;  
      
    IF v_ObjIDs.COUNT <> 0 THEN 
     FORALL i IN v_ObjIDs.FIRST .. v_ObjIDs.LAST 
       UPDATE D_FO07_TMP FO07 
          SET FO07.t_ObjectIDMinSP = v_ObjIDs(i) 
        WHERE FO07.T_ID_FM_FO07    = v_FoPFIIDs(i);
        v_FoPFIIDs.delete; v_ObjIDs.delete;
    END IF;
    
    IF(v_Prm.isFutures) THEN
      --Курс Расчётная Цена 
      SELECT GTCODE.T_OBJECTID, FO07.T_ID_FM_FO07       
       BULK COLLECT INTO v_ObjIDs, v_FoPFIIDs
       FROM DGTCODE_DBT GTCODE, D_FO07_TMP FO07 
      WHERE GTCODE.T_APPLICATIONID IN (RSB_GTFN.GT_PAYMENTS_CSV, RSB_GTFN.GT_CSV)
        AND GTCODE.T_OBJECTKIND = RSB_GTFN.RG_RATE
        AND GTCODE.T_OBJECTCODE = FO07.T_UNIQCODECP;  
      
      IF v_ObjIDs.COUNT <> 0 THEN 
       FORALL i IN v_ObjIDs.FIRST .. v_ObjIDs.LAST 
         UPDATE D_FO07_TMP FO07 
            SET FO07.t_ObjectIDCP = v_ObjIDs(i) 
          WHERE FO07.T_ID_FM_FO07 = v_FoPFIIDs(i);
          v_FoPFIIDs.delete; v_ObjIDs.delete;
      END IF;      
      --Курс Расчётная Цена для главы Г
      SELECT GTCODE.T_OBJECTID, FO07.T_ID_FM_FO07       
       BULK COLLECT INTO v_ObjIDs, v_FoPFIIDs
       FROM DGTCODE_DBT GTCODE, D_FO07_TMP FO07 
      WHERE GTCODE.T_APPLICATIONID IN (RSB_GTFN.GT_PAYMENTS_CSV, RSB_GTFN.GT_CSV)
        AND GTCODE.T_OBJECTKIND = RSB_GTFN.RG_RATE
        AND GTCODE.T_OBJECTCODE = FO07.T_UNIQCODECPG;
      
      IF v_ObjIDs.COUNT <> 0 THEN 
       FORALL i IN v_ObjIDs.FIRST .. v_ObjIDs.LAST 
         UPDATE D_FO07_TMP FO07 
            SET FO07.t_ObjectIDCPG = v_ObjIDs(i) 
          WHERE FO07.T_ID_FM_FO07  = v_FoPFIIDs(i);
          v_FoPFIIDs.delete; v_ObjIDs.delete;
      END IF;   
      
      SELECT FIN.t_FI_KIND, FO07.T_ID_FM_FO07
       BULK COLLECT INTO v_FI_Kinds, v_FoPFIIDs
       FROM DFININSTR_DBT FIN, D_FO07_TMP FO07, DOBJCODE_DBT CODE 
      WHERE FIN.T_FIID = CODE.T_OBJECTID         
        AND CODE.T_OBJECTTYPE = 9 --Фин. инструмент
        AND CODE.T_CODEKIND IN (11, 13) --Коды на ММВБ и срочном рынке
        AND CODE.T_CODE = FO07.T_BASE_CONTR
        AND NVL(FO07.t_ObjectIDCPG, 0) = 0;
       
      IF v_FoPFIIDs.COUNT <> 0 THEN
        FORALL i IN v_FoPFIIDs.FIRST .. v_FoPFIIDs.LAST
          UPDATE D_FO07_TMP FO07 
             SET FO07.t_FI_KIND    = v_FI_Kinds (i)
           WHERE FO07.T_ID_FM_FO07 = v_FoPFIIDs(i);
           v_FoPFIIDs.delete; v_FI_Kinds.delete;
      END IF;     
    ELSE
      --Курс Теоретическая цена
      SELECT GTCODE.T_OBJECTID, FO07.T_ID_FM_FO07       
       BULK COLLECT INTO v_ObjIDs, v_FoPFIIDs
       FROM DGTCODE_DBT GTCODE, D_FO07_TMP FO07 
      WHERE GTCODE.T_APPLICATIONID IN (RSB_GTFN.GT_PAYMENTS_CSV, RSB_GTFN.GT_CSV)
        AND GTCODE.T_OBJECTKIND = RSB_GTFN.RG_RATE
        AND GTCODE.T_OBJECTCODE = FO07.T_UNIQCODETP;  
      
      IF v_ObjIDs.COUNT <> 0 THEN 
       FORALL i IN v_ObjIDs.FIRST .. v_ObjIDs.LAST 
         UPDATE D_FO07_TMP FO07 
            SET FO07.t_ObjectIDTP = v_ObjIDs(i) 
          WHERE FO07.T_ID_FM_FO07  = v_FoPFIIDs(i);
          v_FoPFIIDs.delete; v_ObjIDs.delete;
      END IF;  
    END IF;
     
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(v_Prm.SeanceID, RSB_GTFN.RG_RATE, RSB_GTLOG.ISSUE_FAULT,
                           'Внимание! Произошла непредвиденная ошибка при обработке данных в ф-и UpdateObjCodeRateFO07(), ' ||
                           'SeanceID=' || TO_CHAR(v_Prm.SeanceID) || ', Source_Code=' || v_Prm.Source_Code || 
                           ', SessionID=' || TO_CHAR(USERENV('sessionid')) || ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;
  
  --Обновление информации по объектам шлюза и кодам итогов f07 и o07
  FUNCTION UpdateObjCodeFO07(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_DVKind IN NUMBER, p_isPFI IN NUMBER, p_PrmStr IN VARCHAR2)
    RETURN NUMBER
  IS
  BEGIN
    IF(p_IsPFI = 0) THEN
       RETURN UpdateObjCodeRateFO07(p_SeanceID, p_SourceCode, p_DVKind, p_PrmStr);
    ELSE
       RETURN UpdateObjCodePFIFO07(p_SeanceID, p_SourceCode, p_DVKind, p_PrmStr);
    END IF;
  END;
  
  --Установка параметров ПИ, которых нет в таблице Payments
  PROCEDURE SetPFIInfo(p_PFI IN OUT NOCOPY DATAPFI_t, isFutures IN BOOLEAN)
  IS
  BEGIN
     p_PFI.ObjectIDCP := 0;
    IF(isFutures) THEN
       p_PFI.PutNum   := 0;
       p_PFI.EvrOpNum := 0;
    ELSE
       IF(p_PFI.Put = 'C') THEN
           p_PFI.PutNum := 2;       
       ELSE
           p_PFI.PutNum := 1;  
       END IF;
       IF(p_PFI.EvrOp = 'A') THEN
           p_PFI.EvrOpNum := 1;       
       ELSE
           p_PFI.EvrOpNum := 2;  
       END IF;
    END IF;
  END;

  --Заполнение и сохранение параметров курса ПИ
  FUNCTION PutPFIInfoPMRate(p_Prm IN OUT NOCOPY PRMREC_t, p_PFI IN OUT NOCOPY DATAPFI_t, p_RateKind IN NUMBER, p_ErrMes IN OUT NOCOPY VARCHAR2, p_WarnMes IN OUT NOCOPY VARCHAR2) RETURN NUMBER
  IS
    v_stat         NUMBER(5) := 0;
    v_UniqCode     VARCHAR2(40);
    v_Rate         NUMBER(32,12) := 0;
  BEGIN
  
    IF p_RateKind = RSB_GTFN.RATETYPE_MIN_PRICE THEN
       v_UniqCode := p_PFI.UniqCodeMinP;
       v_Rate := p_PFI.Low;
    ELSIF p_RateKind = RSB_GTFN.RATETYPE_MAX_PRICE THEN
       v_UniqCode := p_PFI.UniqCodeMaxP;
       v_Rate := p_PFI.vHigh;
    ELSIF p_RateKind = RSB_GTFN.RATETYPE_CLOSE_PRICE THEN
       v_UniqCode := p_PFI.UniqCodeCLP;
       v_Rate := p_PFI.vClose;
    ELSIF p_RateKind = RSB_GTFN.RATETYPE_MINSTEP_PRICE THEN
       v_UniqCode := p_PFI.UniqCodeMinSP;
       v_Rate := p_PFI.Tick_Price;
    ELSIF p_RateKind = RSB_GTFN.RATETYPE_CALC_PRICE THEN
       v_UniqCode := p_PFI.UniqCodeCP;
       v_Rate := p_PFI.Settl;
    ELSIF p_RateKind = RSB_GTFN.RATETYPE_CALC_PRICE_G THEN
       v_UniqCode := p_PFI.UniqCodeCPG;
       IF p_PFI.FI_Kind = RSI_RSB_FIInstr.FIKIND_ARTICLE THEN
          v_Rate := p_PFI.Settl;
       ELSIF p_PFI.FI_Kind = RSI_RSB_FIInstr.FIKIND_ALLFI THEN
          p_WarnMes := 'Невозможно определить вид базового актива "'|| p_PFI.Base_Contr ||'" для загрузки курса вида "Расчетная цена для главы Г';
          RETURN 0;
       ELSE
          v_Rate := p_PFI.Settl_RUR;
       END IF;
    ELSIF p_RateKind = RSB_GTFN.RATETYPE_TEORETIC_COST THEN
       v_UniqCode := p_PFI.UniqCodeTP;
       v_Rate := p_PFI.TheorPrice;
    ELSE
       RETURN 0;
    END IF;

    IF v_Rate <> 0 THEN
      v_stat := RSI_GT.InitRec(RSB_GTFN.RG_RATE,
                              'Котировка финансового инструмента '||p_PFI.Contract||' вида '||TO_CHAR(p_RateKind)||' на '||CASE WHEN SUBSTR(TO_CHAR(p_PFI.StartDate,'dd'), 1, 1) = '0' THEN ' ' ELSE '' END || TRIM(LEADING '0' FROM TO_CHAR(p_PFI.StartDate, 'dd.mm.yyyy')), --Наименование объекта
                               0, --проверять существование объекта в системе по коду
                               v_UniqCode, --код объекта
                               0, --ID клиента
                               p_ErrMes);

      IF v_stat = 0 THEN 
        RSI_GT.SetParmByName('RGFIRT_ISIN_ISO',      p_PFI.Contract);
        RSI_GT.SetParmByName('RGFIRT_ISRELATIVE',    RSB_GTFN.UNSET_CHAR);
        RSI_GT.SetParmByName('RGFIRT_FOR_CB',        RSB_GTFN.SET_CHAR);
        RSI_GT.SetParmByName('RGFIRT_RATEKIND',      p_RateKind);
        RSI_GT.SetParmByName('RGFIRT_ISDOMINANT',    RSB_GTFN.UNSET_CHAR);
        RSI_GT.SetParmByName('RGFIRT_ISINVERSE',     RSB_GTFN.UNSET_CHAR);
        RSI_GT.SetParmByName('RGFIRT_SCALE',         1);
        RSI_GT.SetParmByName('RGFIRT_RATE_ISREALID', RSB_GTFN.SET_CHAR);
        RSI_GT.SetParmByName('RGFIRT_SINCEDATE',     p_PFI.StartDate);
        RSI_GT.SetParmByName('RGFIRT_FINCODEKIND',   RSB_GTFN.FICK_USERCODE);
        RSI_GT.SetParmByName('RGFIRT_MARKETPLACE',   RSB_GTFN.MMVB_CODE);
        RSI_GT.SetParmByName('RGFIRT_PARTYCODEKIND', CNST.PTCK_MICEX);
        RSI_GT.SetParmByName('RGFIRT_RATE',          TRIM(LEADING ' ' FROM TO_CHAR(v_Rate, '9999999999999990.00000')));
        RSI_GT.SetParmByName('RGFIRT_TCC_FLAG',      -1);
        
        IF (p_RateKind = RSB_GTFN.RATETYPE_MINSTEP_PRICE OR p_RateKind = RSB_GTFN.RATETYPE_CLOSE_PRICE) THEN
           RSI_GT.SetParmByName('RGFIRT_POINT', 5);
        END IF;

        --проверка ошибок после добавления параметров
        IF RSI_GT.GetLastError <> CHR(1) THEN
          v_stat := 1;
          p_ErrMes := RSI_GT.GetLastError;
        END IF;
      END IF;
    END IF;

    RETURN v_stat;
  END;
  
  --Обработка записей по данным буферной таблицы FO07 для курсов
  PROCEDURE ProcessReplRecByTmp_RateFO07(p_Prm IN OUT NOCOPY PRMREC_t)
  IS
    v_PFI     DATAPFI_t;
    v_stat    NUMBER(5) := 0;
    v_ErrMes  VARCHAR2(1000) := CHR(1);
    v_WarnMes VARCHAR2(1000) := CHR(1);
    TYPE PFIARR_t IS TABLE OF DATAPFI_t;
    v_PFIArr PFIARR_t := PFIARR_t();
  BEGIN
    --инициализация глобального лога для накопления записей для вставки
    RSB_GTLOG.Init(BATCH_SIZE, p_Prm.SeanceID, RSB_GTFN.RG_RATE);

    --инициализация глобального накопителя объектов ЗР для вставки
    v_stat := RSI_GT.Init(RSI_GT.DIRECT_INSERT_YES, p_Prm.SeanceID, p_Prm.Source_Code, RSI_GT.GTCODE_RECEIVE, v_ErrMes);
    IF v_stat <> 0 THEN
      RSB_GTLOG.Error('Внимание! '||v_ErrMes);
    END IF;
    IF v_stat = 0 THEN
      FOR cData IN (SELECT NVL(fo07.T_ID_FM_FO07, 0) ID_FM_FO07,
                           NVL(fo07.T_CONTRACT, CHR(1)) Contract,
                           NVL(fo07.T_STARTDATE, RSI_GT.ZeroDate) StartDate,
                           NVL(fo07.T_LOW, 0) Low,
                           NVL(fo07.T_HIGH, 0) vHigh,
                           NVL(fo07.T_CLOSE, 0) vClose,
                           NVL(fo07.T_SETTL, 0) Settl,
                           NVL(fo07.T_TICK_PRICE, 0) Tick_Price,
                           NVL(fo07.T_BASE_CONTR, CHR(1)) Base_Contr,
                           NVL(fo07.T_FI_KIND, 0) FI_Kind,
                           NVL(fo07.T_SETTL_RUR, 0) Settl_RUR,
                           NVL(fo07.T_THEORPRICE, 0) TheorPrice,
                           NVL(fo07.T_UNIQCODECP, CHR(1)) UniqCodeCP,
                           NVL(fo07.T_OBJECTIDCP, 0) ObjectIDCP,
                           NVL(fo07.T_UNIQCODECPG, CHR(1)) UniqCodeCPG,
                           NVL(fo07.T_OBJECTIDCPG, 0) ObjectIDCPG,
                           NVL(fo07.T_UNIQCODEMINP, CHR(1)) UniqCodeMinP,
                           NVL(fo07.T_OBJECTIDMINP, 0) ObjectIDMinP,
                           NVL(fo07.T_UNIQCODEMAXP, CHR(1)) UniqCodeMaxP,
                           NVL(fo07.T_OBJECTIDMAXP, 0) ObjectIDMaxP,
                           NVL(fo07.T_UNIQCODETP, CHR(1)) UniqCodeTP,
                           NVL(fo07.T_OBJECTIDTP, 0) ObjectIDTP,
                           NVL(fo07.T_UNIQCODEMINSP, CHR(1)) UniqCodeMinSP,
                           NVL(fo07.T_OBJECTIDMINSP, 0) ObjectIDMinSP,
                           NVL(fo07.T_UNIQCODECLP, CHR(1)) UniqCodeCLP,
                           NVL(fo07.T_OBJECTIDCLP, 0) ObjectIDCLP
                      FROM D_FO07_TMP fo07
                   )
      LOOP
        v_ErrMes  := CHR(1);
        v_WarnMes := CHR(1);
        IF cData.ObjectIDMinP > 0 THEN
           RSB_GTLOG.Message('Котировка (min) для ПИ "'|| cData.Contract || '"' || ' за дату ' || TO_CHAR(cData.StartDate, 'DD.MM.YYYY')||' уже загружена в шлюз', RSB_GTLOG.ISSUE_WARNING);
        END IF;
        IF cData.ObjectIDMaxP > 0 THEN
           RSB_GTLOG.Message('Котировка (max) для ПИ "'|| cData.Contract || '"' || ' за дату ' || TO_CHAR(cData.StartDate, 'DD.MM.YYYY')||' уже загружена в шлюз', RSB_GTLOG.ISSUE_WARNING);
        END IF;
        IF cData.ObjectIDMinSP > 0 THEN
           RSB_GTLOG.Message('Котировка минимальной стоимости шага цены для ПИ "'|| cData.Contract || '"' || ' за дату ' || TO_CHAR(cData.StartDate, 'DD.MM.YYYY')||' уже загружена в шлюз', RSB_GTLOG.ISSUE_WARNING);
        END IF;
        IF cData.ObjectIDCP > 0 THEN
            RSB_GTLOG.Message('Котировка расчётной цены для ПИ "'|| cData.Contract || '"' || ' за дату ' || TO_CHAR(cData.StartDate, 'DD.MM.YYYY')||' уже загружена в шлюз', RSB_GTLOG.ISSUE_WARNING);
        END IF;
        IF cData.ObjectIDCPG > 0 THEN
           RSB_GTLOG.Message('Котировка расчётной цены для главы Г для ПИ "'|| cData.Contract || '"' || ' за дату ' || TO_CHAR(cData.StartDate, 'DD.MM.YYYY')||' уже загружена в шлюз', RSB_GTLOG.ISSUE_WARNING);
        END IF;
        IF cData.ObjectIDTP > 0 THEN
           RSB_GTLOG.Message('Котировка теоретической цены для ПИ "'|| cData.Contract || '"' || ' за дату ' || TO_CHAR(cData.StartDate, 'DD.MM.YYYY')||' уже загружена в шлюз', RSB_GTLOG.ISSUE_WARNING);
        END IF;
        IF cData.ObjectIDCLP > 0 THEN
           RSB_GTLOG.Message('Котировка цены закрытия для ПИ "'|| cData.Contract || '"' || ' за дату ' || TO_CHAR(cData.StartDate, 'DD.MM.YYYY')||' уже загружена в шлюз', RSB_GTLOG.ISSUE_WARNING);
        END IF;

        v_PFI.ID_FM_FO07 := cData.ID_FM_FO07;
        v_PFI.Contract      := cData.Contract;
        v_PFI.StartDate     := cData.StartDate;
        v_PFI.Low           := cData.Low; 
        v_PFI.vHigh         := cData.vHigh;
        v_PFI.vClose        := cData.vClose;
        v_PFI.Settl         := cData.Settl;
        v_PFI.Tick_Price    := cData.Tick_Price;
        v_PFI.Base_Contr    := cData.Base_Contr;    
        v_PFI.FI_Kind       := cData.FI_Kind;     
        v_PFI.Settl_RUR     := cData.Settl_RUR;
        v_PFI.TheorPrice    := cData.TheorPrice;
        v_PFI.UniqCodeCP    := cData.UniqCodeCP;
        v_PFI.UniqCodeCPG   := cData.UniqCodeCPG;
        v_PFI.UniqCodeMinP  := cData.UniqCodeMinP;
        v_PFI.UniqCodeMaxP  := cData.UniqCodeMaxP;
        v_PFI.UniqCodeTP    := cData.UniqCodeTP;
        v_PFI.UniqCodeMinSP := cData.UniqCodeMinSP;
        v_PFI.UniqCodeCLP   := cData.UniqCodeCLP;
        IF cData.ObjectIDMinP = 0 THEN -- курс "Минимальная цена"
          IF PutPFIInfoPMRate(p_Prm, v_PFI, RSB_GTFN.RATETYPE_MIN_PRICE, v_ErrMes, v_WarnMes) = 0 THEN
            v_stat := RSI_GT.SaveBatch(BATCH_SIZE, v_ErrMes); --сохранить по мере накопления
          END IF;

          IF v_ErrMes <> CHR(1) THEN
            RSB_GTLOG.Error('Внимание! '||v_ErrMes);
          END IF;

          IF v_WarnMes <> CHR(1) THEN
            RSB_GTLOG.Warning(v_WarnMes);
          END IF;

          v_ErrMes  := CHR(1);
          v_WarnMes := CHR(1);
        END IF;

        IF cData.ObjectIDMaxP = 0 THEN -- курс "Максимальная цена"
          IF PutPFIInfoPMRate(p_Prm, v_PFI, RSB_GTFN.RATETYPE_MAX_PRICE, v_ErrMes, v_WarnMes) = 0 THEN
            v_stat := RSI_GT.SaveBatch(BATCH_SIZE, v_ErrMes); --сохранить по мере накопления
          END IF;

          IF v_ErrMes <> CHR(1) THEN
            RSB_GTLOG.Error('Внимание! '||v_ErrMes);
          END IF;

          IF v_WarnMes <> CHR(1) THEN
            RSB_GTLOG.Warning(v_WarnMes);
          END IF;

          v_ErrMes  := CHR(1);
          v_WarnMes := CHR(1);
        END IF;

        IF cData.ObjectIDMinSP = 0 THEN -- курс "Стоимость минимального шага цены"
          IF PutPFIInfoPMRate(p_Prm, v_PFI, RSB_GTFN.RATETYPE_MINSTEP_PRICE, v_ErrMes, v_WarnMes) = 0 THEN
            v_stat := RSI_GT.SaveBatch(BATCH_SIZE, v_ErrMes); --сохранить по мере накопления
          END IF;

          IF v_ErrMes <> CHR(1) THEN
            RSB_GTLOG.Error('Внимание! '||v_ErrMes);
          END IF;

          IF v_WarnMes <> CHR(1) THEN
            RSB_GTLOG.Warning(v_WarnMes);
          END IF;

          v_ErrMes  := CHR(1);
          v_WarnMes := CHR(1);
        END IF;

        IF cData.ObjectIDCLP = 0 THEN -- курс "Цена закрытия"
          IF PutPFIInfoPMRate(p_Prm, v_PFI, RSB_GTFN.RATETYPE_CLOSE_PRICE, v_ErrMes, v_WarnMes) = 0 THEN
            v_stat := RSI_GT.SaveBatch(BATCH_SIZE, v_ErrMes); --сохранить по мере накопления
          END IF;


          IF v_ErrMes <> CHR(1) THEN
            RSB_GTLOG.Error('Внимание! '||v_ErrMes);
          END IF;


          IF v_WarnMes <> CHR(1) THEN
            RSB_GTLOG.Warning(v_WarnMes);
          END IF;


          v_ErrMes  := CHR(1);
          v_WarnMes := CHR(1);
        END IF;

        IF cData.ObjectIDCP = 0 AND p_Prm.isFutures = true THEN -- курс "Расчётная цена"
          IF PutPFIInfoPMRate(p_Prm, v_PFI, RSB_GTFN.RATETYPE_CALC_PRICE, v_ErrMes, v_WarnMes) = 0 THEN
            v_stat := RSI_GT.SaveBatch(BATCH_SIZE, v_ErrMes); --сохранить по мере накопления
          END IF;

          IF v_ErrMes <> CHR(1) THEN
            RSB_GTLOG.Error('Внимание! '||v_ErrMes);
          END IF;

          IF v_WarnMes <> CHR(1) THEN
            RSB_GTLOG.Warning(v_WarnMes);
          END IF;

          v_ErrMes  := CHR(1);
          v_WarnMes := CHR(1);
        END IF;

        IF cData.ObjectIDCPG = 0 AND p_Prm.isFutures = true THEN -- курс "Расчетная цена для главы Г"
          IF PutPFIInfoPMRate(p_Prm, v_PFI, RSB_GTFN.RATETYPE_CALC_PRICE_G, v_ErrMes, v_WarnMes) = 0 THEN
            v_stat := RSI_GT.SaveBatch(BATCH_SIZE, v_ErrMes); --сохранить по мере накопления
          END IF;

          IF v_ErrMes <> CHR(1) THEN
            RSB_GTLOG.Error('Внимание! '||v_ErrMes);
          END IF;

          IF v_WarnMes <> CHR(1) THEN
            RSB_GTLOG.Warning(v_WarnMes);
          END IF;

          v_ErrMes  := CHR(1);
          v_WarnMes := CHR(1);
        END IF;

        IF cData.ObjectIDTP = 0 AND p_Prm.isFutures = false THEN -- курс "Теоретическая цена"
          IF PutPFIInfoPMRate(p_Prm, v_PFI, RSB_GTFN.RATETYPE_TEORETIC_COST, v_ErrMes, v_WarnMes) = 0 THEN
            v_stat := RSI_GT.SaveBatch(BATCH_SIZE, v_ErrMes); --сохранить по мере накопления
          END IF;

          IF v_ErrMes <> CHR(1) THEN
            RSB_GTLOG.Error('Внимание! '||v_ErrMes);
          END IF;

          IF v_WarnMes <> CHR(1) THEN
            RSB_GTLOG.Warning(v_WarnMes);
          END IF;

          v_ErrMes  := CHR(1);
          v_WarnMes := CHR(1);
        END IF;       
      END LOOP;

    END IF;

    IF v_stat = 0 THEN
      --сохранение накопленных объектов ЗР
      v_stat := RSI_GT.Save(v_ErrMes);
      IF v_stat <> 0 THEN
        RSB_GTLOG.Error('Внимание! '||v_ErrMes);
      END IF;

    END IF;

    --сохранение накопленных записей лога
    RSB_GTLOG.Save; 

  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_Prm.SeanceID, RSB_GTFN.RG_RATE, RSB_GTLOG.ISSUE_FAULT,
                           'Внимание! Произошла непредвиденная ошибка при обработке данных в ф-и ProcessReplRecByTmp_RateFO07(), ' ||
                           'SeanceID=' || TO_CHAR(p_Prm.SeanceID) || ', Source_Code=' || p_Prm.Source_Code || 
                           ', SessionID=' || TO_CHAR(USERENV('sessionid')) || ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));

  END;

  
  --Заполнение и сохранение параметров ПИ
  FUNCTION PutPFIInfoPM(p_Prm IN OUT NOCOPY PRMREC_t, p_PFI IN OUT NOCOPY DATAPFI_t, isFutures IN BOOLEAN, p_ErrMes IN OUT NOCOPY VARCHAR2, p_WarnMes IN OUT NOCOPY VARCHAR2) RETURN NUMBER
  IS
    v_stat         NUMBER(5) := 0;

  BEGIN

    v_stat := RSI_GT.InitRec(RSB_GTFN.RG_PFI,
                            'ПИ с кодом № '||p_PFI.Contract, --Наименование объекта
                             0, --проверять существование объекта в системе по коду
                             p_PFI.UniqCodePFI, --код объекта
                             0, --ID клиента
                             p_ErrMes);

    IF v_stat = 0 THEN 
      RSI_GT.SetParmByName('RGDVPFI_CONTRACT', p_PFI.Contract);
      RSI_GT.SetParmByName('RGDVPFI_NAME', p_PFI.NameContr);
      RSI_GT.SetParmByName('RGDVPFI_DATE', p_PFI.StartDate);
      RSI_GT.SetParmByName('RGDVPFI_EXECUTION', p_PFI.ExecutionDate);
      RSI_GT.SetParmByName('RGDVPFI_TICK', p_PFI.Tick);
      RSI_GT.SetParmByName('RGDVPFI_TICK_PRICE', p_PFI.Tick_Price);
      IF(isFutures) THEN
         RSI_GT.SetParmByName('RGDVPFI_AVOIRKIND', RSB_GTFN.DERIVATIVE_FUTURES);
         RSI_GT.SetParmByName('RGDVPFI_BASE_FUT', p_PFI.Base_Contr);
         RSI_GT.SetParmByName('RGDVPFI_LOT_VOLUME', p_PFI.Lot_Volume);
         RSI_GT.SetParmByName('RGDVPFI_L_TRADEDAY', p_PFI.L_TradeDay);
         RSI_GT.SetParmByName('RGDVPFI_TYPE_EXEC', p_PFI.Type_Exec);
         RSI_GT.SetParmByName('RGDVPFI_IS_PERCENT', p_PFI.Is_Percent);
      ELSE
         RSI_GT.SetParmByName('RGDVPFI_AVOIRKIND', RSB_GTFN.DERIVATIVE_OPTION);
         RSI_GT.SetParmByName('RGDVPFI_FUT_CONTR', p_PFI.Base_Contr);
         RSI_GT.SetParmByName('RGDVPFI_LOT_VOLUME', 1);
         RSI_GT.SetParmByName('RGDVPFI_TYPE_EXEC', 1);
         RSI_GT.SetParmByName('RGDVPFI_OPTIONTYPE',  p_PFI.PutNum);
         RSI_GT.SetParmByName('RGDVPFI_EVROP',  p_PFI.EvrOpNum);
         RSI_GT.SetParmByName('RGDVPFI_STRIKE',  p_PFI.Strike);
         RSI_GT.SetParmByName('RGDVPFI_ISMARGINOP',  p_PFI.Fut_Type);

      END IF;

      RSI_GT.SetParmByName('RGDVPFI_SECTION', p_PFI.Section);

      --проверка ошибок после добавления параметров
      IF RSI_GT.GetLastError <> CHR(1) THEN
        v_stat := 1;
        p_ErrMes := RSI_GT.GetLastError;
      END IF;

    END IF;


    RETURN v_stat;
  END;

  
  --Обработка записей по данным буферной таблицы FO07 для ПИ
  PROCEDURE ProcessReplRecByTmp_PFIFO07(p_Prm IN OUT NOCOPY PRMREC_t)
  IS
    v_PFI     DATAPFI_t;
    v_stat    NUMBER(5) := 0;
    v_ErrMes  VARCHAR2(1000) := CHR(1);
    v_WarnMes VARCHAR2(1000) := CHR(1);
    TYPE PFIARR_t IS TABLE OF DATAPFI_t;

    v_PFIArr PFIARR_t := PFIARR_t();

  BEGIN
    --инициализация глобального лога для накопления записей для вставки
    RSB_GTLOG.Init(BATCH_SIZE, p_Prm.SeanceID, RSB_GTFN.RG_PFI);

    --инициализация глобального накопителя объектов ЗР для вставки
    v_stat := RSI_GT.Init(RSI_GT.DIRECT_INSERT_YES, p_Prm.SeanceID, p_Prm.Source_Code, RSI_GT.GTCODE_RECEIVE, v_ErrMes);
    IF v_stat <> 0 THEN
      RSB_GTLOG.Error('Внимание! '||v_ErrMes);
    END IF;

    IF v_stat = 0 THEN
      FOR cData IN (SELECT NVL(fo07.T_ID_FM_FO07, 0) ID_FM_FO07,
                           NVL(fo07.T_CONTRACT, CHR(1)) Contract,
                           NVL(fo07.T_STARTDATE, RSI_GT.ZeroDate) StartDate,
                           NVL(fo07.T_EXECUTIONDATE, RSI_GT.ZeroDate) ExecutionDate,
                           NVL(fo07.T_LOW, 0) Low,
                           NVL(fo07.T_HIGH, 0) vHigh,
                           NVL(fo07.T_SETTL, 0) Settl,
                           NVL(fo07.T_TICK_PRICE, 0) Tick_Price,
                           NVL(fo07.T_TICK, 0) Tick,
                           NVL(fo07.T_BASE_CONTR, CHR(1)) Base_Contr,
                           NVL(fo07.T_NAMECONTR, CHR(1)) NameContr,
                           NVL(fo07.T_IS_PERCENT, 0) Is_Percent,
                           NVL(fo07.T_SETTL_RUR, 0) Settl_RUR,
                           NVL(fo07.T_LOT_VOLUME, 0) Lot_Volume,
                           NVL(fo07.T_TYPE_EXEC, 0) Type_Exec,
                           NVL(fo07.T_L_TRADEDAY, RSI_GT.ZeroDate) L_TradeDay,
                           NVL(fo07.T_STRIKE, 0) Strike,
                           NVL(fo07.T_PUT, CHR(1)) Put,
                           NVL(fo07.T_EVROP, CHR(1)) EvrOp,
                           NVL(fo07.T_THEORPRICE, 0) TheorPrice,
                           NVL(fo07.T_FUT_TYPE, 0) Fut_Type,
                           NVL(fo07.T_UNIQCODEPFI, CHR(1)) UniqCodePFI,
                           NVL(fo07.T_ObjectIDPFI, 0) ObjectIDPFI,
                           NVL(fo07.T_SECTION, CHR(1)) Section
                      FROM D_FO07_TMP fo07
                   )
      LOOP
        v_ErrMes  := CHR(1);
        v_WarnMes := CHR(1);
        IF cData.ObjectIDPFI > 0 THEN
           RSB_GTLOG.Message('Спецификация ПИ с кодом "'|| cData.UniqCodePFI || '"' || ' уже загружена в шлюз'|| CHR(10)|| CHR(10)||'Запись репликации с действием создать для объекта ' || TO_CHAR(cData.ObjectIDPFI) || ' уже передавалась в RS-Bank.' || CHR(10) || 'Повторная репликация невозможна.', RSB_GTLOG.ISSUE_WARNING);
        ELSE               
           v_PFI.ID_FM_FO07 := cData.ID_FM_FO07;
           v_PFI.Contract      := cData.Contract;
           v_PFI.StartDate     := cData.StartDate;
           v_PFI.ExecutionDate := cData.ExecutionDate;
           v_PFI.Low           := cData.Low; 
           v_PFI.vHigh         := cData.vHigh;
           v_PFI.Settl         := cData.Settl;
           v_PFI.Tick_Price    := cData.Tick_Price;
           v_PFI.Tick          := cData.Tick;
           v_PFI.Base_Contr    := cData.Base_Contr;    
           v_PFI.NameContr     := cData.NameContr;
           v_PFI.Is_Percent    := cData.Is_Percent;
           v_PFI.Settl_RUR     := cData.Settl_RUR;
           v_PFI.Lot_Volume    := cData.Lot_Volume;
           v_PFI.Type_Exec     := cData.Type_Exec;
           v_PFI.L_TradeDay    := cData.L_TradeDay;
           v_PFI.Strike        := cData.Strike;
           v_PFI.Put           := cData.Put;
           v_PFI.EvrOp         := cData.EvrOp;
           v_PFI.TheorPrice    := cData.TheorPrice;
           v_PFI.Fut_Type      := cData.Fut_Type;
           v_PFI.UniqCodePFI   := cData.UniqCodePFI;
           v_PFI.Section       := cData.Section;
           SetPFIInfo(v_PFI, p_Prm.isFutures);
           IF PutPFIInfoPM(p_Prm, v_PFI, p_Prm.isFutures, v_ErrMes, v_WarnMes) = 0 THEN
             v_stat := RSI_GT.SaveBatch(BATCH_SIZE, v_ErrMes); --сохранить по мере накопления
           END IF;

        END IF;


        IF v_ErrMes <> CHR(1) THEN
          RSB_GTLOG.Error('Внимание! '||v_ErrMes);
        END IF;


        IF v_WarnMes <> CHR(1) THEN
          RSB_GTLOG.Warning(v_WarnMes);
        END IF;

      END LOOP;

    END IF;


    IF v_stat = 0 THEN
      --сохранение накопленных объектов ЗР
      v_stat := RSI_GT.Save(v_ErrMes);
      IF v_stat <> 0 THEN
        RSB_GTLOG.Error('Внимание! '||v_ErrMes);
      END IF;

    END IF;

    --сохранение накопленных записей лога
    RSB_GTLOG.Save; 

  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_Prm.SeanceID, RSB_GTFN.RG_PFI, RSB_GTLOG.ISSUE_FAULT,
                           'Внимание! Произошла непредвиденная ошибка при обработке данных в ф-и ProcessReplRecByTmp_PFIFO07(), ' ||
                           'SeanceID=' || TO_CHAR(p_Prm.SeanceID) || ', Source_Code=' || p_Prm.Source_Code || 
                           ', SessionID=' || TO_CHAR(USERENV('sessionid')) || ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));

  END;


  --Создание объектов записей репликаций по данным временной таблицы FO07
  FUNCTION CreateReplRecByTmp_FO07(p_SeanceID IN NUMBER, p_ImpDate IN DATE, p_SourceCode IN VARCHAR2, p_DVKind IN NUMBER, p_isPFI IN NUMBER, p_PrmStr IN VARCHAR2)
    RETURN NUMBER
  IS
    v_Prm PRMREC_t;
    vObjectType NUMBER(5) := RSB_GTFN.RG_PFI;

  BEGIN
    IF(p_IsPFI = 0) THEN
      vObjectType := RSB_GTFN.RG_RATE;
    END IF;    
    --инициализация используемых параметров
    SetPrmRecByStr(v_Prm, p_PrmStr);
    v_Prm.SeanceID           := p_SeanceID;
    v_Prm.Imp_Date           := p_ImpDate;
    v_Prm.Source_Code        := p_SourceCode;
    IF(p_DVKind = RSB_GTFN.DERIVATIVE_FUTURES) THEN
      v_Prm.isFutures := true;
    ELSE
      v_Prm.isFutures := false;

    END IF;

    IF(p_isPFI = 0) THEN
      --загрузка данных по курсам ПИ
      ProcessReplRecByTmp_RateFO07(v_Prm);
    ELSE
      --загрузка данных по производным инструментам
      ProcessReplRecByTmp_PFIFO07(v_Prm);

    END IF;


    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, vObjectType, RSB_GTLOG.ISSUE_FAULT,
                           'Внимание! Произошла непредвиденная ошибка в ф-и CreateReplRecByTmp_FO07(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', ObjectKind=' || TO_CHAR(vObjectType) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;

  END;


-- ФУНКЦИИ ДЛЯ ОБРАБОТКИ ИНФОРМАЦИИ ПО КОМИССИЯМ И СБОРАМ

  --Очистка временной таблицы комиссий и сборов PAY
  FUNCTION DelPAY(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER
  IS
  BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE D_PAY_TMP';
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DEFCOMM, RSB_GTLOG.ISSUE_FAULT,
                           'Внимание! Произошла непредвиденная ошибка в ф-и DelPAY(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DEFCOMM) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;

  END;

  
  --Заполнение временной таблицы по данным PAY на основании Payments
  FUNCTION FillFromPAY(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2, p_Beg_date IN DATE, p_End_date IN DATE)
    RETURN NUMBER
  IS
  BEGIN 
    EXECUTE IMMEDIATE
      'INSERT INTO D_PAY_TMP(
        T_ID_FM_PAY,
        T_ID_MB_REQUISITES,
        T_DATE,
        T_KOD,
        T_ACCOUNT,
        T_TYPE,       
        T_ID_PAY,
        T_TYPE_PAY,
        T_PAY,
        T_DU,        
        T_PURPOSE,
        T_UNIQCOMCODE,
        T_BELONG,        
        T_TYPEOBJ,
        T_TYPERQ,        
        T_OBJECTID,
        T_ISREPEAT
     ) 
     (SELECT 
        NVL(PAY.ID_FM_PAY, 0), 
        NVL(PAY.ID_MB_REQUISITES, 0), 
        NVL(PAY.DATE_CLEARING, TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')),
        NVL(PAY.KOD, CHR(1)),
        NVL(PAY.ACCOUNT, CHR(1)),
        NVL(PAY.TYPE, CHR(1)),       
        NVL(PAY.ID_PAY, 0), 
        NVL(PAY.TYPE_PAY, 0), 
        NVL(PAY.PAY, 0), 
        NVL(PAY.DU, 0),         
        CONVERT(NVL(PAY.PURPOSE, CHR(1)), ''RU8PC866''),
        TO_CHAR(NVL(PAY.DATE_CLEARING, TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')), ''ddmmyyyy'') || TO_CHAR(NVL(PAY.ID_PAY, 0)), 
        0,
        T_KINDOBJ,
        T_KINDRQ,
        0,
        0
      FROM '||p_Synonim||'.FM_PAY PAY, DDL_IMPORTOPER_DBT OPER
     WHERE PAY.ID_MB_REQUISITES IN (SELECT NVL(PA.ID_MB_REQUISITES, 0) FROM '||p_Synonim||'.PROCESSING_ACTUAL PA WHERE PA.FILE_TYPE = ''pay'' AND NVL(PA.TRADE_DATE, TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')) BETWEEN :Beg_date AND :End_date)
       AND NVL(PAY.ACCOUNT, CHR(1)) = '''||CSV_ACCOUNT_OUR_BANK||'''
       AND NVL(PAY.DU, 0) = 0
       AND NVL(PAY.TYPE, CHR(1)) = ''MN''
       AND NVL(PAY.DATE_CLEARING, TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')) >= :Beg_date1
       AND NVL(PAY.DATE_CLEARING, TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')) <= :End_date1
       AND NVL(PAY.PAY, 0) <> 0
       AND NVL(PAY.ZRID, 0) = 0 
       AND OPER.T_NUMBER = NVL(PAY.TYPE_PAY, 0)
       AND OPER.T_KINDOBJ >= 0
     ) '
    USING p_Beg_date, p_End_date, p_Beg_date, p_End_date;
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DEFCOMM, RSB_GTLOG.ISSUE_FAULT,
                           'Внимание! Произошла непредвиденная ошибка в ф-и FillFromPAY(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DEFCOMM) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;

  END; 
  
  --Обновление ID уже существующих объектов во временной таблице PAY
  FUNCTION UpdateObjPAY(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER
  IS
    TYPE ObjID_t IS TABLE OF DGTCODE_DBT.T_OBJECTID%TYPE;
    TYPE PayID_t IS TABLE OF D_PAY_TMP.T_ID_FM_PAY%TYPE;

    v_ObjIDs ObjID_t;
    v_PayIDs  PayID_t;

  BEGIN
    SELECT GTCODE.T_OBJECTID, PAY.T_ID_FM_PAY
      BULK COLLECT INTO v_ObjIDs, v_PayIDs
      FROM DGTCODE_DBT GTCODE, D_PAY_TMP PAY
     WHERE GTCODE.T_APPLICATIONID IN (RSB_GTFN.GT_PAYMENTS_CSV, RSB_GTFN.GT_CSV)
       AND ((GTCODE.T_OBJECTKIND = RSB_GTFN.RG_DEFCOMM AND PAY.T_TYPEOBJ = 0) OR 
            (GTCODE.T_OBJECTKIND = RSB_GTFN.RG_MONEYMOTION AND PAY.T_TYPEOBJ <> 0)
           )
       AND GTCODE.T_OBJECTCODE = PAY.T_UNIQCOMCODE;

       
    IF v_PayIDs.COUNT <> 0 THEN
      FORALL i IN v_PayIDs.FIRST .. v_PayIDs.LAST
        UPDATE D_PAY_TMP PAY
           SET PAY.t_OBJECTID = v_ObjIDs (i)
         WHERE PAY.T_ID_FM_PAY = v_PayIDs (i);

        v_ObjIDs.Delete;
        v_PayIDs.Delete;
     END IF;

     
    --Проверим повторения внутри пачки
    UPDATE D_PAY_TMP PAY
       SET PAY.T_ISREPEAT = 1
     WHERE EXISTS (SELECT 1
                     FROM D_PAY_TMP PAY1
                    WHERE PAY.T_UNIQCOMCODE = PAY1.T_UNIQCOMCODE
                      AND PAY.T_TYPEOBJ = PAY1.T_TYPEOBJ
                      AND PAY.T_ID_FM_PAY > PAY1.T_ID_FM_PAY);    
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DEFCOMM, RSB_GTLOG.ISSUE_FAULT,
                           'Внимание! Произошла непредвиденная ошибка в ф-и UpdateObjPAY(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DEFCOMM) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;

  END;

  
  --Обновление ZRID загруженных комиссий и сборов из PAY в таблице Payments
  FUNCTION UpdatePAY_PM(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2)
    RETURN NUMBER
  IS
    TYPE ObjID_t IS TABLE OF DGTSNCREC_DBT.T_RECORDID%TYPE; 
    TYPE PmID_t IS TABLE OF D_PAY_TMP.T_ID_FM_PAY%TYPE; 
    v_ObjID ObjID_t; 
    v_PmID   PmID_t; 
  BEGIN
    SELECT PAY.T_ID_FM_PAY, GTSNCREC.T_RECORDID 
      BULK COLLECT INTO v_PmID, v_ObjID 
      FROM DGTSNCREC_DBT GTSNCREC, 
           DGTCODE_DBT GTCODE, 
           DGTOBJECT_DBT GTOBJECT, 
           D_PAY_TMP PAY 
     WHERE GTSNCREC.T_SEANCEID = p_SeanceID
       AND GTCODE.T_OBJECTKIND IN (RSB_GTFN.RG_DEFCOMM, RSB_GTFN.RG_MONEYMOTION)
       AND GTCODE.T_OBJECTID   = GTSNCREC.T_OBJECTID 
       AND GTOBJECT.T_OBJECTID = GTSNCREC.T_OBJECTID 
       AND GTCODE.T_OBJECTCODE = PAY.T_UNIQCOMCODE;


    IF v_PmID.COUNT <> 0 THEN
      FORALL i IN v_PmID.FIRST .. v_PmID.LAST 
        EXECUTE IMMEDIATE
          'UPDATE '||p_Synonim||'.FM_PAY 
              SET ZRID = :v_ObjIDi 
            WHERE ID_FM_PAY = :v_PmIDi'
        USING IN v_ObjID(i), IN v_PmID(i);

        v_ObjID.Delete;
        v_PmID.Delete;
    END IF; 
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DEFCOMM, RSB_GTLOG.ISSUE_FAULT,
                           'Внимание! Произошла непредвиденная ошибка в ф-и UpdatePAY_PM(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DEFCOMM) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE));
    RETURN 1;

  END;

  
  --Заполнение и сохранение параметров комиссий, сборов
  FUNCTION PutComInfoPM(p_Pay IN OUT NOCOPY D_PAY_TMP%ROWTYPE, p_ErrMes IN OUT NOCOPY VARCHAR2) RETURN NUMBER
  IS
    v_stat        NUMBER(5) := 0;
    v_Direction   NUMBER(5) := 0;
    v_comm_number number(10);
  BEGIN

    IF p_Pay.t_TypeObj = 0 THEN
      v_stat := RSI_GT.InitRec(RSB_GTFN.RG_DEFCOMM,
                               'Биржевая периодическая комиссия № ' || p_Pay.t_UniqComCode, --Наименование объекта
                               0, --проверять существование объекта в системе по коду
                               p_Pay.t_UniqComCode, --код объекта
                               0, --ID клиента
                               p_ErrMes);

      IF v_stat = 0 THEN
        v_comm_number := case when p_Pay.t_Type_Pay = 1829 then commiss_read.get_comm_number_by_code(p_code  => 'КлиентКлирИспПИ')
                                                           else commiss_read.get_comm_number_by_code(p_code  => 'МскБиржСбор')
                         end;
        RSI_GT.SetParmByName('RGCM_COMM_NUMBER', v_comm_number);
        RSI_GT.SetParmByName('RGCM_FEE_TYPE',    RSB_GTFN.SF_FEE_TYPE_ONCE);
        RSI_GT.SetParmByName('RGCM_SERV_KIND',   RSB_GTFN.PTSK_DV);
        RSI_GT.SetParmByName('RGCM_PAYER_CODE',  p_Pay.t_Kod);
        
        RSI_GT.SetParmByName('RGCM_SUM_COMM',    p_Pay.t_Pay);
        RSI_GT.SetParmByName('RGCM_DATE_FEE',    p_Pay.t_Date);
        RSI_GT.SetParmByName('RGCM_ID_PAY',      p_Pay.t_UniqComCode);       
        RSI_GT.SetParmByName('RGCM_PURPOSE',     p_Pay.t_Purpose);
        
        --проверка ошибок после добавления параметров
        IF RSI_GT.GetLastError <> CHR(1) THEN
          v_stat := 1;
          p_ErrMes := RSI_GT.GetLastError;
        END IF;

      END IF;

    ELSE
      v_stat := RSI_GT.InitRec(RSB_GTFN.RG_MONEYMOTION,
                               'Движение денежных средств № ' || p_Pay.t_UniqComCode, --Наименование объекта
                               0, --проверять существование объекта в системе по коду
                               p_Pay.t_UniqComCode, --код объекта
                               0, --ID клиента
                               p_ErrMes);

      IF v_stat = 0 THEN
        RSI_GT.SetParmByName('NAME',     p_Pay.t_UniqComCode);
        RSI_GT.SetParmByName('TYPE_PAY', p_Pay.t_Type_Pay);
        RSI_GT.SetParmByName('BELONG',   p_Pay.t_Belong);
        RSI_GT.SetParmByName('NUMBER',   p_Pay.t_UniqComCode);        
        RSI_GT.SetParmByName('DATE',     p_Pay.t_Date);
        RSI_GT.SetParmByName('CLIENT',   p_Pay.t_Kod);
        RSI_GT.SetParmByName('SUM',      p_Pay.t_Pay);       
        RSI_GT.SetParmByName('PURPOSE',  p_Pay.t_Purpose);
        RSI_GT.SetParmByName('RQ',       p_Pay.t_TypeRQ);
        
        --проверка ошибок после добавления параметров
        IF RSI_GT.GetLastError <> CHR(1) THEN
          v_stat := 1;
          p_ErrMes := RSI_GT.GetLastError;
        END IF;

      END IF; 
    END IF;   

    RETURN v_stat;
  END;


  --Создание объектов записей репликаций по данным временной таблицы PAY
  FUNCTION CreateReplRecByTmp_PAY(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER
  IS
    TYPE CDATA_t IS REF CURSOR RETURN D_PAY_TMP%ROWTYPE;
    v_cData   CDATA_t;
    v_Pay     D_PAY_TMP%ROWTYPE;
    v_stat    NUMBER(5) := 0;
    v_ErrMes  VARCHAR2(1000) := CHR(1);
    v_WarnMes VARCHAR2(1000) := CHR(1);
    v_Skip    NUMBER(5) := 0;

  BEGIN
    --инициализация глобального лога для накопления записей для вставки
    RSB_GTLOG.Init(BATCH_SIZE, p_SeanceID, RSB_GTFN.RG_DEFCOMM);

    --инициализация глобального накопителя объектов ЗР для вставки
    v_stat := RSI_GT.Init(RSI_GT.DIRECT_INSERT_YES, p_SeanceID, p_SourceCode, RSI_GT.GTCODE_RECEIVE, v_ErrMes);
    IF v_stat <> 0 THEN
      RSB_GTLOG.Error('Внимание! '||v_ErrMes);
    END IF;


    IF v_stat = 0 THEN
      OPEN v_cData FOR
           --важен порядок и наличие всех полей, чтобы сработал FETCH
           SELECT t.T_ID_FM_PAY,
                  t.T_ID_MB_REQUISITES,
                  NVL(t.T_DATE, RSI_GT.ZeroDate) T_DATE,
                  NVL(t.T_KOD, CHR(1)) T_KOD,
                  NVL(t.T_ACCOUNT, CHR(1)) T_ACCOUNT,
                  NVL(t.T_TYPE, CHR(1)) T_TYPE, 
                  NVL(t.T_ID_PAY, 0) T_ID_PAY, 
                  NVL(t.T_TYPE_PAY, 0) T_TYPE_PAY, 
                  NVL(t.T_PAY, 0) T_PAY, 
                  NVL(t.T_DU, 0) T_DU, 
                  NVL(t.T_PURPOSE, CHR(1)) T_PURPOSE, 
                  NVL(t.T_UNIQCOMCODE, CHR(1)) T_UNIQCOMCODE,
                  NVL(t.T_BELONG, 0) T_BELONG, 
                  NVL(t.T_TYPEOBJ, 0) T_TYPEOBJ, 
                  NVL(t.T_TYPERQ, 0) T_TYPERQ,                   
                  NVL(t.T_OBJECTID, 0) T_OBJECTID,
                  NVL(t.T_ISREPEAT, 0) T_ISREPEAT
            FROM D_PAY_TMP t
           ORDER BY t.T_ID_FM_PAY;
      LOOP
        FETCH v_cData INTO v_Pay;
        EXIT WHEN v_cData%NOTFOUND OR v_cData%NOTFOUND IS NULL;

        v_ErrMes  := CHR(1);
        v_WarnMes := CHR(1);
        v_Skip    := 0;

        IF v_Pay.t_ObjectID > 0 THEN
          IF v_Pay.t_TypeObj = 0 THEN
            v_WarnMes := 'Ошибка при загрузке комиссии с кодом "' || v_Pay.t_UniqComCode || '"' || CHR(10) || CHR(10) || 'Запись репликации с действием создать для объекта ' || TO_CHAR(v_Pay.t_ObjectID) || ' уже передавалась в RS-Bank.' || CHR(10) || 'Повторная репликация невозможна.';
          ELSE
            v_WarnMes := 'Ошибка при загрузке перевода с кодом "' || v_Pay.t_UniqComCode || '"' || CHR(10) || CHR(10) || 'Запись репликации с действием создать для объекта ' || TO_CHAR(v_Pay.t_ObjectID) || ' уже передавалась в RS-Bank.' || CHR(10) || 'Повторная репликация невозможна.';

          END IF;

          v_Skip := 1;
       ELSIF v_Pay.t_IsRepeat > 0 THEN 
          IF v_Pay.t_TypeObj = 0 THEN
            v_WarnMes := 'Ошибка при загрузке комиссии с кодом "' || v_Pay.t_UniqComCode || '"' || CHR(10) || CHR(10) || 'Запись репликации с действием создать для данного объекта уже обработана в текущей сессии.' || CHR(10) || 'Повторная репликация невозможна.';
          ELSE
            v_WarnMes := 'Ошибка при загрузке перевода с кодом "' || v_Pay.t_UniqComCode || '"' || CHR(10) || CHR(10) || 'Запись репликации с действием создать для данного объекта уже обработана в текущей сессии.' || CHR(10) || 'Повторная репликация невозможна.';

          END IF;

          v_Skip := 1;             
       END IF;


        IF v_ErrMes = CHR(1) AND v_Skip = 0 THEN
          IF PutComInfoPM(v_Pay, v_ErrMes) = 0 THEN
            v_stat := RSI_GT.SaveBatch(BATCH_SIZE, v_ErrMes); --сохранить по мере накопления
          END IF;


        END IF;


        IF v_ErrMes <> CHR(1) THEN
          RSB_GTLOG.Error('Внимание! '||v_ErrMes);
        END IF;


        IF v_WarnMes <> CHR(1) THEN
          RSB_GTLOG.Message(v_WarnMes, RSB_GTLOG.ISSUE_WARNING);
        END IF;

      END LOOP;

    END IF;


    IF v_stat = 0 THEN
      --сохранение накопленных объектов ЗР
      v_stat := RSI_GT.Save(v_ErrMes);
      IF v_stat <> 0 THEN
        RSB_GTLOG.Error('Внимание! '||v_ErrMes);
      END IF;

    END IF;


    --сохранение накопленных записей лога
    RSB_GTLOG.Save;

    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
      RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DEFCOMM, RSB_GTLOG.ISSUE_FAULT,
                           'Внимание! Произошла непредвиденная ошибка в ф-и CreateReplRecByTmp_PAY(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DEFCOMM) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;

  END;

  
-- ФУНКЦИИ ДЛЯ ОБРАБОТКИ ИНФОРМАЦИИ О ДОГОВОРАХ ОБСЛУЖИВАНИЯ

  --Очистка временной таблицы информации о договорах обслуживания MON
  FUNCTION DelMON(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER
  IS
  BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE D_MON_TMP';
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_MON, RSB_GTLOG.ISSUE_FAULT,
                           'Внимание! Произошла непредвиденная ошибка в ф-и DelMON(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_MON) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;

  END;

  
  --Заполнение временной таблицы по данным MON на основании Payments
  FUNCTION FillFromMON(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2, p_Beg_date IN DATE, p_End_date IN DATE)
    RETURN NUMBER
  IS
  BEGIN 
    EXECUTE IMMEDIATE
      'INSERT INTO D_MON_TMP(
        T_ID_FM_MON,
        T_ID_MB_REQUISITES,
        T_DATE,
        T_KOD,
        T_ACCOUNT,
        T_TYPE,
        T_GO,
        T_UNIQMONCODE,
        T_OBJECTID,
        T_ISREPEAT
     ) 
     (SELECT 
        NVL(MON.ID_FM_MON, 0), 
        NVL(MON.ID_MB_REQUISITES, 0), 
        NVL(MON.DATE_CLEARING, TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')),
        NVL(MON.KOD, CHR(1)),
        NVL(MON.ACCOUNT, CHR(1)),
        NVL(MON.TYPE, CHR(1)), 
        NVL(MON.GO, 0), 
        NVL(MON.KOD, CHR(1)) || DECODE(SUBSTR(to_char(NVL(PA.TRADE_DATE, TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')),''dd''), 1, 1), ''0'', '' '', '''') || TRIM(LEADING ''0'' FROM to_char(NVL(PA.TRADE_DATE, TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')), ''dd.mm.yyyy'')), 
        0,
        0
      FROM '||p_Synonim||'.FM_MON MON, '||p_Synonim||'.PROCESSING_ACTUAL PA
     WHERE MON.ID_MB_REQUISITES = PA.ID_MB_REQUISITES 
       AND PA.FILE_TYPE = ''mon'' 
       AND NVL(PA.TRADE_DATE, TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')) BETWEEN :Beg_date AND :End_date 
       AND NVL(MON.ACCOUNT, CHR(1)) = '''||CSV_ACCOUNT_OUR_BANK||'''
       AND NVL(MON.TYPE, CHR(1)) = ''MN''
       AND NVL(MON.ZRID, 0) = 0 
     ) '
    USING p_Beg_date, p_End_date;
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_MON, RSB_GTLOG.ISSUE_FAULT,
                           'Внимание! Произошла непредвиденная ошибка в ф-и FillFromMON(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_MON) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;

  END; 
  
  --Обновление ID уже существующих объектов во временной таблице MON
  FUNCTION UpdateObjMON(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER
  IS
    TYPE ObjID_t IS TABLE OF DGTCODE_DBT.T_OBJECTID%TYPE;
    TYPE MonID_t IS TABLE OF D_MON_TMP.T_ID_FM_MON%TYPE;

    v_ObjIDs ObjID_t;
    v_MonIDs MonID_t;

  BEGIN
    SELECT GTCODE.T_OBJECTID, MON.T_ID_FM_MON
      BULK COLLECT INTO v_ObjIDs, v_MonIDs
      FROM DGTCODE_DBT GTCODE, D_MON_TMP MON
     WHERE GTCODE.T_APPLICATIONID IN (RSB_GTFN.GT_PAYMENTS_CSV, RSB_GTFN.GT_CSV)
       AND GTCODE.T_OBJECTKIND = RSB_GTFN.RG_MON
       AND GTCODE.T_OBJECTCODE = MON.T_UNIQMONCODE;

       
    IF v_MonIDs.COUNT <> 0 THEN
      FORALL i IN v_MonIDs.FIRST .. v_MonIDs.LAST
        UPDATE D_MON_TMP MON
           SET MON.t_ObjectID = v_ObjIDs (i)
         WHERE MON.T_ID_FM_MON = v_MonIDs (i);

        v_ObjIDs.Delete;
        v_MonIDs.Delete;
     END IF;

     
    --Проверим повторения внутри пачки
    UPDATE D_MON_TMP MON
       SET MON.T_ISREPEAT = 1
     WHERE EXISTS (SELECT 1
                     FROM D_MON_TMP MON1
                    WHERE MON.T_UNIQMONCODE = MON1.T_UNIQMONCODE
                      AND MON.T_ID_FM_MON > MON1.T_ID_FM_MON); 
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_MON, RSB_GTLOG.ISSUE_FAULT,
                           'Внимание! Произошла непредвиденная ошибка в ф-и UpdateObjMON(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_MON) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;

  END;

  
  --Обновление ZRID загруженной информации о договорах обслуживания из MON в таблице Payments
  FUNCTION UpdateMON_PM(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2)
    RETURN NUMBER
  IS
    TYPE ObjID_t IS TABLE OF DGTSNCREC_DBT.T_RECORDID%TYPE; 
    TYPE PmID_t IS TABLE OF D_MON_TMP.T_ID_FM_MON%TYPE; 
    v_ObjID ObjID_t; 
    v_PmID  PmID_t; 
  BEGIN
    SELECT MON.T_ID_FM_MON, GTSNCREC.T_RECORDID 
      BULK COLLECT INTO v_PmID, v_ObjID 
      FROM DGTSNCREC_DBT GTSNCREC, 
           DGTCODE_DBT GTCODE, 
           DGTOBJECT_DBT GTOBJECT, 
           D_MON_TMP MON 
     WHERE GTSNCREC.T_SEANCEID = p_SeanceID
       AND GTCODE.T_OBJECTKIND = RSB_GTFN.RG_MON
       AND GTCODE.T_OBJECTID   = GTSNCREC.T_OBJECTID 
       AND GTOBJECT.T_OBJECTID = GTSNCREC.T_OBJECTID 
       AND GTCODE.T_OBJECTCODE = MON.T_UNIQMONCODE;


    IF v_PmID.COUNT <> 0 THEN
      FORALL i IN v_PmID.FIRST .. v_PmID.LAST 
        EXECUTE IMMEDIATE
          'UPDATE '||p_Synonim||'.FM_MON 
              SET ZRID = :v_ObjIDi 
            WHERE ID_FM_MON = :v_PmIDi'
        USING IN v_ObjID(i), IN v_PmID(i);

        v_ObjID.Delete;
        v_PmID.Delete;
    END IF; 
    
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_MON, RSB_GTLOG.ISSUE_FAULT,
                           'Внимание! Произошла непредвиденная ошибка в ф-и UpdateMON_PM(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_MON) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;

  END;

  
  --Заполнение и сохранение параметров информации по договорам обслуживания
  FUNCTION PutMonInfoPM(p_Mon IN OUT NOCOPY D_MON_TMP%ROWTYPE, p_ErrMes IN OUT NOCOPY VARCHAR2) RETURN NUMBER
  IS
    v_stat      NUMBER(5) := 0;
    v_Direction NUMBER(5) := 0;

  BEGIN

    v_stat := RSI_GT.InitRec(RSB_GTFN.RG_MON,
                             'Информация по договору обслуживания клиента с кодом: ' || p_Mon.t_Kod, --Наименование объекта
                             0, --проверять существование объекта в системе по коду
                             p_Mon.t_UniqMonCode, --код объекта
                             0, --ID клиента
                             p_ErrMes);

    IF v_stat = 0 THEN
      RSI_GT.SetParmByName('RGMON_DATE',    p_Mon.t_Date);
      RSI_GT.SetParmByName('RGMON_KOD',     p_Mon.t_Kod);
      RSI_GT.SetParmByName('RGMON_ACCOUNT', p_Mon.t_Account);
      RSI_GT.SetParmByName('RGMON_TYPE',    p_Mon.t_Type);
      RSI_GT.SetParmByName('RGMON_GO',      p_Mon.t_Go);

      --проверка ошибок после добавления параметров
      IF RSI_GT.GetLastError <> CHR(1) THEN
        v_stat := 1;
        p_ErrMes := RSI_GT.GetLastError;
      END IF;

    END IF;


    RETURN v_stat;
  END;


  --Создание объектов записей репликаций по данным временной таблицы MON
  FUNCTION CreateReplRecByTmp_MON(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER
  IS
    TYPE CDATA_t IS REF CURSOR RETURN D_MON_TMP%ROWTYPE;
    v_cData   CDATA_t;
    v_Mon     D_MON_TMP%ROWTYPE;
    v_stat    NUMBER(5) := 0;
    v_ErrMes  VARCHAR2(1000) := CHR(1);
    v_WarnMes VARCHAR2(1000) := CHR(1);
    v_Skip    NUMBER(5) := 0;

  BEGIN
    --инициализация глобального лога для накопления записей для вставки
    RSB_GTLOG.Init(BATCH_SIZE, p_SeanceID, RSB_GTFN.RG_MON);

    --инициализация глобального накопителя объектов ЗР для вставки
    v_stat := RSI_GT.Init(RSI_GT.DIRECT_INSERT_YES, p_SeanceID, p_SourceCode, RSI_GT.GTCODE_RECEIVE, v_ErrMes);
    IF v_stat <> 0 THEN
      RSB_GTLOG.Error('Внимание! '||v_ErrMes);
    END IF;


    IF v_stat = 0 THEN
      OPEN v_cData FOR
           --важен порядок и наличие всех полей, чтобы сработал FETCH
           SELECT t.T_ID_FM_MON,
                  t.T_ID_MB_REQUISITES,
                  NVL(t.T_DATE, RSI_GT.ZeroDate) T_DATE,
                  NVL(t.T_KOD, CHR(1)) T_KOD,
                  NVL(t.T_ACCOUNT, CHR(1)) T_ACCOUNT,
                  NVL(t.T_TYPE, CHR(1)) T_TYPE, 
                  NVL(t.T_GO, 0) T_GO, 
                  NVL(t.T_UNIQMONCODE, CHR(1)) T_UNIQMONCODE,
                  NVL(t.T_OBJECTID, 0) T_OBJECTID,
                  NVL(t.T_ISREPEAT, 0) T_ISREPEAT
            FROM D_MON_TMP t
           ORDER BY t.T_ID_FM_MON;
      LOOP
        FETCH v_cData INTO v_Mon;
        EXIT WHEN v_cData%NOTFOUND OR v_cData%NOTFOUND IS NULL;

        v_ErrMes  := CHR(1);
        v_WarnMes := CHR(1);
        v_Skip    := 0;

        IF v_Mon.t_ObjectID > 0THEN
          v_WarnMes := 'Ошибка при загрузке информации о договоре обслуживания клиента с кодом "' || v_Mon.t_Kod || '"' || CHR(10) || CHR(10) || 'Запись репликации с действием создать для объекта ' || TO_CHAR(v_Mon.t_ObjectID) || ' уже передавалась в RS-Bank.' || CHR(10) || 'Повторная репликация невозможна.';
          v_Skip := 1;   
        ELSIF v_Mon.t_IsRepeat > 0 THEN 
          v_WarnMes := 'Ошибка при загрузке информации о договоре обслуживания клиента с кодом "' || v_Mon.t_Kod || '"' || CHR(10) || CHR(10) || 'Запись репликации с действием создать для данного объекта уже обработана в текущей сессии.' || CHR(10) || 'Повторная репликация невозможна.';
          v_Skip := 1;             
        END IF;


        IF v_ErrMes = CHR(1) AND v_Skip = 0 THEN
          IF PutMonInfoPM(v_Mon, v_ErrMes) = 0 THEN
            v_stat := RSI_GT.SaveBatch(BATCH_SIZE, v_ErrMes); --сохранить по мере накопления
          END IF;

        END IF;


        IF v_ErrMes <> CHR(1) THEN
          RSB_GTLOG.Error('Внимание! '||v_ErrMes);
        END IF;


        IF v_WarnMes <> CHR(1) THEN
          RSB_GTLOG.Message(v_WarnMes, RSB_GTLOG.ISSUE_WARNING);
        END IF;

      END LOOP;

    END IF;


    IF v_stat = 0 THEN
      --сохранение накопленных объектов ЗР
      v_stat := RSI_GT.Save(v_ErrMes);
      IF v_stat <> 0 THEN
        RSB_GTLOG.Error('Внимание! '||v_ErrMes);
      END IF;

    END IF;


    --сохранение накопленных записей лога
    RSB_GTLOG.Save;

    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
      RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_MON, RSB_GTLOG.ISSUE_FAULT,
                           'Внимание! Произошла непредвиденная ошибка в ф-и CreateReplRecByTmp_MON(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_MON) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;

  END;


END RSB_GTIM_CSV;
/