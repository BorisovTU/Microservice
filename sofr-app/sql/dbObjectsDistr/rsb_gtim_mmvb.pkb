CREATE OR REPLACE PACKAGE BODY RSB_GTIM_MMVB
IS
  BATCH_SIZE CONSTANT NUMBER(5) := 100; --Размер пачки для сохранения объектов

  DOCKIND_REQ_CLIENT_DEAL CONSTANT NUMBER(5) := 350; --Заявка клиента на сделку

  --Структура используемых параметров
  TYPE PRMREC_t IS RECORD (deals_bo           NUMBER(5), --Признак импорта сделок БО ЦБ
                           deals_seb          NUMBER(5), --Признак импорта сделок СО СЭБ
                           CPFirmIdConst      DOBJCODE_DBT.T_CODE%TYPE, --Код ЦК для ММВБ
                           SeanceID           DGTSEANCE_DBT.T_SEANCEID%TYPE, --Идентификатор сеанса
                           imp_date           DATE, --Дата импорта
                           Source_Code        DGTAPP_DBT.T_CODE%TYPE, --Код источника
                           IMPORT_SETTLETIME  NUMBER(5), --Значение настройки 'SECUR\IMPORT_SETTLETIME'
                           FileImport         NUMBER(5), --Тип обрабатываемого файла
                           IsRSHB             NUMBER(5), --Признак реализации для РСХБ
                           MMVB_Code          DOBJCODE_DBT.T_CODE%TYPE, --Код ММВБ
                           FORM_ORDERS_IMPORT NUMBER(5) --Значение настройки 'SECUR\ФОРМИР. ПОРУЧЕНИЯ ПРИ ИМПОРТЕ'
                          );

  --Общая структура по данным сделок/клиринга
  TYPE DATADEALS_t IS RECORD (AccInt          NUMBER(32, 12),
                              AccInt2         NUMBER(32, 12),
                              Amount          NUMBER(32, 12),
                              Amount2         NUMBER(32, 12),
                              BoardId         VARCHAR2(4),
                              BoardType       VARCHAR2(30),
                              BrokerRef       VARCHAR2(20),
                              BuySell         VARCHAR2(1),
                              CalcItogDate_   DATE,
                              CalendId        NUMBER(10),
                              ClientCode      VARCHAR2(12),
                              ClientContrID   NUMBER(10),
                              ClientID        NUMBER(10),
                              ClrComm         NUMBER(32, 12),
                              CPFirmId        VARCHAR2(12),
                              CPFirmId_ID     NUMBER(10),
                              CurrencyId      VARCHAR2(4),
                              CurrencyId_FIID NUMBER(10),
                              DealTypeID      NUMBER(5),
                              Decimals        NUMBER(5),
                              DOC_NO          VARCHAR2(32),
                              ExhComm         NUMBER(32, 12),
                              ExtSettleCode   VARCHAR2(12),
                              GateDealType    VARCHAR2(4),
                              InfType         NUMBER(5),
                              IsCliring       NUMBER(5),
                              IsRepo          NUMBER(5),
                              IsSeb           NUMBER(5),
                              IsRSHB          NUMBER(5),
                              FIID            NUMBER(10),
                              MarketReportID  NUMBER(10),
                              MarketSchemeID  NUMBER(10),
                              MatchRef        VARCHAR2(10),
                              NDay1_          NUMBER(10),
                              NDay2_          NUMBER(10),
                              OrderNo         VARCHAR2(20),
                              OrdTypeCode     VARCHAR2(4),
                              FaceValue       NUMBER(32, 12),
                              PaymDate1_      DATE,
                              PaymDate2_      DATE,
                              Portfolio_      NUMBER(5),
                              Price           NUMBER(32,12),
                              Price2          NUMBER(32,12),
                              PriceType       VARCHAR2(4),
                              Quantity        NUMBER(10),
                              RecNo           NUMBER(10), --тип?
                              RepoPart        NUMBER(5),
                              RepoPeriod      NUMBER(5),
                              RepoRate        NUMBER(32, 12),
                              RepoValue       NUMBER(32, 12),
                              SecurityId      VARCHAR2(12),
                              SecurityType    VARCHAR2(4),
                              Session         NUMBER(5),
                              SettleCode      VARCHAR2(12),
                              SettleDate      DATE,
                              SettleTime      DATE,
                              TradeDate       DATE,
                              TradeSessionDate DATE,
                              TradeNo         VARCHAR2(20),
                              TradeTime       DATE,
                              TradeType       VARCHAR2(1),
                              TrdAccId        VARCHAR2(12),
                              UINNumber       VARCHAR2(50),
                              UnderWr         NUMBER(5),
                              UnderWr_Pokupka NUMBER(5),
                              Value_          NUMBER(32,12),
                              Value2_         NUMBER(32,12),
                              vClientCode     VARCHAR2(12),
                              WorkSettleDate_ DATE,
                              IsOTC           NUMBER(5),
                              IsFloating      NUMBER(5),
                              BenchMark    NUMBER(10),
                              SpredRate  NUMBER(32, 12),
                              IndRate  NUMBER(32, 12)
                             );
                             
   FUNCTION CheckPKUPortfolio (v_FIID  IN NUMBER, v_Date  IN DATE)
   RETURN NUMBER
  IS
    v_cnt NUMBER;
  BEGIN
    SELECT COUNT(1) INTO v_cnt 
      FROM DPMWRTSUM_DBT
    WHERE T_FIID = v_FIID AND T_PORTFOLIO = 3 and T_DATE <= v_Date AND T_AMOUNT > 0;
 
    RETURN v_cnt;
  END;
  
  FUNCTION GetPortfolio (v_FIID  IN NUMBER, v_Date  IN DATE)
    RETURN NUMBER
  IS
    v_FIKind          NUMBER;
    v_AvoirKind      NUMBER;
    v_Portfolio       NUMBER;
    v_PortfolioReg  NUMBER := Rsb_Common.GetRegIntValue('SECUR\МСФО\ПОРТФЕЛЬ ПО УМОЛЧАНИЮ', 0);
  BEGIN
     BEGIN
       SELECT T_FI_KIND, T_AVOIRKIND INTO v_FIKind, v_AvoirKind FROM DFININSTR_DBT WHERE T_FIID = v_FIID;
     EXCEPTION
      WHEN NO_DATA_FOUND THEN v_FIKind := -1; v_AvoirKind := -1;
     END;
     
     IF v_FIKind != -1 AND v_AvoirKind != -1 THEN
        IF RSB_FIInstr.FI_AvrKindsGetRoot( v_FIKind,v_AvoirKind ) = RSI_RSB_FIInstr.AVOIRKIND_KSU THEN
          v_Portfolio := 12;
        ELSIF CheckPKUPortfolio(v_FIID, v_Date) > 0 THEN
          v_Portfolio := 3;
        ELSIF RSB_SECUR.GetMainObjAttr (RSB_SECUR.OBJTYPE_AVOIRISS, LPAD(v_FIID, 10, '0'), 61, V_Date) > 0 THEN
          v_Portfolio := RSB_SECUR.GetMainObjAttr (RSB_SECUR.OBJTYPE_AVOIRISS, LPAD(v_FIID, 10, '0'), 61, V_Date);
        ELSE
          v_Portfolio := CASE WHEN v_PortfolioReg = 0 THEN 1 ELSE 2 END;
        END IF; 
     ELSE
       v_Portfolio := RSB_PMWRTOFF.KINDPORT_UNDEF;
     END IF;
     
     RETURN  v_Portfolio;
  END;

  FUNCTION GetImportSettleTime RETURN NUMBER
  IS
  BEGIN
    RETURN CASE WHEN Rsb_Common.GetRegBoolValue('SECUR\IMPORT_SETTLETIME', 0) = TRUE THEN 1 ELSE 0 END;
  EXCEPTION WHEN OTHERS THEN
    RETURN 0;
  END;

  FUNCTION GetFormOrdersImport RETURN NUMBER
  IS
  BEGIN
    RETURN CASE WHEN Rsb_Common.GetRegBoolValue('SECUR\ФОРМИР. ПОРУЧЕНИЯ ПРИ ИМПОРТЕ', 0) = TRUE THEN 1 ELSE 0 END;
  EXCEPTION WHEN OTHERS THEN
    RETURN 0;
  END;

  FUNCTION GetTechBoardCodes RETURN VARCHAR
  IS
  BEGIN
    --Если переделывать получение тех бордов на настройку, то правка будет только тут
    RETURN RSB_GTFN.TECH_BOARD_CODES;
  END;

  FUNCTION CheckBoardIsTech (p_BoardId IN VARCHAR2) RETURN NUMBER
  IS
  BEGIN
    RETURN CASE WHEN TRIM(p_BoardId) IS NOT NULL AND INSTR(GetTechBoardCodes(), TRIM(p_BoardId)) <> 0 THEN 1 ELSE 0 END;
  EXCEPTION WHEN OTHERS THEN
    RETURN 0;
  END;

  --Инициализировать структуру параметров по передамаемым в строке параметрам
  PROCEDURE SetPrmRecByStr(p_Prm IN OUT NOCOPY PRMREC_t, p_PrmStr IN VARCHAR2)
  IS
    v_PrmStrMap RSB_GTFN.STRMAP_T;
    v_idx NUMBER(5);
  BEGIN
    v_PrmStrMap := RSB_GTFN.GetPrmMapByStr(p_PrmStr);

    --определить параметр deals_bo
    BEGIN
      IF v_PrmStrMap.EXISTS('deals_bo') THEN
        p_Prm.deals_bo := TO_NUMBER(v_PrmStrMap('deals_bo'));
      ELSE
        p_Prm.deals_bo := 0;
      END IF;
    EXCEPTION WHEN OTHERS THEN p_Prm.deals_bo := 0;
    END;

    --определить параметр deals_seb
    BEGIN
      IF v_PrmStrMap.EXISTS('deals_seb') THEN
        p_Prm.deals_seb := TO_NUMBER(v_PrmStrMap('deals_seb'));
      ELSE
        p_Prm.deals_seb := 0;
      END IF;
    EXCEPTION WHEN OTHERS THEN p_Prm.deals_seb := 0;
    END;

    --определить параметр CPFirmIdConst
    BEGIN
      IF v_PrmStrMap.EXISTS('CPFirmIdConst') THEN
        p_Prm.CPFirmIdConst := NVL(v_PrmStrMap('CPFirmIdConst'), CHR(1));
      ELSE
        p_Prm.CPFirmIdConst := CHR(1);
      END IF;
    EXCEPTION WHEN OTHERS THEN p_Prm.CPFirmIdConst := CHR(1);
    END;

    --определить параметр IsRSHB
    BEGIN
      IF v_PrmStrMap.EXISTS('IsRSHB') THEN
        p_Prm.IsRSHB := TO_NUMBER(v_PrmStrMap('IsRSHB'));
      ELSE
        p_Prm.IsRSHB := 0;
      END IF;
    EXCEPTION WHEN OTHERS THEN p_Prm.IsRSHB := 0;
    END;

    --определить параметр MMVB_Code
    BEGIN
      IF v_PrmStrMap.EXISTS('MMVB_Code') THEN
        p_Prm.MMVB_Code := NVL(v_PrmStrMap('MMVB_Code'), CHR(1));
      ELSE
        p_Prm.MMVB_Code := CHR(1);
      END IF;
    EXCEPTION WHEN OTHERS THEN p_Prm.MMVB_Code := CHR(1);
    END;
  END;

  --Подготовить отчеты биржи и обновить данные во временной таблице
  PROCEDURE PrepareMarketReportsEQM3T(p_Prm IN OUT NOCOPY PRMREC_t)
  IS
    TYPE DOCNOARR_t    IS TABLE OF D_EQM3T_TMP.T_DOC_NO%TYPE;
    TYPE MRKREPIDARR_t IS TABLE OF NUMBER(10);
    DocNoArr         DOCNOARR_t := DOCNOARR_t();
    MrkRepIDArr      MRKREPIDARR_t := MRKREPIDARR_t();
    v_stat           NUMBER(5);
    v_MarketReportID NUMBER(10);
    v_ErrMes         VARCHAR2(1000);
  BEGIN
    FOR cData IN (SELECT DISTINCT EQM3T.t_DOC_NO DOC_NO FROM D_EQM3T_TMP EQM3T)
    LOOP
      v_stat := RSB_GTFN.WriteMarketReport(p_Prm.SeanceID, p_Prm.Source_Code, v_MarketReportID, v_ErrMes, cData.DOC_NO, p_Prm.imp_date, p_Prm.MMVB_Code);

      IF v_ErrMes <> CHR(1) THEN
        RSB_GTLOG.DirectInsMsg(p_Prm.SeanceID, RSB_GTFN.RG_DEAL, 
                               (CASE WHEN INSTR(v_ErrMes, 'Уже существует запись в DGTCODE_DBT с такими параметрами') = 1 THEN RSB_GTLOG.ISSUE_WARNING ELSE RSB_GTLOG.ISSUE_FAULT END), 
                               'Ошибка при вставке документа-подтверждения "Отчет биржи"' || CHR(10) || v_ErrMes);
      END IF;

      IF v_MarketReportID > 0 THEN
        DocNoArr.Extend();
        DocNoArr(DocNoArr.last) := cData.DOC_NO;
        MrkRepIDArr.Extend();
        MrkRepIDArr(MrkRepIDArr.last) := v_MarketReportID;
      END IF;
    END LOOP;

    --привяжем отчеты к данным во временной таблице
    IF DocNoArr.COUNT > 0 THEN
      FORALL i IN DocNoArr.FIRST .. DocNoArr.LAST
        UPDATE D_EQM3T_TMP
           SET t_MarketReportID = MrkRepIDArr(i)
         WHERE t_DOC_NO = DocNoArr(i);

      DocNoArr.DELETE;
      MrkRepIDArr.DELETE;
    END IF;
  END;

  --Подготовить отчеты биржи и обновить данные во временной таблице
  PROCEDURE PrepareMarketReportsSEM03(p_Prm IN OUT NOCOPY PRMREC_t)
  IS
    TYPE DOCNOARR_t    IS TABLE OF D_SEM03_TMP.T_DOC_NO%TYPE;
    TYPE MRKREPIDARR_t IS TABLE OF NUMBER(10);
    DocNoArr         DOCNOARR_t := DOCNOARR_t();
    MrkRepIDArr      MRKREPIDARR_t := MRKREPIDARR_t();
    v_stat           NUMBER(5);
    v_MarketReportID NUMBER(10);
    v_ErrMes         VARCHAR2(1000);
  BEGIN
    FOR cData IN (SELECT DISTINCT SEM03.t_DOC_NO DOC_NO FROM D_SEM03_TMP SEM03)
    LOOP
      v_stat := RSB_GTFN.WriteMarketReport(p_Prm.SeanceID, p_Prm.Source_Code, v_MarketReportID, v_ErrMes, cData.DOC_NO, p_Prm.imp_date, p_Prm.MMVB_Code);

      IF v_ErrMes <> CHR(1) THEN
        RSB_GTLOG.DirectInsMsg(p_Prm.SeanceID, RSB_GTFN.RG_DEAL,
                               (CASE WHEN INSTR(v_ErrMes, 'Уже существует запись в DGTCODE_DBT с такими параметрами') = 1 THEN RSB_GTLOG.ISSUE_WARNING ELSE RSB_GTLOG.ISSUE_FAULT END), 
                               'Ошибка при вставке документа-подтверждения "Отчет биржи"' || CHR(10) || v_ErrMes);
      END IF;

      IF v_MarketReportID > 0 THEN
        DocNoArr.Extend();
        DocNoArr(DocNoArr.last) := cData.DOC_NO;
        MrkRepIDArr.Extend();
        MrkRepIDArr(MrkRepIDArr.last) := v_MarketReportID;
      END IF;
    END LOOP;

    --привяжем отчеты к данным во временной таблице
    IF DocNoArr.COUNT > 0 THEN
      FORALL i IN DocNoArr.FIRST .. DocNoArr.LAST
        UPDATE D_SEM03_TMP
           SET t_MarketReportID = MrkRepIDArr(i)
         WHERE t_DOC_NO = DocNoArr(i);

      DocNoArr.DELETE;
      MrkRepIDArr.DELETE;
    END IF;
  END;

  --Подготовить отчеты биржи и обновить данные во временной таблице
  PROCEDURE PrepareMarketReportsEQM06(p_Prm IN OUT NOCOPY PRMREC_t)
  IS
    TYPE DOCNOARR_t    IS TABLE OF D_EQM06_TMP.T_DOC_NO%TYPE;
    TYPE MRKREPIDARR_t IS TABLE OF NUMBER(10);
    DocNoArr         DOCNOARR_t := DOCNOARR_t();
    MrkRepIDArr      MRKREPIDARR_t := MRKREPIDARR_t();
    v_stat           NUMBER(5);
    v_MarketReportID NUMBER(10);
    v_ErrMes         VARCHAR2(1000);
  BEGIN
    FOR cData IN (SELECT DISTINCT EQM06.t_DOC_NO DOC_NO FROM D_EQM06_TMP EQM06 WHERE EQM06.t_BoardId = 'RPNG' AND EQM06.t_InfType = 2)
    LOOP
      v_stat := RSB_GTFN.WriteMarketReport(p_Prm.SeanceID, p_Prm.Source_Code, v_MarketReportID, v_ErrMes, cData.DOC_NO, p_Prm.imp_date, p_Prm.MMVB_Code);

      IF v_ErrMes <> CHR(1) THEN
        RSB_GTLOG.DirectInsMsg(p_Prm.SeanceID, RSB_GTFN.RG_CLEARING,
                               (CASE WHEN INSTR(v_ErrMes, 'Уже существует запись в DGTCODE_DBT с такими параметрами') = 1 THEN RSB_GTLOG.ISSUE_WARNING ELSE RSB_GTLOG.ISSUE_FAULT END), 
                               'Ошибка при вставке документа-подтверждения "Отчет биржи"' || CHR(10) || v_ErrMes);
      END IF;

      IF v_MarketReportID > 0 THEN
        DocNoArr.Extend();
        DocNoArr(DocNoArr.last) := cData.DOC_NO;
        MrkRepIDArr.Extend();
        MrkRepIDArr(MrkRepIDArr.last) := v_MarketReportID;
      END IF;
    END LOOP;

    --привяжем отчеты к данным во временной таблице
    IF DocNoArr.COUNT > 0 THEN
      FORALL i IN DocNoArr.FIRST .. DocNoArr.LAST
        UPDATE D_EQM06_TMP
           SET t_MarketReportID = MrkRepIDArr(i)
         WHERE t_DOC_NO = DocNoArr(i)
           AND t_BoardId = 'RPNG'
           AND t_InfType = 2;

      DocNoArr.DELETE;
      MrkRepIDArr.DELETE;
    END IF;
  END;

  FUNCTION IsFitDeal(p_Prm IN OUT NOCOPY PRMREC_t, p_Deal IN OUT NOCOPY DATADEALS_t) RETURN NUMBER
  IS
  BEGIN
    RETURN CASE WHEN (p_Prm.deals_bo = 1 AND p_Deal.IsSEB = 0) OR (p_Prm.deals_seb = 1 AND p_Deal.IsSEB = 1)
                THEN 1
                ELSE 0
            END;
  END;

  FUNCTION GetBenchMark(BenchMark IN VARCHAR2) RETURN NUMBER
  IS
    FIID NUMBER := -1;
  BEGIN
    BEGIN
      SELECT T_OBJECTID INTO FIID
        FROM DOBJCODE_DBT
      WHERE T_OBJECTTYPE = 9
          AND T_CODEKIND = 11
          AND T_CODE = BenchMark;
    EXCEPTION
       WHEN NO_DATA_FOUND THEN FIID := -1;
    END;
    RETURN FIID;
  END;

  --Заполнение и сохранение параметров заявки
  FUNCTION PutRequestInfo(p_Prm IN OUT NOCOPY PRMREC_t, p_Req IN OUT NOCOPY D_SEM02_TMP%ROWTYPE, p_ErrMes IN OUT NOCOPY VARCHAR2, p_WarnMes IN OUT NOCOPY VARCHAR2) RETURN NUMBER
  IS
    v_stat       NUMBER(5) := 0;
    v_TraderCode D_SEM02_TMP.t_BrokerRef%TYPE;
  BEGIN
    v_stat := RSI_GT.InitRec(RSB_GTFN.RG_REQ,
                             'Заявка на сделку №' || p_Req.t_OrderNo, --Наименование объекта
                             0, --проверять существование объекта в системе по коду
                             p_Req.t_OrderNo || '_' || TO_CHAR(p_Req.t_FileSessionNo), --код объекта
                             p_Req.t_PartyID, --ID клиента
                             p_ErrMes);

    IF v_stat = 0 THEN
      RSI_GT.SetParmByName('RGDLRQ_KIND',        DOCKIND_REQ_CLIENT_DEAL);
      RSI_GT.SetParmByName('RGDLRQ_CODE',        p_Req.t_OrderNo);
      RSI_GT.SetParmByName('RGDLRQ_CODETS',      p_Req.t_OrderNo);
      RSI_GT.SetParmByName('RGDLRQ_DATE',        p_Req.t_TradeDate);
      IF p_Req.t_TradeSessionDate <> RSI_GT.ZeroDate AND p_Req.t_TradeSessionDate <> p_Req.t_TradeDate THEN
        RSI_GT.SetParmByName('RGDLRQ_SESSIONDATE', p_Req.t_TradeSessionDate);         
      END IF;
      RSI_GT.SetParmByName('RGDLRQ_TIME',        p_Req.t_EntryTime);
      RSI_GT.SetParmByName('RGDLRQ_PARTY',       p_Prm.MMVB_Code);
      RSI_GT.SetParmByName('RGDLRQ_BUYSALE',     p_Req.t_BuySell);
      RSI_GT.SetParmByName('RGDLRQ_FIID',        p_Req.t_SecurityId); 
      RSI_GT.SetParmByName('RGDLRQ_AMOUNT',      p_Req.t_Quantity);
      RSI_GT.SetParmByName('RGDLRQ_PRICE',       p_Req.t_Price);
      RSI_GT.SetParmByName('RGDLRQ_REPORATE',    p_Req.t_RepoRate);
      RSI_GT.SetParmByName('RGDLRQ_STATUS',      p_Req.t_Status);
      RSI_GT.SetParmByName('RGDLRQ_PRICETYPE',   p_Req.t_PriceType);
      RSI_GT.SetParmByName('RGDLRQ_CURRENCYID',  p_Req.t_CurrencyId);
      RSI_GT.SetParmByName('RGDLRQ_PRICEFIID',   p_Req.t_CurrencyId); --валюта цены = валюте расчетов
      RSI_GT.SetParmByName('RGDLRQ_ORDTYPECODE', p_Req.t_OrdTypeCode);
      RSI_GT.SetParmByName('RGDLRQ_GRNDNUM',     p_Req.t_SessionNo || p_Req.t_RecNo);
      RSI_GT.SetParmByName('RGDLRQ_CLIENT',      p_Req.t_vClientCode);

      v_TraderCode := RSB_GTFN.GetRefNote(p_Req.t_BrokerRef, '/');
      IF v_TraderCode = 'spec' THEN
        v_TraderCode := CHR(1);
      END IF;
      RSI_GT.SetParmByName('RGDLRQ_TRADERCODE', v_TraderCode);

      IF p_Req.t_BrokerRef <> CHR(1) THEN
        RSI_GT.SetParmByName('RGDLRQ_BROKERREF', p_Req.t_BrokerRef);
      END IF;

      RSI_GT.SetParmByName('RGDLRQ_TRDACCID',    p_Req.t_TrdAccID);
      IF p_Req.t_UnderWr = 1 THEN
        RSI_GT.SetParmByName('RGDLRQ_UNDERWR',   RSB_GTFN.SET_CHAR);
      END IF;

      --проверка ошибок после добавления параметров
      IF RSI_GT.GetLastError <> CHR(1) THEN
        v_stat := 1;
        p_ErrMes := RSI_GT.GetLastError;
      END IF;
    END IF;

    RETURN v_stat;
  END;

  --Обработка записей по данным буферной таблицы
  PROCEDURE ProcessReplRecByTmp_SEM02(p_Prm IN OUT NOCOPY PRMREC_t)
  IS
    TYPE CDATA_t IS REF CURSOR RETURN D_SEM02_TMP%ROWTYPE;
    v_cData   CDATA_t;
    v_Prm     PRMREC_t;
    v_Req     D_SEM02_TMP%ROWTYPE;
    v_stat    NUMBER(5) := 0;
    v_ErrMes  VARCHAR2(1000) := CHR(1);
    v_WarnMes VARCHAR2(1000) := CHR(1);
  BEGIN
    --инициализация глобального лога для накопления записей для вставки
    RSB_GTLOG.Init(BATCH_SIZE, p_Prm.SeanceID, RSB_GTFN.RG_REQ);

    --инициализация глобального накопителя объектов ЗР для вставки
    v_stat := RSI_GT.Init(RSI_GT.DIRECT_INSERT_YES, p_Prm.SeanceID, p_Prm.Source_Code, RSI_GT.GTCODE_RECEIVE, v_ErrMes);
    IF v_stat <> 0 THEN
      RSB_GTLOG.Error(v_ErrMes);
    END IF;

    IF v_stat = 0 THEN
      OPEN v_cData FOR
           --важен порядок и наличие всех полей, чтобы сработал FETCH
           SELECT T_ID_MB_SEM02,
                  T_ID_MB_REQUISITES,
                  T_ID_PROCESSING_LOG,
                  NVL(t.T_TRADEDATE, RSI_GT.ZeroDate) TRADEDATE,
                  NVL(t.T_TRADESESSIONDATE, RSI_GT.ZeroDate) TRADESESSIONDATE,
                  NVL(t.T_SESSIONNO, 0) SESSIONNO,
                  NVL(t.T_RECNO, 0) RECNO,
                  NVL(t.T_ORDERNO, CHR(1)) ORDERNO,
                  NVL(t.T_STATUS, CHR(1)) STATUS,
                  NVL(t.T_BUYSELL, CHR(1)) BUYSELL,
                  NVL(t.T_SECURITYID, CHR(1)) SECURITYID,
                  NVL(t.T_PRICETYPE, CHR(1)) PRICETYPE,
                  NVL(t.T_CURRENCYID, CHR(1)) CURRENCYID,
                  NVL(t.T_PRICE, 0) PRICE,
                  NVL(t.T_QUANTITY, 0) QUANTITY,
                  NVL(t.T_ENTRYTIME, RSI_GT.ZeroDate) ENTRYTIME,
                  NVL(t.T_AMENDTIME, RSI_GT.ZeroDate) AMENDTIME,
                  NVL(t.T_BROKERREF, CHR(1)) BROKERREF,
                  NVL(t.T_REPORATE, 0) REPORATE,
                  NVL(t.T_CLIENTCODE, CHR(1)) CLIENTCODE,
                  NVL(t.T_TRDACCID, CHR(1)) TRDACCID,
                  NVL(t.T_REPOVALUE, 0) REPOVALUE,
                  NVL(t.T_ORDTYPECODE, CHR(1)) ORDTYPECODE,
                  NVL(t.T_PARTYID, 0) PARTYID,
                  NVL(t.T_SFCONTRID, 0) SFCONTRID,
                  NVL(t.T_IsSEB, 0) ISSEB,
                  NVL(t.T_OBJECTID, 0) OBJECTID,
                  NVL(t.T_IsREPO, 0) IsREPO,
                  NVL(t.T_vCLIENTCODE, CHR(1)) vCLIENTCODE,
                  NVL(t.T_FIID, -1) FIID,
                  NVL(t.T_FILESESSIONNO, 0) FILESESSIONNO,
                  NVL(t.T_UNDERWR, 0) UNDERWR
            FROM D_SEM02_TMP t
           ORDER BY T_FILESESSIONNO, T_SESSIONNO, T_RECNO;
      LOOP
        FETCH v_cData INTO v_Req;
        EXIT WHEN v_cData%NOTFOUND OR v_cData%NOTFOUND IS NULL;

        v_ErrMes  := CHR(1);
        v_WarnMes := CHR(1);

        IF v_Req.t_ObjectID > 0 THEN
          v_ErrMes := 'Ошибка при загрузке заявки на сделку № "' || v_Req.t_OrderNo || '"' || CHR(10) || 'Запись репликации с действием создать для объекта ' || TO_CHAR(v_Req.t_ObjectID) || ' уже передавалась в RS-Bank.' || CHR(10) || 'Повторная репликация невозможна.';
        ELSIF v_Req.t_vClientCode <> CHR(1) AND v_Req.t_PartyID <= 0 THEN
          v_ErrMes := 'Ошибка при загрузке заявки на сделку № "' || v_Req.t_OrderNo || '"' || CHR(10) || 'Не найден субъект через краткий код на бирже "' || v_Req.t_vClientCode || '"';
        ELSIF v_Req.t_SecurityId <> CHR(1) AND v_Req.t_FIID = -1 THEN
          v_ErrMes := 'Ошибка при загрузке заявки на сделку № "' || v_Req.t_OrderNo || '"' || CHR(10) || 'В справочнике не найден ФИ с кодом "' || v_Req.t_SecurityId || '"';
        ELSIF LENGTH(TRIM(TO_CHAR(v_Req.t_Quantity))) > 16 THEN
          v_ErrMes := 'Ошибка при загрузке заявки на сделку № "' || v_Req.t_OrderNo || '"' || CHR(10) || 'Превышено максимально возможное количество загружаемых знаков параметра Quantity';
        END IF;

        IF v_ErrMes = CHR(1) THEN
          IF PutRequestInfo(p_Prm, v_Req, v_ErrMes, v_WarnMes) = 0 THEN
            v_stat := RSI_GT.SaveBatch(BATCH_SIZE, v_ErrMes); --сохранить по мере накопления
          END IF;
        END IF;

        IF v_ErrMes <> CHR(1) THEN
          RSB_GTLOG.Error(v_ErrMes);
        END IF;

        IF v_WarnMes <> CHR(1) THEN
          RSB_GTLOG.Warning(v_WarnMes);
        END IF;
      END LOOP;
      CLOSE v_cData;
    END IF;

    IF v_stat = 0 THEN
      --сохранение накопленных объектов ЗР
      v_stat := RSI_GT.Save(v_ErrMes);
      IF v_stat <> 0 THEN
        RSB_GTLOG.Error(v_ErrMes);
      END IF;
    END IF;

    --сохранение накопленных записей лога
    RSB_GTLOG.Save;

  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_Prm.SeanceID, RSB_GTFN.RG_REQ, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка при обработке данных в ф-и ProcessReplRecByTmp_SEM02(), ' ||
                           'SeanceID=' || TO_CHAR(p_Prm.SeanceID) || ', FileImport=' || TO_CHAR(p_Prm.FileImport) ||
                           ', Source_Code=' || p_Prm.Source_Code || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
  END;

  --Создание объектов записей репликаций по данным временной таблицы SEM02
  FUNCTION CreateReplRecByTmp_SEM02(p_SeanceID IN NUMBER, p_ImpDate IN DATE, p_SourceCode IN VARCHAR2, p_PrmStr IN VARCHAR2)
    RETURN NUMBER
  IS
    v_Prm PRMREC_t;
  BEGIN
    --инициализация используемых параметров
    SetPrmRecByStr(v_Prm, p_PrmStr);
    v_Prm.SeanceID           := p_SeanceID;
    v_Prm.imp_date           := p_ImpDate;
    v_Prm.Source_Code        := p_SourceCode;

    ProcessReplRecByTmp_SEM02(v_Prm);

    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_REQ, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и CreateReplRecByTmp_SEM02(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_REQ) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE));
    RETURN 1;
  END;

  PROCEDURE SetPartPrmDeal_PM(p_Deal IN OUT NOCOPY DATADEALS_t)
  IS
  BEGIN
    IF p_Deal.IsREPO = 1 THEN
      IF p_Deal.IsCliring = 1 THEN
        IF p_Deal.RepoPart = 1 THEN
          p_Deal.GateDealType := 'R' || p_Deal.BuySell;
        ELSE
          IF p_Deal.BuySell = 'B' THEN
            p_Deal.GateDealType := 'RS';
          ELSE
            p_Deal.GateDealType := 'RB';
          END IF;
        END IF;
      ELSE
        p_Deal.GateDealType := 'R' || p_Deal.BuySell;
      END IF;

      IF LOWER(p_Deal.SecurityType) = 'ксу' OR LOWER(p_Deal.SecurityType) = 'Єбг' THEN
        p_Deal.GateDealType := p_Deal.GateDealType || 'G';
      END IF;
    ELSE
      p_Deal.GateDealType := p_Deal.BuySell;
    END IF;
  END;

  PROCEDURE SetUINNumber(p_Deal IN OUT NOCOPY DATADEALS_t)
  IS
  BEGIN
    p_Deal.UINNumber := CASE WHEN p_Deal.vClientCode <> CHR(1)
                             THEN p_Deal.BuySell || '/' || p_Deal.TradeNo
                             WHEN p_Deal.IsOTC = 1 AND p_Deal.ClientCode = CHR(1)
                             THEN p_Deal.BuySell || '/' || p_Deal.TradeNo || '/' || 'ОТС'
                             ELSE p_Deal.TradeNo
                         END;
  END;

  --Заполнение и сохранение параметров сделки
  FUNCTION PutDealInfoPM(p_Prm IN OUT NOCOPY PRMREC_t, p_Deal IN OUT NOCOPY DATADEALS_t, p_ErrMes IN OUT NOCOPY VARCHAR2, p_WarnMes IN OUT NOCOPY VARCHAR2) RETURN NUMBER
  IS
    v_stat         NUMBER(5) := 0;
    v_DealDate     DATE;
    v_RefNote      VARCHAR2(20);
    v_Relativ      CHAR;
    v_PaymDate     DATE;
    v_NDay         NUMBER(10) := 0;
    v_IndexYm      NUMBER(5);
    v_IndexYn      NUMBER(5);
  BEGIN

    v_stat := RSI_GT.InitRec(CASE WHEN p_Deal.IsCliring = 1 THEN RSB_GTFN.RG_CLEARING ELSE RSB_GTFN.RG_DEAL END,
                             CASE WHEN p_Deal.IsCliring = 1 THEN 'Клиринг. ' ELSE '' END || CASE WHEN p_Deal.IsOTC = 1 THEN 'Внебиржевая сделка на ММВБ №' ELSE 'Сделка на ММВБ №' END || p_Deal.UINNumber, --Наименование объекта
                             0, --проверять существование объекта в системе по коду
                             p_Deal.UINNumber || CASE WHEN p_Deal.IsCliring = 1 THEN '_c' || p_Deal.RepoPart ELSE '' END, --код объекта
                             p_Deal.ClientID, --ID клиента
                             p_ErrMes);

    IF v_stat = 0 THEN
      --дата сделки
      IF p_Deal.TradeDate = RSI_GT.ZeroDate THEN
        v_DealDate := p_Prm.imp_date;
      ELSE
        v_DealDate := p_Deal.TradeDate;
      END IF;

      RSI_GT.SetParmByName('RGDLTC_ISIN', p_Deal.SecurityId);

      RSI_GT.SetParmByName('RGDLTC_DEALCODETS', p_Deal.TradeNo);

      RSI_GT.SetParmByName('RGDLTC_DEALTIME',   p_Deal.TradeTime);
      
      IF p_Deal.IsCliring = 1 AND p_Prm.IMPORT_SETTLETIME = 1 AND p_Deal.SettleTime <> RSI_GT.ZeroTime THEN
        RSI_GT.SetParmByName('RGDLLG_SUPTIME_01', p_Deal.SettleTime);
        RSI_GT.SetParmByName('RGDLLG_SUPTIME_02', p_Deal.SettleTime);
      ELSE
        RSI_GT.SetParmByName('RGDLLG_SUPTIME_01', p_Deal.TradeTime);
        RSI_GT.SetParmByName('RGDLLG_SUPTIME_02', p_Deal.TradeTime);
      END IF;

      IF p_Deal.vClientCode <> CHR(1) THEN
        --по единому краткому коду клиента из ДБО находим клиента
        RSI_GT.SetParmByName('RGDLTC_CLIENT', p_Deal.vClientCode);
      END IF;

      IF p_Deal.BrokerRef <> CHR(1) THEN
        RSI_GT.SetParmByName('RGDLTC_BROKERREF', p_Deal.BrokerRef);
      END IF;

      v_RefNote := RSB_GTFN.GetRefNote(TRIM(p_Deal.BrokerRef), '/'); --Код информации Трейдера

      IF v_RefNote <> CHR(1) THEN
        RSI_GT.SetParmByName('RGDLTC_NOTEVAL1', v_RefNote);
        IF p_Prm.FileImport = RSB_GTFN.FileSEM03 AND p_Deal.vClientCode <> CHR(1) THEN
          RSI_GT.SetParmByName('RGDLTC_TRADERCODE', v_RefNote);
        END IF;
      END IF;

      IF (p_Prm.FileImport = RSB_GTFN.FileSEM03 OR p_Prm.FileImport = RSB_GTFN.FileEQM3T) AND p_Deal.IsREPO = 0 AND p_Deal.IsSEB = 0 THEN
        IF p_Deal.Portfolio_ <> RSB_PMWRTOFF.KINDPORT_UNDEF THEN
          RSI_GT.SetParmByName('RGDLTC_PORTFOLIO', p_Deal.Portfolio_);
        END IF;
      END IF;

      RSI_GT.SetParmByName('RGDLTC_DEALTYPE', p_Deal.DealTypeID);

      RSI_GT.SetParmByName('RGDLTC_PRICE',   p_Deal.Price);
      RSI_GT.SetParmByName('RGDLTC_NUMLOTS', p_Deal.Quantity);

      IF p_Deal.IsREPO = 1 THEN
        RSI_GT.SetParmByName('RGDLLG_PRICE_02',   p_Deal.Price2  ); --цена по 2-ой части
        RSI_GT.SetParmByName('RGDLTC_INCOMERATE', p_Deal.RepoRate); --ставка Репо
      END IF;

      --цена в процентах
      v_Relativ := CASE WHEN UPPER(p_Deal.PriceType) = 'PERC' THEN RSB_GTFN.SET_CHAR ELSE RSB_GTFN.UNSET_CHAR END;
      RSI_GT.SetParmByName('RGDLLG_RELATIVEPRICE_01', v_Relativ);
      IF p_Deal.IsREPO = 1 THEN
        RSI_GT.SetParmByName('RGDLLG_RELATIVEPRICE_02', v_Relativ);
      END IF;

      --НКД - для купонной ценной бумаги
      RSI_GT.SetParmByName('RGDLLG_NKD_01', ROUND(p_Deal.AccInt, 2));

      --сумма сделки без НКД
      RSI_GT.SetParmByName('RGDLLG_COST_01', ROUND(p_Deal.Value_, 2));

      --общая сумма сделки
      RSI_GT.SetParmByName('RGDLLG_TOTALCOST_01', ROUND(p_Deal.Amount, 2));

      IF p_Deal.BuySell = 'B' THEN
        RSI_GT.SetParmByName(RSB_GTFN.GetParmName('RGDLPM_ISFIXAMOUNT', Rsb_Payment.CAi), RSB_GTFN.SET_CHAR);
      ELSE
        RSI_GT.SetParmByName(RSB_GTFN.GetParmName('RGDLPM_ISFIXAMOUNT', Rsb_Payment.CAi), RSB_GTFN.UNSET_CHAR);
      END IF;

      IF p_Deal.IsREPO = 1 THEN
        IF(p_Deal.BuySell = 'B') THEN
          RSI_GT.SetParmByName(RSB_GTFN.GetParmName('RGDLPM_ISFIXAMOUNT', Rsb_Payment.CRi), RSB_GTFN.UNSET_CHAR);
        ELSE
          RSI_GT.SetParmByName(RSB_GTFN.GetParmName('RGDLPM_ISFIXAMOUNT', Rsb_Payment.CRi), RSB_GTFN.SET_CHAR);
        END IF;
      END IF;

      --номер договора с клиентом
      RSI_GT.SetParmByName('RGDLTC_CONTRID', CHR(1));

      RSI_GT.SetParmByName('RGDLTC_DEALCODE', p_Deal.UINNumber);

      --код расчетов
      RSI_GT.SetParmByName('RGDLTC_PCODE', p_Deal.SettleCode);

      --признак ОРЦБ
      RSI_GT.SetParmByName('RGDLTC_USERFLAG1', RSB_GTFN.SET_CHAR);
      RSI_GT.SetParmByName('RGDLTC_USERFLAG3', RSB_GTFN.SET_CHAR);

      --Вид заявки в соответствии с правилами торгов
      RSI_GT.SetParmByName('RGDLTC_ORDTYPECODE', p_Deal.OrdTypeCode);

      --тип сделки на ММВБ
      IF p_Prm.FileImport IN (RSB_GTFN.FileSEM03, RSB_GTFN.FileASTS,RSB_GTFN.FileEQM3T) THEN
        RSI_GT.SetParmByName('RGDLTC_KIND', p_Deal.TradeType);
      ELSE
        IF p_Deal.IsREPO = 1 THEN
          RSI_GT.SetParmByName('RGDLTC_KIND', 'R');
        ELSE
          RSI_GT.SetParmByName('RGDLTC_KIND', 'T');
        END IF;
      END IF;

      RSI_GT.SetParmByName('RGDLTC_BOARDID', p_Deal.BoardID);
      IF p_Prm.FileImport = RSB_GTFN.FileSEM03 OR p_Prm.FileImport = RSB_GTFN.FileEQM3T THEN
        RSI_GT.SetParmByName('RGDLTC_BOARDTYPE', p_Deal.BoardType);
      END IF;

      --комиссия биржи
      --Все три биржевые комиссии в импортированной сделке обрабатываются одной суммой - это правильно
      IF p_Deal.IsCliring = 0 THEN
        RSI_GT.SetParmByName('RGDLTC_AMOUNT_COMM', p_Deal.ExhComm );
        RSI_GT.SetParmByName('RGDLTC_CLRCOMM', p_Deal.ClrComm );
      ELSE -- p_Deal.IsCliring = true
        RSI_GT.SetParmByName('RGDLTC_REPOPART', p_Deal.RepoPart);
        RSI_GT.SetParmByName('RGDLTC_BUYSELL', p_Deal.BuySell);
        RSI_GT.SetParmByName('RGDLTC_CLIRINGTIME', p_Deal.SettleTime);
        RSI_GT.SetParmByName('RGDLTC_SESSION', p_Deal.Session);
        RSI_GT.SetParmByName('RGDLTC_EXTSETTLECODE', p_Deal.ExtSettleCode);
      END IF;

      --дата сделки
      RSI_GT.SetParmByName('RGDLTC_DEALDATE', v_DealDate);

      IF p_Deal.CPFirmId <> CHR(1) THEN
        RSI_GT.SetParmByName('RGDLTC_PARTY', p_Deal.CPFirmId);
      ELSE
        --для СделкаToday (не срочных и не РЕПО) в качестве контрагента ставим субъекта с принадлежностью 'Центральный контрагент'
        IF ( (p_Prm.FileImport = RSB_GTFN.FileSEM03 OR p_Prm.FileImport = RSB_GTFN.FileEQM3T) AND p_Deal.IsREPO = 0 AND 
             p_Deal.SettleDate = v_DealDate AND v_DealDate >= TO_DATE('01.11.2011', 'DD.MM.YYYY')
           ) OR p_Prm.FileImport = RSB_GTFN.FileASTS THEN
          p_Deal.CPFirmId := p_Prm.CPFirmIdConst;
          IF p_Deal.CPFirmId <> CHR(1) THEN
            RSI_GT.SetParmByName('RGDLTC_PARTY', p_Deal.CPFirmId);
          END IF;
        END IF;
      END IF;

      v_PaymDate := v_DealDate;

      RSI_GT.SetParmByName('RGDLTC_MARKET', p_Prm.MMVB_Code);
      --суммы по второй части для РЕПО
      IF p_Deal.IsREPO = 1 THEN
        IF p_Deal.RepoPart <> 2 THEN --заполним даты исполнения по 1 ч по сделке и по клирингу
          v_NDay := p_Deal.NDay1_;
          v_PaymDate := p_Deal.PaymDate1_;

          IF p_Deal.IsCliring = 1 THEN
            RSI_GT.SetParmByName('RGDLLG_SUPDATE_01', p_Deal.SettleDate);
          ELSE
            RSI_GT.SetParmByName('RGDLLG_SUPDATE_01', v_PaymDate);
            RSI_GT.SetParmByName('RGDLLG_PAYDATE_01', v_PaymDate);
          END IF;
        ELSE --даты исполнения по 2 ч по клирингу (не по сделке) не заполняем - вычислим при вставке сделки в БОЦБ
          RSI_GT.SetParmByName('RGDLLG_SUPDATE_01', p_Deal.SettleDate);
        END IF;
       
        IF p_Deal.IsCliring = 0 THEN --заполним даты исполнения по 2 ч по сделке(не клиринг)
          v_IndexYm := INSTR(UPPER(p_Deal.SettleCode), 'Y', 1);
          v_IndexYn := INSTR(UPPER(p_Deal.SettleCode), 'Y', v_IndexYm + 1);

          IF v_IndexYm = 1 AND v_IndexYn > v_IndexYm THEN --обрабатываем конструкции вида Y0/Yn, Ym/Yn, Y0/YnW, Y0/YnM
            IF p_Deal.SettleDate = p_Deal.WorkSettleDate_ AND p_Deal.CalcItogDate_ <> p_Deal.SettleDate THEN
              p_WarnMes := 'Срок сделки "' || p_Deal.TradeNo || '", определенный параметром SettleDate = ' || TO_CHAR(p_Deal.SettleDate, 'DD.MM.YYYY') || ', не соответствует коду расчетов "' || p_Deal.SettleCode  || '" по сделке.';
            ELSIF p_Deal.SettleDate <> p_Deal.WorkSettleDate_ AND p_Deal.WorkSettleDate_ <> p_Deal.CalcItogDate_ THEN
              p_ErrMes := 'По сделке "' || p_Deal.TradeNo || '", дата SettleDate = ' || TO_CHAR(p_Deal.SettleDate, 'DD.MM.YYYY') || ', указанная в файле выгрузки, определилась как ' || TO_CHAR(p_Deal.CalcItogDate_, 'DD.MM.YYYY') || ' по календарю ' || TO_CHAR(p_Deal.CalendId) || '. Исправьте параметры дня ' || TO_CHAR(p_Deal.SettleDate, 'DD.MM.YYYY') || ' в перечисленных календарях: вид обслуживания = "Банковское", признак баланса установлен.';
            END IF;

            v_NDay := p_Deal.NDay2_;
            v_PaymDate := p_Deal.SettleDate;

          ELSIF INSTR(UPPER(p_Deal.SettleCode), 'T0/Y') = 1 THEN --обрабатываем конструкции вида T0/Yn
            v_NDay := p_Deal.NDay2_;
            v_PaymDate := p_Deal.PaymDate2_;

            IF p_Deal.SettleDate = p_Deal.WorkSettleDate_ AND v_PaymDate <> p_Deal.SettleDate THEN
               p_WarnMes := 'Срок сделки "' || p_Deal.TradeNo || '", определенный параметром SettleDate = ' || TO_CHAR(p_Deal.SettleDate, 'DD.MM.YYYY') || ', не соответствует коду расчетов "' || p_Deal.SettleCode || '" по сделке.';
            ELSIF p_Deal.SettleDate <> p_Deal.WorkSettleDate_ AND p_Deal.WorkSettleDate_ <> v_PaymDate THEN
               p_ErrMes := 'По сделке "' || p_Deal.TradeNo || '", дата SettleDate = ' || TO_CHAR(p_Deal.SettleDate, 'DD.MM.YYYY') || ', указанная в файле выгрузки, определилась как ' || TO_CHAR(v_PaymDate, 'DD.MM.YYYY') || ' по календарю ' || TO_CHAR(p_Deal.CalendId) || '. Исправьте параметры дня ' || TO_CHAR(p_Deal.SettleDate, 'DD.MM.YYYY') || ' в перечисленных календарях: вид обслуживания = "Банковское", признак баланса установлен.';
            END IF;

            v_PaymDate := p_Deal.SettleDate;
          ELSE
            v_PaymDate := v_PaymDate + p_Deal.RepoPeriod;
            v_NDay := p_Deal.NDay1_;
          END IF;
         
          RSI_GT.SetParmByName('RGDLLG_SUPDATE_02', v_PaymDate);
          RSI_GT.SetParmByName('RGDLLG_PAYDATE_02', v_PaymDate);
        END IF;

        RSI_GT.SetParmByName('RGDLLG_COST_02',      ROUND(p_Deal.Value2_, 2));
        RSI_GT.SetParmByName('RGDLLG_NKD_02',       ROUND(p_Deal.AccInt2, 2));
        RSI_GT.SetParmByName('RGDLLG_TOTALCOST_02', ROUND(p_Deal.Amount2, 2));
      ELSE
        RSI_GT.SetParmByName('RGDLLG_SUPDATE_01', p_Deal.SettleDate);
        RSI_GT.SetParmByName('RGDLLG_PAYDATE_01', p_Deal.SettleDate);
        v_PaymDate := p_Deal.SettleDate;
        v_NDay := p_Deal.NDay1_;
        IF p_Deal.SettleDate <> p_Deal.WorkSettleDate_ THEN
          p_WarnMes := 'По сделке "' || p_Deal.TradeNo || '", дата SettleDate = ' || TO_CHAR(p_Deal.SettleDate, 'DD.MM.YYYY') || ', указанная в файле выгрузки, попадает на выходной день. Расчетная дата - ' || TO_CHAR(p_Deal.WorkSettleDate_, 'DD.MM.YYYY') || '.';
        END IF;
      END IF;
         
      IF v_NDay > 0 THEN
        RSI_GT.SetParmByName('RGDLTC_DAYBEFOREEXECUTE', v_NDay);
      END IF;
      
      RSI_GT.SetParmByName(RSB_GTFN.GetParmName('RGDLPM_DATE', Rsb_Payment.BAi), p_Deal.SettleDate);
      RSI_GT.SetParmByName(RSB_GTFN.GetParmName('RGDLPM_DATE', Rsb_Payment.CAi), p_Deal.SettleDate);

      RSI_GT.SetParmByName('RGDLTC_MARKETSCHEME', p_Deal.MarketSchemeID);

      --идентификатор документа-подтверждения вида 'Отчет биржи'
      RSI_GT.SetParmByName('RGDLTC_MARKETREPORT_ID', p_Deal.MarketReportID);

      --статус сделки - отложенная
      RSI_GT.SetParmByName('RGDLTC_DEALSTATUS', RSB_GTFN.DL_PREPARING);

      --дата комиссии
      RSI_GT.SetParmByName('RGDLTC_DATE_CM',  p_Prm.imp_date);

      RSI_GT.SetParmByName('RGDLPM_NUMBPAYMS', 0);

      --точность цены
      RSI_GT.SetParmByName('RGDLLG_POINT_01', p_Deal.Decimals);
      RSI_GT.SetParmByName('RGDLLG_POINT_02', p_Deal.Decimals);
      --торговый счет, в счет которого заключена данная сделка
      RSI_GT.SetParmByName('RGDLTC_TRDACCID', p_Deal.TrdAccId);

      IF (p_Prm.FileImport = RSB_GTFN.FileSEM03 OR p_Prm.FileImport = RSB_GTFN.FileEQM3T) THEN
        RSI_GT.SetParmByName('RGDLTC_ORDERNOM', p_Deal.OrderNo);
        --поле "ссылка"
        RSI_GT.SetParmByName('RGDLTC_MATCHREF', p_Deal.MatchRef);
        --номинальная стоимость одной ценной бумаги
        RSI_GT.SetParmByName('RGDLTC_FACEVALUE', p_Deal.FaceValue);

        IF p_Deal.TradeSessionDate <> RSI_GT.ZeroDate AND p_Deal.TradeSessionDate <> v_DealDate THEN
          RSI_GT.SetParmByName('RGDLTC_SESSIONDATE', p_Deal.TradeSessionDate);
        END IF;
      END IF;

      RSI_GT.SetParmByName(RSB_GTFN.GetParmName('RGDLPM_PAYFI', Rsb_Payment.BAi), p_Deal.CurrencyId);
      RSI_GT.SetParmByName(RSB_GTFN.GetParmName('RGDLPM_PAYFI', Rsb_Payment.CAi), p_Deal.CurrencyId);
      RSI_GT.SetParmByName(RSB_GTFN.GetParmName('RGDLPM_PAYFI', Rsb_Payment.PM_PURP_AVANCE), p_Deal.CurrencyId);

      IF p_Deal.IsREPO = 1 THEN
        RSI_GT.SetParmByName(RSB_GTFN.GetParmName('RGDLPM_PAYFI', Rsb_Payment.BRi), p_Deal.CurrencyId);
        RSI_GT.SetParmByName(RSB_GTFN.GetParmName('RGDLPM_PAYFI', Rsb_Payment.CRi), p_Deal.CurrencyId);
        RSI_GT.SetParmByName(RSB_GTFN.GetParmName('RGDLPM_PAYFI', Rsb_Payment.PM_PURP_BACK_AVANCE), p_Deal.CurrencyId);
      END IF;

      IF (p_Prm.FileImport = RSB_GTFN.FileSEM03 OR p_Prm.FileImport = RSB_GTFN.FileEQM3T) AND p_Deal.vClientCode <> CHR(1) AND p_Prm.FORM_ORDERS_IMPORT = 1 THEN
        RSI_GT.SetParmByName('RGDLTC_INDOC', p_Deal.OrderNo);
        --Дата поручения на сделку
        RSI_GT.SetParmByName('RGDLTC_INDOCDATE', v_DealDate);
        --Время поручения на сделку
        RSI_GT.SetParmByName('RGDLTC_INDOCTIME', p_Deal.TradeTime - (1/1440*1));
      END IF;

      IF p_Prm.FileImport = RSB_GTFN.FileASTS THEN
        RSI_GT.SetParmByName('RGDLTC_PROGNOS', RSB_GTFN.SET_CHAR);
      END IF;

      IF p_Prm.FileImport = RSB_GTFN.FileEQM06 AND (p_Deal.UnderWr = 1 OR p_Deal.UnderWr_Pokupka = 1) THEN
        RSI_GT.SetParmByName('RGDLTC_UNDERWR', RSB_GTFN.SET_CHAR);
      END IF;

      IF (p_Prm.FileImport = RSB_GTFN.FileSEM03 OR p_Prm.FileImport = RSB_GTFN.FileEQM06) AND p_Deal.IsFloating = 1 AND p_Deal.IsREPO = 1 THEN
        RSI_GT.SetParmByName('RGDLTC_ISFLOATING', RSB_GTFN.SET_CHAR);
        IF p_Deal.BenchMark <> -1 THEN
          RSI_GT.SetParmByName('RGDLTC_BENCHMARK', p_Deal.BenchMark);
        END IF;
        RSI_GT.SetParmByName('RGDLTC_REPORATE', p_Deal.SpredRate);
        RSI_GT.SetParmByName('RGDLTC_BMRATE', p_Deal.IndRate);
      END IF;

      --проверка ошибок после добавления параметров
      IF RSI_GT.GetLastError <> CHR(1) THEN
        v_stat := 1;
        p_ErrMes := RSI_GT.GetLastError;
      END IF;
    END IF;

    RETURN v_stat;
  END;

  FUNCTION SetTypeOperationBOCB(p_Deal IN OUT NOCOPY DATADEALS_t, p_ErrMes IN OUT NOCOPY VARCHAR2) RETURN NUMBER
  IS
  BEGIN
    RETURN RSB_GTFN.GetTypeOperationBOCB('B', p_Deal.GateDealType, p_Deal.DealTypeID, p_ErrMes, 0, (CASE WHEN p_Deal.TradeDate = p_Deal.SettleDate THEN 1 ELSE 0 END), p_Deal.IsSEB, p_Deal.UnderWr, p_Deal.UnderWr_Pokupka, p_Deal.IsRSHB, p_Deal.IsOTC);
  END;

  --Обработка записей по данным буферной таблицы
  PROCEDURE ProcessReplRecByTmp_SEM03(p_Prm IN OUT NOCOPY PRMREC_t)
  IS
    v_Prm     PRMREC_t;
    v_Deal    DATADEALS_t;
    v_stat    NUMBER(5) := 0;
    v_ErrMes  VARCHAR2(1000) := CHR(1);
    v_WarnMes VARCHAR2(1000) := CHR(1);
  BEGIN
    --инициализация глобального лога для накопления записей для вставки
    RSB_GTLOG.Init(BATCH_SIZE, p_Prm.SeanceID, RSB_GTFN.RG_DEAL);

    --инициализация глобального накопителя объектов ЗР для вставки
    v_stat := RSI_GT.Init(RSI_GT.DIRECT_INSERT_YES, p_Prm.SeanceID, p_Prm.Source_Code, RSI_GT.GTCODE_RECEIVE, v_ErrMes);
    IF v_stat <> 0 THEN
      RSB_GTLOG.Error(v_ErrMes);
    END IF;

    IF v_stat = 0 THEN
      FOR cData IN (SELECT NVL(eqm06.T_ID_MB_REQUISITES,0) isSem06,
                           T_ID_MB_SEM03 ID_MB_SEM03,
                           NVL(t.T_CPFirmId, CHR(1)) CPFirmId,
                           NVL(DECODE(eqm06.T_RepoPart, 2, 0, eqm06.T_CLRCOMM), 0) CLRCOMM_06,
                           NVL(DECODE(eqm06.T_RepoPart, 2, CHR(1), eqm06.T_TRDACCID), CHR(1)) TRDACCID_06,
                           NVL(eqm06Part2.T_AMOUNT, 0) AMOUNT2_06,
                           NVL(eqm06Part2.T_ACCINT, 0) ACCINT2_06,
                           NVL(eqm06Part2.T_PRICE, 0) PRICE2_06,
                           NVL(eqm06Part2.T_VALUE, 0) VALUE2_06,
                           NVL(t.T_TRADEDATE, RSI_GT.ZeroDate) TRADEDATE,
                           NVL(t.T_TRADESESSIONDATE, RSI_GT.ZeroDate) TRADESESSIONDATE,
                           NVL(t.T_CURRENCYID, CHR(1)) CURRENCYID,
                           NVL(t.T_BOARDID, CHR(1)) BOARDID,
                           NVL(t.T_BOARDNAME, CHR(1)) BOARDNAME,
                           NVL(t.T_SETTLEDATE, RSI_GT.ZeroDate) SETTLEDATE,
                           NVL(t.T_SECURITYID, CHR(1)) SECURITYID,
                           NVL(t.T_SECURITYTYPE, CHR(1)) SECURITYTYPE,
                           NVL(t.T_FACEVALUE, 0) FACEVALUE, 
                           NVL(t.T_FIID, -1) FIID, 
                           NVL(t.T_PRICETYPE, CHR(1)) PRICETYPE,
                           NVL(t.T_TRDACCID, CHR(1)) TRDACCID,
                           NVL(t.T_RECNO, 0) RECNO,
                           NVL(t.T_TRADENO, 0) TRADENO,
                           NVL(t.T_TRADETIME, RSI_GT.ZeroDate) TRADETIME,
                           NVL(t.T_BUYSELL, CHR(1)) BUYSELL,
                           NVL(t.T_SETTLECODE, CHR(1)) SETTLECODE,
                           NVL(t.T_DECIMALS, 0) DECIMALS,
                           NVL(t.T_PRICE, 0) PRICE,
                           NVL(t.T_VALUE, 0) VALUE,
                           NVL(t.T_QUANTITY, 0) QUANTITY,
                           NVL(t.T_AMOUNT, 0) AMOUNT,
                           NVL(t.T_EXCHCOMM, 0) EXCHCOMM,
                           NVL(t.T_ORDERNO, CHR(1)) ORDERNO,
                           NVL(t.T_ACCINT, 0) ACCINT,
                           NVL(t.T_REPOVALUE, 0) REPOVALUE,
                           NVL(t.T_REPOPERIOD, 0) REPOPERIOD,
                           NVL(t.T_REPORATE, 0) REPORATE,
                           NVL(t.T_TRADETYPE, CHR(1)) TRADETYPE,
                           NVL(t.T_PRICE2, 0) PRICE2,
                           NVL(t.T_ACCINT2, 0) ACCINT2,
                           NVL(t.T_CLIENTCODE, CHR(1)) CLIENTCODE,
                           NVL(t.T_VCLIENTCODE, CHR(1)) VCLIENTCODE,
                           NVL(t.T_MATCHREF, CHR(1)) MATCHREF,
                           NVL(t.T_BROKERREF, CHR(1)) BROKERREF,
                           NVL(t.T_IsSEB, 0) IsSEB, 
                           NVL(t.T_PARTYID, 0) PARTYID, 
                           NVL(t.T_SFCONTRID, 0) SFCONTRID, 
                           NVL(t.T_CPFIRMPARTYID, 0) CPFIRMPARTYID, 
                           NVL(t.T_CURID, 0) CURID, 
                           NVL(t.T_CALENDID, 0) CALENDID,
                           NVL(t.T_MARKETSCHEMEID, 0) MARKETSCHEMEID, 
                           NVL(t.T_NDAY1, 0) NDAY1, 
                           NVL(t.T_NDAY2, 0) NDAY2, 
                           NVL(t.T_PAYMDATE1, RSI_GT.ZeroDate) PAYMDATE1, 
                           NVL(t.T_PAYMDATE2, RSI_GT.ZeroDate) PAYMDATE2, 
                           NVL(t.T_CALCITOGDATE, RSI_GT.ZeroDate) CALCITOGDATE, 
                           NVL(t.T_WORKSETTLEDATE, RSI_GT.ZeroDate) WORKSETTLEDATE, 
                           NVL(t.T_OBJECTID, 0) OBJECTID, 
                           NVL(t.T_PORTFOLIO, 0) PORTFOLIO, 
                           NVL(t.T_UNDERWR, 0) UNDERWR, 
                           NVL(t.T_UNDERWR_BUY, 0) UNDERWR_BUY, 
                           NVL(t.T_DOC_NO, CHR(1)) DOC_NO,
                           NVL(t.T_IsREPO, 0) IsREPO,
                           NVL(t.T_MarketReportID, 0) MarketReportID,
                           NVL(t.T_RateType, CHR(1)) RateType,
                           NVL(t.t_BenchMark, CHR(1)) BenchMark,
                           NVL(eqm06.t_BenchMarkRate, 0) IndRate,
                           NVL(eqm06.t_CurRepoRate, 0) IncomeRate
                      FROM D_SEM03_TMP t, D_EQM06_TMP eqm06, D_EQM06_TMP eqm06Part2
                     WHERE t.T_TRADENO = eqm06.T_TRADENO(+)
                       AND t.T_BUYSELL = eqm06.T_BUYSELL(+)
                       AND t.T_vCLIENTCODE = eqm06.T_vCLIENTCODE(+)
                       AND t.T_TRADENO = eqm06Part2.T_TRADENO(+)
                       AND 2 = eqm06Part2.T_Repopart(+)
                       AND (3 = eqm06Part2.T_InfType(+) OR 6 = eqm06Part2.T_InfType(+))
                       AND t.T_vCLIENTCODE = eqm06Part2.T_vCLIENTCODE(+)
                     ORDER BY t.T_DOC_NO, t.t_recno
                   )
      LOOP
        v_ErrMes  := CHR(1);
        v_WarnMes := CHR(1);

        v_Deal.AccInt          := cData.AccInt;
        v_Deal.AccInt2         := CASE WHEN cData.AccInt2_06 = 0 THEN cData.ACCINT2 ELSE cData.AccInt2_06 END;
        v_Deal.Amount          := cData.Amount;
        v_Deal.Amount2         := cData.Amount2_06;
        v_Deal.BoardId         := cData.BoardId;
        v_Deal.BoardType       := cData.BoardName;
        v_Deal.BrokerRef       := cData.BrokerRef;
        v_Deal.BuySell         := cData.BuySell;
        v_Deal.CalcItogDate_   := cData.CalcItogDate;
        v_Deal.CalendID        := cData.CalendID;
        v_Deal.ClientCode      := cData.ClientCode;
        v_Deal.ClientContrID   := cData.SfContrID;
        v_Deal.ClientID        := cData.PartyID;
        v_Deal.ClrComm         := cData.CLRCOMM_06;
        v_Deal.CPFirmId        := cData.CPFirmId;
        v_Deal.CPFirmId_ID     := cData.CPFirmPartyID;
        v_Deal.CurrencyId      := cData.CurrencyId;
        v_Deal.CurrencyId_FIID := cData.CurID;
        v_Deal.Decimals        := cData.Decimals;
        v_Deal.DOC_NO          := cData.DOC_NO;
        v_Deal.ExhComm         := cData.EXCHCOMM;
        v_Deal.IsCliring       := 0;
        v_Deal.IsRepo          := cData.IsRepo;
        v_Deal.IsSeb           := cData.IsSeb;
        v_Deal.FIID            := cData.FIID;
        v_Deal.MarketReportID  := cData.MarketReportID;
        v_Deal.MarketSchemeID  := cData.MarketSchemeID;
        v_Deal.MatchRef        := cData.MatchRef;
        v_Deal.NDay1_          := cData.NDay1;
        v_Deal.NDay2_          := cData.NDay2;
        v_Deal.OrderNo         := cData.OrderNo;
        v_Deal.OrdTypeCode     := CHR(1);
        v_Deal.FaceValue       := cData.FaceValue;
        v_Deal.PaymDate1_      := cData.PaymDate1;
        v_Deal.PaymDate2_      := cData.PaymDate2;
        v_Deal.Portfolio_      := cData.Portfolio;
        v_Deal.Price           := cData.Price;
        v_Deal.Price2          := CASE WHEN cData.Price2_06 = 0 THEN cData.PRICE2 ELSE cData.Price2_06 END;
        v_Deal.PriceType       := cData.PriceType;
        v_Deal.Quantity        := cData.Quantity;
        v_Deal.RecNo           := cData.RecNo; --тип?
        v_Deal.RepoPart        := 0;
        v_Deal.RepoPeriod      := cData.RepoPeriod;
        v_Deal.RepoRate        := cData.RepoRate;
        v_Deal.RepoValue       := cData.RepoValue;
        v_Deal.SecurityId      := cData.SecurityId;
        v_Deal.SecurityType    := cData.SecurityType;
        v_Deal.SettleCode      := cData.SettleCode;
        v_Deal.SettleDate      := cData.SettleDate;
        v_Deal.SettleTime      := RSI_GT.ZeroTime;
        v_Deal.TradeDate       := cData.TradeDate;
        v_Deal.TradeSessionDate:= cData.TradeSessionDate;
        v_Deal.TradeNo         := cData.TradeNo;
        v_Deal.TradeTime       := cData.TradeTime;
        v_Deal.TradeType       := cData.TradeType;
        v_Deal.TrdAccId        := CASE WHEN cData.TrdAccId_06 = CHR(1) THEN cData.TrdAccId ELSE cData.TrdAccId_06 END;
        v_Deal.UnderWr         := CASE WHEN cData.UnderWr = 0 THEN 0 ELSE 1 END;
        v_Deal.UnderWr_Pokupka := CASE WHEN cData.UnderWr_Buy = 0 THEN 0 ELSE 1 END;
        v_Deal.Value_          := cData.Value;
        v_Deal.Value2_         := cData.Value2_06;
        v_Deal.vClientCode     := cData.vClientCode;
        v_Deal.WorkSettleDate_ := cData.WorkSettleDate;
        v_Deal.IsRSHB          := p_Prm.IsRSHB;
        IF cData.RateType = 'FLOATING' and v_Deal.IsRepo = 1 THEN
          v_Deal.IsFloating := 1;
          v_Deal.BenchMark := CASE WHEN cData.BenchMark <> CHR(1) THEN GetBenchMark(cData.BenchMark) ELSE -1 END;
          v_Deal.SpredRate := cData.RepoRate;
          v_Deal.IndRate := cData.IndRate;
          v_Deal.RepoRate := cData.IncomeRate;
        ELSE
          v_Deal.IsFloating := 0;
          v_Deal.BenchMark := -1;
        END IF;

        SetPartPrmDeal_PM(v_Deal);
        SetUINNumber(v_Deal);

        IF cData.CPFirmId <> CHR(1) AND cData.CPFirmPartyID = 0 THEN
          v_ErrMes := 'Ошибка при загрузке сделки с кодом "' || v_Deal.UINNumber || '"' || CHR(10) || 'В ГКБО не найден субъект с принадлежностью "Центральный контрагент"';
        ELSIF cData.OBJECTID > 0 THEN
          v_ErrMes := 'Ошибка при загрузке сделки с кодом "' || v_Deal.UINNumber || '"' || CHR(10) || 'Запись репликации с действием создать для объекта ' || TO_CHAR(cData.OBJECTID) || ' уже передавалась в RS-Bank.' || CHR(10) || 'Повторная репликация невозможна.';
        ELSIF v_Deal.vClientCode <> CHR(1) AND v_Deal.ClientID <= 0 THEN
          v_ErrMes := 'Ошибка при загрузке сделки с кодом "' || v_Deal.UINNumber || '"' || CHR(10) || 'Не найден субъект через краткий код на бирже "' || v_Deal.vClientCode || '"';
        ELSIF v_Deal.CurrencyId <> CHR(1) AND cData.CurID = -1 THEN
          v_ErrMes := 'Ошибка при загрузке сделки с кодом "' || v_Deal.UINNumber || '"' || CHR(10) || 'В справочнике не найден ФИ с кодом "' || v_Deal.CurrencyId || '"';
        ELSIF v_Deal.SecurityId <> CHR(1) AND cData.FIID = -1 THEN
          v_ErrMes := 'Ошибка при загрузке сделки с кодом "' || v_Deal.UINNumber || '"' || CHR(10) || 'В справочнике не найден ФИ с кодом "' || v_Deal.SecurityId || '"';
        ELSIF LENGTH(TRIM(TO_CHAR(v_Deal.Quantity))) > 16 THEN
          v_ErrMes := 'Ошибка при загрузке сделки с кодом "' || v_Deal.UINNumber || '"' || CHR(10) || 'Превышено максимально возможное количество загружаемых знаков параметра Quantity';
        ELSIF SetTypeOperationBOCB(v_Deal, v_ErrMes) <> 0 THEN
          v_ErrMes:= 'Ошибка при загрузке сделки с кодом "' || v_Deal.UINNumber || '"' || CHR(10) || v_ErrMes;
        END IF;

      --IF IsFitDeal(p_Prm, v_Deal) = 1 THEN
          IF v_ErrMes = CHR(1) THEN

            IF cData.isSem06 = 0 THEN
              v_ErrMes := 'Ошибка при загрузке сделки с кодом "' || v_Deal.UINNumber || CHR(10) || '"Нет данных в файле EQM06';
            ELSE
              IF PutDealInfoPM(p_Prm, v_Deal, v_ErrMes, v_WarnMes) = 0 THEN
                --RSI_GT.DoInitRec(); --перенести объекты ЗР в накопители (нужна только при DIRECT_INSERT_NO)
                v_stat := RSI_GT.SaveBatch(BATCH_SIZE, v_ErrMes); --сохранить по мере накопления
              END IF;
            END IF;

          END IF;
        --v_NumImportDeals := v_NumImportDeals + 1; --фильтр IsFitDeal встроен в отбор записей
      --END IF;

        IF v_ErrMes <> CHR(1) THEN
          RSB_GTLOG.Error(v_ErrMes);
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
        RSB_GTLOG.Error(v_ErrMes);
      END IF;
    END IF;

    --сохранение накопленных записей лога
    RSB_GTLOG.Save;

  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_Prm.SeanceID, RSB_GTFN.RG_DEAL, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка при обработке данных в ф-и ProcessReplRecByTmp_SEM03(), ' ||
                           'SeanceID=' || TO_CHAR(p_Prm.SeanceID) || ', FileImport=' || TO_CHAR(p_Prm.FileImport) ||
                           ', Source_Code=' || p_Prm.Source_Code || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
  END;

  --Создание объектов записей репликаций по данным временной таблицы SEM03
  FUNCTION CreateReplRecByTmp_SEM03(p_SeanceID IN NUMBER, p_ImpDate IN DATE, p_SourceCode IN VARCHAR2, p_PrmStr IN VARCHAR2)
    RETURN NUMBER
  IS
    v_Prm PRMREC_t;
  BEGIN
    --инициализация используемых параметров
    SetPrmRecByStr(v_Prm, p_PrmStr);
    v_Prm.SeanceID           := p_SeanceID;
    v_Prm.imp_date           := p_ImpDate;
    v_Prm.Source_Code        := p_SourceCode;
    v_Prm.IMPORT_SETTLETIME  := GetImportSettleTime;
    v_Prm.FORM_ORDERS_IMPORT := GetFormOrdersImport;
    v_Prm.FileImport         := RSB_GTFN.FileSEM03;
    --подготовка отчетов биржи и обновление данных во временной таблице
    PrepareMarketReportsSEM03(v_Prm);

    ProcessReplRecByTmp_SEM03(v_Prm);

    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DEAL, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и CreateReplRecByTmp_SEM03(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DEAL) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;

  FUNCTION IsCliringData(p_FileImport IN NUMBER, p_Deal IN DATADEALS_t) RETURN NUMBER
  IS
  BEGIN
    IF p_FileImport <> RSB_GTFN.FileEQM06 THEN
       RETURN 0;
    END IF;

    IF   (p_Deal.InfType = 1 AND p_Deal.RepoPart = 0 AND p_Deal.BuySell = 'B') -- сделка покупки
      OR (p_Deal.InfType = 1 AND p_Deal.RepoPart = 0 AND p_Deal.BuySell = 'S') -- сделка продажи
      OR (p_Deal.InfType = 2 AND p_Deal.RepoPart = 0 AND p_Deal.BuySell = 'B') -- сделка покупки
      OR (p_Deal.InfType = 2 AND p_Deal.RepoPart = 0 AND p_Deal.BuySell = 'S') -- сделка продажи

      OR (p_Deal.InfType = 1 AND p_Deal.RepoPart = 1 AND p_Deal.BuySell = 'S') -- 1 часть сделки прямого РЕПО
      OR (p_Deal.InfType = 1 AND p_Deal.RepoPart = 1 AND p_Deal.BuySell = 'B') -- 1 часть сделки обратного РЕПО
      OR (p_Deal.InfType = 1 AND p_Deal.RepoPart = 2 AND p_Deal.BuySell = 'S') -- 2 часть сделки обратного РЕПО
      OR (p_Deal.InfType = 1 AND p_Deal.RepoPart = 2 AND p_Deal.BuySell = 'B') -- 2 часть сделки прямого РЕПО 

      OR (p_Deal.InfType = 2 AND p_Deal.RepoPart = 1 AND p_Deal.BuySell = 'S') -- 1 часть сделки прямого РЕПО  
      OR (p_Deal.InfType = 2 AND p_Deal.RepoPart = 1 AND p_Deal.BuySell = 'B') -- 1 часть сделки обратного РЕПО
      OR (p_Deal.InfType = 2 AND p_Deal.RepoPart = 2 AND p_Deal.BuySell = 'S') -- 2 часть сделки обратного РЕПО
      OR (p_Deal.InfType = 2 AND p_Deal.RepoPart = 2 AND p_Deal.BuySell = 'B') -- 2 часть сделки прямого РЕПО 
    THEN
      RETURN 1;
    END IF;
  
    RETURN 0;
  END;

  --Обработка записей по данным буферной таблицы
  PROCEDURE ProcessReplRecByTmp_EQM06(p_Prm IN OUT NOCOPY PRMREC_t)
  IS
    v_Prm     PRMREC_t;
    v_Deal    DATADEALS_t;
    v_stat    NUMBER(5) := 0;
    v_ErrMes  VARCHAR2(1000) := CHR(1);
    v_WarnMes VARCHAR2(1000) := CHR(1);
    v_Skip    NUMBER(5) := 0;
  BEGIN
    --инициализация глобального лога для накопления записей для вставки
    RSB_GTLOG.Init(BATCH_SIZE, p_Prm.SeanceID, RSB_GTFN.RG_CLEARING);

    --инициализация глобального накопителя объектов ЗР для вставки
    v_stat := RSI_GT.Init(RSI_GT.DIRECT_INSERT_YES, p_Prm.SeanceID, p_Prm.Source_Code, RSI_GT.GTCODE_RECEIVE, v_ErrMes);
    IF v_stat <> 0 THEN
      RSB_GTLOG.Error(v_ErrMes);
    END IF;

    IF v_stat = 0 THEN
      FOR cData IN (SELECT t.T_ID_MB_EQM06 ID_MB_EQM06,
                           DECODE((SELECT NVL(LOWER(T_SECURITYTYPE), '')
                                     FROM D_SEM03_TMP sem03_KSU
                                    WHERE sem03_KSU.T_SECURITYID = t.T_SECURITYID AND ROWNUM = 1),
                                  'ксу', 'ксу', 'Єбг', 'ксу', '') SECURITYTYPE,
                           NVL(t.T_RECNO, 0) RECNO,
                           NVL(t.T_EXTSETTLECODE, CHR(1)) EXTSETTLECODE,
                           NVL(t.T_CURRENCYID, CHR(1)) CURRENCYID,
                           NVL(t.T_INFTYPE, 0) INFTYPE,
                           NVL(t.T_SESSIONNUM, -1) SESSIONNUM,
                           NVL(t.T_SETTLEDATE, RSI_GT.ZeroDate) SETTLEDATE,
                           NVL(t.T_BOARDID, CHR(1)) BOARDID,
                           NVL(t.T_SECURITYID, CHR(1)) SECURITYID,
                           NVL(t.T_PRICETYPE, CHR(1)) PRICETYPE,
                           NVL(t.T_REPOPART, 0) REPOPART,
                           NVL(t.T_TRDACCID, CHR(1)) TRDACCID,
                           NVL(t.T_CLIENTCODE, CHR(1)) CLIENTCODE,
                           NVL(t.T_vCLIENTCODE, CHR(1)) vCLIENTCODE,
                           NVL(t.T_BUYSELL, CHR(1)) BUYSELL,
                           NVL(t.T_TRADEDATE, RSI_GT.ZeroDate) TRADEDATE,
                           NVL(t.T_TRADENO, 0) TRADENO,
                           NVL(t.T_SETTLECODE, CHR(1)) SETTLECODE,
                           NVL(t.T_DECIMALS, 0) DECIMALS,
                           NVL(t.T_PRICE, 0) PRICE,
                           NVL(t.T_VALUE, 0) VALUE,
                           NVL(t.T_AMOUNT, 0) AMOUNT,
                           NVL(t.T_QUANTITY, 0) QUANTITY,
                           NVL(t.T_EXCHCOMM, 0) EXCHCOMM,
                           NVL(t.T_ACCINT, 0) ACCINT,
                           NVL(t.T_REPOPERIOD, 0) REPOPERIOD,
                           NVL(t.T_REPORTTIME, RSI_GT.ZeroDate) REPORTTIME,
                           NVL(t.T_SETTLETIME, RSI_GT.ZeroDate) SETTLETIME,
                           NVL(DECODE(t.T_REPOPART, 1, Repo_2.t_ACCINT, 0), 0) ACCINT2,
                           NVL(DECODE(t.T_REPOPART, 1, Repo_2.t_AMOUNT, 0), 0) AMOUNT2,
                           NVL(DECODE(t.T_REPOPART, 1, Repo_2.t_VALUE, 0), 0) VALUE2,
                           NVL(DECODE(t.T_REPOPART, 1, Repo_2.t_PRICE, 0), 0) PRICE2,
                           (CASE WHEN NVL(Repo_2.T_ID_MB_EQM06, 0) = 0
                                 THEN 0
                                 ELSE 1
                             END) IsPart2,
                           NVL(Repo_2.T_SETTLEDATE, RSI_GT.ZeroDate) SETTLEDATE2,
                           (CASE WHEN t.T_REPOPART = 1 AND t.T_BOARDID = 'RPNG' AND t.T_INFTYPE = 2
                                 THEN NVL (Repo_2.t_CLRCOMM, 0)
                                 ELSE 0
                             END) CLRCOMM,
                           NVL(t.T_CPFirmId, CHR(1)) CPFirmId,
                           NVL(t.T_IsSEB, 0) ISSEB,
                           NVL(t.T_PARTYID, 0) PARTYID, 
                           NVL(t.T_SFCONTRID, 0) SFCONTRID, 
                           NVL(t.T_CPFIRMPARTYID, 0) CPFIRMPARTYID, 
                           NVL(t.T_CURID, 0) CURID, 
                           NVL(t.T_FIID, 0) FIID, 
                           NVL(t.T_CALENDID, 0) CALENDID, 
                           NVL(t.T_MARKETSCHEMEID, 0) MARKETSCHEMEID, 
                           NVL(t.T_NDAY1, 0) NDAY1, 
                           NVL(t.T_NDAY2, 0) NDAY2, 
                           NVL(t.T_PAYMDATE1, RSI_GT.ZeroDate) PAYMDATE1, 
                           NVL(t.T_PAYMDATE2, RSI_GT.ZeroDate) PAYMDATE2, 
                           NVL(t.T_CALCITOGDATE, RSI_GT.ZeroDate) CALCITOGDATE, 
                           NVL(t.T_WORKSETTLEDATE, RSI_GT.ZeroDate) WORKSETTLEDATE, 
                           NVL(t.T_OBJECTID, 0) OBJECTID, 
                           NVL(t.T_PORTFOLIO, 0) PORTFOLIO, 
                           NVL(t.T_DOC_NO, CHR(1)) DOC_NO,
                           NVL(t.T_IsREPO, 0) IsREPO,
                           NVL(t.T_UNDERWR, 0) UNDERWR,
                           NVL(t.T_UNDERWR_BUY, 0) UNDERWR_BUY,
                           NVL(t.t_RateType, CHR(1)) RateType,
                           NVL(t.t_BenchMark, CHR(1)) BenchMark,
                           NVL(t.t_RepoRate, 0) SpredRate,
                           NVL(t.t_BenchMarkRate, 0) IndRate,
                           NVL(t.t_CurRepoRate, 0) IncomeRate
                      FROM D_EQM06_TMP t, D_EQM06_TMP Repo_2
                     WHERE t.T_INFTYPE IN (1, 2)
                       AND NOT EXISTS(SELECT 1
                                        FROM D_EQM06_TMP Repo_1
                                       WHERE t.t_tradeno = Repo_1.t_tradeno
                                         AND Repo_1.T_INFTYPE = 1
                                         AND Repo_1.T_REPOPART = 1
                                         AND t.T_INFTYPE = 1
                                         AND t.T_REPOPART = 2)
                       AND t.t_tradeno = Repo_2.t_tradeno(+)
                       AND Repo_2.T_INFTYPE(+) IN (1, 2, 3, 6)
                       AND Repo_2.T_REPOPART(+) = 2
                       AND t.t_vClientCode = Repo_2.t_vClientCode(+)
                     ORDER BY t.T_ID_MB_EQM06
                   )
      LOOP
        v_ErrMes  := CHR(1);
        v_WarnMes := CHR(1);
        v_Skip    := 0;

        v_Deal.AccInt          := cData.AccInt;
        v_Deal.AccInt2         := cData.AccInt2;
        v_Deal.Amount          := cData.Amount;
        v_Deal.Amount2         := cData.Amount2;
        v_Deal.BoardId         := cData.BoardId;
        v_Deal.BoardType       := CHR(1);
        v_Deal.BrokerRef       := CHR(1);
        v_Deal.BuySell         := cData.BuySell;
        v_Deal.CalcItogDate_   := cData.CalcItogDate;
        v_Deal.CalendID        := cData.CalendID;
        v_Deal.ClientCode      := cData.ClientCode;
        v_Deal.ClientContrID   := cData.SfContrID;
        v_Deal.ClientID        := cData.PartyID;
        v_Deal.ClrComm         := cData.ClrComm;
        v_Deal.CPFirmId        := cData.CPFirmId;
        v_Deal.CPFirmId_ID     := cData.CPFirmPartyID;
        v_Deal.CurrencyId      := cData.CurrencyId;
        v_Deal.CurrencyId_FIID := cData.CurID;
        v_Deal.Decimals        := cData.Decimals;
        v_Deal.DOC_NO          := cData.DOC_NO;
        v_Deal.ExhComm         := cData.EXCHCOMM;
        v_Deal.ExtSettleCode   := cData.ExtSettleCode;
        v_Deal.InfType         := cData.InfType;
        v_Deal.IsRepo          := cData.IsRepo;
        v_Deal.IsSeb           := cData.IsSeb;
        v_Deal.FIID            := cData.FIID;
        v_Deal.MarketReportID  := 0;
        v_Deal.MarketSchemeID  := cData.MarketSchemeID;
        v_Deal.MatchRef        := CHR(1);
        v_Deal.NDay1_          := cData.NDay1;
        v_Deal.NDay2_          := cData.NDay2;
        v_Deal.OrderNo         := CHR(1);
        v_Deal.OrdTypeCode     := CHR(1);
        v_Deal.FaceValue       := 0;
        v_Deal.PaymDate1_      := cData.PaymDate1;
        v_Deal.PaymDate2_      := cData.PaymDate2;
        v_Deal.Portfolio_      := cData.Portfolio;
        v_Deal.Price           := cData.Price;
        v_Deal.Price2          := cData.Price2;
        v_Deal.PriceType       := cData.PriceType;
        v_Deal.Quantity        := cData.Quantity;
        v_Deal.RecNo           := 0;
        v_Deal.RepoPart        := cData.RepoPart;
        v_Deal.RepoPeriod      := cData.RepoPeriod;
        v_Deal.RepoRate        := 0;
        v_Deal.RepoValue       := 0;
        v_Deal.SecurityId      := cData.SecurityId;
        v_Deal.SecurityType    := NVL(cData.SecurityType, CHR(1));
        v_Deal.Session         := cData.SessionNum;
        v_Deal.SettleCode      := cData.SettleCode;
        v_Deal.SettleDate      := cData.SettleDate;
        v_Deal.SettleTime      := cData.SettleTime;
        v_Deal.TradeDate       := cData.TradeDate;
        v_Deal.TradeNo         := cData.TradeNo;
        v_Deal.TradeTime       := cData.ReportTime;
        v_Deal.TradeType       := CHR(1);
        v_Deal.TrdAccId        := cData.TrdAccId;
        v_Deal.UnderWr         := CASE WHEN cData.UnderWr = 0 THEN 0 ELSE 1 END;
        v_Deal.UnderWr_Pokupka := CASE WHEN cData.UnderWr_Buy = 0 THEN 0 ELSE 1 END;
        v_Deal.Value_          := cData.Value;
        v_Deal.Value2_         := cData.Value2;
        v_Deal.vClientCode     := cData.vClientCode;
        v_Deal.WorkSettleDate_ := cData.WorkSettleDate;
        v_Deal.IsRSHB          := p_Prm.IsRSHB;
        IF cData.RateType = 'FLOATING' AND v_Deal.IsRepo = 1 THEN
          v_Deal.IsFloating := 1;
          v_Deal.BenchMark := CASE WHEN cData.BenchMark <> CHR(1) THEN GetBenchMark(cData.BenchMark) ELSE -1 END;
          v_Deal.RepoRate := cData.IncomeRate;
          v_Deal.SpredRate := cData.SpredRate;
          v_Deal.IndRate := cData.IndRate;
        ELSE
          v_Deal.IsFloating := 0;
          v_Deal.BenchMark := -1;
          v_Deal.SpredRate := 0;
          v_Deal.IndRate := 0;
        END IF;

        SetUINNumber(v_Deal);

        IF v_Deal.RepoPart = 1 AND cData.IsPart2 = 1 THEN
          RSB_GTFN.FindDataPart2(v_Deal.SettleDate, cData.SettleDate2, v_Deal.Amount, v_Deal.Amount2, v_Deal.RepoRate);
        END IF;

        IF cData.CPFirmId <> CHR(1) AND cData.CPFirmPartyID = 0 THEN
          v_ErrMes := 'Ошибка при загрузке сделки с кодом "' || v_Deal.UINNumber || '"' || CHR(10) || 'В ГКБО не найден субъект с принадлежностью "Центральный контрагент"';
        ELSIF cData.OBJECTID > 0 THEN
          v_ErrMes := 'Ошибка при загрузке сделки с кодом "' || v_Deal.UINNumber || '"' || CHR(10) || 'Запись репликации с действием создать для объекта ' || TO_CHAR(cData.OBJECTID) || ' уже передавалась в RS-Bank.' || CHR(10) || 'Повторная репликация невозможна.';
        ELSIF v_Deal.vClientCode <> CHR(1) AND v_Deal.ClientID <= 0 THEN
          v_ErrMes := 'Ошибка при загрузке сделки с кодом "' || v_Deal.UINNumber || '"' || CHR(10) || 'Не найден субъект через краткий код на бирже "' || v_Deal.vClientCode || '"';
        ELSIF v_Deal.BoardId <> CHR(1) AND CheckBoardIsTech(v_Deal.BoardId) = 1 THEN
          RSB_GTLOG.Message(
            'Техническая сделка вида "' || v_Deal.BoardId || '" с кодом "' || v_Deal.UINNumber ||
            '", не подлежит автоматической загрузке, требуется ввести и обработать сделку в ручном режиме после расчета лимитов',
            RSB_GTLOG.ISSUE_LOAD_WARNING
          );
          v_Skip := 1;
        ELSIF v_Deal.CurrencyId <> CHR(1) AND cData.CurID = -1 THEN
          v_ErrMes := 'Ошибка при загрузке сделки с кодом "' || v_Deal.UINNumber || '"' || CHR(10) || 'В справочнике не найден ФИ с кодом "' || v_Deal.CurrencyId || '"';
        ELSIF v_Deal.SecurityId <> CHR(1) AND cData.FIID = -1 THEN
          v_ErrMes := 'Ошибка при загрузке сделки с кодом "' || v_Deal.UINNumber || '"' || CHR(10) || 'В справочнике не найден ФИ с кодом "' || v_Deal.SecurityId || '"';
        ELSIF LENGTH(TRIM(TO_CHAR(v_Deal.Quantity))) > 16 THEN
          v_ErrMes := 'Ошибка при загрузке сделки с кодом "' || v_Deal.UINNumber || '"' || CHR(10) || 'Превышено максимально возможное количество загружаемых знаков параметра Quantity';
        END IF;

      --IF IsFitDeal(p_Prm, v_Deal) = 1 THEN
          IF v_ErrMes = CHR(1) AND v_Skip = 0 THEN
            IF v_Deal.BoardId = 'RPNG' AND v_Deal.InfType = 2 THEN
              v_Deal.IsCliring := 0;
              SetPartPrmDeal_PM(v_Deal);

              IF SetTypeOperationBOCB(v_Deal, v_ErrMes) <> 0 THEN
                v_ErrMes:= 'Ошибка при загрузке сделки с кодом "' || v_Deal.UINNumber || '"' || CHR(10) || v_ErrMes;
              ELSE
                IF PutDealInfoPM(p_Prm, v_Deal, v_ErrMes, v_WarnMes) = 0 THEN
                  v_stat := RSI_GT.SaveBatch(BATCH_SIZE, v_ErrMes); --сохранить по мере накопления
                END IF;
              END IF;

              IF v_ErrMes <> CHR(1) THEN
                RSB_GTLOG.Error(v_ErrMes);
                v_ErrMes := CHR(1);
              END IF;
            END IF;

            IF IsCliringData(p_Prm.FileImport, v_Deal) = 1 THEN
              v_Deal.IsCliring := 1;
              SetPartPrmDeal_PM(v_Deal);

              IF SetTypeOperationBOCB(v_Deal, v_ErrMes) <> 0 THEN
                v_ErrMes:= 'Ошибка при загрузке сделки с кодом "' || v_Deal.UINNumber || '"' || CHR(10) || v_ErrMes;
              ELSE
                IF PutDealInfoPM(p_Prm, v_Deal, v_ErrMes, v_WarnMes) = 0 THEN
                  v_stat := RSI_GT.SaveBatch(BATCH_SIZE, v_ErrMes); --сохранить по мере накопления
                END IF;
              END IF;
            END IF;
          END IF;
        --v_NumImportDeals := v_NumImportDeals + 1; --фильтр IsFitDeal встроен в отбор записей
      --END IF;

        IF v_ErrMes <> CHR(1) THEN
          RSB_GTLOG.Error(v_ErrMes);
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
        RSB_GTLOG.Error(v_ErrMes);
      END IF;
    END IF;

    --сохранение накопленных записей лога
    RSB_GTLOG.Save;

  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_Prm.SeanceID, RSB_GTFN.RG_CLEARING, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка при обработке данных в ф-и ProcessReplRecByTmp_EQM06(), ' ||
                           'SeanceID=' || TO_CHAR(p_Prm.SeanceID) || ', FileImport=' || TO_CHAR(p_Prm.FileImport) ||
                           ', Source_Code=' || p_Prm.Source_Code || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
  END;

  --Создание объектов записей репликаций по данным временной таблицы EQM06
  FUNCTION CreateReplRecByTmp_EQM06(p_SeanceID IN NUMBER, p_ImpDate IN DATE, p_SourceCode IN VARCHAR2, p_PrmStr IN VARCHAR2)
    RETURN NUMBER
  IS
    v_Prm PRMREC_t;
  BEGIN
    --инициализация используемых параметров
    SetPrmRecByStr(v_Prm, p_PrmStr);
    v_Prm.SeanceID           := p_SeanceID;
    v_Prm.imp_date           := p_ImpDate;
    v_Prm.Source_Code        := p_SourceCode;
    v_Prm.IMPORT_SETTLETIME  := GetImportSettleTime;
    v_Prm.FORM_ORDERS_IMPORT := GetFormOrdersImport;
    v_Prm.FileImport         := RSB_GTFN.FileEQM06;
    --подготовка отчетов биржи и обновление данных во временной таблице
    PrepareMarketReportsEQM06(v_Prm);

    ProcessReplRecByTmp_EQM06(v_Prm);

    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_CLEARING, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и CreateReplRecByTmp_EQM06(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_CLEARING) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;
  
  FUNCTION DelEQM2T(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER
  IS
  BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE D_EQM2T_TMP';
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_REQ, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и DelEQM2T(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=EQM2T' || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_REQ) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;
  
  FUNCTION DelEQM3T(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER
  IS
  BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE D_EQM3T_TMP';
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DEAL, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и DelEQM3T(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=EQM3T' || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DEAL) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;
  
  FUNCTION FillEQM2T(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2, p_ImpDate IN DATE)
    RETURN NUMBER
  IS
  BEGIN
    EXECUTE IMMEDIATE
      'INSERT INTO D_EQM2T_TMP(
         T_ID_MB_EQM2T            
        ,T_ID_MB_REQUISITES      
        ,T_TRADEDATE 
        ,T_BOARDID             
        ,T_SESSIONNO             
        ,T_RECNO                 
        ,T_ORDERNO               
        ,T_STATUS                
        ,T_BUYSELL                
        ,T_SECURITYID            
        ,T_PRICETYPE              
        ,T_CURRENCYID             
        ,T_PRICE                  
        ,T_QUANTITY              
        ,T_ENTRYTIME              
        ,T_AMENDTIME              
        ,T_BROKERREF                            
        ,T_CLIENTCODE             
        ,T_TRDACCID                       
        ,T_ORDTYPECODE           
        ,T_PARTYID           
        ,T_SFCONTRID           
        ,T_OBJECTID 
        ,T_vClientCode 
        )
        (SELECT
         T.ID_MB_EQM2T         
        ,NVL(T.ID_MB_REQUISITES, 0)          
        ,NVL(T.TRADEDATE, TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')) 
        ,NVL(T.BOARDID, CHR(1))             
        ,(SELECT NVL(ORD_NUM, 0) FROM '||p_Synonim||'.PROCESSING_ACTUAL WHERE ID_MB_REQUISITES = T.ID_MB_REQUISITES AND FILE_TYPE = ''EQM2T'' AND TRADE_DATE = :p_ImpDate AND ROWNUM = 1)         
        ,NVL(T.RECNO, 0)                  
        ,NVL(T.ORDERNO, CHR(1))                
        ,NVL(T.STATUS, CHR(1))                 
        ,NVL(T.BUYSELL, CHR(1))                
        ,NVL(T.SECURITYID, CHR(1))             
        ,NVL(T.PRICETYPE, CHR(1))              
        ,NVL(T.CURRENCYID, CHR(1))           
        ,NVL(T.PRICE, 0)                  
        ,NVL(T.QUANTITY, 0)               
        ,NVL(T.ENTRYTIME, TO_DATE(''01.01.0001 00:00:00'', ''dd.mm.yyyy HH24:MI:SS''))              
        ,NVL(T.AMENDTIME, TO_DATE(''01.01.0001 00:00:00'', ''dd.mm.yyyy HH24:MI:SS''))              
        ,NVL(T.BROKERREF, CHR(1))                            
        ,NVL(T.CLIENTCODE, CHR(1))             
        ,NVL(T.TRDACCID, CHR(1))                  
        ,NVL(T.ORDTYPECODE, CHR(1))                   
        ,0           
        ,0           
        ,0  
        ,NVL(CASE WHEN NVL(CLIENTCODE, CHR(1)) <> CHR(1)  
                  THEN CLIENTCODE 
                  ELSE CASE WHEN NVL(BROKERREF, CHR(1)) <> CHR(1) 
                            THEN CASE WHEN INSTR(BROKERREF, ''/'') > 0 
                                      THEN SUBSTR(BROKERREF, 1, INSTR(BROKERREF, ''/'') - 1) 
                                      ELSE CHR(1) 
                                  END 
                            ELSE CHR(1) 
                        END 
              END, 
             CHR(1))  
           FROM '||p_Synonim||'.MB_EQM2T T
          WHERE T.ID_MB_REQUISITES IN (SELECT ID_MB_REQUISITES FROM '||p_Synonim||'.PROCESSING_ACTUAL WHERE FILE_TYPE = ''EQM2T'' AND TRADE_DATE = :p_ImpDate) 
            AND NVL(T.STATUS, CHR(1)) <> ''O'' 
            AND NVL(T.TRADEDATE, TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')) = :p_ImpDate 
            AND T.ZRID = 0
        )'
    USING IN p_ImpDate, IN p_ImpDate, IN p_ImpDate;
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_REQ, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и FillEQM2T(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=EQM2T' || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_REQ) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;
  
  FUNCTION UpdateObjEQM2T(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER
  IS
    TYPE ObjID_t IS TABLE OF DGTCODE_DBT.T_OBJECTID%TYPE;
    TYPE SemID_t IS TABLE OF D_EQM2T_TMP.T_ID_MB_EQM2T%TYPE;
    v_ObjIDs ObjID_t;
    v_SemID  SemID_t;
  BEGIN
    SELECT GTCODE.T_OBJECTID, EQM2T.T_ID_MB_EQM2T
      BULK COLLECT INTO v_ObjIDs, v_semID
      FROM DGTCODE_DBT GTCODE, D_EQM2T_TMP EQM2T
     WHERE GTCODE.T_APPLICATIONID IN (RSB_GTFN.GT_PAYMENTS, RSB_GTFN.GT_MMVB3)
       AND GTCODE.T_OBJECTKIND = RSB_GTFN.RG_REQ
       AND GTCODE.T_OBJECTCODE = EQM2T.T_ORDERNO;
    IF v_semID.COUNT <> 0 THEN
      FORALL i IN v_semID.FIRST .. v_semID.LAST
        UPDATE D_EQM2T_TMP EQM2T
           SET EQM2T.T_OBJECTID = v_ObjIDs (i)
         WHERE EQM2T.T_ID_MB_EQM2T = v_semID (i);
     END IF;
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_REQ, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и UpdateObjEQM2T(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=EQM2T' || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_REQ) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;
  
  FUNCTION UpdateEQM2T_PM(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2)
    RETURN NUMBER
  IS
    TYPE ObjID_t IS TABLE OF DGTSNCREC_DBT.T_RECORDID%TYPE;
    TYPE PmID_t IS TABLE OF D_EQM2T_TMP.T_ID_MB_EQM2T%TYPE;
    v_ObjIDs ObjID_t;
    v_PmID   PmID_t;
  BEGIN
    SELECT EQM2T.T_ID_MB_EQM2T, GTSNCREC.T_RECORDID
      BULK COLLECT INTO v_PmID, v_ObjIDs
      FROM dgtsncrec_dbt gtsncrec,
           dgtcode_dbt gtcode,
           dgtobject_dbt gtobject,
           D_EQM2T_TMP EQM2T
     WHERE gtsncrec.t_seanceid = p_SeanceID
       AND GTCODE.T_OBJECTKIND = RSB_GTFN.RG_REQ
       AND GTCODE.T_OBJECTID = GTSNCREC.T_OBJECTID
       AND GTOBJECT.T_OBJECTID = GTSNCREC.T_OBJECTID
       AND GTCODE.T_OBJECTCODE = EQM2T.T_ORDERNO;

    IF v_PmID.COUNT <> 0 THEN
      FORALL i IN v_PmID.FIRST .. v_PmID.LAST
        EXECUTE IMMEDIATE
          'UPDATE '||p_Synonim||'.MB_EQM2T EQM2T
              SET ZRID = :v_ObjIDsi
            WHERE ID_MB_EQM2T = :v_PmIDi'
        USING IN v_ObjIDs(i), IN v_PmID(i);
    END IF;
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_REQ, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и UpdateEQM2T_PM(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=EQM2T' || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_REQ) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;
  
  --Заполнение и сохранение параметров заявки на внебиржевую сделку
  FUNCTION PutRequestOTCInfo(p_Prm IN OUT NOCOPY PRMREC_t, p_Req IN OUT NOCOPY D_EQM2T_TMP%ROWTYPE, p_ErrMes IN OUT NOCOPY VARCHAR2, p_WarnMes IN OUT NOCOPY VARCHAR2) RETURN NUMBER
  IS
    v_stat       NUMBER(5) := 0;
    v_TraderCode D_EQM2T_TMP.t_BrokerRef%TYPE;
  BEGIN
    v_stat := RSI_GT.InitRec(RSB_GTFN.RG_REQ,
                             'Заявка на внебиржевую сделку №' || p_Req.t_OrderNo, --Наименование объекта
                             0, --проверять существование объекта в системе по коду
                             p_Req.t_OrderNo, --код объекта
                             p_Req.t_PartyID, --ID клиента
                             p_ErrMes);

    IF v_stat = 0 THEN
      RSI_GT.SetParmByName('RGDLRQ_KIND',        DOCKIND_REQ_CLIENT_DEAL);
      RSI_GT.SetParmByName('RGDLRQ_CODE',        p_Req.t_OrderNo);
      RSI_GT.SetParmByName('RGDLRQ_CODETS',      p_Req.t_OrderNo);
      RSI_GT.SetParmByName('RGDLRQ_DATE',        p_Req.t_TradeDate);
      RSI_GT.SetParmByName('RGDLRQ_TIME',        p_Req.t_EntryTime);
      RSI_GT.SetParmByName('RGDLRQ_PARTY',       p_Prm.MMVB_Code); 
      RSI_GT.SetParmByName('RGDLRQ_BUYSALE',     p_Req.t_BuySell);
      RSI_GT.SetParmByName('RGDLRQ_FIID',        p_Req.t_SecurityId);
      RSI_GT.SetParmByName('RGDLRQ_AMOUNT',      p_Req.t_Quantity);
      RSI_GT.SetParmByName('RGDLRQ_PRICE',       p_Req.t_Price);
      RSI_GT.SetParmByName('RGDLRQ_STATUS',      p_Req.t_Status);
      RSI_GT.SetParmByName('RGDLRQ_PRICETYPE',   p_Req.t_PriceType);
      RSI_GT.SetParmByName('RGDLRQ_CURRENCYID',  p_Req.t_CurrencyId);
      RSI_GT.SetParmByName('RGDLRQ_PRICEFIID',   p_Req.t_CurrencyId); --валюта цены = валюте расчетов
      RSI_GT.SetParmByName('RGDLRQ_ORDTYPECODE', p_Req.t_OrdTypeCode);
      RSI_GT.SetParmByName('RGDLRQ_GRNDNUM',     p_Req.t_SessionNo || p_Req.t_RecNo);
      RSI_GT.SetParmByName('RGDLRQ_CLIENT',      p_Req.t_vClientCode);
      RSI_GT.SetParmByName('RGDLRQ_ISOTC',       RSB_GTFN.SET_CHAR);

      v_TraderCode := RSB_GTFN.GetRefNote(p_Req.t_BrokerRef, '/');
      IF v_TraderCode = 'spec' THEN
        v_TraderCode := CHR(1);
      END IF;
      RSI_GT.SetParmByName('RGDLRQ_TRADERCODE', v_TraderCode);

      IF p_Req.t_BrokerRef <> CHR(1) THEN
        RSI_GT.SetParmByName('RGDLRQ_BROKERREF', p_Req.t_BrokerRef);
      END IF;

      --проверка ошибок после добавления параметров
      IF RSI_GT.GetLastError <> CHR(1) THEN
        v_stat := 1;
        p_ErrMes := RSI_GT.GetLastError;
      END IF;
    END IF;

    RETURN v_stat;
  END;

  --Обработка записей по данным буферной таблицы
  PROCEDURE ProcessReplRecByTmp_EQM2T(p_Prm IN OUT NOCOPY PRMREC_t)
  IS
    TYPE CDATA_t IS REF CURSOR RETURN D_EQM2T_TMP%ROWTYPE;
    v_cData   CDATA_t;
    v_Prm     PRMREC_t;
    v_Req     D_EQM2T_TMP%ROWTYPE;
    v_stat    NUMBER(5) := 0;
    v_ErrMes  VARCHAR2(1000) := CHR(1);
    v_WarnMes VARCHAR2(1000) := CHR(1);
    v_Skip    NUMBER(5) := 0;
  BEGIN
    --инициализация глобального лога для накопления записей для вставки
    RSB_GTLOG.Init(BATCH_SIZE, p_Prm.SeanceID, RSB_GTFN.RG_REQ);

    --инициализация глобального накопителя объектов ЗР для вставки
    v_stat := RSI_GT.Init(RSI_GT.DIRECT_INSERT_YES, p_Prm.SeanceID, p_Prm.Source_Code, RSI_GT.GTCODE_RECEIVE, v_ErrMes);
    IF v_stat <> 0 THEN
      RSB_GTLOG.Error(v_ErrMes);
    END IF;

    IF v_stat = 0 THEN
      OPEN v_cData FOR
           --важен порядок и наличие всех полей, чтобы сработал FETCH
           SELECT T_ID_MB_EQM2T,
                  T_ID_MB_REQUISITES,
                  NVL(t.T_TRADEDATE, RSI_GT.ZeroDate) TRADEDATE,
                  NVL(t.T_BOARDID, CHR(1)) BOARDID,
                  NVL(t.T_SESSIONNO, 0) SESSIONNO,
                  NVL(t.T_RECNO, 0) RECNO,
                  NVL(t.T_ORDERNO, CHR(1)) ORDERNO,
                  NVL(t.T_STATUS, CHR(1)) STATUS,
                  NVL(t.T_BUYSELL, CHR(1)) BUYSELL,
                  NVL(t.T_SECURITYID, CHR(1)) SECURITYID,
                  NVL(t.T_PRICETYPE, CHR(1)) PRICETYPE,
                  NVL(t.T_CURRENCYID, CHR(1)) CURRENCYID,
                  NVL(t.T_PRICE, 0) PRICE,
                  NVL(t.T_QUANTITY, 0) QUANTITY,
                  NVL(t.T_ENTRYTIME, RSI_GT.ZeroDate) ENTRYTIME,
                  NVL(t.T_AMENDTIME, RSI_GT.ZeroDate) AMENDTIME,
                  NVL(t.T_BROKERREF, CHR(1)) BROKERREF,
                  NVL(t.T_CLIENTCODE, CHR(1)) CLIENTCODE,
                  NVL(t.T_TRDACCID, CHR(1)) TRDACCID,
                  NVL(t.T_ORDTYPECODE, CHR(1)) ORDTYPECODE,
                  NVL(t.T_PARTYID, 0) PARTYID,
                  NVL(t.T_SFCONTRID, 0) SFCONTRID,
                  NVL(t.T_OBJECTID, 0) OBJECTID,
                  NVL(t.T_vCLIENTCODE, CHR(1)) vCLIENTCODE
            FROM D_EQM2T_TMP t
           WHERE t.T_TRADEDATE = p_Prm.imp_date
           ORDER BY T_SESSIONNO, T_RECNO;
      LOOP
        FETCH v_cData INTO v_Req;
        EXIT WHEN v_cData%NOTFOUND OR v_cData%NOTFOUND IS NULL;

        v_ErrMes  := CHR(1);
        v_WarnMes := CHR(1);
        v_Skip    := 0;

        IF v_Req.t_ObjectID > 0 THEN
          v_ErrMes := 'Ошибка при загрузке заявки на внебиржевую сделку № "' || v_Req.t_OrderNo || '"' || CHR(10) || 'Запись репликации с действием создать для объекта ' || TO_CHAR(v_Req.t_ObjectID) || ' уже передавалась в RS-Bank.' || CHR(10) || 'Повторная репликация невозможна.';
        ELSIF v_Req.t_vClientCode <> CHR(1) AND v_Req.t_PartyID <= 0 THEN
          v_ErrMes := 'Ошибка при загрузке заявки на внебиржевую сделку № "' || v_Req.t_OrderNo || '"' || CHR(10) || 'Не найден субъект через краткий код на бирже "' || v_Req.t_vClientCode || '"';
        ELSIF LENGTH(TRIM(TO_CHAR(v_Req.t_Quantity))) > 16 THEN
          v_ErrMes := 'Ошибка при загрузке заявки на внебиржевую сделку № "' || v_Req.t_OrderNo || '"' || CHR(10) || 'Превышено максимально возможное количество загружаемых знаков параметра Quantity';
       /* ELSIF v_Req.t_vClientCode = CHR(1) THEN
          RSB_GTLOG.Message(
            'ПРЕДУПРЕЖДЕНИЕ: Не загружена заявка на внебиржевую сделку № "' || v_Req.t_OrderNo || '"' || CHR(10) || 'В файле импорта не заданы атрибуты ClientCode и BrokerRef',
            RSB_GTLOG.ISSUE_LOAD_WARNING
          );
          v_Skip := 1;*/
        END IF;

        IF v_ErrMes = CHR(1) and v_Skip = 0 THEN
          IF PutRequestOTCInfo(p_Prm, v_Req, v_ErrMes, v_WarnMes) = 0 THEN
            v_stat := RSI_GT.SaveBatch(BATCH_SIZE, v_ErrMes); --сохранить по мере накопления
          END IF;
        END IF;

        IF v_ErrMes <> CHR(1) THEN
          RSB_GTLOG.Error(v_ErrMes);
        END IF;

        IF v_WarnMes <> CHR(1) THEN
          RSB_GTLOG.Warning(v_WarnMes);
        END IF;
      END LOOP;
      CLOSE v_cData;
    END IF;

    IF v_stat = 0 THEN
      --сохранение накопленных объектов ЗР
      v_stat := RSI_GT.Save(v_ErrMes);
      IF v_stat <> 0 THEN
        RSB_GTLOG.Error(v_ErrMes);
      END IF;
    END IF;

    --сохранение накопленных записей лога
    RSB_GTLOG.Save;

  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_Prm.SeanceID, RSB_GTFN.RG_REQ, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка при обработке данных в ф-и ProcessReplRecByTmp_EQM2T(), ' ||
                           'SeanceID=' || TO_CHAR(p_Prm.SeanceID) || ', FileImport=EQM2T' || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_REQ) ||
                           ', Source_Code=' || p_Prm.Source_Code || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
  END;

  --Создание объектов записей репликаций по данным временной таблицы EQM2T
  FUNCTION CreateReplRecByTmp_EQM2T(p_SeanceID IN NUMBER, p_ImpDate IN DATE, p_SourceCode IN VARCHAR2, p_MMVB_Code IN VARCHAR2)
    RETURN NUMBER
  IS
    v_Prm PRMREC_t;
  BEGIN
    --инициализация используемых параметров
    v_Prm.SeanceID    := p_SeanceID;
    v_Prm.imp_date    := p_ImpDate;
    v_Prm.Source_Code := p_SourceCode;
    v_Prm.MMVB_Code   := p_MMVB_Code;

    ProcessReplRecByTmp_EQM2T(v_Prm);

    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_REQ, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и CreateReplRecByTmp_EQM2T(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=EQM2T' || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_REQ) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;
  FUNCTION FillEQM3T(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2, p_ImpDate IN DATE)
    RETURN NUMBER
  IS
  BEGIN  
    EXECUTE IMMEDIATE
      'INSERT INTO D_EQM3T_TMP(
          T_ID_MB_EQM3T 
        , T_ID_MB_REQUISITES 
        , T_CURRENCYID 
        , T_BOARDID 
        , T_BOARDNAME 
        , T_TRADEDATE 
        , T_SETTLEDATE 
        , T_SECURITYID 
        , T_PRICETYPE 
        , T_TRDACCID 
        , T_TRADENO 
        , T_TRADETIME 
        , T_BUYSELL 
        , T_SETTLECODE 
        , T_DECIMALS 
        , T_PRICE 
        , T_QUANTITY 
        , T_VALUE 
        , T_AMOUNT 
        , T_CLRCOMM 
        , T_ORDERNO 
        , T_CPFIRMID 
        , T_ACCINT 
        , T_CLIENTCODE 
        , T_VCLIENTCODE 
        , T_TRADETYPE
        , T_MATCHREF
        , T_ORDTYPECODE
        , T_DOC_NO 
        , T_PARTYID 
        , T_SFCONTRID 
        , T_ISSEB 
        , T_CPFIRMPARTYID 
        , T_CURID 
        , T_CALENDID 
        , T_MARKETSCHEMEID 
        , T_NDAY1 
        , T_NDAY2 
        , T_PAYMDATE1 
        , T_PAYMDATE2 
        , T_CALCITOGDATE 
        , T_WORKSETTLEDATE 
        , T_OBJECTID 
        , T_PORTFOLIO 
        , T_FIID 
        , T_UNDERWR 
        , T_UNDERWR_BUY 
        , T_ISREPO 
        , T_REPOPERIOD
        , T_MARKETREPORTID
        , T_ISFINDEQM06
        , T_EXTSETTLECODE 
        )
        (SELECT
         T.ID_MB_EQM3T         
        ,NVL(T.ID_MB_REQUISITES, 0)          
        ,NVL(T.CURRENCYID, CHR(1))             
        ,NVL(T.BOARDID, CHR(1))             
        ,NVL(T.BOARDNAME, CHR(1))                        
        ,NVL(T.TRADEDATE, TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')) 
        ,NVL(T.SETTLEDATE, TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')) 
        ,NVL(T.SECURITYID, CHR(1))             
        ,NVL(T.PRICETYPE, CHR(1))             
        ,NVL(T.TRDACCID, CHR(1))             
        ,NVL(T.TRADENO, CHR(1))             
        ,NVL(T.TRADETIME, TO_DATE(''01.01.0001 00:00:00'', ''dd.mm.yyyy HH24:MI:SS''))              
        ,NVL(T.BUYSELL, CHR(1))             
        ,NVL(T.SETTLECODE, CHR(1)) 
        ,NVL(DECIMALS,0)                 
        ,NVL(PRICE,0)                    
        ,NVL(QUANTITY,0)                
        ,NVL(VALUE,0)                    
        ,NVL(AMOUNT,0)                   
        ,NVL(CLRCOMM,0)                
        ,NVL(ORDERNO, CHR(1))            
        ,NVL(CPFIRMID, CHR(1))            
        ,NVL(ACCINT,0)                    
        ,NVL(T.CLIENTCODE, CHR(1))             
        ,NVL(CASE WHEN NVL(CLIENTCODE, CHR(1)) <> CHR(1)  
                  THEN CLIENTCODE 
                  ELSE CASE WHEN NVL(BROKERREF, CHR(1)) <> CHR(1) 
                            THEN CASE WHEN INSTR(BROKERREF, ''/'') > 0 
                                      THEN SUBSTR(BROKERREF, 1, INSTR(BROKERREF, ''/'') - 1) 
                                      ELSE CHR(1) 
                                  END 
                            ELSE CHR(1) 
                        END 
              END, 
             CHR(1)) 
        ,NVL(TRADETYPE, CHR(1))    
        ,NVL(MATCHREF, CHR(1))    
        ,NVL(ORDTYPECODE, CHR(1))              
        ,CHR(1) 
        ,0 
        ,0 
        ,0 
        ,0 
        ,-1 
        ,-1
        ,0 
        ,0 
        ,0 
        ,TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')
        ,TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')
        ,TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')
        ,TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')
        ,0  
        ,-1 
        ,-1
        ,0 
        ,0 
        ,0 --не РЕПО
        ,0
        ,0
        ,CASE WHEN NVL (eqm06.T_ID_MB_REQUISITES,0) <> 0 THEN 1 ELSE 0 END
        ,NVL(eqm06.T_EXTSETTLECODE, CHR(1))
           FROM '||p_Synonim||'.MB_EQM3T T, D_EQM06_TMP eqm06
          WHERE T.ID_MB_REQUISITES IN (SELECT ID_MB_REQUISITES FROM '||p_Synonim||'.PROCESSING_ACTUAL WHERE FILE_TYPE = ''EQM3T'' AND TRADE_DATE = :p_ImpDate )
            AND NVL(T.TRADEDATE, TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')) = :p_ImpDate
            AND T.ZRID = 0
            AND T.TRADENO = eqm06.T_TRADENO(+)       
            AND T.BUYSELL = eqm06.T_BUYSELL(+)      
            AND NVL(CASE WHEN NVL(T.CLIENTCODE, CHR(1)) <> CHR(1)  
                  THEN T.CLIENTCODE 
                  ELSE CASE WHEN NVL(T.BROKERREF, CHR(1)) <> CHR(1) 
                            THEN CASE WHEN INSTR(T.BROKERREF, ''/'') > 0 
                                      THEN SUBSTR(T.BROKERREF, 1, INSTR(T.BROKERREF, ''/'') - 1) 
                                      ELSE CHR(1) 
                                  END 
                            ELSE CHR(1) 
                        END 
              END, 
             CHR(1)) = eqm06.T_vCLIENTCODE(+)
           
        )'
        
    USING IN p_ImpDate, IN p_ImpDate;
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DEAL, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и FillEQM3T(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=EQM3T' || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DEAL) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE)|| ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500) 
                           );
    RETURN 1;
  END;
  
  FUNCTION UpdateObjEQM3T(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER
  IS
    TYPE ObjID_t IS TABLE OF DGTCODE_DBT.T_OBJECTID%TYPE;
    TYPE SemID_t IS TABLE OF D_EQM3T_TMP.T_ID_MB_EQM3T%TYPE;
    v_ObjIDs ObjID_t;
    v_SemID  SemID_t;
  BEGIN
    SELECT GTCODE.T_OBJECTID, EQM3T.T_ID_MB_EQM3T
      BULK COLLECT INTO v_ObjIDs, v_semID
      FROM DGTCODE_DBT GTCODE, D_EQM3T_TMP EQM3T
     WHERE GTCODE.T_APPLICATIONID IN (RSB_GTFN.GT_PAYMENTS, RSB_GTFN.GT_MMVB3)
       AND GTCODE.T_OBJECTKIND = RSB_GTFN.RG_DEAL
       AND GTCODE.T_OBJECTCODE = EQM3T.T_BUYSELL || '/' || EQM3T.T_TRADENO || DECODE(EQM3T.T_CLIENTCODE, CHR(1), '/ОТС', '');
    IF v_semID.COUNT <> 0 THEN
      FORALL i IN v_semID.FIRST .. v_semID.LAST
        UPDATE D_EQM3T_TMP EQM3T
           SET EQM3T.T_OBJECTID = v_ObjIDs (i)
         WHERE EQM3T.T_ID_MB_EQM3T = v_semID (i);
     END IF;
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DEAL, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и UpdateObjEQM3T(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=EQM3T' || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DEAL) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE)|| ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500)
                           );
    RETURN 1;
  END;
  
  FUNCTION UpdateContrEQM3T(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2,p_ContrID IN NUMBER)
    RETURN NUMBER
  IS
  BEGIN
        EXECUTE IMMEDIATE
          'UPDATE D_EQM3T_TMP EQM3T
              SET T_CPFIRMPARTYID = :p_ContrID'
        USING IN p_ContrID;
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DEAL, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и UpdateContrEQM3T(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=EQM3T' || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DEAL) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500)
                           );
    RETURN 1;
  END;

  FUNCTION UpdateDocEQM3T(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2)
    RETURN NUMBER
  IS
  BEGIN

    EXECUTE IMMEDIATE
      'UPDATE D_EQM3T_TMP EQM3T
           SET EQM3T.T_DOC_NO =  (SELECT requi.DOC_NO  FROM '||p_Synonim||'.MB_REQUISITES requi WHERE requi.ID_MB_REQUISITES = EQM3T.t_ID_MB_REQUISITES  AND ROWNUM = 1 )  '
       ;
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DEAL, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и UpdateDocEQM3T(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=EQM3T' || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DEAL) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500)
                           );
    RETURN 1;
  END;

  FUNCTION UpdateESCEQM3T(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2)
    RETURN NUMBER
  IS
  BEGIN

    EXECUTE IMMEDIATE
      'UPDATE D_EQM3T_TMP EQM3T
           SET EQM3T.T_ISFINDEQM06 = 1, 
           EQM3T.T_EXTSETTLECODE =  (SELECT requi.T_EXTSETTLECODE  FROM '||p_Synonim||'.MB_REQUISITES requi WHERE requi.ID_MB_REQUISITES = EQM3T.t_ID_MB_REQUISITES  AND ROWNUM = 1 )  '
       ;
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DEAL, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и UpdateDocEQM3T(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=EQM3T' || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DEAL) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500)
                           );
    RETURN 1;
  END;
  
  FUNCTION UpdateEQM3T_PM(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2)
    RETURN NUMBER
  IS
    TYPE ObjID_t IS TABLE OF DGTSNCREC_DBT.T_RECORDID%TYPE;
    TYPE PmID_t IS TABLE OF D_EQM3T_TMP.T_ID_MB_EQM3T%TYPE;
    v_ObjIDs ObjID_t;
    v_PmID   PmID_t;
  BEGIN
    SELECT EQM3T.T_ID_MB_EQM3T, GTSNCREC.T_RECORDID
      BULK COLLECT INTO v_PmID, v_ObjIDs
      FROM dgtsncrec_dbt gtsncrec,
           dgtcode_dbt gtcode,
           dgtobject_dbt gtobject,
           D_EQM3T_TMP EQM3T
     WHERE gtsncrec.t_seanceid = p_SeanceID
       AND GTCODE.T_OBJECTKIND = RSB_GTFN.RG_DEAL
       AND GTCODE.T_OBJECTID = GTSNCREC.T_OBJECTID
       AND GTOBJECT.T_OBJECTID = GTSNCREC.T_OBJECTID
       AND GTCODE.T_OBJECTCODE = EQM3T.T_BUYSELL || '/' || EQM3T.T_TRADENO || DECODE(EQM3T.T_CLIENTCODE, CHR(1), '/ОТС', '');

    IF v_PmID.COUNT <> 0 THEN
      FORALL i IN v_PmID.FIRST .. v_PmID.LAST
        EXECUTE IMMEDIATE
          'UPDATE '||p_Synonim||'.MB_EQM3T EQM3T
              SET ZRID = :v_ObjIDsi
            WHERE ID_MB_EQM3T = :v_PmIDi'
        USING IN v_ObjIDs(i), IN v_PmID(i);
    END IF;
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DEAL, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и UpdateEQM3T_PM(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=EQM3T' || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DEAL) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500)
                           );
    RETURN 1;
  END;

  --Обработка записей по данным буферной таблицы
  PROCEDURE ProcessReplRecByTmp_EQM3T(p_Prm IN OUT NOCOPY PRMREC_t)
  IS
    v_Prm     PRMREC_t;
    v_Deal    DATADEALS_t;
    v_stat    NUMBER(5) := 0;
    v_ErrMes  VARCHAR2(1000) := CHR(1);
    v_WarnMes VARCHAR2(1000) := CHR(1);
    v_Skip    NUMBER(5) := 0;
  BEGIN
    --инициализация глобального лога для накопления записей для вставки
    RSB_GTLOG.Init(BATCH_SIZE, p_Prm.SeanceID, RSB_GTFN.RG_DEAL);

    --инициализация глобального накопителя объектов ЗР для вставки
    v_stat := RSI_GT.Init(RSI_GT.DIRECT_INSERT_YES, p_Prm.SeanceID, p_Prm.Source_Code, RSI_GT.GTCODE_RECEIVE, v_ErrMes);
    IF v_stat <> 0 THEN
      RSB_GTLOG.Error(v_ErrMes);
    END IF;

    IF v_stat = 0 THEN
    FOR cData IN (SELECT t.*
                    FROM D_EQM3T_TMP t
                   ORDER BY t.T_DOC_NO, t.t_recno
                 )
      LOOP
        v_ErrMes  := CHR(1);
        v_WarnMes := CHR(1);
        v_Skip    := 0;

        v_Deal.AccInt          := cData.t_AccInt;
        v_Deal.Amount          := cData.t_Amount;
        v_Deal.BoardId         := cData.t_BoardId;
        v_Deal.BoardType       := cData.t_BoardName;
        v_Deal.BrokerRef       := cData.t_BrokerRef;
        v_Deal.BuySell         := cData.t_BuySell;
--        v_Deal.CalcItogDate_   := cData.CalcItogDate;
        v_Deal.CalendID        := cData.t_CalendID;
        v_Deal.ClientCode      := cData.t_ClientCode;
        v_Deal.ClientContrID   := cData.t_SfContrID;
        v_Deal.ClientID        := cData.t_PartyID;
        v_Deal.ClrComm         := cData.t_CLRCOMM;
        v_Deal.CPFirmId        := cData.t_CPFirmId;
        v_Deal.CPFirmId_ID     := cData.t_CPFirmPartyID;
        v_Deal.CurrencyId      := cData.t_CurrencyId;
        v_Deal.CurrencyId_FIID := cData.t_CurID;
        v_Deal.Decimals        := cData.t_Decimals;
        v_Deal.DOC_NO          := cData.t_DOC_NO;
--        v_Deal.ExhComm         := cData.EXCHCOMM;
      --v_Deal.GateDealType
        v_Deal.IsCliring       := 0;
        v_Deal.IsRepo          := 0;
        v_Deal.IsSeb           := cData.t_IsSeb;
        v_Deal.FIID            := cData.t_FIID;
        v_Deal.MarketReportID  := cData.t_MarketReportID;
        v_Deal.MarketSchemeID  := cData.t_MarketSchemeID;
        v_Deal.MatchRef        := cData.t_MatchRef;
        v_Deal.NDay1_          := cData.t_NDay1;
        v_Deal.NDay2_          := cData.t_NDay2;
        v_Deal.OrderNo         := cData.t_OrderNo;
        v_Deal.OrdTypeCode     := cData.t_OrdTypeCode;
        v_Deal.FaceValue       := cData.t_FaceValue;
        v_Deal.PaymDate1_      := cData.t_PaymDate1;
        v_Deal.PaymDate2_      := cData.t_PaymDate2;
        IF  v_Deal.ClientCode <> CHR(1) THEN
          v_Deal.Portfolio_      := RSB_PMWRTOFF.KINDPORT_CLIENT;
        ELSE
           v_Deal.Portfolio_ := GetPortfolio(cData.t_FIID, cData.t_SettleDate);
        END IF;
        v_Deal.Price           := cData.t_Price;
        v_Deal.PriceType       := cData.t_PriceType;
        v_Deal.Quantity        := cData.t_Quantity;
        IF(cData.t_Quantity = 0 AND v_Deal.Price <> 0) THEN
          v_Deal.Quantity      := cData.t_Value/v_Deal.Price;
        END IF;
        v_Deal.RecNo           := cData.t_RecNo; --тип?
        v_Deal.RepoPart        := 0;
        v_Deal.RepoPeriod      := 0;
        v_Deal.RepoRate        := 0;
        v_Deal.RepoValue       := 0;
        v_Deal.SecurityId      := cData.t_SecurityId;
        --v_Deal.SecurityType    := cData.t_SecurityType;
        v_Deal.SettleCode      := cData.t_SettleCode;
        v_Deal.SettleDate      := cData.t_SettleDate;
        v_Deal.SettleTime      := RSI_GT.ZeroTime;
        v_Deal.TradeDate       := cData.t_TradeDate;
        v_Deal.TradeSessionDate := RSI_GT.ZeroDate;
        v_Deal.TradeNo         := cData.t_TradeNo;
        v_Deal.TradeTime       := cData.t_TradeTime;
        v_Deal.TradeType       := cData.t_TradeType;
        v_Deal.TrdAccId        := cData.t_TrdAccId ;
        v_Deal.UnderWr         := 0;
        v_Deal.UnderWr_Pokupka := 0;
        v_Deal.Value_          := cData.t_Value;
        v_Deal.vClientCode     := cData.t_vClientCode;
        v_Deal.WorkSettleDate_ := cData.t_WorkSettleDate;
        v_Deal.IsRSHB          := p_Prm.IsRSHB;
        v_Deal.IsOTC           := 1;
        

        SetPartPrmDeal_PM(v_Deal);
        SetUINNumber(v_Deal);

        IF cData.t_CPFirmId <> CHR(1) AND cData.t_CPFirmPartyID = 0 THEN
          v_ErrMes := 'Ошибка при загрузке сделки с кодом "' || v_Deal.UINNumber || '"' || CHR(10) || 'В ГКБО не найден субъект с принадлежностью "Центральный контрагент"';
        ELSIF cData.t_OBJECTID > 0 THEN
          v_ErrMes := 'Ошибка при загрузке сделки с кодом "' || v_Deal.UINNumber || '"' || CHR(10) || 'Запись репликации с действием создать для объекта ' || TO_CHAR(cData.t_OBJECTID) || ' уже передавалась в RS-Bank.' || CHR(10) || 'Повторная репликация невозможна.';
        ELSIF v_Deal.vClientCode <> CHR(1) AND v_Deal.ClientID <= 0 THEN
          v_ErrMes := 'Ошибка при загрузке сделки с кодом "' || v_Deal.UINNumber || '"' || CHR(10) || 'Не найден субъект через краткий код на бирже "' || v_Deal.vClientCode || '"';
        ELSIF v_Deal.CurrencyId <> CHR(1) AND cData.t_CurID = -1 THEN
          v_ErrMes := 'Ошибка при загрузке сделки с кодом "' || v_Deal.UINNumber || '"' || CHR(10) || 'В справочнике не найден ФИ с кодом "' || v_Deal.CurrencyId || '"';
        ELSIF v_Deal.SecurityId <> CHR(1) AND cData.t_FIID = -1 THEN
          v_ErrMes := 'Ошибка при загрузке сделки с кодом "' || v_Deal.UINNumber || '"' || CHR(10) || 'В справочнике не найден ФИ с кодом "' || v_Deal.SecurityId || '"';
        ELSIF LENGTH(TRIM(TO_CHAR(v_Deal.Quantity))) > 16 THEN
          v_ErrMes := 'Ошибка при загрузке сделки с кодом "' || v_Deal.UINNumber || '"' || CHR(10) || 'Превышено максимально возможное количество загружаемых знаков параметра Quantity';
        ELSIF SetTypeOperationBOCB(v_Deal, v_ErrMes) <> 0 THEN
          v_ErrMes:= 'Ошибка при загрузке сделки с кодом "' || v_Deal.UINNumber || '"' || CHR(10) || v_ErrMes;
        -- BIQ-6801
        /*ELSIF v_Deal.vClientCode = CHR(1) THEN
          RSB_GTLOG.Message(
            'ПРЕДУПРЕЖДЕНИЕ: Не загружена внебиржевая сделка с кодом "' || v_Deal.UINNumber || '"' || CHR(10) || 'В файле импорта не заданы атрибуты ClientCode и BrokerRef',
            RSB_GTLOG.ISSUE_LOAD_WARNING
          );
          v_Skip := 1; */
        END IF;

        IF v_ErrMes = CHR(1) and v_Skip = 0 THEN
            IF PutDealInfoPM(p_Prm, v_Deal, v_ErrMes, v_WarnMes) = 0 THEN
              --RSI_GT.DoInitRec(); --перенести объекты ЗР в накопители (нужна только при DIRECT_INSERT_NO)
              v_stat := RSI_GT.SaveBatch(BATCH_SIZE, v_ErrMes); --сохранить по мере накопления
            END IF;
        END IF;

        IF v_ErrMes <> CHR(1) THEN
          RSB_GTLOG.Error(v_ErrMes);
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
        RSB_GTLOG.Error(v_ErrMes);
      END IF;
    END IF;

    --сохранение накопленных записей лога
    RSB_GTLOG.Save;

  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_Prm.SeanceID, RSB_GTFN.RG_DEAL, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка при обработке данных в ф-и ProcessReplRecByTmp_EQM3T(), ' ||
                           'SeanceID=' || TO_CHAR(p_Prm.SeanceID) || ', FileImport=' || TO_CHAR(p_Prm.FileImport) ||
                           ', Source_Code=' || p_Prm.Source_Code || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500)
                           );
  END;

  --Создание объектов записей репликаций по данным временной таблицы EQM3T
  FUNCTION CreateReplRecByTmp_EQM3T(p_SeanceID IN NUMBER, p_ImpDate IN DATE, p_SourceCode IN VARCHAR2, p_PrmStr IN VARCHAR2)
    RETURN NUMBER
  IS
    v_Prm PRMREC_t;
  BEGIN
    --инициализация используемых параметров
    v_Prm.SeanceID           := p_SeanceID;
    SetPrmRecByStr(v_Prm, p_PrmStr);
    v_Prm.SeanceID           := p_SeanceID;
    v_Prm.imp_date           := p_ImpDate;
    v_Prm.Source_Code        := p_SourceCode;
    v_Prm.IMPORT_SETTLETIME  := GetImportSettleTime;
    v_Prm.FORM_ORDERS_IMPORT := GetFormOrdersImport;
    v_Prm.FileImport         := RSB_GTFN.FileEQM3T;
    --подготовка отчетов биржи и обновление данных во временной таблице
    PrepareMarketReportsEQM3T(v_Prm);

    ProcessReplRecByTmp_EQM3T(v_Prm);

    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DEAL, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и CreateReplRecByTmp_EQM3T(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DEAL) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE)|| ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500)
                           );
    RETURN 1;
  END;

  FUNCTION DelEQM13(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER
  IS
  BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE D_EQM13_TMP';
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DLITOGCLVR, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и DelEQM13(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=EQM13' || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DLITOGCLVR) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;
  
  FUNCTION FillEQM13(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2, p_ImpDate IN DATE)
    RETURN NUMBER
  IS
  BEGIN
    EXECUTE IMMEDIATE
      'INSERT INTO D_EQM13_TMP(
         T_ID_MB_EQM13
        ,T_ID_MB_REQUISITES
        ,T_REPORTDATE
        ,T_REPORTDATESTR
        ,T_EXTCODE
        ,T_CURRENCYID
        ,T_NETTOSUM
        ,T_LIABTYPE
        ,T_DOCTYPE
        ,T_DOCNO
        ,T_OBJECTID
        )
        (SELECT
         T.ID_MB_EQM13
        ,NVL(T.ID_MB_REQUISITES, 0)
        ,NVL(T.REPORTDATE, TO_DATE(''01.01.0001'', ''DD.MM.YYYY''))
        ,DECODE(SUBSTR(to_char(NVL(T.REPORTDATE, TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')),''dd''), 1, 1), ''0'', '' '', '''') || TRIM(LEADING ''0'' FROM to_char(NVL(T.REPORTDATE, TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')), ''dd.mm.yyyy''))
        ,NVL(T.EXTSETTLECODE, CHR(1))
        ,NVL(T.CURRENCYID, CHR(1))
        ,NVL(T.CREDIT, 0) - NVL(T.DEBIT, 0)
        ,NVL(T.DATATYPE, CHR(1))
        ,''EQM13''
        ,CHR(1)
        ,0
           FROM '||p_Synonim||'.MB_EQM13 T
          WHERE T.ID_MB_REQUISITES IN (SELECT ID_MB_REQUISITES FROM '||p_Synonim||'.PROCESSING_ACTUAL WHERE FILE_TYPE = ''EQM13'' AND TRADE_DATE = :p_ImpDate)
            AND T.ZRID = 0
            AND NVL(T.POSTYPE, CHR(1)) = ''C'' 
            AND SUBSTR(NVL(T.DATATYPE, CHR(1)), 1, 5) != ''DEPO_''
        )'
    USING IN p_ImpDate;
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DLITOGCLVR, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и FillEQM13(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=EQM13' || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DLITOGCLVR) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;
  
  FUNCTION UpdateDocNoEQM13(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2)
    RETURN NUMBER
  IS
  BEGIN
    EXECUTE IMMEDIATE
      'UPDATE D_EQM13_TMP EQM13
          SET EQM13.T_DOCNO = (SELECT REQUI.DOC_NO
                                 FROM '||p_Synonim||'.MB_REQUISITES REQUI
                                WHERE REQUI.ID_MB_REQUISITES = EQM13.t_ID_MB_REQUISITES
                                  AND ROWNUM = 1)';
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DLITOGCLVR, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и UpdateDocNoEQM13(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=EQM13' || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DLITOGCLVR) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;
  
  FUNCTION UpdateObjEQM13(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER
  IS
    TYPE ObjID_t IS TABLE OF DGTCODE_DBT.T_OBJECTID%TYPE;
    TYPE SemID_t IS TABLE OF D_EQM13_TMP.T_ID_MB_EQM13%TYPE;
    v_ObjIDs ObjID_t;
    v_SemID  SemID_t;
  BEGIN
    SELECT GTCODE.T_OBJECTID, EQM13.T_ID_MB_EQM13
      BULK COLLECT INTO v_ObjIDs, v_semID
      FROM DGTCODE_DBT GTCODE, D_EQM13_TMP EQM13
     WHERE GTCODE.T_APPLICATIONID IN (RSB_GTFN.GT_PAYMENTS, RSB_GTFN.GT_MMVB3)
       AND GTCODE.T_OBJECTKIND = RSB_GTFN.RG_DLITOGCLVR
       AND GTCODE.T_OBJECTCODE = EQM13.T_DOCTYPE || '_' || EQM13.T_DOCNO || '_' || EQM13.T_EXTCODE || '_' || EQM13.T_CURRENCYID || '_' || EQM13.T_LIABTYPE || '_' || EQM13.T_REPORTDATESTR;
    IF v_semID.COUNT <> 0 THEN
      FORALL i IN v_semID.FIRST .. v_semID.LAST
        UPDATE D_EQM13_TMP EQM13
           SET EQM13.T_OBJECTID = v_ObjIDs (i)
         WHERE EQM13.T_ID_MB_EQM13 = v_semID (i);
     END IF;
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DLITOGCLVR, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и UpdateObjEQM13(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=EQM13' || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DLITOGCLVR) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;
  
  FUNCTION UpdateEQM13_PM(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2)
    RETURN NUMBER
  IS
    TYPE ObjID_t IS TABLE OF DGTSNCREC_DBT.T_RECORDID%TYPE;
    TYPE PmID_t IS TABLE OF D_EQM13_TMP.T_ID_MB_EQM13%TYPE;
    v_ObjIDs ObjID_t;
    v_PmID   PmID_t;
  BEGIN
    SELECT EQM13.T_ID_MB_EQM13, GTSNCREC.T_RECORDID
      BULK COLLECT INTO v_PmID, v_ObjIDs
      FROM dgtsncrec_dbt gtsncrec,
           dgtcode_dbt gtcode,
           dgtobject_dbt gtobject,
           D_EQM13_TMP EQM13
     WHERE gtsncrec.t_seanceid = p_SeanceID
       AND GTCODE.T_OBJECTKIND = RSB_GTFN.RG_DLITOGCLVR
       AND GTCODE.T_OBJECTID = GTSNCREC.T_OBJECTID
       AND GTOBJECT.T_OBJECTID = GTSNCREC.T_OBJECTID
       AND GTCODE.T_OBJECTCODE = EQM13.T_DOCTYPE || '_' || EQM13.T_DOCNO || '_' || EQM13.T_EXTCODE || '_' || EQM13.T_CURRENCYID || '_' || EQM13.T_LIABTYPE || '_' || EQM13.T_REPORTDATESTR;

    IF v_PmID.COUNT <> 0 THEN
      FORALL i IN v_PmID.FIRST .. v_PmID.LAST
        EXECUTE IMMEDIATE
          'UPDATE '||p_Synonim||'.MB_EQM13 EQM13
              SET ZRID = :v_ObjIDsi
            WHERE ID_MB_EQM13 = :v_PmIDi'
        USING IN v_ObjIDs(i), IN v_PmID(i);
    END IF;
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DLITOGCLVR, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и UpdateEQM13_PM(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=EQM13' || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DLITOGCLVR) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;
  
  --Заполнение и сохранение параметров нетто-ТО
  FUNCTION PutItogClInfoPM(p_ItogCl IN OUT NOCOPY D_EQM13_TMP%ROWTYPE, p_ErrMes IN OUT NOCOPY VARCHAR2) RETURN NUMBER
  IS
    v_stat NUMBER(5) := 0;
  BEGIN

    v_stat := RSI_GT.InitRec(RSB_GTFN.RG_DLITOGCLVR,
                             'Нетто-требование/нетто-обязательство ' || p_ItogCl.t_LiabType || ' на ' || p_ItogCl.t_CurrencyID || ' c расчетным кодом ' || p_ItogCl.t_ExtCode || ' на ' || p_ItogCl.t_ReportDateStr || ' из документа ' || p_ItogCl.t_DocNo, --Наименование объекта
                             0, --проверять существование объекта в системе по коду
                             p_ItogCl.t_DocType || '_' || p_ItogCl.t_DocNo || '_' || p_ItogCl.t_ExtCode || '_' || p_ItogCl.t_CurrencyID || '_' || p_ItogCl.t_LiabType || '_' || p_ItogCl.t_ReportDateStr, --код объекта
                             0, --ID клиента
                             p_ErrMes);

    IF v_stat = 0 THEN
      RSI_GT.SetParmByName('RGDLICVR_TYPE',     p_ItogCl.t_DocType);
      RSI_GT.SetParmByName('RGDLICVR_DATE',     p_ItogCl.t_ReportDate);
      RSI_GT.SetParmByName('RGDLICVR_EXTCODE',  p_ItogCl.t_ExtCode);
      RSI_GT.SetParmByName('RGDLICVR_CURRENCY', p_ItogCl.t_CurrencyID);
      RSI_GT.SetParmByName('RGDLICVR_NETTOSUM', p_ItogCl.t_NettoSum);
      RSI_GT.SetParmByName('RGDLICVR_LIABTYPE', p_ItogCl.t_LiabType);  

      --проверка ошибок после добавления параметров
      IF RSI_GT.GetLastError <> CHR(1) THEN
        v_stat := 1;
        p_ErrMes := RSI_GT.GetLastError;
      END IF;
    END IF;

    RETURN v_stat;
  END;
  
  --Создание объектов записей репликаций по данным временной таблицы EQM13
  FUNCTION CreateReplRecByTmp_EQM13(p_SeanceID IN NUMBER, p_ImpDate IN DATE, p_SourceCode IN VARCHAR2) RETURN NUMBER
  IS
    TYPE CDATA_t IS REF CURSOR RETURN D_EQM13_TMP%ROWTYPE;
    v_cData   CDATA_t;
    v_ItogCl  D_EQM13_TMP%ROWTYPE;
    v_stat    NUMBER(5) := 0;
    v_ErrMes  VARCHAR2(1000) := CHR(1);
    v_WarnMes VARCHAR2(1000) := CHR(1);
  BEGIN
    --инициализация глобального лога для накопления записей для вставки
    RSB_GTLOG.Init(BATCH_SIZE, p_SeanceID, RSB_GTFN.RG_DLITOGCLVR);

    --инициализация глобального накопителя объектов ЗР для вставки
    v_stat := RSI_GT.Init(RSI_GT.DIRECT_INSERT_YES, p_SeanceID, p_SourceCode, RSI_GT.GTCODE_RECEIVE, v_ErrMes);
    IF v_stat <> 0 THEN
      RSB_GTLOG.Error(v_ErrMes);
    END IF;

    IF v_stat = 0 THEN
      OPEN v_cData FOR
           --важен порядок и наличие всех полей, чтобы сработал FETCH
           SELECT T_ID_MB_EQM13,
                  T_ID_MB_REQUISITES,
                  NVL(t.T_DOCNO, CHR(1)) DOCNO,
                  NVL(t.T_DOCTYPE, CHR(1)) DOCTYPE,
                  NVL(t.T_REPORTDATE, RSI_GT.ZeroDate) REPORTDATE,
                  NVL(t.T_EXTCODE, CHR(1)) EXTCODE,
                  NVL(t.T_CURRENCYID, CHR(1)) CURRENCYID,
                  NVL(t.T_NETTOSUM, 0) NETTOSUM,
                  NVL(t.T_LIABTYPE, CHR(1)) LIABTYPE,
                  NVL(t.T_OBJECTID, 0) OBJECTID,
                  NVL(t.T_REPORTDATESTR, CHR(1)) REPORTDATESTR
            FROM D_EQM13_TMP t
           WHERE t.T_REPORTDATE = p_ImpDate
           ORDER BY t.T_ID_MB_EQM13;
      LOOP
        FETCH v_cData INTO v_ItogCl;
        EXIT WHEN v_cData%NOTFOUND OR v_cData%NOTFOUND IS NULL;

        v_ErrMes  := CHR(1);
        v_WarnMes := CHR(1);

        IF v_ItogCl.t_ObjectID > 0 THEN
          v_ErrMes := 'Ошибка при загрузке итоговых нетто-требований и нетто-обязательств "' || v_ItogCl.t_DocType || '_' || v_ItogCl.t_DocNo || '_' || v_ItogCl.t_ExtCode || '_' || v_ItogCl.t_CurrencyID || '_' || v_ItogCl.t_LiabType || '_' || v_ItogCl.t_ReportDateStr || '"' || 
                      CHR(10) || 'Запись репликации с действием создать для объекта ' || TO_CHAR(v_ItogCl.t_ObjectID) || ' уже передавалась в RS-Bank.' || CHR(10) || 'Повторная репликация невозможна.';
        END IF;

        IF v_ErrMes = CHR(1) THEN
          IF PutItogClInfoPM(v_ItogCl, v_ErrMes) = 0 THEN
            v_stat := RSI_GT.SaveBatch(BATCH_SIZE, v_ErrMes); --сохранить по мере накопления
          END IF;
        END IF;

        IF v_ErrMes <> CHR(1) THEN
          RSB_GTLOG.Error(v_ErrMes);
        END IF;

        IF v_WarnMes <> CHR(1) THEN
          RSB_GTLOG.Warning(v_WarnMes);
        END IF;
      END LOOP;
      CLOSE v_cData;
    END IF;

    IF v_stat = 0 THEN
      --сохранение накопленных объектов ЗР
      v_stat := RSI_GT.Save(v_ErrMes);
      IF v_stat <> 0 THEN
        RSB_GTLOG.Error(v_ErrMes);
      END IF;
    END IF;

    --сохранение накопленных записей лога
    RSB_GTLOG.Save;
    
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DLITOGCLVR, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка при обработке данных в ф-и CreateReplRecByTmp_EQM13(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=EQM13' || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DLITOGCLVR) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;
  
  FUNCTION DelEQM99(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER
  IS
  BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE D_EQM99_TMP';
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DLKSUWRT, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и DelEQM99(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=EQM99' || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DLKSUWRT) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;
  
  FUNCTION FillEQM99(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2, p_ImpDate IN DATE)
    RETURN NUMBER
  IS
  BEGIN
    EXECUTE IMMEDIATE
      'INSERT INTO D_EQM99_TMP(
         T_ID_MB_EQM99
        ,T_ID_MB_REQUISITES
        ,T_REPORTDATE
        ,T_REPORTDATESTR
        ,T_BANKACCID
        ,T_TRDACCID
        ,T_SECURITYID
        ,T_ISIN
        ,T_OPERATIONCODE
        ,T_OPERATIONTIME
        ,T_DOCNO
        ,T_DEBIT
        ,T_CREDIT
        ,T_OBJECTID
        )
        (SELECT
         T.ID_MB_EQM99
        ,NVL(T.ID_MB_REQUISITES, 0)
        ,NVL(T.REPORTDATE, TO_DATE(''01.01.0001'', ''DD.MM.YYYY''))
        ,DECODE(SUBSTR(to_char(NVL(T.REPORTDATE, TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')),''dd''), 1, 1), ''0'', '' '', '''') || TRIM(LEADING ''0'' FROM to_char(NVL(T.REPORTDATE, TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')), ''dd.mm.yyyy''))
        ,NVL(T.BANKACCID, CHR(1))
        ,NVL(T.TRDACCID, CHR(1))
        ,NVL(T.SECURITYID, CHR(1))
        ,NVL(T.ISIN, CHR(1))
        ,NVL(T.OPERATIONCODE, CHR(1))
        ,NVL(T.OPERATIONTIME, TO_DATE(''01.01.0001 00:00:00'', ''dd.mm.yyyy HH24:MI:SS'')) 
        ,NVL(T.DOCNO, CHR(1))
        ,NVL(T.DEBIT, 0)
        ,NVL(T.CREDIT, 0)
        ,0
           FROM '||p_Synonim||'.MB_EQM99 T
          WHERE T.ID_MB_REQUISITES IN (SELECT ID_MB_REQUISITES FROM '||p_Synonim||'.PROCESSING_ACTUAL WHERE FILE_TYPE = ''EQM99'' AND TRADE_DATE = :p_ImpDate)
            AND T.ZRID = 0
            AND NVL(T.POSTYPE, CHR(1)) = ''S'' 
            AND NVL(T.OPERATIONCODE, CHR(1)) IN (''22'', ''23'')
            AND NVL(T.DOCNO, CHR(1)) != CHR(1)
        )'
    USING IN p_ImpDate;
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DLKSUWRT, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и FillEQM99(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=EQM99' || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DLKSUWRT) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;
  
  FUNCTION UpdateObjEQM99(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER
  IS
    TYPE ObjID_t IS TABLE OF DGTCODE_DBT.T_OBJECTID%TYPE;
    TYPE SemID_t IS TABLE OF D_EQM99_TMP.T_ID_MB_EQM99%TYPE;
    v_ObjIDs ObjID_t;
    v_SemID  SemID_t;
  BEGIN
    SELECT GTCODE.T_OBJECTID, EQM99.T_ID_MB_EQM99
      BULK COLLECT INTO v_ObjIDs, v_semID
      FROM DGTCODE_DBT GTCODE, D_EQM99_TMP EQM99
     WHERE GTCODE.T_APPLICATIONID IN (RSB_GTFN.GT_PAYMENTS, RSB_GTFN.GT_MMVB3)
       AND GTCODE.T_OBJECTKIND = RSB_GTFN.RG_DLKSUWRT
       AND GTCODE.T_OBJECTCODE = EQM99.T_DOCNO || '_' || EQM99.T_REPORTDATESTR;
    IF v_semID.COUNT <> 0 THEN
      FORALL i IN v_semID.FIRST .. v_semID.LAST
        UPDATE D_EQM99_TMP EQM99
           SET EQM99.T_OBJECTID = v_ObjIDs (i)
         WHERE EQM99.T_ID_MB_EQM99 = v_semID (i);
     END IF;
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DLKSUWRT, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и UpdateObjEQM99(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=EQM99' || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DLKSUWRT) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;
  
  FUNCTION UpdateEQM99_PM(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2)
    RETURN NUMBER
  IS
    TYPE ObjID_t IS TABLE OF DGTSNCREC_DBT.T_RECORDID%TYPE;
    TYPE PmID_t IS TABLE OF D_EQM99_TMP.T_ID_MB_EQM99%TYPE;
    v_ObjIDs ObjID_t;
    v_PmID   PmID_t;
  BEGIN
    SELECT EQM99.T_ID_MB_EQM99, GTSNCREC.T_RECORDID
      BULK COLLECT INTO v_PmID, v_ObjIDs
      FROM dgtsncrec_dbt gtsncrec,
           dgtcode_dbt gtcode,
           dgtobject_dbt gtobject,
           D_EQM99_TMP EQM99
     WHERE gtsncrec.t_seanceid = p_SeanceID
       AND GTCODE.T_OBJECTKIND = RSB_GTFN.RG_DLKSUWRT
       AND GTCODE.T_OBJECTID = GTSNCREC.T_OBJECTID
       AND GTOBJECT.T_OBJECTID = GTSNCREC.T_OBJECTID
       AND GTCODE.T_OBJECTCODE = EQM99.T_DOCNO || '_' || EQM99.T_REPORTDATESTR;

    IF v_PmID.COUNT <> 0 THEN
      FORALL i IN v_PmID.FIRST .. v_PmID.LAST
        EXECUTE IMMEDIATE
          'UPDATE '||p_Synonim||'.MB_EQM99 EQM99
              SET ZRID = :v_ObjIDsi
            WHERE ID_MB_EQM99 = :v_PmIDi'
        USING IN v_ObjIDs(i), IN v_PmID(i);
    END IF;
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DLKSUWRT, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и UpdateEQM99_PM(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=EQM99' || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DLKSUWRT) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;
  
  --Заполнение и сохранение параметров КСУ
  FUNCTION PutKSUWRTInfoPM(p_KSUWRT IN OUT NOCOPY D_EQM99_TMP%ROWTYPE, p_MMVB_Code IN VARCHAR2, p_ErrMes IN OUT NOCOPY VARCHAR2) RETURN NUMBER
  IS
    v_stat NUMBER(5) := 0;
  BEGIN

    v_stat := RSI_GT.InitRec(RSB_GTFN.RG_DLKSUWRT,
                             'Выдача/погашение КСУ № ' || p_KSUWRT.t_DocNo || ' на ' || p_KSUWRT.t_ReportDateStr, --Наименование объекта
                             0, --проверять существование объекта в системе по коду
                             p_KSUWRT.t_DocNo || '_' || p_KSUWRT.t_ReportDateStr, --код объекта
                             0, --ID клиента
                             p_ErrMes);
                             
    IF v_stat = 0 THEN    
      IF p_KSUWRT.t_Credit != 0 THEN
         RSI_GT.SetParmByName('RGDLWRT_DEALTYPE', RSB_GTFN.OPERKIND_AVRWRTIN);
         RSI_GT.SetParmByName('RGDLWRT_AMOUNT',   abs(p_KSUWRT.t_Credit)); 
      ELSIF p_KSUWRT.t_Debit != 0 THEN
         RSI_GT.SetParmByName('RGDLWRT_DEALTYPE', RSB_GTFN.OPERKIND_AVRWRTOUT);
         RSI_GT.SetParmByName('RGDLWRT_AMOUNT',   abs(p_KSUWRT.t_Debit)); 
      END IF;

      RSI_GT.SetParmByName('RGDLWRT_DEALCODETS', p_KSUWRT.t_DocNo);
      RSI_GT.SetParmByName('RGDLWRT_DEALDATE',   p_KSUWRT.t_ReportDate);
      RSI_GT.SetParmByName('RGDLWRT_DEALTIME',   p_KSUWRT.t_OperationTime);
      RSI_GT.SetParmByName('RGDLWRT_ISIN',       p_KSUWRT.t_ISIN);
      RSI_GT.SetParmByName('RGDLWRT_SECURITYID', p_KSUWRT.t_SecurityId);
      RSI_GT.SetParmByName('RGDLWRT_BANKACCID',  p_KSUWRT.t_BankAccID);
      RSI_GT.SetParmByName('RGDLWRT_TRDACCID',   p_KSUWRT.t_TrdAccID);
      RSI_GT.SetParmByName('RGDLWRT_MARKET',     p_MMVB_Code);
 
      --проверка ошибок после добавления параметров
      IF RSI_GT.GetLastError <> CHR(1) THEN
        v_stat := 1;
        p_ErrMes := RSI_GT.GetLastError;
      END IF;
    END IF;

    RETURN v_stat;
  END;
  
  --Создание объектов записей репликаций по данным временной таблицы EQM99
  FUNCTION CreateReplRecByTmp_EQM99(p_SeanceID IN NUMBER, p_ImpDate IN DATE, p_SourceCode IN VARCHAR2, p_MMVB_Code IN VARCHAR2) RETURN NUMBER
  IS
    TYPE CDATA_t IS REF CURSOR RETURN D_EQM99_TMP%ROWTYPE;
    v_cData   CDATA_t;
    v_KSUWRT  D_EQM99_TMP%ROWTYPE;
    v_stat    NUMBER(5) := 0;
    v_ErrMes  VARCHAR2(1000) := CHR(1);
    v_WarnMes VARCHAR2(1000) := CHR(1);
  BEGIN
    --инициализация глобального лога для накопления записей для вставки
    RSB_GTLOG.Init(BATCH_SIZE, p_SeanceID, RSB_GTFN.RG_DLKSUWRT);

    --инициализация глобального накопителя объектов ЗР для вставки
    v_stat := RSI_GT.Init(RSI_GT.DIRECT_INSERT_YES, p_SeanceID, p_SourceCode, RSI_GT.GTCODE_RECEIVE, v_ErrMes);
    IF v_stat <> 0 THEN
      RSB_GTLOG.Error(v_ErrMes);
    END IF;

    IF v_stat = 0 THEN
      OPEN v_cData FOR
           --важен порядок и наличие всех полей, чтобы сработал FETCH
           SELECT T_ID_MB_EQM99,
                  T_ID_MB_REQUISITES,
                  NVL(t.T_REPORTDATE, RSI_GT.ZeroDate) REPORTDATE,
                  NVL(t.T_BANKACCID, CHR(1)) BANKACCID,
                  NVL(t.T_TRDACCID, CHR(1)) TRDACCID,
                  NVL(t.T_SECURITYID, CHR(1)) SECURITYID,
                  NVL(t.T_ISIN, CHR(1)) ISIN,
                  NVL(t.T_OPERATIONCODE, CHR(1)) OPERATIONCODE,
                  NVL(t.T_OPERATIONTIME, TO_DATE('01.01.0001 00:00:00', 'dd.mm.yyyy HH24:MI:SS')) OPERATIONTIME,
                  NVL(t.T_DOCNO, CHR(1)) ISIN,
                  NVL(t.T_DEBIT, 0) DEBIT,
                  NVL(t.T_CREDIT, 0) CREDIT,
                  NVL(t.T_OBJECTID, 0) OBJECTID,
                  NVL(t.T_REPORTDATESTR, CHR(1)) REPORTDATESTR
            FROM D_EQM99_TMP t
           WHERE t.T_REPORTDATE = p_ImpDate
           ORDER BY t.T_ID_MB_EQM99;
      LOOP
        FETCH v_cData INTO v_KSUWRT;
        EXIT WHEN v_cData%NOTFOUND OR v_cData%NOTFOUND IS NULL;

        v_ErrMes  := CHR(1);
        v_WarnMes := CHR(1);

        IF v_KSUWRT.t_ObjectID > 0 THEN
          v_ErrMes := 'Ошибка при загрузке данных о выдаче/погашении КСУ с кодом "' || v_KSUWRT.t_DocNo || '"' || CHR(10) || 
                      'Запись репликации с действием создать для объекта ' || TO_CHAR(v_KSUWRT.t_ObjectID) || ' уже передавалась в RS-Bank.' || CHR(10) || 'Повторная репликация невозможна.';
        END IF;

        IF v_ErrMes = CHR(1) THEN
          IF PutKSUWRTInfoPM(v_KSUWRT, p_MMVB_Code, v_ErrMes) = 0 THEN
            v_stat := RSI_GT.SaveBatch(BATCH_SIZE, v_ErrMes); --сохранить по мере накопления
          END IF;
        END IF;

        IF v_ErrMes <> CHR(1) THEN
          RSB_GTLOG.Error(v_ErrMes);
        END IF;

        IF v_WarnMes <> CHR(1) THEN
          RSB_GTLOG.Warning(v_WarnMes);
        END IF;
      END LOOP;
      CLOSE v_cData;
    END IF;

    IF v_stat = 0 THEN
      --сохранение накопленных объектов ЗР
      v_stat := RSI_GT.Save(v_ErrMes);
      IF v_stat <> 0 THEN
        RSB_GTLOG.Error(v_ErrMes);
      END IF;
    END IF;

    --сохранение накопленных записей лога
    RSB_GTLOG.Save;
    
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DLKSUWRT, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка при обработке данных в ф-и CreateReplRecByTmp_EQM99(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=EQM99' || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DLKSUWRT) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;

END RSB_GTIM_MMVB;
/
