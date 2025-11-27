CREATE OR REPLACE PACKAGE BODY RSB_GTIM_SPB
IS
  BATCH_SIZE CONSTANT NUMBER(5) := 100; --Размер пачки для сохранения объектов записей репликаций
  
  --Структура используемых параметров
  TYPE PRMREC_t IS RECORD (SeanceID           DGTSEANCE_DBT.T_SEANCEID%TYPE, --Идентификатор сеанса
                           Source_Code        DGTAPP_DBT.T_CODE%TYPE, --Код источника
                           Kind               NUMBER(5), --Источник
                           deals_bo           NUMBER(5), --Признак импорта сделок БО ЦБ
                           CPFirmIdConst      DOBJCODE_DBT.T_CODE%TYPE, --Код ЦК для ММВБ
                           imp_date           DATE, --Дата импорта
                           IMPORT_SETTLETIME  NUMBER(5), --Значение настройки 'SECUR\IMPORT_SETTLETIME'
                           FileImport         NUMBER(5), --Тип обрабатываемого файла
                           SPB_Code           DOBJCODE_DBT.T_CODE%TYPE, --Код СПБ
                           FORM_ORDERS_IMPORT NUMBER(5) --Значение настройки 'SECUR\ФОРМИР. ПОРУЧЕНИЯ ПРИ ИМПОРТЕ'
                          );

  --Общая структура по данным заявкам
  TYPE DATAREQ_t IS RECORD (  BuySell         VARCHAR2(1),
                              EntryTime       DATE,
                              AmendTime       DATE,
                              Status          VARCHAR2(32),
                              ClientCode      VARCHAR2(12),
                              StatusReason    VARCHAR2(32),
                              OrderNo         VARCHAR2(32),
                              TradeDate       DATE,
                              RepoRate        NUMBER(38, 8),
                              RecNo           NUMBER(10),
                              SecurityId      VARCHAR2(12),
                              Quantity        NUMBER(32, 12),
                              Price           NUMBER(32, 12),
                              PriceType       VARCHAR2(12),
                              CurrencyId      VARCHAR2(4),
                              TrdAccId        VARCHAR2(12),
                              RepoValue       NUMBER(32, 12),
                              IsTrust         NUMBER(5),
                              IsSeb           NUMBER(5),
                              OrdTypeCode     VARCHAR2(4),
                              PriceFIID       VARCHAR2(4),
                              IsAddress       VARCHAR2(8),
                              PartyID         NUMBER(10),
                              SfContrID       NUMBER(10),
                              Commentar       VARCHAR2(24)
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
                              CPFirmId        VARCHAR2(12),
                              CPFirmId_ID     NUMBER(10),
                              CurrencyId      VARCHAR2(4),
                              CurrencyId_FIID NUMBER(10),
                              DealTypeID      NUMBER(5),
                              Decimals        NUMBER(5),
                              DOC_NO          VARCHAR2(32),
                              ExhComm         NUMBER(32, 12),
                              ClrComm         NUMBER(32, 12),
                              ExtSettleCode   VARCHAR2(12),
                              GateDealType    VARCHAR2(4),
                              IsCliring       NUMBER(5),
                              IsRepo          NUMBER(5),
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
                              RecNo           NUMBER(10), 
                              RepoPart        NUMBER(5),
                              RepoPeriod      NUMBER(5),
                              RepoRate        NUMBER(32, 12),
                              RepoValue       NUMBER(32, 12),
                              SecurityId      VARCHAR2(12),
                              SecurityType    VARCHAR2(4),
                              Session         NUMBER(5),
                              SettleCode      VARCHAR2(12),
                              SettleDate      DATE,
                              SettleDate2     DATE,
                              SettleTime      DATE,
                              CliringTime     DATE,
                              TradeDate       DATE,
                              TradeNo         VARCHAR2(20),
                              TradeTime       DATE,
                              TradeType       VARCHAR2(1),
                              TrdAccId        VARCHAR2(12),
                              UINNumber       VARCHAR2(50),
                              Value_          NUMBER(32,12),
                              Value2_         NUMBER(32,12),
                              CcpCode         VARCHAR2(16),
                              PrimaryOrderID  VARCHAR2(20),
                              SpecialPeriod   VARCHAR2(32),
                              TradePeriod     VARCHAR2(7),
                              ISIN            VARCHAR2(12),
                              ClrAccCode      VARCHAR2(12),
                              TradeModeID     NUMBER(5),
                              InfType         NUMBER(5),
                              WorkSettleDate_ DATE,
                              Commentar       VARCHAR2(64)
                             );
                           
    -- Общая структура движения денежных средств                              
    TYPE DATAMONEYMOTION_t IS RECORD  (EntryNumber      VARCHAR2(211),
                                       ClrAccCode       VARCHAR2(16),
                                       BankAccCode      VARCHAR2(32),
                                       Debit            NUMBER(32, 12),
                                       Credit           NUMBER(32, 12),
                                       ReportDate       DATE,
                                       CurrencyID       VARCHAR2(3),
                                       CodeType         NUMBER(10),
                                       OtherClirAcc     VARCHAR2(32)
                             );

  -- Общая структура нетто-итогов                         
  TYPE DATANETTOITOG_t IS RECORD  (ClrAccCode       VARCHAR2(16),
                                   ReportDate       DATE,
                                   CurrencyID       VARCHAR2(3),
                                   NettoSum         NUMBER(32, 12),
                                   ObjectCode       VARCHAR2(36)
                                  );

  FUNCTION UpdateClrAccCodeType_SPB03(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_MarketID IN NUMBER) RETURN NUMBER
  IS
    TYPE CodeType_t   IS TABLE OF DDL_EXTSETTLECODE_DBT.t_CodeType%TYPE;
    TYPE ClrAccCode_t IS TABLE OF D_SPB03_TMP.t_ClrAccCode%TYPE;
    v_CodeType CodeType_t;
    v_Code     ClrAccCode_t;
  BEGIN
     SELECT DISTINCT esc.t_CodeType, Tb.t_ClrAccCode
       BULK COLLECT INTO v_CodeType, v_Code
       FROM DDL_EXTSETTLECODE_DBT esc, D_SPB03_TMP Tb
      WHERE esc.t_MarketID = p_MarketID
        AND esc.t_SettleCode = Tb.t_ClrAccCode;

     IF v_Code.COUNT <> 0 THEN
       FORALL i IN v_Code.FIRST .. v_Code.LAST
         UPDATE D_SPB03_TMP Tb
            SET Tb.t_ClrAccCodeType = v_CodeType(i)
          WHERE Tb.t_ClrAccCode = v_Code(i);
       v_CodeType.DELETE;
       v_Code.DELETE;
     END IF;

    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DEAL, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и UpdateClrAccCodeType_SPB03(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=SPB03' ||
                           ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DEAL) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;
  
  FUNCTION GetImportSettleTime RETURN NUMBER
  IS
  BEGIN
    RETURN CASE WHEN Rsb_Common.GetRegBoolValue('SECUR\IMPORT_SETTLETIME', 0) = TRUE
                THEN 1
                ELSE 0
            END;
  EXCEPTION WHEN OTHERS THEN
    RETURN 0;
  END;

  FUNCTION GetFormOrdersImport RETURN NUMBER
  IS
  BEGIN
    RETURN CASE WHEN Rsb_Common.GetRegBoolValue('SECUR\ФОРМИР. ПОРУЧЕНИЯ ПРИ ИМПОРТЕ', 0) = TRUE
                THEN 1
                ELSE 0
            END;
  EXCEPTION WHEN OTHERS THEN
    RETURN 0;
  END;

  --Инициализировать структуру параметров по передамаемым параметрам в строке
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

    --определить параметр CPFirmIdConst
    BEGIN
      IF v_PrmStrMap.EXISTS('CPFirmIdConst') THEN
        p_Prm.CPFirmIdConst := NVL(v_PrmStrMap('CPFirmIdConst'), CHR(1));
      ELSE
        p_Prm.CPFirmIdConst := CHR(1);
      END IF;
    EXCEPTION WHEN OTHERS THEN p_Prm.CPFirmIdConst := CHR(1);
    END;

    --определить параметр SPB_Code
    BEGIN
      IF v_PrmStrMap.EXISTS('SPB_Code') THEN
        p_Prm.SPB_Code := NVL(v_PrmStrMap('SPB_Code'), CHR(1));
      ELSE
        p_Prm.SPB_Code := CHR(1);
      END IF;
    EXCEPTION WHEN OTHERS THEN p_Prm.SPB_Code := CHR(1);
    END;
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
    ELSE
      p_Deal.GateDealType := p_Deal.BuySell;
    END IF;
  END;
  
  --Получение кода трейдера из комментария
  FUNCTION GetTraderCodeSPB(p_Commentar IN VARCHAR2, p_ClientCode IN VARCHAR2) RETURN VARCHAR2
  IS
    v_TraderCode VARCHAR2(64);
  BEGIN
    v_TraderCode := CASE WHEN INSTR(p_Commentar, p_ClientCode) > 0
                         THEN NVL(SUBSTR(p_Commentar, INSTR(p_Commentar, p_ClientCode) + LENGTH(p_ClientCode) + 1), CHR(1))
                         ELSE RSB_GTFN.GetRefNote(p_Commentar, '/')
                     END;
                     
    RETURN CASE WHEN INSTR(v_TraderCode, '/') > 0
                THEN NVL(SUBSTR(v_TraderCode, 1, INSTR(v_TraderCode, '/') - 1), CHR(1))
                ELSE v_TraderCode
            END;    
  END;

  --Формирование уникального номера сделки
  PROCEDURE SetUINNumber(p_Deal IN OUT NOCOPY DATADEALS_t, p_Prm IN PRMREC_t)
  IS
  BEGIN
    IF p_Deal.ClientCode <> CHR(1) THEN
      p_Deal.UINNumber := p_Deal.BuySell || '/' || p_Deal.TradeNo || 's';
    ELSE
      p_Deal.UINNumber := p_Deal.TradeNo;
    END IF;
  END;
  
  --Заполнение и сохранение параметров заявки
  FUNCTION PutRequestInfo(p_Prm IN OUT NOCOPY PRMREC_t, p_Req IN OUT NOCOPY DATAREQ_t, p_ErrMes IN OUT NOCOPY VARCHAR2, p_WarnMes IN OUT NOCOPY VARCHAR2) RETURN NUMBER
  IS
    v_stat         NUMBER(5) := 0;
    v_DealDate     DATE;
    v_TraderCode   VARCHAR2(64);
  BEGIN

    v_stat := RSI_GT.InitRec( RSB_GTFN.RG_REQ ,
                             'Заявка на сделку №' || p_Req.OrderNo, --Наименование объекта
                             0, --проверять существование объекта в системе по коду
                             p_Req.OrderNo, --код объекта
                             p_Req.PartyID, --ID клиента
                             p_ErrMes);

    IF v_stat = 0 THEN

      RSI_GT.SetParmByName('RGDLRQ_KIND',       350/*DOCKIND_REQ_CLIENT_DEAL*/);
      RSI_GT.SetParmByName('RGDLRQ_CODE',       p_Req.OrderNo);
      RSI_GT.SetParmByName('RGDLRQ_CODETS',     p_Req.OrderNo);
      RSI_GT.SetParmByName('RGDLRQ_DATE',       p_Req.TradeDate);
      RSI_GT.SetParmByName('RGDLRQ_TIME',       p_Req.EntryTime);
      RSI_GT.SetParmByName('RGDLRQ_PARTY',      p_Prm.SPB_Code);
      RSI_GT.SetParmByName('RGDLRQ_BUYSALE',    p_Req.BuySell);
      RSI_GT.SetParmByName('RGDLRQ_FIID',       p_Req.SecurityId); 
      RSI_GT.SetParmByName('RGDLRQ_AMOUNT',     Round(p_Req.Quantity,2));
      RSI_GT.SetParmByName('RGDLRQ_PRICE',      p_Req.Price);
      RSI_GT.SetParmByName('RGDLRQ_REPORATE',   p_Req.RepoRate);
      RSI_GT.SetParmByName('RGDLRQ_STATUS',     p_Req.Status);
      RSI_GT.SetParmByName('RGDLRQ_PRICETYPE',  p_Req.PriceType);
      RSI_GT.SetParmByName('RGDLRQ_CURRENCYID', p_Req.CurrencyId);
      RSI_GT.SetParmByName('RGDLRQ_PRICEFIID',  p_Req.CurrencyId); -- валюта цены = валюте расчетов
      RSI_GT.SetParmByName('RGDLRQ_ORDTYPECODE',p_Req.OrdTypeCode);
      RSI_GT.SetParmByName('RGDLRQ_ISADDRESS',  p_Req.IsAddress);
      RSI_GT.SetParmByName('RGDLRQ_CLIENT',     p_Req.ClientCode);
      RSI_GT.SetParmByName('RGDLRQ_GRNDNUM',    '');

      v_TraderCode := GetTraderCodeSPB(p_Req.Commentar, p_Req.ClientCode);
      IF v_TraderCode <> CHR(1) THEN
        RSI_GT.SetParmByName('RGDLRQ_TRADERCODE', v_TraderCode);
      END IF;

      --проверка ошибок после добавления параметров
      IF RSI_GT.GetLastError <> CHR(1) THEN
        v_stat := 1;
        p_ErrMes := RSI_GT.GetLastError;
      END IF;
    END IF;

    RETURN v_stat;
  END;

  --Заполнение и сохранение параметров сделки/клиринга
  FUNCTION PutDealInfoPM(p_Prm IN OUT NOCOPY PRMREC_t, p_Deal IN OUT NOCOPY DATADEALS_t, p_ErrMes IN OUT NOCOPY VARCHAR2, p_WarnMes IN OUT NOCOPY VARCHAR2) RETURN NUMBER
  IS
    v_stat         NUMBER(5) := 0;
    v_DealDate     DATE;
    v_Relativ      CHAR;
    v_PaymDate     DATE;
    v_NDay         NUMBER(10) := 0;
    v_IndexYm      NUMBER(5);
    v_IndexYn      NUMBER(5);
    v_TraderCode   VARCHAR2(64);
  BEGIN

    v_stat := RSI_GT.InitRec(CASE WHEN p_Deal.IsCliring = 1 THEN RSB_GTFN.RG_CLEARING ELSE RSB_GTFN.RG_DEAL END,
                             CASE WHEN p_Deal.IsCliring = 1 THEN 'Клиринг. ' ELSE '' END || 'Сделка на ПАО СПБ №' || p_Deal.UINNumber, --Наименование объекта
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

      RSI_GT.SetParmByName('RGDLTC_ISIN', p_Deal.ISIN);
      RSI_GT.SetParmByName('RGDLTC_DEALCODETS', p_Deal.TradeNo);
      RSI_GT.SetParmByName('RGDLTC_DEALTIME',   p_Deal.TradeTime);
      
      IF p_Deal.IsCliring = 1 AND p_Deal.CliringTime <> RSI_GT.ZeroTime THEN
        RSI_GT.SetParmByName('RGDLLG_SUPTIME_01', p_Deal.CliringTime);
        RSI_GT.SetParmByName('RGDLLG_SUPTIME_02', p_Deal.CliringTime);
      END IF;

      IF p_Deal.ClientCode <> CHR(1) THEN
        --по единому краткому коду клиента из ДБО находим клиента
        RSI_GT.SetParmByName('RGDLTC_CLIENT', p_Deal.ClientCode);
      END IF;

      IF ( ( p_Prm.FileImport = RSB_GTFN.FileSPB03 OR p_Prm.FileImport = RSB_GTFN.FileMFB06C )AND p_Deal.IsREPO = 0)  THEN
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
      RSI_GT.SetParmByName('RGDLLG_NKD_01', Round(p_Deal.AccInt, 2));

      --сумма сделки без НКД
      RSI_GT.SetParmByName('RGDLLG_COST_01', Round(p_Deal.Value_, 2));

      --общая сумма сделки
      RSI_GT.SetParmByName('RGDLLG_TOTALCOST_01', Round(p_Deal.Amount, 2));

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
      RSI_GT.SetParmByName('RGDLTC_USERFLAG1', CNST.SET_CHAR);
      RSI_GT.SetParmByName('RGDLTC_USERFLAG3', CNST.SET_CHAR);

      --Вид заявки в соответствии с правилами торгов
     -- RSI_GT.SetParmByName('RGDLTC_ORDTYPECODE', p_Deal.OrdTypeCode);

      --тип сделки на ММВБ
      IF p_Prm.FileImport IN (RSB_GTFN.FileSPB03) THEN
        RSI_GT.SetParmByName('RGDLTC_KIND', p_Deal.TradeType);
      ELSE
        IF p_Deal.IsREPO = 1 THEN
          RSI_GT.SetParmByName('RGDLTC_KIND', 'R');
        ELSE
          RSI_GT.SetParmByName('RGDLTC_KIND', 'T');
        END IF;
      END IF;
      --комиссия биржи
      --Все три биржевые комиссии в импортированной сделке обрабатываются одной суммой - это правильно
      IF p_Deal.IsCliring = 0 THEN
        RSI_GT.SetParmByName('RGDLTC_AMOUNT_COMM', Round(p_Deal.ExhComm, 2));
        RSI_GT.SetParmByName('RGDLTC_CLRCOMM', Round(p_Deal.ClrComm, 2));
      ELSE -- p_Deal.IsCliring = true
        RSI_GT.SetParmByName('RGDLTC_REPOPART', p_Deal.RepoPart);
        RSI_GT.SetParmByName('RGDLTC_BUYSELL', p_Deal.BuySell);
        RSI_GT.SetParmByName('RGDLTC_CLIRINGTIME', p_Deal.CliringTime);
        RSI_GT.SetParmByName('RGDLTC_SESSION', p_Deal.Session);
      END IF;

      --дата сделки
      RSI_GT.SetParmByName('RGDLTC_DEALDATE', v_DealDate);

      IF ( p_Prm.FileImport = RSB_GTFN.FileSPB03 AND p_Deal.CcpCode <> CHR(1)) THEN
        RSI_GT.SetParmByName('RGDLTC_PARTY', p_Deal.CcpCode);
      ELSIF p_Deal.CPFirmId <> CHR(1) THEN
        RSI_GT.SetParmByName('RGDLTC_PARTY', p_Deal.CPFirmId);
      END IF;

      v_PaymDate := v_DealDate;

      RSI_GT.SetParmByName('RGDLTC_MARKET', p_Prm.SPB_Code);
      --суммы по второй части для РЕПО
      IF p_Deal.IsREPO = 1 THEN
        v_NDay := p_Deal.NDay1_;
        v_PaymDate := p_Deal.PaymDate1_;
        IF p_Deal.RepoPart <> 2 THEN --заполним даты исполнения по 1 ч по сделке и по клирингу

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
          v_PaymDate := p_Deal.PaymDate2_;

          IF p_Deal.RepoPart = 2 THEN
            v_PaymDate := p_Deal.PaymDate2_;

            IF p_Deal.SettleDate2 = p_Deal.WorkSettleDate_ AND v_PaymDate <> p_Deal.SettleDate2 THEN
               p_WarnMes := 'Срок сделки "' || p_Deal.TradeNo || '", определенный параметром SettleDate = ' || TO_CHAR(p_Deal.SettleDate2, 'DD.MM.YYYY') || ', не соответствует коду расчетов "' || TO_CHAR(p_Deal.SettleCode, 'DD.MM.YYYY') || '" по сделке.';
            ELSIF p_Deal.SettleDate2 <> p_Deal.WorkSettleDate_ AND p_Deal.WorkSettleDate_ <> v_PaymDate THEN
               p_ErrMes := 'По сделке "' || p_Deal.TradeNo || '", дата SettleDate = ' || TO_CHAR(p_Deal.SettleDate2, 'DD.MM.YYYY') || ', указанная в файле выгрузки, определилась как ' || TO_CHAR(v_PaymDate, 'DD.MM.YYYY') || ' по календарю ' || TO_CHAR(p_Deal.CalendId) || '. Исправьте параметры дня ' || TO_CHAR(p_Deal.SettleDate2, 'DD.MM.YYYY') || ' в перечисленных календарях: вид обслуживания = "Банковское", признак баланса установлен.';
            END IF;

            v_PaymDate := p_Deal.SettleDate2;
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
        v_PaymDate := p_Deal.PaymDate2_;
        v_NDay := p_Deal.NDay1_;
        IF p_Deal.SettleDate <> p_Deal.WorkSettleDate_ THEN
          p_ErrMes := 'По сделке "' || p_Deal.TradeNo || '", дата SettleDate = ' || TO_CHAR(p_Deal.SettleDate, 'DD.MM.YYYY') || ', указанная в файле выгрузки, определилась как ' || TO_CHAR(p_Deal.WorkSettleDate_, 'DD.MM.YYYY') || ' по календарю ' || TO_CHAR(p_Deal.CalendId) || '. Исправьте параметры дня ' || TO_CHAR(p_Deal.SettleDate, 'DD.MM.YYYY') || ' в перечисленных календарях: вид обслуживания = "Банковское", признак баланса установлен.';
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
      RSI_GT.SetParmByName('RGDLTC_DATE_CM', p_Prm.imp_date);

      RSI_GT.SetParmByName('RGDLPM_NUMBPAYMS', 0);

      --точность цены
      RSI_GT.SetParmByName('RGDLLG_POINT_01', p_Deal.Decimals);
      RSI_GT.SetParmByName('RGDLLG_POINT_02', p_Deal.Decimals);

      RSI_GT.SetParmByName(RSB_GTFN.GetParmName('RGDLPM_PAYFI', Rsb_Payment.BAi), p_Deal.CurrencyId);
      RSI_GT.SetParmByName(RSB_GTFN.GetParmName('RGDLPM_PAYFI', Rsb_Payment.CAi), p_Deal.CurrencyId);
      RSI_GT.SetParmByName(RSB_GTFN.GetParmName('RGDLPM_PAYFI', Rsb_Payment.PM_PURP_AVANCE), p_Deal.CurrencyId);

      IF p_Deal.IsREPO = 1 THEN
        RSI_GT.SetParmByName(RSB_GTFN.GetParmName('RGDLPM_PAYFI', Rsb_Payment.BRi), p_Deal.CurrencyId);
        RSI_GT.SetParmByName(RSB_GTFN.GetParmName('RGDLPM_PAYFI', Rsb_Payment.CRi), p_Deal.CurrencyId);
        RSI_GT.SetParmByName(RSB_GTFN.GetParmName('RGDLPM_PAYFI', Rsb_Payment.PM_PURP_BACK_AVANCE), p_Deal.CurrencyId);
      END IF;

      IF p_Prm.FileImport = RSB_GTFN.FileSPB03 THEN
        RSI_GT.SetParmByName('RGDLTC_FACEVALUE', p_Deal.FaceValue);
        IF p_Deal.PrimaryOrderID <> CHR(1) THEN
           RSI_GT.SetParmByName('RGDLTC_ORDERNOM', p_Deal.PrimaryOrderID);
        ELSE
           RSI_GT.SetParmByName('RGDLTC_ORDERNOM', p_Deal.OrderNo);
        END IF;
        RSI_GT.SetParmByName('RGDLLG_CFI_01', p_Deal.CurrencyId);
        IF p_Deal.IsREPO = 1 THEN
          RSI_GT.SetParmByName('RGDLLG_CFI_02', p_Deal.CurrencyId);
        END IF;
        RSI_GT.SetParmByName('RGDLTC_TRADEPERIOD', p_Deal.TradePeriod);
        IF p_Deal.SpecialPeriod <> CHR(1) THEN
           RSI_GT.SetParmByName('RGDLTC_SPECIALPERIOD', p_Deal.SpecialPeriod);
        END IF;
      END IF;

      IF p_Prm.FileImport = RSB_GTFN.FileSPB03 AND p_Deal.ClientCode <> CHR(1) AND p_Prm.FORM_ORDERS_IMPORT = 1 THEN
        RSI_GT.SetParmByName('RGDLTC_INDOC', p_Deal.PrimaryOrderID);

        --Дата поручения на сделку
        RSI_GT.SetParmByName('RGDLTC_INDOCDATE', v_DealDate);
        --Время поручения на сделку
        RSI_GT.SetParmByName('RGDLTC_INDOCTIME', p_Deal.TradeTime - (1/1440*1));
      END IF;
      
      IF p_Prm.FileImport = RSB_GTFN.FileSPB03 AND p_Deal.ClientCode <> CHR(1) THEN
        v_TraderCode := GetTraderCodeSPB(p_Deal.Commentar, p_Deal.ClientCode);
        IF v_TraderCode <> CHR(1) THEN
          RSI_GT.SetParmByName('RGDLTC_TRADERCODE', v_TraderCode);
        END IF;
      END IF;

      --проверка ошибок после добавления параметров
      IF RSI_GT.GetLastError <> CHR(1) THEN
        v_stat := 1;
        p_ErrMes := RSI_GT.GetLastError;
      END IF;
    END IF;

    RETURN v_stat;
  END;

  --Подготовить отчеты биржи и обновить данные во временной таблице SPB03
  PROCEDURE PrepareMarketReportsSPB03(p_Prm IN OUT NOCOPY PRMREC_t)
  IS
    TYPE DOCNOARR_t    IS TABLE OF D_SPB03_TMP.T_DOC_NO%TYPE;
    TYPE MRKREPIDARR_t IS TABLE OF NUMBER(10);
    DocNoArr         DOCNOARR_t := DOCNOARR_t();
    MrkRepIDArr      MRKREPIDARR_t := MRKREPIDARR_t();
    v_stat           NUMBER(5);
    v_MarketReportID NUMBER(10);
    v_ErrMes         VARCHAR2(512);
  BEGIN
    FOR cData IN (SELECT DISTINCT SPB03.t_DOC_NO DOC_NO FROM D_SPB03_TMP SPB03)
    LOOP
      v_stat := RSB_GTFN.WriteMarketReport(p_Prm.SeanceID, p_Prm.Source_Code, v_MarketReportID, v_ErrMes, cData.DOC_NO, p_Prm.imp_date, p_Prm.SPB_Code);

      IF v_ErrMes <> CHR(1) THEN
        RSB_GTLOG.Error(v_ErrMes);
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
        UPDATE D_SPB03_TMP
           SET t_MarketReportID = MrkRepIDArr(i)
         WHERE t_DOC_NO = DocNoArr(i);

      DocNoArr.DELETE;
      MrkRepIDArr.DELETE;
    END IF;
  END;

  --Подготовить отчеты биржи и обновить данные во временной таблице MFB06C
  PROCEDURE PrepareMarketReportsMFB06C(p_Prm IN OUT NOCOPY PRMREC_t)
  IS
    TYPE DOCNOARR_t    IS TABLE OF D_SPB03_TMP.T_DOC_NO%TYPE;
    TYPE MRKREPIDARR_t IS TABLE OF NUMBER(10);
    DocNoArr         DOCNOARR_t := DOCNOARR_t();
    MrkRepIDArr      MRKREPIDARR_t := MRKREPIDARR_t();
    v_stat           NUMBER(5);
    v_MarketReportID NUMBER(10);
    v_ErrMes         VARCHAR2(512);
  BEGIN
    FOR cData IN (SELECT DISTINCT MFB06C.t_DOC_NO DOC_NO FROM D_MFB06C_TMP MFB06C WHERE MFB06C.t_IsOutExcRepo = 1 )
    LOOP
      v_stat := RSB_GTFN.WriteMarketReport(p_Prm.SeanceID, p_Prm.Source_Code, v_MarketReportID, v_ErrMes, cData.DOC_NO, p_Prm.imp_date, p_Prm.SPB_Code);

      IF v_ErrMes <> CHR(1) THEN
        RSB_GTLOG.Error(v_ErrMes);
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
        UPDATE D_MFB06C_TMP
           SET t_MarketReportID = MrkRepIDArr(i)
         WHERE t_DOC_NO = DocNoArr(i)
         AND t_IsOutExcRepo = 1;

      DocNoArr.DELETE;
      MrkRepIDArr.DELETE;
    END IF;
  END;

---------------------------
  FUNCTION DayInYear(YYYY IN INTEGER) RETURN NUMBER
  IS
    v_CountDay NUMBER;
  BEGIN
    v_CountDay := TO_DATE('31.12.'||TO_CHAR(YYYY), 'DD.MM.YYYY') - TO_DATE('01.01.'||TO_CHAR(YYYY), 'DD.MM.YYYY') + 1;
    RETURN v_CountDay;
  END;

---------------------------
  PROCEDURE FindDataPart2(SettleDate IN DATE, SettleDate2 IN DATE, Amount IN NUMBER, Amount2 IN NUMBER, RepoRate IN OUT NUMBER)
  IS
    N NUMBER(32, 12);
    YYYY_beg DATE; 
    YYYY_end DATE; 
    YYYY_cur DATE;
    YYYY_begI INTEGER; 
    YYYY_endI INTEGER; 
    YYYY_curI INTEGER;
  BEGIN
    RepoRate := 0;
    IF SettleDate <> SettleDate2 THEN
      YYYY_begI:= EXTRACT(YEAR FROM SettleDate);
      YYYY_endI:= EXTRACT(YEAR FROM SettleDate2);
      IF YYYY_begI = YYYY_endI THEN
        N := (SettleDate2 - SettleDate)/DayInYear(YYYY_begI);
      ELSE 
        YYYY_curI := YYYY_begI + 1; 
        N := (TO_DATE( '31.12.' || TO_CHAR(YYYY_curI), 'DD.MM.YYYY') - SettleDate) / DayInYear(YYYY_begI);
        WHILE YYYY_curI < YYYY_endI
        LOOP 
          N := N + 1;
          YYYY_curI := YYYY_curI + 1;
        END LOOP;
        N := N + (SettleDate2 - (TO_DATE( '31.12.' || TO_CHAR(YYYY_curI), 'DD.MM.YYYY'))) / DayInYear(YYYY_begI);
      END IF;
    END IF;
    
    IF (Amount * N) <> 0 THEN
      RepoRate := ROUND(((Amount2 - Amount) / (Amount * N)) * 100, 4);
    END IF;
  END;

---------------------------
  FUNCTION ProcessReplRecByTmp_SPB03(p_Prm IN OUT NOCOPY PRMREC_t) RETURN NUMBER
  IS
    v_stat    NUMBER(5) := 0;
    v_ErrMes  VARCHAR2(512) := CHR(1);
    v_WarnMes VARCHAR2(512) := CHR(1);
    v_Deal    DATADEALS_t;
  BEGIN

    FOR cData IN (SELECT t.*
                    FROM D_SPB03_TMP t
                   ORDER BY t.T_DOC_NO, t.t_recno
                 )
    LOOP
      v_ErrMes  := CHR(1);
      v_WarnMes := CHR(1);
      v_Deal.DOC_NO          := cData.t_DOC_NO; 
      v_Deal.CLRACCCODE      := cData.t_CLRACCCODE;
      v_Deal.RecNo           := cData.t_RECNO;
      v_Deal.CurrencyId      := cData.t_CURRENCYID;
      v_Deal.SettleDate      := cData.t_SETTLEDATE;
      v_Deal.SecurityId      := cData.t_SECURITYID;
      v_Deal.ISIN            := cData.t_ISIN;
      v_Deal.FaceValue       := cData.t_FACEVALUE;
      v_Deal.PriceType       := cData.t_PRICETYPE;
      v_Deal.ClientCode      := cData.t_CLIENTCODE;
      v_Deal.REPOPART        := cData.t_REPOPART;
      v_Deal.BuySell         := cData.t_BUYSELL;
      v_Deal.SettleCode      := cData.t_SETTLECODE;
      v_Deal.TradeNo         := cData.t_TRADENO;
      v_Deal.TradeDate       := cData.t_TRADEDATE;
      v_Deal.TradeTime       := cData.t_TRADETIME;
      v_Deal.TradeType       := cData.t_TRADETYPE;
      v_Deal.TRADEPERIOD     := cData.t_TRADEPERIOD;
      v_Deal.SPECIALPERIOD   := cData.t_SPECIALPERIOD;
      v_Deal.PRIMARYORDERID  := cData.t_PRIMARYORDERID;
      v_Deal.ORDERNO         := cData.t_ORDERID;
      v_Deal.Decimals        := cData.t_DECIMALS;
      v_Deal.Price           := cData.t_PRICE;
      v_Deal.Quantity        := cData.t_QUANTITY;
      v_Deal.Value_          := cData.t_VALUE;
      v_Deal.Amount          := cData.t_AMOUNT;
      v_Deal.ExhComm         := cData.t_EXCHCOMM;
      v_Deal.CLRCOMM         := cData.t_CLRCOMM;
      v_Deal.CCPCODE         := cData.t_CCPCODE;
      v_Deal.CPFirmId        := cData.t_CPFIRMID;
      v_Deal.RepoRate        := cData.t_REPORATE;
      v_Deal.AccInt          := cData.t_ACCINT;
      v_Deal.PRICE2          := cData.t_PRICE2;
      v_Deal.SecurityType    := cData.t_SECURITYTYPE;
      v_Deal.Commentar       := cData.t_COMMENTAR;
      v_Deal.CalcItogDate_   := cData.t_CalcItogDate;
      v_Deal.CalendID        := cData.t_CalendID;
      v_Deal.ClientCode      := cData.t_ClientCode;
      v_Deal.ClientContrID   := cData.t_SfContrID;
      v_Deal.ClientID        := cData.t_PartyID;
      v_Deal.ClrComm         := cData.t_ClrComm;
      v_Deal.CPFirmId        := cData.t_CPFirmId;
      v_Deal.CPFirmId_ID     := cData.t_CPFirmPartyID;
      v_Deal.CurrencyId      := cData.t_CurrencyId;
      v_Deal.CurrencyId_FIID := cData.t_CurID;
      v_Deal.NDay1_          := cData.t_NDay1;
      v_Deal.PaymDate1_      := cData.t_PaymDate1;
      v_Deal.PaymDate2_      := cData.t_PaymDate2;
      v_Deal.Portfolio_      := cData.t_Portfolio;
      v_Deal.MarketReportID  := cData.t_MarketReportID;
      v_Deal.MarketSchemeID  := cData.t_MarketSchemeID;
      v_Deal.WorkSettleDate_ := cData.t_WorkSettleDate;
      v_Deal.IsRepo          := cData.t_IsRepo;
      v_Deal.IsCliring       := 0;
      v_Deal.TradeModeID     := 0;
         
      IF v_Deal.IsREPO = 1 THEN
        v_Deal.SettleDate2    := to_date(cData.t_SettleDate);
        v_Deal.AccInt2        := cData.t_ACCINT2;
        v_Deal.Amount2        := cData.t_AMOUNT2;
        v_Deal.Value2_        := cData.t_VALUE2;
        v_Deal.Price2         := cData.t_PRICE2_2;
        FindDataPart2(cData.t_SettleDate, cData.t_SettleDate2, v_Deal.Amount, v_Deal.Amount2, v_Deal.RepoRate);
      END IF;
      
      SetPartPrmDeal_PM(v_Deal);
      SetUINNumber(v_Deal, p_Prm);

      IF cData.t_OBJECTID > 0 THEN
        v_ErrMes := 'Ошибка при загрузке сделки с кодом "' || v_Deal.UINNumber || '"' || CHR(10) || 'Запись репликации с действием создать для объекта ' || TO_CHAR(cData.t_OBJECTID) || ' уже передавалась в RS-Bank.' || CHR(10) || 'Повторная репликация невозможна.';
      ELSIF cData.t_ClrAccCodeType = 0 THEN
        v_ErrMes := 'Ошибка при загрузке сделки с кодом "' || v_Deal.UINNumber || '"' || CHR(10) || 'В справочнике расчетных кодов банка не найден код "' || v_Deal.ClrAccCode || '"';
      ELSIF cData.t_ClrAccCodeType <> Rsb_Derivatives.ALG_SP_MONEY_SOURCE_CLIENT THEN
        v_ErrMes := 'Не загружена сделка с кодом "' || v_Deal.UINNumber || '"' || CHR(10) || 'В соответствии с кодом ТКС "' || v_Deal.ClrAccCode || '" сделка не является клиентской';
      ELSIF cData.t_ClrAccCodeType = Rsb_Derivatives.ALG_SP_MONEY_SOURCE_CLIENT AND v_Deal.ClientCode = CHR(1) THEN
        v_ErrMes := 'Ошибка при загрузке клиентской сделки с кодом "' || v_Deal.UINNumber || '"' || CHR(10) || 'В файле импорта не задан атрибут ClientCode';
      ELSIF v_Deal.ClientID <= 0 THEN
        v_ErrMes := 'Ошибка при загрузке сделки с кодом "' || v_Deal.UINNumber || '"' || CHR(10) || 'Не найден субъект через краткий код на бирже "' || v_Deal.ClientCode || '"';
      ELSIF cData.t_CPFirmId <> CHR(1) AND cData.t_CPFirmPartyID = 0 THEN 
        v_ErrMes := 'Ошибка при загрузке сделки с кодом "' || v_Deal.UINNumber || '"' || CHR(10) || 'В ГКБО не найден субъект с принадлежностью "Центральный контрагент"';
      ELSIF v_Deal.CurrencyId <> CHR(1) AND cData.t_CurID = -1 THEN
        v_ErrMes := 'Ошибка при загрузке сделки с кодом "' || v_Deal.UINNumber || '"' || CHR(10) || 'В справочнике не найден ФИ с кодом "' || v_Deal.CurrencyId || '"';
      ELSIF v_Deal.ISIN <> CHR(1) AND cData.t_FIID = -1 THEN
        v_ErrMes := 'Ошибка при загрузке сделки с кодом "' || v_Deal.UINNumber || '"' || CHR(10) || 'В справочнике не найден ФИ с ISIN "' || v_Deal.ISIN || '"'; 
      ELSIF LENGTH(TRIM(TO_CHAR(v_Deal.Quantity))) > 16 THEN
        v_ErrMes := 'Ошибка при загрузке сделки с кодом "' || v_Deal.UINNumber || '"' || CHR(10) || 'Превышено максимально возможное количество загружаемых знаков параметра Quantity';
      ELSIF RSB_GTFN.GetTypeOperationBOCB('B', v_Deal.GateDealType, v_Deal.DealTypeID, v_ErrMes, 0, (CASE WHEN v_Deal.TradeDate = v_Deal.SettleDate THEN 1 ELSE 0 END), 0/*v_Deal.IsSEB*/, 0/*v_Deal.UnderWr*/, 0/*v_Deal.UnderWr_Pokupka*/, 1, 0) <> 0 THEN
        IF v_ErrMes = CHR(1) THEN v_ErrMes:= 'Ошибка при загрузке сделки с кодом "' || v_Deal.UINNumber || '"' || CHR(10) || 'Не удалось определить тип операции БОЦБ'; END IF;
      END IF;

      IF v_ErrMes = CHR(1) THEN
        IF PutDealInfoPM(p_Prm, v_Deal, v_ErrMes, v_WarnMes) = 0 THEN
          v_stat := RSI_GT.SaveBatch(BATCH_SIZE, v_ErrMes);
        END IF;
      END IF;
      
      IF v_ErrMes <> CHR(1) THEN
        RSB_GTLOG.Error(v_ErrMes);
      END IF;

      IF v_WarnMes <> CHR(1) THEN
        RSB_GTLOG.Warning(v_WarnMes);
      END IF;
    END LOOP;

    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.Error('Произошла непредвиденная ошибка при обработке данных в ф-и ProcessReplRecByTmp_SPB03(), ' ||
                    'SeanceID=' || TO_CHAR(p_Prm.SeanceID) || ', FileImport=' || TO_CHAR(p_Prm.FileImport) ||
                    ', Source_Code=' || p_Prm.Source_Code || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                    ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;

---------------------------
  FUNCTION ProcessReplRecByTmp_MFB06C(p_Prm IN OUT NOCOPY PRMREC_t) RETURN NUMBER
  IS
    v_stat    NUMBER(5) := 0;
    v_ErrMes  VARCHAR2(512) := CHR(1);
    v_WarnMes VARCHAR2(512) := CHR(1);
    v_Deal    DATADEALS_t;
  BEGIN

    FOR cData IN (  SELECT t.T_ID_SPB_MFB06C ID_SPB_MFB06C,
                     NVL (t.T_RECNO, 0) RECNO,
                     NVL (t.T_CLIENTCODE, CHR (1)) CLIENTCODE,
                     NVL (t.T_CURRENCYID, CHR (1)) CURRENCYID,
                     NVL (t.T_INFTYPE, 0) INFTYPE,
                     NVL (t.T_CLEARINGTIME, TO_DATE ('01.01.0001 00:00:00', 'dd.mm.yyyy HH24:MI:SS'))CLEARINGTIME,
                     NVL (t.T_SETTLEDATE, TO_DATE ('01.01.0001', 'DD.MM.YYYY')) SETTLEDATE,
                     NVL (t.T_SECURITYID, CHR (1)) SECURITYID,
                     NVL (t.T_ISIN, CHR (1)) ISIN,
                     NVL (t.T_PRICETYPE, CHR (1)) PRICETYPE,
                     NVL (t.T_REPOPART, 0) REPOPART,
                     NVL (t.T_BUYSELL, CHR (1)) BUYSELL,
                     NVL (t.T_SETTLECODE, CHR (1)) SETTLECODE,
                     NVL (t.T_CLRACCCODE, CHR (1)) CLRACCCODE,
                     NVL (t.T_TRADEDATE, TO_DATE ('01.01.0001', 'DD.MM.YYYY')) TRADEDATE,
                     NVL (t.T_TRADENO, 0) TRADENO,
                     NVL (t.T_DECIMALS, 0) DECIMALS,
                     NVL (t.T_PRICE, 0) PRICE,
                     NVL (t.T_QUANTITY, 0) QUANTITY,
                     NVL (t.T_VALUE, 0) VALUE,
                     NVL (t.T_AMOUNT, 0) AMOUNT,
                     NVL (t.T_EXCHCOMM, 0) EXCHCOMM,
                     NVL (t.T_ACCINT, 0) ACCINT,
                     NVL (t.T_CPFIRMID, CHR (1)) CPFIRMID,
                     NVL (t.T_REPOPERIOD, 0) REPOPERIOD,
                     NVL (t.T_REPORTTIME, TO_DATE ('01.01.0001', 'DD.MM.YYYY')) REPORTTIME,
                     NVL (t.T_CLRCOMM, 0) CLRCOMM,
                     NVL (t.T_TRADEMODEID, 0) TRADEMODEID,
                     NVL (t.T_TRADEPERIOD, 0) TRADEPERIOD,
                     NVL (t.T_IsREPO, 0) ISREPO,
                     NVL (t.T_ISERROR, 0) ISERROR,
                     NVL (t.T_ISNOTFIND, 0) ISNOTFIND,
                     NVL (t.T_PARTYID, 0) PARTYID,
                     NVL (t.T_SFCONTRID, 0) SFCONTRID,
                     NVL (t.T_CPFIRMPARTYID, 0) CPFIRMPARTYID,
                     NVL (t.T_CURID, 0) CURID,
                     NVL (t.T_CALENDID, 0) CALENDID,
                     NVL (t.T_MARKETSCHEMEID, 0) MARKETSCHEMEID,
                     NVL (t.T_NDAY1, 0) NDAY1,
                     NVL (t.T_NDAY2, 0) NDAY2,
                     NVL (t.T_PAYMDATE1, TO_DATE ('01.01.0001', 'DD.MM.YYYY')) PAYMDATE1,
                     NVL (t.T_PAYMDATE2, TO_DATE ('01.01.0001', 'DD.MM.YYYY')) PAYMDATE2,
                     NVL (t.T_CALCITOGDATE, TO_DATE ('01.01.0001', 'DD.MM.YYYY')) CALCITOGDATE,
                     NVL (t.T_WORKSETTLEDATE, TO_DATE ('01.01.0001', 'DD.MM.YYYY')) WORKSETTLEDATE,
                     NVL (t.T_OBJECTID, 0) OBJECTID,
                     NVL (t.T_PORTFOLIO, 0) PORTFOLIO,
                     NVL (t.T_ISOUTEXCREPO, 0) ISOUTEXCREPO,
                     NVL (t.T_FIID, -1) FIID,
                     NVL (t.T_MarketReportID, 0) MarketReportID,
                     NVL (DECODE (t.T_REPOPART, 1, Repo_2.t_ACCINT, 0), 0) ACCINT2,
                     NVL (DECODE (t.T_REPOPART, 1, Repo_2.t_AMOUNT, 0), 0) AMOUNT2,
                     NVL (DECODE (t.T_REPOPART, 1, Repo_2.t_VALUE, 0), 0) VALUE2,
                     NVL (DECODE (t.T_REPOPART, 1, Repo_2.t_PRICE, 0), 0) PRICE2,
                     NVL (Repo_2.T_SETTLEDATE, TO_DATE ('01.01.0001', 'DD.MM.YYYY')) SETTLEDATE2,
                     NVL (t.T_DOC_NO, CHR (1)) DOC_NO
                FROM D_MFB06C_TMP t, D_MFB06C_TMP Repo_2
               WHERE     t.T_INFTYPE IN (1, 2)
                     AND (   (   ( (t.T_INFTYPE = 1) OR (t.T_INFTYPE = 2))
                              OR ( (t.T_REPOPART = 1) OR (t.T_REPOPART = 2)))
                          OR (t.T_REPOPART = 0))
                     AND NOT EXISTS
                               (SELECT 1
                                  FROM D_MFB06C_TMP Repo_1
                                 WHERE     t.t_tradeno = Repo_1.t_tradeno
                                       AND Repo_1.T_INFTYPE = 1
                                       AND Repo_1.T_REPOPART = 1
                                       AND t.T_INFTYPE = 1
                                       AND t.T_REPOPART = 2)
                     AND t.t_tradeno = Repo_2.t_tradeno(+)
                     AND Repo_2.T_INFTYPE(+) IN (1, 2, 3)
                     AND Repo_2.T_REPOPART(+) = 2
                     AND t.t_clientCode = Repo_2.t_clientCode(+)
            ORDER BY t.T_ID_SPB_MFB06C
                 )
    LOOP
      v_ErrMes  := CHR(1);
      v_WarnMes := CHR(1);

      v_Deal.DOC_NO          := cData.DOC_NO;
      v_Deal.ClientCode      := cData.CLIENTCODE;
      v_Deal.CurrencyId      := cData.CURRENCYID;
      v_Deal.SettleDate      := cData.SETTLEDATE;
      v_Deal.SecurityId      := cData.SECURITYID;
      v_Deal.ISIN            := cData.ISIN;
      v_Deal.PriceType       := cData.PRICETYPE;
      v_Deal.REPOPART        := cData.REPOPART;
      v_Deal.BuySell         := cData.BUYSELL;
      v_Deal.SettleCode      := cData.SETTLECODE;
      v_Deal.TradeDate       := cData.TRADEDATE;
      v_Deal.TradeNo         := cData.TRADENO;
      v_Deal.Decimals        := cData.DECIMALS;
      v_Deal.Price           := cData.PRICE;
      v_Deal.Quantity        := cData.QUANTITY;
      v_Deal.Value_          := cData.VALUE;
      v_Deal.Amount          := cData.AMOUNT;
      v_Deal.ExhComm         := cData.EXCHCOMM;
      v_Deal.AccInt          := cData.ACCINT;
      v_Deal.CPFirmId        := cData.CPFIRMID;
      v_Deal.TRADEPERIOD     := cData.TRADEPERIOD;
      v_Deal.CLRACCCODE      := cData.CLRACCCODE;
      v_Deal.IsRepo          := cData.IsRepo;
      v_Deal.RepoRate        := 0;
      v_Deal.TradeModeID     := cData.TRADEMODEID;
      v_Deal.RepoPeriod      := cData.REPOPERIOD;
      v_Deal.CPFirmId        := cData.CPFirmId;

      v_Deal.InfType         := cData.InfType;
      v_Deal.CliringTime     := cData.CLEARINGTIME;

      v_Deal.CalendID        := cData.CalendID;
      v_Deal.ClientCode      := cData.ClientCode;
      v_Deal.ClientContrID   := cData.SfContrID;
      v_Deal.ClientID        := cData.PartyID;
      v_Deal.CPFirmId_ID     := cData.CPFirmPartyID;
      v_Deal.CurrencyId      := cData.CurrencyId;
      v_Deal.CurrencyId_FIID := cData.CurID;
      v_Deal.NDay1_          := cData.NDay1;
      v_Deal.PaymDate1_      := cData.PaymDate1;
      v_Deal.PaymDate2_      := cData.PaymDate2;
      v_Deal.Portfolio_      := cData.Portfolio;
      v_Deal.MarketReportID  := cData.MarketReportID;
      v_Deal.MarketSchemeID  := cData.MarketSchemeID;
      v_Deal.WorkSettleDate_ := cData.WorkSettleDate;
      v_Deal.Session         := cData.TRADEPERIOD;

      IF v_Deal.IsREPO = 1 THEN
        v_Deal.AccInt2        := cData.ACCINT2;
        v_Deal.Amount2        := cData.AMOUNT2;
        v_Deal.Value2_        := cData.VALUE2;
        v_Deal.Price2         := cData.PRICE2;
        FindDataPart2(cData.SettleDate, cData.SettleDate2, v_Deal.Amount, v_Deal.Amount2, v_Deal.RepoRate);
        IF cData.ISOUTEXCREPO = 1 THEN
          v_Deal.ClrComm        := cData.CLRCOMM;  
        END IF;
      END IF;
      v_Deal.IsCliring        := 1;  

      SetPartPrmDeal_PM(v_Deal);
      SetUINNumber(v_Deal, p_Prm);

      IF (cData.CPFirmId <> CHR(1) AND cData.CPFirmPartyID = 0 ) THEN 
        v_ErrMes := 'Ошибка при загрузке сделки с кодом "' || v_Deal.UINNumber || '"' || CHR(10) || 'В ГКБО не найден субъект с принадлежностью "Центральный контрагент"';
      ELSIF cData.OBJECTID > 0 THEN
        v_ErrMes := 'Ошибка при загрузке сделки с кодом "' || v_Deal.UINNumber || '"' || CHR(10) || 'Запись репликации с действием создать для объекта ' || TO_CHAR(cData.OBJECTID) || ' уже передавалась в RS-Bank.' || CHR(10) || 'Повторная репликация невозможна.';
      ELSIF v_Deal.ClientCode <> CHR(1) AND v_Deal.ClientID <= 0 THEN
        v_ErrMes := 'Ошибка при загрузке сделки с кодом "' || v_Deal.UINNumber || '"' || CHR(10) || 'Не найден субъект через краткий код на бирже "' || v_Deal.ClientCode || '"';
      ELSIF v_Deal.CurrencyId <> CHR(1) AND cData.CurID = -1 THEN
        v_ErrMes := 'Ошибка при загрузке сделки с кодом "' || v_Deal.UINNumber || '"' || CHR(10) || 'В справочнике не найден ФИ с кодом "' || v_Deal.CurrencyId || '"';
      ELSIF v_Deal.ISIN <> CHR(1) AND cData.FIID = -1 THEN
        v_ErrMes := 'Ошибка при загрузке сделки с кодом "' || v_Deal.UINNumber || '"' || CHR(10) || 'В справочнике не найден ФИ с ISIN "' || v_Deal.ISIN || '"';
      ELSIF LENGTH(TRIM(TO_CHAR(v_Deal.Quantity))) > 16 THEN
        v_ErrMes := 'Ошибка при загрузке сделки с кодом "' || v_Deal.UINNumber || '"' || CHR(10) || 'Превышено максимально возможное количество загружаемых знаков параметра Quantity';
      ELSIF RSB_GTFN.GetTypeOperationBOCB('B', v_Deal.GateDealType, v_Deal.DealTypeID, v_ErrMes, 0, (CASE WHEN v_Deal.TradeDate = v_Deal.SettleDate THEN 1 ELSE 0 END), 0/*v_Deal.IsSEB*/, 0/*v_Deal.UnderWr*/, 0/*v_Deal.UnderWr_Pokupka*/, 1, 0) <> 0 THEN
        IF v_ErrMes = CHR(1) THEN v_ErrMes:= 'Ошибка при загрузке сделки с кодом "' || v_Deal.UINNumber || '"' || CHR(10) || 'Не удалось определить тип операции БОЦБ'; END IF;
      END IF;

      IF v_ErrMes = CHR(1) THEN
        IF(cData.ISOUTEXCREPO = 1) THEN
          v_Deal.IsCliring        := 0;  
          SetPartPrmDeal_PM(v_Deal);

          IF PutDealInfoPM(p_Prm, v_Deal, v_ErrMes, v_WarnMes) = 0 THEN
            v_stat := RSI_GT.SaveBatch(BATCH_SIZE, v_ErrMes); 
          END IF;
        END IF;

        v_Deal.IsCliring := 1;  
        SetPartPrmDeal_PM(v_Deal);

        IF PutDealInfoPM(p_Prm, v_Deal, v_ErrMes, v_WarnMes) = 0 THEN
          v_stat := RSI_GT.SaveBatch(BATCH_SIZE, v_ErrMes); 
        END IF;
      END IF;
      
      IF v_ErrMes <> CHR(1) THEN
        RSB_GTLOG.Error(v_ErrMes);
      END IF;

      IF v_WarnMes <> CHR(1) THEN
        RSB_GTLOG.Warning(v_WarnMes);
      END IF;
    END LOOP;

    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.Error('Произошла непредвиденная ошибка при обработке данных в ф-и ProcessReplRecByTmp_MFB06C(), ' ||
                    'SeanceID=' || TO_CHAR(p_Prm.SeanceID) || ', FileImport=' || TO_CHAR(p_Prm.FileImport) ||
                    ', Source_Code=' || p_Prm.Source_Code || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                    ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;

---------------------------
  FUNCTION ProcessReplRecByTmp_ORDERS(p_Prm IN OUT NOCOPY PRMREC_t) RETURN NUMBER
  IS
    v_stat     NUMBER(5) := 0;
    v_ErrMes   VARCHAR2(512) := CHR(1);
    v_WarnMes  VARCHAR2(512) := CHR(1);
    v_Status   VARCHAR2(32)  := CHR(1);
    v_Streason VARCHAR2(32)  := CHR(1);
    v_Req     DATAREQ_t;
  BEGIN

    FOR cData IN (  SELECT T_ID_SPB_ORDERS ID_SPB_ORDERS,   
                      nvl(t.T_ORDERNO,CHR(1)) ORDERNO, 
                      nvl(t.T_TRADEDATE,to_date('01.01.0001','DD.MM.YYYY')) TRADEDATE, 
                      nvl(t.T_TRADETIME,to_date('01.01.0001','DD.MM.YYYY')) ENTRYTIME, 
                      nvl(t.T_CLIENTCODE,chr(1)) CLIENTCODE, 
                      nvl(t.T_BUYSELL,chr(1)) BUYSELL, 
                      nvl(t.T_IS_ADDRESS,chr(1)) IS_ADDRESS,
                      nvl(t.T_SECURITYID,chr(1)) SECURITYID, 
                      nvl(t.T_QUOTE_CURRENCY,0) QUOTE_CURRENCY, 
                      nvl(t.T_CURRENCYID,chr(1)) CURRENCYID, 
                      nvl(t.T_PRICE,0) PRICE, 
                      nvl(t.T_REPO_PRICE,0) REPO_PRICE,
                      nvl(t.T_QUANTITY,0) QUANTITY, 
                      nvl(t.T_STATUS,chr(1)) STATUS, 
                      nvl(t.T_STATUS_REASON,chr(1)) STATUS_REASON, 
                      nvl(t.T_ACTION,chr(1)) ACTION, 
                      nvl(t.T_PARTYID,0) PARTYID, 
                      nvl(t.T_OBJECTID,0) OBJECTID,
                      nvl(t.T_COMMENTAR,chr(1)) COMMENTAR,
                      nvl(t.T_FIID,0) FIID 
                FROM  D_ORDERS_TMP t
               WHERE  t.T_ACTION <> 'New' 
                  AND t.T_ID_SPB_ORDERS 
                      IN (  SELECT MIN(T_ID_SPB_ORDERS) ID_SPB_ORDERS 
                              FROM D_ORDERS_TMP mn 
                             WHERE mn.T_ACTION <> 'New' 
                          AND t.T_ORDERNO = mn.T_ORDERNO) 
                  AND nvl(t.T_CLIENTCODE,chr(1)) <> chr(1)
               ORDER BY T_ID_SPB_ORDERS)
    LOOP
      v_ErrMes  := CHR(1);
      v_WarnMes := CHR(1);
      v_Req.OrderNo     := to_char(cData.ORDERNO);
      v_Req.TradeDate   := cData.TRADEDATE;
      v_Req.EntryTime   := cData.ENTRYTIME;
      v_Req.ClientCode  := cData.CLIENTCODE;
      v_Req.BuySell     := cData.BUYSELL;
      v_Req.IsAddress   := cData.IS_ADDRESS;
      v_Req.SecurityId  := cData.SECURITYID;
      v_Req.PriceFIID   := cData.QUOTE_CURRENCY;
      v_Req.CurrencyId  := cData.CURRENCYID;
      v_Req.Price       := cData.PRICE;
      v_Req.RepoRate    := cData.REPO_PRICE;
      v_Req.Quantity    := cData.QUANTITY;
      v_Req.PartyID     := cData.PARTYID;
      v_Req.Commentar   := cData.COMMENTAR;
      v_Req.OrdTypeCode := '';
      v_Req.PriceType   := 'CASH';
      v_Status          := trim(cData.STATUS);

      IF((v_Status = 'NEW') OR (v_Status = 'FILLED') OR (v_Status = 'PARTFILLED')) THEN
        v_Req.Status := 'M';
      ELSIF((v_Status = 'CANCELLED') OR (v_Status = 'PARTCANCELLED')) THEN
        v_Streason := trim(cData.STATUS_REASON);
        if((v_Streason = 'USER_CANCEL') OR (v_Streason = 'USER_MASS_CANCEL') OR
           (v_Streason = 'BROKER_CANCEL') OR (v_Streason = 'BROKER_MASS_CANCEL')) THEN
          v_Req.Status := 'W';
        ELSIF( (v_Streason = 'CTRPARTY_DECLINE') OR (v_Streason = 'EXPIRED_CROSSTRADE') OR (v_Streason = 'EXPIRED_ORDERBOOK_CROSS')) THEN
          v_Req.Status := 'F';
        ELSE
          v_Req.Status := 'C';
        END IF;
      ELSE
        v_Req.Status := '';
      END IF;

      IF cData.OBJECTID > 0 THEN
        v_ErrMes := 'Ошибка при загрузке сделки с кодом "' || v_Req.OrderNo || '"' || CHR(10) || 'Запись репликации с действием создать для объекта ' || TO_CHAR(cData.OBJECTID) || ' уже передавалась в RS-Bank.' || CHR(10) || 'Повторная репликация невозможна.';
      ELSIF v_Req.ClientCode <> CHR(1) AND v_Req.PartyID <= 0 THEN
        v_ErrMes := 'Ошибка при загрузке сделки с кодом "' || v_Req.OrderNo || '"' || CHR(10) || 'Не найден субъект через краткий код на бирже "' || v_Req.ClientCode || '"';
      ELSIF v_Req.SecurityId <> CHR(1) AND cData.FIID = -1 THEN
        v_ErrMes := 'Ошибка при загрузке заявки с кодом "' || v_Req.OrderNo || '"' || CHR(10) || 'В справочнике не найден ФИ с кодом "' || v_Req.SecurityId || '"';
      ELSIF LENGTH(TRIM(TO_CHAR(v_Req.Quantity))) > 16 THEN
        v_ErrMes := 'Ошибка при загрузке сделки с кодом "' || v_Req.OrderNo || '"' || CHR(10) || 'Превышено максимально возможное количество загружаемых знаков параметра Quantity';
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

    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.Error('Произошла непредвиденная ошибка при обработке данных в ф-и ProcessReplRecByTmp_ORDERS(), ' ||
                    'SeanceID=' || TO_CHAR(p_Prm.SeanceID) || ', FileImport=' || TO_CHAR(p_Prm.FileImport) ||
                    ', Source_Code=' || p_Prm.Source_Code || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                    ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;


  --Создание объектов записей репликаций по данным временной таблицы SPB03 (сделки)
  FUNCTION CreateReplRecByTmp_SPB03(p_SeanceID IN NUMBER, p_ImpDate IN DATE, p_SourceCode IN VARCHAR2, p_PrmStr IN VARCHAR2) RETURN NUMBER
  IS
    v_stat    NUMBER(5) := 0;
    v_ErrMes  VARCHAR2(512) := CHR(1);
    v_Prm     PRMREC_t;
  BEGIN
    RSB_GTLOG.Init(BATCH_SIZE, p_SeanceID, RSB_GTFN.RG_DEAL);
    SetPrmRecByStr(v_Prm, p_PrmStr);
    v_Prm.SeanceID           := p_SeanceID;
    v_Prm.imp_date           := p_ImpDate;
    v_Prm.Source_Code        := p_SourceCode;
    v_Prm.IMPORT_SETTLETIME  := GetImportSettleTime;
    v_Prm.FileImport         := RSB_GTFN.FileSPB03;
    v_Prm.FORM_ORDERS_IMPORT := GetFormOrdersImport;
    PrepareMarketReportsSPB03(v_Prm);
    v_stat := RSI_GT.Init(RSI_GT.DIRECT_INSERT_YES, p_SeanceID, p_SourceCode, RSI_GT.GTCODE_RECEIVE, v_ErrMes);
    IF v_stat <> 0 THEN
      RSB_GTLOG.Error(v_ErrMes);
    END IF;

    IF v_stat = 0 THEN
      v_stat := ProcessReplRecByTmp_SPB03(v_Prm);
    END IF;

    IF v_stat = 0 THEN
      v_stat := RSI_GT.Save(v_ErrMes);
      IF v_stat <> 0 THEN
        RSB_GTLOG.Error(v_ErrMes);
      END IF;
    END IF;
    RSB_GTLOG.Save;

    RETURN v_stat;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DEAL, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и CreateReplRecByTmp_SPB03(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DEAL) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;

  --Создание объектов записей репликаций по данным временной таблицы MFB06C (клиринг)
  FUNCTION CreateReplRecByTmp_MFB06C(p_SeanceID IN NUMBER, p_ImpDate IN DATE, p_SourceCode IN VARCHAR2, p_PrmStr IN VARCHAR2) RETURN NUMBER
  IS
    v_stat    NUMBER(5) := 0;
    v_ErrMes  VARCHAR2(512) := CHR(1);
    v_Prm     PRMREC_t;
  BEGIN
    RSB_GTLOG.Init(BATCH_SIZE, p_SeanceID, RSB_GTFN.RG_CLEARING);
    SetPrmRecByStr(v_Prm, p_PrmStr);
    v_Prm.SeanceID           := p_SeanceID;
    v_Prm.imp_date           := p_ImpDate;
    v_Prm.Source_Code        := p_SourceCode;
    v_Prm.IMPORT_SETTLETIME  := GetImportSettleTime;
    v_Prm.FileImport         := RSB_GTFN.FileMFB06C;
    v_Prm.FORM_ORDERS_IMPORT := GetFormOrdersImport;

    PrepareMarketReportsMFB06C(v_Prm);

    v_stat := RSI_GT.Init(RSI_GT.DIRECT_INSERT_YES, p_SeanceID, p_SourceCode, RSI_GT.GTCODE_RECEIVE, v_ErrMes);
    IF v_stat <> 0 THEN
      RSB_GTLOG.Error(v_ErrMes);
    END IF;

    IF v_stat = 0 THEN
      v_stat := ProcessReplRecByTmp_MFB06C(v_Prm);
    END IF;

    IF v_stat = 0 THEN
      v_stat := RSI_GT.Save(v_ErrMes);
      IF v_stat <> 0 THEN
        RSB_GTLOG.Error(v_ErrMes);
      END IF;
    END IF;

    RSB_GTLOG.Save;

    RETURN v_stat;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_CLEARING, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и CreateReplRecByTmp_MFB06C(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_CLEARING) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;

  --Создание объектов записей репликаций по данным временной таблицы ORDERS (заявки)
  FUNCTION CreateReplRecByTmp_ORDERS(p_SeanceID IN NUMBER, p_ImpDate IN DATE, p_SourceCode IN VARCHAR2, p_PrmStr IN VARCHAR2) RETURN NUMBER
  IS
    v_stat    NUMBER(5) := 0;
    v_ErrMes  VARCHAR2(512) := CHR(1);
    v_Prm     PRMREC_t;
  BEGIN
    RSB_GTLOG.Init(BATCH_SIZE, p_SeanceID, RSB_GTFN.RG_REQ);

    SetPrmRecByStr(v_Prm, p_PrmStr);
    v_Prm.SeanceID           := p_SeanceID;
    v_Prm.imp_date           := p_ImpDate;
    v_Prm.Source_Code        := p_SourceCode;
    v_Prm.IMPORT_SETTLETIME  := GetImportSettleTime;
    v_Prm.FORM_ORDERS_IMPORT := GetFormOrdersImport;

    v_stat := RSI_GT.Init(RSI_GT.DIRECT_INSERT_YES, p_SeanceID, p_SourceCode, RSI_GT.GTCODE_RECEIVE, v_ErrMes);
    IF v_stat <> 0 THEN
      RSB_GTLOG.Error(v_ErrMes);
    END IF;

    IF v_stat = 0 THEN
      v_stat := ProcessReplRecByTmp_ORDERS(v_Prm);
    END IF;

    IF v_stat = 0 THEN
      v_stat := RSI_GT.Save(v_ErrMes);
      IF v_stat <> 0 THEN
        RSB_GTLOG.Error(v_ErrMes);
      END IF;
    END IF;

    RSB_GTLOG.Save;

    RETURN v_stat;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_REQ, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и CreateReplRecByTmp_ORDERS(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_REQ) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;

  --ОБРАБОТКА ИНФОРМАЦИИ О ДВИЖЕНИИ ДЕНЕЖНЫХ СРЕДСТВ MFB99
  
  --Очистка временной таблицы для данных MFB99
  FUNCTION DelMFB99(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER
  IS
    v_Kind NUMBER(5);
  BEGIN

    v_Kind := RSB_GTFN.RG_MONEYMOTION;
    EXECUTE IMMEDIATE 'TRUNCATE TABLE D_MFB99_TMP';
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, v_Kind , RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и DelMFB99(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=MFB99' ||
                           ', ObjectKind=' || TO_CHAR(v_Kind) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;
  
  --Заполнение временной таблицы для данных MFB99
  FUNCTION FillMFB99(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2, p_Imp_date IN DATE)
    RETURN NUMBER
  IS
  BEGIN
    EXECUTE IMMEDIATE
      'INSERT INTO D_MFB99_TMP(
         T_ID_SPB_MFB99
        ,T_ID_MB_REQUISITES
        ,T_CLRACCCODE
        ,T_BANKACCCODE
        ,T_DEBIT
        ,T_CREDIT
        ,T_REPORTDATE
        ,T_CURRENCYID
        ,T_ENTRYNUMBER
        ,T_CODETYPE
        ,T_OTHERCLIRACC
        )
        (SELECT
         NVL(T.ID_SPB_MFB99, 0)
        ,NVL(T.ID_MB_REQUISITES, 0)
        ,NVL(T.CLRACCCODE, CHR(1))
        ,NVL(T.BANKACCCODE, CHR(1))
        ,NVL(T.DEBIT, 0)
        ,NVL(T.CREDIT, 0)
        ,NVL(T.REPORTDATE, TO_DATE(''01.01.0001'', ''DD.MM.YYYY''))
        ,NVL(T.CURRENCYID, CHR(1))
        ,''MFB99_''||T.REPORTNUMBER||''_''||T.CLRACCCODE||''_''||T.BANKACCCODE||''_''||T.OPENINGBALANCE||''_''||T.OPERATIONCODE||''_''||TRIM('' ''  FROM to_char (DEBIT, ''9999999999999999999999999990.00''))||''_''||TRIM('' ''  FROM to_char (CREDIT, ''9999999999999999999999999990.00''))
        ,DECODE (T.OPERATIONCODE, ''1'',CASE WHEN T.DEBIT <> 0 THEN 812 ELSE 916 END, 813)
        ,(SELECT T2.BANKACCCODE FROM '||p_Synonim||'.SPB_MFB99 T2 WHERE T2.ID_MB_REQUISITES = T.ID_MB_REQUISITES
            AND T2.ZRID = 0 AND T2.POSTYPE = ''C'' AND  T2.OPERATIONCODE = ''2'' AND T2.BANKACCCODE != T.BANKACCCODE AND T2.DEBIT = T.DEBIT AND T2.CREDIT = T.CREDIT) 
           FROM '||p_Synonim||'.SPB_MFB99 T
          WHERE T.ID_MB_REQUISITES IN (SELECT ID_MB_REQUISITES FROM '||p_Synonim||'.PROCESSING_ACTUAL WHERE FILE_TYPE = ''MFB99'' AND TRADE_DATE = :p_Imp_date)
            AND T.ZRID = 0 AND T.POSTYPE = ''C'' AND  T.OPERATIONCODE IN (''1'', ''2'')
        )'
    USING IN p_Imp_date;
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_MONEYMOTION, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и FillMFB99, ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=MFB99' || ' ' || TO_CHAR(p_Imp_date) || ' ' || p_Synonim || ' ' ||
                           ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_MONEYMOTION) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;
  
  FUNCTION UpdateObjMFB99(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER
  IS 
    TYPE ObjID_t  IS TABLE OF DGTCODE_DBT.T_OBJECTID%TYPE;
    TYPE MoneyMotion_t IS TABLE OF D_MFB99_TMP.T_ID_SPB_MFB99%TYPE;
    v_ObjIDs  ObjID_t;
    p_MoneyMotionID  MoneyMotion_t;
    v_Kind NUMBER(5);
  BEGIN
    v_Kind := RSB_GTFN.RG_MONEYMOTION;
    SELECT GTCODE.T_OBJECTID, MFB99.T_ID_SPB_MFB99
      BULK COLLECT INTO v_ObjIDs, p_MoneyMotionID
      FROM DGTCODE_DBT GTCODE, D_MFB99_TMP MFB99
     WHERE GTCODE.T_APPLICATIONID IN (RSB_GTFN.GT_SPB, RSB_GTFN.GT_PAYMENTS_SPB)
       AND GTCODE.T_OBJECTKIND IN (v_Kind)
       AND GTCODE.T_OBJECTCODE = MFB99.T_ENTRYNUMBER||'_'||CASE WHEN SUBSTR(to_char(t_ReportDate,'dd'), 1, 1) = '0' THEN ' ' ELSE '' END || TRIM(LEADING '0' FROM to_char(t_ReportDate, 'dd.mm.yyyy'));
    IF p_MoneyMotionID.COUNT <> 0 THEN
      FORALL i IN p_MoneyMotionID.FIRST .. p_MoneyMotionID.LAST
        UPDATE D_MFB99_TMP MFB99
           SET MFB99.t_ObjectID = v_ObjIDs (i)
         WHERE MFB99.T_ID_SPB_MFB99 = p_MoneyMotionID (i);
     END IF;
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, v_Kind, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и UpdateObjMFB99(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=MFB99' ||
                           ', ObjectKind=' ||  TO_CHAR(v_Kind) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;
  
  FUNCTION UpdateMFB99_PM(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2)
    RETURN NUMBER
  IS
    TYPE ObjID_t IS TABLE OF DGTSNCREC_DBT.T_RECORDID%TYPE;
    TYPE PmID_t IS TABLE OF D_MFB99_TMP.T_ID_SPB_MFB99%TYPE;
    v_ObjIDs ObjID_t;
    v_PmID   PmID_t;
    v_Kind NUMBER(5);
  BEGIN
    v_Kind := RSB_GTFN.RG_MONEYMOTION;
    SELECT MFB99.T_ID_SPB_MFB99, GTSNCREC.T_RECORDID
      BULK COLLECT INTO v_PmID, v_ObjIDs
      FROM dgtsncrec_dbt gtsncrec,
           dgtcode_dbt gtcode,
           dgtobject_dbt gtobject,
           D_MFB99_TMP MFB99
     WHERE gtsncrec.t_seanceid = p_SeanceID
       AND GTCODE.T_OBJECTKIND = v_Kind
       AND GTCODE.T_OBJECTID   = GTSNCREC.T_OBJECTID
       AND GTOBJECT.T_OBJECTID = GTSNCREC.T_OBJECTID
       AND GTCODE.T_OBJECTCODE = MFB99.T_ENTRYNUMBER||'_'||CASE WHEN SUBSTR(to_char(t_ReportDate,'dd'), 1, 1) = '0' THEN ' ' ELSE '' END || TRIM(LEADING '0' FROM to_char(t_ReportDate, 'dd.mm.yyyy'));

    IF v_PmID.COUNT <> 0 THEN
      FORALL i IN v_PmID.FIRST .. v_PmID.LAST
        EXECUTE IMMEDIATE
          'UPDATE '||p_Synonim||'.SPB_MFB99 MFB99
              SET ZRID = :v_ObjIDsi
            WHERE ID_SPB_MFB99 = :v_PmIDi'
        USING IN v_ObjIDs(i), IN v_PmID(i);
    END IF;
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, v_Kind, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и UpdateCCX99_PM(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=CUX22' ||
                           ', ObjectKind=' || TO_CHAR(v_Kind) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;
  
  --Заполнение и сохранение параметров сделки
  FUNCTION PutMoneyMotion(p_Prm IN OUT NOCOPY PRMREC_t, p_MoneyMotion IN OUT NOCOPY DATAMONEYMOTION_t, p_ErrMes IN OUT NOCOPY VARCHAR2) RETURN NUMBER
  IS
    v_stat         NUMBER(5) := 0;
    v_RG_ObjKind   NUMBER(5) := 0;
    v_RG_ObjCode   VARCHAR2(25) := CHR(1);
  BEGIN

    v_stat := RSI_GT.InitRec(p_Prm.Kind,
                             'Движение денежных средств №' || p_MoneyMotion.EntryNumber ||' на ' || CASE WHEN SUBSTR(to_char(p_MoneyMotion.ReportDate,'dd'), 1, 1) = '0' THEN ' ' ELSE '' END || TRIM(LEADING '0' FROM to_char(p_MoneyMotion.ReportDate, 'dd.mm.yyyy')),--поправить для 9999
                             0, --проверять существование объекта в системе по коду
                             p_MoneyMotion.EntryNumber||'_'||CASE WHEN SUBSTR(to_char(p_MoneyMotion.ReportDate,'dd'), 1, 1) = '0' THEN ' ' ELSE '' END || TRIM(LEADING '0' FROM to_char(p_MoneyMotion.ReportDate, 'dd.mm.yyyy')), --код объекта
                             0,--p_MoneyMotion.ClientID, --ID клиента
                             p_ErrMes);

    IF v_stat = 0 THEN
    
        RSI_GT.SetParmByName('ACCOUNT',         p_MoneyMotion.BankAccCode);
        RSI_GT.SetParmByName('CODETYPE',        p_MoneyMotion.CodeType);
        RSI_GT.SetParmByName('DEBIT',           p_MoneyMotion.Debit);
        RSI_GT.SetParmByName('CREDIT',          p_MoneyMotion.Credit);
        RSI_GT.SetParmByName('DATE' ,           p_MoneyMotion.ReportDate);
        RSI_GT.SetParmByName('OTHERKLIRACC' ,   p_MoneyMotion.OtherClirAcc);
        RSI_GT.SetParmByName('EXT_SETTLECODE',  p_MoneyMotion.ClrAccCode);
        RSI_GT.SetParmByName('CURRENCYID' ,     p_MoneyMotion.CurrencyID);

      --проверка ошибок после добавления параметров
      IF RSI_GT.GetLastError <> CHR(1) THEN
        v_stat := 1;
        p_ErrMes := RSI_GT.GetLastError;
      END IF;
    END IF;

    RETURN v_stat;
  END;
  
 --Обработка записей по данным буферной таблицы
  PROCEDURE ProcessReplRecByTmp_MFB99(p_Prm IN OUT NOCOPY PRMREC_t)
  IS
    v_MoneyMotion    DATAMONEYMOTION_t;
    v_stat    NUMBER(5) := 0;
    v_ErrMes  VARCHAR2(1000) := CHR(1);
    v_WarnMes VARCHAR2(1000) := CHR(1);
  BEGIN
    --инициализация глобального лога для накопления записей для вставки
    RSB_GTLOG.Init(BATCH_SIZE, p_Prm.SeanceID, p_Prm.Kind);

    --инициализация глобального накопителя объектов ЗР для вставки
    v_stat := RSI_GT.Init(RSI_GT.DIRECT_INSERT_YES, p_Prm.SeanceID, p_Prm.Source_Code, RSI_GT.GTCODE_RECEIVE, v_ErrMes);
    IF v_stat <> 0 THEN
      RSB_GTLOG.Error(v_ErrMes);
    END IF;

    IF v_stat = 0 THEN
      FOR cData IN (SELECT t.*
                      FROM D_MFB99_TMP t
                   )
      LOOP
        v_ErrMes  := CHR(1);
        v_WarnMes := CHR(1);

        v_MoneyMotion.EntryNumber   := cData.t_EntryNumber;
        v_MoneyMotion.ClrAccCode    := cData.t_ClrAccCode;
        v_MoneyMotion.BankAccCode   := cData.t_BankAccCode;
        v_MoneyMotion.Debit         := cData.t_Debit;
        v_MoneyMotion.Credit        := cData.t_Credit;
        v_MoneyMotion.ReportDate    := cData.t_ReportDate;
        v_MoneyMotion.CurrencyID    := cData.t_CurrencyID;
        v_MoneyMotion.CodeType      := cData.t_CodeType;
        v_MoneyMotion.OtherClirAcc  := cData.t_OtherClirAcc;

        IF cData.t_ObjectID > 0 THEN
          v_ErrMes := 'Ошибка при загрузке движения денежных средств №' || v_MoneyMotion.EntryNumber ||' на ' || CASE WHEN SUBSTR(to_char(v_MoneyMotion.ReportDate,'dd'), 1, 1) = '0' THEN ' ' ELSE '' END || TRIM(LEADING '0' FROM to_char(v_MoneyMotion.ReportDate, 'dd.mm.yyyy')) || CHR(10) || 'Запись репликации с действием создать для объекта ' || TO_CHAR(cData.t_ObjectID ) || ' уже передавалась в RS-Bank.' || CHR(10) || 'Повторная репликация невозможна.';
        END IF;

        IF v_ErrMes = CHR(1) THEN
          IF PutMoneyMotion(p_Prm, v_MoneyMotion, v_ErrMes) = 0 THEN
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
    RSB_GTLOG.DirectInsMsg(p_Prm.SeanceID, p_Prm.Kind, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка при обработке данных в ф-и ProcessReplRecByTmp_MFB99(), ' ||
                           'SeanceID=' || TO_CHAR(p_Prm.SeanceID) || ', FileImport=MFB99' ||
                           ', ObjectKind=' || TO_CHAR(p_Prm.Kind) ||
                           ', Source_Code=' || p_Prm.Source_Code || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
  END;
  
  FUNCTION CreateReplRecByTmp_MFB99(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER
  IS
    v_Prm PRMREC_t;
  BEGIN
    --инициализация используемых параметров
    v_Prm.SeanceID    := p_SeanceID;
    v_Prm.Source_Code := p_SourceCode;
    v_Prm.Kind := RSB_GTFN.RG_MONEYMOTION;

    ProcessReplRecByTmp_MFB99(v_Prm);

    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, v_Prm.Kind, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и CreateReplRecByTmp_MFB99(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=MFB99' ||
                           ', ObjectKind=' || TO_CHAR(v_Prm.Kind) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;

  --ОБРАБОТКА ИНФОРМАЦИИ О НЕТТО-ТРЕБОВАНИЯХ И НЕТТО-ОБЯЗАТЕЛЬСТВАХ MFB13
  
  --Очистка временной таблицы для данных MFB13
  FUNCTION DelMFB13(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER
  IS
  BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE D_MFB13_TMP';
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DLITOGCLVR , RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и DelMFB13(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=MFB13' ||
                           ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DLITOGCLVR) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;
  
  --Заполнение временной таблицы для данных MFB99
  FUNCTION FillMFB13(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2, p_Imp_date IN DATE)
    RETURN NUMBER
  IS
  BEGIN
    EXECUTE IMMEDIATE
      'INSERT INTO D_MFB13_TMP(
         T_ID_SPB_MFB13
        ,T_ID_MB_REQUISITES
        ,T_REPORTDATE
        ,T_CLRACCCODE
         ,T_CURRENCYID
        ,T_NETTOSUM
        ,T_OBJECTCODE
        )
        (SELECT
         NVL(T.ID_SPB_MFB13, 0)
        ,NVL(T.ID_MB_REQUISITES, 0)
        ,NVL(T.REPORTDATE, TO_DATE(''01.01.0001'', ''DD.MM.YYYY''))
        ,NVL(T.CLRACCCODE, CHR(1))
        ,NVL(T.CURRENCYID, CHR(1))
        ,NVL(T.CREDIT, 0) - NVL(T.DEBIT, 0)
        ,''MFB13_''||T.CLRACCCODE||''_''||T.CURRENCYID||''_''||CASE WHEN SUBSTR(to_char(NVL(T.REPORTDATE, TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')),''dd''), 1, 1) = ''0'' THEN '' '' ELSE '''' END || TRIM(LEADING ''0'' FROM to_char(NVL(T.REPORTDATE, TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')), ''dd.mm.yyyy''))
           FROM '||p_Synonim||'.SPB_MFB13 T
          WHERE T.ID_MB_REQUISITES IN (SELECT ID_MB_REQUISITES FROM '||p_Synonim||'.PROCESSING_ACTUAL WHERE FILE_TYPE = ''MFB13'' AND TRADE_DATE = :p_Imp_date)
            AND T.ZRID = 0 AND T.POSTYPE = ''C'' AND  T.FINALOBLIGATIONS = ''Y''
        )'
    USING IN p_Imp_date;
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DLITOGCLVR, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и FillMFB13, ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=MFB13' || ' ' || TO_CHAR(p_Imp_date) || ' ' || p_Synonim || ' ' ||
                           ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DLITOGCLVR) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;
  
  FUNCTION UpdateObjMFB13(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER
  IS 
    TYPE ObjID_t  IS TABLE OF DGTCODE_DBT.T_OBJECTID%TYPE;
    TYPE NettoItog_t IS TABLE OF D_MFB13_TMP.T_ID_SPB_MFB13%TYPE;
    v_ObjIDs  ObjID_t;
    p_NettoItogID  NettoItog_t;
  BEGIN
    SELECT GTCODE.T_OBJECTID, MFB13.T_ID_SPB_MFB13
      BULK COLLECT INTO v_ObjIDs, p_NettoItogID
      FROM DGTCODE_DBT GTCODE, D_MFB13_TMP MFB13
     WHERE GTCODE.T_APPLICATIONID IN (RSB_GTFN.GT_SPB, RSB_GTFN.GT_PAYMENTS_SPB)
       AND GTCODE.T_OBJECTKIND = RSB_GTFN.RG_DLITOGCLVR
       AND GTCODE.T_OBJECTCODE = MFB13.T_OBJECTCODE;
    IF p_NettoItogID.COUNT <> 0 THEN
      FORALL i IN p_NettoItogID.FIRST .. p_NettoItogID.LAST
        UPDATE D_MFB13_TMP MFB13
           SET MFB13.t_ObjectID = v_ObjIDs (i)
         WHERE MFB13.T_ID_SPB_MFB13 = p_NettoItogID (i);
     END IF;
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DLITOGCLVR, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и UpdateObjMFB13(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=MFB13' ||
                           ', ObjectKind=' ||  TO_CHAR(RSB_GTFN.RG_DLITOGCLVR) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;
  
  FUNCTION UpdateMFB13_PM(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2)
    RETURN NUMBER
  IS
    TYPE ObjID_t IS TABLE OF DGTSNCREC_DBT.T_RECORDID%TYPE;
    TYPE PmID_t IS TABLE OF D_MFB13_TMP.T_ID_SPB_MFB13%TYPE;
    v_ObjIDs ObjID_t;
    v_PmID   PmID_t;
    v_Kind NUMBER(5);
  BEGIN
    SELECT MFB13.T_ID_SPB_MFB13, GTSNCREC.T_RECORDID 
      BULK COLLECT INTO v_PmID, v_ObjIDs
      FROM dgtsncrec_dbt gtsncrec,
           dgtcode_dbt gtcode,
           dgtobject_dbt gtobject,
           D_MFB13_TMP MFB13
     WHERE gtsncrec.t_seanceid = p_SeanceID
       AND GTCODE.T_OBJECTKIND = RSB_GTFN.RG_DLITOGCLVR
       AND GTCODE.T_OBJECTID   = GTSNCREC.T_OBJECTID
       AND GTOBJECT.T_OBJECTID = GTSNCREC.T_OBJECTID
       AND GTCODE.T_OBJECTCODE = MFB13.T_OBJECTCODE;

    IF v_PmID.COUNT <> 0 THEN
      FORALL i IN v_PmID.FIRST .. v_PmID.LAST
        EXECUTE IMMEDIATE
          'UPDATE '||p_Synonim||'.SPB_MFB13 MFB13
              SET ZRID = :v_ObjIDsi
            WHERE ID_SPB_MFB13 = :v_PmIDi'
        USING IN v_ObjIDs(i), IN v_PmID(i);
    END IF;
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DLITOGCLVR, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и UpdateCCX99_PM(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=CUX22' ||
                           ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DLITOGCLVR) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;
  
  --Заполнение и сохранение параметров сделки
  FUNCTION PutNettoItog(p_Prm IN OUT NOCOPY PRMREC_t, p_NettoItog IN OUT NOCOPY DATANETTOITOG_t, p_ErrMes IN OUT NOCOPY VARCHAR2) RETURN NUMBER
  IS
    v_stat         NUMBER(5) := 0;
    v_RG_ObjKind   NUMBER(5) := 0;
    v_RG_ObjCode   VARCHAR2(25) := CHR(1);
  BEGIN

    v_stat := RSI_GT.InitRec(p_Prm.Kind,
                             'Нетто-требование/нетто-обязательство на ' || p_NettoItog.CurrencyID ||' с кодом ТКС '||p_NettoItog.ClrAccCode||' на ' || CASE WHEN SUBSTR(to_char(p_NettoItog.ReportDate,'dd'), 1, 1) = '0' THEN ' ' ELSE '' END || TRIM(LEADING '0' FROM to_char(p_NettoItog.ReportDate, 'dd.mm.yyyy')),--поправить для 9999
                             0, --проверять существование объекта в системе по коду
                             p_NettoItog.ObjectCode, --код объекта
                             0, --ID клиента
                             p_ErrMes);

    IF v_stat = 0 THEN
    
        RSI_GT.SetParmByName('RGDLICVR_TYPE',      'MFB13');
        RSI_GT.SetParmByName('RGDLICVR_DATE',      p_NettoItog.ReportDate);
        RSI_GT.SetParmByName('RGDLICVR_EXTCODE',   p_NettoItog.ClrAccCode);
        RSI_GT.SetParmByName('RGDLICVR_CURRENCY',  p_NettoItog.CurrencyID);
        RSI_GT.SetParmByName('RGDLICVR_NETTOSUM',  p_NettoItog.NettoSum);

      --проверка ошибок после добавления параметров
      IF RSI_GT.GetLastError <> CHR(1) THEN
        v_stat := 1;
        p_ErrMes := RSI_GT.GetLastError;
      END IF;
    END IF;

    RETURN v_stat;
  END;
  
 --Обработка записей по данным буферной таблицы
  PROCEDURE ProcessReplRecByTmp_MFB13(p_Prm IN OUT NOCOPY PRMREC_t)
  IS
    v_NettoItog DATANETTOITOG_t;
    v_stat      NUMBER(5) := 0;
    v_ErrMes    VARCHAR2(1000) := CHR(1);
    v_WarnMes   VARCHAR2(1000) := CHR(1);
  BEGIN
    --инициализация глобального лога для накопления записей для вставки
    RSB_GTLOG.Init(BATCH_SIZE, p_Prm.SeanceID, p_Prm.Kind);

    --инициализация глобального накопителя объектов ЗР для вставки
    v_stat := RSI_GT.Init(RSI_GT.DIRECT_INSERT_YES, p_Prm.SeanceID, p_Prm.Source_Code, RSI_GT.GTCODE_RECEIVE, v_ErrMes);
    IF v_stat <> 0 THEN
      RSB_GTLOG.Error(v_ErrMes);
    END IF;

    IF v_stat = 0 THEN
      FOR cData IN (SELECT t.*
                      FROM D_MFB13_TMP t
                   )
      LOOP
        v_ErrMes  := CHR(1);
        v_WarnMes := CHR(1);

        v_NettoItog.ClrAccCode    := cData.t_ClrAccCode;
        v_NettoItog.ReportDate    := cData.t_ReportDate;
        v_NettoItog.CurrencyID    := cData.t_CurrencyID;
        v_NettoItog.NettoSum      := cData.t_NettoSum;
        v_NettoItog.ObjectCode    := cData.t_ObjectCode;

        IF cData.t_ObjectID > 0 THEN
          v_ErrMes := 'Ошибка при загрузке итоговых нетто-требований и нетто-обязательств ' || v_NettoItog.ObjectCode || CHR(10) || 'Запись репликации с действием создать для объекта ' || TO_CHAR(cData.t_ObjectID) || ' уже передавалась в RS-Bank.' || CHR(10) || 'Повторная репликация невозможна.';
        END IF;

        IF v_ErrMes = CHR(1) THEN
          IF PutNettoItog(p_Prm, v_NettoItog, v_ErrMes) = 0 THEN
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
    RSB_GTLOG.DirectInsMsg(p_Prm.SeanceID, p_Prm.Kind, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка при обработке данных в ф-и ProcessReplRecByTmp_MFB13(), ' ||
                           'SeanceID=' || TO_CHAR(p_Prm.SeanceID) || ', FileImport=MFB13' ||
                           ', ObjectKind=' || TO_CHAR(p_Prm.Kind) ||
                           ', Source_Code=' || p_Prm.Source_Code || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
  END;
  
  FUNCTION CreateReplRecByTmp_MFB13(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER
  IS
    v_Prm PRMREC_t;
  BEGIN
    --инициализация используемых параметров
    v_Prm.SeanceID    := p_SeanceID;
    v_Prm.Source_Code := p_SourceCode;
    v_Prm.Kind        := RSB_GTFN.RG_DLITOGCLVR;

    ProcessReplRecByTmp_MFB13(v_Prm);

    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID,v_Prm.Kind, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и CreateReplRecByTmp_MFB13(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=MFB13' ||
                           ', ObjectKind=' || TO_CHAR(v_Prm.Kind) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;

END RSB_GTIM_SPB;
/