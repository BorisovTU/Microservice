CREATE OR REPLACE PACKAGE BODY RSB_GTIM_VR
IS
  BATCH_SIZE CONSTANT NUMBER(5) := 100; -- Размер пачки для сохранения объектов записей репликаций

  --Структура используемых параметров
  TYPE PRMREC_t IS RECORD (SeanceID             DGTSEANCE_DBT.T_SEANCEID%TYPE, --Идентификатор сеанса
                           Source_Code          DGTAPP_DBT.T_CODE%TYPE, --Код источника
                           IsRSHB               NUMBER(5), --Признак реализации для РСХБ
                           MMVB_Code            DOBJCODE_DBT.T_CODE%TYPE, --Код ММВБ
                           Using_ВнебиржФорвард NUMBER(5), --Используемый вид операции Внебиржевой форвард
                           Using_ВалютныйСВОП   NUMBER(5), --Используемый вид операции Валютный СВОП
                           Kind                 NUMBER(5)  --Источник
                          );

 --Общая структура по данным сделок
  TYPE DATADEALS_t IS RECORD (BuySell          VARCHAR2(1),
                              BuySell2         VARCHAR2(1),
                              ClientCode       VARCHAR2(12),
                              ClientID         NUMBER(10),
                              ClrComm          NUMBER(32, 12),
                              CoCurrencyId     VARCHAR2(4),
                              CoCurrencyId2    VARCHAR2(4),
                              CodeDeal         VARCHAR2(50),
                              CurrencyId       VARCHAR2(20),
                              CurrencyId2      VARCHAR2(20),
                              Decimals         NUMBER(5),
                              DocNo            VARCHAR2(12),
                              ExchComm         NUMBER(32, 12),
                              ExtSettleCode    VARCHAR2(20),
                              InsideCode       VARCHAR2(50),
                              ITSComm          NUMBER(32, 12),
                              Kind             VARCHAR2(5),
                              MainSecShortName VARCHAR2(10),
                              MarketReportID   NUMBER(10),
                              OrderNo          VARCHAR2(20),
                              Price            NUMBER(32, 12),
                              Price2           NUMBER(32, 12),
                              Quantity         NUMBER(32, 12),
                              Quantity2        NUMBER(32, 12),
                              ReportDate       DATE,
                              RepoTradeNo      VARCHAR2(20),
                              SecShortName     VARCHAR2(10),
                              SettleDate       DATE,
                              SettleDate2      DATE,
                              TradeNo          VARCHAR2(20),
                              TradeNo2         VARCHAR2(20),
                              TradeTime        DATE,
                              Value_           NUMBER(32, 12),
                              Value_2          NUMBER(32, 12),
                              BrokerRef        VARCHAR2(20),
                              FaceValue        NUMBER(32, 12)
                             );

    TYPE DATAVARM_t IS RECORD  (ReportDate       DATE,
                              ReportDateStr    VARCHAR2(10),
                              Kind             VARCHAR2(20),
                              TradeNo          VARCHAR2(20),
                              CodeDeal         VARCHAR2(33),
                              Margin           NUMBER(32, 12)
                             );

 TYPE DATAITOGCL_t IS RECORD (ReportDate       DATE,
                              ReportDateStr    VARCHAR2(10),
                              DocType          VARCHAR2(5),
                              ExtCode          VARCHAR2(20),
                              CurrencyId       VARCHAR2(4),
                              NettoSum         NUMBER(32, 12)
                             );

  TYPE DATAMONEYMOTION_t IS RECORD  (Account          VARCHAR2(25),
                                     CodeType         VARCHAR2(5),
                                     CodeTypeInt      NUMBER(5),
                                     Number_          VARCHAR2(35),
                                     CodeDeal         VARCHAR2(45),
                                     CorAcc           VARCHAR2(25),
                                     PurposePayment   VARCHAR2(210),
                                     Debit            NUMBER(32, 12),
                                     Credit           NUMBER(32, 12),
                                     Date_            DATE,
                                     DateStr          VARCHAR2(10),
                                     PayAcc           VARCHAR2(25),
                                     RecAcc           VARCHAR2(25),
                                     ExtSettleCode    VARCHAR2(5),
                                     OtherKlirAcc     VARCHAR2(25),
                                     CurrencyId       VARCHAR2(3),
                                     ClientCode       VARCHAR2(12)
                             );

--Подготовить отчеты биржи и обновить данные во временной таблице
  PROCEDURE PrepareMarketReportsCUX23(p_Prm IN OUT NOCOPY PRMREC_t)
  IS
    TYPE DOCNOARR_t    IS TABLE OF D_CUX23_TMP.t_DocNo%TYPE;
    TYPE MRKREPIDARR_t IS TABLE OF NUMBER(10);
    DocNoArr         DOCNOARR_t := DOCNOARR_t();
    MrkRepIDArr      MRKREPIDARR_t := MRKREPIDARR_t();
    v_stat           NUMBER(5);
    v_MarketReportID NUMBER(10);
    v_ErrMes         VARCHAR2(1000);
  BEGIN
    FOR cData IN (SELECT DISTINCT CUX23.t_DocNo, CUX23.t_ReportDate FROM D_CUX23_TMP CUX23)
    LOOP
      v_stat := RSB_GTFN.WriteMarketReport(p_Prm.SeanceID, p_Prm.Source_Code, v_MarketReportID, v_ErrMes, cData.t_DocNo, cData.t_ReportDate, p_Prm.MMVB_Code);

      IF v_ErrMes <> CHR(1) THEN
        RSB_GTLOG.DirectInsMsg(p_Prm.SeanceID, RSB_GTFN.RG_DVFXDEAL, RSB_GTLOG.ISSUE_FAULT, 'Ошибка при вставке документа-подтверждения "Отчет биржи"' || CHR(10) || v_ErrMes);
      END IF;

      IF v_MarketReportID > 0 THEN
        DocNoArr.Extend();
        DocNoArr(DocNoArr.last) := cData.t_DocNo;
        MrkRepIDArr.Extend();
        MrkRepIDArr(MrkRepIDArr.last) := v_MarketReportID;
      END IF;
    END LOOP;

    --привяжем отчеты к данным во временной таблице
    IF DocNoArr.COUNT > 0 THEN
      FORALL i IN DocNoArr.FIRST .. DocNoArr.LAST
        UPDATE D_CUX23_TMP
           SET t_MarketReportID = MrkRepIDArr(i)
         WHERE t_DocNo = DocNoArr(i);

      DocNoArr.DELETE;
      MrkRepIDArr.DELETE;
    END IF;
  END;

  PROCEDURE SetCodeDeal(p_Prm IN OUT NOCOPY PRMREC_t, p_Deal IN OUT NOCOPY DATADEALS_t)
  IS
  BEGIN
    p_Deal.CodeDeal := 'CUX23_' || p_Deal.BuySell || '/' 
                    || CASE WHEN p_Deal.Kind = p_Prm.Using_ВалютныйСВОП OR p_Deal.Kind = КороткийСВОП THEN p_Deal.RepoTradeNo ELSE p_Deal.TradeNo END
                    || '_' || CASE WHEN SUBSTR(to_char(p_Deal.ReportDate,'dd'), 1, 1) = '0' THEN ' ' ELSE '' END
                    || TRIM(LEADING '0' FROM to_char(p_Deal.ReportDate, 'dd.mm.yyyy'));
  END;

  PROCEDURE SetInsideCode(p_Prm IN OUT NOCOPY PRMREC_t, p_Deal IN OUT NOCOPY DATADEALS_t)
  IS
  BEGIN
    p_Deal.InsideCode := p_Deal.BuySell || '/'
                      || CASE WHEN SUBSTR(to_char(p_Deal.ReportDate,'dd'), 1, 1) = '0' THEN ' ' ELSE '' END
                      || TRIM(LEADING '0' FROM to_char(p_Deal.ReportDate, 'ddmmyyyy')) || '0'
                      || CASE WHEN p_Deal.Kind = p_Prm.Using_ВалютныйСВОП OR p_Deal.Kind = КороткийСВОП OR (p_Prm.IsRSHB = 1 AND p_Deal.Kind = КороткийСВОП_Кл) THEN p_Deal.RepoTradeNo ELSE p_Deal.TradeNo END;
  END;

  --Получение кода трейдера из комментария
  FUNCTION GetTraderCodeVR(p_BrokerRef IN VARCHAR2, p_ClientCode IN VARCHAR2) RETURN VARCHAR2
  IS
    v_TraderCode VARCHAR2(64);
    v_ClientCode VARCHAR2(12);
    v_Flag NUMBER := 0;
  BEGIN
    v_TraderCode := p_BrokerRef;
    v_ClientCode := p_ClientCode;
  
    IF v_ClientCode <> CHR(1) THEN
      IF INSTR(v_ClientCode, 'CU') = 1 THEN
        v_ClientCode := NVL(SUBSTR(v_ClientCode, 3), CHR(1));
      END IF;

      WHILE INSTR(v_TraderCode, v_ClientCode) > 0 LOOP
        v_TraderCode := NVL(SUBSTR(v_TraderCode, INSTR(v_TraderCode, v_ClientCode) + LENGTH(v_ClientCode) + 1), CHR(1));
        v_Flag := 1;
      END LOOP;
    END IF;
    
    IF v_Flag <> 1 THEN
      v_TraderCode := RSB_GTFN.GetRefNote(v_TraderCode, '/');
    END IF;
                     
    RETURN v_TraderCode;    
  END;

  --Заполнение и сохранение параметров сделки
  FUNCTION PutDealInfoPM(p_Prm IN OUT NOCOPY PRMREC_t, p_Deal IN OUT NOCOPY DATADEALS_t, p_ErrMes IN OUT NOCOPY VARCHAR2) RETURN NUMBER
  IS
    v_stat         NUMBER(5) := 0;
    v_RG_ObjKind   NUMBER(5) := 0;
    v_RG_ObjCode   VARCHAR2(25) := CHR(1);
    v_TraderCode   VARCHAR2(20);
  BEGIN

    IF p_Deal.Kind = p_Prm.Using_ВалютныйСВОП OR p_Deal.Kind = p_Prm.Using_ВнебиржФорвард THEN
      v_RG_ObjKind := RSB_GTFN.RG_DVNDEAL;
      v_RG_ObjCode := 'Внебирж. сделка с ПИ № ';
    ELSIF p_Deal.Kind = КороткийСВОП OR p_Deal.Kind = ПИКО_ПокупкаПродажа OR (p_Prm.IsRSHB = 1 AND (p_Deal.Kind = ПИКО_ПокупкаПродажа_Кл OR p_Deal.Kind = КороткийСВОП_Кл)) THEN
      v_RG_ObjKind := RSB_GTFN.RG_DVFXDEAL;
      v_RG_ObjCode := 'Конверс. сделка в ПИ № ';
    END IF;

    v_stat := RSI_GT.InitRec(v_RG_ObjKind,
                             v_RG_ObjCode || p_Deal.InsideCode || ' на ' || CASE WHEN SUBSTR(to_char(p_Deal.ReportDate,'dd'), 1, 1) = '0' THEN ' ' ELSE '' END  || TRIM(LEADING '0' FROM to_char(p_Deal.ReportDate, 'dd.mm.yyyy')), --Наименование объекта
                             0, --проверять существование объекта в системе по коду
                             p_Deal.CodeDeal, --код объекта
                             p_Deal.ClientID, --ID клиента
                             p_ErrMes);

    IF v_stat = 0 THEN

      v_TraderCode := GetTraderCodeVR(p_Deal.BrokerRef, p_Deal.ClientCode);

      IF v_RG_ObjKind = RSB_GTFN.RG_DVFXDEAL THEN

        RSI_GT.SetParmByName('RGDVFXDL_KIND',    p_Deal.Kind);
        RSI_GT.SetParmByName('RGDVFXDL_BUYSELL', p_Deal.BuySell);
        RSI_GT.SetParmByName('RGDVFXDL_CODE', p_Deal.InsideCode);

        IF p_Deal.Kind = КороткийСВОП OR (p_Prm.IsRSHB = 1 AND p_Deal.Kind = КороткийСВОП_Кл) THEN
          RSI_GT.SetParmByName('RGDVFXDL_EXTCODE', p_Deal.RepoTradeNo);
        ELSE
          RSI_GT.SetParmByName('RGDVFXDL_EXTCODE', p_Deal.TradeNo);
        END IF;

        RSI_GT.SetParmByName('RGDVFXDL_DATE',             p_Deal.ReportDate);
        RSI_GT.SetParmByName('RGDVFXDL_TIME',             p_Deal.TradeTime);
        RSI_GT.SetParmByName('RGDVFXDL_CLIENT',           p_Deal.ClientCode);
        RSI_GT.SetParmByName('RGDVFXDL_ISIN',             p_Deal.CurrencyId);
        RSI_GT.SetParmByName('RGDVFXDL_AMOUNT',           p_Deal.Quantity);
        RSI_GT.SetParmByName('RGDVFXDL_PRICE',            p_Deal.Price);
        RSI_GT.SetParmByName('RGDVFXDL_PRICEFIID',        p_Deal.CoCurrencyId);
        RSI_GT.SetParmByName('RGDVFXDL_POINT',            p_Deal.Decimals);
        RSI_GT.SetParmByName('RGDVFXDL_COST',             p_Deal.Value_);
        RSI_GT.SetParmByName('RGDVFXDL_EXECDATE',         p_Deal.SettleDate);
        RSI_GT.SetParmByName('RGDVFXDL_ISIN_2',           p_Deal.CurrencyId2);
        RSI_GT.SetParmByName('RGDVFXDL_AMOUNT_2',         p_Deal.Quantity2);
        RSI_GT.SetParmByName('RGDVFXDL_PRICE_2',          p_Deal.Price2);
        RSI_GT.SetParmByName('RGDVFXDL_PRICEFIID_2',      p_Deal.CoCurrencyId2);
        RSI_GT.SetParmByName('RGDVFXDL_COST_2',           p_Deal.Value_2);
        RSI_GT.SetParmByName('RGDVFXDL_EXECDATE_2',       p_Deal.SettleDate2);
        RSI_GT.SetParmByName('RGDVFXDL_MARKETREPORT_ID',  p_Deal.MarketReportID);
        RSI_GT.SetParmByName('RGDVFXDL_AMOUNT_COMM',      p_Deal.ExchComm);
        RSI_GT.SetParmByName('RGDVFXDL_CLRCOMM',          p_Deal.ClrComm);
        RSI_GT.SetParmByName('RGDVFXDL_ITSCOMM',          p_Deal.ITSComm);
        RSI_GT.SetParmByName('RGDVFXDL_SECSHORTNAME',     p_Deal.SecShortName);
        RSI_GT.SetParmByName('RGDVFXDL_MAINSECSHORTNAME', p_Deal.MainSecShortName);
        RSI_GT.SetParmByName('RGDVFXDL_EXTSETTLECODE',    p_Deal.ExtSettleCode);
        RSI_GT.SetParmByName('RGDVFXDL_REQCODE',          p_Deal.OrderNo);
        RSI_GT.SetParmByName('RGDVFXDL_TRADERCODE',       v_TraderCode);
        RSI_GT.SetParmByName('RGDVFXDL_FACEVALUE',        p_Deal.FaceValue);

      ELSIF p_Deal.Kind = p_Prm.Using_ВалютныйСВОП OR p_Deal.Kind = p_Prm.Using_ВнебиржФорвард THEN

        IF p_Deal.Kind = p_Prm.Using_ВалютныйСВОП THEN
          RSI_GT.SetParmByName('RGDVNDL_EXTCODE',     p_Deal.RepoTradeNo);
          RSI_GT.SetParmByName('RGDVNDL_BUYSELL',     p_Deal.BuySell || '/' || p_Deal.BuySell2);
          RSI_GT.SetParmByName('RGDVNDL_CUX23CODE',   p_Deal.TradeNo2);
          RSI_GT.SetParmByName('RGDVNDL_ISIN_2',      p_Deal.CurrencyId2);
          RSI_GT.SetParmByName('RGDVNDL_AMOUNT_2',    p_Deal.Quantity2);
          RSI_GT.SetParmByName('RGDVNDL_PRICE_2',     p_Deal.Price2);
          RSI_GT.SetParmByName('RGDVNDL_PRICEFIID_2', p_Deal.CoCurrencyId2);
          RSI_GT.SetParmByName('RGDVNDL_COST_2',      p_Deal.Value_2);
          RSI_GT.SetParmByName('RGDVNDL_EXECDATE_2',  p_Deal.SettleDate2);
        ELSE
          RSI_GT.SetParmByName('RGDVNDL_EXTCODE',     p_Deal.TradeNo);
          RSI_GT.SetParmByName('RGDVNDL_BUYSELL',     p_Deal.BuySell);
          RSI_GT.SetParmByName('RGDVNDL_CUX23CODE',   p_Deal.TradeNo);
        END IF;

        RSI_GT.SetParmByName('RGDVNDL_CODE',            p_Deal.InsideCode);
        RSI_GT.SetParmByName('RGDVNDL_KIND',            p_Deal.Kind);
        RSI_GT.SetParmByName('RGDVNDL_DATE',            p_Deal.ReportDate);
        RSI_GT.SetParmByName('RGDVNDL_TIME',            p_Deal.TradeTime);
        RSI_GT.SetParmByName('RGDVNDL_CLIENT',          p_Deal.ClientCode);
        RSI_GT.SetParmByName('RGDVNDL_ISIN',            p_Deal.CurrencyId);
        RSI_GT.SetParmByName('RGDVNDL_AMOUNT',          p_Deal.Quantity);
        RSI_GT.SetParmByName('RGDVNDL_PRICE',           p_Deal.Price);
        RSI_GT.SetParmByName('RGDVNDL_PRICEFIID',       p_Deal.CoCurrencyId);
        RSI_GT.SetParmByName('RGDVNDL_COST',            p_Deal.Value_);
        RSI_GT.SetParmByName('RGDVNDL_EXECDATE',        p_Deal.SettleDate);
        RSI_GT.SetParmByName('RGDVNDL_MARKETREPORT_ID', p_Deal.MarketReportID);
        RSI_GT.SetParmByName('RGDVNDL_AMOUNT_COMM',     p_Deal.ExchComm);
        RSI_GT.SetParmByName('RGDVNDL_CLRCOMM',         p_Deal.ClrComm);
        RSI_GT.SetParmByName('RGDVNDL_ITSCOMM',         p_Deal.ITSComm);
        RSI_GT.SetParmByName('RGDVNDL_EXTSETTLECODE',   p_Deal.ExtSettleCode);
        RSI_GT.SetParmByName('RGDVNDL_REQCODE',         p_Deal.OrderNo);
        RSI_GT.SetParmByName('RGDVNDL_TRADERCODE',      v_TraderCode);
        RSI_GT.SetParmByName('RGDVNDL_FACEVALUE',       p_Deal.FaceValue);
      END IF;

      RSI_GT.SetParmByName('RGDV_PROGNOS', '0');

      --проверка ошибок после добавления параметров
      IF RSI_GT.GetLastError <> CHR(1) THEN
        v_stat := 1;
        p_ErrMes := RSI_GT.GetLastError;
      END IF;
    END IF;

    RETURN v_stat;
  END;

  --Обработка записей по данным буферной таблицы
  PROCEDURE ProcessReplRecByTmp_CUX23(p_Prm IN OUT NOCOPY PRMREC_t)
  IS
    v_Deal    DATADEALS_t;
    v_stat    NUMBER(5) := 0;
    v_ErrMes  VARCHAR2(1000) := CHR(1);
    v_WarnMes VARCHAR2(1000) := CHR(1);
  BEGIN
    --инициализация глобального лога для накопления записей для вставки
    RSB_GTLOG.Init(BATCH_SIZE, p_Prm.SeanceID, RSB_GTFN.RG_DVFXDEAL);

    --инициализация глобального накопителя объектов ЗР для вставки
    v_stat := RSI_GT.Init(RSI_GT.DIRECT_INSERT_YES, p_Prm.SeanceID, p_Prm.Source_Code, RSI_GT.GTCODE_RECEIVE, v_ErrMes);
    IF v_stat <> 0 THEN
      RSB_GTLOG.Error(v_ErrMes);
    END IF;

    IF v_stat = 0 THEN
      FOR cData IN (SELECT t.*
                      FROM D_CUX23_TMP t
                     ORDER BY t.T_REPORTDATE, t.T_DOCNO, t.T_ID_CM_CUX23
                   )
      LOOP
        v_ErrMes  := CHR(1);
        v_WarnMes := CHR(1);

        v_Deal.ClientID := cData.t_ClientID;
        v_Deal.DocNo := cData.t_DocNo;
        v_Deal.Kind := cData.t_Kind;
        v_Deal.TradeNo := cData.t_TradeNo;
        v_Deal.RepoTradeNo := cData.t_RepoTradeNo;
        v_Deal.OrderNo := cData.t_OrderNo;
        v_Deal.BuySell := cData.t_BuySell;
        v_Deal.ReportDate := cData.t_ReportDate;
        v_Deal.TradeTime := cData.t_TradeTime;
        v_Deal.ClientCode := cData.t_ClientCode;
        v_Deal.CurrencyId := cData.t_CurrencyId;
        v_Deal.CoCurrencyId := cData.t_CoCurrencyId;
        v_Deal.Quantity := cData.t_Quantity;
        v_Deal.Price := cData.t_Price;
        v_Deal.Value_ := cData.t_Value;
        v_Deal.SettleDate := cData.t_SettleDate;
        v_Deal.ExchComm := cData.t_ExchComm;
        v_Deal.ClrComm := cData.t_ClrComm;
        v_Deal.ITSComm := cData.t_ITSComm;
        v_Deal.SecShortName := cData.t_SecShortName;
        v_Deal.ExtSettleCode := cData.t_ExtSettleCode;
        v_Deal.Decimals := cData.t_Decimals;
        v_Deal.MainSecShortName := cData.t_MainSecShortName;
        v_Deal.MarketReportID := cData.t_MarketReportID;
        v_Deal.BrokerRef := cData.t_BrokerRef;

        v_Deal.BuySell2 := cData.t_BuySell2;
        v_Deal.Quantity2 := cData.t_Quantity2;
        v_Deal.Price2 := cData.t_Price2;
        v_Deal.Value_2 := cData.t_Value2;
        v_Deal.SettleDate2 := cData.t_SettleDate2;
        v_Deal.TradeNo2 := cData.t_TradeNo2;
        v_Deal.CurrencyId2 := cData.t_CurrencyId2;
        v_Deal.CoCurrencyId2 := cData.t_CoCurrencyId2;
        v_Deal.FaceValue := cData.t_FaceValue;

        SetCodeDeal(p_Prm, v_Deal);
        SetInsideCode(p_Prm, v_Deal);

        IF cData.t_ObjectID > 0 THEN
          v_ErrMes := 'Ошибка при загрузке сделки "' || v_Deal.CodeDeal || '"' || CHR(10) || 'Запись репликации с действием создать для объекта ' || TO_CHAR(cData.t_ObjectID ) || ' уже передавалась в RS-Bank.' || CHR(10) || 'Повторная репликация невозможна.';
        ELSIF v_Deal.ClientCode <> CHR(1) AND v_Deal.ClientID <= 0 THEN
          v_ErrMes := 'Ошибка при загрузке сделки "' || v_Deal.CodeDeal || '"' || CHR(10) || 'Не найден субъект через краткий код на бирже "' || v_Deal.ClientCode || '"';
        ELSIF cData.t_ErrStat = NOTUSED_EXTCODE THEN
          v_ErrMes := 'Ошибка при загрузке сделки "' || v_Deal.CodeDeal || '"' || CHR(10) || 'Расчетный код ' || v_Deal.ExtSettleCode || ' не может быть использован.';
        ELSIF cData.t_ErrStat = NOTFOUND_EXTCODE THEN
          v_ErrMes := 'Ошибка при загрузке сделки "' || v_Deal.CodeDeal || '"' || CHR(10) || 'В справочнике расчетных кодов банка не найден код ' || v_Deal.ExtSettleCode;
        ELSIF cData.t_ErrStat = INCORRECT_EXTCODE THEN
          v_ErrMes := 'Ошибка при загрузке сделки "' || v_Deal.CodeDeal || '"' || CHR(10) || 'В справочнике расчетных кодов банка для расчетного кода ' || v_Deal.ExtSettleCode || ' задан некорректный биржевой рынок!';
        ELSIF cData.t_ErrStat = NOTFOUND_PARTYID THEN
          v_ErrMes := 'Ошибка при загрузке сделки "' || v_Deal.CodeDeal || '"' || CHR(10) || 'Ошибка при определении срока сделки: В ГКБО не найден субъект с кодом "ММВБ" вида ' || CNST.PTCK_MICEX;
        ELSIF cData.t_ErrStat = NOTFOUND_FIID THEN
          v_ErrMes := 'Ошибка при загрузке сделки "' || v_Deal.CodeDeal || '"' || CHR(10) || 'Ошибка при определении срока сделки: В ГКБО не найден ФИ с кодом "' || v_Deal.CurrencyID || '" вида ' || RSB_SECUR.CODE_MICEX;
        ELSIF cData.t_ErrStat = NOTFOUND_CALCFIID THEN
          v_ErrMes := 'Ошибка при загрузке сделки "' || v_Deal.CodeDeal || '"' || CHR(10) || 'Ошибка при определении срока сделки: В ГКБО не найден ФИ с кодом "' || v_Deal.CoCurrencyID || '" вида ' || RSB_SECUR.CODE_MICEX;
        END IF;

        IF v_ErrMes = CHR(1) THEN
          IF PutDealInfoPM(p_Prm, v_Deal, v_ErrMes) = 0 THEN
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
    RSB_GTLOG.DirectInsMsg(p_Prm.SeanceID, RSB_GTFN.RG_DVFXDEAL, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка при обработке данных в ф-и ProcessReplRecByTmp_CUX23(), ' ||
                           'SeanceID=' || TO_CHAR(p_Prm.SeanceID) || ', FileImport=CUX23' ||
                           ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DVNDEAL) || '/' || TO_CHAR(RSB_GTFN.RG_DVFXDEAL) ||
                           ', Source_Code=' || p_Prm.Source_Code || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
  END;

  --Создание объектов записей репликаций по данным временной таблицы CUX23
  FUNCTION CreateReplRecByTmp_CUX23(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_IsRSHB IN NUMBER, p_MMVB_Code IN VARCHAR2)
    RETURN NUMBER
  IS
    v_Prm PRMREC_t;
  BEGIN
    --инициализация используемых параметров
    v_Prm.SeanceID    := p_SeanceID;
    v_Prm.Source_Code := p_SourceCode;
    v_Prm.IsRSHB      := p_IsRSHB;
    v_Prm.MMVB_Code   := p_MMVB_Code;

    IF v_Prm.IsRSHB = 1 THEN
       v_Prm.Using_ВнебиржФорвард := ВнебиржФорвард_РСХБ;
       v_Prm.Using_ВалютныйСВОП := ВалютныйСВОП_РСХБ;
    ELSE
       v_Prm.Using_ВнебиржФорвард := ВнебиржФорвард;
       v_Prm.Using_ВалютныйСВОП := ВалютныйСВОП;
    END IF;

    --подготовка отчетов биржи и обновление данных во временной таблице
    PrepareMarketReportsCUX23(v_Prm);

    ProcessReplRecByTmp_CUX23(v_Prm);

    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DVFXDEAL, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и CreateReplRecByTmp_CUX23(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=CUX23' ||
                           ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DVNDEAL) || '/' || TO_CHAR(RSB_GTFN.RG_DVFXDEAL) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;

  FUNCTION GetReqCode(p_Req IN OUT NOCOPY D_CUX22_TMP%ROWTYPE) RETURN VARCHAR2
  IS
  BEGIN
    RETURN p_Req.t_OrderNo || '_VR_' || CASE WHEN SUBSTR(to_char(p_Req.t_ReportDate,'dd'), 1, 1) = '0' THEN ' ' ELSE '' END || TRIM(LEADING '0' FROM to_char(p_Req.t_ReportDate, 'dd.mm.yyyy'));
  END;

  --Заполнение и сохранение параметров заявки
  FUNCTION PutRequestsInfoPM(p_Req IN OUT NOCOPY D_CUX22_TMP%ROWTYPE, p_ErrMes IN OUT NOCOPY VARCHAR2) RETURN NUMBER
  IS
    v_stat      NUMBER(5) := 0;
    v_Direction NUMBER(5) := 0;
  BEGIN

    v_stat := RSI_GT.InitRec(RSB_GTFN.RG_DVREQ,
                             'Заявка на сделку №' || p_Req.t_OrderNo, --Наименование объекта
                             0, --проверять существование объекта в системе по коду
                             GetReqCode(p_Req), --код объекта
                             p_Req.t_ClientID, --ID клиента
                             p_ErrMes);

    IF v_stat = 0 THEN
      RSI_GT.SetParmByName('RGDVRQ_OPERKIND',     -1);
      RSI_GT.SetParmByName('RGDVRQ_FI_CODE',      p_Req.t_CurrencyId);
      RSI_GT.SetParmByName('RGDVRQ_CODETS',       p_Req.t_OrderNo);
      RSI_GT.SetParmByName('RGDVRQ_DATE',         p_Req.t_ReportDate);
      RSI_GT.SetParmByName('RGDVRQ_TIME',         p_Req.t_EntryTime);
      RSI_GT.SetParmByName('RGDVRQ_CLIENT',       p_Req.t_ClientCode);
      RSI_GT.SetParmByName('RGDVRQ_FI_CODE_2',    p_Req.t_CoCurrencyId);
      RSI_GT.SetParmByName('RGDVRQ_PRICE',        p_Req.t_Price);
      RSI_GT.SetParmByName('RGDVRQ_VOL',          p_Req.t_Quantity);
      RSI_GT.SetParmByName('RGDVRQ_STATUS',       p_Req.t_Stat);
      RSI_GT.SetParmByName('RGDVRQ_ORDTYPECODE',  p_Req.t_OrderType);
      RSI_GT.SetParmByName('RGDVRQ_SECSHORTNAME', p_Req.t_SecShortName);
      RSI_GT.SetParmByName('RGDVRQ_TRADEGROUP',   p_Req.t_TradeGroup);

      IF p_Req.t_TradeGroup = 'T' THEN
        v_Direction := CASE WHEN p_Req.t_BuySell = 'B' THEN RSB_GTFN.DL_REQ_DIRECTION_B ELSE RSB_GTFN.DL_REQ_DIRECTION_S END;
      ELSIF p_Req.t_TradeGroup = 'S' THEN
        v_Direction := CASE WHEN p_Req.t_BuySell = 'B' THEN RSB_GTFN.DL_REQ_DIRECTION_BS ELSE RSB_GTFN.DL_REQ_DIRECTION_SB END;
      END IF;
      RSI_GT.SetParmByName('RGDVRQ_DIRECTION',    v_Direction);

      --проверка ошибок после добавления параметров
      IF RSI_GT.GetLastError <> CHR(1) THEN
        v_stat := 1;
        p_ErrMes := RSI_GT.GetLastError;
      END IF;
    END IF;

    RETURN v_stat;
  END;

  --Создание объектов записей репликаций по данным временной таблицы CUX22
  FUNCTION CreateReplRecByTmp_CUX22(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER
  IS
    TYPE CDATA_t IS REF CURSOR RETURN D_CUX22_TMP%ROWTYPE;
    v_cData   CDATA_t;
    v_Req     D_CUX22_TMP%ROWTYPE;
    v_stat    NUMBER(5) := 0;
    v_ErrMes  VARCHAR2(1000) := CHR(1);
    v_WarnMes VARCHAR2(1000) := CHR(1);
  BEGIN
    --инициализация глобального лога для накопления записей для вставки
    RSB_GTLOG.Init(BATCH_SIZE, p_SeanceID, RSB_GTFN.RG_DVREQ);

    --инициализация глобального накопителя объектов ЗР для вставки
    v_stat := RSI_GT.Init(RSI_GT.DIRECT_INSERT_YES, p_SeanceID, p_SourceCode, RSI_GT.GTCODE_RECEIVE, v_ErrMes);
    IF v_stat <> 0 THEN
      RSB_GTLOG.Error(v_ErrMes);
    END IF;

    IF v_stat = 0 THEN
      OPEN v_cData FOR
           --важен порядок и наличие всех полей, чтобы сработал FETCH
           SELECT T_ID_CM_CUX22,
                  T_ID_MB_REQUISITES,
                  NVL(t.T_REPORTDATE, RSI_GT.ZeroDate) REPORTDATE,
                  NVL(t.T_CURRENCYID, CHR(1)) CURRENCYID,
                  NVL(t.T_COCURRENCYID, CHR(1)) COCURRENCYID,
                  NVL(t.T_SECSHORTNAME, CHR(1)) SECSHORTNAME,
                  NVL(t.T_TRADEGROUP, CHR(1)) TRADEGROUP,
                  NVL(t.T_ORDERNO, CHR(1)) ORDERNO,
                  NVL(t.T_ENTRYTIME, RSI_GT.ZeroDate) ENTRYTIME,
                  NVL(t.T_BUYSELL, CHR(1)) BUYSELL,
                  NVL(t.T_ORDERTYPE, CHR(1)) ORDERTYPE,
                  NVL(t.T_QUANTITY, 0) QUANTITY,
                  NVL(t.T_DECIMALS, 0) DECIMALS,
                  NVL(t.T_PRICE, 0) PRICE,
                  NVL(t.T_STAT, CHR(1)) STAT,
                  NVL(t.T_CLIENTCODE, CHR(1)) CLIENTCODE,
                  NVL(t.T_CLIENTID, 0) CLIENTID,
                  NVL(t.T_EXTSETTLECODE, CHR(1)) EXTSETTLECODE,
                  NVL(t.T_OBJECTID, 0) OBJECTID,
                  NVL(t.T_ERRSTAT, 0) ERRSTAT
            FROM D_CUX22_TMP t
           ORDER BY t.T_REPORTDATE, t.T_ID_CM_CUX22;
      LOOP
        FETCH v_cData INTO v_Req;
        EXIT WHEN v_cData%NOTFOUND OR v_cData%NOTFOUND IS NULL;

        v_ErrMes  := CHR(1);
        v_WarnMes := CHR(1);

        IF v_Req.t_ObjectID > 0 THEN
          v_ErrMes := 'Ошибка при загрузке заявки "' || GetReqCode(v_Req) || '"' || CHR(10) || 'Запись репликации с действием создать для объекта ' || TO_CHAR(v_Req.t_ObjectID) || ' уже передавалась в RS-Bank.' || CHR(10) || 'Повторная репликация невозможна.';
        ELSIF v_Req.t_ClientCode <> CHR(1) AND v_Req.t_ClientID <= 0 THEN
          v_ErrMes := 'Ошибка при загрузке заявки "' || GetReqCode(v_Req) || '"' || CHR(10) || 'Не найден субъект через краткий код на бирже "' || v_Req.t_ClientCode || '"';
        ELSIF v_Req.t_ErrStat = NOTUSED_EXTCODE THEN
          v_ErrMes := 'Ошибка при загрузке заявки "' || GetReqCode(v_Req) || '"' || CHR(10) || 'Расчетный код ' || v_Req.t_ExtSettleCode || ' не может быть использован.';
        ELSIF v_Req.t_ErrStat = NOTFOUND_EXTCODE THEN
          v_ErrMes := 'Ошибка при загрузке заявки "' || GetReqCode(v_Req) || '"' || CHR(10) || 'В справочнике расчетных кодов банка не найден код ' || v_Req.t_ExtSettleCode;
        ELSIF v_Req.t_ErrStat = INCORRECT_EXTCODE THEN
          v_ErrMes := 'Ошибка при загрузке заявки "' || GetReqCode(v_Req) || '"' || CHR(10) || 'В справочнике расчетных кодов банка для расчетного кода ' || v_Req.t_ExtSettleCode || ' задан некорректный биржевой рынок!';
        ELSIF LENGTH(TRIM(TO_CHAR(v_Req.t_Quantity))) > 16 THEN
          v_ErrMes := 'Ошибка при загрузке заявки "' || GetReqCode(v_Req) || '"' || CHR(10) || 'Превышено максимально возможное количество загружаемых знаков параметра Quantity';
        END IF;

        IF v_ErrMes = CHR(1) THEN
          IF PutRequestsInfoPM(v_Req, v_ErrMes) = 0 THEN
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

    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DVREQ, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и CreateReplRecByTmp_CUX22(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=CUX22' ||
                           ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DVREQ) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;

  FUNCTION FillCUX23(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2, p_Beg_date IN DATE, p_End_date IN DATE)
    RETURN NUMBER
  IS
    RegVal_ReplacebleList VARCHAR2(200) := ','||REPLACE(RSB_COMMON.GetRegStrValue('РСХБ\БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ\ШЛЮЗ\ПОДМЕНЯЕМЫЕ КОДЫ ПРИ ЗАГРУЗКЕ'),' ','')||',';
    RegVal_Prefix VARCHAR2(200) := TRIM(RSB_COMMON.GetRegStrValue('РСХБ\БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ\ШЛЮЗ\ПРЕФИКС ПОДМЕНЯЕМЫХ КОДОВ'));
  BEGIN
    EXECUTE IMMEDIATE
      'INSERT INTO D_CUX23_TMP(
         T_ID_CM_CUX23
        ,T_ID_MB_REQUISITES
        ,T_REPORTDATE
        ,T_EXTSETTLECODE
        ,T_CURRENCYID
        ,T_COCURRENCYID
        ,T_SECSHORTNAME
        ,T_SETTLEDATE
        ,T_MAINSECSHORTNAME
        ,T_TRADEGROUP
        ,T_TRADEDERIV
        ,T_TRADENO
        ,T_REPOTRADENO
        ,T_BUYSELL
        ,T_ORDERNO
        ,T_TRADETIME
        ,T_PRICE
        ,T_QUANTITY
        ,T_VALUE
        ,T_CLIENTCODE
        ,T_EXCHCOMM
        ,T_CLRCOMM
        ,T_ITSCOMM
        ,T_DECIMALS
        ,T_DOCNO
        ,T_KIND
        ,T_CLIENTID
        ,T_OBJECTID
        ,T_ERRSTAT
        ,T_CURRENCYID2
        ,T_COCURRENCYID2
        ,T_SETTLEDATE2
        ,T_TRADENO2
        ,T_BUYSELL2
        ,T_PRICE2
        ,T_QUANTITY2
        ,T_VALUE2 
        ,T_BROKERREF
        ,T_FACEVALUE)
        (SELECT
         T.ID_CM_CUX23
        ,NVL(T.ID_MB_REQUISITES, 0)
        ,NVL(T.REPORTDATE, TO_DATE(''01.01.0001'', ''DD.MM.YYYY''))
        ,NVL(T.EXTSETTLECODE, CHR(1))
        ,CASE WHEN NVL(T.CLIENTCODE, CHR(1)) <> CHR(1) AND NVL(T.CURRENCYID, CHR(1)) <> CHR(1) AND INSTR(:RegVal_ReplacebleList, '',''||NVL(T.CURRENCYID, CHR(1))||'','') <> 0 THEN :RegVal_Prefix||NVL(T.CURRENCYID, CHR(1)) ELSE NVL(T.CURRENCYID, CHR(1)) END
        ,NVL(T.COCURRENCYID, CHR(1))
        ,NVL(T.SECSHORTNAME, CHR(1))
        ,NVL(T.SETTLEDATE, TO_DATE(''01.01.0001'', ''DD.MM.YYYY''))
        ,NVL(T.MAINSECSHORTNAME, CHR(1))
        ,NVL(T.TRADEGROUP, CHR(1))
        ,NVL(T.TRADEDERIV, CHR(1))
        ,NVL(T.TRADENO, CHR(1))
        ,NVL(T.REPOTRADENO, CHR(1))
        ,NVL(T.BUYSELL, CHR(1))
        ,NVL(T.ORDERNO, CHR(1))
        ,NVL(T.TRADETIME, TO_DATE(''01.01.0001 00:00:00'', ''dd.mm.yyyy HH24:MI:SS''))
        ,NVL(T.PRICE, 0)
        ,NVL(T.QUANTITY, 0)
        ,NVL(T.VALUE, 0)
        ,NVL(T.CLIENTCODE, CHR(1))
        ,NVL(T.EXCHCOMM, 0)
        ,NVL(T.CLRCOMM, 0)
        ,NVL(T.ITSCOMM, 0)
        ,NVL(T.DECIMALS, 0)
        ,CHR(1)
        ,0
        ,0
        ,0
        ,0
        ,CASE WHEN NVL(T.CLIENTCODE, CHR(1)) <> CHR(1) AND NVL(T1.CURRENCYID, CHR(1)) <> CHR(1) AND INSTR(:RegVal_ReplacebleList, '',''||NVL(T1.CURRENCYID, CHR(1))||'','') <> 0 THEN :RegVal_Prefix||NVL(T1.CURRENCYID, CHR(1)) ELSE NVL(T1.CURRENCYID, CHR(1)) END
        ,NVL(T1.COCURRENCYID, CHR(1))
        ,NVL(T1.SETTLEDATE, TO_DATE(''01.01.0001'', ''DD.MM.YYYY''))
        ,NVL(T1.TRADENO, CHR(1))
        ,NVL(T1.BUYSELL, CHR(1))
        ,NVL(T1.PRICE, 0)
        ,NVL(T1.QUANTITY, 0)
        ,NVL(T1.VALUE, 0)
        ,NVL(T.BROKERREF, CHR(1))                            
        ,NVL(T.FACEVALUE, 0)                            
           FROM '||p_Synonim||'.CM_CUX23 T
      LEFT JOIN '||p_Synonim||'.CM_CUX23 T1
             ON T1.ID_MB_REQUISITES = T.ID_MB_REQUISITES
            AND T1.ID_CM_CUX23 != T.ID_CM_CUX23
            AND NVL(T.REPOTRADENO, CHR(1)) != CHR(1) and NVL(T1.REPOTRADENO, CHR(1)) = NVL(T.REPOTRADENO, CHR(1)) and NVL(T1.ORDERNO, CHR(1)) = NVL(T.ORDERNO, CHR(1))
            AND (    (NVL(T1.SETTLEDATE, TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')) > NVL(T.SETTLEDATE, TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')))
                  OR (     NVL(T1.SETTLEDATE, TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')) = NVL(T.SETTLEDATE, TO_DATE(''01.01.0001'', ''DD.MM.YYYY''))
                       AND NVL(T1.CLRCOMM, 0) = 0 AND NVL(T1.EXCHCOMM, 0) = 0 AND NVL(T1.ITSCOMM, 0) = 0
                     )
                )
          WHERE T.ID_MB_REQUISITES IN (SELECT NVL(ID_MB_REQUISITES, 0) FROM '||p_Synonim||'.PROCESSING_ACTUAL WHERE FILE_TYPE = ''CUX23'' AND TRADE_DATE BETWEEN :p_Beg_date AND :p_End_date)
            AND T.ZRID = 0
            AND NOT EXISTS (SELECT 1 FROM '||p_Synonim||'.CM_CUX23 T2
                             WHERE T2.ID_MB_REQUISITES = T.ID_MB_REQUISITES
                               AND T2.ID_CM_CUX23 != T.ID_CM_CUX23
                               AND NVL(T.REPOTRADENO, CHR(1)) != CHR(1) and NVL(T2.REPOTRADENO, CHR(1)) = NVL(T.REPOTRADENO, CHR(1)) and NVL(T2.ORDERNO, CHR(1)) = NVL(T.ORDERNO, CHR(1))
                               AND (    (NVL(T.SETTLEDATE, TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')) > NVL(T2.SETTLEDATE, TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')))
                                     OR (     NVL(T.SETTLEDATE, TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')) = NVL(T2.SETTLEDATE, TO_DATE(''01.01.0001'', ''DD.MM.YYYY''))
                                          AND NVL(T.CLRCOMM, 0) = 0 AND NVL(T.EXCHCOMM, 0) = 0 AND NVL(T.ITSCOMM, 0) = 0
                                        )
                                   )
                           )
            AND NVL(T.TRADEGROUP, CHR(1)) IN (''S'',''T'')
        )'
    USING IN RegVal_ReplacebleList, IN RegVal_Prefix, IN RegVal_ReplacebleList, IN RegVal_Prefix, IN p_Beg_date, IN p_End_date;
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DVFXDEAL, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и FillCUX23(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=CUX23' ||
                           ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DVNDEAL) || '/' || TO_CHAR(RSB_GTFN.RG_DVFXDEAL) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;

  FUNCTION FillCUX22(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2, p_Beg_date IN DATE, p_End_date IN DATE)
    RETURN NUMBER
  IS
    RegVal_ReplacebleList VARCHAR2(200) := ','||REPLACE(RSB_COMMON.GetRegStrValue('РСХБ\БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ\ШЛЮЗ\ПОДМЕНЯЕМЫЕ КОДЫ ПРИ ЗАГРУЗКЕ'),' ','')||',';
    RegVal_Prefix VARCHAR2(200) := TRIM(RSB_COMMON.GetRegStrValue('РСХБ\БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ\ШЛЮЗ\ПРЕФИКС ПОДМЕНЯЕМЫХ КОДОВ'));
  BEGIN
    EXECUTE IMMEDIATE
      'INSERT INTO D_CUX22_TMP(
         T_ID_CM_CUX22
        ,T_ID_MB_REQUISITES
        ,T_REPORTDATE
        ,T_EXTSETTLECODE
        ,T_CURRENCYID
        ,T_COCURRENCYID
        ,T_SECSHORTNAME
        ,T_TRADEGROUP
        ,T_ORDERNO
        ,T_ENTRYTIME
        ,T_BUYSELL
        ,T_ORDERTYPE
        ,T_QUANTITY
        ,T_DECIMALS
        ,T_PRICE
        ,T_STAT
        ,T_CLIENTCODE
        ,T_CLIENTID
        ,T_OBJECTID
        ,T_ERRSTAT
        )
        (SELECT
         T.ID_CM_CUX22
        ,NVL(T.ID_MB_REQUISITES, 0)
        ,NVL(T.REPORTDATE, TO_DATE(''01.01.0001'', ''DD.MM.YYYY''))
        ,NVL(T.EXTSETTLECODE, CHR(1))
        ,CASE WHEN NVL(T.CLIENTCODE, CHR(1)) <> CHR(1) AND NVL(T.CURRENCYID, CHR(1)) <> CHR(1) AND INSTR(:RegVal_ReplacebleList, '',''||NVL(T.CURRENCYID, CHR(1))||'','') <> 0 THEN :RegVal_Prefix||NVL(T.CURRENCYID, CHR(1)) ELSE NVL(T.CURRENCYID, CHR(1)) END
        ,NVL(T.COCURRENCYID, CHR(1))
        ,NVL(T.SECSHORTNAME, CHR(1))
        ,NVL(T.TRADEGROUP, CHR(1))
        ,NVL(T.ORDERNO, CHR(1))
        ,NVL(T.ENTRYTIME, TO_DATE(''01.01.0001 00:00:00'', ''dd.mm.yyyy HH24:MI:SS''))
        ,NVL(T.BUYSELL, CHR(1))
        ,NVL(T.ORDERTYPE, CHR(1))
        ,NVL(T.QUANTITY, 0)
        ,NVL(T.DECIMALS, 0)
        ,NVL(T.PRICE, 0)
        ,NVL(T.STATUS, CHR(1))
        ,NVL(T.CLIENTCODE, CHR(1))
        ,0
        ,0
        ,0
           FROM '||p_Synonim||'.CM_CUX22 T
          WHERE T.ID_MB_REQUISITES IN (SELECT ID_MB_REQUISITES FROM '||p_Synonim||'.PROCESSING_ACTUAL WHERE FILE_TYPE = ''CUX22'' AND TRADE_DATE BETWEEN :p_Beg_date AND :p_End_date)
            AND T.ZRID = 0
        )'
    USING IN RegVal_ReplacebleList, IN RegVal_Prefix, IN p_Beg_date, IN p_End_date;
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DVREQ, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и FillCUX22(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=CUX22' || ' ' || TO_CHAR(p_Beg_date) || ' ' || TO_CHAR(p_End_date) || ' ' || p_Synonim || ' ' ||
                           ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DVREQ) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;

  FUNCTION UpdateDocNoCUX23(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2)
    RETURN NUMBER
  IS
  BEGIN
    EXECUTE IMMEDIATE
      'UPDATE D_CUX23_TMP CUX23
          SET CUX23.T_DOCNO = (SELECT REQUI.DOC_NO
                                 FROM '||p_Synonim||'.MB_REQUISITES REQUI
                                WHERE REQUI.ID_MB_REQUISITES = CUX23.t_ID_MB_REQUISITES
                                  AND ROWNUM = 1)';
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DVFXDEAL, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и UpdateDocNoCUX23(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=CUX23' ||
                           ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DVNDEAL) || '/' || TO_CHAR(RSB_GTFN.RG_DVFXDEAL) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;

  FUNCTION CheckExtSettleCodeCUX23(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Own_deals IN NUMBER, p_Client_deals IN NUMBER)
    RETURN NUMBER
  IS
    v_ErrStat NUMBER(5) := 0;
  BEGIN
    FOR c IN(SELECT DISTINCT tb.t_ExtSettleCode, NVL(ext.t_CodeType, 0) t_CodeType, NVL(ext.t_MarketKind, 0) t_MarketKind, NVL(ext.t_InPool, CHR(0)) t_InPool
               FROM D_CUX23_TMP tb, ddl_extsettlecode_dbt ext
              WHERE ext.t_SettleCode(+) = tb.t_ExtSettleCode ) LOOP
      v_ErrStat := 0;
      IF c.t_CodeType = 0 THEN
        v_ErrStat := NOTFOUND_EXTCODE;
      ELSE
        IF (NOT ((c.t_CodeType = Rsb_Derivatives.ALG_SP_MONEY_SOURCE_OWN AND p_Own_deals = 1) OR
                 (c.t_CodeType = Rsb_Derivatives.ALG_SP_MONEY_SOURCE_CLIENT AND p_Client_deals = 1)
                )
           ) THEN
          DELETE D_CUX23_TMP tb
           WHERE tb.t_ExtSettleCode = c.t_ExtSettleCode;
         ELSIF (NOT (((c.t_MarketKind = Rsb_Secur.DL_MARKETKIND_SETTLE_CURRENCY) AND (c.t_InPool <> CNST.SET_CHAR)) OR
                     ((c.t_MarketKind = Rsb_Secur.DL_MARKETKIND_SETTLE_STOCK_TPLUS) AND (c.t_InPool = CNST.SET_CHAR))
                    )
               ) THEN
           v_ErrStat := INCORRECT_EXTCODE;
         END IF;
       END IF;
       IF v_ErrStat <> 0 THEN
         UPDATE D_CUX23_TMP tb
            SET tb.t_ErrStat = v_ErrStat
          WHERE tb.t_ExtSettleCode = c.t_ExtSettleCode;
       END IF;
      END LOOP;
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DVFXDEAL, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и CheckExtSettleCodeCUX23(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=CUX23' ||
                           ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DVNDEAL) || '/' || TO_CHAR(RSB_GTFN.RG_DVFXDEAL) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;

  FUNCTION CheckExtSettleCodeCUX22(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER
  IS
    v_ErrStat NUMBER(5) := 0;
  BEGIN
    FOR c IN(SELECT DISTINCT tb.t_ExtSettleCode, NVL(ext.t_CodeType, 0) t_CodeType, NVL(ext.t_MarketKind, 0) t_MarketKind, NVL(ext.t_InPool, CHR(0)) t_InPool
               FROM D_CUX22_TMP tb, ddl_extsettlecode_dbt ext
              WHERE ext.t_SettleCode(+) = tb.t_ExtSettleCode ) LOOP
      v_ErrStat := 0;
      IF c.t_CodeType = 0 THEN
        v_ErrStat := NOTFOUND_EXTCODE;
      ELSE
        IF NOT c.t_CodeType = Rsb_Derivatives.ALG_SP_MONEY_SOURCE_CLIENT THEN
          DELETE D_CUX22_TMP tb
          WHERE tb.t_ExtSettleCode = c.t_ExtSettleCode;

         ELSIF (NOT (((c.t_MarketKind = Rsb_Secur.DL_MARKETKIND_SETTLE_CURRENCY) AND (c.t_InPool <> CNST.SET_CHAR)) OR
                     ((c.t_MarketKind = Rsb_Secur.DL_MARKETKIND_SETTLE_STOCK_TPLUS) AND (c.t_InPool = CNST.SET_CHAR))
                    )
               ) THEN
           v_ErrStat := INCORRECT_EXTCODE;
         END IF;
       END IF;
       IF v_ErrStat <> 0 THEN
         UPDATE D_CUX22_TMP tb
            SET tb.t_ErrStat = v_ErrStat
          WHERE tb.t_ExtSettleCode = c.t_ExtSettleCode;
       END IF;
      END LOOP;
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DVREQ, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и CheckExtSettleCodeCUX22(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=CUX22' ||
                           ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DVREQ) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;

  FUNCTION UpdateClientID_CUX23(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_MarketID IN NUMBER)
    RETURN NUMBER
  IS
    TYPE PtID_t IS TABLE OF DSFCONTR_DBT.T_PARTYID%TYPE;
    TYPE Code_t IS TABLE OF D_CUX23_TMP.T_CLIENTCODE%TYPE;
    v_PtID PtID_t;
    v_Code Code_t;
  BEGIN
     SELECT /*+ index(Tb D_CUX23_TMP_IDX2) */ DISTINCT sfcontr.T_PartyID, Tb.t_ClientCode
       BULK COLLECT INTO v_PtID, v_Code
       FROM DDLCONTR_DBT dlcontr,
            DDLCONTRMP_DBT contrmp,
            DSFCONTR_DBT sfcontr,
            D_CUX23_TMP Tb
      WHERE dlcontr.T_DLCONTRID = contrmp.T_DLCONTRID
        AND sfcontr.T_ID = contrmp.T_SFCONTRID
        AND sfcontr.T_SERVKIND = RSB_GTFN.PTSK_CM
        AND contrmp.T_MPCODE = Tb.t_ClientCode
        AND contrmp.T_MarketID = p_MarketID
        AND sfcontr.t_DateBegin <= Tb.t_ReportDate
        AND (    sfcontr.t_DateClose > Tb.t_ReportDate
              OR sfcontr.T_DateCLose = RSI_GT.ZeroDate)
        AND sfcontr.t_DateBegin =
              (SELECT MAX (sf.t_DateBegin)
                 FROM DSFCONTR_DBT sf
                WHERE sf.T_SERVKIND = sfcontr.T_SERVKIND
                  AND sf.T_ID = contrmp.T_SFCONTRID
                  AND contrmp.T_MPCODE = Tb.t_ClientCode
                  AND contrmp.T_MarketID = p_MarketID
                  AND sf.t_PartyID = sfcontr.t_PartyID
                  AND sf.t_DateBegin <= Tb.t_ReportDate)
        AND Tb.t_ClientCode <> CHR(1);
     IF v_Code.COUNT <> 0 THEN
       FORALL i IN v_Code.FIRST .. v_Code.LAST
         UPDATE D_CUX23_TMP Tb
            SET Tb.t_ClientID = v_PtID (i)
          WHERE Tb.t_ClientCode = v_Code (i);
       v_Code.delete;
       v_PtID.delete;
     END IF;

     SELECT distinct t.t_objectid, Tb.t_ClientCode
       BULK COLLECT INTO v_PtID, v_Code
       FROM dobjcode_dbt t, D_CUX23_TMP Tb
      WHERE Tb.t_ClientID <= 0
        AND Tb.t_ClientCode <> CHR(1)
        AND (     t.t_objecttype = Rsb_Secur.OBJTYPE_PARTY
              AND t.t_codekind = CNST.PTCK_MICEX
              AND t.t_code = Tb.t_ClientCode
              AND t.t_state = 0
              AND t.t_autokey =
                    (SELECT MIN (code.t_autokey)
                       FROM dobjcode_dbt code
                      WHERE code.t_objecttype = t.t_objecttype
                        AND code.t_codekind = t.t_codekind
                        AND code.t_code = t.t_code
                        AND code.t_state = t.t_state));
    IF v_Code.COUNT <> 0 THEN
      FORALL i IN v_Code.FIRST .. v_Code.LAST
           UPDATE D_CUX23_TMP Tb
              SET Tb.t_ClientID = v_PtID (i)
            WHERE Tb.t_ClientCode = v_Code (i);
      v_Code.delete;
      v_PtID.delete;
    END IF;

    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DVFXDEAL, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и UpdateClientID_CUX23(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=CUX23' ||
                           ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DVNDEAL) || '/' || TO_CHAR(RSB_GTFN.RG_DVFXDEAL) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;

  FUNCTION UpdateClientID_CUX22(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_MarketID IN NUMBER)
    RETURN NUMBER
  IS
    TYPE PtID_t IS TABLE OF DSFCONTR_DBT.T_PARTYID%TYPE;
    TYPE Code_t IS TABLE OF D_CUX22_TMP.T_CLIENTCODE%TYPE;
    v_PtID PtID_t;
    v_Code Code_t;
  BEGIN
     SELECT /*+ index(Tb D_CUX22_TMP_IDX2) */ DISTINCT sfcontr.T_PartyID, Tb.t_ClientCode
       BULK COLLECT INTO v_PtID, v_Code
       FROM DDLCONTR_DBT dlcontr,
            DDLCONTRMP_DBT contrmp,
            DSFCONTR_DBT sfcontr,
            D_CUX22_TMP Tb
      WHERE dlcontr.T_DLCONTRID = contrmp.T_DLCONTRID
        AND sfcontr.T_ID = contrmp.T_SFCONTRID
        AND sfcontr.T_SERVKIND = RSB_GTFN.PTSK_CM
        AND contrmp.T_MPCODE = Tb.t_ClientCode
        AND contrmp.T_MarketID = p_MarketID
        AND sfcontr.t_DateBegin <= Tb.t_ReportDate
        AND (    sfcontr.t_DateClose > Tb.t_ReportDate
              OR sfcontr.T_DateCLose = RSI_GT.ZeroDate)
        AND sfcontr.t_DateBegin =
              (SELECT MAX (sf.t_DateBegin)
                 FROM DSFCONTR_DBT sf
                WHERE sf.T_SERVKIND = sfcontr.T_SERVKIND
                  AND sf.T_ID = contrmp.T_SFCONTRID
                  AND contrmp.T_MPCODE = Tb.t_ClientCode
                  AND contrmp.T_MarketID = p_MarketID
                  AND sf.t_PartyID = sfcontr.t_PartyID
                  AND sf.t_DateBegin <= Tb.t_ReportDate)
        AND Tb.t_ClientCode <> CHR(1);
     IF v_Code.COUNT <> 0 THEN
       FORALL i IN v_Code.FIRST .. v_Code.LAST
         UPDATE D_CUX22_TMP Tb
            SET Tb.t_ClientID = v_PtID (i)
          WHERE Tb.t_ClientCode = v_Code (i);
       v_Code.delete;
       v_PtID.delete;
     END IF;

     SELECT distinct t.t_objectid, Tb.t_ClientCode
       BULK COLLECT INTO v_PtID, v_Code
       FROM dobjcode_dbt t, D_CUX22_TMP Tb
      WHERE Tb.t_ClientID <= 0
        AND Tb.t_ClientCode <> CHR(1)
        AND (     t.t_objecttype = Rsb_Secur.OBJTYPE_PARTY
              AND t.t_codekind = CNST.PTCK_MICEX
              AND t.t_code = Tb.t_ClientCode
              AND t.t_state = 0
              AND t.t_autokey =
                    (SELECT MIN (code.t_autokey)
                       FROM dobjcode_dbt code
                      WHERE code.t_objecttype = t.t_objecttype
                        AND code.t_codekind = t.t_codekind
                        AND code.t_code = t.t_code
                        AND code.t_state = t.t_state));
    IF v_Code.COUNT <> 0 THEN
      FORALL i IN v_Code.FIRST .. v_Code.LAST
        UPDATE D_CUX22_TMP Tb
           SET Tb.t_ClientID = v_PtID (i)
         WHERE Tb.t_ClientCode = v_Code (i);
      v_Code.delete;
      v_PtID.delete;
    END IF;

    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DVREQ, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и UpdateClientID_CUX22(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=CUX22' ||
                           ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DVREQ) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;

  FUNCTION UpdateObjCUX23(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_IsRSHB IN NUMBER)
    RETURN NUMBER
  IS
    TYPE ObjID_t IS TABLE OF DGTCODE_DBT.T_OBJECTID%TYPE;
    TYPE SemID_t IS TABLE OF D_CUX23_TMP.T_ID_CM_CUX23%TYPE;
    v_ObjIDs ObjID_t;
    v_SemID  SemID_t;
  BEGIN
    SELECT GTCODE.T_OBJECTID, CUX23.t_ID_CM_CUX23
      BULK COLLECT INTO v_ObjIDs, v_semID
      FROM DGTCODE_DBT GTCODE, D_CUX23_TMP CUX23
     WHERE GTCODE.T_APPLICATIONID IN (RSB_GTFN.GT_PAYMENTS_VR, RSB_GTFN.GT_VR)
       AND GTCODE.T_OBJECTKIND IN (RSB_GTFN.RG_DVNDEAL, RSB_GTFN.RG_DVFXDEAL)
       AND GTCODE.T_OBJECTCODE = 'CUX23_' || CUX23.T_BUYSELL || '/'
                               || CASE WHEN (CUX23.T_KIND = ВалютныйСВОП AND p_IsRSHB = 0) OR (CUX23.T_KIND = ВалютныйСВОП_РСХБ AND p_IsRSHB = 1) OR CUX23.T_KIND = КороткийСВОП
                                       THEN CUX23.T_REPOTRADENO ELSE CUX23.T_TRADENO END || '_'
                               || DECODE(SUBSTR(to_char(CUX23.T_REPORTDATE,'dd'), 1, 1), '0', ' ', '') || TRIM(LEADING '0' FROM to_char(CUX23.T_REPORTDATE, 'dd.mm.yyyy'));
    IF v_semID.COUNT <> 0 THEN
      FORALL i IN v_semID.FIRST .. v_semID.LAST
        UPDATE d_CUX23_TMP CUX23
           SET CUX23.t_ObjectID = v_ObjIDs (i)
         WHERE CUX23.t_ID_CM_CUX23 = v_semID (i);
     END IF;
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DVFXDEAL, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и UpdateObjCUX23(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=CUX23' ||
                           ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DVNDEAL) || '/' || TO_CHAR(RSB_GTFN.RG_DVFXDEAL) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;

  FUNCTION UpdateObjCUX22(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER
  IS
    TYPE ObjID_t IS TABLE OF DGTCODE_DBT.T_OBJECTID%TYPE;
    TYPE SemID_t IS TABLE OF D_CUX22_TMP.T_ID_CM_CUX22%TYPE;
    v_ObjIDs ObjID_t;
    v_SemID  SemID_t;
  BEGIN
    SELECT GTCODE.T_OBJECTID, CUX22.t_ID_CM_CUX22
      BULK COLLECT INTO v_ObjIDs, v_semID
      FROM DGTCODE_DBT GTCODE, D_CUX22_TMP CUX22
     WHERE GTCODE.T_APPLICATIONID IN (RSB_GTFN.GT_PAYMENTS_VR, RSB_GTFN.GT_VR)
       AND GTCODE.T_OBJECTKIND = RSB_GTFN.RG_DVREQ
       AND GTCODE.T_OBJECTCODE = CUX22.T_ORDERNO || '_VR_' || DECODE(SUBSTR(to_char(CUX22.T_REPORTDATE,'dd'), 1, 1), '0', ' ', '') || TRIM(LEADING '0' FROM to_char(CUX22.T_REPORTDATE, 'dd.mm.yyyy'));
    IF v_semID.COUNT <> 0 THEN
      FORALL i IN v_semID.FIRST .. v_semID.LAST
        UPDATE d_CUX22_TMP CUX22
           SET CUX22.t_ObjectID = v_ObjIDs (i)
         WHERE CUX22.t_ID_CM_CUX22 = v_semID (i);
     END IF;
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DVREQ, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и UpdateObjCUX22(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=CUX22' ||
                           ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DVREQ) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;

  FUNCTION UpdateCUX23_PM(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2, p_IsRSHB IN NUMBER)
    RETURN NUMBER
  IS
    TYPE ObjID_t IS TABLE OF DGTSNCREC_DBT.T_RECORDID%TYPE;
    TYPE PmID_t IS TABLE OF D_CUX23_TMP.T_ID_CM_CUX23%TYPE;
    v_ObjIDs ObjID_t;
    v_PmID   PmID_t;
  BEGIN
    SELECT CUX23.T_ID_CM_CUX23, GTSNCREC.T_RECORDID
      BULK COLLECT INTO v_PmID, v_ObjIDs
      FROM dgtsncrec_dbt gtsncrec,
           dgtcode_dbt gtcode,
           dgtobject_dbt gtobject,
           D_CUX23_TMP CUX23
     WHERE gtsncrec.t_seanceid = p_SeanceID
       AND GTCODE.T_OBJECTKIND IN (RSB_GTFN.RG_DVNDEAL, RSB_GTFN.RG_DVFXDEAL)
       AND GTCODE.T_OBJECTID = GTSNCREC.T_OBJECTID
       AND GTOBJECT.T_OBJECTID = GTSNCREC.T_OBJECTID
       AND GTCODE.T_OBJECTCODE = 'CUX23_' || CUX23.T_BUYSELL || '/' 
                               || CASE WHEN (CUX23.T_KIND = ВалютныйСВОП AND p_IsRSHB = 0) OR (CUX23.T_KIND = ВалютныйСВОП_РСХБ AND p_IsRSHB = 1) OR CUX23.T_KIND = КороткийСВОП
                                       THEN CUX23.T_REPOTRADENO ELSE CUX23.T_TRADENO END || '_'
                               || DECODE(SUBSTR(to_char(CUX23.T_REPORTDATE,'dd'), 1, 1), '0', ' ', '') || TRIM(LEADING '0' FROM to_char(CUX23.T_REPORTDATE, 'dd.mm.yyyy'));

    IF v_PmID.COUNT <> 0 THEN
      FORALL i IN v_PmID.FIRST .. v_PmID.LAST
        EXECUTE IMMEDIATE
          'UPDATE '||p_Synonim||'.CM_CUX23 CUX23
              SET ZRID = :v_ObjIDsi
            WHERE ID_CM_CUX23 = :v_PmIDi'
        USING IN v_ObjIDs(i), IN v_PmID(i);
    END IF;
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DVFXDEAL, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и UpdateCUX23_PM(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=CUX23' ||
                           ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DVNDEAL) || '/' || TO_CHAR(RSB_GTFN.RG_DVFXDEAL) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;

  FUNCTION UpdateCUX22_PM(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2)
    RETURN NUMBER
  IS
    TYPE ObjID_t IS TABLE OF DGTSNCREC_DBT.T_RECORDID%TYPE;
    TYPE PmID_t IS TABLE OF D_CUX22_TMP.T_ID_CM_CUX22%TYPE;
    v_ObjIDs ObjID_t;
    v_PmID   PmID_t;
  BEGIN
    SELECT CUX22.T_ID_CM_CUX22, GTSNCREC.T_RECORDID
      BULK COLLECT INTO v_PmID, v_ObjIDs
      FROM dgtsncrec_dbt gtsncrec,
           dgtcode_dbt gtcode,
           dgtobject_dbt gtobject,
           D_CUX22_TMP CUX22
     WHERE gtsncrec.t_seanceid = p_SeanceID
       AND GTCODE.T_OBJECTKIND = RSB_GTFN.RG_DVREQ
       AND GTCODE.T_OBJECTID = GTSNCREC.T_OBJECTID
       AND GTOBJECT.T_OBJECTID = GTSNCREC.T_OBJECTID
       AND GTCODE.T_OBJECTCODE = CUX22.T_ORDERNO || '_VR_' || DECODE(SUBSTR(to_char(CUX22.T_REPORTDATE,'dd'), 1, 1), '0', ' ', '') || TRIM(LEADING '0' FROM to_char(CUX22.T_REPORTDATE, 'dd.mm.yyyy'));

    IF v_PmID.COUNT <> 0 THEN
      FORALL i IN v_PmID.FIRST .. v_PmID.LAST
        EXECUTE IMMEDIATE
          'UPDATE '||p_Synonim||'.CM_CUX22 CUX22
              SET ZRID = :v_ObjIDsi
            WHERE ID_CM_CUX22 = :v_PmIDi'
        USING IN v_ObjIDs(i), IN v_PmID(i);
    END IF;
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DVREQ, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и UpdateCUX22_PM(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=CUX22' ||
                           ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DVREQ) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;

  FUNCTION DelCUX23(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER
  IS
  BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE D_CUX23_TMP';
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DVFXDEAL, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и DelCUX23(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=CUX23' ||
                           ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DVNDEAL) || '/' || TO_CHAR(RSB_GTFN.RG_DVFXDEAL) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;

  FUNCTION DelCUX22(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER
  IS
  BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE D_CUX22_TMP';
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DVREQ, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и DelCUX22(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=CUX22' ||
                           ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DVREQ) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;

  FUNCTION PutDeleteDealInfo(p_Prm IN OUT NOCOPY PRMREC_t, p_ErrMsg OUT VARCHAR2, p_DealCode IN VARCHAR2, p_DealKind IN NUMBER, p_DealClient IN NUMBER, p_DealDate IN DATE)
    RETURN NUMBER
  IS
    v_stat NUMBER(5) := 0;
    v_RG_ObjKind NUMBER(5) := 0;
    v_RG_ObjCode VARCHAR2(25) := CHR(1);
  BEGIN
    p_ErrMsg := CHR(1);

    IF p_DealKind = p_Prm.Using_ВалютныйСВОП OR p_DealKind = p_Prm.Using_ВнебиржФорвард THEN
      v_RG_ObjKind := RSB_GTFN.RG_DVNDEAL;
      v_RG_ObjCode := 'Внебирж. сделка с ПИ № ';
    ELSIF p_DealKind = КороткийСВОП OR p_DealKind = ПИКО_ПокупкаПродажа OR (p_Prm.IsRSHB = 1 AND (p_DealKind = ПИКО_ПокупкаПродажа_Кл OR p_DealKind = КороткийСВОП_Кл)) THEN
      v_RG_ObjKind := RSB_GTFN.RG_DVFXDEAL;
      v_RG_ObjCode := 'Конверс. сделка в ПИ № ';
    END IF;

    v_stat := RSI_GT.Init(RSI_GT.DIRECT_INSERT_YES, p_Prm.SeanceID, p_Prm.Source_Code, RSI_GT.GTCODE_RECEIVE, p_ErrMsg);

    IF v_stat = 0 THEN
      v_stat := RSI_GT.InitRec(v_RG_ObjKind,
                               v_RG_ObjCode || p_DealCode || ' на ' || CASE WHEN SUBSTR(to_char(p_DealDate,'dd'), 1, 1) = '0' THEN ' ' ELSE '' END || TRIM(LEADING '0' FROM to_char(p_DealDate, 'dd.mm.yyyy')), --Наименование объекта
                               1, --проверять существование объекта в системе по коду
                               p_DealCode, --код объекта
                               p_DealClient, --ID клиента
                               p_ErrMsg);
    END IF;

    IF v_stat = 0 THEN
      --добавляем параметры
      IF v_RG_ObjKind = RSB_GTFN.RG_DVFXDEAL THEN
        RSI_GT.SetParmByName('RGDVFXDL_CODE',   p_DealCode);
        RSI_GT.SetParmByName('RGDVFXDL_KIND',   p_DealKind);
        RSI_GT.SetParmByName('RGDVFXDL_CLIENT', p_DealClient);
      ELSIF p_DealKind = p_Prm.Using_ВалютныйСВОП OR p_DealKind = p_Prm.Using_ВнебиржФорвард THEN
        RSI_GT.SetParmByName('RGDVNDL_CODE',    p_DealCode);
        RSI_GT.SetParmByName('RGDVNDL_KIND',    p_DealKind);
        RSI_GT.SetParmByName('RGDVNDL_CLIENT',  p_DealClient);
      END IF;

      RSI_GT.SetParmByName('RGDV_PROGNOS', 'D');

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

  FUNCTION MarkDealsForDelete(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Beg_date IN DATE, p_End_date IN DATE, p_IsRSHB IN NUMBER, p_NumImport OUT NUMBER)
    RETURN NUMBER
  IS
    v_Prm PRMREC_t;
    v_stat NUMBER;
    v_ErrMes VARCHAR2(1000);
  BEGIN
    p_NumImport := 0;

    --инициализация используемых параметров
    v_Prm.SeanceID    := p_SeanceID;
    v_Prm.Source_Code := p_SourceCode;
    v_Prm.IsRSHB      := p_IsRSHB;

    IF v_Prm.IsRSHB = 1 THEN
       v_Prm.Using_ВнебиржФорвард := ВнебиржФорвард_РСХБ;
       v_Prm.Using_ВалютныйСВОП := ВалютныйСВОП_РСХБ;
    ELSE
       v_Prm.Using_ВнебиржФорвард := ВнебиржФорвард;
       v_Prm.Using_ВалютныйСВОП := ВалютныйСВОП;
    END IF;

    FOR cData IN (SELECT D.T_CODE, D.T_KIND, D.T_CLIENT, D.T_DATE
                    FROM ddvndeal_dbt D
                   WHERE (     D.T_KIND IN (v_Prm.Using_ВалютныйСВОП, v_Prm.Using_ВнебиржФорвард, КороткийСВОП, ПИКО_ПокупкаПродажа)
                           OR (D.T_KIND IN (ПИКО_ПокупкаПродажа_Кл, КороткийСВОП_Кл) AND v_Prm.IsRSHB = 1)
                         )
                     AND D.T_PROGNOS = CNST.SET_CHAR
                     AND D.T_DATE >= p_Beg_date
                     AND D.T_DATE <= p_End_date
                     AND D.T_CODE NOT IN (SELECT P.T_STRINGVAL
                                            FROM dgtrecord_dbt R, DGTAPP_DBT A, dgtobject_dbt O, dgtrecprm_dbt P
                                           WHERE A.T_APPLICATIONID IN (RSB_GTFN.GT_VR, RSB_GTFN.GT_PAYMENTS_VR)
                                             AND R.T_APPLICATIONID_FROM = A.T_APPLICATIONID
                                             AND R.T_OBJECTID = O.T_OBJECTID
                                             AND O.T_OBJECTKIND IN (RSB_GTFN.RG_DVNDEAL, RSB_GTFN.RG_DVFXDEAL)
                                             AND R.T_RECORDID = P.T_RECORDID
                                             AND P.T_KOPRMID IN (SELECT T_KOPRMID
                                                                   FROM dgtkoprm_dbt
                                                                  WHERE (t_objectkind = RSB_GTFN.RG_DVNDEAL AND t_code = 3)
                                                                     OR (t_objectkind = RSB_GTFN.RG_DVFXDEAL AND t_code = 3)
                                                                )
                                             AND R.T_ACTIONID in (1,3)
                                             AND R.T_STATUSID = 2
                                         )
                 )
    LOOP
      v_stat := PutDeleteDealInfo(v_Prm, v_ErrMes, cData.t_Code, cData.t_Kind, cData.t_Client, cData.t_Date);

      IF v_ErrMes <> CHR(1) THEN
        RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DVFXDEAL, RSB_GTLOG.ISSUE_FAULT, 'Ошибка при создании ЗР для удаления неподтверждённой сделки "'|| cData.t_Code || '"' || CHR(10) || v_ErrMes);
      END IF;

      p_NumImport := p_NumImport + 1;
    END LOOP;

    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DVFXDEAL, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка при обработке данных в ф-и MarkDealsForDelete(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=CUX23' ||
                           ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DVNDEAL) || '/' || TO_CHAR(RSB_GTFN.RG_DVFXDEAL) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;

  FUNCTION RG_GetNumWorkDaysForPeriodByCode(p_SourceID IN NUMBER, p_dateFrom IN DATE, p_dateTo IN DATE, p_FIID_CODE IN VARCHAR2, p_CalcFIID_CODE IN VARCHAR2, p_PartyID_CODE IN VARCHAR2, p_NumWorkDays OUT NUMBER, p_IsRSHB IN NUMBER)
    RETURN NUMBER
  IS
    v_PartyID NUMBER(10) := -1;
    v_FIID NUMBER(10) := -1;
    v_CalcFIID NUMBER(10) := -1;
    v_Code_FinIn NUMBER(5);
    v_Code_Subject NUMBER(5);
    v_ErrMes VARCHAR2(1000);
    v_SfContrID NUMBER(10);
  BEGIN
   v_Code_FinIn   := RSB_SECUR.CODE_MICEX;
   v_Code_Subject := CNST.PTCK_MICEX;

   IF (p_PartyID_CODE IS NOT NULL) AND p_PartyID_CODE <> CHR(1) THEN
     IF RSB_GTFN.GetRealID(RSB_GTFN.RG_PARTY, p_PartyID_CODE, v_Code_Subject, v_PartyID, p_IsRSHB, v_ErrMes, NULL, NULL, NULL, v_SfContrID) <> 0 THEN
       RETURN NOTFOUND_PARTYID;
     END IF;
   END IF;

   IF (p_FIID_CODE IS NOT NULL) AND p_FIID_CODE <> CHR(1) THEN
     IF RSB_GTFN.GetRealID(RSB_GTFN.RG_CURRENCY, p_FIID_CODE, v_Code_FinIn, v_FIID, p_IsRSHB, v_ErrMes, NULL, NULL, NULL, v_SfContrID) <> 0 THEN
       RETURN NOTFOUND_FIID;
     END IF;
   END IF;

   IF (p_CalcFIID_CODE IS NOT NULL) AND p_CalcFIID_CODE <> CHR(1) THEN
     IF RSB_GTFN.GetRealID(RSB_GTFN.RG_CURRENCY, p_CalcFIID_CODE, v_Code_FinIn, v_CalcFIID, p_IsRSHB, v_ErrMes, NULL, NULL, NULL, v_SfContrID) <> 0 THEN
       RETURN NOTFOUND_FIID;
     END IF;
   END IF;

   p_NumWorkDays := RSI_DlCalendars.GetNumWorkDaysForPeriod(p_dateFrom, p_dateTo, v_CalcFIID, v_FIID, v_PartyID);

   RETURN 0;
  END;

  FUNCTION UpdateKindCUX23(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_IsRSHB IN NUMBER)
    RETURN NUMBER
  IS
    v_Kind NUMBER(5) := 0;
    v_NumWorkDays NUMBER(5) := 0;
    v_ErrStat NUMBER(5) := 0;
  BEGIN
    FOR c IN(SELECT DISTINCT CUX23.t_TradeGroup, CUX23.t_TradeDeriv, DECODE(CUX23.t_ClientCode, CHR(1), 0, 1) t_IsClient, CUX23.t_ReportDate, CUX23.t_SettleDate2, CUX23.t_CurrencyId, CUX23.t_CoCurrencyId
               FROM D_CUX23_TMP CUX23
              WHERE CUX23.t_ErrStat = 0) LOOP
      v_Kind := 0; v_ErrStat := 0;
      IF c.t_TradeGroup = 'S' THEN
        IF p_IsRSHB = 1 THEN
          IF c.t_TradeDeriv = 'N' THEN
            v_ErrStat := RG_GetNumWorkDaysForPeriodByCode(RSB_GTFN.GT_PAYMENTS_VR, c.t_ReportDate, c.t_SettleDate2, c.t_CurrencyId, c.t_CoCurrencyId, RSB_GTFN.MMVB_CODE, v_NumWorkDays, p_IsRSHB);
            IF v_ErrStat = 0 THEN
              IF v_NumWorkDays >= 3 THEN
                v_Kind := ВалютныйСВОП_РСХБ;
              ELSE
                v_Kind := CASE WHEN c.t_IsClient = 1 THEN КороткийСВОП_Кл ELSE КороткийСВОП END;
              END IF;
            END IF;
          END IF;
        ELSE
          IF c.t_TradeDeriv = 'Y' THEN
            v_Kind := ВалютныйСВОП;
          ELSIF c.t_TradeDeriv = 'N' THEN
            v_Kind := КороткийСВОП;
          END IF;
        END IF;
      ELSE
        IF c.t_TradeDeriv = 'Y' THEN
          v_Kind := CASE WHEN p_IsRSHB = 1 THEN ВнебиржФорвард_РСХБ ELSE ВнебиржФорвард END;
        ELSIF c.t_TradeDeriv = 'N' THEN
          IF p_IsRSHB = 1 AND c.t_IsClient = 1 THEN
            v_Kind := ПИКО_ПокупкаПродажа_Кл;
          ELSE
            v_Kind := ПИКО_ПокупкаПродажа;
          END IF;
        END IF;
      END IF;

      IF v_Kind = 0 AND v_ErrStat = 0 THEN --Не обрабатываем такие сделки
        DELETE FROM D_CUX23_TMP CUX23
         WHERE CUX23.t_TradeGroup = c.t_TradeGroup
           AND CUX23.t_TradeDeriv = c.t_TradeDeriv
           AND DECODE (CUX23.t_ClientCode, CHR(1), 0, 1) = c.t_IsClient
           AND CUX23.t_ReportDate = c.t_ReportDate
           AND (CUX23.t_TradeGroup != 'S' OR CUX23.t_SettleDate2 = c.t_SettleDate2)
           AND CUX23.t_CurrencyId = c.t_CurrencyId
           AND CUX23.t_CoCurrencyId = c.t_CoCurrencyId
           AND CUX23.t_ErrStat = 0;
      ELSE
        UPDATE D_CUX23_TMP CUX23
           SET CUX23.t_Kind = v_Kind, CUX23.t_ErrStat = v_ErrStat
         WHERE CUX23.t_TradeGroup = c.t_TradeGroup
           AND CUX23.t_TradeDeriv = c.t_TradeDeriv
           AND DECODE (CUX23.t_ClientCode, CHR(1), 0, 1) = c.t_IsClient
           AND CUX23.t_ReportDate = c.t_ReportDate
           AND (CUX23.t_TradeGroup != 'S' OR CUX23.t_SettleDate2 = c.t_SettleDate2)
           AND CUX23.t_CurrencyId = c.t_CurrencyId
           AND CUX23.t_CoCurrencyId = c.t_CoCurrencyId
           AND CUX23.t_ErrStat = 0;
      END IF;
    END LOOP;

    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DVFXDEAL, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка при обработке данных в ф-и UpdateKindCUX23(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=CUX23' ||
                           ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DVNDEAL) || '/' || TO_CHAR(RSB_GTFN.RG_DVFXDEAL) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;

  FUNCTION IsAdminOrOperBO(p_Oper IN NUMBER)
    RETURN NUMBER
  IS
    v_RetVal NUMBER := 0;
  BEGIN
    SELECT 1 INTO v_RetVal
      FROM dacsoprole_dbt t, DACSROLETREE_DBT tt
     WHERE T.T_ROLEID = TT.T_ROLEID
       AND TT.T_NAME IN ('[13] Специалист БУ БО', '[14] Специалист БО', 'Администратор')
       AND t.t_oper = p_Oper
     FETCH FIRST ROW ONLY;
    RETURN v_RetVal;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN RETURN 0;
  END;

  FUNCTION DelCCX10(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER
  IS
  BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE D_CCX10_TMP';
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DVCOMMISVR, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и DelCCX10(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=CCX10' ||
                           ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DVCOMMISVR) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;

  FUNCTION FillCCX10(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2, p_Beg_date IN DATE, p_End_date IN DATE, p_IsMain IN NUMBER)
    RETURN NUMBER
  IS
  BEGIN
    EXECUTE IMMEDIATE
      'INSERT INTO D_CCX10_TMP(
         T_ID_CM_CCX10
        ,T_ID_MB_REQUISITES
        ,T_REPORTDATE
        ,T_REPORTDATESTR
        ,T_EXTCODE
        ,T_COMMISTYPE
        ,T_COMMISNAME
        ,T_COMMSUM
        ,T_ITSVAT
        ,T_DOCNO
        ,T_OBJECTID
        )
        (SELECT
         T.ID_CM_CCX10
        ,NVL(T.ID_MB_REQUISITES, 0)
        ,NVL(T.REPORTDATE, TO_DATE(''01.01.0001'', ''DD.MM.YYYY''))
        ,DECODE(SUBSTR(to_char(NVL(T.REPORTDATE, TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')),''dd''), 1, 1), ''0'', '' '', '''') || TRIM(LEADING ''0'' FROM to_char(NVL(T.REPORTDATE, TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')), ''dd.mm.yyyy''))
        ,NVL(T.EXTSETTLECODE, CHR(1))
        ,NVL(TO_NUMBER(T.COMMISTYPE), 0)
        ,NVL(T.COMMISNAME, CHR(1))
        ,NVL(T.COMM, 0)
        ,NVL(T.ITSVAT, 0)
        ,0
        ,0
           FROM '||p_Synonim||'.CM_CCX10 T, DDL_EXTSETTLECODE_DBT EXT
          WHERE T.ID_MB_REQUISITES IN (SELECT ID_MB_REQUISITES FROM '||p_Synonim||'.PROCESSING_ACTUAL WHERE FILE_TYPE = ''CCX10'' AND TRADE_DATE BETWEEN :p_Beg_date AND :p_End_date)
            AND T.ZRID = 0
            AND EXT.T_SETTLECODE (+) = NVL(T.EXTSETTLECODE, CHR(1))
            AND (    (:p_IsMain = 1 AND NVL(EXT.T_CODETYPE, 0) != :IsClient)
                  OR (:p_IsMain != 1 AND NVL(EXT.T_CODETYPE, 0) = :IsClient AND NVL(TO_NUMBER(T.COMMISTYPE), 0) = 99)
                )
        )'
    USING IN p_Beg_date, IN p_End_date, IN p_IsMain, IN RSB_DERIVATIVES.ALG_SP_MONEY_SOURCE_CLIENT, IN p_IsMain, IN RSB_DERIVATIVES.ALG_SP_MONEY_SOURCE_CLIENT;
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DVCOMMISVR, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и FillCCX10(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=CCX10' ||
                           ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DVCOMMISVR) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;

  FUNCTION UpdateDocNoCCX10(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2)
    RETURN NUMBER
  IS
  BEGIN
    EXECUTE IMMEDIATE
      'UPDATE D_CCX10_TMP CCX10
          SET CCX10.T_DOCNO = (SELECT REQUI.DOC_NO
                                 FROM '||p_Synonim||'.MB_REQUISITES REQUI
                                WHERE REQUI.ID_MB_REQUISITES = CCX10.t_ID_MB_REQUISITES
                                  AND ROWNUM = 1)';
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DVCOMMISVR, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и UpdateDocNoCCX10(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=CCX10' ||
                           ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DVCOMMISVR) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;

  FUNCTION UpdateObjCCX10(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER
  IS
    TYPE ObjID_t IS TABLE OF DGTCODE_DBT.T_OBJECTID%TYPE;
    TYPE SemID_t IS TABLE OF D_CCX10_TMP.T_ID_CM_CCX10%TYPE;
    v_ObjIDs ObjID_t;
    v_SemID  SemID_t;
  BEGIN
    SELECT GTCODE.T_OBJECTID, CCX10.t_ID_CM_CCX10
      BULK COLLECT INTO v_ObjIDs, v_semID
      FROM DGTCODE_DBT GTCODE, D_CCX10_TMP CCX10
     WHERE GTCODE.T_APPLICATIONID IN (RSB_GTFN.GT_PAYMENTS_VR, RSB_GTFN.GT_CVVR, RSB_GTFN.GT_CX99)
       AND GTCODE.T_OBJECTKIND = RSB_GTFN.RG_DVCOMMISVR
       AND GTCODE.T_OBJECTCODE = CCX10.T_DOCNO || '_' || CCX10.T_EXTCODE || '_' || TO_CHAR(CCX10.T_COMMISTYPE) || '_' || CCX10.T_REPORTDATESTR;
    IF v_semID.COUNT <> 0 THEN
      FORALL i IN v_semID.FIRST .. v_semID.LAST
        UPDATE D_CCX10_TMP CCX10
           SET CCX10.t_ObjectID = v_ObjIDs (i)
         WHERE CCX10.t_ID_CM_CCX10 = v_semID (i);
     END IF;
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DVCOMMISVR, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и UpdateObjCCX10(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=CCX10' ||
                           ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DVCOMMISVR) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;

  --Заполнение и сохранение параметров комиссии
  FUNCTION PutCommisInfoPM(p_Comm IN OUT NOCOPY D_CCX10_TMP%ROWTYPE, p_ErrMes IN OUT NOCOPY VARCHAR2) RETURN NUMBER
  IS
    v_stat NUMBER(5) := 0;
  BEGIN

    v_stat := RSI_GT.InitRec(RSB_GTFN.RG_DVCOMMISVR,
                             'Комиссионное вознаграждение с расчетным кодом ' || p_Comm.t_ExtCode || ' вида ' || TO_CHAR(p_Comm.t_CommisType) || ' на ' || p_Comm.t_ReportDateStr, --Наименование объекта
                             0, --проверять существование объекта в системе по коду
                             p_Comm.t_DocNo || '_' || p_Comm.t_ExtCode || '_' || TO_CHAR(p_Comm.t_CommisType) || '_' || p_Comm.t_ReportDateStr, --код объекта
                             0, --ID клиента
                             p_ErrMes);

    IF v_stat = 0 THEN
      RSI_GT.SetParmByName('RGDVCVVR_DATE',       p_Comm.t_ReportDate);
      RSI_GT.SetParmByName('RGDVCVVR_EXTCODE',    p_Comm.t_ExtCode);
      RSI_GT.SetParmByName('RGDVCVVR_COMMISTYPE', p_Comm.t_CommisType);
      RSI_GT.SetParmByName('RGDVCVVR_COMMISNAME', p_Comm.t_CommisName);
      RSI_GT.SetParmByName('RGDVCVVR_COMMSUM',    p_Comm.t_CommSum);
      RSI_GT.SetParmByName('RGDVCVVR_ITSVAT',     p_Comm.t_ITSVAT);

      --проверка ошибок после добавления параметров
      IF RSI_GT.GetLastError <> CHR(1) THEN
        v_stat := 1;
        p_ErrMes := RSI_GT.GetLastError;
      END IF;
    END IF;

    RETURN v_stat;
  END;

  --Создание объектов записей репликаций по данным временной таблицы CCX10
  FUNCTION CreateReplRecByTmp_CCX10(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER
  IS
    TYPE CDATA_t IS REF CURSOR RETURN D_CCX10_TMP%ROWTYPE;
    v_cData   CDATA_t;
    v_Comm    D_CCX10_TMP%ROWTYPE;
    v_stat    NUMBER(5) := 0;
    v_ErrMes  VARCHAR2(1000) := CHR(1);
    v_WarnMes VARCHAR2(1000) := CHR(1);
  BEGIN
    --инициализация глобального лога для накопления записей для вставки
    RSB_GTLOG.Init(BATCH_SIZE, p_SeanceID, RSB_GTFN.RG_DVCOMMISVR);

    --инициализация глобального накопителя объектов ЗР для вставки
    v_stat := RSI_GT.Init(RSI_GT.DIRECT_INSERT_YES, p_SeanceID, p_SourceCode, RSI_GT.GTCODE_RECEIVE, v_ErrMes);
    IF v_stat <> 0 THEN
      RSB_GTLOG.Error(v_ErrMes);
    END IF;

    IF v_stat = 0 THEN
      OPEN v_cData FOR
           --важен порядок и наличие всех полей, чтобы сработал FETCH
           SELECT T_ID_CM_CCX10,
                  T_ID_MB_REQUISITES,
                  NVL(t.T_DOCNO, CHR(1)) DOCNO,
                  NVL(t.T_REPORTDATE, RSI_GT.ZeroDate) REPORTDATE,
                  NVL(t.T_EXTCODE, CHR(1)) EXTCODE,
                  NVL(t.T_COMMISTYPE, 0) COMMISTYPE,
                  NVL(t.T_COMMISNAME, CHR(1)) COMMISNAME,
                  NVL(t.T_COMMSUM, 0) COMMSUM,
                  NVL(t.T_ITSVAT, 0) ITSVAT,
                  NVL(t.T_OBJECTID, 0) OBJECTID,
                  NVL(t.T_REPORTDATESTR, CHR(1)) REPORTDATESTR
            FROM D_CCX10_TMP t
           ORDER BY t.T_REPORTDATE, t.T_ID_CM_CCX10;
      LOOP
        FETCH v_cData INTO v_Comm;
        EXIT WHEN v_cData%NOTFOUND OR v_cData%NOTFOUND IS NULL;

        v_ErrMes  := CHR(1);
        v_WarnMes := CHR(1);

        IF v_Comm.t_ObjectID > 0 THEN
          v_ErrMes := 'Ошибка при загрузке комиссионного вознаграждения "' || v_Comm.t_ExtCode || '_' || TO_CHAR(v_Comm.t_CommisType) || '_' || v_Comm.t_ReportDateStr || '"' || CHR(10) ||
                      'Запись репликации с действием создать для объекта ' || TO_CHAR(v_Comm.t_ObjectID) || ' уже передавалась в RS-Bank.' || CHR(10) || 'Повторная репликация невозможна.';
        END IF;

        IF v_ErrMes = CHR(1) THEN
          IF PutCommisInfoPM(v_Comm, v_ErrMes) = 0 THEN
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

    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DVCOMMISVR, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и CreateReplRecByTmp_CCX10(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=CCX10' ||
                           ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DVCOMMISVR) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;

  FUNCTION UpdateCCX10_PM(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2)
    RETURN NUMBER
  IS
    TYPE ObjID_t IS TABLE OF DGTSNCREC_DBT.T_RECORDID%TYPE;
    TYPE PmID_t IS TABLE OF D_CCX10_TMP.T_ID_CM_CCX10%TYPE;
    v_ObjIDs ObjID_t;
    v_PmID   PmID_t;
  BEGIN
    SELECT CCX10.T_ID_CM_CCX10, GTSNCREC.T_RECORDID
      BULK COLLECT INTO v_PmID, v_ObjIDs
      FROM dgtsncrec_dbt gtsncrec,
           dgtcode_dbt gtcode,
           dgtobject_dbt gtobject,
           D_CCX10_TMP CCX10
     WHERE gtsncrec.t_seanceid = p_SeanceID
       AND GTCODE.T_OBJECTKIND = RSB_GTFN.RG_DVCOMMISVR
       AND GTCODE.T_OBJECTID = GTSNCREC.T_OBJECTID
       AND GTOBJECT.T_OBJECTID = GTSNCREC.T_OBJECTID
       AND GTCODE.T_OBJECTCODE = CCX10.T_DOCNO || '_' || CCX10.T_EXTCODE || '_' || TO_CHAR(CCX10.T_COMMISTYPE) || '_' || CCX10.T_REPORTDATESTR;

    IF v_PmID.COUNT <> 0 THEN
      FORALL i IN v_PmID.FIRST .. v_PmID.LAST
        EXECUTE IMMEDIATE
          'UPDATE '||p_Synonim||'.CM_CCX10 CCX10
              SET ZRID = :v_ObjIDsi
            WHERE ID_CM_CCX10 = :v_PmIDi'
        USING IN v_ObjIDs(i), IN v_PmID(i);
    END IF;
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DVCOMMISVR, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и UpdateCCX10_PM(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=CCX10' ||
                           ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DVCOMMISVR) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;

  FUNCTION DelCCX17(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER
  IS
  BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE D_CCX17_TMP';
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DVNDEALCALC, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и DelCCX17(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=CCX17' ||
                           ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DVNDEALCALC) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;

  FUNCTION FillCCX17(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2, p_Beg_date IN DATE, p_End_date IN DATE)
    RETURN NUMBER
  IS
  BEGIN
    EXECUTE IMMEDIATE
      'INSERT INTO D_CCX17_TMP(
         T_ID_CM_CCX17
        ,T_ID_MB_REQUISITES
        ,T_REPORTDATE
        ,T_REPORTDATESTR
        ,T_TRADENO
        ,T_TRADEGROUP
        ,T_MARGIN
        ,T_KIND
        ,T_CODEDEAL
        ,T_OBJECTID
        )
        (SELECT
         T.ID_CM_CCX17
        ,NVL(T.ID_MB_REQUISITES, 0)
        ,NVL(T.REPORTDATE, TO_DATE(''01.01.0001'', ''DD.MM.YYYY''))
        ,DECODE(SUBSTR(to_char(T.REPORTDATE,''DD''), 1, 1), ''0'', '' '', '''') || TRIM(LEADING ''0'' FROM to_char(T.REPORTDATE, ''DD.MM.YYYY''))
        ,NVL(T.TRADENO, CHR(1))
        ,NVL(T.TRADEGROUP, CHR(1))
        ,NVL(T.VARM, 0)
        ,(DECODE (T.TRADEGROUP, ''S'', '|| ВалютныйСВОП_РСХБ ||','||ВнебиржФорвард_РСХБ||' ))
        ,( NVL(T.TRADENO, CHR(1)) || ''_'' ||DECODE(SUBSTR(to_char(T.REPORTDATE,''DD''), 1, 1), ''0'', '' '', '''') || TRIM(LEADING ''0'' FROM to_char(T.REPORTDATE, ''DD.MM.YYYY'')) ||''_i'' )
        ,0
           FROM '||p_Synonim||'.CM_CCX17 T
          WHERE T.ID_MB_REQUISITES IN (SELECT ID_MB_REQUISITES FROM '||p_Synonim||'.PROCESSING_ACTUAL WHERE FILE_TYPE = ''CCX17'' AND TRADE_DATE BETWEEN :p_Beg_date AND :p_End_date)
            AND T.ZRID = 0
        )'
    USING IN p_Beg_date, IN p_End_date;
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DVNDEALCALC, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и FillCCX17, ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=CCX17' || ' ' || TO_CHAR(p_Beg_date) || ' ' || TO_CHAR(p_End_date) || ' ' || p_Synonim || ' ' ||
                           ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DVNDEALCALC) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;

  FUNCTION UpdateObjCCX17(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER
  IS
    TYPE ObjID_t  IS TABLE OF DGTCODE_DBT.T_OBJECTID%TYPE;
    TYPE VarmID_t IS TABLE OF D_CCX17_TMP.T_ID_CM_CCX17%TYPE;
    v_ObjIDs  ObjID_t;
    v_VarmID  VarmID_t;
  BEGIN
    SELECT GTCODE.T_OBJECTID, CCX17.t_ID_CM_CCX17
      BULK COLLECT INTO v_ObjIDs, v_VarmID
      FROM DGTCODE_DBT GTCODE, D_CCX17_TMP CCX17
     WHERE GTCODE.T_APPLICATIONID IN (RSB_GTFN.GT_PAYMENTS_VR, RSB_GTFN.GT_VARM)
       AND GTCODE.T_OBJECTKIND IN (RSB_GTFN.RG_DVNDEALCALC)
       AND GTCODE.T_OBJECTCODE = CCX17.T_CODEDEAL;
    IF v_VarmID.COUNT <> 0 THEN
      FORALL i IN v_VarmID.FIRST .. v_VarmID.LAST
        UPDATE d_CCX17_TMP CCX17
           SET CCX17.t_ObjectID = v_ObjIDs (i)
         WHERE CCX17.t_ID_CM_CCX17 = v_VarmID (i);
     END IF;
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DVNDEALCALC, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и UpdateObjCCX17(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=CCX17' ||
                           ', ObjectKind=' ||  TO_CHAR(RSB_GTFN.RG_DVNDEALCALC) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;

  FUNCTION UpdateCCX17_PM(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2)
    RETURN NUMBER
  IS
    TYPE ObjID_t IS TABLE OF DGTSNCREC_DBT.T_RECORDID%TYPE;
    TYPE PmID_t IS TABLE OF D_CCX17_TMP.T_ID_CM_CCX17%TYPE;
    v_ObjIDs ObjID_t;
    v_PmID   PmID_t;
  BEGIN
    SELECT CCX17.T_ID_CM_CCX17, GTSNCREC.T_RECORDID
      BULK COLLECT INTO v_PmID, v_ObjIDs
      FROM dgtsncrec_dbt gtsncrec,
           dgtcode_dbt gtcode,
           dgtobject_dbt gtobject,
           D_CCX17_TMP CCX17
     WHERE gtsncrec.t_seanceid = p_SeanceID
       AND GTCODE.T_OBJECTKIND = RSB_GTFN.RG_DVNDEALCALC
       AND GTCODE.T_OBJECTID = GTSNCREC.T_OBJECTID
       AND GTOBJECT.T_OBJECTID = GTSNCREC.T_OBJECTID
       AND GTCODE.T_OBJECTCODE = CCX17.T_CODEDEAL;

    IF v_PmID.COUNT <> 0 THEN
      FORALL i IN v_PmID.FIRST .. v_PmID.LAST
        EXECUTE IMMEDIATE
          'UPDATE '||p_Synonim||'.CM_CCX17 CCX17
              SET ZRID = :v_ObjIDsi
            WHERE ID_CM_CCX17 = :v_PmIDi'
        USING IN v_ObjIDs(i), IN v_PmID(i);
    END IF;
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DVNDEALCALC, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и UpdateCCX17_PM(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=CCX17' ||
                           ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DVNDEALCALC) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;

  --Заполнение и сохранение параметров сделки
  FUNCTION PutMarginInfo(p_Prm IN OUT NOCOPY PRMREC_t, p_Varm IN OUT NOCOPY DATAVARM_t, p_ErrMes IN OUT NOCOPY VARCHAR2) RETURN NUMBER
  IS
    v_stat         NUMBER(5) := 0;
    v_RG_ObjKind   NUMBER(5) := 0;
    v_RG_ObjCode   VARCHAR2(25) := CHR(1);
  BEGIN

    v_stat := RSI_GT.InitRec(RSB_GTFN.RG_DVNDEALCALC,
                             'Расчеты по ПФИ для внебирж. сделки № ' || p_Varm.TradeNo || ' на ' || p_Varm.ReportDateStr,
                             0, --проверять существование объекта в системе по коду
                             p_Varm.CodeDeal, --код объекта
                             0,--p_Varm.ClientID, --ID клиента
                             p_ErrMes);

    IF v_stat = 0 THEN

        RSI_GT.SetParmByName('RGDVNDL_KIND',    p_Varm.Kind);
        RSI_GT.SetParmByName('RGDVNDL_EXTCODE', p_Varm.TradeNo);
        RSI_GT.SetParmByName('RGDVNDL_DATE',    p_Varm.ReportDate);
        RSI_GT.SetParmByName('RGDVNDL_MARGIN' , p_Varm.Margin);

      --проверка ошибок после добавления параметров
      IF RSI_GT.GetLastError <> CHR(1) THEN
        v_stat := 1;
        p_ErrMes := RSI_GT.GetLastError;
      END IF;
    END IF;

    RETURN v_stat;
  END;

  --Обработка записей по данным буферной таблицы
  PROCEDURE ProcessReplRecByTmp_CCX17(p_Prm IN OUT NOCOPY PRMREC_t)
  IS
    v_Varm    DATAVARM_t;
    v_stat    NUMBER(5) := 0;
    v_ErrMes  VARCHAR2(1000) := CHR(1);
    v_WarnMes VARCHAR2(1000) := CHR(1);
  BEGIN
    --инициализация глобального лога для накопления записей для вставки
    RSB_GTLOG.Init(BATCH_SIZE, p_Prm.SeanceID, RSB_GTFN.RG_DVNDEALCALC);

    --инициализация глобального накопителя объектов ЗР для вставки
    v_stat := RSI_GT.Init(RSI_GT.DIRECT_INSERT_YES, p_Prm.SeanceID, p_Prm.Source_Code, RSI_GT.GTCODE_RECEIVE, v_ErrMes);
    IF v_stat <> 0 THEN
      RSB_GTLOG.Error(v_ErrMes);
    END IF;

    IF v_stat = 0 THEN
      FOR cData IN (SELECT t.*
                      FROM D_CCX17_TMP t
                     ORDER BY t.T_REPORTDATE, t.T_ID_CM_CCX17
                   )
      LOOP
        v_ErrMes  := CHR(1);
        v_WarnMes := CHR(1);

        v_Varm.Kind          := cData.t_Kind;
        v_Varm.TradeNo       := cData.t_TradeNo;
        v_Varm.ReportDate    := cData.t_ReportDate;
        v_Varm.ReportDateStr := cData.t_ReportDateStr;
        v_Varm.CodeDeal      := cData.t_CodeDeal;
        v_Varm.Margin        := cData.t_Margin;

        IF cData.t_ObjectID > 0 THEN
          v_ErrMes := 'Ошибка при загрузке сделки "' || v_Varm.CodeDeal || '"' || CHR(10) || 'Запись репликации с действием создать для объекта ' || TO_CHAR(cData.t_ObjectID ) || ' уже передавалась в RS-Bank.' || CHR(10) || 'Повторная репликация невозможна.';
        END IF;

        IF v_ErrMes = CHR(1) THEN
          IF PutMarginInfo(p_Prm, v_Varm, v_ErrMes) = 0 THEN
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
    RSB_GTLOG.DirectInsMsg(p_Prm.SeanceID, RSB_GTFN.RG_DVNDEALCALC, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка при обработке данных в ф-и ProcessReplRecByTmp_CCX17(), ' ||
                           'SeanceID=' || TO_CHAR(p_Prm.SeanceID) || ', FileImport=CCX17' ||
                           ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DVNDEALCALC) ||
                           ', Source_Code=' || p_Prm.Source_Code || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
  END;

  FUNCTION CreateReplRecByTmp_CCX17(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER
  IS
    v_Prm PRMREC_t;
  BEGIN
    --инициализация используемых параметров
    v_Prm.SeanceID    := p_SeanceID;
    v_Prm.Source_Code := p_SourceCode;

    ProcessReplRecByTmp_CCX17(v_Prm);

    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DVNDEALCALC, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и CreateReplRecByTmp_CCX17(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=CCX17' ||
                           ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DVNDEALCALC) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;

  FUNCTION DelCCX4(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER
  IS
  BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE D_CCX4_TMP';
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DLITOGCLVR, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и DelCCX4(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=CCX04' ||
                           ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DLITOGCLVR) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;

  FUNCTION FillCCX4(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2, p_Beg_date IN DATE, p_End_date IN DATE) --В РСХБ собираем информацию только по CCX04, на CCX4P не смотрим
    RETURN NUMBER
  IS
  BEGIN
    EXECUTE IMMEDIATE
      'INSERT INTO D_CCX4_TMP(
         T_ID_CM_CCX4
        ,T_ID_MB_REQUISITES
        ,T_REPORTDATE
        ,T_REPORTDATESTR
        ,T_EXTCODE
        ,T_CURRENCYID
        ,T_NETTOSUM
        ,T_DOCTYPE
        ,T_OBJECTID
        )
        (SELECT
         T.ID_CM_CCX04
        ,NVL(T.ID_MB_REQUISITES, 0)
        ,NVL(T.REPORTDATE, TO_DATE(''01.01.0001'', ''DD.MM.YYYY''))
        ,DECODE(SUBSTR(to_char(NVL(T.REPORTDATE, TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')),''dd''), 1, 1), ''0'', '' '', '''') || TRIM(LEADING ''0'' FROM to_char(NVL(T.REPORTDATE, TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')), ''dd.mm.yyyy''))
        ,NVL(T.EXTSETTLECODE, CHR(1))
        ,NVL(T.CURRENCYID, CHR(1))
        ,NVL(T.NETTOSUM, 0)
        ,''CCX04''
        ,0
           FROM '||p_Synonim||'.CM_CCX04 T, DDL_EXTSETTLECODE_DBT EXT
          WHERE T.ID_MB_REQUISITES IN (SELECT ID_MB_REQUISITES FROM '||p_Synonim||'.PROCESSING_ACTUAL WHERE FILE_TYPE = ''CCX04'' AND TRADE_DATE BETWEEN :p_Beg_date AND :p_End_date)
            AND T.ZRID = 0
            AND EXT.T_SETTLECODE (+) = NVL(T.EXTSETTLECODE, CHR(1))
            AND (    (     NVL(EXT.T_MARKETKIND, 0) = :IsCurrency
                       AND NVL(EXT.T_CODETYPE, 0)   = :IsClient
                     )
                  OR NVL(T.EXTSETTLECODE, CHR(1)) IN (''16791'', ''16818'', ''16820'', ''16821'', ''16822'', ''16823'', ''16824'', ''16825'', ''17957'', ''16826'')
                )
        )'
    USING IN p_Beg_date, IN p_End_date, IN RSB_SECUR.DV_MARKETKIND_CURRENCY, IN RSB_DERIVATIVES.ALG_SP_MONEY_SOURCE_CLIENT;

    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DLITOGCLVR, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и FillCCX4(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=CCX04' ||
                           ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DLITOGCLVR) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;

  FUNCTION UpdateObjCCX4(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER
  IS
    TYPE ObjID_t IS TABLE OF DGTCODE_DBT.T_OBJECTID%TYPE;
    TYPE SemID_t IS TABLE OF D_CCX4_TMP.T_ID_CM_CCX4%TYPE;
    v_ObjIDs ObjID_t;
    v_SemID  SemID_t;
  BEGIN
    SELECT GTCODE.T_OBJECTID, CCX4.t_ID_CM_CCX4
      BULK COLLECT INTO v_ObjIDs, v_semID
      FROM DGTCODE_DBT GTCODE, D_CCX4_TMP CCX4
     WHERE GTCODE.T_APPLICATIONID IN (RSB_GTFN.GT_PAYMENTS_VR, RSB_GTFN.GT_ICVR)
       AND GTCODE.T_OBJECTKIND = RSB_GTFN.RG_DLITOGCLVR
       AND GTCODE.T_OBJECTCODE = CCX4.T_DOCTYPE || '_' || CCX4.T_EXTCODE || '_' || CCX4.T_CURRENCYID || '_' || CCX4.T_REPORTDATESTR;
    IF v_semID.COUNT <> 0 THEN
      FORALL i IN v_semID.FIRST .. v_semID.LAST
        UPDATE D_CCX4_TMP CCX4
           SET CCX4.t_ObjectID = v_ObjIDs (i)
         WHERE CCX4.t_ID_CM_CCX4 = v_semID (i)
           AND CCX4.T_DOCTYPE = 'CCX04';
     END IF;
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DLITOGCLVR, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и UpdateObjCCX4(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=CCX04' ||
                           ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DLITOGCLVR) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;

  --Заполнение и сохранение параметров нетто-ТО
  FUNCTION PutItogClInfoPM(p_ItogCl IN OUT NOCOPY DATAITOGCL_t, p_ErrMes IN OUT NOCOPY VARCHAR2) RETURN NUMBER
  IS
    v_stat NUMBER(5) := 0;
  BEGIN

    v_stat := RSI_GT.InitRec(RSB_GTFN.RG_DLITOGCLVR,
                             'Итоговые нетто-требования и нетто-обязательства на ' || p_ItogCl.CurrencyID || ' c расчетным кодом ' || p_ItogCl.ExtCode || ' на ' || p_ItogCl.ReportDateStr, --Наименование объекта
                             0, --проверять существование объекта в системе по коду
                             p_ItogCl.DocType || '_' || p_ItogCl.ExtCode || '_' || p_ItogCl.CurrencyID || '_' || p_ItogCl.ReportDateStr, --код объекта
                             0, --ID клиента
                             p_ErrMes);

    IF v_stat = 0 THEN
      RSI_GT.SetParmByName('RGDLICVR_TYPE',     p_ItogCl.DocType);
      RSI_GT.SetParmByName('RGDLICVR_DATE',     p_ItogCl.ReportDate);
      RSI_GT.SetParmByName('RGDLICVR_EXTCODE',  p_ItogCl.ExtCode);
      RSI_GT.SetParmByName('RGDLICVR_CURRENCY', p_ItogCl.CurrencyID);
      RSI_GT.SetParmByName('RGDLICVR_NETTOSUM', p_ItogCl.NettoSum);

      --проверка ошибок после добавления параметров
      IF RSI_GT.GetLastError <> CHR(1) THEN
        v_stat := 1;
        p_ErrMes := RSI_GT.GetLastError;
      END IF;
    END IF;

    RETURN v_stat;
  END;

  --Создание объектов записей репликаций по данным временной таблицы CCX4
  FUNCTION CreateReplRecByTmp_CCX4(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER
  IS
    v_ItogCl  DATAITOGCL_t;
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
      FOR cData IN (SELECT t.T_DOCTYPE, t.T_REPORTDATE, t.T_REPORTDATESTR, t.T_EXTCODE, t.T_CURRENCYID, t.T_NETTOSUM, t.t_OBJECTID
                      FROM D_CCX4_TMP t
                  GROUP BY t.T_ID_MB_REQUISITES, t.T_DOCTYPE, t.T_REPORTDATE, t.T_REPORTDATESTR, t.T_EXTCODE, t.T_CURRENCYID, t.T_NETTOSUM, t.t_OBJECTID
                  ORDER BY t.T_REPORTDATE, t.T_DOCTYPE
                   )
      LOOP
        v_ErrMes  := CHR(1);
        v_WarnMes := CHR(1);

        v_ItogCl.DocType       := cData.t_DocType;
        v_ItogCl.ReportDate    := cData.t_ReportDate;
        v_ItogCl.ReportDateStr := cData.t_ReportDateStr;
        v_ItogCl.ExtCode       := cData.t_ExtCode;
        v_ItogCl.CurrencyID    := cData.t_CurrencyID;
        v_ItogCl.NettoSum      := cData.t_NettoSum;

        v_ErrMes  := CHR(1);
        v_WarnMes := CHR(1);

        IF cData.t_ObjectID > 0 THEN
          v_ErrMes := 'Ошибка при загрузке итоговых нетто-требований и нетто-обязательств "' || v_ItogCl.DocType || '_' || v_ItogCl.ExtCode || '_' || v_ItogCl.CurrencyID || '_' || v_ItogCl.ReportDateStr || '"' || CHR(10) ||
                      'Запись репликации с действием создать для объекта ' || TO_CHAR(cData.t_ObjectID) || ' уже передавалась в RS-Bank.' || CHR(10) || 'Повторная репликация невозможна.';
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
                           'Произошла непредвиденная ошибка в ф-и CreateReplRecByTmp_CCX4(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=CCX04' ||
                           ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DLITOGCLVR) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;

  FUNCTION UpdateCCX4_PM(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2)
    RETURN NUMBER
  IS
    TYPE ObjID_t IS TABLE OF DGTSNCREC_DBT.T_RECORDID%TYPE;
    TYPE PmID_t IS TABLE OF D_CCX4_TMP.T_ID_CM_CCX4%TYPE;
    v_ObjIDs ObjID_t;
    v_PmID   PmID_t;
  BEGIN
    SELECT CCX4.T_ID_CM_CCX4, GTSNCREC.T_RECORDID
      BULK COLLECT INTO v_PmID, v_ObjIDs
      FROM dgtsncrec_dbt gtsncrec,
           dgtcode_dbt gtcode,
           dgtobject_dbt gtobject,
           D_CCX4_TMP CCX4
     WHERE gtsncrec.t_seanceid = p_SeanceID
       AND GTCODE.T_OBJECTKIND = RSB_GTFN.RG_DLITOGCLVR
       AND GTCODE.T_OBJECTID = GTSNCREC.T_OBJECTID
       AND GTOBJECT.T_OBJECTID = GTSNCREC.T_OBJECTID
       AND GTCODE.T_OBJECTCODE = CCX4.T_DOCTYPE || '_' || CCX4.T_EXTCODE || '_' || CCX4.T_CURRENCYID || '_' || CCX4.T_REPORTDATESTR;

    IF v_PmID.COUNT <> 0 THEN
      FORALL i IN v_PmID.FIRST .. v_PmID.LAST
        EXECUTE IMMEDIATE --Раз записи пока берем только из CCX04, то и обновляем только её
          'UPDATE '||p_Synonim||'.CM_CCX04 CCX04
              SET ZRID = :v_ObjIDsi
            WHERE ID_CM_CCX04 = :v_PmIDi'
        USING IN v_ObjIDs(i), IN v_PmID(i);
    END IF;
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_DLITOGCLVR, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и UpdateCCX4_PM(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=CCX04' ||
                           ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_DLITOGCLVR) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;

  FUNCTION DelCCX99(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_IsVR IN NUMBER)
    RETURN NUMBER
  IS
    v_Kind NUMBER(5);
  BEGIN
    IF(p_IsVR = 1) THEN
      v_Kind := RSB_GTFN.RG_MONEYMOTION_CM;
    ELSE
      v_Kind := RSB_GTFN.RG_MONEYMOTION;
    END IF;
    EXECUTE IMMEDIATE 'TRUNCATE TABLE D_CCX99_TMP';
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, v_Kind , RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и DelCCX99(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=CCX99' ||
                           ', ObjectKind=' || TO_CHAR(v_Kind) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;

  FUNCTION FillCCX99(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2, p_Beg_date IN DATE, p_End_date IN DATE)
    RETURN NUMBER
  IS
  BEGIN
    EXECUTE IMMEDIATE
      'INSERT INTO D_CCX99_TMP(
         T_ID_CM_CCX99
        ,T_ID_MB_REQUISITES
        ,T_EXTSETTLECODE
        ,T_EXTSETTLECODEID
        ,T_ACCOUNT
        ,T_CODETYPE
        ,T_CODETYPEINT
        ,T_DEBIT
        ,T_CREDIT
        ,T_PAY_ACC
        ,T_REC_ACC
        ,T_NUMBER
        ,T_CODEDEAL
        ,T_DATE
        ,T_DATESTR
        ,T_PURPOSE_PAYMENT
        ,T_COR_ACC
        ,T_OBJECTID
        ,T_OTHERKLIRACC
        )
        (SELECT
         T.ID_CM_CCX99
        ,NVL(T.ID_MB_REQUISITES, 0)
        ,NVL(TO_CHAR(T.ID), CHR(1))
        ,NVL(TO_NUMBER(T.ID), 0)
        ,NVL(T.ACCOUNT, CHR(1))
        ,NVL(TO_CHAR(T.CODETYPE), CHR(1))
        ,NVL(TO_NUMBER(T.CODETYPE), 0)
        ,NVL(T.DEBIT, 0)
        ,NVL(T.CREDIT, 0)
        ,NVL(T.PAY_ACC, CHR(1))
        ,NVL(T.REC_ACC, CHR(1))
        ,NVL(TO_CHAR(T.NUMBER_DOCUM), CHR(1))
        ,NVL(TO_CHAR(T.NUMBER_DOCUM), CHR(1))
        ,NVL(T.DATE_TRANSACTION, TO_DATE(''01.01.0001'', ''DD.MM.YYYY''))
        ,DECODE(SUBSTR(to_char(T.DATE_TRANSACTION,''DD''), 1, 1), ''0'', '' '', '''') || TRIM(LEADING ''0'' FROM to_char(T.DATE_TRANSACTION, ''DD.MM.YYYY''))
        ,NVL(T.PURPOSE_PAYMENT, CHR(1))
        ,NVL(T.COR_ACC, CHR(1))
        ,0
        ,(CASE WHEN NVL(TO_NUMBER(T.CODETYPE), 0) = 20 THEN NVL(T2.ACCOUNT, CHR(1)) ELSE CHR(1) END)
           FROM '||p_Synonim||'.CM_CCX99 T
           LEFT JOIN '||p_Synonim||'.CM_CCX99 T2
           ON NVL(TO_NUMBER(T2.CODETYPE), 0) = 20
             AND T2.DEBIT > 0
             AND ( T2.ID IN ( 1356,13385,6263,10120,10121,10122,10123,10124,10125,10126,11140,485,17958,11139)
               OR  TO_CHAR(T2.ID) IN (SELECT SC.T_SETTLECODE FROM DDL_EXTSETTLECODE_DBT SC WHERE SC.T_MARKETKIND = :IsStock))
             AND T.CREDIT = T2.DEBIT
             AND T.DATE_TRANSACTION = T2.DATE_TRANSACTION
             AND T.CODETYPE = T2.CODETYPE
             AND SUBSTR(T.PURPOSE_PAYMENT, 1, LENGTH(T.PURPOSE_PAYMENT)-1) = SUBSTR(T2.PURPOSE_PAYMENT, 1, LENGTH(T2.PURPOSE_PAYMENT)-1)
             AND T2.ID_MB_REQUISITES IN (SELECT ID_MB_REQUISITES FROM '||p_Synonim||'.PROCESSING_ACTUAL WHERE FILE_TYPE = ''CCX99'' AND TRADE_DATE BETWEEN :p_Beg_date AND :p_End_date)
          WHERE T.ID_MB_REQUISITES IN (SELECT ID_MB_REQUISITES FROM '||p_Synonim||'.PROCESSING_ACTUAL WHERE FILE_TYPE = ''CCX99'' AND TRADE_DATE BETWEEN :p_Beg_date AND :p_End_date)
            AND T.ZRID = 0
             AND ( T.ID IN ( 1356,13385,6263,10120,10121,10122,10123,10124,10125,10126,11140,17958,11139)
               OR  TO_CHAR(T.ID) IN (SELECT SC.T_SETTLECODE FROM DDL_EXTSETTLECODE_DBT SC WHERE SC.T_MARKETKIND = :IsStock))
            AND NVL(TO_NUMBER(T.CODETYPE), 0) IN(916,918,812,20)
            AND ( (TO_NUMBER(T.CODETYPE) = 916 AND SUBSTR(T.PAY_ACC,1,5) = ''30411'')
                  OR (TO_NUMBER(T.CODETYPE) IN (918,812) AND SUBSTR(T.REC_ACC,1,5) = ''30411'')
                  OR (NVL(TO_NUMBER(T.CODETYPE), 0) = 20 AND T.CREDIT > 0))
        )'
    USING IN RSB_SECUR.DV_MARKETKIND_STOCK, IN p_Beg_date, IN p_End_date,IN p_Beg_date, IN p_End_date, IN RSB_SECUR.DV_MARKETKIND_STOCK;
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_MONEYMOTION, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и FillCCX99, ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=CCX99' || ' ' || TO_CHAR(p_Beg_date) || ' ' || TO_CHAR(p_End_date) || ' ' || p_Synonim || ' ' ||
                           ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_MONEYMOTION) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;

  FUNCTION FillCCX99VR(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2, p_Beg_date IN DATE, p_End_date IN DATE)
    RETURN NUMBER
  IS
  BEGIN
    EXECUTE IMMEDIATE
      'INSERT INTO D_CCX99_TMP(
         T_ID_CM_CCX99
        ,T_ID_MB_REQUISITES
        ,T_EXTSETTLECODE
        ,T_EXTSETTLECODEID
        ,T_ACCOUNT
        ,T_CODETYPE
        ,T_CODETYPEINT
        ,T_DEBIT
        ,T_CREDIT
        ,T_PAY_ACC
        ,T_REC_ACC
        ,T_NUMBER
        ,T_CODEDEAL
        ,T_DATE
        ,T_DATESTR
        ,T_PURPOSE_PAYMENT
        ,T_COR_ACC
        ,T_OBJECTID
        ,T_OTHERKLIRACC
        )
        (SELECT
         T.ID_CM_CCX99
        ,NVL(T.ID_MB_REQUISITES, 0)
        ,NVL(TO_CHAR(T.ID), CHR(1))
        ,NVL(TO_NUMBER(T.ID), 0)
        ,NVL(T.ACCOUNT, CHR(1))
        ,NVL(TO_CHAR(T.CODETYPE), CHR(1))
        ,NVL(TO_NUMBER(T.CODETYPE), 0)
        ,NVL(T.DEBIT, 0)
        ,NVL(T.CREDIT, 0)
        ,NVL(T.PAY_ACC, CHR(1))
        ,NVL(T.REC_ACC, CHR(1))
        ,NVL(TO_CHAR(T.NUMBER_DOCUM), CHR(1))
        ,NVL(TO_CHAR(T.NUMBER_DOCUM), CHR(1))
        ,NVL(T.DATE_TRANSACTION, TO_DATE(''01.01.0001'', ''DD.MM.YYYY''))
        ,DECODE(SUBSTR(to_char(T.DATE_TRANSACTION,''DD''), 1, 1), ''0'', '' '', '''') || TRIM(LEADING ''0'' FROM to_char(T.DATE_TRANSACTION, ''DD.MM.YYYY''))
        ,NVL(T.PURPOSE_PAYMENT, CHR(1))
        ,NVL(T.COR_ACC, CHR(1))
        ,0
        ,(CASE WHEN NVL(TO_NUMBER(T.CODETYPE), 0) IN(20, 813) THEN NVL(T2.ACCOUNT, CHR(1)) ELSE CHR(1) END)
           FROM '||p_Synonim||'.CM_CCX99 T
           LEFT JOIN '||p_Synonim||'.CM_CCX99 T2
           ON (
               (NVL(TO_NUMBER(T2.CODETYPE), 0)  IN (20, 813)
               AND T2.DEBIT > 0
               AND T.DEBIT = T2.DEBIT)
              OR (NVL(TO_NUMBER(T2.CODETYPE), 0)  IN (813)
               AND T2.CREDIT > 0
               AND T.CREDIT = T2.CREDIT
               AND EXISTS (SELECT 1 FROM '||p_Synonim||'.MB_REQUISITES REQ WHERE REQ.ID_MB_REQUISITES = T2.ID_MB_REQUISITES AND SUBSTR(REQ.FILE_NAME,1,2) = ''MB'')
               )
              )
             AND T.DATE_TRANSACTION = T2.DATE_TRANSACTION
             AND T.CODETYPE = T2.CODETYPE
             AND SUBSTR(T.PURPOSE_PAYMENT, 1, LENGTH(T.PURPOSE_PAYMENT)-1) = SUBSTR(T2.PURPOSE_PAYMENT, 1, LENGTH(T2.PURPOSE_PAYMENT)-1)
             AND T2.ID_MB_REQUISITES IN (SELECT ID_MB_REQUISITES FROM '||p_Synonim||'.PROCESSING_ACTUAL WHERE FILE_TYPE = ''CCX99'' AND TRADE_DATE BETWEEN :p_Beg_date AND :p_End_date)
          WHERE T.ID_MB_REQUISITES IN (SELECT ID_MB_REQUISITES FROM '||p_Synonim||'.PROCESSING_ACTUAL WHERE FILE_TYPE = ''CCX99'' AND TRADE_DATE BETWEEN :p_Beg_date AND :p_End_date)
            AND T.ZRID = 0
            AND ( T.ID IN ( 16791,16818,16820,16821,16822,16823,16824,16825,17957,16826)
               OR  TO_CHAR(T.ID) IN (SELECT SC.T_SETTLECODE FROM DDL_EXTSETTLECODE_DBT SC WHERE SC.T_MARKETKIND = :IsCurrency AND SC.T_CODETYPE = :IsClient))
            AND NVL(TO_NUMBER(T.CODETYPE), 0) IN(20, 812, 813, 916, 918)
            AND EXISTS (SELECT 1 FROM '||p_Synonim||'.MB_REQUISITES REQ WHERE REQ.ID_MB_REQUISITES = T.ID_MB_REQUISITES AND SUBSTR(REQ.FILE_NAME,1,2) = ''MB'')
            AND (T.CODETYPE NOT IN (812, 916) OR (SUBSTR(T.ACCOUNT, 6,3) <>''810''))
        )'
    USING IN p_Beg_date, IN p_End_date, IN p_Beg_date, IN p_End_date, IN RSB_SECUR.DV_MARKETKIND_CURRENCY, IN RSB_DERIVATIVES.ALG_SP_MONEY_SOURCE_CLIENT;
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, RSB_GTFN.RG_MONEYMOTION_CM, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и FillCCX99VR, ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=CCX99' || ' ' || TO_CHAR(p_Beg_date) || ' ' || TO_CHAR(p_End_date) || ' ' || p_Synonim || ' ' ||
                           ', ObjectKind=' || TO_CHAR(RSB_GTFN.RG_MONEYMOTION_CM) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;

  FUNCTION UpdateObjCCX99(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_IsVR IN NUMBER)
    RETURN NUMBER
  IS
    TYPE ObjID_t  IS TABLE OF DGTCODE_DBT.T_OBJECTID%TYPE;
    TYPE MoneyMotion_t IS TABLE OF D_CCX99_TMP.T_ID_CM_CCX99%TYPE;
    v_ObjIDs  ObjID_t;
    p_MoneyMotionID  MoneyMotion_t;
    v_Kind NUMBER(5);
  BEGIN
    IF(p_IsVR = 1) THEN
      v_Kind := RSB_GTFN.RG_MONEYMOTION_CM;
    ELSE
      v_Kind := RSB_GTFN.RG_MONEYMOTION;
    END IF;
    SELECT GTCODE.T_OBJECTID, CCX99.t_ID_CM_CCX99
      BULK COLLECT INTO v_ObjIDs, p_MoneyMotionID
      FROM DGTCODE_DBT GTCODE, D_CCX99_TMP CCX99
     WHERE GTCODE.T_APPLICATIONID IN (RSB_GTFN.GT_PAYMENTS_VR, RSB_GTFN.GT_MMVB3, RSB_GTFN.GT_CX99)
       AND GTCODE.T_OBJECTKIND IN (v_Kind)
       AND GTCODE.T_OBJECTCODE = CCX99.T_CODEDEAL;
    IF p_MoneyMotionID.COUNT <> 0 THEN
      FORALL i IN p_MoneyMotionID.FIRST .. p_MoneyMotionID.LAST
        UPDATE d_CCX99_TMP CCX99
           SET CCX99.t_ObjectID = v_ObjIDs (i)
         WHERE CCX99.t_ID_CM_CCX99 = p_MoneyMotionID (i);
     END IF;
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, v_Kind, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и UpdateObjCCX99(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=CCX99' ||
                           ', ObjectKind=' ||  TO_CHAR(v_Kind) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;

  FUNCTION UpdateCCX99_PM(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2, p_IsVR IN NUMBER)
    RETURN NUMBER
  IS
    TYPE ObjID_t IS TABLE OF DGTSNCREC_DBT.T_RECORDID%TYPE;
    TYPE PmID_t IS TABLE OF D_CCX99_TMP.T_ID_CM_CCX99%TYPE;
    v_ObjIDs ObjID_t;
    v_PmID   PmID_t;
    v_Kind NUMBER(5);
  BEGIN
    IF(p_IsVR = 1) THEN
      v_Kind := RSB_GTFN.RG_MONEYMOTION_CM;
    ELSE
      v_Kind := RSB_GTFN.RG_MONEYMOTION;
    END IF;
    SELECT CCX99.T_ID_CM_CCX99, GTSNCREC.T_RECORDID
      BULK COLLECT INTO v_PmID, v_ObjIDs
      FROM dgtsncrec_dbt gtsncrec,
           dgtcode_dbt gtcode,
           dgtobject_dbt gtobject,
           D_CCX99_TMP CCX99
     WHERE gtsncrec.t_seanceid = p_SeanceID
       AND GTCODE.T_OBJECTKIND = v_Kind
       AND GTCODE.T_OBJECTID = GTSNCREC.T_OBJECTID
       AND GTOBJECT.T_OBJECTID = GTSNCREC.T_OBJECTID
       AND GTCODE.T_OBJECTCODE = CCX99.T_CODEDEAL;

    IF v_PmID.COUNT <> 0 THEN
      FORALL i IN v_PmID.FIRST .. v_PmID.LAST
        EXECUTE IMMEDIATE
          'UPDATE '||p_Synonim||'.CM_CCX99 CCX99
              SET ZRID = :v_ObjIDsi
            WHERE ID_CM_CCX99 = :v_PmIDi'
        USING IN v_ObjIDs(i), IN v_PmID(i);
    END IF;
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, v_Kind, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и UpdateCCX99_PM(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=CCX99' ||
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
                             'Движение денежных средств №' || p_MoneyMotion.Number_,--поправить для 9999
                             0, --проверять существование объекта в системе по коду
                             p_MoneyMotion.CodeDeal, --код объекта
                             0,--p_MoneyMotion.ClientID, --ID клиента
                             p_ErrMes);

    IF v_stat = 0 THEN

        RSI_GT.SetParmByName('CODETYPE',        p_MoneyMotion.CodeTypeInt);
        RSI_GT.SetParmByName('DEBIT',           p_MoneyMotion.Debit);
        RSI_GT.SetParmByName('CREDIT',          p_MoneyMotion.Credit);
        RSI_GT.SetParmByName('DATE' ,           p_MoneyMotion.Date_);
--        RSI_GT.SetParmByName('EXTCODEID',       p_MoneyMotion.ExtSettleCode); --нет EXTCODEID
        RSI_GT.SetParmByName('EXT_SETTLECODE',  p_MoneyMotion.ExtSettleCode);
        IF(p_MoneyMotion.CodeTypeInt = 9999)THEN
           RSI_GT.SetParmByName('NUMBER',        p_MoneyMotion.Number_);
           RSI_GT.SetParmByName('CURRENCYID',    p_MoneyMotion.CurrencyID);
           RSI_GT.SetParmByName('CLIENTCODE',    p_MoneyMotion.ClientCode);
        END IF;
        RSI_GT.SetParmByName('ACCOUNT',         p_MoneyMotion.Account);
        RSI_GT.SetParmByName('COR_ACC' ,        p_MoneyMotion.CorAcc);
        RSI_GT.SetParmByName('PAY_ACC',         p_MoneyMotion.PayAcc);
        RSI_GT.SetParmByName('REC_ACC' ,        p_MoneyMotion.RecAcc);
        RSI_GT.SetParmByName('PURPOSE_PAYMENT', p_MoneyMotion.PurposePayment);
        RSI_GT.SetParmByName('OTHERKLIRACC' ,   p_MoneyMotion.OtherKlirAcc);

      --проверка ошибок после добавления параметров
      IF RSI_GT.GetLastError <> CHR(1) THEN
        v_stat := 1;
        p_ErrMes := RSI_GT.GetLastError;
      END IF;
    END IF;

    RETURN v_stat;
  END;

  --Обработка записей по данным буферной таблицы
  PROCEDURE ProcessReplRecByTmp_CCX99(p_Prm IN OUT NOCOPY PRMREC_t)
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
                      FROM D_CCX99_TMP t
                     ORDER BY t.T_DATE, t.T_ID_CM_CCX99
                   )
      LOOP
        v_ErrMes  := CHR(1);
        v_WarnMes := CHR(1);

        v_MoneyMotion.Account       := cData.t_Account;
        v_MoneyMotion.CodeType      := cData.t_CodeType;
        v_MoneyMotion.CodeTypeInt   := cData.t_CodeTypeInt;
        v_MoneyMotion.Number_       := cData.t_Number;
        v_MoneyMotion.CodeDeal      := cData.t_CodeDeal;
        v_MoneyMotion.CorAcc        := cData.t_Cor_Acc;
        v_MoneyMotion.PayAcc        := cData.t_Pay_Acc;
        v_MoneyMotion.Debit         := cData.t_Debit;
        v_MoneyMotion.Credit        := cData.t_Credit;
        v_MoneyMotion.RecAcc        := cData.t_Rec_Acc;
        v_MoneyMotion.PurposePayment:= cData.t_Purpose_Payment;
        v_MoneyMotion.Date_         := cData.t_Date;
        v_MoneyMotion.DateStr       := cData.t_DateStr;
        v_MoneyMotion.ExtSettleCode := cData.t_ExtSettleCode;
        v_MoneyMotion.OtherKlirAcc  := cData.t_OtherKlirAcc;
        v_MoneyMotion.CurrencyId    := cData.t_Currency;
        v_MoneyMotion.ClientCode    := cData.t_ClientCode;

        IF cData.t_ObjectID > 0 THEN
          v_ErrMes := 'Ошибка при загрузке сделки "' || v_MoneyMotion.CodeDeal || '"' || CHR(10) || 'Запись репликации с действием создать для объекта ' || TO_CHAR(cData.t_ObjectID ) || ' уже передавалась в RS-Bank.' || CHR(10) || 'Повторная репликация невозможна.';
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
                           'Произошла непредвиденная ошибка при обработке данных в ф-и ProcessReplRecByTmp_CCX99(), ' ||
                           'SeanceID=' || TO_CHAR(p_Prm.SeanceID) || ', FileImport=CCX99' ||
                           ', ObjectKind=' || TO_CHAR(p_Prm.Kind) ||
                           ', Source_Code=' || p_Prm.Source_Code || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
  END;

  FUNCTION CreateReplRecByTmp_CCX99(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_IsVR IN NUMBER)
    RETURN NUMBER
  IS
    v_Prm PRMREC_t;
  BEGIN
    --инициализация используемых параметров
    v_Prm.SeanceID    := p_SeanceID;
    v_Prm.Source_Code := p_SourceCode;
    IF(p_IsVR = 1) THEN
      v_Prm.Kind := RSB_GTFN.RG_MONEYMOTION_CM;
    ELSE
      v_Prm.Kind := RSB_GTFN.RG_MONEYMOTION;
    END IF;

    ProcessReplRecByTmp_CCX99(v_Prm);

    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    RSB_GTLOG.DirectInsMsg(p_SeanceID, v_Prm.Kind, RSB_GTLOG.ISSUE_FAULT,
                           'Произошла непредвиденная ошибка в ф-и CreateReplRecByTmp_CCX99(), ' ||
                           'SeanceID=' || TO_CHAR(p_SeanceID) || ', FileImport=CCX99' ||
                           ', ObjectKind=' || TO_CHAR(v_Prm.Kind) ||
                           ', Source_Code=' || p_SourceCode || ', SessionID=' || TO_CHAR(USERENV('sessionid')) ||
                           ', SysDate=' || TO_CHAR(SYSDATE) || ', SQLERRM=' || SUBSTR(SQLERRM(SQLCODE), 1, 500));
    RETURN 1;
  END;


END RSB_GTIM_VR;
/