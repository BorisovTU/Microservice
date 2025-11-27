CREATE OR REPLACE PACKAGE BODY RSI_MMARK_MSFO
IS
   -- Получить название стандартной процедуры МСФО
   FUNCTION GetMSFOProc_def(
    ProcType NUMBER,
    OperationKind NUMBER := 0
   )
      RETURN VARCHAR2
   IS
      MSFOProc VARCHAR2 (512) := LoansConst.STR_EMPTY;
   BEGIN
      CASE (ProcType)
         WHEN MMarkConst.MM_MSFOPROC_CALCRPS THEN
            MSFOProc := 'RSI_MMARK_MSFO.CalcRPS';
         WHEN MMarkConst.MM_MSFOPROC_GETMLVLEX THEN
            MSFOProc := 'RSI_MMARK_MSFO.GetMlvlEx';
         WHEN MMarkConst.MM_MSFOPROC_GETASCTG THEN
            MSFOProc := 'RSI_MMARK_MSFO.GetASCtg';
         WHEN MMarkConst.MM_MSFOPROC_CALCDDA THEN
            MSFOProc := 'RSI_MMARK_MSFO.CalcDDA';
         WHEN MMarkConst.MM_MSFOPROC_CALCCOSTS THEN
            MSFOProc := 'RSI_MMARK_MSFO.CalcCosts';
         WHEN MMarkConst.MM_MSFOPROC_CALCEPS THEN
            MSFOProc := 'RSI_MMARK_MSFO.CalcEPS';
         WHEN MMarkConst.MM_MSFOPROC_CALCGBV THEN
            MSFOProc := 'RSI_MMARK_MSFO.CalcGBV';
         WHEN MMarkConst.MM_MSFOPROC_CALCAS THEN
            MSFOProc := 'RSI_MMARK_MSFO.CalcAS';
         WHEN MMarkConst.MM_MSFOPROC_CALCSS THEN
            MSFOProc := 'RSI_MMARK_MSFO.CalcSS';
         WHEN MMarkConst.MM_MSFOPROC_CALCASSESRES THEN
            MSFOProc := 'RSI_MMARK_MSFO.CalcAssesRes';
         WHEN MMarkConst.MM_MSFOPROC_CALCADJCOST THEN
            MSFOProc := 'RSI_MMARK_MSFO.CalcAdjCost';
         WHEN MMarkConst.MM_MSFOPROC_CALCADJAS THEN
            MSFOProc := 'RSI_MMARK_MSFO.CalcAdjAS';
         WHEN MMarkConst.MM_MSFOPROC_CALCADJSS THEN
            MSFOProc := 'RSI_MMARK_MSFO.CalcAdjSS';
         WHEN MMarkConst.MM_MSFOPROC_CALCADJINT THEN
            MSFOProc := 'RSI_MMARK_MSFO.CalcAdjInt';
         WHEN MMarkConst.MM_MSFOPROC_CALCFINRESFA THEN
            MSFOProc := 'RSI_MMARK_MSFO.CalcFinResFA';
         WHEN MMarkConst.MM_MSFOPROC_CALCOTHINT THEN
            MSFOProc := 'RSI_MMARK_MSFO.CalcOthInt';
         WHEN MMarkConst.MM_MSFOPROC_CALCOTHOPINT THEN
            MSFOProc := 'RSI_MMARK_MSFO.CalcOthOpInt';
         WHEN MMarkConst.MM_MSFOPROC_CALCFINRESFO THEN
            MSFOProc := 'RSI_MMARK_MSFO.CalcFinResFO';
         WHEN MMarkConst.MM_MSFOPROC_FILLXIRR THEN
            MSFOProc := 'RSI_MMARK_MSFO.FillXirr';
         WHEN MMarkConst.MM_MSFOPROC_TESTMARKET THEN
            MSFOProc := 'RSI_MMARK_MSFO.TestMarket';
         WHEN MMarkConst.MM_MSFOPROC_CALCPRIZE THEN
            MSFOProc := 'RSI_MMARK_MSFO.CalcPrize';
         WHEN MMarkConst.MM_MSFOPROC_CALCDISCOUNT THEN
            MSFOProc := 'RSI_MMARK_MSFO.CalcDiscount';
         WHEN MMarkConst.MM_MSFOPROC_CALCHEDGADJAM THEN
            MSFOProc := 'RSI_MMARK_MSFO.CalcHedgAdjAm';
         ELSE
            RETURN MSFOProc;
      END CASE;

      IF (MMark_UTL.IsBUY(OperationKind) > 0) THEN
         MSFOProc := MSFOProc || '_BUY_def';
      ELSIF (MMark_UTL.IsSALE(OperationKind) > 0) THEN
         MSFOProc := MSFOProc || '_SALE_def'; 
      ELSE
         MSFOProc := MSFOProc || '_def';
      END IF;

      RETURN MSFOProc;
   END GetMSFOProc_def;

   -- Получить путь до настроек процедуры МСФО
   FUNCTION GetMSFORegPath(
    ProcType NUMBER,
    OperationKind NUMBER := 0
  )
    RETURN VARCHAR2
   IS
      MSFORegPath    VARCHAR2(512) := 'MMARK\ПРОЦЕДУРЫ_МСФО';
   BEGIN
      CASE (ProcType)
         WHEN MMarkConst.MM_MSFOPROC_CALCRPS THEN
            MSFORegPath := MSFORegPath || '\РАСЧЕТ_РПС';
         WHEN MMarkConst.MM_MSFOPROC_GETMLVLEX THEN
            MSFORegPath := MSFORegPath || '\ПРЕВЫШЕНИЕ_УС';
         WHEN MMarkConst.MM_MSFOPROC_GETASCTG THEN
            MSFORegPath := MSFORegPath || '\КАТЕГОРИЯ_ОЦЕНКИ';
         WHEN MMarkConst.MM_MSFOPROC_CALCDDA THEN
            MSFORegPath := MSFORegPath || '\РАСЧЕТ_ОТСРОЧ_РАЗНИЦЫ';
         WHEN MMarkConst.MM_MSFOPROC_CALCCOSTS THEN
            MSFORegPath := MSFORegPath || '\РАСЧЕТ_ЗАТРАТ';
         WHEN MMarkConst.MM_MSFOPROC_CALCEPS THEN
            MSFORegPath := MSFORegPath || '\РАСЧЕТ_ЭПС';
         WHEN MMarkConst.MM_MSFOPROC_CALCGBV THEN
            MSFORegPath := MSFORegPath || '\РАСЧЕТ_ВБС';
         WHEN MMarkConst.MM_MSFOPROC_CALCAS THEN
            MSFORegPath := MSFORegPath || '\РАСЧЕТ_АС';
         WHEN MMarkConst.MM_MSFOPROC_CALCSS THEN
            MSFORegPath := MSFORegPath || '\РАСЧЕТ_СС';
         WHEN MMarkConst.MM_MSFOPROC_CALCASSESRES THEN
            MSFORegPath := MSFORegPath || '\РАСЧЕТ_ОЦЕН_РЕЗЕРВА';
         WHEN MMarkConst.MM_MSFOPROC_CALCADJCOST THEN
            MSFORegPath := MSFORegPath || '\РАСЧЕТ_КОРР_СТОИМОСТИ';
         WHEN MMarkConst.MM_MSFOPROC_CALCADJAS THEN
            MSFORegPath := MSFORegPath || '\РАСЧЕТ_КОРР_АС';
         WHEN MMarkConst.MM_MSFOPROC_CALCADJSS THEN
            MSFORegPath := MSFORegPath || '\РАСЧЕТ_КОРР_СС';
         WHEN MMarkConst.MM_MSFOPROC_CALCADJINT THEN
            MSFORegPath := MSFORegPath || '\РАСЧЕТ_КОРР_%ДОХОДА';
         WHEN MMarkConst.MM_MSFOPROC_CALCFINRESFA THEN
            MSFORegPath := MSFORegPath || '\РАСЧЕТ_ФИНРЕЗ_ФА';
         WHEN MMarkConst.MM_MSFOPROC_CALCOTHINT THEN
            MSFORegPath := MSFORegPath || '\РАСЧЕТ_ПРОЧИХ_%ДОХОДОВ';
         WHEN MMarkConst.MM_MSFOPROC_CALCOTHOPINT THEN
            MSFORegPath := MSFORegPath || '\РАСЧЕТ_ПРОЧИХ_ОПЕРДОХОДОВ';
         WHEN MMarkConst.MM_MSFOPROC_CALCFINRESFO THEN
            MSFORegPath := MSFORegPath || '\РАСЧЕТ_ФИНРЕЗ_ФО';
         WHEN MMarkConst.MM_MSFOPROC_FILLXIRR THEN
            MSFORegPath := MSFORegPath || '\ДЕНЕЖНЫЕ_ПОТОКИ';
         WHEN MMarkConst.MM_MSFOPROC_TESTMARKET THEN
            MSFORegPath := MSFORegPath || '\ТЕСТ_НА_РЫНОЧНОСТЬ';
         WHEN MMarkConst.MM_MSFOPROC_CALCPRIZE THEN
            MSFORegPath := MSFORegPath || '\РАСЧЕТ_ПРЕМИИ';
         WHEN MMarkConst.MM_MSFOPROC_CALCDISCOUNT THEN
            MSFORegPath := MSFORegPath || '\РАСЧЕТ_ДИСКОНТА';
         WHEN MMarkConst.MM_MSFOPROC_CALCHEDGADJAM THEN
            MSFORegPath := MSFORegPath || '\РАСЧЕТ_АМОРТИЗАЦИИ_КОР_ХЕДЖ';
         ELSE
            RETURN LoansConst.STR_EMPTY;
      END CASE;

      IF (MMark_UTL.IsBUY(OperationKind) > 0) THEN
         MSFORegPath := MSFORegPath || '\ПРИВЛЕЧЕНИЕ';
      ELSIF (MMark_UTL.IsSALE(OperationKind) > 0) THEN
         MSFORegPath := MSFORegPath || '\РАЗМЕЩЕНИЕ';
      END IF;

      RETURN MSFORegPath;
   END GetMSFORegPath;

   -- Получить путь до настроки ISSTDTPROC
   FUNCTION GetMSFORegISSTDTPROC(
      ProcType NUMBER,
      OperationKind NUMBER := 0
   )
      RETURN VARCHAR2
   IS
      MSFORegPath    VARCHAR2(512) := GetMSFORegPath(ProcType, OperationKind);
   BEGIN
      IF (MSFORegPath <> LoansConst.STR_EMPTY) THEN
         MSFORegPath := MSFORegPath || '\ISSTDTPROC';
      END IF;

      RETURN MSFORegPath;
   END GetMSFORegISSTDTPROC;

  -- Получить путь до настроки USERPROC
  FUNCTION GetMSFORegUSERPROC(
    ProcType NUMBER,
    OperationKind NUMBER := 0
  )
    RETURN VARCHAR2
  IS
    MSFORegPath    VARCHAR2(512) := GetMSFORegPath(ProcType, OperationKind);
  BEGIN
   IF (MSFORegPath <> LoansConst.STR_EMPTY) THEN
     MSFORegPath := MSFORegPath || '\USERPROC';
   END IF;
   RETURN MSFORegPath;
  END GetMSFORegUSERPROC;

  -- Получить название процедуры МСФО
  FUNCTION GetMSFOProc(
    ProcType NUMBER,
    OperationKind NUMBER := 0
  )
    RETURN VARCHAR2
  IS
    isStd          CHAR := CHR(0);
    MSFOProc       VARCHAR2 (512) := LoansConst.STR_EMPTY;
    MSFORegPath    VARCHAR2(512) := LoansConst.STR_EMPTY; 
  BEGIN
    MSFORegPath := GetMSFORegISSTDTPROC(ProcType, OperationKind);
    IF (MSFORegPath <> LoansConst.STR_EMPTY) THEN
        isStd := RSB_Common.GetRegFlagValue(MSFORegPath);
        IF (isStd = LoansConst.CHAR_EMPTY) THEN
            MSFORegPath := GetMSFORegUSERPROC(ProcType, OperationKind);
            IF (MSFORegPath <> LoansConst.STR_EMPTY) THEN
                MSFOProc := RSB_Common.GetRegStrValue(MSFORegPath);
            END IF;
        ELSE
            MSFOProc := GetMSFOProc_def(ProcType, OperationKind);
        END IF;
    ELSE
        MSFOProc := GetMSFOProc_def(ProcType, OperationKind);
    END IF;

    RETURN MSFOProc;
    EXCEPTION WHEN OTHERS THEN RETURN LoansConst.STR_EMPTY;
  END GetMSFOProc;

  -- Заполнение таблицы денежных потоков для процедуры расчета ЭПС
  FUNCTION FillXirr_def(
    objid       IN NUMBER,
    objn        IN NUMBER,
    calcdate    IN DATE,
    woutclose   IN NUMBER,
    ContextType IN NUMBER DEFAULT MMarkConst.CN_CONTEXT_NONE
  )
    RETURN NUMBER
  IS
      TICK       DDL_TICK_DBT%ROWTYPE;
      LEG        DDL_LEG_DBT%ROWTYPE;
      CreditLimit NUMBER := 0;
  BEGIN
    DELETE FROM DXIRR_TMP;
    IF (objid = MMarkConst.DL_IBCDOC) THEN
      INSERT INTO DXIRR_TMP (T_DATE, T_SUM)
      SELECT paym.t_valuedate,
            paym.t_futurepayeramount * decode(paym.t_payer, RsbSessionData.OurBank, -1, 1)
        FROM dpmpaym_dbt paym
      WHERE (woutclose = 0 OR paym.t_PaymStatus != 32000)
        AND paym.t_dockind     = objid
        AND paym.t_documentid  = objn
      UNION
      SELECT paym2.t_valuedate,
            link.t_amount * decode(paym2.t_payer, RsbSessionData.OurBank, -1, 1)
        FROM dpmpaym_dbt paym1,
            dpmpaym_dbt paym2,
            dpmlink_dbt link
      WHERE link.t_initialpayment = paym1.t_paymentid
        AND link.t_purposepayment = paym2.t_paymentid
        AND paym1.t_dockind       = objid
        AND paym1.t_documentid    = objn
      ORDER BY t_valuedate; 
    ELSIF (objid = MMarkConst.DL_CREDITLN) THEN
      SELECT * INTO TICK FROM DDL_TICK_DBT WHERE T_DEALID = objn;
      SELECT * INTO LEG FROM DDL_LEG_DBT WHERE T_LEGKIND = 0 AND T_DEALID = objn AND T_LEGID = 1;
      CreditLimit := GREATEST(TICK.t_DebtLimit, TICK.t_IssuanceLimit);
      INSERT INTO DXIRR_TMP (T_DATE, T_SUM) VALUES (LEG.t_Maturity, CreditLimit);
      INSERT INTO DXIRR_TMP (T_DATE, T_SUM) VALUES (LEG.t_Maturity, (CreditLimit * LEG.t_Price * (LEG.t_Duration/365))/100);
    END IF;

    RETURN 0;
  END FillXirr_def;

  -- Заполнение таблицы денежных потоков для процедуры расчета ЭПС. Алгоритм по-умолчанию для сделок привлечения
  FUNCTION FillXirr_BUY_def(
    objid       IN NUMBER,
    objn        IN NUMBER,
    calcdate    IN DATE,
    woutclose   IN NUMBER,
    ContextType IN NUMBER DEFAULT MMarkConst.CN_CONTEXT_NONE
  )
    RETURN NUMBER
  IS
  BEGIN
    RETURN FillXirr_def(objid, objn, calcdate, woutclose, ContextType); 
  END FillXirr_BUY_def;

  -- Заполнение таблицы денежных потоков для процедуры расчета ЭПС. Алгоритм по-умолчанию для сделок размещения
  FUNCTION FillXirr_SALE_def(
    objid       IN NUMBER,
    objn        IN NUMBER,
    calcdate    IN DATE,
    woutclose   IN NUMBER,
    ContextType IN NUMBER DEFAULT MMarkConst.CN_CONTEXT_NONE
  )
    RETURN NUMBER
  IS
  BEGIN
    RETURN FillXirr_def(objid, objn, calcdate, woutclose, ContextType); 
  END FillXirr_SALE_def;

  --Расчет ЭПС (алгоритм)
  FUNCTION CalcEPS_def(
    objid       IN NUMBER,
    objn        IN NUMBER,
    calcdate    IN DATE,
    ContextType IN NUMBER DEFAULT MMarkConst.CN_CONTEXT_NONE
  )
    RETURN FLOAT
   IS
      accuracy   INTEGER  := -RSB_COMMON.GetRegIntValue('MMARK\ЭПС_ТОЧНОСТЬ_РЕЗУЛЬТАТА');
      iternum    INTEGER  :=  RSB_COMMON.GetRegIntValue('MMARK\ЭПС_ЧИСЛО_ИТЕРАЦИЙ');
      epsval     NUMBER   := 0;
      stat       NUMBER   := 0;
   BEGIN
      IF (objid = MMarkConst.DL_CREDITLN) THEN
         SELECT T_PRICE INTO epsval FROM DDL_LEG_DBT WHERE T_LEGKIND = 0 AND T_DEALID = objn AND T_LEGID = 1;

      ELSIF (objid = MMarkConst.DL_IBCDOC) THEN
         stat   := RSO_MMARK_MSFO.FillXirr(objid, objn, calcdate, 0, ContextType);
         epsVal := CalcEPS_def(0, 0, calcdate, MMarkConst.CN_CONTEXT_NONE);

      ELSE
         EXECUTE IMMEDIATE
         'with t as                                                                                                                         '||
         '(                                                                                                                                 '||
         '  SELECT t_date as dt, t_sum as summ FROM DXIRR_TMP                                                                               '||
         ')                                                                                                                                 '||
         '  select decode(rate, NULL, -1, 0, -1, rate)                                                                                     '||
         '    FROM (SELECT *                                                                                                                '||
         '            FROM t                                                                                                                '||
         '           model                                                                                                                  '||
         '    dimension by (row_number() over (order by dt) rn)                                                                             '||
         '        measures (dt - first_value(dt) over (order by dt) dt, summ s, 0 sg, 1 sgch, 0 ss, 0 sss, -0.99 a, 200 b, 0 x, 0 rate, 0 iter) '||
         '           rules iterate(' || to_char(iternum) || ')                                                                              '||
         '                 (                                                                                                                '||
         '                   x[1] = a[1] + (b[1] - a[1])/2,                                                                                 '||
         '                   ss[any] = s[CV()] / power(1 + x[1], dt[CV()]/365),                                                             '||
         '                   sss[1] = sum(ss)[any],                                                                                         '||
         '                   sgch[1] = decode(sign(abs(sss[1]) - power(10, ' || to_char(accuracy) || ')), -1, -1, 1),                       '||
         '                   sg[1] = decode(sg[1], 0, decode(sign(sss[1]), -1, -1, 1), sg[1]),                                              '||
         '                   a[1] = decode(sign(sss[1]), sg[1] * sgch[1], a[1], x[1]),                                                      '||
         '                   b[1] = decode(sign(sss[1]), sg[1] * sgch[1], x[1], b[1]),                                                      '||
         '                   rate[1] = decode(sign(abs(sss[1]) - power(10, ' || to_char(accuracy) || ')), 1, rate[1], x[1]),                '||
         '                   iter[1] = iteration_number + 1                                                                                 '||
         '                 )                                                                                                                '||
         '         )                                                                                                                        '||
         '   WHERE rn = 1                                                                                                                   '
         INTO epsval;

         IF (epsval != -1) THEN
           epsval := round(epsval*100, abs(accuracy));
         ELSE
           epsval := 0;
         END IF;
      END IF;

      RETURN epsval;

      EXCEPTION WHEN NO_DATA_FOUND THEN RETURN -1;
   END CalcEPS_def;

   -- Расчет РПС. Алгоритм по-умолчанию
   FUNCTION CalcRPS_def(
    objid       IN NUMBER,
    objn        IN NUMBER,
    calcdate    IN DATE,
    ContextType IN NUMBER DEFAULT MMarkConst.CN_CONTEXT_NONE
  )
    RETURN FLOAT
   IS
      pMinValue        NUMBER := -1;
      pMaxValue        NUMBER := -1;
      pSourceDataLayer NUMBER;
      pProductKindID   NUMBER := 0;
      AsCategory       NUMBER := PM_COMMON.GetObjAttrValue(103/*OBJECTTYPE_MMARKDEAL*/, LPAD(objn, 34, '0'), 5, calcdate);
      dSourceDataLayer NUMBER := 0;
      pMarketRateID    NUMBER := 0;
      PFI              NUMBER := 0;
      mFromMAKS NUMBER :=
                   NVL(RSB_COMMON.GetRegIntValue (
                       'РСХБ\БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ\ТРАНСФЕРНЫЕ СТАВКИ МАКС\ИМПОРТ ТР.СТАВОК ИСП. KAFKA'), 0);

      TICK       DDL_TICK_DBT%ROWTYPE;
      LEG        DDL_LEG_DBT%ROWTYPE;
      epsval     FLOAT := 0;
      rpsval     FLOAT := 0;
   BEGIN
      SELECT * INTO TICK FROM DDL_TICK_DBT WHERE T_DEALID = objn;
      SELECT * INTO LEG FROM DDL_LEG_DBT WHERE T_LEGKIND = 0 AND T_DEALID = objn AND T_LEGID = 1;

      SELECT t_attrid INTO dSourceDataLayer FROM dobjatcor_dbt WHERE t_groupid = 7 AND t_object = LPAD(objn, 34, '0');

      IF (objid = MMarkConst.DL_IBCDOC) THEN
         PFI := LEG.t_PFI;
         IF (RSB_Common.GetRegFlagValue('MMARK\ОБЩИЕ\БАНКОВСКИЕ_ПРОДУКТЫ') = LoansConst.CHAR_EMPTY OR TICK.t_Product = 0) THEN

            IF (MMark_UTL.IsBUY(MMark_UTL.GetOperationGroup(TICK.t_DealType)) > 0) THEN
               pProductKindID := RSB_Common.GetRegIntValue('MMARK\ОБЩИЕ\БАНКОВСКИЕ_ПРОДУКТЫ\ПРИВЛЕЧЕНИЕ');
            ELSE
               pProductKindID := RSB_Common.GetRegIntValue('MMARK\ОБЩИЕ\БАНКОВСКИЕ_ПРОДУКТЫ\РАЗМЕЩЕНИЕ');
            END IF;

         ELSE
            SELECT t_ProductID INTO pProductKindID FROM dprdclient_dbt
              INNER JOIN ddl_tick_dbt ON ddl_tick_dbt.t_Product = dprdclient_dbt.t_ClientProductID
              WHERE ddl_tick_dbt.t_BOfficeKind = objid AND ddl_tick_dbt.t_DealID = objn;
         END IF;
      ELSE 
         PFI := TICK.t_LimitCur;
         pProductKindID := 0;
      END IF;

      IF (RSI_MARKETRATE.SelectRangeMarketRateEx(20, TICK.t_Department, 54, pProductKindID, 5,
                                             0, 0, LEG.T_DURATION, LEG.t_Principal, PFI, 1,
                                             calcdate, pMinValue, pMaxValue, pSourceDataLayer, pMarketRateID) > 0)
      THEN
         RAISE NO_DATA_FOUND;
      END IF;
      
      /*DEF-75770 убрала проверку SourceDataLayer cо значением категории на сделке, т.к. из кафки грузят все возможные значения для SourceDataLayer, нет смысла сравнивать.*/
      IF mFromMAKS = 0 THEN
          IF (dSourceDataLayer != 0 and dSourceDataLayer != pSourceDataLayer)
          THEN
             RAISE NO_DATA_FOUND;
          END IF;
      ELSE
          IF (dSourceDataLayer = 0)
          THEN
             RAISE NO_DATA_FOUND;
          END IF;
      END IF;

      IF (objid = MMarkConst.DL_CREDITLN) THEN
         epsval := LEG.T_PRICE;
      ELSIF (objid = MMarkConst.DL_IBCDOC) THEN
         CASE (RSB_COMMON.GetRegIntValue('MMARK\ТЕСТ_НА_РЫНОЧНОСТЬ'))
            WHEN 1 THEN
            BEGIN
               epsval := LEG.T_PRICE / POWER(10, LEG.T_POINT);
            END;

            WHEN 2 THEN
            BEGIN
               epsval := RSO_MMARK_MSFO.CalcEPS(objid, objn, calcdate, MMarkConst.CN_CONTEXT_NONE);
            END;

            WHEN 3 THEN
            BEGIN
               epsval := RSO_MMARK_MSFO.CalcEPS(objid, objn, calcdate, MMarkConst.CN_CONTEXT_NONE);

               --FillXirr(objid, objn); Согласно ТЗ, надо заполнить исходя из того, что ставка по сделке равна pMinValue
               pMinValue := CalcEPS_def(0, 0, calcdate, MMarkConst.CN_CONTEXT_NONE);

               --FillXirr(objid, objn); Согласно ТЗ, надо заполнить исходя из того, что ставка по сделке равна pMaxValue
               pMaxValue := CalcEPS_def(0, 0, calcdate, MMarkConst.CN_CONTEXT_NONE);
            END;

            WHEN 4 THEN
            BEGIN
               IF (AsCategory = 1) THEN -- "Категория оценки" = "Оцениваемые по амортизированной стоимости (линейный метод)"
                  epsval := LEG.T_PRICE / POWER(10, LEG.T_POINT);
               ELSE
                  epsval := RSO_MMARK_MSFO.CalcEPS(objid, objn, calcdate, MMarkConst.CN_CONTEXT_NONE);
               END IF;
            END;

            WHEN 5 THEN
            BEGIN
               IF (AsCategory = 1) THEN -- "Категория оценки" = "Оцениваемые по амортизированной стоимости (линейный метод)"
                  epsval := LEG.T_PRICE / POWER(10, LEG.T_POINT);
               ELSE
                  epsval := RSO_MMARK_MSFO.CalcEPS(objid, objn, calcdate, MMarkConst.CN_CONTEXT_NONE);

                  --FillXirr(objid, objn); Согласно ТЗ, надо заполнить исходя из того, что ставка по сделке равна pMinValue
                  pMinValue := CalcEPS_def(0, 0, calcdate, MMarkConst.CN_CONTEXT_NONE);

                  --FillXirr(objid, objn); Согласно ТЗ, надо заполнить исходя из того, что ставка по сделке равна pMaxValue
                  pMaxValue := CalcEPS_def(0, 0, calcdate, MMarkConst.CN_CONTEXT_NONE);
               END IF;
            END;
         ELSE
            RETURN -1;
         END CASE;
      ELSE
         RAISE NO_DATA_FOUND;
      END IF;

      IF ((epsval = -1)OR(pMinValue = -1)OR(pMaxValue = -1)) THEN
         RAISE NO_DATA_FOUND;
      ELSIF (epsval > pMinValue AND epsval < pMaxValue) THEN
         rpsval := epsval;
      ELSIF (epsval <= pMinValue) THEN
         rpsval := pMinValue;
      ELSE
         rpsval := pMaxValue;
      END IF;

      RETURN rpsval;
      EXCEPTION
        WHEN OTHERS THEN RETURN -1;
   END CalcRPS_def;
   
   -- Определение превышения уровня существенности. Алгоритм по-умолчанию
   FUNCTION GetMlvlEx_def(
     objid       IN NUMBER,
     objn        IN NUMBER,
     lvlID       IN NUMBER,
     calcdate    IN DATE,
     ContextType IN NUMBER DEFAULT MMarkConst.CN_CONTEXT_NONE
   )
     RETURN NUMBER
   IS
     s1     FLOAT   := 0;
     s2     FLOAT   := 0;
     ss1    ANYDATA;
     ss2    ANYDATA;
     spvar1 CHAR(255);
     spvar2 CHAR(255);
     p      FLOAT   := 0;
     mlvl   FLOAT   := 0;
     sp     FLOAT   := 0;
     stat   NUMBER  := 0;
     res    NUMBER  := 0;
     DevMat CHAR;
   BEGIN
     SELECT T_VALUE, T_CALC_FROM, T_COMPARE_WITH
       INTO p, spvar1, spvar2
       FROM DML_HISTORY_DBT
      WHERE T_LEVEL_ID_REF = LvlID
        AND T_DATE = (SELECT MAX(t_date)
                        FROM DML_HISTORY_DBT
                       WHERE T_DATE <= calcdate
                         AND T_LEVEL_ID_REF = LvlID)
        AND rownum = 1;

     stat := RSI_Loans_SpVariable.CalcFormulaExt(ss1, spvar1, LOANSCONST.SPTYPE_NUMBER, objid, objn, calcdate, LOANSCONST.alg_out_rest, MMarkConst.CN_CONTEXT_EXCESS_MTRLTY);
     IF (stat = 0 AND ss1.AccessNumber IS NOT NULL AND ss1.AccessNumber <> -1) THEN
       s1 := ss1.AccessNumber;
     ELSE
         RAISE NO_DATA_FOUND;
     END IF;
     
     stat := RSI_Loans_SpVariable.CalcFormulaExt(ss2, spvar2, LOANSCONST.SPTYPE_NUMBER, objid, objn, calcdate, LOANSCONST.alg_out_rest, MMarkConst.CN_CONTEXT_EXCESS_MTRLTY);
     IF (stat = 0 AND ss2.AccessNumber IS NOT NULL AND ss2.AccessNumber <> -1) THEN
       s2 := ss2.AccessNumber;
     ELSE
         RAISE NO_DATA_FOUND;
     END IF;
    
     mlvl := s1 * p/100;

     SELECT T_IS_DEVMAT
       INTO DevMat
       FROM DML_LEVELS_DBT
      WHERE T_ID = LvlID;

     IF (DevMat = CHR(88)) THEN
         sp := ABS(s1 - s2);
 
         IF (mlvl <= sp) THEN
             res := 1; 
         END IF;
     ELSE
         IF (mlvl >= s2) THEN
             res := 1;
         END IF;
     END IF;

     RETURN res;
     EXCEPTION WHEN NO_DATA_FOUND THEN RETURN -1;
   END GetMlvlEx_def;

   -- Определение категории оценки. Алгоритм по-умолчанию
   FUNCTION GetASCtg_def(
     objid        IN NUMBER,
     objn         IN NUMBER,
     dealduration IN NUMBER,
     srok         IN CHAR,
     flagtypedeal IN NUMBER,
     isTradeFin   IN CHAR,
     calcdate     IN DATE,
     ContextType  IN NUMBER DEFAULT MMarkConst.CN_CONTEXT_NONE
   )
     RETURN NUMBER
   IS
     res NUMBER := 0; -- Номер записи из системного справочника "Категория оценки договора"
   BEGIN
     IF (((dealduration <= 365 OR srok = LoansConst.CHAR_CROSS) AND flagtypedeal != 2 AND flagtypedeal != 3) 
       OR isTradeFin = LoansConst.CHAR_CROSS) THEN
       res := 1;
     ELSE
       res := 2;
     END IF;
     RETURN res;
   END GetASCtg_def;


   -- Расчет суммы отсроченной разницы. Алгоритм по-умолчанию
   FUNCTION CalcDDA_def(
     objid       IN NUMBER,
     objn        IN NUMBER,
     calcdate    IN DATE,
     ContextType IN NUMBER DEFAULT MMarkConst.CN_CONTEXT_NONE,
     operationid IN NUMBER DEFAULT 0,
     stepid      IN NUMBER DEFAULT 0
   )
     RETURN NUMBER
   IS
     OPn        NUMBER  := 0;
     res        NUMBER  := 0;
     opncalc    NUMBER  := 0;
     typeproc   INTEGER := RSB_COMMON.GetRegIntValue('MMARK\ОТРАЖЕНИЕ_ОТСРОЧЕННОЙ_РАЗНИЦЫ');
     tk         NUMBER  := 0;
     tz         NUMBER  := 0;
     stat       NUMBER  := 0;
     stepdate   DATE    := TO_DATE('01.01.0001', 'DD.MM.YYYY');
     dealend    DATE    := TO_DATE('01.01.0001', 'DD.MM.YYYY');
     dealstart  DATE    := TO_DATE('01.01.0001', 'DD.MM.YYYY');
     dealgroup  NUMBER  := 0;
     pmprncsum  NUMBER  := 0;
     pmprncstep NUMBER := 0;
     curprinc   NUMBER  := 0;
     LEG   DDL_LEG_DBT%ROWTYPE;
     TICK  DDL_TICK_DBT%ROWTYPE;

   BEGIN
     -- Получить значение OPn из параметров сделки "Неучтенная отсроченная разница"
     OPn := MMARKCOMMON.GetNoteTextMoney(103/*OBJTYPE_MMARKDEAL*/, objn, 5/*MN_UNRECORDED*/, calcdate);
     IF (OPn = 0 OR OPn IS NULL)
     THEN
       RETURN -1;
     END IF;
     OPn := ABS(OPn);
     SELECT * INTO LEG  FROM DDL_LEG_DBT WHERE T_DEALID = objn;
     SELECT * INTO TICK FROM DDL_TICK_DBT WHERE T_DEALID = objn;

     IF (ContextType = MMarkConst.CN_CONTEXT_PM_PRINC_EXEC) THEN
       SELECT t_dealgroup INTO dealgroup FROM DDL_TICK_DBT WHERE T_DEALID = ObjN;
       -- получить номер шага исполнения входящего/исходящего платежа по ОД
       IF ((MMARK_UTL.IsSALE(dealgroup) > 0)) THEN
         pmprncstep := MMarkCommon.GetLastStepBeforeStep(operationid, stepid, 'i');
       ELSE
         pmprncstep := MMarkCommon.GetLastStepBeforeStep(operationid, stepid, 'g');
       END IF;
       IF (pmprncstep IS NULL) THEN
         RETURN -1;
       END IF;
       pmprncsum := MMarkCommon.GetPaymSum(operationid, pmprncstep, calcdate, RSB_Payment.PM_PURP_PRINC_RET);
       IF (pmprncsum = 0) THEN
         RETURN 0;
       END IF;
       IF (TICK.t_FlagTypeDeal = 4) THEN
         curprinc := MMarkCommon.GetAccRest(objid, objn, calcdate, MMarkConst.tdr_odmainrest, 1, LEG.t_pfi);
       ELSE
         curprinc := MMarkCommon.GetAccRest(objid, objn, calcdate, MMarkConst.tdr_mainrest, 1, LEG.t_pfi);
       END IF;
       
       res := OPn * (pmprncsum/(curprinc + pmprncsum));
       RETURN res;

     END IF;

     stat := MMarkCommon.GetDateStepBySymbol(ObjN, 'д', stepdate);
     IF (stat = 0)
     THEN
       -- шаг учет отсроченных единиц выполнен
       tk := calcdate - stepdate;
     ELSE
      -- иначе получить дату выполнения шага ?Оценка при первоначальном признании?
       stat := MMarkCommon.GetDateStepBySymbol(ObjN, 'Ц', stepdate);
       IF (stat = 0)
       THEN
         tk := calcdate - stepdate;
       ELSE
         res := -1;
       RETURN res;
     END IF;

   END IF;

   -- получить дату окончания договора
   BEGIN
     SELECT t_start, t_maturity INTO dealstart, dealend FROM DDL_LEG_DBT WHERE T_DEALID = ObjN;
   END;

   -- равномерный учет
   CASE
     WHEN typeproc = 1 THEN

       -- расчет tz
       tz := dealend - stepdate;

       IF (tz = 0)
       THEN
         opncalc := OPn;
       ELSE
         opncalc := OPn * (tk/tz);
       END IF;
         res := LEAST(OPn, opncalc);

     WHEN typeproc = 2 THEN
       res := OPn * (tk/(dealend - dealstart));

   END CASE;

      RETURN res;
      EXCEPTION WHEN NO_DATA_FOUND THEN RETURN -1;
   END CalcDDA_def;

   -- Расчет суммы затрат. Алгоритм по-умолчанию
   FUNCTION CalcCosts_def(
     objid       IN NUMBER,
     objn        IN NUMBER,
     calcdate    IN DATE,
     ContextType IN NUMBER DEFAULT MMarkConst.CN_CONTEXT_NONE,
     operationid IN NUMBER DEFAULT 0,
     stepid      IN NUMBER DEFAULT 0
   )
     RETURN NUMBER
   IS
     Uc         NUMBER := 0; -- нераспределенные затраты
     Cr         NUMBER := 0;
     stat       NUMBER := 0;
     stepdate   DATE   := TO_DATE('01.01.0001', 'DD.MM.YYYY');
     dealend    DATE   := TO_DATE('01.01.0001', 'DD.MM.YYYY');
     dealstart  DATE   := TO_DATE('01.01.0001', 'DD.MM.YYYY');
     tk         NUMBER := 0;
     tz         NUMBER := 0;
     uccalc     NUMBER := 0;
     dealgroup  NUMBER := 0;
     pmprncsum  NUMBER := 0;
     pmprncstep NUMBER := 0;
     curprinc   NUMBER := 0;
     LEG   DDL_LEG_DBT%ROWTYPE;
     TICK  DDL_TICK_DBT%ROWTYPE;

   BEGIN
     Uc := MMARKCOMMON.GetNoteTextMoney(103/*OBJTYPE_MMARKDEAL*/, objn, 14/*MN_UNALLOCATED*/, calcdate);
     IF (Uc = 0 OR Uc IS NULL) THEN
       RETURN -1;
     END IF;
     Uc := ABS(Uc);

     SELECT * INTO LEG  FROM DDL_LEG_DBT WHERE T_DEALID = objn;
     SELECT * INTO TICK FROM DDL_TICK_DBT WHERE T_DEALID = objn;

     IF (ContextType = MMarkConst.CN_CONTEXT_PM_PRINC_EXEC) THEN
       SELECT t_dealgroup INTO dealgroup FROM DDL_TICK_DBT WHERE T_DEALID = ObjN;
       -- получить номер шага исполнения входящего/исходящего платежа по ОД
       IF ((MMARK_UTL.IsSALE(dealgroup) > 0)) THEN
         pmprncstep := MMarkCommon.GetLastStepBeforeStep(operationid, stepid, 'i');
       ELSE
         pmprncstep := MMarkCommon.GetLastStepBeforeStep(operationid, stepid, 'g');
       END IF;
       IF (pmprncstep IS NULL) THEN
         RETURN -1;
       END IF;
       pmprncsum := MMarkCommon.GetPaymSum(operationid, pmprncstep, calcdate, RSB_Payment.PM_PURP_PRINC_RET);
       IF (pmprncsum = 0) THEN
         RETURN 0;
       END IF;
       IF (TICK.t_FlagTypeDeal = 4) THEN
         curprinc := MMarkCommon.GetAccRest(objid, objn, calcdate, MMarkConst.tdr_odmainrest, 1, LEG.t_pfi);
       ELSE
         curprinc := MMarkCommon.GetAccRest(objid, objn, calcdate, MMarkConst.tdr_mainrest, 1, LEG.t_pfi);
       END IF;
       Cr := Uc * (pmprncsum/(curprinc + pmprncsum));
       RETURN Cr;

     END IF;

     -- получить дату окончания договора
     BEGIN
       SELECT t_start, t_maturity INTO dealstart, dealend FROM DDL_LEG_DBT WHERE T_DEALID = ObjN;
     END;

     stat := MMarkCommon.GetDateStepBySymbol(ObjN, 'х', stepdate);
     IF (stat = 0)
     THEN
       -- шаг учет отражения расходов по затратам выполнен
       tk := calcdate - stepdate;
     ELSE
       -- иначе получить дату выполнения шага Учет затрат
       stat := MMarkCommon.GetDateStepBySymbol(ObjN, 'я', stepdate);
       IF (stat = 0)
       THEN
         tk := calcdate - stepdate;
       ELSE
       RETURN -1;
       END IF;
     END IF;

     CASE (RSB_COMMON.GetRegIntValue('MMARK\ТЕСТ_НА_РЫНОЧНОСТЬ'))
       WHEN 1 THEN
         Cr := 1;

         tz := dealend - stepdate;
         IF (tz = 0)
         THEN
           uccalc := Uc;
         ELSE
           uccalc := Uc * (tk/tz);
         END IF;
         Cr := LEAST(Uc, uccalc);

       WHEN 2 THEN
         Cr := Uc * (tk/(dealend - dealstart));

     END CASE;
     RETURN Cr;

   END CalcCosts_def;

   -- Расчет ЭПС. Алгоритм по-умолчанию для сделок привлечения
   FUNCTION CalcEPS_BUY_def(
     objid       IN NUMBER,
     objn        IN NUMBER,
     calcdate    IN DATE,
     ContextType IN NUMBER DEFAULT MMarkConst.CN_CONTEXT_NONE
   )
     RETURN FLOAT
   IS
   BEGIN
     RETURN CalcEPS_def(objid, objn, calcdate, ContextType);
   END CalcEPS_BUY_def;

   -- Расчет ЭПС. Алгоритм по-умолчанию для сделок размещения
   FUNCTION CalcEPS_SALE_def(
     objid       IN NUMBER,
     objn        IN NUMBER,
     calcdate    IN DATE,
     ContextType IN NUMBER DEFAULT MMarkConst.CN_CONTEXT_NONE
   )
     RETURN FLOAT
   IS
   BEGIN
     RETURN CalcEPS_def(objid, objn, calcdate, ContextType);
   END CalcEPS_SALE_def;

   -- Расчет валовой балансовой стоимости. Алгоритм по-умолчанию
   FUNCTION CalcGBV_def(
     objid       IN NUMBER,
     objn        IN NUMBER,
     calcdate    IN DATE,
     ContextType IN NUMBER DEFAULT MMarkConst.CN_CONTEXT_NONE
   )
     RETURN NUMBER
   IS
     epsval     NUMBER := 0;
     gbvval     NUMBER := 0;
     markettest NUMBER := 0;
     datatype   NUMBER := 0;
     stat       NUMBER := 0;
     
   BEGIN
     SELECT t_attrid INTO markettest FROM dobjatcor_dbt WHERE t_groupid = 6 AND t_object = LPAD(objn, 34, '0');
     SELECT t_attrid INTO datatype   FROM dobjatcor_dbt WHERE t_groupid = 8 AND t_object = LPAD(objn, 34, '0');
     
     IF (markettest = 2 AND datatype = 1) THEN
       epsval := RSO_MMARK_MSFO.CalcRPS(objid, objn, calcdate, ContextType);
     ELSE
       epsval := RSO_MMARK_MSFO.CalcEPS(objid, objn, calcdate, ContextType);
     END IF;
     
     stat   := RSO_MMARK_MSFO.FillXirr(objid, objn, calcdate, 1, ContextType); 

     with t as
     (
       SELECT t_date as dt, t_sum as summ FROM DXIRR_TMP WHERE t_date >= calcdate
     )
     SELECT decode(sum(rate), null, 0, sum(rate)) INTO gbvval
       FROM (SELECT *
               FROM t
              model
       dimension by (row_number() over (order by dt) rn)
           measures (dt, summ s, 0 rate)
              rules (rate[any] = s[CV()] / power(1 + (epsval/100), (dt[CV()] - calcdate)/365))
            );

     RETURN ABS(gbvval);
     EXCEPTION WHEN NO_DATA_FOUND THEN RETURN -1;
   END CalcGBV_def;

   -- Расчет АС. Алгоритм по-умолчанию для сделок привлечения
   FUNCTION CalcAS_BUY_def(
     objid       IN NUMBER,
     objn        IN NUMBER,
     calcdate    IN DATE,
     ContextType IN NUMBER DEFAULT MMarkConst.CN_CONTEXT_NONE
   )
     RETURN NUMBER
   IS
     epsval     NUMBER := 0;
     asval      NUMBER := 0;
     markettest NUMBER := 0;
     datatype   NUMBER := 0;
     stat       NUMBER := 0;
     
   BEGIN
     SELECT t_attrid INTO markettest FROM dobjatcor_dbt WHERE t_groupid = 6 AND t_object = LPAD(objn, 34, '0');
     SELECT t_attrid INTO datatype   FROM dobjatcor_dbt WHERE t_groupid = 8 AND t_object = LPAD(objn, 34, '0');
     
     IF (markettest = 2 AND datatype = 1) THEN
       epsval := RSO_MMARK_MSFO.CalcRPS(objid, objn, calcdate, ContextType);
     ELSE
       epsval := RSO_MMARK_MSFO.CalcEPS(objid, objn, calcdate, ContextType);
     END IF;
     
     stat   := RSO_MMARK_MSFO.FillXirr(objid, objn, calcdate, 1, ContextType); 

     with t as
     (
       SELECT t_date as dt, t_sum as summ FROM DXIRR_TMP WHERE t_date >= calcdate
     )
     SELECT decode(sum(rate), null, 0, sum(rate)) INTO asval
       FROM (SELECT *
               FROM t
              model
       dimension by (row_number() over (order by dt) rn)
           measures (dt, summ s, 0 rate)
              rules (rate[any] = s[CV()] / power(1 + (epsval/100), (dt[CV()] - calcdate)/365))
            );

     RETURN ABS(asval);
     EXCEPTION WHEN NO_DATA_FOUND THEN RETURN 0;
   END CalcAS_BUY_def;

   -- Расчет АС. Алгоритм по-умолчанию для сделок размещения
   FUNCTION CalcAS_SALE_def(
     objid       IN NUMBER,
     objn        IN NUMBER,
     calcdate    IN DATE,
     ContextType IN NUMBER DEFAULT MMarkConst.CN_CONTEXT_NONE
   )
     RETURN NUMBER
   IS
     gbv  FLOAT := 0;
     Socr NUMBER := 0; --текущий оценочный резерв
   BEGIN
     gbv  := RSO_MMARK_MSFO.CalcGBV(objid, objn, calcdate);
     Socr := MMARKCOMMON.GetNoteTextMoney(103/*OBJTYPE_MMARKDEAL*/, objn, 11/*MN_CURRENT*/, calcdate);
     IF (Socr IS NULL) THEN
       Socr := 0;
     END IF;
     RETURN gbv - Socr;

   END CalcAS_SALE_def;

   -- Расчет СС (алгоритм)
   FUNCTION CalcSS_def(
     objid       IN NUMBER,
     objn        IN NUMBER,
     calcdate    IN DATE,
     ContextType IN NUMBER DEFAULT MMarkConst.CN_CONTEXT_NONE
   )
     RETURN NUMBER
   IS
     epsval     NUMBER := 0;
     ssval      NUMBER := 0;
     stat       NUMBER := 0;
     
   BEGIN
     epsval := RSO_MMARK_MSFO.CalcRPS(objid, objn, calcdate, ContextType);
     
     stat   := RSO_MMARK_MSFO.FillXirr(objid, objn, calcdate, 1, ContextType); 

     with t as
     (
       SELECT t_date as dt, t_sum as summ FROM DXIRR_TMP WHERE t_date >= calcdate
     )
     SELECT decode(sum(rate), null, 0, sum(rate)) INTO ssval
       FROM (SELECT *
               FROM t
              model
       dimension by (row_number() over (order by dt) rn)
           measures (dt, summ s, 0 rate)
              rules (rate[any] = s[CV()] / power(1 + (epsval/100), (dt[CV()] - calcdate)/365))
            );

     RETURN ABS(ssval);
     EXCEPTION WHEN NO_DATA_FOUND THEN RETURN -1;
   END CalcSS_def;

   -- Расчет СС. Алгоритм по-умолчанию для сделок привлечения
   FUNCTION CalcSS_BUY_def(
     objid       IN NUMBER,
     objn        IN NUMBER,
     calcdate    IN DATE,
     ContextType IN NUMBER DEFAULT MMarkConst.CN_CONTEXT_NONE
   )
     RETURN NUMBER
   IS
   BEGIN
     RETURN CalcSS_def(objid, objn, calcdate, ContextType);
   END CalcSS_BUY_def;

   -- Расчет СС. Алгоритм по-умолчанию для сделок размещения
   FUNCTION CalcSS_SALE_def(
     objid       IN NUMBER,
     objn        IN NUMBER,
     calcdate    IN DATE,
     ContextType IN NUMBER DEFAULT MMarkConst.CN_CONTEXT_NONE
   )
     RETURN NUMBER
   IS
   BEGIN
     RETURN CalcSS_def(objid, objn, calcdate, ContextType);
   END CalcSS_SALE_def;

   -- Расчет оценочного резерва. Алгоритм по-умолчанию для сделок размешения
   FUNCTION CalcAssesRes_def(
     objid       IN NUMBER,
     objn        IN NUMBER,
     amountbase  IN NUMBER,
     calcdate    IN DATE,
     ContextType IN NUMBER DEFAULT MMarkConst.CN_CONTEXT_NONE
   )
     RETURN NUMBER
   IS
     percloss  NUMBER  := 0;
     dealfiid  INTEGER := 0;
   BEGIN
   
     SELECT t_percloss INTO percloss FROM dmmhstimp_dbt WHERE T_DEALID = objn
     AND t_date IN
       (SELECT max(t_date) FROM dmmhstimp_dbt WHERE t_date <= calcdate AND T_DEALID = objn);

     SELECT t_pfi INTO dealfiid FROM DDL_LEG_DBT
       WHERE T_DEALID = objn;

     RETURN RSI_RSB_FIInstr.ConvSum(amountbase, dealfiid, 0, calcdate)*(percloss/100);
     EXCEPTION WHEN OTHERS THEN RETURN 0;
   END CalcAssesRes_def;

   -- Расчет суммы корректировки стоимости. Алгоритм по-умолчанию
   FUNCTION CalcAdjCost_def(
     objid       IN NUMBER,
     objn        IN NUMBER,
     calcdate    IN DATE,
     ContextType IN NUMBER DEFAULT MMarkConst.CN_CONTEXT_NONE
   )
     RETURN FLOAT
   IS
   BEGIN
     RETURN -1;
   END CalcAdjCost_def;

   -- Расчет суммы корректировки АС. Алгоритм по-умолчанию для сделок привлечения
   FUNCTION CalcAdjAS_BUY_def(
     objid       IN NUMBER,
     objn        IN NUMBER,
     calcdate    IN DATE,
     ContextType IN NUMBER DEFAULT MMarkConst.CN_CONTEXT_NONE
   )
     RETURN NUMBER
   IS
     res        NUMBER := 0;
     PercExp    NUMBER := 0; -- Процентный расход
     ExpSum     NUMBER := RSO_MMARK_MSFO.CalcCosts(objid, objn, calcdate, ContextType); -- Сумма затрат
     AC         NUMBER := MMARKCOMMON.GetNoteTextMoney(103/*OBJTYPE_MMARKDEAL*/, objn, 7/*MN_AMORTIZED*/, calcdate); -- АС
     EPS        FLOAT  := RSO_MMARK_MSFO.CalcEPS(objid, objn, calcdate, ContextType);
     tk         NUMBER := 0;
     prevdate   DATE   := TO_DATE('01.01.0001', 'DD.MM.YYYY');
     prevpercdt DATE   := TO_DATE('01.01.0001', 'DD.MM.YYYY');
     pastpmsum  NUMBER := 0;
     stat       NUMBER := 0;
     PrcContrID NUMBER := 0;
     costs      NUMBER := 0;

     LEG   DDL_LEG_DBT%ROWTYPE;
   BEGIN
     SELECT * INTO LEG  FROM DDL_LEG_DBT WHERE T_DEALID = objn;
     IF (AC IS NULL) THEN
       AC := 0;
     END IF;

     IF (ExpSum = -1) THEN
       ExpSum := 0;
     END IF;

     stat := MMarkCommon.GetDateStepBySymbol(ObjN, 'm', prevdate);
     prevpercdt := prevdate + 1;
     IF (stat = -1) THEN
        prevdate   := LEG.t_Start;
        prevpercdt := prevdate;
        
        SELECT NVL(SUM(CASE WHEN (pm.t_PartPaymRestAmountMain < pm.t_Amount AND pm.t_PartPaymRestAmountMain > 0) THEN pm.t_PartPaymRestAmountMain ELSE pm.t_Amount END), 0)
        INTO costs
        FROM dpmpaym_dbt pm
        WHERE t_dockind = objid AND t_documentid = objn AND pm.t_PaymStatus = 32000 AND t_valuedate <= calcdate AND t_purpose = 83 /*Затраты*/;
        
        AC := LEG.t_Principal + costs;
     END IF;
     tk := calcdate - prevdate;
     pastpmsum := MMARKCOMMON.GetPastPaymSum(objid, objn, prevdate);

     SELECT t_contractid INTO PrcContrID FROM dprccontract_dbt WHERE t_objecttype = 103 AND t_contracttype = 7
       AND t_objectid IN (SELECT t_dealcode FROM ddl_tick_dbt WHERE t_dealid = objn AND t_bofficekind = objid);

     stat := RSI_RSB_PERCENT.Prc_CalcPercentValues(PercExp, PrcContrID, prevpercdt, calcdate, 0);
     IF (stat = -1) THEN
       PercExp := 0;
     END IF;

     res := ((AC - pastpmsum)*(power((1 + (EPS/100)), (tk/365))-1)) - (PercExp + ExpSum);

     RETURN res;
   END CalcAdjAS_BUY_def;

   -- Расчет суммы корректировки АС. Алгоритм по-умолчанию для сделок размещения
   FUNCTION CalcAdjAS_SALE_def(
     objid       IN NUMBER,
     objn        IN NUMBER,
     calcdate    IN DATE,
     ContextType IN NUMBER DEFAULT MMarkConst.CN_CONTEXT_NONE
   )
     RETURN NUMBER
   IS
     res        NUMBER := 0;
     PercIncome NUMBER := 0; -- Процентный доход
     OthPercInc NUMBER := 0; -- Прочий процентный доход, будет реализован позднее по запросу 259909
     ExpSum     NUMBER := RSO_MMARK_MSFO.CalcCosts(objid, objn, calcdate, ContextType); -- Сумма затрат
     gbv        NUMBER := MMARKCOMMON.GetNoteTextMoney(103/*OBJTYPE_MMARKDEAL*/, objn, 8/*MN_GROSS*/, calcdate); -- АС
     EPS        FLOAT  := RSO_MMARK_MSFO.CalcEPS(objid, objn, calcdate, ContextType);
     tk         NUMBER := 0;
     prevdate   DATE   := TO_DATE('01.01.0001', 'DD.MM.YYYY');
     prevpercdt DATE   := TO_DATE('01.01.0001', 'DD.MM.YYYY');
     pastpmsum  NUMBER := 0;
     stat       NUMBER := 0;
     PrcContrID NUMBER := 0;
     costs      NUMBER := 0;
     prize      NUMBER := RSO_MMARK_MSFO.CalcPrize(objid, objn, calcdate, ContextType);
     discount   NUMBER := RSO_MMARK_MSFO.CalcDiscount(objid, objn, calcdate, ContextType);

     LEG   DDL_LEG_DBT%ROWTYPE;
   BEGIN
     SELECT * INTO LEG  FROM DDL_LEG_DBT WHERE T_DEALID = objn;
     IF (gbv IS NULL) THEN
       gbv := 0;
     END IF;
     IF (ExpSum = -1) THEN
       ExpSum := 0;
     END IF;
     IF (prize = -1) THEN
        prize := 0;
     END IF;
     IF (discount = -1) THEN
        discount := 0;
     END IF;

     stat := MMarkCommon.GetDateStepBySymbol(ObjN, 'm', prevdate);
     prevpercdt := prevdate + 1;
     IF (stat = -1) THEN
        prevdate   := LEG.t_Start;
        prevpercdt := prevdate;

        SELECT NVL(SUM(CASE WHEN (pm.t_PartPaymRestAmountMain < pm.t_Amount AND pm.t_PartPaymRestAmountMain > 0) THEN pm.t_PartPaymRestAmountMain ELSE pm.t_Amount END), 0)
        INTO costs
        FROM dpmpaym_dbt pm
        WHERE t_dockind = objid AND t_documentid = objn AND pm.t_PaymStatus = 32000 AND t_valuedate <= calcdate AND t_purpose = 83 /*Затраты*/;

        gbv := LEG.t_Principal + costs;
     END IF;
     tk := calcdate - prevdate;
     pastpmsum := MMARKCOMMON.GetPastPaymSum(objid, objn, prevdate);

     SELECT t_contractid INTO PrcContrID FROM dprccontract_dbt WHERE t_objecttype = 103 AND t_contracttype = 7
       AND t_objectid IN (SELECT t_dealcode FROM ddl_tick_dbt WHERE t_dealid = objn AND t_bofficekind = objid);

     stat := RSI_RSB_PERCENT.Prc_CalcPercentValues(PercIncome, PrcContrID, prevpercdt, calcdate, 0);
     IF (stat = -1) THEN
       PercIncome := 0;
     END IF;

     res := ((gbv - pastpmsum)*(power((1 + (EPS/100)), (tk/365))-1)) - (PercIncome + OthPercInc - ExpSum - costs + discount - prize );

     RETURN res;
   END CalcAdjAS_SALE_def;

   -- Расчет суммы корректировки СС. Алгоритм по-умолчанию для сделок привлечения
   FUNCTION CalcAdjSS_BUY_def(
     objid       IN NUMBER,
     objn        IN NUMBER,
     calcdate    IN DATE,
     ContextType IN NUMBER DEFAULT MMarkConst.CN_CONTEXT_NONE
   )
     RETURN NUMBER
   IS
      bs    NUMBER := 0;
      SP    NUMBER := 0;
      Sperc NUMBER := 0;

      Spk   NUMBER := 0;
      Sok   NUMBER := 0;
      Spp   NUMBER := 0;
      Sop   NUMBER := 0;
      SS    NUMBER := RSO_MMARK_MSFO.CalcSS(objid, objn, calcdate, ContextType);

      TICK  DDL_TICK_DBT%ROWTYPE;
      LEG   DDL_LEG_DBT%ROWTYPE;
   BEGIN
      SELECT * INTO TICK FROM DDL_TICK_DBT WHERE T_BOFFICEKIND = objid AND T_DEALID = objn;
      SELECT * INTO LEG  FROM DDL_LEG_DBT WHERE T_DEALID = objn;
      
      SELECT SUM(T_AMOUNT) INTO Sperc FROM DPMPAYM_DBT WHERE T_DOCKIND = objid AND T_DOCUMENTID = objn AND T_PURPOSE = 11; 
      SP := LEG.t_Principal + Sperc;

      Spk := MMARKCOMMON.GetAccRest(objid, objn, calcdate, MMarkConst.tdr_adjatR, 1, LEG.t_pfi);
      Sok := MMARKCOMMON.GetAccRest(objid, objn, calcdate, MMarkConst.tdr_adjatP, 1, LEG.t_pfi);

      Spp := MMARKCOMMON.GetAccRest(objid, objn, calcdate, MMarkConst.tdr_rev3R, 1, LEG.t_pfi);
      Sop := MMARKCOMMON.GetAccRest(objid, objn, calcdate, MMarkConst.tdr_rev3P, 1, LEG.t_pfi);

      bs := SP + Spk - Sok + Spp - Sop;

      RETURN SS - bs;
   END CalcAdjSS_BUY_def;

   -- Расчет суммы корректировки СС. Алгоритм по-умолчанию для сделок размещения
   FUNCTION CalcAdjSS_SALE_def( 
     objid       IN NUMBER,
     objn        IN NUMBER,
     calcdate    IN DATE,
     ContextType IN NUMBER DEFAULT MMarkConst.CN_CONTEXT_NONE
   )
     RETURN NUMBER
   IS
      bs    NUMBER := 0;
      SR    NUMBER := 0;
      Sperc NUMBER := 0;
      Scom  NUMBER := 0;

      Spk   NUMBER := 0;
      Sok   NUMBER := 0;
      Spp   NUMBER := 0;
      Sop   NUMBER := 0;
      Socr  NUMBER := 0;
      SS    NUMBER := RSO_MMARK_MSFO.CalcSS(objid, objn, calcdate, ContextType);
      AsCategory NUMBER := PM_COMMON.GetObjAttrValue(103/*OBJECTTYPE_MMARKDEAL*/, LPAD(objn, 34, '0'), 5, calcdate);

      TICK  DDL_TICK_DBT%ROWTYPE;
      LEG   DDL_LEG_DBT%ROWTYPE;
   BEGIN
      SELECT * INTO TICK FROM DDL_TICK_DBT WHERE T_BOFFICEKIND = objid AND T_DEALID = objn;
      SELECT * INTO LEG  FROM DDL_LEG_DBT WHERE T_DEALID = objn;
      
      SELECT SUM(T_AMOUNT) INTO Sperc FROM DPMPAYM_DBT WHERE T_DOCKIND = objid AND T_DOCUMENTID = objn AND T_PURPOSE = 11; 
      SELECT NVL(SUM(T_SUM), 0) INTO Scom FROM DDLCOMIS_DBT WHERE T_DOCKIND = objid AND T_DOCID = objn;
      SR := LEG.t_Principal + Sperc + Scom;

      Spk := MMARKCOMMON.GetAccRest(objid, objn, calcdate, MMarkConst.TDR_ADJPLP, 1, LEG.t_pfi);
      Sok := MMARKCOMMON.GetAccRest(objid, objn, calcdate, MMarkConst.tdr_adjplR, 1, LEG.t_pfi);

      IF (AsCategory = 3) THEN

        Spp := MMARKCOMMON.GetAccRest(objid, objn, calcdate, MMarkConst.tdr_rev1R, 1, LEG.t_pfi);
        Sop := MMARKCOMMON.GetAccRest(objid, objn, calcdate, MMarkConst.tdr_rev1P, 1, LEG.t_pfi);
      ELSE IF (AsCategory = 4) THEN
        Spp := MMARKCOMMON.GetAccRest(objid, objn, calcdate, MMarkConst.tdr_rev2R, 1, LEG.t_pfi);
        Sop := MMARKCOMMON.GetAccRest(objid, objn, calcdate, MMarkConst.tdr_rev2P, 1, LEG.t_pfi);
        END IF;
      END IF;

      Socr := MMARKCOMMON.GetNoteTextMoney(103/*OBJTYPE_MMARKDEAL*/, objn, 11/*MN_CURRENT*/, calcdate);
      IF (Socr IS NULL) THEN
        Socr := 0;
      END IF;

      bs := SR + Spk - Sok + Spp - Sop + Socr;

      RETURN SS - bs;
   END CalcAdjSS_SALE_def;

   -- Расчет суммы корректировки процентного дохода по КО ФА. Алгоритм по-умолчанию
   FUNCTION CalcAdjInt_def(
     objid       IN NUMBER,
     objn        IN NUMBER,
     calcdate    IN DATE,
     ContextType IN NUMBER DEFAULT MMarkConst.CN_CONTEXT_NONE
   )
     RETURN NUMBER
   IS
     res        NUMBER := 0;
     AsCategory NUMBER := 0; -- Категория оценки
     PercIncome NUMBER := 0; -- Процентный доход
     OthPercInc NUMBER := 0; -- Прочий процентный доход, будет реализован позднее по запросу 259909
     ExpSum     NUMBER := RSO_MMARK_MSFO.CalcCosts(objid, objn, calcdate, ContextType); -- Сумма затрат
     rez        NUMBER := 0; -- Оценочный резерв
     gbv        NUMBER := RSO_MMARK_MSFO.CalcGBV(objid, objn, calcdate, ContextType); -- ВБС
     EPS        FLOAT  := 0;
     tk         NUMBER := 0;
     CostAdj    NUMBER := 0; -- Корректировка стоимости ФА
     prevdate   DATE   := TO_DATE('01.01.0001', 'DD.MM.YYYY');
     stat       NUMBER := 0;
   BEGIN
     AsCategory := PM_COMMON.GetObjAttrValue(103/*OBJECTTYPE_MMARKDEAL*/, LPAD(objn, 34, '0'), 5, calcdate);

     SELECT t_cost INTO PercIncome FROM ddl_leg_dbt WHERE t_dealid = objn AND t_legid = 1;
     If (ExpSum = -1) THEN
        ExpSum := 0;
     END IF;

     rez := MMARKCOMMON.GetNoteTextMoney(103/*OBJTYPE_MMARKDEAL*/, objn, 11/*MN_CURRENT*/, calcdate);
     IF (rez IS NULL) THEN
       rez := 0;
     END IF;
     IF (AsCategory = 1) THEN
       res := (PercIncome + OthPercInc - ExpSum)*(rez/gbv);
     ELSE
       IF (AsCategory = 2) THEN
         stat := MMarkCommon.GetDateStepBySymbol(ObjN, 'н', prevdate);
         IF (stat = -1) THEN
            SELECT MAX(t_valuedate) INTO prevdate FROM dpmpaym_dbt
              WHERE t_dockind = objid AND t_documentid = objn AND t_purpose = 9 AND t_paymstatus = 32000;
         END IF;
         tk :=   calcdate - prevdate;

         CostAdj := RSO_MMARK_MSFO.CalcAdjAS(objid, objn, calcdate, ContextType);
         EPS     := RSO_MMARK_MSFO.CalcEPS(objid, objn, calcdate, ContextType);

         res := (PercIncome + OthPercInc - ExpSum + CostAdj) - ((gbv - rez)*(power((1 + (EPS/100)), (tk/365))-1));
       ELSE
         RETURN 0;
       END IF;
     END IF;

     RETURN res;
     EXCEPTION WHEN NO_DATA_FOUND THEN RETURN -1;
   END CalcAdjInt_def;

   -- Расчет фин. результата от несущественной модификации ФА. Алгоритм по-умолчанию
   FUNCTION CalcFinResFA_def(
     objid       IN NUMBER,
     objn        IN NUMBER,
     calcdate    IN DATE,
     ContextType IN NUMBER DEFAULT MMarkConst.CN_CONTEXT_NONE
   )
     RETURN NUMBER
   IS
      GBVold NUMBER(32,12) := MMARKCOMMON.GetNoteTextMoney(103/*OBJTYPE_MMARKDEAL*/, objn, 8/*MN_GROSS*/, calcdate); -- ВБС
      GBVnew NUMBER(32,12) := RSO_MMARK_MSFO.CalcGBV(objid, objn, calcdate, ContextType);
   BEGIN
      IF (GBVnew = -1) THEN
         GBVnew := 0;
      END IF;
      IF (GBVold IS NULL) THEN
         GBVold := GBVnew;
      END IF;
      RETURN GBVnew - GBVold;

   END CalcFinResFA_def;

   -- Расчет суммы прочих процентных доходов для учета в ОФР. Алгоритм по-умолчанию
   FUNCTION CalcOthInt_def(
     objid       IN NUMBER,
     objn        IN NUMBER,
     calcdate    IN DATE,
     ContextType IN NUMBER DEFAULT MMarkConst.CN_CONTEXT_NONE,
     operationid IN NUMBER DEFAULT 0,
     stepid      IN NUMBER DEFAULT 0
   )
     RETURN NUMBER
   IS
     Uc         NUMBER := 0; -- Нераспределенная процентная комиссия
     Cr         NUMBER := 0;
     stat       NUMBER := 0;
     stepdate   DATE   := TO_DATE('01.01.0001', 'DD.MM.YYYY');
     dealend    DATE   := TO_DATE('01.01.0001', 'DD.MM.YYYY');
     dealstart  DATE   := TO_DATE('01.01.0001', 'DD.MM.YYYY');
     tk         NUMBER := 0;
     tz         NUMBER := 0;
     uccalc     NUMBER := 0;
     dealgroup  NUMBER := 0;
     pmprncsum  NUMBER := 0;
     pmprncstep NUMBER := 0;
     curprinc   NUMBER := 0;
     LEG   DDL_LEG_DBT%ROWTYPE;

   BEGIN
     Uc := MMARKCOMMON.GetNoteTextMoney(103/*OBJTYPE_MMARKDEAL*/, objn, 12/*MN_RETAINED*/, calcdate);
     IF (Uc = 0 OR Uc IS NULL) THEN
       RETURN -1;
     END IF;
     Uc := ABS(Uc);

     SELECT * INTO LEG  FROM DDL_LEG_DBT WHERE T_DEALID = objn;

     IF (ContextType = MMarkConst.CN_CONTEXT_PM_PRINC_EXEC) THEN
       SELECT t_dealgroup INTO dealgroup FROM DDL_TICK_DBT WHERE T_DEALID = ObjN;
       -- получить номер шага исполнения входящего/исходящего платежа по ОД
       IF ((MMARK_UTL.IsSALE(dealgroup) > 0)) THEN
         pmprncstep := MMarkCommon.GetLastStepBeforeStep(operationid, stepid, 'i');
       ELSE
         pmprncstep := MMarkCommon.GetLastStepBeforeStep(operationid, stepid, 'g');
       END IF;
       IF (pmprncstep IS NULL) THEN
         RETURN -1;
       END IF;
       pmprncsum := MMarkCommon.GetPaymSum(operationid, pmprncstep, calcdate, RSB_Payment.PM_PURP_PRINC_RET);
       IF (pmprncsum = 0) THEN
         RETURN 0;
       END IF;
       curprinc := MMarkCommon.GetAccRest(objid, objn, calcdate, MMarkConst.tdr_mainrest, 1, LEG.t_pfi);
       Cr := Uc * (pmprncsum/(curprinc + pmprncsum));
       RETURN Cr;

     END IF;

     -- получить дату окончания договора
     BEGIN
       SELECT t_start, t_maturity INTO dealstart, dealend FROM DDL_LEG_DBT WHERE T_DEALID = ObjN;
     END;

     stat := MMarkCommon.GetDateStepBySymbol(ObjN, 'щ', stepdate);
     IF (stat = 0)
     THEN
       -- шаг учет отражения расходов по затратам выполнен
       tk := calcdate - stepdate;
     ELSE
       -- иначе получить дату выполнения шага Учет затрат
       stat := MMarkCommon.GetDateStepBySymbol(ObjN, 'ш', stepdate);
       IF (stat = 0)
       THEN
         tk := calcdate - stepdate;
       ELSE
       RETURN -1;
       END IF;
     END IF;

     CASE (RSB_COMMON.GetRegIntValue('MMARK\ТЕСТ_НА_РЫНОЧНОСТЬ'))
       WHEN 1 THEN
         Cr := 1;

         tz := dealend - stepdate;
         IF (tz = 0)
         THEN
           uccalc := Uc;
         ELSE
           uccalc := Uc * (tk/tz);
         END IF;
         Cr := LEAST(Uc, uccalc);

       WHEN 2 THEN
         Cr := Uc * (tk/(dealend - dealstart));

     END CASE;
     RETURN Cr;

   END CalcOthInt_def;

   -- Расчет суммы прочих операционных доходов для учета в ОФР. Алгоритм по-умолчанию
   FUNCTION CalcOthOpInt_def(
     objid       IN NUMBER,
     objn        IN NUMBER,
     calcdate    IN DATE,
     ContextType IN NUMBER DEFAULT MMarkConst.CN_CONTEXT_NONE,
     operationid IN NUMBER DEFAULT 0,
     stepid      IN NUMBER DEFAULT 0
   )
     RETURN NUMBER
   IS
     Uc         NUMBER := 0; -- нераспределенные затраты
     Cr         NUMBER := 0;
     stat       NUMBER := 0;
     stepdate   DATE   := TO_DATE('01.01.0001', 'DD.MM.YYYY');
     dealend    DATE   := TO_DATE('01.01.0001', 'DD.MM.YYYY');
     dealstart  DATE   := TO_DATE('01.01.0001', 'DD.MM.YYYY');
     tk         NUMBER := 0;
     tz         NUMBER := 0;
     uccalc     NUMBER := 0;
     dealgroup  NUMBER := 0;
     pmprncsum  NUMBER := 0;
     pmprncstep NUMBER := 0;
     curprinc   NUMBER := 0;
     LEG   DDL_LEG_DBT%ROWTYPE;

   BEGIN
     Uc := MMARKCOMMON.GetNoteTextMoney(103/*OBJTYPE_MMARKDEAL*/, objn, 13/*MN_OPERATIONAL*/, calcdate);
     IF (Uc = 0 OR Uc IS NULL) THEN
       RETURN -1;
     END IF;
     Uc := ABS(Uc);

     SELECT * INTO LEG  FROM DDL_LEG_DBT WHERE T_DEALID = objn;

     IF (ContextType = MMarkConst.CN_CONTEXT_PM_PRINC_EXEC) THEN
       SELECT t_dealgroup INTO dealgroup FROM DDL_TICK_DBT WHERE T_DEALID = ObjN;
       -- получить номер шага исполнения входящего/исходящего платежа по ОД
       IF ((MMARK_UTL.IsSALE(dealgroup) > 0)) THEN
         pmprncstep := MMarkCommon.GetLastStepBeforeStep(operationid, stepid, 'i');
       ELSE
         pmprncstep := MMarkCommon.GetLastStepBeforeStep(operationid, stepid, 'g');
       END IF;
       IF (pmprncstep IS NULL) THEN
         RETURN -1;
       END IF;
       pmprncsum := MMarkCommon.GetPaymSum(operationid, pmprncstep, calcdate, RSB_Payment.PM_PURP_PRINC_RET);
       IF (pmprncsum = 0) THEN
         RETURN 0;
       END IF;
       curprinc := MMarkCommon.GetAccRest(objid, objn, calcdate, MMarkConst.tdr_mainrest, 1, LEG.t_pfi);
       Cr := Uc * (pmprncsum/(curprinc + pmprncsum));
       RETURN Cr;

     END IF;

     -- получить дату окончания договора
     BEGIN
       SELECT t_start, t_maturity INTO dealstart, dealend FROM DDL_LEG_DBT WHERE T_DEALID = ObjN;
     END;

     stat := MMarkCommon.GetDateStepBySymbol(ObjN, 'щ', stepdate);
     IF (stat = 0)
     THEN
       -- шаг учет отражения расходов по затратам выполнен
       tk := calcdate - stepdate;
     ELSE
       -- иначе получить дату выполнения шага Учет затрат
       stat := MMarkCommon.GetDateStepBySymbol(ObjN, 'ш', stepdate);
       IF (stat = 0)
       THEN
         tk := calcdate - stepdate;
       ELSE
       RETURN -1;
       END IF;
     END IF;

     CASE (RSB_COMMON.GetRegIntValue('MMARK\ТЕСТ_НА_РЫНОЧНОСТЬ'))
       WHEN 1 THEN
         Cr := 1;

         tz := dealend - stepdate;
         IF (tz = 0)
         THEN
           uccalc := Uc;
         ELSE
           uccalc := Uc * (tk/tz);
         END IF;
         Cr := LEAST(Uc, uccalc);

       WHEN 2 THEN
         Cr := Uc * (tk/(dealend - dealstart));

     END CASE;
   RETURN Cr;

   END CalcOthOpInt_def;

   -- Расчет фин. результата от несущественной модификации ФО. Алгоритм по-умолчанию
   FUNCTION CalcFinResFO_def(
     objid       IN NUMBER,
     objn        IN NUMBER,
     calcdate    IN DATE,
     ContextType IN NUMBER DEFAULT MMarkConst.CN_CONTEXT_NONE
   )
      RETURN NUMBER
   IS
      ASold NUMBER(32,12) := MMARKCOMMON.GetNoteTextMoney(103/*OBJTYPE_MMARKDEAL*/, objn, 7/*MN_AMORTIZED*/, calcdate); -- АС
      ASnew NUMBER(32,12) := RSO_MMARK_MSFO.CalcAS(objid, objn, calcdate, ContextType);
   BEGIN
      IF (ASnew = -1) THEN
         ASnew := 0;
      END IF;
      IF (ASold IS NULL) THEN
         ASold := ASnew;
      END IF;
      RETURN ASnew - ASold;
   END CalcFinResFO_def;

   -- Тест на рыночность
   FUNCTION TestMarket_def(
     objid       IN NUMBER,
     objn        IN NUMBER,
     calcdate    IN DATE,
     ContextType IN NUMBER DEFAULT MMarkConst.CN_CONTEXT_NONE
   )
   RETURN NUMBER
   IS
      rps        NUMBER := RSO_MMARK_MSFO.CalcRPS(objid, objn, calcdate, ContextType);
      marketTest NUMBER := RSB_COMMON.GetRegIntValue('MMARK\ТЕСТ_НА_РЫНОЧНОСТЬ');
      mlvl       NUMBER := 0;  --Отклонение ЭПС от РПС
      AsCategory NUMBER := PM_COMMON.GetObjAttrValue(103/*OBJECTTYPE_MMARKDEAL*/, LPAD(objn, 34, '0'), 5, calcdate);
      mtrval     NUMBER; --Значение ставки для теста на рыночность
      mtr        NUMBER := 0; --1 - ставка по договору, 2 - ЭПС
   BEGIN
   
      IF (objid = MMarkConst.DL_IBCDOC) THEN
         mtr := PM_COMMON.GetObjAttrValue(103/*OBJECTTYPE_MMARKDEAL*/, LPAD(objn, 34, '0'), 11, calcdate);
         IF (mtr = 1) THEN
            SELECT t_price / POWER(10, t_point) INTO mtrval FROM ddl_leg_dbt WHERE t_dealid = objn AND t_legid = 1;
         ELSE
            mtrval := RSO_MMARK_MSFO.CalcEPS(objid, objn, calcdate, ContextType);
         END IF;
      ELSIF (objid = MMarkConst.DL_CREDITLN) THEN
         SELECT t_price INTO mtrval FROM ddl_leg_dbt WHERE t_dealid = objn AND t_legid = 1;
         mtr  := 1;
      END IF;
      
      IF (mtr = 1) THEN
         mlvl := RSO_MMARK_MSFO.GetMlvlEx(objid, objn, 12, calcdate, ContextType); --Отклонение СТ от РПС
      ELSE
         mlvl := RSO_MMARK_MSFO.GetMlvlEx(objid, objn, 7, calcdate, ContextType);  --Отклонение ЭПС от РПС
      END IF;
      
      IF (rps = mtrval) THEN
         RETURN 0;
      END IF;
      IF (mlvl = 0) THEN
         RETURN 0;
      END IF;
      
      IF (marketTest = 1 AND objid = MMarkConst.DL_IBCDOC) THEN
         RETURN 1;
      ELSIF (marketTest = 2 AND objid = MMarkConst.DL_IBCDOC) THEN
         --Если ставка по договору, то закончить проверку
         IF (mtr = 1) THEN
            RETURN 1;
         END IF;
         --В параметре ?Ставка для теста на рыночность? установить значение ?Ставка по договору?
         UPDATE DOBJATCOR_DBT
            SET T_ATTRID = 1
         WHERE T_OBJECTTYPE = 103/*OBJTYPE_MMARKDEAL*/
            AND T_OBJECT = LPAD(objn, 34, '0')
            AND T_GROUPID = 11
            AND calcdate BETWEEN T_VALIDFROMDATE AND T_VALIDTODATE;
         --Второй цикл проверки для ставки по договору  
         RETURN RSO_MMARK_MSFO.TestMarket(objid, objn, calcdate, ContextType);
         
      ELSIF (objid = MMarkConst.DL_CREDITLN) THEN
         RETURN 1;         
      ELSE
         RETURN -1;
      END IF;

      RETURN -1;
   END TestMarket_def;

   -- Процедура расчета суммы премии для отражения в ОФР
   FUNCTION CalcPrize_def(
     objid       IN NUMBER,
     objn        IN NUMBER,
     calcdate    IN DATE,
     ContextType IN NUMBER DEFAULT MMarkConst.CN_CONTEXT_NONE,
     operationid IN NUMBER DEFAULT 0,
     stepid      IN NUMBER DEFAULT 0,
     PmAmount    In NUMBER DEFAULT 0
   )
     RETURN NUMBER
   IS
     Pn         NUMBER := 0; -- Премия, подлежащая отражению в ОФР
     Cr         NUMBER := 0;
     stat       NUMBER := 0;
     stepdate   DATE   := TO_DATE('01.01.0001', 'DD.MM.YYYY');
     dealend    DATE   := TO_DATE('01.01.0001', 'DD.MM.YYYY');
     dealstart  DATE   := TO_DATE('01.01.0001', 'DD.MM.YYYY');
     tk         NUMBER := 0;
     tz         NUMBER := 0;
     uccalc     NUMBER := 0;
     dealgroup  NUMBER := 0;
     pmprncsum  NUMBER := 0;
     pmprncstep NUMBER := 0;
     curprinc   NUMBER := 0;
     LEG   DDL_LEG_DBT%ROWTYPE;

   BEGIN
     Pn := MMARKCOMMON.GetNoteTextMoney(103/*OBJTYPE_MMARKDEAL*/, objn, 16/*MN_PRIZE*/, calcdate);
     IF (Pn = 0 OR Pn IS NULL) THEN
       RETURN 0;
     END IF;
     Pn := ABS(Pn);

     SELECT * INTO LEG  FROM DDL_LEG_DBT WHERE T_DEALID = objn;

     IF (ContextType = MMarkConst.CN_CONTEXT_PM_PRINC_EXEC) THEN
       IF (PmAmount = 0) THEN
         SELECT t_dealgroup INTO dealgroup FROM DDL_TICK_DBT WHERE T_DEALID = ObjN;
         -- получить номер шага исполнения входящего/исходящего платежа по ОД
         pmprncstep := MMarkCommon.GetLastStepBeforeStep(operationid, stepid, 'i');
         IF (pmprncstep IS NULL) THEN
           RETURN -1;
         END IF;
         pmprncsum := MMarkCommon.GetPaymSum(operationid, pmprncstep, calcdate, RSB_Payment.PM_PURP_PRINC_RET);
         IF (pmprncsum = 0) THEN
           RETURN 0;
         END IF;
       ELSE
         pmprncsum := PmAmount;
       END IF;
       curprinc := MMarkCommon.GetAccRest(objid, objn, calcdate, MMarkConst.tdr_rights, 1, LEG.t_pfi);
       Cr := Pn * (pmprncsum/curprinc);
       RETURN Cr;

     END IF;

     -- получить дату окончания договора
     BEGIN
       SELECT t_start, t_maturity INTO dealstart, dealend FROM DDL_LEG_DBT WHERE T_DEALID = ObjN;
     END;

     stat := MMarkCommon.GetDateStepBySymbol(ObjN, 'U', stepdate);
     IF (stat = 0)
     THEN
       -- шаг отражения расходов по премии
       tk := calcdate - stepdate;
     ELSE
       -- иначе получить дату выполнения шага исполнения исходящих платежей по ОД
       stat := MMarkCommon.GetDateStepBySymbol(ObjN, 'g', stepdate);
       IF (stat = 0)
       THEN
         tk := calcdate - stepdate;
       ELSE
       RETURN -1;
       END IF;
     END IF;

     tz := dealend - stepdate;
     IF (tz = 0)
     THEN
       uccalc := Pn;
     ELSE
       uccalc := Pn * (tk/tz);
     END IF;
     Cr := LEAST(Pn, uccalc);
   RETURN Cr;

   END CalcPrize_def;

   -- Процедура расчета суммы дисконта для отражения в ОФР
   FUNCTION CalcDiscount_def(
     objid       IN NUMBER,
     objn        IN NUMBER,
     calcdate    IN DATE,
     ContextType IN NUMBER DEFAULT MMarkConst.CN_CONTEXT_NONE,
     operationid IN NUMBER DEFAULT 0,
     stepid      IN NUMBER DEFAULT 0,
     PmAmount    In NUMBER DEFAULT 0
   )
     RETURN NUMBER
   IS
     Dn         NUMBER := 0; -- Дисконт, подлежащая отражению в ОФР
     Cr         NUMBER := 0;
     stat       NUMBER := 0;
     stepdate   DATE   := TO_DATE('01.01.0001', 'DD.MM.YYYY');
     dealend    DATE   := TO_DATE('01.01.0001', 'DD.MM.YYYY');
     dealstart  DATE   := TO_DATE('01.01.0001', 'DD.MM.YYYY');
     tk         NUMBER := 0;
     tz         NUMBER := 0;
     uccalc     NUMBER := 0;
     dealgroup  NUMBER := 0;
     pmprncsum  NUMBER := 0;
     pmprncstep NUMBER := 0;
     curprinc   NUMBER := 0;
     LEG   DDL_LEG_DBT%ROWTYPE;

   BEGIN
     Dn := MMARKCOMMON.GetNoteTextMoney(103/*OBJTYPE_MMARKDEAL*/, objn, 17/*MN_DISCOUNT*/, calcdate);
     IF (Dn = 0 OR Dn IS NULL) THEN
       RETURN 0;
     END IF;
     Dn := ABS(Dn);

     SELECT * INTO LEG  FROM DDL_LEG_DBT WHERE T_DEALID = objn;

     IF (ContextType = MMarkConst.CN_CONTEXT_PM_PRINC_EXEC) THEN
       IF (PmAmount = 0) THEN
         SELECT t_dealgroup INTO dealgroup FROM DDL_TICK_DBT WHERE T_DEALID = ObjN;
         -- получить номер шага исполнения входящего/исходящего платежа по ОД
         pmprncstep := MMarkCommon.GetLastStepBeforeStep(operationid, stepid, 'i');
         IF (pmprncstep IS NULL) THEN
           RETURN -1;
         END IF;
         pmprncsum := MMarkCommon.GetPaymSum(operationid, pmprncstep, calcdate, RSB_Payment.PM_PURP_PRINC_RET);
         IF (pmprncsum = 0) THEN
           RETURN 0;
         END IF;
       ELSE
         pmprncsum := PmAmount;
       END IF;

       curprinc := MMarkCommon.GetAccRest(objid, objn, calcdate, MMarkConst.tdr_rights, 1, LEG.t_pfi);
       Cr := Dn * (pmprncsum/curprinc);
       RETURN Cr;

     END IF;

     -- получить дату окончания договора
     BEGIN
       SELECT t_start, t_maturity INTO dealstart, dealend FROM DDL_LEG_DBT WHERE T_DEALID = ObjN;
     END;

     stat := MMarkCommon.GetDateStepBySymbol(ObjN, 'U', stepdate);
     IF (stat = 0)
     THEN
       -- шаг отражения расходов по премии
       tk := calcdate - stepdate;
     ELSE
       -- иначе получить дату выполнения шага исполнения исходящих платежей по ОД
       stat := MMarkCommon.GetDateStepBySymbol(ObjN, 'g', stepdate);
       IF (stat = 0)
       THEN
         tk := calcdate - stepdate;
       ELSE
       RETURN -1;
       END IF;
     END IF;

     tz := dealend - stepdate;
     IF (tz = 0)
     THEN
       uccalc := Dn;
     ELSE
       uccalc :=Dn * (tk/tz);
     END IF;
     Cr := LEAST(Dn, uccalc);
   RETURN Cr;

   END CalcDiscount_def;

       --Расчет амортизации корректировок хеджирования
  FUNCTION CalcHedgAdjAm_def(
    hedginstrid IN NUMBER,
    calcdate    IN DATE,
    ContextType IN NUMBER DEFAULT MMarkConst.CN_CONTEXT_NONE
  )
    RETURN NUMBER
   IS
     OK         NUMBER := 0; -- Сумма амортизации корректировки от хеджирования
     Cr         NUMBER := 0;
     stat       NUMBER := 0;
     stepdate   DATE   := TO_DATE('01.01.0001', 'DD.MM.YYYY');
     tk         NUMBER := 0;
     tz         NUMBER := 0;
     uccalc     NUMBER := 0;
     negative   NUMBER := 0;
     LEG   DDL_LEG_DBT%ROWTYPE;
     TICK  DDL_TICK_DBT%ROWTYPE;
     HEDGINSTR DDLHDGRELATION_DBT%ROWTYPE;
     CorCat     VARCHAR2(26);
     CorAccount VARCHAR2(26);
   BEGIN
     SELECT * INTO HEDGINSTR FROM DDLHDGRELATION_DBT WHERE T_ID = hedginstrid;
     SELECT * INTO LEG  FROM DDL_LEG_DBT WHERE T_LEGKIND = 0 AND T_DEALID = HEDGINSTR.T_OBJID AND T_LEGID = 1;
     SELECT * INTO TICK FROM DDL_TICK_DBT WHERE T_DEALID = HEDGINSTR.T_OBJID;

     IF (MMark_UTL.IsSALE(MMark_UTL.GetOperationGroup(TICK.t_DealType)) > 0) THEN
        --OK := MMarkCommon.GetAccRest(MMarkConst.DL_IBCDOC, HEDGINSTR.T_DEALID, calcdate, MMarkConst.tdr_hedgadjplR, 1, 0);
        CorAccount := MMarkCommon.GetAccountNumber(HEDGINSTR.T_OBJID, MMarkConst.tdr_hedgadjplR, calcdate, TO_DATE('31.12.9999', 'dd.mm.yyyy'), 10000 + hedginstrid);
        OK := RSB_ACCOUNT.restall (CorAccount, 1, 0, calcdate);
        IF (OK = 0) THEN
            CorAccount := MMarkCommon.GetAccountNumber(HEDGINSTR.T_OBJID, MMarkConst.tdr_hedgadjplP, calcdate, TO_DATE('31.12.9999', 'dd.mm.yyyy'), 10000 + hedginstrid);
            OK := RSB_ACCOUNT.restall (CorAccount, 1, 0, calcdate);
        END IF;
     ELSE
        CorAccount := MMarkCommon.GetAccountNumber(HEDGINSTR.T_OBJID, MMarkConst.tdr_hedgadjatR, calcdate, TO_DATE('31.12.9999', 'dd.mm.yyyy'), 10000 + hedginstrid);
        OK := RSB_ACCOUNT.restall (CorAccount, 1, 0, calcdate);
        IF (OK = 0) THEN
            CorAccount := MMarkCommon.GetAccountNumber(HEDGINSTR.T_OBJID, MMarkConst.tdr_hedgadjatP, calcdate, TO_DATE('31.12.9999', 'dd.mm.yyyy'), 10000 + hedginstrid);
            OK := RSB_ACCOUNT.restall (CorAccount, 1, 0, calcdate);
        END IF;
        OK := -OK;
     END IF;

     IF (OK < 0) THEN
        negative := 1;
     END IF;

     OK := ABS(OK);

     stat := MMarkCommon.GetDateStepBySymbol(HEDGINSTR.T_OBJID, 'Ж', stepdate);
     IF (stat = 0 and stepdate >= HEDGINSTR.T_ENDDATE)
     THEN
       -- шаг отражения расходов по премии
       tk := calcdate - stepdate;
       tz := LEG.T_MATURITY - stepdate;
     ELSE
       tk := calcdate - HEDGINSTR.T_ENDDATE;
       tz := LEG.T_MATURITY - HEDGINSTR.T_ENDDATE;
     END IF;

     IF (tz = 0)
     THEN
       uccalc := OK;
     ELSE
       uccalc := OK * (tk/tz);
     END IF;
     Cr := LEAST(OK, uccalc);

     IF (negative = 1) THEN
        Cr := -Cr;
     END IF;

   RETURN Cr;

   END CalcHedgAdjAm_def;

END RSI_MMARK_MSFO;
/