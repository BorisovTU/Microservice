CREATE OR REPLACE PACKAGE BODY RSB_SPREPFUN IS

  g_BPP_ACCOUNT_METHOD NUMBER := NULL;
  g_PAIR_OVER_ACC     NUMBER := NULL;

  FUNCTION BPP_ACCOUNT_METHOD RETURN NUMBER DETERMINISTIC
  IS
  BEGIN
    IF g_BPP_ACCOUNT_METHOD IS NULL THEN
      g_BPP_ACCOUNT_METHOD := Rsb_Common.GetRegIntValue('SECUR\СПОСОБ ВЕДЕНИЯ СЧЕТОВ БПП');
    END IF;
    RETURN g_BPP_ACCOUNT_METHOD;
  END; 

  FUNCTION PAIR_OVER_ACC RETURN NUMBER DETERMINISTIC
  IS
  BEGIN
    IF g_PAIR_OVER_ACC IS NULL THEN
      g_PAIR_OVER_ACC := 0;
      
      IF Rsb_Common.GetRegFlagValue('SECUR\ПАРНОСТЬ СЧЕТОВ ПЕРЕОЦЕНКИ') = 'X' THEN
        g_PAIR_OVER_ACC := 1;
      END IF;
    END IF;
    RETURN g_PAIR_OVER_ACC;
  END;


  PROCEDURE AddRepError(pMessage IN VARCHAR2)
  IS
    v_SessionID NUMBER := 0;
    v_RepKind   NUMBER := 0;
  BEGIN

    IF g_SessionID IS NOT NULL THEN
      v_SessionID := g_SessionID;
    END IF;

    IF g_RepKind IS NOT NULL THEN
      v_RepKind := g_RepKind;
    END IF;


    INSERT INTO DARNUREPERR_DBT VALUES (v_SessionID, v_RepKind, pMessage);
  END;

  --Скопировано из RSB_FIINSTR.GetRateByMP
  FUNCTION FindRateByMP(p_FromFI       IN NUMBER,
                        p_ToFI         IN NUMBER,
                        p_RateType     IN NUMBER, --Тип курса
                        p_MarketID     IN NUMBER, --Торговая площадка
                        p_Section      IN NUMBER, --Секция торговой площадки
                        p_RateRec      OUT DRATEDEF_DBT%ROWTYPE ) RETURN NUMBER IS
  BEGIN

   SELECT * INTO p_RateRec
   FROM (SELECT RD.*
         FROM  DRATEDEF_DBT RD
         WHERE RD.T_MARKET_PLACE = p_MarketID
           AND RD.T_TYPE = p_RateType
           AND RD.T_SECTION = (CASE WHEN p_Section = 0 THEN RD.T_SECTION ELSE p_Section END)
           AND ((    RD.T_OTHERFI = p_FromFI
                 AND RD.T_FIID =    p_ToFI  )
               OR
               (    RD.T_OTHERFI = p_ToFI
                AND RD.T_FIID =    p_FromFI ))
           ORDER BY T_FIID, T_OTHERFI, T_TYPE, T_MARKET_PLACE, T_SECTION) sel
           WHERE ROWNUM =1;

    RETURN 0;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
         RETURN 1;
  END;

  --Скопировано из RSB_FIINSTR.GetDominantRate
  FUNCTION FindDominantRate( p_FromFI   IN  NUMBER,
                             p_ToFI     IN  NUMBER,
                             p_RateRec  OUT dratedef_dbt%ROWTYPE
                          ) RETURN NUMBER
  IS
  BEGIN

   SELECT * INTO p_RateRec
   FROM (SELECT rd.*
           FROM dratedef_dbt rd
          WHERE rd.t_IsDominant = 'X'
            AND (   (    rd.t_OtherFI = p_FromFI
                     AND rd.t_FIID =    p_ToFI  )
                 OR (    rd.t_OTHERFI = p_ToFI
                     AND rd.t_FIID =    p_FromFI ))
    ORDER BY t_FIID, t_OtherFI, t_Type) sel
    WHERE ROWNUM = 1;

    RETURN 0;
  EXCEPTION
    WHEN OTHERS THEN
         RETURN 1;
  END;

  FUNCTION FindRateByType( p_FromFI   IN  NUMBER,
                           p_ToFI     IN  NUMBER,
                           p_RateType IN  NUMBER, --Тип курса
                           p_RateRec  OUT dratedef_dbt%ROWTYPE
                         ) RETURN NUMBER
  IS
  BEGIN

   SELECT * INTO p_RateRec
   FROM (SELECT rd.*
           FROM dratedef_dbt rd
          WHERE rd.t_Type = p_RateType
            AND (   (    rd.t_OtherFI = p_FromFI
                     AND rd.t_FIID =    p_ToFI  )
                 OR (    rd.t_OTHERFI = p_ToFI
                     AND rd.t_FIID =    p_FromFI ))
    ORDER BY t_FIID, t_OtherFI, t_Type) sel
    WHERE ROWNUM = 1;

    RETURN 0;
  EXCEPTION
    WHEN OTHERS THEN
         RETURN 1;
  END;


  --Аналог ПолучитьКурс в макросах
  FUNCTION GetCourse(p_RateRec  OUT DRATEDEF_DBT%ROWTYPE, 
                     p_ToFI     IN NUMBER, 
                     p_FromFI   IN NUMBER, 
                     p_RateType IN NUMBER DEFAULT 0, 
                     p_MarketID IN NUMBER DEFAULT -1, 
                     p_Section  IN NUMBER DEFAULT 0) RETURN NUMBER
  AS
    v_err NUMBER := 0;
  BEGIN

    IF p_RateType > 0 THEN
      IF p_MarketID <= 0 THEN
        v_err := FindRateByType(p_FromFI, p_ToFI, p_RateType, p_RateRec);
        IF v_err <> 0 THEN
          --если курс не найден, то пытаемся сменить базу котировки
          v_err := FindRateByType(p_ToFI, p_FromFI, p_RateType, p_RateRec);
          IF v_err = 0 THEN
            p_RateRec.t_IsInverse := (CASE WHEN p_RateRec.t_IsInverse = 'X' THEN CHR(0) ELSE 'X' END);
          END IF;
        END IF;
      ELSE
        v_err := FindRateByMP(p_FromFI, p_ToFI, p_RateType, p_MarketID, p_Section, p_RateRec );
        IF v_err <> 0 THEN
          v_err := FindRateByMP(p_ToFI, p_FromFI, p_RateType, p_MarketID, p_Section, p_RateRec );
          IF v_err = 0 THEN
            p_RateRec.t_IsInverse := (CASE WHEN p_RateRec.t_IsInverse = 'X' THEN CHR(0) ELSE 'X' END);
          END IF;
        END IF;
      END IF;

    ELSE
      v_err := FindDominantRate(p_FromFI, p_ToFI, p_RateRec);
      IF v_err <> 0 THEN
        v_err := FindDominantRate(p_ToFI, p_FromFI, p_RateRec);
        IF v_err = 0 THEN
          p_RateRec.t_IsInverse := (CASE WHEN p_RateRec.t_IsInverse = 'X' THEN CHR(0) ELSE 'X' END);
        END IF;
      END IF;
    END IF;

    RETURN v_err;
  END;

  --Аналог ПолучитьПрямойКурсПоТипу в макосах
  FUNCTION GetDirectRateByType(p_RateRec OUT DRATEDEF_DBT%ROWTYPE, 
                               p_RateDate IN DATE, 
                               p_ToFI IN NUMBER, 
                               p_FromFI IN NUMBER, 
                               p_RateType IN NUMBER) RETURN NUMBER 
  AS 
    v_RateID NUMBER := 0;
    v_err NUMBER := 0;
  BEGIN
    BEGIN
      SELECT t_rateid INTO v_RateID
        FROM (SELECT t_rateid 
                  FROM (SELECT rate.t_rateid, rate.t_sincedate 
                          FROM dratedef_dbt rate 
                         WHERE rate.t_otherfi = p_FromFI
                           AND rate.t_FIID    = p_ToFI
                           AND rate.t_type    = p_RateType
                           AND t_sincedate    = (SELECT MAX (t_sincedate) 
                                                   FROM dratedef_dbt 
                                                  WHERE     t_otherfi = rate.t_otherfi 
                                                        AND t_type    = rate.t_type  
                                                        AND t_sincedate <= p_RateDate
                                                ) 
                        UNION 
                          SELECT r.t_rateid, h.t_sincedate 
                            FROM dratehist_dbt h, dratedef_dbt r 
                           WHERE     r.t_rateid    = h.t_rateid 
                                 AND r.t_otherfi   = p_FromFI
                                 AND r.t_FIID      = p_ToFI
                                 AND r.t_type      = p_RateType
                                 AND h.t_sincedate = ( SELECT MAX (h2.t_sincedate)                             
                                                         FROM dratehist_dbt h2, dratedef_dbt r2 
                                                        WHERE r2.t_rateid  = h2.t_rateid 
                                                          AND r2.t_otherfi = r.t_otherfi 
                                                          AND r2.t_type    = r.t_type  
                                                          AND h2.t_sincedate <= p_RateDate
                                                     ) 
                       ) 
              ORDER BY t_sincedate DESC 
             ) 
       WHERE ROWNUM = 1; 

      EXCEPTION
        WHEN OTHERS THEN v_err := 1;
    END;

    IF v_RateID > 0 THEN
      SELECT * INTO p_RateRec
        FROM dratedef_dbt
        WHERE t_RateID = v_RateID;
    END IF;

    RETURN v_err;
  END;

  --Аналог ПолучитьКурсПоТипу из макроса
  FUNCTION GetRateByType(p_RateRec OUT DRATEDEF_DBT%ROWTYPE, 
                         p_RateDate IN DATE, 
                         p_ToFI IN NUMBER, 
                         p_FromFI IN NUMBER, 
                         p_RateType IN NUMBER) RETURN NUMBER
  AS
    v_err NUMBER := 0;
  BEGIN

    v_err := GetDirectRateByType(p_RateRec, p_RateDate, p_ToFI, p_FromFI, p_RateType);
    IF v_err <> 0 THEN
      v_err := GetDirectRateByType(p_RateRec, p_RateDate, p_FromFI, p_ToFI, p_RateType);
      IF v_err = 0 THEN
        p_RateRec.t_IsInverse := (CASE WHEN p_RateRec.t_IsInverse = 'X' THEN CHR(0) ELSE 'X' END);
      END IF;
    END IF;

    RETURN v_err;
  END;

  --Аналог ПолучитьЗначениеКурса из макроса
  FUNCTION GetRateValue(p_RateRec IN OUT DRATEDEF_DBT%ROWTYPE, p_SinceDate IN DATE) RETURN NUMBER
  AS
    v_RateHist DRATEHIST_DBT%ROWTYPE;
    v_find NUMBER := 1;
  BEGIN
    IF( p_RateRec.t_SinceDate > p_SinceDate ) THEN 
    -- Записать в структуру курса вместо текущего курса значение курса на дату Date
      BEGIN
        SELECT * INTO v_RateHist
          FROM dratehist_dbt
        WHERE t_RateID = p_RateRec.t_RateID
          AND t_SinceDate <= p_SinceDate
          AND ROWNUM = 1
        ORDER BY t_SinceDate DESC;
      EXCEPTION
          WHEN OTHERS THEN v_find := 0;
      END;
      IF v_find <> 0 THEN
        p_RateRec.t_IsInverse := v_RateHist.t_IsInverse;   
        p_RateRec.t_Rate      := v_RateHist.t_Rate;        
        p_RateRec.t_Scale     := v_RateHist.t_Scale;       
        p_RateRec.t_Point     := v_RateHist.t_Point;       
        p_RateRec.t_InputDate := v_RateHist.t_InputDate;   
        p_RateRec.t_InputTime := v_RateHist.t_InputTime;   
        p_RateRec.t_Oper      := v_RateHist.t_Oper;        
        p_RateRec.t_SinceDate := v_RateHist.t_SinceDate;
      END IF;
    END IF;

    RETURN (CASE WHEN v_find = 1 THEN 0 ELSE 1 END);
  END;

  FUNCTION GetCurrencyConvertErrorMsg(p_fiidFrom IN NUMBER, p_fiidTo IN NUMBER, p_ADate IN DATE) RETURN VARCHAR2
  AS
  BEGIN
    RETURN 'Не могу сконвертировать сумму из ' || GetISO_Number(p_fiidFrom) || ' в ' || GetISO_Number(p_fiidTo) || ' на ' || TO_CHAR(p_ADate,'DD.MM.YYYY');
  END;

  --Аналог GetRateValueOnDate из макроса
  FUNCTION GetRateValueOnDate(p_RateRec IN DRATEDEF_DBT%ROWTYPE,
                              p_ADate IN DATE, 
                              p_FromFI IN NUMBER, 
                              p_ToFI IN NUMBER, 
                              p_NeedSayError IN NUMBER, 
                              p_Error OUT NUMBER, 
                              p_SinceDate OUT DATE) RETURN NUMBER
  AS

    v_K NUMBER := 0;
    
    v_IndexNom davoiriss_dbt.t_IndexNom%type := CHR(0);
    v_FaceValue NUMBER;

    v_RateDef DRATEDEF_DBT%ROWTYPE;

  BEGIN

    p_Error := 0;

    v_RateDef := p_RateRec;

    IF GetRateValue(v_RateDef, p_ADate) = 0 AND v_RateDef.t_OtherFI = p_FromFI THEN
      p_SinceDate := v_RateDef.t_SinceDate;

      IF v_RateDef.t_IsInverse = 'X' THEN
        --Обратная коировка
        v_K := v_RateDef.t_Rate / v_RateDef.t_Scale / POWER(10, v_RateDef.t_Point);

        IF v_K <> 0 THEN
          v_K := 1.0 / v_K;
        END IF;

      ELSIF v_RateDef.t_IsRelative = 'X' THEN
        --Относительная цена
        BEGIN
          SELECT t_IndexNom INTO v_IndexNom
            FROM davoiriss_dbt
           WHERE t_FIID = p_FromFI;

          EXCEPTION
            WHEN OTHERS THEN v_IndexNom := CHR(0);
        END;

        v_FaceValue := RSI_RSB_FIInstr.FI_GetNominalOnDate(p_FromFI, p_ADate);
        
        IF  v_FaceValue > 0 THEN
          v_K := v_RateDef.t_Rate / v_RateDef.t_Scale / POWER(10, v_RateDef.t_Point) / 100.0 * v_FaceValue;
        ELSE
          IF v_IndexNom = 'X' THEN
            BEGIN
              SELECT t_FaceValue INTO v_FaceValue
                FROM dv_fi_facevalue_hist
               WHERE t_FIID = p_FromFI
                 AND t_BegDate <= p_ADate
               ORDER BY t_BegDate DESC;
        
              IF  v_FaceValue > 0 THEN
                 v_K := v_RateDef.t_Rate / v_RateDef.t_Scale / POWER(10, v_RateDef.t_Point) / 100.0 * v_FaceValue;
              END IF;
            EXCEPTION
              WHEN OTHERS THEN NULL;
            END;
          END IF;
        END IF;
      ELSE
        v_K := v_RateDef.t_Rate / v_RateDef.t_Scale / POWER(10, v_RateDef.t_Point);
      END IF;
    ELSE
      p_Error := 1;
    END IF;

    IF p_Error <> 0 AND p_NeedSayError <> 0 THEN
      AddRepError(GetCurrencyConvertErrorMsg(p_FromFI, p_ToFI, p_ADate));
    END IF;

    RETURN v_K;
  END;


  --Аналог GetRateOnDate в макросах
  FUNCTION GetRateOnDate(p_ADate IN DATE, 
                         p_FromFI IN NUMBER, 
                         p_ToFI IN NUMBER, 
                         p_NeedSayError IN NUMBER, 
                         p_RateType IN NUMBER, 
                         p_Error OUT NUMBER, 
                         p_SinceDate OUT DATE, 
                         p_MarketID IN NUMBER, 
                         p_Section IN NUMBER) RETURN NUMBER
  AS
    v_RateRec DRATEDEF_DBT%ROWTYPE;
    v_K NUMBER := 0.0;
  BEGIN

    p_Error := 0;

    IF p_FromFI = p_ToFI THEN
      p_SinceDate := TO_DATE('01.01.0001','DD.MM.YYYY');
      RETURN 1.0;
    END IF;

    IF p_RateType <= 0 THEN
      --Если тип курса не задан, то берем основной
      p_Error := GetCourse(v_RateRec, p_ToFI, p_FromFI);
    ELSE

      IF p_MarketID > 0 THEN
         p_Error := GetCourse(v_RateRec, p_ToFI, p_FromFI, p_RateType, p_MarketID, p_Section);
         IF p_Error <> 0 THEN
           IF p_Section > 0 THEN
             p_Error := GetCourse(v_RateRec, p_ToFI, p_FromFI, p_RateType, p_MarketID, 0);
           END IF;

           IF p_Error <> 0 THEN
             p_Error := GetRateByType(v_RateRec, p_ADate, p_ToFI, p_FromFI, p_RateType);
           END IF;
         END IF;
      ELSE
        p_Error := GetRateByType(v_RateRec, p_ADate, p_ToFI, p_FromFI, p_RateType);
      END IF;
    END IF;

    IF p_Error = 0 THEN
      v_K := GetRateValueOnDate(v_RateRec, p_ADate, p_FromFI, p_ToFI, p_NeedSayError, p_Error, p_SinceDate );
    ELSE
      p_Error := 1;
    END IF;

    IF p_Error <> 0 AND p_NeedSayError <> 0 THEN
      AddRepError(GetCurrencyConvertErrorMsg(p_FromFI, p_ToFI, p_ADate));
    END IF;

    RETURN v_K;
  END;

  --Аналог SmartConvertSum из макроса
  FUNCTION SmartConvertSum(p_sumTo OUT NUMBER, 
                           p_sumFrom IN NUMBER, 
                           p_sinceDate IN DATE, 
                           p_fiidFrom IN NUMBER,
                           p_fiidTo IN NUMBER, 
                           p_SayError IN NUMBER DEFAULT 0) RETURN NUMBER
  AS
  BEGIN
     p_sumTo := 0;

     IF p_fiidFrom = p_fiidTo OR p_sumFrom = 0 THEN
       p_sumTo := p_sumFrom;
     ELSE
       p_sumTo := RSI_RSB_FIINSTR.ConvSum(p_sumFrom, p_fiidFrom, p_fiidTo, p_sinceDate, 1);
      
       IF p_sumTo IS NULL OR p_sumTo = 0 THEN 
          p_sumTo := 0;
          IF p_SayError <> 0 THEN
             AddRepError(GetCurrencyConvertErrorMsg(p_fiidFrom, p_fiidTo, p_sinceDate));
          END IF;
           
          RETURN 1;
       END IF;
     END IF;

     RETURN 0;
  END;

  FUNCTION SmartConvertSum_Ex(p_sumFrom IN NUMBER, 
                              p_sinceDate IN DATE, 
                              p_fiidFrom IN NUMBER,
                              p_fiidTo IN NUMBER, 
                              p_SayError IN NUMBER DEFAULT 0) RETURN NUMBER
  AS
    v_err   NUMBER := 0;
    v_sumTo NUMBER := 0;
  BEGIN

    v_err := SmartConvertSum(v_sumTo, 
                             p_sumFrom, 
                             p_sinceDate, 
                             p_fiidFrom,
                             p_fiidTo, 
                             p_SayError);

    IF v_err <> 0 THEN
      v_sumTo := 0;
    END IF;

    RETURN v_sumTo;
  END;


  --Аналог ConvSumDbl из макроса
  FUNCTION ConvSumDbl(p_SumToDbl OUT NUMBER, 
                      p_SumFromDbl IN NUMBER, 
                      p_ADate IN DATE, 
                      p_FromFI IN NUMBER, 
                      p_ToFI IN NUMBER,
                      p_NeedSayError IN NUMBER, 
                      p_RateType IN NUMBER, 
                      p_SinceDate IN OUT DATE, 
                      p_MarketID IN NUMBER DEFAULT -1, 
                      p_Section IN NUMBER DEFAULT 0) RETURN NUMBER
  AS
    v_K NUMBER := 0.0;
    v_err NUMBER := 0;
    v_Inverse NUMBER := 0;
  BEGIN

    p_SumToDbl := 0;

    v_K := GetRateOnDate(p_ADate, p_FromFI, p_ToFI, 0, p_RateType, v_err, p_SinceDate, p_MarketID, p_Section);
    IF v_err <> 0 THEN
      v_Inverse := 1;
      v_K := GetRateOnDate(p_ADate, p_ToFI, p_FromFI, 0, p_RateType, v_err, p_SinceDate, p_MarketID, p_Section );
    END IF;

    IF v_err = 0 THEN
      IF v_Inverse = 1 THEN
        IF v_K <> 0 THEN
          p_SumToDbl := p_SumFromDbl / v_K;
        END IF;
      ELSE
        p_SumToDbl := p_SumFromDbl * v_K;
      END IF;
    END IF;

    IF v_err <> 0 AND p_NeedSayError <> 0 THEN
      AddRepError(GetCurrencyConvertErrorMsg(p_FromFI, p_ToFI, p_ADate));
    END IF;

    RETURN v_err;
  END;

  --Аналог ConvSumCrossDbl из макроса
  FUNCTION ConvSumCrossDbl(p_SumToDbl OUT NUMBER, 
                           p_SumFromDbl IN NUMBER, 
                           p_ADate IN DATE, 
                           p_FromFI IN NUMBER, 
                           p_ToFI IN NUMBER,
                           p_NeedSayError IN NUMBER, 
                           p_RateType IN NUMBER, 
                           p_SinceDate IN OUT DATE, 
                           p_MarketID IN NUMBER DEFAULT -1, 
                           p_Section IN NUMBER DEFAULT 0) RETURN NUMBER
  AS
    v_SumFIFrom NUMBER := 0.0; 
    v_SumFITo   NUMBER := 0.0;

    v_err NUMBER := 0;

  BEGIN

    p_SumToDbl := 0.0;

    IF ConvSumDbl(v_SumFIFrom, 1, p_ADate, p_FromFI, RSI_RSB_FIInstr.NATCUR, 0, p_RateType, p_SinceDate, p_MarketID, p_Section) = 0 
       AND ConvSumDbl(v_SumFITo, 1, p_ADate, p_ToFI, RSI_RSB_FIInstr.NATCUR, 0, p_RateType, p_SinceDate, p_MarketID, p_Section) = 0 THEN

       IF v_SumFITo <> 0 THEN
         p_SumToDbl := p_SumFromDbl * v_SumFIFrom / v_SumFITo;
       END IF;
    ELSE
      v_err := 1;
    END IF;

    IF v_err <> 0 AND p_NeedSayError <> 0 THEN
      AddRepError(GetCurrencyConvertErrorMsg(p_FromFI, p_ToFI, p_ADate));
    END IF;

    RETURN v_err;
  END;

  --Аналог SmartConvertSumDbl из макроса
  FUNCTION SmartConvertSumDbl(  p_SumToDbl OUT NUMBER, 
                                p_SumFromDbl IN NUMBER, 
                                p_ADate IN DATE, 
                                p_FromFI IN NUMBER, 
                                p_ToFI IN NUMBER,
                                p_NeedSayError IN NUMBER, 
                                p_RateType IN NUMBER, 
                                p_SinceDate IN OUT DATE, 
                                p_MarketID IN NUMBER, 
                                p_Section IN NUMBER, 
                                p_ErrorMes OUT VARCHAR2) RETURN NUMBER
  AS
    v_err NUMBER := 0;
  BEGIN

    p_SumToDbl := 0.0;
    p_ErrorMes := '';

    IF p_FromFI = p_ToFI OR p_SumFromDbl = 0 THEN
      p_SumToDbl := p_SumFromDbl;
    ELSE

      IF ConvSumDbl(p_SumToDbl, p_SumFromDbl, p_ADate, p_FromFI, p_ToFI, 0, p_RateType, p_SinceDate, p_MarketID, p_Section) <> 0 THEN --Не нашли
        IF ConvSumCrossDbl(p_SumToDbl, p_SumFromDbl, p_ADate, p_FromFI, p_ToFI, 0, p_RateType, p_SinceDate, p_MarketID, p_Section) <> 0 THEN --Не нашли
          p_ErrorMes := GetCurrencyConvertErrorMsg(p_FromFI, p_ToFI, p_ADate);
          IF p_NeedSayError <> 0 THEN
            AddRepError(p_ErrorMes);
          END IF;
          v_err := 1;
        END IF;
      END IF;

    END IF;

    RETURN v_err;
  END;

  FUNCTION SmartConvertSumDbl_Ex(  p_SumToDbl OUT NUMBER, 
                                   p_SumFromDbl IN NUMBER, 
                                   p_ADate IN DATE, 
                                   p_FromFI IN NUMBER, 
                                   p_ToFI IN NUMBER,
                                   p_NeedSayError IN NUMBER) RETURN NUMBER
  AS
    v_SinceDate DATE;
    v_ErrorMes VARCHAR2(1000);
  BEGIN
    RETURN SmartConvertSumDbl(p_SumToDbl, 
                              p_SumFromDbl, 
                              p_ADate, 
                              p_FromFI, 
                              p_ToFI,
                              p_NeedSayError, 
                              0, 
                              v_SinceDate, 
                              -1, 
                              0, 
                              v_ErrorMes);
  END;

  FUNCTION SmartConvertSumDbl_Ex2( p_SumFromDbl IN NUMBER, 
                                   p_ADate IN DATE, 
                                   p_FromFI IN NUMBER, 
                                   p_ToFI IN NUMBER,
                                   p_NeedSayError IN NUMBER) RETURN NUMBER
  AS
    v_SinceDate DATE;
    v_ErrorMes VARCHAR2(1000);
    v_SumToDbl NUMBER := 0;
    v_err      NUMBER := 0; 
  BEGIN
    v_err := SmartConvertSumDbl(v_SumToDbl, 
                                p_SumFromDbl, 
                                p_ADate, 
                                p_FromFI, 
                                p_ToFI,
                                p_NeedSayError, 
                                0, 
                                v_SinceDate, 
                                -1, 
                                0, 
                                v_ErrorMes);
    IF v_err <> 0 THEN
      v_SumToDbl := 0;
    END IF;

    RETURN v_SumToDbl;
  END;

  --Аналог GetRateOnDateCrossDbl
  FUNCTION GetRateOnDateCrossDbl(p_ADate IN DATE, 
                                 p_FromFI IN NUMBER, 
                                 p_ToFI IN NUMBER,
                                 p_NeedSayError IN NUMBER, 
                                 p_RateType IN NUMBER DEFAULT 0, 
                                 p_Error OUT NUMBER, 
                                 p_SinceDate OUT DATE,
                                 p_MarketID IN NUMBER DEFAULT -1,
                                 p_Section IN NUMBER DEFAULT 0) RETURN NUMBER
  AS
    v_K NUMBER := 0.0;
  BEGIN
    p_Error := 0;

    IF p_FromFI = p_ToFI THEN
      v_K := 1.0;
    ELSE
      IF ConvSumDbl(v_K, 1.0, p_ADate, p_FromFI, p_ToFI, 0, p_RateType, p_SinceDate, p_MarketID, p_Section) <> 0 THEN --Не нашли
        IF ConvSumCrossDbl(v_K, 1.0, p_ADate, p_FromFI, p_ToFI, 0, p_RateType, p_SinceDate, p_MarketID, p_Section) <> 0 THEN --Не нашли
          IF p_NeedSayError <> 0 THEN
            AddRepError(GetCurrencyConvertErrorMsg(p_FromFI, p_ToFI, p_ADate));
          END IF;
          p_Error := 1;
          v_K := 0.0;
        END IF;
      END IF;
      
    END IF;

    RETURN v_K;
  END;

  FUNCTION GetRateOnDateCrossDbl_Ex(p_ADate IN DATE, 
                                    p_FromFI IN NUMBER, 
                                    p_ToFI IN NUMBER,
                                    p_NeedSayError IN NUMBER, 
                                    p_RateType IN NUMBER DEFAULT 0) RETURN NUMBER
  AS
    v_Error NUMBER;
    v_SinceDate DATE;
  BEGIN
    RETURN GetRateOnDateCrossDbl(p_ADate, 
                                 p_FromFI, 
                                 p_ToFI,
                                 p_NeedSayError, 
                                 p_RateType, 
                                 v_Error, 
                                 v_SinceDate);
  END;

  FUNCTION GetISO_Number(p_FIID IN NUMBER) RETURN VARCHAR2 DETERMINISTIC
  AS
    v_ISO dfininstr_dbt.t_ISO_Number%Type;
  BEGIN
    
    SELECT f.t_ISO_Number INTO v_ISO FROM dfininstr_dbt f WHERE f.t_FIID = p_FIID;

    RETURN v_ISO;

    EXCEPTION WHEN NO_DATA_FOUND THEN RETURN CHR(1);
  END;

  --Аналог ПолучитьКурсПостановкиНаБаланс из макроса
  FUNCTION GetBalanceRate(p_BOfficeKind IN NUMBER, p_DealID IN NUMBER, p_RQID IN NUMBER DEFAULT 0) RETURN NUMBER
  AS
    v_RQID NUMBER := 0;
    v_Course NUMBER := 0.0;
  BEGIN

    v_RQID := p_RQID;

    IF v_RQID = 0 THEN
      BEGIN
        SELECT dlrq.t_ID INTO v_RQID
          FROM ddlrq_dbt dlrq, ddl_leg_dbt Leg 
         WHERE dlrq.t_DocKind = p_BOfficeKind
           AND dlrq.t_DocID = p_DealID
           AND dlrq.t_Type = RSI_DLRQ.DLRQ_TYPE_DELIVERY 
           AND dlrq.t_DealPart = 1
           AND Leg.t_DealID = dlrq.t_DocID 
           AND Leg.t_LegID = 0 
           AND Leg.t_LegKind = 0 
           AND dlrq.t_State = RSI_DLRQ.DLRQ_STATE_EXEC 
           AND Leg.t_RejectDate = TO_DATE('01.01.0001','DD.MM.YYYY');

        EXCEPTION WHEN NO_DATA_FOUND THEN v_RQID := 0;
      END;
    END IF;

    IF v_RQID > 0 THEN
      BEGIN 
        SELECT rsb_struct.getDouble(t_Text) INTO v_Course 
          FROM dnotetext_dbt 
         WHERE t_DocumentID = LPAD(v_RQID, 10, '0') 
           AND t_ObjectType = RSB_SECUR.OBJTYPE_DLRQ 
           AND t_NoteKind = 44 --Курс постановки на баланс
           AND t_Date <= TO_DATE('01.01.2100','DD.MM.YYYY')
           AND t_ValidToDate >= TO_DATE('01.01.2100','DD.MM.YYYY');  --Такая дата в макросе используется
        EXCEPTION WHEN NO_DATA_FOUND THEN v_Course := 0.0; 
      END;
    END IF;

    RETURN v_Course;
  END;

  PROCEDURE GetCoupRetData(p_FIID IN NUMBER, 
                          p_Number_Coupon IN VARCHAR2, 
                          p_Amount IN NUMBER, 
                          p_CoupSum IN OUT NUMBER, 
                          p_Maturity IN OUT DATE)
  AS
    v_CoupSum NUMBER;
    v_Maturity DATE;
  BEGIN
    SELECT (rq.t_Amount/leg.t_Principal*p_Amount) NKD, 
            decode(rq.t_FactDate, TO_DATE('01.01.0001', 'DD.MM.YYYY'), rq.t_PlanDate, rq.t_FactDate) Maturity  INTO v_CoupSum, v_Maturity
        FROM ddl_tick_dbt tick, ddl_leg_dbt leg, ddlrq_dbt rq 
        WHERE tick.t_BofficeKind = RSB_SECUR.DL_RETIREMENT
          AND tick.t_PFI = p_FIID
          AND p_Number_Coupon = CASE rsb_secur.IsRet_Partly(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(tick.t_DealType, tick.t_BofficeKind))) 
                                    WHEN 0 THEN tick.t_Number_Coupon 
                                    ELSE tick.T_NUMBER_PARTLY END
          AND leg.t_DealID = tick.t_DealID 
          AND leg.t_LegKind = 0 --LEG_KIND_DL_TICK
          AND leg.t_LegID = 0 
          AND rq.t_DocKind = tick.t_BofficeKind 
          AND rq.t_DocID = tick.t_DealID 
          AND rq.t_DealPart = 1 
          AND rq.t_Type = RSI_DLRQ.DLRQ_TYPE_PAYMENT
          AND RowNum = 1;
    p_Maturity := v_Maturity;
    p_CoupSum := v_CoupSum;
    EXCEPTION WHEN OTHERS THEN NULL;  -- не меняем входные значения                
  END;

  --Аналог SP_GetCouponSumByPeriod из макроса
  FUNCTION SP_GetCouponSumByPeriod(p_FIID IN NUMBER, 
                                   p_Amount IN NUMBER, 
                                   p_DateBeg IN DATE, 
                                   p_DateEnd IN DATE, 
                                   p_IsPartial IN NUMBER, 
                                   p_ToFIID IN NUMBER, 
                                   p_CouponCount OUT NUMBER, 
                                   p_ExcludeDate IN DATE, 
                                   p_CalcByDates IN NUMBER, 
                                   p_IsClosed IN NUMBER DEFAULT 0,
                                   p_CoupRetData IN NUMBER DEFAULT 0 ) RETURN NUMBER
  AS
    v_Sum NUMBER := 0;
    v_S NUMBER;
    v_NumCoupon NUMBER := 0;
    v_TaxDepMode NUMBER := 0;
    v_DrawingDate DATE;
    v_Maturity DATE;
    v_err NUMBER := 0;
  BEGIN
    IF Rsb_Common.GetRegBoolValue('SECUR\РЕЖИМ ХРАНИЛИЩА ДАННЫХ ДЛЯ НУ') = TRUE THEN
      v_TaxDepMode := 1;
    END IF;

    FOR one_rec IN (SELECT FI.t_FaceValueFI, FI.t_FaceValue, warnt.t_DrawingDate as DrawingDate, warnt.t_IncomeRate, warnt.t_IncomeScale, warnt.t_Number 
                     FROM dfiwarnts_dbt warnt, dfininstr_dbt FI 
                    WHERE warnt.t_FIID = p_FIID 
                      AND warnt.t_IsPartial = DECODE(p_IsPartial, 1, 'X', CHR(0)) 
                      AND warnt.t_DrawingDate <= p_DateEnd 
                      AND warnt.t_DrawingDate >= p_DateBeg 
                      AND 1 = (CASE WHEN p_ExcludeDate != TO_DATE('01.01.0001','DD.MM.YYYY') AND warnt.t_DrawingDate = p_ExcludeDate THEN 0
                                    ELSE 1 END)
                      AND 1 = (CASE WHEN p_IsClosed = 0 THEN 1 
                                    WHEN p_IsClosed <> 0 AND v_TaxDepMode = 0 AND warnt.t_SpIsClosed = 'X' THEN 1
                                    WHEN p_IsClosed <> 0 AND v_TaxDepMode = 0 AND warnt.t_IsClosed = 'X' THEN 1
                                    ELSE 0 END)
                      AND FI.t_FIID = warnt.t_FIID 
                    ORDER BY warnt.t_DrawingDate ASC
                   )
     LOOP
       v_DrawingDate := one_rec.DrawingDate;
       v_S := 0;
       v_Maturity := v_DrawingDate;

       IF ( p_CoupRetData !=0 ) THEN
         GetCoupRetData(p_FIID, one_rec.t_Number, p_Amount, v_S, v_Maturity); -- если ошибка, то значения v_S и v_Maturity не изменяются
       END IF;       
       
       IF p_IsPartial <> 0 THEN --Сумма частичных погашений
         v_S := p_Amount * one_rec.t_FaceValue * one_rec.t_IncomeRate/GREATEST(1, one_rec.t_IncomeScale)/100.0;
       ELSE --Сумма купонов
         IF v_S = 0 THEN
           v_S := RSI_RSB_FIINSTR.CalcNKD(p_FIID, v_DrawingDate, p_Amount, 1);
         END IF;
       END IF;

       v_S := round( v_S, 2);
       IF p_ToFIID >= 0 THEN
         v_err := SmartConvertSumDbl_Ex(v_S, v_S, v_Maturity, one_rec.t_FaceValueFI, p_ToFIID, 1);
       END IF;

       IF p_CalcByDates <> 0 THEN
         v_S := v_S * (p_DateBeg - v_DrawingDate);
       END IF;

       v_Sum := v_Sum + v_S;
       v_NumCoupon := v_NumCoupon + 1;

    END LOOP;

    p_CouponCount := v_NumCoupon;
    RETURN v_Sum;
  END;

  FUNCTION GetCouponSumByPeriod(p_FIID IN NUMBER, 
                                p_Amount IN NUMBER, 
                                p_DateBeg IN DATE, 
                                p_DateEnd IN DATE, 
                                p_CouponCount OUT NUMBER, 
                                p_ExcludeDate IN DATE, 
                                p_CalcByDates IN NUMBER, 
                                p_IsClosed IN NUMBER DEFAULT 0,
                                p_CoupRetData IN NUMBER DEFAULT 0) RETURN NUMBER
  AS
  BEGIN
    RETURN SP_GetCouponSumByPeriod(p_FIID, p_Amount, p_DateBeg, p_DateEnd, 0, -1, p_CouponCount, p_ExcludeDate, p_CalcByDates, p_IsClosed, p_CoupRetData);
  END;

  FUNCTION GetCouponSumByPeriod_Rub(p_FIID IN NUMBER, 
                                    p_Amount IN NUMBER, 
                                    p_DateBeg IN DATE, 
                                    p_DateEnd IN DATE, 
                                    p_CouponCount OUT NUMBER, 
                                    p_ExcludeDate IN DATE, 
                                    p_CalcByDates IN NUMBER, 
                                    p_IsClosed IN NUMBER DEFAULT 0,
                                    p_CoupRetData IN NUMBER DEFAULT 0) RETURN NUMBER
  AS
  BEGIN
    RETURN SP_GetCouponSumByPeriod(p_FIID, p_Amount, p_DateBeg, p_DateEnd, 0, RSI_RSB_FIInstr.NATCUR, p_CouponCount, p_ExcludeDate, p_CalcByDates, p_IsClosed, p_CoupRetData);
  END;

  FUNCTION GetPartialSumByPeriod(p_FIID IN NUMBER, 
                                 p_Amount IN NUMBER, 
                                 p_DateBeg IN DATE, 
                                 p_DateEnd IN DATE, 
                                 p_CouponCount OUT NUMBER, 
                                 p_ExcludeDate IN DATE, 
                                 p_CalcByDates IN NUMBER, 
                                 p_IsClosed IN NUMBER DEFAULT 0) RETURN NUMBER
  AS 
  BEGIN
    RETURN SP_GetCouponSumByPeriod(p_FIID, p_Amount, p_DateBeg, p_DateEnd, 1, -1, p_CouponCount, p_ExcludeDate, p_CalcByDates, p_IsClosed);
  END;

  FUNCTION GetPartialSumByPeriod_Rub(p_FIID IN NUMBER, 
                                     p_Amount IN NUMBER, 
                                     p_DateBeg IN DATE, 
                                     p_DateEnd IN DATE, 
                                     p_CouponCount OUT NUMBER, 
                                     p_ExcludeDate IN DATE, 
                                     p_CalcByDates IN NUMBER, 
                                     p_IsClosed IN NUMBER DEFAULT 0) RETURN NUMBER
  AS
  BEGIN
    RETURN SP_GetCouponSumByPeriod(p_FIID, p_Amount, p_DateBeg, p_DateEnd, 1, RSI_RSB_FIInstr.NATCUR, p_CouponCount, p_ExcludeDate, p_CalcByDates, p_IsClosed);
  END;
























  FUNCTION GetFIRoleByPortfolioBonus(p_Portfolio IN NUMBER, p_BPP IN NUMBER DEFAULT 0) RETURN NUMBER
  AS
    v_FIRole NUMBER := FIROLE_BA;
  BEGIN
    IF p_Portfolio = RSB_PMWRTOFF.KINDPORT_TRADE THEN
      IF p_BPP > 0 THEN
        v_FIRole := FIROLE_BONUS_TP_BPP_PAID;
      ELSE
        v_FIRole := FIROLE_BONUS_TP_PAID;
      END IF;
    ELSIF p_Portfolio = RSB_PMWRTOFF.KINDPORT_SALE THEN
      IF p_BPP > 0 THEN
        v_FIRole := FIROLE_BONUS_PPR_BPP_PAID;
      ELSE
        v_FIRole := FIROLE_BONUS_PPR_PAID;
      END IF;
    ELSIF p_Portfolio = RSB_PMWRTOFF.KINDPORT_RETIRE THEN
      IF p_BPP > 0 THEN
        v_FIRole := FIROLE_BONUS_PUDP_BPP_PAID;
      ELSE
        v_FIRole := FIROLE_BONUS_PUDP_PAID;
      END IF;
    END IF;

    RETURN v_FIRole;
  END;

  FUNCTION GetFIRoleByPortfolio(p_Portfolio IN NUMBER, 
                                p_Discount  IN NUMBER DEFAULT 0, 
                                p_Percent   IN NUMBER DEFAULT 0, 
                                p_Bpp       IN NUMBER DEFAULT 0, 
                                p_IsOverdue IN NUMBER DEFAULT 0, 
                                p_Bonus     IN NUMBER DEFAULT 0
                               ) RETURN NUMBER
  AS
   v_FIRole NUMBER := FIROLE_BA;
  BEGIN

   IF p_IsOverdue > 0 THEN
     v_FIRole := FIROLE_BA_OVERDUE;
   END IF;

   IF p_Portfolio = RSB_PMWRTOFF.KINDPORT_TRADE THEN
     IF p_Discount > 0 THEN
       IF p_Bpp > 0 THEN
         v_FIRole := FIROLE_DD_TP_BPP;
       ELSE
         v_FIRole := FIROLE_DD_TP;
       END IF;
     ELSIF p_Percent > 0 THEN
       IF p_Bpp > 0 THEN
          v_FIRole := FIROLE_PD_TP_BPP;
       ELSE
          v_FIRole := FIROLE_PD_TP;
       END IF;
     ELSIF p_Bonus > 0 THEN
        IF p_Bpp > 0 THEN
           v_FIRole := FIROLE_BONUS_TP_BPP;
        ELSE
           v_FIRole := FIROLE_BONUS_TP;
        END IF;
     ELSE
        IF p_Bpp > 0 THEN
          IF p_IsOverdue > 0 THEN
             v_FIRole := FIROLE_BA_SSPU_BPP_OVERDUE;
          ELSE
             v_FIRole := FIROLE_BA_SSPU_BPP;
          END IF;
        ELSE
          IF p_IsOverdue > 0 THEN
             v_FIRole := FIROLE_BA_TP_OVERDUE;
          ELSE
             v_FIRole := FIROLE_BA_TP;
          END IF;
        END IF;
     END IF;
   ELSIF p_Portfolio = RSB_PMWRTOFF.KINDPORT_SALE THEN
      IF p_Discount > 0 THEN
         IF p_Bpp > 0 THEN
            v_FIRole := FIROLE_DD_PPR_BPP;
         ELSE
            v_FIRole := FIROLE_DD_PPR;
         END IF;
      ELSIF p_Percent > 0 THEN
         IF p_Bpp > 0 THEN
            v_FIRole := FIROLE_PD_PPR_BPP;
         ELSE
            v_FIRole := FIROLE_PD_PPR;
         END IF;
      ELSIF p_Bonus > 0 THEN
         IF p_Bpp > 0 THEN
            v_FIRole := FIROLE_BONUS_PPR_BPP;
         ELSE
            v_FIRole := FIROLE_BONUS_PPR;
         END IF;
      ELSE
         IF p_Bpp  > 0 THEN
           IF p_IsOverdue > 0 THEN
              v_FIRole := FIROLE_BA_SSSD_BPP_OVERDUE;
           ELSE
              v_FIRole := FIROLE_BA_SSSD_BPP;
           END IF;
         ELSE
           IF p_IsOverdue > 0 THEN
              v_FIRole := FIROLE_BA_PPR_OVERDUE;
           ELSE
              v_FIRole := FIROLE_BA_PPR;
           END IF;
         END IF;
      END IF;
   ELSIF p_Portfolio = RSB_PMWRTOFF.KINDPORT_CONTR THEN
      IF p_Bpp  > 0 THEN
        IF p_IsOverdue > 0 THEN
           v_FIRole := FIROLE_BA_CONTR_BPP_OVERDUE;
        ELSE
           v_FIRole := FIROLE_BA_CONTR_BPP;
        END IF;
      ELSE
        IF p_IsOverdue > 0 THEN
           v_FIRole := FIROLE_BA_CONTR_OVERDUE;
        ELSE
           v_FIRole := FIROLE_BAINCONTR;
        END IF;
      END IF;
   ELSIF p_Portfolio = RSB_PMWRTOFF.KINDPORT_PROMISSORY THEN
      IF p_Discount > 0 THEN
        v_FIRole := FIROLE_DD_PDO;
      ELSIF p_Percent > 0 THEN
        v_FIRole := FIROLE_PD_PDO;
      ELSE
        v_FIRole := FIROLE_BAINPROMISSORY;
      END IF;
   ELSIF p_Portfolio = RSB_PMWRTOFF.KINDPORT_RETIRE THEN
      IF p_Discount > 0 THEN
         IF p_Bpp > 0 THEN
            v_FIRole := FIROLE_DD_PUDP_BPP;
         ELSE
            v_FIRole := FIROLE_DD_PUDP;
         END IF;
      ELSIF p_Percent > 0 THEN
         IF p_Bpp > 0 THEN
            v_FIRole := FIROLE_PD_PUDP_BPP;
         ELSE
            v_FIRole := FIROLE_PD_PUDP;
         END IF;
      ELSIF p_Bonus > 0 THEN
         IF p_Bpp > 0 THEN
            v_FIRole := FIROLE_BONUS_PUDP_BPP;
         ELSE
            v_FIRole := FIROLE_BONUS_PUDP;
         END IF;
      ELSE
         IF p_Bpp > 0 THEN
           IF p_IsOverdue > 0 THEN
              v_FIRole := FIROLE_BA_ASCB_BPP_OVERDUE;
           ELSE
              v_FIRole := FIROLE_BA_ASCB_BPP;
           END IF;
         ELSE
           IF p_IsOverdue > 0 THEN
              v_FIRole := FIROLE_BA_PUDP_OVERDUE;
           ELSE
              v_FIRole := FIROLE_BA_PUDP;
           END IF;
         END IF;
      END IF;
   ELSIF (p_Portfolio = RSB_PMWRTOFF.KINDPORT_BACK) OR (p_Portfolio = RSB_PMWRTOFF.KINDPORT_BACK_KSU) THEN
      v_FIRole := FIROLE_BA_BACK;
   ELSIF (p_Portfolio = RSB_PMWRTOFF.KINDPORT_BASICDEBT) OR (p_Portfolio = RSB_PMWRTOFF.KINDPORT_BACK_BPP_KSU) THEN
      v_FIRole := FIROLE_BA;
   END IF;

   RETURN v_FIRole;
  END;


  FUNCTION GetAccountID(p_DocKind   IN NUMBER,
                        p_DocID     IN NUMBER,
                        p_CatCode   IN VARCHAR2,
                        p_Date      IN DATE,
                        p_FIRole    IN NUMBER,
                        p_FIID      IN NUMBER,
                        p_Portfolio IN NUMBER,
                        p_IsBPP     IN NUMBER,
                        p_IncType   IN NUMBER DEFAULT 0,
                        p_ResType   IN NUMBER DEFAULT 0
                       ) RETURN NUMBER
  AS
    v_AccountID NUMBER := 0;
  BEGIN
    
    FOR one_rec IN (SELECT acc.t_AccountID
                      FROM (SELECT t_ID, t_Class1, t_Class2, t_Class3, t_Class4, t_Class5, t_Class6, t_Class7, t_Class8
                              FROM dmccateg_dbt
                             WHERE t_LevelType = 1
                               AND t_Code = p_CatCode
                           ) cat, dmcaccdoc_dbt mcacc, dmctempl_dbt tpl, daccount_dbt acc
                     WHERE mcacc.t_CatID         = cat.t_ID
                       AND mcacc.t_DocKind       = p_DocKind
                       AND mcacc.t_DocID         = p_DocID
                       AND ((p_DocKind > 0 AND p_DocID > 0) OR mcacc.t_IsCommon = 'X')
                       AND mcacc.t_ActivateDate <= p_Date
                       AND (mcacc.t_DisablingDate = TO_DATE('01.01.0001','DD.MM.YYYY') OR mcacc.t_DisablingDate >= p_Date) 
                       AND (p_FIRole <= 0 OR mcacc.t_FIRole = p_FIRole)
                       AND (p_FIID <= 0 OR mcacc.t_FIID = p_FIID)
                       AND mcacc.t_Owner = UnknownParty
                       AND tpl.t_CatID = mcacc.t_CatID
                       AND tpl.t_Number = mcacc.t_TemplNum
                       AND p_IsBPP = (CASE WHEN cat.t_Class1 = 1653 /*LLCLASS_IS_AVOIR_BPP*/ AND tpl.t_Value1 <> 0 THEN 1
                                           WHEN cat.t_Class2 = 1653 /*LLCLASS_IS_AVOIR_BPP*/ AND tpl.t_Value2 <> 0 THEN 1
                                           WHEN cat.t_Class3 = 1653 /*LLCLASS_IS_AVOIR_BPP*/ AND tpl.t_Value3 <> 0 THEN 1
                                           WHEN cat.t_Class4 = 1653 /*LLCLASS_IS_AVOIR_BPP*/ AND tpl.t_Value4 <> 0 THEN 1
                                           WHEN cat.t_Class5 = 1653 /*LLCLASS_IS_AVOIR_BPP*/ AND tpl.t_Value5 <> 0 THEN 1
                                           WHEN cat.t_Class6 = 1653 /*LLCLASS_IS_AVOIR_BPP*/ AND tpl.t_Value6 <> 0 THEN 1
                                           WHEN cat.t_Class7 = 1653 /*LLCLASS_IS_AVOIR_BPP*/ AND tpl.t_Value7 <> 0 THEN 1
                                           WHEN cat.t_Class8 = 1653 /*LLCLASS_IS_AVOIR_BPP*/ AND tpl.t_Value8 <> 0 THEN 1
                                      ELSE 0 END
                                     )
                       AND (p_Portfolio <= 0 OR p_Portfolio = (CASE WHEN cat.t_Class1 = 474 /*LLCLASS_KINDPORT*/ THEN tpl.t_Value1
                                                                    WHEN cat.t_Class2 = 474 /*LLCLASS_KINDPORT*/ THEN tpl.t_Value2
                                                                    WHEN cat.t_Class3 = 474 /*LLCLASS_KINDPORT*/ THEN tpl.t_Value3
                                                                    WHEN cat.t_Class4 = 474 /*LLCLASS_KINDPORT*/ THEN tpl.t_Value4
                                                                    WHEN cat.t_Class5 = 474 /*LLCLASS_KINDPORT*/ THEN tpl.t_Value5
                                                                    WHEN cat.t_Class6 = 474 /*LLCLASS_KINDPORT*/ THEN tpl.t_Value6
                                                                    WHEN cat.t_Class7 = 474 /*LLCLASS_KINDPORT*/ THEN tpl.t_Value7
                                                                    WHEN cat.t_Class8 = 474 /*LLCLASS_KINDPORT*/ THEN tpl.t_Value8
                                                               ELSE 0 END
                                                              )
                           )
                       AND (p_IncType <= 0 OR p_IncType = (CASE WHEN cat.t_Class1 = 1654 /*LLCLASS_KIND_ACC_PDD*/ THEN tpl.t_Value1
                                                                WHEN cat.t_Class2 = 1654 /*LLCLASS_KIND_ACC_PDD*/ THEN tpl.t_Value2
                                                                WHEN cat.t_Class3 = 1654 /*LLCLASS_KIND_ACC_PDD*/ THEN tpl.t_Value3
                                                                WHEN cat.t_Class4 = 1654 /*LLCLASS_KIND_ACC_PDD*/ THEN tpl.t_Value4
                                                                WHEN cat.t_Class5 = 1654 /*LLCLASS_KIND_ACC_PDD*/ THEN tpl.t_Value5
                                                                WHEN cat.t_Class6 = 1654 /*LLCLASS_KIND_ACC_PDD*/ THEN tpl.t_Value6
                                                                WHEN cat.t_Class7 = 1654 /*LLCLASS_KIND_ACC_PDD*/ THEN tpl.t_Value7
                                                                WHEN cat.t_Class8 = 1654 /*LLCLASS_KIND_ACC_PDD*/ THEN tpl.t_Value8
                                                           ELSE 0 END
                                                          )
                           )
                       AND (p_ResType <= 0 OR p_ResType = (CASE WHEN cat.t_Class1 = 530 /*LLCLASS_KINDRESERV_KIND*/ THEN tpl.t_Value1
                                                                WHEN cat.t_Class2 = 530 /*LLCLASS_KINDRESERV_KIND*/ THEN tpl.t_Value2
                                                                WHEN cat.t_Class3 = 530 /*LLCLASS_KINDRESERV_KIND*/ THEN tpl.t_Value3
                                                                WHEN cat.t_Class4 = 530 /*LLCLASS_KINDRESERV_KIND*/ THEN tpl.t_Value4
                                                                WHEN cat.t_Class5 = 530 /*LLCLASS_KINDRESERV_KIND*/ THEN tpl.t_Value5
                                                                WHEN cat.t_Class6 = 530 /*LLCLASS_KINDRESERV_KIND*/ THEN tpl.t_Value6
                                                                WHEN cat.t_Class7 = 530 /*LLCLASS_KINDRESERV_KIND*/ THEN tpl.t_Value7
                                                                WHEN cat.t_Class8 = 530 /*LLCLASS_KINDRESERV_KIND*/ THEN tpl.t_Value8
                                                           ELSE 0 END
                                                          )
                           )
                       AND acc.t_Chapter = mcacc.t_Chapter
                       AND acc.t_Account = mcacc.t_Account
                       AND acc.t_Code_Currency = mcacc.t_Currency
                       AND (acc.t_Close_Date = TO_DATE('01.01.0001','DD.MM.YYYY') OR acc.t_Close_Date >= p_Date)
                     ORDER BY mcacc.t_ActivateDate DESC, mcacc.t_ID DESC
                    )
    LOOP
      v_AccountID := one_rec.t_AccountID;

      EXIT;
    END LOOP;

    IF v_AccountID = 0 AND p_DocKind > 0 AND p_DocID > 0 THEN
      v_AccountID :=  GetAccountID(0, 0, p_CatCode, p_Date, p_FIRole, p_FIID, p_Portfolio, p_IsBPP, p_IncType, p_ResType);
      IF v_AccountID = 0 THEN
        v_AccountID :=  GetAccountID(0, 0, p_CatCode, p_Date, -1, p_FIID, p_Portfolio, p_IsBPP, p_IncType, p_ResType);
      END IF;
    END IF;

    RETURN v_AccountID;
  END;

  PROCEDURE GetDealData(p_DealID       IN NUMBER, 
                        p_LotBuy_Sale  IN NUMBER,
                        p_Deal        OUT DDL_TICK_DBT%ROWTYPE, 
                        p_Group       OUT NUMBER,
                        p_DocKind     OUT NUMBER,
                        p_DocID       OUT NUMBER,
                        p_DealPart    OUT NUMBER
                       )
  IS
    v_LegKind NUMBER := 0;
  BEGIN

    IF p_DealID > 0 THEN
      SELECT tk.*
        INTO p_Deal
        FROM ddl_tick_dbt tk
       WHERE tk.t_DealID = p_DealID;

      p_Group := RSB_SECUR.get_OperationGroup(RSB_SECUR.get_OperSysTypes(p_Deal.t_DealType, p_Deal.t_BOfficeKind));

      p_DealPart := 1;

      IF RSB_SECUR.IsREPO(p_Group) > 0 OR RSB_SECUR.IsBACKSALE(p_Group) > 0 THEN
        
          IF  (RSB_SECUR.IsBuy(p_Group) > 0 AND p_LotBuy_Sale = RSB_PMWRTOFF.PM_WRITEOFF_SUM_SALE) OR
              (RSB_SECUR.IsSale(p_Group) > 0 AND (p_LotBuy_Sale = RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY OR p_LotBuy_Sale = RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY_BO)) THEN
                      
            v_LegKind  := 0; 
            p_DealPart := 2;
          ELSE
            v_LegKind := 0; 
          END IF;

          p_DocKind := RSB_SECUR.DL_SECURLEG;

          SELECT t_ID
            INTO p_DocID
            FROM ddl_leg_dbt
           WHERE t_DealID  = p_DealID
             AND t_LegKind = v_LegKind
             AND t_LegID   = 0;

      ELSE
        p_DocKind := p_Deal.t_BOfficeKind;
        p_DocID   := p_Deal.t_DealID;

        IF p_Deal.t_BOfficeKind = RSB_SECUR.DL_CONVAVR AND p_LotBuy_Sale <> RSB_PMWRTOFF.PM_WRITEOFF_SUM_SALE THEN
          p_DealPart := 2;
        END IF;

      END IF;

    END IF;

  END;

  FUNCTION GetLotCostAccountID(p_SumID IN NUMBER, p_Date IN DATE) RETURN NUMBER
  AS
    v_AccountID NUMBER := 0;
    v_FIRole    NUMBER := 0;

    v_Portfolio dpmwrtsum_dbt.t_Portfolio%type;
    v_State     dpmwrtsum_dbt.t_State%type;
    v_Amount    dpmwrtsum_dbt.t_Amount%type;
    v_AmountBD  dpmwrtsum_dbt.t_AmountBD%type;
    v_Buy_Sale  dpmwrtsum_dbt.t_Buy_Sale%type;
    v_FIID      dpmwrtsum_dbt.t_FIID%type;

    v_CatCode   dmccateg_dbt.t_Code%type;

    v_LotDealID NUMBER := 0;
    v_BuyDealID NUMBER := 0;

    v_Deal      ddl_tick_dbt%rowtype;
    v_DealGroup NUMBER := 0;

    v_DocKind  NUMBER := 0;
    v_DocID    NUMBER := 0;
    v_DealPart NUMBER := 0;

    v_RqState NUMBER := 0;

    v_IsBPP   NUMBER := 0;
    
  BEGIN

    BEGIN
      SELECT lot.t_DealID, lot.t_FIID, hist.t_Portfolio, hist.t_State, hist.t_Amount, hist.t_AmountBD, hist.t_Buy_Sale,
             (CASE WHEN lot.t_DocKind = RSB_SECUR.DLDOC_PAYMENT THEN (SELECT rq.t_DocID FROM ddlrq_dbt rq WHERE rq.t_ID = lot.t_DocID) ELSE -1 END)
        INTO v_BuyDealID, v_FIID, v_Portfolio, v_State, v_Amount, v_AmountBD, v_Buy_Sale, v_LotDealID
        FROM dpmwrtsum_dbt lot, v_scwrthistex hist 
       WHERE lot.t_SumID = p_SumID
         AND hist.t_SumID = lot.t_SumID 
         AND hist.t_Instance = (SELECT MAX(hist1.t_instance) 
                                  FROM v_scwrthist hist1
                                 WHERE hist1.t_SumID = hist.t_SumID 
                                   AND hist1.t_ChangeDate <= p_Date);

      EXCEPTION
        WHEN NO_DATA_FOUND THEN RETURN 0;
    END;

    IF (v_State = RSB_PMWRTOFF.PM_WRTSUM_SALE_BPP) AND ((v_Portfolio = RSB_PMWRTOFF.KINDPORT_SSPU) OR (v_Portfolio = RSB_PMWRTOFF.KINDPORT_SSSD) OR (v_Portfolio = RSB_PMWRTOFF.KINDPORT_ASCB)) THEN

      GetDealData(v_LotDealID, v_Buy_Sale, v_Deal, v_DealGroup, v_DocKind, v_DocID, v_DealPart);
    
      IF v_Portfolio = RSB_PMWRTOFF.KINDPORT_SSPU THEN
        v_FIRole := FIROLE_BA_SSPU_BPP;
      ELSIF v_Portfolio = RSB_PMWRTOFF.KINDPORT_SSSD THEN
        v_FIRole := FIROLE_BA_SSSD_BPP;
      ELSIF v_Portfolio = RSB_PMWRTOFF.KINDPORT_ASCB THEN
        v_FIRole := FIROLE_BA_ASCB_BPP;
      END IF;

      IF BPP_ACCOUNT_METHOD = 1 THEN
        v_CatCode := 'Наш портфель ц/б';
        v_IsBPP   := 1;
      ELSE
        IF RSB_SECUR.IsBasket(v_DealGroup) = 1 THEN
          v_CatCode := 'Ц/б, Корзина БПП';
        ELSE
          v_CatCode := 'Ц/б, БПП';
        END IF;
      END IF;

      v_AccountID := GetAccountID(v_DocKind, 
                                  v_DocID, 
                                  v_CatCode, 
                                  p_Date, 
                                  v_FIRole, 
                                  (CASE WHEN v_CatCode = 'Наш портфель ц/б' THEN v_FIID ELSE -1 END), 
                                  v_Portfolio, 
                                  v_IsBPP);

    ELSIF v_Portfolio = RSB_PMWRTOFF.KINDPORT_CONTR AND v_State = RSB_PMWRTOFF.PM_WRTSUM_SALE_BPP THEN
      
      GetDealData(v_LotDealID, v_Buy_Sale, v_Deal, v_DealGroup, v_DocKind, v_DocID, v_DealPart);
      
      v_FIRole := FIROLE_BA_CONTR_BPP;

      IF BPP_ACCOUNT_METHOD = 1 THEN
        v_CatCode := 'Наш портфель ПКУ, ц/б';
        v_IsBPP   := 1;
      ELSE
        IF RSB_SECUR.IsBasket(v_DealGroup) = 1 THEN
          v_CatCode := 'Ц/б, Корзина ПКУ БПП';
        ELSE
          v_CatCode := 'Ц/б, ПКУ БПП';
        END IF;
      END IF;

      v_AccountID := GetAccountID(v_DocKind, 
                                  v_DocID, 
                                  v_CatCode, 
                                  p_Date, 
                                  v_FIRole, 
                                  (CASE WHEN v_CatCode = 'Наш портфель ПКУ, ц/б' THEN v_FIID ELSE -1 END), 
                                  v_Portfolio, 
                                  v_IsBPP);

    /*Если лот ПВО и есть остаток - то остаток лежит на счете Ц/б ПВО*/
    ELSIF v_Portfolio = RSB_PMWRTOFF.KINDPORT_BACK AND v_State = RSB_PMWRTOFF.PM_WRTSUM_FORM THEN

      GetDealData(v_BuyDealID, v_Buy_Sale, v_Deal, v_DealGroup, v_DocKind, v_DocID, v_DealPart);
      
      IF RSB_SECUR.IsAvrWrtIn(v_DealGroup) = 1 THEN
        v_CatCode := 'Наш портфель ц/б';

        v_FIRole := GetFIRoleByPortfolio(v_Portfolio);

        v_AccountID := GetAccountID(v_DocKind, v_DocID, v_CatCode, p_Date, v_FIRole, v_FIID, v_Portfolio, 0);
      ELSE
        IF RSB_SECUR.IsBasket(v_DealGroup) = 1 THEN
          v_CatCode := 'Ц/б, Корзина ПВО';
        ELSE
          v_CatCode := 'Ц/б, ПВО';
        END IF;

        v_AccountID := GetAccountID(v_DocKind, v_DocID, v_CatCode, p_Date, FIROLE_BA_BACK, (CASE WHEN v_CatCode = 'Наш портфель ц/б' THEN v_FIID ELSE -1 END), -1, 0);
        IF v_AccountID = 0 THEN
          /*счет мог открыться и по FIROLE_BA*/
          v_AccountID := GetAccountID(v_DocKind, v_DocID, v_CatCode, p_Date, FIROLE_BA, (CASE WHEN v_CatCode = 'Наш портфель ц/б' THEN v_FIID ELSE -1 END), -1, 0);
        END IF;
      END IF;

    ELSIF (v_Portfolio = RSB_PMWRTOFF.KINDPORT_SSPU OR
           v_Portfolio = RSB_PMWRTOFF.KINDPORT_SSSD OR
           v_Portfolio = RSB_PMWRTOFF.KINDPORT_ASCB OR
           v_Portfolio = RSB_PMWRTOFF.KINDPORT_CONTR OR
           v_Portfolio = RSB_PMWRTOFF.KINDPORT_PROMISSORY
          ) AND v_State = RSB_PMWRTOFF.PM_WRTSUM_FORM THEN
          
      v_DocKind := 0;
      v_DocID   := 0;

      IF v_Portfolio = RSB_PMWRTOFF.KINDPORT_CONTR AND p_Date >= TO_DATE('01.11.2014','DD.MM.YYYY') THEN
        v_CatCode := 'Наш портфель ПКУ, ц/б';
      ELSE
        v_CatCode := 'Наш портфель ц/б';
      END IF;

      BEGIN
        SELECT opr.t_DocKind, TO_NUMBER(opr.t_DocumentID) 
          INTO v_DocKind, v_DocID
          FROM (SELECT hist.* 
                  FROM v_scwrthist hist 
                 WHERE hist.t_SumID = p_SumID ) t, doproper_dbt opr 
         WHERE t.t_instance = (SELECT MAX(v.t_instance) 
                                 FROM v_scwrthist v 
                                WHERE v.t_SumID = t.t_SumID
                                  AND v.t_ChangeDate <= p_Date
                                  AND v.t_Action = RSB_PMWRTOFF.PM_WRT_UPDTMODE_GLOBALTRANSF
                                  AND v.t_State = RSB_PMWRTOFF.PM_WRTSUM_FORM)
           AND opr.t_ID_Operation = t.t_ID_Operation;

        EXCEPTION
             WHEN NO_DATA_FOUND THEN v_DocKind := 0; v_DocID := 0;

      END;

      IF v_DocKind > 0 AND v_DocID > 0 THEN
        v_FIRole := FIROLE_DSTA;
      ELSE
        v_DocKind := RSB_SECUR.DLDOC_ISSUE;
        v_DocID   := v_FIID;

        v_FIRole := GetFIRoleByPortfolio(v_Portfolio);
      END IF;

      IF v_Portfolio = RSB_PMWRTOFF.KINDPORT_CONTR AND p_Date >= TO_DATE('01.11.2014','DD.MM.YYYY') THEN
        v_AccountID := GetAccountID(v_DocKind, v_DocID, v_CatCode, p_Date, v_FIRole, v_FIID, -1, 0);
      ELSE
        v_AccountID := GetAccountID(v_DocKind, v_DocID, v_CatCode, p_Date, v_FIRole, v_FIID, v_Portfolio, 0);
      END IF;

    ELSIF v_Portfolio = RSB_PMWRTOFF.KINDPORT_BASICDEBT AND v_State = RSB_PMWRTOFF.PM_WRTSUM_NOTFORM AND v_Amount > 0 THEN
      
      GetDealData(v_LotDealID, v_Buy_Sale, v_Deal, v_DealGroup, v_DocKind, v_DocID, v_DealPart);  

      IF RSB_SECUR.IsBasket(v_DealGroup) = 1 THEN
        v_CatCode := 'Ц/б, ПВО_БПП, Корзина';
      ELSE
        v_CatCode := 'Ц/б, ПВО_БПП';
      END IF;

      v_AccountID := GetAccountID(v_DocKind, v_DocID, v_CatCode, p_Date, -1, -1, -1, 0);

    ELSIF v_Portfolio = RSB_PMWRTOFF.KINDPORT_BACK_BPP_KSU AND v_Amount > 0 THEN

      GetDealData(v_BuyDealID, v_Buy_Sale, v_Deal, v_DealGroup, v_DocKind, v_DocID, v_DealPart);

      IF RSB_SECUR.IsBasket(v_DealGroup) = 1 THEN
        v_CatCode := '+ОД, Корзина';
      ELSE
        v_CatCode := '+ОД';
      END IF;

      v_AccountID := GetAccountID(v_DocKind, v_DocID, v_CatCode, p_Date, -1, -1, -1, 0);

    ELSIF v_Buy_Sale = RSB_PMWRTOFF.PM_WRITEOFF_SUM_SALE AND v_State = RSB_PMWRTOFF.PM_WRTSUM_NOTFORM AND v_AmountBD > 0 THEN

      GetDealData(v_BuyDealID, v_Buy_Sale, v_Deal, v_DealGroup, v_DocKind, v_DocID, v_DealPart);

      BEGIN
        SELECT RSI_DLRQ.RSI_GetRQStateOnDate(t_ID, p_Date)
          INTO v_RqState
          FROM ddlrq_dbt
         WHERE t_DocKind  = v_Deal.t_BOfficeKind
           AND t_DocID    = v_Deal.t_DealID
           AND t_DealPart = v_DealPart
           AND t_Type     = RSI_DLRQ.DLRQ_TYPE_DELIVERY
           AND t_FIID     = v_FIID;

        EXCEPTION
             WHEN NO_DATA_FOUND THEN v_RqState := 0;
      END;

      IF v_RqState = RSI_DLRQ.DLRQ_STATE_OVERDUE OR v_RqState = RSI_DLRQ.DLRQ_STATE_DELAYED THEN
        IF RSB_SECUR.IsBasket(v_DealGroup) = 1 THEN
          v_CatCode := 'Обяз. с н.с., корзина';
        ELSE
          v_CatCode := 'Обяз. с н.с.';
        END IF;
      ELSE
        IF RSB_SECUR.IsBasket(v_DealGroup) = 1 THEN
          v_CatCode := '-ОД, Корзина';
        ELSE
          v_CatCode := '-ОД';
        END IF;
      END IF;

      v_AccountID := GetAccountID(v_DocKind, v_DocID, v_CatCode, p_Date, -1, -1, -1, 0);

    ELSIF v_Portfolio = RSB_PMWRTOFF.KINDPORT_BACK_KSU AND v_Amount > 0 THEN

      GetDealData(v_BuyDealID, v_Buy_Sale, v_Deal, v_DealGroup, v_DocKind, v_DocID, v_DealPart);
      
      IF RSB_SECUR.IsDealKSU(v_DealGroup) = 1 THEN
        v_CatCode := 'Ц/б, ПВО';
      ELSE
        v_CatCode := 'Полученные КСУ';
      END IF;

      v_AccountID := GetAccountID(v_DocKind, v_DocID, v_CatCode, p_Date, -1, -1, -1, 0);
    END IF;

    RETURN v_AccountID;
  END;
  
  FUNCTION GetAnyDirectRateIdByTypeMP(p_RateDate IN DATE,
                                      p_FromFI   IN NUMBER,
                                      p_RateType IN NUMBER,
                                      p_MarketId IN NUMBER DEFAULT -1) RETURN NUMBER
  AS
    v_RateID NUMBER := 0;
  BEGIN
    BEGIN
      SELECT t_rateid INTO v_RateID
        FROM (SELECT rate.t_rateid
                          FROM dratedef_dbt rate
                         WHERE rate.t_otherfi = p_FromFI
                           AND rate.t_type    = p_RateType
                           AND (p_MarketId = -1 OR rate.t_Market_Place = p_MarketId)
                           AND t_sincedate    = p_RateDate
                        UNION ALL
                          SELECT r.t_rateid
                            FROM dratehist_dbt h, dratedef_dbt r
                           WHERE     r.t_rateid    = h.t_rateid
                                 AND r.t_otherfi   = p_FromFI
                                 AND r.t_type      = p_RateType
                                 AND (p_MarketId = -1 OR r.t_Market_Place = p_MarketId)
                                 AND h.t_sincedate = p_RateDate
             )
       WHERE ROWNUM = 1;

      EXCEPTION
        WHEN OTHERS THEN v_RateId := -1;
    END;

    RETURN v_RateId;
  END;
  
  FUNCTION GetRateIdByMPWithMaxTradeVolume(p_sinceDate IN DATE,
                                           p_fiidFrom  IN NUMBER,
                                           p_rateType  IN NUMBER DEFAULT 0,
                                           p_NDays     IN NUMBER DEFAULT -1,
                                           p_SayError  IN NUMBER DEFAULT 0
                                          ) RETURN NUMBER
  IS
    v_cnt        NUMBER;
    v_RateRec    dratedef_dbt%ROWTYPE;
    v_MarketId   NUMBER := -1;
    v_RateDate   DATE;
    v_RateId     NUMBER := -1;
    v_err        NUMBER := 0;
    
    c_MICEX_code CONSTANT VARCHAR(2000) := Rsb_Common.GetRegStrValue('SECUR\MICEX_CODE');
    c_SPBEX_code CONSTANT VARCHAR(2000) := Rsb_Common.GetRegStrValue('SECUR\SPBEX_CODE');
    
    v_MICEX      NUMBER :=  -1;
    v_SPBEX      NUMBER :=  -1;
    
    FUNCTION GetPartyId(p_PartyCode VARCHAR) RETURN NUMBER
    IS
      v_PartyId NUMBER;
    BEGIN
      SELECT t_ObjectId INTO v_PartyId
        FROM dobjcode_dbt
       WHERE t_ObjectType = 3
         AND t_CodeKind = 1
         AND t_Code = p_PartyCode;
         
      RETURN v_PartyId;
         
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        BEGIN
          IF p_SayError != 0 THEN 
            AddRepError('Не найден субъект по коду "' || p_PartyCode || '"');
          END IF;
          
          v_err := 1;
        END;
    END;
  BEGIN

    BEGIN
      SELECT MAX (CASE WHEN r.t_SinceDate <= p_sinceDate THEN r.t_SinceDate
                       ELSE NVL((SELECT MAX(hist.t_SinceDate) 
                                   FROM dratehist_dbt hist 
                                  WHERE hist.t_RateID = r.t_RateID 
                                    AND hist.t_SinceDate <= p_sinceDate), 
                                to_date('01.01.0001','dd.mm.yyyy')) 
                  END) 
        INTO v_RateDate
        FROM dratedef_dbt r
       WHERE     r.t_OtherFi = p_fiidFrom
             AND (p_rateType = 0 OR r.t_Type = p_rateType)
             AND (   r.t_SinceDate <= p_sinceDate AND (p_NDays < 0 OR p_sinceDate - r.t_SinceDate <= p_NDays)
                  OR EXISTS
                        (SELECT 1
                           FROM dratehist_dbt h
                          WHERE     h.t_RateId = r.t_RateId
                                AND h.t_SinceDate <= p_sinceDate
                                AND (p_NDays < 0 OR p_sinceDate - h.t_SinceDate <= p_NDays)));
    EXCEPTION 
      WHEN OTHERS THEN v_err := 1;
    END;

    IF v_err = 0 THEN
      SELECT COUNT (DISTINCT r.t_Market_Place)
        INTO v_cnt
        FROM dratedef_dbt r
       WHERE     r.t_OtherFi = p_fiidFrom
             AND (p_rateType = 0 OR r.t_Type = p_rateType)
             AND (   r.t_SinceDate = v_RateDate
                  OR EXISTS
                        (SELECT 1
                           FROM dratehist_dbt h
                          WHERE     h.t_RateId = r.t_RateId
                                AND h.t_SinceDate = v_RateDate));
   
      CASE
        WHEN v_cnt = 0 THEN v_err := 1;
   
        WHEN v_cnt = 1 THEN
          BEGIN
            v_RateId := GetAnyDirectRateIdByTypeMP(v_RateDate, p_fiidFrom, p_RateType);
   
          EXCEPTION
            WHEN OTHERS THEN v_err := 1;
          END;
          
        WHEN v_cnt > 1 THEN
          BEGIN
            IF c_MICEX_code IS NULL THEN
              IF p_SayError != 0 THEN
                AddRepError('Не задано значение настройки SECUR\MICEX_CODE');
              END IF;
              
              v_err := 1;
            END IF;
          
            IF c_SPBEX_code IS NULL THEN
              IF p_SayError != 0 THEN
                AddRepError('Не задано значение настройки SECUR\SPBEX_CODE');
              END IF;
              
              v_err := 1;        
            END IF;
        
            IF v_err = 0 THEN
              v_MICEX := GetPartyId(c_MICEX_code);
            END IF;
          
            IF v_err = 0 THEN
              v_SPBEX := GetPartyId(c_SPBEX_code);
            END IF;
   
            IF v_err = 0 THEN
              BEGIN
                SELECT CASE 
                         WHEN t_Type = RATETYPE_TRADEVOLUME THEN v_MICEX 
                         WHEN t_Type = RATETYPE_TRADEVOLUME_SPB THEN v_SPBEX
                       END t_MarketID
                  INTO v_MarketId
                  FROM (  SELECT t_Type,
                                 RSB_SPREPFUN.SmartConvertSumDbl_ex2 (t_rate,
                                                                      p_sinceDate,
                                                                      t_FIID,
                                                                      MIN (t_FIID) OVER (),
                                                                      0)
                                    t_ConvRate
                            FROM (SELECT rate.t_RateId,
                                         rate.t_Type,
                                         rate.t_FIID,
                                         CASE 
                                           WHEN rate.T_IsInverse = CHR(88) THEN 1.0 / (rate.t_Rate / rate.t_Scale / POWER (10, rate.t_Point))
                                           WHEN rate.T_IsRelative = CHR(88) THEN rate.t_Rate / rate.t_Scale / POWER (10, rate.t_Point) / 100.0 * RSI_RSB_FIINSTR.FI_GetNominalOnDate(rate.t_OtherFI, p_sinceDate)
                                           ELSE rate.t_Rate / rate.t_Scale / POWER (10, rate.t_Point)
                                         END t_Rate
                                    FROM dratedef_dbt rate
                                   WHERE     rate.t_OtherFI = p_fiidFrom
                                         AND rate.t_Type IN (RATETYPE_TRADEVOLUME, RATETYPE_TRADEVOLUME_SPB)
                                         AND rate.t_SinceDate = v_RateDate
                                  UNION
                                  SELECT r.t_RateId,
                                         r.t_Type,
                                         r.t_FIID,
                                         CASE 
                                           WHEN h.T_IsInverse = CHR(88) THEN 1.0 / (h.t_Rate / h.t_Scale / POWER (10, h.t_Point))
                                           WHEN r.T_IsRelative = CHR(88) THEN h.t_Rate / h.t_Scale / POWER (10, h.t_Point) / 100.0 * RSI_RSB_FIINSTR.FI_GetNominalOnDate(r.t_OtherFI, p_sinceDate)
                                           ELSE h.t_Rate / h.t_Scale / POWER (10, h.t_Point)
                                         END t_Rate
                                    FROM dratedef_dbt r, dratehist_dbt h
                                   WHERE     h.t_RateId = r.t_RateId
                                         AND r.t_OtherFI = p_fiidFrom
                                         AND r.t_Type IN (RATETYPE_TRADEVOLUME, RATETYPE_TRADEVOLUME_SPB)
                                         AND h.t_SinceDate =
                                                (SELECT MAX (h2.t_sincedate)
                                                   FROM dratehist_dbt h2
                                                  WHERE h2.t_rateid = h.t_rateid AND h2.t_sincedate = v_RateDate))
                      ORDER BY t_ConvRate DESC, t_Type ASC)
                 WHERE ROWNUM = 1;
              EXCEPTION 
                WHEN NO_DATA_FOUND THEN v_MarketId := v_MICEX;
                WHEN OTHERS THEN v_err := 1;
              END;
            END IF;
   
            IF v_err = 0 THEN
              v_RateId := GetAnyDirectRateIdByTypeMP(v_RateDate, p_fiidFrom, p_RateType, v_MarketId);
            END IF;
            
          EXCEPTION
            WHEN OTHERS THEN v_err := 1;
          END;
      END CASE;
    END IF;
    
    IF v_err != 0 THEN
      IF p_SayError != 0 THEN
        AddRepError(GetCurrencyConvertErrorMsg(p_fiidFrom, NULL, p_sinceDate));
      END IF;
    
      v_RateId := -1;
    END IF;
    
    RETURN v_RateId;
    
  END GetRateIdByMPWithMaxTradeVolume;
  
  FUNCTION GetCourse(p_RateId IN NUMBER, p_SinceDate IN DATE) RETURN NUMBER
  IS
    v_RateRec   dratedef_dbt%ROWTYPE;
    v_Course    NUMBER := 0.0;
    v_err       NUMBER := 0;
    v_SinceDate DATE;
  BEGIN
    IF p_RateID > 0 THEN
      SELECT * INTO v_RateRec
        FROM dratedef_dbt
        WHERE t_RateID = p_RateID;
        
      v_Course := GetRateValueOnDate(v_RateRec, p_SinceDate, v_RateRec.t_OtherFI, v_RateRec.t_FIID, 0, v_err, v_SinceDate);
    END IF;
    
    RETURN v_Course;
  EXCEPTION
    WHEN OTHERS THEN RETURN 0.0;
  END GetCourse;
  
  FUNCTION GetCourseFI(p_RateId IN NUMBER) RETURN NUMBER
  IS
    v_FIID NUMBER := -1;
    v_ISRELATIVE  dratedef_dbt.t_isrelative%type;
    v_OTHERFI dratedef_dbt.t_otherfi%type;
  BEGIN
    IF p_RateID > 0 THEN
      SELECT t_FIID, t_isrelative, t_otherfi INTO v_FIID, v_ISRELATIVE, v_OTHERFI
        FROM dratedef_dbt
       WHERE t_RateID = p_RateID;
    END IF;
    
    /** если курс в % от номинала, то возвращаем валюту номинала */
    IF v_ISRELATIVE = 'X' THEN
       v_FIID := RSI_RSB_FIINSTR.FI_GetFaceValueFI(v_OTHERFI);
    END IF;
    
    RETURN v_FIID;
  EXCEPTION
    WHEN OTHERS THEN RETURN -1;
  END GetCourseFI;
  
END RSB_SPREPFUN;
/
