CREATE OR REPLACE PACKAGE BODY RSI_RSB_FIInstr IS
  FI_ConvSum_CourseDate DATE   := NULL;  -- дата курса, который использовался в ConvSum

  LastErrorMessage VARCHAR2(1024) := '';

  ReturnIncomeRate NUMBER := 0.0;

  v_fi_kind_a FI_KIND_TYPE;

  m_NewQuothistBuff dquothist_dbt%ROWTYPE; -- буфер новой записи истории внутреннего курса
  m_NewQuotdefBuff  dquotdef_dbt%ROWTYPE; -- буфер новой записи внутреннего курса
  m_NewRatehistBuff dratehist_dbt%ROWTYPE; -- буфер новой записи истории внешнего курса
  m_NewRatedefBuff  dratedef_dbt%ROWTYPE; -- буфер новой записи внешнего курса

  --Функция приводит ставку к нормальному виду: точность не меньше 4х и повышается, если ставка меньше 1.
  PROCEDURE MakeNormRate
  (
    p_Numerator NUMBER
   ,p_Denominator NUMBER
   ,p_Rate OUT NUMBER
   ,p_Scale OUT NUMBER
   ,p_Point OUT NUMBER
   ,p_IsInverse OUT CHAR
  ) AS
  BEGIN
    p_Scale := 1;
    p_Point := 4;
    p_IsInverse := CASE WHEN p_Numerator > p_Denominator THEN CHR(0) ELSE CHR(88) END;
    WHILE
      CASE WHEN p_IsInverse = CHR(0)
      THEN p_Numerator * POWER(10, p_Point) / p_Denominator
      ELSE p_Denominator * POWER(10, p_Point) / p_Numerator
      END < 1
    LOOP
      p_Point := p_Point + 1;
    END LOOP;
    p_Rate :=
      CASE WHEN p_IsInverse = CHR(0)
      THEN p_Numerator * POWER(10, p_Point) / p_Denominator
      ELSE p_Denominator * POWER(10, p_Point) / p_Numerator
      END;
    p_Rate := ROUND(p_Rate, 0);
  END;

  --Функция определения курса по формуле (1): Rate = (r1*m2*(10**s2)) / (r2*m1*(10**s1))
  PROCEDURE DetermineRate_1
  (
    p_Rate1 NUMBER
   ,p_Scale1 NUMBER
   ,p_Point1 NUMBER
   ,p_Rate2 NUMBER
   ,p_Scale2 NUMBER
   ,p_Point2 NUMBER
   ,p_Rate OUT NUMBER
   ,p_Scale OUT NUMBER
   ,p_Point OUT NUMBER
   ,p_IsInverse OUT CHAR
  ) AS
    m_Numerator NUMBER;
    m_Denominator NUMBER;
    m_Point NUMBER;
  BEGIN
    m_Point := p_Point2 - p_Point1;
    IF m_Point = 0 THEN
      m_Numerator := p_Rate1 * p_Scale2;
      m_Denominator := p_Rate2 * p_Scale1;
    ELSIF m_Point > 0 THEN
      m_Numerator := p_Rate1 * p_Scale2 * POWER(10, m_Point);
      m_Denominator := p_Rate2 * p_Scale1;
    ELSE
      m_Numerator := p_Rate1 * p_Scale2;
      m_Denominator := p_Rate2 * p_Scale1 * POWER(10, -m_Point);
    END IF;
    MakeNormRate(m_Numerator, m_Denominator, p_Rate, p_Scale, p_Point, p_IsInverse);
  END;

  --Функция определения курса по формуле (2): Rate = (m1*m2*(10**(s2+s1))) / (r1*r2)
  PROCEDURE DetermineRate_2
  (
    p_Rate1 NUMBER
   ,p_Scale1 NUMBER
   ,p_Point1 NUMBER
   ,p_Rate2 NUMBER
   ,p_Scale2 NUMBER
   ,p_Point2 NUMBER
   ,p_Rate OUT NUMBER
   ,p_Scale OUT NUMBER
   ,p_Point OUT NUMBER
   ,p_IsInverse OUT CHAR
  ) AS
    m_Numerator NUMBER;
    m_Denominator NUMBER;
  BEGIN
    m_Numerator := p_Scale1 * p_Scale2 * POWER(10, p_Point1 + p_Point2);
    m_Denominator := p_Rate1 * p_Rate2;
    MakeNormRate(m_Numerator, m_Denominator, p_Rate, p_Scale, p_Point, p_IsInverse);
  END;

  --Функция определения курса по формуле (3): Rate = (r1*r2) / (m1*m2*(10**(s2+s1)))
  PROCEDURE DetermineRate_3
  (
    p_Rate1 NUMBER
   ,p_Scale1 NUMBER
   ,p_Point1 NUMBER
   ,p_Rate2 NUMBER
   ,p_Scale2 NUMBER
   ,p_Point2 NUMBER
   ,p_Rate OUT NUMBER
   ,p_Scale OUT NUMBER
   ,p_Point OUT NUMBER
   ,p_IsInverse OUT CHAR
  ) AS
    m_Numerator NUMBER;
    m_Denominator NUMBER;
  BEGIN
    m_Numerator := p_Rate1 * p_Rate2;
    m_Denominator := p_Scale1 * p_Scale2 * POWER(10, p_Point1 + p_Point2);
    MakeNormRate(m_Numerator, m_Denominator, p_Rate, p_Scale, p_Point, p_IsInverse);
  END;

  --Функция определения курса по формуле (4): Rate = (r2*m1*(10**s1)) / (r1*m2*(10**s2))
  PROCEDURE DetermineRate_4
  (
    p_Rate1 NUMBER
   ,p_Scale1 NUMBER
   ,p_Point1 NUMBER
   ,p_Rate2 NUMBER
   ,p_Scale2 NUMBER
   ,p_Point2 NUMBER
   ,p_Rate OUT NUMBER
   ,p_Scale OUT NUMBER
   ,p_Point OUT NUMBER
   ,p_IsInverse OUT CHAR
  ) AS
    m_Numerator NUMBER;
    m_Denominator NUMBER;
    m_Point NUMBER;
  BEGIN
    m_Point := p_Point2 - p_Point1;
    IF m_Point = 0 THEN
      m_Numerator := p_Rate2 * p_Scale1;
      m_Denominator := p_Rate1 * p_Scale2;
    ELSIF m_Point > 0 THEN
      m_Numerator := p_Rate2 * p_Scale1;
      m_Denominator := p_Rate1 * p_Scale2 * POWER(10, m_Point);
    ELSE
      m_Numerator := p_Rate2 * p_Scale1 * POWER(10, -m_Point);
      m_Denominator := p_Rate1 * p_Scale2;
    END IF;
    MakeNormRate(m_Numerator, m_Denominator, p_Rate, p_Scale, p_Point, p_IsInverse);
  END;

  --Определение курса между валютами, на основании кросс-курсов между этими валютами и третьей валюты.
  PROCEDURE FI_DetermineRate
  (
    p_Rate1 NUMBER --числитель (курс между базовой и кроссируемой валютой)
   ,p_Scale1 NUMBER
   ,p_Point1 NUMBER
   ,p_IsInverse1 CHAR
   ,p_Rate2 NUMBER --знаменатель (курс между котируемой и кроссируемой валютой)
   ,p_Scale2 NUMBER
   ,p_Point2 NUMBER
   ,p_IsInverse2 CHAR
   ,p_Rate OUT NUMBER --возвращаемый курс
   ,p_Scale OUT NUMBER
   ,p_Point OUT NUMBER
   ,p_IsInverse OUT CHAR
  ) AS
  BEGIN
    IF p_IsInverse1 = CHR(0) AND p_IsInverse2 = CHR(0) THEN
      DetermineRate_1(p_Rate1, p_Scale1, p_Point1, p_Rate2, p_Scale2, p_Point2, p_Rate, p_Scale, p_Point, p_IsInverse);
    ELSIF p_IsInverse1 = CHR(88) AND p_IsInverse2 = CHR(0) THEN
      DetermineRate_2(p_Rate1, p_Scale1, p_Point1, p_Rate2, p_Scale2, p_Point2, p_Rate, p_Scale, p_Point, p_IsInverse);
    ELSIF p_IsInverse1 = CHR(0) AND p_IsInverse2 = CHR(88) THEN
      DetermineRate_3(p_Rate1, p_Scale1, p_Point1, p_Rate2, p_Scale2, p_Point2, p_Rate, p_Scale, p_Point, p_IsInverse);
    ELSE
      DetermineRate_4(p_Rate1, p_Scale1, p_Point1, p_Rate2, p_Scale2, p_Point2, p_Rate, p_Scale, p_Point, p_IsInverse);
    END IF;
  END;

  FUNCTION FI_ReturnIncomeRate return NUMBER is
  BEGIN
    return ReturnIncomeRate;
  END;

  PROCEDURE InitError
  AS
  BEGIN
     LastErrorMessage := '';
  END;

  PROCEDURE SetError( ErrNum IN INTEGER, ErrMes IN VARCHAR2 DEFAULT NULL )
  AS
  BEGIN
     IF( ErrMes IS NULL ) THEN
        LastErrorMessage := '';
     ELSE
        LastErrorMessage := ErrMes;
     END IF;
     RAISE_APPLICATION_ERROR( ErrNum,'' );
  END;

  PROCEDURE GetLastErrorMessage( ErrMes OUT VARCHAR2 )
  AS
  BEGIN
     ErrMes := LastErrorMessage;
  END;

  FUNCTION FI_AvrKindsGetRoot( FI_Kind IN NUMBER, AvoirKind IN NUMBER ) RETURN  NUMBER DETERMINISTIC
  IS
    RootAvoirKind NUMBER := 0;
  BEGIN
     SELECT t_ROOT into RootAvoirKind
       FROM davrkinds_dbt
      WHERE t_AvoirKind = AvoirKind AND
            t_FI_Kind   = FI_Kind;

     RETURN RootAvoirKind;

  EXCEPTION
     WHEN NO_DATA_FOUND THEN RETURN 0;

  END FI_AvrKindsGetRoot;

  FUNCTION FI_AvrKindsGetRootByFIID( FIID IN NUMBER ) RETURN  NUMBER DETERMINISTIC
  IS
    RootAvoirKind NUMBER := 0;
  BEGIN
     SELECT FI_AvrKindsGetRoot(t_FI_Kind,t_AvoirKind) into RootAvoirKind
       FROM dfininstr_dbt
      WHERE t_FIID = FIID;

     RETURN RootAvoirKind;

  EXCEPTION
     WHEN NO_DATA_FOUND THEN RETURN 0;

  END FI_AvrKindsGetRootByFIID;


  FUNCTION FI_IsSecurIndividual( FIID IN NUMBER )
    RETURN NUMBER DETERMINISTIC
  IS
    v_IsIndividual davrkinds_dbt.t_IsEmissive%TYPE;
  BEGIN
    SELECT avrk.t_IsIndividual
      INTO v_IsIndividual
      FROM dfininstr_dbt fin,
           davrkinds_dbt avrk
     WHERE fin.t_FIID = FIID
       AND avrk.t_FI_Kind = 2
       AND avrk.t_AvoirKind = fin.t_AvoirKind;

    IF v_IsIndividual = CHR(88) THEN
      RETURN 1;
    END IF;

    RETURN 0;

    EXCEPTION
       WHEN NO_DATA_FOUND THEN
         RETURN 0;
  END;

  FUNCTION FI_IsSecurEmissive( FIID IN NUMBER )
    RETURN NUMBER DETERMINISTIC
  IS
    v_IsEmissive davrkinds_dbt.t_IsEmissive%TYPE;
  BEGIN
    SELECT avrk.t_IsEmissive
      INTO v_IsEmissive
      FROM dfininstr_dbt fin,
           davrkinds_dbt avrk
     WHERE fin.t_FIID = FIID
       AND avrk.t_FI_Kind = 2
       AND avrk.t_AvoirKind = fin.t_AvoirKind;

    IF v_IsEmissive = CHR(88) THEN
      RETURN 1;
    END IF;

    RETURN 0;

    EXCEPTION
       WHEN NO_DATA_FOUND THEN
         RETURN 0;
  END;

  -- является ли фининстумент валютой
  FUNCTION FI_IsCurrency( p_FIID IN NUMBER )
    RETURN NUMBER DETERMINISTIC
  IS
    v_fi_kind dfininstr_dbt.t_fi_kind%TYPE;
    v_TpFIID INTEGER;
  BEGIN
    v_TpFIID := p_FIID;

    IF v_fi_kind_a.exists(v_TpFIID) THEN
      v_fi_kind := v_fi_kind_a(v_TpFIID);
    ELSE
      BEGIN
        SELECT t_fi_kind INTO v_fi_kind_a(v_TpFIID)
          FROM dfininstr_dbt
         WHERE t_FIID = p_FIID;

        v_fi_kind := v_fi_kind_a(v_TpFIID);
      EXCEPTION
        WHEN no_data_found THEN NULL;
      END;
    END IF;

    IF v_fi_kind = FIKIND_CURRENCY THEN
      RETURN 1;
    END IF;

    RETURN 0;
  EXCEPTION
       WHEN NO_DATA_FOUND THEN
         RETURN 0;
  END;



  -- проверка - является ли данная ц/б облигацией
  FUNCTION FI_IsAvrKindBond( AvoirKind IN NUMBER ) RETURN BOOLEAN DETERMINISTIC
  IS
  begin
    return (FI_AvrKindsGetRoot( 2, AvoirKind ) = AVOIRKIND_BOND);
  END FI_IsAvrKindBond;

  -- проверка - является ли данная ц/б КСУ
  FUNCTION FI_IsKSU( FIID IN NUMBER ) RETURN NUMBER DETERMINISTIC
  IS
  begin
    IF(FI_AvrKindsGetRootByFIID( FIID ) = AVOIRKIND_KSU) THEN
      return 1;
    END IF;

    return 0;
  END FI_IsKSU;

  -- проверка - является ли данная ц/б корзиной
  FUNCTION FI_IsBasket( FIID IN NUMBER ) RETURN NUMBER DETERMINISTIC
  IS
  begin
    IF(FI_AvrKindsGetRootByFIID( FIID ) = AVOIRISSKIND_BASKET) THEN
      return 1;
    END IF;

    return 0;
  END FI_IsBasket;

  -- проверка - является ли данная ц/б ИСУ
  FUNCTION FI_IsISU( FIID IN NUMBER ) RETURN NUMBER DETERMINISTIC
  IS
  begin
    IF(FI_AvrKindsGetRootByFIID( FIID ) = AVOIRKIND_HYPOTHECARY_CERT) THEN
      return 1;
    END IF;

    return 0;
  END FI_IsISU;

  -- проверка - является ли данная ц/б БИО
  FUNCTION FI_IsBIO( FIID IN NUMBER ) RETURN NUMBER DETERMINISTIC
  IS
    v_AvrKind INTEGER :=-1 ;
  BEGIN

      BEGIN
        SELECT t_Avoirkind INTO v_AvrKind
          FROM dfininstr_dbt
         WHERE t_FIID = FIID;

      EXCEPTION
        WHEN no_data_found THEN v_AvrKind := -1;
      END;

    IF v_AvrKind = AVOIRKIND_BOND_CORPORATE_BIO THEN
      RETURN 1;
    END IF;

    RETURN 0;
  END FI_IsBIO;

  -- Определить относится ли подвид CheckAvrKind к виду AvrKind
  -- т.е. является ли AvrKind родителем CheckAvrKind
  FUNCTION FI_AvrKindsEQ( FI_Kind IN NUMBER, AvoirKind IN NUMBER, CheckAvoirKind IN NUMBER ) RETURN NUMBER DETERMINISTIC
  IS
    ExistRec NUMBER := 0;
  begin
    if AvoirKind = CheckAvoirKind then
      ExistRec := 1;
    else
      SELECT 1 into ExistRec
        FROM dual
       WHERE AvoirKind IN ( SELECT t_AvoirKind
                              FROM davrkinds_dbt
                        START WITH t_FI_Kind = FI_Kind AND t_AvoirKind = CheckAvoirKind
                        CONNECT BY t_FI_Kind = PRIOR t_FI_Kind AND t_AvoirKind = PRIOR t_Parent
                          );
      if (ExistRec IS NULL) OR (ExistRec != 1) then
        ExistRec := 0;
      end if;

    end if;

    return ExistRec;

    EXCEPTION
       WHEN NO_DATA_FOUND THEN
         RETURN 0;
  END FI_AvrKindsEQ;

------------------------------------------------------------------------------------
  function  GetRate(
                    pType       IN  NUMBER,
                    pFromFI     IN  NUMBER,
                    pToFI       IN  NUMBER,
                    pRateDef    OUT DRATEDEF_DBT%ROWTYPE ) return NUMBER is
  begin

   SELECT * INTO  pRateDef
   FROM (SELECT RD.*
         FROM  DRATEDEF_DBT RD
         WHERE RD.T_TYPE = pType
           AND ((     RD.T_OTHERFI = pFromFI
                 AND RD.T_FIID =    pToFI  )
               OR
               (    RD.T_OTHERFI = pToFI
                AND RD.T_FIID =    pFromFI ))
           ORDER BY T_FIID, T_OTHERFI, T_TYPE) sel
           WHERE ROWNUM=1  ;

    return 1;
  exception
    WHEN NO_DATA_FOUND THEN
         RETURN -1;
    WHEN OTHERS THEN
         DBMS_OUTPUT.PUT_LINE('Ошибка при получении курса' || SQLERRM);
         RETURN -1;
  end;
------------------------------------------------------------------------------------
  FUNCTION GetDominantRate
  (
    pFromFI     IN  NUMBER
   ,pToFI       IN  NUMBER
   ,pRateDef    OUT dratedef_dbt%ROWTYPE
  )
  RETURN NUMBER
  IS
  BEGIN

   SELECT * INTO  pRateDef
   FROM (SELECT rd.*
           FROM dratedef_dbt rd
          WHERE rd.t_IsDominant = 'X'
            AND (   (    rd.t_OtherFI = pFromFI
                     AND rd.t_FIID =    pToFI  )
                 OR (    rd.t_OTHERFI = pToFI
                     AND rd.t_FIID =    pFromFI ))
    ORDER BY t_FIID, t_OtherFI, t_Type) sel
    WHERE ROWNUM = 1;

    RETURN 1;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
         RETURN -1;
    WHEN OTHERS THEN
         dbms_output.put_line('Ошибка при получении курса' || SQLERRM);
         RETURN -1;
  END;

-------------------------------------------------------------------------------------------------
  function  GetRateByMP(
                    pFromFI       IN NUMBER,
                    pToFI         IN NUMBER,
                    pType         IN NUMBER, --Тип курса
                    pMarket_Place IN NUMBER, --Торговая площадка
                    pSection      IN NUMBER, --Секция торговой площадки
                    pRateDef    OUT DRATEDEF_DBT%ROWTYPE ) return NUMBER is
  begin

   SELECT * INTO  pRateDef
   FROM (SELECT RD.*
         FROM  DRATEDEF_DBT RD
         WHERE RD.T_MARKET_PLACE = pMarket_Place
           AND RD.T_TYPE = pType
           AND RD.T_SECTION = (CASE WHEN pSection = -1 THEN RD.T_SECTION ELSE pSection END)
           AND ((    RD.T_OTHERFI = pFromFI
                 AND RD.T_FIID =    pToFI  )
               OR
               (    RD.T_OTHERFI = pToFI
                AND RD.T_FIID =    pFromFI ))
           ORDER BY T_FIID, T_OTHERFI, T_TYPE, T_MARKET_PLACE, T_SECTION) sel
           WHERE ROWNUM=1  ;

    return 1;
  exception
    WHEN NO_DATA_FOUND THEN
         RETURN -1;
    WHEN OTHERS THEN
         DBMS_OUTPUT.PUT_LINE('Ошибка при получении курса' || SQLERRM);
         RETURN -1;
  end;

----------------------------------------------------------------------------------------------------------
  FUNCTION ConvSum_ex2
  (
    SumB     IN NUMBER
   ,pFromFI  IN NUMBER
   ,pToFI    IN NUMBER
   ,pbdate   IN DATE
   ,pRecRateDef IN  DRATEDEF_DBT%ROWTYPE
   ,pRevflag  IN OUT VARCHAR2
   ,pround   IN NUMBER DEFAULT 0 --признак округл. до копеек, по умолч. не округл.
   ,pOnlyRate IN NUMBER DEFAULT 0
   ,pRateType  IN OUT NUMBER --Вид курса, если < -1 не нужно возвращать параметры [pRateType; pIsInverse],= -1 курс еще не определен, = 0 определять не нужно
   ,pRate      OUT NUMBER --Курс
   ,pScale     OUT NUMBER --Масштаб
   ,pPoint     OUT NUMBER --Округление
   ,pIsInverse OUT CHAR   --Признак обратной котировки
   ,pISMANUALINPUT   IN NUMBER DEFAULT -1
  )
  RETURN NUMBER IS

    vRdOtherFI     NUMBER;
    vd             NUMBER;
    vdenominator   NUMBER;
    vnumerator     NUMBER;

    FV             NUMBER;
    FVs            NUMBER;
    FVp            NUMBER;
    IsInv          CHAR;
    IsRel          CHAR;
    stat           NUMBER;
    CalcSum        NUMBER;
    CurNom         NUMBER;
    vtmpFiPoint    NUMBER;

    vRecFinIstr  DFININSTR_DBT%ROWTYPE;
    vRecRateHist DRATEHIST_DBT%ROWTYPE;
 begin

   FV    := 0;
   FVs   := 1;
   FVp   := 0;
   IsInv := CHR(0);

   FI_ConvSum_CourseDate := NULL;

   SELECT *  INTO vRecFinIstr
   FROM DFININSTR_DBT
   WHERE  T_FIID = pRecRateDef.t_OtherFI;
   vtmpFiPoint := vRecFinIstr.T_Point;
   stat := FI_GetCurrentNominal( vRecFinIstr.T_FIID, CurNom,  vtmpFiPoint, pbdate );
   if stat = 1 then
     FV  := ( CurNom * POWER(10,vtmpFiPoint));
     FVp := vtmpFiPoint;
     FVs := vRecFinIstr.T_SCALE;
   end if;

   if vRecFinIstr.T_ISINVERSE = 'X' then
     IsInv := 'X';
   else
     IsInv := CHR(0);
   end if;

   if pRecRateDef.T_ISRELATIVE = 'X' then
     IsRel := 'X';
   else
     IsRel := CHR(0);
   end if;

   if( pbdate >= pRecRateDef.T_SINCEDATE ) then

     if( pISMANUALINPUT = -1 or (pISMANUALINPUT = 0 and pRecRateDef.t_ISMANUALINPUT != 'X') or (pISMANUALINPUT = 1 and pRecRateDef.t_ISMANUALINPUT = 'X') )then
        if (pRecRateDef.T_ISINVERSE = 'X' AND pRevflag = 'X')
        OR (pRecRateDef.T_ISINVERSE <> 'X' AND pRevflag <> 'X') then
          pRevflag := CHR(0);
        else
          pRevflag := 'X';
        end if;

        CalcSum := ConvertSum( SumB, pRecRateDef.T_RATE, pRecRateDef.T_SCALE, pRecRateDef.T_POINT, pRevflag, IsRel,
        FV, FVs, FVp, IsInv, pround, pOnlyRate);

        FI_ConvSum_CourseDate := pRecRateDef.T_SINCEDATE;

        IF pRateType >= -1 THEN
          pRateType := CASE WHEN pRateType = 0 OR (pRateType > 0 AND pRateType != pRecRateDef.t_type) THEN 0 ELSE pRecRateDef.t_type END;
          pRate := pRecRateDef.t_rate;
          pScale := pRecRateDef.t_scale;
          pPoint := pRecRateDef.t_point;
          pIsInverse := pRevflag;
        END IF;
     end if;
   else
     SELECT * INTO  vRecRateHist
     FROM (SELECT RH.*
           FROM  DRATEHIST_DBT RH
           WHERE RH.T_RATEID = pRecRateDef.T_RATEID
             AND RH.T_SINCEDATE <= pbdate
             ORDER BY T_RATEID, T_SINCEDATE DESC) sel
             WHERE ROWNUM=1 ;

     if( pISMANUALINPUT = -1 or (pISMANUALINPUT = 0 and vRecRateHist.t_ISMANUALINPUT != 'X') or (pISMANUALINPUT = 1 and vRecRateHist.t_ISMANUALINPUT = 'X') ) then
        if (vRecRateHist.T_ISINVERSE = 'X' AND pRevflag = 'X')
        OR (vRecRateHist.T_ISINVERSE <> 'X' AND pRevflag <> 'X') then
          pRevflag := CHR(0);
        else
          pRevflag := 'X';
        end if;


        CalcSum := ConvertSum( SumB, vRecRateHist.T_RATE, vRecRateHist.T_SCALE, vRecRateHist.T_POINT, pRevflag, IsRel,
        FV, FVs, FVp, IsInv, pround,pOnlyRate );

        FI_ConvSum_CourseDate := vRecRateHist.T_SINCEDATE;

        IF pRateType >= -1 THEN
          pRateType := CASE WHEN pRateType = 0 OR (pRateType > 0 AND pRateType != pRecRateDef.t_type) THEN 0 ELSE pRecRateDef.t_type END;
          pRate := vRecRateHist.t_rate;
          pScale := vRecRateHist.t_scale;
          pPoint := vRecRateHist.t_point;
          pIsInverse := pRevflag;
        END IF;
     end if;

   end if;

   return CalcSum;

  exception
    WHEN NO_DATA_FOUND THEN
         RETURN NULL;
    WHEN OTHERS THEN
         DBMS_OUTPUT.PUT_LINE('Ошибка' || SQLERRM);
         RETURN NULL;
  end;

  FUNCTION ConvSum_ex
  (
    SumB     IN NUMBER
   ,pFromFI  IN NUMBER
   ,pToFI    IN NUMBER
   ,pbdate   IN DATE
   ,pRecRateDef IN  DRATEDEF_DBT%ROWTYPE
   ,pRevflag  IN OUT VARCHAR2
   ,pround   IN NUMBER DEFAULT 0 --признак округл. до копеек, по умолч. не округл.
   ,pOnlyRate IN NUMBER DEFAULT 0
   ,pISMANUALINPUT   IN NUMBER DEFAULT -1
  )
  RETURN NUMBER IS
    m_RateType  NUMBER; --Вид курса
    m_Rate      NUMBER; --Курс
    m_Scale     NUMBER; --Масштаб
    m_Point     NUMBER; --Округление
    m_IsInverse CHAR;   --Признак обратной котировки

  BEGIN
    m_RateType := -2;
    RETURN ConvSum_ex2( SumB, pFromFI, pToFI, pbdate, pRecRateDef, pRevflag, pround, pOnlyRate, m_RateType, m_Rate, m_Scale, m_Point, m_IsInverse, pISMANUALINPUT );
  END;

----------------------------------------------------------------------------------------------------------
-- Функция определения суммы конверсии с использованием данных о номинале ФИ
  function  ConvertSum(
                    SumB       IN NUMBER,           -- Исходная сумма
                    Rate       IN NUMBER,           -- Курс
                    Scale      IN NUMBER,           -- Масштаб
                    Point      IN NUMBER,           -- Округление
                    OutRate    IN CHAR,          -- Признак обратной котировки
                    IsRelative IN CHAR,          -- Признак относительной котировки
                    FaceValue  IN NUMBER,           -- Номинал (относительно чего задается курс, если он относительный)
                    FV_Scale   IN NUMBER,
                    FV_Point   IN NUMBER,
                    IsInverse  IN CHAR,
                    pround   IN NUMBER DEFAULT 0, --признак округл. до копеек, по умолч. не округл.
                    pOnlyRate IN NUMBER DEFAULT 0
           ) return NUMBER is
    vd             NUMBER;
    vnumerator     NUMBER;
    vdenominator   NUMBER;
    stat           NUMBER;
    vswap          NUMBER;
    FVden          NUMBER;
    vpoint         NUMBER;
  begin

    vnumerator := Rate; -- Числитель
    vdenominator := GREATEST( 1, Scale ); -- Знаменатель

    vpoint := GREATEST( 0, Point );

    if IsRelative = 'X' then
        vpoint := vpoint + 2;  --???
        vnumerator := vnumerator * FaceValue;
        FVden := GREATEST( 1, FV_Scale );
        vdenominator := vdenominator * FVden;
        vpoint := vpoint + FV_Point;
        if IsInverse = 'X' then
            vswap := vdenominator;
            vdenominator := vnumerator;
            vnumerator := vswap;
        end if;
    end if;

    if  vpoint >= 4 then
        vnumerator := vnumerator / (10000);
        vpoint := vpoint - 4;

        while  vpoint > 0 loop
            vdenominator := vdenominator * 10;
            vpoint := vpoint - 1;
        end loop;

    else
        while  vpoint > 0 loop
            vnumerator := vnumerator / 10;
            vpoint := vpoint - 1;
        end loop;
    end if;

    if OutRate = 'X' then
        vswap := vdenominator;
        vdenominator := vnumerator;
        vnumerator := vswap;
    end if;

    if vdenominator = 0 then  --Иначе возникнет деление на 0
        return 0;
    end if;

    vd := SumB * vnumerator / vdenominator;

    if pOnlyRate = 1 and FaceValue <> 0 then

      vdenominator := FaceValue;

      if IsRelative = 'X' then
         vpoint := GREATEST( 0, FV_Point );

         while  vpoint > 0 loop
             vdenominator := vdenominator / 10;
             vpoint := vpoint - 1;
         end loop;

         vdenominator := vdenominator / GREATEST( 1, FV_Scale );
      end if;

      vd := 100.0*vd/vdenominator;

    end if;

    if pround <> 0 then
      vd := ROUND( vd, 2 );
    end if;

    return vd;
  end;

------------------------------------------------------------------------------------
  --Функция пересчёта суммы в валюте по курсу к национальной валюте
  FUNCTION ConvSumNat
  (
    SumB       IN NUMBER           --конвертируемая сумма
   ,pFI        IN NUMBER           --валюта, для которой осуществляется конвертация
   ,pDirection IN CHAR             --направление конвертации: 'X' соответствует конвертации из заданной валюты в национальную, chr(0) - обратной конвертации из национальной валюты в заданную
   ,pConvDate  IN DATE             --Дата курса
   ,pRound     IN NUMBER DEFAULT 0 --признак округления (см. функцию ConvSum)
   ,pRateType  IN OUT NUMBER --Вид курса, если < -1 не нужно возвращать параметры [pRateType; pIsInverse],= -1 курс еще не определен, = 0 определять не нужно
   ,pRate      OUT NUMBER --Курс
   ,pScale     OUT NUMBER --Масштаб
   ,pPoint     OUT NUMBER --Округление
   ,pIsInverse OUT CHAR   --Признак обратной котировки
  )
  RETURN NUMBER IS

    vRevflag     CHAR;
    stat         NUMBER;
    vRecRateDef  DRATEDEF_DBT%ROWTYPE;
    v_CrsFI_Code VARCHAR2(10);
    v_CrsFIID    NUMBER;
    CalcSum      NUMBER;
    m_Rate1 NUMBER; --числитель (курс между базовой и кроссируемой валютой)
    m_Scale1 NUMBER;
    m_Point1 NUMBER;
    m_IsInverse1 CHAR;
    m_Rate2 NUMBER; --знаменатель (курс между котируемой и кроссируемой валютой)
    m_Scale2 NUMBER;
    m_Point2 NUMBER;
    m_IsInverse2 CHAR;

    v_IsCur NUMBER;
  BEGIN

    IF pFI = NATCUR THEN
      RETURN SumB;
    END IF;

    stat := GetDominantRate(pFI, NATCUR, vRecRateDef);

    IF stat <> 1 THEN
      v_IsCur := FI_IsCurrency( pFI );

      IF v_IsCur != 1 THEN
        RETURN NULL;
      END IF;

      v_CrsFI_Code := rsb_common.GetRegStrValue('CB\FINTOOLS\CURRCALC\CROSSCURRENCY', 0);

      SELECT t_fiid INTO v_CrsFIID
        FROM dfininstr_dbt
       WHERE t_fi_code = v_CrsFI_Code;

      IF pDirection = 'X' THEN
        stat := GetDominantRate(pFI, v_CrsFIID, vRecRateDef);

        IF stat <> 1 THEN RETURN NULL; END IF;

        IF   (vRecRateDef.T_OTHERFI = pFI AND vRecRateDef.T_FIID = v_CrsFIID)
        THEN
          vRevflag := CHR(0);
        ELSE
          vRevflag := 'X';
        END IF;

        CalcSum := ConvSum_ex2(SumB, pFI, v_CrsFIID, pConvDate - 1, vRecRateDef, vRevflag, pRound, 0, pRateType, m_Rate1, m_Scale1, m_Point1, m_IsInverse1);

        stat := GetDominantRate(v_CrsFIID, NATCUR, vRecRateDef);

        IF stat <> 1 THEN RETURN NULL; END IF;

        IF   (vRecRateDef.T_OTHERFI = v_CrsFIID AND vRecRateDef.T_FIID = NATCUR)
        THEN
          vRevflag := CHR(0);
        ELSE
          vRevflag := 'X';
        END IF;

        CalcSum := ConvSum_ex2(CalcSum, v_CrsFIID, NATCUR, pConvDate, vRecRateDef, vRevflag, pRound, 0, pRateType, m_Rate2, m_Scale2, m_Point2, m_IsInverse2);
        IF pRateType >= -1 THEN
          FI_DetermineRate
          (
            m_Rate1 --числитель (курс между базовой и кроссируемой валютой)
           ,m_Scale1
           ,m_Point1
           ,m_IsInverse1
           ,m_Rate2 --знаменатель (курс между котируемой и кроссируемой валютой)
           ,m_Scale2
           ,m_Point2
           ,m_IsInverse2
           ,pRate --возвращаемый курс
           ,pScale
           ,pPoint
           ,pIsInverse
          );
        END IF;
        RETURN CalcSum;
      ELSE
        stat := GetDominantRate(NATCUR, v_CrsFIID, vRecRateDef);

        IF stat <> 1 THEN RETURN NULL; END IF;

        IF   (vRecRateDef.T_OTHERFI = NATCUR AND vRecRateDef.T_FIID = v_CrsFIID)
        THEN
          vRevflag := CHR(0);
        ELSE
          vRevflag := 'X';
        END IF;

        CalcSum := ConvSum_ex2(SumB, NATCUR, v_CrsFIID, pConvDate, vRecRateDef, vRevflag, pRound, 0, pRateType, m_Rate1, m_Scale1, m_Point1, m_IsInverse1);

        stat := GetDominantRate(v_CrsFIID, pFI, vRecRateDef);

        IF stat <> 1 THEN RETURN NULL; END IF;

        IF   (vRecRateDef.T_OTHERFI = v_CrsFIID AND vRecRateDef.T_FIID = pFI)
        THEN
          vRevflag := CHR(0);
        ELSE
          vRevflag := 'X';
        END IF;

        CalcSum := ConvSum_ex2(CalcSum, v_CrsFIID, pFI, pConvDate - 1, vRecRateDef, vRevflag, pRound, 0, pRateType, m_Rate2, m_Scale2, m_Point2, m_IsInverse2);
        IF pRateType >= -1 THEN
          FI_DetermineRate
          (
            m_Rate1 --числитель (курс между базовой и кроссируемой валютой)
           ,m_Scale1
           ,m_Point1
           ,m_IsInverse1
           ,m_Rate2 --знаменатель (курс между котируемой и кроссируемой валютой)
           ,m_Scale2
           ,m_Point2
           ,m_IsInverse2
           ,pRate --возвращаемый курс
           ,pScale
           ,pPoint
           ,pIsInverse
          );
        END IF;
        RETURN CalcSum;
      END IF;
    END IF;

    IF   (vRecRateDef.T_OTHERFI = pFI AND vRecRateDef.T_FIID = NATCUR AND pDirection = 'X')
      OR (vRecRateDef.T_FIID = pFI AND vRecRateDef.T_OTHERFI = NATCUR AND pDirection != 'X')
    THEN
      vRevflag := CHR(0);
    ELSE
      vRevflag := 'X';
    END IF;

    IF pDirection = 'X' THEN
      RETURN ConvSum_ex2(SumB, pFI, NATCUR, pConvDate, vRecRateDef, vRevflag, pRound, 0, pRateType, pRate, pScale, pPoint, pIsInverse);
    ELSE
      RETURN ConvSum_ex2(SumB, NATCUR, pFI, pConvDate, vRecRateDef, vRevflag, pRound, 0, pRateType, pRate, pScale, pPoint, pIsInverse);
    END IF;

  EXCEPTION
    WHEN NO_DATA_FOUND THEN
         RETURN NULL;
    WHEN OTHERS THEN
         DBMS_OUTPUT.PUT_LINE('Ошибка' || SQLERRM);
         RETURN NULL;
  END ConvSumNat;

------------------------------------------------------------------------------------
  --Функция пересчёта суммы в валюте по курсу заданного вида к национальной валюте
  FUNCTION ConvSumNatType
  (
    SumB       IN NUMBER           --конвертируемая сумма
   ,pFI        IN NUMBER           --валюта, для которой осуществляется конвертация
   ,pDirection IN CHAR             --направление конвертации: 'X' соответствует конвертации из заданной валюты в национальную, chr(0) - обратной конвертации из национальной валюты в заданную
   ,pConvDate  IN DATE             --Дата курса
   ,pType      IN NUMBER           --вид курса
   ,pRound     IN NUMBER DEFAULT 0 --признак округления (см. функцию ConvSum)
   ,pRateType  IN OUT NUMBER --Вид курса, если < -1 не нужно возвращать параметры [pRateType; pIsInverse],= -1 курс еще не определен, = 0 определять не нужно
   ,pRate      OUT NUMBER --Курс
   ,pScale     OUT NUMBER --Масштаб
   ,pPoint     OUT NUMBER --Округление
   ,pIsInverse OUT CHAR   --Признак обратной котировки
  )
  RETURN NUMBER IS

    vRevflag     CHAR;
    stat         NUMBER;
    vRecRateDef  DRATEDEF_DBT%ROWTYPE;
    v_CrsFI_Code VARCHAR2(10);
    v_CrsFIID    NUMBER;
    v_6Type    NUMBER;
    CalcSum    NUMBER;
    m_Rate1 NUMBER; --числитель (курс между базовой и кроссируемой валютой)
    m_Scale1 NUMBER;
    m_Point1 NUMBER;
    m_IsInverse1 CHAR;
    m_Rate2 NUMBER; --знаменатель (курс между котируемой и кроссируемой валютой)
    m_Scale2 NUMBER;
    m_Point2 NUMBER;
    m_IsInverse2 CHAR;
    v_IsCur NUMBER;
  BEGIN

    IF pFI = NATCUR THEN
      RETURN SumB;
    END IF;

    v_6Type := rsb_common.GetRegIntValue('CB\FINTOOLS\CBCURRATEKIND', 0);

    stat := GetRate( pType, pFI, NATCUR, vRecRateDef);

    v_IsCur := FI_IsCurrency( pFI );

    IF stat <> 1 AND v_6Type = pType AND v_IsCur = 1 THEN

      v_CrsFI_Code := rsb_common.GetRegStrValue('CB\FINTOOLS\CURRCALC\CROSSCURRENCY', 0);

      SELECT t_fiid INTO v_CrsFIID
        FROM dfininstr_dbt
       WHERE t_fi_code = v_CrsFI_Code;

      IF pDirection = 'X' THEN
        stat := GetRate( pType, pFI, v_CrsFIID, vRecRateDef);

        IF stat <> 1 THEN RETURN NULL; END IF;

        IF   (vRecRateDef.T_OTHERFI = pFI AND vRecRateDef.T_FIID = v_CrsFIID)
        THEN
          vRevflag := CHR(0);
        ELSE
          vRevflag := 'X';
        END IF;

        CalcSum := ConvSum_ex2(SumB, pFI, v_CrsFIID, pConvDate - 1, vRecRateDef, vRevflag, pRound, 0, pRateType, m_Rate1, m_Scale1, m_Point1, m_IsInverse1);

        stat := GetRate( pType, v_CrsFIID, NATCUR, vRecRateDef);

        IF stat <> 1 THEN RETURN NULL; END IF;

        IF   (vRecRateDef.T_OTHERFI = v_CrsFIID AND vRecRateDef.T_FIID = NATCUR)
        THEN
          vRevflag := CHR(0);
        ELSE
          vRevflag := 'X';
        END IF;

        CalcSum := ConvSum_ex2(CalcSum, v_CrsFIID, NATCUR, pConvDate, vRecRateDef, vRevflag, pRound, 0, pRateType, m_Rate2, m_Scale2, m_Point2, m_IsInverse2);
        IF pRateType >= -1 THEN
          FI_DetermineRate
          (
            m_Rate1 --числитель (курс между базовой и кроссируемой валютой)
           ,m_Scale1
           ,m_Point1
           ,m_IsInverse1
           ,m_Rate2 --знаменатель (курс между котируемой и кроссируемой валютой)
           ,m_Scale2
           ,m_Point2
           ,m_IsInverse2
           ,pRate --возвращаемый курс
           ,pScale
           ,pPoint
           ,pIsInverse
          );
        END IF;
        RETURN CalcSum;
      ELSE
        stat := GetRate( pType, NATCUR, v_CrsFIID, vRecRateDef);

        IF stat <> 1 THEN RETURN NULL; END IF;

        IF   (vRecRateDef.T_OTHERFI = NATCUR AND vRecRateDef.T_FIID = v_CrsFIID)
        THEN
          vRevflag := CHR(0);
        ELSE
          vRevflag := 'X';
        END IF;

        CalcSum := ConvSum_ex2(SumB, NATCUR, v_CrsFIID, pConvDate, vRecRateDef, vRevflag, pRound, 0, pRateType, m_Rate1, m_Scale1, m_Point1, m_IsInverse1);

        stat := GetRate( pType, v_CrsFIID, pFI, vRecRateDef);

        IF stat <> 1 THEN RETURN NULL; END IF;

        IF   (vRecRateDef.T_OTHERFI = v_CrsFIID AND vRecRateDef.T_FIID = pFI)
        THEN
          vRevflag := CHR(0);
        ELSE
          vRevflag := 'X';
        END IF;

        CalcSum := ConvSum_ex2(CalcSum, v_CrsFIID, pFI, pConvDate - 1, vRecRateDef, vRevflag, pRound, 0, pRateType, m_Rate2, m_Scale2, m_Point2, m_IsInverse2);
        IF pRateType >= -1 THEN
          FI_DetermineRate
          (
            m_Rate1 --числитель (курс между базовой и кроссируемой валютой)
           ,m_Scale1
           ,m_Point1
           ,m_IsInverse1
           ,m_Rate2 --знаменатель (курс между котируемой и кроссируемой валютой)
           ,m_Scale2
           ,m_Point2
           ,m_IsInverse2
           ,pRate --возвращаемый курс
           ,pScale
           ,pPoint
           ,pIsInverse
          );
        END IF;
        RETURN CalcSum;
      END IF;
    END IF;

    IF stat <> 1 THEN RETURN NULL; END IF;

    IF   (vRecRateDef.T_OTHERFI = pFI AND vRecRateDef.T_FIID = NATCUR AND pDirection = 'X')
      OR (vRecRateDef.T_FIID = pFI AND vRecRateDef.T_OTHERFI = NATCUR AND pDirection != 'X')
    THEN
      vRevflag := CHR(0);
    ELSE
      vRevflag := 'X';
    END IF;

    IF pDirection = 'X' THEN
      RETURN ConvSum_ex2(SumB, pFI, NATCUR, pConvDate, vRecRateDef, vRevflag, pRound, 0, pRateType, pRate, pScale, pPoint, pIsInverse);
    ELSE
      RETURN ConvSum_ex2(SumB, NATCUR, pFI, pConvDate, vRecRateDef, vRevflag, pRound, 0, pRateType, pRate, pScale, pPoint, pIsInverse);
    END IF;

  EXCEPTION
    WHEN NO_DATA_FOUND THEN
         RETURN NULL;
    WHEN OTHERS THEN
         DBMS_OUTPUT.PUT_LINE('Ошибка' || SQLERRM);
         RETURN NULL;
  END ConvSumNatType;

------------------------------------------------------------------------------------
  -- Функция определения суммы конверсии за дату
  FUNCTION ConvSum2
  (
    SumB     IN NUMBER  --Исходная сумма
   ,pFromFI  IN NUMBER  --Исходный Фин. инструмент
   ,pToFI    IN NUMBER  --Фин. инструмент, в который надо пересчитать
   ,pbdate   IN DATE    --Дата курса
   ,pround   IN NUMBER DEFAULT 0 --признак округл. до копеек, по умолч. не округл. (<>0 - округл.)
   ,pRateType  IN OUT NUMBER --Вид курса, если <-1 то возвращать параметры [pRateType; pIsInverse] не нужно
   ,pRate      OUT NUMBER --Курс
   ,pScale     OUT NUMBER --Масштаб
   ,pPoint     OUT NUMBER --Округление
   ,pIsInverse OUT CHAR   --Признак обратной котировки
  )
  RETURN NUMBER IS

    vRevflag       CHAR;
    stat           NUMBER;
    vRecRateDef  DRATEDEF_DBT%ROWTYPE;
  BEGIN

   IF pFromFI = pToFI THEN
     RETURN SumB;
   END IF;

   IF pToFI = NATCUR THEN
     RETURN ConvSumNat( SumB, pFromFI, 'X', pbdate, pround, pRateType, pRate, pScale, pPoint, pIsInverse );
   ELSIF pFromFI = NATCUR THEN
     RETURN ConvSumNat( SumB, pToFI, CHR(0), pbdate, pround, pRateType, pRate, pScale, pPoint, pIsInverse );
   ELSE
     stat := GetDominantRate( pFromFI, pToFI, vRecRateDef );

     IF stat <> 1 THEN
       RETURN CalcSumCross2(SumB, pFromFI, pToFI, pbdate, pround, pRateType, pRate, pScale, pPoint, pIsInverse);
     END IF;

     IF vRecRateDef.T_OTHERFI = pFromFI AND vRecRateDef.T_FIID = pToFI THEN
       vRevflag := CHR(0);
     ELSE
       vRevflag := 'X';
     END IF;

     RETURN ConvSum_ex2( SumB, pFromFI, pToFI, pbdate, vRecRateDef, vRevflag, pround, 0, pRateType, pRate, pScale, pPoint, pIsInverse);

   END IF;

  EXCEPTION
    WHEN NO_DATA_FOUND THEN
         RETURN NULL;
    WHEN OTHERS THEN
         DBMS_OUTPUT.PUT_LINE('Ошибка' || SQLERRM);
         RETURN NULL;
  END ConvSum2;

------------------------------------------------------------------------------------
  -- Функция определения суммы конверсии за дату
  FUNCTION ConvSum
  (
    SumB     IN NUMBER
   ,pFromFI  IN NUMBER
   ,pToFI    IN NUMBER
   ,pbdate   IN DATE
   ,pround   IN NUMBER DEFAULT 0 --признак округл. до копеек, по умолч. не округл.
  )
  RETURN NUMBER IS

    m_RateType  NUMBER; --Вид курса
    m_Rate      NUMBER; --Курс
    m_Scale     NUMBER; --Масштаб
    m_Point     NUMBER; --Округление
    m_IsInverse CHAR;   --Признак обратной котировки

  BEGIN
    m_RateType := -2;
    RETURN ConvSum2( SumB, pFromFI, pToFI, pbdate, pround, m_RateType, m_Rate, m_Scale, m_Point, m_IsInverse );
  END ConvSum;

------------------------------------------------------------------------------------
  FUNCTION ConvSumType2
  (
    SumB     IN NUMBER
   ,pFromFI  IN NUMBER
   ,pToFI    IN NUMBER
   ,pType    IN NUMBER
   ,pbdate   IN DATE
   ,pround   IN NUMBER DEFAULT 0 --признак округл. до копеек, по умолч. не округл.
   ,pRateType  IN OUT NUMBER --Вид курса, если <-1 то возвращать параметры [pRateType; pIsInverse] не нужно
   ,pRate      OUT NUMBER --Курс
   ,pScale     OUT NUMBER --Масштаб
   ,pPoint     OUT NUMBER --Округление
   ,pIsInverse OUT CHAR   --Признак обратной котировки
  )
  RETURN NUMBER IS

    vRevflag       CHAR;
    stat           NUMBER;
    vRecRateDef    DRATEDEF_DBT%ROWTYPE;
    v_6Type        NUMBER;
    CalcSum        NUMBER;
    m_Rate1 NUMBER; --числитель (курс между базовой и кроссируемой валютой)
    m_Scale1 NUMBER;
    m_Point1 NUMBER;
    m_IsInverse1 CHAR;
    m_Rate2 NUMBER; --знаменатель (курс между котируемой и кроссируемой валютой)
    m_Scale2 NUMBER;
    m_Point2 NUMBER;
    m_IsInverse2 CHAR;

  BEGIN

   IF pFromFI = pToFI THEN
     RETURN SumB;
   END IF;

   v_6Type := rsb_common.GetRegIntValue('CB\FINTOOLS\CBCURRATEKIND', 0);

   IF v_6Type != pType THEN
     stat := GetRate( pType, pFromFI, pToFI, vRecRateDef );
     IF stat <> 1 THEN RETURN NULL; END IF;
     IF vRecRateDef.T_OTHERFI = pFromFI AND vRecRateDef.T_FIID = pToFI THEN
       vRevflag := CHR(0);
     ELSE
       vRevflag := 'X';
     END IF;

     IF pRateType >= -1 THEN
       pRateType  := vRecRateDef.t_Type     ;
       pRate      := vRecRateDef.t_Rate     ;
       pScale     := vRecRateDef.t_Scale    ;
       pPoint     := vRecRateDef.t_Point    ;
       pIsInverse := vRevflag;
     END IF;

     RETURN ConvSum_ex( SumB, pFromFI, pToFI, pbdate, vRecRateDef, vRevflag, pround );
   ELSE

     IF pToFI = NATCUR THEN
       RETURN ConvSumNatType( SumB, pFromFI, 'X', pbdate, pType, pround, pRateType, pRate, pScale, pPoint, pIsInverse );
     ELSIF pFromFI = NATCUR THEN
       RETURN ConvSumNatType( SumB, pToFI, CHR(0), pbdate, pType, pround, pRateType, pRate, pScale, pPoint, pIsInverse );
     ELSE
       CalcSum := ConvSumNatType( SumB, pFromFI, 'X', pbdate, pType, 0, pRateType, m_Rate1, m_Scale1, m_Point1, m_IsInverse1 );
       CalcSum := ConvSumNatType( CalcSum, pToFI, CHR(0), pbdate, pType, pround, pRateType, m_Rate2, m_Scale2, m_Point2, m_IsInverse2 );
       IF pRateType >= -1 THEN
         FI_DetermineRate
         (
           m_Rate1 --числитель (курс между базовой и кроссируемой валютой)
          ,m_Scale1
          ,m_Point1
          ,m_IsInverse1
          ,m_Rate2 --знаменатель (курс между котируемой и кроссируемой валютой)
          ,m_Scale2
          ,m_Point2
          ,CASE WHEN m_IsInverse2 = 'X' THEN CHR(0) ELSE 'X' END
          ,pRate --возвращаемый курс
          ,pScale
          ,pPoint
          ,pIsInverse
         );
       END IF;
       RETURN CalcSum;
     END IF;

   END IF;
   EXCEPTION
     WHEN NO_DATA_FOUND THEN
          RETURN NULL;
     WHEN OTHERS THEN
          DBMS_OUTPUT.PUT_LINE('Ошибка' || SQLERRM);
          RETURN NULL;
  END;

------------------------------------------------------------------------------------
  FUNCTION ConvSumType
  (
    SumB     IN NUMBER
   ,pFromFI  IN NUMBER
   ,pToFI    IN NUMBER
   ,pType    IN NUMBER
   ,pbdate   IN DATE
   ,pround   IN NUMBER DEFAULT 0 --признак округл. до копеек, по умолч. не округл.
  )
  RETURN NUMBER IS

    m_RateType  NUMBER; --Вид курса
    m_Rate      NUMBER; --Курс
    m_Scale     NUMBER; --Масштаб
    m_Point     NUMBER; --Округление
    m_IsInverse CHAR;   --Признак обратной котировки

  BEGIN
    m_RateType := -2;
    RETURN ConvSumType2( SumB, pFromFI, pToFI, pType, pbdate, pround, m_RateType, m_Rate, m_Scale, m_Point, m_IsInverse );
  END;

------------------------------------------------------------------------------------
  function ConvSumMP   (SumB          IN NUMBER,
                        pFromFI       IN NUMBER,
                        pToFI         IN NUMBER,
                        pType         IN NUMBER, --Тип курса
                        pMarket_Place IN NUMBER, --Торговая площадка
                        pSection      IN NUMBER, --Секция торговой площадки
                        pbdate        IN DATE,
                        pround   IN NUMBER DEFAULT 0 --признак округл. до копеек, по умолч. не округл.
                      )
           return NUMBER is

    vRevflag       CHAR;
    stat           NUMBER;
    vRecRateDef  DRATEDEF_DBT%ROWTYPE;
 begin

   if pFromFI = pToFI then
     return SumB;
   end if;

   stat := GetRateByMP( pFromFI, pToFI, pType, pMarket_Place, pSection, vRecRateDef );
   if stat <> 1 then return NULL; end if;
   if vRecRateDef.T_OTHERFI = pFromFI and vRecRateDef.T_FIID = pToFI then
     vRevflag := CHR(0);
   else
     vRevflag := 'X';
   end if;

   return ConvSum_ex( SumB, pFromFI, pToFI, pbdate, vRecRateDef, vRevflag, pround );

   exception
     WHEN NO_DATA_FOUND THEN
          RETURN NULL;
     WHEN OTHERS THEN
          DBMS_OUTPUT.PUT_LINE('Ошибка' || SQLERRM);
          RETURN NULL;
  end;

----------------------------------------------------------------------------------------------------------
  function FI_GetCurrentNominal( pFIID              IN NUMBER,
                                 pCurrentNominal    IN OUT NUMBER,
                                 pNominalPoint      IN OUT NUMBER,
                                 pDate              IN DATE,
                                 pIsClosed          IN NUMBER
                               )
           return NUMBER is
    stat           NUMBER := 0;
    vtmpNominal    NUMBER;
    vdenominator   NUMBER; -- Знаменатель
    vnumerator     NUMBER; -- Числитель
    vrate          NUMBER;

 begin

   stat := FI_GetNominal( pFIID, pNominalPoint, vtmpNominal, pDate );

   if  stat = 1  then
      --если бумага с частичным погашением, берем сумму процентов частичных погашений
      --на дату (независимо от их выполненности )
      vrate := FI_GetPartialPersent( pFIID, pDate, pIsClosed );

      pCurrentNominal := vtmpNominal*(1 - (vrate / 100.));

   end if;

   return stat;

  exception
    WHEN NO_DATA_FOUND THEN
         RETURN NULL;
    WHEN OTHERS THEN
         DBMS_OUTPUT.PUT_LINE('Ошибка' || SQLERRM);
         RETURN NULL;
  end;

  ----------------------------------------------------------------------------------------------------------
  function FI_GetNominalOnDate( pFIID              IN NUMBER,
                                pDate              IN DATE,
                                pIsClosed          IN NUMBER
                              ) return NUMBER is

    stat           NUMBER;
    vtmpNominal    NUMBER;
    pNominalPoint  NUMBER;
  begin

    stat := FI_GetCurrentNominal( pFIID, vtmpNominal, pNominalPoint, pDate, pIsClosed );

    return vtmpNominal;

  end;

  ------------------------------------------------------------------------------------------------------
  function FI_GetFaceValueFI(pFIID IN NUMBER)
  return NUMBER is
    vFaceValueFI           NUMBER;
    vRecFinIstr  DFININSTR_DBT%ROWTYPE;
  begin

    SELECT *  INTO vRecFinIstr
    FROM DFININSTR_DBT
    WHERE  T_FIID = pFIID;

    if FI_AvrKindsGetRoot( 2, vRecFinIstr.t_AvoirKind ) = AVOIRKIND_INVESTMENT_SHARE THEN
      SELECT t_FormValueFIID INTO vFaceValueFI
      FROM davrinvst_dbt
      WHERE t_FIID = pFIID;
    else
      vFaceValueFI := vRecFinIstr.t_FaceValueFI;
    end if;

    return vFaceValueFI;
    exception
      WHEN NO_DATA_FOUND THEN
           RETURN -1;
      WHEN OTHERS THEN
           DBMS_OUTPUT.PUT_LINE('Ошибка' || SQLERRM);
           RETURN -1;
  end;

------------------------------------------------------------------------------------------------------
  function FI_GetNominal( pFIID              IN NUMBER,
                          pPoint             IN OUT NUMBER,
                          pNominal_lrate     IN OUT NUMBER,
                          pDate              IN DATE
                        )
           return NUMBER is

    stat           NUMBER;
    vtmpNominal    NUMBER;
    vDegree        NUMBER;

    vdenominator   NUMBER;
    vnumerator     NUMBER;

    vRecFinIstr  DFININSTR_DBT%ROWTYPE;

    InvShareFormValue DAVRINVST_DBT.t_FormValue%Type;
    vFaceValue        DFININSTR_DBT.t_FaceValue%Type;
    IndexNom          DAVOIRISS_DBT.t_IndexNom%TYPE;
 begin

  SELECT *  INTO vRecFinIstr
  FROM DFININSTR_DBT
  WHERE  T_FIID = pFIID;

  begin
    SELECT avr.t_IndexNom
      INTO IndexNom
      FROM davoiriss_dbt avr
     WHERE avr.T_FIID = pFIID;
  exception
    when NO_DATA_FOUND then IndexNom := chr(0);
    when OTHERS then
         IndexNom := chr(0);
         dbms_output.put_line('Ошибка' || SQLERRM);
  end;

  pPoint := vRecFinIstr.T_POINT;

  --если просим вернуть точность и не считать точный номинал,
  --значит внутри функции пересчет номинала для учета точности и масштаба не делаем
  vDegree := 0;

  --Nikonorov Evgeny
  --Закомментраил этот кусок по запросу 127292.
  --Не понятно, зачем он здесь вообще был, так как, например, пложительная точность не учитывалась никак,
  --а вот отрицательная очень портила номинал, увеличивая его пропорционально точности
  --if  vRecFinIstr.T_POINT < 0 then
  --   vDegree := ABS(vRecFinIstr.T_POINT); --для учета отрицательной точности округления
  --end if;

  if( FI_AvrKindsGetRoot(2, vRecFinIstr.t_AvoirKind) = AVOIRKIND_INVESTMENT_SHARE ) THEN
    -- для пая номинал берем из анкеты пая
    SELECT t_FormValue INTO InvShareFormValue
      FROM davrinvst_dbt
     WHERE t_FIID = pFIID;

    pNominal_lrate := InvShareFormValue;
  else
    -- выполним пересчет номинала для учета точности и масштаба
    -- в связи с изменениями коснувшимися способа хранения  номинала в fiinstr.dbt точность Point
    -- теперь учитывать не нужно #69750
    stat := PrepareRate(POWER(10, vDegree), vRecFinIstr.T_SCALE, /*vRecFinIstr.T_POINT*/0, FALSE, FALSE, 0, 0, 0, FALSE, vnumerator, vdenominator);

    if( vdenominator = 0 ) then
       -- CB_GetError( Buff );
       -- stat = Buff.stat;
       return NULL;
    end if;

    IF( pDate = TO_DATE('01.01.0001', 'DD.MM.YYYY') ) THEN
       vFaceValue := vRecFinIstr.t_FaceValue;
    ELSE
       BEGIN
         IF( IndexNom = chr(88) ) THEN -- для индексированного номинала точно на дату
            BEGIN
            SELECT F.t_FaceValue INTO vFaceValue
              FROM (SELECT t_FaceValue
                      FROM DV_FI_FACEVALUE_HIST
                     WHERE t_FIID    = pFIID
                       AND t_BegDate = pDate
                     ORDER BY t_ID DESC) F
             WHERE ROWNUM = 1;
             EXCEPTION
               WHEN NO_DATA_FOUND
               --последний известный номинал
               THEN SELECT F.t_FaceValue INTO vFaceValue
                      FROM (SELECT t_FaceValue
                              FROM DV_FI_FACEVALUE_HIST
                             WHERE t_FIID     = pFIID
                               AND t_BegDate <= pDate
                             ORDER BY t_ID DESC) F
                     WHERE ROWNUM = 1;
             END;
         ELSE
            SELECT F.t_FaceValue INTO vFaceValue
              FROM (SELECT t_FaceValue
                      FROM DV_FI_FACEVALUE_HIST
                     WHERE t_FIID = pFIID
                       AND pDate >= t_BegDate
                     ORDER BY t_BegDate DESC) F
             WHERE ROWNUM = 1;
         END IF;

       EXCEPTION
          WHEN NO_DATA_FOUND THEN vFaceValue := CASE WHEN IndexNom = chr(88) THEN 0.0 ELSE vRecFinIstr.t_FaceValue END;
          WHEN OTHERS THEN vFaceValue := CASE WHEN IndexNom = chr(88) THEN 0.0 ELSE vRecFinIstr.t_FaceValue END;
       END;
    END IF;

    pNominal_lrate := vFaceValue * vnumerator / vdenominator;

  end if;

  return 1;
  exception
    WHEN NO_DATA_FOUND THEN
         RETURN NULL;
    WHEN OTHERS THEN
         DBMS_OUTPUT.PUT_LINE('Ошибка' || SQLERRM);
         RETURN NULL;
  end;

  --Вычисляет текущий объем выпуска на дату
  function FI_GetQTYOnDate( pFIID  IN NUMBER,
                            pDate  IN DATE
                          )
           return NUMBER is
     vQty DAVOIRISS_DBT.t_Qty%Type;
  begin

     BEGIN
       SELECT F.t_Qty INTO vQty
         FROM (SELECT t_Qty
                 FROM DV_FI_QTY_HIST
                WHERE t_FIID = pFIID AND
                      (t_Sort = 2 or (pDate <= t_EndDate and pDate >= t_BegDate) ) --t_Sort = 2 - активный
                ORDER BY t_Sort ASC, t_EndDate ASC) F
        WHERE ROWNUM = 1;
     EXCEPTION
        WHEN NO_DATA_FOUND THEN vQty := 0;
        WHEN OTHERS THEN vQty := 0;
     END;

     RETURN vQty;
  end;

--Определяет текущего эмитента по выпуску на дату
  function FI_GetIssuerOnDate( pFIID  IN NUMBER,
                            pDate  IN DATE
                          )
           return NUMBER is
     vIssuer DFININSTR_DBT.T_ISSUER%Type;
  begin

     BEGIN
       SELECT F.T_ISSUER INTO vIssuer
         FROM (SELECT T_ISSUER
                 FROM DV_FI_ISSUER_HIST
                WHERE t_FIID = pFIID AND
                      (t_Sort = 2 or (pDate <= t_EndDate and pDate >= t_BegDate) ) --t_Sort = 2 - активный
                ORDER BY t_Sort ASC, t_EndDate ASC) F
        WHERE ROWNUM = 1;
     EXCEPTION
        WHEN NO_DATA_FOUND THEN vIssuer := 0;
        WHEN OTHERS THEN vIssuer := 0;
     END;

     RETURN vIssuer;
  end;

--Возвращает T_AutoKey кода, действующего на дату, без учета активен код или нет
  FUNCTION FI_GetObjCodeOnDate (pFIID         IN NUMBER,
                                pObjectType   IN NUMBER,
                                pCodeKind     IN NUMBER,
                                pDate         IN DATE)
     RETURN NUMBER
  IS
     vBankDate   DOBJCODE_DBT.T_BankDate%TYPE;
     vAutoKey    DOBJCODE_DBT.T_AutoKey%TYPE;
  BEGIN
     BEGIN
        SELECT objcode.T_BankDate
          INTO vBankDate
          FROM (  SELECT t_BankDate
                    FROM DOBJCODE_DBT
                   WHERE t_ObjectType = pObjectType
                     AND t_CodeKind = pCodeKind
                     AND t_ObjectID = pFIID
                     AND t_BankDate <= pDate
                ORDER BY t_BankDate DESC) objcode
         WHERE ROWNUM = 1;
     EXCEPTION
        WHEN NO_DATA_FOUND  THEN vBankDate := TO_DATE ('01.01.0001', 'dd.mm.yyyy');
        WHEN OTHERS  THEN vBankDate := TO_DATE ('01.01.0001', 'dd.mm.yyyy');
     END;

     IF vBankDate <> TO_DATE ('01.01.0001', 'dd.mm.yyyy') THEN
        BEGIN
           SELECT objcode.T_AutoKey
             INTO vAutoKey
             FROM (  SELECT t_AutoKey
                       FROM DOBJCODE_DBT
                      WHERE t_ObjectType = pObjectType
                        AND t_CodeKind = pCodeKind
                        AND t_ObjectID = pFIID
                        AND t_BankDate = vBankDate
                        AND t_BankCloseDate = TO_DATE ('01.01.0001', 'dd.mm.yyyy')) objcode
            WHERE ROWNUM = 1;
        EXCEPTION
           WHEN NO_DATA_FOUND  THEN vAutoKey := NULL;
           WHEN OTHERS  THEN vAutoKey := NULL;
        END;

        IF vAutoKey IS NULL THEN
           BEGIN
              SELECT objcode.t_AutoKey
                INTO vAutoKey
                FROM (  SELECT max(t_AutoKey) t_AutoKey
                          FROM DOBJCODE_DBT
                         WHERE t_ObjectType = pObjectType
                           AND t_CodeKind = pCodeKind
                           AND t_ObjectID = pFIID
                           AND t_BankDate = vBankDate) objcode;
           EXCEPTION
              WHEN NO_DATA_FOUND  THEN vAutoKey := NULL;
              WHEN OTHERS  THEN vAutoKey := NULL;
           END;
        END IF;
     END IF;

     RETURN vAutoKey;
  END;

-----------------------------------------------------------------------------------------------------
  function  PrepareRate(
                    Rate       IN NUMBER,           -- Курс
                    Scale      IN NUMBER,           -- Масштаб
                    Point      IN NUMBER,           -- Округление
                    OutRate    IN BOOLEAN,             -- Признак обратной котировки
                    IsRelative IN BOOLEAN,             -- Признак относительной котировки
                    FaceValue  IN NUMBER,           -- Номинал (относительно чего задается курс, если он относительный)
                    FV_Scale   IN NUMBER,
                    FV_Point   IN NUMBER,
                    IsInverse  IN BOOLEAN,
                    vnumerator   IN OUT NUMBER,    -- Числитель !!!
                    vdenominator IN OUT NUMBER     -- Знаменатель !!!
    ) return NUMBER is
    vswap          NUMBER;
    vFVden         NUMBER;
    vpoint         NUMBER;
  begin

    vnumerator := Rate; -- Числитель
    vdenominator := GREATEST( 1, Scale ); -- Знаменатель

    vpoint := GREATEST( 0, Point );

    if IsRelative = TRUE then
        vpoint := vpoint + 2;  --???
        vnumerator := vnumerator * FaceValue;
        vFVden := GREATEST( 1, FV_Scale );
        vdenominator := vdenominator * vFVden;
        vpoint := vpoint + FV_Point;
        if IsInverse = TRUE then
            vswap := vdenominator;
            vdenominator := vnumerator;
            vnumerator := vswap;
        end if;
    end if;

    if  vpoint >= 4 then
        vnumerator := vnumerator / (10000);
        vpoint := vpoint - 4;

        while  vpoint > 0 loop
            vdenominator := vdenominator * 10;
            vpoint := vpoint - 1;
        end loop;

    else
        while  vpoint > 0 loop
            vnumerator := vnumerator / 10;
            vpoint := vpoint - 1;
        end loop;
    end if;

    if OutRate = TRUE then
        vswap := vdenominator;
        vdenominator := vnumerator;
        vnumerator := vswap;
    end if;

    return 1;
  end;

---------------------------------------------------------------------------------------------------
  function  FI_IsAvoirissPartly(
                    FIID                     IN NUMBER,
                    sum_all_rate_partly      IN OUT NUMBER,
                    pDate                    IN DATE,
                    IsClose                  IN NUMBER DEFAULT 0 /* 1 - T_ISCLOSE, 2 - T_SPISCLOSE, 3 - T_TSISCLOSE. */
                    ) return NUMBER is
    exist_partly  NUMBER;
    rate_partly   NUMBER;

   cursor c_fiwarnts  ( pFIID NUMBER ) is
          SELECT  t.*
          FROM dfiwarnts_dbt t
          WHERE  t.T_IsPartial = 'X'
            AND  t.T_FIID      = FIID
            AND (CASE WHEN (IsClose = 1)
                        THEN t.T_IsClosed
                      WHEN (IsClose = 2)
                        THEN t.T_SPIsClosed
                      WHEN (IsClose = 3)
                        THEN t.T_TSIsClosed
                      ELSE 'X'
                 END
                ) = 'X'
          ORDER BY t.T_IsPartial, t.T_FIID, t.t_DrawingDate  ;
  begin

   exist_partly   := 0;
   rate_partly    := 0.0;

   <<Out>>for fiwarnts_rec in c_fiwarnts( FIID ) loop
      EXIT Out WHEN (fiwarnts_rec.T_DrawingDate > pDate); -- AND pDate != ZeroDate;
      rate_partly := fiwarnts_rec.t_IncomeRate/GREATEST( 1., fiwarnts_rec.T_IncomeScale);

      sum_all_rate_partly := sum_all_rate_partly + rate_partly;

      exist_partly := 1;
   end loop;

   return exist_partly;

  end;

  FUNCTION FI_GetPartialPersent( FIID IN NUMBER, CalcDate IN DATE, IsClosed IN NUMBER ) RETURN NUMBER DETERMINISTIC
  IS
     AllPercent NUMBER := 0;
  BEGIN

     BEGIN
        SELECT NVL(SUM( T_INCOMERATE/GREATEST( 1., T_INCOMESCALE) ), 0)
          INTO AllPercent
          FROM dfiwarnts_dbt
        WHERE     T_FIID      = FIID
              AND T_IsPartial = 'X'
              AND (CASE WHEN (IsClosed = 1)
                          THEN T_IsClosed
                        WHEN (IsClosed = 2)
                          THEN T_SPIsClosed
                        WHEN (IsClosed = 3)
                          THEN T_TSIsClosed
                        ELSE 'X'
                   END
                  ) = 'X'
              AND T_DRAWINGDATE <= CalcDate;
     EXCEPTION
        WHEN NO_DATA_FOUND THEN AllPercent := 0;
     END;

     RETURN AllPercent;
  END; -- FI_GetPartialPersent

  FUNCTION FI_GetPartialPersentByName( pFIID              IN NUMBER,
                                       pNumber            IN VARCHAR2
                                     ) return NUMBER DETERMINISTIC
  IS
     PartPercent NUMBER := 0;
  BEGIN

     BEGIN
        SELECT T_INCOMERATE/GREATEST( 1., T_INCOMESCALE)
          INTO PartPercent
          FROM dfiwarnts_dbt
        WHERE     T_FIID      = pFIID
              AND T_IsPartial = 'X'
              AND t_Number    = pNumber;
     EXCEPTION
        WHEN NO_DATA_FOUND THEN PartPercent := 0;
     END;

     RETURN PartPercent;
  END; -- FI_GetPartialPersentByName

  -- Вернуть дату погашения ЧП
  FUNCTION FI_GetPartialDrawingDate( pFIID              IN NUMBER,
                                     pNumber            IN VARCHAR2
                                   ) return DATE DETERMINISTIC
  IS
     DrawingDate DATE;
  BEGIN

     BEGIN
        SELECT T_DrawingDate
          INTO DrawingDate
          FROM dfiwarnts_dbt
        WHERE     T_FIID      = pFIID
              AND T_IsPartial = 'X'
              AND t_Number    = pNumber;
     EXCEPTION
        WHEN NO_DATA_FOUND THEN DrawingDate := TO_DATE( '01.01.0001', 'dd.mm.yyyy' );
     END;

     RETURN DrawingDate;
  END; -- FI_GetPartialDrawingDate

  -- Вернуть дату погашения купона
  FUNCTION FI_GetCouponDrawingDate( pFIID              IN NUMBER,
                                    pNumber            IN VARCHAR2
                                  ) return DATE DETERMINISTIC
  IS
     DrawingDate DATE;
  BEGIN

     BEGIN
        SELECT T_DrawingDate
          INTO DrawingDate
          FROM dfiwarnts_dbt
        WHERE     T_FIID      = pFIID
              AND T_IsPartial = chr(0)
              AND t_Number    = pNumber;
     EXCEPTION
        WHEN NO_DATA_FOUND THEN DrawingDate := TO_DATE( '01.01.0001', 'dd.mm.yyyy' );
     END;

     RETURN DrawingDate;
  END; -- FI_GetCouponDrawingDate

  -- Получить дату погашения последнего известного купона
  FUNCTION FI_GetDateLastKnownCoupon(pFIID IN NUMBER) RETURN DATE
  IS
     v_DrawingDate DATE := ZERO_DATE;
  BEGIN
     SELECT NVL(MAX(t_DrawingDate), ZERO_DATE) INTO v_DrawingDate
       FROM DFIWARNTS_DBT
      WHERE t_FIID = pFIID
        AND t_IsPartial = CHR(0)
        AND (t_IncomeRate > 0 or t_IncomeVolume > 0);

     RETURN v_DrawingDate;
  END FI_GetDateLastKnownCoupon;

  -- Получить ближайшую дату оферты
  FUNCTION FI_GetOfferDate( FIID IN NUMBER, BegDate IN DATE ) RETURN DATE
  IS
     v_OFFERDATE DATE;
  BEGIN
     SELECT NVL(MIN(t_DateRedemption),ZERO_DATE) INTO v_OFFERDATE
       FROM doffers_dbt
      WHERE t_FIID = FIID
        AND t_DateRedemption >= BegDate;

     RETURN v_OFFERDATE;
     EXCEPTION
        WHEN OTHERS THEN
          RETURN ZERO_DATE;
  END FI_GetOfferDate;

  -- Вернуть дату погашения выпуска
  FUNCTION FI_GetNominalDrawingDate(pFIID IN NUMBER, pTermless IN CHAR, pBegDate IN DATE)
    RETURN DATE
    --DETERMINISTIC
  IS
    DrawingDate DATE;
  BEGIN
    BEGIN
      SELECT
        DECODE(fi.t_AvoirKind
              ,AVOIRKIND_DEPOSITORY_RECEIPT, (SELECT fi2.t_DrawingDate
                                                FROM dfininstr_dbt fi2
                                               WHERE fi2.t_FIID = fi.t_FIID
                                               START WITH fi2.t_FIID = fi.t_FIID
                                              CONNECT BY fi2.t_ParentFI <> fi2.t_FIID
                                                 AND fi2.t_FIID = PRIOR fi2.t_ParentFI
                                                 AND FI_AvrKindsEQ(FIKIND_AVOIRISS, AVOIRKIND_DEPOSITORY_RECEIPT, PRIOR fi2.t_AvoirKind) = 1)
              ,CASE WHEN fi.t_DrawingDate = ZERO_DATE and pTermless = 'X'
                    THEN CASE WHEN pBegDate != ZERO_DATE AND FI_GetOfferDate(pFIID, pBegDate) != ZERO_DATE
                              THEN FI_GetOfferDate(pFIID, pBegDate)
                              ELSE FI_GetDateLastKnownCoupon(fi.t_FIID)
                         END
                    ELSE fi.t_DrawingDate
               END)
          AS t_DrawingDate
      INTO
        DrawingDate
      FROM
        dfininstr_dbt fi
      WHERE
        fi.t_FIID = pFIID;
    EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
        DrawingDate := TO_DATE('01.01.0001', 'DD.MM.YYYY');
    END;

    RETURN DrawingDate;
  END; -- FI_GetCouponDrawingDate

-----------------------------------------------------------------------------------------------------
  function  FI_GetIncomeType( FIID IN NUMBER    --FIID ценной бумаги
                            ) return NUMBER
  is  --Возвращает тип дохода из анкеты ЦБ
     IncomeType NUMBER;
  begin

    if( FI_IsCouponAvoiriss(FIID) = 1 ) then
        return FI_INCOME_TYPE_COUPON;
    else
       begin
          SELECT AVR.T_INCOMETYPE
            INTO IncomeType
            FROM DFININSTR_DBT FI, DAVOIRISS_DBT AVR
           WHERE     FI.T_FIID = FIID
                 AND AVR.T_FIID = FI.T_FIID;
       exception
          WHEN NO_DATA_FOUND THEN
               RETURN FI_INCOME_TYPE_UNKNOWN;
          WHEN OTHERS THEN
               DBMS_OUTPUT.PUT_LINE('Ошибка' || SQLERRM);
               RETURN NULL;
       end;

       return IncomeType;
    end if;

    return FI_INCOME_TYPE_UNKNOWN;
  end;

  FUNCTION FI_HasCoupon( FIID IN NUMBER ) RETURN BOOLEAN DETERMINISTIC
  IS
     NumCoupon NUMBER := 0;
  BEGIN
     SELECT count(1) INTO NumCoupon
     FROM   dfiwarnts_dbt
     WHERE      T_FIID = FIID
            AND T_ISPARTIAL = chr(0);

     return (NumCoupon > 0);
  END; -- FI_HasCoupon

  FUNCTION FI_HasCouponSQL( FIID IN NUMBER ) RETURN NUMBER DETERMINISTIC
  IS
  BEGIN
     IF( FI_HasCoupon( FIID ) ) THEN
        RETURN 1;
     END IF;

     RETURN 0;
  END; -- FI_HasCouponSQL

  FUNCTION FI_HasPartialDischarge( FIID IN NUMBER ) RETURN BOOLEAN DETERMINISTIC
  IS
     NumCoupon NUMBER := 0;
  BEGIN
     SELECT count(1) INTO NumCoupon
     FROM   dfiwarnts_dbt
     WHERE      T_FIID = FIID
            AND T_ISPARTIAL = chr(88);

     return (NumCoupon > 0);
  END; -- FI_HasPartialDischarge

  FUNCTION FI_HasPartialDischargeSQL( FIID IN NUMBER ) RETURN NUMBER DETERMINISTIC
  IS
  BEGIN
     IF( FI_HasPartialDischarge( FIID ) ) THEN
        RETURN 1;
     END IF;

     RETURN 0;
  END; -- FI_HasPartialDischargeSQL

  FUNCTION FI_IsResponsible( FIID IN NUMBER, OperDate IN DATE ) RETURN NUMBER DETERMINISTIC
  IS
     QualityCategory dobjattr_dbt.t_NumInList % TYPE;
     RegIncomeActive BOOLEAN;
  BEGIN
     BEGIN
         SELECT Attr.t_NumInList INTO QualityCategory
           FROM dobjatcor_dbt AtCor, dobjattr_dbt Attr
          WHERE     AtCor.t_ObjectType = 12 -- OBJTYPE_AVOIRISS
                AND AtCor.t_GroupID    = 13 -- Категория качества
                AND AtCor.t_Object     = LPAD( FIID, 10, '0' )
                AND AtCor.t_ValidFromDate  = ( SELECT MAX(t.T_ValidFromDate)
                                                 FROM DOBJATCOR_DBT t
                                                WHERE     t.T_ObjectType = AtCor.T_ObjectType
                                                      AND t.T_GroupID    = AtCor.T_GroupID
                                                      AND t.t_Object     = AtCor.t_Object
                                                      AND t.T_ValidFromDate <= OperDate
                                             )
                AND Attr.t_AttrID      = AtCor.t_AttrID
                AND Attr.t_ObjectType  = AtCor.t_ObjectType
                AND Attr.t_GroupID     = AtCor.t_GroupID;

     EXCEPTION
        WHEN NO_DATA_FOUND THEN QualityCategory := chr(0);
        WHEN OTHERS THEN
           return 0;
     END;

     IF( QualityCategory = '1' OR QualityCategory = '2' ) THEN
        return 1;
     ELSIF( QualityCategory = '4' OR QualityCategory = '5' ) THEN
        return 0;
     ELSIF( QualityCategory = '3' ) THEN
        RegIncomeActive := Rsb_Common.GetRegBoolValue( 'COMMON\ПЕРЕМЕННЫЕ\ДОХОДЫ 3Й КАТЕГОРИИ КАЧЕСТВА', 0 );
        IF( RegIncomeActive = true ) THEN
           return 1;
        ELSE
           return 0;
        END IF;
     END IF;

     RETURN 0;
  END; -- FI_IsResponsible


  --квалифицированная/неквалифицированная ц/б на дату
  /*
  Неквалифицированные ценные бумаги:
  -  ценные бумаги, эмитент которых не является резидентом, и для которых задано значение кате-гории "Квалификация в качестве ценной бумаги" равное "Не квалифицирована в качестве ценной бумаги"

  Квалифицированные ценные бумаги:
  -  все ценные бумаги, эмитент которых является резидентом,
  -  ценные бумаги, эмитент которых не является резидентом, и для которых НЕ задано значение категории "Квалификация:" равное "Не квалифицирована в качестве ценной бумаги"

  !!! т.е.  квалифицированными считаем все бумаги, кроме отмеченных как "неквалифицирован-ные";
  Другими словами, ц/б, для которых категория "Квалификация:" не задана, считаем квалифицированными.
  */

  FUNCTION FI_IsQualified( FIID IN NUMBER, OnDate IN DATE ) RETURN NUMBER DETERMINISTIC
  IS
     IsQualified NUMBER;
     NotResident dparty_dbt.t_NotResident % TYPE;
  BEGIN
    IsQualified := 1; --будем считать, что по-умолчанию квалифицированная

    BEGIN
         SELECT Attr.t_NumInList INTO IsQualified
           FROM dobjatcor_dbt AtCor, dobjattr_dbt Attr
          WHERE     AtCor.t_ObjectType = 12 -- OBJTYPE_AVOIRISS
                AND AtCor.t_GroupID    = 28 -- Квалификация в качестве ценной бумаги
                AND AtCor.t_Object     = LPAD( FIID, 10, '0' )
                AND AtCor.t_ValidFromDate  = ( SELECT MAX(t.T_ValidFromDate)
                                                 FROM DOBJATCOR_DBT t
                                                WHERE     t.T_ObjectType = AtCor.T_ObjectType
                                                      AND t.T_GroupID    = AtCor.T_GroupID
                                                      AND t.t_Object     = AtCor.t_Object
                                                      AND t.T_ValidFromDate <= OnDate
                                             )
                AND Attr.t_AttrID      = AtCor.t_AttrID
                AND Attr.t_ObjectType  = AtCor.t_ObjectType
                AND Attr.t_GroupID     = AtCor.t_GroupID;
    EXCEPTION
       WHEN NO_DATA_FOUND THEN
          IsQualified := 1; -- если значение не задано, то считаем квалифицированной
    END;

    --если получили, что ц/б неквалифицированная, то проверим на всякий случай эмитента
    --если эмитент - резидент, то всё равно будем считать квалифицированной
    IF IsQualified = 0 THEN
      BEGIN
        SELECT pt.t_NotResident INTO NotResident
          FROM dparty_dbt pt, dfininstr_dbt fin
         WHERE fin.t_FIID = FIID
           AND pt.t_PartyID = fin.t_Issuer;

        IF NotResident = CHR(0) THEN
          IsQualified := 1; -- у резидента всегда квалифицированные ц/б
        END IF;
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            IsQualified := IsQualified; -- если не нашли эмитента, то оставляем как есть
      END;
    END IF;

    RETURN IsQualified;
  END; --FI_IsQualified

  -- Проверяет, является ли ц/б обращающейся
  FUNCTION FI_CirculateInMarket( FIID IN NUMBER, OperDate IN DATE ) RETURN NUMBER DETERMINISTIC
  IS
     CirculateCategory dobjattr_dbt.t_NumInList % TYPE;
  BEGIN
     BEGIN
         SELECT Attr.t_NumInList INTO CirculateCategory
           FROM dobjatcor_dbt AtCor, dobjattr_dbt Attr
          WHERE     AtCor.t_ObjectType = 12 -- OBJTYPE_AVOIRISS
                AND AtCor.t_GroupID    = 17 -- Обращается на ОРЦБ
                AND AtCor.t_Object     = LPAD( FIID, 10, '0' )
                AND AtCor.t_ValidFromDate  = ( SELECT MAX(t.T_ValidFromDate)
                                                 FROM DOBJATCOR_DBT t
                                                WHERE     t.T_ObjectType = AtCor.T_ObjectType
                                                      AND t.T_GroupID    = AtCor.T_GroupID
                                                      AND t.t_Object     = AtCor.t_Object
                                                      AND t.T_ValidFromDate <= OperDate
                                             )
                AND Attr.t_AttrID      = AtCor.t_AttrID
                AND Attr.t_ObjectType  = AtCor.t_ObjectType
                AND Attr.t_GroupID     = AtCor.t_GroupID;

     EXCEPTION
        --Не задано - то же что "Обращается"
        WHEN NO_DATA_FOUND THEN CirculateCategory := '1';
        WHEN OTHERS THEN
           return 0;
     END;

     IF( CirculateCategory = '1' ) THEN
        return 1;
     ELSIF( CirculateCategory = '2' ) THEN
        return 0;
     END IF;

     RETURN 0;
  END; -- FI_IsCirculateInMarket

  --Получение значения категории с номером GroupID по ц/б (OBJTYPE_AVOIRISS) на дату
  PROCEDURE FI_FindObjAttrOnDate( FIID      IN NUMBER,
                                  OperDate  IN DATE,
                                  GroupID   IN NUMBER,
                                  NumInList OUT dobjattr_dbt.t_NumInList%TYPE
                                )
  IS
  BEGIN
     BEGIN
         SELECT Attr.t_NumInList INTO NumInList
           FROM dobjatcor_dbt AtCor, dobjattr_dbt Attr
          WHERE AtCor.t_ObjectType = 12 -- OBJTYPE_AVOIRISS
            AND AtCor.t_GroupID    = GroupID -- номер типа категории
            AND AtCor.t_Object     = LPAD(FIID, 10, '0')
            AND AtCor.t_ValidToDate >= OperDate
            AND AtCor.t_ValidFromDate = ( SELECT MAX(t.T_ValidFromDate)
                                            FROM DOBJATCOR_DBT t
                                           WHERE t.t_ObjectType     = AtCor.T_ObjectType
                                             AND t.t_GroupID        = AtCor.T_GroupID
                                             AND t.t_Object         = AtCor.t_Object
                                             AND t.t_ValidFromDate <= OperDate
                                             AND t.t_ValidToDate   >= OperDate
                                        )
            AND Attr.t_AttrID      = AtCor.t_AttrID
            AND Attr.t_ObjectType  = AtCor.t_ObjectType
            AND Attr.t_GroupID     = AtCor.t_GroupID;

     EXCEPTION
        WHEN NO_DATA_FOUND THEN NumInList := chr(0);
        WHEN OTHERS THEN  NumInList := chr(0);
     END;

  END; -- FI_FindObjAttrOnDate


  --Надежность определения ТСС
  --GroupID - можно передать как для БО ЦБ (27), так и для ДУ (29)
  FUNCTION FI_ExistNOSS( FIID IN NUMBER, OperDate IN DATE, GroupID IN NUMBER ) RETURN NUMBER DETERMINISTIC
  IS
     NumInList dobjattr_dbt.t_NumInList % TYPE;
     st NUMBER := 0;
  BEGIN

     FI_FindObjAttrOnDate( FIID,
                           OperDate,
                           GroupID,
                           NumInList
                         );

     IF( NumInList = '1' ) THEN
        return 1;
     ELSIF( NumInList = '0' ) THEN
        return 0;
     END IF;

     RETURN 0;
  END; -- FI_ExistNOSS

---------------------------------------------------------------------------------------------------
  function  FI_IsCouponAvoiriss( FIID IN NUMBER  --FIID ценной бумаги
                               ) return NUMBER is  --Возвращает 1 если бумага купонная иначе 0
  AvrKind    NUMBER;
  begin
    begin
       SELECT T_AVOIRKIND INTO AvrKind FROM DFININSTR_DBT WHERE T_FIID = FIID;
     exception
       WHEN NO_DATA_FOUND THEN
            RETURN 0;
       WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Ошибка' || SQLERRM);
            RETURN NULL;
    end;

    IF( FI_IsAvrKindBond( AvrKind ) AND FI_HasCoupon( FIID ) ) THEN
       return 1;
    END IF;
    return 0;
  end;

  function  FI_IsCouponFI( FIID IN NUMBER  --FIID ценной бумаги
                         ) return NUMBER is  --Возвращает 1 если бумага купонная иначе 0
    fin    DFININSTR_DBT%ROWTYPE;
  begin
    begin
       SELECT * INTO fin FROM DFININSTR_DBT WHERE T_FIID = FIID;
     exception
       WHEN NO_DATA_FOUND THEN
            RETURN 0;
       WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Ошибка' || SQLERRM);
            RETURN NULL;
    end;

    if( FI_AvrKindsGetRoot( 2, fin.t_AvoirKind ) = AVOIRKIND_DEPOSITORY_RECEIPT AND fin.t_ParentFI <> FIID )THEN
      return FI_IsCouponFI(fin.t_ParentFI);
    elsif FI_IsAvrKindBond( fin.t_AvoirKind ) THEN
      return 1;
    end if;

    return 0;
  end;

  -- Проверка, что на ЦБ установлен признак Право отказа от выплаты купона
  -- Возвращает 1 если для ЦБ установлен признак Право отказа от выплаты купона, иначе 0
  FUNCTION FI_IsAvoirissCouponRefuseRight( FIID IN NUMBER ) RETURN NUMBER DETERMINISTIC 
  IS
    CouponRefuseRight davoiriss_dbt.t_CouponRefuseRight%TYPE;
  BEGIN
    BEGIN
       SELECT av.t_CouponRefuseRight 
         INTO CouponRefuseRight 
         FROM davoiriss_dbt av
       WHERE av.t_FIID = FIID;
    EXCEPTION
       WHEN OTHERS THEN
            RETURN NULL;
    END;

    IF( CouponRefuseRight = 'X' ) THEN
       RETURN 1;
    END IF;
    
    RETURN 0;
  END;

-- Проверить установлен ли признак "Отказ от выплаты" на купоне
  FUNCTION FI_IsCouponPaymentRefuse( FIID IN NUMBER, -- FIID ценной бумаги
                                     CouponNumber IN dfiwarnts_dbt.t_Number%TYPE -- номер купона
                                    ) RETURN NUMBER -- Возвращает 1 если установлен признак отказа от выплаты купона, иначе 0
  IS
    PaymentRefuse dfiwarnts_dbt.t_PaymentRefuse%TYPE;
  BEGIN
    BEGIN
       SELECT fw.t_PaymentRefuse 
         INTO PaymentRefuse 
         FROM dfiwarnts_dbt fw
       WHERE fw.t_FIID = FIID
         AND fw.t_Number = CouponNumber
         AND EXISTS(SELECT 1 FROM davoiriss_dbt av WHERE av.t_FIID = fw.t_FIID AND av.t_CouponRefuseRight = 'X');
    EXCEPTION
       WHEN OTHERS THEN
            RETURN NULL;
    END;

    IF( PaymentRefuse = 'X' ) THEN
       RETURN 1;
    END IF;
    
    RETURN 0;
  END;


-----------------------------------------------------------------------------------------------------
  function rsNDaysf(DATE1 IN DATE, DATE2 IN DATE) return NUMBER is
    Y1 NUMBER;
    M1 NUMBER;
    D1 NUMBER;
    Y2 NUMBER;
    M2 NUMBER;
    D2 NUMBER;
  begin
    Y1:= TO_NUMBER(TO_CHAR(DATE1, 'YYYY'));
    M1:= TO_NUMBER(TO_CHAR(DATE1, 'MM'));
    D1:= TO_NUMBER(TO_CHAR(DATE1, 'DD'));
    Y2:= TO_NUMBER(TO_CHAR(DATE2, 'YYYY'));
    M2:= TO_NUMBER(TO_CHAR(DATE2, 'MM'));
    D2:= TO_NUMBER(TO_CHAR(DATE2, 'DD'));

    if (D1 = 31) then
      D1 := 30;
    end if;

    if ((D2 = 31) AND (D1 > 29)) then
      D2 := 30;
    end if;

    return (360 * (Y2 - Y1) + 30 * (M2 - M1) + (D2 - D1));

  exception
    when NO_DATA_FOUND then
      return NULL;
    when OTHERS then
      dbms_output.put_line('Ошибка' || SQLERRM);
      return NULL;
  end;

-----------------------------------------------------------------------------------------------------
  function rsNDaysp( d1 IN DATE, CorrectDate IN NUMBER DEFAULT 0 , IsEuroBondBase IN NUMBER DEFAULT 0 ) return NUMBER is
  Years    NUMBER;
  Months  NUMBER;
  Days    NUMBER;
  d2      DATE;
  LastDay NUMBER := 0;

  begin

    Years  := TO_NUMBER(TO_CHAR( d1, 'YYYY'));
    Months := TO_NUMBER(TO_CHAR( d1, 'MM'));
    Days   := TO_NUMBER(TO_CHAR( d1, 'DD'));

    -- Определим, последний ли это день месяца. Последний - если в след. день 1-е число.
    if (CorrectDate = 1) then
       d2 := d1 + 1;

       if (TO_NUMBER(TO_CHAR( d2, 'DD')) = 1) then
          LastDay := 1;
       end if;
    end if;

    if( Days = 31 ) then
       return (Years * 360 + (Months-1) * 30 + Days - 1);

     -- Если CorrectDate == 1 и CalcDate  - последний день месяца,
     -- то за дату расчета принимается последний день месяца в соответствии с базисом расчета.
     -- Например, если базис -30 дней в месяце, то 31.01и  28.02- это 30-й день месяца. См. 138703.
     -- Но для 30 и 31 это и так работало, так что только для 28 и 29.
     -- Для базиса 30E/360 (IsEuroBond = 1) для февраля дату не корректриуем
    elsif( (Days = 29) and (LastDay = 1) and (IsEuroBondBase = 0)) then
       return (Years * 360 + (Months-1) * 30 + Days + 1);

    elsif( (Days = 28) and (LastDay = 1) and (IsEuroBondBase = 0) ) then
       return (Years * 360 + (Months-1) * 30 + Days + 2);

    end if;

    return (Years * 360 + (Months-1) * 30 + Days);

  exception
    when NO_DATA_FOUND then
         return NULL;
    when OTHERS then
         dbms_output.put_line('Ошибка' || SQLERRM);
         return NULL;
  end;

-----------------------------------------------------------------------------------------------------
  -- Рассчитать сумму дохода
  FUNCTION IncomeValueSum( RelativeIncome IN CHAR,
                           FaceValue      IN NUMBER,
                           IncomeRate     IN NUMBER,
                           IncomeScale    IN NUMBER,
                           IncomeVolume   IN NUMBER,
                           DaysInYear IN NUMBER,
                           T_all      IN NUMBER,
                           NKDBase_Kind IN NUMBER,
                           Amount     IN NUMBER DEFAULT 1,
                           IsFirstPeriodFirstCoupon IN NUMBER DEFAULT 0,
                           M IN NUMBER DEFAULT 0 -- количество выплат купонного дохода в году (Actual/Actual)
                         ) return NUMBER
  IS
  BEGIN
    if( RelativeIncome = 'X' ) then  --% от номинала
       if( NKDBase_Kind = 11 and IsFirstPeriodFirstCoupon = 0 ) then
          return ( FaceValue * Amount*(IncomeRate/(M*100.)) / GREATEST( 1., IncomeScale));
       else
          return ( FaceValue * Amount*(IncomeRate/100.) * T_all/GREATEST( 1., IncomeScale)/DaysInYear);
       end if;
    else
       return ( IncomeVolume*Amount/GREATEST( 1., IncomeScale) );
    end if;
  END;

  -- Получить сумму дохода по бумаге (не НКД)
  FUNCTION AvoirissSum( FIID       IN NUMBER,
                        FaceValue  IN NUMBER,
                        DaysInYear IN NUMBER,
                        T_all      IN NUMBER,
                        NKDBase_Kind IN NUMBER,
                        Amount     IN NUMBER DEFAULT 1 ) return NUMBER
  IS

    RelativeIncome CHAR := chr(0);
    IncomeType     DAVOIRISS_DBT.T_IncomeType%TYPE;
    IncomeRate     DAVOIRISS_DBT.T_IncomeRate%TYPE;
    IncomeVolume   DAVOIRISS_DBT.T_IncomeVolume%TYPE;
  BEGIN

    select T_IncomeType, T_IncomeRate, T_IncomeVolume
      into IncomeType, IncomeRate, IncomeVolume
      from DAVOIRISS_DBT
     where T_FIID      = FIID;

     if( IncomeType = 1 /*FI_INCOME_TYPE_PERCENT*/ ) then
        RelativeIncome := 'X';
     end if;

     return IncomeValueSum( RelativeIncome, FaceValue, IncomeRate, 1, IncomeVolume, DaysInYear, T_all, NKDBase_Kind, Amount );

  exception
    when NO_DATA_FOUND then
         return 0;
    when OTHERS then
         dbms_output.put_line('Ошибка' || SQLERRM);
         return NULL;
  END;

  -- Получить последнюю известную ставку купона ФИ.
  FUNCTION FI_GetLatestKnownRate( FIID IN NUMBER ) RETURN NUMBER
  IS
    RelativeIncome CHAR(1);
    Rate NUMBER := 0.0;
    Income NUMBER;
  BEGIN
    BEGIN
      select t_RelativeIncome,
             case when t_RelativeIncome = CHR(0) then RSI_RSB_FIInstr.CalcNKD_Ex(t_FIID, t_DrawingDate, 1, 1) else t_IncomeVolume end t_CalcIncomeVolume,
             case when t_RelativeIncome = CHR(0) then RSI_RSB_FIInstr.FI_ReturnIncomeRate() else t_IncomeRate end t_CalcIncomeRate
        INTO RelativeIncome, Income, Rate
        from DFIWARNTS_DBT
       where t_FIID = FIID
         and t_IsPartial = CHR(0)
         and (case when t_RelativeIncome = CHR(0) then (case when t_IncomeVolume <> 0.0 then 1 else 0 end) else (case when t_IncomeRate <> 0.0 then 1 else 0 end) end) = 1
         and rownum = 1
       order by t_DrawingDate desc;

      if( RelativeIncome = chr(0) ) then
        Rate := RSI_RSB_FIInstr.FI_ReturnIncomeRate();
      end if;
    EXCEPTION
       WHEN OTHERS THEN Rate := 0.0;
    END;

    RETURN Rate;
  END;

  --Получить сумму дохода по купону (НКД)
  FUNCTION CouponsSum( FIID       IN NUMBER,
                       CoupNumber IN DFIWARNTS_DBT.T_NUMBER%TYPE,
                       FaceValue  IN NUMBER,
                       CouponSum  IN OUT NUMBER,
                       DaysInYear IN NUMBER,
                       T_all      IN NUMBER,
                       NKDBase_Kind IN NUMBER,
                       Amount     IN NUMBER DEFAULT 1,
                       UseLatestKnownRate IN NUMBER DEFAULT 0, -- Признак использовать последнюю известную ставку для неопределенных купонов
                       IsFirstPeriodFirstCoupon IN NUMBER DEFAULT 0,
                       CntCoupPaymsInYear IN NUMBER DEFAULT 0,
                       CoupHistID IN NUMBER DEFAULT 0 -- ID записи истории изменения ставки купона
                     ) return NUMBER
  IS

    RelativeIncome DFIWARNTS_DBT.T_RelativeIncome%TYPE;
    IncomeRate     DFIWARNTS_DBT.T_IncomeRate%TYPE;
    IncomeScale    DFIWARNTS_DBT.T_IncomeScale%TYPE;
    IncomeVolume   DFIWARNTS_DBT.T_IncomeVolume%TYPE;
  BEGIN

    IF CoupHistID = 0 THEN
      select T_RelativeIncome, T_IncomeRate, T_IncomeScale, T_IncomeVolume
       into RelativeIncome, IncomeRate, IncomeScale, IncomeVolume
        from dfiwarnts_dbt
       where     T_IsPartial = chr(0)
             and T_FIID      = FIID
             and T_Number    = CoupNumber;
    ELSE -- возьмем данные о ставке(доходе) из истории
      SELECT w.T_RelativeIncome, h.T_IncomeRate, w.T_IncomeScale, h.T_IncomeVolume
       INTO RelativeIncome, IncomeRate, IncomeScale, IncomeVolume
        FROM dfiwarnts_dbt w, DFLRHIST_DBT h
       WHERE     w.T_IsPartial = chr(0)
             and w.T_FIID      = FIID
             and w.T_Number    = CoupNumber
             and h.t_ID        = CoupHistID
             and h.T_FIWARNTID = w.t_ID;
    END IF;

    -- для неопределенных купонов рассчитаем по последней известной ставке
    if (IncomeRate = 0.0 and IncomeVolume = 0.0 and UseLatestKnownRate = 1) then
      IncomeRate := FI_GetLatestKnownRate(FIID);
      RelativeIncome := CHR(88);
    end if;

    -- Для купонов с установленным признаком "Отказ от выплаты" дохода нет
    if ((FI_IsCouponPaymentRefuse( FIID, CoupNumber ) = 1) AND (Rsb_Common.GetRegBoolValue('COMMON\WORK_MODE\USE_COUPON_REFUSE'))) then
      CouponSum := 0;
    else
      CouponSum := IncomeValueSum( RelativeIncome, FaceValue, IncomeRate, IncomeScale, IncomeVolume, DaysInYear, T_all, NKDBase_Kind, Amount, IsFirstPeriodFirstCoupon, CntCoupPaymsInYear );
    end if;

    return CouponSum;

  exception
    when NO_DATA_FOUND then
         return 0;
    when OTHERS then
         dbms_output.put_line('Ошибка' || SQLERRM);
         return NULL;
  END;

  --Получить параметры для расчета дохода
  FUNCTION GetIncomeCalculateData( IsCalcNKD IN BOOLEAN, FIID IN NUMBER, CalcDate IN DATE, LastDate IN NUMBER, CouponNumber OUT VARCHAR2, BeginDate OUT DATE, EndDate OUT DATE, RelativeIncome OUT VARCHAR2, IsFirstCoupon OUT NUMBER, IsFloatingRate OUT NUMBER ) return BOOLEAN
  IS
     DrawingDate  DATE;
     IncomeType   NUMBER;
     FDateFiw1    DATE;
     DDateFiw1    DATE;
     NumFiw1      DFIWARNTS_DBT.T_Number%TYPE;
     FDateFiw2    DATE;
     DDateFiw2    DATE;
     NumFiw2      DFIWARNTS_DBT.T_Number%TYPE;
     TmpCount     NUMBER := 0;
     AvoirKind    NUMBER := 0;
  BEGIN
     IsFirstCoupon := 0;

    -- дата расчета не задана - доход нулевой
     if( (CalcDate is null) OR (CalcDate = TO_DATE( '01.01.0001', 'dd.mm.yyyy' )) ) then
        return false;
     end if;

     --получаем дату погашения ценной бумаги
     begin
       SELECT RSI_RSB_FIInstr.FI_GetNominalDrawingDate(fi.T_FIID, av.t_Termless), fi.T_AvoirKind, DECODE(av.T_FloatingRate, 'X', 1)
         INTO DrawingDate, AvoirKind, IsFloatingRate
         FROM dfininstr_dbt fi, davoiriss_dbt av
        WHERE fi.T_FIID = FIID
          AND av.T_FIID = fi.T_FIID;
     exception
       when NO_DATA_FOUND then
            return false;
       when OTHERS then
            dbms_output.put_line('Ошибка' || SQLERRM);
            return false;
     end;

     if( FI_IsAvrKindBond(AvoirKind) != true ) then
         return false;
     end if;

     --IL 03.07.03 если ищем доход после даты погашения выпуска то он == 0
     if( (DrawingDate != TO_DATE( '01.01.0001', 'dd.mm.yyyy' )) AND (DrawingDate < CalcDate) ) then
        return false;
     end if;

     BEGIN
        if( IsCalcNKD = true ) then
           --Ищем текущий купон на дату CalcDate
           SELECT T_DrawingDate, T_FirstDate, T_Number, t_RelativeIncome
             INTO EndDate, BeginDate, CouponNumber, RelativeIncome
             FROM dfiwarnts_dbt
            WHERE     T_IsPartial = chr(0)
                  AND T_FIID      = FIID
                  AND CalcDate between T_FirstDate AND T_DrawingDate;

           SELECT count(1)
             INTO TmpCount
             FROM DFIWARNTS_DBT
            WHERE t_IsPartial   = chr(0)
              AND t_FIID        = FIID
              AND t_DrawingDate < EndDate;

           IF( TmpCount = 0 ) THEN
              IsFirstCoupon := 1;
           END IF;
        else
           SELECT T_InCirculationDate + 1, t_IncomeType
             INTO BeginDate, IncomeType
             FROM DAVOIRISS_DBT
            WHERE T_FIID = FIID;

            EndDate := DrawingDate;
            if( IncomeType = 1 /*FI_INCOME_TYPE_PERCENT*/ ) then
               RelativeIncome := 'X';
            else
               RelativeIncome := chr(0);
            end if;
            CouponNumber := NULL;
        end if;
     EXCEPTION
       when NO_DATA_FOUND then
            return false;  -- нет дохода
       when OTHERS THEN

            SELECT t_FIRSTDATE, t_DRAWINGDATE, t_NUMBER
              INTO FDateFiw1, DDateFiw1, NumFiw1
              FROM DFIWARNTS_DBT
             WHERE T_ISPARTIAL != 'X'
               AND t_FIID       = FIID
               AND t_DRAWINGDATE = (SELECT MIN(t_DRAWINGDATE)
                                     FROM DFIWARNTS_DBT
                                    WHERE T_ISPARTIAL != 'X'
                                      AND t_FIID       = FIID
                                       AND CalcDate between T_FirstDate AND T_DrawingDate)
               AND rownum = 1;

            SELECT t_FIRSTDATE, t_DRAWINGDATE, t_NUMBER
              INTO FDateFiw2, DDateFiw2, NumFiw2
              FROM DFIWARNTS_DBT
             WHERE T_ISPARTIAL != 'X'
               AND t_FIID       = FIID
               AND t_DRAWINGDATE = (SELECT MAX(t_DRAWINGDATE)
                                     FROM DFIWARNTS_DBT
                                    WHERE T_ISPARTIAL != 'X'
                                      AND t_FIID       = FIID
                                       AND CalcDate between T_FirstDate AND T_DrawingDate)
               AND rownum = 1;

              SetError( FI_ERROR_20201, '|по ц/б с ID = '||FIID||'|купонный период с '||TO_CHAR(FDateFiw1,'DD.MM.YYYY')||' по '||TO_CHAR(DDateFiw1,'DD.MM.YYYY')||' |'||NumFiw1||' купона пересекается с |купонным периодом с '||TO_CHAR(FDateFiw2,'DD.MM.YYYY')||' по '||TO_CHAR(DDateFiw2,'DD.MM.YYYY')||' |'||NumFiw2||' купона' );
              return false;  -- нет дохода
     END;

     if( BeginDate <= TO_DATE( '01.01.0001 ', 'dd.mm.yyyy' ) ) then
        return false; -- нет дохода
     end if;

     --IL 24.08.2000 Если дата выполнения сделки совпадает с датой погашения купона.бумаги, то доход = 0
     if( ( LastDate = 0 ) AND (EndDate = CalcDate) ) then
        return false;
     end if;

     return true;
  END;

  --число дней в году
  FUNCTION GetDaysInYearByDate( CalcDate IN DATE ) return NUMBER
  IS
  BEGIN
    return (TO_DATE('01.01.'||TO_CHAR(TO_NUMBER(TO_CHAR( CalcDate, 'YYYY'))+1),'DD.MM.YYYY')-TO_DATE('01.01.'||TO_CHAR(TO_NUMBER(TO_CHAR( CalcDate, 'YYYY'))),'DD.MM.YYYY'));
  END;

  --число дней в году
  FUNCTION FI_GetDaysInYear( CurYear IN NUMBER ) return NUMBER
  IS
  BEGIN
    return TO_DATE('01.01.'||TO_CHAR(CurYear+1),'DD.MM.YYYY')-TO_DATE('01.01.'||TO_CHAR(CurYear),'DD.MM.YYYY');
  END;

  --число дней в году по базису
  FUNCTION FI_GetDaysInYearByBase( pFIID IN NUMBER, pCalcDate IN DATE ) return NUMBER
  IS
    NKDBase_Kind      DAVOIRISS_DBT.T_NKDBase_Kind%TYPE;
    IndexNom          DAVOIRISS_DBT.T_IndexNom%TYPE;
    DaysInYear        NUMBER := -1;
  BEGIN
    --получаем базис расчета дохода
    begin
      SELECT avr.T_NKDBase_Kind, avr.t_IndexNom
        INTO NKDBase_Kind, IndexNom
        FROM davoiriss_dbt avr, dfininstr_dbt fin
       WHERE avr.T_FIID = pFIID
         AND fin.t_FIID = avr.t_FIID;
    exception
      when NO_DATA_FOUND then
           return -1;
      when OTHERS then
           dbms_output.put_line('Ошибка' || SQLERRM);
           return -1;
    end;

    if( NKDBase_Kind = 0) then -- 365 в году, по календарю в месяце
       DaysInYear := 365;
    elsif( NKDBase_Kind = 1) then -- 360 в году, 30 в месяце
       DaysInYear := 360;
    elsif( NKDBase_Kind = 2) then -- 360 в году, по календарю в месяце
       DaysInYear := 360;
    elsif( NKDBase_Kind = 3) then -- 365 в году, 30 в месяце
       DaysInYear := 365;
    elsif( NKDBase_Kind IN (4, 11)) then -- дней в году по календарю, по календарю в месяце
       if IndexNom = 'X' then
         DaysInYear := 365; -- Так в официальной формуле
       else
         DaysInYear := GetDaysInYearByDate( pCalcDate );
       end if;
    elsif( NKDBase_Kind = 5) then -- дней в году по календарю, в месяце 30
       DaysInYear := GetDaysInYearByDate( pCalcDate );
    elsif( NKDBase_Kind = 6) then -- дней в году по продолжительности купонных периодов, в месяце по календарю
       -- !!! пока так !!!
       DaysInYear := GetDaysInYearByDate( pCalcDate );
    elsif( NKDBase_Kind = 7) then -- Act/365L - в месяце по календарю, в году по календарю по окончанию куп. периода
       -- !!! пока так !!!
       DaysInYear := GetDaysInYearByDate( pCalcDate );
    elsif( NKDBase_Kind = 8) then -- 364 в году, по календарю в месяце
       DaysInYear := 364;
    elsif( NKDBase_Kind = 9) then --30E/360 в году 360 дней, в месяце 30 дней (Eurobond)
       DaysInYear := 360;
    elsif (NKDBase_Kind = 10) then -- 30/360 ISDA
      DaysInYear := 360;
    end if;

    return DaysInYear;
  END FI_GetDaysInYearByBase;

  -- количество выплат купонного дохода в году
  FUNCTION FI_CntCoupPayms( FIID IN NUMBER ) RETURN NUMBER
  IS
    cnt NUMBER;
  BEGIN
    -- максимальное количество в любом году, считаем как среднее число выплат
    begin
      select max(Count(1)) into cnt
        from dfiwarnts_dbt fw
       where fw.t_FIID = FIID
       group by extract(year from fw.T_DrawingDate);
    exception
      when NO_DATA_FOUND then
           cnt := 0;
      when OTHERS then
           dbms_output.put_line('Ошибка' || SQLERRM);
           cnt := 0;
    end;
    return cnt;
  END;

  -- Расчет дохода по бумаге за период
  FUNCTION  CalcIncomeValueForPeriod( FIID                     IN NUMBER,
                                      CalcDate                 IN DATE,
                                      Amount                   IN NUMBER,
                                      CoupNumber               IN VARCHAR2,
                                      FirstDate                IN DATE,
                                      EndDate                  IN DATE,
                                      RelativeIncome           IN CHAR,
                                      NKDRound_Kind            IN DAVOIRISS_DBT.T_NKDRound_Kind%TYPE,
                                      CorrectDate              IN NUMBER DEFAULT 0,
                                      CorrectNominalDate       IN NUMBER DEFAULT 0,
                                      IsFirstPeriodFirstCoupon IN NUMBER DEFAULT 0,
                                      NoRound                  IN NUMBER DEFAULT 0,
                                      InFirstCoupDate          IN DATE DEFAULT TO_DATE('01010001', 'DDMMYYYY'),
                                      UseLatestKnownRate       IN NUMBER DEFAULT 0, -- Признак использовать последнюю известную ставку для неопределенных купонов
                                      CoupHistID               IN NUMBER DEFAULT 0
                                    ) return NUMBER
  IS

   cursor c_fiwarntsper is SELECT t_DRAWINGDATE
                             FROM DFIWARNTS_DBT
                            WHERE T_ISPARTIAL != 'X'
                              AND t_FIID       = FIID
                         ORDER BY T_DrawingDate;

   prevDate          DATE;
   NKDBase_Kind      DAVOIRISS_DBT.T_NKDBase_Kind%TYPE;
   DaysInYear        NUMBER;
   T_cur             NUMBER;
   T_all             NUMBER;
   Nominal           NUMBER;
   AccurateCouponSum NUMBER := 0;
   CurYear           NUMBER;
   FirstDateCurYear  DATE;
   FirstDatePrevYear DATE;
   NeedContinue      BOOLEAN := true;
   Period            NUMBER;
   tmpCouponSum      NUMBER;
   vtmpFiPoint       NUMBER;
   --
   DrawingDate       DATE;
   LastWarntDate     DATE;
   AmountWarnts      NUMBER;
   FirstWarntDate    DATE;
   AmountDays        NUMBER;
   DaysInLastYear    NUMBER;
   LastWarntPer      DATE;
   FirstWarntPer     DATE;
   FirstPer          DATE;
   FirstWarntLastPer DATE;
   AmountAvr         NUMBER;
   IncomeScale       NUMBER;
   AvoirKind         NUMBER;
   NominalDate       DATE;
   EndCouponPeriod   DATE;
   FirstCoupDate     DATE := TO_DATE('01010001', 'DDMMYYYY');
   IndexNom          DAVOIRISS_DBT.T_IndexNom%TYPE;
   M                 NUMBER := 0; -- количество выплат купонного дохода в году
   AddDay            BOOLEAN;
  begin

    if( Amount = 0 ) then
       return 0;
    end if;


    --получаем базис расчета дохода
    begin
      SELECT avr.T_NKDBase_Kind, fin.t_AvoirKind, avr.t_IndexNom
        INTO NKDBase_Kind, AvoirKind, IndexNom
        FROM davoiriss_dbt avr, dfininstr_dbt fin
       WHERE avr.T_FIID = FIID
         AND fin.t_FIID = avr.t_FIID;
    exception
      when NO_DATA_FOUND then
           return -1;
      when OTHERS then
           dbms_output.put_line('Ошибка' || SQLERRM);
           return -1;
    end;

    if ((IsFirstPeriodFirstCoupon = 0) and (NKDBase_Kind <> 10)) then
      prevDate := FirstDate - 1;
      if (InFirstCoupDate != TO_DATE('01010001', 'DDMMYYYY')) then
        FirstCoupDate := InFirstCoupDate - 1;
      end if;
    else
      prevDate := FirstDate;
      if (InFirstCoupDate != TO_DATE('01010001', 'DDMMYYYY')) then
        FirstCoupDate := InFirstCoupDate;
      end if;
    end if;

    if IndexNom = 'X' then
      NominalDate := CalcDate;
    else
      if( (CorrectNominalDate <> 0) and (IsFirstPeriodFirstCoupon <> 0) ) then
         NominalDate := prevDate + 1;
      else
         NominalDate := prevDate;
      end if;
    end if;

    if( FI_GetCurrentNominal( FIID, Nominal, vtmpFiPoint, NominalDate ) IS NULL ) then
       return -1;
    end if;

    if( NKDRound_Kind = 2 ) then
       AmountAvr := Amount;
    else
       AmountAvr := 1;
    end if;

    if( NKDBase_Kind = 0) then -- 365 в году, по календарю в месяце
       DaysInYear := 365;
       T_cur := CalcDate - prevDate;
       if (FirstCoupDate != TO_DATE('01010001', 'DDMMYYYY')) then
         T_all := EndDate - FirstCoupDate;
       else
         T_all := EndDate - prevDate;
       end if;
    elsif( NKDBase_Kind = 1) then -- 360 в году, 30 в месяце
       DaysInYear := 360;
       T_cur := rsNDaysp(CalcDate, CorrectDate) - rsNDaysp(prevDate, CorrectDate);
       if (FirstCoupDate != TO_DATE('01010001', 'DDMMYYYY')) then
         T_all := rsNDaysp(EndDate, CorrectDate) - rsNDaysp(FirstCoupDate, CorrectDate); --период начисления дохода
       else
         T_all := rsNDaysp(EndDate, CorrectDate) - rsNDaysp(prevDate, CorrectDate); --период начисления дохода
       end if;
    elsif( NKDBase_Kind = 2) then -- 360 в году, по календарю в месяце
       DaysInYear := 360;
       T_cur := CalcDate - prevDate;
       if (FirstCoupDate != TO_DATE('01010001', 'DDMMYYYY')) then
         T_all := EndDate - FirstCoupDate;
       else
         T_all := EndDate - prevDate;
       end if;
    elsif( NKDBase_Kind = 3) then -- 365 в году, 30 в месяце
       DaysInYear := 365;
       T_cur := rsNDaysp(CalcDate, CorrectDate) - rsNDaysp(prevDate, CorrectDate);
       if (FirstCoupDate != TO_DATE('01010001', 'DDMMYYYY')) then
         T_all := rsNDaysp(EndDate, CorrectDate) - rsNDaysp(FirstCoupDate, CorrectDate); --период начисления дохода
       else
         T_all := rsNDaysp(EndDate, CorrectDate) - rsNDaysp(prevDate, CorrectDate); --период начисления дохода
       end if;
    elsif( NKDBase_Kind IN (4, 11) ) then -- дней в году по календарю, по календарю в месяце/ Actual/Actual (ICMA)
       if IndexNom = 'X' then
         DaysInYear := 365; -- Так в официальной формуле
       else
         DaysInYear := GetDaysInYearByDate( CalcDate );
       end if;

       T_cur := CalcDate - prevDate;
       if (FirstCoupDate != TO_DATE('01010001', 'DDMMYYYY')) then
         T_all := EndDate - FirstCoupDate;
       else
         T_all := EndDate - prevDate;
       end if;
    elsif( NKDBase_Kind = 5) then -- дней в году по календарю, в месяце 30
       DaysInYear := GetDaysInYearByDate( CalcDate );
       T_cur := rsNDaysp(CalcDate, CorrectDate) - rsNDaysp(prevDate, CorrectDate);
      if (FirstCoupDate != TO_DATE('01010001', 'DDMMYYYY')) then
        T_all := rsNDaysp(EndDate, CorrectDate) - rsNDaysp(FirstCoupDate, CorrectDate); --период начисления дохода
      else
        T_all := rsNDaysp(EndDate, CorrectDate) - rsNDaysp(prevDate, CorrectDate); --период начисления дохода
      end if;
    elsif( NKDBase_Kind = 6) then -- дней в году по продолжительности купонных периодов, в месяце по календарю
       SELECT min(F.t_FIRSTDATE) INTO FirstPer --начало первого куп периода
         FROM DFIWARNTS_DBT F
        WHERE F.T_ISPARTIAL != 'X'
          AND F.t_FIID       = FIID;
     -- находим очередной год, на кот приходится дата расчета
       FirstWarntPer := FirstPer;

        begin
           for fiwarnts_rec in c_fiwarntsper loop
              LastWarntPer := fiwarnts_rec.T_DrawingDate;
              if( ((LastWarntPer - FirstWarntPer) >= 359) AND ((LastWarntPer - FirstWarntPer) <= 367) ) then
                 EXIT WHEN ( (CalcDate >= FirstWarntPer) AND (CalcDate <= LastWarntPer) );
                 FirstWarntPer := LastWarntPer + 1;
              end if;
           end loop;
        end;

     -- находим начало последнего очередного года
       FirstWarntLastPer := FirstPer;

        begin
           for fiwarnts_rec in c_fiwarntsper loop
              if( ((fiwarnts_rec.T_DrawingDate - FirstWarntPer) >= 359) AND
                  ((fiwarnts_rec.T_DrawingDate - FirstWarntPer) <= 367) ) then
                 FirstWarntLastPer := fiwarnts_rec.T_DrawingDate + 1 ;
              end if;
           end loop;
        end;

       -- Самый последний купон
       SELECT F.t_FIRSTDATE, F.t_DRAWINGDATE INTO FirstWarntDate, LastWarntDate
         FROM DFIWARNTS_DBT F
        WHERE F.T_ISPARTIAL != 'X'
          AND F.t_FIID       = FIID
          AND F.t_DRAWINGDATE = (SELECT MAX(F1.t_DRAWINGDATE)
                                   FROM DFIWARNTS_DBT F1
                                  WHERE F1.T_ISPARTIAL != 'X'
                                    AND F1.t_FIID       = FIID);
       --находим дату погашения обл
       select RSI_RSB_FIInstr.FI_GetNominalDrawingDate(F.t_FIID,
                                                       (select t_Termless from davoiriss_dbt where t_FIID = F.t_FIID))
         into DrawingDate
         from dfininstr_dbt F
        where F.T_FIID = FIID;

       --находим количество дней в последнем очередном году
       SELECT NVL( Sum( F.t_DRAWINGDATE - F.t_FIRSTDATE + 1 ), 0) INTO DaysInLastYear
         FROM DFIWARNTS_DBT F
        WHERE F.T_ISPARTIAL   != 'X'
          AND F.t_FIID         = FIID
          AND F.t_DRAWINGDATE >= FirstWarntLastPer
          AND F.t_DRAWINGDATE <= DrawingDate;  --дата погашения облигации

       T_cur := CalcDate - prevDate;
       if (FirstCoupDate != TO_DATE('01010001', 'DDMMYYYY')) then
         T_all := EndDate - FirstCoupDate;
       else
         T_all := EndDate - prevDate;
       end if;

    elsif( NKDBase_Kind = 7) then -- Act/365L - в месяце по календарю, в году по календарю по окончанию куп. периода

      SELECT F.t_DRAWINGDATE INTO EndCouponPeriod --дата окончания купонного периода
        FROM DFIWARNTS_DBT F
       WHERE F.T_ISPARTIAL != 'X'
         AND F.t_FIID = FIID
         AND F.T_NUMBER = CoupNumber;

      DaysInYear := GetDaysInYearByDate( EndCouponPeriod );
      T_cur := CalcDate - prevDate;
      if (FirstCoupDate != TO_DATE('01010001', 'DDMMYYYY')) then
        T_all := EndDate - FirstCoupDate;
      else
        T_all := EndDate - prevDate;
      end if;

    elsif( NKDBase_Kind = 8) then -- 364 в году, по календарю в месяце
       DaysInYear := 364;
       T_cur := CalcDate - prevDate;
      if (FirstCoupDate != TO_DATE('01010001', 'DDMMYYYY')) then
        T_all := EndDate - FirstCoupDate;
      else
        T_all := EndDate - prevDate;
      end if;


    elsif( NKDBase_Kind = 9) then --30E/360 в году 360 дней, в месяце 30 дней (Eurobond)
       DaysInYear := 360;
       T_cur := rsNDaysp(CalcDate, CorrectDate, EURO_BOND_BASE) - rsNDaysp(prevDate, CorrectDate, EURO_BOND_BASE);
       if (FirstCoupDate != TO_DATE('01010001', 'DDMMYYYY')) then
         T_all := rsNDaysp(EndDate, CorrectDate, EURO_BOND_BASE) - rsNDaysp(FirstCoupDate, CorrectDate, EURO_BOND_BASE);
       else
         T_all := rsNDaysp(EndDate, CorrectDate, EURO_BOND_BASE) - rsNDaysp(prevDate, CorrectDate, EURO_BOND_BASE);
       end if;

    elsif (NKDBase_Kind = 10) then -- 30/360 ISDA
      DaysInYear := 360;
      T_cur := rsNDaysf(prevDate, CalcDate);
      if (FirstCoupDate != TO_DATE('01010001', 'DDMMYYYY')) then
        T_all := rsNDaysf(FirstCoupDate, EndDate);
      else
        T_all := rsNDaysf(prevDate, EndDate);
      end if;
    end if;

    -- первый период первого купона
    if ((IsFirstPeriodFirstCoupon = 1) and (NKDBase_Kind <> 10)) then
       AddDay := true;
       -- если дата начала приходится на 31-й день (для базисов где 30 дней)
       if ( NKDBase_Kind IN (1,3,5,9) ) then
          if( TO_NUMBER(TO_CHAR( FirstCoupDate, 'DD')) = 31 ) then
             AddDay := false;
          end if;
       end if;

       if( AddDay = true ) then
          T_cur := T_cur + 1;
          T_all := T_all + 1;
       end if;
    end if;

    if( T_all = 0 ) then
       return 0;
    end if;

    --так как года могут быть и високосными, то если период дохода попадает в разные
    --года, будем считать по частям, т.е. если первая часть попадает в обычный год,
    --а вторая в високосный, то общая сумма будет такова А/365 + В/366
    if( ( NKDBase_Kind IN (4, 5) OR (NKDBase_Kind = 11 AND IsFirstPeriodFirstCoupon <> 0) ) AND
        ( TO_CHAR( prevDate, 'YYYY') != TO_CHAR( CalcDate, 'YYYY') ) AND
        ( RelativeIncome = 'X' )
      ) then

      CurYear := TO_NUMBER(TO_CHAR( prevDate, 'YYYY'));

      while( NeedContinue = true ) LOOP
        FirstDateCurYear  := TO_DATE('31.12.'||(TO_CHAR( CurYear)),'DD.MM.YYYY');
        FirstDatePrevYear := TO_DATE('31.12.'||(TO_CHAR( CurYear-1)),'DD.MM.YYYY');

        if( NKDBase_Kind IN (4, 11) ) then -- по календарю и в году и в месяце

           if( CurYear = TO_NUMBER(TO_CHAR( prevDate, 'YYYY')) ) then
              Period := FirstDateCurYear - prevDate;
              if( IsFirstPeriodFirstCoupon <> 0 ) then
                 Period := Period + 1;
              end if;
           elsif( CurYear = TO_NUMBER(TO_CHAR( CalcDate, 'YYYY')) ) then
              Period := CalcDate - FirstDatePrevYear;
           else
              if IndexNom = 'X' then
                 Period := 365; -- Так в официальной формуле
              else
                 Period := FI_GetDaysInYear( CurYear );
              end if;
           end if;
        else

           if( CurYear = TO_NUMBER(TO_CHAR( prevDate, 'YYYY')) ) then
              Period := rsNDaysp( FirstDateCurYear ) - rsNDaysp( prevDate );
              if( IsFirstPeriodFirstCoupon <> 0 ) then
                 Period := Period + 1;
              end if;
           elsif(CurYear = TO_NUMBER(TO_CHAR( CalcDate, 'YYYY')) ) then
              Period := rsNDaysp( CalcDate ) - rsNDaysp( FirstDatePrevYear );
           else
              Period := rsNDaysp( FirstDateCurYear ) - rsNDaysp( FirstDatePrevYear );
           end if;
        end if;

        if( Period != 0 ) then
           if( (CoupNumber is not null) AND (CoupNumber != chr(0)) ) then -- НКД для купона
              AccurateCouponSum := AccurateCouponSum + CouponsSum( FIID, CoupNumber, Nominal, tmpCouponSum, CASE WHEN IndexNom = 'X' THEN 365 ELSE FI_GetDaysInYear(CurYear) END, Period, NKDBase_Kind, AmountAvr, UseLatestKnownRate, IsFirstPeriodFirstCoupon, 0, CoupHistID );
           else -- доход для бумаги
              AccurateCouponSum := AccurateCouponSum + AvoirissSum( FIID, Nominal, CASE WHEN IndexNom = 'X' THEN 365 ELSE FI_GetDaysInYear(CurYear) END, Period, NKDBase_Kind, AmountAvr );
           end if;
           -- пересчет за время действия
           -- AccurateCouponSum := AccurateCouponSum * T_cur/Period;
        end if;

        CurYear := CurYear + 1;

        if( CurYear > TO_NUMBER(TO_CHAR( CalcDate, 'YYYY')) ) then
           NeedContinue := false;
        end if;
      end loop;

    else
        if( (CoupNumber is not null) AND (CoupNumber != chr(0)) ) then -- НКД для купона
           if( NKDBase_Kind = 6 ) then
              begin

                 if( RelativeIncome != chr(0) ) then  --% от номинала
                    if( (TO_NUMBER(TO_CHAR( EndDate, 'YYYY')) = TO_NUMBER(TO_CHAR( LastWarntDate, 'YYYY'))) AND (DaysInLastYear < 360) ) then
                       AmountDays := (TO_DATE('01.01.'||TO_CHAR(TO_NUMBER(TO_CHAR( FirstWarntDate, 'YYYY'))+1),'DD.MM.YYYY')-TO_DATE('01.01.'||TO_CHAR(TO_NUMBER(TO_CHAR( FirstWarntDate, 'YYYY'))),'DD.MM.YYYY'));
                       AccurateCouponSum := CouponsSum( FIID, CoupNumber, Nominal, AccurateCouponSum, AmountDays, T_all, NKDBase_Kind, AmountAvr, UseLatestKnownRate, 0, 0, CoupHistID );
                    else
                    --количество купонов в очередном году
                       SELECT Count(1) INTO AmountWarnts
                         FROM DFIWARNTS_DBT F
                        WHERE F.T_ISPARTIAL   != 'X'
                          AND F.t_FIID         = FIID
                          AND F.t_DRAWINGDATE >= FirstWarntPer
                          AND F.t_DRAWINGDATE <= LastWarntPer;

                       AccurateCouponSum := CouponsSum( FIID, CoupNumber, Nominal, AccurateCouponSum, AmountWarnts, 1, NKDBase_Kind, AmountAvr, UseLatestKnownRate, 0, 0, CoupHistID );
                    end if;
                 else
                    AccurateCouponSum := CouponsSum( FIID, CoupNumber, Nominal, AccurateCouponSum, DaysInYear, T_all, NKDBase_Kind, AmountAvr, UseLatestKnownRate, 0, 0, CoupHistID );
                 end if;
              end;
           else
              if(NKDBase_Kind = 11 and IsFirstPeriodFirstCoupon = 0) then
                M := FI_CntCoupPayms(FIID);
              end if;
              AccurateCouponSum := CouponsSum( FIID, CoupNumber, Nominal, AccurateCouponSum, DaysInYear,  T_all, NKDBase_Kind, AmountAvr, UseLatestKnownRate, IsFirstPeriodFirstCoupon, M, CoupHistID );
           end if;
        else -- доход для бумаги
           AccurateCouponSum := AvoirissSum( FIID, Nominal, DaysInYear, T_all, NKDBase_Kind, AmountAvr );
        end if;

        -- пересчет за время действия
        AccurateCouponSum := AccurateCouponSum * T_cur/T_all;

    end if;

    if( (Nominal = 0) OR (Amount = 0) OR (T_all = 0) ) then
       ReturnIncomeRate := 0;
    elsif FI_AvrKindsGetRoot( 2, AvoirKind ) = AVOIRKIND_DEPOSITORY_RECEIPT THEN
       ReturnIncomeRate := 0;
    else

      BEGIN

        select T_IncomeScale
          into IncomeScale
          from dfiwarnts_dbt
         where     T_IsPartial = chr(0)
               and T_FIID      = FIID
               and T_Number    = CoupNumber;

        if(NKDBase_Kind = 11 and IsFirstPeriodFirstCoupon = 0) then
          if(NoRound <> 0) then
            ReturnIncomeRate := (AccurateCouponSum * GREATEST( 1., IncomeScale) * M * 100. * T_all) / (Nominal * Amount * T_cur);
          else
            ReturnIncomeRate := (ROUND(AccurateCouponSum, 2) * GREATEST( 1., IncomeScale) * M * 100. * T_all) / (Nominal * Amount * T_cur);
          end if;
        else
          if(NoRound <> 0) then
            ReturnIncomeRate := ((AccurateCouponSum * GREATEST( 1., IncomeScale) * DaysInYear) * 100.) / (Nominal * Amount * T_all);
          else
            ReturnIncomeRate := ((ROUND(AccurateCouponSum, 2) * GREATEST( 1., IncomeScale) * DaysInYear) * 100.) / (Nominal * Amount * T_all);
          end if;
        end if;

      EXCEPTION
        WHEN NO_DATA_FOUND THEN
       ReturnIncomeRate := 0;
      end;

    end if;

    if(NoRound <> 0) then
      return AccurateCouponSum;
    end if;

    return ROUND(AccurateCouponSum, 2);
  end; --CalcIncomeValueForPeriod

  -- Расчет дохода по бумаге
  FUNCTION  CalcIncomeValue( IsCalcNKD  IN BOOLEAN,
                             FIID     IN NUMBER,
                             CalcDate IN DATE,
                             Amount   IN NUMBER,
                             LastDate IN NUMBER,
                             CorrectDate IN NUMBER DEFAULT 0,
                             NoRound IN NUMBER DEFAULT 0,
                             UseLatestKnownRate IN NUMBER DEFAULT 0, -- Признак использовать последнюю известную ставку для неопределенных купонов
                             UseCoupRateHist IN NUMBER DEFAULT 1
                           ) return NUMBER
  IS
    cursor c_fipartial( BeginDate IN DATE, EndDate IN DATE ) is
       SELECT *
         FROM dfiwarnts_dbt
        WHERE     T_IsPartial = chr(88)
              AND T_FIID      = FIID
              AND T_DrawingDate >= BeginDate
              AND T_DrawingDate <  EndDate
     ORDER BY T_IsPartial, T_FIID, T_DrawingDate;

    CURSOR C_FLRHIST (CoupNumber IN VARCHAR2) IS
                        SELECT T_BEGDATE, T_ENDDATE, T_INCOMERATE, T_INCOMEVOLUME, T_ID
                          FROM DFLRHIST_DBT
                         WHERE T_FIWARNTID IN ( SELECT T_ID FROM DFIWARNTS_DBT WHERE T_FIID = FIID AND T_ISPARTIAL != 'X' AND T_NUMBER = CoupNumber )
                           AND T_BEGDATE <= CalcDate
                      ORDER BY T_ENDDATE;

    FirstDate          DATE;
    EndDate            DATE;
    nextPerDate        DATE;
    prevPerDate        DATE;
    Income             NUMBER  := 0;
    v_Income           NUMBER  := 0;
    ExistPartial       BOOLEAN := false;
    CouponNumber       DFIWARNTS_DBT.T_NUMBER%TYPE;
    RelativeIncome     CHAR;
    CorrectNominalDate NUMBER := 0;
    IsFirstCoupon      NUMBER := 0;
    IsFirstPeriod      NUMBER := -1;
    IsFirstPeriodFirstCoupon NUMBER := 0;
    FirstCoupDate      DATE;
    NewReturnIncomeRate NUMBER := 0.0;
    FloatingRate       NUMBER;
    ExistFRateData     BOOLEAN := false;
    NKDRound_Kind      DAVOIRISS_DBT.T_NKDRound_KIND%TYPE;
    v_LastHistToAllCoupPeriod NUMBER := 0;
  BEGIN
  
     begin
       SELECT avr.T_NKDRound_Kind
         INTO NKDRound_Kind
         FROM davoiriss_dbt avr, dfininstr_dbt fin
        WHERE avr.T_FIID = FIID
          AND fin.t_FIID = avr.t_FIID;
     exception
       when NO_DATA_FOUND then
            return -1;
       when OTHERS then
            dbms_output.put_line('Ошибка' || SQLERRM);
            return -1;
     end;

     --Ищем текущий купон на дату CalcDate
     if( GetIncomeCalculateData( IsCalcNKD, FIID, CalcDate, LastDate, CouponNumber, FirstDate, EndDate, RelativeIncome, IsFirstCoupon, FloatingRate ) = false) then
        ReturnIncomeRate := 0.0;
        return 0; -- нет дохода
     end if;

     FirstCoupDate := FirstDate;
     prevPerDate := FirstDate;

     ExistFRateData := FALSE;
     -- для купона с плавающей ставкой проверим наличие данных
     IF (UseCoupRateHist = 1) AND (FloatingRate = 1) AND (CouponNumber is not null) AND (CouponNumber != chr(0))  THEN
       FOR FLRHIST_rec IN C_FLRHIST(CouponNumber) LOOP
         ExistFRateData := true;
         EXIT;
       END LOOP;
     END IF;

     -- для купона с плавающей ставкой
     IF (ExistFRateData = TRUE)  THEN
-- !!!! Для купона с плавающей ставкой нужно доработать учет ЧП !!!!
        IF substr(rsb_struct.getChar(rsi_rsb_kernel.GetNote(PM_COMMON.OBJTYPE_AVOIRISS, LPAD(FIID, 10, '0'), 38, CalcDate)),1,1) = 'X' THEN
          v_LastHistToAllCoupPeriod := 1;
        END IF;

        IF v_LastHistToAllCoupPeriod <> 0 THEN
          FOR one_rec IN (SELECT T_BEGDATE, T_ENDDATE, T_INCOMERATE, T_INCOMEVOLUME, T_ID
                            FROM DFLRHIST_DBT
                           WHERE T_FIWARNTID IN ( SELECT T_ID FROM DFIWARNTS_DBT WHERE T_FIID = FIID AND T_ISPARTIAL != 'X' AND T_NUMBER = CouponNumber )
                             AND T_BEGDATE <= CalcDate
                           ORDER BY T_ENDDATE DESC)
          LOOP

            if( IsFirstCoupon <> 0 ) then
               IsFirstPeriodFirstCoupon := 1;
            else
               IsFirstPeriodFirstCoupon := 0;
            end if;

            if( one_rec.T_EndDate <= CalcDate ) then -- период заканчивается раньше даты, на которую просят расчитать НКД
               nextPerDate := one_rec.T_EndDate;
            else
               nextPerDate := CalcDate;
            end if;

            v_Income := CalcIncomeValueForPeriod( FIID, nextPerDate, Amount, CouponNumber, prevPerDate, /*nextPerDate*/one_rec.T_EndDate, RelativeIncome, NKDRound_Kind, CorrectDate, CorrectNominalDate, IsFirstPeriodFirstCoupon, 1, TO_DATE('01010001', 'DDMMYYYY'), UseLatestKnownRate, one_rec.t_ID );

            if( v_Income = -1 ) then
               Income := v_Income;
            else
               Income := Income + v_Income;
            end if;

            NewReturnIncomeRate := ReturnIncomeRate;

            EXIT;
          END LOOP;
        ELSE

          FOR FLRHIST_rec IN C_FLRHIST(CouponNumber) LOOP
             if( IsFirstPeriod = -1 ) then
                IsFirstPeriod := 1;
             end if;

             if( FLRHIST_rec.T_EndDate <= CalcDate ) then -- период заканчивается раньше даты, на которую просят расчитать НКД
                nextPerDate := FLRHIST_rec.T_EndDate;
             else
                nextPerDate := CalcDate;
             end if;

             if( (IsFirstCoupon <> 0) and (IsFirstPeriod <> 0) ) then
                IsFirstPeriodFirstCoupon := 1;
             else
                IsFirstPeriodFirstCoupon := 0;
             end if;

             v_Income := CalcIncomeValueForPeriod( FIID, nextPerDate, Amount, CouponNumber, prevPerDate, /*nextPerDate*/FLRHIST_rec.T_EndDate, RelativeIncome, NKDRound_Kind, CorrectDate, CorrectNominalDate, IsFirstPeriodFirstCoupon, 1, TO_DATE('01010001', 'DDMMYYYY'), UseLatestKnownRate, FLRHIST_rec.t_ID );

             if( v_Income = -1 ) then
                Income := v_Income;
                --NewReturnIncomeRate := ReturnIncomeRate;
             else
                Income := Income + v_Income;
                --NewReturnIncomeRate := NewReturnIncomeRate + ReturnIncomeRate;
                NewReturnIncomeRate := ReturnIncomeRate;
             end if;

             prevPerDate := nextPerDate+1;
             IsFirstPeriod := 0;
          exit when( (FLRHIST_rec.T_EndDate >= CalcDate) or (v_Income = -1) );
          END LOOP;
        END IF;

        IF NoRound = 0 THEN
          Income := ROUND(Income, 2);
        END IF;

     ELSE
        -- ЧП, дата погашения которых >= даты начала текущего купонного периода
        for fiwarnts_rec in c_fipartial( FirstDate, EndDate ) loop

           if( IsFirstPeriod = -1 ) then
              IsFirstPeriod := 1;
           end if;

           if( fiwarnts_rec.T_DrawingDate <= CalcDate ) then -- ЧП заканчивается раньше дата, на которую просят расчитать НКД
              nextPerDate := fiwarnts_rec.T_DrawingDate;
           else
              nextPerDate := CalcDate;
           end if;

           if( fiwarnts_rec.T_DrawingDate = FirstDate ) then
              CorrectNominalDate := 1; -- Случай, когда дата начала купонного периода совпадает с датой частичного погашения
                                       -- в качестве номинала (N) берется номинал ц/б с учетом этого ЧП
           else
              CorrectNominalDate := 0;
           end if;

           if( (IsFirstCoupon <> 0) and (IsFirstPeriod <> 0) ) then
              IsFirstPeriodFirstCoupon := 1;
           else
              IsFirstPeriodFirstCoupon := 0;
           end if;

           v_Income := CalcIncomeValueForPeriod( FIID, nextPerDate, Amount, CouponNumber, prevPerDate, /*nextPerDate*/EndDate, RelativeIncome, NKDRound_Kind, CorrectDate, CorrectNominalDate, IsFirstPeriodFirstCoupon, NoRound );

           if( v_Income = -1 ) then
              Income := v_Income;
              NewReturnIncomeRate := ReturnIncomeRate;
           else
              Income := Income + v_Income;
              NewReturnIncomeRate := NewReturnIncomeRate + ReturnIncomeRate;
              ExistPartial := true;
           end if;

           prevPerDate := nextPerDate+1;
           IsFirstPeriod := 0;
        exit when( (fiwarnts_rec.T_DrawingDate >= CalcDate) or (v_Income = -1) );
        end loop;

        if( Income <> -1 ) then
           if( (ExistPartial = true) AND (nextPerDate < CalcDate) ) then -- если ЧП есть не на весь купонный период
              Income := Income + CalcIncomeValueForPeriod( FIID, CalcDate, Amount, CouponNumber, prevPerDate, EndDate, RelativeIncome, NKDRound_Kind, CorrectDate, 0, 0, NoRound, FirstCoupDate, UseLatestKnownRate);
              NewReturnIncomeRate := NewReturnIncomeRate + ReturnIncomeRate;
           elsif( ExistPartial = false ) then -- Нет ЧП
              Income := CalcIncomeValueForPeriod( FIID, CalcDate, Amount, CouponNumber, FirstDate, EndDate, RelativeIncome, NKDRound_Kind, CorrectDate, 0, IsFirstCoupon, NoRound, FirstCoupDate, UseLatestKnownRate);
              NewReturnIncomeRate := ReturnIncomeRate;
           end if;
        end if;

     END IF;
     
     if( NKDRound_Kind = 1 ) then
        if(NoRound <> 0) then
          Income := Income * Amount;
        else
          Income := ROUND(Income, 2) * Amount;
        end if;
     elsif( NKDRound_Kind = 2 ) then
--       AccurateCouponSum := ROUND(AccurateCouponSum  * Amount, 2);
        if(NoRound = 0) then
          Income := ROUND(Income, 2);
        end if;

     else
        if(NoRound <> 0) then
          Income := Income * Amount;
        else
          Income := ROUND(Income, 2) * Amount;
        end if;
     end if;

     ReturnIncomeRate := NewReturnIncomeRate;

     return Income;
  END;

  -- Расчет НКД
  FUNCTION  CalcNKD( FIID     IN NUMBER,
                     CalcDate IN DATE,
                     Amount   IN NUMBER,
                     LastDate IN NUMBER,
                     CorrectDate IN NUMBER DEFAULT 0,
                     NoRound  IN NUMBER DEFAULT 0,
                     UseCoupRateHist IN NUMBER DEFAULT 1
                   ) return NUMBER
  IS
  BEGIN
     return CalcIncomeValue( true, FIID, CalcDate, Amount, LastDate, CorrectDate, NoRound, 0, UseCoupRateHist );
  END;

  -- Расчет НКД
  FUNCTION  CalcNKD_Ex( FIID       IN NUMBER,
                        CalcDate   IN DATE,
                        Amount     IN NUMBER,
                        LastDate   IN NUMBER,
                        CorrectDate IN NUMBER DEFAULT 0,
                        UseCoupRateHist IN NUMBER DEFAULT 1
                      ) return NUMBER
  IS
  BEGIN
     return CalcNKD( FIID, CalcDate, Amount, LastDate, CorrectDate, 0, UseCoupRateHist );
  END;

  -- Расчет НКД без округления
  FUNCTION  CalcNKD_Ex_NoRound( FIID       IN NUMBER,
                                CalcDate   IN DATE,
                                Amount     IN NUMBER,
                                LastDate   IN NUMBER,
                                CorrectDate IN NUMBER DEFAULT 0,
                                UseCoupRateHist IN NUMBER DEFAULT 1
                              ) return NUMBER
  IS
  BEGIN
     return CalcNKD( FIID, CalcDate, Amount, LastDate, CorrectDate, 1, UseCoupRateHist );
  END;

  --минимальное значение из всех курсов вида pType на дату pDate,
  --но эта дата должна быть не ранее, чем за pNDays дней от pDate
  function FI_GetMinRate(
                    pFromFI     IN  NUMBER,
                    pToFI       IN  NUMBER,
                    pType       IN  NUMBER,
                    pDate       IN  DATE,
                    pNDays      IN  NUMBER,
                    pRateID     OUT NUMBER,
                    pSinceDate  OUT DATE ) return NUMBER is

   BEGIN

      return FI_GetRate( pFromFI, pToFI, pType, pDate, pNDays, 1, pRateID, pSinceDate);

    EXCEPTION
      WHEN NO_DATA_FOUND THEN
           return NULL;
      WHEN OTHERS THEN
           dbms_output.put_line('Ошибка' || SQLERRM);
           return NULL;
  end;

  --минимальное значение из всех курсов вида pType на дату pDate,
  --но эта дата должна быть не ранее, чем за pNDays дней от pDate
  function FI_GetMinRateMonth(
                    pFromFI     IN  NUMBER,
                    pToFI       IN  NUMBER,
                    pType       IN  NUMBER,
                    pDate       IN  DATE,
                    pNMonths    IN  NUMBER,
                    pRateID     OUT NUMBER,
                    pSinceDate  OUT DATE,
                    pMarketCountry IN VARCHAR2 DEFAULT CHR(1),
                    pIsForeignMarket IN NUMBER DEFAULT 0,
                    pOnlyRate IN NUMBER DEFAULT 0) return NUMBER is

   BEGIN

      return FI_GetRate( pFromFI, pToFI, pType, pDate, pDate-add_months(pDate,-pNMonths)-1, 1, pRateID, pSinceDate, false, pMarketCountry, pIsForeignMarket, pOnlyRate, 1);

    EXCEPTION
      WHEN NO_DATA_FOUND THEN
           return NULL;
      WHEN OTHERS THEN
           dbms_output.put_line('Ошибка' || SQLERRM);
           return NULL;
  end;
-------------------------------------------------------------------------------------------------------
  function FI_GetRate( pFromFI          IN  NUMBER,
                       pToFI            IN  NUMBER,
                       pType            IN  NUMBER,
                       pDate            IN  DATE,
                       pNDays           IN  NUMBER,
                       pIsMaxMin        IN  NUMBER,
                       pRateID          OUT NUMBER,
                       pSinceDate       OUT DATE,
                       pIsMrkt          IN  BOOLEAN,
                       pMarketCountry   IN VARCHAR2 DEFAULT CHR(1),
                       pIsForeignMarket IN NUMBER DEFAULT 0,
                       pOnlyRate        IN NUMBER DEFAULT 0,
                       pCanUseCross     IN NUMBER DEFAULT 0,
                       pMarket_Place    IN NUMBER DEFAULT -1,
                       pISMANUALINPUT   IN NUMBER DEFAULT -1
                     ) return NUMBER is

   vRevflag       CHAR;
   vCurCourse     NUMBER := NULL;
   vCourse        NUMBER := NULL;
   vMinInt        NUMBER := pNDays;
   vIsMarket      BOOLEAN:= False;
   v_Count        NUMBER;
   v_CourseDate   DATE;
   v_RecRateDef   DRATEDEF_DBT%ROWTYPE;
   v_type         NUMBER;
   vRateID        NUMBER := 0;
   vSinceDate     DATE := TO_DATE( '01.01.0001 ', 'dd.mm.yyyy' );
   v_MaxDate      DATE := TO_DATE( '01.01.0001 ', 'dd.mm.yyyy' );
   v_break        NUMBER := 0;

   type CourseData is record
   (
     t_RateID       NUMBER(10),
     t_SinceDate    DATE,
     t_Course       number(32,12),
     t_Market_Place NUMBER(10),
     t_Course_Quantity number(32,12)
   );

   type CourseData_t is table of CourseData;

   v_CourseData   CourseData_t := CourseData_t();
   v_cRateDef_sql varchar2(32000) ;
   v_cursor number;
   v_dummy PLS_INTEGER;
   CURSOR cRateDef is
      SELECT RateDef.*, cast(null as number ) as Priority
        FROM DRATEDEF_DBT RateDef
       WHERE 1=2 ;
   RateDef_rec cRateDef%rowtype;
   RateDef_cur sys_refcursor ;

   /*CURSOR cRateDef is
      SELECT RateDef.*,
             (CASE WHEN rsi_rsb_kernel.GetNote(PM_COMMON.OBJTYPE_PARTY, TO_CHAR((CASE WHEN RateDef.t_Market_Place <= 0 THEN RateDef.t_Informator ELSE RateDef.t_Market_Place END), 'FM0999999999'), 55\*Приоритет биржи для поиска котировок*\, pDate) IS NULL
                   THEN 99999999999999999999999999999999999
                   ELSE rsb_struct.getInt(rsi_rsb_kernel.GetNote(PM_COMMON.OBJTYPE_PARTY, TO_CHAR((CASE WHEN RateDef.t_Market_Place <= 0 THEN RateDef.t_Informator ELSE RateDef.t_Market_Place END), 'FM0999999999'), 55\*Приоритет биржи для поиска котировок*\, pDate))
               END) as Priority
        FROM DRATEDEF_DBT RateDef
       WHERE     RateDef.t_Type    = pType AND
                 ( (RateDef.t_OtherFI = pFromFI AND(RateDef.t_FIID = pToFI or pToFI = -1)) OR
                   ((RateDef.t_OtherFI = pToFI or pToFI = -1) AND RateDef.t_FIID    = pFromFI) OR
                   ( RateDef.t_OtherFI = pFromFI AND RateDef.t_IsRelative = chr(88) ) OR
                   ( pCanUseCross > 0 AND RateDef.t_OtherFI = pFromFI )
                 )
         AND 1 = ( CASE WHEN pIsForeignMarket > 0 AND EXISTS(SELECT 1 FROM DPARTY_DBT mrkt
                                                              WHERE mrkt.t_PartyID = (CASE WHEN RateDef.t_Market_Place <= 0 THEN RateDef.t_Informator ELSE RateDef.t_Market_Place END)
                                                                AND mrkt.t_NotResident = 'X') THEN 1
                        WHEN pIsForeignMarket = 0 OR pIsForeignMarket IS NULL THEN 1
                        ELSE 0 END)
         AND (CASE WHEN RateDef.t_Market_Place <= 0 THEN RateDef.t_Informator ELSE RateDef.t_Market_Place END) = ( CASE WHEN pMarket_Place > 0 THEN pMarket_Place ELSE (CASE WHEN RateDef.t_Market_Place <= 0 THEN RateDef.t_Informator ELSE RateDef.t_Market_Place END) END )
         AND 1 = ( CASE WHEN pMarketCountry IS NULL OR pMarketCountry = CHR(0) OR pMarketCountry = CHR(1) THEN 1
                        WHEN pMarketCountry = 'RUS' AND EXISTS(SELECT 1 FROM DPARTY_DBT mrkt
                                                                WHERE mrkt.t_PartyID = (CASE WHEN RateDef.t_Market_Place <= 0 THEN RateDef.t_Informator ELSE RateDef.t_Market_Place END)
                                                                  AND mrkt.t_NotResident = CHR(0)) THEN 1
                        WHEN EXISTS(SELECT 1 FROM DPARTY_DBT mrkt
                                     WHERE mrkt.t_PartyID = (CASE WHEN RateDef.t_Market_Place <= 0 THEN RateDef.t_Informator ELSE RateDef.t_Market_Place END)
                                       AND mrkt.t_NotResident = 'X'
                                       AND mrkt.t_NRCountry = pMarketCountry) THEN 1
                        ELSE 0 END)
    ORDER BY Priority;*/
   BEGIN
       v_cRateDef_sql:='SELECT RateDef.*,
             (CASE WHEN rsi_rsb_kernel.GetNote(:OBJTYPE_PARTY, TO_CHAR((CASE WHEN RateDef.t_Market_Place <= 0 THEN RateDef.t_Informator ELSE RateDef.t_Market_Place END), ''FM0999999999''), 55/*Приоритет биржи для поиска котировок*/, :pDate) IS NULL
                   THEN 99999999999999999999999999999999999
                   ELSE rsb_struct.getInt(rsi_rsb_kernel.GetNote(:OBJTYPE_PARTY, TO_CHAR((CASE WHEN RateDef.t_Market_Place <= 0 THEN RateDef.t_Informator ELSE RateDef.t_Market_Place END), ''FM0999999999''), 55/*Приоритет биржи для поиска котировок*/, :pDate))
               END) as Priority
        FROM DRATEDEF_DBT RateDef
       WHERE     RateDef.t_Type    = :pType AND
                 ( ( RateDef.t_OtherFI = :pFromFI '||case when pToFI !=-1 then ' AND RateDef.t_FIID    = :pToFI ' end||') OR
                   ( RateDef.t_FIID    = :pFromFI '||case when pToFI !=-1 then ' AND RateDef.t_OtherFI = :pToFI ' end||') OR
                   ( RateDef.t_OtherFI = :pFromFI AND RateDef.t_IsRelative = chr(88))
                 '|| case when pCanUseCross > 0 then ' OR  RateDef.t_OtherFI = :pFromFI ' end||' )' ;
        
       v_cRateDef_sql:= v_cRateDef_sql || CASE WHEN pIsForeignMarket > 0 then  
                     ' AND EXISTS(SELECT 1 FROM DPARTY_DBT mrkt
                                   WHERE mrkt.t_PartyID = (CASE WHEN RateDef.t_Market_Place <= 0 THEN RateDef.t_Informator ELSE RateDef.t_Market_Place END)
                                                             AND mrkt.t_NotResident = ''X'') '
                        WHEN pIsForeignMarket = 0 OR pIsForeignMarket IS NULL THEN null
                        ELSE ' AND 1=2' 
                        END ;

       v_cRateDef_sql:= v_cRateDef_sql ||CASE WHEN pMarket_Place > 0 THEN 
            '        AND :pMarket_Place = CASE WHEN RateDef.t_Market_Place <= 0 THEN RateDef.t_Informator ELSE RateDef.t_Market_Place END ' end ;
     
       v_cRateDef_sql:= v_cRateDef_sql ||
         CASE WHEN pMarketCountry IS NULL OR pMarketCountry = CHR(0) OR pMarketCountry = CHR(1) THEN null
              WHEN pMarketCountry = 'RUS' then ' AND EXISTS(SELECT 1 FROM DPARTY_DBT mrkt
                                                                WHERE mrkt.t_PartyID = (CASE WHEN RateDef.t_Market_Place <= 0 THEN RateDef.t_Informator ELSE RateDef.t_Market_Place END)
                                                                  AND mrkt.t_NotResident = CHR(0)) '
              ELSE ' AND EXISTS(SELECT 1 FROM DPARTY_DBT mrkt
                           WHERE mrkt.t_PartyID = (CASE WHEN RateDef.t_Market_Place <= 0 THEN RateDef.t_Informator ELSE RateDef.t_Market_Place END)
                                  AND mrkt.t_NotResident = ''X''
                                   AND mrkt.t_NRCountry = :pMarketCountry) ' 
              END ;
              
      v_cRateDef_sql:= v_cRateDef_sql || ' ORDER BY Priority';

       v_cursor := DBMS_SQL.open_cursor;
       DBMS_SQL.parse (v_cursor, v_cRateDef_sql, DBMS_SQL.native);
       DBMS_SQL.bind_variable (v_cursor, ':OBJTYPE_PARTY',PM_COMMON.OBJTYPE_PARTY);
       DBMS_SQL.bind_variable (v_cursor, ':pDate',pDate);
       DBMS_SQL.bind_variable (v_cursor, ':pType',pType);
       DBMS_SQL.bind_variable (v_cursor, ':pFromFI',pFromFI);

       if pToFI !=-1 then
          DBMS_SQL.bind_variable (v_cursor, ':pToFI',pToFI);
       end if;
       if pMarket_Place > 0 then
          DBMS_SQL.bind_variable (v_cursor, ':pMarket_Place',pMarket_Place);
       end if;
       if not (pMarketCountry IS NULL OR pMarketCountry = CHR(0) OR pMarketCountry = CHR(1) or  pMarketCountry = 'RUS') then
          DBMS_SQL.bind_variable (v_cursor, ':pMarketCountry',pMarketCountry);     
       end if;
       v_dummy := DBMS_SQL.EXECUTE (v_cursor);
       RateDef_cur := DBMS_SQL.to_refcursor (v_cursor);       
   
      pRateID    := NULL;
      pSinceDate := NULL;


   --   FOR RateDef_rec in cRateDef loop
   loop
       FETCH RateDef_cur INTO RateDef_rec ;
       EXIT WHEN RateDef_cur%NOTFOUND;  
       if( (RateDef_rec.T_OTHERFI = pFromFI and RateDef_rec.T_FIID = pToFI) or
             (RateDef_rec.t_OtherFI = pFromFI AND RateDef_rec.t_IsRelative = chr(88)) or
             (RateDef_rec.t_OtherFI = pFromFI AND pCanUseCross > 0) or
             (RateDef_rec.t_OtherFI = pFromFI AND pToFI = -1)
           ) then
           vRevflag := CHR(0);
         else
           vRevflag := 'X';
         end if;

         begin
           select count(1) into v_Count from dpartyown_dbt where t_PartyID = (CASE WHEN RateDef_rec.t_Market_Place <= 0 THEN RateDef_rec.t_Informator ELSE RateDef_rec.t_Market_Place END) and t_PartyKind = 3; -- проверяем что курс с биржы

           if( v_Count > 0 ) then
              vIsMarket := True;
           else
              vIsMarket := False;
           end if;
         exception
           when OTHERS then vIsMarket := False;
         end;

         SELECT * INTO v_RecRateDef
           FROM dratedef_dbt
          WHERE t_RateID = RateDef_rec.t_RateID;

         vCurCourse := ConvSum_ex( 1, RateDef_rec.t_OtherFI, RateDef_rec.t_FIID, pDate, v_RecRateDef, vRevflag, 0, pOnlyRate, pISMANUALINPUT );

         if(pToFI != -1 AND pCanUseCross > 0 AND RateDef_rec.t_FIID <> pToFI) then
           v_CourseDate := FI_ConvSum_CourseDate;
           vCurCourse := ConvSum( vCurCourse, RateDef_rec.t_FIID, pToFI, v_CourseDate, 0 );
           FI_ConvSum_CourseDate := v_CourseDate;
         end if;

         if( pIsMaxMin = 1 or pIsMaxMin = 2 )then
            if( ((vCurCourse IS NOT NULL) and (vMinInt >= (pDate - FI_ConvSum_CourseDate))) and
                (( (pIsMrkt = vIsMarket) AND (pIsMrkt = True) ) or
                         (pIsMrkt = False)
                )
                       ) then

               if( pDate - v_MaxDate > pDate - FI_ConvSum_CourseDate )then
                  v_MaxDate := FI_ConvSum_CourseDate;
               end if;

               v_CourseData.extend;

               v_CourseData(v_CourseData.LAST).t_RateID       := RateDef_rec.t_RateID;
               v_CourseData(v_CourseData.LAST).t_SinceDate    := FI_ConvSum_CourseDate;
               v_CourseData(v_CourseData.LAST).t_Course       := vCurCourse;
               v_CourseData(v_CourseData.LAST).t_Market_Place := (CASE WHEN RateDef_rec.t_Market_Place <= 0 THEN RateDef_rec.t_Informator ELSE RateDef_rec.t_Market_Place END);
               v_CourseData(v_CourseData.LAST).t_Course_Quantity := -1;

               v_type := Rsb_Common.GetRegIntValue('SECUR\ВИД КУРСА КОЛИЧЕСТВО СДЕЛОК', 0);
               if( v_type > 0 )then
                  vRateID := 0;
                  v_CourseData(v_CourseData.LAST).t_Course_Quantity := FI_GetRate( pFromFI,
                                                                                   pToFI,
                                                                                   v_type,
                                                                                   v_CourseData(v_CourseData.LAST).t_SinceDate,
                                                                                   0,
                                                                                   0,
                                                                                   vRateID,
                                                                                   vSinceDate,
                                                                                   pIsMrkt,
                                                                                   null,
                                                                                   null,
                                                                                   null,
                                                                                   null,
                                                                                   v_CourseData(v_CourseData.LAST).t_Market_Place
                                                                                 );
                  if( vRateID <= 0 ) then
                     v_CourseData(v_CourseData.LAST).t_Course_Quantity := -1;
                  end if;
               end if;
            end if;
         else
            if( (vCurCourse IS NOT NULL) AND
                (vMinInt >= (pDate - FI_ConvSum_CourseDate))
              ) THEN
               if( pSinceDate is null or pSinceDate != FI_ConvSum_CourseDate ) THEN
                  if( ( (pIsMrkt = vIsMarket) AND (pIsMrkt = True) ) or
                      (pIsMrkt = False)
                    ) then
                     pRateID    := RateDef_rec.t_RateID;
                     pSinceDate := FI_ConvSum_CourseDate;
                     vCourse    := vCurCourse;
                     vMinInt    := pDate - FI_ConvSum_CourseDate;
                  end if;
               end if;
            end if;
         end if;

      END LOOP;
   CLOSE RateDef_cur ;

      if( pIsMaxMin = 1 or pIsMaxMin = 2 )then
         v_break := 0;
         vCourse := NULL;

         IF v_CourseData IS NOT EMPTY THEN
            for i in v_CourseData.FIRST .. v_CourseData.LAST
            loop
               exit when v_break = 1;

               if( v_CourseData(i).t_SinceDate = v_MaxDate )then

                  if( v_CourseData(i).t_Course_Quantity > 1 )then
                     if( vCourse is not null )then
                        vCourse := NULL;
                        v_break    := 1;
                     else
                        pRateID    := v_CourseData(i).t_RateID;
                        pSinceDate := v_CourseData(i).t_SinceDate;
                        vCourse    := v_CourseData(i).t_Course;
                     end if;
                  elsif( v_CourseData(i).t_Course_Quantity = -1 )then
                     vCourse := NULL;
                     v_break    := 1;
                  end if;

               end if;

            end loop;

            if( vCourse is NULL )then
               for i in v_CourseData.FIRST .. v_CourseData.LAST
               loop
                  if( v_CourseData(i).t_SinceDate = v_MaxDate )then
                    if( ((pIsMaxMin = 1) AND ((vCourse IS NULL) OR (v_CourseData(i).t_Course < vCourse ))) OR
                        ((pIsMaxMin = 2) AND ((vCourse IS NULL) OR (v_CourseData(i).t_Course > vCourse )))
                      ) then -- есть подходящий курс данного вида
                           pRateID    := v_CourseData(i).t_RateID;
                           pSinceDate := v_CourseData(i).t_SinceDate;
                           vCourse    := v_CourseData(i).t_Course;
                    end if;
                  end if;
               end loop;
            end if;

            v_CourseData.delete;
         END IF;
      end if;

      if( vCourse IS NULL ) then
         vCourse    := 0;
         pRateID    := 0;
         pSinceDate := TO_DATE( '01.01.0001 ', 'dd.mm.yyyy' );
      end if;

      return vCourse;

    EXCEPTION
      WHEN NO_DATA_FOUND THEN
           pRateID    := 0;
           pSinceDate := TO_DATE( '01.01.0001 ', 'dd.mm.yyyy' );
           return 0;
      WHEN OTHERS THEN
           pRateID    := 0;
           pSinceDate := TO_DATE( '01.01.0001 ', 'dd.mm.yyyy' );
           dbms_output.put_line('Ошибка' || SQLERRM);
           return 0;
  end;

  -- Функция определения суммы конверсии по кросскурсам
  FUNCTION CalcSumCross2
  (
    SumB     IN NUMBER   --Исходная сумма
   ,pFromFI  IN NUMBER   --Исходный Фин. инструмент
   ,pToFI    IN NUMBER   --Фин. инструмент, в который надо пересчитать
   ,pbdate   IN DATE     --Дата курса
   ,pround   IN NUMBER DEFAULT 1 --признак округл. до копеек, по умолч округл.
   ,pRateType  IN OUT NUMBER --Вид курса, если <-1 то возвращать параметр pRateType не нужно
   ,pRate      OUT NUMBER --Курс
   ,pScale     OUT NUMBER --Масштаб
   ,pPoint     OUT NUMBER --Округление
   ,pIsInverse OUT CHAR   --Признак обратной котировки
  )
  RETURN NUMBER IS

     CalcSum     NUMBER;
     Point2      NUMBER;
     SaveRateType NUMBER;
  BEGIN
    IF pFromFI = pToFI THEN
      CalcSum := SumB;
    ELSE
      SaveRateType := pRateType;

      IF( pFromFI <> NATCUR ) THEN
        CalcSum := ConvSumNat( SumB, pFromFI, 'X', pbdate, 0, pRateType, pRate, pScale, pPoint, pIsInverse );
      ELSE
        CalcSum := SumB;
      END IF;

      IF( pToFI <> NATCUR AND CalcSum IS NOT NULL) THEN
        CalcSum := ConvSumNat( CalcSum, pToFI, CHR(0), pbdate, 0, pRateType, pRate, pScale, Point2, pIsInverse );

        IF SaveRateType = -1 THEN
          MakeNormRate( CalcSum, SumB, pRate, pScale, pPoint, pIsInverse );
/*
          pRate := CalcSum / SumB;
          IF( Point2 > pPoint ) THEN
            pPoint := Point2;
          END IF;
          pRate := pRate  * POWER(10, pPoint) ;
          pRate := ROUND(pRate, 0);
*/
        END IF;

      END IF;
    END IF;

    IF pround <> 0 THEN
        CalcSum := ROUND( CalcSum, 2 );
    END IF;

    RETURN CalcSum;

    EXCEPTION
      WHEN NO_DATA_FOUND THEN
           RETURN NULL;
      WHEN OTHERS THEN
           DBMS_OUTPUT.PUT_LINE('Ошибка' || SQLERRM);
           RETURN NULL;
  END;

  -- Функция определения суммы конверсии по кросскурсам
  FUNCTION CalcSumCross
  (
    SumB     IN NUMBER   --Исходная сумма
   ,pFromFI  IN NUMBER   --Исходный Фин. инструмент
   ,pToFI    IN NUMBER   --Фин. инструмент, в который надо пересчитать
   ,pbdate   IN DATE     --Дата курса
   ,pround   IN NUMBER DEFAULT 1 --признак округл. до копеек, по умолч округл.
  )
  RETURN NUMBER IS

    CalcSum     NUMBER;

    m_RateType NUMBER;
    m_Rate NUMBER;
    m_Scale NUMBER;
    m_Point NUMBER;
    m_IsInverse CHAR;

  BEGIN
    m_RateType := -2;
    RETURN CalcSumCross2(SumB, pFromFI, pToFI, pbdate, pround, m_RateType, m_Rate, m_Scale, m_Point, m_IsInverse);
  END;

  FUNCTION FI_CalcIncomeValue( FIID      IN NUMBER, -- Выпуск ц/б
                               CalcDate  IN DATE,   -- Дата начисления
                               Amount    IN NUMBER, -- Количество
                               LastDate  IN NUMBER, -- Признак начисления дохода в дату погашения:
                                                    --   0 - в дату погашения доход равен нулю
                                                    --   1 - в дату погашения доход начисляется
                               CorrectDate IN NUMBER DEFAULT 0,--  Признак коррекции последней даты месяца
                                                    --   0 - CalcDate не корректируется в соответствии с базисом расчета (умолч.)
                                                    --   1 - CalcDate корректируется в соответствии с базисом расчета
                               NoRound IN NUMBER DEFAULT 0, --  Признак округления
                                                    --   0 - округлять (умолч.)
                                                    --   1 - не округлять
                               UseLatestKnownRate IN NUMBER DEFAULT 0 -- Признак использовать последнюю известную ставку для неопределенных купонов

                             ) RETURN NUMBER DETERMINISTIC
  IS
     v_DrawingDate   DFININSTR_DBT.T_DrawingDate%TYPE;
     v_CalcDate      DATE;
     v_IsCalcNKD     BOOLEAN;
  BEGIN
     InitError();

     -- Если CalcDate при расчете больше даты погашения, то принять CalcDate равной дате погашения
     BEGIN
       SELECT T_DrawingDate
         INTO v_DrawingDate
         FROM DFININSTR_DBT
        WHERE t_FIID = FIID;
     EXCEPTION
       when NO_DATA_FOUND then return 0; -- нет бумажки нет и дохода
     END;

     if( (CalcDate > v_DrawingDate) and (v_DrawingDate != TO_DATE( '01.01.0001 ', 'dd.mm.yyyy' )) ) then
        v_CalcDate := v_DrawingDate;
     else
        v_CalcDate := CalcDate;
     end if;

     -- Если ц/б купонная, то расчитывается НКД на дату CalcDate.
     if( FI_IsCouponAvoiriss( FIID ) != 0 ) then
        v_IsCalcNKD := true;
     else
        v_IsCalcNKD := false;
     end if;

     return CalcIncomeValue( v_IsCalcNKD, FIID, v_CalcDate, Amount, LastDate, CorrectDate, NoRound, UseLatestKnownRate );
  END; --FI_CalcIncomeValue

  -- Возвращает общий процентный доход по облигации за весь купонный период или за весь период погашения
  -- Применяется как для купонных, так и для бескупонных облигаций с указанным доходом.
  FUNCTION FI_CalcTotalIncome( FIID      IN NUMBER, -- Выпуск ц/б
                               Coupon    IN VARCHAR2,--Номер купона (не задается для выпуска)
                               Amount    IN NUMBER  -- Количество
                             ) RETURN NUMBER DETERMINISTIC
  IS
     v_DrawingDate  DATE;
     v_IsCalcNKD     BOOLEAN;

  BEGIN

     InitError();

     if( (Coupon is not NULL) AND (Coupon != chr(0)) ) then
        BEGIN
          SELECT T_DrawingDate
            INTO v_DrawingDate
            FROM DFIWARNTS_DBT
           WHERE     t_FIID      = FIID
                 AND t_IsPartial =  chr(0)
                 AND t_Number    = Coupon;
        EXCEPTION
          when NO_DATA_FOUND then return 0; -- нет купона нет и дохода
        END;
        v_IsCalcNKD := true;
     else
        BEGIN
          select RSI_RSB_FIInstr.FI_GetNominalDrawingDate(F.t_FIID,
                                                          (select t_Termless from davoiriss_dbt where t_FIID = F.t_FIID))
            INTO v_DrawingDate
            FROM DFININSTR_DBT F
           WHERE F.t_FIID = FIID;
        EXCEPTION
          when NO_DATA_FOUND then return 0.0; -- нет бумажки нет и дохода
        END;
        v_IsCalcNKD := false;
     end if;

     return CalcIncomeValue( v_IsCalcNKD, FIID, v_DrawingDate, Amount, 1 );
  END; -- FI_CalcTotalIncome


  function FI_GetRateMP( pFromFI       IN NUMBER,
                         pToFI         IN NUMBER,
                         pType         IN NUMBER,
                         pDate         IN DATE,
                         pMarket_Place IN NUMBER, --Торговая площадка
                         pSection      IN NUMBER, --Секция торговой площадки
                         pOnlyThisDate IN NUMBER DEFAULT 0 ) return NUMBER is
    v_Course NUMBER;

  BEGIN
    v_Course := 0;
    v_Course := ConvSumMP( 1, pFromFI, pToFI, pType, pMarket_Place, pSection, pDate);

    IF( ( (pOnlyThisDate = 1) AND (FI_ConvSum_CourseDate = pDate) ) OR (pOnlyThisDate = 0) ) THEN
       return v_Course;
    END IF;

    return 0.0;
  EXCEPTION
    when OTHERS then return 0.0;
  END; --FI_GetRateMP


  -- проверяем нужно ли искать курс НКД за дату и если нужно то ищем
  FUNCTION FindNKDCource( p_FIID IN NUMBER, p_CalcDate IN DATE, p_IsTrust IN NUMBER DEFAULT 0)
    RETURN NUMBER
  IS
    v_FaceValueFI NUMBER;
    v_Rate        NUMBER;
    v_RateID      NUMBER;
    v_RateDate    DATE;
    v_NeedFindNKDCource BOOLEAN;
    v_NKDCourseType     NUMBER  := 15;
  BEGIN

    if( p_IsTrust = 1 ) then
       v_NeedFindNKDCource := Rsb_Common.GetRegBoolValue('ДОВЕРИТЕЛЬНОЕ УПРАВЛЕНИЕ\РАСЧЕТ НКД ПО КУРСУ');
    else
       v_NeedFindNKDCource := Rsb_Common.GetRegBoolValue('SECUR\РАСЧЕТ НКД ПО КУРСУ');
    end if;

    if( v_NeedFindNKDCource ) then

       if( p_IsTrust = 1 ) then
          v_NKDCourseType := Rsb_Common.GetRegIntValue('ДОВЕРИТЕЛЬНОЕ УПРАВЛЕНИЕ\ВИД КУРСА НКД ДЛЯ ЦБ');
       else
          v_NKDCourseType := Rsb_Common.GetRegIntValue('SECUR\ВИД КУРСА НКД ДЛЯ ЦБ');
       end if;

    BEGIN
      select t_FaceValueFI into v_FaceValueFI
        from dfininstr_dbt
       where t_FIID = p_FIID;
    EXCEPTION
       WHEN OTHERS THEN
         return -1;
    END;

       v_Rate := RSI_RSB_FIInstr.FI_GetRate( p_FIID, v_FaceValueFI, v_NKDCourseType, p_CalcDate, 0, 0, v_RateID, v_RateDate, False);
       if( v_Rate > 0 or ( v_Rate = 0 and v_RateID > 0 ) ) then
          return v_Rate;
       end if;

    end if;

    return -1;
  EXCEPTION
     WHEN OTHERS THEN
       return -1;
  END;

  --Вычисляет накопленный процентный доход по облигации на дату
  FUNCTION FI_CalcNKD( FIID      IN NUMBER,
                       CalcDate  IN DATE,
                       Amount    IN NUMBER,
                       IsTrust   IN NUMBER
                     ) RETURN NUMBER DETERMINISTIC
  IS
    v_NKDCourse NUMBER := 0;
  BEGIN

    IF FI_HasCoupon(FIID) THEN
       v_NKDCourse := FindNKDCource(FIID, CalcDate, IsTrust);
       IF v_NKDCourse >= 0 THEN
         RETURN ROUND(Amount * v_NKDCourse, 2);
       ELSE
         RETURN ROUND(FI_CalcIncomeValue(FIID, CalcDate, Amount, 0, 0), 2);
       END IF;
    END IF;

    RETURN 0;

  END; --FI_CalcNKD

  -- Процедура заполнения буфера внутреннего курса по данным из истории курса
  PROCEDURE FillQuotdefByHist
  (
    p_QuotHistBuff IN dquothist_dbt%ROWTYPE -- буфер истории внутреннего курса
   ,p_QuotDefBuff OUT dquotdef_dbt%ROWTYPE -- буфер внутреннего курса
  )
  IS
  BEGIN
    p_QuotDefBuff.t_RateID      := p_QuotHistBuff.t_RateID;
    p_QuotDefBuff.t_Rate        := p_QuotHistBuff.t_Rate;
    p_QuotDefBuff.t_Scale       := p_QuotHistBuff.t_Scale;
    p_QuotDefBuff.t_Point       := p_QuotHistBuff.t_Point;
    p_QuotDefBuff.t_RoundAfter  := p_QuotHistBuff.t_RoundAfter;
    p_QuotDefBuff.t_IsDerived   := p_QuotHistBuff.t_IsDerived;
    p_QuotDefBuff.t_BaseRate    := p_QuotHistBuff.t_BaseRate;
    p_QuotDefBuff.t_RateExp     := p_QuotHistBuff.t_RateExp;
    p_QuotDefBuff.t_Ground      := p_QuotHistBuff.t_Ground;
    p_QuotDefBuff.t_RoundBefore := p_QuotHistBuff.t_RoundBefore;
    p_QuotDefBuff.t_SinceDate   := p_QuotHistBuff.t_SinceDate;
    p_QuotDefBuff.t_SinceTime   := p_QuotHistBuff.t_SinceTime;
    p_QuotDefBuff.t_InputDate   := p_QuotHistBuff.t_InputDate;
    p_QuotDefBuff.t_InputTime   := p_QuotHistBuff.t_InputTime;
    p_QuotDefBuff.t_Oper        := p_QuotHistBuff.t_Oper;
  END;

  -- Процедура заполнения буфера внешнего курса по данным из истории курса
  PROCEDURE FillRatedefByHist
  (
    p_RateHistBuff IN dratehist_dbt%ROWTYPE -- буфер истории внешнего курса
   ,p_RateDefBuff OUT dratedef_dbt%ROWTYPE -- буфер внешнего курса
  )
  IS
  BEGIN
    p_RateDefBuff.t_IsInverse := p_RateHistBuff.t_IsInverse;
    p_RateDefBuff.t_Rate      := p_RateHistBuff.t_Rate     ;
    p_RateDefBuff.t_Scale     := p_RateHistBuff.t_Scale    ;
    p_RateDefBuff.t_Point     := p_RateHistBuff.t_Point    ;
    p_RateDefBuff.t_InputDate := p_RateHistBuff.t_InputDate;
    p_RateDefBuff.t_InputTime := p_RateHistBuff.t_InputTime;
    p_RateDefBuff.t_Oper      := p_RateHistBuff.t_Oper     ;
    p_RateDefBuff.t_SinceDate := p_RateHistBuff.t_SinceDate;
  END;

  PROCEDURE SetNewQuotdefBuff
  (
    p_NewQuotdefBuff IN dquotdef_dbt%ROWTYPE
  )
  IS
  BEGIN
    m_NewQuotdefBuff := p_NewQuotdefBuff;
  END;

  PROCEDURE SetNewQuothistBuff
  (
    p_NewQuothistBuff IN dquothist_dbt%ROWTYPE
  )
  IS
  BEGIN
    m_NewQuothistBuff := p_NewQuothistBuff;
  END;

  PROCEDURE SetNewRatedefBuff
  (
    p_NewRatedefBuff IN dratedef_dbt%ROWTYPE
  )
  IS
  BEGIN
    m_NewRatedefBuff := p_NewRatedefBuff;
  END;

  PROCEDURE SetNewRatehistBuff
  (
    p_NewRatehistBuff IN dratehist_dbt%ROWTYPE
  )
  IS
  BEGIN
    m_NewRatehistBuff := p_NewRatehistBuff;
  END;

  -- Функция получения параметров внутреннего курса для подразделения с учетом истории
  FUNCTION RestoreQuotdefForDate
  (
    p_Branch IN NUMBER -- подразделение
   ,p_Type IN NUMBER -- тип внутреннего курса
   ,p_FromFI IN NUMBER -- валюта
   ,p_ToFI IN NUMBER -- валюта
   ,p_Date IN DATE -- дата, на которую действует курс
   ,p_Time IN DATE DEFAULT MAX_TIME
   ,p_QuotDefBuff OUT dquotdef_dbt%ROWTYPE -- буфер внутреннего курса
  )
  RETURN NUMBER
  IS
    m_stat NUMBER := 0;
    m_QuotHistBuff dquothist_dbt%ROWTYPE; -- буфер истории внутреннего курса
  BEGIN
    BEGIN
      SELECT * INTO p_QuotDefBuff
        FROM dquotdef_dbt
       WHERE t_Branch = p_Branch
         AND t_Type = p_Type
         AND t_BaseFIID = p_FromFI
         AND t_QuotedFIID = p_ToFI;
    EXCEPTION WHEN OTHERS THEN
      IF     m_NewQuotdefBuff.t_Branch = p_Branch
         AND m_NewQuotdefBuff.t_Type = p_Type
         AND m_NewQuotdefBuff.t_BaseFIID = p_FromFI
         AND m_NewQuotdefBuff.t_QuotedFIID = p_ToFI THEN
        p_QuotDefBuff := m_NewQuotdefBuff;
      ELSE
        m_stat := 4;
      END IF;
    END;

    IF m_stat = 0 THEN
      IF (p_QuotDefBuff.t_SinceDate > p_Date) OR (p_QuotDefBuff.t_SinceDate = p_Date AND p_QuotDefBuff.t_SinceTime > p_Time) THEN
        IF     m_NewQuothistBuff.t_RateID = p_QuotDefBuff.t_RateID
           AND m_NewQuothistBuff.t_SinceDate = p_Date AND m_NewQuothistBuff.t_SinceTime <= p_Time THEN
          m_QuotHistBuff := m_NewQuothistBuff;
        ELSE
          BEGIN
            SELECT * INTO m_QuotHistBuff
              FROM dquothist_dbt
             WHERE t_RateID = p_QuotDefBuff.t_RateID
               AND t_SinceDate <= p_Date
               AND t_SinceTime <= p_Time
               AND ROWNUM = 1
             ORDER BY t_SinceDate DESC, t_SinceTime DESC;
          EXCEPTION WHEN OTHERS THEN
            IF     m_NewQuothistBuff.t_RateID = p_QuotDefBuff.t_RateID
               AND m_NewQuothistBuff.t_SinceDate <= p_Date AND m_NewQuothistBuff.t_SinceTime <= p_Time  THEN
              m_QuotHistBuff := m_NewQuothistBuff;
            ELSE
              m_stat := 4;
            END IF;
          END;
        END IF;

        IF m_stat = 0 THEN
          FillQuotdefByHist
          (
            m_QuotHistBuff
           ,p_QuotDefBuff
          );
        END IF;
      END IF;
    END IF;

    RETURN m_stat;
  END;

  -- Функция получения параметров внешнего курса с учетом истории
  FUNCTION RestoreRatedefForDate
  (
    p_Type IN NUMBER -- тип внутреннего курса
   ,p_FromFI IN NUMBER -- валюта
   ,p_ToFI IN NUMBER -- валюта
   ,p_Date IN DATE -- дата, на которую действует курс
   ,p_RateDefBuff OUT dratedef_dbt%ROWTYPE -- буфер внешнего курса
  )
  RETURN NUMBER
  IS
    m_stat NUMBER := 0;
    m_RateHistBuff dratehist_dbt%ROWTYPE; -- буфер истории внешнего курса
  BEGIN
    BEGIN
      SELECT * INTO p_RateDefBuff
        FROM dratedef_dbt
       WHERE t_Type = p_Type
         AND t_FIID = p_FromFI
         AND t_OtherFI = p_ToFI;
    EXCEPTION WHEN OTHERS THEN
      IF     m_NewRatedefBuff.t_Type = p_Type
         AND m_NewRatedefBuff.t_FIID = p_FromFI
         AND m_NewRatedefBuff.t_OtherFI = p_ToFI THEN
        p_RateDefBuff := m_NewRatedefBuff;
      ELSE
        m_stat := 4;
      END IF;
    END;

    IF m_stat = 0 THEN
      IF p_RateDefBuff.t_SinceDate > p_Date THEN
        IF     m_NewRatehistBuff.t_RateID = p_RateDefBuff.t_RateID
           AND m_NewRatehistBuff.t_SinceDate = p_Date THEN
          m_RateHistBuff := m_NewRatehistBuff;
        ELSE
          BEGIN
            SELECT * INTO m_RateHistBuff
              FROM dratehist_dbt
             WHERE t_RateID = p_RateDefBuff.t_RateID
               AND t_SinceDate <= p_Date
               AND ROWNUM = 1
             ORDER BY t_SinceDate DESC;
          EXCEPTION WHEN OTHERS THEN
            IF     m_NewRatehistBuff.t_RateID = p_RateDefBuff.t_RateID
               AND m_NewRatehistBuff.t_SinceDate <= p_Date THEN
              m_RateHistBuff := m_NewRatehistBuff;
            ELSE
              m_stat := 4;
            END IF;
          END;
        END IF;

        IF m_stat = 0 THEN
          FillRatedefByHist
          (
            m_RateHistBuff
           ,p_RateDefBuff
          );
        END IF;
      END IF;
    END IF;

    RETURN m_stat;
  END;

  -- Функция получения параметров внутреннего курса для подразделения
  FUNCTION FindQuotdefForDate
  (
    p_Branch IN NUMBER -- подразделение
   ,p_Type IN NUMBER -- тип внутреннего курса
   ,p_FromFI IN NUMBER -- валюта
   ,p_ToFI IN NUMBER -- валюта
   ,p_Date IN DATE -- дата, на которую действует курс
   ,p_Time IN DATE DEFAULT MAX_TIME
   ,p_QuotDefBuff OUT dquotdef_dbt%ROWTYPE -- буфер внутреннего курса
  )
  RETURN NUMBER
  IS
    m_stat NUMBER := 0;
  BEGIN
    m_stat := RestoreQuotdefForDate
    (
      p_Branch
     ,p_Type
     ,p_FromFI
     ,p_ToFI
     ,p_Date
     ,p_Time
     ,p_QuotDefBuff
    );

    IF m_stat <> 0 THEN
      m_stat := RestoreQuotdefForDate
      (
        p_Branch
       ,p_Type
       ,p_ToFI
       ,p_FromFI
       ,p_Date
       ,p_Time
       ,p_QuotDefBuff
      );
    END IF;

    RETURN m_stat;
  END;

  -- Функция получения параметров внешнего курса
  FUNCTION FindRatedefForDate
  (
    p_Type IN NUMBER -- тип внешнего курса
   ,p_FromFI IN NUMBER -- валюта
   ,p_ToFI IN NUMBER -- валюта
   ,p_Date IN DATE -- дата, на которую действует курс
   ,p_RateDefBuff OUT dratedef_dbt%ROWTYPE -- буфер внешнего курса
  )
  RETURN NUMBER
  IS
    m_stat NUMBER := 0;
  BEGIN
    m_stat := RestoreRatedefForDate
    (
      p_Type
     ,p_FromFI
     ,p_ToFI
     ,p_Date
     ,p_RateDefBuff
    );

    IF m_stat <> 0 THEN
      m_stat := RestoreRatedefForDate
      (
        p_Type
       ,p_ToFI
       ,p_FromFI
       ,p_Date
       ,p_RateDefBuff
      );
    END IF;

    RETURN m_stat;
  END;

  -- Функция получения параметров внутреннего курса для терр.структуры
  FUNCTION GetDprtFindQuotdefForDate
  (
    p_Branch IN NUMBER -- подразделение
   ,p_Type IN NUMBER -- тип внутреннего курса
   ,p_FromFI IN NUMBER -- валюта
   ,p_ToFI IN NUMBER -- валюта
   ,p_Date IN DATE -- дата, на которую действует курс
   ,p_Time IN DATE DEFAULT MAX_TIME
   ,p_QuotDefBuff OUT dquotdef_dbt%ROWTYPE -- буфер внутреннего курса
  )
  RETURN NUMBER
  IS
    m_stat NUMBER := 4;
  BEGIN
    DECLARE CURSOR c_dp_dep IS
      SELECT *
        FROM ddp_dep_dbt
       START WITH t_Code = p_Branch
       CONNECT BY t_Code = PRIOR t_ParentCode;
    BEGIN
      FOR v_dp_dep IN c_dp_dep LOOP
        IF v_dp_dep.t_Status = 2 THEN
          m_stat := FindQuotdefForDate
          (
            v_dp_dep.t_Code
           ,p_Type
           ,p_FromFI
           ,p_ToFI
           ,p_Date
           ,p_Time
           ,p_QuotDefBuff
          );

          IF m_stat = 0 THEN
            RETURN m_stat;
          END IF;
        END IF;
      END LOOP;
    END;
    RETURN m_stat;
  END;

  -- Функция получения параметров фининструмента
  FUNCTION FindFininstr
  (
    p_FIID IN NUMBER -- валюта
   ,p_FininstrBuff OUT dfininstr_dbt%ROWTYPE -- буфер внешнего курса
  )
  RETURN NUMBER
  IS
    m_stat NUMBER := 0;
  BEGIN
    BEGIN
      SELECT * INTO p_FininstrBuff
        FROM dfininstr_dbt
       WHERE t_FIID = p_FIID;
    EXCEPTION WHEN OTHERS THEN m_stat := 4;
    END;

    RETURN m_stat;
  END;



  -- Функция конвертации суммы по типу курса (внешние и внутренние курсы)
  FUNCTION FI_CalcSumType
  (
    p_Sum IN NUMBER -- сумма для пересчета в другую валюту
   ,p_FromFI IN NUMBER -- валюта
   ,p_ToFI IN NUMBER -- валюта
   ,p_Type IN NUMBER -- тип курса
   ,p_Date IN DATE DEFAULT ZERO_DATE -- дата курса
   ,p_Time IN DATE DEFAULT MAX_TIME  -- время курса
   ,p_Branch IN NUMBER DEFAULT 0-- подразделение
   ,p_CalcSum OUT NUMBER -- сумма пересчитанная по курсу
   ,p_RateValType OUT NUMBER -- параметр курса пересчета: тип курса
   ,p_RateValRate OUT NUMBER -- значение курса пересчета
   ,p_RateValScale OUT NUMBER -- масштаб курса пересчета
   ,p_RateValPoint OUT NUMBER -- точность курса пересчета
   ,p_RateValIsInverse OUT CHAR -- признак обратного курса пересчета
  )
  RETURN NUMBER
  IS
    m_stat NUMBER := 0;
    m_IsInternalRatetype CHAR(1); -- признак внутреннего курса
    m_QuotDefBuff dquotdef_dbt%ROWTYPE; -- буфер данных внутреннего курса
    m_RateDefBuff dratedef_dbt%ROWTYPE; -- буфер внешнего курса
    m_FininstrBuff dfininstr_dbt%ROWTYPE; -- буфер фининструмента
    m_FV NUMBER := 0; -- значение
    m_FVs NUMBER := 1; -- шкала
    m_FVp NUMBER := 0; -- точность
    m_IsInv CHAR(1) := UNSET_CHAR; -- признак обратного курса
    m_Numerator NUMBER;
    m_Denominator NUMBER;
    m_Swap NUMBER;
    m_Reverse INTEGER := 0;
    m_OutRate CHAR(1);
    m_Date DATE;
    m_Branch NUMBER;
  BEGIN
    m_Date := CASE WHEN p_Date = ZERO_DATE THEN RsbSessionData.curdate ELSE p_Date END;
    m_Branch := CASE WHEN p_Branch = 0 THEN RsbSessionData.OperDprtNode ELSE p_Branch END;

    BEGIN
      SELECT t_IsInternal INTO m_IsInternalRatetype FROM dratetype_dbt WHERE t_Type = p_Type;
    EXCEPTION WHEN OTHERS THEN m_stat := ERR_RATETYPE_NOT_FOUND;
    END;

    IF m_stat = 0 THEN
      IF m_IsInternalRatetype = SET_CHAR THEN
        m_stat := GetDprtFindQuotdefForDate
        (
          m_Branch
         ,p_Type
         ,p_FromFI
         ,p_ToFI
         ,m_Date
         ,p_Time
         ,m_QuotDefBuff
        );

        IF m_stat = 0 THEN
          IF m_QuotDefBuff.t_IsDerived = SET_CHAR THEN
            m_stat := FindRatedefForDate
            (
              m_QuotDefBuff.t_BaseRate
             ,p_FromFI
             ,p_ToFI
             ,m_Date
             ,m_RateDefBuff
            );

            IF m_stat = 0 THEN
              IF m_RateDefBuff.t_IsRelative = SET_CHAR THEN
                m_stat := FindFininstr
                (
                  m_RateDefBuff.t_OtherFI
                 ,m_FininstrBuff
                );

                IF m_stat = 0 THEN
                  m_FV    := m_FininstrBuff.t_FaceValue;
                  m_FVp   := m_FininstrBuff.t_Point    ;
                  m_FVs   := m_FininstrBuff.t_Scale    ;
                  m_IsInv := m_FininstrBuff.t_IsInverse;
                END IF;
              END IF;

              IF m_stat = 0 THEN
                m_Reverse := m_Reverse + CASE WHEN m_RateDefBuff.t_OtherFI <> m_QuotDefBuff.t_BaseFIID THEN 1 ELSE 0 END;
                m_Reverse := m_Reverse + CASE WHEN m_RateDefBuff.t_IsInverse = SET_CHAR THEN 1 ELSE 0 END;
                m_Reverse := MOD(m_Reverse, 2);

                m_Numerator := m_RateDefBuff.t_Rate;
                m_Denominator := POWER(10, m_RateDefBuff.t_Point);

                IF m_QuotDefBuff.t_RoundBefore = SET_CHAR THEN
                  IF m_Reverse = 1 THEN
                    m_Numerator := POWER(10, m_QuotDefBuff.t_Point) * m_Denominator / m_Numerator;
                  ELSE
                    IF m_RateDefBuff.t_Point > m_QuotDefBuff.t_Point THEN
                      m_Numerator := m_Numerator / POWER(10, m_RateDefBuff.t_Point - m_QuotDefBuff.t_Point);
                    END IF;
                  END IF;
                  m_Denominator := POWER(10, m_QuotDefBuff.t_Point);
                  m_Numerator := ROUND(m_Numerator);
                ELSE
                  IF m_Reverse = 1 THEN
                    m_Swap := m_Denominator;
                    m_Denominator := m_Numerator;
                    m_Numerator := m_Swap;
                  END IF;
                END IF;

                IF    m_QuotDefBuff.t_RateExp = RATEEXP_MLT_PR THEN
                  m_Numerator := m_Numerator * m_QuotDefBuff.t_Rate;
                  m_Denominator := m_Denominator * 100;
                ELSIF m_QuotDefBuff.t_RateExp = RATEEXP_MLT_CF THEN
                  m_Numerator := m_Numerator * m_QuotDefBuff.t_Rate;
                ELSIF m_QuotDefBuff.t_RateExp = RATEEXP_PLS_PR THEN
                  m_Numerator := m_Numerator * (100 + m_QuotDefBuff.t_Rate);
                  m_Denominator := m_Denominator * 100;
                ELSIF m_QuotDefBuff.t_RateExp = RATEEXP_MNS_PR THEN
                  m_Numerator := m_Numerator * (100 - m_QuotDefBuff.t_Rate);
                  m_Denominator := m_Denominator * 100;
                ELSIF m_QuotDefBuff.t_RateExp = RATEEXP_PLS_VL THEN
                  m_Numerator := m_Numerator * m_QuotDefBuff.t_Scale;
                  m_Numerator := m_Numerator + m_QuotDefBuff.t_Rate * m_QuotDefBuff.t_Scale * m_Denominator;
                  m_Denominator := m_Denominator * m_QuotDefBuff.t_Scale;
                ELSIF m_QuotDefBuff.t_RateExp = RATEEXP_MNS_VL THEN
                  m_Numerator := m_Numerator * m_QuotDefBuff.t_Scale;
                  m_Numerator := m_Numerator - m_QuotDefBuff.t_Rate * m_QuotDefBuff.t_Scale * m_Denominator;
                  m_Denominator := m_Denominator * m_QuotDefBuff.t_Scale;
                ELSE
                  m_stat := 1;
                END IF;

                IF m_stat = 0 THEN
                  m_Numerator := m_Numerator * POWER(10, m_QuotDefBuff.t_Point) / m_Denominator;

                  IF m_QuotDefBuff.t_RoundAfter = SET_CHAR THEN
                    m_Numerator := ROUND(m_Numerator);
                  ELSE
                    m_Numerator := TRUNC(m_Numerator);
                  END IF;
                  m_OutRate := CASE WHEN m_QuotDefBuff.t_BaseFIID = p_FromFI THEN UNSET_CHAR ELSE SET_CHAR END;

                  p_CalcSum := ConvertSum
                  (
                    p_Sum
                   ,m_Numerator
                   ,m_RateDefBuff.t_Scale
                   ,m_QuotDefBuff.t_Point
                   ,m_OutRate
                   ,m_RateDefBuff.t_IsRelative
                   ,m_FV
                   ,m_FVs
                   ,m_FVp
                   ,m_IsInv
                  );

                  p_RateValType      := p_Type;
                  p_RateValRate      := m_Numerator;
                  p_RateValScale     := m_RateDefBuff.t_Scale;
                  p_RateValPoint     := m_QuotDefBuff.t_Point;
                  p_RateValIsInverse := m_OutRate;
                END IF;

              END IF;
            END IF;
          ELSE
            m_OutRate := CASE WHEN m_QuotDefBuff.t_BaseFIID = p_FromFI THEN UNSET_CHAR ELSE SET_CHAR END;

            p_CalcSum := ConvertSum
            (
              p_Sum
             ,m_QuotDefBuff.t_Rate
             ,m_QuotDefBuff.t_Scale
             ,0
             ,m_OutRate
             ,UNSET_CHAR
             ,0
             ,1
             ,0
             ,UNSET_CHAR
            );

            p_RateValType      := p_Type;
            p_RateValRate      := m_QuotDefBuff.t_Rate;
            p_RateValScale     := m_QuotDefBuff.t_Scale;
            p_RateValPoint     := 0;
            p_RateValIsInverse := m_OutRate;
          END IF;
        END IF;
      ELSE
        p_RateValType := -1;

        p_CalcSum := ConvSumType2
        (
          p_Sum
         ,p_FromFI
         ,p_ToFI
         ,p_Type
         ,m_Date
         ,2
         ,p_RateValType
         ,p_RateValRate
         ,p_RateValScale
         ,p_RateValPoint
         ,p_RateValIsInverse
        );
      END IF;
    END IF;

    RETURN m_stat;
  END;

  -- Функция конвертации суммы по типу курса (внешние и внутренние курсы)
  FUNCTION FI_CalcSumTypeEx
  (
    p_Sum IN NUMBER -- сумма для пересчета в другую валюту
   ,p_FromFI IN NUMBER -- валюта
   ,p_ToFI IN NUMBER -- валюта
   ,p_Type IN NUMBER -- тип курса
   ,p_Date IN DATE DEFAULT ZERO_DATE -- дата курса
   ,p_Time IN DATE DEFAULT MAX_TIME  -- время курса
  )
  RETURN NUMBER
  IS
    m_stat NUMBER := 0;

    m_CalcSum           NUMBER; -- сумма пересчитанная по курсу
    m_RateValType       NUMBER; -- параметр курса пересчета: тип курса
    m_RateValRate       NUMBER; -- значение курса пересчета
    m_RateValScale      NUMBER; -- масштаб курса пересчета
    m_RateValPoint      NUMBER; -- точность курса пересчета
    m_RateValIsInverse  CHAR(1); -- признак обратного курса пересчета

  BEGIN

    IF(p_FromFI = p_ToFI) THEN
      m_CalcSum := p_Sum;
    ELSE
      m_stat := FI_CalcSumType( p_Sum
                               ,p_FromFI
                               ,p_ToFI
                               ,p_Type
                               ,p_Date
                               ,p_Time
                               ,0
                               ,m_CalcSum
                               ,m_RateValType
                               ,m_RateValRate
                               ,m_RateValScale
                               ,m_RateValPoint
                               ,m_RateValIsInverse
                              );
    END IF;

    IF(m_stat = 0) THEN
      RETURN m_CalcSum;
    ELSE
      RETURN NULL;
    END IF;
  END;




  -- Функция определяет, есть ли купон с нулевой суммой или ставкой по бумаге, на дату
  FUNCTION FI_HasZeroCoupons( FIID IN NUMBER, CalcDate IN DATE ) RETURN NUMBER DETERMINISTIC
  IS
     v_RetVal  NUMBER := 0;
  BEGIN
     --Проверяем текущий купон на дату CalcDate
     SELECT count(1) into v_RetVal
       FROM dfiwarnts_dbt
      WHERE     T_IsPartial = chr(0)
            AND T_FIID      = FIID
            AND CalcDate between T_FirstDate AND T_DrawingDate
            AND T_INCOMERATE = 0 AND T_INCOMEVOLUME = 0;

     IF v_RetVal > 0 THEN
        v_RetVal := 1;
     END IF;

     RETURN v_RetVal;
  END; -- FI_HasZeroCoupons


  -- Функция получения параметров внешнего курса с учетом истории
  FUNCTION GetAnyRatedefForDate
  (
    p_Type IN NUMBER -- тип внутреннего курса
   ,p_FromFI IN NUMBER -- валюта
   ,p_ToFI IN NUMBER -- валюта
   ,p_Date IN DATE -- дата, на которую действует курс
   ,p_RateDefBuff OUT dratedef_dbt%ROWTYPE -- буфер внешнего курса
   ,p_IsInverse OUT CHAR -- признак обратного курса
  )
  RETURN NUMBER
  AS
    m_stat NUMBER := 0;
  BEGIN
    p_IsInverse := UNSET_CHAR;
    m_stat := RestoreRatedefForDate
    (
      p_Type
     ,p_ToFI
     ,p_FromFI
     ,p_Date
     ,p_RateDefBuff
    );

    IF m_stat != 0 THEN
      p_IsInverse := SET_CHAR;
      m_stat := RestoreRatedefForDate
      (
        p_Type
       ,p_FromFI
       ,p_ToFI
       ,p_Date
       ,p_RateDefBuff
      );
    END IF;

    RETURN m_stat;
  END;

  FUNCTION ConvertSumCross
  (
    p_Sum IN NUMBER              -- Исходная сумма
   ,p_F_Rate IN NUMBER           -- Курс
   ,p_F_Scale IN NUMBER          -- Масштаб
   ,p_F_Point IN NUMBER          -- Округление
   ,p_F_OutRate IN CHAR          -- Признак обратной котировки
   ,p_F_IsRelative IN CHAR       -- Признак относительной котировки
   ,p_F_FaceValue IN NUMBER      -- Номинал (относительно чего задается курс, если он относительный)
   ,p_T_Rate IN NUMBER           -- Курс
   ,p_T_Scale IN NUMBER          -- Масштаб
   ,p_T_Point IN NUMBER          -- Округление
   ,p_T_OutRate IN CHAR          -- Признак обратной котировки
   ,p_T_IsRelative IN CHAR       -- Признак относительной котировки
   ,p_T_FaceValue IN NUMBER      -- Номинал (относительно чего задается курс, если он относительный)
  )
  RETURN NUMBER
  IS
    m_stat NUMBER := 0;
    v_F_Numerator NUMBER;
    v_F_Denominator NUMBER;
    v_T_Numerator NUMBER;
    v_T_Denominator NUMBER;
    v_Sum NUMBER;
  BEGIN
    m_stat := PrepareRate
    (
      p_F_Rate
     ,p_F_Scale
     ,p_F_Point
     ,CASE WHEN p_F_OutRate = SET_CHAR THEN TRUE ELSE FALSE END
     ,CASE WHEN p_F_IsRelative = SET_CHAR THEN TRUE ELSE FALSE END
     ,p_F_FaceValue
     ,1
     ,0
     ,FALSE
     ,v_F_Numerator
     ,v_F_Denominator
    );
    m_stat := 0;
    IF m_stat = 0 THEN
      m_stat := PrepareRate
      (
        p_T_Rate
       ,p_T_Scale
       ,p_T_Point
       ,CASE WHEN p_T_OutRate = SET_CHAR THEN TRUE ELSE FALSE END
       ,CASE WHEN p_T_IsRelative = SET_CHAR THEN TRUE ELSE FALSE END
       ,p_T_FaceValue
       ,1
       ,0
       ,FALSE
       ,v_T_Numerator
       ,v_T_Denominator
      );
      m_stat := 0;
    END IF;

    IF v_F_Denominator = 0 OR v_T_Numerator = 0 THEN
      v_Sum := 0;
    ELSE
      v_Sum := (p_Sum * v_F_Numerator * v_T_Denominator) / (v_F_Denominator * v_T_Numerator);
    END IF;

    RETURN v_Sum;
  END;

  FUNCTION CreateErrFinInstr
  (
    p_FIID IN NUMBER
   ,p_OtherFI IN NUMBER
   ,p_SinceDate IN DATE
   ,p_Type IN NUMBER
  )
  RETURN NUMBER
  IS
    m_stat NUMBER := 4;
    v_SinceDate DATE;
    v_TypeName VARCHAR2(100);
    v_FIID_Ccy VARCHAR2(100);
    v_OtherFI_Ccy VARCHAR2(100);
    --v_ErrMsg VARCHAR2(2047);
  BEGIN
    v_SinceDate := CASE WHEN p_SinceDate IS NULL THEN RsbSessionData.curdate ELSE p_SinceDate END;

    BEGIN
      SELECT t_TypeName INTO v_TypeName FROM dratetype_dbt WHERE t_Type = p_Type;
    EXCEPTION WHEN OTHERS THEN v_TypeName := TO_CHAR(p_Type);
    END;

    BEGIN
      SELECT t_Ccy INTO v_FIID_Ccy FROM dfininstr_dbt WHERE t_FIID = p_FIID;
    EXCEPTION WHEN OTHERS THEN NULL;
    END;

    BEGIN
      SELECT t_Ccy INTO v_OtherFI_Ccy FROM dfininstr_dbt WHERE t_FIID = p_OtherFI;
    EXCEPTION WHEN OTHERS THEN NULL;
    END;

    IF v_FIID_Ccy IS NULL OR v_OtherFI_Ccy IS NULL THEN
      IF p_Type != 0 THEN
        --1489 Не найден курс на дату %02d.%02d.%02d с видом "%s"
        m_stat := 1489;
        rsi_errors.CreateErrMsg(m_stat, '%02d.%02d.%02d', TO_CHAR(v_SinceDate, 'DD.MM.YY'), '%s', v_TypeName);
      ELSE
        --1499 Не найден основной курс на дату %02d.%02d.%02d
        m_stat := 1499;
        rsi_errors.CreateErrMsg(m_stat, '%02d.%02d.%02d', TO_CHAR(v_SinceDate, 'DD.MM.YY'));
      END IF;
    ELSE
      IF p_Type != 0 THEN
        --1488 Не найден курс %s/%s на дату %02d.%02d.%02d с видом "%s"
        m_stat := 1488;
        rsi_errors.CreateErrMsg(m_stat, '%s', v_FIID_Ccy, '%s', v_OtherFI_Ccy, '%02d.%02d.%02d', TO_CHAR(v_SinceDate, 'DD.MM.YY'), '%s', v_TypeName);
      ELSE
        --521 Отсутствует основной курс %s/%s на дату %02d.%02d.%02d
        m_stat := 521;
        rsi_errors.CreateErrMsg(m_stat, '%s', v_FIID_Ccy, '%s', v_OtherFI_Ccy, '%02d.%02d.%02d', TO_CHAR(v_SinceDate, 'DD.MM.YY'));
      END IF;
    END IF;

    RETURN m_stat;
  END;

  -- Функция конвертации суммы обязательства/эквивалента по типу курса
  FUNCTION FI_GetRateCalcSumEqv
  (
    p_Code_Currency IN NUMBER -- Валюта обязательства
   ,p_CurrencyEq IN NUMBER -- Валюта-эквивалент
   ,p_CurrencyEq_RateDate IN NUMBER -- Смещение даты курса валюты-эквивалента в меньшую сторону от даты проводки для счетов НВПИ
   ,p_CurrencyEq_RateType IN NUMBER -- Вид курса валюты-эквивалента
   ,p_CurrencyEq_RateExtra IN NUMBER -- Наценка на курс валюты-эквивалента
   ,p_Date IN DATE DEFAULT ZERO_DATE -- Дата
   ,p_Direct IN NUMBER -- Направление конвертации: = 0 - из p_CurrencyEq в p_Code_Currency, != 0 - наоборот.
   ,p_Sum IN NUMBER -- Сумма для пересчета в другую валюту
   ,p_CalcSum OUT NUMBER -- Сумма пересчитанная по курсу
   ,p_Rate OUT NUMBER -- Курс пересчета
  )
  RETURN NUMBER
  IS
    m_stat NUMBER := 0;
    v_RateDate DATE;
    v_FromFI NUMBER;
    v_ToFI NUMBER;
    v_rd dratedef_dbt%ROWTYPE;
    v_Rate NUMBER := 10000;
    v_Scale NUMBER := 1;
    v_Point NUMBER := 4;
    v_IsInverse CHAR := UNSET_CHAR;
    v_Numerator NUMBER;
    v_Denominator NUMBER;
    v_BRate NUMBER := 10000;
    v_BScale NUMBER := 1;
    v_BPoint NUMBER := 4;
    v_BIsInverse CHAR := UNSET_CHAR;
    v_QRate NUMBER := 10000;
    v_QScale NUMBER := 1;
    v_QPoint NUMBER := 4;
    v_QIsInverse CHAR := UNSET_CHAR;
  BEGIN

    p_CalcSum := 0;
    p_Rate := 0;

    v_RateDate := p_Date - p_CurrencyEq_RateDate;
    v_FromFI := CASE WHEN p_Direct != 0 THEN p_Code_Currency ELSE p_CurrencyEq    END;
    v_ToFI   := CASE WHEN p_Direct != 0 THEN p_CurrencyEq    ELSE p_Code_Currency END;

    IF (v_FromFI = NATCUR OR v_ToFI = NATCUR) AND v_FromFI != v_ToFI THEN
      -- если одна из валют национальная, ищем по типу курса
      IF p_CurrencyEq_RateType != 0 THEN

        m_stat := GetAnyRatedefForDate
        (
          p_CurrencyEq_RateType
         ,v_FromFI
         ,v_ToFI
         ,v_RateDate
         ,v_rd
         ,v_IsInverse
        );
        IF m_stat != 0 THEN
          m_stat := CreateErrFinInstr
          (
            v_FromFI
           ,v_ToFI
           ,v_RateDate
           ,p_CurrencyEq_RateType
          );
        ELSE --m_stat != 0

          IF v_rd.t_IsInverse = SET_CHAR THEN
            -- Если и так обратная котировка, то еще раз перевернуть
            v_IsInverse := CASE WHEN v_IsInverse = SET_CHAR THEN UNSET_CHAR ELSE SET_CHAR END;
          END IF;

          m_stat := PrepareRate
          (
            v_rd.t_Rate
           ,v_rd.t_Scale
           ,v_rd.t_Point
           ,CASE WHEN v_IsInverse = SET_CHAR THEN TRUE ELSE FALSE END
           ,FALSE
           ,0
           ,1
           ,0
           ,CASE WHEN v_rd.t_IsInverse = SET_CHAR THEN TRUE ELSE FALSE END
           ,v_Numerator
           ,v_Denominator
          );
          m_stat := 0;

          IF v_Denominator = 0 THEN
            v_Rate := 0;
          ELSE
            v_Rate := v_Numerator / v_Denominator;
          END IF;

          IF m_stat = 0 THEN
            p_CalcSum := ConvertSum
            (
              p_Sum
             ,v_rd.t_Rate
             ,v_rd.t_Scale
             ,v_rd.t_Point
             ,v_IsInverse
             ,UNSET_CHAR
             ,1
             ,0
             ,1
             ,UNSET_CHAR
            );
          END IF;

        END IF; --m_stat != 0

      ELSE --CurrencyEq_RateType != 0
      -- если не задан тип, ищем по основному курсу
        m_stat := FI_GetDominantRate
        (
          v_ToFI       -- котируемый ФИ
         ,v_FromFI     -- базовый ФИ
         ,v_Rate       -- курс
         ,v_Scale      -- масштаб
         ,v_Point      -- точность
         ,v_IsInverse  -- признак обратной котировки
         ,v_RateDate   -- дата курса
        );
        IF m_stat != 0 THEN
          m_stat := CreateErrFinInstr
          (
            v_FromFI
           ,v_ToFI
           ,v_RateDate
           ,0
          );
        END IF;

        IF m_stat = 0 THEN
          p_CalcSum := ConvertSum
          (
            p_Sum
           ,v_Rate
           ,v_Scale
           ,v_Point
           ,v_IsInverse
           ,UNSET_CHAR
           ,1
           ,0
           ,1
           ,UNSET_CHAR
          );

          m_stat := PrepareRate
          (
            v_Rate
           ,v_Scale
           ,v_Point
           ,CASE WHEN v_IsInverse = SET_CHAR THEN TRUE ELSE FALSE END
           ,FALSE
           ,0
           ,1
           ,0
           ,FALSE
           ,v_Numerator
           ,v_Denominator
          );
          m_stat := 0;

          IF v_Denominator = 0 THEN
            v_Rate := 0;
          ELSE
            v_Rate := v_Numerator / v_Denominator;
          END IF;

        END IF; --m_stat != 0;

      END IF; --CurrencyEq_RateType != 0
    ELSIF v_FromFI != NATCUR AND v_ToFI != NATCUR THEN
      IF p_CurrencyEq_RateType != 0 THEN
        IF p_Direct != 0 THEN
          m_stat := FI_GetDominantRate
          (
            NATCUR       -- котируемый ФИ
           ,v_FromFI     -- базовый ФИ
           ,v_BRate      -- курс
           ,v_BScale     -- масштаб
           ,v_BPoint     -- точность
           ,v_BIsInverse -- признак обратной котировки
           ,p_Date       -- дата курса
          );
          IF m_stat != 0 THEN
            m_stat := CreateErrFinInstr
            (
              v_FromFI
             ,NATCUR
             ,p_Date
             ,0
            );
          END IF;
          IF m_stat = 0 THEN
            m_stat := GetAnyRatedefForDate
            (
              p_CurrencyEq_RateType
             ,v_ToFI
             ,NATCUR
             ,v_RateDate
             ,v_rd
             ,v_QIsInverse
            );
            IF m_stat != 0 THEN
              m_stat := CreateErrFinInstr
              (
                v_ToFI
               ,NATCUR
               ,v_RateDate
               ,p_CurrencyEq_RateType
              );
            END IF;
          END IF;
          IF m_stat = 0 THEN
            IF v_rd.t_IsInverse = SET_CHAR OR v_QIsInverse = SET_CHAR THEN
              IF v_rd.t_IsInverse = SET_CHAR THEN
                -- Если и так обратная котировка, то еще раз перевернуть
                v_QIsInverse := CASE WHEN v_QIsInverse = SET_CHAR THEN UNSET_CHAR ELSE SET_CHAR END;
              END IF;
            END IF;
            v_QRate := v_rd.t_Rate;
            v_QScale := v_rd.t_Scale;
            v_QPoint := v_rd.t_Point;
          END IF;
        ELSE --p_Direct != 0
          m_stat := FI_GetDominantRate
          (
            NATCUR       -- котируемый ФИ
           ,v_ToFI       -- базовый ФИ
           ,v_QRate      -- курс
           ,v_QScale     -- масштаб
           ,v_QPoint     -- точность
           ,v_QIsInverse -- признак обратной котировки
           ,p_Date       -- дата курса
          );
          IF m_stat != 0 THEN
            m_stat := CreateErrFinInstr
            (
              v_ToFI
             ,NATCUR
             ,p_Date
             ,0
            );
          END IF;
          IF m_stat = 0 THEN
            m_stat := GetAnyRatedefForDate
            (
              p_CurrencyEq_RateType
             ,v_FromFI
             ,NATCUR
             ,v_RateDate
             ,v_rd
             ,v_BIsInverse
            );
            IF m_stat != 0 THEN
              m_stat := CreateErrFinInstr
              (
                v_FromFI
               ,NATCUR
               ,v_RateDate
               ,p_CurrencyEq_RateType
              );
            END IF;
          END IF;
          IF m_stat = 0 THEN
            IF v_rd.t_IsInverse = SET_CHAR OR v_BIsInverse = SET_CHAR THEN
              IF v_rd.t_IsInverse = SET_CHAR THEN
                -- Если и так обратная котировка, то еще раз перевернуть
                v_BIsInverse := CASE WHEN v_BIsInverse = SET_CHAR THEN UNSET_CHAR ELSE SET_CHAR END;
              END IF;
            END IF;
            v_BRate := v_rd.t_Rate;
            v_BScale := v_rd.t_Scale;
            v_BPoint := v_rd.t_Point;
          END IF;
        END IF; --p_Direct != 0

        IF m_stat = 0 THEN
          FI_DetermineRate
          (
            v_BRate --числитель (курс между базовой и кроссируемой валютой)
           ,v_BScale
           ,v_BPoint
           ,v_BIsInverse
           ,v_QRate --знаменатель (курс между котируемой и кроссируемой валютой)
           ,v_QScale
           ,v_QPoint
           ,v_QIsInverse
           ,v_Rate --возвращаемый курс
           ,v_Scale
           ,v_Point
           ,v_IsInverse
          );
        END IF;
        IF m_stat = 0 THEN
          p_CalcSum := ConvertSumCross
          (
            p_Sum
           ,v_BRate           -- Курс
           ,v_BScale          -- Масштаб
           ,v_BPoint          -- Округление
           ,v_BIsInverse      -- Признак обратной котировки
           ,UNSET_CHAR        -- Признак относительной котировки
           ,1                 -- Номинал (относительно чего задается курс, если он относительный)
           ,v_QRate           -- Курс
           ,v_QScale          -- Масштаб
           ,v_QPoint          -- Округление
           ,v_QIsInverse      -- Признак обратной котировки
           ,UNSET_CHAR        -- Признак относительной котировки
           ,1                 -- Номинал (относительно чего задается курс, если он относительный)
          );
        END IF;
      ELSE --CurrencyEq_RateType != 0
        IF v_FromFI = v_ToFI THEN
          p_CalcSum := p_Sum;
        ELSE
          v_RateDate := CASE WHEN p_Direct = 0 THEN p_Date - p_CurrencyEq_RateDate ELSE p_Date END;
          m_stat := FI_GetDominantRate
          (
            NATCUR       -- котируемый ФИ
           ,v_FromFI     -- базовый ФИ
           ,v_BRate      -- курс
           ,v_BScale     -- масштаб
           ,v_BPoint     -- точность
           ,v_BIsInverse -- признак обратной котировки
           ,v_RateDate   -- дата курса
          );
          IF m_stat != 0 THEN
            m_stat := CreateErrFinInstr
            (
              v_FromFI
             ,NATCUR
             ,v_RateDate
             ,0
            );
          END IF;

          v_RateDate := CASE WHEN p_Direct = 0 THEN p_Date ELSE p_Date - p_CurrencyEq_RateDate END;
          m_stat := FI_GetDominantRate
          (
            NATCUR       -- котируемый ФИ
           ,v_ToFI       -- базовый ФИ
           ,v_QRate      -- курс
           ,v_QScale     -- масштаб
           ,v_QPoint     -- точность
           ,v_QIsInverse -- признак обратной котировки
           ,p_Date       -- дата курса
          );
          IF m_stat != 0 THEN
            m_stat := CreateErrFinInstr
            (
              v_ToFI
             ,NATCUR
             ,p_Date
             ,0
            );
          END IF;

          IF m_stat = 0 THEN
            FI_DetermineRate
            (
              v_BRate --числитель (курс между базовой и кроссируемой валютой)
             ,v_BScale
             ,v_BPoint
             ,v_BIsInverse
             ,v_QRate --знаменатель (курс между котируемой и кроссируемой валютой)
             ,v_QScale
             ,v_QPoint
             ,v_QIsInverse
             ,v_Rate --возвращаемый курс
             ,v_Scale
             ,v_Point
             ,v_IsInverse
            );
          END IF;
          IF m_stat = 0 THEN
            p_CalcSum := ConvertSumCross
            (
              p_Sum
             ,v_BRate           -- Курс
             ,v_BScale          -- Масштаб
             ,v_BPoint          -- Округление
             ,v_BIsInverse      -- Признак обратной котировки
             ,UNSET_CHAR        -- Признак относительной котировки
             ,1                 -- Номинал (относительно чего задается курс, если он относительный)
             ,v_QRate           -- Курс
             ,v_QScale          -- Масштаб
             ,v_QPoint          -- Округление
             ,v_QIsInverse      -- Признак обратной котировки
             ,UNSET_CHAR        -- Признак относительной котировки
             ,1                 -- Номинал (относительно чего задается курс, если он относительный)
            );
          END IF;
        END IF;
      END IF; --CurrencyEq_RateType != 0
    END IF; --(v_FromFI = NATCUR OR v_ToFI = NATCUR) AND v_FromFI != v_ToFI

    IF m_stat = 0 THEN
      IF p_Direct = 0 THEN
        p_Rate := v_Rate / (1 + p_CurrencyEq_RateExtra / 100);
        p_CalcSum := p_CalcSum * (1 + p_CurrencyEq_RateExtra / 100);
      ELSE
        p_Rate := v_Rate * (1 + p_CurrencyEq_RateExtra / 100);
        p_CalcSum := p_CalcSum / (1 + p_CurrencyEq_RateExtra / 100);
      END IF;
    END IF;

    RETURN m_stat;
  END; -- FI_GetRateCalcSumEqv


  -- Определение суммы в ВО
  FUNCTION FI_CalcSumAccountFromEqv
  (
    p_Code_Currency IN NUMBER -- Валюта обязательства
   ,p_CurrencyEq IN NUMBER -- Валюта-эквивалент
   ,p_CurrencyEq_RateDate IN NUMBER -- Смещение даты курса валюты-эквивалента в меньшую сторону от даты проводки для счетов НВПИ
   ,p_CurrencyEq_RateType IN NUMBER -- Вид курса валюты-эквивалента
   ,p_CurrencyEq_RateExtra IN NUMBER -- Наценка на курс валюты-эквивалента
   ,p_Date IN DATE DEFAULT ZERO_DATE -- Дата
   ,p_Sum IN NUMBER -- Сумма для пересчета в другую валюту
   ,p_CalcSum OUT NUMBER -- Сумма пересчитанная по курсу
  )
  RETURN NUMBER
  IS
    m_stat NUMBER := 0;
    v_Rate NUMBER;
  BEGIN
    m_stat := FI_GetRateCalcSumEqv
    (
      p_Code_Currency
     ,p_CurrencyEq
     ,p_CurrencyEq_RateDate
     ,p_CurrencyEq_RateType
     ,p_CurrencyEq_RateExtra
     ,p_Date
     ,0
     ,p_Sum
     ,p_CalcSum
     ,v_Rate
    );
    RETURN m_stat;
  END;

  -- Определение суммы в ВЭ
  FUNCTION FI_CalcSumEqvFromAccount
  (
    p_Code_Currency IN NUMBER -- Валюта обязательства
   ,p_CurrencyEq IN NUMBER -- Валюта-эквивалент
   ,p_CurrencyEq_RateDate IN NUMBER -- Смещение даты курса валюты-эквивалента в меньшую сторону от даты проводки для счетов НВПИ
   ,p_CurrencyEq_RateType IN NUMBER -- Вид курса валюты-эквивалента
   ,p_CurrencyEq_RateExtra IN NUMBER -- Наценка на курс валюты-эквивалента
   ,p_Date IN DATE DEFAULT ZERO_DATE -- Дата
   ,p_Sum IN NUMBER -- Сумма для пересчета в другую валюту
   ,p_CalcSum OUT NUMBER -- Сумма пересчитанная по курсу
  )
  RETURN NUMBER
  IS
    m_stat NUMBER := 0;
    v_Rate NUMBER;
  BEGIN
    m_stat := FI_GetRateCalcSumEqv
    (
      p_Code_Currency
     ,p_CurrencyEq
     ,p_CurrencyEq_RateDate
     ,p_CurrencyEq_RateType
     ,p_CurrencyEq_RateExtra
     ,p_Date
     ,1
     ,p_Sum
     ,p_CalcSum
     ,v_Rate
    );
    RETURN m_stat;
  END;

  -- Получить основной курс на дату
  FUNCTION GetDominantRateOnDate
  (
    p_QuotFI IN NUMBER       -- котируемый ФИ
   ,p_BaseFI IN NUMBER       -- базовый ФИ
   ,p_Rate OUT NUMBER        -- курс
   ,p_Scale OUT NUMBER       -- масштаб
   ,p_Point OUT NUMBER       -- точность
   ,p_IsInverse OUT CHAR     -- признак обратной котировки
   ,p_Date IN DATE           -- дата курса
  )
  RETURN NUMBER
  IS
    m_stat NUMBER := 0;
    v_revflag CHAR := UNSET_CHAR;
    v_rd dratedef_dbt%ROWTYPE;
  BEGIN
    m_stat := GetDominantRate
    (
      p_QuotFI
     ,p_BaseFI
     ,v_rd
    ); -- В случае успеха функция возвращает 1
    IF m_stat = 1 THEN
      IF v_rd.t_FIID != p_QuotFI AND v_rd.t_OtherFI != p_BaseFI THEN
        v_revflag := SET_CHAR;
      END IF;
      m_stat := 0;
    ELSE
      m_stat := 4;
    END IF;

    IF m_stat = 0 THEN
      m_stat := RestoreRatedefForDate
      (
        v_rd.t_Type
       ,v_rd.t_FIID
       ,v_rd.t_OtherFI
       ,p_Date
       ,v_rd
      );

      IF m_stat = 0 THEN
        p_Rate := v_rd.t_Rate;
        p_Scale := v_rd.t_Scale;
        p_Point := v_rd.t_Point;
        p_IsInverse := v_rd.t_IsInverse;
        IF v_revflag = SET_CHAR THEN
          p_IsInverse := CASE WHEN p_IsInverse = SET_CHAR THEN UNSET_CHAR ELSE SET_CHAR END;
        END IF;
      END IF;
    END IF;

    RETURN m_stat;
  END;

  FUNCTION GetCrossCurrency
  RETURN NUMBER
  IS
    v_CrossCurrency_Code VARCHAR2(100);
  BEGIN
    IF g_CrossCurrency IS NULL THEN
      g_CrossCurrency := -1;
      v_CrossCurrency_Code := rsb_common.GetRegStrValue('CB\FINTOOLS\CURRCALC\CROSSCURRENCY', 0);
      IF v_CrossCurrency_Code <> CHR(1) THEN
        BEGIN
          SELECT t_FIID INTO g_CrossCurrency
            FROM dfininstr_dbt
           WHERE t_FI_Code = v_CrossCurrency_Code;
        EXCEPTION WHEN OTHERS THEN NULL;
        END;
      END IF;
    END IF;
    RETURN g_CrossCurrency;
  END;

  -- Возвращает основной курс между валютами
  FUNCTION FI_GetDominantRate
  (
    p_QuotFI IN NUMBER       -- котируемый ФИ
   ,p_BaseFI IN NUMBER       -- базовый ФИ
   ,p_Rate OUT NUMBER        -- курс
   ,p_Scale OUT NUMBER       -- масштаб
   ,p_Point OUT NUMBER       -- точность
   ,p_IsInverse OUT CHAR     -- признак обратной котировки
   ,p_Date IN DATE           -- дата курса
  )
  RETURN NUMBER
  IS
    m_stat NUMBER := 0;
    v_CrsFIID NUMBER;
    v_BRate NUMBER := 10000;
    v_BScale NUMBER := 1;
    v_BPoint NUMBER := 4;
    v_BIsInverse CHAR := UNSET_CHAR;
    v_QRate NUMBER := 10000;
    v_QScale NUMBER := 1;
    v_QPoint NUMBER := 4;
    v_QIsInverse CHAR := UNSET_CHAR;
    v_RateDate DATE;
  BEGIN
    p_Rate := 10000;
    p_Scale := 1;
    p_Point := 4;
    p_IsInverse := UNSET_CHAR;
    -- Найти прямой основной курс за дату
    m_stat := GetDominantRateOnDate
    (
      p_QuotFI
     ,p_BaseFI
     ,p_Rate
     ,p_Scale
     ,p_Point
     ,p_IsInverse
     ,p_Date
    );
    -- Если не найден прямой, то найти обратный основной курс за дату
    IF m_stat != 0 THEN
      m_stat := GetDominantRateOnDate
      (
        p_BaseFI
       ,p_QuotFI
       ,p_Rate
       ,p_Scale
       ,p_Point
       ,p_IsInverse
       ,p_Date
      );
      IF m_stat = 0 THEN
        p_IsInverse := CASE WHEN p_IsInverse = SET_CHAR THEN UNSET_CHAR ELSE SET_CHAR END;
      END IF;
    END IF;
    -- Если не найден прямой и обратный основной курс за дату
    IF m_stat != 0 THEN
      m_stat := 0;

      v_CrsFIID := GetCrossCurrency;
      IF v_CrsFIID < 0 THEN
        m_stat := 4;
      END IF;

      IF m_stat != 0 THEN
        -- 20350 Не задан курс валюты по 6-Т к национальной валюте
        m_stat := 20350;
        rsi_errors.CreateErrMsg(m_stat);
      END IF;

      IF v_CrsFIID != p_BaseFI AND v_CrsFIID != p_QuotFI THEN
        IF m_stat = 0 THEN
          v_RateDate := CASE WHEN p_BaseFI = NATCUR THEN p_Date - 1 ELSE p_Date END;
          -- Найти прямой основной курс за дату
          m_stat := GetDominantRateOnDate
          (
            v_CrsFIID
           ,p_BaseFI
           ,v_BRate
           ,v_BScale
           ,v_BPoint
           ,v_BIsInverse
           ,v_RateDate
          );
          -- Если не найден прямой, то найти обратный основной курс за дату
          IF m_stat != 0 THEN
            m_stat := GetDominantRateOnDate
            (
              p_BaseFI
             ,v_CrsFIID
             ,v_BRate
             ,v_BScale
             ,v_BPoint
             ,v_BIsInverse
             ,v_RateDate
            );
            IF m_stat = 0 THEN
              v_BIsInverse := CASE WHEN v_BIsInverse = SET_CHAR THEN UNSET_CHAR ELSE SET_CHAR END;
            END IF;
          END IF;
          --Если ничего не нашли, то ошибка
          IF m_stat != 0 THEN
            IF p_BaseFI = NATCUR THEN
              -- 20350 Не задан курс валюты по 6-Т к национальной валюте
              m_stat := 20350;
              rsi_errors.CreateErrMsg(m_stat);
            ELSE
              m_stat := CreateErrFinInstr
              (
                p_BaseFI
               ,v_CrsFIID
               ,v_RateDate
               ,0
              );
            END IF;
          END IF;
        END IF;

        IF m_stat = 0 THEN
          v_RateDate := CASE WHEN p_QuotFI = NATCUR THEN p_Date - 1 ELSE p_Date END;
          -- Найти прямой основной курс за дату
          m_stat := GetDominantRateOnDate
          (
            v_CrsFIID
           ,p_QuotFI
           ,v_QRate
           ,v_QScale
           ,v_QPoint
           ,v_QIsInverse
           ,v_RateDate
          );
          -- Если не найден прямой, то найти обратный основной курс за дату
          IF m_stat != 0 THEN
            m_stat := GetDominantRateOnDate
            (
              p_QuotFI
             ,v_CrsFIID
             ,v_QRate
             ,v_QScale
             ,v_QPoint
             ,v_QIsInverse
             ,v_RateDate
            );
            IF m_stat = 0 THEN
              v_QIsInverse := CASE WHEN v_QIsInverse = SET_CHAR THEN UNSET_CHAR ELSE SET_CHAR END;
            END IF;
          END IF;
          --Если ничего не нашли, то ошибка
          IF m_stat != 0 THEN
            IF p_QuotFI = NATCUR THEN
              -- 20350 Не задан курс валюты по 6-Т к национальной валюте
              m_stat := 20350;
              rsi_errors.CreateErrMsg(m_stat);
            ELSE
              m_stat := CreateErrFinInstr
              (
                p_QuotFI
               ,v_CrsFIID
               ,v_RateDate
               ,0
              );
            END IF;
          END IF;
        END IF;

        IF m_stat = 0 THEN
          FI_DetermineRate
          (
            v_BRate --числитель (курс между базовой и кроссируемой валютой)
           ,v_BScale
           ,v_BPoint
           ,v_BIsInverse
           ,v_QRate --знаменатель (курс между котируемой и кроссируемой валютой)
           ,v_QScale
           ,v_QPoint
           ,v_QIsInverse
           ,p_Rate --возвращаемый курс
           ,p_Scale
           ,p_Point
           ,p_IsInverse
          );
        END IF;
      END IF;

    END IF;

    RETURN m_stat;
  END;

  FUNCTION FI_IsQuoted (p_FIID IN NUMBER, p_OnDate IN DATE)
     RETURN NUMBER
     DETERMINISTIC
  IS
     m_FIID     davoiriss_dbt.t_FIID%TYPE;
     m_OnDate   DATE;
     m_AttrID   dobjatcor_dbt.t_AttrID%TYPE;
  BEGIN
     IF NOT p_FIID > -1
     THEN
        RETURN 0;
     END IF;

     -- Проверяем, является ли финансовый инструмент ценной бумагой.
     -- Если нет, то он котируемый.
     -- Если передали в качестве параметра буфер бумаги, то искать нет смысла.

     BEGIN
        SELECT avr.t_FIID
          INTO m_FIID
          FROM davoiriss_dbt avr
         WHERE avr.t_FIID = p_FIID;
     EXCEPTION
        WHEN NO_DATA_FOUND
        THEN
           RETURN 1;
        WHEN OTHERS
        THEN
           RETURN 0;
     END;

     -- Финансовый инструмент является ценной бумагой.
     -- Получаем значение категории.

     m_OnDate :=
        CASE
           WHEN p_OnDate = ZERO_DATE THEN RsbSessionData.curdate
           ELSE p_OnDate
        END;

     BEGIN
        SELECT ac.t_AttrID
          INTO m_AttrID
          FROM dfininstr_dbt fi, dobjatcor_dbt ac
         WHERE     fi.t_FIID = m_FIID
               AND ac.t_ObjectType = 12                      -- OBJTYPE_AVOIRISS
               AND ac.t_GroupID = 18                          -- OBJGROUP_QUOTED
               AND ac.t_Object = LPAD (fi.t_FIID, 10, '0')
               AND ac.t_ValidFromDate =
                      (SELECT MAX (t.T_ValidFromDate)
                         FROM dobjatcor_dbt t
                        WHERE     t.t_ObjectType = ac.t_ObjectType
                              AND t.t_GroupID = ac.t_GroupID
                              AND t.t_Object = ac.t_Object
                              AND t.t_ValidFromDate <= m_OnDate);
     EXCEPTION
        WHEN OTHERS
        THEN
           RETURN 0;
     END;

     IF m_AttrID = 1                                       -- OBJATTR_QUOTED_YES
     THEN
        RETURN 1;
     END IF;

     RETURN 0;
  END;

  -- Проверка - есть ли на данный выпуск ссылки.
  -- Есть ли fininstr c MainFIID равным FIID данного.
  FUNCTION FI_IsMainAvr( FIID IN NUMBER )
    RETURN NUMBER
    DETERMINISTIC
  IS
    IsMainAvr NUMBER := 0;
  BEGIN
    SELECT
      COUNT( 1 )
    INTO
      IsMainAvr
    FROM
      dfininstr_dbt
    WHERE
      t_MainFIID = FIID;

    IF ( IsMainAvr > 0 )
    THEN
      RETURN 1;
    END IF;

    RETURN 0;
  END; -- FI_IsMainAvr

 /**
 * Получение реального вида ФИ
 * Т.к. драгметаллы для клиентов заведены как базовый актив Валюта, то вид базового инструмента необходимо определять по буквенному ISO-коду валюты 
 * Если ISO-код начинается с латинской буквы "A", то ФИ - драгметалл
 * @since RSHB 108
 * @qtest NO
 * @param p_FIID Идентификатор ФИ
 * @return вид ФИ 
 */
  FUNCTION FI_GetRealFIKind(p_FIID IN NUMBER)
    RETURN NUMBER deterministic
  IS
    v_RetFIKind dfininstr_dbt.t_FI_Kind%TYPE;
    v_ISO dfininstr_dbt.t_ISO_Number%TYPE;
  BEGIN
    BEGIN
      SELECT t_FI_Kind, t_ISO_Number
        INTO v_RetFIKind, v_ISO 
        FROM dfininstr_dbt 
       WHERE t_FIID = p_FIID;
    EXCEPTION 
      WHEN NO_DATA_FOUND THEN v_RetFIKind := 0;
    END;

    IF v_RetFIKind = RSI_RSB_FIInstr.FIKIND_CURRENCY THEN
      IF SUBSTR(v_ISO, 1, 1) = 'A' THEN
        v_RetFIKind := RSI_RSB_FIInstr.FIKIND_METAL;
      END IF;
    END IF;
  
    RETURN v_RetFIKind;
  END FI_GetRealFIKind;

END RSI_RSB_FIInstr;
/
