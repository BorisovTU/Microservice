CREATE OR REPLACE PACKAGE BODY RSB_GTFN
IS
  ПокупкаОбрПродажа   NUMBER(5) := 2110; --Покупка с обратной продажей
  ПродажаОбрВыкуп     NUMBER(5) := 2115; --Продажа с обратным выкупом
  РепоПокупкаВнеб     NUMBER(5) := 2132; --Репо покупка внебиржевая
  РепоПродажаВнеб     NUMBER(5) := 2137; --Репо продажа внебиржевая
  ПокупкаВнеб         NUMBER(5) := 2183; --Покупка ц/б внебиржевая
  ПродажаВнеб         NUMBER(5) := 2193; --Продажа ц/б внебиржевая
  РепоПокупкаБирж     NUMBER(5) := 2122; --Репо покупка биржевая
  РепоПродажаБирж     NUMBER(5) := 2127; --Репо продажа биржевая
  ПокупкаБиржДУ       NUMBER(5) := 2145; --Покупка ц/б биржевая ДУ
  ПродажаБиржДУ       NUMBER(5) := 2155; --Продажа ц/б биржевая ДУ
  ПокупкаБиржСЭБ      NUMBER(5) := 3143; --Покупка СЭБ биржевая
  ПродажаБиржСЭБ      NUMBER(5) := 3153; --Продажа СЭБ биржевая
  ПокупкаБиржToday    NUMBER(5) := 2144; --Покупка ц/б биржевая today
  ПродажаБиржToday    NUMBER(5) := 2154; --Продажа ц/б биржевая today
  ПокупкаБирж         NUMBER(5) := 2143; --Покупка ц/б биржевая
  ПродажаБирж         NUMBER(5) := 2153; --Продажа ц/б биржевая
  РепоПокупкаБиржКСУ  NUMBER(5) := 2123; --Обратное РЕПО ОРЦБ с КСУ без признания ц/б
  РепоПродажаБиржКСУ  NUMBER(5) := 2128; --Прямое РЕПО ОРЦБ с КСУ без признания ц/б
  ПокупкаБрокер       NUMBER(5) := 2162; --Покупка ц/б через брокера
  ПродажаБрокер       NUMBER(5) := 2172; --Продажа ц/б через брокера
  ПокупкаОТС          NUMBER(5) := 12198; --Покупка ц/б ОТС
  ПродажаOTC          NUMBER(5) := 12199; --Продажа ц/б ОТС
  --!!!пользовательские типы операций!!!
  ПродажаБиржTodayUnderWr CONSTANT NUMBER(5) := 32731; --Продажа ц/б биржевая today андеррайтинг. BIQ-5485
  ПокупкаБиржTodayUnderWr CONSTANT NUMBER(5) := 32143; --Покупка ц/б биржевая. Услуги агента. BIQ-8720

  NUMBER_LLVALUE_BANKCODE CONSTANT NUMBER(5) := 5054; --DEF-36355 Cписок кодов банка перенесен в справочник 5054 Коды банка для срочного рынка (BankCode)  
  
  FIN DFININSTR_DBT%ROWTYPE; --ДЛЯ КЕШИРОВАНИЯ ПОДКАЧЕК

  PROCEDURE SetOperationType(p_IsRSHB IN NUMBER)
  IS 
  BEGIN
    IF(p_IsRSHB = 1) THEN
      ПокупкаОбрПродажа   := 12110; --Покупка с обратной продажей
      ПродажаОбрВыкуп     := 12115; --Продажа с обратным выкупом
      РепоПокупкаВнеб     := 12132; --Репо покупка внебиржевая
      РепоПродажаВнеб     := 12137; --Репо продажа внебиржевая
      ПокупкаВнеб         := 12183; --Покупка ц/б внебиржевая
      ПродажаВнеб         := 12193; --Продажа ц/б внебиржевая
      РепоПокупкаБирж     := 12122; --Репо покупка биржевая
      РепоПродажаБирж     := 12127; --Репо продажа биржевая
      ПокупкаБиржДУ       := 12145; --Покупка ц/б биржевая ДУ
      ПродажаБиржДУ       := 12155; --Продажа ц/б биржевая ДУ
      ПокупкаБиржСЭБ      := 13143; --Покупка СЭБ биржевая
      ПродажаБиржСЭБ      := 13153; --Продажа СЭБ биржевая
      ПокупкаБиржToday    := 12144; --Покупка ц/б биржевая today
      ПродажаБиржToday    := 12154; --Продажа ц/б биржевая today
      ПокупкаБирж         := 12143; --Покупка ц/б биржевая
      ПродажаБирж         := 12153; --Продажа ц/б биржевая
      РепоПокупкаБиржКСУ  := 12123; --Обратное РЕПО ОРЦБ с КСУ без признания ц/б
      РепоПродажаБиржКСУ  := 12128; --Прямое РЕПО ОРЦБ с КСУ без признания ц/б
      ПокупкаБрокер       := 12162; --Покупка ц/б через брокера
      ПродажаБрокер       := 12172; --Продажа ц/б через брокера  
      ПокупкаОТС          := 12198; --Покупка ц/б ОТС
      ПродажаOTC          := 12199; --Продажа ц/б ОТС
    ELSE
      ПокупкаОбрПродажа   := 2110; --Покупка с обратной продажей
      ПродажаОбрВыкуп     := 2115; --Продажа с обратным выкупом
      РепоПокупкаВнеб     := 2132; --Репо покупка внебиржевая
      РепоПродажаВнеб     := 2137; --Репо продажа внебиржевая
      ПокупкаВнеб         := 2183; --Покупка ц/б внебиржевая
      ПродажаВнеб         := 2193; --Продажа ц/б внебиржевая
      РепоПокупкаБирж     := 2122; --Репо покупка биржевая
      РепоПродажаБирж     := 2127; --Репо продажа биржевая
      ПокупкаБиржДУ       := 2145; --Покупка ц/б биржевая ДУ
      ПродажаБиржДУ       := 2155; --Продажа ц/б биржевая ДУ
      ПокупкаБиржСЭБ      := 3143; --Покупка СЭБ биржевая
      ПродажаБиржСЭБ      := 3153; --Продажа СЭБ биржевая
      ПокупкаБиржToday    := 2144; --Покупка ц/б биржевая today
      ПродажаБиржToday    := 2154; --Продажа ц/б биржевая today
      ПокупкаБирж         := 2143; --Покупка ц/б биржевая
      ПродажаБирж         := 2153; --Продажа ц/б биржевая
      РепоПокупкаБиржКСУ  := 2123; --Обратное РЕПО ОРЦБ с КСУ без признания ц/б
      РепоПродажаБиржКСУ  := 2128; --Прямое РЕПО ОРЦБ с КСУ без признания ц/б
      ПокупкаБрокер       := 2162; --Покупка ц/б через брокера
      ПродажаБрокер       := 2172; --Продажа ц/б через брокера    
      ПокупкаОТС          := 12198; --Покупка ц/б ОТС
      ПродажаOTC          := 12199; --Продажа ц/б ОТС
    END IF;
  
  END; 

  --Создание документа-подтверждения "Отчет биржи"
  FUNCTION WriteMarketReport(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_OutMarketReportID OUT NUMBER, p_ErrMsg OUT VARCHAR2,
                             p_ReportNumber IN VARCHAR2, p_ImpDate IN DATE, p_RgPartyObject IN VARCHAR2)
    RETURN NUMBER
  IS
    v_stat NUMBER(5) := 0;
  BEGIN
    p_ErrMsg            := CHR(1);
    p_OutMarketReportID := 0;

    v_stat := RSI_GT.Init(RSI_GT.DIRECT_INSERT_YES, p_SeanceID, p_SourceCode, RSI_GT.GTCODE_RECEIVE, p_ErrMsg);

    IF v_stat = 0 THEN
      v_stat := RSI_GT.InitRec(RG_MARKETREPORT, 'Отчет биржи/брокера №' || p_ReportNumber, 1, p_ReportNumber, 0, p_ErrMsg);

      p_OutMarketReportID := RSI_GT.GetObjectID;
    END IF;

    IF v_stat = 0 THEN
      --добавляем параметры
      RSI_GT.SetParmByName('RGMKRP_NUMBER', p_ReportNumber);
      RSI_GT.SetParmByName('RGMKRP_DATE',   p_ImpDate);
      RSI_GT.SetParmByName('RGMKRP_AUTHOR', p_RgPartyObject);

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

  --ПолучитьВидОперацииБОЦБ
  FUNCTION GetTypeOperationBOCB(p_GateDealPlace IN VARCHAR2, p_GateDealType IN VARCHAR2, p_DealTypeID OUT NUMBER, p_ErrMes OUT VARCHAR2, p_IsDealDU IN NUMBER, p_IsTODAY IN NUMBER, p_IsSEB IN NUMBER, p_UnderWr IN NUMBER, p_UnderWr_Pokupka IN NUMBER, p_IsRSHB IN NUMBER, p_IsOTC IN NUMBER)
    RETURN NUMBER
  IS
    v_GateDealPlace VARCHAR2(1) := UPPER(p_GateDealPlace);
    v_GateDealType  VARCHAR2(4) := UPPER(p_GateDealType);
  BEGIN
    p_DealTypeID := 0;
    p_ErrMes     := CHR(1);

    IF v_GateDealPlace NOT IN ('V', 'B', 'D') THEN
      p_ErrMes := 'В файле импорта неверно указан вид сделки ("' || v_GateDealPlace || '")';
      RETURN 1;
    END IF;
    SetOperationType(p_IsRSHB );

    p_DealTypeID := CASE WHEN v_GateDealPlace = 'V' --внебиржевая
                         THEN CASE WHEN v_GateDealType = 'BS'
                                   THEN ПокупкаОбрПродажа --Покупка с обратной продажей
                                   WHEN v_GateDealType = 'SB'
                                   THEN ПродажаОбрВыкуп --Продажа с обратным выкупом
                                   WHEN v_GateDealType = 'RBS'
                                   THEN РепоПокупкаВнеб --Продажа с обратным выкупом
                                   WHEN v_GateDealType = 'RSB'
                                   THEN РепоПродажаВнеб --Репо продажа внебиржевая
                                   WHEN v_GateDealType = 'B'
                                   THEN ПокупкаВнеб --Покупка ц/б внебиржевая
                                   WHEN v_GateDealType = 'S'
                                   THEN ПродажаВнеб --Продажа ц/б внебиржевая
                                   ELSE 0
                               END
                         WHEN v_GateDealPlace = 'B' --биржевая
                         THEN CASE WHEN v_GateDealType IN ('RBS', 'RB')
                                   THEN РепоПокупкаБирж --Репо покупка биржевая
                                   WHEN v_GateDealType = 'B'
                                   THEN CASE WHEN p_IsDealDU = 1
                                             THEN ПокупкаБиржДУ --Покупка ц/б биржевая ДУ
                                             WHEN p_IsOTC = 1
                                             THEN ПокупкаОТС   --Покупка ОТС
                                             ELSE CASE WHEN p_IsSeb = 1
                                                       THEN ПокупкаБиржСЭБ --Покупка ц/б биржевая СЭБ
                                                       WHEN p_IsTODAY = 1 AND p_UnderWr_Pokupka = 1
                                                       THEN ПокупкаБиржTodayUnderWr --Покупка ц/б биржевая today андеррайтинг
                                                       WHEN p_IsTODAY = 1
                                                       THEN ПокупкаБиржToday --Покупка ц/б биржевая today
                                                       ELSE ПокупкаБирж --Покупка ц/б биржевая
                                                   END
                                         END
                                   WHEN v_GateDealType IN ('RSB', 'RS')
                                   THEN РепоПродажаБирж --Репо продажа биржевая
                                   WHEN v_GateDealType = 'S'
                                   THEN CASE WHEN p_IsDealDU = 1
                                             THEN ПродажаБиржДУ --Продажа ц/б биржевая ДУ
                                             WHEN p_IsOTC = 1
                                             THEN ПродажаOTC   --Продажа ОТС
                                             ELSE CASE WHEN p_IsSeb = 1
                                                       THEN ПродажаБиржСЭБ --Продажа ц/б биржевая СЭБ
                                                       WHEN p_IsTODAY = 1 AND p_UnderWr = 1
                                                       THEN ПродажаБиржTodayUnderWr --Продажа ц/б биржевая today андеррайтинг
                                                       WHEN p_IsTODAY = 1
                                                       THEN ПродажаБиржToday --Продажа ц/б биржевая today
                                                       ELSE ПродажаБирж --Продажа ц/б биржевая
                                                   END
                                         END
                                   WHEN v_GateDealType = 'RBG'
                                   THEN РепоПокупкаБиржКСУ --Обратное РЕПО ОРЦБ с КСУ без признания ц/б
                                   WHEN v_GateDealType = 'RSG'
                                   THEN РепоПродажаБиржКСУ --Прямое РЕПО ОРЦБ с КСУ без признания ц/б
                                   ELSE 0
                               END
                         WHEN v_GateDealPlace = 'D' --через брокера
                         THEN CASE WHEN v_GateDealType = 'B'
                                   THEN ПокупкаБрокер --Покупка ц/б через брокера
                                   WHEN v_GateDealType = 'S'
                                   THEN ПродажаБрокер --Продажа ц/б через брокера
                                   ELSE 0
                               END
                         ELSE 0
                     END;

    IF p_DealTypeID <= 0 THEN
      p_ErrMes := 'В файле импорта неверно указан тип сделки ("' || v_GateDealType || '")';
      RETURN 1;
    END IF;

    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    p_ErrMes := 'Произошла непредвиденная ошибка в ф-и GetTypeOperationBOCB()';
    RETURN 1;
  END;

  --получить наименование параметра для платежа с заданным назначением
  FUNCTION GetParmName(p_Name IN VARCHAR2, p_Purp IN NUMBER) RETURN VARCHAR2
  IS
  BEGIN
    RETURN p_Name || CASE WHEN p_Purp = Rsb_Payment.BAi                 THEN '_00'
                          WHEN p_Purp = Rsb_Payment.CAi                 THEN '_01'
                          WHEN p_Purp = Rsb_Payment.PM_PURP_AVANCE      THEN '_02'
                          WHEN p_Purp = Rsb_Payment.BRi                 THEN '_03'
                          WHEN p_Purp = Rsb_Payment.CRi                 THEN '_04'
                          WHEN p_Purp = Rsb_Payment.PM_PURP_BACK_AVANCE THEN '_05'
                          ELSE CHR(0)
                      END;
  END;

  --Получить из строки(например 'prm1=15;prm2=дата2') параметров ассоциативную коллекцию
  FUNCTION GetPrmMapByStr(p_PrmStr IN VARCHAR2) RETURN STRMAP_T
  IS
    v_PrmStrMap STRMAP_T;
    v_idx NUMBER(5);
  BEGIN
    FOR cData IN (SELECT regexp_substr(t.t_val, '[^;]+', 1, LEVEL) val
                    FROM (SELECT p_PrmStr t_val FROM DUAL ) t
                 CONNECT BY regexp_substr(t.t_val, '[^;]+', 1, LEVEL) IS NOT NULL
                 )
    LOOP
      v_idx := INSTR(cData.val, '=');
      
      IF v_idx > 0 THEN
        v_PrmStrMap(SUBSTR(cData.val, 1, v_idx - 1)) := SUBSTR(cData.val, v_idx + 1);
      END IF;
    END LOOP;

    RETURN v_PrmStrMap;
  END;

  FUNCTION GetRefNote(p_Code IN VARCHAR2, p_split IN VARCHAR2) RETURN VARCHAR2
  IS
  BEGIN
    RETURN CASE WHEN INSTR(p_Code, p_split) > 0
                THEN NVL(SUBSTR(p_Code, INSTR(p_Code, p_split) + 1), CHR(1))
                ELSE p_Code
            END;
  END;

  FUNCTION GetClientCode(p_ClientCodeInFile IN VARCHAR2) RETURN VARCHAR2
  IS
    v_ClientCodeEndPos NUMBER(5);
  BEGIN
    IF p_ClientCodeInFile NOT IN (CHR(1), ' ') THEN
      v_ClientCodeEndPos := INSTR(p_ClientCodeInFile, '/');
      IF v_ClientCodeEndPos > 0 THEN
        RETURN SUBSTR(p_ClientCodeInFile, 1, v_ClientCodeEndPos - 1);
      ELSE
        RETURN CHR(1);
      END IF;
    END IF;

    RETURN CHR(1);
  END;

  FUNCTION DayInYear(p_YYYY IN NUMBER) RETURN NUMBER
  IS
  BEGIN
    RETURN TO_DATE('31.12.' || TO_CHAR(p_YYYY), 'DD.MM.YYYY') - TO_DATE('01.01.' || TO_CHAR(p_YYYY), 'DD.MM.YYYY') + 1;
  END;

  --НайтиДанныеПо2Части
  PROCEDURE FindDataPart2(p_SettleDate IN DATE, p_SettleDate2 IN DATE, p_Amount IN NUMBER, p_Amount2 IN NUMBER, p_RepoRate IN OUT NOCOPY NUMBER)
  IS
    v_N         NUMBER(32, 12);
    v_YYYY_begI NUMBER(5);
    v_YYYY_endI NUMBER(5);
    v_YYYY_curI NUMBER(5);
  BEGIN
    p_RepoRate := 0;

    IF p_SettleDate <> p_SettleDate2 THEN
      v_YYYY_begI:= EXTRACT(YEAR FROM p_SettleDate);
      v_YYYY_endI:= EXTRACT(YEAR FROM p_SettleDate2);

      IF v_YYYY_begI = v_YYYY_endI THEN
        v_N := (p_SettleDate2 - p_SettleDate) / DayInYear(v_YYYY_begI);
      ELSE 
        v_YYYY_curI := v_YYYY_begI + 1; 
        v_N := (TO_DATE( '31.12.' || TO_CHAR(v_YYYY_curI), 'DD.MM.YYYY') - p_SettleDate) / DayInYear(v_YYYY_begI);

        WHILE v_YYYY_curI < v_YYYY_endI
        LOOP 
          v_N := v_N + 1;
          v_YYYY_curI := v_YYYY_curI + 1;
        END LOOP;

        v_N := v_N + (p_SettleDate2 - (TO_DATE('31.12.' || TO_CHAR(v_YYYY_curI), 'DD.MM.YYYY' ))) / DayInYear(v_YYYY_begI);
      END IF;
    END IF;
    
    IF p_Amount * v_N <> 0 THEN
      p_RepoRate := ROUND(((p_Amount2 - p_Amount) / (p_Amount * v_N)) * 100, 4);
    END IF;
  END;

  -- НАЙТИ ВНУТРЕННИЙ ИДЕНТИФИКАТОР PARTYID СУБЪЕКТА ПО ЕГО ВНЕШНЕМУ КОДУ CODE ВИДА CODEKIND
  FUNCTION FINDPARTCODE( P_CODE IN VARCHAR2, P_CODEKIND IN INTEGER ) RETURN DPARTCODE_DBT.T_PARTYID%TYPE
  AS
    V_PARTYID DPARTCODE_DBT.T_PARTYID%TYPE;
  BEGIN

    SELECT T_PARTYID INTO V_PARTYID
      FROM DPARTCODE_DBT
     WHERE T_CODE = P_CODE
       AND T_CODEKIND = P_CODEKIND;

    RETURN V_PARTYID;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN RETURN NULL;
  END FINDPARTCODE;  

  FUNCTION RGDLFUN_GETCLIENTID(KOD IN VARCHAR2, CODEKIND IN INTEGER ) RETURN INTEGER
  IS
    PARTYID INTEGER;
  BEGIN

    PARTYID := FINDPARTCODE(KOD, CODEKIND);
    IF(PARTYID IS NULL) THEN
       RETURN -1;
    END IF;
      RETURN PARTYID;
  END;
  
  -- ПОЛУЧИТЬ ПАРАМЕТРЫ ФИНАНСОВОГО ИНСТРУМЕНТ
  FUNCTION FINDFININSTR(P_FIID IN NUMBER, P_FININSTRBUFF OUT DFININSTR_DBT%ROWTYPE) RETURN NUMBER
  IS
    M_STAT NUMBER := 0;
  BEGIN
    BEGIN
      SELECT * INTO P_FININSTRBUFF
        FROM DFININSTR_DBT
       WHERE T_FIID = P_FIID;
    EXCEPTION WHEN OTHERS THEN M_STAT := 4;
    END;

    RETURN M_STAT;
  END;
  
  FUNCTION GETFININ(PGATECODE IN VARCHAR2, PCODEKIND IN NUMBER DEFAULT 0, PISSUES IN OUT NOCOPY DAVOIRISS_DBT%ROWTYPE, 
                    PFIN IN OUT NOCOPY DFININSTR_DBT%ROWTYPE) RETURN NUMBER
  IS
    V_STAT NUMBER := 0;
  BEGIN
    IF(PCODEKIND > 0) THEN
      IF(PCODEKIND = RSB_SECUR.CODE_ISIN) THEN
        SELECT * INTO PISSUES
          FROM DAVOIRISS_DBT
         WHERE T_ISIN = PGATECODE;

        V_STAT := FINDFININSTR(PISSUES.T_FIID, PFIN);
      ELSIF (PCODEKIND = RSB_SECUR.CODE_MICEX) THEN
        SELECT FI.* INTO PFIN
          FROM DFININSTR_DBT FI, DOBJCODE_DBT CODE
         WHERE CODE.T_OBJECTID = FI.T_FIID
           AND CODE.T_OBJECTTYPE = RSB_SECUR.OBJTYPE_FININSTR
           AND CODE.T_CODEKIND = PCODEKIND
           AND CODE.T_CODE = PGATECODE
           AND ROWNUM < 2;
      ELSE
        V_STAT := 4;
      END IF;
    ELSE
      V_STAT := FINDFININSTR(TO_NUMBER(PGATECODE), FIN);
    END IF;

    RETURN V_STAT;
    EXCEPTION WHEN OTHERS THEN RETURN 4;
  END;
  
  -- ПО КРАТКОМУ КОДУ НА БИРЖЕ 
  FUNCTION RGDLFUN_GETCLIENTID_BYMPCODE(MPCODE IN VARCHAR2, SERVKIND IN NUMBER, ONDATE IN DATE, 
                                        MARKETID IN INTEGER, SFCONTRID OUT NUMBER) RETURN NUMBER
  IS
    PARTYID NUMBER := -1;
  BEGIN
    SFCONTRID := 0;
    FOR CDATA IN (
      SELECT SFCONTR.T_PARTYID, SFCONTR.T_ID SFCONTRID
        INTO PARTYID, SFCONTRID
        FROM DDLCONTR_DBT DLCONTR, DDLCONTRMP_DBT CONTRMP, DSFCONTR_DBT SFCONTR
       WHERE DLCONTR.T_DLCONTRID   = CONTRMP.T_DLCONTRID
         AND SFCONTR.T_ID          = CONTRMP.T_SFCONTRID
         AND SFCONTR.T_SERVKIND    = SERVKIND
         AND CONTRMP.T_MPCODE      = MPCODE
         AND CONTRMP.T_MARKETID    = MARKETID
         AND SFCONTR.T_DATEBEGIN  <= ONDATE
         AND ( SFCONTR.T_DATECLOSE > ONDATE OR SFCONTR.T_DATECLOSE = RSI_GT.ZeroDate )
      ORDER BY SFCONTR.T_DATEBEGIN DESC )
    LOOP
      PARTYID := CDATA.T_PARTYID;
      SFCONTRID := CDATA.SFCONTRID;
      EXIT;
    END LOOP;
    
    IF (PARTYID <= 0) THEN
      RETURN -1;
    END IF;
    
    RETURN PARTYID;
  EXCEPTION WHEN OTHERS THEN RETURN -1;
  END;
  
  FUNCTION GETREALID(KINDOBJ IN NUMBER, GATECODE IN VARCHAR2, GKBO_CODEKIND IN NUMBER,
                     REALID OUT NUMBER, ISRSHB IN NUMBER DEFAULT 0, ERRMES OUT VARCHAR2, CLIENTBYMPCODE IN BOOLEAN DEFAULT NULL,
                     SERVKIND IN NUMBER DEFAULT 0, ONDATE IN DATE DEFAULT RSI_GT.ZeroDate, SFCONTRID OUT NUMBER, 
                     MARKETID IN NUMBER DEFAULT NULL) RETURN NUMBER
  IS
    STAT NUMBER(5) := 0;
    PARTYID NUMBER(10) := -1;
    MARKET NUMBER(10);
    B_CODE VARCHAR(32767);
    BYMPCODE BOOLEAN;
    V_ISSUES DAVOIRISS_DBT%ROWTYPE;
    V_FIN DFININSTR_DBT%ROWTYPE;
  BEGIN
    REALID := -1;
    ERRMES := CHR(1);
    
    IF( (KINDOBJ = RG_AVOIRISS) OR (KINDOBJ = RG_CURRENCY) ) THEN
      IF( GETFININ( GATECODE, GKBO_CODEKIND, V_ISSUES, V_FIN ) != 0) THEN
        ERRMES := 'В ГКБО НЕ НАЙДЕН ФИ С КОДОМ "' || GATECODE || '" ВИДА ' || GKBO_CODEKIND;  
        STAT := 1;
      ELSE
        REALID := V_FIN.T_FIID;
      END IF;
      
    ELSIF(KINDOBJ = RG_PARTY) THEN
      SFCONTRID := -1; -- ДОГОВОР КЛИЕНТА
      BYMPCODE := ((CLIENTBYMPCODE IS NOT NULL) AND CLIENTBYMPCODE); -- ИСКАТЬ ПО ККК (КРАТКИЙ КОД КЛИЕНТА НА БИРЖЕВОЙ ПЛОЩАДКЕ)
      
      IF(BYMPCODE) THEN
        MARKET := 0;
        B_CODE := CHR(1);
        
        IF ((MARKETID IS NOT NULL) AND (MARKETID > 0)) THEN
          MARKET := MARKETID;
        ELSE
          IF (GKBO_CODEKIND = CNST.PTCK_MICEX) THEN
            B_CODE := MMVB_CODE;
          ELSIF (GKBO_CODEKIND = CNST.PTCK_SPBEX) THEN
            B_CODE := SPB_CODE;
          ELSE
            B_CODE := MMVB_CODE; -- ПО УМОЛЧАНИЮ
          END IF;
          MARKET := RGDLFUN_GETCLIENTID(B_CODE, CNST.PTCK_CONTR);
        END IF;
        
        IF (MARKET <= 0 ) THEN
          ERRMES := ERRMES || 'В ГКБО НЕ НАЙДЕН СУБЪЕКТ С КОДОМ "' || B_CODE || '" ВИДА ' || GKBO_CODEKIND;
          STAT := 1;
        ELSE
          PARTYID := RGDLFUN_GETCLIENTID_BYMPCODE(GATECODE, SERVKIND, ONDATE, MARKET, SFCONTRID);
        END IF;
        
        NULL;
      END IF;
      
      IF (PARTYID <= 0) THEN
        IF ISRSHB = 1 AND GATECODE != ' ' AND CheckBankCode(GATECODE) <> 0 THEN
          PARTYID := RSBSESSIONDATA.OurBank;
        ELSE   
          PARTYID := RGDLFUN_GETCLIENTID(GATECODE, GKBO_CODEKIND);
        END IF;
        
        IF (PARTYID <= 0) THEN
          IF(BYMPCODE) THEN
            ERRMES := 'НЕ НАЙДЕН СУБЪЕКТ ЧЕРЕЗ КРАТКИЙ КОД НА БИРЖЕ "' || GATECODE || '"';  
          END IF;

          IF(ERRMES IS NOT NULL) THEN
            ERRMES := ERRMES || CHR(10);
          END IF;

          IF((NOT BYMPCODE) OR (GKBO_CODEKIND != -1)) THEN
            ERRMES := ERRMES || 'В ГКБО НЕ НАЙДЕН СУБЪЕКТ С КОДОМ "' || GATECODE || '" ВИДА ' || GKBO_CODEKIND;
          END IF;

          STAT := 1;
        ELSE
          REALID := PARTYID;
        END IF;
      ELSE
        REALID := PARTYID;
      END IF;
    END IF;
    
    RETURN STAT;
    EXCEPTION WHEN OTHERS THEN RETURN 1;
  END;

  FUNCTION CheckBankCode(p_Code IN VARCHAR2) RETURN NUMBER
  IS
    v_Res NUMBER(5) := 0;
  BEGIN
     BEGIN
        SELECT 1 INTO v_Res
          FROM dllvalues_dbt
         WHERE t_List = NUMBER_LLVALUE_BANKCODE
           AND t_Code = p_Code;
     EXCEPTION WHEN NO_DATA_FOUND THEN v_Res := 0;
     END;

     RETURN v_Res;
  END;

END RSB_GTFN;
/
