CREATE OR REPLACE PACKAGE BODY RSB_DL724REP
IS
  TYPE AvrValueData_t IS RECORD (t_SumRub  NUMBER,
                                 t_Sum     NUMBER,
                                 t_Course  NUMBER,
                                 t_Rate    NUMBER,
                                 t_NKD     NUMBER,
                                 t_NKDRub  NUMBER,
                                 t_MarketPrice NUMBER,
                                 t_MarketPriceRub NUMBER,
                                 t_FindCourseFi NUMBER);

  TYPE AvrMPData_t IS RECORD (t_MarketPrice  NUMBER,
                              t_Rate         NUMBER,
                              t_FindCourseFi NUMBER,
                              t_Message      VARCHAR2(255));
                              

  FUNCTION GetBankRole(pSfContrId IN NUMBER, pEndDate IN DATE, pIsRep12 IN NUMBER)
  RETURN NUMBER
  DETERMINISTIC
  IS
    v_SfContrID dsfcontr_dbt.t_ID%TYPE;
    v_AttrID NUMBER(10);
  BEGIN
    IF pIsRep12 = 1 THEN
      RETURN 2;
    END IF;

    --Найдем субдоговор фондового рынка ММВБ
    BEGIN
      SELECT sf.t_ID INTO v_SfContrID
        FROM dsfcontr_dbt sf, ddlcontrmp_dbt mp, ddlcontr_dbt dl
       WHERE dl.t_SfContrID = pSfContrId
         AND mp.t_DlContrID = dl.t_DlContrID  
         AND mp.t_SfContrID = sf.t_ID
         AND sf.t_ServKind = 1 
         AND sf.t_ServKindSub = 8
         AND mp.t_MarketID = 2 /*ММВБ*/
         AND (sf.t_DateClose > pEndDate OR sf.t_DateClose = to_date('01.01.0001','DD.MM.YYYY'));
    EXCEPTION
      WHEN NO_DATA_FOUND THEN v_SfContrID := -1;
    END;

    v_AttrID := RSB_SECUR.GetMainObjAttr(659, LPAD(v_SfContrID, 10, '0'), 6, pEndDate); --Предоставлять брокеру право использования ценных бумаг в его интересах

    IF v_AttrID = 1 /*Да*/ THEN
      RETURN 1;
    ELSIF v_AttrID = 2 /*Нет*/ THEN
      RETURN 2;
    ELSE /*не задано*/
      RETURN 1;
    END IF;

  END GetBankRole;


 /**
 * Получение значения категории на ДБО "ОКВЭД для 724 ф" 
 * @since RSHB 107.1
 * @qtest NO
 * @param pContrID Идентификатор субдоговора
 * @param pEndDate Дата, на которую получаем значение
 * @return Установленное значение в формате строки 
 */
  FUNCTION GetCatOKVEDby724(pDlContrID IN NUMBER, pEndDate IN DATE)
  RETURN VARCHAR 
  DETERMINISTIC
  IS
    v_RetVal VARCHAR(35); 
  BEGIN
    SELECT Attr.t_NumInList INTO v_RetVal
      FROM dobjatcor_dbt AtCor, dobjattr_dbt Attr
     WHERE AtCor.t_ObjectType = 207
       AND AtCor.t_GroupID    = 199 --ОКВЭД для 724 ф
       AND AtCor.t_Object     = LPAD(pDlContrID , 34, '0' )
       AND AtCor.t_ValidFromDate  = ( SELECT MAX(t.T_ValidFromDate)
                                        FROM DOBJATCOR_DBT t
                                       WHERE t.T_ObjectType = AtCor.T_ObjectType
                                         AND t.T_GroupID    = AtCor.T_GroupID
                                         AND t.t_Object     = AtCor.t_Object
                                         AND t.T_ValidFromDate <= pEndDate
                                         AND (t.T_ValidToDate >= pEndDate OR t.T_ValidToDate = TO_DATE('01.01.0001', 'DD.MM.YYYY'))
                                    )
       AND (AtCor.T_ValidToDate >= pEndDate OR AtCor.T_ValidToDate = TO_DATE('01.01.0001', 'DD.MM.YYYY'))
       AND Attr.t_AttrID      = AtCor.t_AttrID
       AND Attr.t_ObjectType  = AtCor.t_ObjectType
       AND Attr.t_GroupID     = AtCor.t_GroupID
       AND ROWNUM             = 1;
     RETURN v_RetVal;
   EXCEPTION
     WHEN NO_DATA_FOUND THEN RETURN CHR(1);
   END GetCatOKVEDby724;
  
   
  FUNCTION GetGroup_Contr(pPartyId IN NUMBER, pEndDate IN DATE, pIsRep12 IN NUMBER, pDlContrID IN NUMBER)
  RETURN NUMBER
  DETERMINISTIC
  IS
    type t_partykinds is table of number(1) index by binary_integer;
    l_partykinds t_partykinds;
    v_Found NUMBER := 0;
    type_fl NUMBER := 0;
    v_NumInList VARCHAR(35);
    v_IsDO NUMBER := 0;
  BEGIN

/*
Указывается один из следующих кодов:
1  Используется для клиента с принадлежностью "НПФ"
2  Используется для клиента с принадлежностью "Доверительный управляющий" и категорией на ДБО "ОКВЭД для 724 ф" = "66.30.1 - Управление инвестиционными фондами"
3  Используется для клиента с принадлежностью "Доверительный управляющий" и категорией на ДБО "ОКВЭД для 724 ф" = "66.30.3 - Управление пенсионными накоплениями негосударственных пенсионных фондов"
                                                                                                              или "66.30.4 - Управление пенсионными резервами негосударственных пенсионных фондов".
4  Используется для клиента с принадлежностью "Доверительный управляющий" и категорией на ДБО "ОКВЭД для 724 ф" = "66.30.6 - Управление на основе индивидуальных договоров доверительного управления активами".
5  Используется для клиента с принадлежностью "Доверительный управляющий" и категорией на ДБО "ОКВЭД для 724 ф" = "64.99.22 - Деятельность по формированию целевого капитала некоммерческих организаций".
6  Используется для клиента с принадлежностью "Банк".
7  Используется для клиента с принадлежностью "Брокер".
8  Используется для клиента с принадлежностью "Страховая компания".
9  Используется для клиента с категорией на ДБО "ОКВЭД для 724 ф" = "64.30 - Деятельность инвестиционных фондов и аналогичных финансовых организаций".
                      УК АИФ - если у клиента принадлежность = "Доверительное управление", иначе  АИФ
10 Используется для клиента с принадлежностью "Финансовая организация".
11 Используется для клиента-физлица, у которого в карточке клиента  имеется отметка в поле "Предприниматель".
12 Используется для клиента-юрлица, не удовлетворяющего условиям для кодов 1-11.
13 Используется для клиента-физлица.

2  - Банк
65 - Доверительный управляющий
78 - НПФ
22 - Брокер
30 - Страховая компания
69 - Финансовая организация
*/

    IF pIsRep12 = 1 THEN
      RETURN 0;
    END IF;

    for p in (select distinct t_partykind
                from dpartyown_dbt
               where t_partyid = pPartyId
                 and t_partykind in (78, 65, 2, 22, 30, 69))
    loop
      l_partykinds(p.t_partykind) := 1;
    end loop;

    if l_partykinds.exists(78) then
      return 1;
    end if;

    if l_partykinds.exists(65) then
      v_NumInList := GetCatOKVEDby724(pDlContrID, pEndDate);
    
      IF v_NumInList <> CHR(1) THEN
        IF v_NumInList = '66.30.1' THEN
          RETURN 2;
        ELSIF v_NumInList = '66.30.3' OR v_NumInList = '66.30.4' THEN
          RETURN 3;
        ELSIF v_NumInList = '66.30.6' THEN
          RETURN 4;
        ELSIF v_NumInList = '64.99.22' THEN
          RETURN 5;
        END IF;
      END IF;        
    END IF;

    if l_partykinds.exists(2) then
      return 6;
    end if;

    if l_partykinds.exists(22) then
      return 7;
    end if;

    if l_partykinds.exists(30) then
      return 8;
    end if;

    v_NumInList := GetCatOKVEDby724(pDlContrID, pEndDate);
    IF v_NumInList <> CHR(1) THEN
      IF v_NumInList = '64.30' THEN
        IF l_partykinds.exists(65) THEN
          RETURN 92; -- вывести 9, но в группировке это будет отдельно, от аналогичного 91, который тоже выводится как 9
        ELSE
          RETURN 91; -- вывести 9, но в группировке это будет отдельно, от аналогичного 92, который тоже выводится как 9
        END IF;
      END IF;
    END IF;

    if l_partykinds.exists(69) then
      return 10;
    end if;

    BEGIN
      SELECT
         (CASE WHEN EXISTS (SELECT 1 FROM DPERSN_DBT WHERE T_PersonID = t.t_partyid and t_IsEmployer = 'X') THEN 11
         ELSE (CASE WHEN t.t_LegalForm = 2 THEN 13 ELSE 12 END) END) into type_fl
        FROM dparty_dbt t
       WHERE t.t_partyid = pPartyId;

      RETURN type_fl;
    EXCEPTION
      WHEN NO_DATA_FOUND
        THEN type_fl := 0;
    END;

    RETURN type_fl;
  END GetGroup_Contr;


  FUNCTION is_activeClient(pSessionId IN NUMBER, pPartyId IN NUMBER, pBegDate  IN DATE, pEndDate IN DATE, pIsSecur IN NUMBER, pIsDV IN NUMBER, pIsRep12 IN NUMBER)
  RETURN NUMBER
  DETERMINISTIC
  IS
    v_Found NUMBER := 0;
  BEGIN
/*
Активным считается клиент брокерского обслуживания, у которого есть откры-тые/закрытые сделки
(в подсистемах БОЦБ, ФИССиКО), за исключением операций зачис-ления/списания денежных средств и ц/б,
дата заключения которых входит в отчетный пе-риод (в параметрах сделки в качестве ДО клиента
указан либо сам ДО либо субдоговор отобранного ДБО).
*/

    IF pIsRep12 = 1 THEN
      RETURN 0;
    END IF;

    if pIsSecur = 1 then
      BEGIN
        SELECT 1
          INTO v_Found
          FROM ddl_tick_dbt tick
         WHERE tick.T_BOFFICEKIND IN (DL_SECURITYDOC, DL_RETIREMENT, DL_ISSUE_UNION, DL_CONVAVR)
           AND tick.t_DealStatus > 0
           AND tick.t_DealDate BETWEEN pBegDate AND pEndDate
           AND (tick.t_ClientID = pPartyId OR
                (tick.t_PartyID = pPartyId AND tick.t_IsPartyClient = 'X')
               )
           AND ROWNUM = 1;
      EXCEPTION
        WHEN NO_DATA_FOUND
          THEN v_Found := 0;
      END;

      if v_Found > 0 then
        RETURN 1;
      end if;
    end if;

    if pIsDV = 1 then
      BEGIN
        SELECT 1
          INTO v_Found
          FROM DDVNDEAL_DBT dvndeal
         WHERE     DVNDEAL.T_CLIENT = pPartyId
               AND DVNDEAL.T_DATE BETWEEN pBegDate AND pEndDate
               AND DVNDEAL.T_STATE > 0
           AND ROWNUM = 1;

      EXCEPTION
        WHEN NO_DATA_FOUND
          THEN v_Found := 0;
      END;

      if v_Found > 0 then
        RETURN 1;
      end if;


      BEGIN
        SELECT 1
          INTO v_Found
          FROM DDVDEAL_DBT dvdeal
         WHERE     DVDEAL.T_CLIENT = pPartyId
               AND DVDEAL.T_DATE BETWEEN pBegDate AND pEndDate
               AND DVDEAL.T_STATE > 0
           AND ROWNUM = 1;

      EXCEPTION
        WHEN NO_DATA_FOUND
          THEN v_Found := 0;
      END;

      if v_Found > 0 then
        RETURN 1;
      end if;
    end if;

    RETURN 0;
  END is_activeClient;



  FUNCTION GetFIName(pFIID IN NUMBER)
  RETURN dfininstr_dbt.t_Name%type
  DETERMINISTIC
  IS
    v_Name dfininstr_dbt.t_Name%type;
  BEGIN
    SELECT t_Name
      INTO v_Name
      FROM dfininstr_dbt
     WHERE t_FIID = pFIID;

    RETURN v_Name;
  EXCEPTION
    WHEN OTHERS
      THEN RETURN NULL;
  END GetFIName;

  FUNCTION CheckPartyType(pPartyId IN NUMBER, pPartyType IN NUMBER)
  RETURN NUMBER
  DETERMINISTIC
  IS
    v_isPartyType NUMBER := 0;
  BEGIN
    SELECT 1 INTO v_isPartyType
      FROM dpartyown_dbt
     WHERE t_PartyID = pPartyId
       AND t_PartyKind = pPartyType;

    RETURN v_isPartyType;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RETURN 0;
  END CheckPartyType;

  FUNCTION HasOGRN(pPartyID IN NUMBER)
  RETURN NUMBER
  DETERMINISTIC
  IS
  BEGIN
    RETURN CASE
             WHEN RSI_RSBPARTY.GetPartyCode(pPartyID, 27) = '0'
               THEN 0
             ELSE 1
           END;
  END HasOGRN;


  FUNCTION GetOkatoCode(pParty IN NUMBER, pNRCountry IN VARCHAR2, pNotResident IN CHAR, pOnDate DATE, pIsRep12 IN NUMBER)
  RETURN VARCHAR
  DETERMINISTIC
  IS
    v_okatoCode VARCHAR(20);
    v_isBaikonur NUMBER := 0;
  BEGIN
    IF (pNotResident = CNST.SET_CHAR) THEN
      RETURN '0';
    ELSE
      BEGIN
        SELECT 1 INTO v_isBaikonur
          FROM dadress_dbt
         WHERE t_partyid = pParty AND ((LOWER(t_adress) LIKE '%байконур%') AND (LOWER(t_district) LIKE '%байконур%'))
           AND ROWNUM = 1;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN v_isBaikonur := 0;
      END;
      IF (v_isBaikonur = 1) THEN
        RETURN '55';
      END IF;
    END IF;

    IF pIsRep12 = 1 THEN
      RETURN '00';
    END IF;

    v_okatoCode := RSB_SECUR.GetObjAttrNumber(RSB_SECUR.OBJTYPE_PARTY,
                                              12,
                                              RSB_SECUR.GetMainObjAttr(RSB_SECUR.OBJTYPE_PARTY,
                                                                       LPAD(pParty, 10, '0'),
                                                                       12,
                                                                       pOnDate));

    IF v_okatoCode = CHR(1) THEN
      v_okatoCode := Rsb_Secur.SC_GetObjCodeOnDate(cnst.OBJTYPE_PARTY, 73, pParty, pOnDate);
      IF (v_okatoCode IS NULL) THEN
         v_okatoCode := '00';
      END IF;
    END IF;

    IF length(v_okatoCode) > 5 THEN
      v_okatoCode := substr(v_okatoCode, 1, 5);
    END IF;
    
    IF (length(v_okatoCode) = 5) THEN
      IF (SUBSTR(v_okatoCode,3,1) = '0' AND SUBSTR(v_okatoCode,4,1) = '0' AND SUBSTR(v_okatoCode,5,1) = '0') THEN
         v_okatoCode := substr(v_okatoCode, 1, 2);
      END IF;
    ELSIF (length(v_okatoCode) > 2) THEN
      v_okatoCode := substr(v_okatoCode, 1, 2);
    END IF;

    RETURN v_okatoCode;
  END GetOkatoCode;


  FUNCTION GetOKSM_Code(pParty IN NUMBER, pNRCountry IN VARCHAR2, pNotResident IN CHAR, pSuperior IN NUMBER, pOnDate DATE, pIsRep12 IN NUMBER)
  RETURN VARCHAR
  DETERMINISTIC
  IS
    v_oksmCode VARCHAR(3) := CHR(1);
  BEGIN
    IF pIsRep12 = 1 THEN
      RETURN '999';
    END IF;

    IF CHeckPartyType(pParty, PTK_INTERNATIONAL_ORG) = 1 THEN
      IF (RSB_SECUR.GetMainObjAttr (RSB_SECUR.OBJTYPE_PARTY, LPAD(pParty, 10, '0'), 16, pOnDate) = 17) THEN
        v_oksmCode := '996';
      ELSE
        v_oksmCode := '998';
      END IF;
    ELSIF (pNotResident <> CNST.SET_CHAR) THEN
      v_oksmCode := '643';
    END IF;

    IF v_oksmCode = CHR(1) THEN
      BEGIN
        SELECT (CASE WHEN pNRCountry <> CHR (0) AND pNRCountry <> CHR (1) AND pNRCountry IS NOT NULL
          THEN
            (SELECT T_CODENUM3
               FROM DCOUNTRY_DBT DC
              WHERE DC.T_CODELAT3 = pNRCountry
                AND ROWNUM = 1)
          ELSE
             '999'
          END)
        INTO v_oksmCode
        FROM DUAL;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
           v_oksmCode := '999';
      END;
    END IF;

    RETURN v_oksmCode;
  END GetOKSM_Code;

  FUNCTION GetLastRateOnDateByType (pVN IN NUMBER, pDate IN DATE, pRateType IN NUMBER, pMarketID IN NUMBER)
  RETURN NUMBER
  DETERMINISTIC
  IS
    v_FIID   NUMBER;
  BEGIN
    SELECT /*+ result_cache */ t_fiid
      INTO v_FIID
     FROM (SELECT t_fiid
               FROM (SELECT rate.t_sincedate, rate.t_type, rate.t_fiid, rate.T_ISRELATIVE, rate.t_OtherFI
                       FROM dratedef_dbt rate
                      WHERE rate.t_otherfi = pVN /*string(Fiid_from) */
                        AND (pMarketID IS NULL OR pMarketID <= 0 OR rate.t_Market_Place = pMarketID)
                        AND t_sincedate    = (SELECT MAX (t_sincedate)
                                                FROM dratedef_dbt
                                               WHERE     t_otherfi = rate.t_otherfi
                                                     AND t_type    = rate.t_type
                                                     AND (pMarketID IS NULL OR pMarketID <= 0 OR rate.t_Market_Place = pMarketID)
                                                     AND t_sincedate <= pDate /*RateDate*/
                                             )
                     UNION ALL
                       SELECT h.t_sincedate, r.t_type, r.t_fiid, r.T_ISRELATIVE, r.t_OtherFI
                         FROM dratehist_dbt h, dratedef_dbt r
                        WHERE     r.t_rateid    = h.t_rateid
                              AND r.t_otherfi   = pVN  /*string(Fiid_from) */
                              AND (pMarketID IS NULL OR pMarketID <= 0 OR r.t_Market_Place = pMarketID)
                              AND h.t_sincedate = ( SELECT MAX (h2.t_sincedate)
                                                      FROM dratehist_dbt h2
                                                     WHERE h.t_rateid  = h2.t_rateid
                                                       AND h2.t_sincedate <= pDate /*RateDate*/
                                                  )
                    ) WHERE t_type IN (pRateType)
            ORDER BY t_sincedate DESC,
                     (CASE
                         WHEN T_ISRELATIVE != CHR (88) THEN 1
                         WHEN T_ISRELATIVE = CHR (88)
                              AND t_fiid = (SELECT fin.t_facevaluefi
                                              FROM dfininstr_dbt fin
                                             WHERE fin.t_fiid = t_otherfi) THEN 1
                         ELSE 0
                      END) DESC,
                     t_type ASC
          )
    WHERE ROWNUM = 1;

    RETURN v_FIID;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      IF pMarketID IS NOT NULL AND pMarketID > 0 THEN
        RETURN GetLastRateOnDateByType (pVN, pDate, pRateType, NULL);
      ELSE
        RETURN NULL;
      END IF;
  END GetLastRateOnDateByType;

 FUNCTION IsDateAfterWorkDayM (
                                   p_Date IN DATE,
                                   p_SinceDate IN DATE,
                                   p_identProgram IN NUMBER,
                                   p_maxDaysCnt IN NUMBER,
                                   p_CalParamArr RSI_DlCalendars.calparamarr_t)
      RETURN NUMBER deterministic
  AS
      v_recCount        NUMBER (10) := 0;
      v_objType         NUMBER (10) := 0;
      v_objTypeFromParm NUMBER (10);
      v_DayType         NUMBER (5);
      v_operName        DDLCALENOPRS_DBT.T_NAME%TYPE;
      l_sql             VARCHAR2 (32767);
      l_whereSql        VARCHAR2 (32767);
      v_CalParamArrEmpty     RSI_DlCalendars.calparamarr_t;
      v_CalParamArr     RSI_DlCalendars.calparamarr_t;
      v_Cur             SYS_REFCURSOR;
      TYPE CalendRow IS RECORD
    (
        T_CALKINDID     NUMBER,
        T_COUNT         NUMBER
    );

    TYPE CalendTable IS TABLE OF CalendRow;

    p_CalendTable CalendTable;
  BEGIN
     v_CalParamArrEmpty.DELETE;
     v_CalParamArr := NVL(p_CalParamArr,v_CalParamArrEmpty);

      v_objTypeFromParm :=
          CASE
              WHEN v_CalParamArr.EXISTS ('ObjectType')
              THEN
                  TO_NUMBER (v_CalParamArr ('ObjectType'))
              ELSE
                  NULL
          END;
      v_operName :=
          CASE
              WHEN v_CalParamArr.EXISTS ('Object')
              THEN
                  v_CalParamArr ('Object')
              ELSE
                  NULL
          END;
      v_DayType :=
          CASE
              WHEN v_CalParamArr.EXISTS ('DayType')
              THEN
                  TO_NUMBER (v_CalParamArr ('DayType'))
              ELSE
                  0
          END;

      SELECT COUNT (*)
        INTO v_recCount
        FROM DDLCALENOPRS_DBT dlcalenoprs
       WHERE     dlcalenoprs.t_NAME = v_operName
             AND dlcalenoprs.t_IDENTPROGRAM = p_identProgram
             AND dlcalenoprs.t_OBJTYPE =
                 CASE
                     WHEN v_objTypeFromParm > 0 THEN v_objTypeFromParm
                     ELSE dlcalenoprs.t_OBJTYPE
                 END;

      IF (v_recCount = 1)
      THEN
          SELECT t_OBJTYPE
            INTO v_objType
            FROM DDLCALENOPRS_DBT dlcalenoprs
           WHERE     dlcalenoprs.t_NAME = v_operName
                 AND dlcalenoprs.t_IDENTPROGRAM = p_identProgram
                 AND dlcalenoprs.t_OBJTYPE =
                     CASE
                         WHEN v_objTypeFromParm > 0 THEN v_objTypeFromParm
                         ELSE dlcalenoprs.t_OBJTYPE
                     END;

          v_CalParamArr ('ObjectType') := TO_CHAR (v_objType);
      END IF;

      IF (    (GREATEST (NVL(v_objTypeFromParm,-1), v_objType) = RSI_DlCalendars.DL_CALLNK_MARKET)
          AND (v_DayType = 0))
      THEN
          v_CalParamArr ('DayType') := RSI_DlCalendars.DL_CALLNK_MRKTDAY_TRADE;
      END IF;

      l_sql := 'SELECT prm.*, 0 ';

      BEGIN
          FOR param_knd IN (SELECT * FROM DDLCALPARAMKND_DBT)
          LOOP
              IF (v_CalParamArr.EXISTS (param_knd.T_CODE))
              THEN
                  l_sql :=
                         l_sql
                      || ' + COALESCE (
                 (SELECT 1
                    FROM DDLCALPARAMLNK_DBT lnk
                   WHERE     lnk.T_CALPARAMID = prm.T_ID
                         AND lnk.T_KNDCODE = '''
                      || param_knd.T_CODE
                      || '''
                         AND lnk.T_VALUE = '''
                      || v_CalParamArr (param_knd.T_CODE)
                      || '''),
                 0)';
                  l_whereSql :=
                         l_whereSql
                      || ' AND NOT EXISTS
                   (SELECT 1
                      FROM DDLCALPARAMLNK_DBT lnk
                     WHERE     lnk.T_CALPARAMID = prm.T_ID
                           AND lnk.T_KNDCODE = '''
                      || param_knd.T_CODE
                      || '''
                           AND lnk.T_VALUE <> '''
                      || v_CalParamArr (param_knd.T_CODE)
                      || ''')';
              ELSE
                  l_sql :=
                         l_sql
                      || ' + COALESCE (
                 CASE
                     WHEN NOT EXISTS
                              (SELECT 1
                                 FROM DDLCALPARAMLNK_DBT lnk
                                WHERE     lnk.T_CALPARAMID = prm.T_ID
                                      AND lnk.T_KNDCODE = '''
                      || param_knd.T_CODE
                      || ''')
                     THEN
                         1
                     ELSE
                         NULL
                 END,
                 0)';
              END IF;
          END LOOP;

          l_sql :=
                 l_sql
              || ' AS t_Count
              FROM DDLCALPARAM_DBT prm
     WHERE     T_IDENTPROGRAM = '
              || TO_CHAR (p_identProgram)
              || l_whereSql
              || ' ORDER BY t_count DESC';

          l_sql :=
              'select T_CALKINDID, T_COUNT from (' || l_sql || ') q1';

          OPEN v_Cur for l_sql;

          FETCH v_Cur BULK COLLECT INTO p_CalendTable;

          CLOSE v_Cur;

          FOR i IN p_CalendTable.FIRST .. p_CalendTable.LAST
          LOOP
              IF (p_maxDaysCnt > 0) THEN
                IF (((CASE WHEN RSI_RSBCALENDAR.IsWorkDay(p_Date, p_CalendTable(i).T_CALKINDID)  = 1
                           THEN p_Date 
                           ELSE RSI_RSBCALENDAR.GetDateAfterWorkDay(p_Date, -1, p_CalendTable(i).T_CALKINDID) 
                      END) - p_SinceDate) <= p_maxDaysCnt) THEN
                   RETURN 0;
                END IF;
              ELSE
                IF ((CASE WHEN RSI_RSBCALENDAR.IsWorkDay(p_Date, p_CalendTable(i).T_CALKINDID)  = 1
                         THEN p_Date 
                         ELSE RSI_RSBCALENDAR.GetDateAfterWorkDay(p_Date, -1, p_CalendTable(i).T_CALKINDID) 
                         END) = p_SinceDate) THEN
                   RETURN 0;
                END IF;
              END IF;
          END LOOP;

         RETURN 1;

      EXCEPTION
          WHEN OTHERS
          THEN
              NULL;
      END;

      RETURN 1;
  END;

  FUNCTION GetRateU (pFromFI IN NUMBER, pDate IN DATE, pToFI IN NUMBER, pRateType IN NUMBER, pMarketID IN NUMBER)
  RETURN NUMBER
  DETERMINISTIC
  IS
    v_SinceDate DATE;
    v_Rate      NUMBER := 1.0;
    v_err       NUMBER;
    p_calparamarr RSI_DlCalendars.calparamarr_t;
  BEGIN
    v_Rate := RSB_SPREPFUN.GetRateOnDateCrossDbl(pDate, pFromFI, pToFI, 0, pRateType, v_err, v_SinceDate, pMarketID);

    IF v_Rate IS NULL OR v_Rate = 0.0 THEN
      RETURN NULL;
    END IF;

    p_calparamarr('Market') := 2;
    p_calparamarr('DayType') := RSI_DlCalendars.DL_CALLNK_MRKTDAY_TRADE;
    p_calparamarr('ObjectType') := RSI_DlCalendars.DL_CALLNK_MARKET;

    IF (pRateType in (RATETYPE_NKD, RATETYPE_CLOSE)) THEN   --для указанных курсов проверка в пределах 1 дня
      IF (IsDateAfterWorkDayM(pDate, v_SinceDate, 83, 0, p_calparamarr) = 1)
        THEN
          RETURN NULL;
      END IF;
    ELSE
      IF (IsDateAfterWorkDayM(pDate, v_SinceDate, 83,90, p_calparamarr) = 1) 
        THEN
          RETURN NULL;
      END IF;
    END IF;

    RETURN v_Rate;
  END GetRateU;

  PROCEDURE GetRate90 (pBO IN NUMBER, pFIID IN NUMBER, pDate IN DATE, pFIID_Nom IN NUMBER, pMP OUT NUMBER, pDealCode OUT VARCHAR)
  DETERMINISTIC
  IS
    v_fiid     NUMBER;
    v_Rate     NUMBER;
  BEGIN
    /*--индекс DDL_TICK_DBT_IDXC так же включает в себя сортировку
    select \*+ result_cache index(t DDL_TICK_DBT_IDXC) *\
           leg.t_cost / leg.t_principal mp, leg.t_cfi as fiid, t.t_dealcode code
      INTO pMP, v_fiid, pDealCode
      from  ddl_tick_dbt t, ddl_leg_dbt leg
     WHERE t.t_bofficekind = pBO
       AND t.t_dealstatus > 0 --DL_PREPARING
       AND t.t_pfi = pFIID
       AND t.t_dealdate >= pDate - 90
       AND t.t_dealdate <= pDate
       AND leg.t_dealid = t.t_dealid
       AND leg.t_legkind = 0
       and leg.t_legid = 0
       and rownum < 2;*/

    select /*+ result_cache */
           mp, fiid, code
      INTO pMP, v_fiid, pDealCode
    from ( select /*+ index(t DDL_TICK_DBT_IDXE) */
           leg.t_cost / leg.t_principal mp, leg.t_cfi as fiid, t.t_dealcode code
      from  ddl_tick_dbt t, ddl_leg_dbt leg
     WHERE t.t_bofficekind = pBO
       AND t.t_dealstatus > 0 --DL_PREPARING
       AND t.t_pfi = pFIID
       AND t.t_dealdate >= pDate - 90
       AND t.t_dealdate <= pDate
       AND leg.t_dealid = t.t_dealid
       AND leg.t_legkind = 0
       and leg.t_legid = 0
       order by t.t_dealdate desc,t.t_dealtime desc)
       where rownum < 2;

    IF v_fiid != RSI_RSB_FIINSTR.NATCUR THEN
      IF v_fiid != pFIID_Nom THEN
        v_Rate := RSI_RSB_FIINSTR.CalcSumCross (1.0, v_fiid, pFIID_Nom, pDate, 0);
      ELSE
        v_Rate := RSI_RSB_FIINSTR.CalcSumCross (1.0, v_fiid, RSI_RSB_FIINSTR.NATCUR, pDate, 0);
      END IF;

      IF v_Rate IS NULL OR v_Rate = 0.0 THEN
        pMP := NULL;
        RETURN;
      END IF;
    END IF;

    IF v_fiid != pFIID_Nom THEN
      pMP := v_Rate * pMP;
    END IF;
  EXCEPTION
    WHEN OTHERS
      THEN RETURN;
  END GetRate90;

  FUNCTION GetAvoirMarketPrice (pFIID        IN  NUMBER, -- ФИ бумаги
                                pVN          IN  NUMBER, -- валюта номинала
                                pDate        IN  DATE,
                                pMarketId    IN  NUMBER)
  RETURN AvrMPData_t deterministic
  IS
    v_MarketPrice  NUMBER;
    v_FindCourseFI NUMBER := 0;
    v_DealCode     ddl_tick_dbt.t_DealCode%type;
    v_Rate         NUMBER;
    v_result       AvrMPData_t;
  BEGIN
    v_Rate := RSI_RSB_FIINSTR.CalcSumCross (1.0, pVN, RSI_RSB_FIINSTR.NATCUR, pDate, 0);
    IF v_Rate IS NOT NULL AND v_Rate <> 0.0 THEN
      IF pFIID != RSI_RSB_FIINSTR.NATCUR THEN
        --Рыночная цена
        v_FindCourseFi := GetLastRateOnDateByType (pFIID, pDate, RATETYPE_MARKET_PRICE, pMarketID);
        --DBMS_OUTPUT.PUT_LINE('fvfi='||v_FindCourseFi);
        IF v_FindCourseFi IS NOT NULL THEN
          v_MarketPrice := GetRateU (pFIID, pDate, v_FindCourseFI, RATETYPE_MARKET_PRICE, pMarketID);
          IF v_MarketPrice IS NOT NULL AND pVN != v_FindCourseFI THEN
            v_Rate := RSI_RSB_FIINSTR.CalcSumCross (1.0, v_FindCourseFI, RSI_RSB_FIINSTR.NATCUR, pDate, 0);
          END IF;
        END IF;

        --Котировка Bloomberg
        IF v_MarketPrice IS NULL OR v_MarketPrice = 0.0 THEN
          v_FindCourseFi := GetLastRateOnDateByType (pFIID, pDate, RATETYPE_BLOOMBERG_PRICE, pMarketID);
          IF v_FindCourseFi IS NOT NULL THEN
            v_MarketPrice := GetRateU (pFIID, pDate, v_FindCourseFI, RATETYPE_BLOOMBERG_PRICE, pMarketID);
            IF v_MarketPrice IS NOT NULL AND pVN != v_FindCourseFI THEN
              v_Rate := RSI_RSB_FIINSTR.CalcSumCross (1.0, v_FindCourseFI, RSI_RSB_FIINSTR.NATCUR, pDate, 0);
            END IF;
          END IF;
        END IF;

        --Последняя сделка за 90 дней
        IF v_MarketPrice IS NULL OR v_MarketPrice = 0.0 THEN
          IF RSB_SECUR.GetObjAttrNumber (RSB_SECUR.OBJTYPE_AVOIRISS,
                                         NOT_USE_DEALS_ATTR_GRP,
                                         RSB_SECUR.GetMainObjAttr (RSB_SECUR.OBJTYPE_AVOIRISS, LPAD (pFIID, 10, '0'), NOT_USE_DEALS_ATTR_GRP, pDate) ) != '0' THEN
            GetRate90 (RSB_SECUR.DL_SECURITYDOC, pFIID, pDate, pVN, v_MarketPrice, v_DealCode);
            IF v_MarketPrice IS NOT NULL THEN
              v_result.t_Message := 'Котировку для ц/б ' || GetFIName(pFIID) || ' равную ' || v_MarketPrice || ' определил по сделке ' || v_DealCode;
            END IF;
          END IF;
        END IF;

        --Мотивированное суждение
        IF v_MarketPrice IS NULL OR v_MarketPrice = 0.0 THEN
          v_FindCourseFi := GetLastRateOnDateByType (pFIID, pDate, RATETYPE_REASONED_PRICE, pMarketID);
          IF v_FindCourseFi IS NOT NULL THEN
            v_MarketPrice := GetRateU (pFIID, pDate, v_FindCourseFI, RATETYPE_REASONED_PRICE, pMarketID);
            IF v_MarketPrice IS NOT NULL AND pVN != v_FindCourseFI THEN
              v_Rate := RSI_RSB_FIINSTR.CalcSumCross (1.0, v_FindCourseFI, RSI_RSB_FIINSTR.NATCUR, pDate, 0);
            END IF;
          END IF;
        END IF;

        IF v_MarketPrice IS NULL OR v_MarketPrice = 0.0 THEN
          v_result.t_Message := 'Не определена рыночная стоимость ц/б "' || GetFIName(pFIID) || '"';
        END IF;
      ELSE
        v_MarketPrice := 1.0;
      END IF;
    END IF;
    --DBMS_OUTPUT.PUT_LINE('market_price='||v_MarketPrice);
    v_result.t_MarketPrice  := v_MarketPrice;
    v_result.t_Rate         := v_Rate;
    v_result.t_FindCourseFI := v_FindCourseFi;

    RETURN v_result;
  END GetAvoirMarketPrice;

  FUNCTION GetAvoirValue (pVN          IN  NUMBER,
                          pFVFI        IN  NUMBER,
                          pCount       IN  NUMBER,
                          pDate        IN  DATE,
                          pUseNKDRate  IN  BOOLEAN,
                          pMarketId    IN  NUMBER)
  RETURN AvrValueData_t
  IS
    v_MPData AvrMPData_t;
    v_result AvrValueData_t;
    v_NKD     NUMBER;
    v_NKDRate NUMBER := 0;
  BEGIN
    v_MPData := GetAvoirMarketPrice(pVN, pFVFI, pDate, pMarketId);

    IF v_MPData.t_Message IS NOT NULL THEN
      RSB_SPREPFUN.AddRepError(v_MPData.t_Message);
    END IF;

    IF    v_MPData.t_MarketPrice IS NOT NULL
      AND v_MPData.t_MarketPrice > 0.0
    THEN
      IF (pUseNKDRate) THEN
        v_NKDRate := GetRateU (pVN, pDate, pFVFI, RATETYPE_NKD, pMarketId);
        IF (v_NKDRate IS NOT NULL) THEN
          v_NKD := v_NKDRate * pCount;
        ELSE
          v_NKD := 0;
        END IF;
      ELSE
         v_NKD := RSI_RSB_FIINSTR.CalcNKD (pVN, pDate, pCount, 0);
      END IF;

      v_result.t_Sum          := v_MPData.t_MarketPrice * pCount + v_NKD;
      v_result.t_SumRub       := v_result.t_Sum * v_MPData.t_Rate;
      v_result.t_Course       := v_MPData.t_MarketPrice + v_NKD / pCount;
      v_result.t_Rate         := v_MPData.t_Rate;
      v_result.t_NKD          := v_NKD;
      v_result.t_NKDRub       := v_NKD * v_MPData.t_Rate;
      v_result.t_MarketPrice  := v_MPData.t_MarketPrice;
      v_result.t_MarketPriceRub := v_MPData.t_MarketPrice * v_MPData.t_Rate;
      v_result.t_FindCourseFi := v_MPData.t_FindCourseFi;
    END IF;

    return v_result;
  END GetAvoirValue;

  FUNCTION GetAnyLastCourseOnDate (pRateDate IN DATE, pFIID IN NUMBER)
  RETURN NUMBER
  IS
    v_rateid NUMBER;
    v_type   NUMBER;
    v_FIID   NUMBER;
    v_Course NUMBER;
  BEGIN
    SELECT t_rateid, t_type, t_fiid
      INTO v_rateid, v_type, v_fiid
      FROM (SELECT t_rateid, t_type, t_fiid
              FROM (SELECT rate.t_rateid, rate.t_sincedate, rate.t_type, rate.t_fiid
                      FROM dratedef_dbt rate
                     WHERE rate.t_otherfi = pFIID
                       AND t_sincedate = (SELECT MAX(t_sincedate)
                                            FROM dratedef_dbt
                                           WHERE t_otherfi = rate.t_otherfi
                                             AND t_type = rate.t_type
                                             AND t_sincedate <= pRateDate
                                         )
                     UNION ALL
                    SELECT r.t_rateid, h.t_sincedate, r.t_type, r.t_fiid
                      FROM dratehist_dbt h, dratedef_dbt r
                     WHERE r.t_rateid = h.t_rateid
                       AND r.t_otherfi = pFIID
                       AND h.t_sincedate = ( SELECT MAX(h2.t_sincedate)
                                               FROM dratehist_dbt h2, dratedef_dbt r2
                                              WHERE r2.t_rateid = h2.t_rateid
                                                AND r2.t_otherfi = r.t_otherfi
                                                AND r2.t_type = r.t_type
                                                AND h2.t_sincedate <= pRateDate
                                           )
                   )
            ORDER BY t_sincedate DESC
           )
    WHERE ROWNUM = 1;

    v_Course := RSI_RSB_FIINSTR.ConvSumType (1.0, pFIID, v_FIID, v_Type, pRateDate);
    IF v_Course IS NOT NULL AND v_FIID != RSI_RSB_FIINSTR.NATCUR THEN
      v_Course := RSI_RSB_FIINSTR.ConvSum (v_Course, v_FIID, RSI_RSB_FIINSTR.NATCUR, pRateDate);
    END IF;

    IF v_Course IS NULL THEN
      RSB_SPREPFUN.AddRepError(RSB_SPREPFUN.GetCurrencyConvertErrorMsg(pFIID, v_FIID, pRateDate));
      RETURN NULL;
    END IF;

    RETURN v_Course;
  EXCEPTION
    WHEN NO_DATA_FOUND
      THEN RETURN NULL;
  END GetAnyLastCourseOnDate;


  -- Для раздела 8
  PROCEDURE CollectPFIDeals(pSessionID IN NUMBER, pClientContrId IN NUMBER, pCONTR_GROUPID IN VARCHAR2,
                           pParty IN NUMBER, pCLIENT_GROUPID IN VARCHAR2, pEndDate IN DATE)
  IS
    c_MPType CONSTANT NUMBER := Rsb_Common.GetRegIntValue('ПРОИЗВОДНЫЕ ИНСТРУМЕНТЫ\ВИД КУРСА "РЫНОЧНАЯ ЦЕНА"', 0);

    v_PFIData d724pfi_dbt%rowtype;

    v_PriceRub        NUMBER;
    v_Sum             NUMBER;
    v_BAPriceRub      NUMBER;
    v_MarketPrice     NUMBER;
    v_Rate            NUMBER;
    v_WorkDate        DATE := pEndDate;
    v_CostPFI_FO      NUMBER;
    
    p_calparamarr RSI_DlCalendars.calparamarr_t;
    v_calendId    NUMBER := 0;
  BEGIN
    p_calparamarr('Market') := 2;
    p_calparamarr('MarketPlace') := RSI_DlCalendars.DL_CALLNK_MARKETPLACE_DV;
    p_calparamarr('DayType') := RSI_DlCalendars.DL_CALLNK_MRKTDAY_TRADE;
    p_calparamarr('ObjectType') := RSI_DlCalendars.DL_CALLNK_MARKET;
    v_calendId := RSI_DlCalendars.DL_GetCalendByDynParam(158,p_calparamarr);
    if (RSI_RSBCALENDAR.IsWorkDay(v_WorkDate, v_calendId) != 1) then
      v_WorkDate := RSI_RSBCALENDAR.GetDateAfterWorkDay(v_WorkDate, -1, v_calendId);
    end if; 
    
    FOR one_rec IN ( SELECT ndeal.t_ID,
                            ndeal.t_DVKind,
                            ndeal.t_DocKind,
                            '' t_ExtCode,
                            '' t_ExtName,
                            case when ndeal.t_DVKind = RSB_DERIVATIVES.DV_OPTION then 'Опцион'
                                 when ndeal.t_DVKind = RSB_DERIVATIVES.DV_FORWARD AND ndeal.t_Sector = 'X' AND ndeal.t_MarketKind = 2 then 'Валютный фьючерс'
                                 when ndeal.t_DVKind = RSB_DERIVATIVES.DV_FORWARD then 'Форвард'
                                 when ndeal.t_DVKind = RSB_DERIVATIVES.DV_CURSWAP then 'Валютный своп'
                                 when ndeal.t_DVKind = RSB_DERIVATIVES.DV_CURSWAP_FX then 'Своп'
                                 when ndeal.t_DVKind = RSB_DERIVATIVES.DV_PCTSWAP AND nfi2.t_FIID <> nfi.t_FIID then 'CIRS'
                                 when ndeal.t_DVKind = RSB_DERIVATIVES.DV_PCTSWAP then 'IRS' else 'Иное' end t_PFIName,
                            nfi.t_Amount,
                            case when ndeal.t_Type in (RSB_DERIVATIVES.ALG_DV_BUY, RSB_DERIVATIVES.ALG_DV_SB) then 1
                                 when ndeal.t_Type in (RSB_DERIVATIVES.ALG_DV_SALE, RSB_DERIVATIVES.ALG_DV_BS) then 2 else 0 end t_Dir,
                            fin.t_Name t_BAName,
                            fin.t_FIID t_BA_FIID,
                            fin.t_FI_Kind,
                            fin.t_FaceValueFI,
                            nfi.t_Price,
                            nfi.t_PriceFIID,
                            ndeal.t_Forvard,
                            ndeal.t_Bonus,
                            ndeal.t_BonusFIID,
                            (SELECT NVL(sum(RSI_RSB_FIINSTR.ConvSum(paym.t_Amount, paym.t_PayFIID, RSI_RSB_FIINSTR.NATCUR, pEndDate)), 0)
                               FROM dpmpaym_dbt paym
                              WHERE paym.t_DocKind = ndeal.t_DocKind
                                AND paym.t_DocumentID = ndeal.t_ID
                                AND paym.t_Purpose = PM_PURP_GARANT
                                AND paym.t_ValueDate <= pEndDate
                                AND paym.t_PaymStatus <> RSB_PAYMENT.PM_REJECTED
                            ) t_Guaranty
                       FROM ddvndeal_dbt ndeal LEFT OUTER JOIN ddvnfi_dbt nfi2 ON nfi2.t_DealID = ndeal.t_ID AND nfi2.t_Type = 2, ddvnfi_dbt nfi, dfininstr_dbt fin, doproper_dbt oper
                      WHERE ndeal.t_Client = pParty
                        AND ndeal.t_ClientContr = pClientContrId
                        AND ndeal.t_Date <= pEndDate
                        AND nfi.t_DealID = ndeal.t_ID
                        AND nfi.t_Type = case when ndeal.t_Forvard = 'X' then 1 else 0 end
                        AND fin.t_FIID = nfi.t_FIID
                        AND oper.t_DocKind = ndeal.t_DocKind
                        AND oper.t_DocumentID = lpad(ndeal.t_ID, 34, '0')
                        AND ndeal.t_State > 0
                        AND EXISTS (SELECT 1
                                      FROM dpmpaym_dbt paym
                                     WHERE paym.t_DocKind = ndeal.t_DocKind
                                       AND paym.t_DocumentID = ndeal.t_ID
                                       AND paym.t_ValueDate > v_WorkDate
                                       AND paym.t_PaymStatus <> RSB_PAYMENT.PM_REJECTED)
                        AND (    ndeal.t_DVKind IN (RSB_DERIVATIVES.DV_FORWARD, RSB_DERIVATIVES.DV_OPTION, RSB_DERIVATIVES.DV_PCTSWAP)
                             OR (    ndeal.t_DVKind IN (RSB_DERIVATIVES.DV_CURSWAP, RSB_DERIVATIVES.DV_CURSWAP_FX)
                                 AND NOT EXISTS (SELECT 1
                                                   FROM dpmpaym_dbt paym
                                                  WHERE paym.t_DocKind = ndeal.t_DocKind
                                                    AND paym.t_DocumentID = ndeal.t_ID
                                                    AND paym.t_Purpose IN (RSB_PAYMENT.BAi, RSB_PAYMENT.CAi)   -- Платежи по 1ч
                                                    AND paym.t_ValueDate > v_WorkDate
                                                    AND paym.t_PaymStatus <> RSB_PAYMENT.PM_REJECTED)
                                 AND EXISTS (SELECT 1
                                               FROM dpmpaym_dbt paym
                                              WHERE paym.t_DocKind = ndeal.t_DocKind
                                                AND paym.t_DocumentID = ndeal.t_ID
                                                AND paym.t_Purpose IN (RSB_PAYMENT.BRi, RSB_PAYMENT.CRi)   -- Платежи по 2ч
                                                AND paym.t_ValueDate > v_WorkDate
                                                AND paym.t_PaymStatus <> RSB_PAYMENT.PM_REJECTED)
                                )
                            ))
    LOOP
      v_CostPFI_FO := 0;
      IF one_rec.t_DocKind = RSB_DERIVATIVES.DL_DVNDEAL
        AND one_rec.t_DVKind = RSB_DERIVATIVES.DV_FORWARD
      THEN
        IF one_rec.t_PriceFIID != RSI_RSB_FIINSTR.NATCUR THEN
          IF RSB_SPREPFUN.SmartConvertSum(v_PriceRub, one_rec.t_Price, pEndDate, one_rec.t_PriceFIID, RSI_RSB_FIINSTR.NATCUR, 1) != 0
          THEN
            CONTINUE;
          END IF;
        ELSE
          v_PriceRub := one_rec.t_Price;
        END IF;

        IF   one_rec.t_FI_Kind = RSI_RSB_FIINSTR.FIKIND_CURRENCY
          OR one_rec.t_FI_Kind = RSI_RSB_FIINSTR.FIKIND_METAL
        THEN
          IF RSB_SPREPFUN.SmartConvertSum(v_BAPriceRub, 1.0, pEndDate, one_rec.t_BA_FIID, RSI_RSB_FIINSTR.NATCUR, 1) != 0
          THEN
            CONTINUE;
          END IF;
        ELSIF  one_rec.t_FI_Kind = RSI_RSB_FIINSTR.FIKIND_AVOIRISS THEN
          v_MarketPrice := RSI_RSB_FIINSTR.ConvSumType (1.0, one_rec.t_BA_FIID, one_rec.t_FaceValueFI, c_MPType, pEndDate);
          IF v_MarketPrice IS NOT NULL THEN
            IF one_rec.t_FaceValueFI != RSI_RSB_FIINSTR.NATCUR THEN
              IF RSB_SPREPFUN.SmartConvertSum(v_Rate, 1.0, pEndDate, one_rec.t_FaceValueFI, RSI_RSB_FIINSTR.NATCUR, 1) != 0
              THEN
                CONTINUE;
              END IF;
            ELSE
              v_Rate := 1;
            END IF;

            v_BAPriceRub := v_Rate * v_MarketPrice;
          ELSE
            v_BAPriceRub := 0;
            RSB_SPREPFUN.AddRepError('Не определена рыночная стоимость ц/б "' || one_rec.t_BAName || '"');
            CONTINUE;
          END IF;
        ELSE
          v_BAPriceRub := GetAnyLastCourseOnDate (pEndDate, one_rec.t_BA_FIID);
          IF v_BAPriceRub IS NULL THEN
            CONTINUE;
          END IF;
        END IF;

        v_Sum := (v_BAPriceRub - v_PriceRub) * one_rec.t_Amount;
        v_CostPFI_FO := v_Sum;

      ELSIF (   one_rec.t_DocKind = RSB_DERIVATIVES.DL_DVNDEAL
            AND one_rec.t_DVKind = RSB_DERIVATIVES.DV_OPTION)
      THEN
        IF one_rec.t_BonusFIID != RSI_RSB_FIINSTR.NATCUR THEN
          IF RSB_SPREPFUN.SmartConvertSum(v_PriceRub, one_rec.t_Bonus, pEndDate, one_rec.t_BonusFIID, RSI_RSB_FIINSTR.NATCUR, 1) != 0
          THEN
            CONTINUE;
          END IF;
        ELSE
          v_PriceRub := one_rec.t_Bonus;
        END IF;
        v_Sum := v_PriceRub;
        v_CostPFI_FO := v_Sum;
/*
        IF   one_rec.t_FI_Kind = RSI_RSB_FIINSTR.FIKIND_CURRENCY
          OR one_rec.t_FI_Kind = RSI_RSB_FIINSTR.FIKIND_METAL
        THEN
          IF RSB_SPREPFUN.SmartConvertSum(v_BAPriceRub, 1.0, pEndDate, one_rec.t_BA_FIID, RSI_RSB_FIINSTR.NATCUR, 1) != 0
          THEN
            CONTINUE;
          END IF;
        ELSIF  one_rec.t_FI_Kind = RSI_RSB_FIINSTR.FIKIND_AVOIRISS THEN
          v_MarketPrice := RSI_RSB_FIINSTR.ConvSumType (1.0, one_rec.t_BA_FIID, one_rec.t_FaceValueFI, c_MPType, pEndDate);
          IF v_MarketPrice IS NOT NULL THEN
            IF one_rec.t_FaceValueFI != RSI_RSB_FIINSTR.NATCUR THEN
              IF RSB_SPREPFUN.SmartConvertSum(v_Rate, 1.0, pEndDate, one_rec.t_FaceValueFI, RSI_RSB_FIINSTR.NATCUR, 1) != 0
              THEN
                CONTINUE;
              END IF;
            ELSE
              v_Rate := 1;
            END IF;

            v_BAPriceRub := v_Rate * v_MarketPrice;
          ELSE
            v_BAPriceRub := 0;
            RSB_SPREPFUN.AddRepError('Не определена рыночная стоимость ц/б "' || one_rec.t_BAName || '"');
            CONTINUE;
          END IF;
        ELSE
          v_BAPriceRub := GetAnyLastCourseOnDate (pEndDate, one_rec.t_BA_FIID);
          IF v_BAPriceRub IS NULL THEN
            CONTINUE;
          END IF;
        END IF;

        v_Sum := (v_BAPriceRub - v_PriceRub) * one_rec.t_Amount;
*/
      ELSIF one_rec.t_DVKind = RSB_DERIVATIVES.DV_PCTSWAP THEN
        v_Sum := 0;
        IF RSB_SPREPFUN.SmartConvertSum(v_CostPFI_FO, one_rec.t_Amount, pEndDate, one_rec.t_BA_FIID, RSI_RSB_FIINSTR.NATCUR, 1) != 0
        THEN
          CONTINUE;
        END IF;

      ELSIF one_rec.t_DVKind = RSB_DERIVATIVES.DV_CURSWAP
         OR one_rec.t_DVKind = RSB_DERIVATIVES.DV_CURSWAP_FX
      THEN
        IF RSB_SPREPFUN.SmartConvertSum(v_Sum, one_rec.t_Amount, pEndDate, one_rec.t_BA_FIID, RSI_RSB_FIINSTR.NATCUR, 1) != 0
        THEN
          v_Sum := 0;
          CONTINUE;
        END IF;
        v_CostPFI_FO := v_Sum;
      END IF;

      v_PFIData.t_SessionID      := pSessionID;
      v_PFIData.t_Sysdate        := sysdate;
      v_PFIData.t_ContrID        := pClientContrId;
      v_PFIData.t_Contr_GroupID  := pCONTR_GROUPID;
      v_PFIData.t_PartyID        := pParty;
      v_PFIData.t_Client_GroupID := pCLIENT_GROUPID;
      v_PFIData.t_DocKind        := one_rec.t_DocKind;
      v_PFIData.t_DocID          := one_rec.t_ID;
      v_PFIData.t_ExtCode        := one_rec.t_ExtCode;
      v_PFIData.t_ExtName        := TRIM(one_rec.t_ExtName);
      v_PFIData.t_PFIName        := one_rec.t_PFIName; -- по нему группировка
      v_PFIData.t_Amount         := CASE
                                      WHEN one_rec.t_DocKind = RSB_DERIVATIVES.DL_DVDEAL
                                        OR one_rec.t_DVKind IN (RSB_DERIVATIVES.DV_CURSWAP, RSB_DERIVATIVES.DV_CURSWAP_FX)
                                      THEN one_rec.t_Amount
                                      ELSE 1
                                    END;
      v_PFIData.t_Dir            := one_rec.t_Dir;
      v_PFIData.t_CostPFI        := v_Sum;
      v_PFIData.t_Guaranty       := one_rec.t_Guaranty;
      v_PFIData.t_CostPFI_FO     := v_CostPFI_FO;

      INSERT INTO d724pfi_dbt VALUES v_PFIData;
    END LOOP;

  END CollectPFIDeals;
  
    -- Для раздела 8
  PROCEDURE CollectPFITurn(pSessionID IN NUMBER, pClientContrId IN NUMBER, pCONTR_GROUPID IN VARCHAR2,
                           pParty IN NUMBER, pCLIENT_GROUPID IN VARCHAR2, pEndDate IN DATE, pUseNKDRate BOOLEAN)
  IS
    v_PFIData d724pfi_dbt%rowtype;

    v_PriceRub        NUMBER;
    v_Sum             NUMBER;
    v_WorkDate        DATE := pEndDate;
    
    v_ClosePrice      NUMBER;
    v_MinStepPrice    NUMBER;
    v_FindCourseFI    NUMBER := 0;
    v_CostPFI_FO      NUMBER := 0;
    v_AvrValue        AvrValueData_t;
    
    p_calparamarr RSI_DlCalendars.calparamarr_t;
    v_calendId    NUMBER := 0;
  BEGIN
    p_calparamarr('Market') := 2;
    p_calparamarr('MarketPlace') := RSI_DlCalendars.DL_CALLNK_MARKETPLACE_DV;
    p_calparamarr('DayType') := RSI_DlCalendars.DL_CALLNK_MRKTDAY_TRADE;
    p_calparamarr('ObjectType') := RSI_DlCalendars.DL_CALLNK_MARKET;
    v_calendId := RSI_DlCalendars.DL_GetCalendByDynParam(158,p_calparamarr);
    if (RSI_RSBCALENDAR.IsWorkDay(v_WorkDate, v_calendId) != 1) then
      v_WorkDate := RSI_RSBCALENDAR.GetDateAfterWorkDay(v_WorkDate, -1, v_calendId);
    end if; 
    
    FOR one_rec IN ( SELECT fiturn.t_ID,
                            fiturn.t_FIID,
                            fin.t_AvoirKind t_DVKind,
                            RSB_DERIVATIVES.DL_DVFIPOS t_DocKind,
                            NVL(rsb_secur.SC_GetObjCodeOnDate(CNST.OBJTYPE_FININSTR, 11, fin.t_FIID, pEndDate),'') t_ExtCode,
                            NVL((SELECT case when t_shortname = chr(1) then t_name else t_shortname end t_ShortName FROM dparty_dbt WHERE t_PartyID = fin.t_Issuer),'') t_ExtName,
                            fin.t_Name t_PFIName,
                            (CASE WHEN fiturn.t_LongPosition - fiturn.t_ShortPosition >= 0 THEN 1
                                  ELSE 2
                              END
                            ) t_Dir,
                            base.t_Name t_BAName,
                            base.t_FIID t_BA_FIID,
                            base.t_FI_Kind,
                            base.t_FaceValueFI,
                            base.t_AvoirKind,
                            deriv.t_Strike t_Price,
                            deriv.t_IsMarginOp,
                            fin.t_FaceValue,
                            case when fin.t_AvoirKind = RSB_DERIVATIVES.DV_DERIVATIVE_FUTURES AND base.t_FI_Kind = 3 THEN fin.t_ParentFI
                                  when fin.t_AvoirKind = RSB_DERIVATIVES.DV_DERIVATIVE_FUTURES then deriv.t_TickFIID
                                  else deriv.t_StrikeFIID
                            end t_PriceFIID,
                            deriv.t_tick,
                            abs(fiturn.t_LongPosition - fiturn.t_ShortPosition) t_PosAmount,
                            case when base.t_FIID <> fin.t_FaceValueFI then 'X' else chr(0) end t_Forvard,
                            nvl(rsi_rsb_fiinstr.convsum(fiturn.t_Guaranty, fin.t_ParentFI, RSI_RSB_FIINSTR.NATCUR, pEndDate), 0) t_Guaranty
                       FROM dfininstr_dbt fin, dfininstr_dbt base, dfideriv_dbt deriv, ddvfiturn_dbt fiturn
                      WHERE fiturn.t_Client = pParty
                        AND fiturn.t_ClientContr = pClientContrId
                        AND fin.t_FIID = deriv.t_FIID 
                        AND fiturn.t_FIID = deriv.t_FIID
                        and base.t_FIID = case when NVL((SELECT fin2.t_FI_Kind FROM dfininstr_dbt fin2 WHERE fin2.t_FIID = fin.t_FaceValueFI), -1) = RSI_RSB_FIINSTR.FIKIND_DERIVATIVE
                                               then NVL((SELECT fin2.t_FaceValueFI FROM dfininstr_dbt fin2 WHERE fin2.t_FIID = fin.t_FaceValueFI), -1)
                                               else fin.t_FaceValueFI end
                        AND abs(fiturn.t_LongPosition - fiturn.t_ShortPosition) > 0
                        AND deriv.t_InCirculationDate <= pEndDate
                        AND fiturn.t_Date = v_WorkDate)
    LOOP
        
      IF (NOT (one_rec.t_DVKind = RSB_DERIVATIVES.DV_OPTION AND one_rec.t_IsMarginOp = CHR(88))) THEN    
        v_ClosePrice := 0;
        v_FindCourseFi := GetLastRateOnDateByType (one_rec.t_FIID, pEndDate, RATETYPE_CLOSE, 0);
        IF v_FindCourseFi IS NOT NULL THEN
          v_ClosePrice := GetRateU (one_rec.t_FIID, pEndDate, v_FindCourseFI, RATETYPE_CLOSE, 0);
        END IF;
       
        IF (v_ClosePrice IS NULL) THEN
          v_ClosePrice := 0;
        END IF;
        
        IF (v_ClosePrice = 0) THEN
          v_FindCourseFi := GetLastRateOnDateByType (one_rec.t_FIID, pEndDate, RATETYPE_CALC_PRICE, 0);
          IF v_FindCourseFi IS NOT NULL THEN
            v_ClosePrice := GetRateU (one_rec.t_FIID, pEndDate, v_FindCourseFI, RATETYPE_CALC_PRICE, 0);
          END IF;
        END IF;
        
        IF (v_ClosePrice IS NULL) THEN
          v_ClosePrice := 0;
        END IF;
        
        v_MinStepPrice := 0;
        v_FindCourseFi := GetLastRateOnDateByType (one_rec.t_FIID, pEndDate, RATETYPE_MIN_PRICE_STEP, 0);
        IF v_FindCourseFi IS NOT NULL THEN
          v_MinStepPrice := GetRateU (one_rec.t_FIID, pEndDate, v_FindCourseFI, RATETYPE_MIN_PRICE_STEP, 0);
        END IF;
       
        IF (v_MinStepPrice IS NULL) THEN
          v_MinStepPrice := 0;
        END IF;
       
        IF (one_rec.t_tick != 0) THEN
          v_PriceRub := v_ClosePrice * v_MinStepPrice/one_rec.t_tick;
        ELSE
          v_PriceRub := 0; 
        END IF;
       
        v_Sum := v_PriceRub * one_rec.t_PosAmount;
     
        IF (one_rec.t_DVKind = RSB_DERIVATIVES.DV_DERIVATIVE_FUTURES AND (one_rec.t_FI_Kind = RSI_RSB_FIINSTR.FIKIND_INDEX OR 
          RSB_FIInstr.FI_AvrKindsGetRoot(one_rec.t_FI_KIND, one_rec.t_AvoirKind) = RSI_RSB_FIINSTR.AVOIRISSKIND_BASKET)) THEN
          v_CostPFI_FO := 0;
        ELSE
          IF (one_rec.t_FI_Kind = RSI_RSB_FIINSTR.FIKIND_AVOIRISS) THEN
            v_AvrValue := GetAvoirValue(one_rec.t_BA_FIID, one_rec.t_FaceValueFI, ABS(one_rec.t_FaceValue), pEndDate, pUseNKDRate, -1);
            IF v_AvrValue.t_SumRub IS NULL THEN
              v_CostPFI_FO := 0;
            ELSE
              v_CostPFI_FO := v_AvrValue.t_SumRub * one_rec.t_PosAmount;
            END IF;
          ELSIF (one_rec.t_DVKind = RSB_DERIVATIVES.DV_OPTION AND one_rec.t_AvoirKind = RSB_DERIVATIVES.DV_DERIVATIVE_FUTURES) THEN
            v_ClosePrice := 0;
            
            v_FindCourseFi := GetLastRateOnDateByType (one_rec.t_BA_FIID, pEndDate, RATETYPE_CLOSE, 0);
            IF v_FindCourseFi IS NOT NULL THEN
                v_ClosePrice := GetRateU (one_rec.t_BA_FIID, pEndDate, v_FindCourseFI, RATETYPE_CLOSE, 0);
            ELSE
                v_ClosePrice := 0;
            END IF;
       
            IF (v_ClosePrice IS NULL) THEN
              v_ClosePrice := 0;
            END IF;
        
            v_MinStepPrice := 0;
            v_FindCourseFi := GetLastRateOnDateByType (one_rec.t_BA_FIID, pEndDate, RATETYPE_MIN_PRICE_STEP, 0);
            IF v_FindCourseFi IS NOT NULL THEN
              v_MinStepPrice := GetRateU (one_rec.t_BA_FIID, pEndDate, v_FindCourseFI, RATETYPE_MIN_PRICE_STEP, 0);
            ELSE
              v_MinStepPrice := 0;
            END IF;
       
            IF (v_MinStepPrice IS NULL) THEN
              v_MinStepPrice := 0;
            END IF;
       
            IF (one_rec.t_tick != 0) THEN
              v_PriceRub := v_ClosePrice * v_MinStepPrice/one_rec.t_tick;
            ELSE
              v_PriceRub := 0; 
            END IF;
       
            v_Sum := v_PriceRub * one_rec.t_PosAmount;
            
            v_CostPFI_FO := v_Sum;
        
          ELSE
            v_CostPFI_FO := 0;  
          END IF;
       
        END IF;
      ELSE
        v_PriceRub := 0;
        v_Sum := 0;
        v_CostPFI_FO := 0;
      END IF;

      v_PFIData.t_SessionID      := pSessionID;
      v_PFIData.t_Sysdate        := sysdate;
      v_PFIData.t_ContrID        := pClientContrId;
      v_PFIData.t_Contr_GroupID  := pCONTR_GROUPID;
      v_PFIData.t_PartyID        := pParty;
      v_PFIData.t_Client_GroupID := pCLIENT_GROUPID;
      v_PFIData.t_DocKind        := one_rec.t_DocKind;
      v_PFIData.t_DocID          := one_rec.t_ID;
      v_PFIData.t_ExtCode        := one_rec.t_ExtCode;
      v_PFIData.t_ExtName        := TRIM(one_rec.t_ExtName);
      v_PFIData.t_PFIName        := one_rec.t_PFIName; -- по нему группировка
      v_PFIData.t_Amount         := one_rec.t_PosAmount;
      v_PFIData.t_Dir            := one_rec.t_Dir;
      v_PFIData.t_CostPFI        := v_Sum;
      v_PFIData.t_Guaranty       := one_rec.t_Guaranty;
      v_PFIData.t_CostPFI_FO     := v_CostPFI_FO;

      INSERT INTO d724pfi_dbt VALUES v_PFIData;
    END LOOP;

  END CollectPFITurn;


  PROCEDURE GetSumRQByDeal (pDealID    IN  NUMBER,
                            pIsREPO    IN  NUMBER,
                            pIsSale    IN  NUMBER,
                            pSumTrRub  OUT NUMBER,
                            pSumObRub  OUT NUMBER,
                            pSumTrAmount  OUT NUMBER,
                            pSumObAmount  OUT NUMBER,
                            pSumTr     OUT NUMBER,
                            pSumOb     OUT NUMBER,
                            pMarketID  IN  NUMBER,
                            pEndDate   IN  DATE)
  IS
    v_s         NUMBER;
    v_s_CFI     NUMBER;
    v_s_Amount NUMBER;
    v_sum_pay1  NUMBER := 0; -- сумма ТО типа "оплата" + "аванс" по 1-й части
    v_sum_pay2  NUMBER := 0; -- сумма ТО типа "оплата" + "аванс" по 2-й части
    v_sum_del1  NUMBER := 0; -- сумма ТО типа "поставка" по 1-й части
    v_sum_del2  NUMBER := 0; -- сумма ТО типа "поставка" по 2-й части
    v_sum_Perc  NUMBER := 0; -- сумма ТО типа "проценты"
    
    v_sum_pay1_CFI  NUMBER := 0; -- сумма ТО типа "оплата" + "аванс" по 1-й части
    v_sum_pay2_CFI  NUMBER := 0; -- сумма ТО типа "оплата" + "аванс" по 2-й части
    v_sum_del1_CFI  NUMBER := 0; -- сумма ТО типа "поставка" по 1-й части
    v_sum_del2_CFI  NUMBER := 0; -- сумма ТО типа "поставка" по 2-й части
    v_sum_del1_Amount  NUMBER := 0; -- сумма ТО типа "поставка" по 1-й части
    v_sum_del2_Amount  NUMBER := 0; -- сумма ТО типа "поставка" по 2-й части
    v_sum_Perc_CFI  NUMBER := 0; -- сумма ТО типа "проценты"
    
    v_Kind_Perc NUMBER := -1;-- вид ТО процентов
    v_UseNKDRate BOOLEAN := Rsb_Common.GetRegBoolValue('SECUR\РАСЧЕТ НКД ПО КУРСУ', 0);
    
    v_AvrValue        AvrValueData_t;
  BEGIN
    FOR one_rq IN (SELECT rq.t_Type, rq.t_Kind, rq.t_DealPart, rq.t_Amount, rq.t_FIID, fin.t_FaceValueFI
                     FROM v_rqhistex rq, dfininstr_dbt fin
                    WHERE     fin.t_FIID = rq.t_FIID
                          AND rq.t_State NOT IN (RSI_DLRQ.DLRQ_STATE_EXEC, RSI_DLRQ.DLRQ_STATE_REJECT)
                          AND rq.t_Type IN (RSI_DLRQ.DLRQ_TYPE_PAYMENT, RSI_DLRQ.DLRQ_TYPE_DELIVERY, RSI_DLRQ.DLRQ_TYPE_INCREPO)
                          AND rq.t_DocId = pDealID
                          AND rq.t_DocKind = RSB_SECUR.DL_SECURITYDOC
                          and rq.t_instance = (select MAX(h1.t_instance) from v_rqhistex h1 where h1.t_rqid = rq.t_rqid and h1.t_changedate <= pEndDate))
    LOOP
      IF   one_rq.t_Type = RSI_DLRQ.DLRQ_TYPE_PAYMENT
        OR one_rq.t_Type = RSI_DLRQ.DLRQ_TYPE_INCREPO
      THEN
        v_s_CFI := one_rq.t_Amount;
        IF one_rq.t_FIID = RSI_RSB_FIINSTR.NATCUR THEN
          v_s     := one_rq.t_Amount;
        ELSE
          v_s     := RSI_RSB_FIINSTR.ConvSum (one_rq.t_Amount, one_rq.t_FIID, RSI_RSB_FIINSTR.NATCUR, pEndDate);
        END IF;

        IF one_rq.t_Type = RSI_DLRQ.DLRQ_TYPE_PAYMENT THEN
          IF one_rq.t_DealPart = 1 THEN
            v_sum_pay1 := v_sum_pay1 + v_s;
            v_sum_pay1_CFI := v_sum_pay1_CFI + v_s_CFI;
          ELSE
            v_sum_pay2 := v_sum_pay2 + v_s;
            v_sum_pay2_CFI := v_sum_pay2_CFI + v_s_CFI;
          END IF;
        ELSE
          v_sum_Perc     := v_sum_Perc + v_s;
          v_sum_Perc_CFI := v_sum_Perc_CFI + v_s_CFI;
          v_Kind_Perc := one_rq.t_Kind;
        END IF;
      ELSE
        v_AvrValue := GetAvoirValue (one_rq.t_FIID,
                              one_rq.t_FaceValueFI,
                              one_rq.t_Amount,
                              pEndDate,
                              v_UseNKDRate,
                              pMarketID);
                              
        v_s :=  v_AvrValue.t_SumRub;             
        v_s_CFI := v_AvrValue.t_Sum;
        v_s_Amount := one_rq.t_Amount;
        

        IF v_s IS NOT NULL THEN
          IF one_rq.t_DealPart = 1 THEN
            v_sum_del1 := v_sum_del1 + v_s;
            v_sum_del1_CFI := v_sum_del1_CFI + v_s_CFI;
            v_sum_del1_Amount := v_sum_del1_Amount + v_s_Amount;
            
          ELSE
            v_sum_del2 := v_sum_del2 + v_s;
            v_sum_del2_CFI := v_sum_del2_CFI + v_s_CFI;
            v_sum_del2_Amount := v_sum_del2_Amount + v_s_Amount;
          END IF;
        END IF;
        
      END IF;
    END LOOP;

    IF (v_sum_pay1 <> 0 OR v_sum_del1 <> 0) AND pIsREPO = 1 THEN
      pSumTrRub := 0;
      pSumObRub := 0;
      
      pSumTr := 0;
      pSumOb := 0;
      
      pSumTrAmount := 0;
      pSumObAmount := 0;
      RETURN;
    END IF;

    IF pIsREPO = 1 AND pIsSale = 1 THEN
      pSumTrRub := v_sum_del2;
      pSumObRub := v_sum_pay2;
      
      pSumTr := v_sum_del2_CFI;
      pSumOb := v_sum_pay2_CFI;
      
      pSumTrAmount := v_sum_del2_Amount;
      pSumObAmount := v_sum_pay2_CFI;

      IF v_Kind_Perc = RSI_DLRQ.DLRQ_KIND_REQUEST THEN -- Требование
        pSumTrRub := pSumTrRub + v_sum_Perc;
        pSumTrAmount := pSumTrAmount + v_sum_Perc_CFI;
        pSumTr := pSumTr + v_sum_Perc_CFI;
      ELSIF v_Kind_Perc = RSI_DLRQ.DLRQ_KIND_COMMIT THEN
        pSumObRub := pSumObRub + v_sum_Perc;
        pSumObAmount := pSumObAmount + v_sum_Perc_CFI;
        pSumOb := pSumOb + v_sum_Perc_CFI;
      END IF;

    ELSIF pIsREPO = 1 THEN
      pSumTrRub := v_sum_pay2;
      pSumObRub := v_sum_del2;
      
      pSumTrAmount := v_sum_pay2_CFI;
      pSumObAmount := v_sum_del2_Amount;
      
      pSumTr := v_sum_pay2_CFI;
      pSumOb := v_sum_del2_CFI;
      
      IF v_Kind_Perc = RSI_DLRQ.DLRQ_KIND_REQUEST THEN -- Требование
        pSumTrRub := pSumTrRub + v_sum_Perc;
        pSumTr := pSumTr + v_sum_Perc_CFI;
        pSumTrAmount := pSumTrAmount + v_sum_Perc_CFI;
      ELSIF v_Kind_Perc = RSI_DLRQ.DLRQ_KIND_COMMIT THEN
        pSumObRub := pSumObRub + v_sum_Perc;
        pSumOb := pSumOb + v_sum_Perc_CFI;
        pSumObAmount := pSumObAmount + v_sum_Perc_CFI;
      END IF;

    ELSIF pIsSale = 1 THEN
      pSumTrRub := v_sum_pay1;
      pSumObRub := v_sum_del1;
      
      pSumTr := v_sum_pay1_CFI;
      pSumOb := v_sum_del1_CFI;
      
      pSumTrAmount := v_sum_pay1_CFI;
      pSumObAmount := v_sum_del1_Amount;

    ELSE
      pSumTrRub := v_sum_del1;
      pSumObRub := v_sum_pay1;
      
      pSumTr := v_sum_del1_CFI;
      pSumOb := v_sum_pay1_CFI;
      
      pSumTrAmount := v_sum_del1_Amount;
      pSumObAmount := v_sum_pay1_CFI;
    END IF;
  END GetSumRQByDeal;

  PROCEDURE CollectArrearSCData(pSessionID IN NUMBER, pClientContrId IN NUMBER, pCONTR_GROUPID IN VARCHAR2,
                                pParty IN NUMBER, pCLIENT_GROUPID IN VARCHAR2, pEndDate IN DATE)
  IS
    v_ArrerSCData d724arrear_dbt%rowtype;
    v_SumTrRub       NUMBER := 0;
    v_SumObRub       NUMBER := 0;
    
    v_SumTrAmount       NUMBER := 0;
    v_SumObAmount       NUMBER := 0;
    
    v_SumTr       NUMBER := 0;
    v_SumOb       NUMBER := 0;
    
    v_SumCom      NUMBER := 0;
    v_SumComRub   NUMBER := 0;
  BEGIN
    FOR one_rec IN (SELECT  tick.t_DealId, tick.t_DealCode, tick.t_DealCodeTS, tick.t_ClientId, tick.t_PartyId, tick.t_MarketId,
                  RSB_SECUR.IsBuy (Opr.oGrp) t_IsBuy,
                  RSB_SECUR.IsSale (Opr.oGrp) t_IsSale,
                  RSB_SECUR.IsREPO (Opr.oGrp) t_IsREPO,
                  RSB_SECUR.GetMainObjAttr (RSB_SECUR.OBJTYPE_SECDEAL, LPAD (tick.t_DealId, 34, '0'), ATTR_SPEC_DEAL, pEndDate)  t_SpecDeal
               FROM ddl_tick_dbt tick, 
                    (SELECT t_Kind_Operation, t_DocKind, rsb_secur.get_OperationGroup(t_SysTypes) oGrp FROM doprkoper_dbt) Opr
              WHERE     tick.t_BofficeKind = RSB_SECUR.DL_SECURITYDOC
                    AND Opr.t_Kind_Operation = tick.t_DealType
                    AND Opr.t_DocKind = tick.t_BOfficeKind
                    AND (   RSB_SECUR.IsBuy (Opr.oGrp) = 1
                         OR RSB_SECUR.IsSale (Opr.oGrp) = 1
                         OR RSB_SECUR.IsREPO (Opr.oGrp) = 1)
                    AND tick.t_ClientId = pParty
                    and tick.t_dealstatus != 0 --DL_PREPARING
                    AND tick.t_DealDate <= pEndDate
                    AND (   tick.t_CloseDate = RSI_RSB_FIINSTR.ZERO_DATE
                         or tick.t_CloseDate > pEndDate)
                    AND tick.t_ClientContrId = pClientContrId)
    LOOP
      GetSumRQByDeal (one_rec.t_DealId,
                      one_rec.t_IsREPO,
                      one_rec.t_IsSale,
                      v_SumTrRub,
                      v_SumObRub,
                      v_SumTrAmount,
                      v_SumObAmount,
                      v_SumTr,
                      v_SumOb,
                      one_rec.t_MarketId,
                      pEndDate);
      IF v_SumTrRub = 0 AND v_SumObRub = 0 THEN
        CONTINUE;
      END IF;

      v_ArrerSCData.t_SessionID        := pSessionID;
      v_ArrerSCData.t_ContrID          := pClientContrId;
      v_ArrerSCData.t_Contr_GroupID    := pCONTR_GROUPID;
      v_ArrerSCData.t_PartyID          := pParty;
      v_ArrerSCData.t_Client_GroupID   := pCLIENT_GROUPID;
      v_ArrerSCData.t_DocKind          := RSB_SECUR.DL_SECURITYDOC;
      v_ArrerSCData.t_DocID            := one_rec.t_DealId;
      v_ArrerSCData.t_ContractorID     := one_rec.t_PartyID;
      v_ArrerSCData.t_Sysdate          := sysdate;
      v_ArrerSCData.t_DealCode         := one_rec.t_DealCode;
      v_ArrerSCData.t_DealCodeTS       := one_rec.t_DealCodeTS;

      IF one_rec.t_IsREPO = 1 THEN
        v_ArrerSCData.t_KindRepo := CASE
                                      WHEN one_rec.t_IsSale = 1 THEN 1 -- Прямое
                                      ELSE 2 --Обратное
                                    END;
        IF (one_rec.t_SpecDeal = 1) THEN
          v_ArrerSCData.t_SubKind  := 2; -- Репо по переносу позиций
        ELSE                          
          v_ArrerSCData.t_SubKind  := 1; -- Репо
        END IF;
      ELSE
        v_ArrerSCData.t_KindRepo := 0;
        v_ArrerSCData.t_SubKind  := 3; -- с ц/б не РЕПО
      END IF;

      IF v_SumTrRub > 0 THEN
        v_ArrerSCData.t_Kind      := 1; -- Требование
        v_ArrerSCData.t_Value     := v_SumTrRub;
        v_ArrerSCData.t_ValueNRur := v_SumTr;
        v_ArrerSCData.t_ValueAmount := v_SumTrAmount;

        INSERT INTO d724arrear_dbt VALUES v_ArrerSCData;
      END IF;

      IF v_SumObRub > 0 THEN
        v_ArrerSCData.t_Kind      := 2; -- Обязательство
        v_ArrerSCData.t_Value     := v_SumObRub;
        v_ArrerSCData.t_ValueNRur := v_SumOb;
        v_ArrerSCData.t_ValueAmount := v_SumObAmount;

        INSERT INTO d724arrear_dbt VALUES v_ArrerSCData;
      END IF;
      
      FOR one_com IN (
                    SELECT t_FIID, t_ReceiverID, SUM(t_Amount) t_Amount FROM (
                      SELECT rq.t_FIID, (case when sfcomis.t_ReceiverID > 1 then one_rec.t_PartyID else sfcomis.t_ReceiverID end) t_ReceiverID,  t_Amount
                      FROM ddlrq_dbt rq, ddlcomis_dbt dlcomis, dsfcontr_dbt sfcontr, dsfcomiss_dbt sfcomis
                     WHERE     rq.t_DocId = one_rec.t_DealId
                           AND rq.t_DocKind = RSB_SECUR.DL_SECURITYDOC
                           AND rq.t_Type = RSI_DLRQ.DLRQ_TYPE_COMISS
                           AND rq.t_SourceObjKind = 4721
                           AND rq.t_SourceObjID   = dlcomis.t_ID
                           AND dlcomis.t_DocKind = RSB_SECUR.DL_SECURITYDOC
                           AND dlcomis.t_DocID   = one_rec.t_DealId
                           AND sfcontr.t_id = dlcomis.t_Contract
                           AND sfcontr.t_PartyID = pParty
                           AND sfcomis.t_FeeType = dlcomis.t_FeeType
                           AND sfcomis.t_Number = dlcomis.t_ComNumber
                           AND (dlcomis.t_FactPayDate = to_date('01010001', 'ddmmyyyy') or dlcomis.t_FactPayDate > pEndDate) )
                           GROUP BY t_FIID, t_ReceiverID)
      LOOP
        v_SumCom := one_com.t_Amount;
        IF one_com.t_FIID = RSI_RSB_FIINSTR.NATCUR THEN
          v_SumComRub := one_com.t_Amount;
        ELSE
          v_SumComRub := RSI_RSB_FIINSTR.ConvSum (one_com.t_Amount, one_com.t_FIID, RSI_RSB_FIINSTR.NATCUR, pEndDate);
        END IF;

        v_ArrerSCData.t_SubKind  := 5; -- комиссии
        v_ArrerSCData.t_Kind     := 2; -- Обязательство
        v_ArrerSCData.t_Value    := v_SumComRub;
        v_ArrerSCData.t_ValueNRur:= v_SumCom;
        v_ArrerSCData.t_ValueAmount  := v_SumCom;
        v_ArrerSCData.t_ContractorID := one_com.t_ReceiverID;

        INSERT INTO d724arrear_dbt VALUES v_ArrerSCData;
      END LOOP;
    END LOOP;
  END CollectArrearSCData;

  PROCEDURE CollectArrearPFIData(pSessionID IN NUMBER, pClientContrId IN NUMBER, pCONTR_GROUPID IN VARCHAR2,
                                 pParty IN NUMBER, pCLIENT_GROUPID IN VARCHAR2, pEndDate IN DATE)
  IS
    c_MPType CONSTANT NUMBER := Rsb_Common.GetRegIntValue('ПРОИЗВОДНЫЕ ИНСТРУМЕНТЫ\ВИД КУРСА "РЫНОЧНАЯ ЦЕНА"', 0);

    v_ArrearPFIData   d724arrear_dbt%rowtype;
    v_SumTrRub           NUMBER := 0;
    v_SumObRub           NUMBER := 0;
    v_SumComRub          NUMBER := 0;
    
    v_SumTr              NUMBER := 0;
    v_SumOb              NUMBER := 0;
    v_SumCom             NUMBER := 0;
    
    v_Price           NUMBER := 0;
    v_PriceRub        NUMBER := 0;
    
    v_MarketPrice     NUMBER := 0;
    v_Rate            NUMBER := 0;
    
    v_ComisSumRub        NUMBER := 0;
    v_tmpComisSumRub     NUMBER := 0;
    v_PaidComisSumRub    NUMBER := 0;
    v_tmpPaidComisSumRub NUMBER := 0;
    
    v_ComisSum        NUMBER := 0;
    v_PaidComisSum    NUMBER := 0;
    v_tmpPaidComisSum NUMBER := 0;
  BEGIN
    FOR one_rec IN (SELECT ndeal.t_ID,
                           ndeal.t_DocKind,
                           ndeal.t_Code,
                           ndeal.t_ExtCode,
                           ndeal.t_DvKind,
                           ndeal.t_Contractor
                      FROM ddvndeal_dbt ndeal, doproper_dbt oper
                     WHERE ndeal.t_Client = pParty
                       AND ndeal.t_ClientContr = pClientContrId
                       AND ndeal.t_Date <= pEndDate
                       AND ( (ndeal.t_IsPFI = 'X' AND  ndeal.t_DVKind IN (RSB_DERIVATIVES.DV_FORWARD,
                                                   RSB_DERIVATIVES.DV_OPTION,
                                                   RSB_DERIVATIVES.DV_PCTSWAP))
                            OR  ndeal.t_DVKind IN (RSB_DERIVATIVES.DV_FORWARD_FX,
                                                   RSB_DERIVATIVES.DV_BANKNOTE_FX)
                            OR ( ((ndeal.t_IsPFI = 'X'  and ndeal.t_DVKind = RSB_DERIVATIVES.DV_CURSWAP) OR (ndeal.t_DVKind = RSB_DERIVATIVES.DV_CURSWAP_FX))
                                AND NOT EXISTS (SELECT 1
                                                  FROM dpmpaym_dbt paym
                                                 WHERE paym.t_DocKind = ndeal.t_DocKind
                                                   AND paym.t_DocumentID = ndeal.t_ID
                                                   AND paym.t_Purpose IN (RSB_PAYMENT.BAi,RSB_PAYMENT.CAi)   -- Платежи по 1ч
                                                   AND paym.t_ValueDate > pEndDate
                                                   AND paym.t_PaymStatus <> RSB_PAYMENT.PM_REJECTED)
                                AND EXISTS (SELECT 1
                                              FROM dpmpaym_dbt paym
                                             WHERE paym.t_DocKind = ndeal.t_DocKind
                                               AND paym.t_DocumentID = ndeal.t_ID
                                               AND paym.t_Purpose IN (RSB_PAYMENT.BRi,RSB_PAYMENT.CRi)   -- Платежи по 2ч
                                               AND paym.t_ValueDate > pEndDate
                                               AND paym.t_PaymStatus <> RSB_PAYMENT.PM_REJECTED)
                               )
                           )
                       AND oper.t_DocKind = ndeal.t_DocKind
                       AND oper.t_DocumentID = lpad(ndeal.t_ID, 34, '0')
                       AND ndeal.t_State > 0
                       AND EXISTS (SELECT 1
                                     FROM dpmpaym_dbt paym
                                    WHERE paym.t_DocKind = ndeal.t_DocKind
                                      AND paym.t_DocumentID = ndeal.t_ID
                                      AND paym.t_ValueDate > pEndDate
                                      AND paym.t_PaymStatus <> RSB_PAYMENT.PM_REJECTED)
                     )
    LOOP
      v_ArrearPFIData.t_SubKind := 0;
      v_SumTrRub  := 0;
      v_SumTr     := 0;
      v_SumObRub  := 0;
      v_SumOb     := 0;
      v_SumComRub := 0;
      v_SumCom    := 0;
      
      FOR one_pm IN (SELECT paym.t_Amount, paym.t_Purpose, paym.t_PayFIID, fin.t_FaceValueFI, fin.t_FI_Kind, fin.t_Name,
                            case when paym.t_Payer = pParty then 0 else 1 end t_IsReq
                       FROM dpmpaym_dbt paym, dfininstr_dbt fin
                      WHERE paym.t_DocKind = one_rec.t_DocKind
                        AND paym.t_DocumentID = one_rec.t_ID
                        AND paym.t_Purpose NOT IN (RSB_PAYMENT.PM_PURP_COMMARKET,
                                                   PM_PURP_COMMBANK,
                                                   RSB_PAYMENT.PM_PURP_COMBROKER)
                        AND paym.t_ValueDate > pEndDate
                        AND paym.t_PaymStatus <> RSB_PAYMENT.PM_REJECTED
                        AND paym.t_PayFIID = fin.t_FIID)
      LOOP
        IF   one_pm.t_FI_Kind = RSI_RSB_FIINSTR.FIKIND_CURRENCY
          OR one_pm.t_FI_Kind = RSI_RSB_FIINSTR.FIKIND_METAL
        THEN
          v_Price := one_pm.t_Amount;
          IF one_pm.t_PayFIID != RSI_RSB_FIINSTR.NATCUR THEN
            v_PriceRub := RSI_RSB_FIINSTR.ConvSum(one_pm.t_Amount, one_pm.t_PayFIID,  RSI_RSB_FIINSTR.NATCUR, pEndDate);
            IF v_PriceRub IS NULL THEN
              CONTINUE;
            END IF;
          ELSE
            v_PriceRub := one_pm.t_Amount;
          END IF;
          
          IF (one_rec.t_DvKInd = RSB_DERIVATIVES.DV_FORWARD_FX and one_pm.t_Purpose in (RSB_PAYMENT.BAi, RSB_PAYMENT.BRi)) THEN
             IF (rsi_rsb_fiinstr.FI_GetRealFIKind(p_FIID => one_pm.t_PayFIID) = RSI_RSB_FIINSTR.FIKIND_CURRENCY) THEN
               v_ArrearPFIData.t_SubKind := 6;  /* сделки с иностранной валютой */
             ELSE
               v_ArrearPFIData.t_SubKind := 7;  /* сделки с драгоценными металлами */
             END IF;
          END IF;

        ELSIF one_pm.t_FI_Kind = RSI_RSB_FIINSTR.FIKIND_AVOIRISS THEN
          v_Price := one_pm.t_Amount;
        
          v_MarketPrice := RSI_RSB_FIINSTR.ConvSumType (1.0, one_pm.t_PayFIID, one_pm.t_FaceValueFI, c_MPType, pEndDate);
          IF v_MarketPrice IS NOT NULL THEN
            IF one_pm.t_FaceValueFI != RSI_RSB_FIINSTR.NATCUR THEN
              v_Rate := RSI_RSB_FIINSTR.ConvSum (1.0, one_pm.t_FaceValueFI, RSI_RSB_FIINSTR.NATCUR, pEndDate);
              IF v_Rate IS NULL THEN
                CONTINUE;
              END IF;
            ELSE
              v_Rate := 1;
            END IF;

            v_PriceRub := v_Rate * v_MarketPrice * one_pm.t_Amount;
          ELSE
            v_PriceRub := 0;
            RSB_SPREPFUN.AddRepError('Не определена рыночная стоимость ц/б "' || one_pm.t_Name || '"');
            CONTINUE;
          END IF;

        ELSE
          v_Price := one_pm.t_Amount;
          v_PriceRub := GetAnyLastCourseOnDate(pEndDate, one_pm.t_PayFIID);
          IF v_PriceRub IS NULL THEN
            CONTINUE;
          END IF;
        END IF;

        IF one_pm.t_IsReq = 1 THEN
          v_SumTrRub := v_SumTrRub + v_PriceRub;
          v_SumTr    := v_SumTr + v_Price;
        ELSE
          v_SumObRub := v_SumObRub + v_PriceRub;
          v_SumOb    := v_SumOb + v_Price;
        END IF;
      END LOOP;

      v_ArrearPFIData.t_SessionID      := pSessionID;
      v_ArrearPFIData.t_Sysdate        := sysdate;
      v_ArrearPFIData.t_ContrID        := pClientContrId;
      v_ArrearPFIData.t_Contr_GroupID  := pCONTR_GROUPID;
      v_ArrearPFIData.t_PartyID        := pParty;
      v_ArrearPFIData.t_Client_GroupID := pCLIENT_GROUPID;
      v_ArrearPFIData.t_DocKind        := one_rec.t_DocKind;
      v_ArrearPFIData.t_DocID          := one_rec.t_ID;
      v_ArrearPFIData.t_ContractorID   := one_rec.t_Contractor;
      v_ArrearPFIData.t_KindRepo       := 0;
      
      if (v_ArrearPFIData.t_SubKind = 0) then
        if (one_rec.t_DVKind = RSB_DERIVATIVES.DV_FORWARD_FX) then
          v_ArrearPFIData.t_SubKind        := 5; -- прочие требования (обязательства), в том числе неуплаченные комиссии.
        else
          v_ArrearPFIData.t_SubKind        := 4; -- ПФИ
        end if;
      end if;
      v_ArrearPFIData.t_DealCode       := one_rec.t_Code;
      v_ArrearPFIData.t_DealCodeTS     := one_rec.t_ExtCode;

      IF v_SumTrRub > 0 THEN
        v_ArrearPFIData.t_Kind         := 1; -- Требование
        v_ArrearPFIData.t_Value        := v_SumTrRub;
        v_ArrearPFIData.t_ValueNRur    := v_SumTr;
        v_ArrearPFIData.t_ValueAmount  := v_SumTr;

        INSERT INTO d724arrear_dbt VALUES v_ArrearPFIData;
      END IF;

      IF v_SumObRub > 0 THEN
        v_ArrearPFIData.t_Kind         := 2; -- Обязательство
        v_ArrearPFIData.t_Value        := v_SumObRub;
        v_ArrearPFIData.t_ValueNRur    := v_SumOb;
        v_ArrearPFIData.t_ValueAmount := v_SumOb;

        INSERT INTO d724arrear_dbt VALUES v_ArrearPFIData;
      END IF;

     
      v_ComisSum        := 0;
      v_ComisSumRub     := 0;
      v_PaidComisSum    := 0;
      v_PaidComisSumRub := 0;
      
      FOR one_com IN (SELECT t_FIID_Comm, t_ReceiverID, SUM(t_Sum) t_Sum FROM (
                      SELECT sfc.t_FIID_Comm, sfc.t_ReceiverID, t_Sum
                        FROM ddlcomis_dbt comis, dsfcomiss_dbt sfc, dsfcontr_dbt sfcontr
                       WHERE comis.t_DocKind = one_rec.t_DocKind
                         AND comis.t_DocID = one_rec.t_ID
                         AND comis.t_Date <= pEndDate
                         AND comis.t_FeeType = sfc.t_FeeType
                         AND comis.t_ComNumber = sfc.t_Number
                         AND sfcontr.t_id = comis.t_Contract
                         AND sfcontr.t_PartyID = pParty )
                         GROUP BY t_FIID_Comm, t_ReceiverID)
      LOOP
        v_tmpComisSumRub := RSI_RSB_FIINSTR.ConvSum(one_com.t_Sum, one_com.t_FIID_Comm, RSI_RSB_FIINSTR.NATCUR, pEndDate);
        IF v_tmpComisSumRub IS NOT NULL THEN
          v_ComisSumRub := v_tmpComisSumRub;
        END IF;
        v_ComisSum := one_com.t_Sum;
        
        SELECT NVL(SUM(paym.t_Amount), 0) INTO v_tmpPaidComisSum
          FROM dpmpaym_dbt paym
         WHERE paym.t_DocKind = one_rec.t_DocKind
           AND paym.t_DocumentID = one_rec.t_ID
           AND paym.t_Purpose IN (RSB_PAYMENT.PM_PURP_COMMARKET,
                                  PM_PURP_COMMBANK,
                                  RSB_PAYMENT.PM_PURP_COMBROKER)
           AND paym.t_ValueDate <= pEndDate
           AND paym.t_PaymStatus <> RSB_PAYMENT.PM_REJECTED
           AND paym.t_PayFIID = one_com.t_FIID_Comm
           AND paym.t_Payer = pParty
           AND paym.t_Receiver = one_com.t_ReceiverID;
                     
        v_tmpPaidComisSumRub := RSI_RSB_FIINSTR.ConvSum(v_tmpPaidComisSum, one_com.t_FIID_Comm, RSI_RSB_FIINSTR.NATCUR, pEndDate);
        IF v_tmpPaidComisSumRub IS NOT NULL THEN
          v_PaidComisSumRub := v_tmpPaidComisSumRub;
        END IF;
        v_PaidComisSum := v_tmpPaidComisSum;
        
        v_SumComRub := v_ComisSumRub - v_PaidComisSumRub;
        v_SumCom    := v_ComisSum - v_PaidComisSum;
        
        IF (v_SumComRub > 0) THEN
          v_ArrearPFIData.t_SubKind   := 5; -- комиссии
          v_ArrearPFIData.t_Kind      := 2; -- Обязательство
          v_ArrearPFIData.t_Value     := v_SumComRub;
          v_ArrearPFIData.t_ValueNRur := v_SumCom;
          v_ArrearPFIData.t_ValueAmount := v_SumCom;
          v_ArrearPFIData.t_ContractorID := (case when one_com.t_ReceiverID > 1 then one_rec.t_Contractor else one_com.t_ReceiverID end);

          INSERT INTO d724arrear_dbt VALUES v_ArrearPFIData;
        END IF;
        
      END LOOP;

    END LOOP;
  END CollectArrearPFIData;

  procedure CollectMetallData (pSessionId number,
                               pBegDate date,
                               pEndDate date)
  is
  begin
    insert into d724_metall_details (t_contr_groupid,
                                     t_client_groupid,
                                     t_clientcode,
                                     t_partyid,
                                     t_name,
                                     t_parent_sf_id,
                                     t_sf_id,
                                     t_account,
                                     t_rest,
                                     t_fiid,
                                     t_rest_rub)
    with contrs as (
          select --+ cardinality(c 700000) cardinality(cl 150000)
                 c.t_contr_groupid,
                 cl.t_client_groupid,
                 cl.t_clientcode,
                 cl.t_partyid,
                 cl.t_name,
                 c.t_sf_number,
                 c.t_party,
                 c.t_parent_sf_id,
                 c.t_sf_id
            from d724contr_dbt c
            join d724client_dbt cl on cl.t_sessionid = c.t_sessionid
                                  and cl.t_partyid = c.t_party
           where c.t_sessionid = pSessionId
             and c.t_sf_servkind = 21
             and c.t_sf_subkind = 8)
         ,accs as (
          select a.t_accountid,
                 a.t_account,
                 a.t_code_currency,
                 a.t_chapter
            from daccount_dbt a
           where (a.t_account like '30601%' or a.t_account like '30606%')
             and rsi_rsb_fiinstr.FI_GetRealFIKind(p_FIID => a.t_code_currency) = RSI_RSB_FIInstr.FIKIND_METAL
             and a.t_chapter = 1
             and (a.t_close_date = to_date('01.01.0001','DD.MM.YYYY') or a.t_close_date >= pBegDate)
             and a.t_open_date <= pEndDate
            )
          ,acc_w_docs as (
          select --+ cardianlity (a 1000) 
                 c.*,
                 a.t_account,
                 a.t_code_currency,
                 m.t_clientcontrid,
                 abs(rsb_account.restac(p_account => a.t_account,
                                        p_cur     => a.t_code_currency,
                                        p_date    => pEndDate,
                                        p_chapter => a.t_chapter,
                                        p_r0      => null)) t_rest
            from accs a
            join dmcaccdoc_dbt m on m.t_account = a.t_account
                                and m.t_catid = 70
            join contrs c on c.t_sf_id = m.t_clientcontrid
           where m.t_iscommon = chr(88)
          )
          select d.t_contr_groupid,
                 d.t_client_groupid,
                 d.t_clientcode,
                 d.t_partyid,
                 d.t_name,
                 d.t_parent_sf_id,
                 d.t_sf_id,
                 d.t_account,
                 d.t_rest,
                 d.t_code_currency,
                 round(rsi_rsb_fiinstr.ConvSum(SumB    => d.t_rest,
                                         pFromFI => d.t_code_currency,
                                         pToFI   => rsi_rsb_fiinstr.natcur,
                                         pbdate  => pEndDate)) t_rest_rub
            from acc_w_docs d;
  end CollectMetallData;

  PROCEDURE ClearTables(pSessionId IN NUMBER,
                        pCountDay IN NUMBER default 3)
  IS
  v_CountDay integer := greatest(nvl(pCountDay,3),2);
  BEGIN
     it_log.log(p_msg  => 'pSessionID='||pSessionId||' '||
                'Удаление старых записей из промежуточных таблиц отчета',
                p_msg_type => it_log.C_MSG_TYPE__MSG) ;
  
    DELETE FROM d724_metall_details;
    DELETE FROM D724ARREAR_DBT   WHERE t_SessionId = pSessionId;
    DELETE FROM D724PFI_DBT      WHERE t_SessionId = pSessionId;
    DELETE FROM D724WRTAVR_DBT   WHERE t_SessionId = pSessionId;
    DELETE FROM D724WRTMONEY_DBT WHERE t_SessionId = pSessionId;
    DELETE FROM D724FIREST_DBT   WHERE t_SessionId = pSessionId;
    DELETE FROM D724ACCREST_DBT  WHERE t_SessionId = pSessionId;
    DELETE FROM D724CLIENT_DBT   WHERE t_SessionId = pSessionId;
    DELETE FROM D724CONTR_DBT    WHERE t_SessionId = pSessionId;
    DELETE FROM D724R3CLIENT_GROUP WHERE t_SessionId = pSessionId;

    DELETE FROM D724ARREAR_DBT   WHERE t_sysdate < sysdate - v_CountDay;
    DELETE FROM D724PFI_DBT      WHERE t_sysdate < sysdate - v_CountDay;
    DELETE FROM D724WRTAVR_DBT   WHERE t_sysdate < sysdate - v_CountDay;
    DELETE FROM D724WRTMONEY_DBT WHERE t_sysdate < sysdate - v_CountDay;
    DELETE FROM D724FIREST_DBT   WHERE t_sysdate < sysdate - v_CountDay;
    DELETE FROM D724ACCREST_DBT  WHERE t_sysdate < sysdate - v_CountDay;
    DELETE FROM D724CLIENT_DBT   WHERE t_sysdate < sysdate - v_CountDay;
    DELETE FROM D724CONTR_DBT    WHERE t_sysdate < sysdate - v_CountDay;
    DELETE FROM D724R3CLIENT_GROUP WHERE t_sysdate < sysdate - v_CountDay;
    
    commit;
    
     it_log.log(p_msg  => 'pSessionID='||pSessionId||' '||
                'Завершено удаление старых записей из промежуточных таблиц отчета',
                p_msg_type => it_log.C_MSG_TYPE__MSG) ;

  END;

  PROCEDURE FillTableContr(pSessionId IN NUMBER, pBegDate IN DATE, pEndDate IN DATE, pIsSecur IN NUMBER, pIsDV IN NUMBER, pIsRep12 IN NUMBER)
  IS
    v_Group_Number VARCHAR2(100) := CHR(1);

  BEGIN
/*
  Для подраздела 1.2 отбираются договоры обслуживания (договоры фондового ди-линга)
или субдоговор с видом обслуживания "Фондовый дилинг", если в банке применяется механизм ДБО,
если имеется отметка при запуске "По договорам мо-дуля "Бэк-офис ценных бумаг",
срочного рынка и валютного рынка, если в форме запуска указан признак "По договорам модуля "ФИССиКО".
Отбираемые догово-ры должны быть открытыми на последний день отчетного периода
или же иметь дату закрытия, которая попадает в отчетный период. Договоры объединяются в группы
по следующим критериям в следующем порядке приоритетности:
1. По наличию признака ИИС
2. По принадлежности у субъекта-клиента по договору (перечень указан в ячейке 1.2.4)
Для каждой сформированной группы присваивается номер, начиная с 1.
Данное значение является идентификационным кодом группы договоров,
которое указывается в соответствующей графе в подразделе. Далее в подраздел вносятся параметры
для каждой сформированной группы.
*/

    IF gl_IsOptim = true THEN
      RSB_SPREPFUN.AddRepError( to_char(SYSDATE, 'hh24:mi:ss')||' FillTableContr Begin ' );
      commit;
    END IF;
    
    it_log.log(p_msg  => 'pSessionID='||pSessionId||' '||
                'Отбор договоров обслуживания',
                p_msg_type => it_log.C_MSG_TYPE__MSG) ;

    INSERT INTO D724CONTR_DBT(T_SESSIONID,
                              T_PARTY,
                              T_SF_ID,
                              T_SF_NUMBER,
                              T_SF_BEG,
                              T_SF_END,
                              T_SF_SERVKIND,
                              T_SF_SUBKIND,
                              T_IIS,
                              T_BANKROLE,
                              T_GROUP_CONTR,
                              T_CONTR_GROUPID,
                              T_PARENT_SF_ID,
                              T_PSF_BEG,
                              T_PSF_END)
    SELECT pSessionId AS t_SessionID,
       sfcontr.t_partyid AS party,
       sfcontr.t_ID,
       sfcontr.t_number sfnum,
       sfcontr.t_datebegin sfbeg,
       sfcontr.t_dateclose sfcls,
       sfcontr.t_servkind sfsk,
       sfcontr.t_ServKindSub subkind,
       case when dlcontr.t_iis = chr(88) then 1 else 2 end iis,
       GetBankRole(dlcontr.t_sfcontrid, pEndDate, 0) as BankRole,
       GetGroup_Contr(sfcontr.t_partyid, pEndDate, pIsRep12, dlcontr.t_dlcontrid) as group_contr,
       CHR(1),
       dlcontr.t_sfcontrid t_parentid,
       psfcontr.t_datebegin psfbeg,
       psfcontr.t_dateclose psfcls
        FROM dsfcontr_dbt sfcontr, ddlcontr_dbt dlcontr, ddlcontrmp_dbt dlcontrmp, dsfcontr_dbt psfcontr
       WHERE sfcontr.t_PartyId <> RsbSessionData.OurBank
         AND dlcontrmp.t_sfcontrid= sfcontr.t_id
         AND dlcontr.t_dlcontrid = dlcontrmp.t_dlcontrid
         AND dlcontr.t_IsUnderWriting != chr(88)
         AND dlcontr.t_BondAgent != chr(88)
         AND (
              ( ( (sfcontr.t_ServKind = PTSK_STOCKDL and pIsSecur = 1) OR
                  (sfcontr.t_ServKind IN (PTSK_DV, PTSK_CM) and pIsDV = 1)
                ) AND
                ( sfcontr.t_DateBegin <= pEndDate AND
                  ( sfcontr.t_DateClose = to_date('01010001', 'ddmmyyyy')
                    OR  sfcontr.t_DateClose >= pBegDate
                  )
                )
              )
         AND psfcontr.t_id = dlcontr.t_sfcontrid
              /*OR EXISTS
                  (SELECT 1
                     FROM ddlcontr_dbt dlcontr,
                          ddlcontrmp_dbt dlcontrmp,
                          dsfcontr_dbt subcontr
                   WHERE     dlcontr.t_SFContrId = sfcontr.t_Id
                         AND dlcontrmp.t_DlContrId = dlcontr.t_DlContrId
                         AND dlcontrmp.t_SFContrId = subcontr.t_Id
                         AND ((subcontr.t_ServKind = PTSK_STOCKDL and pIsSecur = 1) OR
                              (subcontr.t_ServKind IN (PTSK_DV, PTSK_CM) and pIsDV = 1)
                             )
                         AND (    subcontr.t_DateBegin <= pEndDate
                             AND (   subcontr.t_DateClose = to_date('01010001', 'ddmmyyyy')
                                  OR subcontr.t_DateClose >= pBegDate
                                 )
                             )
                  )*/
             );
    COMMIT;
    
    it_log.log(p_msg  => 'pSessionID='||pSessionId||' '||
                'Завершен отбор договоров обслуживания',
                p_msg_type => it_log.C_MSG_TYPE__MSG) ;

    IF gl_IsOptim = true THEN
      RSB_SPREPFUN.AddRepError( to_char(SYSDATE, 'hh24:mi:ss')||' FillTableContr 2 ' );
      commit;
    END IF;

    FOR Group_rec IN (
                      SELECT T_IIS, T_BANKROLE, T_GROUP_CONTR
                        FROM D724CONTR_DBT
                       WHERE T_SESSIONID = pSessionId
                       GROUP BY T_IIS, T_BANKROLE, T_GROUP_CONTR
                       ORDER BY T_IIS, T_BANKROLE, T_GROUP_CONTR
                      )
    LOOP

      --v_Group_Number := Group_rec.T_IIS||Group_rec.T_BANKROLE||Group_rec.T_GROUP_CONTR;
      
      IF Group_rec.T_IIS = 1 THEN
        v_Group_Number := 'ИИС';

      ELSIF Group_rec.T_IIS = 2 THEN
        v_Group_Number := 'НИИС';

      ELSE
        v_Group_Number := '_';
      END IF;


      IF Group_rec.T_GROUP_CONTR = 1 THEN
        v_Group_Number := v_Group_Number||'_НПФ';

      ELSIF Group_rec.T_GROUP_CONTR = 2 THEN
        v_Group_Number := v_Group_Number||'_УК ПИФ';

      ELSIF Group_rec.T_GROUP_CONTR = 3 THEN
        v_Group_Number := v_Group_Number||'_УК НПФ';

      ELSIF Group_rec.T_GROUP_CONTR = 4 THEN
        v_Group_Number := v_Group_Number||'_УК ДУ';

      ELSIF Group_rec.T_GROUP_CONTR = 5 THEN
        v_Group_Number := v_Group_Number||'_УК ЦК';

      ELSIF Group_rec.T_GROUP_CONTR = 6 THEN
        v_Group_Number := v_Group_Number||'_Банк';

      ELSIF Group_rec.T_GROUP_CONTR = 7 THEN
        v_Group_Number := v_Group_Number||'_Субброкер';

      ELSIF Group_rec.T_GROUP_CONTR = 8 THEN
        v_Group_Number := v_Group_Number||'_СО';

      ELSIF Group_rec.T_GROUP_CONTR = 91 THEN
        v_Group_Number := v_Group_Number||'_АИФ';

      ELSIF Group_rec.T_GROUP_CONTR = 92 THEN
        v_Group_Number := v_Group_Number||'_УК АИФ';

      ELSIF Group_rec.T_GROUP_CONTR = 10 THEN
        v_Group_Number := v_Group_Number||'_ДФО';

      ELSIF Group_rec.T_GROUP_CONTR = 11 THEN
        v_Group_Number := v_Group_Number||'_ИП(д)';

      ELSIF Group_rec.T_GROUP_CONTR = 12 THEN
        v_Group_Number := v_Group_Number||'_ЮЛ(д)';

      ELSIF Group_rec.T_GROUP_CONTR = 13 THEN
        v_Group_Number := v_Group_Number||'_ФЛ(д)';

      ELSE
        v_Group_Number := v_Group_Number||'_';
      END IF;

      UPDATE D724CONTR_DBT
         SET T_CONTR_GROUPID = v_Group_Number
       WHERE T_SESSIONID   = pSessionId
         AND T_IIS         = Group_rec.T_IIS
         AND T_BANKROLE    = Group_rec.T_BANKROLE
         AND T_GROUP_CONTR = Group_rec.T_GROUP_CONTR;
    END LOOP;

    COMMIT;
    
    it_log.log(p_msg  => 'pSessionID='||pSessionId||' '||
                'Коды групп договоров обновлены',
                p_msg_type => it_log.C_MSG_TYPE__MSG) ;


    IF gl_IsOptim = true THEN
      RSB_SPREPFUN.AddRepError( to_char(SYSDATE, 'hh24:mi:ss')||' FillTableContr End ' );
      commit;
    END IF;

  END;


  PROCEDURE FillTableClient(pSessionId IN NUMBER, pBegDate IN DATE, pEndDate IN DATE, pIsSecur IN NUMBER, pIsDV IN NUMBER, pIsRep12 IN NUMBER)
  IS
    v_Group_Number VARCHAR2(100) := CHR(1);
  BEGIN
/*
Для подраздела 1.1 отбираются клиенты, которые формируют группы по следующим параметрам с приоритетом:
1. По типу (юр. Лицо, физ. Лицо, ИП)
2. По Квалификации (квал. Или неквал.)
3. По статусу активности (активный или неактивный)
Активным считается клиент брокерского обслуживания, у которого есть открытые/закрытые сделки
(в подсистемах БОЦБ, ФИССиКО), за исключением операций зачисления/списания денежных средств и ц/б,
дата заключения которых входит в отчетный период (в параметрах сделки в качестве ДО клиента
указан либо сам ДО либо субдоговор отобранного ДБО).
Активным считается клиент депозитарного обслуживания, у которого:
есть открытые/закрытые депозитарные инвентарные операции, удовлетворяющие условиям:
o  операция выполняется по счету депо, который выступает объектом ДО клиента;
o  дата приема поручения депо входит в отчетный период;
o  значение категории на клиенте "Не учитывать в форме 707" = "нет" или не заполнена.
4. По коду ОКСМ
5. По коду ОКАТО
Для каждой сформированной группы присваивается номер, начиная с 1. Данное значение является
идентификационным кодом группы клиента, которое указывается в соответствующей графе в подразделе.
Далее в подраздел вносятся параметры для каждой сформированной группы.
Отбираются клиенты по алгоритму.
1. Имеется следующий вид обслуживания:
1.1.  фондового дилинга (если в форме запуска указан признак "По договорам модуля "Бэк-офис ценных бумаг");
1.2.  срочного рынка и валютного рынка (если в форме запуска указан признак "По договорам модуля "ФИССиКО"),
2. Для клиента выполняются следующие условия:
2.1.  Существует хотя бы один из нижеуказанных договоров:
"  договор обслуживания с видом обслуживания "Фондовый дилинг"
или субдоговор с видом обслуживания "Фондовый дилинг", если в банке применяется механизм ДБО.
"  договор обслуживания с видом обслуживания "Срочные контракты" и/или "Валютный рынок" или ДБО,
по которому зарегистрирован хотя бы один субдоговор с видом обслуживания "Срочные контракты"
и/или "Валютный рынок" (если в форме запуска указан признак "По договорам модуля ФИССиКО")
2.2.  На дату окончания отчетного периода (т.е. на последний день указанного квартала)
договор с клиентом открыт или дата закрытия договора входит в отчетный период.
*/

    IF gl_IsOptim = true THEN
      RSB_SPREPFUN.AddRepError( to_char(SYSDATE, 'hh24:mi:ss')||' FillTableClient Begin ' );
      commit;
    END IF;
    
    it_log.log(p_msg  => 'pSessionID='||pSessionId||' '||
                'Отбор клиентов',
                p_msg_type => it_log.C_MSG_TYPE__MSG) ;

    INSERT INTO D724CLIENT_DBT(T_SESSIONID,
                               T_PARTYID,
                               T_SHORTNAME,
                               T_NAME,
                               T_TYPE_FL,
                               T_CLIENTCODE,
                               T_KI,
                               T_ACTIVECLIENT,
                               T_CODE_OKATO,
                               T_CODE_OKSM,
                               T_CLIENT_GROUPID)
    SELECT t_SessionID,
           party,
           shortname,
           name,
           fl,
           ClientCode,
           qi,
           is_active,
           case when (GetOkatoCode(party, t_NRCountry, t_NotResident, pEndDate, pIsRep12)<>'00') 
                  then GetOkatoCode(party, t_NRCountry, t_NotResident, pEndDate, pIsRep12)
             else GetOkatoCode(party, t_NRCountry, t_NotResident, RSBSESSIONDATA.curdate, pIsRep12) 
             end as code_okato,
           GetOKSM_Code(party, t_NRCountry, t_NotResident, t_Superior, pEndDate, pIsRep12)  as code_oksm,
           CHR(1)
    FROM  (
      SELECT pSessionId AS t_SessionID,
         t.t_partyid AS party,
         TRIM(t.t_shortname) shortname,
         t.t_name name,
         (CASE WHEN EXISTS (SELECT 1 FROM DPERSN_DBT WHERE T_PersonID = t.t_partyid and t_IsEmployer = 'X') THEN 3
         ELSE (CASE WHEN t.t_LegalForm = 2 THEN 2 ELSE 1 END) END) fl,
         RSI_RSBPARTY.GetPartyCode (t.t_PartyID, 1) AS ClientCode,
         CASE
            WHEN (EXISTS
                     (SELECT 1
                        FROM DV_SCQINVHIST hist
                       WHERE     hist.t_PartyID = t.t_PartyID
                             AND hist.t_State = 1
                             AND hist.t_BegDate <= pEndDate
                             AND (hist.t_EndDate = TO_DATE ('01.01.0001', 'DD.MM.YYYY') OR hist.t_EndDate > pEndDate)))
            THEN
               1
            ELSE
               2
         END
            AS qi,
         (CASE WHEN is_activeClient(pSessionId, t.t_PartyID, pBegDate, pEndDate, pIsSecur, pIsDV, pIsRep12) = 1 THEN 1 ELSE 2 END) AS is_active,
         t.t_Superior,
         NVL((SELECT /*+ index(hist DPTPRMHIST_DBT_IDX1)*/ CASE WHEN t_ValueBefore = CHR (1) THEN CHR (0) ELSE CHR (88) END t_NotResident
            FROM (
             SELECT hist.t_ValueBefore
               FROM dptprmhist_dbt hist
              WHERE hist.t_PartyID = t.t_PartyID
                AND hist.t_ParamKindID = RSI_RSBPARTY.PARTY_NOTRESIDENT
                AND hist.t_BankDate > pEndDate
                AND ROWNUM < 2
              ORDER BY hist.t_BankDate)
           ), t.t_NotResident) t_NotResident,
           NVL((SELECT /*+ index(hist DPTPRMHIST_DBT_IDX1)*/ t_ValueBefore
            FROM (
             SELECT hist.t_ValueBefore
               FROM dptprmhist_dbt hist
              WHERE hist.t_PartyID = t.t_PartyID
                AND hist.t_ParamKindID = RSI_RSBPARTY.PARTY_NRCOUNTRY
                AND hist.t_BankDate > pEndDate
                AND ROWNUM < 2
              ORDER BY hist.t_BankDate)
           ), t.t_NRCountry) t_NRCountry
        FROM dparty_dbt t
       WHERE exists ( select 1
                        from dclient_dbt cl
                       where cl.t_PartyID = t.t_PartyID
                         AND ( (cl.t_ServiceKind = PTSK_STOCKDL and pIsSecur = 1) OR
                               (cl.t_ServiceKind IN (PTSK_DV, PTSK_CM) and pIsDV = 1)
                             )
                         AND cl.t_StartDate <= pEndDate
                         AND (cl.t_FinishDate > pBegDate or cl.t_FinishDate = TO_DATE ('01.01.0001', 'DD.MM.YYYY'))
                    )
         AND exists (select 1 from D724CONTR_DBT Contr where Contr.T_SESSIONID = pSessionId and Contr.T_PARTY = t.t_PartyId));
    COMMIT;
    
    it_log.log(p_msg  => 'pSessionID='||pSessionId||' '||
                'Завершен отбор клиентов',
                p_msg_type => it_log.C_MSG_TYPE__MSG) ;

    IF gl_IsOptim = true THEN
      RSB_SPREPFUN.AddRepError( to_char(SYSDATE, 'hh24:mi:ss')||' FillTableClient 2 ' );
      commit;
    END IF;

    FOR Group_rec IN (
                      SELECT T_TYPE_FL, T_KI, T_ACTIVECLIENT, T_CODE_OKSM, T_CODE_OKATO
                        FROM D724CLIENT_DBT
                       WHERE T_SESSIONID = pSessionId
                       GROUP BY T_TYPE_FL, T_KI, T_ACTIVECLIENT, T_CODE_OKSM, T_CODE_OKATO
                       ORDER BY T_TYPE_FL, T_KI, T_ACTIVECLIENT, T_CODE_OKSM, T_CODE_OKATO
                      )
    LOOP
      --v_Group_Number := '1'||Group_rec.T_TYPE_FL||Group_rec.T_KI||Group_rec.T_ACTIVECLIENT||Group_rec.T_CODE_OKSM||(case when Group_rec.T_CODE_OKATO = '0' then '' else Group_rec.T_CODE_OKATO end);
      v_Group_Number := 'БД';

      IF Group_rec.T_TYPE_FL = 1 THEN
        v_Group_Number := v_Group_Number||'_ЮЛ';

      ELSIF Group_rec.T_TYPE_FL = 2 THEN
        v_Group_Number := v_Group_Number||'_ФЛ';

      ELSIF Group_rec.T_TYPE_FL = 3 THEN
        v_Group_Number := v_Group_Number||'_ИП';

      ELSE
        v_Group_Number := v_Group_Number||'_';
      END IF;

      IF Group_rec.T_KI = 1 THEN
        v_Group_Number := v_Group_Number||'_КИ';

      ELSIF Group_rec.T_KI = 2 THEN
        v_Group_Number := v_Group_Number||'_НКИ';

      ELSE
        v_Group_Number := v_Group_Number||'_';
      END IF;


      IF Group_rec.T_ACTIVECLIENT = 1 THEN
        v_Group_Number := v_Group_Number||'_А';

      ELSIF Group_rec.T_ACTIVECLIENT = 2 THEN
        v_Group_Number := v_Group_Number||'_Н';

      ELSE
        v_Group_Number := v_Group_Number||'_';
      END IF;

      v_Group_Number := v_Group_Number||'_'||Group_rec.T_CODE_OKSM||(case when Group_rec.T_CODE_OKATO = '0' then '' else '_' || Group_rec.T_CODE_OKATO end);

      UPDATE D724CLIENT_DBT
         SET T_CLIENT_GROUPID = v_Group_Number
       WHERE T_SESSIONID   = pSessionId
         AND T_TYPE_FL     = Group_rec.T_TYPE_FL
         AND T_KI          = Group_rec.T_KI
         AND T_ACTIVECLIENT= Group_rec.T_ACTIVECLIENT
         AND T_CODE_OKSM   = Group_rec.T_CODE_OKSM
         AND T_CODE_OKATO  = Group_rec.T_CODE_OKATO;
    END LOOP;
    
    it_log.log(p_msg  => 'pSessionID='||pSessionId||' '||
             'Коды групп клиентов обновлены. Начало расчета количества договоров для клиентов на начало/конец периода, количества открытых и закрытых в отчетном периоде',
              p_msg_type => it_log.C_MSG_TYPE__MSG) ;
    
      UPDATE D724CLIENT_DBT CL
            SET T_CONTR = (CASE WHEN EXISTS (select /*+ index(CONTR D724CONTR_DBT_IDX3 )*/ 1
                  from D724CONTR_DBT CONTR
                 where CONTR.T_SESSIONID = pSessionID
                   and CONTR.T_PARTY = CL.T_PARTYID
                   and (CONTR.T_PSF_END = to_date('01.01.0001','DD.MM.YYYY') or CONTR.T_PSF_END > pEndDate)) THEN 1 ELSE 0 END),
            T_IISCONTR = (CASE WHEN EXISTS (select /*+ index(CONTR D724CONTR_DBT_IDX3 )*/ 1
                  from D724CONTR_DBT CONTR
                 where CONTR.T_SESSIONID = pSessionID
                   and CONTR.T_PARTY = CL.T_PARTYID
                   and CONTR.T_IIS = 1
                   and (CONTR.T_PSF_END = to_date('01.01.0001','DD.MM.YYYY') or CONTR.T_PSF_END > pEndDate)) THEN 1 ELSE 0 END),
            T_BEGCONTR = (CASE WHEN
                    exists (select /*+ index(CONTR D724CONTR_DBT_IDX3 )*/ 1
                  from D724CONTR_DBT CONTR
                 where CONTR.T_SESSIONID = pSessionID
                   and CONTR.T_PARTY = CL.T_PARTYID
                   and CONTR.T_PSF_BEG between pBegDate and pEndDate) THEN 1 ELSE 0 END),
            T_ENDCONTR = (CASE WHEN exists (select /*+ index(CONTR D724CONTR_DBT_IDX3 )*/ 1
                  from D724CONTR_DBT CONTR
                 where CONTR.T_SESSIONID = pSessionID
                   and CONTR.T_PARTY = CL.T_PARTYID
                   and CONTR.T_PSF_END between pBegDate and pEndDate) THEN 1 ELSE 0 END),
            T_PERIODSTARTCONTR = (CASE WHEN exists (select /*+ index(CONTR D724CONTR_DBT_IDX3 )*/ 1
                  from D724CONTR_DBT CONTR
                 where CONTR.T_SESSIONID = pSessionID
                   and CONTR.T_PARTY = CL.T_PARTYID
                   and CONTR.T_PSF_BEG < pBegDate) THEN 1 ELSE 0 END),
            T_PERIODENDCONTR = (CASE WHEN exists (select /*+ index(CONTR D724CONTR_DBT_IDX3 )*/ 1
                  from D724CONTR_DBT CONTR
                 where CONTR.T_SESSIONID = pSessionID
                   and CONTR.T_PARTY = CL.T_PARTYID
                   and  (CONTR.T_PSF_END > pEndDate or CONTR.T_PSF_END = TO_DATE('01.01.0001','DD.MM.YYYY'))) THEN 1 ELSE 0 END),
            T_PERIODENDCONTRIIS = (CASE WHEN exists (select /*+ index(CONTR D724CONTR_DBT_IDX3 )*/ 1
                  from D724CONTR_DBT CONTR
                 where CONTR.T_SESSIONID = pSessionID
                   and CONTR.T_PARTY = CL.T_PARTYID
                   and CONTR.T_IIS = 1
                   and  (CONTR.T_PSF_END > pEndDate or CONTR.T_PSF_END = TO_DATE('01.01.0001','DD.MM.YYYY'))) THEN 1 ELSE 0 END)
                WHERE CL.T_SESSIONID   = pSessionId;
                   

    COMMIT;
    
    it_log.log(p_msg  => 'pSessionID='||pSessionId||' '||
             'Расчет количества договоров завершен',
             p_msg_type => it_log.C_MSG_TYPE__MSG) ;

    IF gl_IsOptim = true THEN
      RSB_SPREPFUN.AddRepError( to_char(SYSDATE, 'hh24:mi:ss')||' FillTableClient End ' );
      commit;
    END IF;
  END;


  -- Обработка ДО
  PROCEDURE ProcessDO(pSessionID IN NUMBER,
                      pmin_ClientContrId IN NUMBER,
                      pmax_ClientContrId IN NUMBER,
                      pBegDate IN DATE,
                      pEndDate IN DATE,
                      pIsRep12 IN NUMBER,
                      pChapter IN NUMBER,
                      pIsParallel IN NUMBER DEFAULT 1)
  IS
    v_Party          NUMBER := 0;
    v_SF_BEG         DATE;
    v_SF_END         DATE;
    v_SF_SERVKIND    NUMBER := 0;
    v_IIS            CHAR;
    v_CONTR_GROUPID  VARCHAR2(100) := CHR(1);
    v_CLIENT_GROUPID VARCHAR2(100) := CHR(1);

    v_AvrValue      AvrValueData_t;
    v_tmp_sum NUMBER := 0;
    v_TotalCost NUMBER := 0;
    v_UseNKDRate BOOLEAN := Rsb_Common.GetRegBoolValue('SECUR\РАСЧЕТ НКД ПО КУРСУ', 0);
    
    v_IssuerName VARCHAR2(400) := CHR(1);
    v_IssuerOKSMO VARCHAR2(4) := CHR(1);
    v_ISIN VARCHAR(25) := CHR(1);
    v_LSIN VARCHAR(35) := CHR(1);

    v_firest d724firest_dbt%rowtype;
    v_wrtavr d724wrtavr_dbt%rowtype;
    v_accrest d724accrest_dbt%rowtype;
    v_ClientContrId D724CONTR_DBT.T_SF_ID%type;
  BEGIN
    RSB_SPREPFUN.g_RepKind   := 0;
    RSB_SPREPFUN.g_SessionID := pSessionID;
    
    it_log.log(p_msg  => 'pSessionID='||pSessionId||' '||
                'Запуск потока обработки ДО, диапазон ДО для обработки:'||pmin_ClientContrId||'-'||pmax_ClientContrId ,
                p_msg_type => it_log.C_MSG_TYPE__MSG) ;

    for cur_contr in (select /*+ ordered */
                             Contr.t_sf_id,
                             Contr.T_PARTY,
                             Contr.T_SF_BEG,
                             Contr.T_SF_END,
                             Contr.T_SF_SERVKIND,
                             Contr.T_IIS,
                             Contr.T_CONTR_GROUPID,
                             Client.T_CLIENT_GROUPID,
                             Client.t_activeclient
             from D724CONTR_DBT Contr, D724CLIENT_DBT Client
            WHERE Contr.t_SessionID = pSessionID
              and Contr.t_sf_id between pmin_ClientContrId and pmax_ClientContrId
              AND Contr.T_PARTY = Client.T_PARTYID
              and Client.t_SessionID = pSessionID )
    loop
      v_ClientContrId := cur_contr.t_sf_id ;
      v_Party  := cur_contr.T_PARTY;
      v_SF_BEG := cur_contr.T_SF_BEG;
      v_SF_END := cur_contr.T_SF_END;
      v_SF_SERVKIND := cur_contr.T_SF_SERVKIND;
      v_IIS := cur_contr.T_IIS;
      v_CONTR_GROUPID := cur_contr.T_CONTR_GROUPID;
      v_CLIENT_GROUPID := cur_contr.T_CLIENT_GROUPID;

      IF gl_IsOptim = true THEN
        RSB_SPREPFUN.g_SessionID := v_ClientContrId;
      END IF;

      IF v_SF_SERVKIND = PTSK_STOCKDL  THEN
        -- Остатки по счетам ВУ. Будут нужны для разделов 3 и 6 - для 3 суммарно, а для 6 по-бумажно.
        FOR one_rec IN (
           select distinct mc.t_account,
                  mc.t_currency fiid,
                  rsb_account.restac(mc.t_account, mc.t_currency, pEndDate, mc.t_Chapter, null) count,
                  RSB_FIInstr.FI_AvrKindsGetRoot(fin.t_FI_KIND, fin.t_AvoirKind) t_avrkindsroot,
                  avr.t_isin,
                  avr.t_lsin,
                  fin.t_parentfi,
                  fin.t_issuer,
                  fin.t_drawingdate,
                  fin.t_name,
                  GetOKSM_Code(Issuer.t_PartyID, NVL((SELECT /*+ index(hist DPTPRMHIST_DBT_IDX1)*/ t_ValueBefore
                                                                                   FROM (
                                                                                    SELECT hist.t_ValueBefore
                                                                                      FROM dptprmhist_dbt hist
                                                                                     WHERE hist.t_PartyID = Issuer.t_PartyID
                                                                                       AND hist.t_ParamKindID = RSI_RSBPARTY.PARTY_NRCOUNTRY
                                                                                       AND hist.t_BankDate > pEndDate
                                                                                       AND ROWNUM < 2
                                                                                     ORDER BY hist.t_BankDate)
                                                                                  ), Issuer.t_NRCountry), 
                                                                                  NVL((SELECT /*+ index(hist DPTPRMHIST_DBT_IDX1)*/ CASE WHEN t_ValueBefore = CHR (1) THEN CHR (0) ELSE CHR (88) END t_NotResident
                                                                                   FROM (
                                                                                    SELECT hist.t_ValueBefore
                                                                                      FROM dptprmhist_dbt hist
                                                                                     WHERE hist.t_PartyID = Issuer.t_PartyID
                                                                                       AND hist.t_ParamKindID = RSI_RSBPARTY.PARTY_NOTRESIDENT
                                                                                       AND hist.t_BankDate > pEndDate
                                                                                       AND ROWNUM < 2
                                                                                     ORDER BY hist.t_BankDate)
                                                                                  ), Issuer.t_NotResident), 
                                                                                  issuer.t_Superior, 
                                                                                  pEndDate, 
                                                                                  pIsRep12) t_issuerOKSMO,
                  nvl( (SELECT t_ValueBefore FROM (SELECT hist.t_ValueBefore FROM dptprmhist_dbt hist
                                                                             WHERE hist.t_PartyID = Issuer.t_PartyID
                                                                               AND hist.t_ParamKindID = 110
                                                                               AND hist.t_BankDate > pEndDate
                                                                          ORDER BY hist.t_BankDate ASC FETCH FIRST ROW ONLY) ), (case when issuer.t_shortname = chr(1) then issuer.t_name else  issuer.t_shortname end) ) t_issuername,
                  fin.t_facevaluefi vn,
                  NVL ( (SELECT mrkt.t_Market
                           FROM ddlmarket_dbt mrkt, ddldepset_dbt dep
                          WHERE dep.t_Depositary = mc.t_Place
                            AND mrkt.t_DepSetId = dep.t_DepSetId
                            AND ROWNUM = 1),
                       -1) t_MarketId,
                  (CASE WHEN EXISTS (SELECT 1
                                       FROM dobjatcor_dbt AtCor
                                      WHERE AtCor.t_ObjectType = 12
                                        AND AtCor.t_GroupID    = 28 --Квалификация в качестве ценной бумаги
                                        AND AtCor.t_Object     = LPAD(mc.t_fiid, 10, '0')
                                        AND AtCor.t_AttrID     = 2
                                        AND AtCor.t_validFromDate <= pEndDate
                                        AND AtCor.t_validTODate >= pEndDate
                                        AND ROWNUM = 1
                                    )
                        THEN CHR(88)
                        ELSE CHR(0)
                    END) t_IsNotAvr,
                  (SELECT t_Name
                                       FROM dobjatcor_dbt AtCor, dobjattr_dbt Attr                                      
                                       WHERE AtCor.t_ObjectType = 12
                                        AND AtCor.t_GroupID    = 5 --Код типа ц.б. для формы 711
                                        AND AtCor.t_Object     = LPAD(mc.t_fiid, 10, '0')
                                        AND AtCor.t_validFromDate <= pEndDate
                                        AND AtCor.t_validTODate >= pEndDate
                                        AND Attr.t_ObjectType =  AtCor.t_ObjectType
                                        AND Attr.t_GroupID = AtCor.t_GroupID
                                        AND Attr.t_AttrID = AtCor.t_AttrID
                                        AND ROWNUM = 1
               ) t_AvrType
             from dmcaccdoc_dbt mc, dfininstr_dbt fin, davoiriss_dbt avr, dparty_dbt issuer
            where mc.t_owner = v_Party
              and mc.t_clientcontrid = v_ClientContrId
              and mc.t_Chapter = 22
              and mc.t_CatID IN (354, 368, 369) --'НЦБ, Расч. с клиентом, ВУ', 'ЦБ, Расч. с клиентом, ВУ', 'ФО, Расч. с клиентом, ВУ'
              and mc.t_iscommon = CHR(88)
              and fin.t_fiid = mc.t_fiid
              and avr.t_fiid = fin.t_fiid
              and issuer.t_partyid = fin.t_Issuer)
        LOOP
          IF one_rec.count = 0 THEN
             CONTINUE;
          END IF;
          IF (one_rec.t_DrawingDate != to_date('01.01.0001', 'DD.MM.YYYY') and one_rec.t_DrawingDate <= pEndDate ) THEN
             RSB_SPREPFUN.AddRepError( 'По разделу 6 учитывается погашенная ц/б ' || one_rec.t_Name || ' ' || one_rec.t_ISIN || ' (дата погашения ' || to_char(one_rec.t_DrawingDate, 'DD.MM.YYYY') || ')');
          END IF;

          v_AvrValue := GetAvoirValue(one_rec.fiid, one_rec.vn, ABS(one_rec.count), pEndDate, v_UseNKDRate, one_rec.t_MarketId);
          IF v_AvrValue.t_SumRub IS NULL THEN
            v_TotalCost := 0;
          ELSE
            v_TotalCost := v_AvrValue.t_SumRub;
          END IF;
          
          v_ISIN        := one_rec.t_isin;
          v_LSIN        := one_rec.t_lsin;
          v_IssuerName  := TRIM(one_rec.t_IssuerName);
          v_IssuerOKSMO := one_rec.t_IssuerOKSMO;
          
          --DEF-71129 добавляем закрывающие кавычки, если нужно
          IF (MOD(REGEXP_COUNT(v_IssuerName, '"'), 2) = 1) THEN
             v_IssuerName := v_IssuerName || '"';
          END IF;
          
          IF (one_rec.t_avrkindsroot = RSI_RSB_FIINSTR.AVOIRKIND_DEPOSITORY_RECEIPT) THEN
            BEGIN
              SELECT t_lsin, 
                  nvl( (SELECT t_ValueBefore FROM (SELECT hist.t_ValueBefore FROM dptprmhist_dbt hist
                                                                             WHERE hist.t_PartyID = Issuer.t_PartyID
                                                                               AND hist.t_ParamKindID = 110
                                                                               AND hist.t_BankDate > pEndDate
                                                                          ORDER BY hist.t_BankDate ASC FETCH FIRST ROW ONLY) ), (case when issuer.t_shortname = chr(1) then issuer.t_name else  issuer.t_shortname end) ) t_issuername, 
                          GetOKSM_Code(Issuer.t_PartyID, NVL((SELECT /*+ index(hist DPTPRMHIST_DBT_IDX1)*/ t_ValueBefore
                                                                                   FROM (
                                                                                    SELECT hist.t_ValueBefore
                                                                                      FROM dptprmhist_dbt hist
                                                                                     WHERE hist.t_PartyID = Issuer.t_PartyID
                                                                                       AND hist.t_ParamKindID = RSI_RSBPARTY.PARTY_NRCOUNTRY
                                                                                       AND hist.t_BankDate > pEndDate
                                                                                       AND ROWNUM < 2
                                                                                     ORDER BY hist.t_BankDate)
                                                                                  ), Issuer.t_NRCountry), 
                                                                                  NVL((SELECT /*+ index(hist DPTPRMHIST_DBT_IDX1)*/ CASE WHEN t_ValueBefore = CHR (1) THEN CHR (0) ELSE CHR (88) END t_NotResident
                                                                                   FROM (
                                                                                    SELECT hist.t_ValueBefore
                                                                                      FROM dptprmhist_dbt hist
                                                                                     WHERE hist.t_PartyID = Issuer.t_PartyID
                                                                                       AND hist.t_ParamKindID = RSI_RSBPARTY.PARTY_NOTRESIDENT
                                                                                       AND hist.t_BankDate > pEndDate
                                                                                       AND ROWNUM < 2
                                                                                     ORDER BY hist.t_BankDate)
                                                                                  ), Issuer.t_NotResident), 
                                                                                  issuer.t_Superior, 
                                                                                  pEndDate, 
                                                                                  pIsRep12)  INTO v_LSIN, v_IssuerName, v_IssuerOKSMO
                FROM davoiriss_dbt avr, dfininstr_dbt parentfi, dparty_dbt issuer
                WHERE avr.t_FIID = one_rec.t_ParentFI
                    AND avr.t_FIID = parentfi.t_FIID
                    AND Issuer.t_PartyID = parentfi.t_Issuer
                    AND ROWNUM = 1;
             EXCEPTION
               WHEN NO_DATA_FOUND THEN
                v_LSIN        := one_rec.t_lsin;
                v_IssuerName  := one_rec.t_IssuerName;
                v_IssuerOKSMO := one_rec.t_IssuerOKSMO;
             END;
          ELSIF (one_rec.t_avrkindsroot = RSI_RSB_FIINSTR.AVOIRKIND_INVESTMENT_SHARE) THEN
            BEGIN
              SELECT v_IssuerName || ' (' || T_SZNAMEALG || ' ' ||  T_NAME || ')' INTO v_IssuerName
                FROM DAVRINVST_DBT INVST
                  JOIN DNAMEALG_DBT ALG ON ALG.T_ITYPEALG = 3430 /*ALG_FI_TYPEINVST*/  AND ALG.T_INUMBERALG = INVST.T_TYPE 
              WHERE T_FIID = one_rec.FIID
                   AND ROWNUM = 1;
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                v_IssuerName := one_rec.t_IssuerName;
            END;
         
          END IF;

          v_firest.t_SessionID      := pSessionID;
          v_firest.t_sysdate        := sysdate;
          v_firest.t_ContrID        := v_ClientContrId;
          v_firest.t_Contr_GroupID  := v_CONTR_GROUPID;
          v_firest.t_PartyID        := v_Party;
          v_firest.t_Client_GroupID := v_CLIENT_GROUPID;
          v_firest.t_FIID           := one_rec.fiid;
          v_firest.t_Qnty           := ABS(one_rec.count);
          v_firest.t_TotalCost      := v_TotalCost;
          v_firest.t_IsNotAvr       := one_rec.t_IsNotAvr;
          v_firest.t_Account        := one_rec.t_Account;
          v_firest.t_NKD            := v_AvrValue.t_NKDRub;
          v_firest.t_PriceFI        := v_AvrValue.t_FindCourseFi;
          v_firest.t_Price          := v_AvrValue.t_MarketPrice;
          v_firest.t_PriceRub       := v_AvrValue.t_MarketPriceRub;
          v_firest.t_ISIN           := v_ISIN;
          v_firest.t_LSIN           := v_LSIN;
          v_firest.t_IssuerName     := v_IssuerName;
          v_firest.t_IssuerOKSMO    := v_IssuerOKSMO;
          v_firest.t_AvrType        := one_rec.t_AvrType;

          INSERT INTO d724firest_dbt VALUES v_firest;
        END LOOP;
      END IF;
/*
        FOR one_rec IN (
                       SELECT rsb_account.restac (mc0.t_Account, mc0.t_Currency, pEndDate, mc0.t_Chapter, NULL) count,
                              mc0.t_Account, mc0.t_Currency, mc0.t_Chapter
                         FROM (  SELECT mc.t_Account, mc.t_Currency, mc.T_CHAPTER
                                   FROM dmcaccdoc_dbt mc
                                  WHERE mc.t_owner = v_Party
                                    AND mc.t_clientcontrid = v_ClientContrId
                                    AND mc.t_Chapter = 22
                                    AND mc.t_CatID = 838  --'ДМ клиента, ВУ'
                                    AND mc.t_iscommon = CHR(88)
                                  GROUP BY mc.t_Account, mc.t_Currency, mc.T_CHAPTER
                              ) mc0
                     )
        LOOP
          v_accrest.t_SessionID      := pSessionID;
          v_accrest.t_sysdate        := sysdate;
          v_accrest.t_ContrID        := v_ClientContrId;
          v_accrest.t_Contr_GroupID  := v_CONTR_GROUPID;
          v_accrest.t_PartyID        := v_Party;
          v_accrest.t_Client_GroupID := v_CLIENT_GROUPID;
          v_accrest.t_FI_Kind        := RSI_RSB_FIInstr.FIKIND_METAL;

          v_accrest.t_Account        := one_rec.t_Account;
          v_accrest.t_Chapter        := one_rec.t_Chapter;
          v_accrest.t_FIID           := one_rec.t_Currency;
          v_accrest.t_Amount_FIID    := ABS(one_rec.count);

          IF v_accrest.t_Amount_FIID > 0 THEN
            v_accrest.t_Amount_RUR     := RSI_RSB_FIInstr.ConvSum (v_accrest.t_Amount_FIID,
                                                                 one_rec.t_Currency,
                                                                 RSI_RSB_FIINSTR.NATCUR,
                                                                 pEndDate,
                                                                 7);
            INSERT INTO d724accrest_dbt VALUES v_accrest;
          END IF;
        END LOOP;
      */
        FOR one_rec IN (
                             SELECT mcacc.t_Account,
                                    mcacc.t_Code_Currency,
                                    mcacc.t_Chapter,
                                    rsb_account.restac(mcacc.t_Account,
                                                       mcacc.t_Code_Currency,
                                                       pEndDate,
                                                       mcacc.t_Chapter,
                                                       NULL) t_Rest
                               FROM (SELECT /* ordered use_nl(mc) index(mc DMCACCDOC_DBT_IDXC) */
                                            DISTINCT
                                            acc.t_Code_Currency,
                                            acc.t_Account,
                                            acc.t_Chapter
                                       FROM dmcaccdoc_dbt mc, daccount_dbt acc
                                      WHERE mc.t_catid in (70, 542) --ДС клиента, ц/б  Брокерский счет ДБО
                                        AND mc.t_owner = v_Party
                                        AND mc.t_clientcontrid = v_ClientContrId
                                        AND rsi_rsb_fiinstr.FI_GetRealFIKind(p_FIID => acc.t_code_currency) != RSI_RSB_FIInstr.FIKIND_METAL
                                        AND acc.t_Client = mc.t_Owner
                                        AND acc.t_Account = mc.t_Account
                                        AND acc.t_Chapter = mc.t_Chapter
                                        AND acc.t_Code_Currency = mc.t_Currency
                                        AND acc.t_Open_Date <= pEndDate
                                        AND (   acc.t_Close_Date = TO_DATE('01.01.0001', 'DD.MM.YYYY')
                                             OR acc.t_Close_Date >= pBegDate) ) mcacc
        )
        LOOP
          v_accrest.t_SessionID      := pSessionID;
          v_accrest.t_sysdate        := sysdate;
          v_accrest.t_ContrID        := v_ClientContrId;
          v_accrest.t_Contr_GroupID  := v_CONTR_GROUPID;
          v_accrest.t_PartyID        := v_Party;
          v_accrest.t_Client_GroupID := v_CLIENT_GROUPID;
          v_accrest.t_FI_Kind        := RSI_RSB_FIInstr.FIKIND_CURRENCY;

          v_accrest.t_Account        := one_rec.t_Account;
          v_accrest.t_Chapter        := one_rec.t_Chapter;
          v_accrest.t_FIID           := one_rec.t_Code_Currency;
          v_accrest.t_Amount_FIID    := ABS(one_rec.t_Rest);

          IF v_accrest.t_Amount_FIID > 0 THEN
            v_accrest.t_Amount_RUR     := RSI_RSB_FIInstr.ConvSum (v_accrest.t_Amount_FIID,
                                                                 one_rec.t_Code_Currency,
                                                                 RSI_RSB_FIINSTR.NATCUR,
                                                                 pEndDate);
            INSERT INTO d724accrest_dbt VALUES v_accrest;
          END IF;
        END LOOP;

      IF (pChapter = CHAPTER_ALL OR pChapter = CHAPTER_1_3) THEN
        INSERT INTO D724WRTMONEY_DBT(T_SESSIONID,
                                   T_CONTRID,
                                   T_CONTR_GROUPID,
                                   T_PARTYID,
                                   T_CODE,
                                   T_CLIENT_GROUPID,
                                   T_NPTXOPID,
                                   T_WRTIN,
                                   T_DATE,
                                   T_FIID,
                                   T_SUM,
                                   T_SUMRUB
                                  )
        SELECT /*+ cardinality(nptxop,10) leading(nptxop,oper,docs,trn) index(NPTXOP DNPTXOP_DBT_IDX5) */ 
               pSessionID,
               v_ClientContrId,
               v_CONTR_GROUPID,
               v_Party,
               nptxop.t_Code,
               v_CLIENT_GROUPID,
               nptxop.t_ID    t_nptxopID,
               CASE nptxop.t_subkind_operation
                    WHEN DL_NPTXOP_WRTKIND_ENROL  THEN CNST.SET_CHAR
                    WHEN DL_NPTXOP_WRTKIND_WRTOFF THEN CNST.UNSET_CHAR
               END
                    t_WrtIn,
               nptxop.t_OperDate,
               trn.t_FIID_Payer t_Currency,
               trn.t_Sum_Payer t_Sum,
               trn.t_Sum_Natcur t_SumRub
          FROM dnptxop_dbt nptxop, dacctrn_dbt trn, doproper_dbt oper, doprdocs_dbt docs
         WHERE     NPTXOP.T_DOCKIND = RSB_SECUR.DL_WRTMONEY
               AND NPTXOP.T_OPERDATE BETWEEN pBegDate AND pEndDate
               AND nptxop.t_status = RSI_NPTXC.DL_TXOP_CLOSE
               AND nptxop.t_Client = v_Party
               AND nptxop.t_Contract = v_ClientContrId
               AND nptxop.t_subkind_operation in (DL_NPTXOP_WRTKIND_ENROL, DL_NPTXOP_WRTKIND_WRTOFF)
               AND oper.t_dockind = NPTXOP.T_DOCKIND and oper.t_documentid = LPAD(NPTXOP.t_id, 34, '0')
               AND docs.t_id_operation = oper.t_id_operation and docs.t_dockind = 1 and docs.t_acctrnid = trn.t_acctrnid
               AND trn.t_chapter = 21
               AND decode((SELECT count(1) 
                    FROM dacctrn_dbt trn, doproper_dbt oper, doprdocs_dbt docs 
                   WHERE  oper.t_dockind = NPTXOP.T_DOCKIND and oper.t_documentid = LPAD(NPTXOP.t_id, 34, '0')
                    AND docs.t_id_operation = oper.t_id_operation and docs.t_dockind = 1 and docs.t_acctrnid = trn.t_acctrnid
                    AND trn.t_chapter = 1 and rownum < 2),0,0,1)= 1 
                AND NOT EXISTS (SELECT 1
                       FROM USR_ACC306ENROLL_DBT ae
                     WHERE ae.t_NptxopID = nptxop.t_ID 
                        AND SUBSTR(ae.t_DebetAccount, 1, 5) = '47422' AND SUBSTR(ae.t_CreditAccount, 1, 3) = '306'
                          );  
      END IF;

      IF (pChapter = CHAPTER_ALL OR pChapter = CHAPTER_1_3) THEN
        FOR one_rec IN (SELECT leg.t_PFI FIID,
                               leg.t_Principal,
                               tick.t_DealID,
                               tick.t_MarketId,
                               tick.t_DealDate,
                               tick.t_DealCode,
                               CASE
                                  WHEN RSB_SECUR.IsAvrWrtIn (RSB_SECUR.Get_OperationGroup (oprk.t_SysTypes)) = 1 THEN CHR(88)
                                  WHEN RSB_SECUR.IsAvrWrtOut (RSB_SECUR.Get_OperationGroup (oprk.t_SysTypes)) = 1 THEN CHR(0)
                               END
                                  t_WrtIn,
                               CASE RSI_RSB_FIINSTR.FI_AvrKindsGetRoot( 2, fi.t_AvoirKind )
                                  WHEN RSI_RSB_FIINSTR.AVOIRKIND_INVESTMENT_SHARE THEN (SELECT t_FormValueFIID FROM davrinvst_dbt WHERE t_FIID = fi.t_FIID)
                                  ELSE fi.t_FaceValueFI
                               END
                                  vn
                          FROM ddl_tick_dbt tick, doprkoper_dbt oprk, doproper_dbt opr, ddl_leg_dbt leg, dfininstr_dbt fi
                         WHERE     tick.t_BOfficeKind = RSB_SECUR.DL_AVRWRT
                               AND tick.t_ClientId = v_Party
                               AND tick.t_DealDate BETWEEN pBegDate AND pEndDate
                               AND tick.t_DealStatus = 20
                               AND opr.t_DocumentId = LPAD(tick.t_DealId, 34, '0')
                               AND opr.t_DocKind = tick.T_BOfficeKind
                               AND opr.t_Completed = CHR(88)
                               AND tick.t_ClientContrId = v_ClientContrId
                               AND leg.t_DealId = tick.t_DealId
                               AND leg.t_LegKind = 0 --LEG_KIND_DL_TICK
                               AND leg.t_LegId = 0
                               AND oprk.t_DocKind = tick.t_BOfficeKind
                               AND oprk.t_Kind_Operation = tick.t_DealType
                               AND fi.t_FIID = leg.t_PFI
                               AND fi.t_FI_Kind = RSI_RSB_FIINSTR.FIKIND_AVOIRISS
                               AND EXISTS (
                                  SELECT 1 FROM DOBJATCOR_DBT WHERE T_OBJECTTYPE = 101 AND T_GROUPID = 111 AND T_ATTRID = 2 AND T_OBJECT = LPAD(TICK.T_DEALID, 34, '0')
                               )
        )
        LOOP

          v_AvrValue := GetAvoirValue(one_rec.fiid, one_rec.vn, one_rec.t_Principal, one_rec.t_DealDate, v_UseNKDRate, one_rec.t_MarketId);
          IF v_AvrValue.t_SumRub IS NULL THEN
             v_tmp_sum := 0;
          ELSE
             v_tmp_sum := v_AvrValue.t_SumRub;

          END IF;


          v_wrtavr.t_SessionID      := pSessionID;
          v_wrtavr.t_sysdate        := sysdate;
          v_wrtavr.t_ContrID        := v_ClientContrId;
          v_wrtavr.t_Contr_GroupID  := v_CONTR_GROUPID;
          v_wrtavr.t_PartyID        := v_Party;
          v_wrtavr.t_Client_GroupID := v_CLIENT_GROUPID;
          v_wrtavr.t_DealID         := one_rec.t_DealID;
          v_wrtavr.t_WrtIn          := one_rec.t_WrtIn;
          v_wrtavr.t_IsAvr          := CHR(88);
          v_wrtavr.t_Date           := one_rec.t_DealDate;
          v_wrtavr.t_FIID           := one_rec.fiid;
          v_wrtavr.t_Qnty           := one_rec.t_Principal;
          v_wrtavr.t_TotalCost      := v_tmp_sum;
          v_wrtavr.t_Code           := one_rec.t_DealCode;
          v_wrtavr.t_NKD            := v_AvrValue.t_NKDRub;
          v_wrtavr.t_NKDNRUR        := v_AvrValue.t_NKD;
          v_wrtavr.t_PriceFIID      := v_AvrValue.t_FindCourseFi;
          v_wrtavr.t_Rate           := v_AvrValue.t_MarketPrice;
          v_wrtavr.t_CostNRur       := v_AvrValue.t_Sum;
        

          INSERT INTO d724wrtavr_dbt VALUES v_wrtavr;


        END LOOP;
      END IF;

      IF v_SF_SERVKIND IN (PTSK_DV, PTSK_CM) THEN
        CollectPFIDeals(pSessionID, v_ClientContrId, v_CONTR_GROUPID, v_Party, v_CLIENT_GROUPID, pEndDate);
        CollectPFITurn(pSessionID, v_ClientContrId, v_CONTR_GROUPID, v_Party, v_CLIENT_GROUPID, pEndDate, v_UseNKDRate);
      END IF;


      IF pIsRep12 <> 1 THEN
        IF v_SF_SERVKIND = PTSK_STOCKDL THEN
          CollectArrearSCData(pSessionID, v_ClientContrId, v_CONTR_GROUPID, v_Party, v_CLIENT_GROUPID, pEndDate);
        END IF;


        IF v_SF_SERVKIND IN (PTSK_DV, PTSK_CM) THEN
          CollectArrearPFIData(pSessionID, v_ClientContrId, v_CONTR_GROUPID, v_Party, v_CLIENT_GROUPID, pEndDate);
        END IF;
      END IF;

      COMMIT;
    end loop;
    
     it_log.log(p_msg  => 'pSessionID='||pSessionId||' '||
                'Завершение потока обработки ДО, диапазон ДО для обработки:'||pmin_ClientContrId||'-'||pmax_ClientContrId ,
                p_msg_type => it_log.C_MSG_TYPE__MSG) ;

  exception
    when others then
      rollback;
      it_error.put_error_in_stack;
      RSB_SPREPFUN.AddRepError('ОШИБКА в RSB_DL724REP.ProcessDO :' ||sqlerrm  );
      it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
      commit;
  END ProcessDO;

  --Формирование данных
  PROCEDURE CreateAllData( pBegDate IN DATE,
                           pEndDate IN DATE,
                           pChapter IN NUMBER,
                           pSessionID IN NUMBER,
                           pParallelLevel IN NUMBER,
                           pIsRep12 IN NUMBER )
  IS
    v_task_name VARCHAR2(30);
    v_sql_chunks CLOB;
    v_sql_process VARCHAR2(400);
    v_try NUMBER(5) := 0;
    v_status NUMBER;

  BEGIN


    IF(pParallelLevel > 0) THEN
      v_task_name := DBMS_PARALLEL_EXECUTE.generate_task_name;
      DBMS_PARALLEL_EXECUTE.create_task (task_name => v_task_name);

      v_sql_chunks := 'select min(t_SF_ID) as min_SF_ID, max(t_SF_ID) as max_SF_ID
         from (SELECT t_SF_ID, NTILE('||TO_CHAR(pParallelLevel*10)||') over (order by t_SF_ID) as t_NTILE FROM D724CONTR_DBT WHERE t_SessionID = '||TO_CHAR(pSessionID)||')
         group by t_NTILE' ;

      DBMS_PARALLEL_EXECUTE.create_chunks_by_sql(task_name => v_task_name,
                                                       sql_stmt  => v_sql_chunks,
                                                       by_rowid  => FALSE);

      v_sql_process := 'CALL RSB_DL724REP.ProcessDO('||TO_CHAR(pSessionID)||',:start_id, :end_id, '||
                                                     'TO_DATE('''||TO_CHAR(pBegDate, 'DD.MM.YYYY')||''',''DD.MM.YYYY''), '||
                                                     'TO_DATE('''||TO_CHAR(pEndDate, 'DD.MM.YYYY')||''',''DD.MM.YYYY''), '||
                                                    pIsRep12||', '||pChapter||') ';

      DBMS_PARALLEL_EXECUTE.run_task(task_name => v_task_name,
                                          sql_stmt => v_sql_process,
                                          language_flag => DBMS_SQL.NATIVE,
                                          parallel_level => pParallelLevel);

      v_status := DBMS_PARALLEL_EXECUTE.task_status(v_task_name);
      WHILE(v_try < 2 AND v_status != DBMS_PARALLEL_EXECUTE.FINISHED)
      LOOP
        v_try := v_try + 1;
        DBMS_PARALLEL_EXECUTE.resume_task(v_task_name);
        v_status := DBMS_PARALLEL_EXECUTE.task_status(v_task_name);
      END LOOP;


      DBMS_PARALLEL_EXECUTE.drop_task(v_task_name);
      COMMIT;

    ELSE -- для отладки

     FOR one_do IN (SELECT t_SF_ID FROM D724CONTR_DBT WHERE t_SessionID = pSessionID)
      LOOP
        RSB_DL724REP.ProcessDO(pSessionID,one_do.t_SF_ID,one_do.t_SF_ID,  pBegDate, pEndDate, pIsRep12, pChapter);
      END LOOP;

    END IF;


    IF gl_IsOptim = true THEN
      RSB_SPREPFUN.AddRepError( to_char(SYSDATE, 'hh24:mi:ss')||' CreateAllData End ' );
      commit;
    END IF;


--единичные действия по разреженной базе данных (чтобы например не проверять категорию на всех бумагах из зачислений),
--дополнить по мере появления таких проверок в отчёте

    FOR One_Rec IN (select Wrt.t_FIID
                      FROM d724wrtavr_dbt Wrt
                     WHERE Wrt.t_SessionID = pSessionID
                       AND RSB_SECUR.GetMainObjAttr(12, LPAD(Wrt.t_FIID, 10, '0'), 28, pEndDate) = 2
                       -- Не квалифицирована в качестве ценной бумаги
                     GROUP BY Wrt.t_FIID
                   )
    LOOP
      UPDATE d724wrtavr_dbt
         SET t_IsAvr = CHR(0)
       WHERE t_SessionID = pSessionID
         AND t_FIID = One_Rec.t_FIID;


    END LOOP;

     -- удаление дубликатов счетов
     DELETE FROM D724FIREST_DBT  
     WHERE rowid IN 
     (
           WITH Dups AS
           (
           SELECT rowid, ROW_NUMBER() OVER(PARTITION BY T_CONTR_GROUPID, T_PARTYID, T_FIID, T_ACCOUNT ORDER BY T_CONTRID) AS rn
           FROM D724FIREST_DBT 
            WHERE T_SESSIONID = pSessionID
           )
           SELECT rowid FROM Dups WHERE rn > 1
     );

    COMMIT;

    IF (pChapter = CHAPTER_ALL OR pChapter = CHAPTER_11) THEN
      CollectMetallData(pSessionID => pSessionID,
                        pBegDate      => pBegDate,
                        pEndDate      => pEndDate);
      commit;
    END IF;

  END CreateAllData;


  PROCEDURE FillTableR3CLIENT_GROUP( pBegDate IN DATE,
                           pEndDate IN DATE,
                           pSessionID IN NUMBER,
                           pParallelLevel IN NUMBER)
  as
  begin

   it_log.log(p_msg  => 'pSessionID='||pSessionId||' '||
                'Заполнение промежуточной таблицы для 3-го раздела отчета',
                p_msg_type => it_log.C_MSG_TYPE__MSG) ;

   insert into D724R3CLIENT_GROUP(T_SESSIONID,
                                  T_CLIENT_GROUPID,
                                  T_PARTYID,
                                  T_TYPE_FL,
                                  T_KI,
                                  T_ACTIVECLIENT,
                                  T_CODE_OKSM,
                                  T_CODE_OKATO,
                                  T_ACCKIND,
                                  T_ACCKINDIIS,
                                  T_ACCAMOUNT,
                                  T_ACCAMOUNTIIS,
                                  COUNTCLIENT,
                                  COUNTCLIENTIIS,
                                  COUNTCLIENTBEGCONTR,
                                  COUNTCLIENTENDCONTR)
with cl as ( select /*+ materialize */ CL.T_SESSIONID
            ,CL.T_CLIENT_GROUPID
            ,CL.T_PARTYID
            ,CL.T_TYPE_FL
            ,CL.T_KI
            ,CL.T_ACTIVECLIENT
            ,CL.T_CODE_OKSM
            ,CL.T_CODE_OKATO
          from D724CLIENT_DBT CL
          where CL.T_SESSIONID = pSessionID
          group by CL.T_SESSIONID
            ,CL.T_CLIENT_GROUPID
            ,CL.T_PARTYID
            ,CL.T_TYPE_FL
            ,CL.T_KI
            ,CL.T_ACTIVECLIENT
            ,CL.T_CODE_OKSM
            ,CL.T_CODE_OKATO
       ),
     CL_SUM as (
       select /*+ cardinality(cl 10000000)*/ CL.T_SESSIONID
      ,CL.T_CLIENT_GROUPID
      ,CL.T_PARTYID
      ,CL.T_TYPE_FL
      ,CL.T_KI
      ,CL.T_ACTIVECLIENT
      ,CL.T_CODE_OKSM
      ,CL.T_CODE_OKATO
      ,(select /*+ index(CL1 D724CLIENT_DBT_IDX1 )*/ sum(t_Contr)
          from D724CLIENT_DBT CL1
         where CL1.T_SESSIONID = pSessionID
           and CL1.T_PARTYID = CL.T_PARTYID
           and CL1.T_CLIENT_GROUPID = CL.T_CLIENT_GROUPID ) CountClient
      ,(select /*+ index(CL1 D724CLIENT_DBT_IDX1 )*/ sum(t_IISContr)
          from D724CLIENT_DBT CL1
         where CL1.T_SESSIONID = pSessionID
           and CL1.T_PARTYID = CL.T_PARTYID
           and CL1.T_CLIENT_GROUPID = CL.T_CLIENT_GROUPID) CountClientIIS
      ,((select /*+ index(FIREST D724FIREST_DBT_IDX1 )*/ NVL(sum(FIREST.T_TOTALCOST), 0)
          from D724FIREST_DBT FIREST
         where FIREST.T_SESSIONID = pSessionID
           and FIREST.T_PARTYID = CL.T_PARTYID
           and FIREST.T_CLIENT_GROUPID = CL.T_CLIENT_GROUPID) +
       (select NVL(sum(TREST.T_AMOUNT_RUR), 0)
          from (
          select /*+ index(ACCREST D724ACCREST_DBT_IDX1 )*/  distinct T_CONTR_GROUPID, T_CLIENT_GROUPID, T_ACCOUNT, T_CHAPTER, T_FIID, T_AMOUNT_RUR
           from D724ACCREST_DBT ACCREST
         where ACCREST.T_SESSIONID = pSessionID
           and ACCREST.T_PARTYID = CL.T_PARTYID
           and ACCREST.T_CLIENT_GROUPID = CL.T_CLIENT_GROUPID) TREST) +
     (select NVL(sum(TREST.T_REST_RUB), 0)
          from (
          select distinct T_CONTR_GROUPID, T_CLIENT_GROUPID, T_ACCOUNT, T_FIID, T_REST_RUB
          from  d724_metall_details METALL
         where METALL.T_PARTYID = CL.T_PARTYID
           and METALL.T_CLIENT_GROUPID = CL.T_CLIENT_GROUPID) TREST) +
      (select /*+index(ARREAR D724ARREAR_DBT_IDX1)*/ NVL(sum(ARREAR.T_VALUE), 0)
          from D724ARREAR_DBT ARREAR
         where ARREAR.T_SESSIONID = pSessionID
           and ARREAR.T_PARTYID = CL.T_PARTYID
           and ARREAR.T_CLIENT_GROUPID = CL.T_CLIENT_GROUPID
           and ARREAR.T_KIND = 1) -
      (select /*+index(ARREAR D724ARREAR_DBT_IDX1)*/ NVL(sum(ARREAR.T_VALUE), 0)
          from D724ARREAR_DBT ARREAR
         where ARREAR.T_SESSIONID = pSessionID
           and ARREAR.T_PARTYID = CL.T_PARTYID
           and ARREAR.T_CLIENT_GROUPID = CL.T_CLIENT_GROUPID
           and ARREAR.T_KIND = 2)) AccAmount
      ,((select /*+ index(FIREST D724FIREST_DBT_IDX1 )*/ NVL(sum(FIREST.T_TOTALCOST), 0)
          from D724FIREST_DBT FIREST
         where FIREST.T_SESSIONID = pSessionID
           and FIREST.T_PARTYID = CL.T_PARTYID
           and FIREST.T_CLIENT_GROUPID = CL.T_CLIENT_GROUPID
           and exists (select /*+ index(CONTR D724CONTR_DBT_IDX2 )*/ 1
                  from D724CONTR_DBT CONTR
                 where CONTR.T_SESSIONID = pSessionID
                   and CONTR.T_SF_ID = FIREST.T_CONTRID
                   and CONTR.T_IIS = 1)
           ) +
       (select NVL(sum(TRESTIIS.T_AMOUNT_RUR), 0)
          from (
          select /*+ index(ACCREST D724ACCREST_DBT_IDX1 )*/ distinct T_CONTR_GROUPID, T_CLIENT_GROUPID, T_ACCOUNT, T_CHAPTER, T_FIID, T_AMOUNT_RUR
          from  D724ACCREST_DBT ACCREST
         where ACCREST.T_SESSIONID = pSessionID
           and ACCREST.T_PARTYID = CL.T_PARTYID
           and ACCREST.T_CLIENT_GROUPID = CL.T_CLIENT_GROUPID
           and exists (select /*+ index(CONTR D724CONTR_DBT_IDX2 )*/ 1
                  from D724CONTR_DBT CONTR
                 where CONTR.T_SESSIONID = pSessionID
                   and CONTR.T_SF_ID = ACCREST.T_CONTRID
                   and CONTR.T_IIS = 1)) TRESTIIS
        ) +
         (select NVL(sum(TRESTIIS.T_REST_RUB), 0)
          from (
          select distinct T_CONTR_GROUPID, T_CLIENT_GROUPID, T_ACCOUNT, T_FIID, T_REST_RUB
          from  d724_metall_details METALL
         where METALL.T_PARTYID = CL.T_PARTYID
           and METALL.T_CLIENT_GROUPID = CL.T_CLIENT_GROUPID
           and exists (select /*+ index(CONTR D724CONTR_DBT_IDX2 )*/ 1
                  from D724CONTR_DBT CONTR
                 where CONTR.T_SESSIONID = pSessionID
                   and CONTR.T_SF_ID = METALL.T_SF_ID
                   and CONTR.T_IIS = 1)) TRESTIIS
        ) +
      (select /*+index(ARREAR D724ARREAR_DBT_IDX1)*/ NVL(sum(ARREAR.T_VALUE), 0)
          from D724ARREAR_DBT ARREAR
         where ARREAR.T_SESSIONID = pSessionID
           and ARREAR.T_PARTYID = CL.T_PARTYID
           and ARREAR.T_CLIENT_GROUPID = CL.T_CLIENT_GROUPID
           and ARREAR.T_KIND = 1
          and exists (select /*+ index(CONTR D724CONTR_DBT_IDX2 )*/ 1
                  from D724CONTR_DBT CONTR
                 where CONTR.T_SESSIONID = pSessionID
                   and CONTR.T_SF_ID = ARREAR.T_CONTRID
                   and CONTR.T_IIS = 1)
          ) -
      (select /*+index(ARREAR D724ARREAR_DBT_IDX1)*/ NVL(sum(ARREAR.T_VALUE), 0)
          from D724ARREAR_DBT ARREAR
         where ARREAR.T_SESSIONID = pSessionID
           and ARREAR.T_PARTYID = CL.T_PARTYID
           and ARREAR.T_CLIENT_GROUPID = CL.T_CLIENT_GROUPID
           and ARREAR.T_KIND = 2
           and exists (select /*+ index(CONTR D724CONTR_DBT_IDX2 )*/ 1
                  from D724CONTR_DBT CONTR
                 where CONTR.T_SESSIONID = pSessionID
                   and CONTR.T_SF_ID = ARREAR.T_CONTRID
                   and CONTR.T_IIS = 1)
           )) AccAmountIIS
      ,nvl((select /*+ index(CL1 D724CLIENT_DBT_IDX1 )*/ sum(t_BegContr)
          from D724CLIENT_DBT CL1
         where CL1.T_SESSIONID = pSessionID
           and CL1.T_PARTYID = CL.T_PARTYID
           and CL1.T_CLIENT_GROUPID = CL.T_CLIENT_GROUPID
           and CL1.T_BEGCONTR > CL1.T_PERIODSTARTCONTR), 0) CountClientBegContr
      ,nvl((select /*+ index(CL1 D724CLIENT_DBT_IDX1 )*/ sum(t_EndContr)
          from D724CLIENT_DBT CL1
         where CL1.T_SESSIONID = pSessionID
           and CL1.T_PARTYID = CL.T_PARTYID
           and CL1.T_CLIENT_GROUPID = CL.T_CLIENT_GROUPID
           and CL1.T_ENDCONTR > CL1.T_PERIODENDCONTR), 0) CountClientEndContr
       from CL),
       CL_ACCKIND as (
         select CL_SUM.T_SESSIONID
            ,CL_SUM.T_CLIENT_GROUPID
            ,CL_SUM.T_PARTYID
            ,CL_SUM.T_TYPE_FL
            ,CL_SUM.T_KI
            ,CL_SUM.T_ACTIVECLIENT
            ,CL_SUM.T_CODE_OKSM
            ,CL_SUM.T_CODE_OKATO
            ,CL_SUM.ACCAMOUNT
            ,CL_SUM.ACCAMOUNTIIS
            ,(case when CL_SUM.ACCAMOUNT < 0 then 1
                   when CL_SUM.ACCAMOUNT = 0 then 2
                   when CL_SUM.ACCAMOUNT <= 10000 then 3
                   when CL_SUM.ACCAMOUNT <= 100000 then 4
                   when CL_SUM.ACCAMOUNT <= 1000000 then 5
                   when CL_SUM.ACCAMOUNT <= 6000000 then 6
                   when CL_SUM.ACCAMOUNT <= 10000000 then 7
                   when CL_SUM.ACCAMOUNT <= 50000000 then 8
                   when CL_SUM.ACCAMOUNT <= 100000000 then 9
                   when CL_SUM.ACCAMOUNT <= 500000000 then 10
                   when CL_SUM.ACCAMOUNT <= 1000000000 then 11
                   else 12
              end) ACCKIND
              ,(case when CL_SUM.ACCAMOUNTIIS < 0 then 1
                   when CL_SUM.ACCAMOUNTIIS = 0 then 2
                   when CL_SUM.ACCAMOUNTIIS <= 10000 then 3
                   when CL_SUM.ACCAMOUNTIIS <= 100000 then 4
                   when CL_SUM.ACCAMOUNTIIS <= 1000000 then 5
                   when CL_SUM.ACCAMOUNTIIS <= 6000000 then 6
                   when CL_SUM.ACCAMOUNTIIS <= 10000000 then 7
                   when CL_SUM.ACCAMOUNTIIS <= 50000000 then 8
                   when CL_SUM.ACCAMOUNTIIS <= 100000000 then 9
                   when CL_SUM.ACCAMOUNTIIS <= 500000000 then 10
                   when CL_SUM.ACCAMOUNTIIS <= 1000000000 then 11
                   else 12
              end) ACCKINDIIS
              ,CL_SUM.COUNTCLIENT
              ,CL_SUM.COUNTCLIENTIIS
              ,CL_SUM.COUNTCLIENTBEGCONTR
              ,CL_SUM.COUNTCLIENTENDCONTR
         from CL_SUM
       )
       select 
             T_SESSIONID
            ,T_CLIENT_GROUPID
            ,T_PARTYID
            ,T_TYPE_FL
            ,T_KI
            ,T_ACTIVECLIENT
            ,T_CODE_OKSM
            ,T_CODE_OKATO
            ,ACCKIND
            ,ACCKINDIIS
            ,SUM(ACCAMOUNT)
            ,SUM(ACCAMOUNTIIS)
            ,SUM(COUNTCLIENT)
            ,SUM(COUNTCLIENTIIS)
            ,SUM(COUNTCLIENTBEGCONTR)
            ,SUM(COUNTCLIENTENDCONTR)
         from CL_ACCKIND
         group by T_SESSIONID, T_CLIENT_GROUPID, T_PARTYID, T_TYPE_FL, T_KI, T_ACTIVECLIENT, T_CODE_OKSM, T_CODE_OKATO, ACCKIND, ACCKINDIIS;

       commit;
       
        it_log.log(p_msg  => 'pSessionID='||pSessionId||' '||
                'Промежуточная таблица заполнена',
                p_msg_type => it_log.C_MSG_TYPE__MSG) ;
  end;
  
  function ConvSum (
    p_sum     number,
    p_from_fi number,
    p_to_fi   number,
    p_date    date
  ) return number deterministic is
  begin
    return RSI_RSB_FIINSTR.ConvSum(p_sum, p_from_fi, p_to_fi, p_date);
  end ConvSum;
  
  procedure prepare_cost_avr_detail (
    p_sessionid number,
    p_date      date
  ) is
  begin
  
     it_log.log(p_msg  => 'pSessionID='||p_sessionid||' '||
                'Формирование CSV для расшифровки "Стоимость цб клиентов"',
                p_msg_type => it_log.C_MSG_TYPE__MSG) ;
  
    for i in (select firest.t_client_groupid,
                     firest.t_contr_groupid,
                     cl.t_clientcode,
                     cl.t_name,
                     firest.t_IssuerName,
                     firest.t_ISIN,
                     firest.t_LSIN,
                     (select t_number from dsfcontr_dbt where t_id = firest.t_contrid) t_contr_num,
                     pcontr.t_number t_parent_contr_num,
                     fin.t_name t_avrname,
                     fin.t_facevalue,
                     firest.t_account,
                     replace((case
                               when substr(to_char(firest.t_qnty), 1, 1) = '.' then
                                '0' || to_char(firest.t_qnty)
                               else
                                to_char(firest.t_qnty)
                             end),
                             '.',
                             ',') t_qnty,
                     firest.t_IsNotAvr,
                     firest.t_NKD,
                     firest.t_price,
                     firest.t_totalcost,
                     pricefi.t_CCY t_priceccy,
                     facefi.t_CCY t_faceficcy,
                     nvl(rsb_dl724rep.ConvSum(fin.t_facevalue, fin.t_facevaluefi, 0, p_date), 0) t_facevaluerub,
                     firest.t_PriceRub,
                     nvl(avrkindsroot.t_name, chr(1)) t_avrkind,
                     firest.t_AvrType,
                     firest.t_IssuerOKSMO,
                     r3.t_AccKind
                from d724firest_dbt firest
                join d724client_dbt cl on cl.t_sessionid = firest.t_sessionid
                                      and cl.t_partyid = firest.t_partyid
                join d724contr_dbt cr on cr.t_sessionid = cl.t_sessionid
                                     and cr.t_sf_id = firest.t_contrid
                join d724r3client_group r3 on r3.t_sessionid = firest.t_Sessionid 
                                          and r3.t_client_groupid = firest.t_client_groupid 
                                          and r3.t_partyid = firest.t_partyid
                join dfininstr_dbt fin on fin.t_fiid = firest.t_fiid
                join davoiriss_dbt avr on avr.t_fiid = firest.t_fiid
                join dsfcontr_dbt pcontr on pcontr.t_id = cr.t_parent_sf_id
                left join dfininstr_dbt pricefi on pricefi.t_fiid = firest.t_pricefi
                left join dfininstr_dbt facefi on facefi.t_fiid = fin.t_facevaluefi
                left join dparty_dbt issuer on fin.t_issuer = issuer.t_partyid
                left join davrkinds_dbt avrkindsroot on avrkindsroot.t_fi_kind = fin.t_fi_kind
                                                    and avrkindsroot.t_avoirkind = rsb_fiinstr.fi_avrkindsgetroot(fin.t_fi_kind, fin.t_avoirkind)
               where firest.t_sessionid = p_sessionid
               order by firest.t_client_groupid,
                        firest.t_contr_groupid,
                        pcontr.t_datebegin,
                        firest.t_account)
    loop
      it_rsl_string.append_varchar(it_rsl_string.GetCell(i.t_client_groupid) ||
                                   it_rsl_string.GetCell(i.t_contr_groupid) ||
                                   it_rsl_string.GetCell(i.t_clientcode) ||
                                   it_rsl_string.GetCell(i.t_name) ||
                                   it_rsl_string.GetCell(i.t_parent_contr_num) ||
                                   it_rsl_string.GetCell(i.t_contr_num) ||
                                   it_rsl_string.GetCell(i.t_avrname) ||
                                   it_rsl_string.GetCell(i.t_LSIN) ||
                                   it_rsl_string.GetCell(i.t_ISIN) ||
                                   it_rsl_string.GetCell(i.t_avrkind) ||
                                   it_rsl_string.GetCell(i.t_IssuerName) ||
                                   it_rsl_string.GetCell(i.t_IssuerOKSMO) ||
                                   it_rsl_string.GetCell(i.t_AvrType) ||
                                   it_rsl_string.GetCell(i.t_account) ||
                                   it_rsl_string.GetCell(i.t_qnty) ||
                                   it_rsl_string.GetCell(i.t_IsNotAvr) ||
                                   it_rsl_string.GetCell(i.t_facevalue) ||
                                   it_rsl_string.GetCell(i.t_faceficcy) ||
                                   it_rsl_string.GetCell(i.t_facevaluerub) ||
                                   it_rsl_string.GetCell(i.t_price) ||
                                   it_rsl_string.GetCell(i.t_priceccy) ||
                                   it_rsl_string.GetCell(i.t_PriceRub) ||
                                   it_rsl_string.GetCell(i.t_NKD) ||
                                   it_rsl_string.GetCell(i.t_totalcost) ||
                                   it_rsl_string.GetCell(i.t_AccKind, true));
    end loop;    
    
   it_log.log(p_msg  => 'pSessionID='||p_sessionid||' '||
                'Формирование CSV завершено ',
                p_msg_type => it_log.C_MSG_TYPE__MSG) ;
    
  end prepare_cost_avr_detail;
  
    procedure prepare_clients_detail (
    p_sessionid number,
    p_date      date
  ) is
   v_cntcontr    integer := 0;
   v_cntiiscontr integer := 0;
   v_cntfirst    integer := 0;
   v_cntlast     integer := 0;
  
  begin
  
     it_log.log(p_msg  => 'pSessionID='||p_sessionid||' '||
                'Формирование CSV для расшифровки "Данные о старых и новых клиентах в отчетном периоде"',
                p_msg_type => it_log.C_MSG_TYPE__MSG) ;
  
    for i in (select cl.t_client_groupid, cl.t_clientcode, cl.t_name, r3.t_acckind, cl.t_contr, cl.t_iiscontr, case when cl.t_begcontr > cl.t_periodstartcontr then 1 else 0 end t_isfirst, case when cl.t_endcontr > cl.t_periodendcontr then 1 else 0 end t_islast
                 from d724client_dbt cl
             left join d724r3client_group r3 on r3.t_sessionid = cl.t_Sessionid and r3.t_client_groupid = cl.t_client_groupid and r3.t_partyid = cl.t_partyid
               where cl.t_sessionid = p_sessionid
               order by cl.t_client_groupid,
                        cl.t_clientcode,
                        cl.t_name)
    loop
      it_rsl_string.append_varchar(it_rsl_string.GetCell(i.t_client_groupid) ||
                                   it_rsl_string.GetCell(i.t_clientcode) ||
                                   it_rsl_string.GetCell(i.t_name) ||
                                   it_rsl_string.GetCell(i.t_acckind) ||
                                   it_rsl_string.GetCell(i.t_contr) ||
                                   it_rsl_string.GetCell(i.t_iiscontr) ||
                                   it_rsl_string.GetCell(i.t_isfirst) ||
                                   it_rsl_string.GetCell(i.t_islast, true));
                                   
       v_cntcontr    := v_cntcontr    + i.t_contr;
       v_cntiiscontr := v_cntiiscontr + i.t_iiscontr;
       v_cntfirst    := v_cntfirst    + i.t_isfirst;
       v_cntlast     := v_cntlast     + i.t_islast;
    end loop;   
    it_rsl_string.append_varchar(it_rsl_string.GetCell('Итого:') ||
                                   it_rsl_string.GetCell('') ||
                                   it_rsl_string.GetCell('') ||
                                   it_rsl_string.GetCell('') ||
                                   it_rsl_string.GetCell(v_cntcontr) ||
                                   it_rsl_string.GetCell(v_cntiiscontr) ||
                                   it_rsl_string.GetCell(v_cntfirst) ||
                                   it_rsl_string.GetCell(v_cntlast, true));
     
     it_log.log(p_msg  => 'pSessionID='||p_sessionid||' '||
                'Формирование CSV завершено ',
                p_msg_type => it_log.C_MSG_TYPE__MSG) ;
    
  end prepare_clients_detail;

END RSB_DL724REP;
/
