CREATE OR REPLACE PACKAGE BODY rsi_rsb_account
IS

  TYPE DpDepRec_t   IS RECORD(t_Code       DpDepCode_t
                             ,t_Status     DpDepStatus_t
                             ,t_PartyID    DpDepPartyID_t
                             ,t_ParentCode DpDepParentCode_t
                             ,t_CheckData  BankDprtCheckData_t
                             );
  TYPE DpDepTable_t IS TABLE OF DpDepRec_t;

  TYPE BicCodeRec_t   IS RECORD(t_Code       ObjcodeCode_t
                               ,t_PartyID    DpDepPartyID_t
                               ,t_UERType    BankDprtUERType_t
                               );
  TYPE BicCodeTable_t IS TABLE OF BicCodeRec_t;

  m_DpDeps    DpDepTable_t;
  m_BicCodes  BicCodeTable_t;

  m_KeyPosition NUMBER; --Позиция ключа в номере счета
  m_KeyAlgorithm NUMBER; --Алгоритм расчета ключа в номере счета
  m_MaketKey VARCHAR2(100); --Макет для расчета ключа
  m_LenMaketKey NUMBER; --Длина макета для расчета ключа
  m_MFOMaketKey VARCHAR2(100); --Макет для расчета ключа по БИК
  m_LenMFOMaketKey NUMBER; --Длина макета для расчета ключа по БИК

  TYPE LOC_TRN_ID IS TABLE OF CHAR(1) INDEX BY VARCHAR2(20);
  m_LocTrnID LOC_TRN_ID;

  --Установить обновление массивов для расчета ключа счета
  PROCEDURE setRefreshArrays
  AS
    v_LocTrnID VARCHAR2(20);
  BEGIN
    SELECT NVL(DBMS_TRANSACTION.LOCAL_TRANSACTION_ID, CHR(1)) INTO v_LocTrnID FROM dual;
    IF NOT m_LocTrnID.EXISTS(v_LocTrnID) THEN
      m_LocTrnID(v_LocTrnID) := 'X';
      IF m_DpDeps IS NOT NULL THEN
        m_DpDeps.DELETE;
        m_DpDeps := NULL;
      END IF;
      IF m_BicCodes IS NOT NULL THEN
        m_BicCodes.DELETE;
        m_BicCodes := NULL;
      END IF;
    END IF;
  END setRefreshArrays;

  -- Инициализировать позицию ключа в номере счета
  -- Инициализировать алгоритм расчета ключа в номере счета
  -- Инициализировать макет для расчета ключа
  -- Инициализировать макет для расчета ключа по БИК
  PROCEDURE initAccountKeyData
  AS
    v_Reserv dbank_dbt.t_Reserv%type;
    v_KeyAlgorithm VARCHAR2(100);
  BEGIN
    IF m_DpDeps IS NULL THEN
      SELECT
        dprt.t_Code
       ,dprt.t_Status
       ,dprt.t_PartyID
       ,dprt.t_ParentCode
       ,bank.t_CheckData
        BULK COLLECT INTO m_DpDeps
        FROM ddp_dep_dbt dprt
        FULL OUTER JOIN dbankdprt_dbt bank
          ON (bank.t_PartyID = dprt.t_PartyID)
      ORDER BY dprt.t_Code;
    END IF;
    IF m_BicCodes IS NULL THEN
      SELECT
        oc.t_Code
       ,oc.t_ObjectID
       ,bank.t_UERType
        BULK COLLECT INTO m_BicCodes
        FROM dobjcode_dbt oc
        FULL OUTER JOIN dbankdprt_dbt bank
          ON (bank.t_PartyID = oc.t_ObjectID)
       WHERE oc.t_CodeKind = 3/*PTCK_BIC*/
         AND oc.t_ObjectType = 3 /*OBJTYPE_PARTY*/
         AND oc.t_State = 0
--         AND EXISTS(SELECT 1 FROM ddp_dep_dbt dprt WHERE dprt.t_PartyID = oc.t_ObjectID)
       ORDER BY (SELECT COUNT(*) FROM ddp_dep_dbt dprt WHERE dprt.t_PartyID = oc.t_ObjectID) DESC
      ;
    END IF;
    IF m_KeyPosition IS NULL THEN
      m_KeyPosition := rsb_common.GetRegIntValue('BANKINI\ОБЩИЕ ПАРАМЕТРЫ\КЛЮЧЕВАНИЕ СЧЕТОВ\KEYPOSITION', 0);
      IF m_KeyPosition IS NULL OR m_KeyPosition <= 0 OR m_KeyPosition > 25 THEN
        m_KeyPosition := 9;
      END IF;
/*
    dbms_output.put_line(TO_CHAR(m_KeyPosition));
*/
    END IF;
    IF m_KeyAlgorithm IS NULL THEN
      v_KeyAlgorithm := rsb_common.GetRegStrValue('BANKINI\ОБЩИЕ ПАРАМЕТРЫ\КЛЮЧЕВАНИЕ СЧЕТОВ\KEYALGORITHM', 0);
      IF UPPER(TRIM(v_KeyAlgorithm)) = 'OLD' THEN
        m_KeyAlgorithm := 1;
      ELSIF UPPER(TRIM(v_KeyAlgorithm)) = 'NEW' THEN
        m_KeyAlgorithm := 2;
      ELSE
        m_KeyAlgorithm := 0;
      END IF;
/*
    dbms_output.put_line(TO_CHAR(m_KeyAlgorithm));
*/
    END IF;
    IF   m_MaketKey IS NULL
      OR m_MFOMaketKey IS NULL
    THEN
      SELECT t_Reserv INTO v_Reserv FROM dbank_dbt WHERE t_ID = 0;
      m_MaketKey := utl_raw.cast_to_varchar2(utl_raw.substr(v_Reserv, 646, 21));
      m_MaketKey := m_MaketKey || utl_raw.cast_to_varchar2(utl_raw.substr(v_Reserv, 1101, 6));
      m_LenMaketKey := LENGTH(m_MaketKey);
      m_MFOMaketKey := utl_raw.cast_to_varchar2(utl_raw.substr(v_Reserv, 907, 36));
      m_LenMFOMaketKey := LENGTH(m_MFOMaketKey);
/*
    dbms_output.put_line(m_MaketKey);
    dbms_output.put_line(TO_CHAR(m_LenMaketKey));
    dbms_output.put_line(m_MFOMaketKey);
    dbms_output.put_line(TO_CHAR(m_LenMFOMaketKey));
*/
    END IF;
  END initAccountKeyData;

  -- Получить данные для проверки ключевания
  FUNCTION getBankDprtCheckData(p_Code IN DpDepCode_t) RETURN BankDprtCheckData_t DETERMINISTIC
  AS
  BEGIN
    setRefreshArrays;
    initAccountKeyData;
    FOR i IN m_DpDeps.FIRST .. m_DpDeps.LAST
    LOOP
      IF m_DpDeps(i).t_Code = p_Code THEN
        IF    m_DpDeps(i).t_ParentCode > 0
          AND (   m_DpDeps(i).t_Status != 2/*DEPARTMENT_STATUS_ACTIVE*/
               OR m_DpDeps(i).t_CheckData IS NULL)
        THEN
          m_DpDeps(i).t_CheckData := getBankDprtCheckData(m_DpDeps(i).t_ParentCode);
        END IF;
        RETURN m_DpDeps(i).t_CheckData;
      END IF;
    END LOOP;
    RETURN NULL;
  END getBankDprtCheckData;

  -- Получить тип банка
  FUNCTION getBankDprtUERType(p_Code IN ObjcodeCode_t) RETURN BankDprtUERType_t DETERMINISTIC
  AS
  BEGIN
    setRefreshArrays;
    initAccountKeyData;
    FOR i IN m_BicCodes.FIRST .. m_BicCodes.LAST
    LOOP
      IF m_BicCodes(i).t_Code = p_Code THEN
        RETURN m_BicCodes(i).t_UERType;
      END IF;
    END LOOP;
    RETURN NULL;
  END getBankDprtUERType;

  -- Получить код БИК банка
  FUNCTION getBankDprtCode(p_PartyID IN DpDepPartyID_t) RETURN ObjcodeCode_t DETERMINISTIC
  AS
  BEGIN
    setRefreshArrays;
    initAccountKeyData;
    FOR i IN m_BicCodes.FIRST .. m_BicCodes.LAST
    LOOP
      IF m_BicCodes(i).t_PartyID = p_PartyID THEN
        RETURN m_BicCodes(i).t_Code;
      END IF;
    END LOOP;
    RETURN NULL;
  END getBankDprtCode;

  FUNCTION CtoI(p_Char IN CHAR) RETURN NUMBER
  AS
    v_Char CHAR(1);
  BEGIN
    v_Char := UPPER(p_Char);
    IF    v_Char != '0'
      AND v_Char != '1'
      AND v_Char != '2'
      AND v_Char != '3'
      AND v_Char != '4'
      AND v_Char != '5'
      AND v_Char != '6'
      AND v_Char != '7'
      AND v_Char != '8'
      AND v_Char != '9'
    THEN
      RETURN 0;
    END IF;
    RETURN TO_NUMBER(v_Char);
  END CtoI;

  -- Нововведение - использование алфавитных значений в номере счета
  -- (клиринговая валюта). АВСЕНКМРТХ
  FUNCTION CtoIEx(p_Char IN CHAR) RETURN NUMBER
  AS
    v_Char CHAR(1);
  BEGIN
    v_Char := CASE UPPER(p_Char)
              --Латинские
              WHEN 'A' THEN '0'
              WHEN 'B' THEN '1'
              WHEN 'C' THEN '2'
              WHEN 'E' THEN '3'
              WHEN 'H' THEN '4'
              WHEN 'K' THEN '5'
              WHEN 'M' THEN '6'
              WHEN 'P' THEN '7'
              WHEN 'T' THEN '8'
              WHEN 'X' THEN '9'
              --Кириллица
              WHEN 'А' THEN '0'
              WHEN 'В' THEN '1'
              WHEN 'С' THEN '2'
              WHEN 'Е' THEN '3'
              WHEN 'Н' THEN '4'
              WHEN 'К' THEN '5'
              WHEN 'М' THEN '6'
              WHEN 'Р' THEN '7'
              WHEN 'Т' THEN '8'
              WHEN 'Х' THEN '9'
              ELSE UPPER(p_Char)
              END;
    RETURN CtoI(v_Char);
  END CtoIEx;

  -- Получить счет с ключом
  FUNCTION GetAccountKeyByDprtData(p_Account IN VARCHAR2, p_DprtData IN BankDprtCheckData_t) RETURN VARCHAR2
  AS
    v_LenAccount NUMBER;
    v_LenMaketKey NUMBER;
    v_DprtData VARCHAR2(100);
    v_LenDprtData NUMBER;
    v_KeyAlgorithm NUMBER;
    v_RefA NUMBER;
    v_RefD NUMBER;
    v_RefM NUMBER;
    v_Key NUMBER;
    v_Sum NUMBER;
    v_Char CHAR(1);
    v_OkAccount VARCHAR2(100);
    v_BIKdigits VARCHAR2(3);
    v_UERType NUMBER;
  BEGIN
    --Инициализация данных для расчета ключа
    initAccountKeyData;
    v_LenAccount := LENGTH(p_Account);
    v_DprtData := p_DprtData;
    v_LenDprtData := LENGTH(v_DprtData);
    v_LenMaketKey := m_LenMaketKey;
    v_Sum := 0;

    IF m_KeyAlgorithm = 0 THEN
      v_KeyAlgorithm := CASE WHEN v_LenAccount <= 9 THEN 1 ELSE 2 END;
    END IF;
    --Расчет по старому алгоритму
    IF v_KeyAlgorithm = 1 THEN
/*
    dbms_output.put_line('v_KeyAlgorithm = 1');
*/
      v_RefA := v_LenAccount;
      v_RefD := v_LenDprtData;
      v_RefM := v_LenMaketKey;

      v_RefD := CASE WHEN v_LenDprtData > 5 THEN v_RefD - 1 ELSE v_RefD END;
      FOR i IN 0..8 LOOP
        IF i != 2 THEN
          v_Key := CASE WHEN v_LenAccount > i THEN CtoI(SUBSTR(p_Account, (v_RefA - i), 1)) ELSE 0 END *
                   CASE WHEN v_LenMaketKey > i THEN CtoI(SUBSTR(m_MaketKey, (v_RefM - i), 1)) ELSE 0 END;
          v_Sum := v_Sum + CASE WHEN v_Key > 9 THEN MOD(v_Key, 10) ELSE v_Key END;
        END IF;
      END LOOP;

      v_LenMaketKey := v_LenMaketKey - 9;
      v_RefM := v_RefM - 9;

      FOR i IN 0..7 LOOP
        v_Char := SUBSTR(v_DprtData, (v_RefD - i), 1);
        v_Char := CASE v_Char
                  WHEN 'A' THEN '0'
                  WHEN 'U' THEN '0'
                  WHEN 'O' THEN '0'
                  WHEN 'S' THEN '1'
                  WHEN 'B' THEN '1'
                  WHEN 'F' THEN '2'
                  WHEN 'C' THEN '2'
                  WHEN 'G' THEN '3'
                  WHEN 'D' THEN '3'
                  WHEN 'J' THEN '4'
                  WHEN 'E' THEN '4'
                  WHEN 'L' THEN '5'
                  WHEN 'H' THEN '5'
                  WHEN 'Z' THEN '6'
                  WHEN 'K' THEN '6'
                  WHEN 'V' THEN '7'
                  WHEN 'M' THEN '7'
                  WHEN 'N' THEN '8'
                  WHEN 'P' THEN '8'
                  WHEN 'X' THEN '9'
                  WHEN 'T' THEN '9'
                  ELSE v_Char
                  END;
        v_Key := CASE WHEN v_LenDprtData > i THEN CtoI(v_Char) ELSE 0 END *
                 CASE WHEN v_LenMaketKey > i THEN CtoI(SUBSTR(m_MaketKey, (v_RefM - i), 1)) ELSE 0 END;
        v_Sum := v_Sum + CASE WHEN v_Key > 9 THEN MOD(v_Key, 10) ELSE v_Key END;
      END LOOP;

      v_Sum := v_Sum * 3;
      v_Sum := CASE WHEN v_Sum > 9 THEN MOD(v_Sum, 10) ELSE v_Sum END;

      IF v_LenAccount < 3 THEN
        v_OkAccount := p_Account;
      ELSE
        v_OkAccount := SUBSTR(p_Account, 1, v_LenAccount - 3) || CHR(v_Sum + 48/*'0'*/) || SUBSTR(p_Account, v_LenAccount - 2, 2);
      END IF;
    --Расчет по новому алгоритму
    ELSIF v_KeyAlgorithm = 2 THEN
    -- Новый алгоритм (по письму 515 ЦБ РФ)
/*
    dbms_output.put_line('v_KeyAlgorithm = 2');
*/

      v_BIKdigits := '000';
      -- Цикл по разрядам БИК. Для кредитной организации берем 7,8,9
      -- разряды БИК (для РКЦ - "0", 5й и 6й разряды)
      -- Макет для России :
      --  713 71371371371371371371
      -- LБИК+------- счет --T-----
      --   L--MFOMaketKey    ж
      --                     L--MaketKey
      -- 1. Заполняем условный номер РКЦ/кредитной организации. Это
      --    единственное различие в методе расчета, потому выделено
      --    отдельно.

      v_UERType := getBankDprtUERType(v_DprtData);
      IF    v_UERType IS NOT NULL
        AND (   v_UERType = 1  /*PT_KIND_PAYM_CASH_CENTRE*/
             OR v_UERType = 2) /*PT_KIND_FIELDOFFICE_CENTRALBANK*/
      THEN
        FOR i IN 2..3 LOOP
          IF (i + 4) <= v_LenAccount THEN
            v_BIKdigits := SUBSTR(v_BIKdigits, 1, i - 1) || SUBSTR(v_DprtData, i + 3, 1) || SUBSTR(v_BIKdigits, i + 1);
          END IF;
        END LOOP;
/*
    dbms_output.put_line('getBankDprtUERType(v_DprtData) = 1 or 2');
    dbms_output.put_line('v_BIKdigits = ' || v_BIKdigits);
*/
      ELSE
        FOR i IN 1..3 LOOP
          IF (i + 7) <= v_LenAccount THEN
            v_BIKdigits := SUBSTR(v_BIKdigits, 1, i - 1) || SUBSTR(v_DprtData, i + 6, 1) || SUBSTR(v_BIKdigits, i + 1);
          END IF;
        END LOOP;
/*
    dbms_output.put_line('getBankDprtUERType(v_DprtData) != 1 or 2');
    dbms_output.put_line('v_BIKdigits = ' || v_BIKdigits);
*/
      END IF;
      -- 2. Далее все одинаково.  Цикл по условному номеру.
      FOR i IN 1..3 LOOP
        v_Sum := v_Sum + CtoI(SUBSTR(m_MFOMaketKey, i, 1)) * CtoI(SUBSTR(v_BIKdigits, i, 1));
      END LOOP;
/*
    dbms_output.put_line('v_Sum = ' || TO_CHAR(v_Sum));
*/
      -- 3. Цикл по знакам в счете. Ключевой разряд не считаем!
      -- Возможно наличие в 6м разряде счета алфавитного значения. Это
      -- значение отлавливается функцией CtoIEx(..) в любой
      -- позиции номера лицевого счета.
      FOR i IN 1..v_LenAccount LOOP
        IF i < v_LenMaketKey AND i != m_KeyPosition THEN
          v_Sum := v_Sum + CtoI(SUBSTR(m_MaketKey, i, 1)) * CtoIEx(SUBSTR(p_Account, i, 1));
        END IF;
      END LOOP;

      v_Sum := MOD(MOD(v_Sum, 10) * 3, 10);
/*
    dbms_output.put_line('v_Sum = ' || TO_CHAR(v_Sum));
*/

      IF v_LenAccount < m_KeyPosition THEN
        v_OkAccount := p_Account;
      ELSE
        v_OkAccount := SUBSTR(p_Account, 1, m_KeyPosition - 1) || CHR(v_Sum + 48/*'0'*/) || SUBSTR(p_Account, m_KeyPosition + 1);
      END IF;
    END IF;

    RETURN v_OkAccount;
  END GetAccountKeyByDprtData;

  FUNCTION GetAccountKeyByDprtCode(p_Account IN VARCHAR2, p_Code IN DpDepCode_t DEFAULT NULL) RETURN VARCHAR2
  AS
    v_Code  DpDepCode_t;
    v_DprtData ObjcodeCode_t;
  BEGIN
    v_Code := CASE WHEN p_Code IS NULL THEN RsbSessionData.OperDprt() ELSE p_Code END;
    v_DprtData := getBankDprtCheckData(v_Code);
    RETURN GetAccountKeyByDprtData(p_Account, v_DprtData);
  END GetAccountKeyByDprtCode;

  FUNCTION GetAccountKeyByPartyID(p_Account IN VARCHAR2, p_PartyID IN DpDepPartyID_t) RETURN VARCHAR2
  AS
    v_DprtData ObjcodeCode_t;
  BEGIN
    v_DprtData := getBankDprtCode(p_PartyID);
    RETURN GetAccountKeyByDprtData(p_Account, v_DprtData);
  END GetAccountKeyByPartyID;

  --Проверить ключ счета
  FUNCTION CheckAccountKeyByDprtCode(p_Account IN VARCHAR2, p_Code IN DpDepCode_t DEFAULT NULL) RETURN NUMBER
  AS
    v_Code  DpDepCode_t;
    v_Error NUMBER;
  BEGIN
    v_Code := CASE WHEN p_Code IS NULL THEN RsbSessionData.OperDprt() ELSE p_Code END;
    v_Error := CASE WHEN GetAccountKeyByDprtCode(p_Account, v_Code) != p_Account THEN 1 ELSE 0 END;
    RETURN v_Error;
  END CheckAccountKeyByDprtCode;

  FUNCTION CheckAccountKeyByDprtData(p_Account IN VARCHAR2, p_DprtData IN BankDprtCheckData_t) RETURN NUMBER
  AS
    v_Error NUMBER;
  BEGIN
    v_Error := CASE WHEN GetAccountKeyByDprtData(p_Account, p_DprtData) != p_Account THEN 1 ELSE 0 END;
    RETURN v_Error;
  END CheckAccountKeyByDprtData;

  FUNCTION CheckAccountKeyByPartyID(p_Account IN VARCHAR2, p_PartyID IN DpDepPartyID_t) RETURN NUMBER
  AS
    v_Error NUMBER;
  BEGIN
    v_Error := CASE WHEN GetAccountKeyByPartyID(p_Account, p_PartyID) != p_Account THEN 1 ELSE 0 END;
    RETURN v_Error;
  END CheckAccountKeyByPartyID;

------------------------------------------------------------------------------
-- Функция возвращает параметр RsVox объекта по имени
------------------------------------------------------------------------------
FUNCTION GetRsVoxPrmVal(PrmName IN VARCHAR2) RETURN RAW
AS
BEGIN
RETURN rsvoxprm.GetPrmVal(PrmName);
END GetRsVoxPrmVal;

------------------------------------------------------------------------------
-- Функция определения кредита для рублевых л/с за любой месяц ТЕКУЩЕГО года
------------------------------------------------------------------------------
FUNCTION kreditmontha
(
  p_account  IN VARCHAR2
 ,p_chapter  IN NUMBER
 ,p_month    IN NUMBER
 ,p_cur      IN NUMBER DEFAULT NULL
 ,p_rest_cur IN NUMBER DEFAULT NULL
)
RETURN  drestdate_dbt.t_Credit%TYPE
AS
  v_sum      drestdate_dbt.t_Credit%TYPE := 0;

  v_cur      NUMBER;
  v_rest_cur NUMBER;
BEGIN

  IF (p_cur IS NULL) THEN  
    v_cur := 0; -- NATCUR
  ELSE
    v_cur := p_cur;
  END IF;

  IF (p_rest_cur IS NULL) THEN  
    v_rest_cur := v_cur;
  ELSE
    v_rest_cur := p_rest_cur;
  END IF;

  RETURN kreditmonthac(p_account, p_chapter, v_cur, p_month, v_rest_cur);

EXCEPTION
  WHEN NO_DATA_FOUND THEN
    RETURN v_sum;
  WHEN OTHERS THEN
    rsi_errors.err_msg := 'kreditmontha '||SQLERRM (SQLCODE);
    RETURN 0;

END kreditmontha;

-----------------------------------------------------------------------------
-- Функция определения дебета для рублевых л/с за любой месяц ТЕКУЩЕГО года
-----------------------------------------------------------------------------
FUNCTION debetmontha
(
  p_account  IN VARCHAR2
 ,p_chapter  IN NUMBER
 ,p_month    IN NUMBER
 ,p_cur      IN NUMBER DEFAULT NULL
 ,p_rest_cur IN NUMBER DEFAULT NULL
)
RETURN  drestdate_dbt.t_Debet%TYPE
AS
  v_sum      drestdate_dbt.t_Debet%TYPE := 0;
  v_cur      NUMBER;
  v_rest_cur NUMBER;

BEGIN
  IF p_month < 1 OR p_month > 12 THEN
    RETURN v_sum;
  END IF;

 IF (p_cur IS NULL) THEN  
    v_cur := 0; -- NATCUR
  ELSE
    v_cur := p_cur;
  END IF;

  IF (p_rest_cur IS NULL) THEN  
    v_rest_cur := v_cur;
  ELSE
    v_rest_cur := p_rest_cur;
  END IF;

  RETURN debetmonthac( p_account, p_chapter, v_cur, p_month, v_rest_cur);
  
EXCEPTION
  WHEN NO_DATA_FOUND THEN
    RETURN v_sum;
  WHEN OTHERS THEN
    rsi_errors.err_msg := 'debetmontha '||SQLERRM (SQLCODE);
    RETURN 0;

END debetmontha;

-------------------------------------------------------------------------------
-- Функция определения кредита для валютных л/с за любой месяц ТЕКУЩЕГО года
-------------------------------------------------------------------------------
FUNCTION kreditmonthac
(
  p_account  IN VARCHAR2
 ,p_chapter  IN NUMBER
 ,p_cur      IN NUMBER
 ,p_month    IN NUMBER
 ,p_rest_cur IN NUMBER DEFAULT NULL
)
RETURN drestdate_dbt.t_Credit%TYPE
AS
  v_sum      drestdate_dbt.t_Credit%TYPE := 0;
  v_rest_cur NUMBER(10);
BEGIN
  IF p_month < 1 OR p_month > 12 THEN
    RETURN v_sum;
  END IF;

  -- Если валюта отстатка не передана, считаем что остаток в той же валюте что и счет.
  IF (p_rest_cur IS NULL ) THEN
    v_rest_cur := p_cur;
  ELSE
    v_rest_cur := p_rest_cur;
  END IF; 

  SELECT NVL(SUM(rd.t_Credit), 0) INTO v_sum
    FROM drestdate_dbt rd, daccount_dbt acc
   WHERE acc.t_Account       = p_account
     AND acc.t_Chapter       = p_chapter
     AND acc.t_Code_Currency = p_cur
     AND rd.t_AccountID      = acc.t_AccountID
     AND rd.t_RestCurrency   = v_rest_cur
     AND EXTRACT(YEAR  FROM rd.t_RestDate) = EXTRACT(YEAR FROM CNST.currdate)
     AND EXTRACT(MONTH FROM rd.t_RestDate) = p_month;

  RETURN v_sum;

EXCEPTION
  WHEN NO_DATA_FOUND THEN
    RETURN v_sum;
  WHEN OTHERS THEN
    rsi_errors.err_msg := 'kreditmonthac '||SQLERRM (SQLCODE);
    RETURN 0;

END kreditmonthac;

-----------------------------------------------------------------------------
-- Функция определения дебета для валютных л/с за любой месяц ТЕКУЩЕГО года
-----------------------------------------------------------------------------
FUNCTION debetmonthac
(
  p_account IN VARCHAR2
 ,p_chapter IN NUMBER
 ,p_cur     IN NUMBER
 ,p_month   IN NUMBER
 ,p_rest_cur IN NUMBER DEFAULT NULL
)
RETURN  drestdate_dbt.t_Debet%TYPE
AS
  v_sum      drestdate_dbt.t_Debet%TYPE := 0;
  v_rest_cur NUMBER(10);
BEGIN
  IF p_month < 1 OR p_month > 12 THEN
    RETURN v_sum;
  END IF;

  -- Если валюта отстатка не передана, считаем что остаток в той же валюте что и счет.
  IF (p_rest_cur IS NULL ) THEN
    v_rest_cur := p_cur;
  ELSE
    v_rest_cur := p_rest_cur;
  END IF; 


  SELECT NVL(SUM(rd.t_Debet), 0) INTO v_sum
    FROM drestdate_dbt rd, daccount_dbt acc
   WHERE acc.t_Account       = p_account
     AND acc.t_Chapter       = p_chapter
     AND acc.t_Code_Currency = p_cur
     AND rd.t_AccountID      = acc.t_AccountID
     AND rd.t_RestCurrency   = v_rest_cur
     AND EXTRACT(YEAR  FROM rd.t_RestDate) = EXTRACT(YEAR FROM CNST.currdate)
     AND EXTRACT(MONTH FROM rd.t_RestDate) = p_month;

  RETURN v_sum;

EXCEPTION
  WHEN NO_DATA_FOUND THEN
    RETURN v_sum;
  WHEN OTHERS THEN
    rsi_errors.err_msg := 'debetmonthac '||SQLERRM (SQLCODE);
    RETURN 0;

END debetmonthac;

---------------------------------------------------------------------------
-- Функция определения кредита рублевых л/с за период
---------------------------------------------------------------------------
FUNCTION kredita
(
  p_account  IN VARCHAR2
 ,p_chapter  IN NUMBER
 ,p_date_t   IN DATE
 ,p_date_b   IN DATE
 ,p_cur      IN NUMBER DEFAULT NULL
 ,p_rest_cur IN NUMBER DEFAULT NULL
)
RETURN drestdate_dbt.t_Credit%TYPE
AS
  v_sum        drestdate_dbt.t_Credit%TYPE := 0;

  v_date_t DATE;
  v_date_b DATE;

  v_cur      NUMBER;
  v_rest_cur NUMBER;
BEGIN
  
  IF (p_cur IS NULL) THEN  
    v_cur := 0; -- NATCUR
  ELSE
    v_cur := p_cur;
  END IF;

  IF (p_rest_cur IS NULL) THEN  
    v_rest_cur := v_cur;
  ELSE
    v_rest_cur := p_rest_cur;
  END IF;

  RETURN kreditac( p_account, p_chapter, v_cur, p_date_t, p_date_b, v_rest_cur );

EXCEPTION
  WHEN NO_DATA_FOUND THEN
    RETURN v_sum;
  WHEN OTHERS THEN
    rsi_errors.err_msg := 'kredita ' || SQLERRM (SQLCODE);
    RETURN v_sum;

END kredita;

---------------------------------------------------------------------------
-- Функция определения полного кредита рублевых л/с за период (оборот + СПОД)
---------------------------------------------------------------------------
FUNCTION kredita_full
(
  p_account  IN VARCHAR2
 ,p_chapter  IN NUMBER
 ,p_date_t   IN DATE
 ,p_date_b   IN DATE
 ,p_cur      IN NUMBER DEFAULT NULL
 ,p_rest_cur IN NUMBER DEFAULT NULL
)
RETURN drestdate_dbt.t_Credit%TYPE
AS
  v_sum        drestdate_dbt.t_Credit%TYPE := 0;

  v_date_t DATE;
  v_date_b DATE;

  v_cur      NUMBER;
  v_rest_cur NUMBER;
BEGIN
  
  IF (p_cur IS NULL) THEN  
    v_cur := 0; -- NATCUR
  ELSE
    v_cur := p_cur;
  END IF;

  IF (p_rest_cur IS NULL) THEN  
    v_rest_cur := v_cur;
  ELSE
    v_rest_cur := p_rest_cur;
  END IF;

  RETURN kreditac_full( p_account, p_chapter, v_cur, p_date_t, p_date_b, v_rest_cur );

EXCEPTION
  WHEN NO_DATA_FOUND THEN
    RETURN v_sum;
  WHEN OTHERS THEN
    rsi_errors.err_msg := 'kredita ' || SQLERRM (SQLCODE);
    RETURN v_sum;

END kredita_full;

---------------------------------------------------------------------------
-- Функция определения дебета рублевых л/с за период
---------------------------------------------------------------------------
FUNCTION debeta
(
  p_account  IN VARCHAR2
 ,p_chapter  IN NUMBER
 ,p_date_t   IN DATE
 ,p_date_b   IN DATE
 ,p_cur      IN NUMBER DEFAULT NULL
 ,p_rest_cur IN NUMBER DEFAULT NULL
) RETURN drestdate_dbt.t_Debet%TYPE
AS
  v_sum        drestdate_dbt.t_Debet%TYPE := 0;

  v_date_t DATE;
  v_date_b DATE;
  v_cur      NUMBER;
  v_rest_cur NUMBER;
BEGIN

  IF (p_cur IS NULL) THEN  
    v_cur := 0; -- NATCUR
  ELSE
    v_cur := p_cur;
  END IF;

  IF (p_rest_cur IS NULL) THEN  
    v_rest_cur := v_cur;
  ELSE
    v_rest_cur := p_rest_cur;
  END IF;


  RETURN debetac(p_account, p_chapter, v_cur, p_date_t, p_date_b, v_rest_cur);

EXCEPTION
  WHEN NO_DATA_FOUND THEN
    RETURN v_sum;
  WHEN OTHERS THEN
    rsi_errors.err_msg := 'debeta ' || SQLERRM (SQLCODE);
    RETURN v_sum;

END debeta;

---------------------------------------------------------------------------
-- Функция определения полного дебета рублевых л/с за период (оборот + СПОД)
---------------------------------------------------------------------------
FUNCTION debeta_full
(
  p_account  IN VARCHAR2
 ,p_chapter  IN NUMBER
 ,p_date_t   IN DATE
 ,p_date_b   IN DATE
 ,p_cur      IN NUMBER DEFAULT NULL
 ,p_rest_cur IN NUMBER DEFAULT NULL
) RETURN drestdate_dbt.t_Debet%TYPE
AS
  v_sum        drestdate_dbt.t_Debet%TYPE := 0;

  v_date_t DATE;
  v_date_b DATE;
  v_cur      NUMBER;
  v_rest_cur NUMBER;
BEGIN

  IF (p_cur IS NULL) THEN  
    v_cur := 0; -- NATCUR
  ELSE
    v_cur := p_cur;
  END IF;

  IF (p_rest_cur IS NULL) THEN  
    v_rest_cur := v_cur;
  ELSE
    v_rest_cur := p_rest_cur;
  END IF;


  RETURN debetac_full(p_account, p_chapter, v_cur, p_date_t, p_date_b, v_rest_cur);

EXCEPTION
  WHEN NO_DATA_FOUND THEN
    RETURN v_sum;
  WHEN OTHERS THEN
    rsi_errors.err_msg := 'debeta ' || SQLERRM (SQLCODE);
    RETURN v_sum;

END debeta_full;

---------------------------------------------------------------------------
-- Функция определения (полного) кредита валютных л/с за период
---------------------------------------------------------------------------
FUNCTION private_kreditac
(
  p_account  IN VARCHAR2
 ,p_chapter  IN NUMBER
 ,p_cur      IN NUMBER
 ,p_date_t   IN DATE
 ,p_date_b   IN DATE
 ,p_rest_cur IN NUMBER DEFAULT NULL
 ,p_full     IN CHAR DEFAULT CHR(0)
) 
RETURN  drestdate_dbt.t_Credit%TYPE
AS
  v_sum        drestdate_dbt.t_Credit%TYPE := 0;

  v_date_t DATE;
  v_date_b DATE;
  v_rest_cur NUMBER(10);
BEGIN

  IF p_account IS NULL OR p_date_b < p_date_t THEN
    RETURN v_sum;
  END IF;

  -- Если валюта отстатка не передана, считаем что остаток в той же валюте что и счет.
  IF (p_rest_cur IS NULL ) THEN
    v_rest_cur := p_cur;
  ELSE
    v_rest_cur := p_rest_cur;
  END IF; 

  v_date_t := TRUNC(p_date_t);

  v_date_b := TO_DATE( TO_CHAR(p_date_b,'DD.MM.YYYY') || ' 23:59:59', 'DD.MM.YYYY HH24:MI:SS' );

  SELECT 
      CASE 
      WHEN p_full = CHR(88) 
      THEN NVL(SUM(rd.t_Credit), 0) + NVL(SUM(rd.t_CreditSPOD), 0) 
      ELSE NVL(SUM(rd.t_Credit), 0) 
      END INTO v_sum
    FROM drestdate_dbt rd, daccount_dbt acc
   WHERE acc.t_Account       = p_account
     AND acc.t_Chapter       = p_chapter
     AND acc.t_Code_Currency = p_cur
     AND rd.t_AccountID = acc.t_AccountID
     AND rd.t_RestCurrency = v_rest_cur
     AND rd.t_RestDate >= v_date_t
     AND rd.t_RestDate <= v_date_b;

  IF not ABS(v_sum) > 0 THEN
    v_sum := 0;
  END IF;

  RETURN v_sum;

EXCEPTION
  WHEN NO_DATA_FOUND THEN
    RETURN v_sum;
  WHEN OTHERS THEN
    rsi_errors.err_msg := 'kreditac ' || SQLERRM (SQLCODE);
    RETURN v_sum;
END private_kreditac;

---------------------------------------------------------------------------
-- Функция определения кредита валютных л/с за период
---------------------------------------------------------------------------
FUNCTION kreditac
(
  p_account  IN VARCHAR2
 ,p_chapter  IN NUMBER
 ,p_cur      IN NUMBER
 ,p_date_t   IN DATE
 ,p_date_b   IN DATE
 ,p_rest_cur IN NUMBER DEFAULT NULL
) 
RETURN drestdate_dbt.t_Credit%TYPE
AS
BEGIN
  RETURN 
    private_kreditac
    (
      p_account
     ,p_chapter
     ,p_cur
     ,p_date_t
     ,p_date_b
     ,p_rest_cur
     ,CHR(0)
    );
END kreditac;

---------------------------------------------------------------------------
-- Функция определения полного кредита валютных л/с за период (оборот + СПОД)
---------------------------------------------------------------------------
FUNCTION kreditac_full
(
  p_account  IN VARCHAR2
 ,p_chapter  IN NUMBER
 ,p_cur      IN NUMBER
 ,p_date_t   IN DATE
 ,p_date_b   IN DATE
 ,p_rest_cur IN NUMBER DEFAULT NULL
) 
RETURN drestdate_dbt.t_Credit%TYPE
AS
BEGIN
  RETURN 
    private_kreditac
    (
      p_account
     ,p_chapter
     ,p_cur
     ,p_date_t
     ,p_date_b
     ,p_rest_cur
     ,CHR(88)
    );
END kreditac_full;

---------------------------------------------------------------------------
-- Функция определения (полного) дебета валютных л/с за период
---------------------------------------------------------------------------
FUNCTION private_debetac
(
  p_account  IN VARCHAR2
 ,p_chapter  IN NUMBER
 ,p_cur      IN NUMBER
 ,p_date_t   IN DATE
 ,p_date_b   IN DATE
 ,p_rest_cur IN NUMBER DEFAULT NULL
 ,p_full     IN CHAR DEFAULT CHR(0)
) 
RETURN  drestdate_dbt.t_Debet%TYPE
AS
  v_sum        drestdate_dbt.t_Debet%TYPE := 0;

  v_date_t DATE;
  v_date_b DATE;
  v_rest_cur NUMBER(10);
BEGIN

  IF p_account IS NULL OR p_date_b < p_date_t THEN
    RETURN v_sum;
  END IF;

  v_date_t := TRUNC(p_date_t);

  v_date_b := TO_DATE( TO_CHAR(p_date_b,'DD.MM.YYYY') || ' 23:59:59', 'DD.MM.YYYY HH24:MI:SS' );

  -- Если валюта отстатка не передана, считаем что остаток в той же валюте что и счет.
  IF (p_rest_cur IS NULL ) THEN
    v_rest_cur := p_cur;
  ELSE
    v_rest_cur := p_rest_cur;
  END IF; 

  SELECT 
      CASE 
      WHEN p_full = CHR(88) 
      THEN NVL(SUM(rd.t_Debet), 0) + NVL(SUM(rd.t_DebetSPOD), 0) 
      ELSE NVL(SUM(rd.t_Debet), 0) 
      END INTO v_sum
    FROM drestdate_dbt rd, daccount_dbt acc
   WHERE acc.t_Account       = p_account
     AND acc.t_Chapter       = p_chapter
     AND acc.t_Code_Currency = p_cur
     AND rd.t_AccountID = acc.t_AccountID
     AND rd.t_RestCurrency = v_rest_cur
     AND rd.t_RestDate >= v_date_t
     AND rd.t_RestDate <= v_date_b;

  IF not ABS(v_sum) > 0 THEN
    v_sum := 0;
  END IF;

  RETURN v_sum;

  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RETURN v_sum;
    WHEN OTHERS THEN
      rsi_errors.err_msg := 'debetac ' || SQLERRM (SQLCODE);
      RETURN v_sum;
END private_debetac;

---------------------------------------------------------------------------
-- Функция определения дебета валютных л/с за период
---------------------------------------------------------------------------
FUNCTION debetac
(
  p_account  IN VARCHAR2
 ,p_chapter  IN NUMBER
 ,p_cur      IN NUMBER
 ,p_date_t   IN DATE
 ,p_date_b   IN DATE
 ,p_rest_cur IN NUMBER DEFAULT NULL
) 
RETURN drestdate_dbt.t_Debet%TYPE
AS
BEGIN
  RETURN 
    private_debetac
    (
      p_account
     ,p_chapter
     ,p_cur
     ,p_date_t
     ,p_date_b
     ,p_rest_cur
     ,CHR(0)
    );
END debetac;

---------------------------------------------------------------------------
-- Функция определения полного дебета валютных л/с за период (оборот + СПОД)
---------------------------------------------------------------------------
FUNCTION debetac_full
(
  p_account  IN VARCHAR2
 ,p_chapter  IN NUMBER
 ,p_cur      IN NUMBER
 ,p_date_t   IN DATE
 ,p_date_b   IN DATE
 ,p_rest_cur IN NUMBER DEFAULT NULL
) 
RETURN drestdate_dbt.t_Debet%TYPE
AS
BEGIN
  RETURN 
    private_debetac
    (
      p_account
     ,p_chapter
     ,p_cur
     ,p_date_t
     ,p_date_b
     ,p_rest_cur
     ,CHR(88)
    );
END debetac_full;

---------------------------------------------------------------------------
-- Функция нахождения остатков на рублевых л/с за любую дату.
---------------------------------------------------------------------------
FUNCTION resta
( 
  p_account  IN VARCHAR2
 ,p_date     IN DATE
 ,p_chapter  IN NUMBER
 ,p_r0       IN NUMBER
 ,p_cur      IN NUMBER DEFAULT NULL
 ,p_rest_cur IN NUMBER DEFAULT NULL
) RETURN NUMBER
AS
   v_rest     NUMBER := 0;
   v_date     DATE;
   v_date_b   DATE;
   v_cur      NUMBER;
   v_rest_cur NUMBER;
BEGIN

  IF (p_cur IS NULL) THEN  
    v_cur := 0; -- NATCUR
  ELSE
    v_cur := p_cur;
  END IF;

  IF (p_rest_cur IS NULL) THEN  
    v_rest_cur := v_cur;
  ELSE
    v_rest_cur := p_rest_cur;
  END IF;

  RETURN restac(p_account, v_cur, p_date, p_chapter, p_r0, v_rest_cur);

  EXCEPTION
    WHEN TOO_MANY_ROWS THEN
        RETURN NULL;
    WHEN NO_DATA_FOUND THEN
        RETURN v_rest;
    WHEN OTHERS THEN
        rsi_errors.err_msg := 'resta ' || SQLERRM (SQLCODE);
        RETURN v_rest;

END resta;

-----------------------------------------------------------------------------
-- Функция нахождения остатков на валютных л/с за любую дату
-----------------------------------------------------------------------------
FUNCTION restac
( 
  p_account  IN VARCHAR2
 ,p_cur      IN NUMBER
 ,p_date     IN DATE
 ,p_chapter  IN NUMBER
 ,p_r0       IN NUMBER
 ,p_rest_cur IN NUMBER DEFAULT NULL
) RETURN NUMBER
AS
  v_rest      NUMBER := 0;
  v_date      DATE;
  v_date_b    DATE;
  v_account_id NUMBER;
  v_rest_cur   NUMBER;
BEGIN
  IF p_account IS NULL  THEN
    RETURN v_rest;
  END IF;

  v_date_b := TO_DATE( TO_CHAR(p_date,'DD.MM.YYYY') || ' 23:59:59', 'DD.MM.YYYY HH24:MI:SS' );

  -- Если валюта отстатка не передана, считаем что остаток в той же валюте что и счет.
  IF (p_rest_cur IS NULL ) THEN
    v_rest_cur := p_cur;
  ELSE
    v_rest_cur := p_rest_cur;
  END IF; 

  SELECT t_AccountID INTO v_account_id
    FROM daccount_dbt 
   WHERE t_chapter = p_chapter
     AND t_account = p_account
     AND t_code_currency = p_cur;

  SELECT max(t_RestDate) INTO v_date
    FROM drestdate_dbt
   WHERE t_accountID = v_account_id 
     AND t_restcurrency = v_rest_cur
     AND t_RestDate <= v_date_b;

  IF v_date IS NOT NULL THEN
    SELECT t_rest INTO v_rest
      FROM drestdate_dbt
     WHERE t_accountID = v_account_id 
       AND t_restcurrency = v_rest_cur
       AND t_RestDate = v_date;
  END IF;

  RETURN v_rest;

  EXCEPTION
    WHEN TOO_MANY_ROWS THEN
        RETURN NULL;
    WHEN NO_DATA_FOUND THEN
        RETURN v_rest;
    WHEN OTHERS THEN
        rsi_errors.err_msg := 'restac '||SQLERRM (SQLCODE);
        RETURN v_rest;
END restac;

-- Функция нахождения плановых остатков на рублевых л/с за любую дату.
FUNCTION planresta
( 
  p_account IN VARCHAR2
 ,p_date    IN DATE
 ,p_chapter IN NUMBER
 ,p_cur      IN NUMBER DEFAULT NULL
 ,p_rest_cur IN NUMBER DEFAULT NULL
) RETURN NUMBER
AS
   v_rest     NUMBER := 0;
   v_date     DATE;
   v_date_b   DATE;
   v_cur      NUMBER;
   v_rest_cur NUMBER;
BEGIN

  IF (p_cur IS NULL) THEN  
    v_cur := 0; -- NATCUR
  ELSE
    v_cur := p_cur;
  END IF;

  IF (p_rest_cur IS NULL) THEN  
    v_rest_cur := v_cur;
  ELSE
    v_rest_cur := p_rest_cur;
  END IF;


  RETURN planrestac(p_account, v_cur, p_date, p_chapter, v_rest_cur);

  EXCEPTION
    WHEN TOO_MANY_ROWS THEN
        RETURN NULL;
    WHEN NO_DATA_FOUND THEN
        RETURN v_rest;
    WHEN OTHERS THEN
        rsi_errors.err_msg := 'resta ' || SQLERRM (SQLCODE);
        RETURN v_rest;

END planresta;

-- Функция нахождения плановых остатков на валютных л/с за любую дату
FUNCTION planrestac
( 
  p_account IN VARCHAR2
 ,p_cur     IN NUMBER
 ,p_date    IN DATE
 ,p_chapter IN NUMBER
 ,p_rest_cur IN NUMBER DEFAULT NULL
) RETURN NUMBER
AS
  v_rest     NUMBER := 0;
  v_date     DATE;
  v_date_b   DATE;
  v_account_id NUMBER;
  v_rest_cur   NUMBER;
BEGIN
  IF p_account IS NULL  THEN
    RETURN v_rest;
  END IF;

  v_date_b := TO_DATE( TO_CHAR(p_date,'DD.MM.YYYY') || ' 23:59:59', 'DD.MM.YYYY HH24:MI:SS' );

  -- Если валюта отстатка не передана, считаем что остаток в той же валюте что и счет.
  IF (p_rest_cur IS NULL ) THEN
    v_rest_cur := p_cur;
  ELSE
    v_rest_cur := p_rest_cur;
  END IF; 

  SELECT t_AccountID INTO v_account_id
    FROM daccount_dbt 
   WHERE t_chapter = p_chapter
     AND t_account = p_account
     AND t_code_currency = p_cur;


  SELECT max(t_RestDate) INTO v_date
    FROM drestdate_dbt
   WHERE t_accountID = v_account_id 
     AND t_restcurrency = v_rest_cur
     AND t_RestDate <= v_date_b;

  IF v_date IS NOT NULL THEN
    SELECT t_planrest INTO v_rest
      FROM drestdate_dbt
     WHERE t_accountID = v_account_id 
       AND t_restcurrency = v_rest_cur
       AND t_RestDate = v_date;
  END IF;

  RETURN v_rest;

  EXCEPTION
    WHEN TOO_MANY_ROWS THEN
        RETURN NULL;
    WHEN NO_DATA_FOUND THEN
        RETURN v_rest;
    WHEN OTHERS THEN
        rsi_errors.err_msg := 'restac '||SQLERRM (SQLCODE);
        RETURN v_rest;
END planrestac;

---------------------------------------------------------
   FUNCTION restap (
      p_account   IN   VARCHAR2,
      p_dateb     IN   DATE,
      p_datee     IN   DATE,
      p_chapter   IN   NUMBER,
      p_r0        IN   drestdate_dbt.t_Rest%TYPE,
      p_cur       IN NUMBER DEFAULT NULL,
      p_rest_cur  IN NUMBER DEFAULT NULL
   )
      RETURN drestdate_dbt.t_Rest%TYPE
   AS
     v_cur      NUMBER;
     v_rest_cur NUMBER;
     v_rest     drestdate_dbt.t_Rest%TYPE         := 0;
   BEGIN

     IF (p_cur IS NULL) THEN  
       v_cur := 0; -- NATCUR
     ELSE
       v_cur := p_cur;
     END IF;

     IF (p_rest_cur IS NULL) THEN  
       v_rest_cur := v_cur;
     ELSE
       v_rest_cur := p_rest_cur;
     END IF;


     RETURN restapc (p_account, v_cur, p_dateb, p_datee, p_chapter, p_r0, v_rest_cur);
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RETURN v_rest;
      WHEN OTHERS
      THEN
         rsi_errors.err_msg := 'restap ' || SQLERRM (SQLCODE);
         RETURN v_rest;
   END restap;


   FUNCTION restapc (
      p_account   IN   VARCHAR2,
      p_cur       IN   NUMBER,
      p_dateb     IN   DATE,
      p_datee     IN   DATE,
      p_chapter   IN   NUMBER,
      p_r0        IN   drestdate_dbt.t_Rest%TYPE,
      p_rest_cur  IN NUMBER DEFAULT NULL
   )
      RETURN drestdate_dbt.t_Rest%TYPE
        AS
      v_rest        drestdate_dbt.t_Rest%TYPE         := 0;
      v_temp_rest   drestdate_dbt.t_Rest%TYPE;
      v_datestart   drestdate_dbt.t_RestDate%TYPE;
      v_datee       drestdate_dbt.t_RestDate%TYPE;
      v_dateend     drestdate_dbt.t_RestDate%TYPE;
      v_restdate    drestdate_dbt.t_RestDate%TYPE;
      v_prevdate    drestdate_dbt.t_RestDate%TYPE;
      v_flagfirst   BOOLEAN                           := TRUE;
      v_date_b      DATE;
      v_account_id  NUMBER;
      v_rest_cur    NUMBER;
      CURSOR c_rest (cp_startdate DATE, cp_enddate DATE, cp_account_id NUMBER, cp_rest_cur NUMBER)
      IS                               --параметризованный курсор для выборки
         SELECT   t_rest, t_RestDate
             FROM drestdate_dbt
            WHERE t_accountID = cp_account_id
              AND t_restcurrency = cp_rest_cur
              AND t_RestDate <= TO_DATE( TO_CHAR(cp_enddate,'DD.MM.YYYY') || ' 23:59:59', 'DD.MM.YYYY HH24:MI:SS' )
              AND t_RestDate >= TRUNC (cp_startdate)
         ORDER BY t_RestDate DESC;
   BEGIN
      IF p_account IS NULL
      THEN
         RETURN v_rest;
      END IF;

      v_datee := p_datee;

      -- Если валюта отстатка не передана, считаем что остаток в той же валюте что и счет.
      IF (p_rest_cur IS NULL ) THEN
        v_rest_cur := p_cur;
      ELSE
        v_rest_cur := p_rest_cur;
      END IF; 

      SELECT t_AccountID INTO v_account_id
        FROM daccount_dbt 
       WHERE t_chapter = p_chapter
         AND t_account = p_account
         AND t_code_currency = p_cur;

      IF TRUNC (p_dateb) >= TRUNC (v_datee)
      THEN
        v_date_b := TO_DATE( TO_CHAR(v_datee,'DD.MM.YYYY') || ' 23:59:59', 'DD.MM.YYYY HH24:MI:SS' );
         -- если дата начала больше или равна дате конца - вернуть остаток на этот день
        BEGIN
            SELECT NVL (t_rest, 0)
              INTO v_rest
              FROM drestdate_dbt
             WHERE t_RestDate =
                      (SELECT MAX (t_RestDate)
                         FROM drestdate_dbt
                        WHERE t_accountID = v_account_id
                          AND t_restcurrency = v_rest_cur
                          AND t_RestDate <= v_date_b)
               AND t_accountID = v_account_id
               AND t_restcurrency = v_rest_cur;
        EXCEPTION
            WHEN TOO_MANY_ROWS THEN
                RETURN NULL;
        END;
      ELSE                                  --расчет среднего хронологического
         v_date_b := TO_DATE( TO_CHAR(p_dateb,'DD.MM.YYYY') || ' 23:59:59', 'DD.MM.YYYY HH24:MI:SS' );
         BEGIN
            SELECT t_rest, t_RestDate
              INTO v_rest, v_datestart
              FROM drestdate_dbt
             WHERE t_RestDate =
                      (SELECT MAX (t_RestDate)
                         FROM drestdate_dbt
                        WHERE t_accountID = v_account_id
                          AND t_restcurrency = v_rest_cur
                          AND t_RestDate <= v_date_b)
               AND t_accountID = v_account_id
               AND t_restcurrency = v_rest_cur;

         EXCEPTION
            WHEN TOO_MANY_ROWS THEN
                RETURN NULL;
            WHEN NO_DATA_FOUND
            THEN
               v_datestart := cnst.mindate;
               v_rest := 0;
         END;

         --Определение даты начала и остатка на эту дату
         v_rest := v_rest / 2;               --половина остатка начала периода
         v_restdate := v_datee;
         v_prevdate := v_datee;

         FOR v_restdata IN c_rest (v_datestart, v_datee, v_account_id, v_rest_cur)
         LOOP
            IF (v_flagfirst = TRUE)
            THEN
               v_flagfirst := FALSE;

                  v_rest := v_rest + v_restdata.t_rest / 2;
               --половина остатка конца периода
               --конец считается два раза (как конец и как часть периода)
            --, так и надо.
            END IF;

            v_restdate := v_restdata.t_RestDate;

            IF (TRUNC (v_restdate) <= TRUNC (v_datestart) + 1)
            THEN
               v_restdate := p_dateb + 1;
            END IF;

            v_rest := v_rest + v_restdata.t_rest * (v_prevdate - v_restdate);
            v_prevdate := v_restdate;
         END LOOP;

         v_rest := v_rest / (v_datee - p_dateb);
      END IF;

      RETURN v_rest;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RETURN v_rest;
      WHEN OTHERS
      THEN
         rsi_errors.err_msg := 'restapc ' || SQLERRM (SQLCODE);
         RETURN v_rest;
   END restapc;


---------------------------------------------------------
-- Функция получения остатка на счёте по любой валюте на любую дату
---------------------------------------------------------
FUNCTION restall( p_account  IN daccount_dbt.t_account%type,         -- номер счёта
                  p_chapter  IN daccount_dbt.t_chapter%type,         -- уыртр,
                  p_cur      IN daccount_dbt.t_code_currency%type,   -- валюта
                  p_date     IN DATE,                                -- дата
                  p_rest_cur IN NUMBER DEFAULT NULL                  -- валюта остатка
                )
RETURN drestdate_dbt.t_Rest%TYPE
AS
   v_rest     drestdate_dbt.t_Rest%TYPE  := 0;
BEGIN
   v_rest := restac( p_account, p_cur, p_date, p_chapter, 0, p_rest_cur);

   RETURN v_rest;
END restall;

---------------------------------------------------------
-- функция нахождения остатков на субсчетах (daccsub_dbt) на любую дату в рублях
---------------------------------------------------------
FUNCTION restsa
( 
  p_analitica  IN NUMBER,
  p_subaccount IN NUMBER,
  p_date       IN DATE,
  p_notuseconv IN NUMBER
)
RETURN daccsubrd_dbt.t_Rest%TYPE AS

  v_rest     daccsubrd_dbt.t_Rest%TYPE  := 0;
  v_date     daccsubrd_dbt.t_Date_Carry%TYPE;
  v_fiid     daccvanl_dbt.t_FIID%TYPE;
  v_date_b   DATE;
BEGIN
  IF p_analitica IS NOT NULL THEN

    v_date_b := TO_DATE( TO_CHAR(p_date,'DD.MM.YYYY') || ' 23:59:59', 'DD.MM.YYYY HH24:MI:SS' );

    SELECT MAX(t_Date_Carry) INTO v_date
      FROM  daccsubrd_dbt
     WHERE t_AccAnaliticsID = p_analitica AND
           t_SubAccountID = p_subaccount AND
           t_Date_Carry <= v_date_b;

    IF v_date IS NOT NULL THEN
      SELECT NVL(t_Rest,0) INTO v_rest
        FROM  daccsubrd_dbt
       WHERE t_AccAnaliticsID = p_analitica AND
             t_SubAccountID = p_subaccount AND
             t_Date_Carry = v_date;
    END IF;
    
    IF p_notuseconv = 0 THEN 
      SELECT NVL(t_FIID,0) INTO v_fiid
        FROM daccvanl_dbt
       WHERE t_AccAnaliticsID = p_analitica;
      
      IF v_fiid <> 0 THEN 
        v_rest := RSI_RSB_FIInstr.ConvSum( v_rest, v_fiid, 0, p_date );
      END IF;
    END IF;
  
  END IF;
  RETURN v_rest;
EXCEPTION
    WHEN TOO_MANY_ROWS THEN
        RETURN NULL;
    WHEN NO_DATA_FOUND THEN
        RETURN v_rest;
    WHEN OTHERS THEN
        rsi_errors.err_msg := 'restsa '||SQLERRM (SQLCODE);
        RETURN v_rest;
END restsa;

---------------------------------------------------------------------------
-- Функция определения дебета на субсчетах (daccsub_dbt) за период
---------------------------------------------------------------------------
FUNCTION debetsa
(
  p_analitica  IN NUMBER,
  p_subaccount IN NUMBER,
  p_date_from  IN DATE,
  p_date_till  IN DATE
)
RETURN daccsubrd_dbt.t_Debet%TYPE
AS
  v_sum       daccsubrd_dbt.t_Debet%TYPE := 0;
  v_date_from DATE;
  v_date_till DATE;
BEGIN

  IF p_analitica IS NOT NULL AND p_date_till >= p_date_from THEN

    v_date_from := TRUNC(p_date_from);

    v_date_till := TO_DATE( TO_CHAR(p_date_till,'DD.MM.YYYY') || ' 23:59:59', 'DD.MM.YYYY HH24:MI:SS' );
    
    SELECT NVL(SUM(t_Debet), 0) INTO v_sum
      FROM  daccsubrd_dbt
     WHERE t_AccAnaliticsID = p_analitica AND
           t_SubAccountID = p_subaccount AND
           t_Date_Carry >= v_date_from AND
           t_Date_Carry <= v_date_till;

    IF NOT ABS(v_sum) > 0 THEN
      v_sum := 0;
    END IF;
  
  END IF;

  RETURN v_sum;

EXCEPTION
  WHEN NO_DATA_FOUND THEN
    RETURN v_sum;
  WHEN OTHERS THEN
    rsi_errors.err_msg := 'debetsa ' || SQLERRM (SQLCODE);
    RETURN v_sum;
END debetsa;

---------------------------------------------------------------------------
-- Функция определения кредита на субсчетах (daccsub_dbt) за период
---------------------------------------------------------------------------
FUNCTION creditsa
(
  p_analitica  IN NUMBER,
  p_subaccount IN NUMBER,
  p_date_from  IN DATE,
  p_date_till  IN DATE
)
RETURN daccsubrd_dbt.t_Credit%TYPE
AS
  v_sum       daccsubrd_dbt.t_Credit%TYPE := 0;
  v_date_from DATE;
  v_date_till DATE;
BEGIN

  IF p_analitica IS NOT NULL AND p_date_till >= p_date_from THEN

    v_date_from := TRUNC(p_date_from);

    v_date_till := TO_DATE( TO_CHAR(p_date_till,'DD.MM.YYYY') || ' 23:59:59', 'DD.MM.YYYY HH24:MI:SS' );
    
    SELECT NVL(SUM(t_Credit), 0) INTO v_sum
      FROM  daccsubrd_dbt
     WHERE t_AccAnaliticsID = p_analitica AND
           t_SubAccountID = p_subaccount AND
           t_Date_Carry >= v_date_from AND
           t_Date_Carry <= v_date_till;

    IF NOT ABS(v_sum) > 0 THEN
      v_sum := 0;
    END IF;
  
  END IF;

  RETURN v_sum;

EXCEPTION
  WHEN NO_DATA_FOUND THEN 
    RETURN v_sum;
  WHEN OTHERS THEN
    rsi_errors.err_msg := 'creditsa ' || SQLERRM (SQLCODE);
    RETURN v_sum;
END creditsa;

---------------------------------------------------------
-- функция нахождения остатков на счетах балансового счета
---------------------------------------------------------
FUNCTION RestB( p_Chapter  IN INTEGER,
                p_Balance  IN STRING,
                p_NumPlan  IN INTEGER,
                p_FIID     IN INTEGER,
                p_RestDate IN DATE,
                sqlFilter  IN STRING DEFAULT NULL,
                p_rest_cur  IN NUMBER DEFAULT NULL
              ) RETURN drestdate_dbt.t_Rest%TYPE AS

  v_strSQL VARCHAR2(12000);

  v_rest drestdate_dbt.t_Rest%TYPE;
  v_rest_cur   NUMBER;
BEGIN

  v_rest := 0;

  -- Если валюта отстатка не передана, считаем что остаток в той же валюте что и счет.
  IF (p_rest_cur IS NULL ) THEN
    v_rest_cur := p_FIID;
  ELSE
    v_rest_cur := p_rest_cur;
  END IF; 


  v_strSQL := 'SELECT NVL(sum(NVL(rsi_rsb_account.';

  v_strSQL := v_strSQL || 'RestAC(account.t_Account, account.t_Code_Currency, ';
  v_strSQL := v_strSQL || chr(39) || p_RestDate || chr(39) || ', ';
  v_strSQL := v_strSQL || 'account.t_Chapter, NULL,'|| to_char(v_rest_cur)||'), 0)), 0) ';
  v_strSQL := v_strSQL || 'FROM daccblnc_dbt accblnc, daccount_dbt account';

  v_strSQL := v_strSQL || ' WHERE accblnc.t_Chapter  = ' || to_char(p_Chapter);
  v_strSQL := v_strSQL || ' AND accblnc.t_Code_Currency = ' || to_char(p_FIID);

  IF p_Balance IS NOT NULL THEN

    v_strSQL := v_strSQL || ' AND accblnc.t_Balance' || to_char(p_NumPlan) || ' = ';
    v_strSQL := v_strSQL || chr(39) || p_Balance || chr(39);

  END IF;

  v_strSQL := v_strSQL || ' AND account.t_Chapter = accblnc.t_Chapter';
  v_strSQL := v_strSQL || ' AND account.t_Code_Currency = accblnc.t_Code_Currency';
  v_strSQL := v_strSQL || ' AND account.t_Account = accblnc.t_Account';

  IF sqlFilter IS NOT NULL THEN
     v_strSQL := v_strSQL || ' AND (' || sqlFilter || ')';
  END IF;


  EXECUTE IMMEDIATE v_strSQL INTO v_rest;

  RETURN v_rest;

END RestB;
---------------------------------------------------------

---------------------------------------------------------
-- функция нахождения средних остатков на счетах балансового счета
---------------------------------------------------------
FUNCTION RestBalanceAverage( p_Chapter  IN NUMBER,
                             p_Balance  IN STRING,
                             p_NumPlan  IN NUMBER,
                             p_FIID     IN INTEGER,
                             p_DateTop  IN DATE,
                             p_DateBot  IN DATE,
                             sqlFilter  IN STRING DEFAULT NULL,
                             p_rest_cur IN NUMBER DEFAULT NULL
                           ) RETURN drestdate_dbt.t_Rest%TYPE AS

  v_strSQL VARCHAR2(12000);

  v_Rest drestdate_dbt.t_Rest%TYPE;
  v_rest_cur   NUMBER;
BEGIN

  v_Rest := 0;

  -- Если валюта отстатка не передана, считаем что остаток в той же валюте что и счет.
  IF (p_rest_cur IS NULL ) THEN
    v_rest_cur := p_FIID;
  ELSE
    v_rest_cur := p_rest_cur;
  END IF; 

  v_strSQL := 'SELECT NVL(sum(NVL(rsi_rsb_account.';

  v_strSQL := v_strSQL || 'RestAPC(account.t_Account, account.t_Code_Currency, ';
  v_strSQL := v_strSQL || chr(39) || p_DateTop || chr(39) || ',' || chr(39) || p_DateBot || chr(39) || ',';
  v_strSQL := v_strSQL || 'account.t_Chapter, NULL,'|| to_char(v_rest_cur)||'), 0)), 0) ';
  v_strSQL := v_strSQL || 'FROM daccblnc_dbt accblnc, daccount_dbt account';

  v_strSQL := v_strSQL || ' WHERE accblnc.t_Chapter  = ' || to_char(p_Chapter);
  v_strSQL := v_strSQL || ' AND accblnc.t_Code_Currency = ' || to_char(p_FIID);

  IF p_Balance IS NOT NULL THEN

    v_strSQL := v_strSQL || ' AND accblnc.t_Balance' || to_char(p_NumPlan) || ' = ';
    v_strSQL := v_strSQL || chr(39) || p_Balance || chr(39);

  END IF;

  v_strSQL := v_strSQL || ' AND account.t_Chapter = accblnc.t_Chapter';
  v_strSQL := v_strSQL || ' AND account.t_Code_Currency = accblnc.t_Code_Currency';
  v_strSQL := v_strSQL || ' AND account.t_Account = accblnc.t_Account';

  IF sqlFilter IS NOT NULL THEN
     v_strSQL := v_strSQL || ' AND (' || sqlFilter || ')';
  END IF;

  EXECUTE IMMEDIATE v_strSQL INTO v_Rest;

  RETURN v_Rest;

END RestBalanceAverage;
---------------------------------------------------------

---------------------------------------------------------
-- функция нахождения дебетовых оборотов на счетах балансового счета
---------------------------------------------------------
FUNCTION DebetB( p_Chapter  IN NUMBER,
                 p_Balance  IN STRING,
                 p_NumPlan  IN NUMBER,
                 p_FIID     IN INTEGER,
                 p_DateTop  IN DATE,
                 p_DateBot  IN DATE,
                 sqlFilter  IN STRING DEFAULT NULL,
                 p_rest_cur IN NUMBER DEFAULT NULL
               ) RETURN drestdate_dbt.t_Debet%TYPE AS

  v_strSQL VARCHAR2(2000);

  v_Debet drestdate_dbt.t_Debet%TYPE;
  v_rest_cur NUMBER;
BEGIN

  v_Debet := 0;

  -- Если валюта отстатка не передана, считаем что остаток в той же валюте что и счет.
  IF (p_rest_cur IS NULL ) THEN
    v_rest_cur := p_FIID;
  ELSE
    v_rest_cur := p_rest_cur;
  END IF; 

  v_strSQL := 'SELECT NVL(sum(NVL(rsi_rsb_account.';

  v_strSQL := v_strSQL || 'DebetAC(account.t_Account, account.t_Chapter, account.t_Code_Currency, ';
  v_strSQL := v_strSQL || chr(39) || p_DateTop || chr(39) || ',' || chr(39) || p_DateBot || chr(39);
  v_strSQL := v_strSQL || ','|| to_char(v_rest_cur)||'), 0)), 0) ';
  v_strSQL := v_strSQL || 'FROM daccblnc_dbt accblnc, daccount_dbt account';

  v_strSQL := v_strSQL || ' WHERE accblnc.t_Chapter  = ' || to_char(p_Chapter);
  v_strSQL := v_strSQL || ' AND accblnc.t_Code_Currency = ' || to_char(p_FIID);

  IF p_Balance IS NOT NULL THEN

    v_strSQL := v_strSQL || ' AND accblnc.t_Balance' || to_char(p_NumPlan) || ' = ';
    v_strSQL := v_strSQL || chr(39) || p_Balance || chr(39);

  END IF;

  v_strSQL := v_strSQL || ' AND account.t_Chapter = accblnc.t_Chapter';
  v_strSQL := v_strSQL || ' AND account.t_Code_Currency = accblnc.t_Code_Currency';
  v_strSQL := v_strSQL || ' AND account.t_Account = accblnc.t_Account';

  IF sqlFilter IS NOT NULL THEN
     v_strSQL := v_strSQL || ' AND (' || sqlFilter || ')';
  END IF;

  EXECUTE IMMEDIATE v_strSQL INTO v_Debet;

  RETURN v_Debet;

END DebetB;
---------------------------------------------------------

---------------------------------------------------------
-- функция нахождения дебетовых оборотов на счетах балансового счета
---------------------------------------------------------
FUNCTION CreditB( p_Chapter  IN NUMBER,
                  p_Balance  IN STRING,
                  p_NumPlan  IN NUMBER,
                  p_FIID     IN INTEGER,
                  p_DateTop  IN DATE,
                  p_DateBot  IN DATE,
                  sqlFilter  IN STRING DEFAULT NULL,
                  p_rest_cur IN NUMBER DEFAULT NULL

               ) RETURN drestdate_dbt.t_Credit%TYPE AS

  v_strSQL VARCHAR2(2000);

  v_Kredit drestdate_dbt.t_Credit%TYPE;
  v_rest_cur NUMBER;
BEGIN

  v_Kredit := 0;

  -- Если валюта отстатка не передана, считаем что остаток в той же валюте что и счет.
  IF (p_rest_cur IS NULL ) THEN
    v_rest_cur := p_FIID;
  ELSE
    v_rest_cur := p_rest_cur;
  END IF; 

  v_strSQL := 'SELECT NVL(sum(NVL(rsi_rsb_account.';

  v_strSQL := v_strSQL || 'KreditAC(account.t_Account, account.t_Chapter, account.t_Code_Currency, ';
  v_strSQL := v_strSQL || chr(39) || p_DateTop || chr(39) || ',' || chr(39) || p_DateBot || chr(39) || ',' || to_char(v_rest_cur);
  v_strSQL := v_strSQL || '), 0)), 0) ';
  v_strSQL := v_strSQL || 'FROM daccblnc_dbt accblnc, daccount_dbt account';

  v_strSQL := v_strSQL || ' WHERE accblnc.t_Chapter  = ' || to_char(p_Chapter);
  v_strSQL := v_strSQL || ' AND accblnc.t_Code_Currency = ' || to_char(p_FIID);

  IF p_Balance IS NOT NULL THEN

    v_strSQL := v_strSQL || ' AND accblnc.t_Balance' || to_char(p_NumPlan) || ' = ';
    v_strSQL := v_strSQL || chr(39) || p_Balance || chr(39);

  END IF;

  v_strSQL := v_strSQL || ' AND account.t_Chapter = accblnc.t_Chapter';
  v_strSQL := v_strSQL || ' AND account.t_Code_Currency = accblnc.t_Code_Currency';
  v_strSQL := v_strSQL || ' AND account.t_Account = accblnc.t_Account';

  IF sqlFilter IS NOT NULL THEN
     v_strSQL := v_strSQL || ' AND (' || sqlFilter || ')';
  END IF;

  EXECUTE IMMEDIATE v_strSQL INTO v_Kredit;

  RETURN v_Kredit;

END CreditB;
---------------------------------------------------------
  --Функция получения остатка на счете за дату
  /*
  FUNCTION GetRestEx(
    p_Account    IN  VARCHAR2, --Номер счета
    p_Chapter    IN  NUMBER,   --Глава счета
    p_FIID       IN  NUMBER,   --Валюта счета
    p_OnDate     IN  DATE,     --Дата, на которую необходимо получить остаток
    p_TableName  IN  VARCHAR2, --Имя таблицы с остатками
    p_RestExists OUT NUMBER,   --выходной параметр - Остаток существует, если > 0
    p_Rest       OUT NUMBER,   --выходной параметр - Значение остатка
    p_RestPlan   OUT NUMBER,   --выходной параметр - Значение планового остатка
    p_RestDate   OUT DATE,      --Дата, на которую существует остаток
    p_rest_cur   IN NUMBER DEFAULT NULL -- Валюта остстка
   ) RETURN INTEGER
  IS
    --Переменные
    m_stat INTEGER := 0;
    m_stmt VARCHAR2(1000);
    m_stmt_e VARCHAR2(1000);
    m_stmt_r VARCHAR2(1000);
    m_stmt_c VARCHAR2(1000);
    v_rest_cur NUMBER;
  BEGIN
    p_RestExists := 0;
    p_Rest       := 0;
    p_RestPlan   := 0;
    p_RestDate   := TO_DATE('01010001', 'ddmmyyyy');

    IF (p_rest_cur IS NULL ) THEN
      v_rest_cur := p_FIID;
    ELSE
      v_rest_cur := p_rest_cur;
    END IF; 

    m_stmt_r := 'SELECT t_Rest, t_PlanRest, t_RestDate ';
    m_stmt_c := 'SELECT COUNT(*) ';
    m_stmt_e := ' FROM drestdate_dbt WHERE t_RestDate = ' ||
                '  (SELECT MAX(t.t_RestDate) FROM drestdate_dbt' ||
                '   t WHERE t.t_Account = :1 AND t.t_Code_Currency = :2 AND t.t_Chapter = :3 AND t.t_RestDate <= :4 AND t.t_RestCurrency = :5) ' ||
                '  AND t_Account = :6 AND t_Code_Currency = :7 AND t_Chapter = :8 AND t_RestCurrency = :9';
    m_stmt := m_stmt_c || m_stmt_e;
    EXECUTE IMMEDIATE m_stmt INTO p_RestExists USING p_Account, p_FIID, p_Chapter, p_OnDate, v_rest_cur, p_Account, p_FIID, p_Chapter, v_rest_cur;
    IF p_RestExists > 0 THEN
      m_stmt := m_stmt_r || m_stmt_e;
      EXECUTE IMMEDIATE m_stmt INTO p_Rest, p_RestPlan, p_RestDate USING p_Account, p_FIID, p_Chapter, p_OnDate, v_rest_cur, p_Account, p_FIID, p_Chapter, v_rest_cur;
    END IF;
    RETURN m_stat;
  END;
*/
  --
  -- Получить значение лимита на дату
  --
  FUNCTION GetAccLimit(
                        p_Account  IN  VARCHAR2   -- Номер счета
                       ,p_Chapter  IN  INTEGER    -- Глава счета
                       ,p_FIID     IN  INTEGER    -- Валюта счета
                       ,p_BankDate IN  DATE       -- Дата, на которую необходимо вычислить значение лимита
                      )
  RETURN NUMBER
  IS

    v_Limit NUMBER;

  BEGIN

    BEGIN

      SELECT t_Limit INTO v_Limit
      FROM dacclimit_dbt
      WHERE t_Account       = p_Account
        AND t_Chapter       = p_Chapter
        AND t_Code_Currency = p_FIID
        AND t_LimitDate     = (SELECT max(t_LimitDate)
                               FROM dacclimit_dbt
                               WHERE t_Account       = p_Account
                                 AND t_Chapter       = p_Chapter
                                 AND t_Code_Currency = p_FIID
                                 AND t_LimitDate    <= p_BankDate);
    EXCEPTION

      WHEN NO_DATA_FOUND THEN v_Limit := 0;

    END;

    RETURN v_Limit;

  END;

  -- Получить дату последней записи об остатке для заданных счета и даты
  FUNCTION GetAccLastRestDate
  (
     p_AccountID     IN INTEGER -- Ид. счета
    ,p_Code_Currency IN INTEGER -- Валюта счета
    ,p_OnDate        IN DATE    -- Дата, на которую необходимо получить значение
  )
  RETURN DATE
  IS
    m_LastDate DATE;
  BEGIN
    SELECT MAX(t_RestDate) INTO m_LastDate 
      FROM drestdate_dbt 
     WHERE t_AccountID = p_AccountID
       AND t_RestCurrency = p_Code_Currency
       AND t_RestDate <= p_OnDate;
    
    RETURN m_LastDate;
  END;

  -- Получить дату последней записи об остатке в ВЭ для заданных счета и даты
  FUNCTION GetAccLastRestDateEqv
  (
     p_AccountID     IN INTEGER -- Ид. счета
    ,p_OnDate        IN DATE    -- Дата, на которую необходимо получить значение
  )
  RETURN DATE
  IS
    m_LastDate DATE;
  BEGIN
    SELECT MAX(t_RestDate) INTO m_LastDate 
      FROM dresteqv_dbt 
     WHERE t_AccountID = p_AccountID
       AND t_RestDate <= p_OnDate;
    
    RETURN m_LastDate;
  END;

  /**
  @brief Подсчитывает количество остатков на заданную дату
  @param[in] AccountID ID счета из таблицы daccount_dbt
  @param[in] restDate Дата, на которую необходимо подстчитать записи остатков
  @param[out] curZeroCount Количество нулевых остатков в валюте (RestCurrency != 0)
  @param[out] natcurNonZeroCount Количество ненулевых остатков в нац.валюте (RestCurrency = 0)
  @param[out] recordsCount Общее количество найденных записей на дату
  */ 
   PROCEDURE CheckAccountRest
  (
     AccountID           IN NUMBER  -- ///> ID счета
    ,restDate            IN DATE    -- ///> Дата остатков
    ,curZeroCount       OUT NUMBER  -- ///> Количество нулевых остатков в валюте (RestCurrency != 0)
    ,natcurNonZeroCount OUT NUMBER  -- ///> Количество ненулевых остатков в нац.валюте (RestCurrency = 0)
    ,recordsCount       OUT NUMBER  -- ///> Общее количество найденных записей на дату
  )
  AS
    vRestDate DATE := restDate;
  BEGIN
    IF (vRestDate IS NULL) THEN
      SELECT MAX(t_RestDate) INTO vRestDate
        FROM drestdate_dbt  
       WHERE t_AccountID = AccountID;
    END IF;
  
    SELECT 
      NVL(SUM(CASE WHEN rest.t_RestCurrency > 0 AND rest.t_Rest = 0 AND rest.t_PlanRest = 0 THEN 1 
                   ELSE 0 
                   END), 0), 
      NVL(SUM(CASE WHEN rest.t_RestCurrency = 0 AND (rest.t_Rest != 0 OR rest.t_PlanRest != 0) THEN 1 
                   ELSE 0 
                   END), 0),
      COUNT(1)
      INTO curZeroCount, natcurNonZeroCount, recordsCount
    FROM drestdate_dbt rest 
    WHERE rest.t_AccountID = AccountID
      AND rest.t_RestDate = vRestDate;
  END;

END rsi_rsb_account;
/

