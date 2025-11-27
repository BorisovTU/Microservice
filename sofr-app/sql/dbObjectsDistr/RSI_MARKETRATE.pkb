CREATE OR REPLACE PACKAGE BODY RSI_MARKETRATE IS
    FUNCTION SelectRangeMarketRateEx
    (
        pServiseKind IN NUMBER,            -- Вид обслуживания
        pBranchID IN NUMBER,               -- Подразделение ТС
        pProductKindID IN NUMBER,          -- Вид банковского продукта
        pProductID IN NUMBER,              -- Банковский продукт
        pInterestFrequency IN NUMBER,      -- Периодичность выплаты/ причисления %% по сделке/ договору.
        pContractYear IN NUMBER,           -- Срок договора - года
        pContractMonths IN NUMBER,         -- Срок договора - месяцы
        pContractDays IN NUMBER,           -- Срок договора - дни
        pContractSum IN NUMBER,            -- Сумма договора
        pFiID IN NUMBER,                   -- Валюта договора
        pClientType IN NUMBER,             -- Тип клиента
        pDate IN DATE,                     -- Дата
        pMinValue OUT NOCOPY NUMBER,       -- Минимальное значение интервала рыночной процентной ставки (выходной параметр)
        pMaxValue OUT NOCOPY NUMBER,       -- Максимальное значение интервала рыночной процентной ставки (выходной параметр)
        pSourceDataLayer OUT NOCOPY NUMBER,
        pMarketRateID OUT NOCOPY NUMBER
    ) RETURN NUMBER
    IS
        mCondDays NUMBER;
        mRateID NUMBER;
        mSourceDataLayer NUMBER;
        mDate DATE;
        mDateRange NUMBER;
        mBranchID NUMBER;
        mFromMAKS NUMBER :=
                   NVL(RSB_COMMON.GetRegIntValue (
                       'РСХБ\БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ\ТРАНСФЕРНЫЕ СТАВКИ МАКС\ИМПОРТ ТР.СТАВОК ИСП. KAFKA'), 0);
    BEGIN
        mCondDays := pContractYear * 360 + pContractMonths * 30 + pContractDays;
        pMinValue := 0;
        pMaxValue := 0;
        pSourceDataLayer := 0;

        IF( pBranchID IS NULL OR pBranchID = 0 ) THEN
            mBranchID := RsbSessionData.HeadDprt;
        ELSE
            mBranchID := pBranchID;
        END IF;


        IF pDate IS NULL THEN
            mDate := TO_DATE('01.01.0001','DD.MM.YYYY');
        ELSE
            mDate := pDate;
        END IF;
        
        IF mFromMAKS = 0 THEN
            SELECT T_ID, T_SOURCEDATALAYER INTO mRateID, mSourceDataLayer
        FROM (
          WITH PARAMS
              AS (SELECT pServiseKind AS T_SERVISEKIND,
                          pProductKindID AS T_PRODUCTKINDID,
                          pFiID AS T_FIID,
                          pClientType AS T_CLIENTTYPE,
                          pProductID AS T_PRODUCTID,
                          pInterestFrequency AS T_INTERESTFREQUENCY,
                          pContractSum AS T_CONTRACTSUM,
                          mCondDays AS T_CONDDAYS,
                          mBranchID AS T_BRANCHID,
                          mDate AS T_DATE
                  FROM DUAL)
          SELECT T.T_ID, T.T_SOURCEDATALAYER
              FROM DMARKETRATE_DBT T, PARAMS
          WHERE        T.T_SERVISEKIND = PARAMS.T_SERVISEKIND
                      AND T.T_PRODUCTKINDID = PARAMS.T_PRODUCTKINDID
                      AND T.T_FIID = PARAMS.T_FIID
                      AND (T.T_CLIENTTYPE = PARAMS.T_CLIENTTYPE OR T.T_CLIENTTYPE = 0)
                      AND T.T_BRANCHID IN
                          ( SELECT DepCode FROM                         
                                               (SELECT DISTINCT DEP.DepCode, DEP.DepLevel
                                                FROM DMARKETRATE_DBT RATE, PARAMS, (SELECT DEP.T_CODE as DepCode,  Level as DepLevel 
                                                                                      FROM DDP_DEP_DBT DEP, PARAMS 
                                                                                      START WITH DEP.T_CODE = PARAMS.T_BRANCHID 
                                                                                      CONNECT BY PRIOR DEP.T_PARENTCODE = DEP.T_CODE) DEP
                                               WHERE RATE.T_SERVISEKIND = PARAMS.T_SERVISEKIND
                                                      AND RATE.T_PRODUCTKINDID = PARAMS.T_PRODUCTKINDID
                                                      AND RATE.T_FIID = PARAMS.T_FIID
                                                      AND (RATE.T_CLIENTTYPE = PARAMS.T_CLIENTTYPE OR RATE.T_CLIENTTYPE = 0)
                                                      AND RATE.T_BRANCHID = DEP.DepCode
                                                      AND (RATE.T_PRODUCTID = PARAMS.T_PRODUCTID OR RATE.T_PRODUCTID = 0)
                                                      AND (RATE.T_INTERESTFREQUENCY = PARAMS.T_INTERESTFREQUENCY OR RATE.T_INTERESTFREQUENCY = 0)
                                                      AND EXISTS (SELECT 1 FROM  dvalintmarketrate_dbt, PARAMS 
                                                                           WHERE (t_DeleteDate = TO_DATE('01.01.0001', 'DD.MM.YYYY') OR t_DeleteDate IS NULL) 
                                                                             AND t_MarketRateID = RATE.T_ID 
                                                                             AND t_BeginDate <= PARAMS.T_DATE
                                                                 )
                                                      AND ( (RATE.T_CONTRACTYEAR * 360 + RATE.T_CONTRACTMONTHS * 30 + RATE.T_CONTRACTDAYS) <= PARAMS.T_CONDDAYS
                                                             OR (PARAMS.T_CONDDAYS = 0 AND (RATE.T_CONTRACTYEAR * 360 + RATE.T_CONTRACTMONTHS * 30 + RATE.T_CONTRACTDAYS) = 0))
                                                      AND (RATE.T_CONTRACTSUM < PARAMS.T_CONTRACTSUM OR (PARAMS.T_CONTRACTSUM = 0 AND RATE.T_CONTRACTSUM = 0) )
                                                      AND (RATE.T_NOTUSE = CHR (0) OR (RATE.T_NOTUSE IS NULL AND ROWNUM = 1))
                                               ORDER BY DEP.DepLevel)                 
                             WHERE ROWNUM = 1
                          )
                      AND EXISTS (SELECT 1 FROM  dvalintmarketrate_dbt, PARAMS 
                                           WHERE (t_DeleteDate = TO_DATE('01.01.0001', 'DD.MM.YYYY') OR t_DeleteDate IS NULL) 
                                             AND t_MarketRateID = T.T_ID 
                                             AND t_BeginDate <= PARAMS.T_DATE
                                 )              
                      AND (T.T_PRODUCTID = PARAMS.T_PRODUCTID OR T.T_PRODUCTID = 0)
                      AND (T.T_INTERESTFREQUENCY = PARAMS.T_INTERESTFREQUENCY OR T.T_INTERESTFREQUENCY = 0)
                      AND ( (T.T_CONTRACTYEAR * 360 + T.T_CONTRACTMONTHS * 30 + T.T_CONTRACTDAYS) <= PARAMS.T_CONDDAYS
                             OR (PARAMS.T_CONDDAYS = 0 AND (T.T_CONTRACTYEAR * 360 + T.T_CONTRACTMONTHS * 30 + T.T_CONTRACTDAYS) = 0))
                      AND (T.T_CONTRACTSUM < PARAMS.T_CONTRACTSUM OR (PARAMS.T_CONTRACTSUM = 0 AND T.T_CONTRACTSUM = 0) )
                      AND (T.T_NOTUSE = CHR (0) OR (T.T_NOTUSE IS NULL AND ROWNUM = 1))
          ORDER BY T.T_CLIENTTYPE DESC, T.T_PRODUCTID DESC, T.T_INTERESTFREQUENCY DESC, (T.T_CONTRACTYEAR * 360 + T.T_CONTRACTMONTHS * 30 + T.T_CONTRACTDAYS) DESC,
                  T.T_CONTRACTSUM DESC
        ) WHERE ROWNUM = 1;

        SELECT t_MinValue, t_MaxValue, DateRange INTO pMinValue, pMaxValue, mDateRange
        FROM   (SELECT t_MinValue, t_MaxValue, ABS(t_BeginDate - mDate) DateRange 
                    FROM dvalintmarketrate_dbt
                WHERE (t_DeleteDate = TO_DATE('01.01.0001', 'DD.MM.YYYY') OR t_DeleteDate IS NULL) 
                    AND t_MarketRateID = mRateID 
                    AND t_BeginDate <= mDate
                ORDER BY DateRange, t_BeginDate DESC)
        WHERE ROWNUM <= 1;
      ELSE
        SELECT t_MinValue, t_MaxValue, ABS(t_BeginDate - mDate) DateRange, v.T_MARKETRATEID, m.T_SOURCEDATALAYER 
          INTO pMinValue, pMaxValue, mDateRange, mRateID, mSourceDataLayer
          FROM DMARKETRATE_DBT m
          INNER JOIN dvalintmarketrate_dbt v ON  v.T_MARKETRATEID = m.T_ID
                                            AND (v.t_DeleteDate = TO_DATE('01.01.0001', 'DD.MM.YYYY') OR v.t_DeleteDate IS NULL)
                                            AND (v.T_BEGINDATE <= mDate)
         WHERE m.T_FIID = pFiID         
           AND m.T_CONTRACTDAYS <= mCondDays
           AND (m.T_INTERESTFREQUENCY = pInterestFrequency OR m.T_INTERESTFREQUENCY = 0)
      ORDER BY m.T_CONTRACTDAYS DESC, DateRange, v.t_BeginDate DESC
      FETCH NEXT 1 ROWS ONLY;
      END IF;

        pSourceDataLayer := mSourceDataLayer;
        pMarketRateID    := mRateID;

        RETURN 0;
    EXCEPTION WHEN NO_DATA_FOUND THEN
        RETURN 1;
    END;


    FUNCTION SelectRangeMarketRate
    (
        pServiseKind IN NUMBER,            -- Вид обслуживания
        pBranchID IN NUMBER,               -- Подразделение ТС
        pProductKindID IN NUMBER,          -- Вид банковского продукта
        pProductID IN NUMBER,              -- Банковский продукт
        pContractYear IN NUMBER,           -- Срок договора - года
        pContractMonths IN NUMBER,         -- Срок договора - месяцы
        pContractDays IN NUMBER,           -- Срок договора - дни
        pContractSum IN NUMBER,            -- Сумма договора
        pFiID IN NUMBER,                   -- Валюта договора
        pClientType IN NUMBER,             -- Тип клиента
        pDate IN DATE,                     -- Дата
        pMinValue OUT NOCOPY NUMBER,       -- Минимальное значение интервала рыночной процентной ставки (выходной параметр)
        pMaxValue OUT NOCOPY NUMBER,       -- Максимальное значение интервала рыночной процентной ставки (выходной параметр)
        pSourceDataLayer OUT NOCOPY NUMBER
    ) RETURN NUMBER
    IS
       mStat NUMBER;
       mMarketRateID NUMBER;
    BEGIN
       mStat := SelectRangeMarketRateEx( pServiseKind,
                                         pBranchID,
                                         pProductKindID,
                                         pProductID,
                                         0,
                                         pContractYear,
                                         pContractMonths,
                                         pContractDays,
                                         pContractSum,
                                         pFiID,
                                         pClientType,
                                         pDate,
                                         pMinValue,
                                         pMaxValue,
                                         pSourceDataLayer,
                                         mMarketRateID
                                      );

        RETURN mStat;
    EXCEPTION WHEN NO_DATA_FOUND THEN
        RETURN 1;
    END;

    -- полностью аналогична ф-и SelectRangeMarketRateEx, но ищет ближайшую запись t_MarketRateID, со сроком > Срок договора (PARAMS.T_CONDDAYS)
    FUNCTION SelectRangeMarketRateEx_UpT
    (
        pServiseKind IN NUMBER,            -- Вид обслуживания
        pBranchID IN NUMBER,               -- Подразделение ТС
        pProductKindID IN NUMBER,          -- Вид банковского продукта
        pProductID IN NUMBER,              -- Банковский продукт
        pInterestFrequency IN NUMBER,      -- Периодичность выплаты/ причисления %% по сделке/ договору.
        pContractYear IN NUMBER,           -- Срок договора - года
        pContractMonths IN NUMBER,         -- Срок договора - месяцы
        pContractDays IN NUMBER,           -- Срок договора - дни
        pContractSum IN NUMBER,            -- Сумма договора
        pFiID IN NUMBER,                   -- Валюта договора
        pClientType IN NUMBER,             -- Тип клиента
        pDate IN DATE,                     -- Дата
        pMinValue OUT NOCOPY NUMBER,       -- Минимальное значение интервала рыночной процентной ставки (выходной параметр)
        pMaxValue OUT NOCOPY NUMBER,       -- Максимальное значение интервала рыночной процентной ставки (выходной параметр)
        pSourceDataLayer OUT NOCOPY NUMBER,
        pMarketRateID OUT NOCOPY NUMBER
    ) RETURN NUMBER
    IS
        mCondDays NUMBER;
        mRateID NUMBER;
        mSourceDataLayer NUMBER;
        mDate DATE;
        mDateRange NUMBER;
        mBranchID NUMBER;
        mFromMAKS NUMBER :=
                   NVL(RSB_COMMON.GetRegIntValue (
                       'РСХБ\БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ\ТРАНСФЕРНЫЕ СТАВКИ МАКС\ИМПОРТ ТР.СТАВОК ИСП. KAFKA'), 0);
    BEGIN
        mCondDays := pContractYear * 360 + pContractMonths * 30 + pContractDays;
        pMinValue := 0;
        pMaxValue := 0;
        pSourceDataLayer := 0;

        IF( pBranchID IS NULL OR pBranchID = 0 ) THEN
            mBranchID := RsbSessionData.HeadDprt;
        ELSE
            mBranchID := pBranchID;
        END IF;


        IF pDate IS NULL THEN
            mDate := TO_DATE('01.01.0001','DD.MM.YYYY');
        ELSE
            mDate := pDate;
        END IF;

        IF mFromMAKS = 0 THEN
            SELECT T_ID, T_SOURCEDATALAYER INTO mRateID, mSourceDataLayer
        FROM (
          WITH PARAMS
              AS (SELECT pServiseKind AS T_SERVISEKIND,
                          pProductKindID AS T_PRODUCTKINDID,
                          pFiID AS T_FIID,
                          pClientType AS T_CLIENTTYPE,
                          pProductID AS T_PRODUCTID,
                          pInterestFrequency AS T_INTERESTFREQUENCY,
                          pContractSum AS T_CONTRACTSUM,
                          mCondDays AS T_CONDDAYS,
                          mBranchID AS T_BRANCHID,
                          mDate AS T_DATE
                  FROM DUAL)
          SELECT T.T_ID, T.T_SOURCEDATALAYER
              FROM DMARKETRATE_DBT T, PARAMS
          WHERE        T.T_SERVISEKIND = PARAMS.T_SERVISEKIND
                      AND T.T_PRODUCTKINDID = PARAMS.T_PRODUCTKINDID
                      AND T.T_FIID = PARAMS.T_FIID
                      AND (T.T_CLIENTTYPE = PARAMS.T_CLIENTTYPE OR T.T_CLIENTTYPE = 0)
                      AND T.T_BRANCHID IN
                          ( SELECT DepCode FROM                         
                                               (SELECT DISTINCT DEP.DepCode, DEP.DepLevel
                                                FROM DMARKETRATE_DBT RATE, PARAMS, (SELECT DEP.T_CODE as DepCode,  Level as DepLevel 
                                                                                      FROM DDP_DEP_DBT DEP, PARAMS 
                                                                                      START WITH DEP.T_CODE = PARAMS.T_BRANCHID 
                                                                                      CONNECT BY PRIOR DEP.T_PARENTCODE = DEP.T_CODE) DEP
                                               WHERE RATE.T_SERVISEKIND = PARAMS.T_SERVISEKIND
                                                      AND RATE.T_PRODUCTKINDID = PARAMS.T_PRODUCTKINDID
                                                      AND RATE.T_FIID = PARAMS.T_FIID
                                                      AND (RATE.T_CLIENTTYPE = PARAMS.T_CLIENTTYPE OR RATE.T_CLIENTTYPE = 0)
                                                      AND RATE.T_BRANCHID = DEP.DepCode
                                                      AND (RATE.T_PRODUCTID = PARAMS.T_PRODUCTID OR RATE.T_PRODUCTID = 0)
                                                      AND (RATE.T_INTERESTFREQUENCY = PARAMS.T_INTERESTFREQUENCY OR RATE.T_INTERESTFREQUENCY = 0)
                                                      AND EXISTS (SELECT 1 FROM  dvalintmarketrate_dbt, PARAMS 
                                                                           WHERE (t_DeleteDate = TO_DATE('01.01.0001', 'DD.MM.YYYY') OR t_DeleteDate IS NULL) 
                                                                             AND t_MarketRateID = RATE.T_ID 
                                                                             AND t_BeginDate <= PARAMS.T_DATE
                                                                 )
                                                      AND ( (RATE.T_CONTRACTYEAR * 360 + RATE.T_CONTRACTMONTHS * 30 + RATE.T_CONTRACTDAYS) > PARAMS.T_CONDDAYS )
                                                      AND (RATE.T_CONTRACTSUM < PARAMS.T_CONTRACTSUM OR (PARAMS.T_CONTRACTSUM = 0 AND RATE.T_CONTRACTSUM = 0) )
                                                      AND (RATE.T_NOTUSE = CHR (0) OR (RATE.T_NOTUSE IS NULL AND ROWNUM = 1))
                                               ORDER BY DEP.DepLevel)                 
                             WHERE ROWNUM = 1
                          )
                      AND EXISTS (SELECT 1 FROM  dvalintmarketrate_dbt, PARAMS 
                                           WHERE (t_DeleteDate = TO_DATE('01.01.0001', 'DD.MM.YYYY') OR t_DeleteDate IS NULL) 
                                             AND t_MarketRateID = T.T_ID 
                                             AND t_BeginDate <= PARAMS.T_DATE
                                 )              
                      AND (T.T_PRODUCTID = PARAMS.T_PRODUCTID OR T.T_PRODUCTID = 0)
                      AND (T.T_INTERESTFREQUENCY = PARAMS.T_INTERESTFREQUENCY OR T.T_INTERESTFREQUENCY = 0)
                      AND ( (T.T_CONTRACTYEAR * 360 + T.T_CONTRACTMONTHS * 30 + T.T_CONTRACTDAYS) > PARAMS.T_CONDDAYS )
                      AND (T.T_CONTRACTSUM < PARAMS.T_CONTRACTSUM OR (PARAMS.T_CONTRACTSUM = 0 AND T.T_CONTRACTSUM = 0) )
                      AND (T.T_NOTUSE = CHR (0) OR (T.T_NOTUSE IS NULL AND ROWNUM = 1))
          ORDER BY T.T_CLIENTTYPE DESC, T.T_PRODUCTID DESC, T.T_INTERESTFREQUENCY DESC, (T.T_CONTRACTYEAR * 360 + T.T_CONTRACTMONTHS * 30 + T.T_CONTRACTDAYS),
                  T.T_CONTRACTSUM DESC
        ) WHERE ROWNUM = 1;

        SELECT t_MinValue, t_MaxValue, DateRange INTO pMinValue, pMaxValue, mDateRange
        FROM   (SELECT t_MinValue, t_MaxValue, ABS(t_BeginDate - mDate) DateRange 
                    FROM dvalintmarketrate_dbt
                WHERE (t_DeleteDate = TO_DATE('01.01.0001', 'DD.MM.YYYY') OR t_DeleteDate IS NULL) 
                    AND t_MarketRateID = mRateID 
                    AND t_BeginDate <= mDate
                ORDER BY DateRange, t_BeginDate DESC)
        WHERE ROWNUM <= 1;
      ELSE
        SELECT t_MinValue, t_MaxValue, ABS(t_BeginDate - mDate) DateRange, v.T_MARKETRATEID, m.T_SOURCEDATALAYER 
          INTO pMinValue, pMaxValue, mDateRange, mRateID, mSourceDataLayer
          FROM DMARKETRATE_DBT m
          INNER JOIN dvalintmarketrate_dbt v ON  v.T_MARKETRATEID = m.T_ID
                                            AND (v.t_DeleteDate = TO_DATE('01.01.0001', 'DD.MM.YYYY') OR v.t_DeleteDate IS NULL)
                                            AND (v.T_BEGINDATE <= mDate)
         WHERE m.T_FIID = pFiID         
           AND m.T_CONTRACTDAYS > mCondDays
           AND (m.T_INTERESTFREQUENCY = pInterestFrequency OR m.T_INTERESTFREQUENCY = 0)
      ORDER BY m.T_CONTRACTDAYS, DateRange, v.t_BeginDate DESC
      FETCH NEXT 1 ROWS ONLY;
      END IF;

        pSourceDataLayer := mSourceDataLayer;
        pMarketRateID    := mRateID;

        RETURN 0;
    EXCEPTION WHEN NO_DATA_FOUND THEN
        RETURN 1;
    END;


END RSI_MARKETRATE;
/
