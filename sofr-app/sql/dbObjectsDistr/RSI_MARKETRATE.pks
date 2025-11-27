CREATE OR REPLACE PACKAGE RSI_MARKETRATE
IS
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
    ) RETURN NUMBER;

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
    ) RETURN NUMBER;

    -- полностью аналогична ф-и SelectRangeMarketRateEx, но ищет ближайшую запись t_MarketRateID, со сроком > Срок договора
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
    ) RETURN NUMBER;

END RSI_MARKETRATE;
/
