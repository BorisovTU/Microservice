CREATE OR REPLACE PACKAGE RSB_DLCLBOREP
IS
  PTSK_STOCKDL             CONSTANT NUMBER := 1;    --Фондовый дилинг
  PTSK_VEKSACC             CONSTANT NUMBER := 14;   --Учтенные векселя
  PTSK_DV                  CONSTANT NUMBER := 15;   --Срочные контракты
  PTSK_CM                  CONSTANT NUMBER := 21;   --Валютный рынок
  
  DL_PREPARING             CONSTANT NUMBER := 0;    --Статус сделки "Отложена"
  
  PTK_INTERNATIONAL_ORG    CONSTANT NUMBER := 34;   --Международная организация
  
  RATETYPE_MARKET_PRICE    CONSTANT NUMBER := 1;    --Вид курса "Рыночная цена"
  RATETYPE_REASONED_PRICE  CONSTANT NUMBER := 1001; --Вид курса "Мотивированное суждение"
  RATETYPE_BLOOMBERG_PRICE CONSTANT NUMBER := 1002; --Вид курса "Цена закрытия Bloomberg для ф.707"
  RATETYPE_NKD                     CONSTANT NUMBER := 15;   --Вид курса "НКД на одну бумагу"
  
  MOEX_CALENDAR_ID         CONSTANT NUMBER := 20;   --ID календаря ММВБ

  --Виды биржевого рынка для заявок и сделок ФИССиКО, видов обязательств  
  DV_MARKETKIND_CURRENCY   CONSTANT NUMBER := 2;    --Валютный
  
  PM_PURP_COMMBANK         CONSTANT NUMBER := 72;   --Комиссия Банку
  
  NOT_USE_DEALS_ATTR_GRP   CONSTANT NUMBER := 66;   --Категория "Не использовать в отчетах цену сделок" (Ц/б)
  OUTER_WRT_ATTR_GRP       CONSTANT NUMBER := 111;  --Категория "Внешняя операция" (Сделка)
  
  -- Типы задолженностей
  ARREAR_TYPE_NOT_REPO     CONSTANT ddlclboarrear_dbt.t_ArrearType%type := 'По сделкам, кроме РЕПО';
  ARREAR_TYPE_REPO         CONSTANT ddlclboarrear_dbt.t_ArrearType%type := 'По сделкам РЕПО';
  
  -- Подвиды операции списания/зачисления денежных средств
  DL_NPTXOP_WRTKIND_ENROL  CONSTANT NUMBER := 10;   --Зачисление
  DL_NPTXOP_WRTKIND_WRTOFF CONSTANT NUMBER := 20;   --Списание
  
  -- Обработка ДО
  -- Обработка ДО
  PROCEDURE ProcessDO(pSessionID IN NUMBER,
                      pClientContrIdstart IN NUMBER,
                      pClientContrIdend  IN NUMBER,
                      pBegDate IN DATE,
                      pEndDate IN DATE,
                      pIsParallel IN NUMBER DEFAULT 1);


  --Формирование данных
  PROCEDURE CreateAllData( pBegDate IN DATE,
                           pEndDate IN DATE,
                           pSessionID IN NUMBER,
                           pParallelLevel IN NUMBER,
                           pPartitionCount IN NUMBER,
                           pPartitionNum IN NUMBER );

END RSB_DLCLBOREP;
/