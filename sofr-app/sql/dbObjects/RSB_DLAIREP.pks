CREATE OR REPLACE PACKAGE RSB_DLAIREP
IS
  PTSK_STOCKDL             CONSTANT NUMBER := 1;    --Фондовый дилинг
  PTSK_VEKSACC             CONSTANT NUMBER := 14;   --Учтенные векселя
  PTSK_DV                  CONSTANT NUMBER := 15;   --Срочные контракты
  PTSK_CM                  CONSTANT NUMBER := 21;   --Валютный рынок
  
  DL_VAREPAY               CONSTANT NUMBER := 142;  -- Погашение учтенных векселей
  DL_VAPAWN                CONSTANT NUMBER := 143;  -- Залог учтенных векселей
  
  DL_PREPARING             CONSTANT NUMBER := 0;    --Статус сделки "Отложена"
  DL_READIED               CONSTANT NUMBER := 10;   --Сделка готова (создана операция)
  
  PTK_INTERNATIONAL_ORG    CONSTANT NUMBER := 34;   --Международная организация
  
  RATETYPE_MARKET_PRICE    CONSTANT NUMBER := 1;    --Вид курса "Рыночная цена"
  RATETYPE_NKD             CONSTANT NUMBER := 15;   --Вид курса "НКД на одну бумагу"
  RATETYPE_REASONED_PRICE  CONSTANT NUMBER := 1001; --Вид курса "Мотивированное суждение"
  RATETYPE_BLOOMBERG_PRICE CONSTANT NUMBER := 1002; --Вид курса "Цена закрытия Bloomberg для ф.707"
  
  MOEX_CALENDAR_ID         CONSTANT NUMBER := 20;   --ID календаря ММВБ
  
  RISKLEVEL_NOTINSTALL     CONSTANT NUMBER := 0;    --Без уровня
  RISKLEVEL_USUAL          CONSTANT NUMBER := 1;    --Стандартный
  RISKLEVEL_ELEVATED       CONSTANT NUMBER := 2;    --Повышенный
  RISKLEVEL_SPECIAL        CONSTANT NUMBER := 3;    --Особый

  
  MAX_ROWCOUNT             CONSTANT NUMBER := 1048000; --Максимальное количество строк для печати в Excel
  
  NOT_USE_DEALS_ATTR_GRP    CONSTANT NUMBER := 66;      --Категория "Не использовать в отчетах цену сделок" (Ц/б)
 
   FUNCTION IsDateAfterWorkDayM (
                                   p_Date IN DATE,
                                   p_SinceDate IN DATE,
                                   p_identProgram IN NUMBER,
                                   p_maxDaysCnt IN NUMBER, 
                                   p_CalParamArr RSI_DlCalendars.calparamarr_t)
   RETURN NUMBER DETERMINISTIC;
 
  -- Обработка ДО
  PROCEDURE ProcessDO(pClientContrId IN NUMBER, pSessionID IN NUMBER, pBegDate IN DATE, pEndDate IN DATE, pIsParallel IN NUMBER DEFAULT 1);


  --Формирование данных
  PROCEDURE CreateAllData( pBegDate IN DATE,
                           pEndDate IN DATE,
                           pSessionID IN NUMBER,
                           pParallelLevel IN NUMBER,
                           pPartitionCount IN NUMBER,
                           pPartitionNum IN NUMBER );


  --Формирование csv-файла с данными таблицы Расшифровки
  FUNCTION CreateDecrTableDataCSV(pSessionID IN NUMBER) RETURN NUMBER;
  
  --Получение части даннных
  FUNCTION GetDecrTableDataCSV(pNumber NUMBER DEFAULT 1) RETURN CLOB;
  
  --Удаление данных
  PROCEDURE ClearDecrTableDataCSV;

END RSB_DLAIREP;
/