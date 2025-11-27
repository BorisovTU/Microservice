 /**

  -- Author  : Nikonorov Evgeny
  -- Purpose : Пакет подготовки данных для отчета "Суммы подтвержденных расходов"

# changelog
 |date       |autor          |tasks                                           |note
 |-----------|---------------|------------------------------------------------|-------------------------------------------------------------
 |26.11.2025 |Велигжанин А.В.|DEF-112611                                      |GenSumConfirmChunks (), Генерация заданий для отчета
 |07.11.2025 |Велигжанин А.В.|DEF-109328                                      |CreateRepData (..., p_OnDate)
 |07.11.2025 |Велигжанин А.В.|DEF-109307                                      |GenFiidChunks(), генерация заданий по всем клиентам с полученным ФИ
 |30.10.2025 |Велигжанин А.В.|AVT_BOSS-573_AVT_BOSS-1703                      |InitParallelCalc(), ClearParallelCalc(), DeleteParallelCalc()
 |23.10.2025 |Велигжанин А.В.|AVT_BOSS-1767_AVT_BOSS-1351                     |Перенос задач по доработке в release-122.0
 |25.08.2025 |Велигжанин А.В.|DEF-98100                                       |ProcessDepo(), замена сделок "Зачисления ДЕПО" 
 |           |               |                                                |соответствующими сделками "Зачисления НДФЛ"
 |02.04.2025 |Велигжанин А.В.|DEF-84364                                       |GetNdflTicks(), запоминается валюта во избежании дублей
 |           |               |                                                |UpdateNdflConv(), обновление коэффициентов для 'Зачислений НДФЛ',
 |           |               |                                                |если была конвертация
 |18.11.2024 |Велигжанин А.В.|DEF-73119                                       |GetSubstInfo(), инфа о сделке замещения
 |13.11.2024 |Велигжанин А.В.|DEF-73272                                       |GetNdflTicks(), сбор данных о сделках 'Зачисления НДФЛ' 
 |           |               |                                                |(инфа о которых отсутствует в лотах)
 |26.08.2024 |Велигжанин А.В.|BOSS-2935                                       |Для режима 'ЦБ на дату' данные подготавливаются через
 |           |               |                                                |таблицу лотов dpmwrtsum_dbt (если дата отчета > макс. даты изменения лотов)
 |           |               |                                                |или через архив лотов
 |09.08.2024 |Велигжанин А.В.|DEF-69222                                       |Изменен алгоритм для режима 'Переведенные другому брокеру'

  */

CREATE OR REPLACE PACKAGE RSI_SUMCONFEXP
IS

  /** 
   @brief    Вспомогательные функции для получения данных
  */
  FUNCTION GetDealDate ( p_DealID IN NUMBER ) RETURN DATE deterministic;			-- дата покупки (дата сделки)
  FUNCTION GetSettleDate ( p_DealID IN NUMBER ) RETURN DATE deterministic;			-- дата получения (дата расчетов)
  FUNCTION GetDealCfi ( p_DealID IN NUMBER ) RETURN NUMBER deterministic;			-- валюта сделки
  FUNCTION GetDealDateRate ( p_DealID IN NUMBER ) RETURN DATE deterministic;			-- дата сделки для курса
  FUNCTION GetRate ( p_Sum IN NUMBER, p_FIID IN NUMBER, p_Date IN DATE ) 
    RETURN NUMBER deterministic;								-- Возвращает курс
  FUNCTION GetPrice ( p_DealID IN NUMBER ) RETURN NUMBER deterministic;				-- цена сделки
  FUNCTION GetCost ( p_DealID IN NUMBER ) RETURN NUMBER deterministic;				-- сумма сделки
  FUNCTION GetNKD ( p_DealID IN NUMBER ) RETURN NUMBER deterministic;				-- сумма НКД
  FUNCTION GetTotalCost ( p_DealID IN NUMBER, p_NKD IN NUMBER ) RETURN NUMBER deterministic;	-- полную стоимость
  FUNCTION GetCommiss(
    p_DocKind IN NUMBER
    , p_DocID IN NUMBER
    , p_SubstDate OUT date
  ) RETURN NUMBER deterministic; 	-- комиссии

  /** Тип для коллекции обработанных лотов по конвертации
  */
  type t_Nodes is table of number index by binary_integer;

  /** Тип данных для курсора для получения сделок 'Зачисления НДФЛ'
  */
  TYPE t_NdflRec IS RECORD (
    t_BuyDealDate     DATE             -- Дата сделки
    , t_BuyDealTime   DATE             -- Время сделки
    , t_BuyDealID     NUMBER(10,0)     -- ID сделки
    , t_Amount        NUMBER(32,12)    -- Объем сделки-покупки, соответствующий сделке-продаже
    , t_NKD           NUMBER(32,12)    -- НКД
    , t_Principal     NUMBER(32,12)    -- сумма сделки-покупки
    , t_BuyDocKind    NUMBER(32,12)    -- тип сделки-покупки
  );

  /** Тип данных для курсора для получения данных для режима 'Переведенные другому брокеру'
  */
  TYPE t_MatchTickRec IS RECORD (
      -- инфа о сделке продажи
      t_SaleDealDate		DATE		-- Дата сделки
      , t_SaleDealTime		DATE		-- Время сделки
      , t_SaleDealID            NUMBER(10,0)	-- ID сделки
      -- инфа о сделке покупки
      , t_BuySumID		NUMBER		-- ID лота покупки
      , t_BuyDealDate		DATE		-- Дата сделки
      , t_BuyDealTime		DATE		-- Время сделки
      , t_BuyDealID            	NUMBER(10,0)	-- ID сделки
      , t_Amount                NUMBER(32,12)	-- Объем сделки-покупки, соответствующий сделке-продаже
      , t_IsNdfl           	NUMBER		-- 1 -- значит "Зачисление НДФЛ"
      , t_IsDepo           	NUMBER		-- 1 -- значит "Зачисление ДЕПО"
      , t_NKD            	NUMBER(32,12)	-- НКД
      , t_FIID                  NUMBER          -- ФИ сделки приобретения
      , t_PartyID  		NUMBER		-- Субъект сделки приобретения
      , t_SubstID		NUMBER		-- ID сделки замещения, -1 если нет
      , t_SubstStatus		NUMBER		-- Статус обработки сделки замещения, 0-не обработана, 1-обработана
      , t_ParentID              NUMBER          -- Родитель лота конвертации
      , t_ConvStatus            NUMBER          -- Статус обработки лотов конвертации, 0-не обработаны, 1-обработаны
      , t_IsConv                NUMBER          -- если 1 -- лот конвертации
      , t_Numerator             NUMBER          -- множитель
      , t_Denominator           NUMBER          -- делитель
      , t_SumPrecision          NUMBER          -- точность конвертации
      , t_Principal             NUMBER		-- сумма сделки-покупки
      , t_BuyDocKind            NUMBER(32,12)	-- тип сделки-покупки
      , t_Cur            	NUMBER		-- валюта отчета (не всегда совпадает с t_FIID)
  );

  /** Тип данных для сделок-покупок
  */
  TYPE t_BuyTickRec IS RECORD (
     t_BuyDealID		NUMBER		-- ID сделки-покупки 
     , t_IsConv			NUMBER		-- 0 -- не конвертация, 1 -- конвертация
     , t_Parent			NUMBER		-- родительский лот (для конвертации важно)
     , t_Numerator 		NUMBER         	-- множитель
     , t_Denominator 		NUMBER         	-- делитель
     , t_SumPrecision 		NUMBER 		-- точность конвертации
     , t_Amount 		NUMBER 		-- сумма лота
     , t_DealDate 		DATE		-- дата сделки
     , t_DealTime 		DATE		-- время сделки
     , t_Pfi 			NUMBER		-- валюта сделки-покупки
     , t_Principal 		NUMBER		-- общий объем сделки-покупки
     , t_BuyDocKind 		NUMBER		-- Вид документа сделки-покупки 
     , t_BuySumID		NUMBER		-- лот сделки-покупки 
     , t_IsDepo		    	NUMBER		-- 0 -- не сделка зачисления ДЕПО, 1 -- зачисление ДЕПО
     , t_ClientID		NUMBER		-- ID клиента
  );
  TYPE Tab_BuyTickRec IS TABLE OF t_BuyTickRec;

  /** 
   @brief    Возвращает ID сделки замещения, если она есть, иначе -1
  */
  FUNCTION GetSubstDeal( p_DealID IN NUMBER ) 
    RETURN NUMBER deterministic;

  /** 
   @brief    Возвращает ID родительского лота, если полученный лот является лотом по конвертации.
             Иначе -1.
  */
  FUNCTION GetParentID( p_SumID IN NUMBER ) RETURN NUMBER;

  /** 
   @brief    Получение данных для отчета по договору
   @param[in]  p_DlContrID     	ID ДБО
   @param[in]  p_DlContrID_2  	ID ДБО
   @param[in]  p_GUID     	GUID отчета
   @param[in]  p_OnDate    	дата отчета
   @param[in]  p_CurDate    	дата опер.дня
   @param[in]  p_LaunchMode    	режим запуска отчета
   @param[in]  p_FIID    	фин.инструмент
   @param[out] p_Cnt		Количество записей, обработанных по заданию
  */
  PROCEDURE CreateSumConfirmExpRepDataByContr (
      p_DlContrID IN NUMBER
      , p_DLContrID_2 IN NUMBER
      , p_GUID IN VARCHAR2
      , p_OnDate IN DATE
      , p_CurDate IN DATE
      , p_LaunchMode IN NUMBER
      , p_FIID IN NUMBER
      , p_Cnt OUT NUMBER
  );

  /** 
   @brief    Получение данных для отчета
  */
  PROCEDURE CreateSumConfirmExpRepDataInter( 
      p_GUID IN VARCHAR2
      , p_OnDate IN DATE
      , p_CurDate IN DATE
      , p_LaunchMode IN NUMBER
      , p_ClientID IN NUMBER
      , p_DlContrID IN NUMBER
      , p_FIID IN NUMBER
  );

  /** 
   @brief    Получение данных для отчета
  */
  PROCEDURE CreateSumConfirmExpRepData( 
      p_GUID IN VARCHAR2
      , p_OnDate IN DATE
      , p_LaunchMode IN NUMBER
      , p_ClientID IN NUMBER
      , p_DlContrID IN NUMBER
      , p_FIID IN NUMBER
  );

  /** 
   @brief    Заполняет таблицу dmatchticks_tmp (для режима 'Переведенные другому брокеру'), 
             с информацией о сделках покупках-продажи, которая является входной информацией для работы процедуры CreateRepData()
  */
  PROCEDURE GetMatchTicks( 
     p_DlContrID IN NUMBER, p_OnDate IN DATE, p_CurDate IN DATE, p_PartyID IN NUMBER, p_FIID IN NUMBER
  );

  /** 
   @brief    Заполняет таблицу dmatchticks_tmp (для режима 'ЦБ на дату'), 
             с информацией о сделках покупках-продажи, которая является входной информацией для работы процедуры CreateRepData()
  */
  PROCEDURE GetBuyTicks( 
     p_OnDate IN DATE, p_PartyID IN NUMBER, p_FIID IN NUMBER
  );

  /** 
   @brief    Заполняет таблицу dmatchticks_tmp 
             данными о сделках 'Зачисления НДФЛ' (инфа о которых отсутствует в лотах)
  */
  PROCEDURE GetNdflTicks( 
     p_SaleDealDate IN DATE, p_SaleDealTime IN DATE, p_SaleDealID IN NUMBER, p_PartyID IN NUMBER, p_FIID IN NUMBER, p_Cur IN NUMBER
  );

  /** 
   @brief    Возвращает 1, если полученный лот является лотом сделки "зачисления ДЕПО".
      	     В истории по нему, начальный t_cost будет нулевым.
             В последующем, лоты "зачисления ДЕПО" не обрабатываются.
  */
  FUNCTION IsDepoLot( p_SumID IN NUMBER ) RETURN NUMBER;

  /**
   @brief    Функция для получения row_id задания
   @param[in]	p_CalcID     	номер расчета (itt_parallel_exec.calc_id)
   @param[out]  p_DlContrID    	ID договора брокерского обслуживания
   @param[out]  p_FIID     	ID финансового инструмента
   @param[out]  p_LaunchMode   	режим запуска отчета
   @param[out]  p_OnDate    	дата отчета
   @param[out]  p_CurDate    	дата опер.дня
  */
  FUNCTION GetRowID (
     p_CalcID IN varchar2
     , p_DlContrID OUT NUMBER
     , p_FIID OUT NUMBER
     , p_LaunchMode OUT NUMBER
     , p_OnDate OUT DATE
     , p_CurDate OUT DATE
  ) 
  RETURN NUMBER;

  /**
   @brief    отметка о завершении обработки задания
   @param[in]  p_RowID     	ID задания
   @param[in]  p_GUID     	GUID отчета
   @param[in]  p_Cnt     	кол-во строк, обработанных по заданию
  */
  PROCEDURE EndProcess ( p_RowID IN number, p_GUID IN varchar2, p_Cnt IN number );

  /**
   @brief    Нитка параллельной обработки заданий.
   @param[in]  p_ParaID     	номер процесса
   @param[in]  p_CalcID     	номер расчета (itt_parallel_exec.calc_id)
   @param[in]  p_Limit     	ограничитель итераций для какждого потока, если 0 -- выполняется всё
   @param[in]  p_GUID     	GUID отчета
  */
  PROCEDURE ExecParallelProc ( p_ParaID IN NUMBER, p_CalcID IN VARCHAR2, p_Limit IN NUMBER, p_GUID IN varchar2 );

  /**
   @brief    Процедура параллельного получения данных для отчета.
   @param[in]  p_CalcID     	номер расчета (itt_parallel_exec.calc_id)
   @param[in]  p_ParaLevel     	кол-во параллельных процессов
   @param[in]  p_Limit     	ограничитель итераций для какждого потока, если 0 -- выполняется всё
   @param[in]  p_GUID     	GUID отчета
  */
  PROCEDURE ExecParallel ( p_CalcID IN varchar2, p_ParaLevel IN NUMBER, p_Limit IN number, p_GUID IN varchar2 );

  /** 
   @brief    Процедура обработки необработанных сделок "зачисления ДЕПО".
  */
  PROCEDURE ProcessDepo;

  /** 
   @brief    Установка флага отладки
  */
  PROCEDURE SetDebugFlag( p_DebugFlag IN number );

  /**
   @brief    Функция инициализации отчета для параллельной работы
   @return   номер расчета (см. itt_parallel_exec.calc_id)
  */
  FUNCTION InitParallelCalc RETURN varchar2;

  /**
   @brief    Процедура очистки данных отчета
   @param[in]	p_RepID   	номер расчета (см. itt_parallel_exec.calc_id)
  */
  PROCEDURE ClearParallelCalc(p_RepID IN varchar2);

  /**
   @brief    Процедура удаления данных отчета
   @param[in]	p_RepID   	номер расчета (см. itt_parallel_exec.calc_id)
  */
  PROCEDURE DeleteParallelCalc(p_RepID IN varchar2);

  /**
   @brief    Генерация чанка (задания) для itt_parallel_exec.
   @param[in]  p_CalcID     	номер расчета (itt_parallel_exec.calc_id)
   @param[in]  p_ClientID     	ID клиента
   @param[in]  p_FIID     	ID финансового инструмента
   @param[in]  p_LaunchMode    	режим запуска отчета
   @param[in]  p_OnDate    	дата отчета
   @param[in]  p_CurDate    	дата опер.дня
   @return			Количество сгенеренных заданий
  */
  FUNCTION GenClientChunk ( 
    p_CalcID IN NUMBER
    , p_ClientID IN NUMBER
    , p_FIID IN NUMBER
    , p_LaunchMode IN NUMBER
    , p_OnDate IN DATE
    , p_CurDate IN DATE 
  ) 
  RETURN NUMBER;

  /**
   @brief    Запуск процесса получения данных для отчета. Запускается сервисом ExecuteCode через QManager
   @param[in]  p_worklogid     	ID задания (itt_q_message_log.msgid)
   @param[in]  p_messmeta     	мета-данные задания
  */
  PROCEDURE CallProcess ( 
    p_worklogid integer, p_messmeta  xmltype 
  );

  /**
   @brief    Запуск сервиса ExecuteCode через QManager
   @param[in]  p_CalcID     	ID расчета (itt_parallel_exec.calc_id)
   @param[in]  p_ParallelCnt   	Количество параллельных процессов
   @param[in]  p_Limit   	Ограничитель выполняемых заданий. Если 0, выполняются все.
   @param[in]  p_Guid   	GUID отчета.
  */
  PROCEDURE CallExecuteCodeInQManager( 
    p_CalcID IN varchar2
    , p_ParallelCnt IN NUMBER
    , p_Limit IN NUMBER
    , p_GUID IN varchar2 
  );

  /**
   @brief    Генерация чанков (заданий) для itt_parallel_exec по всем клиентам с полученным ФИ
   @param[in]  p_CalcID     	номер расчета (itt_parallel_exec.calc_id)
   @param[in]  p_FIID     	ID финансового инструмента
   @param[in]  p_LaunchMode    	режим запуска отчета
   @param[in]  p_OnDate    	дата отчета
   @param[in]  p_CurDate    	дата опер.дня
   @return			Количество сгенеренных заданий
  */
  FUNCTION GenFiidChunks ( 
    p_CalcID IN NUMBER
    , p_FIID IN NUMBER
    , p_LaunchMode IN NUMBER
    , p_OnDate IN DATE
    , p_CurDate IN DATE 
  ) 
  RETURN NUMBER;

  /**
   @brief    Генерация заданий для отчета (с последующей обработкой через QManager)
   @param[in]  p_Guid   	GUID отчета.
   @param[in]  p_RepDate    	дата отчета
   @param[in]  p_CurDate    	дата опер.дня
   @param[in]  p_LaunchMode    	режим запуска отчета
   @param[in]  p_ClientID     	ID клиента
   @param[in]  p_DlContrID    	ID договора брокерского обслуживания
   @param[in]  p_FIID     	ID финансового инструмента
   @param[in]  p_Limit     	Ограничитель заданий
   @param[out] p_CalcID     	ID расчета (itt_parallel_exec.calc_id)
   @param[out] p_Cnt     	Количество сгенеренных заданий
  */
  PROCEDURE GenSumConfirmChunks ( 
    p_GUID IN VARCHAR2, p_RepDate IN DATE, p_CurDate IN DATE, p_LaunchMode IN NUMBER
    , p_ClientID IN NUMBER, p_DlContrID IN NUMBER, p_FIID IN NUMBER, p_Limit IN NUMBER
    , p_CalcID OUT NUMBER, p_Cnt OUT NUMBER
  );

END RSI_SUMCONFEXP;
/