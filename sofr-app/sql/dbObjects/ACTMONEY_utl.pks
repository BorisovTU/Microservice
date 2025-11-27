CREATE OR REPLACE PACKAGE ACTMONEY_utl
AS
  /**
   @file 		ACTMONEY_utl.pks
   @brief 		Утилиты для отчета-сверки ДС между БУ и ВУ
     
# changelog
 |date       |autor          |tasks                                           |note
 |-----------|---------------|------------------------------------------------|-------------------------------------------------------------
 |05.03.2025 |Велигжанин А.В.|DEF-83777                                       |доработка ProcessTask (), для ЕДП данные собираются только
 |           |               |                                                |по суб-договора фондового рынка
 |13.02.2025 |Велигжанин А.В.|BOSS-5882_BOSS-7917                             |CreateData(), формирование данных для отчета-сверки
 |06.02.2025 |Велигжанин А.В.|BOSS-5882_BOSS-7711                             |Создание                  
  */


  /**
   @brief    массив с валютами
  */
  TYPE tt_Currency IS TABLE OF number;

  /**
   @brief    Возвращает массив валют для договора
   @param[in]  	p_SfContrID    		ID договора
   @param[in]  	p_FIID 			ID финансового инструмента
  */
  FUNCTION GetCurArray ( 
    p_SfContrID IN NUMBER
    , p_FIID IN NUMBER
  ) 
  RETURN tt_Currency;

  /**
   @brief    Функция возвращает ID суб-договора фондового рынка для ЕДП. Иначе -- NULL.
   @param[in] 	p_DlContrID    		ID договора
  */
  FUNCTION GetSfEdp(p_DlContr IN NUMBER)
     RETURN number;

  /**
   @brief    Запуск процесса получения данных для отчета.
   @param[in]  	p_CalcID     		ID расчета (itt_parallel_exec.calc_id)
   @param[in] 	p_BegDate       	начальная дата отчета
   @param[in] 	p_EndDate     		конечная дата отчета
   @param[in] 	p_PartyID     		ID клиента
   @param[in] 	p_SfContrID    		ID договора
   @param[in] 	p_FIID     		ID финансового инструмента
   @param[in] 	p_DiffFlag     		если 'X', то отчет показывает только различия, иначе всё
  */
  PROCEDURE ProcessTask ( 
    p_CalcID IN varchar2
    , p_BegDate IN DATE
    , p_EndDate IN DATE
    , p_PartyID IN number
    , p_SfContrID IN number
    , p_FIID IN number
    , p_DiffFlag IN varchar2 
  );


  /**
   @brief    Возвращает счет внутреннего учета (ВУ)
   @param[in]  	p_SfContrID    		ID договора
   @param[in]  	p_FIID 			ID финансового инструмента
  */
  FUNCTION GetInnerAcc ( 
    p_SfContrID IN NUMBER
    , p_FIID IN NUMBER
  ) 
  RETURN varchar2;

  /**
   @brief    Возвращает счет бухгалтерского учета (БУ)
   @param[in]  	p_SfContrID    		ID договора
   @param[in]  	p_FIID 			ID финансового инструмента
  */
  FUNCTION GetGbAcc ( 
    p_SfContrID IN NUMBER
    , p_FIID IN NUMBER
  ) 
  RETURN varchar2;

  /**
   @brief    отметка о завершении обработки задания
   @param[in]  p_RowID     	ID задания
   @param[in]  p_Status     	статус задания
  */
  PROCEDURE EndTask( p_RowID IN NUMBER, p_Status IN VARCHAR2 );

  /**
   @brief    Добавление строки в отчет
   @param[in]  	p_RepID     		ID отчета (dlactmoney5882_dbt.t_repid он же itt_parallel_exec.calc_id)
   @param[in] 	p_PartyID     		ID клиента
   @param[in] 	p_SfContrID    		ID договора
   @param[in] 	p_InnerAcc    		Счет ВУ
   @param[in] 	p_GbAcc    		Счет БУ
   @param[in] 	p_FIID     		ID финансового инструмента
   @param[in] 	p_RestDate       	дата расчета остатков
   @param[in] 	p_DiffFlag     		если 'X', то отчет показывает только различия, иначе всё
  */
  PROCEDURE AddRepRow ( 
    p_RepID IN NUMBER, p_PartyID IN NUMBER, p_SfContrID IN NUMBER, p_InnerAcc IN varchar2, p_GbAcc varchar2
    , p_FIID IN NUMBER, p_RestDate IN DATE, p_DiffFlag IN varchar2 
  );

  /**
   @brief    Возвращает ID задания, пригодного для обработки (itt_parallel_exec.row_id)
             Также возвращаются реквизиты задания
   @param[in]  	p_CalcID     		ID расчета (itt_parallel_exec.calc_id)
   @param[out] 	p_BegDate       	начальная дата отчета
   @param[out] 	p_EndDate     		конечная дата отчета
   @param[out] 	p_PartyID     		ID клиента
   @param[out] 	p_SfContrID    		ID договора
   @param[out] 	p_FIID     		ID финансового инструмента
   @param[out] 	p_DiffFlag     		если 'X', то отчет показывает только различия, иначе всё
  */
  FUNCTION GetReadyTask ( 
    p_CalcID IN varchar2
    , p_BegDate OUT DATE
    , p_EndDate OUT DATE
    , p_PartyID OUT number
    , p_SfContrID OUT number
    , p_FIID OUT number
    , p_DiffFlag OUT varchar2 
  ) 
    RETURN number;

  /**
   @brief    Запуск процесса получения данных для отчета.
   @param[in]  p_CalcID     	ID расчета (itt_parallel_exec.calc_id)
  */
  PROCEDURE ReportProcess ( p_ProcessNo IN NUMBER, p_CalcID IN varchar2 );

  /**
   @brief    Запуск процесса получения данных для отчета. Запускается сервисом ExecuteCode через QManager
   @param[in]  p_worklogid     	ID задания (itt_parallel_exec.calc_id)
   @param[in]  p_messmeta     	мета-данные задания
  */
  PROCEDURE CallProcess ( p_worklogid integer, p_messmeta  xmltype );

  /**
   @brief    Запуск сервиса ExecuteCode через QManager
   @param[in]  p_CalcID     	ID расчета (itt_parallel_exec.calc_id)
   @param[in]  p_ParallelCnt   	Количество параллельных процессов
  */
  PROCEDURE CallExecuteCodeInQManager( p_CalcID IN varchar2, p_ParallelCnt IN NUMBER );

  /**
   @brief    Процедура очистки данных отчета-сверки
   @param[in]	p_RepID   	ID отчета
  */
  PROCEDURE ClearRep(p_RepID IN varchar2);

  /**
   @brief    Процедура формирования данных для отчета-сверки
   @param[in]  	p_BegDate       начальная дата отчета
   @param[in]  	p_EndDate     	конечная дата отчета
   @param[in]  	p_FIID     	ID финансового инструмента
   @param[in]  	p_DiffFlag     	если 'X', то отчет показывает только различия, иначе всё
   @param[out]	p_RepID   	Возвращает ID отчета
   @param[out]	p_Cnt   	Количество заданий для параллельного расчета
  */
  PROCEDURE CreateData ( 
    p_BegDate IN DATE
    , p_EndDate IN DATE
    , p_FIID IN number
    , p_DiffFlag IN varchar2 
    , p_RepID OUT varchar2 
    , p_Cnt OUT NUMBER
  );

END ACTMONEY_utl;
/
