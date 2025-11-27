CREATE OR REPLACE PACKAGE OPRDOCS_utl
AS
  /**
   @file 		OPRDOCS_utl.pks
   @brief 		Утилиты для трансформации данных таблицы OPRDOCS_DBT (CCBO-11001_CCBO-11002)
     
# changelog
 |date       |autor          |tasks                                           |note
 |-----------|---------------|------------------------------------------------|-------------------------------------------------------------
 |12.03.2025 |Велигжанин А.В.|BOSS-7698_BOSS-8204                             |fillDTransferControlDbt()
 |20.12.2024 |Велигжанин А.В.|CCBO-11001_CCBO-11002                           |Создание                  
  */

  /**
   @brief    Функция для получения имени партиции для обработки
  */
  FUNCTION GetReadyPartition RETURN varchar2 ;

  /**
   @brief    Заполнение управляющей таблицы DTRANSFERCONTROL_DBT (см. BOSS-7698_BOSS-8204)
   @param[in]  p_CalcID     	ID расчета (itt_parallel_exec.calc_id)
  */
  PROCEDURE fillDTransferControlDbt ( p_CalcID IN NUMBER );

  /**
   @brief    Возвращает 1, если есть необработанный кусочек партиции
   @param[in]  	p_CalcID     		ID расчета (itt_parallel_exec.calc_id)
   @param[in]  	p_PartitionName 	наименование партиции таблицы OPROPER для обработки
   @param[out]	p_StartID     		начальный t_id_operation
   @param[out] 	p_EndID     		конечный t_id_operation
   @param[out] 	p_RowID     		ID задания ( itt_parallel_exec.row_id )
   @param[out] 	p_SrcTable     		имя таблицы-источника
   @param[out]  p_DstTable     		имя таблицы-приемника
  */
  FUNCTION GetChunk ( 
    p_CalcID IN varchar2
    , p_PartitionName IN VARCHAR2
    , p_StartID OUT NUMBER
    , p_EndID OUT NUMBER
    , p_RowID OUT NUMBER 
    , p_SrcTable OUT VARCHAR2
    , p_DstTable OUT VARCHAR2
  ) 
  RETURN number;

  /**
   @brief    Нитка параллельного выполнения.
   @param[in]  p_ParaID     	номер параллельного процесса
   @param[in]  p_CalcID     	ID расчета (itt_parallel_exec.calc_id)
  */
  PROCEDURE CopyParallelProc ( p_ParaID IN NUMBER, p_CalcID IN varchar2 );

  /**
   @brief    Копирование данных для одной партиции для диапазона t_id_operation c p_StartID по p_EndID
   @param[in]  p_ParaID     	номер параллельного процесса
   @param[in]  p_PartitionName 	наименование партиции таблицы OPROPER для обработки
   @param[in]  p_StartID     	начальный t_id_operation
   @param[in]  p_EndID     	конечный t_id_operation
   @param[in]  p_RowID     	ID задания ( itt_parallel_exec.row_id )
   @param[in]  p_SrcTable     	имя таблицы-источника
   @param[in]  p_DstTable     	имя таблицы-приемника
  */
  PROCEDURE CopyOnePartition ( 
    p_ParaID IN NUMBER
    , p_PartitionName IN VARCHAR2
    , p_StartID IN NUMBER
    , p_EndID IN NUMBER
    , p_RowID IN NUMBER 
    , p_SrcTable IN VARCHAR2
    , p_DstTable IN VARCHAR2
  );

  /**
   @brief    Процедура создания суррогатных партиций в таблице-приемнике.
   @param[in]  p_SrcTable     	имя таблицы-источника
   @param[in]  p_DstTable     	имя таблицы-приемника
  */
  PROCEDURE CreateSurrogates ( p_SrcTable IN varchar2, p_DstTable IN varchar2 );

  /**
   @brief    Перенос данных из таблицы DOPRDOCS_DBT в партиционированную таблицу.
             Так как таблица DOPRDOCS_DBT -- большого размера (~140 Gb на 23-12-2024),
             перенос данных осуществляется параллельно несколькими потоками.
  */
  PROCEDURE CopyParallel ( p_CalcID IN varchar2, p_ParaLevel IN NUMBER DEFAULT 20, p_Limit IN NUMBER DEFAULT 0);

  /**
   @brief    Очистка данных перед запуском копирования.
  */
  PROCEDURE CleanEnv ( p_CalcID IN varchar2 );

  /**
   @brief    Завершение calc_id
  */
  PROCEDURE EndParallelExec ( p_CalcID IN varchar2 );

  /**
   @brief    Параллельный REBUILD индексов.
             Нужно пересоздать два индекса, поэтому запускается два потока.
   @param[in]  p_TableName     	имя таблицы
  */
  PROCEDURE IndexParallel ( p_TableName IN varchar2 );

  /**
   @brief    Нитка параллельного выполнения для пересоздания индекса.
   @param[in]  p_ParaID     	номер параллельного процесса
   @param[in]  p_TableName     	наименование таблицы
  */
  PROCEDURE RebuildOneIndex ( p_ParaID IN NUMBER, p_TableName IN varchar2 );

  /**
   @brief    Нитка параллельной генерации заданий для последующего параллельного копирования.
   @param[in]  p_ParaID     	номер параллельного процесса
   @param[in]  p_CalcID     	ID расчета (itt_parallel_exec.calc_id)
   @param[in]  p_ChunkSize     	размер порции
   @param[in]  p_SrcTable     	имя таблицы-источника
   @param[in]  p_DstTable     	имя таблицы-приемника
  */
  PROCEDURE GenParallelProc ( p_ParaID IN NUMBER, p_CalcID IN VARCHAR2, p_ChunkSize IN NUMBER, p_SrcTable IN varchar2, p_DstTable IN varchar2 );

  /**
   @brief    Генератор заданий для параллельного копирования.
             Возвращает itt_parallel_exec.calc_id
             Задания также генерятся несколькими потоками.

   @param[in]  p_CalcID     	ID расчета (itt_parallel_exec.calc_id)
   @param[in]  p_ParaLevel     	количество параллельных процессов
   @param[in]  p_ChunkSize     	размер порции
   @param[in]  p_SrcTable     	имя таблицы-источника
   @param[in]  p_DstTable     	имя таблицы-приемника
  */
  PROCEDURE GenParallel ( 
    p_CalcID IN varchar2
    , p_ParaLevel IN NUMBER DEFAULT 12
    , p_ChunkSize IN number DEFAULT 300000
    , p_SrcTable IN varchar2 DEFAULT 'DOPRDOCS_DBT'
    , p_DstTable IN varchar2 DEFAULT 'DOPRDOCS1_DBT'
  );

  /**
   @brief    Проверка размера
   @param[in]  	p_CalcID     	ID расчета (itt_parallel_exec.calc_id)
   @param[in]  	p_Limit     	кол-во заданий для обработки
   @param[out]	p_Expected   	Кол-во ожидаемых записей
   @param[out]	p_Processed   	Кол-во обработанных записей
  */
  FUNCTION CheckSize ( 
    p_CalcID IN varchar2
    , p_Limit IN number 
    , p_Expected OUT number
    , p_Processed OUT number
  ) 
  RETURN number;

  /**
   @brief    Главная процедура трансформации данных OPRDOCS.
             Возвращает код результата: если 0 -- ok, если > 0 -- ошибка
   @param[in]   p_CalcID     	ID расчета (itt_parallel_exec.calc_id) для заданий параллельного копирования,
                                получается посредством вызова InitProcess()
   @param[in]  	p_SrcTable     	имя исходной таблицы
   @param[in]  	p_DstTable     	имя таблицы-результата
   @param[in]  	p_Limit     	кол-во заданий для обработки
   @param[in]  	p_SwitchFlag    флаг переключения контекста (1 - до копирования, 2 - после копирования), 
                                если задан, то производит переименование таблицы p_SrcTable в таблицу '_OLD'
   @param[in]  	p_FixFlag     	флаг фиксации результата
   @param[out]	p_Expected   	Кол-во ожидаемых записей
   @param[out]	p_Processed   	Кол-во обработанных записей
  */
  FUNCTION ExecProcess ( 
    p_CalcID IN VARCHAR2 
    , p_SrcTable IN varchar2
    , p_DstTable IN varchar2
    , p_Limit IN number
    , p_SwitchFlag IN number
    , p_FixFlag IN number
    , p_Expected OUT number
    , p_Processed OUT number
  ) 
  RETURN NUMBER;

  /**
   @brief    Инициализация параллельного процесса.
             Параллельный процесс используется как для генерации заданий, так и для копирования данных
             Возвращает ID расчета (itt_parallel_exec.calc_id)
  */
  FUNCTION InitProcess RETURN varchar2;

  /**
   @brief    Процедура отката процесса трансформации
   @param[in]  	p_SwitchFlag    флаг переключения контекста (1 - до копирования, 2 - после копирования), 
                                если задан, то производит переименование таблицы p_SrcTable в таблицу '_OLD'
   @param[in]  	p_SrcTable     	имя исходной таблицы
   @param[in]  	p_DstTable     	имя таблицы-результата
  */
  PROCEDURE RollbackProcess ( 
    p_SwitchFlag IN number
    , p_SrcTable IN varchar2
    , p_DstTable IN varchar2 
  );

END OPRDOCS_utl;
/
