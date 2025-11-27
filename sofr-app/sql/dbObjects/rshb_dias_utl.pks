CREATE OR REPLACE PACKAGE rshb_dias_utl
AS
  /**
   @file 		rshb_dias_utl.pks
   @brief 		Утилиты для трансформации данных таблицы SOFR_DIASDEPORESTFULL (по DEF-61499)
     
   # changeLog
   |date       |author         |tasks                                                     |note                                                        
   |-----------|---------------|----------------------------------------------------------|-------------------------------------------------------------
   |2024.02.15 |Велигжанин А.В.|DEF-61499                                                 | Создание                  
    
  */

  /**
   @brief    Процедура переноса данных для одного дня
  */
  PROCEDURE MoveOneDate ( p_ChunkID IN NUMBER, p_Date IN date );

  /**
   @brief    Нитка параллельного выполнения.
  */
  PROCEDURE MoveDiasRestChunk ( p_StartID IN NUMBER, p_EndID IN NUMBER );

  /**
   @brief    Перенос данных из таблицы SOFR_DIASDEPORESTFULL в DDIASRESTDEPO_DBT.
             В исходной таблице (SOFR_DIASDEPORESTFULL) содержатся избыточные данные.
             При переносе данных, производится перенос только последних значений за дату.
             Так как таблица SOFR_DIASDEPORESTFULL -- большого размера (~14 Gb на 13-02-2023),
             перенос данных осуществляется параллельно несколькими потоками.
  */
  PROCEDURE MoveDiasRest ( p_Parallel IN NUMBER DEFAULT 4 );

  /**
   @brief    Процедура переключения представлений.
   @param    p_Type	Вид представления: 0 -- старый, 1 -- новый
  */
  PROCEDURE SwitchView ( p_Type IN NUMBER );

  /**
   @brief    Фиксация доработки. Выполняет переключение на новые структуры данных и удаляет старые таблицы.
  */
  PROCEDURE FixView ( p_Ret IN OUT NUMBER );

END rshb_dias_utl;
/
