CREATE OR REPLACE PACKAGE rshb_calendar IS

 /**

  -- Author  : Велигжанин А.В.
  -- Created : 05.06.2024 17:55
  -- Purpose : утилиты работы с календарями

# changelog
 |date       |autor          |tasks                                           |note
 |-----------|---------------|------------------------------------------------|-------------------------------------------------------------
 |05.06.2024 |Велигжанин А.В.|DEF-65531                                       |Создан

  */

  /**
   @brief    Процедура сброса (или установки) рабочего дня.
   @param[in]    p_CalenID    	        ID календаря
   @param[in]    p_Date    	        Дата
   @param[in]    p_Set    	        Флаг: если 0, рабочий день сбрасывается, иначе устанавливается
  */
  PROCEDURE SetWorkDay( p_CalenID IN number, p_Date IN DATE, p_Set IN NUMBER );

END rshb_calendar;
/
