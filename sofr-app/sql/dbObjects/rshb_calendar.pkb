CREATE OR REPLACE PACKAGE BODY rshb_calendar IS

  --Encoding: Win 866

  /**
   @file    	rshb_calendar.pkb
   @brief       утилиты работы с календарями
  */

  /**
   @brief    Процедура сброса (или установки) рабочего дня.
   @param[in]    p_CalenID    	        ID календаря
   @param[in]    p_Date    	        Дата
   @param[in]    p_Set    	        Флаг: если 0, рабочий день сбрасывается, иначе устанавливается
  */
  PROCEDURE SetWorkDay( p_CalenID IN number, p_Date IN DATE, p_Set IN NUMBER )
  IS
    x_calenDayOld VARCHAR2(4000);
    x_calenDayNew VARCHAR2(4000);
    x_YearNumberDate NUMBER;
    x_Date DATE;
    x_Year NUMBER;
    x_Pos NUMBER;
    x_Balance VARCHAR2(2);
    x_NewBalance VARCHAR2(2);
  BEGIN
    x_Date := trunc(p_Date);
    x_Year := to_number(to_char(x_Date, 'YYYY'));
    SELECT RAWTOHEX(r.t_calendays) INTO x_calenDayOld
      FROM dcalendar_dbt r WHERE r.t_id = p_CalenID and r.t_year = x_Year;
    x_YearNumberDate := x_Date - trunc(x_Date, 'YEAR' );
    x_Pos := x_YearNumberDate * 6 + 2;
    x_Balance := SUBSTR(x_calenDayOld, x_Pos+1, 2);
    IF(p_Set = 0) THEN
      x_NewBalance := '00';
    ELSE
      x_NewBalance := '01';
    END IF;
    IF(x_Balance <> x_NewBalance) THEN
      IF(x_Pos > 0) THEN
         x_calenDayNew := SUBSTR(x_calenDayOld, 1, x_Pos);
      END IF;
      x_calenDayNew := x_calenDayNew || x_NewBalance || SUBSTR(x_calenDayOld, x_Pos + 3);
      UPDATE dcalendar_dbt r 
        SET r.t_calendays = HEXTORAW (x_calenDayNew)
        WHERE r.t_id = p_CalenID and r.t_year = x_Year
        ;
      COMMIT;
    END IF;
  EXCEPTION
    WHEN others THEN 
       NULL;
  END SetWorkDay;

END rshb_calendar;
/
