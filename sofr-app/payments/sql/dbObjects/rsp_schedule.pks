CREATE OR REPLACE PACKAGE RSP_SCHEDULE
IS
  -- Получить ближайшую к p_datetime дату, в которую возможно выполнение плановой процедуры
  FUNCTION GetNearestExecutableDateByParam(p_calendarID IN NUMBER,
                                           p_isWorkDay IN NUMBER,
                                           p_workStartTime IN DATE,
                                           p_workEndTime IN DATE,
                                           p_startTime IN DATE,
                                           p_datetime IN DATE) RETURN DATE;
END;
/
