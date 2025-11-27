CREATE OR REPLACE PACKAGE BODY RSP_SCHEDULE
IS
  /**
  * Получить ближайшую к p_datetime дату, в которую возможно выполнение выбранной плановой процедуры
  * @param p_calendarID ID календаря, по которому определяются рабочие дни
  * @param p_isWorkDay Выполнение только по рабочим дням (0 - нет, 1 - да)
  * @param p_workStartTime Время начала выполнения
  * @param p_workEndTime Время окончания выполнения
  * @param p_startTime Время старта в сутках
  * @param p_datetime Дата, с которой производится поиск
  * @return Ближайшая к p_datetime дата, в которую возможно выполнение плановой процедуры
  */
  FUNCTION GetNearestExecutableDateByParam(p_calendarID IN NUMBER,
                                           p_isWorkDay IN NUMBER,
                                           p_workStartTime IN DATE,
                                           p_workEndTime IN DATE,
                                           p_startTime IN DATE,
                                           p_datetime IN DATE) RETURN DATE
  AS
    l_date DATE;
    l_startDatetime DATE;
    l_startDT DATE;
    l_endDatetime DATE;
    l_endDT DATE;
    l_startTime DATE;
    l_startTM DATE;
    l_isWorkDay NUMBER(1);
    l_nearestDatetime DATE;
    l_isFound BOOLEAN;
  BEGIN
    l_date := TRUNC(p_datetime, 'DDD');

    SELECT NVL(p_workStartTime, l_date),
           NVL(p_workEndTime, l_date + 1 - NUMTODSINTERVAL(1, 'SECOND')),
           NVL(p_startTime, l_date),
           NVL(p_isWorkDay, 0)
      INTO l_startDT,
           l_endDT,
           l_startTM,
           l_isWorkDay
      FROM DUAL;

    l_isFound := FALSE;

    LOOP
      IF l_isWorkDay = 1 THEN
        l_nearestDatetime := RSP_Calendar.GetWorkday(p_calendarID, l_date, 1, 0);
      ELSE
        l_nearestDatetime := l_date;
      END IF;

      l_startDatetime := l_nearestDatetime + (l_startDT - TRUNC(l_startDT, 'DDD'));
      l_endDatetime := l_nearestDatetime + (l_endDT - TRUNC(l_endDT, 'DDD'));
      l_startTime := l_nearestDatetime + (l_startTM - TRUNC(l_startTM, 'DDD'));

      IF l_date = l_nearestDatetime THEN
        l_nearestDatetime := p_datetime;
      END IF;

      IF l_startDatetime <= l_endDatetime THEN
        IF l_startTime >= l_startDatetime THEN
          l_startDatetime := LEAST(l_startTime, l_endDatetime);
        END IF;

        IF l_nearestDatetime < l_startDatetime THEN
          l_nearestDatetime := l_startDatetime;
        END IF;

        IF l_nearestDatetime BETWEEN l_startDatetime AND l_endDatetime THEN
          l_isFound := TRUE;
        END IF;
      ELSE
       IF l_nearestDatetime <= l_endDatetime AND l_startTime <= l_endDatetime THEN
          l_nearestDatetime := GREATEST(l_startTime, l_nearestDatetime);
        ELSE
          IF l_startTime > l_startDatetime THEN
            l_startDatetime := l_startTime;
          END IF;

          l_nearestDatetime := GREATEST(l_startDatetime, l_nearestDatetime);
        END IF;

        l_isFound := TRUE;
      END IF;

      EXIT WHEN l_isFound;

      l_date := TRUNC(l_nearestDatetime, 'DDD') + 1;
    END LOOP;

    RETURN l_nearestDatetime;
  END GetNearestExecutableDateByParam;

END;
/