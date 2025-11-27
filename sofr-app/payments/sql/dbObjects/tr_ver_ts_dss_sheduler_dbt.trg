CREATE OR REPLACE TRIGGER TR_VER_TS_DSS_SHEDULER_DBT
BEFORE INSERT OR UPDATE
ON DSS_SHEDULER_DBT FOR EACH ROW
DECLARE
  l_date DATE;
BEGIN
  IF INSERTING THEN
    IF :new.T_ID IS NULL THEN
      SELECT DSS_SHEDULER_DBT_SEQ.NEXTVAL INTO :new.T_ID FROM dual;
    END IF;
    :new.VERSIONREC := 1;
  ELSIF UPDATING THEN
    IF :old.VERSIONREC = :new.VERSIONREC THEN
      :new.VERSIONREC := :old.VERSIONREC + 1;
    END IF;
  END IF;
  
  :new.TS := localtimestamp;
  
  IF :old.T_PERIODTYPE != :new.T_PERIODTYPE
    OR :old.T_PERIOD != :new.T_PERIOD
    OR :old.T_WORKSTARTTIME != :new.T_WORKSTARTTIME
    OR :old.T_WORKENDTIME != :new.T_WORKENDTIME
    OR :old.T_STARTTIME != :new.T_STARTTIME 
    OR :old.T_WORKTIMETYPE != :new.T_WORKTIMETYPE THEN
      l_date := TRUNC(SYSDATE, 'MI') + (INTERVAL '1' MINUTE);
      BEGIN
        :new.T_NEXTSTAMP := RSP_SCHEDULE.GetNearestExecutableDateByParam(p_calendarID => 2,
                                                                         p_isWorkDay => CASE WHEN :new.T_WORKTIMETYPE = 2 THEN 1 ELSE 0 END,
                                                                         p_workStartTime => :new.T_WORKSTARTTIME,
                                                                         p_workEndTime => :new.T_WORKENDTIME,
                                                                         p_startTime => :new.T_STARTTIME,
                                                                         p_dateTime => l_date);
      EXCEPTION WHEN OTHERS THEN
        :new.T_NEXTSTAMP := l_date;
      END;
  END IF;
END;
/