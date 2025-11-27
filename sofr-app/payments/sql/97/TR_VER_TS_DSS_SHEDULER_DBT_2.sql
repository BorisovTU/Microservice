create or replace TRIGGER TR_VER_TS_DSS_SHEDULER_DBT
before insert or update
on DSS_SHEDULER_DBT for each row
begin
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
  
  IF :old.T_NEXTSTAMP is NULL or (:old.T_PERIODTYPE != :new.T_PERIODTYPE) or (:old.T_PERIOD != :new.T_PERIOD) THEN
    IF :new.T_PERIODTYPE = 1 THEN 
        :new.T_NEXTSTAMP := sysdate + (INTERVAL '1' second) * :new.T_PERIOD;
    ELSIF :new.T_PERIODTYPE = 2 THEN 
        :new.T_NEXTSTAMP := sysdate + (INTERVAL '1' minute) * :new.T_PERIOD;
    ELSIF :new.T_PERIODTYPE = 3 THEN 
        :new.T_NEXTSTAMP := sysdate + (INTERVAL '1' hour) * :new.T_PERIOD;
    ELSIF :new.T_PERIODTYPE = 4 THEN 
        :new.T_NEXTSTAMP := sysdate + (INTERVAL '1' day) * :new.T_PERIOD;
    ELSIF :new.T_PERIODTYPE = 5 THEN 
        :new.T_NEXTSTAMP := case when ((sysdate+:new.T_PERIOD-1) - trunc((sysdate+:new.T_PERIOD-1),'IW')) between 4 and 6
                                 then trunc((sysdate+2+:new.T_PERIOD),'IW')
                                 else sysdate+:new.T_PERIOD
                            end;
    ELSIF :new.T_PERIODTYPE = 6 THEN 
        :new.T_NEXTSTAMP := sysdate + (INTERVAL '7' day) * :new.T_PERIOD;
    ELSIF :new.T_PERIODTYPE = 7 THEN 
        :new.T_NEXTSTAMP := sysdate + (INTERVAL '1' month) * :new.T_PERIOD;
    ELSIF :new.T_PERIODTYPE = 8 THEN 
        :new.T_NEXTSTAMP := sysdate + (INTERVAL '3' month) * :new.T_PERIOD;
    ELSIF :new.T_PERIODTYPE = 9 THEN 
        :new.T_NEXTSTAMP := sysdate + (INTERVAL '1' year) * :new.T_PERIOD;
    ELSE
        :new.T_NEXTSTAMP := sysdate;
    END IF;
  --ELSE
    --:new.T_NEXTSTAMP := :old.T_NEXTSTAMP;
  END IF;
  
end;
