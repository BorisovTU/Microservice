CREATE OR REPLACE TRIGGER DPARTY_DBT_CDI_UPDATE
  AFTER UPDATE
  ON dparty_dbt
  FOR EACH ROW
DECLARE
  v_retval CHAR;
  v_cnt number := 0;
  C_CDI_UPDATEORG constant number := 5055;
  C_STATUS_WAIT constant number := 11;
  C_STATUS_READY constant number := 1;
BEGIN
  v_retval := RSB_COMMON.GetRegFlagValue( 'РСХБ\Интеграция\Взаимодействие с AC CDI\АКТИВНО');

  IF (v_retval = chr(88))
  THEN 
    SELECT count(1) INTO v_cnt
      FROM utableprocessevent_dbt 
     WHERE t_objecttype = C_CDI_UPDATEORG 
       AND t_objectid = :NEW.t_partyid
       AND t_status = C_STATUS_WAIT;
    
    IF v_cnt > 0 then
      /*событие должно быть только одно, но не будем мелочиться*/
      UPDATE utableprocessevent_dbt 
         SET t_status = C_STATUS_READY,
             t_timestamp = sysdate, 
             t_lastupdate = sysdate
       WHERE t_objecttype = C_CDI_UPDATEORG 
         AND t_objectid = :NEW.t_partyid
         AND t_status = C_STATUS_WAIT;
    END IF;
  END IF;
END;
/