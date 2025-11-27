-- Перестроение триггера
CREATE OR REPLACE TRIGGER "DSCDLJR_TMP_T1_AINC"
 BEFORE INSERT OR UPDATE OF t_autoinc ON dscdljr_tmp FOR EACH ROW
DECLARE
 v_id INTEGER;
BEGIN
 IF (:new.t_autoinc = 0 OR :new.t_autoinc IS NULL) THEN
   SELECT dscdljr_tmp_seq.nextval INTO :new.t_autoinc FROM dual;
 ELSE
   IF :new.t_autoinc >= dscdljr_tmp_seq.currval THEN 
     RAISE DUP_VAL_ON_INDEX;
   END IF;
 END IF;
END;
/
