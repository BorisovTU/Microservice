CREATE OR REPLACE TRIGGER dfmrjsrvpersfndr_tmp_t0_ainc
 BEFORE INSERT OR UPDATE OF t_ID ON dfmrjsrvpersfndr_tmp FOR EACH ROW
DECLARE
 v_id INTEGER;
BEGIN
 IF (:new.t_ID = 0 OR :new.t_ID IS NULL) THEN
 SELECT dfmrjsrvpersfndr_dbt_seq.nextval INTO :new.t_ID FROM dual;
 ELSE
 select last_number into v_id from user_sequences where sequence_name = upper ('dfmrjsrvpersfndr_dbt_SEQ');
 IF :new.t_ID >= v_id THEN
 RAISE DUP_VAL_ON_INDEX;
 END IF;
 END IF;
END;
/
