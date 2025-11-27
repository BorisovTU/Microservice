CREATE OR REPLACE TRIGGER dfmrjfailurecode_tmp_t0_ainc
 BEFORE INSERT OR UPDATE OF t_ID ON DFMRJFAILURECODE_TMP FOR EACH ROW
DECLARE
 v_id INTEGER;
BEGIN
 IF (:new.t_ID = 0 OR :new.t_ID IS NULL) THEN
 SELECT dfmrjfailurecode_dbt_seq.nextval INTO :new.t_ID FROM dual;
 ELSE
 select last_number into v_id from user_sequences where sequence_name = upper ('dfmrjfailurecode_dbt_SEQ');
 IF :new.t_ID >= v_id THEN
 RAISE DUP_VAL_ON_INDEX;
 END IF;
 END IF;
END;
/
