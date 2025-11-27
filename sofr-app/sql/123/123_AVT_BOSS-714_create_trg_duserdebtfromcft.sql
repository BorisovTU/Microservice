CREATE OR REPLACE TRIGGER duserdebtfromcft_dbt_t0_ainc
  BEFORE INSERT OR UPDATE OF t_id ON duserdebtfromcft_dbt FOR EACH ROW
DECLARE
  v_id INTEGER;
BEGIN
  IF (:NEW.t_id = 0 OR :NEW.t_id IS NULL) THEN
    SELECT duserdebtfromcft_dbt_seq.NEXTVAL INTO :NEW.t_id FROM DUAL;
  ELSE
    SELECT LAST_NUMBER INTO v_id FROM USER_SEQUENCES WHERE UPPER(SEQUENCE_NAME) = UPPER('duserdebtfromcft_dbt_seq');
    IF :NEW.t_id >= v_id THEN
      RAISE DUP_VAL_ON_INDEX;
    END IF;
  END IF;
END;
/

CREATE OR REPLACE TRIGGER duserdebtfromcfthist_dbt_t0_ainc
  BEFORE INSERT OR UPDATE OF t_id ON duserdebtfromcfthist_dbt FOR EACH ROW
DECLARE
  v_id INTEGER;
BEGIN
  IF (:NEW.t_id = 0 OR :NEW.t_id IS NULL) THEN
    SELECT duserdebtfromcfthist_dbt_seq.NEXTVAL INTO :NEW.t_id FROM DUAL;
  ELSE
    SELECT LAST_NUMBER INTO v_id FROM USER_SEQUENCES WHERE UPPER(SEQUENCE_NAME) = UPPER('duserdebtfromcfthist_dbt_seq');
    IF :NEW.t_id >= v_id THEN
      RAISE DUP_VAL_ON_INDEX;
    END IF;
  END IF;
END;
/


































































