CREATE OR REPLACE TRIGGER DNPTXTEMPLATEOKZ_DBT_T0_AINC
  BEFORE INSERT OR UPDATE OF T_ID ON DNPTXTEMPLATEOKZ_DBT FOR EACH ROW
DECLARE
  v_id INTEGER;
BEGIN
  IF (:NEW.T_ID = 0 OR :NEW.T_ID IS NULL) THEN
    SELECT DNPTXTEMPLATEOKZ_DBT_SEQ.NEXTVAL INTO :NEW.T_ID FROM DUAL;
  ELSE
    SELECT LAST_NUMBER INTO v_id FROM USER_SEQUENCES WHERE UPPER(SEQUENCE_NAME) = UPPER('DNPTXTEMPLATEOKZ_DBT_SEQ');
    IF :NEW.T_ID >= v_id THEN
      RAISE DUP_VAL_ON_INDEX;
    END IF;
  END IF;
END;
/

CREATE OR REPLACE TRIGGER DNPTXTEMPLATEOKZ_DBT_HIS
  AFTER INSERT OR UPDATE ON DNPTXTEMPLATEOKZ_DBT FOR EACH ROW
DECLARE
BEGIN
  INSERT INTO dnptxtemplateokzhis_dbt (t_id, t_oper, t_recdate, t_oldenddate, t_newenddate,
                                       t_oldsofr,t_newsofr,t_olddepo,t_newdepo,t_oldkbk,t_newkbk) 
                               values (:NEW.t_id, :NEW.t_oper, :NEW.t_recdate, NVL(:OLD.t_enddate, TO_DATE('01010001','DDMMYYYY')), :NEW.t_enddate,
                                       NVL(:OLD.t_sofr, chr(0)), :NEW.t_sofr, NVL(:OLD.t_depo, chr(0)), :NEW.t_depo, NVL(:OLD.t_kbk, chr(1)), :NEW.t_kbk
                                       );
END;
/

CREATE OR REPLACE TRIGGER DNPTXTEMPLATEOKZ_DBT_T1
   BEFORE INSERT OR UPDATE
   ON DNPTXTEMPLATEOKZ_DBT
   FOR EACH ROW
DECLARE
   v_cnt   INTEGER;
BEGIN
   SELECT COUNT (1)
     INTO v_cnt
     FROM DNPTXTEMPLATEOKZ_DBT OKZ
    WHERE     :new.T_ENDDATE = OKZ.T_ENDDATE
          AND :new.T_BEGDATE = OKZ.T_BEGDATE
          AND :new.T_ISRESIDENT = OKZ.T_ISRESIDENT
          AND :new.T_KBK = OKZ.T_KBK
          AND :new.T_RATE = OKZ.T_RATE
          AND :new.T_TYPENOB = OKZ.T_TYPENOB
          AND 1 = CASE WHEN :new.T_SOFR = CHR (88) 
                                 THEN case when :new.t_sofr = okz.t_sofr then 1 else 0 end 
                                  ELSE 1 END
          AND 1 = CASE WHEN :new.T_DEPO = CHR (88) 
                                 THEN case when :new.t_depo = okz.t_depo then 1 else 0 end 
                                 ELSE 1 END;

   IF (v_cnt > 0)
   THEN
      RAISE DUP_VAL_ON_INDEX;
   END IF;
END;
/