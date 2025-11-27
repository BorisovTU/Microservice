
CREATE OR REPLACE TRIGGER DNPTXIISSETTING_DBT_T1_HIS
    BEFORE INSERT OR UPDATE
    ON DNPTXIISSETTING_DBT
    FOR EACH ROW
BEGIN
    IF INSERTING THEN
        INSERT INTO DNPTXIISHISTORY_DBT (T_ID,
                                         T_IISID,
                                         T_MINPERIOD1,
                                         T_MINPERIOD2,
                                         T_OPER,
                                         T_RECDATE,
                                         T_TAXPERIOD1,
                                         T_TAXPERIOD2)
             VALUES (0,
                     :NEW.T_ID,
                     0,
                     :NEW.T_MINPERIOD,
                     :NEW.T_OPER,
                     :NEW.T_RECDATE,
                     0,
                     :NEW.T_TAXPERIOD);
    ELSIF UPDATING THEN
        INSERT INTO DNPTXIISHISTORY_DBT (T_ID,
                                         T_IISID,
                                         T_MINPERIOD1,
                                         T_MINPERIOD2,
                                         T_OPER,
                                         T_RECDATE,
                                         T_TAXPERIOD1,
                                         T_TAXPERIOD2)
             VALUES (0,
                     :NEW.T_ID,
                     :OLD.T_MINPERIOD,
                     :NEW.T_MINPERIOD,
                     :NEW.T_OPER,
                     :NEW.T_RECDATE,
                     :OLD.T_TAXPERIOD,
                     :NEW.T_TAXPERIOD);
    END IF;
END DNPTXIISSETTING_DBT_T1_HIS;
/
