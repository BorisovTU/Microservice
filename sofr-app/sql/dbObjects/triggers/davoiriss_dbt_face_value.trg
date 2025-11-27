CREATE OR REPLACE TRIGGER "DAVOIRISS_DBT_FACE_VALUE"
AFTER  INSERT OR UPDATE  ON DAVOIRISS_DBT REFERENCING NEW AS NEW OLD AS OLD FOR EACH ROW
DECLARE

BEGIN

  IF :NEW.T_FIID IS NOT NULL AND :NEW.T_INDEXNOM = 'X'
  THEN
     IF IT_RUDATA.getOffRudataByFiid(:NEW.T_FIID) = 0 then
       it_log.log(p_msg => 'DAVOIRISS_DBT_FACE_VALUE - trigger.', p_msg_type => it_log.C_MSG_TYPE__DEBUG);
       IT_RUDATA.DateOptionsTableWrapper_Rq( p_instr_key => nvl(:NEW.t_isin,:NEW.t_lsin ) );
     END IF;
  END IF;
  
EXCEPTION
  WHEN OTHERS
  THEN
    it_error.put_error_in_stack;
    it_log.log(p_msg => 'Error avoiriss fiid = '||:NEW.T_FIID, p_msg_type => it_log.c_msg_type__error);
    it_error.clear_error_stack;
END;

/