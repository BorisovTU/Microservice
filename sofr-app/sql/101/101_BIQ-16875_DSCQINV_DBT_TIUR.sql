-- Триггер
CREATE OR REPLACE TRIGGER "DSCQINV_DBT_TIUR"
   BEFORE UPDATE OF T_CHANGEDATE ON DSCQINV_DBT
   FOR EACH ROW
DECLARE
   v_OldDate DATE;
   v_NewDate DATE;
BEGIN

  IF( :OLD.T_CHANGEDATE != TO_DATE( '01.01.0001', 'dd.mm.yyyy' ) ) THEN
     v_OldDate := :OLD.T_CHANGEDATE;
  ELSE
     v_OldDate := :OLD.T_REGDATE;
  END IF;

  IF( :NEW.T_CHANGEDATE != TO_DATE( '01.01.0001', 'dd.mm.yyyy' ) ) THEN
     v_NewDate := :NEW.T_CHANGEDATE;
  ELSE
     v_NewDate := :NEW.T_REGDATE;
  END IF;

  IF( v_OldDate > v_NewDate ) THEN
     RAISE_APPLICATION_ERROR(-20201,''); --Нарушена последовательность выполнения операций
  END IF;
  
  IF (:OLD.t_state = :NEW.t_state AND :OLD.t_state = 1 AND :OLD.t_controlDate < :NEW.t_controlDate) THEN
    -- Пролонгация
    INSERT INTO DSCQINVH_DBT
      ( T_PARTYID, T_KIND, T_STATE, T_BEGDATE, T_ENDDATE, T_OPER, T_SYSDATE, T_SYSTIME, T_CAUSE, T_CODE, T_CONTROLDATE )
    VALUES
      ( :NEW.T_PARTYID, :NEW.T_KIND, :OLD.T_STATE, v_OldDate, :NEW.T_CHANGEDATE - 1, :OLD.T_OPER, :OLD.T_SYSDATE, :OLD.T_SYSTIME, :OLD.T_CAUSE, :OLD.T_CODE, :OLD.T_CONTROLDATE );
  END IF;

END DSCQINV_DBT_TIUR;
/