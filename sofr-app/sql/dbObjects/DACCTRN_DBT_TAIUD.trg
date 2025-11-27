CREATE OR REPLACE TRIGGER "DACCTRN_DBT_TAIUD"
  AFTER INSERT OR UPDATE OR DELETE
  ON dacctrn_dbt
DECLARE
  dml_errors EXCEPTION;
  PRAGMA EXCEPTION_INIT (dml_errors, -24381);
  
  PROCEDURE SaveErrors(p_info VARCHAR2) IS
  BEGIN
      FOR idx IN 1 .. SQL%BULK_EXCEPTIONS.COUNT
      LOOP
        it_log.log_handle(p_object => 'DACCTRN_DBT'
                         ,p_msg => p_info || '. Žè¨¡ª : ' || SQLERRM(-SQL%BULK_EXCEPTIONS(idx).ERROR_CODE)
                         ,p_msg_type => it_log.C_MSG_TYPE__ERROR);
      END LOOP;
  END;
BEGIN
  -- „«ï dacctrn_dbt_pickupdel
  IF trgpckg_dacctrn_dbt.v_upickupdel.count > 0 THEN
    BEGIN
      FORALL idx IN INDICES OF trgpckg_dacctrn_dbt.v_upickupdel SAVE EXCEPTIONS
        INSERT INTO uPickupDel_dbt VALUES trgpckg_dacctrn_dbt.v_upickupdel(idx);
      
    EXCEPTION WHEN dml_errors THEN
        SaveErrors('dacctrn_dbt_pickupdel insert');
    END;
    
    trgpckg_dacctrn_dbt.v_upickupdel.delete();
  END IF;
  
  -- „«ï dacctrn_dbt_delete
  IF trgpckg_dacctrn_dbt.v_tableprocessevent.count > 0 THEN
    BEGIN
      FORALL idx IN INDICES OF trgpckg_dacctrn_dbt.v_tableprocessevent SAVE EXCEPTIONS
        UPDATE uTableProcessEvent_dbt
           SET T_STATUS    = trgpckg_dacctrn_dbt.v_tableprocessevent(idx).T_STATUS,
               T_TIMESTAMP = trgpckg_dacctrn_dbt.v_tableprocessevent(idx).T_TIMESTAMP
         WHERE T_RECID = trgpckg_dacctrn_dbt.v_tableprocessevent(idx).T_RECID;
       
    EXCEPTION WHEN dml_errors THEN
        SaveErrors('dacctrn_dbt_delete update');
    END;
    
    trgpckg_dacctrn_dbt.v_tableprocessevent.delete();
  END IF;
  
  -- „«ï dacctrn_dbt_synh
  IF trgpckg_dacctrn_dbt.v_tableprocessevent_ins.count > 0 THEN
    BEGIN
      FORALL idx IN INDICES OF trgpckg_dacctrn_dbt.v_tableprocessevent_ins SAVE EXCEPTIONS
        INSERT INTO uTableProcessEvent_dbt VALUES trgpckg_dacctrn_dbt.v_tableprocessevent_ins(idx);
      
    EXCEPTION WHEN dml_errors THEN
        SaveErrors('dacctrn_dbt_synh insert');
    END;
    
    trgpckg_dacctrn_dbt.v_tableprocessevent_ins.delete();
  END IF;
  
  IF trgpckg_dacctrn_dbt.v_tableprocessevent_upd.count > 0 THEN
    BEGIN
      FORALL idx IN INDICES OF trgpckg_dacctrn_dbt.v_tableprocessevent_upd SAVE EXCEPTIONS
        UPDATE uTableProcessEvent_dbt
           SET T_STATUS    = trgpckg_dacctrn_dbt.v_tableprocessevent_upd(idx).T_STATUS,
               T_TIMESTAMP = trgpckg_dacctrn_dbt.v_tableprocessevent_upd(idx).T_TIMESTAMP,
               T_NOTE = CASE WHEN trgpckg_dacctrn_dbt.v_tableprocessevent_upd(idx).T_NOTE IS NULL 
                             THEN T_NOTE
                             ELSE trgpckg_dacctrn_dbt.v_tableprocessevent_upd(idx).T_NOTE END
         WHERE T_RECID = trgpckg_dacctrn_dbt.v_tableprocessevent_upd(idx).T_RECID;
       
    EXCEPTION WHEN dml_errors THEN
        SaveErrors('dacctrn_dbt_synh update');
    END;
    
    trgpckg_dacctrn_dbt.v_tableprocessevent_upd.delete();
  END IF;
  
EXCEPTION WHEN others THEN
  it_log.log_handle(p_object => 'DACCTRN_DBT'
                   ,p_msg => 'DACCTRN_DBT_TAIUD. Žè¨¡ª : ' || SQLERRM
                   ,p_msg_type => it_log.C_MSG_TYPE__ERROR);
END DACCTRN_DBT_TAIUD;
/