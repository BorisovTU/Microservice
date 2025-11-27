-- Изменения по BOSS-771, BOSS-1410
-- 1) изменить наименование категории
-- 2) добавить запись в справочник DDL_LIMITPRM_DBT
DECLARE
  logID VARCHAR2(32) := 'BOSS-771, BOSS-1410';
  -- Записать сообщение в itt_log
  PROCEDURE LogIt(p_message VARCHAR2)
  AS
  BEGIN
    it_log.log(p_msg => logID||': '||p_message);
  END;
  -- Процедура изменения наименования категории
  PROCEDURE updateObjGroup( 
     p_ObjType IN dobjgroup_dbt.t_objecttype%TYPE
     , p_GroupID IN dobjgroup_dbt.t_groupid%TYPE
     , p_Name IN dobjgroup_dbt.t_name%TYPE
  )
  IS
  BEGIN
    -- изменение наименования категории
    LogIt('изменение наименования категории ');
    UPDATE dobjgroup_dbt r 
    SET r.t_name = p_Name
    WHERE r.t_objecttype = p_ObjType and r.t_groupid = p_GroupID;
    EXECUTE IMMEDIATE 'COMMIT';
    LogIt('изменено наименование категории');
  EXCEPTION
    WHEN others THEN 
      LogIt('ошибка изменения наименования категории');
      EXECUTE IMMEDIATE 'ROLLBACK';
  END;
  -- добавление записи в справочник DDL_LIMITPRM_DBT
  PROCEDURE addLimitPrm
  IS
  BEGIN
    LogIt('Добавление записи в справочник DDL_LIMITPRM_DBT');
    INSERT INTO DDL_LIMITPRM_DBT (
       T_ID, T_MARKETKIND, T_MARKETID, T_FIRMCODE, T_POSCODE, T_ISCORRECT, T_ISDEPO, T_DEPOACC
       , T_KINDLARGERTWO, T_CALENDARID, T_CURID, T_CURLIMITKIND
       , T_CODESCZEROLIMIT, T_CORDIVERG, T_IMPLKIND
    ) VALUES (
       0
       , 3                        -- валютный рынок
       , 2                        -- биржа = ММВБ 
       , 'MC0134700000'
       , 'EQTV'
       , CHR(0), CHR(0)
       , 'MB0134725834'
       , CHR(0)
       , -1                       -- T_CALENDARID
       , 0                        -- T_CURID = НацВ
       , 1                        -- T_CURLIMITKIND 
       , chr(1)                   -- T_CODESCZEROLIMIT
       , 0
       , 2 			  -- Принадлежность ТКС = "Для клиентов 2-го типа"
    );
    EXECUTE IMMEDIATE 'COMMIT';
    LogIt('Добавлена запись в справочник DDL_LIMITPRM_DBT');
  EXCEPTION
   WHEN OTHERS THEN 
      LogIt('ошибка добавления записи в справочник DDL_LIMITPRM_DBT');
      EXECUTE IMMEDIATE 'ROLLBACK';
  END;
BEGIN
  -- 1) изменение наименования категории
  updateObjGroup(659, 6, 'Предоставлять брокеру право использования активов в его интересах');
  -- 2) добавление запись в справочник DDL_LIMITPRM_DBT
  addLimitPrm();
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
END;
/
