-- Изменения по DEF-56486, Пересборка индекса DPMPAYM_DBT_IDX1 (должен быть по T_DOCKIND, T_DOCUMENTID, T_PURPOSE, T_SUBPURPOSE)
DECLARE
  logID VARCHAR2(9) := 'DEF-56486';
  -- Записать сообщение в itt_log
  PROCEDURE LogIt( p_message VARCHAR2 )
  AS
  BEGIN
    it_log.log(p_msg => logID||': '||p_message);
  END;
  -- Пересборка индекса DPMPAYM_DBT_IDX1
  PROCEDURE AlterDPMPAYM_DBT_IDX1
  AS
    x_Cnt NUMBER;
  BEGIN
    SELECT count(*) 
      INTO x_Cnt 
      FROM user_indexes r 
      WHERE r.TABLE_NAME='DPMPAYM_DBT' 
      AND r.INDEX_NAME='DPMPAYM_DBT_IDX1' 
    ;
    IF x_Cnt = 1 THEN
      LogIt('Удаление индекса DPMPAYM_DBT_IDX1');
      EXECUTE IMMEDIATE 'DROP INDEX DPMPAYM_DBT_IDX1';
      LogIt('Индекс DPMPAYM_DBT_IDX1 удален');
    END IF;
    LogIt('Создание индекса DPMPAYM_DBT_IDX1');
    EXECUTE IMMEDIATE 'CREATE INDEX DPMPAYM_DBT_IDX1 ON DPMPAYM_DBT ( T_DOCKIND, T_DOCUMENTID, T_PURPOSE, T_SUBPURPOSE )';
    LogIt('Индекс DPMPAYM_DBT_IDX1 создан');
  EXCEPTION
    WHEN OTHERS THEN
       LogIt('Ошибка модификации индекса DPMPAYM_DBT_IDX1');
  END;
BEGIN
  -- Пересборка индекса DPMPAYM_DBT_IDX1
  AlterDPMPAYM_DBT_IDX1();
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
END;
/
