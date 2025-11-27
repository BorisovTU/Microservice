CREATE OR REPLACE TRIGGER "DFUNCOBJ_DBT_FNS_HIST_U" 
  BEFORE UPDATE ON dfuncobj_dbt
  FOR EACH ROW
DECLARE
  HISTID INTEGER;
BEGIN
  UPDATE DFUNCOBJ_HIST_DBT h
     SET h.t_updateddate = SYSDATE, h.t_sessionid = :new.t_sessionid
   WHERE h.t_funcobjid = :new.t_id
   and h.t_createdate = :new.t_createdate;
   
  SELECT T_ID INTO HISTID from DFUNCOBJ_HIST_DBT
  WHERE t_funcobjid = :new.t_id
   and t_createdate = :new.t_createdate
   and ROWNUM < 2;
  
  IF HISTID IS NOT NULL THEN
    IF :old.t_param <> :new.t_param OR :old.t_priority <> :new.t_priority THEN
      INSERT INTO DFUNCOBJ_HISTPARM_DBT
        (T_HISTID, T_PARAM, T_STATE, T_PRIORITY, T_ACTIONTEXT)
      VALUES
        (HISTID,
         :new.T_PARAM,
         :new.T_STATE,
         :new.T_PRIORITY,
         'Обновление полей: ' || CASE
           WHEN :old.t_param <> :new.t_param THEN
            'Параметры вызова'
           ELSE -- :old.t_priority <> :new.t_priority
            'Приоритет орбаботки'
         END);
    END IF;
    IF :old.t_state <> :new.t_state THEN
      INSERT INTO DFUNCOBJ_HISTPARM_DBT
        (T_HISTID, T_PARAM, T_STATE, T_PRIORITY, T_ACTIONTEXT)
      VALUES
        (HISTID,
         :new.T_PARAM,
         :new.T_STATE,
         :new.T_PRIORITY,
         CASE :new.t_state
           WHEN 1 THEN
            'Возникла ошибка при выполнении. Повтор выполнения' || ': ' || :new.t_errorcode || ' ' || :new.t_errortext
           WHEN 2 THEN
            'Возникла ошибка при выполнении. Остановлена обработка' || ': ' || :new.t_errorcode || ' ' || :new.t_errortext
           WHEN 3 THEN
            'Достигнут максимум повторов выполнения. Обработка остановлена' || ': ' || :new.t_errorcode || ' ' || :new.t_errortext
           WHEN 4 THEN
            'Плановый перезапуск задачи' || ': ' || :new.t_errorcode || ' ' || :new.t_errortext
         END );
    END IF;
  END IF;

END dfuncobj_dbt_fns_hist_u;
/