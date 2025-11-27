-- Изменения по DEF-64540
-- Удаление календаря 20 (так как совпадает с календарем 200)
DECLARE
  logID VARCHAR2(32) := 'DEF-64540';
  -- Записать сообщение в itt_log
  PROCEDURE LogIt(p_message IN VARCHAR2)
  AS
  BEGIN
    it_log.log(p_msg => logID||': '||p_message);
  END;
  -- Удаление календаря
  PROCEDURE delCalendar( p_CalID IN number )
  IS
    x_CalParamID number := -1;
  BEGIN
    LogIt('Поиск календаря: '||p_CalID);
    BEGIN
      SELECT t_id INTO x_CalParamID
        FROM ddlcalparam_dbt p 
        WHERE p.t_calkindid = p_CalID AND rownum = 1;
    EXCEPTION WHEN no_data_found THEN
      LogIt('Не найден календарь: '||p_CalID);
      NULL;
    END;
    IF(x_calparamid <> -1) THEN
      LogIt('Удаление календаря: '||p_CalID);
      DELETE FROM ddlcalparam_dbt p WHERE p.t_id = x_CalParamID;
      DELETE FROM ddlcalparamlnk_dbt l WHERE l.t_calparamid = x_CalParamID;
      DELETE FROM dcalkind_dbt c WHERE c.t_id = p_CalID;
      COMMIT;
      LogIt('Произведено удаление календаря: '||p_CalID);
    END IF;
  EXCEPTION
   WHEN OTHERS THEN 
      LogIt('Ошибка при удалении календаря: '||p_CalID);
      EXECUTE IMMEDIATE 'ROLLBACK';
  END;
BEGIN
  delCalendar(20);           	-- Удаление календаря
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
END;
/
