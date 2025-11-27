-- Изменения по DEF-63762 
-- 1) Удаление дублей, которые могли появиться при выводе разработки в release-102.1.
-- 2) Корректировка T_PARTY (2 -- неправильно, 133124 -- правильно)
DECLARE
  logID VARCHAR2(32) := 'DEF-63762';
  x_Cnt NUMBER;
  x_Stat NUMBER := 0;

  -- Записать сообщение в itt_log
  PROCEDURE LogIt(p_message IN VARCHAR2)
  AS
  BEGIN
    it_log.log(p_msg => logID||': '||p_message);
  END;
  -- Удаление дублей по заявкам BlockedOTC
  PROCEDURE correctBlockedOTC_Double
  IS
    x_I number := 0;
    x_Date DATE;
    x_Time DATE;
    x_Client NUMBER;
    x_Fiid NUMBER;
    x_IsNewBlockedOTC NUMBER := 0;
    x_ID NUMBER;
  BEGIN
    LogIt('Удаление дублей по заявкам BlockedOTC');
    FOR c IN (
      SELECT rq.t_id, rq.t_date, rq.t_time, rq.t_client, rq.t_fiid 
      FROM ddl_req_dbt rq 
      WHERE rq.t_code like 'BlockedOTC%' 
      ORDER by t_date, t_time, t_client, t_fiid
    ) LOOP
      IF(x_I = 0) THEN
        -- первая запись, является новой
        x_IsNewBlockedOTC := 1;
        x_I := 1;
      ELSIF(x_Date = c.t_date AND x_Time = c.t_time AND x_Client = c.t_client AND x_Fiid = c.t_fiid) THEN
        -- дубль
        DELETE FROM ddl_req_dbt r WHERE r.t_id = c.t_id;
      ELSE
        -- не дубль, новая запись
        x_IsNewBlockedOTC := 1;
      END IF;
      -- если новая запись, то запоминаем значения
      IF (x_IsNewBlockedOTC = 1) THEN 
        x_IsNewBlockedOTC := 0;
        x_Date := c.t_date;
        x_Time := c.t_time;
        x_Client := c.t_client;
        x_Fiid := c.t_fiid;
        x_ID := c.t_id;
      END IF;
    END LOOP;
    COMMIT;
    LogIt('Произведено удаление дублей по заявкам BlockedOTC');
  EXCEPTION
   WHEN OTHERS THEN 
      LogIt('Ошибка удаления дублей по заявкам BlockedOTC');
      EXECUTE IMMEDIATE 'ROLLBACK';
  END;
  -- Корректировка T_PARTY по заявкам BlockedOTC
  PROCEDURE correctBlockedOTC_133124
  IS
    x_CorrectParty NUMBER := 133124;
    x_WrongParty NUMBER := 2;
  BEGIN
    LogIt('Корректировка T_PARTY по заявкам BlockedOTC');
    UPDATE ddl_req_dbt rq 
      SET rq.t_party = x_CorrectParty 
      WHERE rq.t_code like 'BlockedOTC%' and rq.t_party = x_WrongParty;
    COMMIT;
    LogIt('Произведена корректировка T_PARTY по заявкам BlockedOTC');
  EXCEPTION
   WHEN OTHERS THEN 
      LogIt('Ошибка корректировки T_PARTY по заявкам BlockedOTC');
      EXECUTE IMMEDIATE 'ROLLBACK';
  END;
BEGIN
  correctBlockedOTC_Double();         		-- Удаление дублей по заявкам BlockedOTC
  correctBlockedOTC_133124();         		-- Корректировка T_PARTY по заявкам BlockedOTC
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
END;
/
