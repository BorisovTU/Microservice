-- Изменения по DEF-63686 (Корректировка данных, в первой версии заявки были созданы без SPGROUND и SPGRDOC, 
-- поэтому они не отображаются в списке, чтобы стали отображаться, нужно пройтись по заявкам BlockedOTC,
-- у которых отсутствуют SPGROUND и SPGRDOC, и создать их
-- Таким образом, скрипт должен отработать только один раз.
DECLARE
  logID VARCHAR2(32) := 'DEF-63686';
  x_Cnt NUMBER;
  x_Stat NUMBER := 0;

  -- Записать сообщение в itt_log
  PROCEDURE LogIt(p_message IN VARCHAR2)
  AS
  BEGIN
    it_log.log(p_msg => logID||': '||p_message);
  END;
  -- Исправление по заявкам BlockedOTC
  PROCEDURE correctBlockedOTC
  IS
    x_SpGroundID number;
  BEGIN
    LogIt('Исправление по заявкам BlockedOTC');
    FOR c IN (
       SELECT 
         rq.t_id, 513 AS t_doclog, 251 AS t_kind, 1 AS t_direction, rq.t_date, rq.t_time
         , rq.t_client AS t_party, pt.t_name AS t_partyname
         , pc.t_code AS t_partycode, 1 AS t_department, 1 AS t_branch
       FROM 
         ddl_req_dbt rq, dparty_dbt pt, dpartcode_dbt pc 
       WHERE rq.t_code like 'BlockedOTC%' 
         and rq.t_id not in (select t_sourcedocid from dspgrdoc_dbt where t_sourcedockind = 350)
         and pt.t_partyid = rq.t_client
         and pc.t_partyid = rq.t_client and pc.t_codekind = 1
    ) LOOP
      INSERT INTO dspground_dbt r ( 
         r.t_doclog, r.t_kind, r.t_direction, r.t_registrdate, r.t_registrtime
         , r.t_party, r.t_partyname, r.t_partycode, r.t_department, r.t_branch
      ) VALUES (
         c.t_doclog, c.t_kind, c.t_direction, c.t_date, c.t_time
         , c.t_party, c.t_partyname, c.t_partycode, c.t_department, c.t_branch
      )
      RETURNING r.t_spgroundid INTO  x_SpGroundID ;
      INSERT INTO dspgrdoc_dbt r ( 
         r.t_sourcedockind, r.t_sourcedocid, r.t_spgroundid, r.t_status
      ) VALUES (
         350, c.t_id, x_SpGroundID, chr(1)
      );
      COMMIT;
    END LOOP;
    LogIt('Произведено исправление по заявкам BlockedOTC');
  EXCEPTION
   WHEN OTHERS THEN 
      LogIt('Ошибка исправления по заявкам BlockedOTC');
      EXECUTE IMMEDIATE 'ROLLBACK';
  END;
BEGIN
  correctBlockedOTC();           	-- Исправление по заявкам BlockedOTC
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
END;
/
