-- Изменения по DEF-56673, Изменение в dmcaccdoc_dbt
DECLARE
  logID VARCHAR2(9) := 'DEF-56673';
  -- Записать сообщение в itt_log
  PROCEDURE LogIt( p_message VARCHAR2 )
  AS
  BEGIN
    it_log.log(p_msg => logID||': '||p_message);
  END;
  -- Изменение в dmcaccdoc_dbt
  PROCEDURE ChangeMcaccdoc( p_CurAcc IN varchar2, p_NewAcc IN varchar2 )
  AS
  BEGIN
    LogIt('Изменение в dmcaccdoc_dbt: замена '||p_CurAcc||' на '||p_NewAcc);
    UPDATE dmcaccdoc_dbt SET t_account = p_NewAcc WHERE t_account = p_CurAcc;
    EXECUTE IMMEDIATE 'COMMIT';
    LogIt('Произведено изменение в dmcaccdoc_dbt');
  EXCEPTION
    WHEN OTHERS THEN
      LogIt('Ошибка изменения в dmcaccdoc_dbt');
  END;
BEGIN
  -- Изменение в dmcaccdoc_dbt (для fiid = 978, Евро)
  ChangeMcaccdoc('70608810899004630101', '70608810800004630103');
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
END;
/
