-- Изменения по DEF-61943
-- Добавление недостающих записей в dpartyown_dbt 
-- Нужно добавить следующих субъектов в список депозитариев (t_partykind = 4) 
-- t_partyid = 131470; -- АО "Новый Регистратор"
-- t_partyid = 131466; -- АО "Регистраторское общество "СТАТУС"
DECLARE
  logID VARCHAR2(32) := 'DEF-61943';
  -- Записать сообщение в itt_log
  PROCEDURE LogIt( p_message VARCHAR2 )
  AS
  BEGIN
    it_log.log(p_msg => logID||': '||p_message);
  END;
  -- Создание таблицы uEntCompareParam_tmp
  PROCEDURE AddPartyOwn(p_PartyID IN number, p_PartyKind IN number DEFAULT 4)
  AS
    x_Cnt number;
  BEGIN
    LogIt('Добавление в dpartyown_dbt, t_PartyID = '||p_PartyID||', t_PartyKind = '||p_PartyKind);
    INSERT INTO dpartyown_dbt r (
      r.t_partyid, r.t_partykind, r.t_superior, r.t_subkind, r.t_branch, r.t_numsession
    ) VALUES (
      p_PartyID, p_PartyKind, 0, 0, 0, 0
    );
    COMMIT;
    LogIt('Добавлена запись в dpartyown_dbt, t_PartyID = '||p_PartyID||', t_PartyKind = '||p_PartyKind);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      LogIt('Ошибка добавления записи в dpartyown_dbt, t_PartyID = '||p_PartyID||', t_PartyKind = '||p_PartyKind);
  END;
BEGIN
  -- Добавление недостающих субъектов в список депозитариев (t_partykind = 4) 
  AddPartyOwn( 131470 ); 							-- АО "Новый Регистратор"
  AddPartyOwn( 131466 ); 							-- АО "Регистраторское общество "СТАТУС"
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
END;
/
