-- Изменения по DEF-68471
-- Корректировка признака ЕДП по счетам '47423' у договоров, где его нет.
DECLARE
  logID VARCHAR2(32) := 'DEF-68471';
  x_Cnt NUMBER;

  -- Записать сообщение в itt_log
  PROCEDURE LogIt(p_message IN VARCHAR2)
  AS
  BEGIN
    it_log.log(p_msg => logID||': '||p_message);
  END;
  -- корректировка признака ЕДП по счету и валюте
  PROCEDURE correctEdpAcc(p_Acc IN varchar2, p_Currency IN number)
  IS
    x_AccID number;
    x_UserTypeAccount varchar2(16);
  BEGIN
    LogIt('корректировка признака ЕДП, account='||p_Acc||', currency='||p_Currency);
    SELECT a.t_usertypeaccount, a.t_accountid 
      INTO x_UserTypeAccount, x_AccID
      FROM daccount_dbt a
      WHERE a.t_account = p_Acc and a.t_code_currency = p_Currency and a.t_chapter = 1
    ;
    IF(instr(x_UserTypeAccount, chr(133)) > 0) THEN
       LogIt('Признак ЕДП уже установлен, account='||p_Acc||', currency='||p_Currency);
    ELSE
       -- если нет признака ЕДП, нужно добавить
       UPDATE daccount_dbt a 
         SET a.t_usertypeaccount = a.t_usertypeaccount||chr(133) 
         WHERE a.t_accountid = x_AccID;
       COMMIT;
       LogIt('произведена корректировка признака ЕДП, account='||p_Acc||', currency='||p_Currency);
    END IF;
  EXCEPTION
   WHEN OTHERS THEN 
      LogIt('ошибка корректировки признака ЕДП, account='||p_Acc||', currency='||p_Currency);
      ROLLBACK;
  END;
  -- корректировка признака ЕДП по счетам договора
  PROCEDURE correctEdpAccounts(p_Number IN varchar2)
  IS
  BEGIN
    LogIt('корректировка признака ЕДП, договор='||p_Number);
    FOR i IN (
       SELECT distinct t_account, t_currency FROM (
         SELECT c.t_number, a.t_account, a.t_currency
         FROM dsfcontr_dbt c, dmcaccdoc_dbt A
         WHERE c.t_number like p_Number||'%' AND c.t_servkindsub = 8
         AND RSB_SECUR.GetGeneralMainObjAttr (659, LPAD (c.t_id, 10, '0'), 102, to_date('31122999','ddmmyyyy')) = 1
         AND A.T_CLIENTCONTRID = c.t_id AND a.t_catid = 818 and a.t_iscommon = 'X' 
       )
    ) LOOP
       correctEdpAcc(i.t_account, i.t_currency);
    END LOOP;
    LogIt('Произведена корректировка признака ЕДП, договор='||p_Number);
  EXCEPTION
   WHEN OTHERS THEN 
      LogIt('ошибка корректировки признака ЕДП, договор='||p_Number);
  END;
BEGIN
  correctEdpAccounts('38-276436-ИИС_');
  correctEdpAccounts('14-979202-ИИС_');
  correctEdpAccounts('03/41-136220_');
  correctEdpAccounts('18054230-ИИС_');
  correctEdpAccounts('58-16615734_');
  correctEdpAccounts('00/04-296356-ИИС_');
  correctEdpAccounts('15/29-65714-ИИС_');
  correctEdpAccounts('73-456345-ИИС_');
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
END;
/