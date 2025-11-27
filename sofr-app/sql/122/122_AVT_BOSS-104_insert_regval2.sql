-- Добавление настроек банка в regparm, regval
DECLARE
  regParmID dregparm_dbt.t_keyid%type;
  stat NUMBER;
  
  parent_folder_regval_name VARCHAR(200)  := 'РСХБ\\БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ';
  notification_qi_status_regval_name VARCHAR(200) := parent_folder_regval_name || '\\УВЕДОМЛЕНИЯ_ПО_ЗАЧ_ДС_ДЛЯ_РК';
  
BEGIN
  stat := RSI_RSB_REGVAL.GETREGPARM(regParmID, parent_folder_regval_name);

  IF stat = 0 THEN
    stat := RSI_RSB_REGVAL.GETREGPARM(regParmID, notification_qi_status_regval_name);
    IF stat != 0 THEN
      stat := RSI_RSB_REGVAL.AddRegFlagValue(notification_qi_status_regval_name, 
                                             CHR(88), CHR(0), CHR(0), 
                                             'NO-не отправлять ОД письмо, что ответ от ЦФТ не пришел вовремя', CHR(0));
    END IF;
  END IF;
END;
/