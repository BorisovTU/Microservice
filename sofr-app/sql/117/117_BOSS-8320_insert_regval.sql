DECLARE
  regParmID dregparm_dbt.t_keyid%type;
  stat NUMBER;
  
  parent_folder_regval_name VARCHAR(200)  := 'РСХБ\\БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ\\ИНТЕГРАЦИЯ КАФКА';
  regval_name VARCHAR(200) := parent_folder_regval_name || '\\ПУШ-УВЕДОМЛЕНИЯ ПО ВЫПЛАТАМ ЦБ'; 
BEGIN
  stat := RSI_RSB_REGVAL.GETREGPARM(regParmID, parent_folder_regval_name);

  IF stat = 0 THEN
    stat := RSI_RSB_REGVAL.GETREGPARM(regParmID, regval_name);
    IF stat != 0 THEN
      stat := RSI_RSB_REGVAL.AddRegFlagValue(regval_name, 
                                             CHR(88), CHR(0), CHR(0), 
                                             'Рубильник по BOSS-8320. Формирование пуш-уведомлений по выплатам по ценным бумагам по сервису SendNonTradingOrderInfoReq', 
                                             CHR(0));
    END IF;
  END IF;

END;
/