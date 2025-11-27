-- Добавление настроек банка в regparm, regval
DECLARE
  regParmID dregparm_dbt.t_keyid%type;
  stat NUMBER;
  
  parent_folder_regval_name VARCHAR(200)  := 'РСХБ\\БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ\\ИМПОРТ СДЕЛОК ИЗ АСУДР-КОНДОР';
  revaluation_notification_email_regval_name VARCHAR(200) := parent_folder_regval_name || '\\EMAIL ДЛЯ ИНФОРМ.ПО ПЕРЕОЦЕНКЕ'; -- email для информирование по переоценке

BEGIN
  stat := RSI_RSB_REGVAL.GETREGPARM(regParmID, parent_folder_regval_name);

  IF stat = 0 THEN
    stat := RSI_RSB_REGVAL.GETREGPARM(regParmID, revaluation_notification_email_regval_name);
    IF stat != 0 THEN
      stat := RSI_RSB_REGVAL.AddRegStringValue(revaluation_notification_email_regval_name, 
                                             CHR(88), CHR(0), CHR(0), 
                                             'Email для информирования по переоценке', '', CHR(1));
    END IF;
  END IF;

END;
/