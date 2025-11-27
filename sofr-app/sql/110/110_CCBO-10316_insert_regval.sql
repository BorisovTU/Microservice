-- Добавление настроек банка в regparm, regval
DECLARE
  regParmID dregparm_dbt.t_keyid%type;
  stat NUMBER;
  
  parent_folder_regval_name VARCHAR(200)  := 'РСХБ\\БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ\\ИМПОРТ СДЕЛОК ИЗ АСУДР-КОНДОР';
  opendeals_use_conveyer_regval_name VARCHAR(200) := parent_folder_regval_name || '\\ИСПОЛН.ОТКР.СДЕЛКИ В КОНВЕЙЕРЕ'; --исполнение открытых сделок в режиме конвейера
  opendeals_count_regval_name VARCHAR(200) := parent_folder_regval_name || '\\КОЛ-ВО ОТКР.СДЕЛОК ДЛЯ КОНВЕЙРА'; --кол-во выбранных открытых сделок для исполнения в конвейере
  
BEGIN
  stat := RSI_RSB_REGVAL.GETREGPARM(regParmID, parent_folder_regval_name);

  IF stat = 0 THEN
    stat := RSI_RSB_REGVAL.GETREGPARM(regParmID, opendeals_use_conveyer_regval_name);
    IF stat != 0 THEN
      stat := RSI_RSB_REGVAL.AddRegFlagValue(opendeals_use_conveyer_regval_name, 
                                             CHR(88), CHR(0), CHR(0), 
                                             'Исполнять открытые сделки в режиме конвейера', CHR(0));
    END IF;
    
    stat := RSI_RSB_REGVAL.GETREGPARM(regParmID, opendeals_count_regval_name);
    IF stat != 0 THEN
      stat := RSI_RSB_REGVAL.AddRegIntegerValue(opendeals_count_regval_name, 
                                             CHR(88), CHR(0), CHR(0), 
                                             'Кол-во выбранных открытых сделок для исполнения в конвейере', 50);
    END IF;
  END IF;

END;
/