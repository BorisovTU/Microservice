-- Добавление настроек банка в regparm, regval
DECLARE
  regParmID dregparm_dbt.t_keyid%type;
  stat NUMBER;
  
  parent_folder_regval_name VARCHAR(200)  := 'РСХБ\\БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ\\ИМПОРТ СДЕЛОК ИЗ АСУДР-КОНДОР';
  use_conveyer_regval_name VARCHAR(200) := parent_folder_regval_name || '\\ИСПОЛНЯТЬ СДЕЛКИ В КОНВЕЙЕРЕ';
  
BEGIN
  stat := RSI_RSB_REGVAL.GETREGPARM(regParmID, parent_folder_regval_name);

  IF stat = 0 THEN
    stat := RSI_RSB_REGVAL.GETREGPARM(regParmID, use_conveyer_regval_name);
    IF stat != 0 THEN
      stat := RSI_RSB_REGVAL.AddRegFlagValue(use_conveyer_regval_name, 
                                             CHR(88), CHR(0), CHR(0), 
                                             'Исполнять сделки в конвейере', CHR(88));
    END IF;
  END IF;

END;
/