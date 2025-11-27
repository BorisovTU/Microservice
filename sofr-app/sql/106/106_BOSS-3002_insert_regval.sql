-- Добавление настроек банка в regparm, regval
DECLARE
  regParmID dregparm_dbt.t_keyid%type;
  stat NUMBER;
  
  parent_regval_name VARCHAR(200) := 'CB\PARTY\ИМПОРТ СПРАВОЧНИКОВ';
  format_okato_regval_name VARCHAR(200) := 'CB\PARTY\ИМПОРТ СПРАВОЧНИКОВ\ФОРМАТ ОКАТО 11 РАЗРЯДОВ';
  
  parent_regval_name2 VARCHAR(200) := 'РСХБ\ИНТЕГРАЦИЯ';
  use_okato_new_functional VARCHAR(200) := 'РСХБ\ИНТЕГРАЦИЯ\ДОП ФУНКЦИОНАЛ ОКАТО';
BEGIN
  stat := RSI_RSB_REGVAL.GETREGPARM(regParmID, parent_regval_name);

  IF stat = 0 THEN
    stat := RSI_RSB_REGVAL.GETREGPARM(regParmID, format_okato_regval_name);
    IF stat != 0 THEN
      stat := RSI_RSB_REGVAL.AddRegFlagValue(format_okato_regval_name, CHR(88), CHR(0), CHR(0), 'Формат ОКАТО 11 разрядов', CHR(88));
    END IF;
  END IF;
  
  stat := RSI_RSB_REGVAL.GETREGPARM(regParmID, parent_regval_name2);

  IF stat = 0 THEN
    stat := RSI_RSB_REGVAL.GETREGPARM(regParmID, use_okato_new_functional);
    IF stat != 0 THEN
      stat := RSI_RSB_REGVAL.AddRegFlagValue(use_okato_new_functional, CHR(88), CHR(0), CHR(0), 'Использовать дополнительный функционал по ОКАТО', CHR(0));
    END IF;
  END IF;
END;
/