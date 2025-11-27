-- Добавление настроек банка в regparm, regval
DECLARE
  regParmID dregparm_dbt.t_keyid%type;
  stat NUMBER;
  
  parent_folder_regval_path VARCHAR(200)  := 'РСХБ\\БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ\\КВАЛИФИКАЦИЯ';
  fl_hidden_copy_regval_name VARCHAR(200) := 'НАПРАВЛЯТЬ СКРЫТУЮ КОПИЮ ПО ФЛ';
  ul_hidden_copy_regval_name VARCHAR(200) := 'НАПРАВЛЯТЬ СКРЫТУЮ КОПИЮ ПО ЮЛ';
  ul_hidden_copy_old_regval_name VARCHAR(200) := 'НАПРАВЛЯТЬ СКРЫТУЮ КОПИЮ';

BEGIN
  stat := RSI_RSB_REGVAL.GETREGPARM(regParmID, parent_folder_regval_path);

  IF stat = 0 THEN
    stat := RSI_RSB_REGVAL.GETREGPARM(regParmID, parent_folder_regval_path || '\\' || ul_hidden_copy_old_regval_name);

    IF stat = 0 THEN
      UPDATE dRegParm_dbt
         SET t_name = ul_hidden_copy_regval_name,
             t_description = 'Почта брокера для получения писем с уведомлениями для ЮЛ'
       WHERE t_keyID = regParmID;
    END IF;
    
    stat := RSI_RSB_REGVAL.GETREGPARM(regParmID, parent_folder_regval_path || '\\' || fl_hidden_copy_regval_name);

    IF stat != 0 THEN
      stat := RSI_RSB_REGVAL.AddRegStringValue(parent_folder_regval_path || '\\' || fl_hidden_copy_regval_name, CHR(88), CHR(0), CHR(0), 'Почта брокера для получения писем с уведомлениями для ФЛ', 'broker@rshb.ru; custody@rshb.ru', chr(1));
    END IF;
  END IF;

END;
/