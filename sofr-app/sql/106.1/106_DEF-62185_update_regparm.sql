-- ΅­®Ά«¥­¨¥ ­ αβΰ®¥ª ΅ ­ª  Ά regparm
DECLARE
  regParmID dregparm_dbt.t_keyid%type;
  stat NUMBER;
  
  parent_folder_regval_path VARCHAR(200)  := '‘•\\…‘… ‘‹“†‚€…\\‚€‹”€–';
  ul_hidden_copy_regval_name VARCHAR(200) := '€€‚‹’ ‘›’“   ‹';
BEGIN
  stat := RSI_RSB_REGVAL.GETREGPARM(regParmID, parent_folder_regval_path || '\\' || ul_hidden_copy_regval_name);

  IF stat = 0 THEN
    UPDATE dRegParm_dbt
       SET t_global = CHR(88)
     WHERE t_keyID = regParmID;
  END IF;
END;
/