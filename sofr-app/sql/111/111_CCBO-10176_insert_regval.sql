-- Добавление настроек банка в regparm, regval
DECLARE
  regParmID dregparm_dbt.t_keyid%type;
  stat NUMBER;
  
  parent_folder_regval_name VARCHAR(200)  := 'РСХБ\\БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ\\ИМПОРТ СДЕЛОК ИЗ АСУДР-КОНДОР';
  max_tomcat_exec_time_regval_name VARCHAR(200) := parent_folder_regval_name || '\\МАКС.ВРЕМЯ ОБРАБ.СДЕЛКИ TOMCAT'; --максимальное время обработки сделки TOMCAT
  
BEGIN
  stat := RSI_RSB_REGVAL.GETREGPARM(regParmID, parent_folder_regval_name);

  IF stat = 0 THEN
    stat := RSI_RSB_REGVAL.GETREGPARM(regParmID, max_tomcat_exec_time_regval_name);
    IF stat != 0 THEN
      stat := RSI_RSB_REGVAL.AddRegIntegerValue(max_tomcat_exec_time_regval_name, 
                                             CHR(88), CHR(0), CHR(0), 
                                             'Кол-во секунд, при котором поддержке уйдет сообщение о превышении времени обработки запроса', 15);
    END IF;
  END IF;

END;
/