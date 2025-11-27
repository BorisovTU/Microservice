-- Добавление новой записи в dllvalues_dbt 
DECLARE
  v_cnt NUMBER;
BEGIN
  SELECT COUNT(1) INTO v_cnt
    FROM dLlValues_dbt
   WHERE t_list = 1143
     AND t_element = 514;
  
  IF v_cnt = 0 THEN
    INSERT INTO dLlValues_dbt
      (t_list,
       t_element, 
       t_code, 
       t_name, 
       t_flag, 
       t_note,
       t_reserve)
    VALUES 
      (1143,
       514,
       '514',
       'Уведомление об успешной пролонгации статуса КИ',
       514,
       'Уведомление об успешной пролонгации статуса Квалифицированного инвестора',
       CHR(1));

     COMMIT;
  END IF;

END;
/