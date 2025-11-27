/*Удалить пункт меню*/
BEGIN
  DELETE FROM ditemuser_dbt
   WHERE t_cIdentProgram = 'S'
     AND t_iCaseItem = 512
     AND t_szNameItem LIKE '%Проверка расчета НДФЛ%';
END;
/