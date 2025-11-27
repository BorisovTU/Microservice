/*Удалить пункт меню*/
BEGIN
  DELETE FROM dmenuitem_dbt
   WHERE t_szNameItem LIKE '%Проверка расчета НДФЛ%'
     AND t_iCaseItem = 512;
END;
/