/*Добавление пользовательского модуля*/
BEGIN
  UPDATE DSHEDULE_DBT
     SET T_PARMS = '-exec:205'
   WHERE T_COMMENT LIKE 'Обработка заявлений на возврат налога'
     AND T_PARMS = '-exec:203';
     
END;
/


