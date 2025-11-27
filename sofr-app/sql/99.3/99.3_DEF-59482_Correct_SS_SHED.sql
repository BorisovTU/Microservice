BEGIN
   UPDATE DSIMPLESERVICE_DBT
   SET t_Name = 'Выполнение процедуры генерации операций удержания',
       t_Description = 'Выполнение процедуры генерации операций удержания'
   WHERE t_ID = 10084;
   
   IT_LOG.LOG('DEF-59482. Корректировка таблицы DSIMPLESERVICE_DBT прошла успешно');
   
   COMMIT;
   
   UPDATE DSS_SHEDULER_DBT
   SET t_Name = 'Выполнение процедуры генерации операций удержания',
       t_Description = 'Выполнение процедуры генерации операций удержания',
       t_ShedulerType = 0
   WHERE t_ID = 10084;
   
   IT_LOG.LOG('DEF-59482. Корректировка таблицы DSS_SHEDULER_DBT прошла успешно');
   
   COMMIT;
   
   UPDATE DSS_FUNC_DBT
   SET t_Name = 'Выполнение процедуры генерации операций удержания'
   WHERE t_ID = 100084;
   
   IT_LOG.LOG('DEF-59482. Корректировка таблицы DSS_FUNC_DBT прошла успешно');
   
   COMMIT;
END;
/