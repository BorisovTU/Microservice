BEGIN
    UPDATE DSIMPLESERVICE_DBT
    SET 
      t_Name        = 'Выполнение процедуры запроса к хранилищу СНОБ',
      t_Description = 'Запрос данных от хранилища СНОБ в рамках доудержания НДФЛ в конце года за Депозитарий'
    WHERE t_ID = 10085;

    UPDATE DSS_SHEDULER_DBT
    SET
      t_Name        = 'Выполнение процедуры запроса к хранилищу СНОБ',
      t_Description = 'Запрос данных от хранилища СНОБ в рамках доудержания НДФЛ в конце года за Депозитарий'
    WHERE t_ID = 10085;

    UPDATE DSS_FUNC_DBT
    SET
      t_Name = 'Выполнение процедуры запроса к хранилищу СНОБ'
    WHERE T_ID = 100085;
END;