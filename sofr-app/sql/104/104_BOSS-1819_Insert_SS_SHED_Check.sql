begin
   INSERT INTO DSIMPLESERVICE_DBT (
                                     t_ID,
                                     t_Name,
                                     t_Description,
                                     t_IsActive
                                  )
                           VALUES (
                                     10086,
                                     'Контроль выполнения процедуры запроса к хран. СНОБ',
                                     'Контроль выполнения запроса данных от хранилища СНОБ в рамках доудержания НДФЛ в конце года за Депозитарий',
                                     CHR(88)
                                  );
                                  
   IT_LOG.LOG('BOSS-1819 CheckRequestSNOBStor. Вставка записи в таблицу DSIMPLESERVICE_DBT прошла успешно');
   COMMIT;

   INSERT INTO DSS_SHEDULER_DBT (
                                   t_ID,
                                   t_Name,
                                   t_Description,
                                   t_Service,
                                   t_Module,
                                   t_ShedulerType,
                                   t_WorkStartTime,
                                   t_WorkEndTime,
                                   t_StartTime,
                                   t_PeriodType,
                                   t_Period,
                                   t_NextStamp,
                                   t_Parameters,
                                   t_SessionID
                                )
                         VALUES (
                                   10086,
                                   'Контроль выполнения процедуры запроса к хран. СНОБ',
                                   'Контроль выполнения запроса данных от хранилища СНОБ в рамках доудержания НДФЛ в конце года за Депозитарий',
                                   10086,
                                   1,
                                   0,
                                   to_date('01.01.0001 00:00:00', 'dd.mm.yyyy hh24:mi:ss'),
                                   to_date('01.01.0001 23:59:59', 'dd.mm.yyyy hh24:mi:ss'),
                                   to_date('01.01.0001 00:00:00', 'dd.mm.yyyy hh24:mi:ss'),
                                   3,
                                   1,
                                   to_date('01.01.2024 00:00:00', 'dd.mm.yyyy hh24:mi:ss'),
                                   CHR(1),
                                   NULL
                                );
                                
   IT_LOG.LOG('BOSS-1819 CheckRequestSNOBStor. Вставка записи в таблицу DSS_SHEDULER_DBT прошла успешно');
   COMMIT;
                                
   INSERT INTO DSS_FUNC_DBT (
                               t_ID,
                               t_Service,
                               t_Level,
                               t_Name,
                               t_Type,
                               t_ExecutorName,
                               t_ExecutorFunc,
                               t_StartDelay,
                               t_Period,
                               t_MaxAttempt,
                               t_Timeout,
                               t_Parameters
                            )
                     VALUES (
                               100086,
                               10086,
                               1,
                               'Контроль выполнения процедуры запроса к хран. СНОБ',
                               1,
                               'CheckRequestSnobStor.mac',
                               'Exec_CheckRequestSnobStor',
                               0,
                               600,
                               10,
                               0,
                               CHR(1)
                            );
                            
   IT_LOG.LOG('BOSS-1819 CheckRequestSNOBStor. Вставка записи в таблицу DSS_FUNC_DBT прошла успешно');
end;
/