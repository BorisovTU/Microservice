begin
   INSERT INTO DSIMPLESERVICE_DBT (
                                     t_ID,
                                     t_Name,
                                     t_Description,
                                     t_IsActive
                                  )
                           VALUES (
                                     10084,
                                     'Выполнение процедуры генерации операций удержания',
                                     'Выполнение процедуры генерации операций удержания',
                                     CHR(88)
                                  );

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
                                   10084,
                                   'Выполнение процедуры генерации операций удержания',
                                   'Выполнение процедуры генерации операций удержания',
                                   10084,
                                   1,
                                   2,
                                   to_date('01.01.0001 00:00:00', 'dd.mm.yyyy hh24:mi:ss'),
                                   to_date('01.01.0001 23:59:59', 'dd.mm.yyyy hh24:mi:ss'),
                                   to_date('01.01.0001 00:00:00', 'dd.mm.yyyy hh24:mi:ss'),
                                   3,
                                   1,
                                   to_date('01.01.2024 00:00:00', 'dd.mm.yyyy hh24:mi:ss'),
                                   CHR(1),
                                   NULL
                                );
                                
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
                               100084,
                               10084,
                               1,
                               'Выполнение процедуры генерации операций удержания',
                               1,
                               'dss_GenNPTXHoldMass',
                               'GenNPTXHoldMass',
                               0,
                               600,
                               10,
                               0,
                               CHR(1)
                            );
end;
/