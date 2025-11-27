DECLARE
   v_Count NUMBER := 0;
BEGIN
   BEGIN
      SELECT COUNT(1) INTO v_Count
      FROM DSHEDULE_DBT
      WHERE t_Comment = 'Операции технической сверки СНОБ (планировщик)';
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN v_Count := 0;
   END;
   
   IF v_Count = 0
   THEN
      INSERT INTO DSHEDULE_DBT (
                                  t_ID,
                                  t_CIdentProgram,
                                  t_EventType,
                                  t_PeriodicalEvent,
                                  t_StartDate,
                                  t_StartTime,
                                  t_EndDate,
                                  t_EndTime,
                                  t_NextDate,
                                  t_NextTime,
                                  t_PeriodType,
                                  t_PeriodLength,
                                  t_WorkDays,
                                  t_DaysOfWeek,
                                  t_DaysOfMonth,
                                  t_Status,
                                  t_Action,
                                  t_Parms,
                                  t_Paused,
                                  t_Comment,
                                  t_SysEvents,
                                  t_Department,
                                  t_Priority,
                                  t_OpenPhase,
                                  t_UserEventCode,
                                  t_UsedAyEvent,
                                  t_EventDayOrder,
                                  t_EventDayKind,
                                  t_EventPeriodType,
                                  t_NotifyOfError,
                                  t_NotifyOfCompletion,
                                  t_OnExactTime,
                                  t_ExactTime,
                                  t_NotifyOfActComplete,
                                  T_RUNACTIONSCHAIN
                               )
                        VALUES (
                                  0,
                                  'Г',
                                  CHR(0),
                                  'X',
                                  to_date('01.01.2024', 'dd.mm.yyyy'),
                                  to_date('01.01.0001 00:00:00', 'dd.mm.yyyy hh24:mi:ss'),
                                  to_date('01.01.0001', 'dd.mm.yyyy'),
                                  to_date('01.01.0001', 'dd.mm.yyyy'),
                                  to_date('02.01.2024', 'dd.mm.yyyy'),
                                  to_date('01.01.0001 00:00:00', 'dd.mm.yyyy hh24:mi:ss'),
                                  4,
                                  1,
                                  CHR(0),
                                  0,
                                  0,
                                  0,
                                  1,
                                  '-exec:20215',
                                  CHR(0),
                                  'Операции технической сверки СНОБ (планировщик)',
                                  0,
                                  1,
                                  0,
                                  0,
                                  CHR(1),
                                  CHR(0),
                                  0,
                                  0,
                                  0,
                                  CHR(0),
                                  CHR(0),
                                  CHR(0),
                                  to_date('01.01.0001', 'dd.mm.yyyy'),
                                  CHR(0),
                                  CHR(0)
                               );
                               
      IT_LOG.LOG('BOSS-1489. Задание планировщика успешно вставлено');
   ELSE
      IT_LOG.LOG('BOSS-1489. Задание планировщика с комментарием ''Операции технической сверки СНОБ (планировщик)'' уже существует' );
   END IF;
END;
/