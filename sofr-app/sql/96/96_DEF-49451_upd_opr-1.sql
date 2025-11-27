DECLARE
  v_DocKind integer := 110; 
  v_OprKind integer := 14918; -- операция: выкуп собственных векселей банка
  v_StartDateKind integer := 11000000;
  v_PayDateKind integer := 11000001;
  v_DocDateKind integer := 11000002;
  v_BlockID integer := 20000904; -- основной блок операции
  -- Процедура добавления шага настройки в основной блок 
  PROCEDURE addStep
  IS
    v_NumStep integer := 5; -- номер вставляемого шага
    v_KindAction integer := 3; -- шаг настроек
    v_Macro varchar2(32) := 'vsv005dt.mac';
    v_Name varchar2(32) := 'Инициализация дат';
    v_Oper integer := 10002; -- операционист
    v_Dummy integer;
  BEGIN 
    SELECT t_number_step INTO v_Dummy FROM doprostep_dbt r 
      WHERE r.t_blockid = v_BlockID and r.t_number_step = v_NumStep AND rownum = 1;
  EXCEPTION WHEN no_data_found THEN
      BEGIN
        INSERT INTO doprostep_dbt r (
          r.t_blockid, r.t_number_step, r.t_kind_action, r.t_dayoffset, r.t_scale, r.t_dayflag, r.t_calendarid
          , r.t_symbol, r.t_previous_step, r.t_modification, r.t_carry_macro, r.t_print_macro, r.t_post_macro
          , r.t_notinuse, r.t_firststep, r.t_name, r.t_datekindid, r.t_rev, r.t_autoexecutestep, r.t_onlyhandcarry
          , r.t_isallowforoper, r.t_operorgroup, r.t_restrictearlyexecution, r.t_usertypes, r.t_initdatekindid
          , r.t_askfordate, r.t_backout, r.t_isbackoutgroup, r.t_massexecutemode, r.t_iscase, r.t_isdistaffexecute
          , r.t_skipinitafterplandate, r.t_masspacksize
        ) VALUES (
          v_BlockID, v_NumStep, v_KindAction, 0, 0, 'X', 0
          , chr(0), 0, 0, v_Macro, v_Macro, v_Macro
          , chr(0), 'X', v_Name, v_StartDateKind, chr(0), 'X', chr(0)
          , v_Oper, 'X', 'X', chr(1), v_DocDateKind
          , chr(0), v_Oper, 'X', 0, chr(0), chr(0)
          , 'X', 0
        );
        COMMIT;
      EXCEPTION WHEN others THEN 
        ROLLBACK;
      END; 
  END;
  -- Процедура корректировки предыдущего первого шага
  PROCEDURE correctStep10
  IS
    v_Step5 integer := 5; -- инициализация дат (нынешний первый шаг)
    v_Step10 integer := 10; -- ожидание акцепта (бывший первый шаг)
  BEGIN 
     UPDATE doprostep_dbt r 
       SET r.t_previous_step = v_Step5, r.t_firststep = chr(0) 
       WHERE r.t_blockid = v_BlockID AND r.t_number_step = v_Step10;
     COMMIT;
  EXCEPTION WHEN others THEN 
     ROLLBACK;
  END;
  -- Процедура корректировки шага 20 (Оплата векселей)
  -- нужно изменить вид даты 
  PROCEDURE correctStep20
  IS
    v_Step20 integer := 20; -- шага 20 (Оплата векселей)
  BEGIN 
     UPDATE doprostep_dbt r 
       SET r.t_datekindid = v_PayDateKind
       WHERE r.t_blockid = v_BlockID AND r.t_number_step = v_Step20;
     COMMIT;
  EXCEPTION WHEN others THEN 
     ROLLBACK;
  END;
  -- Процедура корректировки дат в открытых операциях
  PROCEDURE correctOpenOprDates
  IS
    v_OpenStatus integer := 10; -- статус открытых договоров
    v_Date date;
    v_EmptyDate date := to_date('01-01-0001', 'dd-mm-yyyy');
  BEGIN
    FOR rec IN (
       SELECT t_id_operation 
       FROM doprdates_dbt WHERE t_id_operation IN (
         SELECT o.t_id_operation FROM doproper_dbt o, ddl_order_dbt d 
           WHERE d.t_dockind = v_DocKind AND d.t_contractstatus = v_OpenStatus
           AND d.t_kind_operation = v_OprKind
           AND o.t_dockind = d.t_dockind AND o.t_documentid = lpad(d.t_contractid, 10, '0')
       )
       AND t_datekindid = v_PayDateKind AND t_date = v_EmptyDate
    )
    LOOP
       BEGIN 
         SELECT t_Date INTO v_Date FROM doprdates_dbt
            WHERE t_id_operation = rec.t_id_operation
            AND t_datekindid = v_StartDateKind;
       EXCEPTION WHEN no_data_found THEN
           SELECT trunc(sysdate) INTO v_Date FROM dual;
       END;
       UPDATE doprdates_dbt SET t_date = v_Date 
          WHERE t_id_operation = rec.t_id_operation
          AND t_datekindid = v_PayDateKind;
    END LOOP;   
    COMMIT;
  EXCEPTION
    WHEN others THEN 
      ROLLBACK;
  END;
BEGIN
  -- Добавление шага настройки в основной блок 
  addStep();
  -- Корректировка предыдущего первого шага
  correctStep10();
  -- Корректировка шага 20 (Оплата векселей), нужно изменить вид даты 
  correctStep20();
  -- Корректировка дат в открытых операциях
  correctOpenOprDates();
END;
/
