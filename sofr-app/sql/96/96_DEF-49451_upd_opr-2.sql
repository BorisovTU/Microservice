DECLARE
  v_PayDateKind integer := 11000001; -- дата оплаты
  v_BlockID integer := 1491804; -- Завершение шагов операции
  -- Процедура корректировки вида даты для блока
  PROCEDURE correctStepsDateKind
  IS
  BEGIN 
     UPDATE doprostep_dbt r 
       SET r.t_datekindid = v_PayDateKind
       WHERE r.t_blockid = v_BlockID;
     COMMIT;
  EXCEPTION WHEN others THEN 
     ROLLBACK;
  END;
BEGIN
  -- cкорректировать вид даты для блока
  correctStepsDateKind();
END;
/
