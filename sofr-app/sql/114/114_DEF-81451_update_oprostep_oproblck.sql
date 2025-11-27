BEGIN
  UPDATE doprostep_dbt
     SET t_iscase = CHR(88)
   WHERE t_blockid = 109500
     AND t_number_step = 10;
END;
/

BEGIN
  UPDATE doproblck_dbt
     SET t_symbol = 'O'
   WHERE t_kind_operation = 1095
     AND t_blockid = 109500;
END;
/