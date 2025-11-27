--признак автовыполнения на шаге 107 операции зачисления ДС
begin
  UPDATE doprostep_dbt SET T_AUTOEXECUTESTEP=chr(88) WHERE T_BLOCKID=203702 AND T_NUMBER_STEP=107;
  commit;
end;
