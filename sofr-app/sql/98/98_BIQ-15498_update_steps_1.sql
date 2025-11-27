--установка автовыполнения для шага "Отправка неторгового поручения в QUIK"
begin
  UPDATE doprostep_dbt SET T_AUTOEXECUTESTEP=chr(88) WHERE T_BLOCKID=203702 AND T_NUMBER_STEP=105;
  commit;
end;
