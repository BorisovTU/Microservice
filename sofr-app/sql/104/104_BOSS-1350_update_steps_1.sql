--- изменение шага операции зачисления ЦБ
DECLARE
BEGIN

  UPDATE doprostep_dbt SET T_MASSEXECUTEMODE=0 
  WHERE T_BLOCKID=201100 AND T_NUMBER_STEP=35;
  COMMIT;
  
EXCEPTION
   WHEN OTHERS THEN it_log.log('Ошибка изменения шага Отправка неторгового поручения в QUIK для зачисления ЦБ');
END;
