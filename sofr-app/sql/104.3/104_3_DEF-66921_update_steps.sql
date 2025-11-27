--- изменение шага операции списания ЦБ
DECLARE
BEGIN

  UPDATE doprostep_dbt SET T_NOTINUSE=chr(0) WHERE T_BLOCKID=201000 AND T_NUMBER_STEP=40;
  COMMIT;
  
EXCEPTION
   WHEN OTHERS THEN it_log.log('Ошибка изменения шага Закрытие для списания ЦБ');
END;

/

--- изменение шага операции зачисления ЦБ
DECLARE
BEGIN

  UPDATE doprostep_dbt SET T_NOTINUSE=chr(0) WHERE T_BLOCKID=201100 AND T_NUMBER_STEP=40;
  COMMIT;
  
EXCEPTION
   WHEN OTHERS THEN it_log.log('Ошибка изменения шага Закрытие для зачисления ЦБ');
END;

/