-- обновление шагов операции списания ДС
DECLARE
BEGIN
  update doprostep_dbt SET T_NAME = 'Отправка неторгового поручения в QUIK' WHERE T_BLOCKID = 203702 AND T_NUMBER_STEP = 105;    
EXCEPTION
   WHEN OTHERS THEN it_log.log('Ошибка обновления шага Отправка неторгового поручения в QUIK');
END;
/

DECLARE
BEGIN
  update doprostep_dbt SET T_NAME = 'Получение результата обработки неторгового поручения в QUIK' WHERE T_BLOCKID = 203702 AND T_NUMBER_STEP = 107;    
EXCEPTION
   WHEN OTHERS THEN it_log.log('Ошибка обновления шага Получение результата обработки неторгового поручения в QUIK');
END;
/
