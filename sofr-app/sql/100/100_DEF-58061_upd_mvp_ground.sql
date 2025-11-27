BEGIN
  UPDATE dmultymtd_dbt
     SET T_GROUNDPOSITIVE = 'Нереализованная курсовая разница к сумме {ValSum} {ValFiCode}. Исходный документ Мем.ор. N ''{Numb}''  от {Date}, валюта {ValFiCode}, курс операции {Rate}, текущий курс {Rate_CB}.',
         T_GROUNDNEGATIVE = 'Нереализованная курсовая разница к сумме {ValSum} {ValFiCode}. Исходный документ Мем.ор. N ''{Numb}''  от {Date}, валюта {ValFiCode}, курс операции {Rate}, текущий курс {Rate_CB}.'
   WHERE T_METHODID = 100
     AND T_NAME = 'Доходы и расходы по депо комиссиям';

EXCEPTION 
  WHEN OTHERS THEN 
    it_log.log('Ошибка при изменении основания МВП');
    dbms_output.put_line('Ошибка при изменении основания МВП');
END;
/

BEGIN
  EXECUTE IMMEDIATE 'COMMIT';
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/
