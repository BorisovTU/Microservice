BEGIN
  UPDATE DREGPARM_DBT
     SET T_DESCRIPTION = 'Количество рабочих дней, через которое клиенту отправляются повторные сообщения'
   WHERE LOWER(T_NAME) = LOWER('ПОВТОРНОЕ СООБЩ. КЛИЕНТУ ИИС'); 
EXCEPTION 
  WHEN OTHERS THEN NULL;
END;
/

BEGIN
  EXECUTE IMMEDIATE 'COMMIT';
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/