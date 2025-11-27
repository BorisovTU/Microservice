/*Добавить сообщение*/
BEGIN
   INSERT INTO DBANK_MSG (T_NUMBER, T_PAGE, T_CONTENTS )
   VALUES (26085,0,'Название вида льготы должно соответствовать формату Л_номер вида льготы (Пример: Л_1)');
EXCEPTION 
   WHEN DUP_VAL_ON_INDEX THEN NULL;
END;
/
