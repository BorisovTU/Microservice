/*Добавить сообщение*/
BEGIN
   INSERT INTO DBANK_MSG (T_NUMBER, T_PAGE, T_CONTENTS )
   VALUES (26088,0,'В текущем виде льготы уже есть аналогичное условие');
EXCEPTION 
   WHEN DUP_VAL_ON_INDEX THEN NULL;
END;
/
