/*Добавить сообщение*/
BEGIN
   INSERT INTO DBANK_MSG (T_NUMBER, T_PAGE, T_CONTENTS )
   VALUES (26078,0,'Есть более поздние операции расчета НОБ');
EXCEPTION 
   WHEN DUP_VAL_ON_INDEX THEN NULL;
END;
/
