/*Добавить сообщение*/
BEGIN
   INSERT INTO DBANK_MSG (T_NUMBER, T_PAGE, T_CONTENTS )
   VALUES (26072,0,'Дата начала действия нового значения пересекается с периодом действия предыдущего значения');
EXCEPTION 
   WHEN DUP_VAL_ON_INDEX THEN NULL;
END;
/
