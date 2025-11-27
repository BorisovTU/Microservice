/*Добавить сообщение*/
BEGIN
   INSERT INTO DBANK_MSG (T_NUMBER, T_PAGE, T_CONTENTS )
   VALUES (26074,0,'Есть запись с более поздним периодом действия');
EXCEPTION 
   WHEN DUP_VAL_ON_INDEX THEN NULL;
END;
/

BEGIN
   INSERT INTO DBANK_MSG (T_NUMBER, T_PAGE, T_CONTENTS )
   VALUES (26075,0,'Нельзя удалить единственную запись');
EXCEPTION 
   WHEN DUP_VAL_ON_INDEX THEN NULL;
END;
/
