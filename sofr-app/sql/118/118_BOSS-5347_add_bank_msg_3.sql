/*Добавить сообщение*/
BEGIN
   INSERT INTO DBANK_MSG (T_NUMBER, T_PAGE, T_CONTENTS )
   VALUES (26080,0,'Договор ИИС является закрытым');
EXCEPTION 
   WHEN DUP_VAL_ON_INDEX THEN NULL;
END;
/
