/*Добавить сообщение*/
BEGIN
   INSERT INTO DBANK_MSG (T_NUMBER, T_PAGE, T_CONTENTS )
   VALUES (26081,0,'Максимально допустимое значение равно 32767');
EXCEPTION 
   WHEN DUP_VAL_ON_INDEX THEN NULL;
END;
/
