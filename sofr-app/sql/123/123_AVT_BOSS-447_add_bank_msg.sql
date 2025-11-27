/*Добавить сообщение*/
BEGIN
   INSERT INTO DBANK_MSG (T_NUMBER, T_PAGE, T_CONTENTS )
   VALUES (26083,0,'Вид НДР с таким кодом дохода не существует');
EXCEPTION 
   WHEN DUP_VAL_ON_INDEX THEN NULL;
END;
/
