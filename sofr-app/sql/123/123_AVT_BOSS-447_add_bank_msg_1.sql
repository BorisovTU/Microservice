/*Добавить сообщение*/
BEGIN
   INSERT INTO DBANK_MSG (T_NUMBER, T_PAGE, T_CONTENTS )
   VALUES (26084,0,'Введенный вид льготируемого дохода уже существует');
EXCEPTION 
   WHEN DUP_VAL_ON_INDEX THEN NULL;
END;
/
