/*Добавить сообщение*/
BEGIN
   INSERT INTO DBANK_MSG (T_NUMBER, T_PAGE, T_CONTENTS )
   VALUES (26089,0,'В виде льготы %s уже есть аналогичное условие');
EXCEPTION 
   WHEN DUP_VAL_ON_INDEX THEN NULL;
END;
/
