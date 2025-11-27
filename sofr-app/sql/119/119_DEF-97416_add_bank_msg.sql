/*Добавить сообщение*/
BEGIN
   INSERT INTO DBANK_MSG (T_NUMBER, T_PAGE, T_CONTENTS )
   VALUES (26082,0,'Вы пытаетесь пересчитать несуществующую операцию');
EXCEPTION 
   WHEN DUP_VAL_ON_INDEX THEN NULL;
END;
/
