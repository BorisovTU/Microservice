/*Добавить сообщение*/
BEGIN
   INSERT INTO DBANK_MSG (T_NUMBER, T_PAGE, T_CONTENTS )
   VALUES (26076,0,'Уже существует аналогичная запись в указанный период действия');
EXCEPTION 
   WHEN DUP_VAL_ON_INDEX THEN NULL;
END;
/

BEGIN
   INSERT INTO DBANK_MSG (T_NUMBER, T_PAGE, T_CONTENTS )
   VALUES (26077,0,'Уже существует запись с таким приоритетом для данного вида дохода в указанный период действия');
EXCEPTION 
   WHEN DUP_VAL_ON_INDEX THEN NULL;
END;
/
