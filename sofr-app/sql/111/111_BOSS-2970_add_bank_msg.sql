/*Добавить сообщение*/
BEGIN
   INSERT INTO DBANK_MSG (T_NUMBER, T_PAGE, T_CONTENTS )
   VALUES (26061,0,'Запись с такими параметрами уже существует');
EXCEPTION 
   WHEN DUP_VAL_ON_INDEX THEN NULL;
END;
/

BEGIN
   INSERT INTO DBANK_MSG (T_NUMBER, T_PAGE, T_CONTENTS )
   VALUES (26062,0,'Вводимая дата не является началом налогового периода');
EXCEPTION 
   WHEN DUP_VAL_ON_INDEX THEN NULL;
END;
/

BEGIN
   INSERT INTO DBANK_MSG (T_NUMBER, T_PAGE, T_CONTENTS )
   VALUES (26063,0,'Вводимая дата не является окончанием налогового периода');
EXCEPTION 
   WHEN DUP_VAL_ON_INDEX THEN NULL;
END;
/

BEGIN
   INSERT INTO DBANK_MSG (T_NUMBER, T_PAGE, T_CONTENTS )
   VALUES (26064,0,'Запись для данного вида НОБ уже существует');
EXCEPTION 
   WHEN DUP_VAL_ON_INDEX THEN NULL;
END;
/
