/*Добавить сообщение*/
BEGIN
   INSERT INTO DBANK_MSG (T_NUMBER, T_PAGE, T_CONTENTS )
   VALUES (26091,0,'У вида льготы отсутствуют условия. Необходимо задать хотя бы одно условие.');
EXCEPTION 
   WHEN DUP_VAL_ON_INDEX THEN NULL;
END;
/
