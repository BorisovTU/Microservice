/*Добавить сообщение*/
BEGIN
   INSERT INTO DBANK_MSG (T_NUMBER, T_PAGE, T_CONTENTS )
   VALUES (26087,0,'Необходимо указать хотя бы один из параметров условия');
EXCEPTION 
   WHEN DUP_VAL_ON_INDEX THEN NULL;
END;
/
