/*Добавить сообщение*/
BEGIN
   INSERT INTO DBANK_MSG (T_NUMBER, T_PAGE, T_CONTENTS )
   VALUES (26086,0,'Дата окончания применения льготы должна быть больше даты начала применения льготы');
EXCEPTION 
   WHEN DUP_VAL_ON_INDEX THEN NULL;
END;
/
