/*Добавить сообщение*/
BEGIN
   INSERT INTO DBANK_MSG (T_NUMBER, T_PAGE, T_CONTENTS )
   VALUES (26090,0,'Уже существует вид льготы % с аналогичными условиями, действующий в пересекающемся периоде');
EXCEPTION 
   WHEN DUP_VAL_ON_INDEX THEN NULL;
END;
/
