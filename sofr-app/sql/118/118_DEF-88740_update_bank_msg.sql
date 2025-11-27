BEGIN
  UPDATE dbank_msg SET T_CONTENTS = 'Ошибка Active Directory: Код: %s. Описание: %s.' WHERE t_number = 6069;
  UPDATE dbank_msg SET T_CONTENTS = 'Ошибка Active Directory: Функция: %s. Код: %s. Описание: %s.' WHERE t_number = 6070;
  UPDATE dbank_msg SET T_CONTENTS = 'Ошибка Active Directory: Функция: %s. Код: %s. Описание: %s. Параметры: %s' WHERE t_number = 6071;
EXCEPTION 
   WHEN DUP_VAL_ON_INDEX THEN NULL;
END;
/



