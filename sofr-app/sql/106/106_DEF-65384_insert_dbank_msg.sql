BEGIN
  INSERT INTO DBANK_MSG (T_NUMBER,T_PAGE,T_CONTENTS)
                 VALUES (13400,0,'Ошибка при отправке сообщения по договору: %s');

END;
/