BEGIN
  UPDATE DBANK_MSG 
     SET T_CONTENTS = 'Ошибка при отправке сообщения по договору: %s'
   WHERE T_NUMBER = 13400;
END;
/