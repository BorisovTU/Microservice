BEGIN
  UPDATE DBANK_MSG 
     SET T_CONTENTS = 'По Договору %s клиентом не предоставлены документы о расторжении договора ИИС, заключенного с другим ПУ. Выполнение операции запрещено'
   WHERE T_NUMBER = 20649;

EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/

BEGIN
  EXECUTE IMMEDIATE 'COMMIT';
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/