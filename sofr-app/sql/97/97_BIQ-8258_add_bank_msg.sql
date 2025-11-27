BEGIN
  INSERT INTO DBANK_MSG (T_NUMBER,T_PAGE,T_CONTENTS)
                 VALUES (20649,0,'По Договору клиентом не предоставлены документы о расторжении договора ИИС, заключенного с другим ПУ. Выполнение операции запрещено');

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