DECLARE
   v_OldContents VARCHAR2(512);
BEGIN
   it_log.log('Insert into dbank_msg t_Number = 20650, t_Contents = ''Откат операции расчета НОБ с установленным признаком "Пересчет" отключен настройками реестра''');
   EXECUTE IMMEDIATE 'INSERT INTO DBANK_MSG ( ' ||
                     '                         t_Number, ' ||
                     '                         t_Page, ' ||
                     '                         t_Contents ' ||
                     '                      ) ' ||
                     '               VALUES ( ' ||
                     '                         20650, ' ||
                     '                         0, ' ||
                     '                         ''Откат операции расчета НОБ с установленным признаком "Пересчет" отключен настройками реестра'' ' ||
                     '                      ) ';
                     
   COMMIT;
EXCEPTION
   WHEN DUP_VAL_ON_INDEX
   THEN
      SELECT t_Contents INTO v_OldContents
      FROM DBANK_MSG
      WHERE t_Number = 20650;

      it_log.log('Update dbank_msg t_Number = 20650, v_OldContents = ' || v_OldContents || ', t_Contents = ''Откат операции расчета НОБ с установленным признаком "Пересчет" отключен настройками реестра''');

      EXECUTE IMMEDIATE 'UPDATE DBANK_MSG ' ||
                        'SET t_Contents = ''Откат операции расчета НОБ с установленным признаком "Пересчет" отключен настройками реестра'' ' ||
                        'WHERE t_Number = 20650 ';

      COMMIT;
END;
/