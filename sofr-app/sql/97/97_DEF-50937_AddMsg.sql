BEGIN
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
END;