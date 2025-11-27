BEGIN
  UPDATE DBANK_MSG
    SET t_Contents = 'Откат операции расчета НОБ с установленным признаком "Пересчет" отключен настройками реестра'
    WHERE t_Number = 20650;
                     
  COMMIT;
END;