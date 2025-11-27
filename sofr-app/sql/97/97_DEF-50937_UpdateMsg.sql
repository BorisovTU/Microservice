BEGIN
  UPDATE DBANK_MSG
    SET t_Contents = 'Откат операции расчета НОБ с установленным признаком "Пересчет" отключен настройками реестра'
    WHERE t_Number = 20650
      AND t_Page = 0;
                     
  COMMIT;
END;