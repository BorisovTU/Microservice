-- Исправление некорректных наименований счетов 51*, 61*
DECLARE
BEGIN
  UPDATE dAccount_dbt
     SET t_nameAccount = REPLACE(t_nameAccount, 'ЦБ Клиента, ВУ', 'ЦБ-3 Места хранения ЦБ клиентов, ВУ')
   WHERE t_balance = 51
     AND t_nameAccount like '% (ЦБ Клиента, ВУ)';
  
  UPDATE dAccount_dbt
     SET t_nameAccount = REPLACE(t_nameAccount, 'ЦБ, Расч. с клиентом, ВУ', 'ЦБ-3 Клиента, ВУ')
   WHERE t_balance = 61
     AND t_nameAccount like '% (ЦБ, Расч. с клиентом, ВУ)';
END;
/