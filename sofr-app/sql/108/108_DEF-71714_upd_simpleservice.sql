/*Включить сервис*/
BEGIN
  UPDATE dsimpleservice_dbt
     SET t_IsActive = 'X'
   WHERE t_Name = 'Обработка выплат по погашению ц/б из Диасофта';
END;
/
