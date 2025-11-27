-- BOSS-194 BOSS-2050 Обновить планировщик 
BEGIN
  UPDATE dss_sheduler_dbt
     SET t_workEndTime = TO_DATE('01/01/0001 23:59:59', 'MM/DD/YYYY HH24:MI:SS')
   WHERE t_id = 10052;
   commit;
END;
/