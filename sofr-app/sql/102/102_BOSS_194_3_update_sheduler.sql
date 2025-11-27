/* BOSS-194 BOSS-2050 Включить планировщик  */
begin
   update dss_sheduler_dbt
      SET t_shedulertype = 2
    where t_id = 10052;
   commit;
end;