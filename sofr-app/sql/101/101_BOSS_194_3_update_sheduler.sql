/* BOSS-194 BOSS-2010 Отключить планировщик по умолчанию */
begin
   update dss_sheduler_dbt
      SET t_shedulertype = 0
    where t_id = 10052;
   commit;
end;