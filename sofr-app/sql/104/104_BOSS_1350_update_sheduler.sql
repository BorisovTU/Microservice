/* BOSS-1350 Отключить планировщик по умолчанию */
begin
   update dss_sheduler_dbt
      SET t_shedulertype = 0
    where t_id = 10056;
   commit;
end;