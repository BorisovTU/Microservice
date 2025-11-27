/* BOSS-194 DEF-62306 Отключить планировщик  */
begin
   update dss_sheduler_dbt
      SET t_shedulertype = 0
    where t_id = 10052;
   commit;
end;