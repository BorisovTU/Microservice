begin
update dss_sheduler_dbt s
   set s.t_name        = 'Выгрузка ini-файлов в ИТС QUIK'
      ,s.t_description = 'Выгрузка ini-файлов в ИТС QUIK'
 where s.t_id = 10035;
update dss_func_dbt s
   set s.t_name        = 'Выгрузка ini-файлов в ИТС QUIK'
 where s.t_id = 10035 ;
update DSIMPLESERVICE_DBT s
   set s.t_name        = 'Выгрузка ini-файлов в ИТС QUIK'
      ,s.t_description = 'Выгрузка ini-файлов в ИТС QUIK'
 where s.t_id = 10035;
end;
/