begin
  update dss_sheduler_dbt s
     set s.t_workendtime = to_date('01.01.0001 23:59:59', 'dd.mm.yyyy hh24:mi:ss')
        ,s.t_periodtype = 4
        ,s.t_period = 1
        ,s.t_shedulertype = 2
   where s.t_id = 10094;
end;
