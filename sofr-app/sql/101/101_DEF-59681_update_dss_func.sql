/*DEF-59681 неверное название процедуры */
begin
    update dss_func_dbt  set t_executorfunc = 'ws_check_end_qi'
     where t_service  = 10052;
     
     commit;
end;