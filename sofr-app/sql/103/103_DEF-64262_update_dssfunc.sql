/*таймаут для планировщика DEF-64262*/
declare 
    c_timeout number := 600;
begin
    update dss_func_dbt
        set t_timeout = c_timeout
   where t_service in (10052,10053);
   
   commit;
end;
