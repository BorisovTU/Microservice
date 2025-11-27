/*обновление типа данных*/
declare 
    c_UpdateBrokContract constant number := 16;
begin
    update DDATATYPEWS_DBT set t_parmbo = 'req_distributor_in' where t_id = c_UpdateBrokContract;
    commit;
end;
   
