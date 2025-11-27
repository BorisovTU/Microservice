declare                         
    v_objecttype    number(10); 
    v_type             number(10); 
    v_status_err    number(10); 
begin                                 
    v_objecttype     := 4;  
    v_type              := 3;  
    v_status_err     := 5;
    for c in (select u.t_timestamp, u.t_objectid from utableprocessevent_dbt u where  u.t_objecttype = v_objecttype and u.t_type = v_type and u.t_status in (v_status_err)
                and exists (select 1 from daccount_dbt a where a.t_accountid in (u.t_objectid) and a.t_close_date not in (to_date('01.01.0001','dd.mm.yyyy')))
        ) loop                                                                                           
        update daccount_dbt set t_close_date = c.t_timestamp where t_accountid = c.t_objectid; -- ―€₯©β € βλ                                               
    end loop;       
    commit;                                                                                                               
end;/      