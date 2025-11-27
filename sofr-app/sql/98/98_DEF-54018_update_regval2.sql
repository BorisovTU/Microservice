declare 
    vkeyid number;
begin
    select k.t_keyid into vkeyid
     from 
        dregparm_dbt k,
        dregparm_dbt p
    where k.t_parentid = p.t_keyid
    and upper(k.t_Name) = upper('€Š’ˆ‚Ž') 
    and upper(p.t_Name) = upper('‚‡€ˆŒŽ„…‰‘’‚ˆ… ‘ AC CDI') ;
    
    update dregparm_dbt set t_global = chr(88)
     where t_keyid = vkeyid; 
    
    commit;
exception
    when others then
        rollback;
        it_error.put_error_in_stack;
        it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
end;