/*DEF-57210 адреса получателей*/
declare
    vcnt number;
begin  
    select count(*) into vcnt
    from DLLVALUES_DBT where t_list = 5009 and t_element = 47;
    
    if vcnt > 0 then
        delete from usr_email_addr_dbt where t_group= 47;
        insert into usr_email_addr_dbt(t_group, t_email, t_place, t_comment)
        values(47, 'BACKOFFICE@RSHB.RU','R',chr(1));
        insert into usr_email_addr_dbt(t_group, t_email, t_place, t_comment)
        values(47, 'BO_SECURITIES@RSHB.RU','R',chr(1));        
    end if;

    commit;
end;