declare
    vcnt number;
begin  
    select count(*) into vcnt
    from DLLVALUES_DBT where t_list = 5009 and t_element = 47;
    
    if vcnt = 0 then
        Insert into DLLVALUES_DBT
           (T_LIST, T_ELEMENT, T_CODE, T_NAME, T_FLAG, T_NOTE, T_RESERVE)
         Values
           (5009, 47, '47', 'CdiChangeResidency', 1, 
            'Смена признака резидента AC CDI', chr(1));

        insert into usr_email_addr_dbt(t_group, t_email, t_place, t_comment)
        values(47, 'sofr@go.rshbank.ru','R',chr(1));
    end if;

    commit;
end;