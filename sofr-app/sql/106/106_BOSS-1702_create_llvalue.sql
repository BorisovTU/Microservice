declare 
    vcnt number := 0;
    
    procedure AddLValue(pelement in number) is
    begin
        select count(*) into vcnt
         from dllvalues_dbt where t_list = 5003 and t_element = pelement;
        if vcnt = 0 then
            insert into dllvalues_dbt (T_LIST, T_ELEMENT, T_CODE, T_NAME, T_FLAG, T_NOTE, T_RESERVE)
            values  (5003, pelement, 'PmOpr'||pelement, 'Соответствие DocKind pmpaym и oproper', 450, 
                    'Соответствие DocKind pmpaym и oproper', chr(1));
        end if;
    end;
    
begin
    AddLValue(460);
    AddLValue(283);
    
    commit;
end;