declare
    pelement number;
    psysname varchar2(15) := 'CDI';

    procedure InsDataType (pservice in varchar2, psys in number, pparmbo in varchar2 default 'empty') is
       cnt number;
    begin 
       select count(t_id) into cnt from DDATATYPEWS_DBT where T_PARMIP = pservice;
       if cnt = 0 then
           Insert into DDATATYPEWS_DBT
              (T_ID, T_TYPE, T_ACCOUNTERPARTY, T_PARMIP, T_PARMBO, T_TIMEOUT, T_ISMACINFO)
            Values
              (0, 3, psys, pservice, 'empty', 500, 'X');
       end if;
    end;

begin
    -- система точно уже есть
    select t_element into pelement from DLLVALUES_DBT where t_list = 4067 and upper(t_code) = psysname;
    
    InsDataType ('CdiPublishOrgSOFR', pelement, 'req_distributor_in');
    commit;
end;