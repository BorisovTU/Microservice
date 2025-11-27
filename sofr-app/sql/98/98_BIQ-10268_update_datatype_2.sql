-- Обмен с CDI - входящий
begin
    update DDATATYPEWS_DBT set T_PARMBO = 'req_distributor_in' 
     where T_PARMIP = 'CdiPublishOrgSOFR';
     commit;
end;