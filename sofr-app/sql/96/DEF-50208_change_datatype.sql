/*DEF-50208 BIQ-7294 переименование сервиса*/
begin
    update DDATATYPEWS_DBT set T_PARMIP = 'SendTaxRecalculationSOFR' where  T_PARMIP = 'SendSNOBRecalculationSOFR';
    commit;
end;