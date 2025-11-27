begin
    update dss_sheduler_dbt
        set  t_name = 'Обмен данными с AC CDI'
    where t_name = 'Обмен данными с AC CDI - исходящий';
    
    commit;
end;