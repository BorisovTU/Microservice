begin
    Insert into DLLVALUES_DBT
       (T_LIST, T_ELEMENT, T_CODE, T_NAME, T_FLAG, T_NOTE, T_RESERVE)
     Values
       (5009, 46, '46', 'CdiDublicateRecords', 1, 
        'Дублирование данных AC CDI', chr(1));

    insert into usr_email_addr_dbt(t_group, t_email, t_place, t_comment)
    values(46, 'sofr@go.rshbank.ru','R',chr(1));

    commit;
end;