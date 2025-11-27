begin
    Insert into DLLVALUES_DBT
       (T_LIST, T_ELEMENT, T_CODE, T_NAME, T_FLAG, T_NOTE, T_RESERVE)
     Values
       (5009, 45, '45', 'CdiError', 1, 
        'Ошибка обработки данных AC CDI', chr(1));

    commit;
end;