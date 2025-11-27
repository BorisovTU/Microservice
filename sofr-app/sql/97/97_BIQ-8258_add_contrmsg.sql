begin
    Insert into DLLVALUES_DBT
       (T_LIST, T_ELEMENT, T_CODE, T_NAME, T_FLAG, T_NOTE, T_RESERVE)
     Values
       (1143, 507, '507', 'Уведомление клиенту о необходимости предоставления документов по ИИС', 507, 
        'Уведомление клиенту о необходимости предоставления документов по ИИС', chr(1));

    Insert into DLLVALUES_DBT
       (T_LIST, T_ELEMENT, T_CODE, T_NAME, T_FLAG, T_NOTE, T_RESERVE)
     Values
       (1143, 508, '508', 'Уведомление клиенту о расторжении договора ИИС в связи с непред. док.', 508, 
        'Уведомление клиенту о расторжении договора ИИС в связи с непредоставлением документов', chr(1));
    COMMIT;
end;
/


