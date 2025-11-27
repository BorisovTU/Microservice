begin 
       
    Insert into DNOTEKIND_DBT
       (T_OBJECTTYPE, T_NOTEKIND, T_NOTETYPE, T_NAME, T_KEEPOLDVALUES, T_NOTINUSE, T_ISPROTECTED, T_MAXLEN, T_NOTUSEFIELDUSE, T_MACRONAME, T_DECPL, T_ISPROGONLY)
     Values
       (207, 135, 7, 'Техническое закрытие', chr(0), 
        chr(0), chr(0), 0, chr(0), chr(1), 
        0, chr(0));        
    commit;
end;