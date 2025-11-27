begin
insert into dnotekind_dbt (t_objecttype,t_notekind,t_notetype,t_name,t_keepoldvalues,t_notinuse,t_isprotected,t_maxlen,t_notusefielduse,t_macroname,t_decpl,t_isprogonly)
                   values (131,106,7,'Признак выгрузки корректировки по нулевым лимитам в файл',chr(0),chr(0),chr(0),0,chr(0),chr(1),0,chr(0)) ;
exception
  when dup_val_on_index then
    null;
end;
/
