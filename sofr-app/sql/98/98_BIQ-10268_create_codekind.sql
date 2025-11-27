declare 
    vcnt number;
    c_objecttype CONSTANT number := 3;
    c_codekind CONSTANT number := 110;
    c_shortname varchar2(4 byte) := 'CDI';
    c_name varchar2(80 byte) := 'Код AC CDI';
    c_definition  varchar2(200 byte) := 'Код AC CDI';
begin
    select count(*) into vcnt from DOBJKCODE_DBT
    where T_OBJECTTYPE = c_objecttype and t_codekind = c_codekind;
    
    if vcnt = 0 then
        Insert into DOBJKCODE_DBT
           (T_OBJECTTYPE, 
            T_CODEKIND, T_NAME, T_SHORTNAME, T_MAXCODELEN, T_CASESENSITIVE, 
            T_DEFINITION, T_MACROFILE, T_MACROPROC, T_DUPLICATE)
         Values
           (c_objecttype, c_codekind, c_name, c_shortname, 35, 
            chr(0), c_definition, chr(1), chr(1), chr(0));
       Insert into DOBJALCOD_DBT
          (T_OBJECTTYPE, 
           T_OBJECTKIND, T_OBJSUBKIND, T_CODEKIND)
        Values
          (c_objecttype, 0, 0, c_codekind);
        COMMIT;
    end if;
end; 