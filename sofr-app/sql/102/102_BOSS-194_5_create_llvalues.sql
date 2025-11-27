declare 
   vcnt number;
   v_list number := 1143;
   v_num number := 513;
begin
   select count(*) into vcnt 
   from DLLVALUES_DBT 
   where t_list = v_list
     and T_ELEMENT = v_num;

   if vcnt = 0 then
    Insert into DLLVALUES_DBT
       (T_LIST, T_ELEMENT, T_CODE, T_NAME, T_FLAG, T_NOTE, T_RESERVE)
     Values
       (1143, v_num, to_char(v_num), 'Уведомление об исключении лица из реестра квал.инвесторов', v_num, 
        'Уведомление об исключении лица из реестра лиц, признанных квалифицированными инвесторами', chr(1));
   end if;


   COMMIT;
end;