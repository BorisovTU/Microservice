declare 
   vcnt number;
   v_list number := 1143;
   v_num number := 511;
begin
   select count(*) into vcnt 
   from DLLVALUES_DBT 
   where t_list = v_list
     and T_ELEMENT = v_num;

   if vcnt = 0 then
    Insert into DLLVALUES_DBT
       (T_LIST, T_ELEMENT, T_CODE, T_NAME, T_FLAG, T_NOTE, T_RESERVE)
     Values
       (1143, v_num, to_char(v_num), 'Требование о подтверждении статуса квал.инвестора', v_num, 
        'Требование о подтверждении статуса квалифицированного инвестора', chr(1));
   end if;

   v_num := 512;
   select count(*) into vcnt 
   from DLLVALUES_DBT 
   where t_list = v_list
     and T_ELEMENT = v_num;

   if vcnt = 0 then
    Insert into DLLVALUES_DBT
       (T_LIST, T_ELEMENT, T_CODE, T_NAME, T_FLAG, T_NOTE, T_RESERVE)
     Values
       (1143, v_num, to_char(v_num), 'Уведомление об истечении статуса квал.инвестора', v_num, 
        'Уведомление об истечении статуса квалифицированного инвестора', chr(1));
   end if;

   COMMIT;
end;