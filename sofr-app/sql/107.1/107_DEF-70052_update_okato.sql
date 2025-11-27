declare 
   vattrid number;
   C_OBJECTTYPE number := 3;
   C_GROUPID number := 12;
   C_ADDRESS_TYPE number := 5;
   v_cnt number := 0;
   v_cnt_all number := 0;
   v_date_contr date;
begin
   for datax in (select * from dadress_dbt a
                  where nvl(a.t_okato,chr(1)) <> chr(1) and nvl(a.t_country, chr(1)) ='RUS'
                    and t_type = C_ADDRESS_TYPE
                    and not exists (select 1 from dobjatcor_dbt o 
                                     where o.t_objecttype = C_OBJECTTYPE and o.t_groupid = C_GROUPID and o.t_validtodate = to_date('31129999','ddmmyyyy') 
                                       and o.t_object = lpad(a.t_partyid, 10,'0') )
                    ) loop
      v_cnt_all := v_cnt_all + 1;
      begin
         vattrid := 0;
         begin
            select t_attrid into vattrid
              from dobjattr_dbt where t_objecttype = C_OBJECTTYPE and t_groupid = C_GROUPID and t_numinlist = rpad(datax.t_okato,11,'0');
         exception 
            when no_data_found then 
               /*dbms_output.put_line (datax.t_okato ||' ОКАТО не найдено');*/
               vattrid := 0;
         end;
         
         begin
            -- дата самого первого договора, неважно, открыт или закрыт
            select t_datebegin into v_date_contr
              from dsfcontr_dbt sf 
             where sf.t_partyid = datax.t_partyid and sf.t_servkind = 0
               and sf.t_id = (select min(t_id) from dsfcontr_dbt sf2 
                               where sf2.t_partyid = sf.t_partyid and sf2.t_servkind = 0);
         exception
            when no_data_found then 
               /*dbms_output.put_line (datax.t_partyid ||' договор не найден, установлена текущая дата');*/
               v_date_contr := trunc(sysdate);
         end;

         if vattrid > 0 then
            Insert into DOBJATCOR_DBT
               (T_OBJECTTYPE, T_GROUPID, T_ATTRID, T_OBJECT, T_GENERAL, T_VALIDFROMDATE, 
                T_OPER, T_VALIDTODATE, T_SYSDATE, T_SYSTIME, T_ISAUTO, T_ID)
             Values
               (C_OBJECTTYPE, C_GROUPID, vattrid, lpad(datax.t_partyid,10,'0'), 'X', v_date_contr, 
                1, TO_DATE('31/12/9999', 'dd/mm/yyyy'), sysdate, TO_DATE(to_char(sysdate, '"01/01/0001" HH24:MI:SS'),'dd.mm.yyyy hh24:mi:ss'), 'X', 0);
            
            v_cnt := v_cnt + 1;
         end if;
      exception
        when others then 
            dbms_output.put_line(sqlerrm);
      end;
      
   end loop;
   /*dbms_output.put_line('Всего незаполненных категорий ОКАТО: '||v_cnt_all);
   dbms_output.put_line('Обновлено категорий ОКАТО: '||v_cnt);*/
    
end;