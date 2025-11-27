-- Удаление пункта меню "Отчет по СНОБ и удержанному НДФЛ". Дефект DEF-52968.
declare
   v_cnt NUMBER;
begin
   delete
   from dmenuitem_dbt
   where t_ICaseItem = 20209
     and t_IIdentProgram = 83
   returning count(1) into v_cnt;

   commit;
     
   it_log.log('DEF-52968: delete was successfull. Deleted ' || v_cnt || ' rows');
exception
   when NO_DATA_FOUND
   then it_log.log('DEF-52968: ERROR NO_DATA_FOUND');
end;
/