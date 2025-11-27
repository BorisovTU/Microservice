begin
   delete
   from DMENUITEM_DBT
   where t_IProgItem = 74
     and t_ICaseItem = 5;
     
   delete
   from DITEMUSER_DBT
   where t_CIdentProgram = 'J'
     and t_ICaseItem = 5;
end;