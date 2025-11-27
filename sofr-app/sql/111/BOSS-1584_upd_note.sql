begin
  update dnotekind_dbt n
     set n.t_name = 'Результат обработки последнего неторгового поручения в QUIK'
   where n.t_objecttype = 131
     and n.t_notekind = 105;
end;
/