-- Обнуление цены
begin
  update doverstock_rest_dbt t
     set t.t_price = 0
   where t.t_amount = 0
     and t.t_price > 0;
  commit;
end;
/
