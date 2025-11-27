-- Заполнение поля T_SORT
BEGIN
  update daccount_dbt a1
    set a1.t_sort = substr(a1.t_account,1,8)||substr(a1.t_account,10)
    where a1.t_sort != substr(a1.t_account,1,8)||substr(a1.t_account,10)  and a1.t_account like  '_________9900_______';
  commit;
end;
/
