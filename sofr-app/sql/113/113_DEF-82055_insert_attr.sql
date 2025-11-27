BEGIN
insert into dobjatcor_dbt
  (select t_objecttype, 5, 
           case when t_attrid in (32, 18, 19, 20, 21, 22, 23, 24, 33, 34, 35, 36, 37, 38, 39) then 28 
                else t_attrid
           end, 
           t_object, t_general, t_validfromdate, t_oper, t_validtodate, t_sysdate, t_systime, t_isauto, 0 
     from  dobjatcor_dbt atcor
  where t_objecttype=12 and t_attrid != 25 and t_groupid=1 and not exists (select 1 from dobjatcor_dbt atcor2 where atcor2.t_objecttype=12 and atcor2.t_object=atcor.t_object and t_groupid=5));
END;
/