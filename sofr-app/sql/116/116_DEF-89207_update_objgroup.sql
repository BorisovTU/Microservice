declare
begin
  update dobjgroup_dbt 
     set t_macroname = chr(1)
   where t_objecttype = 659 
     and t_groupid = 6;
end;
/