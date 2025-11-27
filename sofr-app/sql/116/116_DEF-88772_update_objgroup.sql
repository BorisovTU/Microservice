declare
begin
  update dobjgroup_dbt 
     set t_macroname = 'catEditActions.mac' 
   where t_objecttype = 659 
     and t_groupid = 6;
end;
/