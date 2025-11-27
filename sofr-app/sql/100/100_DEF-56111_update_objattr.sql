/* обновление категории */
declare
   v_cnt number(10);
   v_objecttype number := 3;
   v_groupid number := 150;
begin
   update DOBJATTR_DBT set t_numinlist = t_name,  t_nameobject = t_name
   where T_OBJECTTYPE = v_objecttype and T_GROUPID = v_groupid;
   commit;
end;