/* обновление категории */
declare
   v_cnt number(10);
   v_objecttype number := 3;
   v_groupid number := 150;
   v_attrid number := 4;
   v_name_new varchar2(10) := 'PP';
begin
   update DOBJATTR_DBT set t_numinlist = v_name_new,  t_nameobject = v_name_new, t_name = v_name_new
   where T_OBJECTTYPE = v_objecttype and T_GROUPID = v_groupid and t_attrid = v_attrid;
   commit;
end;