BEGIN
update  dobjgroup_dbt  set t_name='Код типа ц.б. для формы 711 (до 01.01.2025)' where t_objecttype=12 and t_name='Код типа ц.б. для формы 711';
insert into dobjgroup_dbt values(12, 5, chr(88), 'Код типа ц.б. для формы 711', chr(88), 18, chr(1), chr(0), 0, chr(0), 0, 0, chr(0), chr(0), chr(0), chr(0), chr(0), chr(0), chr(1) );
insert into dobjattr_dbt 
   (select t_objecttype, 5, t_attrid, t_parentid, t_codelist, t_numinlist, t_nameobject, t_chattr, t_longattr, t_intattr, t_name, t_fullname, t_opendate, t_closedate, t_classificator, t_corractype, t_balance, t_isobject 
     from dobjattr_dbt  
   where t_objecttype=12 and t_groupid=1 
       and t_attrid not in (25, 32, 18, 19, 20, 21, 22, 23, 24, 33, 34, 35, 36, 37, 38, 39));
insert into dobjatcor_dbt  
  (select t_objecttype, 5, 
           case when t_attrid in (32, 18, 19, 20, 21, 22, 23, 24, 33, 34, 35, 36, 37, 38, 39) then 28 
                   when t_attrid = 25 then 47
                    else t_attrid
           end, 
           t_object, t_general, t_validfromdate, t_oper, t_validtodate, t_sysdate, t_systime, t_isauto, 0 
     from  dobjatcor_dbt 
  where t_objecttype=12 and t_groupid=1 );
update dobjatcor_dbt set t_attrid=46 where t_objecttype=12 and t_groupid=5 and t_attrid=47 and t_object in (
   select LPAD(t_fiid, 10, '0') from davoiriss_dbt 
     where t_isin in (
     'US67812M2070',
     'US3682872078',
     'US69343P1057',
     'US6698881090',
     'US55315J1025',
     'US67011E2046',
     'US8688612048',
     'US46630Q2021',
     'US8766292051',
     'US55953Q2021',
     'US8181503025',
     'US29843U2024',
     'US48122U2042',
     'US80585Y3080',
     'US36829G1076',
     'US5591892048',
     'US6074091090',
     'US71922G2093',
     'US73181M1172',
     'US7821834048',
     'US5838406081',
     'US71922G4073',
     'US71922G3083'
     )
   );
END;
/