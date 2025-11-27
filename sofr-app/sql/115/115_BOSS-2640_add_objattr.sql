declare

  v_cnt number(10);
  
begin

  select count(*) into v_cnt
     from DOBJATTR_DBT 
    where T_OBJECTTYPE = 5 and T_GROUPID = 2 and t_AttrID = 3;

   if v_cnt = 0 then
    Insert into DOBJATTR_DBT
      (T_OBJECTTYPE, T_GROUPID, T_ATTRID, T_PARENTID, T_CODELIST, T_NUMINLIST, T_NAMEOBJECT, T_CHATTR, T_LONGATTR, T_INTATTR, T_NAME, T_FULLNAME, T_OPENDATE, T_CLOSEDATE, T_CLASSIFICATOR, T_CORRACTYPE, T_BALANCE, T_ISOBJECT)
    Values
      (5, 2, 3, 0, CHR(1), 
       'ÖÄùë', 'ÖÄùë', CHR(0), 0, 0, 
       'ÖÄùë', 'ÖÄùë', TO_DATE('01.01.0001 00:00:00', 'MM.DD.YYYY HH24:MI:SS'), TO_DATE('01.01.0001 00:00:00', 'MM.DD.YYYY HH24:MI:SS'), 0, 
       CHR(1), CHR(1), CHR(0));
  
  end if;

end;
/