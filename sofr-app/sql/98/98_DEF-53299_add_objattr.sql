declare
   v_cnt number(10);
   v_objecttype number := 207;
   v_groupid number := 125;
begin
   select count(*) into v_cnt
     from DOBJATTR_DBT 
    where T_OBJECTTYPE = v_objecttype and T_GROUPID = v_groupid and T_ATTRID = 10;
 
   if v_cnt = 0 then
    Insert into DOBJATTR_DBT
       (T_OBJECTTYPE, T_GROUPID, T_ATTRID, T_PARENTID, T_CODELIST, 
       T_NUMINLIST, T_NAMEOBJECT, T_CHATTR, T_LONGATTR, T_INTATTR, 
       T_NAME, T_FULLNAME, T_OPENDATE, T_CLOSEDATE, T_CLASSIFICATOR,
       T_CORRACTYPE, T_BALANCE, T_ISOBJECT)
     Values
       (v_objecttype, v_groupid, 10, 0, chr(1), 
        '0', 'NULL', chr(0), 0, 0, 
        'Не установлено', 'Не установлено', TO_DATE('01/01/0001 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), TO_DATE('01/01/0001 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), 0, 
        chr(1), chr(1), chr(0));

    COMMIT;
  end if;
  
EXCEPTION
  WHEN OTHERS THEN NULL;
end;
/
