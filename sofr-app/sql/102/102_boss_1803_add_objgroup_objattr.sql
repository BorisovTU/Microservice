declare
   v_cnt number(10);
   v_objecttype number := 207;
   v_groupid number := 198;
begin
   select count(*) into v_cnt
     from DOBJGROUP_DBT 
    where T_OBJECTTYPE = v_objecttype and T_GROUPID = v_groupid;
 
   if v_cnt = 0 then
    Insert into DOBJGROUP_DBT
       (T_OBJECTTYPE, T_GROUPID, T_TYPE, T_NAME, T_SYSTEM, 
       T_ORDER, T_MACRONAME, T_KEEPOLDVALUES, T_UPDATEFLAG, T_SUCCESSOPFLAG, 
       T_ATTROBJECTTYPE, T_ATTRGROUPID, T_ISHIDDEN, T_FULLNAMEISBASIC, T_SYSHIDDEN, 
       T_NOTUSEFIELDUSE, T_ISMEANPARENTNODE, T_ISMANUALFIRST)
     Values
       (v_objecttype, v_groupid, chr(88), 'Расторжение договора БО по инициативе Банка', '', 
        v_groupid, chr(1), chr(88), 0, chr(0), 
        0, 0, chr(0), chr(0), chr(0), 
        chr(0), chr(0), chr(0));
        
    Insert into DOBJATTR_DBT
       (T_OBJECTTYPE, T_GROUPID, T_ATTRID, T_PARENTID, T_CODELIST, 
       T_NUMINLIST, T_NAMEOBJECT, T_CHATTR, T_LONGATTR, T_INTATTR, 
       T_NAME, T_FULLNAME, T_OPENDATE, T_CLOSEDATE, T_CLASSIFICATOR,
       T_CORRACTYPE, T_BALANCE, T_ISOBJECT)
     Values
       (v_objecttype, v_groupid, 1, 0, chr(1), 
        '1', '1', chr(0), 0, 0, 
        'Да', 'Да', TO_DATE('01/01/0001 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), TO_DATE('01/01/0001 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), 0, 
        chr(1), chr(1), chr(0));
        
    Insert into DOBJATTR_DBT
       (T_OBJECTTYPE, T_GROUPID, T_ATTRID, T_PARENTID, T_CODELIST, 
       T_NUMINLIST, T_NAMEOBJECT, T_CHATTR, T_LONGATTR, T_INTATTR, 
       T_NAME, T_FULLNAME, T_OPENDATE, T_CLOSEDATE, T_CLASSIFICATOR,
       T_CORRACTYPE, T_BALANCE, T_ISOBJECT)
     Values
       (v_objecttype, v_groupid, 2, 0, chr(1), 
        '2', '2', chr(0), 0, 0, 
        'Нет', 'Нет', TO_DATE('01/01/0001 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), TO_DATE('01/01/0001 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), 0, 
        chr(1), chr(1), chr(0));

    COMMIT;
  end if;
  
EXCEPTION
  WHEN OTHERS THEN NULL;
end;
/
