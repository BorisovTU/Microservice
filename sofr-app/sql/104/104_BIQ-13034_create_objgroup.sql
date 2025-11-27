declare

  procedure AddObjGroup (p_objecttype in number, p_groupid in number, p_name in varchar2) is
     v_cnt number(10);
  begin
     select count(*) into v_cnt
       from DOBJGROUP_DBT 
      where T_OBJECTTYPE = p_objecttype and T_GROUPID = p_groupid;

     if v_cnt = 0 then
      Insert into DOBJGROUP_DBT
         (T_OBJECTTYPE, T_GROUPID, T_TYPE, T_NAME, T_SYSTEM, 
         T_ORDER, T_MACRONAME, T_KEEPOLDVALUES, T_UPDATEFLAG, T_SUCCESSOPFLAG, 
         T_ATTROBJECTTYPE, T_ATTRGROUPID, T_ISHIDDEN, T_FULLNAMEISBASIC, T_SYSHIDDEN, 
         T_NOTUSEFIELDUSE, T_ISMEANPARENTNODE, T_ISMANUALFIRST)
       Values
         (p_objecttype, p_groupid, 'X', p_name, '', 
          p_groupid, chr(1), chr(88), 0, chr(0), 
          0, 0, chr(0), chr(0), chr(0), 
          chr(0), chr(0), chr(0));
          
      Insert into DOBJATTR_DBT
         (T_OBJECTTYPE, T_GROUPID, T_ATTRID, T_PARENTID, T_CODELIST, 
         T_NUMINLIST, T_NAMEOBJECT, T_CHATTR, T_LONGATTR, T_INTATTR, 
         T_NAME, T_FULLNAME, T_OPENDATE, T_CLOSEDATE, T_CLASSIFICATOR,
         T_CORRACTYPE, T_BALANCE, T_ISOBJECT)
       Values
         (p_objecttype, p_groupid, 1, 0, chr(1), 
          '1', '1', chr(0), 0, 0, 
          'Да', 'Да', TO_DATE('01/01/0001 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), TO_DATE('01/01/0001 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), 0, 
          chr(1), chr(1), chr(0));
          
      Insert into DOBJATTR_DBT
         (T_OBJECTTYPE, T_GROUPID, T_ATTRID, T_PARENTID, T_CODELIST, 
         T_NUMINLIST, T_NAMEOBJECT, T_CHATTR, T_LONGATTR, T_INTATTR, 
         T_NAME, T_FULLNAME, T_OPENDATE, T_CLOSEDATE, T_CLASSIFICATOR,
         T_CORRACTYPE, T_BALANCE, T_ISOBJECT)
       Values
         (p_objecttype, p_groupid, 2, 0, chr(1), 
          '2', '2', chr(0), 0, 0, 
          'Нет', 'Нет', TO_DATE('01/01/0001 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), TO_DATE('01/01/0001 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), 0, 
          chr(1), chr(1), chr(0));
    end if;
  end;
  
begin

  -- рассчитываем на то, что категории создаются редко и 103 свободна
  AddObjGroup (101, 213, 'Отказано в проведении операции');
  AddObjGroup (101, 215, 'Ц/б достаточно для списания');

  commit;
end;