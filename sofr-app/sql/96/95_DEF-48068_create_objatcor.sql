declare 

   c_groupid constant number := 170;
   c_objecttype constant number := 3;
   
   procedure CreateObjAtrr(p_groupid in number, p_objecttype in number, p_attrid in number, p_attrname in varchar2) is
       cnt number;
   begin
         select count(*) into cnt from DOBJATTR_DBT where T_OBJECTTYPE = p_objecttype and T_GROUPID = p_groupid and t_attrid = p_attrid;
         if cnt = 0 then
             Insert into DOBJATTR_DBT
                (T_OBJECTTYPE, T_GROUPID, T_ATTRID, T_PARENTID, T_CODELIST, 
                T_NUMINLIST, T_NAMEOBJECT, T_CHATTR, T_LONGATTR, T_INTATTR, 
                T_NAME, T_FULLNAME, T_OPENDATE, T_CLOSEDATE, T_CLASSIFICATOR,
                T_CORRACTYPE, T_BALANCE, T_ISOBJECT)
              Values
                (p_objecttype, p_groupid, p_attrid, 0, chr(1), 
                 to_char(p_attrid), to_char(p_attrid), chr(0), 0, 0, 
                 p_attrname, p_attrname, TO_DATE('01/01/0001 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), TO_DATE('01/01/0001 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), 0, 
                 chr(1), chr(1), chr(0));
         end if;  
   end;

begin
   CreateObjAtrr(c_groupid, c_objecttype, 3, 'Выгружено закрытие');
   CreateObjAtrr(c_groupid, c_objecttype, 4, 'Ошибка выгрузки закрытия');
   COMMIT;
end;
