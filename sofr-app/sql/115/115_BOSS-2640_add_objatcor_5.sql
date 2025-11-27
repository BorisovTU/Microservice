/*Беларусь*/
declare

  v_cnt number(10);
  
begin

  select count(*) into v_cnt
     from DOBJATCOR_DBT 
    where T_OBJECTTYPE = 5 and T_GROUPID = 2 and t_AttrID = 3 and t_Object = '0000000022';

  if v_cnt = 0 then
    Insert into DOBJATCOR_DBT
      (T_OBJECTTYPE, T_GROUPID, T_ATTRID, T_OBJECT, T_GENERAL, T_VALIDFROMDATE, T_OPER, T_VALIDTODATE, T_SYSDATE, T_SYSTIME, T_ISAUTO, T_ID)
    Values
      (5, 2, 3, '0000000022', 'X', 
       TO_DATE('01.01.2015 00:00:00', 'MM.DD.YYYY HH24:MI:SS'), 1, TO_DATE('12.31.9999 00:00:00', 'MM.DD.YYYY HH24:MI:SS'), TRUNC(SYSDATE), TO_DATE ('01.01.0001:'|| TO_CHAR (SYSDATE, 'HH24:MI:SS'),'DD.MM.YYYY:HH24:MI:SS'), 
       CHR(0), 0);
  
  end if;

end;
/

/*Армения*/
declare

  v_cnt number(10);
  
begin

  select count(*) into v_cnt
     from DOBJATCOR_DBT 
    where T_OBJECTTYPE = 5 and T_GROUPID = 2 and t_AttrID = 3 and t_Object = '0000000014';

  if v_cnt = 0 then
    Insert into DOBJATCOR_DBT
      (T_OBJECTTYPE, T_GROUPID, T_ATTRID, T_OBJECT, T_GENERAL, T_VALIDFROMDATE, T_OPER, T_VALIDTODATE, T_SYSDATE, T_SYSTIME, T_ISAUTO, T_ID)
    Values
      (5, 2, 3, '0000000014', 'X', 
       TO_DATE('02.01.2015 00:00:00', 'MM.DD.YYYY HH24:MI:SS'), 1, TO_DATE('12.31.9999 00:00:00', 'MM.DD.YYYY HH24:MI:SS'), TRUNC(SYSDATE), TO_DATE ('01.01.0001:'|| TO_CHAR (SYSDATE, 'HH24:MI:SS'),'DD.MM.YYYY:HH24:MI:SS'), 
       CHR(0), 0);
  
  end if;

end;
/

/*Казахстан*/
declare

  v_cnt number(10);
  
begin

  select count(*) into v_cnt
     from DOBJATCOR_DBT 
    where T_OBJECTTYPE = 5 and T_GROUPID = 2 and t_AttrID = 3 and t_Object = '0000000087';

  if v_cnt = 0 then
    Insert into DOBJATCOR_DBT
      (T_OBJECTTYPE, T_GROUPID, T_ATTRID, T_OBJECT, T_GENERAL, T_VALIDFROMDATE, T_OPER, T_VALIDTODATE, T_SYSDATE, T_SYSTIME, T_ISAUTO, T_ID)
    Values
      (5, 2, 3, '0000000087', 'X', 
       TO_DATE('01.01.2015 00:00:00', 'MM.DD.YYYY HH24:MI:SS'), 1, TO_DATE('12.31.9999 00:00:00', 'MM.DD.YYYY HH24:MI:SS'), TRUNC(SYSDATE), TO_DATE ('01.01.0001:'|| TO_CHAR (SYSDATE, 'HH24:MI:SS'),'DD.MM.YYYY:HH24:MI:SS'), 
       CHR(0), 0);
  
  end if;

end;
/

/*Киргизия*/
declare

  v_cnt number(10);
  
begin

  select count(*) into v_cnt
     from DOBJATCOR_DBT 
    where T_OBJECTTYPE = 5 and T_GROUPID = 2 and t_AttrID = 3 and t_Object = '0000000095';

  if v_cnt = 0 then
    Insert into DOBJATCOR_DBT
      (T_OBJECTTYPE, T_GROUPID, T_ATTRID, T_OBJECT, T_GENERAL, T_VALIDFROMDATE, T_OPER, T_VALIDTODATE, T_SYSDATE, T_SYSTIME, T_ISAUTO, T_ID)
    Values
      (5, 2, 3, '0000000095', 'X', 
       TO_DATE('12.08.2015 00:00:00', 'MM.DD.YYYY HH24:MI:SS'), 1, TO_DATE('12.31.9999 00:00:00', 'MM.DD.YYYY HH24:MI:SS'), TRUNC(SYSDATE), TO_DATE ('01.01.0001:'|| TO_CHAR (SYSDATE, 'HH24:MI:SS'),'DD.MM.YYYY:HH24:MI:SS'), 
       CHR(0), 0);
  
  end if;

end;
/

/*Россия*/
declare

  v_cnt number(10);
  
begin

  select count(*) into v_cnt
     from DOBJATCOR_DBT 
    where T_OBJECTTYPE = 5 and T_GROUPID = 2 and t_AttrID = 3 and t_Object = '0000000165';

  if v_cnt = 0 then
    Insert into DOBJATCOR_DBT
      (T_OBJECTTYPE, T_GROUPID, T_ATTRID, T_OBJECT, T_GENERAL, T_VALIDFROMDATE, T_OPER, T_VALIDTODATE, T_SYSDATE, T_SYSTIME, T_ISAUTO, T_ID)
    Values
      (5, 2, 3, '0000000165', 'X', 
       TO_DATE('01.01.2015 00:00:00', 'MM.DD.YYYY HH24:MI:SS'), 1, TO_DATE('12.31.9999 00:00:00', 'MM.DD.YYYY HH24:MI:SS'), TRUNC(SYSDATE), TO_DATE ('01.01.0001:'|| TO_CHAR (SYSDATE, 'HH24:MI:SS'),'DD.MM.YYYY:HH24:MI:SS'), 
       CHR(0), 0);
  
  end if;

end;
/


