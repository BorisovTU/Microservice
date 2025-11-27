/*Установить значение категории для эмитентов*/
declare

  
begin

  Insert into DOBJATCOR_DBT
    (T_OBJECTTYPE, T_GROUPID, T_ATTRID, T_OBJECT, T_GENERAL, T_VALIDFROMDATE, T_OPER, T_VALIDTODATE, T_SYSDATE, T_SYSTIME, T_ISAUTO, T_ID)
  select
    3, 112, 1, LPAD(pt.t_PartyID, 10, '0'), CHR(88), 
     ptrh.t_Date, 1, TO_DATE('12.31.9999 00:00:00', 'MM.DD.YYYY HH24:MI:SS'), TRUNC(SYSDATE), TO_DATE ('01.01.0001:'|| TO_CHAR (SYSDATE, 'HH24:MI:SS'),'DD.MM.YYYY:HH24:MI:SS'), 
     CHR(88), 0
    from dpartyown_dbt ptown, dparty_dbt pt, DPTRESIDENTHIST_DBT ptrh
   where ptown.t_PartyKind = 5 /*Эмитент*/
     and pt.t_PartyID = ptown.t_PartyID
     and pt.t_NRCountry IN ('RUS', 'ARM', 'BLR', 'KAZ', 'KGZ')
     and ptrh.t_ID = (select max(ptrh1.t_ID) from DPTRESIDENTHIST_DBT ptrh1 where ptrh1.t_PartyID = pt.t_PartyID);
  

end;
/

