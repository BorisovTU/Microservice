-- Добавляем категорию "Кассовый метод учета доходов" = "Да" на ц\б
BEGIN
   INSERT INTO DOBJATCOR_DBT(T_OBJECTTYPE, T_GROUPID, T_ATTRID, T_OBJECT, T_GENERAL, T_VALIDFROMDATE, T_OPER, T_VALIDTODATE, T_SYSDATE, T_SYSTIME, T_ISAUTO)
   SELECT 12, -- OBJTYPE_AVOIRISS
          131, -- Кассовый метод учета доходов
          2,  -- Да (по умолчанию)
          LPAD(fin.t_fiid, 10, '0'), --T_OBJECT полученный из FIID ц\б
          CHR(88), --T_GENERAL
          TO_DATE('01.01.0001', 'dd.mm.yyyy'), -- T_VALIDFROMDATE
          9999,                                -- oper
          TO_DATE('31.12.9999', 'dd.mm.yyyy'), -- T_VALIDTODATE
          TRIM(SYSDATE),                       -- T_SYSDATE
          TO_DATE ('01-01-0001 '|| TO_CHAR (sysdate, 'HH24:MI:SS'), 'MM-DD-YYYY HH24:MI:SS'), -- T_SYSTIME
          CHR(88)                              -- T_ISAUTO
     FROM dfininstr_dbt fin, dparty_dbt pt, davoiriss_dbt av
    WHERE pt.t_PartyID = fin.t_ISSUER
      AND pt.t_NotResident = CHR(88)
      AND fin.t_fiid = av.t_fiid 
      AND (av.t_isin not like 'RU%' AND av.t_isin not like 'SU%')
    ;
END;
/
