-- Обновить поля в таблице "DCDRECORDS_DBT"
DECLARE
BEGIN
  UPDATE DCDRECORDS_DBT
     SET T_STATUS = 1 --Обработано
   WHERE T_STATUS IS NULL
     AND UPPER(TRIM(T_PROCRESULT)) = 'OK';

  UPDATE DCDRECORDS_DBT
     SET T_STATUS = 3 --Ошибка обработки
   WHERE T_STATUS IS NULL
     AND UPPER(TRIM(T_PROCRESULT)) <> 'OK' AND UPPER(TRIM(T_PROCRESULT)) <> CHR(1);

  UPDATE DCDRECORDS_DBT
     SET T_STATUS = 0 --Новая запись
   WHERE T_STATUS IS NULL
     AND UPPER(TRIM(T_PROCRESULT)) = CHR(1);   
END;
/

DECLARE
BEGIN
  UPDATE DCDRECORDS_DBT
     SET T_ERROR = CHR(1)
   WHERE T_ERROR IS NULL;
END;
/

DECLARE
BEGIN
  UPDATE DCDRECORDS_DBT CD
     SET CD.T_PARTYID = NVL((SELECT O.T_OBJECTID 
                               FROM DOBJCODE_DBT O 
                              WHERE O.T_OBJECTTYPE = 3
                                AND O.T_CODEKIND = 101 
                                AND O.T_CODE = CD.T_CLIENTID_OBJECTID 
                                AND ROWNUM = 1
                            ), -1)
   WHERE CD.T_PARTYID IS NULL;
END;
/

DECLARE
BEGIN
  UPDATE DCDRECORDS_DBT CD
     SET CD.T_CONTRACTID = NVL((SELECT SF_MP.T_ID  
                                  FROM DSFCONTR_DBT CONTR, DDLCONTR_DBT DLC, DDLCONTRMP_DBT MP, DSFCONTR_DBT SF_MP 
                                 WHERE CONTR.T_NUMBER = CD.T_AGREEMENTNUMBER 
                                   AND DLC.T_SFCONTRID = CONTR.T_ID 
                                   AND MP.T_DLCONTRID = DLC.T_DLCONTRID 
                                   AND SF_MP.T_ID = MP.T_SFCONTRID 
                                   AND SF_MP.T_SERVKIND = 1
                                   AND MP.T_MARKETID = 2
                                   AND SF_MP.T_DATEBEGIN = CD.T_AGREEMENTOPENDATE 
                                   AND ROWNUM = 1
                               ), -1)
   WHERE CD.T_CONTRACTID IS NULL;
END;
/

DECLARE
BEGIN
  UPDATE DCDRECORDS_DBT CD
     SET CD.T_FIID = NVL((SELECT ISS.T_FIID 
                            FROM DAVOIRISS_DBT ISS 
                           WHERE ISS.T_ISIN = CD.T_ISINREGISTRATIONNUMBER 
                             AND ROWNUM = 1
                         ), -1)
   WHERE CD.T_FIID IS NULL;
END;
/