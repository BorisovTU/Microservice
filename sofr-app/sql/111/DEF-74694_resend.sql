/* Formatted on 02.11.2024 13:15:05 (QP5 v5.149.1003.31008) */
DECLARE
BEGIN
   DELETE FROM UQIACCREDITATION_STEP_DBT stp
         WHERE STP.T_ACCREDID IN
                  (SELECT T_ID
                     FROM UQIACCREDITATION_DBT acr
                    WHERE acr.T_ACCRED_RESULT IS NOT NULL
                          AND acr.T_SEND_RESULT LIKE '%DLAR%');

   UPDATE UQIACCREDITATION_DBT
      SET T_SEND_STATUS = 0
    WHERE T_ID IN
             (SELECT T_ID
                FROM UQIACCREDITATION_DBT acr
               WHERE acr.T_ACCRED_RESULT IS NOT NULL
                     AND acr.T_SEND_RESULT LIKE '%DLAR%');

   INSERT INTO dfuncobj_dbt (T_OBJECTTYPE,
                             T_OBJECTID,
                             T_FUNCID,
                             T_PARAM,
                             T_PRIORITY)
      SELECT DISTINCT 5072,
                      acr.T_DLCONTRID,
                      5072,
                      acr.T_DLCONTRID || ';5072;' || acr.T_JMSMESSAGEID,
                      0
        FROM UQIACCREDITATION_DBT acr
       WHERE acr.T_ACCRED_RESULT IS NOT NULL
             AND acr.T_SEND_RESULT LIKE '%DLAR%';
END;
/