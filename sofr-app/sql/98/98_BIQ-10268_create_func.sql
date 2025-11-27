begin

   MERGE INTO DLLVALUES_DBT pt 
      USING (SELECT 5002 AS T_LIST,
                    5054 AS T_ELEMENT,
                    '5054'  AS T_CODE,
                    'Сервис получения детальной информации по ЮЛ из AC CDI' AS T_NAME,
                    5054 AS T_FLAG,
                    '' AS T_NOTE,
                    chr(1) AS T_RESERVE
                    FROM DUAL) ps
      ON ( (pt.T_LIST = ps.T_LIST and pt.T_ELEMENT = ps.T_ELEMENT) or (pt.T_LIST = ps.T_LIST and pt.T_CODE = ps.T_CODE) )
   WHEN NOT MATCHED THEN INSERT
          (pt.T_LIST, pt.T_ELEMENT, pt.T_CODE, pt.T_NAME, pt.T_FLAG, pt.T_NOTE, pt.T_RESERVE) 
   VALUES (ps.T_LIST, ps.T_ELEMENT, ps.T_CODE, ps.T_NAME, ps.T_FLAG, ps.T_NOTE, ps.T_RESERVE);

   MERGE INTO DFUNC_DBT pt 
      USING (SELECT 5054  AS T_FUNCID,
                    'CdiGetOrg' AS T_CODE,
                    'Сервис получения детальной информации по ЮЛ из AC CDI'  AS T_NAME,
                    1 AS T_TYPE,
                    'ws_synch_SOFR' AS T_FILENAME,
                    'ws_CdiGetOrg' AS T_FUNCTIONNAME,
                    0 AS T_INTERVAL,
                    0 AS T_VERSION
                    FROM DUAL) ps
      ON (pt.T_FUNCID = ps.T_FUNCID)
   WHEN NOT MATCHED THEN INSERT
          (pt.T_FUNCID, pt.T_CODE, pt.T_NAME, pt.T_TYPE, pt.T_FILENAME, pt.T_FUNCTIONNAME, pt.T_INTERVAL, pt.T_VERSION)
   VALUES (ps.T_FUNCID, ps.T_CODE, ps.T_NAME, ps.T_TYPE, ps.T_FILENAME, ps.T_FUNCTIONNAME, ps.T_INTERVAL, ps.T_VERSION);

   MERGE INTO DLLVALUES_DBT pt 
      USING (SELECT 5002  AS T_LIST,
                    5055 AS T_ELEMENT,
                    '5055'  AS T_CODE,
                    'Сервис обновления карточек клиентов ЮЛ в АС CDI' AS T_NAME,
                    5055 AS T_FLAG,
                    '' AS T_NOTE,
                    chr(1) AS T_RESERVE
                    FROM DUAL) ps
      ON (pt.T_LIST = ps.T_LIST and pt.T_ELEMENT = ps.T_ELEMENT)
   WHEN NOT MATCHED THEN INSERT
          (pt.T_LIST, pt.T_ELEMENT, pt.T_CODE, pt.T_NAME, pt.T_FLAG, pt.T_NOTE, pt.T_RESERVE) 
   VALUES (ps.T_LIST, ps.T_ELEMENT, ps.T_CODE, ps.T_NAME, ps.T_FLAG, ps.T_NOTE, ps.T_RESERVE);   

   MERGE INTO DFUNC_DBT pt 
      USING (SELECT 5055  AS T_FUNCID,
                    'CdiUpdateOrg' AS T_CODE,
                    'Сервис обновления карточек клиентов ЮЛ в АС CDI'  AS T_NAME,
                    1 AS T_TYPE,
                    'ws_synch_SOFR' AS T_FILENAME,
                    'ws_CdiUpdateOrg' AS T_FUNCTIONNAME,
                    0 AS T_INTERVAL,
                    0 AS T_VERSION
                    FROM DUAL) ps
      ON (pt.T_FUNCID = ps.T_FUNCID)
   WHEN NOT MATCHED THEN INSERT
          (pt.T_FUNCID, pt.T_CODE, pt.T_NAME, pt.T_TYPE, pt.T_FILENAME, pt.T_FUNCTIONNAME, pt.T_INTERVAL, pt.T_VERSION)
   VALUES (ps.T_FUNCID, ps.T_CODE, ps.T_NAME, ps.T_TYPE, ps.T_FILENAME, ps.T_FUNCTIONNAME, ps.T_INTERVAL, ps.T_VERSION);
   
    COMMIT;
end;