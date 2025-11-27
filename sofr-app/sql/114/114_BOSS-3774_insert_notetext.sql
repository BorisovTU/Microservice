BEGIN

 INSERT INTO DNOTETEXT_DBT  ( T_OBJECTTYPE, 
                              T_DOCUMENTID, 
                              T_NOTEKIND, 
                              T_OPER, 
                              T_DATE, 
                              T_TIME, 
                              T_TEXT, 
                              T_VALIDTODATE, 
                              T_BRANCH, 
                              T_NUMSESSION ) 
 SELECT   207, 
          LPAD(d.t_sofrid, 34, '0'), 
          185, 
          1,
          TRUNC(d.t_msgdate), 
          to_date('01010001' || to_char(d.t_msgdate,'hhmiss'),'DDMMYYYYhhmiss'),  
          RPAD(utl_raw.cast_to_raw (d.t_email), 3000, 0), 
          to_date('31129999','DDMMYYYY'), 
          1,
          0 
 FROM dhistmsg2024alarm_dbt d
WHERE d.t_status = 1;
   
END;
/
