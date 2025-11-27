 BEGIN
  
 RSB_Struct.readStruct('dnotetext_dbt');

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
 SELECT   12, 
          LPAD(fin.t_fiid, 10, '0'), 
          126, 
          1,
          TRUNC(sysdate), 
          to_date('01010001' || to_char(sysdate,'hhmiss'),'DDMMYYYYhhmiss'),  
          RSB_Struct.PutString('t_text', RPAD('0',3000, '0'), null, (-1)*53),
          to_date('31129999','DDMMYYYY'), 
          1,
          0 
 FROM dfininstr_dbt fin inner join davoiriss_dbt avo 
       on avo.t_fiid = fin.t_fiid
WHERE avo.t_indexnom = 'X';
   
END;
/