 BEGIN
 
 RSB_Struct.readStruct('dnotetext_dbt');
 
 UPDATE DNOTETEXT_DBT d
    SET d.t_text =  RSB_Struct.PutString('t_text', RPAD('0',3000, '0'), 'X', (-1)*53)
  WHERE d.T_OBJECTTYPE =  12
    AND d.T_NOTEKIND = 126;
   
END;
/