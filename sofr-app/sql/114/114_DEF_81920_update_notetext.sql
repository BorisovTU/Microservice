BEGIN
  
 RSB_Struct.readStruct('dnotetext_dbt');
  
 UPDATE DNOTETEXT_DBT d 
    SET d.t_text = RSB_Struct.PutDate('t_text', RPAD('0',3000, '0'), TRUNC(d.t_date), (-1)*53)
  WHERE d.t_objecttype = 207
    AND d.t_notekind = 185 
    AND d.t_date < (SELECT TRUNC(MAX(d.t_msgdate)) + 1 FROM dhistmsg2024alarm_dbt d);
  
END;
/
