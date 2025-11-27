BEGIN

 INSERT INTO DDLCONTRMSG_DBT  
    (t_Dlcontrid, 
     t_Kind, 
     t_Createdate, 
     t_Createtime, 
     t_Senddate, 
     t_Sendtime,
     t_Senderemail,
     t_Recipientemail,
     t_Sendmesstate 
     ) 
 SELECT  dl.t_dlcontrid,
         515,
         TRUNC(SYSDATE),  
         TO_DATE('01010001 ' || To_Char(SYSDATE, 'HH24MISS'), 'DDMMYYYY HH24MISS'),
         TRUNC(h.t_msgdate),  
         TO_DATE('01010001' || to_char(h.t_msgdate,'hhmiss'),'DDMMYYYYhhmiss'),
         'sofr@rshb.ru',
         h.t_email,
         decode(h.t_status,1,315,100)
          
 FROM DHISTMSG2024ALARM_DBT h LEFT JOIN Ddlcontr_Dbt Dl ON (h.t_sofrid = Dl.t_Sfcontrid);
   
END;
/
