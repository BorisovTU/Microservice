BEGIN

	MERGE INTO DNOTETEXT_DBT note                                                                                                                 
     USING      
     (SELECT d.t_id,
             NVL(LPAD(dl.t_dlcontrid, 34, '0'), d.t_documentid) as t_documentid
     FROM DNOTETEXT_DBT d left join Ddlcontr_Dbt dl on d.t_documentid = LPAD(dl.t_sfcontrid,34,'0')
    WHERE d.t_objecttype = 207
      AND d.t_notekind = 185
      AND d.t_date < (SELECT TRUNC(MAX(d.t_msgdate)) + 1 FROM dhistmsg2024alarm_dbt d) 
     ) r  
          ON (r.t_id = note.t_id)             
     WHEN MATCHED 
        THEN             
          UPDATE SET t_documentid = r.t_documentid;
	   
END;
/
