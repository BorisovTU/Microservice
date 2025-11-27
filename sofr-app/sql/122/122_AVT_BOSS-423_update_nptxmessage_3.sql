BEGIN
	 UPDATE DNPTXMESSAGE_DBT t
		SET t.t_comments = REPLACE(t.t_comments,'руб..', 'руб.')
	 WHERE t.t_comments LIKE '%руб..%';
 
	 UPDATE DNPTXMESSAGE_DBT t
	   SET t.t_comments = REPLACE(t.t_comments,'18 января 2025 года', '17 января 2026 года')
	 WHERE t.t_typeid = 7;
	 
	 UPDATE DNPTXMESSAGE_DBT t
	    SET t.t_comments = REPLACE(t.t_comments,'.  Из', ' из')
	  WHERE t.t_typeid = 7;
END;
/