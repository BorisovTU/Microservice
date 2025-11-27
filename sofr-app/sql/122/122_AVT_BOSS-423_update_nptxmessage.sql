BEGIN
	UPDATE DNPTXMESSAGE_DBT
	   SET t_comments = t_comments ||
					 CHR(10) || CHR(10) ||
					 'С уважением,' || CHR(10) ||
					 'АО "Россельхозбанк"'
	 WHERE t_typeid = 4;
END;
/