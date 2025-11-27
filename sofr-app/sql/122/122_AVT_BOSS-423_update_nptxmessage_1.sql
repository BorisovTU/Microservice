BEGIN
	UPDATE DNPTXMESSAGE_DBT
        SET t_comments = replace(t_comments, '/{', '/ \{')
        WHERE t_typeid = 4;
END;
/