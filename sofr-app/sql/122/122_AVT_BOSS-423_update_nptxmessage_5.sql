BEGIN
	  UPDATE DNPTXMESSAGE_DBT t
	     SET t.t_comments = 
		 REPLACE(t_comments,'Излишне удержанная сумма налога возвращается Банком Клиенту',CHR(10) || 'Излишне удержанная сумма налога возвращается Банком Клиенту')
	   WHERE t.t_typeid = 4;

END;
/