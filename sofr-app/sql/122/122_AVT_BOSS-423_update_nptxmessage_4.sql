BEGIN
	  UPDATE DNPTXMESSAGE_DBT t
	     SET t.t_comments = REPLACE(t_comments,'прописью}).','прописью}) руб.')
	   WHERE t.t_typeid = 4;
 
	 UPDATE DNPTXMESSAGE_DBT t
        SET t.t_comments = 
       REPLACE( 
         REPLACE(
          REPLACE(
           REPLACE(t_comments,
             'Уведомление об излишне удержанной сумме налога на доходы физических лиц ', 'Уведомление об излишне удержанной сумме налога на доходы физических лиц '  || CHR(10) || CHR(10)),
             'В соответствии со', CHR(10) || CHR(10) || 'В соответствии со'),
             '(включительно). ',  '(включительно). ' || CHR(10) || CHR(10)),
             '\{ФИО} ',  '\{ФИО} ' || CHR(10) || CHR(10))
      WHERE t_id = 4;
 
	 UPDATE DNPTXMESSAGE_DBT t
	    SET t.t_comments = REPLACE(t.t_comments,'2025', '2026')
	  WHERE t.t_typeid = 7;

	 UPDATE DNPTXMESSAGE_DBT t
	    SET t.t_comments = REPLACE(t.t_comments,'18', '17')
	  WHERE t.t_typeid = 7;	
	 
	 UPDATE DNPTXMESSAGE_DBT t
	    SET t.t_comments = REPLACE(t.t_comments,'\{ФИО}, ', '\{ФИО}, ' || CHR(10) || CHR(10))
	  WHERE t.t_typeid = 7;	

END;
/