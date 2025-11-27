BEGIN
	UPDATE DNPTXMESSAGE_DBT t
       SET t.t_comments =
       REPLACE(
         REPLACE(
           REPLACE(t.t_comments,
                   'рублей', 'руб.'),
                   'рубля',  'руб.'),
                   'рубль',  'руб.')
    WHERE t.t_comments LIKE '%рубл%';
END;
/