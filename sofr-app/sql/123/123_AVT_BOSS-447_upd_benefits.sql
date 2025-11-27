/*Заполнение настроечной таблицы*/
BEGIN

UPDATE DNPTXBENEFITS_DBT
SET T_STATUS = 2
WHERE T_BENEFITTYPE IN ('Л_1','Л_11','Л_2','Л_7','Л_5','Л_4','Л_9','Л_10','Л_12','Л_8')
AND T_STATUS = 1;

END;
/



