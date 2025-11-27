BEGIN
   DELETE
   FROM DSHEDULE_DBT
   WHERE t_Comment = '╬яхЁрЎшш Єхїэшўхёъющ ётхЁъш ╤═╬┴ (яырэшЁют∙шъ)';
   
   it_log.log('Откат BOSS-1489. Задание планировщика успешно удалено');
EXCEPTION
   WHEN NO_DATA_FOUND
   THEN it_log.log('Откат BOSS-1489. Задание планировщика не найдено');
END;