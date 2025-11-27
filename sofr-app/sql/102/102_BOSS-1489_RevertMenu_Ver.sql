BEGIN
   BEGIN
      DELETE
      FROM DMENUITEM_DBT
      WHERE t_ICaseItem = 20215
        AND t_IsTemplate = CHR(0);

      IT_LOG.LOG('Откат BOSS-1489 из 102. Пункты меню с t_ICaseItem = 20215 успешно удалены');
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN IT_LOG.LOG('Откат BOSS-1489 из 102. Не найдены пункты меню с t_ICaseItem = 20215');
   END;

  COMMIT;
   
   DELETE
   FROM DMENUITEM_DBT
   WHERE LTRIM(t_SzNameItem) = 'Операции технической сверки СНОБ';
   
EXCEPTION
   WHEN NO_DATA_FOUND
   THEN IT_LOG.LOG('Откат BOSS-1489 из 102. Не найдены пункты меню с t_SzNameItem = ''Операции технической сверки СНОБ''');
END;