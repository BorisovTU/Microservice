DECLARE
   v_Cnt_LLVAL NUMBER := 0;
BEGIN
   SELECT COUNT(1)
   INTO v_Cnt_LLVAL
   FROM DLLVALUES_DBT
   WHERE t_List = 5009
     AND t_Element = 79
     AND t_Name = 'CheckRequestSnobStor';

   IF v_Cnt_LLVAL = 0
   THEN
      INSERT INTO DLLVALUES_DBT (
                                   t_List,
                                   t_Element,
                                   t_Code,
                                   t_Name,
                                   t_Flag,
                                   t_Note,
                                   t_Reserve
                                )
                         VALUES (
                                   5009,
                                   79,
                                   '79',
                                   'CheckRequestSnobStor',
                                   1,
                                   'Получатели уведомления о работе процедуры запроса данных от Хранилища СНОБ',
                                   NULL
                                );
                                
      IT_LOG.LOG('BOSS-1819. В таблицу DLLVALUES_DBT вставлена группа рассылки CheckRequestSnobStor');
   ELSE
      IT_LOG.LOG('BOSS-1819. В таблице DLLVALUES_DBT уже есть группа рассылки CheckRequestSnobStor');
   END IF;
   COMMIT;

   INSERT ALL
      --INTO usr_email_addr_dbt (t_ID, t_Group, t_Email, t_Place, t_Comment) VALUES (0, 79, 'khromeyev@bryansk.softlab.ru', 'R', CHR(1))
      INTO usr_email_addr_dbt (t_ID, t_Group, t_Email, t_Place, t_Comment) VALUES (0, 79, 'YudakovANi@rshb.ru', 'R', CHR(1))
      INTO usr_email_addr_dbt (t_ID, t_Group, t_Email, t_Place, t_Comment) VALUES (0, 79, 'FedorovaEN@rshb.ru', 'R', CHR(1))
   SELECT * FROM dual;
      
   IT_LOG.LOG('BOSS-1819. Адреса для группы рассылки CheckRequestSnobStor успешно вставлены в таблицу usr_email_addr_dbt');
END;
/