--Исправление заполнения поля для новых схем расчётов с биржей
UPDATE ddlmarket_dbt SET t_depsetid = 0 where t_depsetid is null/