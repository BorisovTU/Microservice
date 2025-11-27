BEGIN
  -- дропаем старый индекс 
  EXECUTE IMMEDIATE 'drop index DSFCONTR_DBT_IDX9';

  -- добрасываем новую колонку T_DATECLOSE (дата закрытия), в этом случае индекс даст создавать дубликаты 
  -- для договоров у которых различаются даты закрытия
  EXECUTE IMMEDIATE '
    create unique index DSFCONTR_DBT_IDX9 on DSFCONTR_DBT (t_number, t_unique_number, T_DATECLOSE)
      tablespace indx
      pctfree 10
      initrans 2
      maxtrans 255
      storage
      (
        initial 64K
        minextents 1
        maxextents unlimited
      )';
END;
/
