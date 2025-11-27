BEGIN 
  -- делаем новую невидимую колонку
  EXECUTE IMMEDIATE 'alter table DSFCONTR_DBT add t_unique_number varchar2(36) invisible';

  -- коммент со ссылкой на дефект
  EXECUTE IMMEDIATE 'comment on column DSFCONTR_DBT.t_unique_number is ''GUID для контроля номера договора, см DEF-64939 (col)''';
END;
/

BEGIN 
  -- заливаем новую колонку копиями T_ID для дубликатов,
  --  чтобы пара T_ID + t_unique_number могла быть использована в индексе
  UPDATE dsfcontr_dbt
    SET t_unique_number = TO_CHAR(t_id)
    where t_id in ( 
      select contr.t_id -- T_ID all dupes 
        from dsfcontr_dbt contr 
        where contr.t_number in 
          (select dups.t_number from  
            (select cnt.t_number, count(cnt.t_id) 
              from dsfcontr_dbt cnt 
              group by cnt.t_number 
              having count(cnt.t_id) > 1) dups 
          )
      );

  COMMIT;
END;
/

BEGIN
-- делаем индекс (-955)
  EXECUTE IMMEDIATE '
    create unique index DSFCONTR_DBT_IDX9 on DSFCONTR_DBT (t_number, t_unique_number)
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
