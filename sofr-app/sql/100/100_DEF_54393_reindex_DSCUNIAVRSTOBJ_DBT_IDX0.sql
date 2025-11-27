declare
  cnt number;
begin

  select count(*)
    into cnt
    from (select 1 from DSCUNIAVRSTOBJ_DBT t group by (T_ID_OPERATION, T_ISWRT, T_OBJKIND, T_OBJID, T_FIID) having count(*) > 1);
  if cnt > 0
  then
    -- удаление резервной копии   
    begin
      execute immediate 'drop table DSCUNIAVRSTOBJ_DBT_COPY_DEF54393';
    exception
      when others then
        null;
    end;
    -- сохранение резервной копии (DDL в pl/sql возможно выполнить только при помощи execute immediate)
    execute immediate 'create table DSCUNIAVRSTOBJ_DBT_COPY_DEF54393 as select * from DSCUNIAVRSTOBJ_DBT';
  end if;
  -- удаление дублей
  delete from DSCUNIAVRSTOBJ_DBT
   where rowid not in (select min(rowid) from DSCUNIAVRSTOBJ_DBT t group by (T_ID_OPERATION, T_ISWRT, T_OBJKIND, T_OBJID, T_FIID));
  commit;
  select count(*) into cnt from user_indexes i where i.INDEX_NAME = 'DSCUNIAVRSTOBJ_DBT_IDX0';
  if cnt > 0
  then
    -- удаление индекса
    execute immediate 'drop index DSCUNIAVRSTOBJ_DBT_IDX0';
  end if;
  -- создание уникального индекса
  execute immediate 'create unique index DSCUNIAVRSTOBJ_DBT_IDX0 on DSCUNIAVRSTOBJ_DBT (T_ID_OPERATION, T_ISWRT, T_OBJKIND, T_OBJID, T_FIID) tablespace INDX';
  it_log.log('Успешное пересоздание индекса DSCUNIAVRSTOBJ_DBT_IDX0');
exception
  when others then
    dbms_output.put_line('Ошибка при пересоздании индекса DSCUNIAVRSTOBJ_DBT_IDX0');
    it_log.log('Ошибка при пересоздании индекса DSCUNIAVRSTOBJ_DBT_IDX0', p_msg_type => it_log.C_MSG_TYPE__ERROR);
    select count(*) into cnt from user_indexes i where i.INDEX_NAME = 'DSCUNIAVRSTOBJ_DBT_IDX0';
    if cnt > 0
    then
      -- удаление индекса
      execute immediate 'drop index DSCUNIAVRSTOBJ_DBT_IDX0';
    end if;
    execute immediate 'create index DSCUNIAVRSTOBJ_DBT_IDX0 on DSCUNIAVRSTOBJ_DBT (T_ID_OPERATION, T_ISWRT, T_OBJKIND, T_OBJID, T_FIID) tablespace  INDX';
end;
