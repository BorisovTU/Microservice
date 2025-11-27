-- Таблица "DDL_REGIABUF_DBT"
declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where i.TABLE_NAME='DDL_REGIABUF_DBT' and i.INDEX_NAME='DDL_REGIABUF_DBT_IDX1' ;
  if cnt =1 then
    execute immediate 'drop index DDL_REGIABUF_DBT_IDX1';
  end if;
  execute immediate 'CREATE INDEX DDL_REGIABUF_DBT_IDX1 ON DDL_REGIABUF_DBT (T_SESSIONID ASC, T_CALCID ASC, T_PART ASC, T_DEALID ASC, T_DEALSUBKIND ASC, T_DEALPART ASC, T_CLIENTCONTRID ASC, T_ACCTYPE ASC)';
end;
/
