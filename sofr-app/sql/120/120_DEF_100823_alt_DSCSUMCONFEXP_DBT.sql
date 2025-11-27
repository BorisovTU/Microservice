declare
  n      number;
  t_name varchar2(100) := 'DSCSUMCONFEXP_DBT';
  c_name varchar2(100) ;
begin
  c_name := 't_sysdate';
  select count(*) into n from user_tab_columns c where c.TABLE_NAME = upper(t_name) and c.COLUMN_NAME = upper(c_name);
  if n = 0 then
    execute immediate 'alter table DSCSUMCONFEXP_DBT add t_sysdate date default SYSDATE not null';
  end if;
  execute immediate 'comment on column DSCSUMCONFEXP_DBT.t_sysdate is ''Время создания записи ''';
end;
/
