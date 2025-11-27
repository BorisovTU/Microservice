declare
  n      number;
  t_name varchar2(100) := 'DDL_CLIENTINFO_DBT';
  c_name varchar2(100) ;
begin
  c_name := 't_ComPrevious';
  select count(*) into n from user_tab_columns c where c.TABLE_NAME = upper(t_name) and c.COLUMN_NAME = upper(c_name);
  if n = 0 then
    execute immediate 'alter table DDL_CLIENTINFO_DBT add t_ComPrevious number default 0 not null';
  end if;
  execute immediate 'comment on column DDL_CLIENTINFO_DBT.t_ComPrevious  is ''Просроченная комиссия по субдоговору''';
  execute immediate 'comment on column DDL_LIMITFUTURMARK_DBT.t_money306  is ''''';
  execute immediate 'comment on column DDL_LIMITCASHSTOCK_DBT.t_money306  is ''''';
end;
