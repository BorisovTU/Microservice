declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME= 'DDVNDEAL_DBT_IDX6' ;
  if cnt =0 then
     execute immediate 'create index DDVNDEAL_DBT_IDX6 on ddvndeal_dbt(ltrim(ltrim(T_CODE), ''0''))';
  else
     dbms_output.put_line('Индекс существует');
  end if;
end;