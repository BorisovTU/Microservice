
declare
  n      number;
  t_name varchar2(100) := 'DDL_LIMITOP_DBT';
  c_name varchar2(100) := 't_recs';
begin
   select count(*)  into n from user_tab_columns c where c.TABLE_NAME = upper(t_name) and c.COLUMN_NAME = upper(c_name);
   if n = 0 then
     execute immediate 'alter table DDL_LIMITOP_DBT add t_recs number';
   end if;
end;

