declare
  n integer;
begin
  select count(*) into n from user_tables t where t.TABLE_NAME = upper('dtick_tmp');
  if n > 0
  then
    execute immediate 'rename dtick_tmp to dlimit_dltick_dbt';
    execute immediate 'alter index dtick_tmp_idx0 rename to dlimit_dltick_idx0';
    execute immediate 'alter index dtick_tmp_idx1 rename to dlimit_dltick_idx1';
    execute immediate 'alter index dtick_tmp_idx2 rename to dlimit_dltick_idx2';
  end if;
end;
/