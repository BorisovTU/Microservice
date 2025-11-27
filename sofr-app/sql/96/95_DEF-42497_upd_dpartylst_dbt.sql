--обновление dpartylst_dbt
begin
   update dpartylst_dbt set t_locked=chr(0) where t_locked=chr(49);
end;
