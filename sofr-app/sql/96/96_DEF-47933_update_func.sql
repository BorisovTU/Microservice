 -- обновим название функции
begin
 update dfunc_dbt set t_functionname = 'ws_SOFR_addAccountingEntriesROVU' where t_funcid = 5073;
 commit;
end;/