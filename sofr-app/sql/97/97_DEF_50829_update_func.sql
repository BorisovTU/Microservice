--сделаем корректное описание
begin
 update dfunc_dbt set t_name = 'Уведомления о расторжение ИИС' where t_funcid = 8258;
 commit;
end;
/