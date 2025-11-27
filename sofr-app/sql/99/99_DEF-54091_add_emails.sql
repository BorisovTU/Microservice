--DEF-54091 добавление почты в группу
begin
    insert into usr_email_addr_dbt(t_group, t_email, t_place, t_comment)
    values(45, '8888@rshb.ru','R',chr(1));

    commit;
end;

