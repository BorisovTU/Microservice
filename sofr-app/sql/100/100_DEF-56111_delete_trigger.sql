-- триггер больше не нужен
declare 
    E_OBJECT_EXISTS EXCEPTION;
    PRAGMA EXCEPTION_INIT( E_OBJECT_EXISTS, -4080);
begin
    execute immediate 'drop trigger DPARTY_DBT_CDI';
exception
    when E_OBJECT_EXISTS then null;
    when others then         
        it_error.put_error_in_stack;
        it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
end;