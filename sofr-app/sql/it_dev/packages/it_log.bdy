create or replace package body it_log is
 /**************************************************************************************************\
  Пакет для логирования для всего функционала разработки РСХБ-Интех.
  **************************************************************************************************
  Изменения:
  ---------------------------------------------------------------------------------------------------
  Дата        Автор            Jira                          Описание 
  ----------  ---------------  ---------------------------   ----------------------------------------
  24.08.2022  Зыков М.В.       BIQ-12884                     Замена its_main на its_log
  31.01.2022  Зотов Ю.Н.       BIQ-6664 CCBO-506             Создание
 \**************************************************************************************************/
 
  --сохранение строки лога в автономной транзакции
  procedure log_internal_autonomus(p_log_row itt_log%rowtype) as
    pragma autonomous_transaction;
  begin
    insert into itt_log values p_log_row;
    commit;
  end;    
     
 

  
  --внутрення функция логирования (насыщает строку логов дополнительными данными)
  procedure log_internal(p_log_row itt_log%rowtype --строка из таблицы логов
                       , p_call_stack clob -- стек вызова
                        ) as
    v_log_row itt_log%rowtype;

    v_object_name varchar2(255);
    i number;
    j number;
    v_call_stack clob;
    
    v_sid number;
    v_serial number;
    v_sessionid number;
    
    v_line number;
  begin
    v_log_row := p_log_row;
    v_log_row.id_log := its_log.nextval();
    v_log_row.create_sysdate := sysdate;
    
    if v_log_row.msg_type is null then
      v_log_row.msg_type := it_log.C_MSG_TYPE__DEBUG;
    end if;  
    
    v_sid := sys_context ('USERENV','SID');
    v_sessionid := sys_context('USERENV','SESSIONID');
    
    /*select serial#
      into v_serial
     from v$session s 
     where s.SID = v_sid
       and s.audsid = v_sessionid;*/ --пока не хватает прав
       
    v_log_row.sid := v_sid;
    v_log_row.serial := v_serial;
    v_log_row.user_name := sys_context('USERENV','SESSION_USER');
    v_log_row.tran_id := dbms_transaction.local_transaction_id; 
    
    v_call_stack := p_call_stack;

    i := instr(v_call_stack,$$PLSQL_UNIT);
    --dbms_output.put_line(i);
    i := instr(v_call_stack,'package body',i);
    if i=0 then
      v_object_name := 'anonymous block';
    else  
      --dbms_output.put_line(i);
      i := instr(v_call_stack,'.',i)+1;
      --dbms_output.put_line(i);
      j := instr(v_call_stack,chr(10),i)+1;
      --dbms_output.put_line(j);
      v_object_name := substr(v_call_stack,i,j-i-1);
      v_object_name := rtrim(v_object_name,chr(13));
    end if;
    
    i := instr(v_call_stack,chr(10),1,4) + 12;
    
    begin
      v_line := to_number(trim(substr(v_call_stack,i,10)));
    exception
      when others then
        null;
    end;      
    v_log_row.line := v_line;
    
    v_log_row.call_stack := v_call_stack;
    v_log_row.object_name := v_object_name;
    
    if v_log_row.msg_type = it_log.C_MSG_TYPE__ERROR then
      v_log_row.error_stack := it_error.get_error_stack_clob;
    end if;  
    
    
    log_internal_autonomus(p_log_row =>v_log_row);
  end;    



  /*
  Залогировать информацию в автономной транзакции.
  В таблицу логов tit_log вставится строка с содержимым входных параметров.
  Дополнительно будет залогировано:
    sysdate с точносью до секунды;
    Наименование пакета и фукнции/процедуры, из которой было вызвано логирование;
    Строка кода, из которой было вызвано логирование;
    Полный стек вызова;
    SID сессии;
    Имя Oracle-пользователя;
    Идентификатор транзакции;
    В типа C_MSG_TYPE__ERROR будет сохранен стек ошибки из it_error.get_error_stack;
    ...
  */
  procedure log(p_msg varchar2 default null --Текст сообщения
              , p_msg_type varchar2 default null --Тип сообщения. См. константы it_log.C_MSG_TYPE__*. Если null, то C_MSG_TYPE__DEBUG.
              , p_msg_clob clob default null --Дополнительный текст сообщения в формате clob
               )
  as             
    v_log_row itt_log%rowtype;

    v_call_stack clob;
  begin
    v_log_row.msg := p_msg;
    v_log_row.msg_type := p_msg_type;
    v_log_row.msg_clob := p_msg_clob;
    
    v_call_stack := dbms_utility.format_call_stack;
    
    log_internal(v_log_row,v_call_stack);
  end;

  procedure log_handle(p_object varchar2
                      ,p_msg varchar2 default null --Текст сообщения
                      ,p_msg_type varchar2 default null --Тип сообщения. См. константы it_log.C_MSG_TYPE__*. Если null, то C_MSG_TYPE__DEBUG.
                      ,p_msg_clob clob default null --Дополнительный текст сообщения в формате clob
               ) is
    v_log_row itt_log%rowtype;
  begin
    v_log_row.id_log         := its_log.nextval();
    v_log_row.object_name    := upper(p_object);
    v_log_row.msg            := p_msg;
    v_log_row.msg_type       := nvl(p_msg_type, it_log.C_MSG_TYPE__DEBUG);
    v_log_row.msg_clob       := p_msg_clob;
    v_log_row.create_sysdate := sysdate;
    v_log_row.sid            := sys_context ('USERENV','SID');
    v_log_row.user_name      := sys_context('USERENV','SESSION_USER');
    v_log_row.tran_id        := dbms_transaction.local_transaction_id;

    if v_log_row.msg_type = it_log.C_MSG_TYPE__ERROR then
      v_log_row.error_stack := it_error.get_error_stack_clob;
    end if;

    log_internal_autonomus(p_log_row => v_log_row);
  end log_handle;

  procedure log_error(p_object varchar2
                     ,p_msg varchar2 default null --Текст сообщения
                     ,p_msg_clob clob default null --Дополнительный текст сообщения в формате clob
                     ) as
  begin
    it_error.put_error_in_stack;
    log_handle(p_object   => p_object
              ,p_msg      => p_msg
              ,p_msg_type => it_log.c_msg_type__error
              ,p_msg_clob => p_msg_clob);
    it_error.clear_error_stack;
  end log_error; 

end it_log;
/
