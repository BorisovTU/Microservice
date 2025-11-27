create or replace package body it_rs_q_utils is

  /**
   @file it_rs_q_utils
   @brief Пакет для инструментов взвимодействия RSL и QManager
   
       
   # changeLog
   |date       |author      |tasks                                                     |note                                                        
   |-----------|------------|----------------------------------------------------------|-------------------------------------------------------------
   |2023.09.11 |Зыков М.В.  |CCBO-7543 BIQ-13171                                       | Создание;                   
  */
  gc_in_messbody  clob; -- Входящий messbody
  gc_in_messmeta  clob; -- Входящий messmeta
  gc_out_messbody clob; -- Результирующйй messbody
  gc_out_messmeta clob; -- Результирующйй messmeta
  /**
  @brief  Очистка буферов данных
  */
  procedure clear_data_buffer is
  begin
    dbms_lob.trim(lob_loc => gc_in_messbody, newlen => 0);
    dbms_lob.trim(lob_loc => gc_in_messmeta, newlen => 0);
    dbms_lob.trim(lob_loc => gc_out_messbody, newlen => 0);
    dbms_lob.trim(lob_loc => gc_out_messmeta, newlen => 0);
  exception
    when others then
      it_error.put_error_in_stack;
      it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
      raise;
  end;

  /**
  @brief добавляем строки во входящий буфер
  */
  procedure append_in_data_buffer(p_in_messbody in varchar2 default null
                                 ,p_in_messmeta in varchar2 default null) is
  begin
    if length(p_in_messbody) > 0
    then
      dbms_lob.writeappend(lob_loc => gc_in_messbody, amount => length(p_in_messbody), buffer => p_in_messbody);
    end if;
    if length(p_in_messmeta) > 0
    then
      dbms_lob.writeappend(lob_loc => gc_in_messmeta, amount => length(p_in_messmeta), buffer => p_in_messmeta);
    end if;
  exception
    when others then
      it_error.put_error_in_stack;
      it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
      raise;
  end;

  /**
  @brief возвращаем строки из результирующего буфера
  */
  function get_out_data_buffer(p_index        in number
                              ,o_out_messbody out varchar2
                              ,o_out_messmeta out varchar2
                              ,p_len_str      integer default 32676) return number is
    v_amount      integer;
    v_offset      integer;
    v_clob_length number;
  begin
    o_out_messbody := '';
    o_out_messmeta := '';
    v_offset       := ((p_index - 1) * p_len_str) + 1;
    --
    v_clob_length := dbms_lob.getlength(lob_loc => gc_out_messbody);
    if v_offset <= v_clob_length
    then
      v_amount       := least(p_len_str, (v_clob_length - v_offset + 1));
      o_out_messbody := dbms_lob.substr(lob_loc => gc_out_messbody, amount => v_amount, offset => v_offset);
    end if;
    --
    v_clob_length := dbms_lob.getlength(lob_loc => gc_out_messmeta);
    if v_offset <= v_clob_length
    then
      v_amount       := least(p_len_str, (v_clob_length - v_offset + 1));
      o_out_messmeta := dbms_lob.substr(lob_loc => gc_out_messmeta, amount => v_amount, offset => v_offset);
    end if;
    --
    return case when nvl(length(o_out_messbody), 0) > 0 or nvl(length(o_out_messmeta), 0) > 0 then 1 else 0 end;
  exception
    when others then
      it_error.put_error_in_stack;
      it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
      raise;
  end;

  /**
  @brief сохраняет ответ сервиса в буфер 
  */
  procedure set_out_data_buffer(p_out_messbody  clob
                               ,px_out_messmeta xmltype) as
    v_out_messmeta clob := px_out_messmeta.getClobVal;
    v_clob_length  number;
  begin
    v_clob_length := dbms_lob.getlength(lob_loc => p_out_messbody);
    dbms_lob.trim(lob_loc => gc_out_messbody, newlen => 0);
    if v_clob_length > 0
    then
      dbms_lob.copy(gc_out_messbody, p_out_messbody, v_clob_length);
    end if;
    v_clob_length := dbms_lob.getlength(lob_loc => v_out_messmeta);
    dbms_lob.trim(lob_loc => gc_out_messmeta, newlen => 0);
    if v_clob_length > 0
    then
      dbms_lob.copy(gc_out_messmeta, v_out_messmeta, v_clob_length);
    end if;
  exception
    when others then
      it_error.put_error_in_stack;
      it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
      raise;
  end;

  /**
  @brief Синхронный запуск сервиса  
  @return 0 - успех
  @return > 0  - код ошибки
   */
  function do_s_service(p_use_in_buffer boolean -- Использовать Входной буфер
                       ,p_servicename   itt_q_message_log.servicename%type -- Бизнес-процесс
                       ,p_messbody      varchar2 default null -- сообщение        
                       ,p_messmeta      varchar2 default null -- Мета данные
                       ,p_receiver      itt_q_message_log.receiver%type default it_q_message.C_C_SYSTEMNAME -- Система-получатель
                       ,p_priority      itt_q_message_log.priority%type default it_q_message.C_C_MSG_PRIORITY_N -- Очередность синхронных сообщений ( F- быстрая,N- норм)
                       ,p_corrmsgid     itt_q_message_log.corrmsgid%type default null -- ID исходногоо сообщения 
                       ,p_comment       varchar2 default null -- коментарии в лог
                       ,p_timeout       integer default null --  таймаут в секундах
                       ,io_msgid        in out itt_q_message_log.msgid%type -- GUID  сообщения
                       ,o_answerid      out itt_q_message_log.msgid%type -- GUID ответа
                       ,o_errtxt        out varchar2) return number as
    vx_in_messmeta  xmltype;
    v_out_messbody  clob;
    vx_out_messmeta xmltype;
    v_res           number := 0;
  begin
    o_errtxt := ' '; -- Так ...
    if p_use_in_buffer
    then
      vx_in_messmeta := it_xml.Clob_to_xml(p_clob => gc_in_messmeta, p_errparam => ' переменная gc_in_messmeta пакета it_rs_q_utils ');
      it_q_message.do_s_service(p_servicename => p_servicename
                               ,p_receiver => p_receiver
                               ,p_messbody => gc_in_messbody
                               ,p_messmeta => vx_in_messmeta
                               ,p_priority => p_priority
                               ,p_corrmsgid => p_corrmsgid
                               ,p_comment => p_comment
                               ,p_timeout => p_timeout
                               ,io_msgid => io_msgid
                               ,o_answerid => o_answerid
                               ,o_answerbody => v_out_messbody
                               ,o_answermeta => vx_out_messmeta);
    else
      vx_in_messmeta := it_xml.Clob_to_xml(p_clob => p_messmeta
                                          ,p_errparam => ' параметр p_messmeta функции it_rs_q_utils.do_s_service ');
      it_q_message.do_s_service(p_servicename => p_servicename
                               ,p_receiver => p_receiver
                               ,p_messbody => p_messbody
                               ,p_messmeta => vx_in_messmeta
                               ,p_priority => p_priority
                               ,p_corrmsgid => p_corrmsgid
                               ,p_comment => p_comment
                               ,p_timeout => p_timeout
                               ,io_msgid => io_msgid
                               ,o_answerid => o_answerid
                               ,o_answerbody => v_out_messbody
                               ,o_answermeta => vx_out_messmeta);
    end if;
    set_out_data_buffer(p_out_messbody => v_out_messbody, px_out_messmeta => vx_out_messmeta);
    return 0;
  exception
    when others then
      v_res    := abs(sqlcode);
      o_errtxt := it_q_message.get_errtxt(sqlerrm);
      return v_res;
  end;

  /**
  @brief Асинхронный запуск сервиса  
  @return 0 - успех
  @return > 0  - код ошибки
   */
  function do_a_service(p_use_in_buffer boolean -- Использовать Входной буфер
                       ,p_servicename   itt_q_message_log.servicename%type -- Бизнес-процесс
                       ,p_messbody      varchar2 default null -- сообщение        
                       ,p_messmeta      varchar2 default null -- Мета данные
                       ,p_receiver      itt_q_message_log.receiver%type default it_q_message.C_C_SYSTEMNAME -- Система-получатель
                       ,p_corrmsgid     itt_q_message_log.corrmsgid%type default null -- ID исходногоо сообщения 
                       ,p_num_thread    integer default 0
                       ,p_comment       varchar2 default null -- коментарии в лог
                       ,io_msgid        in out itt_q_message_log.msgid%type -- GUID  сообщения
                       ,o_errtxt        out varchar2) return number as
    vx_in_messmeta xmltype;
    v_num_thread   integer := nvl(p_num_thread, 0);
    v_res          number := 0;
  begin
    o_errtxt := ' '; -- Так ...
    it_q_message.thread_next(v_num_thread);
    if p_use_in_buffer
    then
      vx_in_messmeta := it_xml.Clob_to_xml(p_clob => gc_in_messmeta, p_errparam => ' переменная gc_in_messmeta пакета it_rs_q_utils ');
      it_q_message.do_a_service(p_servicename => p_servicename
                               ,p_receiver => p_receiver
                               ,p_messbody => gc_in_messbody
                               ,p_messmeta => vx_in_messmeta
                               ,p_servicegroup => it_q_message.thread_get_servicegroup
                               ,p_queue_num => it_q_message.thread_get_queue_num
                               ,p_corrmsgid => p_corrmsgid
                               ,p_comment => p_comment
                               ,io_msgid => io_msgid);
    else
      vx_in_messmeta := it_xml.Clob_to_xml(p_clob => p_messmeta
                                          ,p_errparam => ' параметр p_messmeta функции it_rs_q_utils.do_a_service ');
      it_q_message.do_a_service(p_servicename => p_servicename
                               ,p_receiver => p_receiver
                               ,p_messbody => p_messbody
                               ,p_messmeta => vx_in_messmeta
                               ,p_servicegroup => it_q_message.thread_get_servicegroup
                               ,p_queue_num => it_q_message.thread_get_queue_num
                               ,p_corrmsgid => p_corrmsgid
                               ,p_comment => p_comment
                               ,io_msgid => io_msgid);
    end if;
    return 0;
  exception
    when others then
      v_res    := abs(sqlcode);
      o_errtxt := it_q_message.get_errtxt(sqlerrm);
      return v_res;
  end;

begin
  dbms_lob.createtemporary(lob_loc => gc_in_messbody, cache => true);
  dbms_lob.createtemporary(lob_loc => gc_in_messmeta, cache => true);
  dbms_lob.createtemporary(lob_loc => gc_out_messbody, cache => true);
  dbms_lob.createtemporary(lob_loc => gc_out_messmeta, cache => true);
end it_rs_q_utils;
/
