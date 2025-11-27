create or replace package it_rs_q_utils is

  /**
   @file it_rs_q_utils
   @brief Пакет для инструментов взвимодействия RSL и QManager
   
       
   # changeLog
   |date       |author      |tasks                                                     |note                                                        
   |-----------|------------|----------------------------------------------------------|-------------------------------------------------------------
   |2023.09.11 |Зыков М.В.  |CCBO-7543 BIQ-13171                                       | Создание;                   
  */
  /**
  @brief  Очистка буферов данных
  */
  procedure clear_data_buffer;

  /**
  @brief добавляем строки во входящий буфер
  */
  procedure append_in_data_buffer(p_in_messbody in varchar2 default null
                                 ,p_in_messmeta in varchar2 default null);

  /**
  @brief возвращаем строки из результирующего буфера
  @return 0 - нет данных
  @return = 1 - не закончено 
  */
  function get_out_data_buffer(p_index        in number
                              ,o_out_messbody out varchar2
                              ,o_out_messmeta out varchar2
                              ,p_len_str      integer default 32676) return number;

  /**
  @brief Синхронный запуск сервиса  
  @return 0 - успех
  @return >0 - код ошибки
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
                       ,o_errtxt        out varchar2) return number;

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
                       ,o_errtxt        out varchar2) return number;

end it_rs_q_utils;
/
