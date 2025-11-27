create or replace package it_quik is

 /***************************************************************************************************\
    Пакет для работы QManagera c QUIK
   **************************************************************************************************
    Изменения:
   ---------------------------------------------------------------------------------------------------
   Дата        Автор            Jira                          Описание 
   ----------  ---------------  ---------------------------   ----------------------------------------
   23.10.2023  Зыков М.В.       BOSS-1230                     BIQ-15498.BOSS-1230 Доработка QManager для передачи сообщений в Кафку
  \**************************************************************************************************/

  C_C_SYSTEM_NAME constant varchar2(128) := 'QUIK';

  -- Упаковщик исходящх сообшений в QIUK через KAFKA
  procedure out_pack_message(p_message     it_q_message_t
                            ,p_expire      date
                            ,o_correlation out varchar2
                            ,o_messbody    out clob
                            ,o_messmeta    out xmltype);

  -- BIQ-15498 Отправка сообщения поручения на ввод денежных средств  для автоматизации процесса обработки поручения и  корректировки лимитов по денежным средствам 
  procedure LimitsNewInstrMonReq(p_msgID     in varchar2 -- ID Сообщения,
                                ,p_JSON      in varchar2
                                ,o_ErrorCode out number -- != 0 ошибка o_ErrorDesc
                                ,o_ErrorDesc out varchar2
                                ,p_comment   in varchar2 default null);

  -- BIQ-15498 Обработка Ответа  поручения на ввод денежных средств  для автоматизации процесса обработки поручения и  корректировки лимитов по денежным средствам 
  procedure LimitsNewInstrMonResp(p_worklogid integer
                                 ,p_messbody  clob
                                 ,p_messmeta  xmltype
                                 ,o_msgid     out varchar2
                                 ,o_MSGCode   out integer
                                 ,o_MSGText   out varchar2
                                 ,o_messbody  out clob
                                 ,o_messmeta  out xmltype);

--BIQ-27551 Отправка сообщения с данными об операции списания ценных бумаг ПКО                             
   procedure sent_nontrade_secure_limits(p_msgID     in varchar2 
                                       ,p_CORRmsgid in varchar2
                                       ,o_ErrorCode out number -- != 0 ошибка o_ErrorDesc
                                       ,o_ErrorDesc out varchar2);


   procedure nontrade_secur_limits_listener(p_worklogid integer
                                 ,p_messbody  clob
                                 ,p_messmeta  xmltype
                                 ,o_msgid     out varchar2
                                 ,o_MSGCode   out integer
                                 ,o_MSGText   out varchar2
                                 ,o_messbody  out clob
                                 ,o_messmeta  out xmltype);  
                                 
end it_quik;
/
