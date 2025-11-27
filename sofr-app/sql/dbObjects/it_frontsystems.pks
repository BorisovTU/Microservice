create or replace package it_frontsystems is
 /**
   @file it_frontsystems.pkb
   @brief Пакет для интеграции 
   
    
   # tag
   - functional_block:Интеграция 
   - code_type:API 
        
   # changeLog
   |date       |author      |tasks           |note                                                        
   |-----------|------------|----------------|-------------------------------------------------------------
   |2024.09.26 |Зыков М.В.  |BOSS-1585       |Реализация в СОФР интеграции с ЕФР и ДБО ФЛ по отправке статусов исполненных поручений в рамках автоматизации поручений на вывод и перевод денежных средств по брокерским счетам                    
    
    
  */

  -- Упаковщик исходящх сообшений в JSON через KAFKA
  procedure out_pack_message(p_message     it_q_message_t
                            ,p_expire      date
                            ,o_correlation out varchar2
                            ,o_messbody    out clob
                            ,o_messmeta    out xmltype);

  procedure save_req_from_json(p_worklogid integer
                              ,p_messbody  clob
                              ,p_messmeta  xmltype
                              ,o_msgid     out varchar2
                              ,o_MSGCode   out integer
                              ,o_MSGText   out varchar2
                              ,o_messbody  out clob
                              ,o_messmeta  out xmltype);

  -- получение json сообщения  о статусе и отправка в траспорт 
  procedure form_status_message_json(p_system_origin  varchar2 --  "ДБО ФЛ" или "ЕФР" в зависимости от источника
                                    ,p_status         varchar2 --  Текстовая расшифровка числового значения из send_order_status()
                                    ,p_decline_reason varchar2 -- Причина отклонения. Определяется по примечанию вида 103 "Отметка об отказе в исполнении"
                                    ,p_clientid       varchar2 -- ЦФТ-id клиента из поручения. Определяется по коду вида 101 для клиента из DNPTXOP.T_CLIENT
                                    ,p_orderid        varchar2 -- Идентификатор поручения из внешней системы.
                                    ,p_operid         DNPTXOP_DBT.T_ID%type -- Идентификатор T_ID из DNPTXOP_DBT.
                                    ,o_ErrorCode      out integer
                                    ,o_ErrorDesc      out varchar2);

  procedure mock_answer(p_worklogid integer
                       ,p_messbody  clob
                       ,p_messmeta  xmltype
                       ,o_msgid     out varchar2
                       ,o_MSGCode   out integer
                       ,o_MSGText   out varchar2
                       ,o_messbody  out clob
                       ,o_messmeta  out xmltype);
end it_frontsystems;
/
