CREATE OR REPLACE package it_sinv is

  C_C_SYSTEM_NAME constant varchar2(128) := 'SINV';
  
  /**
  * Упаковщик исходящих сообшений в Свои Инвестиции через KAFKA
  * @since RSHB 110
  * @qtest NO
  */
  procedure out_pack_message(p_message     it_q_message_t
                            ,p_expire      date
                            ,o_correlation out varchar2
                            ,o_messbody    out clob
                            ,o_messmeta    out xmltype);

  --корпоративные действия. Погашения бумаг. Неторговые поручения. Запрос на блокировку лимитов
  procedure ca_sec_nontrade_ord_lim_listener(
    p_worklogid integer
   ,p_messbody  clob
   ,p_messmeta  xmltype
   ,o_msgid     out varchar2
   ,o_MSGCode   out integer
   ,o_MSGText   out varchar2
   ,o_messbody  out clob
   ,o_messmeta  out xmltype
  );

  procedure send_nontrade_limit_state (
    p_deal_id integer,
    p_limit_status integer
  );

  --корпоративные действия. Погашения бумаг. Неторговые поручения. Исполнение поручения
  procedure ca_sec_nontrade_ord_fin_listener(
    p_worklogid integer
   ,p_messbody  clob
   ,p_messmeta  xmltype
   ,o_msgid     out varchar2
   ,o_MSGCode   out integer
   ,o_MSGText   out varchar2
   ,o_messbody  out clob
   ,o_messmeta  out xmltype
  );

  procedure send_ca_securities_final_state (
    p_id pko_writeoff.id%type
  );
end it_sinv;
/