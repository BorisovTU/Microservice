CREATE OR REPLACE package it_efr is

  C_C_SYSTEM_NAME constant varchar2(128) := 'EFR';
  
  OBJTYPE_EFR_HEADERS constant number := 4180; --Справочник статических header-ов для обмена с ЕФР

  /*Коды критичных ошибок*/
  ERROR_IN_THE_SERVICE       CONSTANT NUMBER(5) := 20000; /*Ошибка в работе сервиса*/
  ERROR_UNEXPECTED_GET_DATA  CONSTANT NUMBER(5) := 2000;  /*Непредвиденная ошибка получения данных в СОФР*/

  /**
  * Упаковщик исходящих сообшений в ЕФР через KAFKA
  * BIQ-18375 Автоматизация отчетной формы "Справка 5798-У" (справка для гос. служащего)
  * @since RSHB 105
  * @qtest NO
  */
  procedure out_pack_message(p_message     it_q_message_t
                            ,p_expire      date
                            ,o_correlation out varchar2
                            ,o_messbody    out clob
                            ,o_messmeta    out xmltype);

end it_efr;
/