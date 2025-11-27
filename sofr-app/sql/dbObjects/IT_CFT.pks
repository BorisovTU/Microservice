CREATE OR REPLACE PACKAGE it_cft IS

  C_C_SYSTEM_NAME               CONSTANT VARCHAR2(128) := 'CFT';
  
  -- Упаковщик исходящх сообшений в ЦФТ через KAFKA
  PROCEDURE out_pack_message(p_message     it_q_message_t
                            ,p_expire      date
                            ,o_correlation out varchar2
                            ,o_messbody    out clob
                            ,o_messmeta    out xmltype);
  
  --  BOSS-7047 Загрузка из ЦФТ номеров счетов, на которых отражаются суммы векселей, полученных в залог
  PROCEDURE SendPromissoryNotesAccList(p_worklogid INTEGER 
                                      ,p_messbody  CLOB 
                                      ,p_messmeta  XMLTYPE 
                                      ,o_msgid     OUT VARCHAR2 
                                      ,o_MSGCode   OUT INTEGER 
                                      ,o_MSGText   OUT VARCHAR2 
                                      ,o_messbody  OUT CLOB 
                                      ,o_messmeta  OUT XMLTYPE);
                                      
/*
    Отправка данных в топик 
    @since RSHB 123
    @qtest NO
    @param p_Date ID фин инструмента
    @param p_DlContr ID фин инструмента
    @param p_CurrencyId ID фин инструмента
    @param o_ErrorCode ID фин инструмента
    @param o_ErrorDesc ID фин инструмента
 */
   PROCEDURE SendDebtListToCFT(p_DlContr    IN  NUMBER DEFAULT -1,
                               p_CurrencyId IN  NUMBER DEFAULT -1);   
                               
  /*
  Обработка входящих сообщений 
  @since RSHB 123
  @qtest NO
  @param p_worklogid  Идентификатор записи в Qmanager
  @param p_messbody   Входящее сообщение (CLOB)
  @param p_messmeta   Заголовок/метаданные (XMLTYPE)
  @param o_msgid      Идентификатор сообщения (OUT)
  @param o_MSGCode    Код результата (OUT)
  @param o_MSGText    Текст результата/ошибки (OUT)
  @param o_messbody   Сформированное сообщение (OUT)
  @param o_messmeta   Сформированные метаданные (OUT)
*/  
   PROCEDURE WrtoffDebtSumFRomCFT (p_worklogid INTEGER,
                                   p_messbody  CLOB,
                                   p_messmeta  XMLTYPE,
                                   o_msgid     OUT VARCHAR2,
                                   o_MSGCode   OUT INTEGER,
                                   o_MSGText   OUT VARCHAR2,
                                   o_messbody  OUT CLOB,
                                   o_messmeta  OUT XMLTYPE);                                
                                      
                                      
END it_cft;
/