create or replace package RSB_DIASOFT is
  /**
   @file RSB_DIASOFT.pks
   @brief Работа по обмену сообщениями DIASOFT. BIQ-13198
     
   # changeLog
   |date       |author       |tasks                                                     |note                                                        
   |-----------|-------------|----------------------------------------------------------|-------------------------------------------------------------
   |2023.11.15 |Сенников И.В.|BIQ-13198                                                 | Создание                  
    
  */

  --Статусы записи DCDRECORDS_DBT (DNAMEALG_DBT.T_ITYPEALG = 7340)
  CDRECORDS_STATUS_NEW              CONSTANT NUMBER := 0;  --Новая запись
  CDRECORDS_STATUS_PROCESSED        CONSTANT NUMBER := 1;  --Обработано
  CDRECORDS_STATUS_VALIDATION_ERROR CONSTANT NUMBER := 2;  --Ошибка валидации
  CDRECORDS_STATUS_PROCESSING_ERROR CONSTANT NUMBER := 3;  --Ошибка обработки
  CDRECORDS_STATUS_REJECTED         CONSTANT NUMBER := 4;  --Отклонено

  -- Создание НДР по строке DCDRECORDS_DBT
  procedure MakeTaxObjByDepoInfo(p_rec DCDRECORDS_DBT%ROWTYPE);

  --  Обработчик SendDepoPaymentInfoReq
  procedure SendDepoPaymentInfoReq(p_worklogid integer
                                  ,p_messbody  clob
                                  ,p_messmeta  xmltype
                                  ,o_msgid     out varchar2
                                  ,o_MSGCode   out integer
                                  ,o_MSGText   out varchar2
                                  ,o_messbody  out clob
                                  ,o_messmeta  out xmltype);

  -- Заполнить СОФРовые ID в записи
  procedure SetSOFR_IDs(p_ID IN NUMBER);
  
  -- Обработка всех выплат по купонам INTR
  PROCEDURE MakeTaxObjForINTR( p_term INTEGER /*срок обработки выплат по погашению*/ );

end RSB_DIASOFT;

