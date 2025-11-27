CREATE OR REPLACE PACKAGE IT_RUDATA
IS

  C_C_SYSTEM_NAME constant varchar2(128) := 'IPS_RUDATA';
  
    /*Загрузка из JSON-структуры данных номинала на конец месяца, приходящийся на выходной день*/
    PROCEDURE Set_Nominal_From_Rudata (
      p_json IN CLOB);
   
/*
  Обёртка для вызова DateOptionsTable_Rq без указания ключа инструмента
  Используется для планировщика, p_instr_key передается null, для выгрузки по всем инструментам 
  @since RSHB 118
  @qtest NO
  @param p_result_code Код результата выполнения (0 ? успех, 1 ? ошибка)
  @param p_result_text Описание результата выполнения или текста ошибки
*/    
  PROCEDURE DateOptionsTableWrapper_Rq(p_result_code   OUT NUMBER,  
                                        p_result_text   OUT VARCHAR);   

/*
  Обёртка для вызова процедуры формирования JSON-запроса DateOptionsTable_Rq.
  Используется при вызове пользовательской функции на ЦБ
  @since RSHB 118
  @qtest NO
  @param p_fiid Ключ инструмента (ISIN/LSIN). 
  @param p_result_code   Код результата выполнения (0 ? успех, 1 ? ошибка)
  @param p_result_text   Описание результата выполнения или текста ошибки
*/     
  PROCEDURE DateOptionsTableWrapper_Rq(p_fiid     IN davoiriss_dbt.t_fiid%type,
                                         p_result_code   OUT NUMBER,  
                                         p_result_text   OUT VARCHAR);
/*
  Обёртка для вызова процедуры формирования JSON-запроса DateOptionsTable_Rq
  @since RSHB 118
  @qtest NO
  @param p_instr_key Ключ инструмента (ISIN/LSIN), передаваемый во внутреннюю процедуру DateOptionsTable_Rq
*/                                        
  PROCEDURE DateOptionsTableWrapper_Rq(p_instr_key     IN VARCHAR2); 

/*
  Формирование тела и заголовка JSON-запроса к методу v2/Bond/DateOptionsTable
  @since RSHB 118
  @qtest NO
  @param p_instr_key     Ключ инструмента (ISIN/LSIN). Если NULL ? формируется список по условиям отбора из базы
  @param p_qman_send     Признак необходимости отправки сообщения в очередь QManager
  @param p_result_code   Код результата выполнения (0 ? успех, 1 ? ошибка)
  @param p_result_text   Описание результата выполнения или текста ошибки
  @param p_bdy           Сформированный JSON body для запроса
  @param p_hdr           Сформированный JSON header для запроса
*/     
  PROCEDURE DateOptionsTable_Rq( p_instr_key     IN VARCHAR2,  
                                  p_qman_send     IN BOOLEAN DEFAULT TRUE,  
                                  p_result_code   OUT NUMBER,  
                                  p_result_text   OUT VARCHAR2,  
                                  p_bdy           OUT CLOB,      
                                  p_hdr           OUT CLOB);
/*
  Обработка ответа от сервиса DateOptionsTable: извлечение и сохранение данных номинале инструмента
  @since RSHB 118
  @qtest NO
  @param p_hdr           JSON-заголовок ответа, содержащий поле symbol
  @param p_bdy           JSON-тело ответа, содержащее данные по current_fv и fv_last_known_date
  @param p_result_code   Код результата выполнения (0 ? успех, 1 ? ошибка)
  @param p_result_text   Описание результата выполнения или текста ошибки
*/                                  
  PROCEDURE DateOptionsTable_Resp ( p_hdr           IN CLOB,   
                                    p_bdy           IN CLOB,       
                                    p_result_code   OUT NUMBER,   
                                    p_result_text   OUT VARCHAR2);                           
/*
  Конвертация текстовой даты в тип DATE (ISO 8601)
  @since RSHB 118
  @qtest NO
  @param p_textDate Дата в строковом формате 'YYYY-MM-DD"T"HH24:MI:SS"Z"'
  @return Значение типа DATE
*/                                  
  FUNCTION getАvoirissFiidByIsin(p_isin VARCHAR2) RETURN davoiriss_dbt.t_fiid%type;

/**
  Получение идентификатора avoiriss по ISIN/LSIN
  @since RSHB 118
  @qtest NO
  @param p_isin Значение ISIN или LSIN
  @return t_fiid или -1 при ошибке
*/  
  FUNCTION getАvoirissIsinByFiid(p_fiid DAVOIRISS_DBT.T_FIID%TYPE) RETURN DAVOIRISS_DBT.T_ISIN%TYPE;

/*
  Получение ISIN/LSIN по fiid
  @since RSHB 118
  @qtest NO
  @param p_fiid Идентификатор fiid
  @return ISIN или LSIN, NULL при ошибке
*/   
  FUNCTION converFromTextToNumber(p_text VARCHAR2) RETURN NUMBER;

/*
  Конвертация текстового значения в число (с учётом замены точки на запятую)
  @since RSHB 118
  @qtest NO
  @param p_text Строковое представление числа
  @return Число с округлением до 12 знаков
*/   
  FUNCTION convertFromTextToDate(p_textDate VARCHAR2) RETURN DATE;

/*
  Проверка признака исключения записи по fiid в справочнике dnotetext_dbt
  @since RSHB 118
  @qtest NO
  @param p_fiid Идентификатор fiid
  @return 1 ? исключаем; 0 ? не исключаем
*/  
  FUNCTION getOffRudataByFiid(p_fiid DAVOIRISS_DBT.T_FIID%TYPE) RETURN NUMBER;

 /*
  Проверка: является ли дата первым или последним выходным днём месяца
  @since RSHB 118
  @qtest NO
  @param p_date Дата для проверки
  @return 1 ? да; 0 ? нет
*/ 
  FUNCTION getIsFirstOrLastDayOff(p_date DATE) RETURN NUMBER;

/*
  Удаление кавычек вокруг true-значений в JSON
  @since RSHB 118
  @qtest NO
  @param p_json JSON в виде CLOB
  @return JSON с приведёнными к булевому виду значениями true
*/ 
  FUNCTION cleanJsonTrueFields(p_json CLOB) RETURN CLOB;
  
/*
  При выполнении сервисной операции начисления доходов расходов 
  исполненяется проверка
  @since RSHB 118
  @qtest NO
  @param p_date Дата проверки
  @param p_end_date дата окончания периода начисления
  @return p_fiid Идентификатор fiid инструмента ЦБ
*/
  FUNCTION checkNDR_FaceValue(p_date     date,
                              p_end_date date,
                              p_fiid     davoiriss_dbt.t_fiid%TYPE) RETURN NUMBER;
                              
                          
 /**
  * Упаковщик исходящх сообшений через KAFKA
  * @since RSHB 118
  * @qtest NO
  * @param p_message Исходное сообщение
  * @param p_expire 
  * @param o_correlation
  * @param o_messbody Упакованное сообщение
  * @param o_messmeta 
  */
  PROCEDURE out_pack_message( p_message     it_q_message_t
                             ,p_expire      date
                             ,o_correlation out varchar2
                             ,o_messbody    out clob
                             ,o_messmeta    out xmltype);
  
END IT_RUDATA;
/