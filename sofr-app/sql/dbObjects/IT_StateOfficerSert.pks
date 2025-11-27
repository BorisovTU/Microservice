CREATE OR REPLACE PACKAGE IT_STATEOFFICERSERT
IS
  /*Виды дохода*/
  INCOME_TYPE_COUPON     CONSTANT NUMBER(5) := 20; /*Купонный доход*/ 
  INCOME_TYPE_SECURITIES CONSTANT NUMBER(5) := 30; /*Доходы от операций с ценными бумагами*/
  INCOME_TYPE_BILLS      CONSTANT NUMBER(5) := 40; /*Доходы по векселям*/                

  /*Коды критичных ошибок*/
  ERROR_CLIENT_NOT_DEFINED   CONSTANT NUMBER(5) := 1000; /*Не удалось определить клиента в СОФР по коду ClientId*/ 
  ERROR_CONTRACT_NOT_DEFINED CONSTANT NUMBER(5) := 1100; /*Не удалось определить действующий договор по клиенту в СОФР с кодом клиента ClientId*/
  ERROR_UNEXPECTED_GET_DATA  CONSTANT NUMBER(5) := 2000; /*Непредвиденная ошибка получения данных в СОФР*/

  /*Коды ошибок при определении стоимостей бумаг*/
  ERROR_AVOIRISS_NOT_DEFINED     CONSTANT NUMBER(5) := 100; /*Не удалось определить в СОФР выпуск по коду*/ 
  ERROR_DIFFERENT_AVOIRISS_COUNT CONSTANT NUMBER(5) := 200; /*Количество бумаг выпуска по сделкам в СОФР не совпадает с количеством бумаг от Диасофт*/
  ERROR_DETERMINATION_COST       CONSTANT NUMBER(5) := 300; /*Ошибка определения покупной стоимости. Количество бумаг выпуска по сделкам в СОФР не совпадает с количеством бумаг от Диасофт (при выявлении на поздних этапах)*/
  ERROR_UNEXPECTED_GET_COST      CONSTANT NUMBER(5) := 900; /*Непредвиденная ошибка определения покупной стоимости по выпуску*/

  /*Виды кодов субъектов*/
  PTCK_CFT  CONSTANT NUMBER(5) := 101; /*Код ЦФТ*/

  /**
  * Обработчик EFR.SendStateOfficerSertSofr
  * BIQ-18375 Автоматизация отчетной формы "Справка 5798-У" (справка для гос. служащего)
  * Доработка формирования отчетной формы путем ее обогащения данными из СОФР входящего сообщения XML из топика Кафки от ЕФР 
  * @since RSHB 104
  * @qtest NO
  */
  PROCEDURE GetReferenceFromEFR(p_worklogid integer     
                               ,p_messbody  clob        
                               ,p_messmeta  xmltype     
                               ,o_msgid     out varchar2
                               ,o_MSGCode   out integer 
                               ,o_MSGText   out varchar2
                               ,o_messbody  out clob    
                               ,o_messmeta  out xmltype);

  /**
  * Обогащениe полученной от ЕФР информации данными по видам дохода и по стоимости приобретения ценных бумаг из СОФР 
  * @since RSHB 104
  * @qtest NO
  * @param p_RefID идентификатор экземпляра запроса от ЕФР
  */
  PROCEDURE AddingDataFromSOFR(p_RefID IN NUMBER);
 
  /**
  * Получение внутреннего идентификатора ценной бумаги в СОФР по коду ISIN/LSIN, возможно содержащемуся в наименовании бумаги 
  * @since RSHB 104
  * @qtest NO
  * @param p_Name     Наименование ц/б
  * @param p_ISINLSIN Идентификатор выпуска ISIN/LSIN
  * @return идентификатор ц/б
  */ 
  FUNCTION GetAvoirFIIDByName(p_Name IN VARCHAR2, p_ISINLSIN OUT VARCHAR2) 
    RETURN NUMBER;

  /**
  * Преобразование числа в строку при необходимости с добавлением нуля в целой части
  * @since RSHB 104
  * @qtest NO
  * @param p_Val Число
  * @return число в формате строки
  */    
  FUNCTION LeadZero(p_Val IN NUMBER)
    RETURN VARCHAR2;

END IT_STATEOFFICERSERT;
/