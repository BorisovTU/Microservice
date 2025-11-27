CREATE OR REPLACE PACKAGE RSB_BASESUM
IS
  /*Виды действий*/
  ACTION_CALC   CONSTANT NUMBER(5) := 1; /*расчет*/  
  ACTION_RECALC CONSTANT NUMBER(5) := 2; /*пересчет*/
  ACTION_DELETE CONSTANT NUMBER(5) := 3; /*удаление*/
  ACTION_INFO   CONSTANT NUMBER(5) := 4; /*информационный расчет*/
  ACTION_CHECK  CONSTANT NUMBER(5) := 5; /*проверка СЧА при подключении опции*/

  /*Виды сообщений в протоколе*/
  ISSUE_ERROR   CONSTANT NUMBER(5) := 1; /*ошибка*/
  ISSUE_WARNING CONSTANT NUMBER(5) := 2; /*предупреждение*/

  /*Периодичность начисления комиссии*/
  SF_FEE_TYPE_PERIOD CONSTANT INTEGER := 1; /*периодическая*/

  /*Длительность периода начисления*/
  SF_TYPE_PERIOD_DAY   CONSTANT INTEGER := 1; /*день*/
  SF_TYPE_PERIOD_MONTH CONSTANT INTEGER := 2; /*месяц*/
  SF_TYPE_PERIOD_QUART CONSTANT INTEGER := 3; /*квартал*/
  SF_TYPE_PERIOD_YEAR  CONSTANT INTEGER := 4; /*год*/

  /*Виды услуг ДБО*/
  DLCONTR_SERVKIND_INVESTADVICE CONSTANT INTEGER := 1; /*инвестиционное консультирование*/

 /**
 * Получим номер комиссии по коду 
 * @since RSHB 101
 * @qtest NO
 * @param p_ComCode код комиссии
 * @return номер комиссии
 */                    
  FUNCTION GetCommissNumber(p_ComCode IN VARCHAR2)
    RETURN NUMBER;

 /**
 * Получение итоговой преобразованной суммы комиссии до взятия процента
 * @since RSHB 101
 * @qtest NO
 * @param p_SfContrID Идентификатор субдоговора
 * @param p_ConComID Идентификатор связки комиссии с договором обслуживания 
 * @param p_BeginDate Дата начала периода начисления
 * @param p_EndDate Дата окончания периода начисления
 * @return преобразованная итоговая сумма комиссии 
 */
  FUNCTION GetItogBaseSum(p_SfContrID IN NUMBER, p_ConComID IN NUMBER, p_BeginDate IN DATE, p_EndDate IN DATE)
    RETURN NUMBER;

 /**
 * Дополнительные действия по комиссии "ИнвестСоветник" при подключении услуги Инвестиционного консультирования 
 * @since RSHB 101
 * @qtest NO
 * @param p_DlContrID Идентификатор ДБО
 * @param p_BegDate Дата подключения услуги
 */
  PROCEDURE ConnectingInvestmentAdvisor(p_DlContrID IN NUMBER, p_BegDate IN DATE);

 /**
 * Дополнительные действия по комиссии "ИнвестСоветник" при отключении услуги Инвестиционного консультирования 
 * @since RSHB 101
 * @qtest NO
 * @param p_DlContrID Идентификатор ДБО
 * @param p_EndDate Дата отключения услуги
 */
  PROCEDURE DisablingInvestmentAdvisor(p_DlContrID IN NUMBER, p_EndDate IN DATE);

 /**
 * Сохранение блока протокола в операции расчета базовых сумм
 * @since RSHB 101
 * @qtest NO
 * @param p_ID Идентификатор операции
 * @param p_Chunk Блок данных для записи в Clob
 */
  PROCEDURE AppendLogToClob(p_ID IN NUMBER, p_Chunk IN CLOB);
  
 /**
 * Получение Id курса ФИ для определения рыночной стоимости на дату
 * @since RSHB 101
 * @qtest NO
 * @param p_FIID Идентификатор ц/б
 * @param p_Date Дата котировки
 * @return идентификатор котировки 
 */
  FUNCTION GetActiveRateId(p_FIID IN NUMBER, p_Date IN DATE) 
    RETURN NUMBER deterministic result_cache;

 /**
 * Расчет/пересчет базовой стоимости и суммы комиссии по ДБО на дату по комиссии ИнвестСоветник
 * @since RSHB 101
 * @qtest NO
 * @param p_OperID Идентификатор операции расчета
 * @param p_DlContrID Идентификатор ДБО
 * @param p_ClientID Идентификатор клиента
 * @param p_Action Вид действия
 * @param p_CalcDate Дата расчета
 * @param p_ComNumber Номер комиссии
 * @param p_FeeType Тип взимания комиссии
 */
  PROCEDURE CalcBaseSumOnInvestAdviser(p_OperID IN NUMBER, p_DlContrID IN NUMBER, p_ClientID IN NUMBER, p_Action IN NUMBER, p_CalcDate IN DATE, p_ComNumber IN NUMBER, p_FeeType IN NUMBER);

END RSB_BASESUM;
/