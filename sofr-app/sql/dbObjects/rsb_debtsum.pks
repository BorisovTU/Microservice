CREATE OR REPLACE PACKAGE RSB_DEBTSUM
IS
  /*Типы задолженности по брокерскому обслуживанию*/
  DEBT_FIX_COM       CONSTANT NUMBER(5) := 1; --Минимальная брокерская комиссия
  DEBT_INVEST_COM    CONSTANT NUMBER(5) := 2; --Комиссия брокера в рамках услуги инвестиционного консультирования
  DEBT_DEAL_COM_2022 CONSTANT NUMBER(5) := 3; --Просроченная брокерская комиссия по сделкам 2022 года
  DEBT_DEAL_COM_2023 CONSTANT NUMBER(5) := 4; --Просроченная брокерская комиссия по сделкам 2023 года
  DEBT_EXPIRED       CONSTANT NUMBER(5) := 5; --Прочая просроченная задолженность

  /*Виды сообщений в протоколе*/
  ISSUE_ERROR   CONSTANT NUMBER(5) := 1; /*ошибка*/
  ISSUE_WARNING CONSTANT NUMBER(5) := 2; /*предупреждение*/

  /*Периодичность начисления комиссии*/
  SF_FEE_TYPE_PERIOD CONSTANT INTEGER := 1; /*периодическая*/

  /*Статусы записи в реестре*/
  DEBTREESTR_STATE_ACTIVE  CONSTANT NUMBER(5) := 1; /*активна*/
  DEBTREESTR_STATE_REMOVED CONSTANT NUMBER(5) := 2; /*удалена*/

 /**
 * Получим номер брокерской комиссии по коду 
 * @since RSHB 115
 * @qtest NO
 * @param p_ComCode код комиссии
 * @return номер комиссии
 */                    
  FUNCTION GetCommissNumber(p_ComCode IN VARCHAR2) RETURN NUMBER;

 /**
 * Добавление записи об ошибке/предупреждении в лог операции списания/выносе на просрочку задолженности
 * @since RSHB 115
 * @qtest NO
 * @param p_Type Тип сообщения
 * @param p_DlContrID Идентификатор ДБО
 * @param p_Text Текст сообщения
 */
  PROCEDURE AddToLog(p_Type IN NUMBER, p_DlContrID IN NUMBER, p_Text IN VARCHAR2);

 /**
 * Сохранение блока протокола в операции списания задолженности
 * @since RSHB 115
 * @qtest NO
 * @param p_ID Идентификатор операции
 * @param p_Chunk Блок данных для записи в Clob
 */
  PROCEDURE WrtOffAppendLogToClob(p_ID IN NUMBER, p_Chunk IN CLOB);

 /**
 * Сохранение блока протокола в операции выноса задолженности на просрочку
 * @since RSHB 116
 * @qtest NO
 * @param p_ID Идентификатор операции
 * @param p_Chunk Блок данных для записи в Clob
 */
  PROCEDURE EnrollAppendLogToClob(p_ID IN NUMBER, p_Chunk IN CLOB);
  
 /**
 * Подготовка данных о задолженности клиентов к списанию
 * @since RSHB 115
 * @qtest NO
 * @param p_OperID Идентификатор операции списания
 * @param p_ProcDate Дата процедуры
 * @param p_ClientID Идентификатор клиента
 * @param p_DlContrID Идентификатор ДБО
 */
  PROCEDURE GetWrtOffDebtSumData(p_OperID IN NUMBER, p_ProcDate IN DATE, p_ClientID IN NUMBER, p_DlContrID IN NUMBER);

 /**
 * Подготовка данных о задолженности клиентов к выносу на просрочку
 * @since RSHB 116
 * @qtest NO
 * @param p_OperID Идентификатор операции списания
 * @param p_ProcDate Дата процедуры
 * @param p_ClientID Идентификатор клиента
 * @param p_DlContrID Идентификатор ДБО
 */
  PROCEDURE GetEnrollDebtSumData(p_OperID IN NUMBER, p_ProcDate IN DATE, p_ClientID IN NUMBER, p_DlContrID IN NUMBER);

 /**
 * Перенос предоставляемых задолженностей для ЦФТ и данных по оплаченным задолженностям в ЦФТ в историю 
 * @since RSHB 123
 * @qtest NO
 */
  PROCEDURE TransferCFTDataToHist;

 /**
 * Подготовка данных о задолженности клиентов к списанию со счетов в ЦФТ с заполнением буферной таблицы
 * @since RSHB 123
 * @qtest NO
 * @param p_ProcDate  Дата процедуры
 * @param p_ClientID  Идентификатор клиента
 * @param p_DlContrID Идентификатор ДБО
 */
  PROCEDURE GetWrtOffDebtDataToCFT(p_ProcDate IN DATE, p_ClientID IN NUMBER, p_DlContrID IN NUMBER);

END RSB_DEBTSUM;
/