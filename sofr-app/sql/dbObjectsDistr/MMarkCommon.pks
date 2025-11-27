create or replace package MMarkCommon
is
    -- Автор: Куперин М.В.

    -- Рализация основных алгоритмов функцинирвания системы, которые возможно
    -- использовать в пользовательском коде

/**
 * получить метод определения параметров кредитного риска за дату
 * @since   6.20.031.7.1. @qtest-YES
 * @param   ControlDate дата, за которую необходимо определить метод получения параметров кредитного риска
 * @return  Номер метода определения параметров кредитного риска
*/
    function GetQCategControl(
        ControlDate in date
    )
        return pls_integer;

/**
 * проверить соответствие категории качества и % резерва за дату
 * возможно приведение в соответсвие ПР и КК, для этого есть 3 режима (параметр ChangeMode):
 * 1) ChangeMode == 1: когда надо привести в соответствие КК и % резерва, возможно изменение как % резерва, так и 
 *                     КК - зависит это настройки метода определения ПКР.
 * 2) ChangeMode == 2: когда надо привести КК в соответствие с % резерва, изменяется только КК
 * 3) ChangeMode == 3: когда надо привести % резерва в соответствие с КК, изменяется только % резерва
 * 4) ChangeMode == 0 (или любое другое значение): проверяется соответствие % резерва и КК без изменения
 * @since   6.20.031.7.1. @qtest-YES
 * @param   QualityCategory категория качества
 * @param   ReservePercent  процент резерва
 * @param   ChangeMode      режим приведения в соответсвие
 * @param   CDate           дата, за которую необходимо проверить соответствие 
 * @return  Номер ошибки, если нет соотвествия и 0, если КК и % резерва соответствуют.
*/   
    function CheckQualityCategory(
        QualityCategory in out ddlreslnk_dbt.t_qualitycategory%TYPE,
        ReservePercent  in out ddlreslnk_dbt.t_reservepercent%TYPE,
        ChangeMode      in     integer default 0,
        CDate           in     date default to_date ('0001-01-01', 'yyyy-mm-dd')
    )
        return number;

/**
 * определить параметры кредитного риска за дату в зависимости от настройки определения ПКР
 * @since   6.20.031.7.1. @qtest-YES
 * @param   DealID           ИД сделки
 * @param   PartyID          ИД контрагента
 * @param   CDate            дата, за которую определяются параметры кредитного риска
 * @param   QualityCategory  категория качества
 * @param   ReservePercent   процент резерва
 * @return  возвращает категорию качества и процент резерва через параметры
*/
    procedure GetRiskParm(
        DealID           in  number,
        PartyID          in  number,
        CDate            in  date,
        QualityCategory  out ddlreslnk_dbt.t_qualitycategory%TYPE,
        ReservePercent   out ddlreslnk_dbt.t_reservepercent%TYPE
    );

    function GetQualityCategory(
        DealID           in  number,
        PartyID          in  number,
        CDate            in  date
    )
        return ddlreslnk_dbt.t_qualitycategory%TYPE;
        
    function GetReservePercent(
        DealID           in  number,
        PartyID          in  number,
        CDate            in  date
    )
        return ddlreslnk_dbt.t_reservepercent%TYPE;

    procedure GetCurrentRiskParm(
        DealID           in  number,
        CDate            in  date,
        QualityCategory  out ddlreslnk_dbt.t_qualitycategory%TYPE,
        ReservePercent   out ddlreslnk_dbt.t_reservepercent%TYPE
    );
        
    function GetCurrentQualityCategory(
        DealID           in  number,
        CDate            in  date
    )
        return ddlreslnk_dbt.t_qualitycategory%TYPE;

    function GetCurrentReservePercent(
        DealID           in  number,
        CDate            in  date
    )
        return ddlreslnk_dbt.t_reservepercent%TYPE;
        
    function GetAccountNumber(
        DealID           in  number,
        AccCat           in  VARCHAR2,
        CDate            in  date
    )
        return dmcaccdoc_dbt.t_account%TYPE;

    function GetAccountNumber(
        DealID           in  number,
        AccCat           in  VARCHAR2,
        BeginDate        in  date,
        EndDate          in  date,
        FiRole           in  number default -1
    )
        return dmcaccdoc_dbt.t_account%TYPE;

    function GetTickStatus(
        DealID           in  number,
        Status           out VARCHAR2
    )
        return number;

/**
 * Получить значение примечания с типом money
 * @param   ObjectType       тип объекта
 * @param   ObjectID         номер объекта
 * @param   NoteKind         вид примечания
 * @return  возвращает категорию качества и процент резерва через параметры
*/
    function GetNoteTextMoney( 
        v_ObjectType IN NUMBER, 
        v_ObjectID   IN NUMBER,
        v_NoteKind   IN NUMBER,
        v_Date       IN DATE
    )
    return NUMBER;

/**
 * Получить значение примечания с типом string
 * @param   ObjectType       тип объекта
 * @param   ObjectID         номер объекта
 * @param   NoteKind         вид примечания
 * @return  возвращает категорию качества и процент резерва через параметры
*/
    function GetNoteTextString(
        v_ObjectType IN NUMBER,
        v_ObjectID   IN NUMBER,
        v_NoteKind   IN NUMBER,
        v_Date       IN DATE
    )
    return VARCHAR2;

  -- получание даты выполнения последнего шага по символу
  FUNCTION GetDateStepBySymbol(ObjN     IN NUMBER,
                               Symb     IN CHAR,
                               StepDate OUT DATE)
    RETURN NUMBER;

  -- получение остатка счета по категории
  function GetAccRest(objid    IN NUMBER,
                      objn     IN NUMBER,
                      opdate   IN DATE,
                      cat      IN VARCHAR2,
                      chapter  IN NUMBER,
                      fiId     IN NUMBER,
                      withMark IN BOOLEAN DEFAULT FALSE
  )
    return number;

  /**
   * функция возвращает сумму платежа, который был исполнен во время выполнения шага stepid
   * @param   stepid           номер шага
   * @param   operationid      номер операции
   * @param   opdate           дата, за которую необходимо получть результат  
   * @param   pm_purpose       назначение платежа
   * @return  возвращает сумму платежа
  */
  FUNCTION GetPaymSum(operationid IN NUMBER,
                      stepid      IN NUMBER,
                      opdate      IN DATE,
                      pm_purpose  IN INTEGER)
    RETURN NUMBER;
  
  /* функция возвращает первый номер шага с символом stepsymb, который был выполнен до шага stepid */  
  FUNCTION GetLastStepBeforeStep(operationid IN NUMBER,
                                 stepid      IN NUMBER,
                                 stepsymb    IN CHAR)
    RETURN NUMBER;

  /* функция возвращает сумму исполненных платежей на дату calcdate */
  FUNCTION GetPastPaymSum(objid    IN NUMBER,
                          objn     IN NUMBER,
                          calcdate IN DATE)
    RETURN NUMBER;

  /**
   * функция рассчитывает сумму процентов на сделке за период DateFrom-DateTo;
   * @param   objid            вид объекта
   * @param   objn             номер объекта
   * @param   DateFrom         дата начала периода
   * @param   DateTo           дата окончания периода
   * @return  возвращает сумму процентов
  */
  FUNCTION CalcPercent(
     objid    IN NUMBER,
     objn     IN NUMBER,
     DateFrom IN DATE,
     DateTo   IN DATE
   )
   RETURN NUMBER;

/**
 * функция формирует строку, содержащую список встречных требований для формы параметров отчетности для ЦБ РФ 
 * @param   dealid  - идентификатор сделки МБК
 * @return  строка
*/
FUNCTION GetCClaimsString(dealid IN NUMBER)
    RETURN VARCHAR2;

end MMarkCommon;
/