CREATE OR REPLACE PACKAGE RSI_NPTXCALC
IS
  g_Limit NUMBER := 5000;

  g_ZeroDate DATE := TO_DATE('01.01.0001', 'DD.MM.YYYY');
  g_MaxDate  DATE := TO_DATE('31.12.9999', 'DD.MM.YYYY');

  g_MaxBuyDateForRepl DATE := TO_DATE('01.03.2022','DD.MM.YYYY');

  g_TaxDepositoryMode      NUMBER := NULL;
  g_ConsRepaymPit          NUMBER := NULL;
  g_IsIncludeMaterialInOut NUMBER := NULL;

  TYPE nptxobj_t IS TABLE OF DNPTXOBJ_TMP%ROWTYPE INDEX BY BINARY_INTEGER;

  FUNCTION TaxDepositoryMode RETURN NUMBER RESULT_CACHE;
  FUNCTION ConsRepaymPit RETURN NUMBER  RESULT_CACHE;
  FUNCTION IsIncludeMaterialInOut RETURN NUMBER  RESULT_CACHE;

  FUNCTION K(p_FromFI IN NUMBER, p_Date IN DATE) RETURN NUMBER RESULT_CACHE;

  PROCEDURE LoadNptxData(p_ClientID IN NUMBER, p_BegDate IN DATE, p_EndDate IN DATE);

  FUNCTION GetNPTXObjSum(p_BegDate IN DATE, p_EndDate IN DATE, p_Client IN NUMBER, p_Kind IN NUMBER, p_IIS IN NUMBER, p_Contract IN NUMBER) RETURN NUMBER;

  FUNCTION GetSumByLnkWithSign_0(p_LinkID IN NUMBER, p_Kind IN NUMBER, p_BegDate IN DATE, p_EndDate IN DATE, p_NoRound IN NUMBER DEFAULT 0) RETURN NUMBER;
  FUNCTION GetSumByLnkWithSign_0_tmp(p_LinkID IN NUMBER, p_Kind IN NUMBER, p_BegDate IN DATE, p_EndDate IN DATE, p_NoRound IN NUMBER DEFAULT 0) RETURN NUMBER;

  /**
  @brief Скорректировать параметр "Технический" в объектах НДР во временной таблице
  @param [in] p_DocID Идентификатор операции (расчета НОБ), в которой выполняется создание и корректировка объектов
  */
  PROCEDURE CorrectTechnicalInObjTMP(p_DocID IN NUMBER, p_Step IN NUMBER, p_Level IN NUMBER);

  /**
  @brief Для объектов НДР, создаваемых в операциях пересчета НОБ, определяем признак "Технический расчет"
  @param [in] pBegDate Дата начала
  @param [in] pEndDate Дата окончания
  @param [in] pClient ID клиента
  @param [in] pKind Вид объектов НДР
  @param [in] pAnaliticKind1 Вид аналитики 1
  @param [in] pAnalitic1 Значение аналитики 1
  @param [in] pAnalitickind2 Вид аналитики 2
  @param [in] pAnalitic2 Значение аналитики 2
  @param [in] pAnaliticKind3 Вид аналитики 3
  @param [in] pAnalitic3 Значение аналитики 3
  @param [in] pAnaliticKind4 Вид аналитики 4
  @param [in] pAnalitic4 Значение аналитики 4
  @param [in] pAnaliticKind5 Вид аналитики 5
  @param [in] pAnalitic5 Значение аналитики 5
  @param [in] pAnaliticKind6 Вид аналитики 6
  @param [in] pAnalitic6 Значение аналитики 6
  @param [in] pTechnical Признак технического расчета, установленный на операции расчета НОБ
  @param [in] pRecalc Признак "Пересчет", установленный на операции расчета НОБ
  */
  FUNCTION IsTechnicalOldNPTXOBJ(pDocID IN NUMBER,
                                 pBegDate IN DATE,
                                 pEndDate IN DATE,
                                 pClient IN NUMBER,
                                 pKind IN NUMBER,
                                 pLevel IN NUMBER,
                                 pAnaliticKind1 IN NUMBER,
                                 pAnalitic1 IN NUMBER,
                                 pAnalitickind2 IN NUMBER,
                                 pAnalitic2 IN NUMBER,
                                 pAnaliticKind3 IN NUMBER,
                                 pAnalitic3 IN NUMBER,
                                 pAnaliticKind4 IN NUMBER,
                                 pAnalitic4 IN NUMBER,
                                 pAnaliticKind5 IN NUMBER,
                                 pAnalitic5 IN NUMBER,
                                 pAnaliticKind6 IN NUMBER,
                                 pAnalitic6 IN NUMBER,
                                 pTechnical IN NUMBER,
                                 pRecalc IN CHAR,
                                 pOutSystCode IN VARCHAR2 DEFAULT CHR(1),
                                 pTransfDate IN DATE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY'),
                                 pTransfKind IN NUMBER DEFAULT 0,
                                 pHolding_Period IN NUMBER DEFAULT 0
                                ) RETURN CHAR;

/**
  @brief Удаление объектов НДР с установленным признаком "Технический"
  @param [in] pDocID ID текущей операции
  @param [in] pStep Шаг текущей операции
  @param [in] pOperDate Дата текущей операии
  @param [in] pCLientID ID клиента
  @param [in] pKind Вид объектов НДР
  @param [in] pAnaliticKind1 Вид аналитики 1
  @param [in] pAnalitic1 Значение аналитики 1
  @param [in] pAnaliticKind2 Вид аналитики 2
  @param [in] pAnalitic2 Значение аналитики 2
  @param [in] pAnaliticKind3 Вид аналитики 3
  @param [in] pAnalitic3 Значение аналитики 3
  @param [in] pAnaliticKind4 Вид аналитики 4
  @param [in] pAnalitic4 Значение аналитики 4
  @param [in] pAnaliticKind5 Вид аналитики 5
  @param [in] pAnalitic5 Значение аналитики 5
  @param [in] pAnaliticKind6 Вид аналитики 6
  @param [in] pAnalitic6 Значение аналитики 6
  @param [in] pExistEarlyNPTXOPTech Существует более ранняя операция расчета НОБ с категорией "Признак технического расчета" = "Технический"
  @param [in] pTechnical Значение категории "Признак технического расчета" на операции
  @param [in] pRecalc Значение признака "Пеесчет" из панели текущей операции
  @param [in] pTransfKind Значение признака трансформации
  */
  PROCEDURE DeleteNPTXOBJTech(pDocID NUMBER,
                              pStep NUMBER,
                              pOperDate DATE,
                              pCLientID NUMBER,
                              pKind NUMBER,
                              pAnaliticKind1 NUMBER,
                              pAnalitic1 NUMBER,
                              pAnaliticKind2 NUMBER,
                              pAnalitic2 NUMBER,
                              pAnaliticKind3 NUMBER,
                              pAnalitic3 NUMBER,
                              pAnaliticKind4 NUMBER,
                              pAnalitic4 NUMBER,
                              pAnaliticKind5 NUMBER,
                              pAnalitic5 NUMBER,
                              pAnaliticKind6 NUMBER,
                              pAnalitic6 NUMBER,
                              pExistEarlyNPTXOPTech NUMBER,
                              pRecalc CHAR,
                              pOutSystCode VARCHAR2 DEFAULT CHR(1),
                              pTransfKind NUMBER DEFAULT 0,
                              pHolding_Period IN NUMBER DEFAULT 0
                             );

  /**
  @brief Получение суммы объектов НДР с признаком "Технический", которые были созданы ранее даты текущей операции
  @param [in] pDocID ID текущей операции
  @param [in] pOperDate Дата текущей операции
  @param [in] pClientID ID клиента
  @param [in] pKind Вид объектов НДР
  @param [in] pAnaliticKind1 Вид аналитики 1
  @param [in] pAnalitic1 Значение аналитики 1
  @param [in] pAnaliticKind2 Вид аналитики 2
  @param [in] pAnalitic2 Значение аналитики 2
  @param [in] pAnaliticKind3 Вид аналитики 3
  @param [in] pAnalitic3 Значение аналитики 3
  @param [in] pAnaliticKind4 Вид аналитики 4
  @param [in] pAnalitic4 Значение аналитики 4
  @param [in] pAnaliticKind5 Вид аналитики 5
  @param [in] pAnalitic5 Значение аналитики 5
  @param [in] pAnaliticKind6 Вид аналитики 6
  @param [in] pAnalitic6 Значение аналитики 6
  @param [in] pExistEarlyNPTXOPTech Существуют операции расчета НОБ с категорией "Признак технического расчета" = "Технический"
  @param [in] pTechnical Значение категории "Признак технического расчета" для текущей операции
  @param [in] pRecalc Значение признака "Пересчет" из панели текущей операции
  @param [in] pTransfKind Значение признака трансформации
  @return NUMBER
  */
  FUNCTION GetSumNPTXOBJTech(pDocID NUMBER,
                             pOperDate DATE,
                             pCLientID NUMBER,
                             pKind NUMBER,
                             pAnaliticKind1 NUMBER,
                             pAnalitic1 NUMBER,
                             pAnaliticKind2 NUMBER,
                             pAnalitic2 NUMBER,
                             pAnaliticKind3 NUMBER,
                             pAnalitic3 NUMBER,
                             pAnaliticKind4 NUMBER,
                             pAnalitic4 NUMBER,
                             pAnaliticKind5 NUMBER,
                             pAnalitic5 NUMBER,
                             pAnaliticKind6 NUMBER,
                             pAnalitic6 NUMBER,
                             pExistEarlyNPTXOPTech NUMBER,
                             pRecalc CHAR,
                             pOutSystCode VARCHAR2 DEFAULT CHR(1),
                             pTransfKind NUMBER DEFAULT 0,
                             pHolding_Period IN NUMBER DEFAULT 0
                            ) RETURN NUMBER;
  
  PROCEDURE CreateTaxObjects1_1(pBegDate IN DATE,
                                pEndDate IN DATE,
                                pClient  IN NUMBER,
                                pIIS     IN NUMBER,
                                pDocID   IN NUMBER,
                                pStep    IN NUMBER,
                                pFIID    IN NUMBER,
                                pContract IN NUMBER,
                                pStat     OUT NUMBER );

  PROCEDURE CreateTaxObjects1_2(pBegDate   IN DATE,
                                pEndDate   IN DATE,
                                pClient    IN NUMBER,
                                pIIS       IN NUMBER,
                                pDocID     IN NUMBER,
                                pStep      IN NUMBER,
                                pFIID      IN NUMBER,
                                pContract  IN NUMBER,
                                pStat     OUT NUMBER);

  PROCEDURE CreateTaxObjects1_3(pBegDate   IN DATE,
                                pEndDate   IN DATE,
                                pClient    IN NUMBER,
                                pIIS       IN NUMBER,
                                pDocID     IN NUMBER,
                                pStep      IN NUMBER,
                                pFIID      IN NUMBER,
                                pContract  IN NUMBER,
                                pStat     OUT NUMBER);

  PROCEDURE CreateTaxObjects1_4(pBegDate   IN DATE,
                                pEndDate   IN DATE,
                                pClient    IN NUMBER,
                                pIIS       IN NUMBER,
                                pDocID     IN NUMBER,
                                pStep      IN NUMBER,
                                pFIID      IN NUMBER,
                                pContract  IN NUMBER,
                                pStat     OUT NUMBER);
  
  PROCEDURE CreateTaxObjects1_5(pBegDate   IN DATE,
                                pEndDate   IN DATE,
                                pClient    IN NUMBER,
                                pIIS       IN NUMBER,
                                pDocID     IN NUMBER,
                                pStep      IN NUMBER,
                                pFIID      IN NUMBER,
                                pContract  IN NUMBER,
                                pStat     OUT NUMBER);

 
  PROCEDURE CreateTaxObjects1_6(pBegDate   IN DATE,
                                pEndDate   IN DATE,
                                pClient    IN NUMBER,
                                pIIS       IN NUMBER,
                                pDocID     IN NUMBER,
                                pStep      IN NUMBER,
                                pFIID      IN NUMBER,
                                pContract  IN NUMBER,
                                pStat     OUT NUMBER);


  PROCEDURE CreateTaxObjects1_7(pBegDate   IN DATE,
                                pEndDate   IN DATE,
                                pClient    IN NUMBER,
                                pIIS       IN NUMBER,
                                pDocID     IN NUMBER,
                                pStep      IN NUMBER,
                                pFIID      IN NUMBER,
                                pContract  IN NUMBER,
                                pStat     OUT NUMBER);

  PROCEDURE CreateTaxObjects1_8(pBegDate   IN DATE,
                                pEndDate   IN DATE,
                                pClient    IN NUMBER,
                                pIIS       IN NUMBER,
                                pDocID     IN NUMBER,
                                pStep      IN NUMBER,
                                pFIID      IN NUMBER,
                                pContract  IN NUMBER,
                                pStat     OUT NUMBER);

  PROCEDURE CreateTaxObjects1_9(pBegDate   IN DATE,
                                pEndDate   IN DATE,
                                pClient    IN NUMBER,
                                pIIS       IN NUMBER,
                                pDocID     IN NUMBER,
                                pStep      IN NUMBER,
                                pFIID      IN NUMBER,
                                pContract  IN NUMBER,
                                pStat     OUT NUMBER);

  PROCEDURE CreateTaxObjects1_10(pBegDate   IN DATE,
                                 pEndDate   IN DATE,
                                 pClient    IN NUMBER,
                                 pIIS       IN NUMBER,
                                 pDocID     IN NUMBER,
                                 pStep      IN NUMBER,
                                 pFIID      IN NUMBER,
                                 pContract  IN NUMBER,
                                 pStat     OUT NUMBER);

  PROCEDURE CreateTaxObjects1_11(pBegDate   IN DATE,
                                 pEndDate   IN DATE,
                                 pClient    IN NUMBER,
                                 pIIS       IN NUMBER,
                                 pDocID     IN NUMBER,
                                 pStep      IN NUMBER,
                                 pFIID      IN NUMBER,
                                 pContract  IN NUMBER,
                                 pStat     OUT NUMBER);

  PROCEDURE CreateTaxObjects1_12(pBegDate   IN DATE,
                                 pEndDate   IN DATE,
                                 pClient    IN NUMBER,
                                 pIIS       IN NUMBER,
                                 pDocID     IN NUMBER,
                                 pStep      IN NUMBER,
                                 pFIID      IN NUMBER,
                                 pContract  IN NUMBER,
                                 pStat     OUT NUMBER);

  PROCEDURE CreateTaxObjects1_13(pBegDate   IN DATE,
                                 pEndDate   IN DATE,
                                 pClient    IN NUMBER,
                                 pIIS       IN NUMBER,
                                 pDocID     IN NUMBER,
                                 pStep      IN NUMBER,
                                 pFIID      IN NUMBER,
                                 pContract  IN NUMBER,
                                 pStat     OUT NUMBER);
  
  PROCEDURE CreateTaxObjects1_14(pBegDate   IN DATE,
                                 pEndDate   IN DATE,
                                 pClient    IN NUMBER,
                                 pIIS       IN NUMBER,
                                 pDocID     IN NUMBER,
                                 pStep      IN NUMBER,
                                 pFIID      IN NUMBER,
                                 pContract  IN NUMBER,
                                 pStat     OUT NUMBER);

  PROCEDURE CreateTaxObjects1_15(pBegDate   IN DATE,
                                 pEndDate   IN DATE,
                                 pClient    IN NUMBER,
                                 pIIS       IN NUMBER,
                                 pDocID     IN NUMBER,
                                 pStep      IN NUMBER,
                                 pFIID      IN NUMBER,
                                 pContract  IN NUMBER,
                                 pStat     OUT NUMBER);

  PROCEDURE CreateTaxObjects1_16(pBegDate   IN DATE,
                                 pEndDate   IN DATE,
                                 pClient    IN NUMBER,
                                 pIIS       IN NUMBER,
                                 pDocID     IN NUMBER,
                                 pStep      IN NUMBER,
                                 pFIID      IN NUMBER,
                                 pContract  IN NUMBER,
                                 pStat     OUT NUMBER);

  PROCEDURE CreateTaxObjects1_17(pBegDate   IN DATE,
                                 pEndDate   IN DATE,
                                 pClient    IN NUMBER,
                                 pIIS       IN NUMBER,
                                 pDocID     IN NUMBER,
                                 pStep      IN NUMBER,
                                 pFIID      IN NUMBER,
                                 pContract  IN NUMBER,
                                 pStat     OUT NUMBER);

  PROCEDURE CreateTaxObjects1_18(pBegDate   IN DATE,
                                 pEndDate   IN DATE,
                                 pClient    IN NUMBER,
                                 pIIS       IN NUMBER,
                                 pDocID     IN NUMBER,
                                 pStep      IN NUMBER,
                                 pFIID      IN NUMBER,
                                 pContract  IN NUMBER,
                                 pStat     OUT NUMBER);

  PROCEDURE CreateTaxObjects1_19(pBegDate   IN DATE,
                                 pEndDate   IN DATE,
                                 pClient    IN NUMBER,
                                 pIIS       IN NUMBER,
                                 pDocID     IN NUMBER,
                                 pStep      IN NUMBER,
                                 pFIID      IN NUMBER,
                                 pContract  IN NUMBER,
                                 pStat     OUT NUMBER);

  PROCEDURE CreateTaxObjects1_20(pBegDate   IN DATE,
                                 pEndDate   IN DATE,
                                 pClient    IN NUMBER,
                                 pIIS       IN NUMBER,
                                 pDocID     IN NUMBER,
                                 pStep      IN NUMBER,
                                 pFIID      IN NUMBER,
                                 pContract  IN NUMBER,
                                 pStat     OUT NUMBER);

  PROCEDURE CreateTaxObjects1_21(pBegDate   IN DATE,
                                 pEndDate   IN DATE,
                                 pClient    IN NUMBER,
                                 pIIS       IN NUMBER,
                                 pDocID     IN NUMBER,
                                 pStep      IN NUMBER,
                                 pFIID      IN NUMBER,
                                 pContract  IN NUMBER,
                                 pStat     OUT NUMBER);

  PROCEDURE CreateTaxObjects1_22(pBegDate   IN DATE,
                                 pEndDate   IN DATE,
                                 pClient    IN NUMBER,
                                 pIIS       IN NUMBER,
                                 pDocID     IN NUMBER,
                                 pStep      IN NUMBER,
                                 pFIID      IN NUMBER,
                                 pContract  IN NUMBER,
                                 pStat     OUT NUMBER);

  PROCEDURE CreateMaterialTaxObjects1_1(pBegDate   IN DATE,
                                        pEndDate   IN DATE,
                                        pClient    IN NUMBER,
                                        pIIS       IN NUMBER,
                                        pDocID     IN NUMBER,
                                        pStep      IN NUMBER,
                                        pFIID      IN NUMBER,
                                        pContract  IN NUMBER,
                                        pStat     OUT NUMBER);

  PROCEDURE CreateMaterialTaxObjects1_2(pBegDate   IN DATE,
                                        pEndDate   IN DATE,
                                        pClient    IN NUMBER,
                                        pIIS       IN NUMBER,
                                        pDocID     IN NUMBER,
                                        pStep      IN NUMBER,
                                        pFIID      IN NUMBER,
                                        pContract  IN NUMBER,
                                        pStat     OUT NUMBER);

  PROCEDURE CreateMaterialTaxObjects1_3(pBegDate   IN DATE,
                                        pEndDate   IN DATE,
                                        pClient    IN NUMBER,
                                        pIIS       IN NUMBER,
                                        pDocID     IN NUMBER,
                                        pStep      IN NUMBER,
                                        pFIID      IN NUMBER,
                                        pContract  IN NUMBER,
                                        pStat     OUT NUMBER);

  PROCEDURE CreateTaxObjects2_0 (pBegDate   IN DATE,
                                 pEndDate   IN DATE,
                                 pClient    IN NUMBER,
                                 pIIS       IN NUMBER,
                                 pDocID     IN NUMBER,
                                 pStep      IN NUMBER,
                                 pFIID      IN NUMBER,
                                 pContract  IN NUMBER,
                                 pStat     OUT NUMBER);
  
  PROCEDURE CreateTaxObjects2_1(pBegDate   IN DATE,
                                pEndDate   IN DATE,
                                pClient    IN NUMBER,
                                pIIS       IN NUMBER,
                                pDocID     IN NUMBER,
                                pStep      IN NUMBER,
                                pFIID      IN NUMBER,
                                pContract  IN NUMBER,
                                pStat     OUT NUMBER);

  PROCEDURE CreateTaxObjects2_2(pBegDate   IN DATE,
                                pEndDate   IN DATE,
                                pClient    IN NUMBER,
                                pIIS       IN NUMBER,
                                pDocID     IN NUMBER,
                                pStep      IN NUMBER,
                                pFIID      IN NUMBER,
                                pContract  IN NUMBER,
                                pStat     OUT NUMBER);

  PROCEDURE CreateTaxObjects2_3(pBegDate   IN DATE,
                                pEndDate   IN DATE,
                                pClient    IN NUMBER,
                                pIIS       IN NUMBER,
                                pDocID     IN NUMBER,
                                pStep      IN NUMBER,
                                pFIID      IN NUMBER,
                                pContract  IN NUMBER,
                                pStat     OUT NUMBER);

  PROCEDURE CreateTaxObjects2_4(pBegDate   IN DATE,
                                pEndDate   IN DATE,
                                pClient    IN NUMBER,
                                pIIS       IN NUMBER,
                                pDocID     IN NUMBER,
                                pStep      IN NUMBER,
                                pFIID      IN NUMBER,
                                pContract  IN NUMBER,
                                pStat     OUT NUMBER);

  PROCEDURE CreateTaxObjects2_5(pBegDate   IN DATE,
                                pEndDate   IN DATE,
                                pClient    IN NUMBER,
                                pIIS       IN NUMBER,
                                pDocID     IN NUMBER,
                                pStep      IN NUMBER,
                                pFIID      IN NUMBER,
                                pContract  IN NUMBER,
                                pStat     OUT NUMBER);

  PROCEDURE CreateTaxObjects2_6(pBegDate   IN DATE,
                                pEndDate   IN DATE,
                                pClient    IN NUMBER,
                                pIIS       IN NUMBER,
                                pDocID     IN NUMBER,
                                pStep      IN NUMBER,
                                pFIID      IN NUMBER,
                                pContract  IN NUMBER,
                                pStat     OUT NUMBER);

  PROCEDURE CreateTaxObjects2_7(pBegDate   IN DATE,
                                pEndDate   IN DATE,
                                pClient    IN NUMBER,
                                pIIS       IN NUMBER,
                                pDocID     IN NUMBER,
                                pStep      IN NUMBER,
                                pFIID      IN NUMBER,
                                pContract  IN NUMBER,
                                pStat     OUT NUMBER);

  PROCEDURE CreateTaxObjects2_8(pBegDate   IN DATE,
                                pEndDate   IN DATE,
                                pClient    IN NUMBER,
                                pIIS       IN NUMBER,
                                pDocID     IN NUMBER,
                                pStep      IN NUMBER,
                                pFIID      IN NUMBER,
                                pContract  IN NUMBER,
                                pStat     OUT NUMBER);

  PROCEDURE CreateTaxObjects2_9(pBegDate   IN DATE,
                                pEndDate   IN DATE,
                                pClient    IN NUMBER,
                                pIIS       IN NUMBER,
                                pDocID     IN NUMBER,
                                pStep      IN NUMBER,
                                pFIID      IN NUMBER,
                                pContract  IN NUMBER,
                                pStat     OUT NUMBER);

  PROCEDURE CreateTaxObjects2_10(pBegDate   IN DATE,
                                 pEndDate   IN DATE,
                                 pClient    IN NUMBER,
                                 pIIS       IN NUMBER,
                                 pDocID     IN NUMBER,
                                 pStep      IN NUMBER,
                                 pFIID      IN NUMBER,
                                 pContract  IN NUMBER,
                                 pStat     OUT NUMBER);

  PROCEDURE CreateTaxObjects2_11(pBegDate   IN DATE,
                                 pEndDate   IN DATE,
                                 pClient    IN NUMBER,
                                 pIIS       IN NUMBER,
                                 pDocID     IN NUMBER,
                                 pStep      IN NUMBER,
                                 pFIID      IN NUMBER,
                                 pContract  IN NUMBER,
                                 pStat     OUT NUMBER);
  
  PROCEDURE CreateTaxObjects2_12(pBegDate   IN DATE,
                                 pEndDate   IN DATE,
                                 pClient    IN NUMBER,
                                 pIIS       IN NUMBER,
                                 pDocID     IN NUMBER,
                                 pStep      IN NUMBER,
                                 pFIID      IN NUMBER,
                                 pContract  IN NUMBER,
                                 pStat     OUT NUMBER);

  PROCEDURE CreateTaxObjects2_13(pBegDate   IN DATE,
                                 pEndDate   IN DATE,
                                 pClient    IN NUMBER,
                                 pIIS       IN NUMBER,
                                 pDocID     IN NUMBER,
                                 pStep      IN NUMBER,
                                 pFIID      IN NUMBER,
                                 pContract  IN NUMBER,
                                 pStat     OUT NUMBER);

  PROCEDURE CreateTaxObjects2_14(pBegDate   IN DATE,
                                 pEndDate   IN DATE,
                                 pClient    IN NUMBER,
                                 pIIS       IN NUMBER,
                                 pDocID     IN NUMBER,
                                 pStep      IN NUMBER,
                                 pFIID      IN NUMBER,
                                 pContract  IN NUMBER,
                                 pStat     OUT NUMBER);

  PROCEDURE CreateTaxObjects2_15(pBegDate   IN DATE,
                                 pEndDate   IN DATE,
                                 pClient    IN NUMBER,
                                 pIIS       IN NUMBER,
                                 pDocID     IN NUMBER,
                                 pStep      IN NUMBER,
                                 pFIID      IN NUMBER,
                                 pContract  IN NUMBER,
                                 pStat     OUT NUMBER);

  PROCEDURE CreateTaxObjects2_16(pBegDate   IN DATE,
                                 pEndDate   IN DATE,
                                 pClient    IN NUMBER,
                                 pIIS       IN NUMBER,
                                 pDocID     IN NUMBER,
                                 pStep      IN NUMBER,
                                 pFIID      IN NUMBER,
                                 pContract  IN NUMBER,
                                 pStat     OUT NUMBER);

  PROCEDURE CreateTaxObjects2_17(pBegDate   IN DATE,
                                 pEndDate   IN DATE,
                                 pClient    IN NUMBER,
                                 pIIS       IN NUMBER,
                                 pDocID     IN NUMBER,
                                 pStep      IN NUMBER,
                                 pFIID      IN NUMBER,
                                 pContract  IN NUMBER,
                                 pStat     OUT NUMBER);

  PROCEDURE CreateTaxObjects2_18(pBegDate   IN DATE,
                                 pEndDate   IN DATE,
                                 pClient    IN NUMBER,
                                 pIIS       IN NUMBER,
                                 pDocID     IN NUMBER,
                                 pStep      IN NUMBER,
                                 pFIID      IN NUMBER,
                                 pContract  IN NUMBER,
                                 pStat     OUT NUMBER);

  PROCEDURE CreateTaxObjects2_19(pBegDate   IN DATE,
                                 pEndDate   IN DATE,
                                 pClient    IN NUMBER,
                                 pIIS       IN NUMBER,
                                 pDocID     IN NUMBER,
                                 pStep      IN NUMBER,
                                 pFIID      IN NUMBER,
                                 pContract  IN NUMBER,
                                 pStat     OUT NUMBER);

  PROCEDURE CreateTaxObjects2_20(pBegDate   IN DATE,
                                 pEndDate   IN DATE,
                                 pClient    IN NUMBER,
                                 pIIS       IN NUMBER,
                                 pDocID     IN NUMBER,
                                 pStep      IN NUMBER,
                                 pFIID      IN NUMBER,
                                 pContract  IN NUMBER,
                                 pStat     OUT NUMBER);

  PROCEDURE CreateTaxObjects2_21(pBegDate   IN DATE,
                                 pEndDate   IN DATE,
                                 pClient    IN NUMBER,
                                 pIIS       IN NUMBER,
                                 pDocID     IN NUMBER,
                                 pStep      IN NUMBER,
                                 pFIID      IN NUMBER,
                                 pContract  IN NUMBER,
                                 pStat     OUT NUMBER);

  PROCEDURE CreateTaxObjects2_22(pBegDate   IN DATE,
                                 pEndDate   IN DATE,
                                 pClient    IN NUMBER,
                                 pIIS       IN NUMBER,
                                 pDocID     IN NUMBER,
                                 pStep      IN NUMBER,
                                 pFIID      IN NUMBER,
                                 pContract  IN NUMBER,
                                 pStat     OUT NUMBER);

  PROCEDURE CreateTaxObjects2_23(pBegDate   IN DATE,
                                 pEndDate   IN DATE,
                                 pClient    IN NUMBER,
                                 pIIS       IN NUMBER,
                                 pDocID     IN NUMBER,
                                 pStep      IN NUMBER,
                                 pFIID      IN NUMBER,
                                 pContract  IN NUMBER,
                                 pStat     OUT NUMBER);

  PROCEDURE CreateTaxObjects2_24(pBegDate   IN DATE,
                                 pEndDate   IN DATE,
                                 pClient    IN NUMBER,
                                 pIIS       IN NUMBER,
                                 pDocID     IN NUMBER,
                                 pStep      IN NUMBER,
                                 pFIID      IN NUMBER,
                                 pContract  IN NUMBER,
                                 pStat     OUT NUMBER);

  PROCEDURE CreateTaxObjects2_25(pBegDate   IN DATE,
                                 pEndDate   IN DATE,
                                 pClient    IN NUMBER,
                                 pIIS       IN NUMBER,
                                 pDocID     IN NUMBER,
                                 pStep      IN NUMBER,
                                 pFIID      IN NUMBER,
                                 pContract  IN NUMBER,
                                 pStat     OUT NUMBER);

  PROCEDURE CreateTaxObjects2_26(pBegDate   IN DATE,
                                 pEndDate   IN DATE,
                                 pClient    IN NUMBER,
                                 pIIS       IN NUMBER,
                                 pDocID     IN NUMBER,
                                 pStep      IN NUMBER,
                                 pFIID      IN NUMBER,
                                 pContract  IN NUMBER,
                                 pStat     OUT NUMBER);

  PROCEDURE CreateTaxObjects2_27(pBegDate   IN DATE,
                                 pEndDate   IN DATE,
                                 pClient    IN NUMBER,
                                 pIIS       IN NUMBER,
                                 pDocID     IN NUMBER,
                                 pStep      IN NUMBER,
                                 pFIID      IN NUMBER,
                                 pContract  IN NUMBER,
                                 pStat     OUT NUMBER);

  PROCEDURE CreateTaxObjects2_28(pBegDate   IN DATE,
                                 pEndDate   IN DATE,
                                 pClient    IN NUMBER,
                                 pIIS       IN NUMBER,
                                 pDocID     IN NUMBER,
                                 pStep      IN NUMBER,
                                 pFIID      IN NUMBER,
                                 pContract  IN NUMBER,
                                 pStat     OUT NUMBER);

  /**
  @brief Расчет объектов НДР 3 уровня
  @param [in] pBegDate Дата начала расчета
  @param [in] pEndDate Дата окончания расчета
  @param [in] pClient ID клиента
  @param [in] pIIS Признак "Договор ИИС"
  @param [in] pDocID ID текущей операции
  @param [in] pStep  ID шага операции
  @param [in] pFIID  ID ценной бумаги
  @param [in] pTechnical Значение категории "Признак технического расчета" на текущей операции
  @param [in] pRecalc Признак технического расчета из панели операции
  @param [in] pExistTechCalc Существование объектов НДР с признаком "Технический", созданных ранее текущей операции
  @param [in] pContract ID Договора
  @param [in] pTransfDate Дата трансформации ИИС в ИИС-3
  @param [in] pTransfKind Период расчета
  @param [out] pStat Статус расчета
  */
  PROCEDURE CreateTaxObjects3_1(pBegDate   IN DATE,
                                pEndDate   IN DATE,
                                pClient    IN NUMBER,
                                pIIS       IN NUMBER,
                                pDocID     IN NUMBER,
                                pStep      IN NUMBER,
                                pFIID      IN NUMBER,
                                pTechnical IN NUMBER,
                                pRecalc    IN CHAR,
                                pExistTechCalc IN NUMBER,
                                pContract   IN NUMBER,
                                pTransfDate IN DATE,
                                pTransfKind IN NUMBER,
                                pDateNDR    IN DATE,
                                pStat     OUT NUMBER);

  /**
  @brief Расчет объектов НДР 3 уровня
  @param [in] pBegDate Дата начала расчета
  @param [in] pEndDate Дата окончания расчета
  @param [in] pClient ID клиента
  @param [in] pIIS Признак "Договор ИИС"
  @param [in] pDocID ID текущей операции
  @param [in] pStep  ID шага операции
  @param [in] pFIID  ID ценной бумаги
  @param [in] pTechnical Значение категории "Признак технического расчета" на текущей операции
  @param [in] pRecalc Признак технического расчета из панели операции
  @param [in] pExistTechCalc Существование объектов НДР с признаком "Технический", созданных ранее текущей операции
  @param [in] pContract ID Договора
  @param [in] pTransfDate Дата трансформации ИИС в ИИС-3
  @param [in] pTransfKind Период расчета
  @param [out] pStat Статус расчета
  */
  PROCEDURE CreateTaxObjects3_2(pBegDate   IN DATE,
                                pEndDate   IN DATE,
                                pClient    IN NUMBER,
                                pIIS       IN NUMBER,
                                pDocID     IN NUMBER,
                                pStep      IN NUMBER,
                                pFIID      IN NUMBER,
                                pTechnical IN NUMBER,
                                pRecalc    IN CHAR,
                                pExistTechCalc IN NUMBER,
                                pContract   IN NUMBER,
                                pTransfDate IN DATE,
                                pTransfKind IN NUMBER,
                                pDateNDR    IN DATE,
                                pStat     OUT NUMBER);

  /**
  @brief Расчет объектов НДР 3 уровня
  @param [in] pBegDate Дата начала расчета
  @param [in] pEndDate Дата окончания расчета
  @param [in] pClient ID клиента
  @param [in] pIIS Признак "Договор ИИС"
  @param [in] pDocID ID текущей операции
  @param [in] pStep  ID шага операции
  @param [in] pFIID  ID ценной бумаги
  @param [in] pTechnical Значение категории "Признак технического расчета" на текущей операции
  @param [in] pRecalc Признак технического расчета из панели операции
  @param [in] pExistTechCalc Существование объектов НДР с признаком "Технический", созданных ранее текущей операции
  @param [in] pContract ID Договора
  @param [in] pTransfDate Дата трансформации ИИС в ИИС-3
  @param [in] pTransfKind Период расчета
  @param [out] pStat Статус расчета
  */
  PROCEDURE CreateTaxObjects3_3(pBegDate   IN DATE,
                                pEndDate   IN DATE,
                                pClient    IN NUMBER,
                                pIIS       IN NUMBER,
                                pDocID     IN NUMBER,
                                pStep      IN NUMBER,
                                pFIID      IN NUMBER,
                                pTechnical IN NUMBER,
                                pRecalc    IN CHAR,
                                pExistTechCalc IN NUMBER,
                                pContract   IN NUMBER,
                                pTransfDate IN DATE,
                                pTransfKind IN NUMBER,
                                pDateNDR    IN DATE,
                                pStat     OUT NUMBER);

  /**
  @brief Расчет объектов НДР 3 уровня
  @param [in] pBegDate Дата начала расчета
  @param [in] pEndDate Дата окончания расчета
  @param [in] pClient ID клиента
  @param [in] pIIS Признак "Договор ИИС"
  @param [in] pDocID ID текущей операции
  @param [in] pStep  ID шага операции
  @param [in] pFIID  ID ценной бумаги
  @param [in] pTechnical Значение категории "Признак технического расчета" на текущей операции
  @param [in] pRecalc Признак технического расчета из панели операции
  @param [in] pExistTechCalc Существование объектов НДР с признаком "Технический", созданных ранее текущей операции
  @param [in] pContract ID Договора
  @param [in] pTransfDate Дата трансформации ИИС в ИИС-3
  @param [in] pTransfKind Период расчета
  @param [out] pStat Статус расчета
  */
  PROCEDURE CreateTaxObjects3_4(pBegDate   IN DATE,
                                pEndDate   IN DATE,
                                pClient    IN NUMBER,
                                pIIS       IN NUMBER,
                                pDocID     IN NUMBER,
                                pStep      IN NUMBER,
                                pFIID      IN NUMBER,
                                pTechnical IN NUMBER,
                                pRecalc    IN CHAR,
                                pExistTechCalc IN NUMBER,
                                pContract   IN NUMBER,
                                pTransfDate IN DATE,
                                pTransfKind IN NUMBER,
                                pDateNDR    IN DATE,
                                pStat     OUT NUMBER);

  /**
  @brief Расчет объектов НДР 3 уровня
  @param [in] pBegDate Дата начала расчета
  @param [in] pEndDate Дата окончания расчета
  @param [in] pClient ID клиента
  @param [in] pIIS Признак "Договор ИИС"
  @param [in] pDocID ID текущей операции
  @param [in] pStep  ID шага операции
  @param [in] pFIID  ID ценной бумаги
  @param [in] pTechnical Значение категории "Признак технического расчета" на текущей операции
  @param [in] pRecalc Признак технического расчета из панели операции
  @param [in] pExistTechCalc Существование объектов НДР с признаком "Технический", созданных ранее текущей операции
  @param [in] pContract ID Договора
  @param [in] pTransfDate Дата трансформации ИИС в ИИС-3
  @param [in] pTransfKind Период расчета
  @param [out] pStat Статус расчета
  */
  PROCEDURE CreateTaxObjects3_5(pBegDate   IN DATE,
                                pEndDate   IN DATE,
                                pClient    IN NUMBER,
                                pIIS       IN NUMBER,
                                pDocID     IN NUMBER,
                                pStep      IN NUMBER,
                                pFIID      IN NUMBER,
                                pTechnical IN NUMBER,
                                pRecalc    IN CHAR,
                                pExistTechCalc IN NUMBER,
                                pContract   IN NUMBER,
                                pTransfDate IN DATE,
                                pTransfKind IN NUMBER,
                                pDateNDR    IN DATE,
                                pStat     OUT NUMBER);

  /**
  @brief Расчет объектов НДР 3 уровня
  @param [in] pBegDate Дата начала расчета
  @param [in] pEndDate Дата окончания расчета
  @param [in] pClient ID клиента
  @param [in] pIIS Признак "Договор ИИС"
  @param [in] pDocID ID текущей операции
  @param [in] pStep  ID шага операции
  @param [in] pFIID  ID ценной бумаги
  @param [in] pTechnical Значение категории "Признак технического расчета" на текущей операции
  @param [in] pRecalc Признак технического расчета из панели операции
  @param [in] pExistTechCalc Существование объектов НДР с признаком "Технический", созданных ранее текущей операции
  @param [in] pContract ID Договора
  @param [in] pTransfDate Дата трансформации ИИС в ИИС-3
  @param [in] pTransfKind Период расчета
  @param [out] pStat Статус расчета
  */
  PROCEDURE CreateTaxObjects3_6(pBegDate   IN DATE,
                                pEndDate   IN DATE,
                                pClient    IN NUMBER,
                                pIIS       IN NUMBER,
                                pDocID     IN NUMBER,
                                pStep      IN NUMBER,
                                pFIID      IN NUMBER,
                                pTechnical IN NUMBER,
                                pRecalc    IN CHAR,
                                pExistTechCalc IN NUMBER,
                                pContract   IN NUMBER,
                                pTransfDate IN DATE,
                                pTransfKind IN NUMBER,
                                pDateNDR    IN DATE,
                                pStat     OUT NUMBER);

  /**
  @brief Расчет объектов НДР 3 уровня
  @param [in] pBegDate Дата начала расчета
  @param [in] pEndDate Дата окончания расчета
  @param [in] pClient ID клиента
  @param [in] pIIS Признак "Договор ИИС"
  @param [in] pDocID ID текущей операции
  @param [in] pStep  ID шага операции
  @param [in] pFIID  ID ценной бумаги
  @param [in] pTechnical Значение категории "Признак технического расчета" на текущей операции
  @param [in] pRecalc Признак технического расчета из панели операции
  @param [in] pExistTechCalc Существование объектов НДР с признаком "Технический", созданных ранее текущей операции
  @param [in] pContract ID Договора
  @param [in] pTransfDate Дата трансформации ИИС в ИИС-3
  @param [in] pTransfKind Период расчета
  @param [out] pStat Статус расчета
  */
  PROCEDURE CreateTaxObjects3_7(pBegDate   IN DATE,
                                pEndDate   IN DATE,
                                pClient    IN NUMBER,
                                pIIS       IN NUMBER,
                                pDocID     IN NUMBER,
                                pStep      IN NUMBER,
                                pFIID      IN NUMBER,
                                pTechnical IN NUMBER,
                                pRecalc    IN CHAR,
                                pExistTechCalc IN NUMBER,
                                pContract   IN NUMBER,
                                pTransfDate IN DATE,
                                pTransfKind IN NUMBER,
                                pDateNDR    IN DATE,
                                pStat     OUT NUMBER);

  /**
  @brief Расчет объектов НДР 3 уровня
  @param [in] pBegDate Дата начала расчета
  @param [in] pEndDate Дата окончания расчета
  @param [in] pClient ID клиента
  @param [in] pIIS Признак "Договор ИИС"
  @param [in] pDocID ID текущей операции
  @param [in] pStep  ID шага операции
  @param [in] pFIID  ID ценной бумаги
  @param [in] pTechnical Значение категории "Признак технического расчета" на текущей операции
  @param [in] pRecalc Признак технического расчета из панели операции
  @param [in] pExistTechCalc Существование объектов НДР с признаком "Технический", созданных ранее текущей операции
  @param [in] pContract ID Договора
  @param [in] pTransfDate Дата трансформации ИИС в ИИС-3
  @param [in] pTransfKind Период расчета
  @param [out] pStat Статус расчета
  */
  PROCEDURE CreateTaxObjects3_8(pBegDate   IN DATE,
                                pEndDate   IN DATE,
                                pClient    IN NUMBER,
                                pIIS       IN NUMBER,
                                pDocID     IN NUMBER,
                                pStep      IN NUMBER,
                                pFIID      IN NUMBER,
                                pTechnical IN NUMBER,
                                pRecalc    IN CHAR,
                                pExistTechCalc IN NUMBER,
                                pContract   IN NUMBER,
                                pTransfDate IN DATE,
                                pTransfKind IN NUMBER,
                                pDateNDR    IN DATE,
                                pStat     OUT NUMBER);

  /**
  @brief Расчет объектов НДР 4 уровня
  @param [in] pBegDate Дата начала расчета
  @param [in] pEndDate Дата окончания расчета
  @param [in] pClient ID клиента
  @param [in] pIIS Признак "Договор ИИС"
  @param [in] pDocID ID текущей операции
  @param [in] pStep  ID шага операции
  @param [in] pFIID  ID ценной бумаги
  @param [in] pTechnical Значение категории "Признак технического расчета" на текущей операции
  @param [in] pRecalc Признак технического расчета из панели операции
  @param [in] pExistTechCalc Существование объектов НДР с признаком "Технический", созданных ранее текущей операции
  @param [in] pTransfDate Дата трансформации ИИС в ИИС-3
  @param [in] pTransfKind Период расчета
  @param [out] pStat Статус расчета
  */
  PROCEDURE CreateTaxObjects4_1(pBegDate   IN DATE,
                                pEndDate   IN DATE,
                                pClient    IN NUMBER,
                                pContract  IN NUMBER,
                                pIIS       IN NUMBER,
                                pDocID     IN NUMBER,
                                pStep      IN NUMBER,
                                pFIID      IN NUMBER,
                                pTechnical IN NUMBER,
                                pRecalc    IN CHAR,
                                pExistTechCalc IN NUMBER,
                                pTransfDate IN DATE,
                                pTransfKind IN NUMBER,
                                pDateNDR    IN DATE,
                                pStat     OUT NUMBER);

  /**
  @brief Расчет объектов НДР 4 уровня
  @param [in] pBegDate Дата начала расчета
  @param [in] pEndDate Дата окончания расчета
  @param [in] pClient ID клиента
  @param [in] pIIS Признак "Договор ИИС"
  @param [in] pDocID ID текущей операции
  @param [in] pStep  ID шага операции
  @param [in] pFIID  ID ценной бумаги
  @param [in] pTechnical Значение категории "Признак технического расчета" на текущей операции
  @param [in] pRecalc Признак технического расчета из панели операции
  @param [in] pExistTechCalc Существование объектов НДР с признаком "Технический", созданных ранее текущей операции
  @param [in] pTransfDate Дата трансформации ИИС в ИИС-3
  @param [in] pTransfKind Период расчета
  @param [out] pStat Статус расчета
  */
  PROCEDURE CreateTaxObjects4_2(pBegDate   IN DATE,
                                pEndDate   IN DATE,
                                pClient    IN NUMBER,
                                pContract  IN NUMBER,
                                pIIS       IN NUMBER,
                                pDocID     IN NUMBER,
                                pStep      IN NUMBER,
                                pFIID      IN NUMBER,
                                pTechnical IN NUMBER,
                                pRecalc    IN CHAR,
                                pExistTechCalc IN NUMBER,
                                pTransfDate IN DATE,
                                pTransfKind IN NUMBER,
                                pDateNDR    IN DATE,
                                pStat     OUT NUMBER);

  /**
  @brief Расчет объектов НДР 4 уровня
  @param [in] pBegDate Дата начала расчета
  @param [in] pEndDate Дата окончания расчета
  @param [in] pClient ID клиента
  @param [in] pIIS Признак "Договор ИИС"
  @param [in] pDocID ID текущей операции
  @param [in] pStep  ID шага операции
  @param [in] pFIID  ID ценной бумаги
  @param [in] pTechnical Значение категории "Признак технического расчета" на текущей операции
  @param [in] pRecalc Признак технического расчета из панели операции
  @param [in] pExistTechCalc Существование объектов НДР с признаком "Технический", созданных ранее текущей операции
  @param [in] pTransfDate Дата трансформации ИИС в ИИС-3
  @param [in] pTransfKind Период расчета
  @param [out] pStat Статус расчета
  */
  PROCEDURE CreateTaxObjects4_3(pBegDate   IN DATE,
                                pEndDate   IN DATE,
                                pClient    IN NUMBER,
                                pContract  IN NUMBER,
                                pIIS       IN NUMBER,
                                pDocID     IN NUMBER,
                                pStep      IN NUMBER,
                                pFIID      IN NUMBER,
                                pTechnical IN NUMBER,
                                pRecalc    IN CHAR,
                                pExistTechCalc IN NUMBER,
                                pTransfDate IN DATE,
                                pTransfKind IN NUMBER,
                                pDateNDR    IN DATE,
                                pStat     OUT NUMBER);

  /**
  @brief Расчет объектов НДР 4 уровня
  @param [in] pBegDate Дата начала расчета
  @param [in] pEndDate Дата окончания расчета
  @param [in] pClient ID клиента
  @param [in] pIIS Признак "Договор ИИС"
  @param [in] pDocID ID текущей операции
  @param [in] pStep  ID шага операции
  @param [in] pFIID  ID ценной бумаги
  @param [in] pTechnical Значение категории "Признак технического расчета" на текущей операции
  @param [in] pRecalc Признак технического расчета из панели операции
  @param [in] pExistTechCalc Существование объектов НДР с признаком "Технический", созданных ранее текущей операции
  @param [in] pTransfDate Дата трансформации ИИС в ИИС-3
  @param [in] pTransfKind Период расчета
  @param [out] pStat Статус расчета
  */
  PROCEDURE CreateTaxObjects4_4(pBegDate   IN DATE,
                                pEndDate   IN DATE,
                                pClient    IN NUMBER,
                                pContract  IN NUMBER,
                                pIIS       IN NUMBER,
                                pDocID     IN NUMBER,
                                pStep      IN NUMBER,
                                pFIID      IN NUMBER,
                                pTechnical IN NUMBER,
                                pRecalc    IN CHAR,
                                pExistTechCalc IN NUMBER,
                                pTransfDate IN DATE,
                                pTransfKind IN NUMBER,
                                pDateNDR    IN DATE,
                                pStat     OUT NUMBER);

  /**
  @brief Расчет объектов НДР 4 уровня
  @param [in] pBegDate Дата начала расчета
  @param [in] pEndDate Дата окончания расчета
  @param [in] pClient ID клиента
  @param [in] pIIS Признак "Договор ИИС"
  @param [in] pDocID ID текущей операции
  @param [in] pStep  ID шага операции
  @param [in] pFIID  ID ценной бумаги
  @param [in] pTechnical Значение категории "Признак технического расчета" на текущей операции
  @param [in] pRecalc Признак технического расчета из панели операции
  @param [in] pExistTechCalc Существование объектов НДР с признаком "Технический", созданных ранее текущей операции
  @param [in] pTransfDate Дата трансформации ИИС в ИИС-3
  @param [in] pTransfKind Период расчета
  @param [out] pStat Статус расчета
  */
  PROCEDURE CreateTaxObjects4_5(pBegDate   IN DATE,
                                pEndDate   IN DATE,
                                pClient    IN NUMBER,
                                pContract  IN NUMBER,
                                pIIS       IN NUMBER,
                                pDocID     IN NUMBER,
                                pStep      IN NUMBER,
                                pFIID      IN NUMBER,
                                pTechnical IN NUMBER,
                                pRecalc    IN CHAR,
                                pExistTechCalc IN NUMBER,
                                pTransfDate IN DATE,
                                pTransfKind IN NUMBER,
                                pDateNDR    IN DATE,
                                pStat     OUT NUMBER);

  /**
  @brief Расчет объектов НДР 4 уровня
  @param [in] pBegDate Дата начала расчета
  @param [in] pEndDate Дата окончания расчета
  @param [in] pClient ID клиента
  @param [in] pIIS Признак "Договор ИИС"
  @param [in] pDocID ID текущей операции
  @param [in] pStep  ID шага операции
  @param [in] pFIID  ID ценной бумаги
  @param [in] pTechnical Значение категории "Признак технического расчета" на текущей операции
  @param [in] pRecalc Признак технического расчета из панели операции
  @param [in] pExistTechCalc Существование объектов НДР с признаком "Технический", созданных ранее текущей операции
  @param [in] pTransfDate Дата трансформации ИИС в ИИС-3
  @param [in] pTransfKind Период расчета
  @param [out] pStat Статус расчета
  */
  PROCEDURE CreateTaxObjects4_6(pBegDate   IN DATE,
                                pEndDate   IN DATE,
                                pClient    IN NUMBER,
                                pContract  IN NUMBER,
                                pIIS       IN NUMBER,
                                pDocID     IN NUMBER,
                                pStep      IN NUMBER,
                                pFIID      IN NUMBER,
                                pTechnical IN NUMBER,
                                pRecalc    IN CHAR,
                                pExistTechCalc IN NUMBER,
                                pTransfDate IN DATE,
                                pTransfKind IN NUMBER,
                                pDateNDR    IN DATE,
                                pStat     OUT NUMBER);

  /**
  @brief Расчет объектов НДР 4 уровня
  @param [in] pBegDate Дата начала расчета
  @param [in] pEndDate Дата окончания расчета
  @param [in] pClient ID клиента
  @param [in] pIIS Признак "Договор ИИС"
  @param [in] pDocID ID текущей операции
  @param [in] pStep  ID шага операции
  @param [in] pFIID  ID ценной бумаги
  @param [in] pTechnical Значение категории "Признак технического расчета" на текущей операции
  @param [in] pRecalc Признак технического расчета из панели операции
  @param [in] pExistTechCalc Существование объектов НДР с признаком "Технический", созданных ранее текущей операции
  @param [in] pTransfDate Дата трансформации ИИС в ИИС-3
  @param [in] pTransfKind Период расчета
  @param [out] pStat Статус расчета
  */
  PROCEDURE CreateTaxObjects4_7(pBegDate   IN DATE,
                                pEndDate   IN DATE,
                                pClient    IN NUMBER,
                                pContract  IN NUMBER,
                                pIIS       IN NUMBER,
                                pDocID     IN NUMBER,
                                pStep      IN NUMBER,
                                pFIID      IN NUMBER,
                                pTechnical IN NUMBER,
                                pRecalc    IN CHAR,
                                pExistTechCalc IN NUMBER,
                                pTransfDate IN DATE,
                                pTransfKind IN NUMBER,
                                pDateNDR    IN DATE,
                                pStat     OUT NUMBER);

  /**
  @brief Расчет объектов НДР 4 уровня
  @param [in] pBegDate Дата начала расчета
  @param [in] pEndDate Дата окончания расчета
  @param [in] pClient ID клиента
  @param [in] pIIS Признак "Договор ИИС"
  @param [in] pDocID ID текущей операции
  @param [in] pStep  ID шага операции
  @param [in] pFIID  ID ценной бумаги
  @param [in] pTechnical Значение категории "Признак технического расчета" на текущей операции
  @param [in] pRecalc Признак технического расчета из панели операции
  @param [in] pExistTechCalc Существование объектов НДР с признаком "Технический", созданных ранее текущей операции
  @param [in] pTransfDate Дата трансформации ИИС в ИИС-3
  @param [in] pTransfKind Период расчета
  @param [out] pStat Статус расчета
  */
  PROCEDURE CreateTaxObjects4_8(pBegDate   IN DATE,
                                pEndDate   IN DATE,
                                pClient    IN NUMBER,
                                pContract  IN NUMBER,
                                pIIS       IN NUMBER,
                                pDocID     IN NUMBER,
                                pStep      IN NUMBER,
                                pFIID      IN NUMBER,
                                pTechnical IN NUMBER,
                                pRecalc    IN CHAR,
                                pExistTechCalc IN NUMBER,
                                pTransfDate IN DATE,
                                pTransfKind IN NUMBER,
                                pDateNDR    IN DATE,
                                pStat     OUT NUMBER);

  /**
  @brief Расчет объектов НДР 4 уровня
  @param [in] pBegDate Дата начала расчета
  @param [in] pEndDate Дата окончания расчета
  @param [in] pClient ID клиента
  @param [in] pIIS Признак "Договор ИИС"
  @param [in] pDocID ID текущей операции
  @param [in] pStep  ID шага операции
  @param [in] pFIID  ID ценной бумаги
  @param [in] pTechnical Значение категории "Признак технического расчета" на текущей операции
  @param [in] pRecalc Признак технического расчета из панели операции
  @param [in] pExistTechCalc Существование объектов НДР с признаком "Технический", созданных ранее текущей операции
  @param [in] pTransfDate Дата трансформации ИИС в ИИС-3
  @param [in] pTransfKind Период расчета
  @param [out] pStat Статус расчета
  */
  PROCEDURE CreateTaxObjects4_9(pBegDate   IN DATE,
                                pEndDate   IN DATE,
                                pClient    IN NUMBER,
                                pContract  IN NUMBER,
                                pIIS       IN NUMBER,
                                pDocID     IN NUMBER,
                                pStep      IN NUMBER,
                                pFIID      IN NUMBER,
                                pTechnical IN NUMBER,
                                pRecalc    IN CHAR,
                                pExistTechCalc IN NUMBER,
                                pTransfDate IN DATE,
                                pTransfKind IN NUMBER,
                                pDateNDR    IN DATE,
                                pStat     OUT NUMBER);

  /**
  @brief Расчет объектов НДР 4 уровня
  @param [in] pBegDate Дата начала расчета
  @param [in] pEndDate Дата окончания расчета
  @param [in] pClient ID клиента
  @param [in] pIIS Признак "Договор ИИС"
  @param [in] pDocID ID текущей операции
  @param [in] pStep  ID шага операции
  @param [in] pFIID  ID ценной бумаги
  @param [in] pTechnical Значение категории "Признак технического расчета" на текущей операции
  @param [in] pRecalc Признак технического расчета из панели операции
  @param [in] pExistTechCalc Существование объектов НДР с признаком "Технический", созданных ранее текущей операции
  @param [in] pTransfDate Дата трансформации ИИС в ИИС-3
  @param [in] pTransfKind Период расчета
  @param [out] pStat Статус расчета
  */
  PROCEDURE CreateTaxObjects4_10(pBegDate   IN DATE,
                                 pEndDate   IN DATE,
                                 pClient    IN NUMBER,
                                 pContract  IN NUMBER,
                                 pIIS       IN NUMBER,
                                 pDocID     IN NUMBER,
                                 pStep      IN NUMBER,
                                 pFIID      IN NUMBER,
                                 pTechnical IN NUMBER,
                                 pRecalc    IN CHAR,
                                 pExistTechCalc IN NUMBER,
                                 pTransfDate IN DATE,
                                 pTransfKind IN NUMBER,
                                 pDateNDR    IN DATE,
                                 pStat     OUT NUMBER);

  /**
  @brief Расчет объектов НДР 4 уровня
  @param [in] pBegDate Дата начала расчета
  @param [in] pEndDate Дата окончания расчета
  @param [in] pClient ID клиента
  @param [in] pIIS Признак "Договор ИИС"
  @param [in] pDocID ID текущей операции
  @param [in] pStep  ID шага операции
  @param [in] pFIID  ID ценной бумаги
  @param [in] pTechnical Значение категории "Признак технического расчета" на текущей операции
  @param [in] pRecalc Признак технического расчета из панели операции
  @param [in] pExistTechCalc Существование объектов НДР с признаком "Технический", созданных ранее текущей операции
  @param [in] pTransfDate Дата трансформации ИИС в ИИС-3
  @param [in] pTransfKind Период расчета
  @param [out] pStat Статус расчета
  */
  PROCEDURE CreateTaxObjects4_11(pBegDate   IN DATE,
                                 pEndDate   IN DATE,
                                 pClient    IN NUMBER,
                                 pContract  IN NUMBER,
                                 pIIS       IN NUMBER,
                                 pDocID     IN NUMBER,
                                 pStep      IN NUMBER,
                                 pFIID      IN NUMBER,
                                 pTechnical IN NUMBER,
                                 pRecalc    IN CHAR,
                                 pExistTechCalc IN NUMBER,
                                 pTransfDate IN DATE,
                                 pTransfKind IN NUMBER,
                                 pDateNDR    IN DATE,
                                 pStat     OUT NUMBER);

  /**
  @brief Расчет объектов НДР 4 уровня
  @param [in] pBegDate Дата начала расчета
  @param [in] pEndDate Дата окончания расчета
  @param [in] pClient ID клиента
  @param [in] pIIS Признак "Договор ИИС"
  @param [in] pDocID ID текущей операции
  @param [in] pStep  ID шага операции
  @param [in] pFIID  ID ценной бумаги
  @param [in] pTechnical Значение категории "Признак технического расчета" на текущей операции
  @param [in] pRecalc Признак технического расчета из панели операции
  @param [in] pExistTechCalc Существование объектов НДР с признаком "Технический", созданных ранее текущей операции
  @param [in] pTransfDate Дата трансформации ИИС в ИИС-3
  @param [in] pTransfKind Период расчета
  @param [out] pStat Статус расчета
  */
  PROCEDURE CreateTaxObjects4_12(pBegDate   IN DATE,
                                 pEndDate   IN DATE,
                                 pClient    IN NUMBER,
                                 pContract  IN NUMBER,
                                 pIIS       IN NUMBER,
                                 pDocID     IN NUMBER,
                                 pStep      IN NUMBER,
                                 pFIID      IN NUMBER,
                                 pTechnical IN NUMBER,
                                 pRecalc    IN CHAR,
                                 pExistTechCalc IN NUMBER,
                                 pTransfDate IN DATE,
                                 pTransfKind IN NUMBER,
                                 pDateNDR    IN DATE,
                                 pStat     OUT NUMBER);

  /**
  @brief Расчет объектов НДР 4 уровня
  @param [in] pBegDate Дата начала расчета
  @param [in] pEndDate Дата окончания расчета
  @param [in] pClient ID клиента
  @param [in] pIIS Признак "Договор ИИС"
  @param [in] pDocID ID текущей операции
  @param [in] pStep  ID шага операции
  @param [in] pFIID  ID ценной бумаги
  @param [in] pTechnical Значение категории "Признак технического расчета" на текущей операции
  @param [in] pRecalc Признак технического расчета из панели операции
  @param [in] pExistTechCalc Существование объектов НДР с признаком "Технический", созданных ранее текущей операции
  @param [in] pTransfDate Дата трансформации ИИС в ИИС-3
  @param [in] pTransfKind Период расчета
  @param [out] pStat Статус расчета
  */
  PROCEDURE CreateTaxObjects4_13(pBegDate   IN DATE,
                                 pEndDate   IN DATE,
                                 pClient    IN NUMBER,
                                 pContract  IN NUMBER,
                                 pIIS       IN NUMBER,
                                 pDocID     IN NUMBER,
                                 pStep      IN NUMBER,
                                 pFIID      IN NUMBER,
                                 pTechnical IN NUMBER,
                                 pRecalc    IN CHAR,
                                 pExistTechCalc IN NUMBER,
                                 pTransfDate IN DATE,
                                 pTransfKind IN NUMBER,
                                 pDateNDR    IN DATE,
                                 pStat     OUT NUMBER);

  /**
  @brief Расчет объектов НДР 4 уровня
  @param [in] pBegDate Дата начала расчета
  @param [in] pEndDate Дата окончания расчета
  @param [in] pClient ID клиента
  @param [in] pIIS Признак "Договор ИИС"
  @param [in] pDocID ID текущей операции
  @param [in] pStep  ID шага операции
  @param [in] pFIID  ID ценной бумаги
  @param [in] pTechnical Значение категории "Признак технического расчета" на текущей операции
  @param [in] pRecalc Признак технического расчета из панели операции
  @param [in] pExistTechCalc Существование объектов НДР с признаком "Технический", созданных ранее текущей операции
  @param [in] pTransfDate Дата трансформации ИИС в ИИС-3
  @param [in] pTransfKind Период расчета
  @param [out] pStat Статус расчета
  */
  PROCEDURE CreateTaxObjects4_14(pBegDate   IN DATE,
                                 pEndDate   IN DATE,
                                 pClient    IN NUMBER,
                                 pContract  IN NUMBER,
                                 pIIS       IN NUMBER,
                                 pDocID     IN NUMBER,
                                 pStep      IN NUMBER,
                                 pFIID      IN NUMBER,
                                 pTechnical IN NUMBER,
                                 pRecalc    IN CHAR,
                                 pExistTechCalc IN NUMBER,
                                 pTransfDate IN DATE,
                                 pTransfKind IN NUMBER,
                                 pDateNDR    IN DATE,
                                 pStat     OUT NUMBER);

  /**
  @brief Расчет объектов НДР 4 уровня
  @param [in] pBegDate Дата начала расчета
  @param [in] pEndDate Дата окончания расчета
  @param [in] pClient ID клиента
  @param [in] pIIS Признак "Договор ИИС"
  @param [in] pDocID ID текущей операции
  @param [in] pStep  ID шага операции
  @param [in] pFIID  ID ценной бумаги
  @param [in] pTechnical Значение категории "Признак технического расчета" на текущей операции
  @param [in] pRecalc Признак технического расчета из панели операции
  @param [in] pExistTechCalc Существование объектов НДР с признаком "Технический", созданных ранее текущей операции
  @param [in] pTransfDate Дата трансформации ИИС в ИИС-3
  @param [in] pTransfKind Период расчета
  @param [out] pStat Статус расчета
  */
  PROCEDURE CreateTaxObjects4_15(pBegDate   IN DATE,
                                 pEndDate   IN DATE,
                                 pClient    IN NUMBER,
                                 pContract  IN NUMBER,
                                 pIIS       IN NUMBER,
                                 pDocID     IN NUMBER,
                                 pStep      IN NUMBER,
                                 pFIID      IN NUMBER,
                                 pTechnical IN NUMBER,
                                 pRecalc    IN CHAR,
                                 pExistTechCalc IN NUMBER,
                                 pTransfDate IN DATE,
                                 pTransfKind IN NUMBER,
                                 pDateNDR    IN DATE,
                                 pStat     OUT NUMBER);

  /**
  @brief Расчет объектов НДР 4 уровня
  @param [in] pBegDate Дата начала расчета
  @param [in] pEndDate Дата окончания расчета
  @param [in] pClient ID клиента
  @param [in] pIIS Признак "Договор ИИС"
  @param [in] pDocID ID текущей операции
  @param [in] pStep  ID шага операции
  @param [in] pFIID  ID ценной бумаги
  @param [in] pTechnical Значение категории "Признак технического расчета" на текущей операции
  @param [in] pRecalc Признак технического расчета из панели операции
  @param [in] pExistTechCalc Существование объектов НДР с признаком "Технический", созданных ранее текущей операции
  @param [in] pTransfDate Дата трансформации ИИС в ИИС-3
  @param [in] pTransfKind Период расчета
  @param [out] pStat Статус расчета
  */
  PROCEDURE CreateTaxObjects4_16(pBegDate   IN DATE,
                                 pEndDate   IN DATE,
                                 pClient    IN NUMBER,
                                 pContract  IN NUMBER,
                                 pIIS       IN NUMBER,
                                 pDocID     IN NUMBER,
                                 pStep      IN NUMBER,
                                 pFIID      IN NUMBER,
                                 pTechnical IN NUMBER,
                                 pRecalc    IN CHAR,
                                 pExistTechCalc IN NUMBER,
                                 pTransfDate IN DATE,
                                 pTransfKind IN NUMBER,
                                 pDateNDR    IN DATE,
                                 pStat     OUT NUMBER);

  /**
  @brief Расчет объектов НДР 4 уровня
  @param [in] pBegDate Дата начала расчета
  @param [in] pEndDate Дата окончания расчета
  @param [in] pClient ID клиента
  @param [in] pIIS Признак "Договор ИИС"
  @param [in] pDocID ID текущей операции
  @param [in] pStep  ID шага операции
  @param [in] pFIID  ID ценной бумаги
  @param [in] pTechnical Значение категории "Признак технического расчета" на текущей операции
  @param [in] pRecalc Признак технического расчета из панели операции
  @param [in] pExistTechCalc Существование объектов НДР с признаком "Технический", созданных ранее текущей операции
  @param [in] pTransfDate Дата трансформации ИИС в ИИС-3
  @param [in] pTransfKind Период расчета
  @param [out] pStat Статус расчета
  */
  PROCEDURE CreateTaxObjects4_17(pBegDate   IN DATE,
                                 pEndDate   IN DATE,
                                 pClient    IN NUMBER,
                                 pContract  IN NUMBER,
                                 pIIS       IN NUMBER,
                                 pDocID     IN NUMBER,
                                 pStep      IN NUMBER,
                                 pFIID      IN NUMBER,
                                 pTechnical IN NUMBER,
                                 pRecalc    IN CHAR,
                                 pExistTechCalc IN NUMBER,
                                 pTransfDate IN DATE,
                                 pTransfKind IN NUMBER,
                                 pDateNDR    IN DATE,
                                 pStat     OUT NUMBER);

  /**
  @brief Расчет объектов НДР 4 уровня
  @param [in] pBegDate Дата начала расчета
  @param [in] pEndDate Дата окончания расчета
  @param [in] pClient ID клиента
  @param [in] pIIS Признак "Договор ИИС"
  @param [in] pDocID ID текущей операции
  @param [in] pStep  ID шага операции
  @param [in] pFIID  ID ценной бумаги
  @param [in] pTechnical Значение категории "Признак технического расчета" на текущей операции
  @param [in] pRecalc Признак технического расчета из панели операции
  @param [in] pExistTechCalc Существование объектов НДР с признаком "Технический", созданных ранее текущей операции
  @param [in] pTransfDate Дата трансформации ИИС в ИИС-3
  @param [in] pTransfKind Период расчета
  @param [out] pStat Статус расчета
  */
  PROCEDURE CreateTaxObjects4_18(pBegDate   IN DATE,
                                 pEndDate   IN DATE,
                                 pClient    IN NUMBER,
                                 pContract  IN NUMBER,
                                 pIIS       IN NUMBER,
                                 pDocID     IN NUMBER,
                                 pStep      IN NUMBER,
                                 pFIID      IN NUMBER,
                                 pTechnical IN NUMBER,
                                 pRecalc    IN CHAR,
                                 pExistTechCalc IN NUMBER,
                                 pTransfDate IN DATE,
                                 pTransfKind IN NUMBER,
                                 pDateNDR    IN DATE,
                                 pStat     OUT NUMBER);

  /**
  @brief Расчет объектов НДР 4 уровня
  @param [in] pBegDate Дата начала расчета
  @param [in] pEndDate Дата окончания расчета
  @param [in] pClient ID клиента
  @param [in] pIIS Признак "Договор ИИС"
  @param [in] pDocID ID текущей операции
  @param [in] pStep  ID шага операции
  @param [in] pFIID  ID ценной бумаги
  @param [in] pTechnical Значение категории "Признак технического расчета" на текущей операции
  @param [in] pRecalc Признак технического расчета из панели операции
  @param [in] pExistTechCalc Существование объектов НДР с признаком "Технический", созданных ранее текущей операции
  @param [in] pTransfDate Дата трансформации ИИС в ИИС-3
  @param [in] pTransfKind Период расчета
  @param [out] pStat Статус расчета
  */
  PROCEDURE CreateTaxObjects4_19(pBegDate   IN DATE,
                                 pEndDate   IN DATE,
                                 pClient    IN NUMBER,
                                 pContract  IN NUMBER,
                                 pIIS       IN NUMBER,
                                 pDocID     IN NUMBER,
                                 pStep      IN NUMBER,
                                 pFIID      IN NUMBER,
                                 pTechnical IN NUMBER,
                                 pRecalc    IN CHAR,
                                 pExistTechCalc IN NUMBER,
                                 pTransfDate IN DATE,
                                 pTransfKind IN NUMBER,
                                 pDateNDR    IN DATE,
                                 pStat     OUT NUMBER);

  /**
  @brief Расчет объектов НДР 4 уровня
  @param [in] pBegDate Дата начала расчета
  @param [in] pEndDate Дата окончания расчета
  @param [in] pClient ID клиента
  @param [in] pIIS Признак "Договор ИИС"
  @param [in] pDocID ID текущей операции
  @param [in] pStep  ID шага операции
  @param [in] pFIID  ID ценной бумаги
  @param [in] pTechnical Значение категории "Признак технического расчета" на текущей операции
  @param [in] pRecalc Признак технического расчета из панели операции
  @param [in] pExistTechCalc Существование объектов НДР с признаком "Технический", созданных ранее текущей операции
  @param [in] pTransfDate Дата трансформации ИИС в ИИС-3
  @param [in] pTransfKind Период расчета
  @param [out] pStat Статус расчета
  */
  PROCEDURE CreateTaxObjects4_20(pBegDate   IN DATE,
                                 pEndDate   IN DATE,
                                 pClient    IN NUMBER,
                                 pContract  IN NUMBER,
                                 pIIS       IN NUMBER,
                                 pDocID     IN NUMBER,
                                 pStep      IN NUMBER,
                                 pFIID      IN NUMBER,
                                 pTechnical IN NUMBER,
                                 pRecalc    IN CHAR,
                                 pExistTechCalc IN NUMBER,
                                 pTransfDate IN DATE,
                                 pTransfKind IN NUMBER,
                                 pDateNDR    IN DATE,
                                 pStat     OUT NUMBER);

  /**
  @brief Расчет объектов НДР 4 уровня
  @param [in] pBegDate Дата начала расчета
  @param [in] pEndDate Дата окончания расчета
  @param [in] pClient ID клиента
  @param [in] pIIS Признак "Договор ИИС"
  @param [in] pDocID ID текущей операции
  @param [in] pStep  ID шага операции
  @param [in] pFIID  ID ценной бумаги
  @param [in] pTechnical Значение категории "Признак технического расчета" на текущей операции
  @param [in] pRecalc Признак технического расчета из панели операции
  @param [in] pExistTechCalc Существование объектов НДР с признаком "Технический", созданных ранее текущей операции
  @param [in] pTransfDate Дата трансформации ИИС в ИИС-3
  @param [in] pTransfKind Период расчета
  @param [out] pStat Статус расчета
  */
  PROCEDURE CreateTaxObjects4_21(pBegDate   IN DATE,
                                 pEndDate   IN DATE,
                                 pClient    IN NUMBER,
                                 pContract  IN NUMBER,
                                 pIIS       IN NUMBER,
                                 pDocID     IN NUMBER,
                                 pStep      IN NUMBER,
                                 pFIID      IN NUMBER,
                                 pTechnical IN NUMBER,
                                 pRecalc    IN CHAR,
                                 pExistTechCalc IN NUMBER,
                                 pTransfDate IN DATE,
                                 pTransfKind IN NUMBER,
                                 pDateNDR    IN DATE,
                                 pStat     OUT NUMBER);

  /**
  @brief Расчет объектов НДР 4 уровня
  @param [in] pBegDate Дата начала расчета
  @param [in] pEndDate Дата окончания расчета
  @param [in] pClient ID клиента
  @param [in] pIIS Признак "Договор ИИС"
  @param [in] pDocID ID текущей операции
  @param [in] pStep  ID шага операции
  @param [in] pFIID  ID ценной бумаги
  @param [in] pTechnical Значение категории "Признак технического расчета" на текущей операции
  @param [in] pRecalc Признак технического расчета из панели операции
  @param [in] pExistTechCalc Существование объектов НДР с признаком "Технический", созданных ранее текущей операции
  @param [in] pTransfDate Дата трансформации ИИС в ИИС-3
  @param [in] pTransfKind Период расчета
  @param [out] pStat Статус расчета
  */
  PROCEDURE CreateTaxObjects4_22(pBegDate   IN DATE,
                                 pEndDate   IN DATE,
                                 pClient    IN NUMBER,
                                 pContract  IN NUMBER,
                                 pIIS       IN NUMBER,
                                 pDocID     IN NUMBER,
                                 pStep      IN NUMBER,
                                 pFIID      IN NUMBER,
                                 pTechnical IN NUMBER,
                                 pRecalc    IN CHAR,
                                 pExistTechCalc IN NUMBER,
                                 pTransfDate IN DATE,
                                 pTransfKind IN NUMBER,
                                 pDateNDR    IN DATE,
                                 pStat     OUT NUMBER);

  /**
  @brief Расчет объектов НДР 4 уровня
  @param [in] pBegDate Дата начала расчета
  @param [in] pEndDate Дата окончания расчета
  @param [in] pClient ID клиента
  @param [in] pIIS Признак "Договор ИИС"
  @param [in] pDocID ID текущей операции
  @param [in] pStep  ID шага операции
  @param [in] pFIID  ID ценной бумаги
  @param [in] pTechnical Значение категории "Признак технического расчета" на текущей операции
  @param [in] pRecalc Признак технического расчета из панели операции
  @param [in] pExistTechCalc Существование объектов НДР с признаком "Технический", созданных ранее текущей операции
  @param [in] pTransfDate Дата трансформации ИИС в ИИС-3
  @param [in] pTransfKind Период расчета
  @param [out] pStat Статус расчета
  */
  PROCEDURE CreateTaxObjects4_23(pBegDate   IN DATE,
                                 pEndDate   IN DATE,
                                 pClient    IN NUMBER,
                                 pContract  IN NUMBER,
                                 pIIS       IN NUMBER,
                                 pDocID     IN NUMBER,
                                 pStep      IN NUMBER,
                                 pFIID      IN NUMBER,
                                 pTechnical IN NUMBER,
                                 pRecalc    IN CHAR,
                                 pExistTechCalc IN NUMBER,
                                 pTransfDate IN DATE,
                                 pTransfKind IN NUMBER,
                                 pDateNDR    IN DATE,
                                 pStat     OUT NUMBER);

  /**
  @brief Расчет объектов НДР 4 уровня
  @param [in] pBegDate Дата начала расчета
  @param [in] pEndDate Дата окончания расчета
  @param [in] pClient ID клиента
  @param [in] pIIS Признак "Договор ИИС"
  @param [in] pDocID ID текущей операции
  @param [in] pStep  ID шага операции
  @param [in] pFIID  ID ценной бумаги
  @param [in] pTechnical Значение категории "Признак технического расчета" на текущей операции
  @param [in] pRecalc Признак технического расчета из панели операции
  @param [in] pExistTechCalc Существование объектов НДР с признаком "Технический", созданных ранее текущей операции
  @param [in] pTransfDate Дата трансформации ИИС в ИИС-3
  @param [in] pTransfKind Период расчета
  @param [out] pStat Статус расчета
  */
  PROCEDURE CreateTaxObjects4_24(pBegDate   IN DATE,
                                 pEndDate   IN DATE,
                                 pClient    IN NUMBER,
                                 pContract  IN NUMBER,
                                 pIIS       IN NUMBER,
                                 pDocID     IN NUMBER,
                                 pStep      IN NUMBER,
                                 pFIID      IN NUMBER,
                                 pTechnical IN NUMBER,
                                 pRecalc    IN CHAR,
                                 pExistTechCalc IN NUMBER,
                                 pTransfDate IN DATE,
                                 pTransfKind IN NUMBER,
                                 pDateNDR    IN DATE,
                                 pStat     OUT NUMBER);

  /**
  @brief Расчет объектов НДР 4 уровня
  @param [in] pBegDate Дата начала расчета
  @param [in] pEndDate Дата окончания расчета
  @param [in] pClient ID клиента
  @param [in] pIIS Признак "Договор ИИС"
  @param [in] pDocID ID текущей операции
  @param [in] pStep  ID шага операции
  @param [in] pFIID  ID ценной бумаги
  @param [in] pTechnical Значение категории "Признак технического расчета" на текущей операции
  @param [in] pRecalc Признак технического расчета из панели операции
  @param [in] pExistTechCalc Существование объектов НДР с признаком "Технический", созданных ранее текущей операции
  @param [out] pStat Статус расчета
  */
  PROCEDURE CreateMaterialTaxObjects4_1(pBegDate   IN DATE,
                                        pEndDate   IN DATE,
                                        pClient    IN NUMBER,
                                        pContract  IN NUMBER,
                                        pIIS       IN NUMBER,
                                        pDocID     IN NUMBER,
                                        pStep      IN NUMBER,
                                        pFIID      IN NUMBER,
                                        pTechnical IN NUMBER,
                                        pRecalc    IN CHAR,
                                        pExistTechCalc IN NUMBER,
                                        pTransfDate IN DATE,
                                        pTransfKind IN NUMBER,
                                        pDateNDR    IN DATE,
                                        pStat     OUT NUMBER);

  FUNCTION IsCloseDBO(p_EndDate IN DATE, p_Client IN NUMBER, pIIS IN NUMBER) RETURN NUMBER;

END RSI_NPTXCALC;
/