CREATE OR REPLACE PACKAGE RSI_NPTXOP
IS

/**
 * Получить параметры NPTXOP из записи, переданной в виде Raw
 * @since 6.20.031.52
 * @qtest NO
 * @param pRawNPTXOP Запись NPTXOP в виде Raw
 * @param pNPTXOP Запись NPTXOP
 */
   PROCEDURE CopyRAWtoNPTXOP( pRawNPTXOP IN RAW, pNPTXOP OUT DNPTXOP_DBT%ROWTYPE );

/**
 * Получить параметры Raw из записи NPTXOP
 * @since 6.20.031.52
 * @qtest NO
 * @param pNPTXOP Запись NPTXOP
 * @param pRawNPTXOP Запись NPTXOP в виде Raw
 */
   PROCEDURE CopyNPTXOPtoRAW( pNPTXOP IN DNPTXOP_DBT%ROWTYPE, pRawNPTXOP IN OUT RAW );

/**
 * Получить количество клиентов к обработке
 * @since 6.20.031.52
 * @qtest NO
 * @param pCreateDate Дата создания операции
 * @param pIIS Признак ИИС
 */
  FUNCTION GetCountClients( pCreateDate IN DATE, pIIS IN CHAR ) RETURN NUMBER;

/**
 * Создание операций по номеру пакета
 * @since 6.20.031.52
 * @qtest NO
 * @param pPackNum Номер пакета обработки данных
 * @param pEndID Не исп.
 * @param pGUID Уникальный идентификатор
 * @param pOperDprt ИД филиала
 * @param pCreateDate Дата создания операции
 * @param pSubKind Подвид операции расчета НОБ для НДФЛ
 * @param pOperNum Номер операции
 * @param pIIS Признак индивидального инвестиционного счета
 * @param pWarnLaterOper Сообщать о более поздних операциях
 * @param pNoFormNoDeals Не формировать при отсутствии сделок
 */
  PROCEDURE CreateNptxOp( pPackNum IN NUMBER, pEndID IN NUMBER, pGUID IN VARCHAR2, pOperDprt IN NUMBER, pOper IN NUMBER,
                          pCreateDate IN DATE,
                          pSubKind IN NUMBER,
                          pOperNum IN VARCHAR2,
                          pIIS IN CHAR,
                          pWarnLaterOper IN CHAR,
                          pNoFormNoDeals IN CHAR,
                          pRecalc IN CHAR DEFAULT CHR(0),
                          pCalcNDFL IN CHAR DEFAULT CHR(0),
                          pOpPrefix IN VARCHAR2 DEFAULT CHR(0),
                          pIsTechnical IN CHAR DEFAULT CHR(0),
                          pClientIdType IN NUMBER DEFAULT 0);

/**
 * Массовое создание операций NPTXOP
 * @since 6.20.031.52
 * @qtest NO
 * @param pCreateDate Дата создания операции
 * @param pSubKind Подвид операции расчета НОБ для НДФЛ
 * @param pOperNum Номер операции
 * @param pIIS Признак индивидального инвестиционного счета
 * @param pWarnLaterOper Сообщать о более поздних операциях
 * @param pNoFormNoDeals Не формировать при отсутствии сделок
 * @param pGUID Уникальный идентификатор
 * @param pExecPackSize Размер пакета обработки данных
 */
  PROCEDURE MasCreateNptxOp( pCreateDate IN DATE,
                             pSubKind IN NUMBER,
                             pOperNum IN VARCHAR2,
                             pIIS IN CHAR,
                             pWarnLaterOper IN CHAR,
                             pNoFormNoDeals IN CHAR,
                             pGUID IN VARCHAR2,
                             pExecPackSize IN NUMBER,
                             pRecalc IN CHAR DEFAULT CHR(0),
                             pCalcNDFL IN CHAR DEFAULT CHR(0),
                             pClientGroup IN NUMBER DEFAULT 0,
                             pClientIdType IN NUMBER DEFAULT 0,
                             pFIID IN NUMBER DEFAULT 0,
                             pTaxPeriod IN NUMBER DEFAULT 0,
                             pPeriodFrom IN DATE DEFAULT TO_DATE('01.01.0001', 'dd.mm.yyyy'),
                             pPeriodTo IN DATE DEFAULT TO_DATE('01.01.0001', 'dd.mm.yyyy'),
                             pOpPrefix IN VARCHAR2 DEFAULT CHR(0),
                             pIsTechnical IN CHAR DEFAULT CHR(0));

/**
  @brief Получить строку ошибок по операции NPTXOP
  @param [in] pNPTXOPID Идентификатор операции
  @return Строка ошибок по операции
 */
  FUNCTION GetStrMes( pNPTXOPID IN NUMBER ) RETURN VARCHAR2;

/**
  @brief Получить количество ошибок по операции NPTXOP
  @param [in] pNPTXOPID Идентификатор операции
  @return Количество ошибок по операции
 */
  FUNCTION GetCntMes( pNPTXOPID IN NUMBER ) RETURN NUMBER;

  FUNCTION NptxCalcTaxPrevDateByKind( p_Client IN NUMBER, p_IIS IN CHAR, p_OperDate IN DATE, pSubKind IN NUMBER, pDlContrID IN NUMBER, pCorrectDate IN NUMBER DEFAULT 0, pAddDay IN NUMBER DEFAULT 0 ) RETURN DATE;

  FUNCTION CheckExistsLucreData(pBegDate   IN DATE,
                                pEndDate   IN DATE,
                                pClient    IN NUMBER) RETURN NUMBER;

END RSI_NPTXOP;
/