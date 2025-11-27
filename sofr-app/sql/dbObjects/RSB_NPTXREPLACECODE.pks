CREATE OR REPLACE PACKAGE RSB_NPTXREPLACECODE
IS
    CLIENT_FROM_ALL          CONSTANT NUMBER (5) := 1;
    CLIENT_FROM_FILE         CONSTANT NUMBER (5) := 2;
    CLIENT_FROM_FILE_DIASOFT CONSTANT NUMBER (5) := 3;

    CLIENT_ID_TYPE_SOFR   CONSTANT NUMBER (5) := 1;
    CLIENT_ID_TYPE_CFT    CONSTANT NUMBER (5) := 2;

    STATE_OPEN            CONSTANT NUMBER (5) := 0;
    STATE_CLOSE           CONSTANT NUMBER (5) := 1;
    STATE_ERR             CONSTANT NUMBER (5) := 2;

    FUNCTION GetOprDate (pClientID     IN NUMBER,
                         pOpenContr    IN CHAR,
                         pCloseContr   IN CHAR,
                         pIIS          IN CHAR,
                         pEndDate      IN DATE,
                         pTaxPeriod    IN NUMBER)
        RETURN DATE;

    FUNCTION GetContrByType (pClientID     IN NUMBER,
                             pOpenContr    IN CHAR,
                             pCloseContr   IN CHAR,
                             pIIS          IN CHAR,
                             pTaxPeriod    IN NUMBER)
        RETURN NUMBER;

    PROCEDURE CheckClient (pPackNum       IN NUMBER,
                           pEndID         IN NUMBER,
                           pSessionID     IN VARCHAR2,
                           pClientGroup   IN NUMBER DEFAULT 0,
                           pClientType    IN NUMBER DEFAULT 0,
                           pOpenContr     IN CHAR,
                           pCloseContr    IN CHAR,
                           pSOFR          IN CHAR,
                           pDiasoft       IN CHAR,
                           pBeginDate     IN DATE,
                           pEndDate       IN DATE);

    PROCEDURE CreateReplaceOp (pPackNum       IN NUMBER,
                               pEndID         IN NUMBER,
                               pSessionID IN VARCHAR2, 
                               pOpPrefix IN VARCHAR2 DEFAULT CHR (0),
                               pSaleDeal           IN CHAR,
                               pChangeCodeIncome   IN CHAR,
                               pSOFR               IN CHAR,
                               pDiasoft            IN CHAR);

    PROCEDURE ExuGenReplaceOp (
        pBeginDate          IN DATE,
        pEndDate            IN DATE,
        pTaxPeriod          IN NUMBER,
        pIIS                IN CHAR,
        pOpenContr          IN CHAR,
        pCloseContr         IN CHAR,
        pSaleDeal           IN CHAR,
        pChangeCodeIncome   IN CHAR,
        pSOFR               IN CHAR,
        pDiasoft            IN CHAR,
        pOpPrefix           IN VARCHAR2 DEFAULT CHR (0),
        pClientGroup        IN NUMBER DEFAULT 0,
        pClientType         IN NUMBER DEFAULT 0,
        pSessionID          IN VARCHAR2);

    FUNCTION ConvertDateTimeToDay (pDate IN DATE, pTime IN DATE)
        RETURN NUMBER;

    PROCEDURE makeDataDiasoft (pOprID    IN NUMBER);

    PROCEDURE deleteNDRbyPay  (pOprID   IN NUMBER,
                               pPayID    IN NUMBER,
                               pClientID IN NUMBER);

END RSB_NPTXREPLACECODE;