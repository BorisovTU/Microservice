CREATE OR REPLACE PACKAGE RSB_REPRECLOTS IS

  TYPE repreclots_t IS TABLE OF DREPRECLOTS_DBT%ROWTYPE INDEX BY BINARY_INTEGER;

  /* à§•≠‚®‰®™†‚Æ‡Î „Á•‚≠ÎÂ ØÆ‡‚‰•´•©. */
  PortfID_Undef         CONSTANT INTEGER := 0;
  PortfID_SSPU          CONSTANT INTEGER := 1;  /* ëëèì_ñÅ */                    
  PortfID_SSSD          CONSTANT INTEGER := 2;  /* ëëëÑ_ñÅ */
  PortfID_Contr         CONSTANT INTEGER := 3;  /* èäì */
  PortfID_Promissory    CONSTANT INTEGER := 4;  /* èÑé */
  PortfID_ASCB          CONSTANT INTEGER := 5;  /* Äë_ñÅ */
  PortfID_Back          CONSTANT INTEGER := 6;  /* èÇé */
  PortfID_Unadmitted    CONSTANT INTEGER := 7;  /* Åèè */


  FUNCTION GetSetBppDate(p_SumID IN NUMBER) RETURN DATE;

  FUNCTION DefinePortfID(p_Portfolio IN NUMBER, p_State IN NUMBER, p_Buy_Sale IN NUMBER, p_DealID IN NUMBER, p_Amount IN NUMBER, p_AmountBD IN NUMBER) RETURN NUMBER;

  FUNCTION GetOverAccID_M(p_FIID IN NUMBER, p_Portfolio IN NUMBER, p_PortfID IN NUMBER, p_Date IN DATE) RETURN NUMBER;

  FUNCTION GetOverAccID_P(p_FIID IN NUMBER, p_Portfolio IN NUMBER, p_PortfID IN NUMBER, p_Date IN DATE) RETURN NUMBER;

  FUNCTION GetIncomeAccID(p_FIID IN NUMBER, p_Portfolio IN NUMBER, p_PortfID IN NUMBER, p_Date IN DATE) RETURN NUMBER;

  FUNCTION GetDiscountAccID(p_FIID IN NUMBER, p_Portfolio IN NUMBER, p_PortfID IN NUMBER, p_Date IN DATE) RETURN NUMBER;

  FUNCTION GetBonusAccID(p_FIID IN NUMBER, p_Portfolio IN NUMBER, p_PortfID IN NUMBER, p_Date IN DATE) RETURN NUMBER;

  FUNCTION GetReserveAccID(p_FIID IN NUMBER, p_Portfolio IN NUMBER, p_PortfID IN NUMBER, p_Date IN DATE) RETURN NUMBER;

  FUNCTION GetPDDReserveAccID(p_FIID IN NUMBER, p_Portfolio IN NUMBER, p_PortfID IN NUMBER, p_Date IN DATE) RETURN NUMBER;

  FUNCTION GetCorrAccID_M(p_FIID IN NUMBER, p_Portfolio IN NUMBER, p_PortfID IN NUMBER, p_Date IN DATE) RETURN NUMBER;

  FUNCTION GetCorrAccID_P(p_FIID IN NUMBER, p_Portfolio IN NUMBER, p_PortfID IN NUMBER, p_Date IN DATE) RETURN NUMBER;

  FUNCTION GetCorrEstResAccID_M(p_FIID IN NUMBER, p_Portfolio IN NUMBER, p_PortfID IN NUMBER, p_Date IN DATE) RETURN NUMBER;

  FUNCTION GetCorrEstResAccID_P(p_FIID IN NUMBER, p_Portfolio IN NUMBER, p_PortfID IN NUMBER, p_Date IN DATE) RETURN NUMBER;

  FUNCTION GetUNKDAccID(p_FIID IN NUMBER, p_Portfolio IN NUMBER, p_PortfID IN NUMBER, p_Date IN DATE) RETURN NUMBER;

  PROCEDURE CorrecData(p_RepDate IN DATE, p_repreclots IN OUT repreclots_t);

  PROCEDURE ProcessFI(p_RepDate       IN DATE,
                      p_FIID          IN NUMBER,
                      p_SSPU          IN NUMBER,
                      p_SSSD          IN NUMBER,
                      p_ASCB          IN NUMBER,
                      p_BPP           IN NUMBER,
                      p_PVO           IN NUMBER,
                      p_PKU           IN NUMBER,
                      p_PDO           IN NUMBER,
                      p_SessionID     IN NUMBER);


  PROCEDURE CreateAllData(p_RepDate       IN DATE,
                          p_FIID          IN NUMBER,
                          p_SSPU          IN NUMBER,
                          p_SSSD          IN NUMBER,
                          p_ASCB          IN NUMBER,
                          p_BPP           IN NUMBER,
                          p_PVO           IN NUMBER,
                          p_PKU           IN NUMBER,
                          p_PDO           IN NUMBER,
                          p_SessionID     IN NUMBER,
                          p_ParallelLevel IN NUMBER);


END RSB_REPRECLOTS;
/

