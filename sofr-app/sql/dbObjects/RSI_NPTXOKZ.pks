CREATE OR REPLACE PACKAGE RSI_NPTXOKZ
IS
   FUNCTION isFirstRecForCalc (p_KBK         IN VARCHAR2,
                               p_Rate        IN NUMBER,
                               p_SysCome     IN NUMBER,
                               p_TypeSNOB    IN NUMBER,
                               p_TaxPeriod   IN NUMBER)
      RETURN NUMBER
      DETERMINISTIC;
END RSI_NPTXOKZ;
/