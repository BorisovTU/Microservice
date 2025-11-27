CREATE OR REPLACE PACKAGE BODY RSI_NPTXOKZ
IS
   FUNCTION isFirstRecForCalc (p_KBK         IN VARCHAR2,
                               p_Rate        IN NUMBER,
                               p_SysCome     IN NUMBER,
                               p_TypeSNOB    IN NUMBER,
                               p_TaxPeriod   IN NUMBER)
      RETURN NUMBER
      DETERMINISTIC
   AS
      v_OKZ             dnptxtemplateokz_dbt%ROWTYPE;
      v_SysComeTemple   NUMBER;
   BEGIN
        SELECT *
          INTO v_OKZ
          FROM dnptxtemplateokz_dbt
         WHERE     t_kbk = p_KBK
               AND t_rate = p_Rate
               AND EXTRACT (YEAR FROM t_enddate) >= p_TaxPeriod
               AND EXTRACT (YEAR FROM t_begdate) <= p_TaxPeriod
      ORDER BY t_typenob FETCH FIRST 1 ROW ONLY;

      v_SysComeTemple := CASE when v_OKZ.t_depo = CHR(88) THEN 2 ELSE 1 END;

      RETURN CASE WHEN v_SysComeTemple = p_SysCome and v_OKZ.t_typenob = p_TypeSNOB THEN 1 ELSE 0 END;
    END;
END RSI_NPTXOKZ;
/