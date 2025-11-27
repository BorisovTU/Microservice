CREATE OR REPLACE PACKAGE RSHB_USER
AS
   -- Обновляет таблицу остатков по КДУ перед проведением сверки с вышестоящим депозитарием
   PROCEDURE UpdateSCRest (pStorID    IN INTEGER,
                           pDepoAcc   IN VARCHAR2,
                           pDate      IN DATE);

   FUNCTION EuroImpNeedKdu (P_Date IN DATE, P_TXT OUT VARCHAR2)
      RETURN INTEGER;

   FUNCTION GetKDURestOnDate (p_FIID IN NUMBER, p_Date IN DATE)
      RETURN NUMBER;

   FUNCTION DetermineOp (p_FIID           IN     INTEGER,
                         p_CalcDiscount      OUT INTEGER,
                         p_CalcBonus         OUT INTEGER,
                         p_CalcOutlay        OUT INTEGER)
      RETURN INTEGER;

   FUNCTION Bond_HasZeroCoupons (FIID IN NUMBER, CalcDate IN DATE)
      RETURN NUMBER;

   FUNCTION GA_GETPARTYID (PTCODE IN VARCHAR2)
      RETURN NUMBER;

   PROCEDURE CheckCoupon (pCheckCouponAmount   IN NUMBER,
                          pCheckCouponNumber   IN NUMBER,
                          pCheckCouponDate     IN NUMBER,
                          pCheckCouponRate     IN NUMBER,
                          pOnlyOwnPortfolio    IN NUMBER,
                          pCurDate             IN DATE,
                          pOnlyNewCoupon       IN NUMBER);

  FUNCTION uGetMainObjAttrRecID(p_ObjectType IN dobjatcor_dbt.t_ObjectType%TYPE,
                                p_Object     IN dobjatcor_dbt.t_Object%TYPE,
                                p_GroupID    IN dobjatcor_dbt.t_GroupID%TYPE,
                                p_Date       IN dobjatcor_dbt.t_ValidFromDate%TYPE)
    RETURN number;

  function uGetChangeAtCorDate(CurAtRecID in integer) return date;

  function uGetDateExclusion(p_partyID in integer) return date;

  procedure insertDateLog(pOperation in varchar2);

  function ConvertNumToBase(pNum  in integer,
                            pBase in varchar2,
                            vStr  out varchar2) return number;

  function uGenerateClientCode_FX(pRefVal in integer, vCode out varchar2)
    return integer;

  function uGenerateUnicClientCode_FX(pRefNum       in integer,
                                      pOperDprt     in integer,
                                      pOperDprtNode in integer,
                                      pDate         in date,
                                      vCode         out varchar2)
    return integer;
                          
END;
/
