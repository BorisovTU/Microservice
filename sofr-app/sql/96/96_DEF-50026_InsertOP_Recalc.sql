DECLARE
BEGIN
  UPDATE DNPTXOBJ_DBT
     SET T_SUM = 4938000,
         T_SUM0 = 4938000
   WHERE T_DATE = TO_DATE('07.12.2018','DD.MM.YYYY')
     AND T_CLIENT = 128422
     AND T_KIND = 10
     AND T_SUM = 4997651.04;
END;
/

DECLARE
BEGIN
  UPDATE DNPTXOBJ_DBT
     SET T_SUM = 1000,
         T_SUM0 = 1000
   WHERE T_DATE = TO_DATE('07.12.2018','DD.MM.YYYY')
     AND T_CLIENT = 128422
     AND T_KIND = 10
     AND T_SUM = 1012.08;
END;
/

DECLARE
   v_nptxop DNPTXOP_DBT%ROWTYPE;
   TYPE ListNPTXOP_t IS TABLE OF DNPTXOP_DBT%ROWTYPE;
   v_ListNPTXOP ListNPTXOP_t := ListNPTXOP_t();
   v_BegDate DATE := TO_DATE('01.01.2021', 'dd.mm.yyyy');
   v_EndDate DATE := TO_DATE('31.12.2021', 'dd.mm.yyyy');
   v_SubKind NUMBER;
   v_i NUMBER := 1;
   v_Count NUMBER := 0;
   v_OperDprt NUMBER(10) := 1;
   v_oper NUMBER(10) := 1;

BEGIN

   v_nptxop.t_Code := '50026_rec_128422';
   v_nptxop.t_ID := dnptxop_dbt_seq.NEXTVAL;
   v_nptxop.t_DocKind := RSI_NPTXC.DL_CALCNDFL;
   v_nptxop.t_OperDate := v_enddate;
   v_nptxop.t_Kind_Operation := 2035;
   v_nptxop.t_Client := 128422;
   v_nptxop.t_Department := 1;
   v_nptxop.t_Oper := 1;
   v_nptxop.t_Status := 0;
   v_nptxop.t_SubKind_Operation := 10; /*Окончание года*/
   v_nptxop.t_IIS := CHR(0);

   v_nptxop.t_Account := CHR(1);
   v_nptxop.t_AccountTax := CHR(1);
   v_nptxop.t_BegRecalcDate := v_begdate;
   v_nptxop.t_CalcNDFL := 'X';
   v_nptxop.t_Contract := 0;
   v_nptxop.t_Currency := 0;
   v_nptxop.t_CurrencySum := 0;
   v_nptxop.t_CurrentYear_Sum := 0;
   v_nptxop.t_EndRecalcDate := v_enddate;
   v_nptxop.t_FIID := -1;
   v_nptxop.t_FlagTax := CHR(0);
   v_nptxop.t_LimitStatus := 0;
   v_nptxop.t_MarketPlace := 0;
   v_nptxop.t_MarketPlace2 := 0;
   v_nptxop.t_MarketSector := 0;
   v_nptxop.t_MarketSector2 := 0;
   v_nptxop.t_Method := 0;
   v_nptxop.t_OutCost := 0;
   v_nptxop.t_OutSum := 0;
   v_nptxop.t_Partial := CHR(0);
   v_nptxop.t_Place := 0;
   v_nptxop.t_Place2 := 0;
   v_nptxop.t_PlaceKind := 0;
   v_nptxop.t_PlaceKind2 := 0;
   v_nptxop.t_PrevDate := v_enddate;
   v_nptxop.t_PrevTaxSum := 0;
   v_nptxop.t_Recalc := 'X';
   v_nptxop.t_Tax := 0;
   v_nptxop.t_TaxBase := 0;
   v_nptxop.t_TaxSum := 0;
   v_nptxop.t_TaxSum2 := 0;
   v_nptxop.t_TaxToPay := 0;
   v_nptxop.t_Time := TO_DATE('01.01.0001 00:00:00','DD.MM.YYYY HH24:MI:SS');
   v_nptxop.t_TotalTaxSum := 0;
   v_nptxop.t_TOUT := 0;
   v_nptxop.t_TaxDp := 0;
   v_ListNPTXOP.EXTEND ();
   v_ListNPTXOP (v_ListNPTXOP.LAST) := v_nptxop;

   IF v_ListNPTXOP.COUNT > 0
   THEN
      FORALL i IN v_ListNPTXOP.FIRST..v_ListNPTXOP.LAST
         INSERT INTO DNPTXOP_DBT
              VALUES v_ListNPTXOP(i);
   END IF;

EXCEPTION
   WHEN OTHERS
   THEN NULL;
END;
/

BEGIN
 EXECUTE IMMEDIATE 'COMMIT';
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/