/*Формирование операций пересчета Скрипт 1*/
DECLARE
   v_nptxop       DNPTXOP_DBT%ROWTYPE; 
   TYPE ListNPTXOP_t IS TABLE OF DNPTXOP_DBT%ROWTYPE; 
   v_ListNPTXOP   ListNPTXOP_t := ListNPTXOP_t (); 
   TYPE nptxobj_t IS TABLE OF DNPTXOBJ_DBT%ROWTYPE
                        INDEX BY BINARY_INTEGER; 
   v_nptxobj      nptxobj_t; 
   v_SubKind      NUMBER;
   v_i            NUMBER := 1;
   v_f            NUMBER := 1; 
   v_BegDate      DATE;
   v_EndDate      DATE;
   v_Year         NUMBER; 
   v_CurDate      DATE;
   v_CurYear      NUMBER; 
   v_OperDprt     NUMBER (10) := 1; 
   PROCEDURE insertOP (p_begdate      IN DATE,
                       p_operdate     IN DATE,
                       p_prevdate     IN DATE,
                       pSubKind       IN NUMBER,
                       p_PartyID      IN NUMBER,
                       p_IIS          IN CHAR,
                       p_UniCodeStr   IN VARCHAR2)
   IS
      v_oper                      NUMBER (10) := 1;
      v_Kind_Operation            NUMBER (10) := 2035;
      OBJTYPE_NPTXCALC   CONSTANT NUMBER (5) := 132;
      REFOBJ_NPTXCALC    CONSTANT NUMBER (5) := 1; 
      FUNCTION Get_Count_OP_Name (OP_name IN VARCHAR2)
         RETURN NUMBER
      IS
         i   NUMBER := 0;
      BEGIN
         SELECT COUNT (1)
           INTO i
           FROM dnptxop_dbt
          WHERE t_code LIKE OP_Name AND t_dockind = 4605; 
         RETURN i;
      END; 
      FUNCTION Get_OP_Name (OP_name IN VARCHAR2)
         RETURN VARCHAR2
      IS
         i             NUMBER := 0;
         flag_exit     NUMBER := 0;
         New_OP_Name   VARCHAR2 (100);
      BEGIN
         IF Get_Count_OP_Name (OP_name) > 0
         THEN
            i := i + 1; 
            LOOP
               EXIT WHEN flag_exit = 1; 
               IF Get_Count_OP_Name (OP_name || '_' || i) = 0
               THEN
                  New_OP_Name := OP_name || '_' || i;
                  flag_exit := 1;
               ELSE
                  i := i + 1;
               END IF;
            END LOOP;
         ELSE
            New_OP_Name := OP_name;
         END IF; 
         RETURN New_OP_Name;
      END;
   BEGIN
      v_nptxop.t_Code :=
         Get_OP_Name (p_UniCodeStr || '_' || v_i || '_' || p_PartyID); 
      v_nptxop.t_ID := dnptxop_dbt_seq.NEXTVAL;
      v_nptxop.t_DocKind := RSI_NPTXC.DL_CALCNDFL;
      v_nptxop.t_OperDate := p_operdate;
      v_nptxop.t_Kind_Operation := v_Kind_Operation;
      v_nptxop.t_Client := p_PartyID;
      v_nptxop.t_Department := v_OperDprt;
      v_nptxop.t_Oper := v_oper;
      v_nptxop.t_Status := RSI_NPTXC.DL_TXOP_Prep;
      v_nptxop.t_SubKind_Operation := pSubKind;
      v_nptxop.t_IIS := p_IIS;
      --ЗПУ
      v_nptxop.t_Account := RSI_RsbOperation.ZERO_STR;
      v_nptxop.t_AccountTax := RSI_RsbOperation.ZERO_STR;
      v_nptxop.t_BegRecalcDate := p_begdate;
      v_nptxop.t_CalcNDFL := CNST.SET_CHAR;
      v_nptxop.t_Contract := 0;
      v_nptxop.t_Currency := 0;
      v_nptxop.t_CurrencySum := 0;
      v_nptxop.t_CurrentYear_Sum := 0;
      v_nptxop.t_EndRecalcDate := p_prevdate;
      v_nptxop.t_FIID := -1;
      v_nptxop.t_FlagTax := CNST.UNSET_CHAR;
      v_nptxop.t_LimitStatus := 0;
      v_nptxop.t_MarketPlace := 0;
      v_nptxop.t_MarketPlace2 := 0;
      v_nptxop.t_MarketSector := 0;
      v_nptxop.t_MarketSector2 := 0;
      v_nptxop.t_Method := 0;
      v_nptxop.t_OutCost := 0;
      v_nptxop.t_OutSum := 0;
      v_nptxop.t_Partial := CNST.UNSET_CHAR;
      v_nptxop.t_Place := 0;
      v_nptxop.t_Place2 := 0;
      v_nptxop.t_PlaceKind := 0;
      v_nptxop.t_PlaceKind2 := 0;
      v_nptxop.t_PrevDate := p_prevdate;
      v_nptxop.t_PrevTaxSum := 0;
      v_nptxop.t_Recalc := CNST.SET_CHAR;
      v_nptxop.t_Tax := 0;
      v_nptxop.t_TaxBase := 0;
      v_nptxop.t_TaxSum := 0;
      v_nptxop.t_TaxSum2 := 0;
      v_nptxop.t_TaxToPay := 0;
      v_nptxop.t_Time := NPTAX.UnknownTime;
      v_nptxop.t_TotalTaxSum := 0;
      v_nptxop.t_TOUT := 0;
      v_nptxop.t_TaxDp := 0;
      v_ListNPTXOP.EXTEND ();
      v_ListNPTXOP (v_ListNPTXOP.LAST) := v_nptxop; 
      v_i := v_i + 1;
   END;
BEGIN
   v_i := 1; 
   SELECT t_CurDate
     INTO v_CurDate
     FROM dcurdate_dbt
    WHERE t_Branch = v_OperDprt AND t_IsMain = 'X'; 
   v_CurYear := EXTRACT (YEAR FROM v_CurDate); 
   FOR cClientData
      IN (SELECT q.t_ClientID
  FROM (SELECT DISTINCT nptxop.t_Client as t_ClientID
          FROM dparty_dbt pt, dnptxop_dbt nptxop
         WHERE pt.t_PartyID IN
               (select op.t_client
                  from dnptxop_dbt op
                 where 1 = 1
                   and op.t_dockind = 4605
                   and op.t_kind_operation = 2035
                   and op.t_subkind_operation = 20
                   and op.t_operdate >= to_date('01012023', 'ddmmyyyy')
                   and op.t_status != 2
                   and not exists
                 (select *
                          from dnptxop_dbt op1
                         where 1 = 1
                           and op1.t_dockind = op.t_dockind
                           and op1.t_kind_operation = op.t_kind_operation
                           and op1.t_subkind_operation =
                               op.t_subkind_operation
                           and op1.t_operdate >= op.t_operdate
                           and op1.t_client = op.t_client
                           and op1.t_status = 2)
                 group by op.t_client
                )
           AND nptxop.t_DocKind = 4605
           AND nptxop.t_Client = pt.t_PartyID
           AND nptxop.t_OperDate = TO_DATE('31.12.2022', 'DD.MM.YYYY')
           AND nptxop.t_SubKind_Operation = 10 /*Окончание года*/
           AND nptxop.t_Status = 2) q
 ORDER BY q.t_ClientID ASC
         )
   LOOP 
     v_BegDate := TO_DATE ('01.01.2022', 'DD.MM.YYYY');
     v_EndDate := TO_DATE ('31.12.2022', 'DD.MM.YYYY');
     v_SubKind := 10; --Окончание года 
     insertOP (v_BegDate,
               v_EndDate,
               v_EndDate,
               v_SubKind,
               cClientData.t_ClientID,
               CNST.UNSET_CHAR,
               'xxxx1');
   END LOOP; 
   --Вставки
   IF v_ListNPTXOP.COUNT > 0
   THEN
      FORALL i IN v_ListNPTXOP.FIRST .. v_ListNPTXOP.LAST
         INSERT INTO DNPTXOP_DBT
              VALUES v_ListNPTXOP (i); 
      v_ListNPTXOP.DELETE;
   END IF;
END; 
/

BEGIN
 EXECUTE IMMEDIATE 'COMMIT';
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/