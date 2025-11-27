 DECLARE
    v_nptxop                    DNPTXOP_DBT%ROWTYPE;
    TYPE ListNPTXOP_t IS TABLE OF DNPTXOP_DBT%ROWTYPE;
    v_ListNPTXOP                ListNPTXOP_t := ListNPTXOP_t ();

    TYPE nptxobj_t IS TABLE OF DNPTXOBJ_DBT%ROWTYPE INDEX BY BINARY_INTEGER;
    v_nptxobj nptxobj_t;

    v_BegDate DATE;
    v_OperDate DATE;
    v_SubKind NUMBER;
    v_i       NUMBER := 1;
    v_f       NUMBER := 1;
    v_UniCodeStr VARCHAR2(15);

    v_StartDate  DATE;
    v_FinishDate DATE;
    v_Year       NUMBER;

    PROCEDURE insertOP(p_begdate IN DATE, p_operdate IN DATE, p_prevdate IN DATE, pSubKind IN NUMBER, p_PartyID IN NUMBER, p_UniCodeStr IN VARCHAR2)
    IS
      v_OperDprt                  NUMBER (10) := 1;
      v_oper                      NUMBER (10) := 1;
      v_Kind_Operation            NUMBER (10) := 2035;
      OBJTYPE_NPTXCALC   CONSTANT NUMBER (5) := 132;
      REFOBJ_NPTXCALC    CONSTANT NUMBER (5) := 1;

      function Get_Count_OP_Name(OP_name in varchar2) return number is
       i number := 0;
      begin 
       select count(1) into i from dnptxop_dbt where t_code like OP_Name and t_dockind = 4605;
        return i;
      end;

     function Get_OP_Name(OP_name in varchar2) return varchar2 is
       i number := 0;
       flag_exit number := 0;
       New_OP_Name varchar2(100);
     begin

      if Get_Count_OP_Name(OP_name) > 0 then
       i := i+1;
       loop
         EXIT WHEN flag_exit = 1;
           if Get_Count_OP_Name(OP_name||'_'||i) = 0 then 
           New_OP_Name := OP_name||'_'||i;
           flag_exit := 1;
         else
           i := i+1;   
         end if;
       end loop;
      else 
        New_OP_Name := OP_name;
      end if;
      return New_OP_Name;
     end;

    BEGIN
        v_nptxop.t_Code := Get_OP_Name(p_UniCodeStr||'_' ||v_i||'_'||p_PartyID);

        v_nptxop.t_ID := dnptxop_dbt_seq.NEXTVAL;
        v_nptxop.t_DocKind := RSI_NPTXC.DL_CALCNDFL;
        v_nptxop.t_OperDate := p_operdate;
        v_nptxop.t_Kind_Operation := v_Kind_Operation;
        v_nptxop.t_Client := p_PartyID;
        v_nptxop.t_Department := v_OperDprt;
        v_nptxop.t_Oper := v_oper;
        v_nptxop.t_Status := RSI_NPTXC.DL_TXOP_Prep;
        v_nptxop.t_SubKind_Operation := pSubKind;
        v_nptxop.t_IIS := CNST.UNSET_CHAR;
        --‡“
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

    FOR clientData IN(    
      SELECT T_CLIENT, EXTRACT(YEAR FROM T_DATE) currYear
        FROM DNPTXOBJ_DBT 
       WHERE T_TECHNICAL = 'X' 
         AND T_DATE BETWEEN TO_DATE('01.01.2022', 'dd.mm.yyyy') AND TO_DATE('31.12.2023', 'dd.mm.yyyy')
       GROUP BY T_CLIENT, EXTRACT(YEAR FROM T_DATE)
   ORDER BY EXTRACT(YEAR FROM T_DATE)
   )
   LOOP
     insertOP(TO_DATE('01.01.'||TO_CHAR(clientData.currYear), 'dd.mm.yyyy'), TO_DATE('31.12.'||TO_CHAR(clientData.currYear), 'dd.mm.yyyy'), TO_DATE('31.12.'||TO_CHAR(clientData.currYear), 'dd.mm.yyyy'), 10, clientData.T_CLIENT, '60114_');
   END LOOP;

    --‚αβ Άª¨
    IF v_ListNPTXOP.COUNT > 0
    THEN
      FORALL i IN v_ListNPTXOP.FIRST .. v_ListNPTXOP.LAST
        INSERT INTO DNPTXOP_DBT
             VALUES v_ListNPTXOP (i);

      v_ListNPTXOP.DELETE;
    END IF;
 
  END;