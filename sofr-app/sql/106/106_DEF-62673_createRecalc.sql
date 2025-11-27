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

    FOR clientData IN(
      SELECT REQQ.ClientID,
             REQQ.DateCancel,
             EXTRACT(YEAR FROM REQQ.DateCancel) YearOfDate
        FROM DNPTXOP_DBT OPER,
             (SELECT T_RECORDPAYMENTQTYID,
                     (SELECT T_PAYMENTDATE FROM (SELECT * FROM DCDRECORDS_DBT WHERE T_RECORDPAYMENTQTYID = MREC.T_RECORDPAYMENTQTYID ORDER BY T_ID DESC) WHERE  ROWNUM = 1) DateCancel,
                     (SELECT T_PAYMENTDATE FROM (SELECT AREC.*, ROW_NUMBER() OVER (ORDER BY AREC.T_ID DESC) RowNo FROM DCDRECORDS_DBT AREC WHERE AREC.T_RECORDPAYMENTQTYID = MREC.T_RECORDPAYMENTQTYID) WHERE RowNo = 2 ) DateActive,
                     (SELECT T_OBJECTID    FROM DOBJCODE_DBT WHERE T_CODE = (SELECT T_CLIENTID_OBJECTID FROM DCDRECORDS_DBT WHERE T_RECORDPAYMENTQTYID = MREC.T_RECORDPAYMENTQTYID AND ROWNUM = 1) AND T_OBJECTTYPE = 3 AND T_CODEKIND = 1 AND ROWNUM =1) ClientID
                FROM DCDRECORDS_DBT MREC WHERE 
                     --Если последняя записть отменена
                     EXISTS(
                       SELECT * FROM(
                         SELECT * FROM(
                           SELECT * FROM DCDRECORDS_DBT IREC WHERE IREC.T_RECORDPAYMENTQTYID = MREC.T_RECORDPAYMENTQTYID ORDER BY T_ID DESC)
                           WHERE ROWNUM = 1) ROWN
                         WHERE LOWER(T_OPERATIONSTATUS) = 'отменена'
                           AND MREC.T_RECORDPAYMENTQTYID = ROWN.T_RECORDPAYMENTQTYID)
                     --А предпоследняя является выплатой купона
                 AND EXISTS (
                   SELECT 1 
                    FROM (SELECT RECR.*, ROW_NUMBER() OVER (ORDER BY RECR.T_ID DESC) RowNo FROM DCDRECORDS_DBT RECR WHERE RECR.T_RECORDPAYMENTQTYID = MREC.T_RECORDPAYMENTQTYID)
                   WHERE RowNo = 2
                     AND T_CORPORATEACTIONTYPE = 'INTR'
                     AND ((T_ISGETTAX <> CHR(88)) OR (T_ISGETTAX is null)) 
                     AND T_ACCOUNTNUMBER like '306%' 
                     AND LOWER(T_OPERATIONSTATUS) = 'активна')
            GROUP BY T_RECORDPAYMENTQTYID) REQQ
       WHERE REQQ.ClientID = OPER.T_CLIENT
         AND OPER.T_OPERDATE BETWEEN REQQ.DateActive AND REQQ.DateCancel
         AND OPER.T_KIND_OPERATION = 2035
         AND OPER.T_SUBKIND_OPERATION IN (20, 10))
   LOOP
     insertOP(TO_DATE('01.01.'||TO_CHAR(clientData.YearOfDate), 'dd.mm.yyyy'), clientData.DateCancel, clientData.DateCancel, 20, clientData.ClientID, '62673_');
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