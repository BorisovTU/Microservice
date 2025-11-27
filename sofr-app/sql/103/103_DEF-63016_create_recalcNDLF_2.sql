/*Формирование операций пересчета по DEF-63016*/
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
   v_OperDprt     NUMBER (10) := 1; 
   v_PrevCLientID NUMBER := -1;
   v_PrevYear     NUMBER := 0;


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

   PROCEDURE CreateOpClient(p_ClientID IN NUMBER, p_StartYear IN NUMBER)
   AS
     v_LastOpDate DATE := TO_DATE ('01.01.0001', 'DD.MM.YYYY');
     v_EndYear NUMBER := 0;
     v_CurYear NUMBER := 0;
   BEGIN

     SELECT NVL(MAX (op.t_OperDate), TO_DATE ('01.01.0001', 'DD.MM.YYYY')) INTO v_LastOpDate 
       FROM dnptxop_DBT op 
      WHERE op.t_DocKind = 4605 AND op.t_Client = p_ClientID 
        AND op.t_OperDate >= TO_DATE ('01.01.' || p_StartYear, 'DD.MM.YYYY');

     v_EndYear := EXTRACT(YEAR FROM v_LastOpDate);

     v_CurYear := p_StartYear;
     WHILE v_CurYear <= v_EndYear
     LOOP
       v_BegDate := TO_DATE ('01.01.'||TO_CHAR(v_CurYear), 'DD.MM.YYYY');
       
       IF v_CurYear = 2024 THEN
         v_EndDate := v_LastOpDate;
         v_SubKind := 20; --Обычный расчет
       ELSE
         v_EndDate := TO_DATE ('31.12.'||TO_CHAR(v_CurYear), 'DD.MM.YYYY');
         v_SubKind := 10; --Окончание года 
       END IF;

       insertOP (v_BegDate,
                 v_EndDate,
                 v_EndDate,
                 v_SubKind,
                 p_ClientID,
                 CNST.UNSET_CHAR,
                 '63016(1)');

       v_CurYear := v_CurYear + 1;
     END LOOP;

   END;

BEGIN
   v_i := 1; 

   FOR one_rec IN (SELECT q.* 
                     FROM (SELECT tk.t_DealID, tk.t_ClientID, 
                                 (SELECT MAX (tk1.t_DealDate) 
                                    FROM ddl_tick_dbt tk1 
                                   WHERE tk1.t_BOfficeKind = tk.t_BOfficeKInd 
                                     AND tk1.t_ClientID = tk.t_ClientID 
                                     AND EXTRACT(YEAR FROM tk1.t_DealDate) < EXTRACT(YEAR FROM tk.t_DealDate) 
                                     AND INSTR (tk1.t_DealCode, 'ДЕПО') > 0 
                                     AND tk1.t_PFI = tk.t_PFI
                                 ) AS MaxDepoDealDate 
                             FROM ddl_tick_dbt tk 
                            WHERE tk.t_BOfficeKind = 127 
                              AND INSTR (tk.t_DealCode, 'NDFL') > 0 
                              AND NOT EXISTS(SELECT 1 
                                               FROM ddl_tick_dbt tk1 
                                              WHERE tk1.t_BOfficeKind = tk.t_BOfficeKInd 
                                                AND tk1.t_ClientID = tk.t_ClientID 
                                                AND EXTRACT(YEAR FROM tk1.t_DealDate) = EXTRACT(YEAR FROM tk.t_DealDate) 
                                                AND INSTR (tk1.t_DealCode, 'ДЕПО') > 0 
                                                AND tk1.t_PFI = tk.t_PFI) 
                              AND EXISTS(SELECT 1 
                                           FROM ddl_tick_dbt tk1 
                                          WHERE tk1.t_BOfficeKind = tk.t_BOfficeKInd 
                                            AND tk1.t_ClientID = tk.t_ClientID 
                                            AND EXTRACT(YEAR FROM tk1.t_DealDate) < EXTRACT(YEAR FROM tk.t_DealDate) 
                                            AND INSTR (tk1.t_DealCode, 'ДЕПО') > 0 
                                            AND tk1.t_PFI = tk.t_PFI) 
                              AND tk.t_ClientID IN (118441, 137526, 140337, 159586, 160313, 160587, 161800, 162530, 162907, 163354, 163444, 
                                                    163790, 164513, 164787, 166376, 166680, 167999, 168380, 168767, 169511, 174874, 175478, 
                                                    175832, 176646, 178073, 178567, 179160, 179937, 180283, 180354, 180648, 181644, 181683, 
                                                    183168, 183520, 183809, 185467, 185525, 187740, 188797, 189840, 191316, 192928, 193049, 
                                                    196092, 197017, 197602, 199009, 200323, 200710, 201876, 203010, 203328, 204554, 205157, 
                                                    207009, 209535, 209743, 213453, 216214, 217260, 219551, 219850, 221045, 221145, 226313, 
                                                    235809, 237846, 239199, 247021, 247862, 271120, 272975, 273629, 273833, 274417, 275275, 
                                                    312651, 312681, 319587, 321805
                                                   )
                          ) q 
                   ORDER BY q.t_ClientID ASC, EXTRACT (YEAR FROM q.MaxDepoDealDate) ASC
                 ) 
   LOOP 
      IF v_PrevClientID != one_rec.t_ClientID THEN
        
        IF v_PrevClientID > 0 THEN
          CreateOpClient(v_PrevClientID, v_PrevYear);
        END IF;
        
        
        v_PrevClientID := one_rec.t_ClientID;
        v_PrevYear     := EXTRACT(YEAR FROM one_rec.MaxDepoDealDate);
      END IF;

      UPDATE ddl_tick_dbt 
         SET t_DealDate = one_rec.MaxDepoDealDate 
       WHERE t_DealID = one_rec.t_DealID; 
        
      UPDATE ddlrq_dbt 
         SET t_PlanDate = one_rec.MaxDepoDealDate, 
             t_FactDate = one_rec.MaxDepoDealDate 
       WHERE t_DocKind = 127 
         AND t_DocID = one_rec.t_DealID; 
             
      UPDATE ddl_leg_dbt 
         SET t_Maturity = one_rec.MaxDepoDealDate, 
             t_Expiry = one_rec.MaxDepoDealDate 
       WHERE t_DealID = one_rec.t_DealID;           
       
   END LOOP; 

   IF v_PrevClientID > 0 THEN
     CreateOpClient(v_PrevClientID, v_PrevYear);
   END IF;
     
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