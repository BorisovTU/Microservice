/*Формирование операций пересчета*/
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
   v_MinYear      NUMBER;


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
                 '62331');

       v_CurYear := v_CurYear + 1;
     END LOOP;

   END;
BEGIN
   v_i := 1; 
   SELECT t_CurDate
     INTO v_CurDate
     FROM dcurdate_dbt
    WHERE t_Branch = v_OperDprt AND t_IsMain = 'X'; 
   v_CurYear := EXTRACT (YEAR FROM v_CurDate); 
   FOR cClientData
      IN (SELECT q.t_Client, q.MinYear 
            FROM (  select q1.t_Client, MIN(q1.BuyYear) as MinYear FROM 
                   (SELECT  /*+ leading(Lot)  index(Lot dnptxlot_dbt_idx8)*/Lot.t_Client, EXTRACT (YEAR FROM Lot.t_BuyDate) AS BuyYear 
                      FROM  v_npx_dnptxlot Lot 
                     WHERE Lot.t_GOID > 0 and Lot.t_Kind = 1 AND Lot.t_Origin IN (1, 2) 
                          AND Lot.t_BuyDate >= TO_DATE ('01.01.2021', 'DD.MM.YYYY') 
                           AND EXISTS(SELECT  /*+ index(obj dnptxobj_dbt_idx2)*/ 1 
                                        FROM dnptxobj_dbt obj 
                                       WHERE obj.t_Client = Lot.t_Client 
                                         AND obj.t_Kind = 10 
                                         AND obj.t_Date = Lot.t_BuyDate 
                                         AND obj.t_Direction = 2 
                                         AND obj.t_AnaliticKind1 = Lot.t_AnaliticKind1 
                                         AND obj.t_Analitic1 = Lot.t_Analitic1 
                                         AND obj.t_AnaliticKind3 = 3010 
                                         AND obj.t_Analitic3 = Lot.t_FIID 
                                         AND obj.t_AnaliticKind6 = 6020 
                                         AND obj.t_Analitic6 = Lot.t_Contract 
                                         AND EXISTS(SELECT  /*+ index(obj1 dnptxobj_dbt_idx2)*/ 1 
                                                      FROM dnptxobj_dbt obj1 
                                                     WHERE obj1.t_Client        = obj.t_Client         
                                                       AND obj1.t_Kind          = obj.t_Kind           
                                                       AND obj1.t_Direction     = obj.t_Direction     
                                                       AND obj1.t_AnaliticKind1 = obj.t_AnaliticKind1 
                                                       AND obj1.t_Analitic1     = obj.t_Analitic1     
                                                       AND obj1.t_AnaliticKind3 = obj.t_AnaliticKind3 
                                                       AND obj1.t_Analitic3     = obj.t_Analitic3     
                                                       AND obj1.t_AnaliticKind6 = obj.t_AnaliticKind6 
                                                       AND obj1.t_Analitic6     = obj.t_Analitic6 
                                                       AND obj1.t_Date          < obj.t_Date     
                                                   ) 
                                      )   
                  ) q1                     
                  GROUP BY q1.t_Client 
                  ORDER BY q1.t_Client ASC 
                  ) q 
 
         )
   LOOP

     v_MinYear := cClientData.MinYear;
     IF cClientData.t_Client = 182187 OR cClientData.t_Client = 242095 THEN
       v_MinYear := 2022;
     END IF;


     CreateOpClient(cClientData.t_Client, cClientData.MinYear);

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