CREATE OR REPLACE PACKAGE BODY RSB_BILL
IS
   TYPE ArrDates_t IS TABLE OF DATE;

     -- Наш ли вексель по эмитенту
   FUNCTION IsOurBanner (p_IssuerID          IN     NUMBER)
      RETURN BOOLEAN
   IS
     v_PartyId NUMBER;
   BEGIN
     SELECT t_PartyID into v_PartyId FROM ddp_dep_dbt WHERE t_PartyID = p_IssuerID;
     RETURN TRUE;
   EXCEPTION
      WHEN OTHERS
      THEN
         RETURN FALSE;
   END IsOurBanner;

-- Возвращает статус векселя на дату
  FUNCTION GetVABnrStatusOnDate(BCID   IN dvsbnrbck_dbt.t_BCID%TYPE,
                                OnDate IN dvsbnrbck_dbt.t_ChangeDate%TYPE)
        RETURN dvsbnrbck_dbt.t_NewABCStatus%TYPE
  IS
        retval dvsbnrbck_dbt.t_NewABCStatus%TYPE := 0; -- По умолчанию - отложенный

  BEGIN

        FOR cur IN (SELECT bck.t_NewABCStatus FROM dvsbnrbck_dbt bck
                  WHERE bck.t_ABCStatus = 'X'
                    AND bck.t_ChangeDate <= onDate
                    AND bck.t_BCID = BCID
               ORDER BY bck.t_ChangeDate DESC, bck.t_ID DESC)
        LOOP
            retval := cur.t_NewABCStatus;
            EXIT;
        END LOOP;

        RETURN retval;
  END GetVABnrStatusOnDate;

  FUNCTION GetVSBnrStatusOnDate(BCID   IN dvsbnrbck_dbt.t_BCID%TYPE,
                                OnDate IN dvsbnrbck_dbt.t_ChangeDate%TYPE)
        RETURN dvsbnrbck_dbt.t_NewABCStatus%TYPE
  IS
        retval dvsbnrbck_dbt.t_NewABCStatus%TYPE := 0; -- По умолчанию - отложенный

  BEGIN

        FOR cur IN (SELECT bck.t_NewABCStatus FROM dvsbnrbck_dbt bck
                  WHERE bck.t_BCStatus = 'X'
                    AND bck.t_ChangeDate <= onDate
                    AND bck.t_BCID = BCID
               ORDER BY bck.t_ChangeDate DESC, bck.t_ID DESC)
        LOOP
            retval := cur.t_NewABCStatus;
            EXIT;
        END LOOP;

        RETURN retval;
  END GetVSBnrStatusOnDate;

-- Возвращает статус векселя на дату
  FUNCTION GetVABnrStateOnDate(BCID   IN dvsbnrbck_dbt.t_BCID%TYPE,
                               OnDate IN dvsbnrbck_dbt.t_ChangeDate%TYPE)
        RETURN dvsbnrbck_dbt.t_NewBCState%TYPE
  IS
        retval dvsbnrbck_dbt.t_NewBCState%TYPE := 0; -- По умолчанию - отложенный

  BEGIN

        FOR cur IN (SELECT bck.t_NewBCState FROM dvsbnrbck_dbt bck
                  WHERE (bck.t_IsBCState = 'A' OR bck.t_IsBCState = 'R')
                    AND bck.t_ChangeDate <= onDate
                    AND bck.t_BCID = BCID
               ORDER BY bck.t_ChangeDate DESC, bck.t_ID DESC)
        LOOP
            retval := cur.t_NewBCState;
            EXIT;
        END LOOP;

        RETURN retval;
  END GetVABnrStateOnDate;


  FUNCTION GetVABnrBOOnDate(BCID   IN dvsbanner_dbt.t_BCID%TYPE,
                            OnDate IN dvsbanner_dbt.t_IssueDate%TYPE)
        RETURN d711bill_tmp.t_BO%TYPE
  IS
        bosymb CHAR;
  BEGIN
        bosymb := CHR(0);

        FOR cur IN (SELECT tick.t_BofficeKind
                      FROM ddl_tick_dbt tick
                     WHERE tick.t_DealID = rsb_bill.GetVABnrBalanceDealID(BCID, OnDate))
        LOOP
           IF cur.t_BofficeKind = DL_VATRUST OR cur.t_BofficeKind = TS_DOC_DECLAR THEN
             bosymb := 'А'; --доверительное управление
           ELSE
             bosymb := 'N'; --учтенные векселя
           END IF;

           EXIT;
        END LOOP;

        RETURN bosymb;
  END GetVABnrBOOnDate;

  --получить ID клиента, для которого был куплен вексель на дату
  FUNCTION GetVABnrClientOnDate(BCID   IN dvsbanner_dbt.t_BCID%TYPE,
                                OnDate IN dvsbanner_dbt.t_IssueDate%TYPE)
        RETURN NUMBER
  IS
        ClientID NUMBER(10);
  BEGIN
        ClientID := -1;
        FOR cur IN (SELECT tick.t_ClientID, tick.t_IsPartyClient, tick.t_PartyID,
                           (SELECT lnk.t_LinkKind
                              FROM dvsordlnk_dbt lnk
                             WHERE lnk.t_DocKind = tick.t_BOfficeKind
                               AND lnk.t_ContractID = tick.t_DealID
                               AND lnk.t_BCID = BCID
                           ) as t_LinkKind
                      FROM ddl_tick_dbt tick
                     WHERE tick.t_DealID = rsb_bill.GetVABnrBalanceDealID(BCID, OnDate))
        LOOP
           ClientID := cur.t_ClientID;
           IF cur.t_LinkKind = VSORDLNK_K_SALE AND cur.t_IsPartyClient = 'X' THEN
             ClientID := cur.t_PartyID;
           END IF;

           EXIT;
        END LOOP;
        RETURN ClientID;
  END GetVABnrClientOnDate;

  --получить договор обслуживания клиента, для которого был куплен вексель на дату
  FUNCTION GetVABnrClientContrIDOnDate(BCID   IN dvsbanner_dbt.t_BCID%TYPE,
                                       OnDate IN dvsbanner_dbt.t_IssueDate%TYPE)
        RETURN NUMBER
  IS
        ClientContrID NUMBER(10);
  BEGIN
        ClientContrID := 0;
        FOR cur IN (SELECT tick.t_ClientContrID
                      FROM ddl_tick_dbt tick
                     WHERE tick.t_DealID = rsb_bill.GetVABnrBalanceDealID(BCID, OnDate))
        LOOP
           ClientContrID := cur.t_ClientContrID;
           EXIT;
        END LOOP;
        RETURN ClientContrID;
  END GetVABnrClientContrIDOnDate;

  --Определеить валюту учета ц/б (кроме собственных)
  FUNCTION GetVABnrPayFIID(BCID    IN dvsbanner_dbt.t_BCID%TYPE,
                           IsTrust IN NUMBER DEFAULT 0,
                           IsTemp  IN NUMBER DEFAULT 0)
        RETURN NUMBER
  IS
     bnr           dvsbanner_dbt%ROWTYPE;
     bnr_tmp       dvsbanner_tmp%ROWTYPE;
     bnrBackOffice dvsbanner_dbt.t_BackOffice%TYPE;
     bnrPayFIID    dvsbanner_dbt.t_PayFIID%TYPE;
     leg           ddl_leg_dbt%ROWTYPE;
     leg_tmp       ddl_leg_tmp%ROWTYPE;
     legPFI        ddl_leg_dbt.t_PFI%TYPE;
     PayFIID       dfininstr_dbt.t_FIID%TYPE;
     ModePayFI     BOOLEAN;
  BEGIN

    IF IsTemp = 0 THEN

      SELECT b.* INTO bnr
        FROM dvsbanner_dbt b
       WHERE b.t_BCID = BCID;

      bnrBackOffice := bnr.t_BackOffice;
      bnrPayFIID := bnr.t_PayFIID;

      SELECT l.* INTO leg
        FROM ddl_leg_dbt l
       WHERE l.t_LegKind = LEG_KIND_VSBANNER
         AND l.t_DealID = BCID
         AND l.t_LegID = 0;

      legPFI := leg.t_PFI;

    ELSE

      SELECT b.* INTO bnr_tmp
        FROM dvsbanner_tmp b
       WHERE b.t_BCID = BCID;

      bnrBackOffice := bnr_tmp.t_BackOffice;
      bnrPayFIID := bnr_tmp.t_PayFIID;

      SELECT l.* INTO leg_tmp
        FROM ddl_leg_tmp l
       WHERE l.t_LegKind = LEG_KIND_VSBANNER
         AND l.t_DealID = BCID
         AND l.t_LegID = 0;

      legPFI := leg_tmp.t_PFI;

    END IF;

    IF IsTrust <> 0 OR bnrBackOffice = 'А' THEN
      PayFIID := legPFI;
    ELSE
      IF bnrPayFIID <> -1 THEN
        PayFIID := bnrPayFIID;
      ELSE
        IF legPFI = RSI_RSB_FIInstr.NATCUR THEN
          PayFIID := RSI_RSB_FIInstr.NATCUR;
        ELSE
          ModePayFI := Rsb_Common.GetRegBoolValue('УЧТЕННЫЕ ВЕКСЕЛЯ\РЕЖИМ РАБОТЫ\УЧЕТ_ВАЛ_ВЕКС_БЕЗ_ОЭП_В_РУБЛЯХ');
          IF ModePayFI = TRUE THEN
            PayFIID := RSI_RSB_FIInstr.NATCUR;
          ELSE
            PayFIID := legPFI;
          END IF;
        END IF;
      END IF;
    END IF;

    RETURN PayFIID;

  END GetVABnrPayFIID;

  --Определить валюту учета ц/б для собственных
  FUNCTION GetVSBnrPayFIID(BCID    IN dvsbanner_dbt.t_BCID%TYPE,
                           IsTemp  IN NUMBER DEFAULT 0)
        RETURN NUMBER
  IS
     bnr           dvsbanner_dbt%ROWTYPE;
     bnr_tmp       dvsbanner_tmp%ROWTYPE;
     bnrBackOffice dvsbanner_dbt.t_BackOffice%TYPE;
     bnrPayFIID    dvsbanner_dbt.t_PayFIID%TYPE;
     leg           ddl_leg_dbt%ROWTYPE;
     leg_tmp       ddl_leg_tmp%ROWTYPE;
     legPFI        ddl_leg_dbt.t_PFI%TYPE;
     PayFIID       dfininstr_dbt.t_FIID%TYPE;
     ModePayFI     BOOLEAN;
  BEGIN

    IF IsTemp = 0 THEN

      SELECT b.* INTO bnr
        FROM dvsbanner_dbt b
       WHERE b.t_BCID = BCID;

      bnrBackOffice := bnr.t_BackOffice;
      bnrPayFIID := bnr.t_PayFIID;

      SELECT l.* INTO leg
        FROM ddl_leg_dbt l
       WHERE l.t_LegKind = LEG_KIND_VSBANNER
         AND l.t_DealID = BCID
         AND l.t_LegID = 0;

      legPFI := leg.t_PFI;

    ELSE

      SELECT b.* INTO bnr_tmp
        FROM dvsbanner_tmp b
       WHERE b.t_BCID = BCID;

      bnrBackOffice := bnr_tmp.t_BackOffice;
      bnrPayFIID := bnr_tmp.t_PayFIID;

      SELECT l.* INTO leg_tmp
        FROM ddl_leg_tmp l
       WHERE l.t_LegKind = LEG_KIND_VSBANNER
         AND l.t_DealID = BCID
         AND l.t_LegID = 0;

      legPFI := leg_tmp.t_PFI;

    END IF;

    IF bnrPayFIID <> -1 THEN
      PayFIID := bnrPayFIID;
    ELSE
      IF legPFI = RSI_RSB_FIInstr.NATCUR THEN
        PayFIID := RSI_RSB_FIInstr.NATCUR;
      ELSE
        ModePayFI := Rsb_Common.GetRegBoolValue('ВЕКСЕЛЯ БАНКА\РЕЖИМ РАБОТЫ\УЧЕТ_ВАЛ_ВЕКС_БЕЗ_ОЭП_В_РУБЛЯХ');
        IF ModePayFI = TRUE THEN
          PayFIID := RSI_RSB_FIInstr.NATCUR;
        ELSE
          PayFIID := legPFI;
        END IF;
      END IF;
    END IF;

    RETURN PayFIID;

  END GetVSBnrPayFIID;

   --Получить цену последней продажи векселя в ВН
   FUNCTION GetLastBnrSalePrice (p_LegId           IN     NUMBER,
                                 p_CalcDate        IN     DATE,
                                 p_LastSalePrice      OUT NUMBER)
      RETURN NUMBER
   IS
      v_PFI          NUMBER;
      v_BCID         NUMBER;
      v_LastSaleFI   NUMBER;
      v_stat         INTEGER;
   BEGIN
      v_stat := 0;

      SELECT leg.t_DealID, leg.t_PFI
        INTO v_BCID, v_PFI
        FROM DDL_LEG_DBT leg
       WHERE leg.t_ID = p_LegId;

        SELECT lnk.t_BCCost, lnk.t_BCCFI
          INTO p_LastSalePrice, v_LastSaleFI
          FROM dvsbnrbck_dbt bck,
               doprdocs_dbt docs,
               dvsordlnk_dbt lnk,
               doproper_dbt oper
         WHERE     lnk.t_DocKind = oper.t_DocKind
               AND LPAD (lnk.t_ContractID, 10, '0') = oper.t_DocumentID
               AND lnk.t_BCID = v_BCID
               AND oper.t_DocKind IN (DL_VEKSELORDER, DL_VSBARTERORDER, DL_VSSALE)
               AND oper.t_ID_Operation = docs.t_ID_Operation
               AND docs.t_DocKind = DL_VSBNRBCK
               AND docs.t_DocumentID = LPAD (bck.t_ID, 10, '0')
               AND bck.t_BCID = v_BCID
               AND bck.t_ChangeDate <= p_CalcDate
               AND bck.t_BCStatus = 'X'
               AND bck.t_NewABCStatus = VSBANNER_STATUS_SENDED
               AND ROWNUM = 1
      ORDER BY bck.t_ID DESC;

      IF (v_LastSaleFI != v_PFI)
      THEN
         p_LastSalePrice :=
            RSI_RSB_FIInstr.ConvSum (p_LastSalePrice,
                                     v_LastSaleFI,
                                     v_PFI,
                                     p_CalcDate,
                                     2);
      END IF;


      RETURN v_stat;
   EXCEPTION
      WHEN OTHERS
      THEN
         RETURN 1;
   END GetLastBnrSalePrice;

  --Получить данные постановки векселя на баланс (УВ)
  PROCEDURE GetVABnrBalancePrm(p_BCID               IN  dvsbanner_dbt.t_BCID%TYPE,
                               p_BoundDate          IN  DATE,
                               p_ClientContrID      IN  ddl_tick_dbt.t_ClientContrID%TYPE,
                               p_SetDealID          IN  ddl_tick_dbt.t_DealID%TYPE,
                               p_BalanceDate        OUT DATE,
                               p_DealID             OUT ddl_tick_dbt.t_DealID%TYPE,
                               p_Cost               OUT dvsordlnk_dbt.t_BCCost%TYPE,
                               p_CostFI             OUT dvsordlnk_dbt.t_BCCFI%TYPE,
                               p_DealType           OUT ddl_tick_dbt.t_DealType%TYPE,
                               p_DealCode           OUT ddl_tick_dbt.t_DealCode%TYPE,
                               p_BOfficeKind        OUT ddl_tick_dbt.t_BOfficeKind%TYPE,
                               p_ClientID           IN  ddl_tick_dbt.t_ClientID%TYPE DEFAULT -1,
                               p_SaleID             IN  ddl_tick_dbt.t_DealID%TYPE DEFAULT -1
                              )
  AS
     v_ClientID NUMBER;
     v_BckID    NUMBER;
  BEGIN

     p_BalanceDate := TO_DATE('01.01.0001','DD.MM.YYYY');
     p_DealID      := 0;
     p_Cost        := 0;
     p_CostFI      := -1;
     p_DealType    := 0;
     p_DealCode    := CHR(1);
     p_BOfficeKind := 0;

     v_ClientID := p_ClientID;
     IF v_ClientID = RSBSESSIONDATA.OurBank THEN
       v_ClientID := -1;
     END IF;

     v_BckID := -1;
     BEGIN
        IF( NVL(p_SaleID, 0) > 0 ) THEN
           SELECT MAX(bck.t_ID) INTO v_BckID
             FROM ddl_tick_dbt tick, dvsbnrbck_dbt bck, doprdocs_dbt oprdocs, doproper_dbt opr
            WHERE bck.t_BCID = p_BCID
              AND bck.t_ChangeDate <= p_BoundDate
              AND bck.t_ABCStatus = 'X'
              AND bck.t_OldABCStatus = VABANNER_STATUS_ACCOUNT
              AND oprdocs.t_DocKind = 191 --изменение учтенного векселя
              AND oprdocs.t_DocumentID = LTRIM(TO_CHAR (bck.t_ID, '0000000000'))
              AND opr.t_ID_Operation = oprdocs.t_ID_Operation
              AND tick.t_BOfficeKind = opr.t_DocKind
              AND LPAD( tick.t_DealID, 34,'0' ) = opr.t_DocumentID
              AND tick.t_DealID      = p_SaleID;
        END IF;
     EXCEPTION WHEN OTHERS THEN v_BckID := -1;
     END;

     FOR cur IN (SELECT bck.t_ChangeDate, tick.t_DealID, lnk.t_BCCost, lnk.t_BCCFI, tick.t_DealType, tick.t_DealCode, tick.t_BOfficeKind,
                        tick.t_ClientID, tick.t_IsPartyClient, tick.t_PartyID, lnk.t_LinkKind
                   FROM ddl_tick_dbt tick, dvsbnrbck_dbt bck, doprdocs_dbt oprdocs, doproper_dbt opr, dvsordlnk_dbt lnk
                  WHERE bck.t_BCID = p_BCID
                    AND bck.t_ChangeDate <= p_BoundDate
                    AND bck.t_ABCStatus = 'X'
                    AND bck.t_NewABCStatus = VABANNER_STATUS_ACCOUNT
                    AND oprdocs.t_DocKind = 191 --изменение учтенного векселя
                    AND oprdocs.t_DocumentID = LTRIM(TO_CHAR (bck.t_ID, '0000000000'))
                    AND opr.t_ID_Operation = oprdocs.t_ID_Operation
                    AND tick.t_BOfficeKind = opr.t_DocKind
                    AND LPAD( tick.t_DealID, 34,'0' ) = opr.t_DocumentID
                    AND lnk.t_ContractID = tick.t_DealID
                    AND lnk.t_BCID = bck.t_BCID
                    AND 1 = (CASE WHEN p_ClientContrID IS NULL OR p_ClientContrID <= 0 OR p_ClientContrID = tick.t_ClientContrID THEN 1 ELSE 0 END)
                    AND 1 = (CASE WHEN p_SetDealID IS NULL OR p_SetDealID <= 0 OR p_SetDealID = tick.t_DealID THEN 1 ELSE 0 END)
                    AND 1 = (CASE WHEN v_BckID <= 0 OR v_BckID > bck.t_ID THEN 1 ELSE 0 END)
                    --AND 1 = (CASE WHEN p_ClientID <= 0 OR (p_ClientID > 0 AND tick.t_ClientID = p_ClientID) OR (p_ClientID = RSBSESSIONDATA.OurBank AND tick.t_ClientID = -1) THEN 1 ELSE 0 END)
                  ORDER BY bck.t_ChangeDate DESC, bck.t_ID DESC  )
      LOOP

        p_BalanceDate := cur.t_ChangeDate;
        p_DealID      := cur.t_DealID;
        p_Cost        := cur.t_BCCost;
        p_CostFI      := cur.t_BCCFI;
        p_DealType    := cur.t_DealType;
        p_DealCode    := cur.t_DealCode;
        p_BOfficeKind := cur.t_BOfficeKind;

        IF p_ClientID > 0 THEN
          IF cur.t_LinkKind = VSORDLNK_K_BUY AND v_ClientID = cur.t_ClientID THEN
            EXIT;
          ELSIF cur.t_LinkKind = VSORDLNK_K_SALE AND cur.t_IsPartyClient = 'X' AND v_ClientID = cur.t_PartyID THEN
            EXIT;
          END IF;
        ELSE
          EXIT;
        END IF;

      END LOOP;

      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          p_BalanceDate := TO_DATE('01.01.0001','DD.MM.YYYY');
          p_DealID      := 0;
          p_Cost        := 0;
          p_CostFI      := -1;
          p_DealType    := 0;
          p_DealCode    := CHR(1);
          p_BOfficeKind := 0;

  END GetVABnrBalancePrm;


  --Получить ID сделки постановки векселя на баланс на дату
  FUNCTION GetVABnrBalanceDealID(p_BCID   IN dvsbanner_dbt.t_BCID%TYPE,
                                 p_OnDate IN dvsbanner_dbt.t_IssueDate%TYPE)
        RETURN NUMBER
  IS
        v_BalanceDate        dpmpaym_dbt.t_ValueDate%TYPE;
        v_DealID             ddl_tick_dbt.t_DealID%TYPE;
        v_Cost               dvsordlnk_dbt.t_BCCost%TYPE;
        v_CostFI             dvsordlnk_dbt.t_BCCFI%TYPE;
        v_DealType           ddl_tick_dbt.t_DealType%TYPE;
        v_DealCode           ddl_tick_dbt.t_DealCode%TYPE;
        v_BOfficeKind        ddl_tick_dbt.t_BOfficeKind%TYPE;
  BEGIN

        GetVABnrBalancePrm(p_BCID, p_OnDate, 0, 0, v_BalanceDate, v_DealID, v_Cost, v_CostFI, v_DealType, v_DealCode, v_BOfficeKind);

        RETURN v_DealID;

  END GetVABnrBalanceDealID;

  --Получить стоимость поставки векселя (в ВН) - оплаченная стоимость в ВР, переведенная в ВН на дату поставки
  FUNCTION GetVABnrCostPFI(p_BCID   IN dvsbanner_dbt.t_BCID%TYPE,
                           p_OnDate IN dvsbanner_dbt.t_IssueDate%TYPE)
        RETURN NUMBER
  IS
        v_AccountedCost dvsordlnk_dbt.t_BCCost%TYPE;   --Учтенная стоимость
        v_PayDate       dpmpaym_dbt.t_ValueDate%TYPE;  --Фактическая дата оплаты
        v_DealPayFIID   dpmpaym_dbt.t_PayFIID%TYPE;    --Валюта расчетов

        v_leg     ddl_leg_dbt%ROWTYPE;
        v_tick    ddl_tick_dbt%ROWTYPE;

        v_BalanceDate  dpmpaym_dbt.t_ValueDate%TYPE;
        v_DealID       ddl_tick_dbt.t_DealID%TYPE;
        v_Cost         dvsordlnk_dbt.t_BCCost%TYPE;
        v_CostFI       dvsordlnk_dbt.t_BCCFI%TYPE;
        v_DealType     ddl_tick_dbt.t_DealType%TYPE;
        v_DealCode     ddl_tick_dbt.t_DealCode%TYPE;
        v_BOfficeKind  ddl_tick_dbt.t_BOfficeKind%TYPE;

  BEGIN
        v_AccountedCost := 0;

        v_DealPayFIID := -1;

        GetVABnrBalancePrm(p_BCID, p_OnDate, 0, 0, v_BalanceDate, v_DealID, v_Cost, v_CostFI, v_DealType, v_DealCode, v_BOfficeKind);
        IF v_DealID > 0 THEN

           SELECT tk.* INTO v_tick
             FROM ddl_tick_dbt tk
            WHERE tk.t_DealID = v_DealID;

           BEGIN
             SELECT pm.t_ValueDate, pm.t_PayFIID INTO v_PayDate, v_DealPayFIID
               FROM dpmpaym_dbt pm
              WHERE pm.t_DocKind = v_BOfficeKind
                AND pm.t_DocumentID = v_DealID
                AND pm.t_Purpose = Rsb_Payment.CAi
                AND ROWNUM = 1;

             EXCEPTION
               WHEN OTHERS THEN
                  v_PayDate := v_BalanceDate; --например в зачислении, платежа по контрактиву нет и дата оплаты равна дате сделки
                  v_DealPayFIID := -1; --например в зачислении, платежа по контрактиву нет и валюты расчетов соответственно тоже
           END;

           SELECT leg.* INTO v_leg
             FROM ddl_leg_dbt leg
            WHERE leg.t_DealID = p_BCID
              AND leg.t_LegKind = LEG_KIND_VSBANNER
              AND leg.t_LegID = 0;

           IF v_BalanceDate <= v_PayDate THEN  --если поставка раньше оплаты
             IF v_CostFI = v_leg.t_PFI THEN
               v_AccountedCost := v_Cost;
             ELSE
               IF v_DealPayFIID = -1 THEN
                 v_AccountedCost := RSI_RSB_FIInstr.ConvSum(v_Cost, v_CostFI, v_leg.t_PFI, v_BalanceDate, 2);
               ELSE
                 v_AccountedCost := RSI_RSB_FIInstr.ConvSum(v_Cost, v_CostFI, v_DealPayFIID, v_BalanceDate, 2); --это сколько реально заплатили в покупке в ВР
                 v_AccountedCost := RSI_RSB_FIInstr.ConvSum(v_AccountedCost, v_DealPayFIID, v_leg.t_PFI, v_BalanceDate, 2);
               END IF;
             END IF;
           ELSE
             v_AccountedCost := RSI_RSB_FIInstr.ConvSum(v_Cost, v_CostFI, v_DealPayFIID, v_PayDate, 2); --это сколько реально заплатили в покупке в ВР
             v_AccountedCost := RSI_RSB_FIInstr.ConvSum(v_AccountedCost, v_DealPayFIID, v_leg.t_PFI, v_BalanceDate, 2);
           END IF;

        END IF;

        RETURN v_AccountedCost;

  END GetVABnrCostPFI;

  --Получить дату учета векселя
  FUNCTION GetVABnrLastBalanceDate(p_BCID   IN dvsbanner_dbt.t_BCID%TYPE,
                                   p_OnDate IN dvsbanner_dbt.t_IssueDate%TYPE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY'))
        RETURN DATE
  IS
        v_Date DATE;
  BEGIN

        IF p_OnDate = TO_DATE('01.01.0001','DD.MM.YYYY') THEN
          SELECT MAX(t_ChangeDate) INTO v_Date
            FROM dvsbnrbck_dbt
           WHERE t_BCID = p_BCID
             AND t_NewABCStatus = VABANNER_STATUS_ACCOUNT;
        ELSE
          SELECT MAX(t_ChangeDate) INTO v_Date
            FROM dvsbnrbck_dbt
           WHERE t_BCID = p_BCID
             AND t_NewABCStatus = VABANNER_STATUS_ACCOUNT
             AND t_ChangeDate <= p_OnDate;
        END IF;

        RETURN v_Date;

        EXCEPTION
          WHEN OTHERS THEN
            RETURN TO_DATE('01.01.0001','DD.MM.YYYY');

  END GetVABnrLastBalanceDate;

  --ID записи в истории по векселю, в результате которой статус векселя изменился на "Учтен"
  FUNCTION GetVABnrLastAccountedEnrolID(p_BCID   IN dvsbanner_dbt.t_BCID%TYPE,
                                        p_OnDate IN dvsbanner_dbt.t_IssueDate%TYPE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY'),
                                        p_BalanceDealID IN NUMBER DEFAULT 0,
                                        p_BalanceDocKind IN NUMBER DEFAULT 0)
        RETURN NUMBER
  IS
        v_Enrolment_ID NUMBER(10);
        v_Date DATE;
  BEGIN
        v_Enrolment_ID := 0;

        v_Date := GetVABnrLastBalanceDate(p_BCID, p_OnDate);

        IF p_BalanceDealID <> 0 AND p_BalanceDocKind <> 0 THEN
          SELECT t_ID INTO v_Enrolment_ID
            FROM (SELECT bck.t_ID
                    FROM dvsbnrbck_dbt bck
                   WHERE bck.t_BCID = p_BCID
                     AND bck.t_ChangeDate = v_Date
                     AND bck.t_NewABCStatus = VABANNER_STATUS_ACCOUNT
                     AND EXISTS(SELECT 1
                                  FROM doprdocs_dbt odoc, doproper_dbt opr
                                 WHERE odoc.t_DocKind = 191
                                   AND TO_NUMBER(odoc.t_DocumentID) = bck.t_ID
                                   AND opr.t_ID_Operation = odoc.t_ID_Operation
                                   AND opr.t_DocKind = p_BalanceDocKind
                                   AND TO_NUMBER(opr.t_DocumentID) = p_BalanceDealID
                                )
                   ORDER BY bck.t_ID DESC)
           WHERE ROWNUM = 1;

        ELSE
          SELECT t_ID INTO v_Enrolment_ID
            FROM (SELECT bck.t_ID
                    FROM dvsbnrbck_dbt bck
                   WHERE bck.t_BCID = p_BCID
                     AND bck.t_ChangeDate = v_Date
                     AND bck.t_NewABCStatus = VABANNER_STATUS_ACCOUNT
                  ORDER BY bck.t_ID DESC)
           WHERE ROWNUM = 1;
        END IF;

        RETURN v_Enrolment_ID;

        EXCEPTION
          WHEN OTHERS THEN
            RETURN 0;

  END GetVABnrLastAccountedEnrolID;

  --Получить сумму учета по векселю в ВН
  FUNCTION GetVABnrAccountedCostPFI(p_BCID   IN dvsbanner_dbt.t_BCID%TYPE,
                                    p_OnDate IN dvsbanner_dbt.t_IssueDate%TYPE)
        RETURN NUMBER
  IS
        v_AccountedCost dvsincome_dbt.t_Perc%TYPE;
  BEGIN
        v_AccountedCost := 0;


        SELECT NVL(SUM(t_Perc), 0) INTO v_AccountedCost
          FROM dvsincome_dbt
         WHERE t_BCID = p_BCID
           AND t_IncomeType = VSINCOMETYPE_ACCOUNTEDSUM
           AND t_Enrolment_ID = GetVABnrLastAccountedEnrolID(p_BCID, p_OnDate)
           AND t_EndDate <= p_OnDate;

        RETURN v_AccountedCost;

        EXCEPTION
          WHEN OTHERS THEN
            RETURN 0;

  END GetVABnrAccountedCostPFI;


  --Получить сумму учета по векселю в ВУ
  FUNCTION GetVABnrAccountedCostAcc(p_BCID   IN dvsbanner_dbt.t_BCID%TYPE,
                                    p_OnDate IN dvsbanner_dbt.t_IssueDate%TYPE)
        RETURN NUMBER
  IS
        v_AccountedCost dvsincome_dbt.t_Perc%TYPE;
  BEGIN
        v_AccountedCost := 0;


        SELECT NVL(SUM(t_AccPerc),0) INTO v_AccountedCost
          FROM dvsincome_dbt
         WHERE t_BCID = p_BCID
           AND t_IncomeType = VSINCOMETYPE_ACCOUNTEDSUM
           AND t_Enrolment_ID = GetVABnrLastAccountedEnrolID(p_BCID, p_OnDate)
           AND t_EndDate <= p_OnDate;

        RETURN v_AccountedCost;

        EXCEPTION
          WHEN OTHERS THEN
            RETURN 0;

  END GetVABnrAccountedCostAcc;

  --Получить сумму начальной премии по векселю в ВН
  FUNCTION GetVABnrStartBonusPFI(p_BCID   IN dvsbanner_dbt.t_BCID%TYPE,
                                 p_OnDate IN dvsbanner_dbt.t_IssueDate%TYPE)
        RETURN NUMBER
  IS
        v_StartBonus dvsincome_dbt.t_Bonus%TYPE;
  BEGIN
        v_StartBonus := 0;

        SELECT t_Perc INTO v_StartBonus
          FROM (SELECT t_Perc
                  FROM dvsincome_dbt
                 WHERE t_BCID = p_BCID
                   AND t_IncomeType = VSINCOMETYPE_BONUSSUM
                   AND t_Enrolment_ID = GetVABnrLastAccountedEnrolID(p_BCID, p_OnDate)
                   AND t_EndDate <= p_OnDate
                 ORDER BY t_EndDate ASC)
         WHERE ROWNUM = 1;

        RETURN v_StartBonus;

        EXCEPTION
          WHEN OTHERS THEN
            RETURN 0;

  END GetVABnrStartBonusPFI;

  --Получить сумму начальной премии по векселю в ВУ
  FUNCTION GetVABnrStartBonusAcc(p_BCID   IN dvsbanner_dbt.t_BCID%TYPE,
                                 p_OnDate IN dvsbanner_dbt.t_IssueDate%TYPE)
        RETURN NUMBER
  IS
        v_StartBonus dvsincome_dbt.t_Bonus%TYPE;
  BEGIN
        v_StartBonus := 0;

        SELECT t_AccPerc INTO v_StartBonus
          FROM (SELECT t_AccPerc
                  FROM dvsincome_dbt
                 WHERE t_BCID = p_BCID
                   AND t_IncomeType = VSINCOMETYPE_BONUSSUM
                   AND t_Enrolment_ID = GetVABnrLastAccountedEnrolID(p_BCID, p_OnDate)
                   AND t_EndDate <= p_OnDate
                 ORDER BY t_EndDate ASC)
         WHERE ROWNUM = 1;

        RETURN v_StartBonus;

        EXCEPTION
          WHEN OTHERS THEN
            RETURN 0;

  END GetVABnrStartBonusAcc;

  --Получить остаток неначисленной премии по векселю на дату в ВН
  FUNCTION GetVABnrRestBonusPFI(p_BCID   IN dvsbanner_dbt.t_BCID%TYPE,
                                p_OnDate IN dvsbanner_dbt.t_IssueDate%TYPE)
        RETURN NUMBER
  IS
        v_RestBonus dvsincome_dbt.t_Bonus%TYPE;
  BEGIN
        v_RestBonus := 0;

        SELECT NVL(SUM(t_Perc),0) INTO v_RestBonus
          FROM dvsincome_dbt
         WHERE t_BCID = p_BCID
           AND t_IncomeType = VSINCOMETYPE_BONUSSUM
           AND t_Enrolment_ID = GetVABnrLastAccountedEnrolID(p_BCID, p_OnDate)
           AND t_EndDate <= p_OnDate;

        RETURN v_RestBonus;

        EXCEPTION
          WHEN OTHERS THEN
            RETURN 0;

  END GetVABnrRestBonusPFI;

  --Получить остаток неначисленной премии по векселю на дату в ВУ
  FUNCTION GetVABnrRestBonusAcc(p_BCID   IN dvsbanner_dbt.t_BCID%TYPE,
                                p_OnDate IN dvsbanner_dbt.t_IssueDate%TYPE)
        RETURN NUMBER
  IS
        v_RestBonus dvsincome_dbt.t_Bonus%TYPE;
  BEGIN
        v_RestBonus := 0;

        SELECT NVL(SUM(t_AccPerc),0) INTO v_RestBonus
          FROM dvsincome_dbt
         WHERE t_BCID = p_BCID
           AND t_IncomeType = VSINCOMETYPE_BONUSSUM
           AND t_Enrolment_ID = GetVABnrLastAccountedEnrolID(p_BCID, p_OnDate)
           AND t_EndDate <= p_OnDate;

        RETURN v_RestBonus;

        EXCEPTION
          WHEN OTHERS THEN
            RETURN 0;

  END GetVABnrRestBonusAcc;

  FUNCTION VSORDLNKSync(p_DocKind IN NUMBER, p_ContractID IN NUMBER) RETURN NUMBER
  AS
    v_Count NUMBER;

  BEGIN

    -- Кол-во записей, которые были конкурентно изменены в реальной базе
    SELECT Count(1) INTO v_Count
      FROM dvsordlnk_dbt real, dvsordlnk_tmp tmp
     WHERE real.t_BCID       = tmp.t_BCID_from
       AND real.t_LinkKind   = tmp.t_LinkKind_from
       AND real.t_DocKind    = tmp.t_DocKind_from
       AND real.t_ContractID = tmp.t_ContractID_from
       AND real.t_Start      = tmp.t_Start_from
       AND (   real.t_InterestChargeDate!= tmp.t_InterestChargeDate_from
            OR real.t_BCCost            != tmp.t_BCCost_from
            OR real.t_TaxBaseAmount     != tmp.t_TaxBaseAmount_from
            OR real.t_TaxAmount         != tmp.t_TaxAmount_from
            OR real.t_IsPartycular      != tmp.t_IsPartycular_from
            OR real.t_Yield             != tmp.t_Yield_from
            OR real.t_Scale             != tmp.t_Scale_from
            OR real.t_Point             != tmp.t_Point_from
            OR real.t_Issuer            != tmp.t_Issuer_from
            OR real.t_IssueDate         != tmp.t_IssueDate_from
            OR real.t_BCCFI             != tmp.t_BCCFI_from
            OR real.t_BCCostR           != tmp.t_BCCostR_from
            OR real.t_BCCostRPoint      != tmp.t_BCCostRPoint_from
            OR real.t_Expiry            != tmp.t_Expiry_from
           );

    --если конкурентных изменений нет, то обрабатываем записи в реальной базе
    IF v_Count = 0 THEN
      --1.Удаление
      DELETE FROM dvsordlnk_dbt real
       WHERE (real.t_BCID,real.t_LinkKind,real.t_DocKind,real.t_ContractID,real.t_Start) IN
             ( SELECT DISTINCT tmp.t_BCID_from,tmp.t_LinkKind_from,tmp.t_DocKind_from,tmp.t_ContractID_from,tmp.t_Start_from
                 FROM dvsordlnk_tmp tmp
                WHERE tmp.t_Action = VSORDLNKACTION_DELETE );

      --2.Обновление
      UPDATE dvsordlnk_dbt real
         SET (real.t_InterestChargeDate,
              real.t_BCCost,
              real.t_TaxBaseAmount,
              real.t_TaxAmount,
              real.t_IsPartycular,
              real.t_Yield,
              real.t_Scale,
              real.t_Point,
              real.t_Issuer,
              real.t_IssueDate,
              real.t_BCCFI,
              real.t_BCCostR,
              real.t_BCCostRPoint,
              real.t_Expiry
             ) =
             ( SELECT tmp.t_InterestChargeDate,
                      tmp.t_BCCost,
                      tmp.t_TaxBaseAmount,
                      tmp.t_TaxAmount,
                      tmp.t_IsPartycular,
                      tmp.t_Yield,
                      tmp.t_Scale,
                      tmp.t_Point,
                      tmp.t_Issuer,
                      tmp.t_IssueDate,
                      tmp.t_BCCFI,
                      tmp.t_BCCostR,
                      tmp.t_BCCostRPoint,
                      tmp.t_Expiry
                 FROM dvsordlnk_tmp tmp
                WHERE tmp.t_BCID_from       = real.t_BCID
                  AND tmp.t_LinkKind_from   = real.t_LinkKind
                  AND tmp.t_DocKind_from    = real.t_DocKind
                  AND tmp.t_ContractID_from = real.t_ContractID
                  AND tmp.t_Start_from      = real.t_Start
                  AND tmp.t_Action = VSORDLNKACTION_UPDATE )
       WHERE (real.t_BCID,real.t_LinkKind,real.t_DocKind,real.t_ContractID,real.t_Start)
          IN ( SELECT tmpa.t_BCID_from,tmpa.t_LinkKind_from,tmpa.t_DocKind_from,tmpa.t_ContractID_from,tmpa.t_Start_from
                 FROM dvsordlnk_tmp tmpa
                WHERE tmpa.t_Action = VSORDLNKACTION_UPDATE);

       --3.Вставка
       INSERT INTO dvsordlnk_dbt real
             (real.t_BCID,
              real.t_ContractID,
              real.t_LinkKind,
              real.t_InterestChargeDate,
              real.t_BCCost,
              real.t_TaxBaseAmount,
              real.t_TaxAmount,
              real.t_IsPartycular,
              real.t_Yield,
              real.t_Scale,
              real.t_Point,
              real.t_DocKind,
              real.t_Issuer,
              real.t_IssueDate,
              real.t_BCCFI,
              real.t_BCCostR,
              real.t_BCCostRPoint,
              real.t_Start,
              real.t_Expiry
             )
        SELECT tmp.t_BCID,
               tmp.t_ContractID,
               tmp.t_LinkKind,
               tmp.t_InterestChargeDate,
               tmp.t_BCCost,
               tmp.t_TaxBaseAmount,
               tmp.t_TaxAmount,
               tmp.t_IsPartycular,
               tmp.t_Yield,
               tmp.t_Scale,
               tmp.t_Point,
               tmp.t_DocKind,
               tmp.t_Issuer,
               tmp.t_IssueDate,
               tmp.t_BCCFI,
               tmp.t_BCCostR,
               tmp.t_BCCostRPoint,
               tmp.t_Start,
               tmp.t_Expiry
          FROM dvsordlnk_tmp tmp
         WHERE (tmp.t_Action = VSORDLNKACTION_INSERT) OR (tmp.t_Action = VSORDLNKACTION_UPDATE AND tmp.t_ContractID_from = 0);

    ELSE
      RETURN 1;
    END IF;

    RETURN 0;


  END VSORDLNKSync;

  --Получить процентную ставку по векселю на дату
  FUNCTION GetBnrRateOnDate(p_BCID IN NUMBER, p_OnDate IN DATE) RETURN NUMBER
  AS
    v_Rate dvsprchst_dbt.t_PercRate%TYPE;
  BEGIN

    v_Rate := 0.0;

    FOR one_row IN (SELECT q.PercRate, q.BegDate
                      FROM ( SELECT leg.t_Price as PercRate, leg.t_InterestStart as BegDate
                               FROM DDL_LEG_DBT leg
                              WHERE leg.t_DealID = p_BCID
                                AND leg.t_LegKind = 1
                                AND leg.t_LegID = 0
                                AND leg.t_InterestStart <= p_OnDate
                             UNION
                             SELECT hst.t_PercRate as PercRate, hst.t_BegDate as BegDate
                               FROM DVSPRCHST_DBT hst
                              WHERE hst.t_BCID = p_BCID
                                AND hst.t_BegDate <= p_OnDate
                           ) q
                     ORDER BY q.BegDate DESC
                   )
    LOOP
      v_Rate := one_row.PercRate;
      EXIT;
    END LOOP;

    RETURN v_Rate;

  END GetBnrRateOnDate;

  --Проверить, находится ли сделка на внебалансе (пока только для покупки и продажи)
  FUNCTION VADealIsOffBalance(p_DealID IN NUMBER) RETURN NUMBER
  AS

    v_IsOffBalance NUMBER := 0;

  BEGIN

     SELECT 1 INTO v_IsOffBalance
       FROM ddl_tick_dbt deal, doproper_dbt opr, doprkdate_dbt kdate1, doprkdate_dbt kdate2, doprdates_dbt odate1, doprdates_dbt odate2
      WHERE deal.t_DealID = p_DealID
        AND opr.t_DocKind = deal.t_BOfficeKind
        AND opr.t_DocumentID = LPAD(deal.t_DealID, 34, '0')
        AND kdate1.t_DocKind = opr.t_DocKind
        AND kdate1.t_NumberDate = 13
        AND odate1.t_ID_Operation = opr.t_ID_Operation
        AND odate1.t_DateKindID = kdate1.t_DateKindID
        AND odate1.t_Date <> TO_DATE('01.01.0001','DD.MM.YYYY')  --сделка поставлена на внебалансовый учет
        AND kdate2.t_DocKind = opr.t_DocKind
        AND kdate2.t_NumberDate = 14
        AND odate2.t_ID_Operation = opr.t_ID_Operation
        AND odate2.t_DateKindID = kdate2.t_DateKindID
        AND odate2.t_Date = TO_DATE('01.01.0001','DD.MM.YYYY');  --сделка не снята с внебалансового учета

    RETURN v_IsOffBalance;

    EXCEPTION
      WHEN OTHERS THEN
        RETURN 0;

  END VADealIsOffBalance;

  --Получить справедливую стоимость по векселю на дату
  FUNCTION GetVABnrFrValueOnDate(p_BCID IN NUMBER
                                ,p_ContractID IN NUMBER
                                ,p_OnDate IN DATE
                                ,p_Accounted IN CHAR)
    RETURN NUMBER
  AS
    v_FairValue dbnrfrval_dbt.t_FairValue%TYPE := 0;
    v_prevBCID NUMBER := 0;
  BEGIN
    v_FairValue := 0;

    FOR one_fr IN (SELECT
                     *
                   FROM
                     (SELECT
                        subq3.t_ContractID
                       ,subq3.t_BegDate
                       ,subq3.t_BCID
                       ,NVL((SELECT
                               frval.t_FairValue
                             FROM
                               dbnrfrval_dbt frval
                             WHERE
                               frval.t_BCID = subq3.t_BCID
                             AND frval.t_ContractID = subq3.t_ContractID
                             AND frval.t_BegDate = (SELECT
                                                      MAX(frval.t_BegDate)
                                                    FROM
                                                      dbnrfrval_dbt frval
                                                    WHERE
                                                      frval.t_BCID = subq3.t_BCID
                                                    AND frval.t_ContractID = subq3.t_ContractID
                                                    AND frval.t_BegDate <= subq3.t_BegDate))
                           ,0)
                          AS t_FairValue
                       ,(SELECT
                           MAX(frval.t_Accounted)
                         FROM
                           dbnrfrval_dbt frval
                         WHERE
                           frval.t_ContractID = subq3.t_ContractID
                         AND frval.t_BegDate = subq3.t_BegDate)
                          AS t_Accounted
                      FROM
                        (SELECT
                           DISTINCT subq1.t_ContractID, subq1.t_BegDate, subq2.t_BCID
                         FROM
                           (SELECT
                              frval.t_ContractID, frval.t_BegDate
                            FROM
                              dbnrfrval_dbt frval) subq1
                          ,(SELECT
                              frval.t_ContractID, frval.t_BCID
                            FROM
                              dbnrfrval_dbt frval) subq2
                         WHERE
                           subq1.t_ContractID = subq2.t_ContractID) subq3) subq4
                   WHERE
                     (p_BCID = 0
                   OR  subq4.t_BCID = p_BCID)
                   AND((p_ContractID = 0
                   AND  subq4.t_ContractID = GetVABnrBalanceDealID(subq4.t_BCID, p_OnDate))
                   OR   subq4.t_ContractID = p_ContractID)
                   AND subq4.t_BegDate <= p_OnDate
                   AND subq4.t_Accounted = p_Accounted
                   ORDER BY
                     subq4.t_BCID DESC, subq4.t_BegDate DESC)
    LOOP
      IF v_prevBCID <> one_fr.t_BCID
      THEN
        v_FairValue := v_FairValue + one_fr.t_FairValue;
      END IF;

      v_prevBCID := one_fr.t_BCID;
    END LOOP;

    RETURN v_FairValue;
  END GetVABnrFrValueOnDate;

  --Проверить, есть ли учтенные или неучтенные записи в истории СС
  FUNCTION ExistsVABnrFrValueOnDate(p_BCID IN NUMBER, p_ContractID IN NUMBER, p_OnDate IN DATE, p_Accounted IN CHAR) RETURN NUMBER
  AS

    v_cnt NUMBER;
  BEGIN

    v_cnt := 0;

    SELECT COUNT(1) INTO v_cnt
      FROM dbnrfrval_dbt
     WHERE (p_BCID = 0 OR t_BCID = p_BCID)
       AND ((p_ContractID = 0  AND t_ContractID = GetVABnrBalanceDealID(t_BCID, p_OnDate)) OR t_ContractID = p_ContractID)
       AND t_BegDate <= p_OnDate
       AND t_Accounted = p_Accounted;

    RETURN v_cnt;

  END ExistsVABnrFrValueOnDate;

  --Изменить учёт СС по векселю
  PROCEDURE ChangeAccountedForBnrFrVal(p_BCID IN NUMBER, p_ContractID IN NUMBER, p_BegDate IN DATE, p_Accounted IN CHAR)
  AS

  BEGIN

    UPDATE DBNRFRVAL_DBT
       SET T_ACCOUNTED = p_Accounted
     WHERE T_BCID = p_BCID
       AND T_CONTRACTID = p_ContractID
       AND T_BEGDATE = p_BegDate;

  END ChangeAccountedForBnrFrVal;

  --Получить изначальную отсроченную разницу
  FUNCTION GetVABnrStartDefDif(p_BCID   IN dvsbanner_dbt.t_BCID%TYPE,
                               p_OnDate IN dvsbanner_dbt.t_IssueDate%TYPE)
        RETURN NUMBER
  IS
        v_StartDefDif dvsincome_dbt.t_DefDif%TYPE;
  BEGIN
        v_StartDefDif := 0;

        SELECT t_Perc INTO v_StartDefDif
          FROM (SELECT t_Perc
                  FROM dvsincome_dbt
                 WHERE t_BCID = p_BCID
                   AND t_IncomeType = VSINCOMETYPE_DEFDIF
                   AND t_Enrolment_ID = GetVABnrLastAccountedEnrolID(p_BCID, p_OnDate)
                   AND t_EndDate <= p_OnDate
                 ORDER BY t_EndDate ASC)
         WHERE ROWNUM = 1;

        RETURN v_StartDefDif;

        EXCEPTION
          WHEN OTHERS THEN
            RETURN 0;

  END GetVABnrStartDefDif;

  --Получить корректировку ПДД по ЭПС
  FUNCTION GetVABnrAdjustmentEIR(p_BCID   IN dvsbanner_dbt.t_BCID%TYPE,
                                  p_OnDate IN dvsbanner_dbt.t_IssueDate%TYPE,
                                  p_АdjustmentEIR OUT NUMBER,
                                  p_AccАdjustmentEIR OUT NUMBER)
  RETURN INTEGER
  IS
  BEGIN

        SELECT sum(t_Perc), sum(t_AccPerc)INTO p_АdjustmentEIR, p_AccАdjustmentEIR
                  FROM dvsincome_dbt
                 WHERE t_BCID = p_BCID
                   AND t_IncomeType = VSINCOMETYPE_EPRPERC
                   AND t_Enrolment_ID = GetVABnrLastAccountedEnrolID(p_BCID, p_OnDate)
                   --AND t_EndDate <= p_OnDate
                 ORDER BY t_EndDate DESC;



        IF(p_АdjustmentEIR != 0 AND p_AccАdjustmentEIR != 0)THEN
            return 0;
        ELSE
            return 1;
        END IF;

        EXCEPTION
          WHEN OTHERS THEN
            RETURN 1;
  END GetVABnrAdjustmentEIR;

  --Получить начальный дисконт
  FUNCTION GetBNRFirstDiscount (
     p_BCID               IN     dvsbanner_dbt.t_BCID%TYPE,
     p_DealId             IN     NUMBER,
     p_OnDate             IN     dvsbanner_dbt.t_IssueDate%TYPE,
     p_FirstDiscount         OUT NUMBER,
     p_AccFirstDiscount      OUT NUMBER)
     RETURN INTEGER
  IS
     v_bnr        dvsbanner_dbt%ROWTYPE;
     v_leg        ddl_leg_dbt%ROWTYPE;
     v_lnk        dvsordlnk_dbt%ROWTYPE;
     v_Cost       dvsordlnk_dbt.T_BCCOST%TYPE;
     v_Enrol      NUMBER;
     v_PayFiid    NUMBER;
     v_isOur      BOOLEAN;
     v_linkKind   NUMBER;
     v_getDealId  NUMBER;
  BEGIN
     SELECT *
       INTO v_bnr
       FROM dvsbanner_dbt bnr
      WHERE bnr.t_BCID = p_BCID;

     v_isOur := IsOurBanner (v_bnr.t_Issuer);
     v_Enrol :=
        CASE
           WHEN v_isOur = FALSE
           THEN
              GetVABnrLastAccountedEnrolID (p_BCID, p_OnDate)
           ELSE
              GetVSBnrLastPayedEnrolID (p_BCID, p_OnDate)
        END;
     v_PayFiid :=
        CASE
           WHEN v_isOur = FALSE THEN GetVABnrPayFIID (p_BCID)
           ELSE GetVSBnrPayFIID (p_BCID)
        END;
     v_linkKind := CASE WHEN v_isOur = FALSE THEN 1 ELSE 0 END;

     BEGIN
        SELECT t_Perc, t_AccPerc
          INTO p_FirstDiscount, p_AccFirstDiscount
          FROM (  SELECT t_Perc, t_AccPerc
                    FROM dvsincome_dbt
                   WHERE     t_BCID = p_BCID
                         AND t_IncomeType = VSINCOMETYPE_FIRSTDISC
                         AND t_Enrolment_ID = v_Enrol
                ORDER BY t_EndDate DESC)
         WHERE ROWNUM = 1;
     EXCEPTION
        WHEN OTHERS
        THEN
           BEGIN
              SELECT *
                INTO v_leg
                FROM ddl_leg_dbt leg
               WHERE     leg.t_LegKind = 1
                     AND leg.t_DealID = p_BCID
                     AND t_LegID = 0;

              v_getDealId := CASE WHEN p_DealId <= 0 THEN (CASE WHEN v_isOur = FALSE THEN RSB_BILL.GetVANODealId(p_BCID,p_OnDate) ELSE RSB_BILL.GetVSNODealId(p_BCID,p_OnDate) END) ELSE p_DealId END;

              SELECT *
                INTO v_lnk
                FROM dvsordlnk_dbt lnk
               WHERE     LNK.T_CONTRACTID = v_getDealId
                     AND LNK.T_BCID = p_BCID
                     AND LNK.T_LINKKIND = v_linkKind;

              v_Cost :=
                 RSI_RSB_FIInstr.ConvSum (v_lnk.T_BCCOST,
                                          v_lnk.T_BCCFI,
                                          v_leg.t_PFI,
                                          GetBNRFirstDate (p_BCID, v_getDealId),
                                          2);
              p_AccFirstDiscount := 0;
              p_FirstDiscount := v_leg.t_Principal - v_Cost;
              p_FirstDiscount :=
                 CASE WHEN p_FirstDiscount < 0 THEN 0 ELSE p_FirstDiscount END;

              IF (p_FirstDiscount > 0)
              THEN
                 p_AccFirstDiscount :=
                    RSI_RSB_FIInstr.ConvSum (
                       p_FirstDiscount,
                       v_leg.t_PFI,
                       v_PayFiid,
                       GetBNRFirstDate (p_BCID, v_getDealId),
                       2);
              END IF;
           EXCEPTION
              WHEN OTHERS
              THEN
                 p_FirstDiscount := 0;
                 p_AccFirstDiscount := 0;
           END;
     END;

     p_FirstDiscount := CASE WHEN v_isOur = TRUE AND p_FirstDiscount < 0 THEN 0 ELSE p_FirstDiscount END;
     p_AccFirstDiscount := CASE WHEN v_isOur = TRUE AND p_AccFirstDiscount < 0 THEN 0 ELSE p_AccFirstDiscount END;

     RETURN 0;
  EXCEPTION
     WHEN OTHERS
     THEN
        p_FirstDiscount := 0;
        p_AccFirstDiscount := 0;
        RETURN 0;
  END GetBNRFirstDiscount;

  --Получить последовательность отчетных дат
  FUNCTION GetSeqRepDates(p_FirstDate IN DATE -- первая дата
                         ,p_LastDate IN DATE -- последняя дата
                         )
    RETURN ArrDates_t
  IS
     v_ArrDates ArrDates_t := ArrDates_t();
     v_RepDate DATE;
  BEGIN
     v_ArrDates.extend();
     v_ArrDates( v_ArrDates.last ) := p_FirstDate;

     v_RepDate := LAST_DAY(p_FirstDate);

     IF (v_RepDate > p_FirstDate) AND (v_RepDate < p_LastDate) THEN
        v_ArrDates.extend();
        v_ArrDates( v_ArrDates.last ) := v_RepDate;
     END IF;

     v_RepDate := LAST_DAY(v_RepDate + 1);

     WHILE (v_RepDate < p_LastDate) LOOP
        v_ArrDates.extend();
        v_ArrDates( v_ArrDates.last ) := v_RepDate;

        v_RepDate := LAST_DAY(v_RepDate + 1);
     END LOOP;

     IF p_LastDate > p_FirstDate THEN
        v_ArrDates.extend();
        v_ArrDates( v_ArrDates.last ) := p_LastDate;
     END IF;

     return v_ArrDates;

     EXCEPTION
       when OTHERS then return null;
  END GetSeqRepDates;

  --Проверка на существенность отклонения с параметром int
  FUNCTION GetEssentialDevInt(p_LevelEssential IN NUMBER,
                              p_Portfolio IN NUMBER,
                              p_CalcDate IN DATE,
                              p_DoCompare IN NUMBER,
                              p_ToCompare1 IN NUMBER,
                              p_ToCompare2 IN NUMBER,
                              p_ObjectKind IN NUMBER,
                              p_ObjectID IN NUMBER,
                              p_S0VA IN NUMBER, --Учтённая цена векселя для УВ
                              p_IsEssential OUT NUMBER,
                              p_RateKind OUT NUMBER,
                              p_RateVal OUT NUMBER
                             )
  RETURN NUMBER
  IS
    v_ReturnVal NUMBER;
    v_IsEssential BOOLEAN;
    v_AbsoluteValue NUMBER   := 0;
    v_RelativeValue NUMBER   := 0;
    v_FirstDate     DATE     := RSB_SECUR.UnknownDate;
    v_LastDate      DATE     := RSB_SECUR.UnknownDate;
    v_ArrDates      ArrDates_t;
    v_LegId         NUMBER;
    v_Issuer        NUMBER;
    v_MethodNameLM    VARCHAR2(30);
    v_MethodNameASEIR VARCHAR2(30);
    v_stat            NUMBER;
    v_AC_LINi         NUMBER   := 0;
    v_AC_EPSi         NUMBER   := 0;
    v_EPS             NUMBER   := -1;
    p_IsOurBanner     BOOLEAN;
    v_S0VA            NUMBER;
    v_BCID            NUMBER;
  BEGIN
    if ((p_LevelEssential = RSB_SECUR.LEVELESSENTIAL_AC) AND (p_ObjectKind = RSB_SECUR.DL_VSBANNER)) THEN
     IF RSB_SECUR.GetLevelEssential(p_LevelEssential,
                          p_Portfolio,
                          p_CalcDate,
                          v_AbsoluteValue,
                          v_RelativeValue
                         ) = 1 THEN
        return 1;
     END IF;
     SELECT leg.t_id, bnr.t_Issuer, bnr.t_BCID
       INTO v_LegId, v_Issuer, v_BCID
       FROM dficert_dbt ficert
            INNER JOIN ddl_leg_dbt leg ON leg.t_dealid = ficert.t_certid
            INNER JOIN dvsbanner_dbt bnr ON bnr.t_bcid = ficert.t_certid
      WHERE     ficert.t_avoirkind = AVOIRISSKIND_BILL
            AND leg.t_LegKind = LEG_KIND_VSBANNER
            AND leg.t_LegID = 0
            AND ficert.t_ficertid = p_ObjectID;
     p_IsOurBanner := IsOurBanner(v_Issuer);
     IF ((p_S0VA is NULL) AND (NOT p_IsOurBanner)) THEN
       v_S0VA := rsb_bill.GetVABnrCostPFI(v_BCID, p_CalcDate);
     ELSE
       v_S0VA := p_S0VA;
     END IF;
     p_IsEssential := 0;
     p_RateKind    := RSB_SECUR.RATE_KIND_UNDEF;
     p_RateVal     := 0;
     IF v_RelativeValue <> 0 THEN
        v_FirstDate := LAST_DAY(p_CalcDate);
        IF GetBnrPlanRepayDate (v_LegId, v_LastDate) != 0 THEN
           RETURN 1;
        END IF;

        IF v_FirstDate > v_LastDate THEN
           return 0;
        END IF;

        if p_IsOurBanner THEN
           v_MethodNameLM := 'CalcVSAS_LN';
           v_MethodNameASEIR := 'CalcVSAS_EIR';
        ELSE
           v_MethodNameLM := 'CalcVAAS_LN';
           v_MethodNameASEIR := 'CalcVAAS_EIR';
        END IF;

        v_stat := 0;

        -- Определяем последовательность предстоящих отчетных дат
        v_ArrDates := GetSeqRepDates(v_FirstDate, v_LastDate);

        IF v_ArrDates IS NOT EMPTY THEN
           -- Проверка на относительную разницу
           FOR i IN v_ArrDates.First .. v_ArrDates.Last LOOP
              -- АС ФИ рассчитанная линейным методом на i-тый период
              --v_AC_LINi := CalcAS_Line(v_CalcKind, p_ObjectID, 0, v_ArrDates(i));
              EXECUTE IMMEDIATE 'begin :1 := rsb_bill.'||v_MethodNameLM||'(:2,:3,:4); end; ' USING IN OUT v_stat, IN v_LegId, IN v_ArrDates(i), OUT v_AC_LINi;
              -- АС ФИ рассчитанная методом ЭПС на i-тый период
              IF p_IsOurBanner THEN
                v_stat := rsb_bill.CalcVSEIR(v_LegId,v_ArrDates(i),v_EPS);
              ELSE
                v_stat := rsb_bill.CalcVAEIR(v_LegId,v_ArrDates(i),p_S0VA,v_EPS);
              END IF;
              --v_AC_EPSi := CalcAS_EPS(v_CalcKind, p_ObjectID, 0, v_ArrDates(i), v_EPS);
              EXECUTE IMMEDIATE 'begin :1 := rsb_bill.'||v_MethodNameASEIR||'(:2,:3,:4,:5); end; ' USING IN OUT v_stat, IN v_LegId, IN v_EPS, IN v_ArrDates(i), OUT v_AC_EPSi;

              IF v_AC_EPSi = 0 OR v_stat != 0 THEN
                 return 1;
              END IF;

              IF ABS(v_AC_LINi - v_AC_EPSi) / v_AC_EPSi > v_RelativeValue THEN
                 p_IsEssential := 1;
                 return 0;
              END IF;
           END LOOP;

           -- Проверка на абсолютную разницу
           IF v_AbsoluteValue <> 0 THEN
              FOR i IN v_ArrDates.First .. v_ArrDates.Last LOOP
                 -- АС ФИ рассчитанная линейным методом на i-тый период
                 --v_AC_LINi := CalcAS_Line(v_CalcKind, p_ObjectID, 0, v_ArrDates(i));
                 EXECUTE IMMEDIATE 'begin :1 := rsb_bill.'||v_MethodNameLM||'(:2,:3,:4); end; ' USING IN OUT v_stat, IN v_LegId, IN v_ArrDates(i), OUT v_AC_LINi;
                 -- АС ФИ рассчитанная методом ЭПС на i-тый период
                 IF p_IsOurBanner THEN
                    v_stat := CalcVSEIR(v_LegId,v_ArrDates(i),v_EPS);
                 ELSE
                    v_stat := CalcVAEIR(v_LegId,v_ArrDates(i),p_S0VA,v_EPS);
                 END IF;
                 --v_AC_EPSi := CalcAS_EPS(v_CalcKind, p_ObjectID, 0, v_ArrDates(i), v_EPS);
                 EXECUTE IMMEDIATE 'begin :1 := rsb_bill.'||v_MethodNameASEIR||'(:2,:3,:4,:5); end; ' USING IN OUT v_stat, IN v_LegId, IN v_EPS, IN v_ArrDates(i), OUT v_AC_EPSi;

                 IF (v_stat != 0) THEN
                   return 0;
                 END IF;

                 IF ABS(v_AC_LINi - v_AC_EPSi) > v_AbsoluteValue THEN
                    p_IsEssential := 1;
                    return 0;
                 END IF;
              END LOOP;
           END IF;
        END IF;
     END IF;
    ELSE
      v_ReturnVal := RSB_SECUR.GetEssentialDev(p_LevelEssential,
                                               p_Portfolio,
                                               p_CalcDate,
                                               p_DoCompare,
                                               p_ToCompare1,
                                               p_ToCompare2,
                                               p_ObjectKind,
                                               p_ObjectID,
                                               v_IsEssential,
                                               p_RateKind,
                                               p_RateVal
                                              );
    END IF;
    p_IsEssential := CASE WHEN v_IsEssential THEN 1 ELSE 0 END;

    RETURN v_ReturnVal;

    EXCEPTION
         WHEN OTHERS THEN
         RETURN 1;

  END GetEssentialDevInt;

  --Получить дату оплаты векселя
  FUNCTION GetVSBnrLastPayedDate(p_BCID   IN dvsbanner_dbt.t_BCID%TYPE,
                                   p_OnDate IN dvsbanner_dbt.t_IssueDate%TYPE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY'))
        RETURN DATE
  IS
        v_Date DATE;
  BEGIN

        IF p_OnDate = TO_DATE('01.01.0001','DD.MM.YYYY') THEN
          SELECT MAX(t_ChangeDate) INTO v_Date
            FROM dvsbnrbck_dbt
           WHERE t_BCID = p_BCID
             AND t_NewABCStatus = VSBANNER_STATUS_PAYED;
        ELSE
          SELECT MAX(t_ChangeDate) INTO v_Date
            FROM dvsbnrbck_dbt
           WHERE t_BCID = p_BCID
             AND t_NewABCStatus = VSBANNER_STATUS_PAYED
             AND t_ChangeDate <= p_OnDate;
        END IF;

        RETURN v_Date;

        EXCEPTION
          WHEN OTHERS THEN
            RETURN TO_DATE('01.01.0001','DD.MM.YYYY');

  END GetVSBnrLastPayedDate;

  --ID записи в истории по векселю, в результате которой статус собственного векселя изменился на "Оплачен"
  FUNCTION GetVSBnrLastPayedEnrolID(p_BCID   IN dvsbanner_dbt.t_BCID%TYPE,
                                        p_OnDate IN dvsbanner_dbt.t_IssueDate%TYPE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY'),
                                        p_BalanceDealID IN NUMBER DEFAULT 0,
                                        p_BalanceDocKind IN NUMBER DEFAULT 0)
        RETURN NUMBER
  IS
        v_Enrolment_ID NUMBER(10);
        v_Date DATE;
  BEGIN
        v_Enrolment_ID := 0;

        v_Date := GetVSBnrLastPayedDate(p_BCID, p_OnDate);

        IF p_BalanceDealID <> 0 AND p_BalanceDocKind <> 0 THEN
          SELECT t_ID INTO v_Enrolment_ID
            FROM (SELECT bck.t_ID
                    FROM dvsbnrbck_dbt bck
                   WHERE bck.t_BCID = p_BCID
                     AND bck.t_ChangeDate = v_Date
                     AND bck.t_NewABCStatus = VSBANNER_STATUS_PAYED
                     AND EXISTS(SELECT 1
                                  FROM doprdocs_dbt odoc, doproper_dbt opr
                                 WHERE odoc.t_DocKind = 191
                                   AND TO_NUMBER(odoc.t_DocumentID) = bck.t_ID
                                   AND opr.t_ID_Operation = odoc.t_ID_Operation
                                   AND opr.t_DocKind = p_BalanceDocKind
                                   AND TO_NUMBER(opr.t_DocumentID) = p_BalanceDealID
                                )
                   ORDER BY bck.t_ID DESC)
           WHERE ROWNUM = 1;

        ELSE
          SELECT t_ID INTO v_Enrolment_ID
            FROM (SELECT bck.t_ID
                    FROM dvsbnrbck_dbt bck
                   WHERE bck.t_BCID = p_BCID
                     AND bck.t_ChangeDate = v_Date
                     AND bck.t_NewABCStatus = VSBANNER_STATUS_PAYED
                  ORDER BY bck.t_ID DESC)
           WHERE ROWNUM = 1;
        END IF;

        RETURN v_Enrolment_ID;

        EXCEPTION
          WHEN OTHERS THEN
            RETURN 0;

  END GetVSBnrLastPayedEnrolID;

  --Получить изначальную отсроченную разницу СВ
  FUNCTION GetVSBnrStartDefDif(p_BCID   IN dvsbanner_dbt.t_BCID%TYPE,
                               p_OnDate IN dvsbanner_dbt.t_IssueDate%TYPE)
        RETURN NUMBER
  IS
        v_StartDefDif dvsincome_dbt.t_DefDif%TYPE;
  BEGIN
        v_StartDefDif := 0;

        SELECT t_Perc INTO v_StartDefDif
          FROM (SELECT t_Perc
                  FROM dvsincome_dbt
                 WHERE t_BCID = p_BCID
                   AND t_IncomeType = VSINCOMETYPE_DEFDIF
                   AND t_Enrolment_ID = GetVSBnrLastPayedEnrolID(p_BCID, p_OnDate)
                   AND t_EndDate <= p_OnDate
                 ORDER BY t_EndDate ASC)
         WHERE ROWNUM = 1;

        RETURN v_StartDefDif;

        EXCEPTION
          WHEN OTHERS THEN
            RETURN 0;

  END GetVSBnrStartDefDif;

  --Получить не отнесённую отсроченную разницу СВ
  FUNCTION GetVSBnrNotWrittenDefDif(p_BCID   IN dvsbanner_dbt.t_BCID%TYPE,
                               p_OnDate IN dvsbanner_dbt.t_IssueDate%TYPE)
        RETURN NUMBER
  IS
        v_DefDif dvsincome_dbt.t_DefDif%TYPE;
  BEGIN
        v_DefDif := 0;

        SELECT NVL(sum(t_DefDif),0) INTO v_DefDif
                  FROM dvsincome_dbt
                 WHERE t_BCID = p_BCID
                   AND (t_IncomeType = VSINCOMETYPE_CHARGE OR t_IncomeType = VSINCOMETYPE_OVERVALUE)
                   AND t_Enrolment_ID = GetVSBnrLastPayedEnrolID(p_BCID, p_OnDate)
                   AND t_EndDate <= p_OnDate;

        v_DefDif := GetVSBnrStartDefDif(p_BCID,p_OnDate) - v_DefDif;

        RETURN v_DefDif;

        EXCEPTION
          WHEN OTHERS THEN
            RETURN 0;

  END GetVSBnrNotWrittenDefDif;

   --Получить корректировку ПДД по ЭПС для СВ
   FUNCTION GetVSBnrAdjustmentEIR (
      p_BCID                IN     dvsbanner_dbt.t_BCID%TYPE,
      p_OnDate              IN     dvsbanner_dbt.t_IssueDate%TYPE,
      p_АdjustmentEIR         OUT NUMBER,
      p_AccАdjustmentEIR      OUT NUMBER)
      RETURN INTEGER
   IS
   BEGIN
      SELECT NVL (SUM (t_Perc), 0), NVL (SUM (t_AccPerc), 0)
        INTO p_АdjustmentEIR, p_AccАdjustmentEIR
        FROM dvsincome_dbt
       WHERE     t_BCID = p_BCID
             AND t_IncomeType = VSINCOMETYPE_EPRPERC
             AND t_Enrolment_ID = GetVSBnrLastPayedEnrolID (p_BCID, p_OnDate)
             /*AND t_EndDate <= p_OnDate*/;

      RETURN 0;
   EXCEPTION
      WHEN OTHERS
      THEN
         RETURN 1;
   END GetVSBnrAdjustmentEIR;

     FUNCTION GetVSStartDate (p_LegId IN NUMBER, p_StartDate OUT DATE)
      RETURN INTEGER
   IS
      v_stat       INT := 0;
      v_DealId     NUMBER := 0;
      v_sStart     DATE;
      v_Start      DATE;
      v_CountBuf   INT := 0;
      v_Issuer     NUMBER;
   BEGIN
      SELECT t_DealId, t_Start
        INTO v_DealId, v_sStart
        FROM DDL_LEG_DBT
       WHERE t_Id = p_LegId;

      SELECT t_Issuer
        INTO v_Issuer
        FROM DVSBANNER_DBT
       WHERE t_BCID = v_DealId;

      SELECT COUNT (*)
        INTO v_CountBuf
        FROM ddp_dep_dbt
       WHERE t_PartyID = v_Issuer;

      IF (v_CountBuf = 0)
      THEN
         v_stat := 1;
      END IF;

      BEGIN
         SELECT lnk.t_InterestChargeDate
           INTO v_Start
           FROM dvsordlnk_dbt lnk,
                doproper_dbt op,
                doprdocs_dbt opd,
                (  SELECT LPAD (hist.t_ID, 10, '0') t_ID
                     FROM dvsbnrbck_dbt hist
                    WHERE     hist.t_BCID = v_DealId
                          AND hist.t_BCStatus = 'X'
                          AND hist.t_NewABCStatus = VSBANNER_STATUS_SENDED
                 ORDER BY hist.t_ChangeDate DESC, hist.t_ID DESC) tmp
          WHERE     ROWNUM = 1
                AND opd.t_DOCKind = DL_VSBNRBCK
                AND opd.t_DocumentID = tmp.t_ID
                AND op.t_ID_Operation = opd.t_ID_Operation
                AND op.t_DocKind IN (DL_VSSALE, DL_VSBARTERORDER)
                AND LPAD (lnk.t_contractid, 10, '0') = op.t_DocumentID
                AND lnk.t_DocKind = op.t_DocKind
                AND lnk.t_BCID = v_DealId;
      EXCEPTION
         WHEN OTHERS
         THEN
            v_Start := NULL;
      END;

      IF v_Start IS NULL
      THEN
         v_Start := v_sStart;
      END IF;

      p_StartDate := v_Start;

      RETURN v_stat;
   EXCEPTION
      WHEN OTHERS
      THEN
         RETURN 1;
   END GetVSStartDate;

   FUNCTION GetDaysInYearByBasis (p_Basis       IN     NUMBER,
                                  p_dateC       IN     DATE,
                                  p_asPeriod    IN     BOOLEAN,
                                  p_DayInYear      OUT NUMBER)
      RETURN INTEGER
   IS
      v_N      NUMBER := 360;
      v_stat   INT := 0;
   BEGIN
      IF p_Basis = BASIS_ACT_ACT
      THEN
         IF p_asPeriod
         THEN
           v_N := add_months(p_dateC, 12) - p_dateC;
         ELSE
           v_n := trunc(add_months(p_dateC, 12), 'year') - trunc(p_dateC, 'year');
         END IF;
      ELSIF p_Basis = BASIS_365_ACT
      THEN
         v_N := 365;
      END IF;

      p_DayInYear := v_N;

      RETURN v_stat;
   EXCEPTION
      WHEN OTHERS
      THEN
         RETURN 1;
   END GetDaysInYearByBasis;


   FUNCTION GetBnrPlanRepayDate (p_LegId IN NUMBER, p_PlanRepayDate OUT DATE)
      RETURN INTEGER
   IS
      v_BCID            NUMBER;
      v_BCTermFormula   NUMBER;
      v_BCFormKind      NUMBER;
      v_Start           DATE;
      v_Maturity        DATE;
      v_Expiry          DATE;
      v_Basis           NUMBER;
      v_Diff            NUMBER;
      v_Issuer          NUMBER;
      v_PlanRepayDate   DATE;
      v_DaysInYear      NUMBER;
      v_PlusOneYearVA   BOOLEAN := Rsb_Common.GetRegBoolValue('УЧТЕННЫЕ ВЕКСЕЛЯ\МСФО\СРОК_ВЕКСЕЛЯ+1ГОД', 0);
      v_PlusOneYearVS   BOOLEAN := Rsb_Common.GetRegBoolValue('ВЕКСЕЛЯ БАНКА\МСФО\СРОК_ВЕКСЕЛЯ+1ГОД', 0);
      v_PlusOneYear     BOOLEAN;
      v_stat            INT := 0;
   BEGIN
      SELECT t_Start,
             t_Maturity,
             t_Expiry,
             t_Basis,
             t_Diff,
             t_DealId
        INTO v_Start,
             v_Maturity,
             v_Expiry,
             v_Basis,
             v_Diff,
             v_BCID
        FROM DDL_LEG_DBT
       WHERE t_ID = p_LegId;

      SELECT t_BCTermFormula, t_BCFormKind, t_Issuer
        INTO v_BCTermFormula, v_BCFormKind, v_Issuer
        FROM DVSBANNER_DBT
       WHERE t_BCID = v_BCID;

       v_PlusOneYear := CASE WHEN IsOurBanner(v_Issuer) = TRUE THEN v_PlusOneYearVS ELSE v_PlusOneYearVA END;

       v_PlusOneYear := CASE WHEN v_PlusOneYear IS NULL THEN TRUE ELSE v_PlusOneYear END;

      IF (v_BCFormKind = VSBANNER_FORMKIND_SIMPLE)
      THEN
         IF (v_BCTermFormula = VS_TERMF_FIXEDDAY)
         THEN
            v_PlanRepayDate := v_Maturity;
         ELSIF (v_BCTermFormula = VS_TERMF_INATIME)
         THEN
            v_PlanRepayDate := v_Maturity;
         ELSIF (v_BCTermFormula = VS_TERMF_DURING)
         THEN
            v_stat :=
               GetDaysInYearByBasis (v_Basis,
                                     v_Start,
                                     TRUE,
                                     v_DaysInYear);
            v_PlanRepayDate := v_Start + v_Diff + CASE WHEN v_PlusOneYear THEN v_DaysInYear ELSE 0 END;
         ELSIF (v_BCTermFormula = VS_TERMF_ATSIGHT)
         THEN
            IF ( (v_Maturity >= v_Start) AND (v_Expiry >= v_Start))
            THEN
               v_PlanRepayDate := v_Expiry;
            ELSIF (v_Maturity >= v_Start)
            THEN
               v_stat :=
                  GetDaysInYearByBasis (v_Basis,
                                        v_Maturity,
                                        TRUE,
                                        v_DaysInYear);
               v_PlanRepayDate := v_Maturity + CASE WHEN v_PlusOneYear THEN v_DaysInYear ELSE 0 END;
            ELSIF (v_Expiry >= v_Start)
            THEN
               v_PlanRepayDate := v_Expiry;
            ELSE
               v_stat :=
                  GetDaysInYearByBasis (v_Basis,
                                        v_Start,
                                        TRUE,
                                        v_DaysInYear);
               v_PlanRepayDate := v_Start + CASE WHEN v_PlusOneYear THEN v_DaysInYear ELSE 0 END;
            END IF;
         END IF;
      ELSE
         v_PlanRepayDate := v_Maturity;
      END IF;

      p_PlanRepayDate := v_PlanRepayDate;

      RETURN v_stat;
   EXCEPTION
      WHEN OTHERS
      THEN
         RETURN 1;
   END GetBnrPlanRepayDate;

   FUNCTION GetBnrNOPlanRepayDate (p_LegId IN NUMBER)
      RETURN DATE
   IS
      v_stat            INT := 0;
      v_ret             DATE := TO_DATE('01.01.0001','DD.MM.YYYY');
   BEGIN
      v_stat := GetBnrPlanRepayDate(p_LegId,v_ret);
      RETURN v_ret;
   EXCEPTION
      WHEN OTHERS
      THEN
         RETURN v_ret;
   END GetBnrNOPlanRepayDate;

   FUNCTION CalcEIRInternal (p_d0      IN     DATE,
                             p_dn      IN     DATE,
                             p_S0      IN     NUMBER,
                             p_N       IN     NUMBER,
                             p_C       IN     NUMBER,
                             p_Basis   IN     NUMBER,
                             p_dc      IN     DATE,
                             p_B       IN     NUMBER,
                             p_EIR        OUT NUMBER)
      RETURN INTEGER
   IS
      v_Sn   NUMBER (32, 12);
   BEGIN
      v_Sn := p_N + (p_N * p_C * (p_dn - p_dc) / p_B);

      delete from DXIRR_TMP;

      INSERT INTO DXIRR_TMP (T_DATE, T_SUM)
           VALUES (p_d0, p_S0);

      INSERT INTO DXIRR_TMP (T_DATE, T_SUM)
           VALUES (p_dn, v_Sn);

      p_EIR := RSB_SECUR.CalcEPSNoFill ();

      RETURN 0;
   EXCEPTION
      WHEN OTHERS
      THEN
         RETURN 1;
   END CalcEIRInternal;

   FUNCTION CalcVSEIR (p_LegId IN NUMBER, p_d0 in DATE, p_EIR OUT NUMBER)
      RETURN INTEGER
   IS
      v_BCID    NUMBER;
      v_d0      DATE;
      v_dn      DATE;
      v_S0      NUMBER (32, 12);
      v_N       NUMBER (32, 12);
      v_C       NUMBER (32, 12);
      v_Basis   NUMBER;
      v_dc      DATE;
      v_B       NUMBER;
      v_stat    INT := 0;
   BEGIN
      SELECT t_DealId
        INTO v_BCID
        FROM DDL_LEG_DBT
       WHERE t_ID = p_LegId;

      if (p_d0 is null) then
        v_d0 := GetVSBnrLastPayedDate (v_BCID);
      else
        v_d0 := p_d0;
      end if;

      v_stat := GetBnrPlanRepayDate (p_LegId, v_dn);

      IF (v_stat = 0)
      THEN
         SELECT T_RECEIPTAMOUNT,
                t_Basis,
                T_INTERESTSTART,
                T_PRINCIPAL,
                T_PRICE / 1000000
           INTO v_S0,
                v_Basis,
                v_dc,
                v_N,
                v_C
           FROM DDL_LEG_DBT
          WHERE t_ID = p_LegId;

         v_S0 := v_S0 * -1;
         v_stat :=
            GetDaysInYearByBasis (v_Basis,
                                  v_dc,
                                  TRUE,
                                  v_B);

         IF (v_stat = 0)
         THEN
            v_stat :=
               CalcEIRInternal (v_d0,
                                v_dn,
                                v_S0,
                                v_N,
                                v_C,
                                v_Basis,
                                v_dc,
                                v_B,
                                p_EIR);
         END IF;
      END IF;

      RETURN v_stat;
   EXCEPTION
      WHEN OTHERS
      THEN
         RETURN 1;
   END CalcVSEIR;

   -- Расчет АС по ЭПС СВ в валюте номинала
   FUNCTION CalcAS_EIRInternal (p_d0         IN     DATE,
                                p_dn         IN     DATE,
                                p_S0         IN     NUMBER,
                                p_N          IN     NUMBER,
                                p_C          IN     NUMBER,
                                p_Basis      IN     NUMBER,
                                p_dc         IN     DATE,
                                p_B          IN     NUMBER,
                                p_CalcDate   IN     DATE,
                                p_EIR        IN     NUMBER,
                                p_ASEIR         OUT NUMBER)
      RETURN INTEGER
   IS
      v_Sn         NUMBER (32, 12) := 0;
      v_asval      NUMBER := 0;
      v_accuracy   INTEGER
         := ABS (
               RSB_COMMON.GetRegIntValue (
                  'SECUR\МСФО\ЭПС_ТОЧНОСТЬ_РЕЗУЛЬТАТА'));
   BEGIN
      v_Sn := p_N + (p_N * p_C * (p_dn - p_dc) / p_B);

      delete from DXIRR_TMP;

      INSERT INTO DXIRR_TMP (T_DATE, T_SUM)
           VALUES (p_d0, p_S0);

      INSERT INTO DXIRR_TMP (T_DATE, T_SUM)
           VALUES (p_dn, v_Sn);

      IF (p_EIR != -1)
      THEN
         WITH t AS
         (
           SELECT t_date AS dt, t_sum AS summ FROM DXIRR_TMP WHERE t_date >= p_CalcDate ORDER BY t_date
         )
         SELECT DECODE(SUM(rate), NULL, 0, SUM(rate)) INTO v_asval
           FROM (SELECT *
                   FROM t
                  MODEL
           DIMENSION BY (ROW_NUMBER() OVER (ORDER BY dt) rn)
               MEASURES (dt, summ s, 0 rate)
                  RULES (rate[ANY] = s[CV()] / POWER(1 + p_EIR/100, (dt[CV()] - p_CalcDate)/365))
                );
      END IF;

      p_ASEIR := ROUND (v_asval, v_accuracy);
      RETURN 0;
   EXCEPTION
      WHEN OTHERS
      THEN
         RETURN 1;
   END CalcAS_EIRInternal;

   -- Расчет АС по ЭПС СВ в валюте номинала
   FUNCTION CalcVSAS_EIR (p_LegId      IN     NUMBER,
                          p_EIR        IN     NUMBER,
                          p_CalcDate   IN     DATE,
                          p_ASEIR         OUT NUMBER)
      RETURN INTEGER
   IS
      v_BCID    NUMBER;
      v_d0      DATE;
      v_dn      DATE;
      v_S0      NUMBER (32, 12) := 0;
      v_N       NUMBER (32, 12);
      v_C       NUMBER (32, 12);
      v_Basis   NUMBER;
      v_dc      DATE;
      v_B       NUMBER;
      v_stat    INTEGER := 0;
   BEGIN
      SELECT t_DealId
        INTO v_BCID
        FROM DDL_LEG_DBT
       WHERE t_ID = p_LegId;

      v_d0 := p_CalcDate;
      v_stat := GetBnrPlanRepayDate (p_LegId, v_dn);

      IF (v_stat = 0)
      THEN
         SELECT t_Basis,
                T_INTERESTSTART,
                T_PRINCIPAL,
                T_PRICE / 1000000
           INTO v_Basis,
                v_dc,
                v_N,
                v_C
           FROM DDL_LEG_DBT
          WHERE t_ID = p_LegId;

         v_S0 := 0;
         v_stat :=
            GetDaysInYearByBasis (v_Basis,
                                  v_dc,
                                  TRUE,
                                  v_B);

         IF (v_stat = 0)
         THEN
            v_stat :=
               CalcAS_EIRInternal (v_d0,
                                   v_dn,
                                   v_S0,
                                   v_N,
                                   v_C,
                                   v_Basis,
                                   v_dc,
                                   v_B,
                                   v_d0,
                                   p_EIR,
                                   p_ASEIR);
         END IF;
      END IF;

      RETURN v_stat;
   END CalcVSAS_EIR;

   -- Расчет корректировки % до ЭПС

   FUNCTION CalcVSPersentEIR (p_LegId               IN     NUMBER,
                              p_CalcDate            IN     DATE,
                              p_InterestIncomeAdd   IN     NUMBER,
                              p_BonusAdd            IN     NUMBER,
                              p_DiscountIncomeAdd   IN     NUMBER,
                              p_FairValue           IN     NUMBER,
                              p_EIR                 IN     NUMBER,
                              p_Ret                    OUT NUMBER)
      RETURN INTEGER
   IS
      v_BCID           NUMBER;
      v_stat           INTEGER := 0;
      v_ASt            NUMBER := 0;
      v_ASt_1          NUMBER := 0;
      v_FirstDate      DATE;
      v_PrevCalcDate   DATE := NULL;
      v_Ret            NUMBER := 0;
   BEGIN
      SELECT t_DealId
        INTO v_BCID
        FROM DDL_LEG_DBT
       WHERE t_ID = p_LegId;

      -- Рассчитываем АС ЭПС на дату t
      v_stat :=
         CalcVSAS_EIR (p_LegId,
                       p_EIR,
                       p_CalcDate,
                       v_ASt);

      IF (v_stat = 0)
      THEN
         v_FirstDate := GetVSBnrLastPayedDate (v_BCID);

         BEGIN
            SELECT MAX (T_OPERDATE)
              INTO v_PrevCalcDate
              FROM DVSINCOME_DBT
             WHERE     T_INCOMETYPE = VSINCOMETYPE_EPRPERC
                   AND T_ENROLMENT_ID =
                          GetVSBnrLastPayedEnrolID (v_BCID, p_CalcDate);
         EXCEPTION
            WHEN OTHERS
            THEN
               v_PrevCalcDate := NULL;
         END;

         IF (v_PrevCalcDate IS NULL)
         THEN
            v_ASt_1 := p_FairValue;
         ELSE
            v_stat :=
               CalcVSAS_EIR (p_LegId,
                             p_EIR,
                             v_PrevCalcDate,
                             v_ASt_1);
         END IF;

         -- Рассчитываем значение Корректировка%_ЭПС
         IF (v_stat = 0)
         THEN
            p_Ret :=
                 (v_ASt - v_ASt_1)
               - p_InterestIncomeAdd
               - p_DiscountIncomeAdd
               + p_BonusAdd;
         END IF;
      END IF;

      RETURN v_stat;
   END CalcVSPersentEIR;

   FUNCTION CalcVAEIR (p_LegId IN NUMBER, p_d0 in DATE, p_S0 in NUMBER, p_EIR OUT NUMBER)
      RETURN INTEGER
   IS
      v_BCID    NUMBER;
      v_d0      DATE;
      v_dn      DATE;
      v_S0      NUMBER (32, 12);
      v_S0cur   NUMBER;
      v_PFI     NUMBER;
      v_Sn      NUMBER (32, 12);
      v_N       NUMBER (32, 12);
      v_C       NUMBER (32, 12);
      v_Basis   NUMBER;
      v_dc      DATE;
      v_B       NUMBER;
      v_stat    INT := 0;
   BEGIN
      SELECT t_DealId, T_PFI
        INTO v_BCID, v_PFI
        FROM DDL_LEG_DBT
       WHERE t_ID = p_LegId;

      if ((p_d0 is NULL) AND (p_S0 is NULL)) then
        v_d0 := GetVABnrLastBalanceDate (v_BCID);
        v_S0 := GetVABnrCostPFI(v_BCID, v_d0);
      else
        v_d0 := p_d0;
        v_S0 := p_S0;
      end if;

      v_stat := GetBnrPlanRepayDate (p_LegId, v_dn);

      IF (v_stat = 0)
      THEN
         SELECT t_Basis,
                T_INTERESTSTART,
                T_PRINCIPAL,
                T_PRICE / 1000000
           INTO v_Basis,
                v_dc,
                v_N,
                v_C
           FROM DDL_LEG_DBT
          WHERE t_ID = p_LegId;

         v_S0 := v_S0 * -1;
         v_stat :=
            GetDaysInYearByBasis (v_Basis,
                                  v_dc,
                                  TRUE,
                                  v_B);

         IF (v_stat = 0)
         THEN
            v_stat :=
               CalcEIRInternal (v_d0,
                                v_dn,
                                v_S0,
                                v_N,
                                v_C,
                                v_Basis,
                                v_dc,
                                v_B,
                                p_EIR);
         END IF;
      END IF;

      RETURN v_stat;
   EXCEPTION
      WHEN OTHERS
      THEN
         RETURN 1;
   END CalcVAEIR;

   -- Расчет АС по ЭПС УВ в валюте номинала

   FUNCTION CalcVAAS_EIR (p_LegId      IN     NUMBER,
                          p_EIR        IN     NUMBER,
                          p_CalcDate   IN     DATE,
                          p_ASEIR         OUT NUMBER)
      RETURN INTEGER
   IS
      v_BCID    NUMBER;
      v_d0      DATE;
      v_dn      DATE;
      v_S0      NUMBER (32, 12) := 0;
      v_N       NUMBER (32, 12);
      v_C       NUMBER (32, 12);
      v_Basis   NUMBER;
      v_dc      DATE;
      v_B       NUMBER;
      v_stat    INTEGER := 0;
   BEGIN
      SELECT t_DealId
        INTO v_BCID
        FROM DDL_LEG_DBT
       WHERE t_ID = p_LegId;

      v_d0 := p_CalcDate;
      v_stat := GetBnrPlanRepayDate (p_LegId, v_dn);

      IF (v_stat = 0)
      THEN
         SELECT t_Basis,
                T_INTERESTSTART,
                T_PRINCIPAL,
                T_PRICE / 1000000
           INTO v_Basis,
                v_dc,
                v_N,
                v_C
           FROM DDL_LEG_DBT
          WHERE t_ID = p_LegId;

         v_S0 := 0;
         v_stat :=
            GetDaysInYearByBasis (v_Basis,
                                  v_dc,
                                  TRUE,
                                  v_B);

         IF (v_stat = 0)
         THEN
            v_stat :=
               CalcAS_EIRInternal (v_d0,
                                   v_dn,
                                   v_S0,
                                   v_N,
                                   v_C,
                                   v_Basis,
                                   v_dc,
                                   v_B,
                                   v_d0,
                                   p_EIR,
                                   p_ASEIR);
         END IF;
      END IF;

      RETURN v_stat;
   END CalcVAAS_EIR;

   -- Расчет корректировки % до ЭПС

   FUNCTION CalcVAPersentEIR (p_LegId               IN     NUMBER,
                              p_CalcDate            IN     DATE,
                              p_InterestIncomeAdd   IN     NUMBER,
                              p_BonusAdd            IN     NUMBER,
                              p_DiscountIncomeAdd   IN     NUMBER,
                              p_FairValue           IN     NUMBER,
                              p_EIR                 IN     NUMBER,
                              p_Ret                    OUT NUMBER)
      RETURN INTEGER
   IS
      v_BCID           NUMBER;
      v_stat           INTEGER := 0;
      v_ASt            NUMBER := 0;
      v_ASt_1          NUMBER := 0;
      v_FirstDate      DATE;
      v_Ret            NUMBER := 0;
      v_PrevCalcDate   DATE := NULL;
   BEGIN
      SELECT t_DealId
        INTO v_BCID
        FROM DDL_LEG_DBT
       WHERE t_ID = p_LegId;

      -- Рассчитываем АС ЭПС на дату t
      v_stat :=
         CalcVAAS_EIR (p_LegId,
                       p_EIR,
                       p_CalcDate,
                       v_ASt);

      IF (v_stat = 0)
      THEN
         v_FirstDate := GetVABnrLastBalanceDate (v_BCID);

         BEGIN
            SELECT MAX (T_OPERDATE)
              INTO v_PrevCalcDate
              FROM DVSINCOME_DBT
             WHERE     T_INCOMETYPE = VSINCOMETYPE_EPRPERC
                   AND T_ENROLMENT_ID =
                          GetVABnrLastAccountedEnrolID (v_BCID, p_CalcDate);
         EXCEPTION
            WHEN OTHERS
            THEN
               v_PrevCalcDate := NULL;
         END;

         IF (v_PrevCalcDate IS NULL)
         THEN
            v_ASt_1 := p_FairValue;
         ELSE
            v_stat :=
               CalcVAAS_EIR (p_LegId,
                             p_EIR,
                             v_PrevCalcDate,
                             v_ASt_1);
         END IF;

         -- Рассчитываем значение Корректировка%_ЭПС
         IF (v_stat = 0)
         THEN
            p_Ret :=
                 (v_ASt - v_ASt_1)
               - p_InterestIncomeAdd
               - p_DiscountIncomeAdd
               + p_BonusAdd;
         END IF;
      END IF;

      RETURN v_stat;
   END CalcVAPersentEIR;

Function GetContractID_MaxDateBnr(BCID IN dvsbanner_dbt.t_BCID%TYPE)
  return ddl_order_dbt.t_ContractID%TYPE
  IS
        retval ddl_order_dbt.t_ContractID%TYPE;
  BEGIN
          select MAX(ord1.t_ContractId)
      into retval
      from dvsordlnk_dbt lnk1, ddl_order_dbt ord1
      where lnk1.t_BCID=BCID AND
            lnk1.T_LINKKIND in (0,1) AND
            ord1.t_ContractID=lnk1.t_ContractID AND
            ord1.t_SignDate= (select Max(ord.t_SignDate)
                                 from dvsordlnk_dbt lnk, ddl_order_dbt ord
                                where lnk.t_BCID = BCID
                                  and lnk.T_LINKKIND in (0,1)
                                  and ord.t_ContractID = lnk.t_ContractID
                                  and ord.t_SignDate > To_Date('01.01.0001','dd.mm.yyyy'));

      return retval;
  END GetContractID_MaxDateBnr;
  -- Получить последний день векселя по состоянию СВ и не больше даты операции

  FUNCTION GetLastDateStateVS(p_BCID    IN NUMBER,
                              p_State   IN VARCHAR2,
                              p_Date    IN DATE)
      RETURN DATE
  IS
      LastDate  DATE;
  BEGIN
      SELECT MAX(vsbnrbck.t_changedate)
      INTO LastDate
      FROM dvsbnrbck_dbt vsbnrbck
      WHERE vsbnrbck.t_BCID = p_BCID AND
            INSTR(vsbnrbck.t_NEWBCSTATE, p_State) != 0 AND
            vsbnrbck.t_changedate <= p_DATE;

      RETURN LastDate;
  END GetLastDateStateVS;

   -- Получить дисконт на дату УВ
   FUNCTION GetDiscountOnDateVA (p_LegId          IN     NUMBER,
                                 p_DealId         IN     NUMBER,
                                 p_CalcDate       IN     DATE,
                                 p_CalcDiscount      OUT NUMBER)
      RETURN NUMBER
   IS
      v_CalcDiscount         NUMBER (32, 12) := 0;
      v_RepayDate            DATE;
      v_SartDate             DATE;
      v_StartDiscount        NUMBER (32, 12);
      v_StartDiscountAcc     NUMBER (32, 12);
      v_P                    NUMBER;
      v_T                    NUMBER;
      v_BCID                 NUMBER;
      v_IncomeDateType       INTEGER
         := RSB_COMMON.GetRegIntValue (
               'COMMON\ДАТА НАЧАЛА НАЧИСЛЕНИЙ');
      v_DiscKind             INTEGER
         := RSB_COMMON.GetRegIntValue (
               'ДОВЕРИТЕЛЬНОЕ УПРАВЛЕНИЕ\РАБОТА С НЕЭМИССИОННЫМИ ЦБ\НАЧИСЛЕНИЕ ДИСКОНТА');
      v_BCTermFormula        INTEGER;
      v_BackOffice           CHAR;
      v_BCPresentationDate   DATE;
   BEGIN
      p_CalcDiscount := 0;

      SELECT leg.t_DealID,
             bnr.t_BCTermFormula,
             bnr.t_BackOffice,
             bnr.t_BCPresentationDate
        INTO v_BCID,
             v_BCTermFormula,
             v_BackOffice,
             v_BCPresentationDate
        FROM DDL_LEG_DBT leg, DVSBANNER_DBT bnr
       WHERE leg.t_ID = p_LegId AND bnr.t_BCID = leg.t_DealID;

      IF GetBnrPlanRepayDate (p_LegId, v_RepayDate) != 0 THEN
         RETURN 1;
      END IF;

      v_SartDate := GetVABnrLastBalanceDate (v_BCID, p_CalcDate);

      IF p_CalcDate < v_SartDate THEN
         RETURN 0;
      END IF;

      IF GetBNRFirstDiscount(v_BCID,
                             p_DealId,
                             p_CalcDate,
                             v_StartDiscount,
                             v_StartDiscountAcc) != 0
      THEN
        RETURN 1;
      END IF;

      v_T := v_RepayDate - v_SartDate;

      IF     (v_BCTermFormula = VS_TERMF_DURING)
         AND (v_BackOffice = 'А')
         AND (v_DiscKind = 1)
      THEN
         IF (v_BCPresentationDate > TO_DATE ('01.01.0001', 'dd.mm.yyyy')) THEN
            v_P := p_CalcDate - v_BCPresentationDate;
         END IF;
      ELSE
         v_P := p_CalcDate - v_SartDate;
      END IF;

      IF v_P > 0 THEN
         IF ((v_BCTermFormula != VS_TERMF_INATIME) AND (v_IncomeDateType = DL_INCOMEDATETYPE_CBR)) THEN
            v_P := v_P + 1;

            IF (p_CalcDate >= v_RepayDate) THEN
               v_P := v_P - 1;
            END IF;
         END IF;

         IF v_P > v_T THEN
            v_P := v_T;
         END IF;

         p_CalcDiscount := ROUND (v_StartDiscount * v_P / v_T, 2);
      ELSE
         p_CalcDiscount := 0;
      END IF;

      RETURN 0;

      EXCEPTION
        WHEN OTHERS THEN
            RETURN 1;
   END GetDiscountOnDateVA;

   -- Получить премию на дату УВ
   FUNCTION GetBonusOnDateVA (p_LegId       IN     NUMBER,
                              p_CalcDate    IN     DATE,
                              p_CalcBonus   OUT    NUMBER)
      RETURN NUMBER
   IS
      v_RepayDate            DATE;
      v_SartDate             DATE;
      v_StartBonus           NUMBER (32, 12);
      v_P                    NUMBER;
      v_T                    NUMBER;
      v_BCID                 NUMBER;
      v_IncomeDateType       INTEGER
         := RSB_COMMON.GetRegIntValue (
               'COMMON\ДАТА НАЧАЛА НАЧИСЛЕНИЙ');
      v_DiscKind             INTEGER
         := RSB_COMMON.GetRegIntValue (
               'ДОВЕРИТЕЛЬНОЕ УПРАВЛЕНИЕ\РАБОТА С НЕЭМИССИОННЫМИ ЦБ\НАЧИСЛЕНИЕ ДИСКОНТА');
      v_BCTermFormula        INTEGER;
      v_BackOffice           CHAR;
      v_BCPresentationDate   DATE;
   BEGIN
      p_CalcBonus := 0;

      SELECT leg.t_DealID,
             bnr.t_BCTermFormula,
             bnr.t_BackOffice,
             bnr.t_BCPresentationDate
        INTO v_BCID,
             v_BCTermFormula,
             v_BackOffice,
             v_BCPresentationDate
        FROM DDL_LEG_DBT leg, DVSBANNER_DBT bnr
       WHERE leg.t_ID = p_LegId AND bnr.t_BCID = leg.t_DealID;

      IF GetBnrPlanRepayDate (p_LegId, v_RepayDate) != 0 THEN
         RETURN 1;
      END IF;

      v_SartDate := GetVABnrLastBalanceDate (v_BCID, p_CalcDate);

      IF p_CalcDate < v_SartDate THEN
         RETURN 0;
      END IF;

      v_StartBonus := GetVABnrStartBonusPFI (v_BCID, p_CalcDate);
      v_T := v_RepayDate - v_SartDate;

      IF     (v_BCTermFormula = VS_TERMF_DURING)
         AND (v_BackOffice = 'А')
         AND (v_DiscKind = 1)
      THEN
         IF (v_BCPresentationDate > TO_DATE ('01.01.0001', 'dd.mm.yyyy')) THEN
            v_P := p_CalcDate - v_BCPresentationDate;
         END IF;
      ELSE
         v_P := p_CalcDate - v_SartDate;
      END IF;

      IF v_P > 0 THEN
         IF ((v_BCTermFormula != VS_TERMF_INATIME) AND (v_IncomeDateType = DL_INCOMEDATETYPE_CBR)) THEN
            v_P := v_P + 1;

            IF (p_CalcDate >= v_RepayDate) THEN
               v_P := v_P - 1;
            END IF;
         END IF;

         IF v_P > v_T THEN
            v_P := v_T;
         END IF;

         p_CalcBonus := ROUND (v_StartBonus * v_P / v_T, 2);
      ELSE
         p_CalcBonus := 0;
      END IF;

      RETURN 0;

      EXCEPTION
        WHEN OTHERS THEN
            RETURN 1;
   END GetBonusOnDateVA;

   -- Получить процент на дату
   FUNCTION GetPrecentOnDate (p_LegId         IN     NUMBER,
                                p_CalcDate      IN     DATE,
                                p_CalcPrecent      OUT NUMBER)
      RETURN NUMBER
   IS
      v_stat               NUMBER;
      v_NumDay             NUMBER;
      v_RepayDate          DATE;
      v_SartDate           DATE;
      v_Basis              NUMBER;
      v_Date               DATE;
      v_N                  NUMBER (32, 12);
      v_C                  NUMBER (32, 12);
   BEGIN
      p_CalcPrecent := 0;

      v_Date := p_CalcDate;

      SELECT t_Basis,
             T_INTERESTSTART,
             T_PRINCIPAL,
             T_PRICE / 1000000
        INTO v_Basis,
             v_SartDate,
             v_N,
             v_C
        FROM DDL_LEG_DBT
       WHERE t_ID = p_LegId;

      IF GetBnrPlanRepayDate (p_LegId, v_RepayDate) != 0 THEN
         RETURN 1;
      END IF;

      IF v_Date < v_SartDate THEN
         RETURN 0;
      ELSIF v_Date > v_RepayDate THEN
         v_Date := v_RepayDate;
      END IF;

      v_stat := GetDaysInYearByBasis (v_Basis,
                                               v_SartDate,
                                               TRUE,
                                               v_NumDay);
      IF v_stat != 0 THEN
        RETURN 1;
      ELSE
        p_CalcPrecent := ROUND (v_N * v_C * (v_Date - v_SartDate) / v_NumDay, 2);
      END IF;

      RETURN 0;

      EXCEPTION
        WHEN OTHERS THEN
            RETURN 1;
   END GetPrecentOnDate;

   -- Расчитать АС ЛН УВ
   FUNCTION CalcVAAS_LN (p_LegId          IN     NUMBER,
                         p_CalcDate       IN     DATE,
                         p_ACt            OUT    NUMBER)
      RETURN NUMBER
   IS
      v_BCID   NUMBER;
      v_AC0    NUMBER;
      v_Sddt   NUMBER;
      v_Spdt   NUMBER;
      v_Sbt    NUMBER;
   BEGIN
      SELECT leg.t_DealID
        INTO v_BCID
        FROM DDL_LEG_DBT leg
       WHERE leg.t_ID = p_LegId;

      /*IF GetDiscountOnDateVA(p_LegId, p_CalcDate, v_Sddt) != 0 THEN
         RETURN 1;
      END IF;*/

      IF GetBonusOnDateVA(p_LegId, p_CalcDate, v_Sbt) != 0 THEN
         RETURN 1;
      END IF;

      IF GetPrecentOnDate(p_LegId, p_CalcDate, v_Spdt) != 0 THEN
         RETURN 1;
      END IF;

      v_AC0 := GetVABnrCostPFI(v_BCID, p_CalcDate);
      p_ACt := v_Sddt - v_Sbt + v_Spdt + v_AC0;

      RETURN 0;

   EXCEPTION
      WHEN OTHERS THEN
         RETURN 1;
   END CalcVAAS_LN;

   -- Получить дисконт на дату СВ
   FUNCTION GetDiscountOnDateVS (p_LegId          IN     NUMBER,
                                 p_DealId         IN     NUMBER,
                                 p_CalcDate       IN     DATE,
                                 p_CalcDiscount      OUT NUMBER)
      RETURN NUMBER
   IS
      v_CalcDiscount       NUMBER (32, 12) := 0;
      v_RepayDate          DATE;
      v_StartDate          DATE;
      v_StartDiscount      NUMBER (32, 12);
      v_StartDiscountAcc   NUMBER (32, 12);
      v_P                  NUMBER;
      v_T                  NUMBER;
      v_BCID               NUMBER;
      v_IncomeDateType     INTEGER
         := RSB_COMMON.GetRegIntValue (
               'COMMON\ДАТА НАЧАЛА НАЧИСЛЕНИЙ');
      v_BCTermFormula      INTEGER;
      v_stat               INTEGER;
   BEGIN
      v_stat := 0;
      p_CalcDiscount := 0;

      SELECT leg.t_DealID, bnr.t_BCTermFormula
      INTO v_BCID, v_BCTermFormula
      FROM ddl_leg_dbt leg, dvsbanner_dbt bnr
      WHERE leg.t_ID = p_LegId AND bnr.t_BCID = leg.t_DealID;

      IF (GetBNRFirstDiscount(v_BCID,
                              p_DealId,
                              p_CalcDate,
                              v_StartDiscount,
                              v_StartDiscountAcc) != 0)
      THEN
         v_stat := 1;
      END IF;

      IF (GetBnrPlanRepayDate (p_LegId, v_RepayDate) != 0)
      THEN
         v_stat := 1;
      END IF;

      IF (GetVSStartDate (p_LegId, v_StartDate) != 0)
      THEN
         v_stat := 1;
      END IF;

      IF p_CalcDate < v_StartDate
      THEN
         v_stat := 1;
      END IF;

      v_T := v_RepayDate - v_StartDate;
      v_P := p_CalcDate - v_StartDate;

      IF ( (v_stat = 0) AND (v_StartDiscount > 0) AND (v_T > 0) AND (v_P > 0))
      THEN
         IF (    (v_BCTermFormula != VS_TERMF_INATIME)
            AND (v_IncomeDateType = DL_INCOMEDATETYPE_CBR))
         THEN
            v_P := v_P + 1;

            IF (p_CalcDate >= v_RepayDate)
            THEN
               v_P := v_P - 1;
            END IF;
         END IF;

         IF v_P > v_T
         THEN
            v_P := v_T;
         END IF;

         p_CalcDiscount := ROUND (v_StartDiscount * v_P / v_T, 2);
      END IF;

      RETURN v_stat;
   EXCEPTION
      WHEN OTHERS
      THEN
         RETURN 1;
   END GetDiscountOnDateVS;

   -- Расчитать АС ЛН СВ
   FUNCTION CalcVSAS_LN (p_LegId          IN     NUMBER,
                         p_CalcDate       IN     DATE,
                         p_ACt            OUT    NUMBER)
      RETURN NUMBER
   IS
      v_BCID   NUMBER;
      v_AC0    NUMBER;
      v_Sddt   NUMBER;
      v_Spdt   NUMBER;
      v_stat   INTEGER;
   BEGIN
      v_stat := 0;
      SELECT leg.t_DealID
        INTO v_BCID
        FROM DDL_LEG_DBT leg
       WHERE leg.t_ID = p_LegId;

      /*IF GetDiscountOnDateVS(p_LegId, p_CalcDate, v_Sddt) != 0 THEN
         v_stat := 1;
      END IF;*/

      IF GetPrecentOnDate(p_LegId, p_CalcDate, v_Spdt) != 0 THEN
         v_stat := 1;
      END IF;

      v_stat := GetLastBnrSalePrice(p_LegId, p_CalcDate, v_AC0);
      if (v_stat = 0) then
         p_ACt := v_Sddt + v_Spdt + v_AC0;
      end if;

      RETURN v_stat;

   EXCEPTION
      WHEN OTHERS THEN
         RETURN 1;
   END CalcVSAS_LN;

   -- Получить DealId для СВ
  FUNCTION GetVSDealId (p_BCID           IN     NUMBER,
                        p_CalcDate       IN     DATE,
                        p_DealId         OUT    NUMBER)
      RETURN NUMBER
   IS
   BEGIN
        SELECT lnk.t_ContractID
          INTO p_DealId
          FROM dvsbnrbck_dbt bck,
               doprdocs_dbt oprdocs,
               doproper_dbt opr,
               dvsordlnk_dbt lnk
         WHERE     bck.t_BCID = p_BCID
               AND bck.t_ChangeDate <= p_CalcDate
               AND bck.t_BCStatus = 'X'
               AND (bck.t_NewABCStatus = VSBANNER_STATUS_PAYED OR bck.t_NewABCStatus = VSBANNER_STATUS_FORMED)
               AND oprdocs.t_DocKind = 191
               AND oprdocs.t_DocumentID = LTRIM (TO_CHAR (bck.t_ID, '0000000000'))
               AND opr.t_ID_Operation = oprdocs.t_ID_Operation
               AND lnk.t_BCID = bck.t_BCID
               AND LNK.T_LINKKIND = 0
               AND LNK.t_ContractID = TO_NUMBER(opr.T_DOCUMENTID)
               AND ROWNUM = 1
      ORDER BY bck.t_ChangeDate DESC, bck.t_ID DESC;

      RETURN 0;

   EXCEPTION
      WHEN OTHERS THEN
         RETURN 1;
   END GetVSDealId;

   -- Получить DealId для СВ
  FUNCTION GetVADealId (p_BCID           IN     NUMBER,
                        p_CalcDate       IN     DATE,
                        p_DealId         OUT    NUMBER)
      RETURN NUMBER
   IS
   BEGIN
        SELECT tick.t_DealID
          INTO p_DealId
          FROM ddl_tick_dbt tick,
               dvsbnrbck_dbt bck,
               doprdocs_dbt oprdocs,
               doproper_dbt opr,
               dvsordlnk_dbt lnk
         WHERE     bck.t_BCID = p_BCID
               AND bck.t_ChangeDate <= p_CalcDate
               AND bck.t_ABCStatus = 'X'
               AND bck.t_NewABCStatus = VABANNER_STATUS_ACCOUNT
               AND bck.t_NewABCStatus != bck.t_OldABCStatus
               AND oprdocs.t_DocKind = 191             --изменение учтенного векселя
               AND oprdocs.t_DocumentID = LTRIM (TO_CHAR (bck.t_ID, '0000000000'))
               AND opr.t_ID_Operation = oprdocs.t_ID_Operation
               AND tick.t_BOfficeKind = opr.t_DocKind
               AND LPAD (tick.t_DealID, 34, '0') = opr.t_DocumentID
               AND lnk.t_ContractID = tick.t_DealID
               AND lnk.t_BCID = bck.t_BCID
               AND ROWNUM = 1
      ORDER BY bck.t_ChangeDate DESC, bck.t_ID DESC;

      RETURN 0;

   EXCEPTION
      WHEN OTHERS THEN
         RETURN 1;
   END GetVADealId;

   FUNCTION GetBNRFirstDate (p_BCID IN NUMBER, p_DealId IN NUMBER)
      RETURN DATE
   IS
      v_DocKind     NUMBER;
      v_DealDate    DATE;
      v_FirstDate   DATE;
      v_bnr         dvsbanner_dbt%ROWTYPE;
      v_leg2        ddl_leg_dbt%ROWTYPE;
   BEGIN
      SELECT *
        INTO v_bnr
        FROM dvsbanner_dbt bnr
       WHERE bnr.t_BCID = p_BCID;

      IF (IsOurBanner (v_bnr.t_Issuer) = TRUE)
      THEN
         SELECT ord.t_CreateDate, ord.t_DocKind INTO v_DealDate, v_DocKind
           FROM ddl_order_dbt ord
          WHERE ord.t_ContractId = p_DealId;
         IF (v_DocKind = DL_VSSTORAGEORDER) THEN
            SELECT step.T_PLAN_DATE
              INTO v_FirstDate
              FROM ddl_order_dbt ord, doproper_dbt opr, doprstep_dbt step
             WHERE     ord.t_ContractId =
                          (SELECT lnk.t_ContractId
                             FROM DOPROPER_DBT oper, dvsordlnk_dbt lnk
                            WHERE     oper.t_DocKind IN (DL_VEKSELORDER,
                                                         DL_VSBARTERORDER,
                                                         DL_VSSALE)
                                  AND LPAD (lnk.t_ContractID, 10, '0') =
                                         oper.t_DocumentID
                                  AND lnk.t_DocKind = oper.t_DocKind
                                  AND lnk.t_BCID = p_BCID
                                  AND oper.T_START_DATE <= v_DealDate)
                   AND ord.t_ContractId <> p_DealId
                   AND opr.t_DocKind = ord.t_DocKind
                   AND opr.t_DocumentId = LPAD (ord.t_ContractId, 10, '0')
                   AND step.t_ID_Operation = opr.t_ID_Operation
                   AND (   (step.t_Symbol = 'М' AND ord.t_DocKind = DL_VSBARTERORDER)
                        OR (    step.t_Symbol = 'О'
                            AND ord.t_DocKind IN (DL_VSSALE, DL_VEKSELORDER)))
                   AND step.T_ISEXECUTE = 'X'
                   AND ROWNUM = 1;
         ELSE
            SELECT step.T_PLAN_DATE
              INTO v_FirstDate
              FROM ddl_order_dbt ord, doproper_dbt opr, doprstep_dbt step
             WHERE     ord.t_ContractId = p_DealId
                   AND opr.t_DocKind = ord.t_DocKind
                   AND opr.t_DocumentId = LPAD (ord.t_ContractId, 10, '0')
                   AND step.t_ID_Operation = opr.t_ID_Operation
                   AND (   (    step.t_Symbol = 'М'
                            AND ord.t_DocKind = DL_VSBARTERORDER)
                        OR (    step.t_Symbol = 'О'
                            AND ord.t_DocKind IN (DL_VSSALE, DL_VEKSELORDER)))
                   AND ROWNUM = 1;
         END IF;
      ELSE
         SELECT *
           INTO v_leg2
           FROM ddl_leg_dbt leg
          WHERE leg.t_LegKind = 0 AND leg.t_DealID = p_DealId AND t_LegID = 0;

         v_FirstDate :=
            CASE
               WHEN v_leg2.t_MaturityIsPrincipal = 'X' THEN v_leg2.T_MATURITY
               ELSE v_leg2.T_EXPIRY
            END;
      END IF;

      RETURN v_FirstDate;
   EXCEPTION
      WHEN OTHERS
      THEN
         RETURN NULL;
   END GetBNRFirstDate;

   -- Получить DealId для СВ
  FUNCTION GetVANODealId (p_BCID           IN     NUMBER,
                        p_CalcDate       IN     DATE)
      RETURN NUMBER
   IS
     v_stat NUMBER;
     v_DealId NUMBER := -1;
   BEGIN
     v_stat := rsb_bill.GetVADealId(p_BCID,p_CalcDate,v_DealId);
     if (v_stat != 0) THEN
       v_DealId := -1;
     END IF;
     RETURN v_DealId;
   EXCEPTION
      WHEN OTHERS THEN
         RETURN -1;
   END GetVANODealId;

   -- Получить DealId для СВ
  FUNCTION GetVSNODealId (p_BCID           IN     NUMBER,
                        p_CalcDate       IN     DATE)
      RETURN NUMBER
   IS
     v_stat NUMBER;
     v_DealId NUMBER := -1;
   BEGIN
     v_stat := rsb_bill.GetVSDealId(p_BCID,p_CalcDate,v_DealId);
     if (v_stat != 0) THEN
       v_DealId := -1;
     END IF;
     RETURN v_DealId;
   EXCEPTION
      WHEN OTHERS THEN
         RETURN -1;
   END GetVSNODealId;

     FUNCTION GetVSNOStartDate (p_LegId IN NUMBER)
      RETURN DATE
   IS
     v_stat NUMBER;
     v_startDate DATE;
   BEGIN
     v_stat := rsb_bill.GetVSStartDate(p_LegId,v_startDate);
     RETURN v_startDate;
   EXCEPTION
      WHEN OTHERS THEN
         RETURN v_startDate;
   END GetVSNOStartDate;

    -- Процедура формирования базы для расчета процентов для договора векселя (на весь период, не ежедневный)
    PROCEDURE PrcBillRestList ( ContractID      IN       NUMBER,    -- ID процентного договора
                                BeginDate       IN       DATE,      -- дата начала периода
                                EndDate         IN       DATE       -- дата окончания периода
                              )
    IS
        v_Principal ddl_leg_dbt.t_Principal%TYPE  := 0;
        v_ContractBeginDate dprccontract_dbt.t_BeginDate%TYPE := TO_DATE('01.01.0001','DD.MM.YYYY');
        v_StartYear NUMBER := extract(year from BeginDate);
        V_EndYear NUMBER := extract(year from EndDate);
    BEGIN

        IF BeginDate > EndDate THEN
          RETURN;
        END IF;

        BEGIN
          SELECT leg.t_Principal, prcc.t_BeginDate INTO v_Principal, v_ContractBeginDate
            FROM ddl_leg_dbt leg, dvsbanner_dbt bnr, dprccontract_dbt prcc
           WHERE prcc.t_ContractID = ContractID
             AND prcc.t_ObjectType = 651
             AND bnr.t_BCID = TO_NUMBER(prcc.t_ObjectID)
             AND leg.t_DealID = bnr.t_BCID
             AND leg.t_LegID = 0
             AND leg.t_LegKind = RSB_BILL.LEG_KIND_VSBANNER;

          EXCEPTION
           WHEN OTHERS
            THEN v_Principal := 0;
        END;

        INSERT INTO dprcbaserest_tmp (t_contractid, t_date, t_rest) VALUES (ContractID, GREATEST(BeginDate,v_ContractBeginDate), v_Principal);

        if (((V_EndYear - v_StartYear) > 1) and ((V_EndYear - v_StartYear) < 100)) then
            WHILE (v_StartYear < (V_EndYear - 1)) LOOP
               v_StartYear := v_StartYear + 1;
               INSERT INTO dprcbaserest_tmp (t_contractid, t_date, t_rest) VALUES (ContractID, TO_DATE('01.01.'||v_StartYear,'DD.MM.YYYY'), v_Principal);
            END LOOP;
        end if;

        INSERT INTO dprcbaserest_tmp (t_contractid, t_date, t_rest) VALUES (ContractID, TO_DATE('01.01.'||V_EndYear,'DD.MM.YYYY'), v_Principal);

    EXCEPTION
        WHEN OTHERS
        THEN
            NULL;
    END; -- PrcBillRestList

  -- ID сделки, соответствующая макс. дата для векселя
  Function GetDealID_MaxDateBnr(BCID IN dvsbanner_dbt.t_BCID%TYPE)
  return ddl_tick_dbt.t_DealID%TYPE
  IS
        retval ddl_tick_dbt.t_DealID%TYPE;
  BEGIN
          select MAX(tick1.t_DealID)
      into retval
      from dvsordlnk_dbt lnk1, ddl_tick_dbt tick1
      where lnk1.t_BCID=BCID AND
            lnk1.t_LinkKind in (0,1) AND
            tick1.t_DealID=lnk1.t_ContractID AND
            tick1.t_DealDate= (select Max(tick.t_DealDate)
                                 from dvsordlnk_dbt lnk, ddl_tick_dbt tick
                                where lnk.t_BCID = BCID
                                  and lnk.t_LinkKind in (0,1)
                                  and tick.t_DealID = lnk.t_ContractID
                                  and tick.t_BofficeKind in (141,142,143,144)
                                  and tick.t_DealStatus >= 10 -- DL_READIED
                                  and tick.t_DealDate > To_Date('01.01.0001', 'DD.MM.YYYY'));

      return retval;
  END GetDealID_MaxDateBnr;

  FUNCTION VSGetSumNPTXKindClientInDate( ClientID IN NUMBER,
                                DateFrom IN DATE, DateTo in DATE, ContrID IN NUMBER)
        RETURN NUMBER
  IS
        SumNat NUMBER(31,12) := 0;
  BEGIN

    SELECT NVL (SUM (nptxobjSum.t_Sum0), 0)
      INTO SumNat
      FROM dnptxobj_dbt nptxobjSum,
           dparty_dbt party
     WHERE     party.t_PartyID = ClientID
           AND nptxobjSum.t_Kind = 1141 /*TXOBJ_PLUSG_2800*/
           AND nptxobjSum.t_Client = party.t_PartyID
           AND nptxobjSum.t_Date BETWEEN DateFrom AND DateTo
           AND NOT EXISTS
                  (SELECT tmp.*
                     FROM dnptxobj_dbt tmp
                    WHERE     tmp.t_ObjID = nptxobjSum.t_ObjID
                          AND tmp.T_ANALITICKIND1 IN (1115      /*TXOBJ_KIND1115*/
                                                          , 1130 /*TXOBJ_KIND1130*/
                                                                , 1120 /*TXOBJ_KIND1120*/
                                                                      )
                          AND tmp.T_ANALITIC1 = ContrID)
           AND EXISTS
                  (SELECT ordlnk.*
                     FROM dvsordlnk_dbt ordlnk
                    WHERE     ORDLNK.T_CONTRACTID = nptxobjSum.T_ANALITIC1
                          AND ORDLNK.T_LINKKIND = 1);
    RETURN SumNat;
  END VSGetSumNPTXKindClientInDate;

  FUNCTION VSGetRestOnAcc( BCID IN NUMBER,
                           CommDate IN DATE)
        RETURN NUMBER
  IS
        SumNat NUMBER(31,12) := 0;
  BEGIN

    SELECT NVL(SUM(rsb_account.restac (q.t_Account,
                               q.t_Currency,
                               CommDate,
                               q.t_Chapter,
                               NULL)), 0) INTO SumNat
      FROM (SELECT DISTINCT accdoc.t_Account, accdoc.t_Currency, accdoc.t_Chapter
              FROM dmccateg_dbt cat, dmcaccdoc_dbt accdoc
             WHERE     cat.t_LevelType = 1
                   AND cat.t_Code IN ('+Корр, Свексель_Хедж',
                                      '-Корр, Свексель_Хедж')
                   AND accdoc.t_CatID = cat.t_ID
                   AND accdoc.t_FIID = BCID
                   AND accdoc.t_DocKind = 4641               /*DL_VEKHDGRELATION*/
                                              ) q;

    RETURN SumNat;
  END VSGetRestOnAcc;

  FUNCTION VAGetRestOnAcc( BCID IN NUMBER,
                           CommDate IN DATE)
        RETURN NUMBER
  IS
        SumNat NUMBER(31,12) := 0;
  BEGIN

    SELECT NVL(SUM(rsb_account.restac (q.t_Account,
                               q.t_Currency,
                               CommDate,
                               q.t_Chapter,
                               NULL)), 0) INTO SumNat
      FROM (SELECT DISTINCT accdoc.t_Account, accdoc.t_Currency, accdoc.t_Chapter
              FROM dmccateg_dbt cat, dmcaccdoc_dbt accdoc
             WHERE     cat.t_LevelType = 1
                   AND cat.t_Code IN ('+Корр, вексель_Хедж',
                                      '-Корр, вексель_Хедж')
                   AND accdoc.t_CatID = cat.t_ID
                   AND accdoc.t_FIID = BCID
                   AND accdoc.t_DocKind = 4641               /*DL_VEKHDGRELATION*/
                                              ) q;

    RETURN SumNat;
  END VAGetRestOnAcc;

  FUNCTION VSGetDateCorrLast( BCID IN NUMBER)
        RETURN DATE
  IS
        LastDate DATE;
  BEGIN

    SELECT MAX (ACCTRN.T_DATE_CARRY) INTO LastDate
      FROM dacctrn_dbt acctrn,
           (SELECT accdoc.*
              FROM dmccateg_dbt cat, dmcaccdoc_dbt accdoc
             WHERE     cat.t_LevelType = 1
                   AND cat.t_Code IN ('+Корр, Свексель_Хедж',
                                      '-Корр, Свексель_Хедж')
                   AND accdoc.t_CatID = cat.t_ID
                   AND accdoc.t_FIID = BCID
                   AND accdoc.t_DocKind = 4641 /*DL_VEKHDGRELATION*/) accounts
     WHERE (   ACCTRN.T_ACCOUNT_PAYER = accounts.t_Account
            OR ACCTRN.T_ACCOUNT_RECEIVER = accounts.t_Account);

    RETURN LastDate;
  END VSGetDateCorrLast;

  FUNCTION VAGetDateCorrLast( BCID IN NUMBER)
        RETURN DATE
  IS
        LastDate DATE;
  BEGIN

    SELECT MAX (ACCTRN.T_DATE_CARRY) INTO LastDate
      FROM dacctrn_dbt acctrn,
           (SELECT accdoc.*
              FROM dmccateg_dbt cat, dmcaccdoc_dbt accdoc
             WHERE     cat.t_LevelType = 1
                   AND cat.t_Code IN ('+Корр, вексель_Хедж',
                                      '-Корр, вексель_Хедж')
                   AND accdoc.t_CatID = cat.t_ID
                   AND accdoc.t_FIID = BCID
                   AND accdoc.t_DocKind = 4641 /*DL_VEKHDGRELATION*/) accounts
     WHERE (   ACCTRN.T_ACCOUNT_PAYER = accounts.t_Account
            OR ACCTRN.T_ACCOUNT_RECEIVER = accounts.t_Account);

    RETURN LastDate;
  END VAGetDateCorrLast;
  
  FUNCTION GetBnrRate(p_bcid in dvsbanner_dbt.t_BCID%TYPE) return number
  IS
    v_facevalue number := 0;
    v_Cost number := 0;
    v_rate number := 0;
    v_formula number := 0;
    v_duration integer := 0;
  BEGIN
    SELECT lnk.t_bccost,
           leg.t_principal,
           leg.t_price/power(10, leg.t_point),
           leg.t_formula,
           leg.t_duration
    INTO v_cost,
         v_facevalue,
         v_rate,
         v_formula,
         v_duration
    FROM dvsordlnk_dbt lnk, dvsbanner_dbt bnr, ddl_leg_dbt leg
    WHERE     lnk.t_bcid = bnr.t_bcid
          AND lnk.t_dockind = CASE WHEN EXISTS(SELECT 1 FROM ddp_dep_dbt WHERE t_partyid = bnr.t_issuer)
                                   THEN 109 --выдача в СВ
                                   ELSE 141 --покупка в УВ
                              END
          AND leg.t_Dealid = bnr.t_bcid
          AND leg.t_legid = 0
          AND leg.t_legkind = 1
          AND bnr.t_bcid = p_bcid;
    
    IF (v_formula = VS_FORMULA_DISCOUNT) THEN
      v_rate := 100*(v_facevalue - v_cost)/v_cost * 365/v_duration;
    END IF;

    RETURN v_rate;
  EXCEPTION
    WHEN others THEN
      RETURN 0;
  END GetBnrRate;
  
  FUNCTION GetBnrPlanRepayDateVS (p_LegId IN NUMBER)
      RETURN DATE
   IS
      v_BCID                NUMBER;
      v_BCTermFormula       NUMBER;
      v_BCFormKind          NUMBER;
      v_Start               DATE;
      v_Maturity            DATE;
      v_BcPresentationDate  DATE;
      v_Diff                NUMBER;
      v_Issuer              NUMBER;
      v_PlanRepayDate       DATE := TO_DATE('01.01.0001','DD.MM.YYYY');

   BEGIN
      SELECT t_Start,
             t_Maturity,
             t_Diff,
             t_DealId
        INTO v_Start,
             v_Maturity,
             v_Diff,
             v_BCID
        FROM DDL_LEG_DBT
       WHERE t_ID = p_LegId;

      SELECT t_BCTermFormula, t_BCFormKind, t_BcPresentationDate
        INTO v_BCTermFormula, v_BCFormKind, v_BcPresentationDate
        FROM DVSBANNER_DBT
       WHERE t_BCID = v_BCID;

      IF (v_BCTermFormula = VS_TERMF_FIXEDDAY)
      THEN
            v_PlanRepayDate := v_Maturity;
      ELSIF (v_BCTermFormula = VS_TERMF_INATIME)
      THEN
            v_PlanRepayDate := v_Maturity;
      ELSIF (v_BCTermFormula = VS_TERMF_DURING)
      THEN
            v_PlanRepayDate := v_BcPresentationDate + v_Diff;
      ELSIF (v_BCTermFormula = VS_TERMF_ATSIGHT)
      THEN
         IF (v_Maturity >= v_Start)
         THEN
            v_PlanRepayDate := v_Maturity;
         ELSE
            v_PlanRepayDate := v_Start;
         END IF;
      END IF;

      RETURN v_PlanRepayDate;
   EXCEPTION
      WHEN OTHERS
      THEN
         RETURN v_PlanRepayDate;
   END GetBnrPlanRepayDateVS;

  function payment_due_date (
    p_bctermformula  dvsbanner_dbt.t_bctermformula%type,
    p_maturity       ddl_leg_dbt.t_maturity%type,
    p_expiry         ddl_leg_dbt.t_expiry%type
  ) return varchar2 is
    l_payment_due_date varchar2(100);
    l_null_date        date := to_date('01.01.0001', 'dd.mm.yyyy');
  begin
    if p_bctermformula in (VS_TERMF_FIXEDDAY, VS_TERMF_INATIME) then
      l_payment_due_date := to_char(p_maturity, 'dd.mm.yyyy');
    elsif p_bctermformula = VS_TERMF_ATSIGHT then     
      l_payment_due_date :=
         case
           when p_maturity =  l_null_date and p_expiry  = l_null_date then 'По предъявлении'
           when p_maturity != l_null_date and p_expiry  = l_null_date then 'По предъявлении, но не ранее ' || to_char(p_maturity, 'dd.mm.yyyy')
           when p_maturity =  l_null_date and p_expiry != l_null_date then 'По предъявлении, но не позднее ' || to_char(p_expiry, 'dd.mm.yyyy')
           else 'По предъявлении, но не ранее ' || to_char(p_maturity, 'dd.mm.yyyy') || ' и не позднее ' || to_char(p_expiry, 'dd.mm.yyyy')
           end;
    end if;
    return l_payment_due_date;
  end payment_due_date;

END RSB_BILL;
/