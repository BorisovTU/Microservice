CREATE OR REPLACE PACKAGE BODY RSB_REPRECLOTS IS

  FUNCTION GetSetBppDate(p_SumID IN NUMBER) RETURN DATE
  AS
    v_SetDate DATE;
  BEGIN

    SELECT T.t_ChangeDate
      INTO v_SetDate
      FROM (SELECT V.* FROM v_scwrthistex V
             WHERE V.t_SumID = p_SumID) T
     WHERE T.t_Instance = (SELECT MIN(Vi.t_instance)
                             FROM v_scwrthistex Vi
                            WHERE Vi.t_SumID  = T.t_SumID
                              AND Vi.t_Action = RSB_PMWRTOFF.PM_WRT_UPDTMODE_DELIVERYBPP);

    RETURN v_SetDate;

    EXCEPTION
        WHEN NO_DATA_FOUND THEN RETURN TO_DATE('01.01.0001','DD.MM.YYYY');

  END;

  FUNCTION DefinePortfID(p_Portfolio IN NUMBER, p_State IN NUMBER, p_Buy_Sale IN NUMBER, p_DealID IN NUMBER, p_Amount IN NUMBER, p_AmountBD IN NUMBER) RETURN NUMBER
  AS
    v_PortfID NUMBER := PortfID_Undef;

    v_Group   NUMBER;
  BEGIN

    IF p_Portfolio = RSB_PMWRTOFF.KINDPORT_SSPU AND p_State = RSB_PMWRTOFF.PM_WRTSUM_FORM THEN
      v_PortfID := PortfID_SSPU;

    ELSIF p_Portfolio = RSB_PMWRTOFF.KINDPORT_SSSD AND p_State = RSB_PMWRTOFF.PM_WRTSUM_FORM THEN
      v_PortfID := PortfID_SSSD;

    ELSIF p_Portfolio = RSB_PMWRTOFF.KINDPORT_ASCB AND p_State = RSB_PMWRTOFF.PM_WRTSUM_FORM THEN
      v_PortfID := PortfID_ASCB;

    ELSIF p_Portfolio = RSB_PMWRTOFF.KINDPORT_CONTR AND p_State = RSB_PMWRTOFF.PM_WRTSUM_FORM THEN
      v_PortfID := PortfID_Contr;
    /*Для портфеля БПП в отчет отбираются лоты, которые проданы из портфелей "ССПУ_ЦБ", "СССД_ЦБ", "АС_ЦБ", "ПКУ" и имеют признак "БПП"*/
    ELSIF (p_Portfolio = RSB_PMWRTOFF.KINDPORT_SSPU OR p_Portfolio = RSB_PMWRTOFF.KINDPORT_SSSD OR p_Portfolio = RSB_PMWRTOFF.KINDPORT_ASCB OR p_Portfolio = RSB_PMWRTOFF.KINDPORT_CONTR) AND
           p_State = RSB_PMWRTOFF.PM_WRTSUM_SALE_BPP THEN
      v_PortfID := PortfID_Unadmitted;
    ELSIF p_Portfolio = RSB_PMWRTOFF.KINDPORT_PROMISSORY AND p_State = RSB_PMWRTOFF.PM_WRTSUM_FORM THEN
      v_PortfID := PortfID_Promissory;
    ELSIF p_Portfolio = RSB_PMWRTOFF.KINDPORT_BACK AND p_State = RSB_PMWRTOFF.PM_WRTSUM_FORM AND p_DealID > 0 AND p_Amount > 0 AND p_AmountBD = 0 THEN
      /*В портфель ПВО отбираются лоты, изначально купленные по сделкам обратного РЕПО БПП,
      не проданные впоследствии*/

      SELECT RSB_SECUR.get_OperationGroup(RSB_SECUR.get_OperSysTypes(tk.t_DealType, tk.t_BOfficeKind))
        INTO v_Group
        FROM ddl_tick_dbt tk
       WHERE tk.t_DealID = p_DealID;

      IF (RSB_SECUR.IsREPO(v_Group) <> 0 and p_Buy_Sale = RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY_BO) OR RSB_SECUR.IsAVRWRTIN(v_Group) <> 0 THEN
        v_PortfID := PortfID_Back;
      END IF;

    END IF;

    RETURN v_PortfID;
  END;

  FUNCTION GetOverAccID_M(p_FIID IN NUMBER, p_Portfolio IN NUMBER, p_PortfID IN NUMBER, p_Date IN DATE) RETURN NUMBER
  AS
    v_AccountID NUMBER := 0;
    v_FiRole_OV NUMBER := 0;

    v_CatCode   dmccateg_dbt.t_Code%type;
  BEGIN

    IF p_PortfID = PortfID_Unadmitted OR p_PortfID = PortfID_SSSD OR p_PortfID = PortfID_SSPU THEN

      v_FiRole_OV := RSB_SPREPFUN.GetFIRoleByPortfolio(p_Portfolio);

      IF p_Portfolio = RSB_PMWRTOFF.KINDPORT_SSPU THEN
        v_CatCode := '-Переоценка, ц/б ССПУ_ЦБ';
      ELSE
        v_CatCode := '-Переоценка, ц/б';
        IF RSB_SPREPFUN.PAIR_OVER_ACC <> 0 THEN
          IF p_Portfolio = RSB_PMWRTOFF.KINDPORT_SSSD THEN
            v_CatCode := '-Переоценка, ц/б СССД_ЦБ';
          END IF;
        END IF;
      END IF;

      v_AccountID := RSB_SPREPFUN.GetAccountID(RSB_SECUR.DLDOC_ISSUE, p_FIID, v_CatCode, p_Date, v_FiRole_OV, p_FIID, p_Portfolio, 0);

    END IF;

    RETURN v_AccountID;
  END;

  FUNCTION GetOverAccID_P(p_FIID IN NUMBER, p_Portfolio IN NUMBER, p_PortfID IN NUMBER, p_Date IN DATE) RETURN NUMBER
  AS
    v_AccountID NUMBER := 0;
    v_FiRole_OV NUMBER := 0;

    v_CatCode   dmccateg_dbt.t_Code%type;
  BEGIN

    IF p_PortfID = PortfID_Unadmitted OR p_PortfID = PortfID_SSSD OR p_PortfID = PortfID_SSPU THEN

      v_FiRole_OV := RSB_SPREPFUN.GetFIRoleByPortfolio(p_Portfolio);

      IF p_Portfolio = RSB_PMWRTOFF.KINDPORT_SSPU THEN
        v_CatCode := '+Переоценка, ц/б ССПУ_ЦБ';
      ELSE
        v_CatCode := '+Переоценка, ц/б';
        IF RSB_SPREPFUN.PAIR_OVER_ACC <> 0 THEN
          IF p_Portfolio = RSB_PMWRTOFF.KINDPORT_SSSD THEN
            v_CatCode := '+Переоценка, ц/б СССД_ЦБ';
          END IF;
        END IF;
      END IF;

      v_AccountID := RSB_SPREPFUN.GetAccountID(RSB_SECUR.DLDOC_ISSUE, p_FIID, v_CatCode, p_Date, v_FiRole_OV, p_FIID, p_Portfolio, 0);

    END IF;

    RETURN v_AccountID;
  END;

  FUNCTION GetIncomeAccID(p_FIID IN NUMBER, p_Portfolio IN NUMBER, p_PortfID IN NUMBER, p_Date IN DATE) RETURN NUMBER
  AS
    v_AccountID NUMBER := 0;
    v_FiRole_PD NUMBER := 0;

    v_RoootAvoirKind NUMBER := 0;
  BEGIN

    SELECT RSI_RSB_FIInstr.FI_AvrKindsGetRoot(t_FI_Kind, t_AvoirKind)
      INTO v_RoootAvoirKind
      FROM dfininstr_dbt
     WHERE t_FIID = p_FIID;

    IF v_RoootAvoirKind = RSI_RSB_FIInstr.AVOIRKIND_BOND AND
       (p_PortfID = PortfID_SSPU OR p_PortfID = PortfID_SSSD OR
        p_PortfID = PortfID_ASCB OR (p_PortfID = PortfID_Unadmitted AND p_Portfolio != RSB_PMWRTOFF.KINDPORT_CONTR)) THEN

      IF p_PortfID = PortfID_SSPU OR p_PortfID = PortfID_SSSD OR p_PortfID = PortfID_ASCB THEN
        v_FiRole_PD := RSB_SPREPFUN.GetFIRoleByPortfolio(p_Portfolio, 0, 1, 0);
      ELSIF p_PortfID = PortfID_Unadmitted THEN
        v_FiRole_PD := RSB_SPREPFUN.GetFIRoleByPortfolio(p_Portfolio, 0, 1, 1);
      ELSE
        v_FiRole_PD := RSB_SPREPFUN.GetFIRoleByPortfolio(p_Portfolio, 0, 1, 0);
      END IF;

      v_AccountID := RSB_SPREPFUN.GetAccountID(RSB_SECUR.DLDOC_ISSUE, p_FIID, 'Начисл.ПДД, ц/б', p_Date, v_FiRole_PD, p_FIID, p_Portfolio, (CASE  WHEN p_PortfID = PortfID_Unadmitted THEN 1 ELSE 0 END), 1 /*ПД*/);

    END IF;

    RETURN v_AccountID;
  END;

  FUNCTION GetDiscountAccID(p_FIID IN NUMBER, p_Portfolio IN NUMBER, p_PortfID IN NUMBER, p_Date IN DATE) RETURN NUMBER
  AS
    v_AccountID NUMBER := 0;
    v_FiRole_DD NUMBER := 0;

    v_RoootAvoirKind NUMBER := 0;
  BEGIN

    SELECT RSI_RSB_FIInstr.FI_AvrKindsGetRoot(t_FI_Kind, t_AvoirKind)
      INTO v_RoootAvoirKind
      FROM dfininstr_dbt
     WHERE t_FIID = p_FIID;

    IF v_RoootAvoirKind = RSI_RSB_FIInstr.AVOIRKIND_BOND AND
       (p_PortfID = PortfID_SSPU OR p_PortfID = PortfID_SSSD OR
        p_PortfID = PortfID_ASCB OR (p_PortfID = PortfID_Unadmitted AND p_Portfolio != RSB_PMWRTOFF.KINDPORT_CONTR)) THEN

      IF p_PortfID = PortfID_SSPU OR p_PortfID = PortfID_SSSD OR p_PortfID = PortfID_ASCB THEN
        v_FiRole_DD := RSB_SPREPFUN.GetFIRoleByPortfolio(p_Portfolio, 1, 0, 0);
      ELSIF p_PortfID = PortfID_Unadmitted THEN
        v_FiRole_DD := RSB_SPREPFUN.GetFIRoleByPortfolio(p_Portfolio, 1, 0, 1);
      END IF;

      v_AccountID := RSB_SPREPFUN.GetAccountID(RSB_SECUR.DLDOC_ISSUE, p_FIID, 'Начисл.ПДД, ц/б', p_Date, v_FiRole_DD, p_FIID, p_Portfolio, (CASE  WHEN p_PortfID = PortfID_Unadmitted THEN 1 ELSE 0 END), 2 /*ДД*/);

    END IF;

    RETURN v_AccountID;
  END;

  FUNCTION GetBonusAccID(p_FIID IN NUMBER, p_Portfolio IN NUMBER, p_PortfID IN NUMBER, p_Date IN DATE) RETURN NUMBER
  AS
    v_AccountID NUMBER := 0;
    v_FiRole_Bon NUMBER := 0;

    v_RoootAvoirKind NUMBER := 0;
  BEGIN

    SELECT RSI_RSB_FIInstr.FI_AvrKindsGetRoot(t_FI_Kind, t_AvoirKind)
      INTO v_RoootAvoirKind
      FROM dfininstr_dbt
     WHERE t_FIID = p_FIID;

    IF v_RoootAvoirKind = RSI_RSB_FIInstr.AVOIRKIND_BOND AND
       (p_PortfID = PortfID_SSPU OR p_PortfID = PortfID_SSSD OR
        p_PortfID = PortfID_ASCB OR (p_PortfID = PortfID_Unadmitted AND p_Portfolio != RSB_PMWRTOFF.KINDPORT_CONTR)) THEN

      IF p_PortfID = PortfID_Unadmitted THEN
        v_FiRole_Bon := RSB_SPREPFUN.GetFIRoleByPortfolioBonus(p_Portfolio, 1);
      ELSE
        v_FiRole_Bon := RSB_SPREPFUN.GetFIRoleByPortfolioBonus(p_Portfolio);
      END IF;

      v_AccountID := RSB_SPREPFUN.GetAccountID(RSB_SECUR.DLDOC_ISSUE, p_FIID, 'Премия, ц/б', p_Date, v_FiRole_Bon, p_FIID, p_Portfolio, (CASE  WHEN p_PortfID = PortfID_Unadmitted THEN 1 ELSE 0 END), 6 /*ПРЕМИЯ*/);

    END IF;

    RETURN v_AccountID;
  END;

  FUNCTION GetReserveAccID(p_FIID IN NUMBER, p_Portfolio IN NUMBER, p_PortfID IN NUMBER, p_Date IN DATE) RETURN NUMBER
  AS
    v_AccountID NUMBER := 0;
    v_FiRoleR   NUMBER := 0;
  BEGIN

    IF (p_PortfID = PortfID_Unadmitted AND p_Portfolio = RSB_PMWRTOFF.KINDPORT_SSSD) OR
       (p_PortfID = PortfID_Unadmitted AND p_Portfolio = RSB_PMWRTOFF.KINDPORT_CONTR) OR
       p_PortfID = PortfID_SSSD OR p_PortfID = PortfID_ASCB OR p_PortfID = PortfID_Promissory OR p_PortfID = PortfID_Contr THEN

      IF p_PortfID = PortfID_Unadmitted AND p_Portfolio = RSB_PMWRTOFF.KINDPORT_SSSD THEN
        v_FIRoleR := RSB_SPREPFUN.FIROLE_BA_PPR_BPP;
      ELSIF p_PortfID = PortfID_SSSD THEN
        v_FIRoleR := RSB_SPREPFUN.FIROLE_BA_PPR;
      ELSIF p_PortfID = PortfID_ASCB THEN
        v_FIRoleR := RSB_SPREPFUN.FIROLE_BA_PUDP;
      ELSIF p_PortfID = PortfID_Promissory THEN
        v_FIRoleR := RSB_SPREPFUN.FIROLE_BAINPROMISSORY;
      ELSIF p_PortfID = PortfID_Contr OR (p_PortfID = PortfID_Unadmitted AND p_Portfolio = RSB_PMWRTOFF.KINDPORT_CONTR) THEN
        v_FIRoleR := RSB_SPREPFUN.FIROLE_BAINCONTR;
      END IF;

      v_AccountID := RSB_SPREPFUN.GetAccountID(RSB_SECUR.DLDOC_ISSUE, p_FIID, 'Резерв ц/б', p_Date, v_FIRoleR, p_FIID, p_Portfolio, 0, 0, 4);

    END IF;

    RETURN v_AccountID;
  END;

  FUNCTION GetPDDReserveAccID(p_FIID IN NUMBER, p_Portfolio IN NUMBER, p_PortfID IN NUMBER, p_Date IN DATE) RETURN NUMBER
  AS
    v_AccountID NUMBER := 0;
    v_FiRole_PD   NUMBER := 0;
  BEGIN

    IF (p_PortfID = PortfID_Unadmitted AND p_Portfolio = RSB_PMWRTOFF.KINDPORT_SSSD) OR
       (p_PortfID = PortfID_Unadmitted AND p_Portfolio = RSB_PMWRTOFF.KINDPORT_CONTR) OR
       p_PortfID = PortfID_SSSD OR p_PortfID = PortfID_ASCB OR p_PortfID = PortfID_Promissory OR p_PortfID = PortfID_Contr THEN

      IF p_PortfID = PortfID_Unadmitted AND p_Portfolio = RSB_PMWRTOFF.KINDPORT_SSSD THEN
        v_FIRole_PD := RSB_SPREPFUN.FIROLE_PD_PPR;
      ELSIF p_PortfID = PortfID_SSSD THEN
        v_FIRole_PD := RSB_SPREPFUN.FIROLE_PD_PPR;
      ELSIF p_PortfID = PortfID_ASCB THEN
        v_FIRole_PD := RSB_SPREPFUN.FIROLE_PD_PUDP;
      ELSIF p_PortfID = PortfID_Promissory THEN
        v_FIRole_PD := RSB_SPREPFUN.FIROLE_PD_PDO;
      ELSIF p_PortfID = PortfID_Contr OR (p_PortfID = PortfID_Unadmitted AND p_Portfolio = RSB_PMWRTOFF.KINDPORT_CONTR) THEN
        v_FIRole_PD := RSB_SPREPFUN.FIROLE_PD_PKU;
      END IF;

      v_AccountID := RSB_SPREPFUN.GetAccountID(RSB_SECUR.DLDOC_ISSUE, p_FIID, 'Резерв ц/б', p_Date, v_FIRole_PD, p_FIID, p_Portfolio, 0, 0, 13);

    END IF;

    RETURN v_AccountID;
  END;

  FUNCTION GetCorrAccID_M(p_FIID IN NUMBER, p_Portfolio IN NUMBER, p_PortfID IN NUMBER, p_Date IN DATE) RETURN NUMBER
  AS
    v_AccountID NUMBER := 0;
    v_FIRole    NUMBER := 0;
  BEGIN

    v_FIRole := RSB_SPREPFUN.GetFIRoleByPortfolio(p_Portfolio);

    v_AccountID := RSB_SPREPFUN.GetAccountID(RSB_SECUR.DLDOC_ISSUE, p_FIID, '-Корректировка, ц/б', p_Date, v_FIRole, p_FIID, -1, 0);

    RETURN v_AccountID;
  END;

  FUNCTION GetCorrAccID_P(p_FIID IN NUMBER, p_Portfolio IN NUMBER, p_PortfID IN NUMBER, p_Date IN DATE) RETURN NUMBER
  AS
    v_AccountID NUMBER := 0;
    v_FIRole    NUMBER := 0;
  BEGIN

    v_FIRole := RSB_SPREPFUN.GetFIRoleByPortfolio(p_Portfolio);

    v_AccountID := RSB_SPREPFUN.GetAccountID(RSB_SECUR.DLDOC_ISSUE, p_FIID, '+Корректировка, ц/б', p_Date, v_FIRole, p_FIID, -1, 0);

    RETURN v_AccountID;
  END;

  FUNCTION GetCorrEstResAccID_M(p_FIID IN NUMBER, p_Portfolio IN NUMBER, p_PortfID IN NUMBER, p_Date IN DATE) RETURN NUMBER
  AS
    v_AccountID NUMBER := 0;
    v_FIRole    NUMBER := 0;
  BEGIN

    v_FIRole := RSB_SPREPFUN.GetFIRoleByPortfolio(p_Portfolio);

    v_AccountID := RSB_SPREPFUN.GetAccountID(RSB_SECUR.DLDOC_ISSUE, p_FIID, '-Кор_Резерв, ЦБ', p_Date, v_FIRole, p_FIID, p_Portfolio, 0);

    RETURN v_AccountID;
  END;

  FUNCTION GetCorrEstResAccID_P(p_FIID IN NUMBER, p_Portfolio IN NUMBER, p_PortfID IN NUMBER, p_Date IN DATE) RETURN NUMBER
  AS
    v_AccountID NUMBER := 0;
    v_FIRole    NUMBER := 0;
  BEGIN

    v_FIRole := RSB_SPREPFUN.GetFIRoleByPortfolio(p_Portfolio);

    v_AccountID := RSB_SPREPFUN.GetAccountID(RSB_SECUR.DLDOC_ISSUE, p_FIID, '+Кор_Резерв, ЦБ', p_Date, v_FIRole, p_FIID, p_Portfolio, 0);

    RETURN v_AccountID;
  END;

  FUNCTION GetUNKDAccID(p_FIID IN NUMBER, p_Portfolio IN NUMBER, p_PortfID IN NUMBER, p_Date IN DATE) RETURN NUMBER
  AS
    v_AccountID NUMBER := 0;
    v_FIRole    NUMBER := 0;
  BEGIN

    IF p_PortfID = PortfID_Unadmitted THEN
      v_FIRole := RSB_SPREPFUN.GetFIRoleByPortfolio(p_Portfolio, 0, 0, 1);
    ELSE
      v_FIRole := RSB_SPREPFUN.GetFIRoleByPortfolio(p_Portfolio);
    END IF;

    v_AccountID := RSB_SPREPFUN.GetAccountID(RSB_SECUR.DLDOC_ISSUE, p_FIID, 'Уплаченный НКД', p_Date, v_FIRole, p_FIID, p_Portfolio, (CASE  WHEN p_PortfID = PortfID_Unadmitted THEN 1 ELSE 0 END));

    RETURN v_AccountID;
  END;

  PROCEDURE CorrecData(p_RepDate IN DATE, p_repreclots IN OUT repreclots_t)
  IS
    v_Currency NUMBER;
    v_BPP_OverAmount NUMBER(32,12);
    v_BPP_CorrSum NUMBER(32,12);
    v_BPP_CorrEstReserveSum NUMBER(32,12);   
  BEGIN
    IF p_repreclots.COUNT > 0 THEN

      FOR indx IN p_repreclots.FIRST .. p_repreclots.LAST
      LOOP

        IF p_repreclots(indx).t_CostInAccID > 0 AND p_repreclots(indx).t_CostInAccRest <> 0 THEN
          SELECT acc.t_Code_Currency
            INTO v_Currency
            FROM daccount_dbt acc
           WHERE acc.t_AccountID = p_repreclots(indx).t_CostInAccID;

          p_repreclots(indx).t_CostInAccRestRUB := RSB_SPREPFUN.SmartConvertSum_Ex(p_repreclots(indx).t_CostInAccRest,
                                                                                   p_RepDate,
                                                                                   v_Currency,
                                                                                   RSI_RSB_FIInstr.NATCUR,
                                                                                   1);
        END IF;

        IF p_repreclots(indx).t_OverAmountAccID > 0 AND p_repreclots(indx).t_OverAmountAccRest <> 0 THEN
          SELECT acc.t_Code_Currency
            INTO v_Currency
            FROM daccount_dbt acc
           WHERE acc.t_AccountID = p_repreclots(indx).t_OverAmountAccID;

          p_repreclots(indx).t_OverAmountAccRestRUB := RSB_SPREPFUN.SmartConvertSum_Ex(p_repreclots(indx).t_OverAmountAccRest,
                                                                                       p_RepDate,
                                                                                       v_Currency,
                                                                                       RSI_RSB_FIInstr.NATCUR,
                                                                                       1);
        END IF;

        IF p_repreclots(indx).t_IncomeAccID > 0 AND p_repreclots(indx).t_IncomeAccRest <> 0 THEN
          SELECT acc.t_Code_Currency
            INTO v_Currency
            FROM daccount_dbt acc
           WHERE acc.t_AccountID = p_repreclots(indx).t_IncomeAccID;

          p_repreclots(indx).t_IncomeAccRestRUB := RSB_SPREPFUN.SmartConvertSum_Ex(p_repreclots(indx).t_IncomeAccRest,
                                                                                   p_RepDate,
                                                                                   v_Currency,
                                                                                   RSI_RSB_FIInstr.NATCUR,
                                                                                   1);
        END IF;

        IF p_repreclots(indx).t_BonusRestAccID > 0 AND p_repreclots(indx).t_BonusRestAccRest <> 0 THEN
          SELECT acc.t_Code_Currency
            INTO v_Currency
            FROM daccount_dbt acc
           WHERE acc.t_AccountID = p_repreclots(indx).t_BonusRestAccID;

          p_repreclots(indx).t_BonusRestAccRestRUB := RSB_SPREPFUN.SmartConvertSum_Ex(p_repreclots(indx).t_BonusRestAccRest,
                                                                                      p_RepDate,
                                                                                      v_Currency,
                                                                                      RSI_RSB_FIInstr.NATCUR,
                                                                                      1);
        END IF;

        IF p_repreclots(indx).t_DiscountAccID > 0 AND p_repreclots(indx).t_DiscountAccRest <> 0 THEN
          SELECT acc.t_Code_Currency
            INTO v_Currency
            FROM daccount_dbt acc
           WHERE acc.t_AccountID = p_repreclots(indx).t_DiscountAccID;

          p_repreclots(indx).t_DiscountAccRestRUB := RSB_SPREPFUN.SmartConvertSum_Ex(p_repreclots(indx).t_DiscountAccRest,
                                                                                     p_RepDate,
                                                                                     v_Currency,
                                                                                     RSI_RSB_FIInstr.NATCUR,
                                                                                     1);
        END IF;

        IF p_repreclots(indx).t_CorrAccID > 0 AND p_repreclots(indx).t_CorrAccRest <> 0 THEN
          SELECT acc.t_Code_Currency
            INTO v_Currency
            FROM daccount_dbt acc
           WHERE acc.t_AccountID = p_repreclots(indx).t_CorrAccID;

          p_repreclots(indx).t_CorrAccRestRUB := RSB_SPREPFUN.SmartConvertSum_Ex(p_repreclots(indx).t_CorrAccRest,
                                                                                 p_RepDate,
                                                                                 v_Currency,
                                                                                 RSI_RSB_FIInstr.NATCUR,
                                                                                 1);
        END IF;

        IF p_repreclots(indx).t_ReserveSumAccID > 0 AND p_repreclots(indx).t_ReserveSumAccRest <> 0 THEN
          SELECT acc.t_Code_Currency
            INTO v_Currency
            FROM daccount_dbt acc
           WHERE acc.t_AccountID = p_repreclots(indx).t_ReserveSumAccID;

          p_repreclots(indx).t_ReserveSumAccRestRUB := RSB_SPREPFUN.SmartConvertSum_Ex(p_repreclots(indx).t_ReserveSumAccRest,
                                                                                       p_RepDate,
                                                                                       v_Currency,
                                                                                       RSI_RSB_FIInstr.NATCUR,
                                                                                       1);
        END IF;

        IF p_repreclots(indx).t_PDD_ReserveSumAccID > 0 AND p_repreclots(indx).t_PDD_ReserveSumAccRest <> 0 THEN
          SELECT acc.t_Code_Currency
            INTO v_Currency
            FROM daccount_dbt acc
           WHERE acc.t_AccountID = p_repreclots(indx).t_PDD_ReserveSumAccID;

          p_repreclots(indx).t_PDD_ReserveSumAccRestRUB := RSB_SPREPFUN.SmartConvertSum_Ex(p_repreclots(indx).t_PDD_ReserveSumAccRest,
                                                                                           p_RepDate,
                                                                                           v_Currency,
                                                                                           RSI_RSB_FIInstr.NATCUR,
                                                                                           1);
        END IF;

        IF p_repreclots(indx).t_CorrEstReserveAccID > 0 AND p_repreclots(indx).t_CorrEstReserveAccRest <> 0 THEN
          SELECT acc.t_Code_Currency
            INTO v_Currency
            FROM daccount_dbt acc
           WHERE acc.t_AccountID = p_repreclots(indx).t_CorrEstReserveAccID;

          p_repreclots(indx).t_CorrEstReserveAccRestRUB := RSB_SPREPFUN.SmartConvertSum_Ex(p_repreclots(indx).t_CorrEstReserveAccRest,
                                                                                           p_RepDate,
                                                                                           v_Currency,
                                                                                           RSI_RSB_FIInstr.NATCUR,
                                                                                           1);
        END IF;

        IF p_repreclots(indx).t_UNKDAccID > 0 AND p_repreclots(indx).t_UNKDAccRest <> 0 THEN
          SELECT acc.t_Code_Currency
            INTO v_Currency
            FROM daccount_dbt acc
           WHERE acc.t_AccountID = p_repreclots(indx).t_UNKDAccID;

          p_repreclots(indx).t_UNKDAccRestRUB := RSB_SPREPFUN.SmartConvertSum_Ex(p_repreclots(indx).t_UNKDAccRest,
                                                                                 p_RepDate,
                                                                                 v_Currency,
                                                                                 RSI_RSB_FIInstr.NATCUR,
                                                                                 1);
        END IF;

        IF p_repreclots(indx).t_PortfID <> PortfID_Unadmitted THEN
          SELECT /*+ leading(WRTSUM) cardinality(WRTSUM,100) index(WRTSUM DPMWRTSUM_DBT_IDX10)*/ NVL (SUM (V1.t_overamount), 0) ,
                      NVL (SUM (V1.t_corrvalue),  0) +NVL (SUM (V1.t_corrinttoEIR),   0),
                      NVL (SUM (V1.t_correstreserve),  0) +NVL (SUM (V1.t_estreserve),   0)
            INTO   v_BPP_OverAmount,
                      v_BPP_CorrSum ,
                      v_BPP_CorrEstReserveSum   

                      FROM dpmwrtsum_dbt wrtsum, v_scwrthistex V1                                               
                     WHERE     wrtsum.t_SumID = V1.t_SumID                                                         
                           AND wrtsum.t_buy_sale = RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY                               
                           AND wrtsum.t_party = -1                                                                    
                           AND wrtsum.t_dockind = RSB_SECUR.DLDOC_PAYMENT                                       
                           AND wrtsum.t_kind = RSB_PMWRTOFF.WRTSUM_KIND_RRWAB2                                                                  
                           AND wrtsum.t_FIID = p_repreclots(indx).t_FIID                                                   
                           AND decode(V1.t_state,RSB_PMWRTOFF.PM_WRTSUM_SALE_BPP,1,0)=1                                       
                           AND decode(V1.t_portfolio,p_repreclots(indx).t_PortfID,1,0)=1                                           
                           AND decode(V1.t_instance,(SELECT MAX (V11.t_instance)                                      
                                                  FROM v_scwrthistex V11                                            
                                                 WHERE     V11.t_sumid = wrtsum.t_sumid                            
                                                       AND V11.t_changedate <=p_RepDate),1,0)=1  ;       
          p_repreclots(indx).t_BPP_OverAmount :=v_BPP_OverAmount;
          p_repreclots(indx).t_BPP_CorrSum :=v_BPP_CorrSum;
          p_repreclots(indx).t_BPP_CorrEstReserveSum :=v_BPP_CorrEstReserveSum;
        END IF;

      END LOOP;
    END IF;
  END;

  PROCEDURE ProcessFI(p_RepDate       IN DATE,
                      p_FIID          IN NUMBER,
                      p_SSPU          IN NUMBER,
                      p_SSSD          IN NUMBER,
                      p_ASCB          IN NUMBER,
                      p_BPP           IN NUMBER,
                      p_PVO           IN NUMBER,
                      p_PKU           IN NUMBER,
                      p_PDO           IN NUMBER,
                      p_SessionID     IN NUMBER)
  IS
    v_CourseCBRF     NUMBER := 0;
    v_DateCourseCBRF DATE   := TO_DATE('01.01.0001','DD.MM.YYYY');
    v_err            NUMBER := 0;
    v_FaceValueFI    NUMBER := -1;
    v_Currency       NUMBER := -1;

    v_repreclots repreclots_t;

  BEGIN

    RSB_SPREPFUN.g_SessionID := p_SessionID;
    RSB_SPREPFUN.g_RepKind   := 0;

    SELECT t_FaceValueFI INTO v_FaceValueFI FROM dfininstr_dbt WHERE t_FIID = p_FIID;

    v_CourseCBRF := RSB_SPREPFUN.GetRateOnDate(p_RepDate,
                                               v_FaceValueFI,
                                               RSI_RSB_FIInstr.NATCUR,
                                               0, 0,
                                               v_err,
                                               v_DateCourseCBRF );

    IF p_BPP > 0 THEN

      SELECT
/*SessionID               */ p_SessionID,
/*FIID                    */ Q.t_FIID,
/*PortfID                 */ Q.t_PortfID,
/*Amount                  */ Q.SumAmount,
/*BuyCostCUR              */ Q.BuyCostCUR,
/*BuyCostNMN              */ Q.BuyCostNMN,
/*OutLay                  */ Q.OutLay,
/*NKDBuy                  */ Q.NKDBuy,
/*CostIn                  */ Q.CostIn,
/*CostInAccID             */ Q.t_CostAccountID,
/*CostInAccRest           */ ABS(NVL((SELECT NVL(RSI_RSB_FIInstr.ConvSum(rsb_account.restac(acc.t_Account, acc.t_Code_Currency, p_RepDate, acc.t_Chapter, null), acc.t_Code_Currency, v_FaceValueFI, p_RepDate, 1),0)
                                        FROM daccount_dbt acc
                                       WHERE acc.t_AccountID = Q.t_CostAccountID), 0)),
/*CostInAccRestRUB        */ 0,
/*OverAmount              */ Q.OverAmount,
/*OverAmountAccID         */ (CASE WHEN Q.OverAmount < 0 AND Q.t_OverAccountID_M > 0 THEN Q.t_OverAccountID_M
                                   WHEN Q.OverAmount > 0 AND Q.t_OverAccountID_P > 0 THEN Q.t_OverAccountID_P
                                   ELSE 0 END),
/*OverAmountAccRest       */ ABS(NVL((SELECT rsb_account.restac(acc.t_Account, acc.t_Code_Currency, p_RepDate, acc.t_Chapter, null)
                                        FROM daccount_dbt acc
                                       WHERE acc.t_AccountID = (CASE WHEN Q.OverAmount < 0 AND Q.t_OverAccountID_M > 0 THEN Q.t_OverAccountID_M
                                                                     WHEN Q.OverAmount > 0 AND Q.t_OverAccountID_P > 0 THEN Q.t_OverAccountID_P
                                                                     ELSE 0 END)), 0)),
/*OverAmountAccRestRUB    */ 0,
/*Income                  */ Q.Income,
/*IncomeAccID             */ Q.t_IncomeAccountID,
/*IncomeAccRest           */ ABS(NVL((SELECT rsb_account.restac(acc.t_Account, acc.t_Code_Currency, p_RepDate, acc.t_Chapter, null)
                                        FROM daccount_dbt acc
                                       WHERE acc.t_AccountID = Q.t_IncomeAccountID), 0)),
/*IncomeAccRestRUB        */ 0,
/*BonusRest               */ Q.BonusRest,
/*BonusRestAccID          */ Q.t_BonusAccountID,
/*BonusRestAccRest        */ ABS(NVL((SELECT rsb_account.restac(acc.t_Account, acc.t_Code_Currency, p_RepDate, acc.t_Chapter, null)
                                        FROM daccount_dbt acc
                                       WHERE acc.t_AccountID = Q.t_BonusAccountID), 0)),
/*BonusRestAccRestRUB     */ 0,
/*Discount                */ Q.Discount,
/*DiscountAccID           */ Q.t_DiscountAccountID,
/*DiscountAccRest         */ ABS(NVL((SELECT rsb_account.restac(acc.t_Account, acc.t_Code_Currency, p_RepDate, acc.t_Chapter, null)
                                        FROM daccount_dbt acc
                                       WHERE acc.t_AccountID = Q.t_DiscountAccountID), 0)),
/*DiscountAccRestRUB      */ 0,
/*CorrSum                 */ Q.CorrSum,
/*CorrAccID               */ (CASE WHEN Q.CorrSum < 0 AND Q.t_CorrAccountID_M > 0 THEN Q.t_CorrAccountID_M
                                   WHEN Q.CorrSum > 0 AND Q.t_CorrAccountID_P > 0 THEN Q.t_CorrAccountID_P
                                   ELSE 0 END),
/*CorrAccRest             */ ABS(NVL((SELECT rsb_account.restac(acc.t_Account, acc.t_Code_Currency, p_RepDate, acc.t_Chapter, null)
                                        FROM daccount_dbt acc
                                       WHERE acc.t_AccountID = (CASE WHEN Q.CorrSum < 0 AND Q.t_CorrAccountID_M > 0 THEN Q.t_CorrAccountID_M
                                                                     WHEN Q.CorrSum > 0 AND Q.t_CorrAccountID_P > 0 THEN Q.t_CorrAccountID_P
                                                                     ELSE 0 END)), 0)),
/*CorrAccRestRUB          */ 0,
/*ReserveSum              */ Q.ReserveSum,
/*ReserveSumAccID         */ Q.t_ReserveAccountID,
/*ReserveSumAccRest       */ ABS(NVL((SELECT rsb_account.restac(acc.t_Account, acc.t_Code_Currency, p_RepDate, acc.t_Chapter, null)
                                        FROM daccount_dbt acc
                                       WHERE acc.t_AccountID = Q.t_ReserveAccountID), 0)),
/*ReserveSumAccRestRUB    */ 0,
/*PDD_ReserveSum          */ Q.PDD_ReserveSum,
/*PDD_ReserveSumAccID     */ Q.t_PDDReserveAccountID,
/*PDD_ReserveSumAccRest   */ ABS(NVL((SELECT rsb_account.restac(acc.t_Account, acc.t_Code_Currency, p_RepDate, acc.t_Chapter, null)
                                        FROM daccount_dbt acc
                                       WHERE acc.t_AccountID = Q.t_PDDReserveAccountID), 0)),
/*PDD_ReserveSumAccRestRUB*/ 0,
/*CorrEstReserveSum       */ Q.CorrEstReserveSum,
/*CorrEstReserveAccID     */ (CASE WHEN Q.CorrEstReserveSum > 0 AND Q.t_CorrEstResAccountID_M > 0 THEN Q.t_CorrEstResAccountID_M
                                   WHEN Q.CorrEstReserveSum < 0 AND Q.t_CorrEstResAccountID_P > 0 THEN Q.t_CorrEstResAccountID_P
                                   ELSE 0 END),
/*CorrEstReserveAccRest   */ ABS(NVL((SELECT rsb_account.restac(acc.t_Account, acc.t_Code_Currency, p_RepDate, acc.t_Chapter, null)
                                        FROM daccount_dbt acc
                                       WHERE acc.t_AccountID = (CASE WHEN Q.CorrEstReserveSum > 0 AND Q.t_CorrEstResAccountID_M > 0 THEN Q.t_CorrEstResAccountID_M
                                                                     WHEN Q.CorrEstReserveSum < 0 AND Q.t_CorrEstResAccountID_P > 0 THEN Q.t_CorrEstResAccountID_P
                                                                     ELSE 0 END)), 0)),
/*CorrEstReserveAccRestRUB*/ 0,
/*UNKDSum                 */ Q.NKDBuy,
/*UNKDAccID               */ Q.t_UNKDAccountID,
/*UNKDAccRest             */ ABS(NVL((SELECT rsb_account.restac(acc.t_Account, acc.t_Code_Currency, p_RepDate, acc.t_Chapter, null)
                                        FROM daccount_dbt acc
                                       WHERE acc.t_AccountID = Q.t_UNKDAccountID), 0)),
/*UNKDAccRestRUB          */ 0,
/*CourseCBRF              */ v_CourseCBRF,
/*DateCourseCBRF          */ v_DateCourseCBRF,
/*BPP_OverAmount          */ 0,
/*BPP_CorrSum             */ 0,
/*BPP_CorrEstReserveSum   */ 0

      BULK COLLECT INTO v_repreclots
      FROM (SELECT Q1.t_FIID, Q1.t_CostAccountID, Q1.t_PortfID, Q1.t_Portfolio,
                   SUM(Q1.t_Amount) as SumAmount,
                   SUM(Q1.t_Sum) as BuyCostCUR,
                   ROUND(SUM(NVL(RSI_RSB_FIInstr.ConvSum(Q1.t_Sum, Q1.t_Currency, v_FaceValueFI, Q1.SetDate, 1),0)),2) as BuyCostNMN,
                   SUM(Q1.t_Outlay) as OutLay,
                   SUM(Q1.t_NKDAmount) as NKDBuy,
                   ROUND(SUM((CASE WHEN Q1.t_Portfolio = RSB_PMWRTOFF.KINDPORT_CONTR THEN NVL(RSI_RSB_FIInstr.ConvSum(Q1.t_Cost, RSI_RSB_FIInstr.NATCUR, v_FaceValueFI, p_RepDate, 1),0)
                                   ELSE Q1.t_Cost END
                                  ) + Q1.t_CostPFI - (Q1.t_BegBonus - Q1.t_Bonus + Q1.t_OldBonus)), 2) as CostIn,
                   SUM(Q1.t_OverAmount) as OverAmount,
                   SUM(Q1.t_InterestIncome) as Income,
                   SUM(Q1.t_DiscountIncome) as Discount,
                   SUM(Q1.t_BegBonus - Q1.t_Bonus + Q1.t_OldBonus) as BonusRest,
                   SUM(Q1.t_ReservAmount) as ReserveSum,
                   SUM(Q1.t_IncomeReserv) as PDD_ReserveSum,
                   SUM(Q1.t_CorrValue + Q1.t_CorrIntToEIR) as CorrSum,
                   SUM(Q1.t_CorrEstReserve + Q1.t_EstReserve) as CorrEstReserveSum,
                   GetOverAccID_M(Q1.t_FIID, Q1.t_Portfolio, Q1.t_PortfID, p_RepDate)       as t_OverAccountID_M,
                   GetOverAccID_P(Q1.t_FIID, Q1.t_Portfolio, Q1.t_PortfID, p_RepDate)       as t_OverAccountID_P,
                   GetIncomeAccID(Q1.t_FIID, Q1.t_Portfolio, Q1.t_PortfID, p_RepDate)       as t_IncomeAccountID,
                   GetDiscountAccID(Q1.t_FIID, Q1.t_Portfolio, Q1.t_PortfID, p_RepDate)     as t_DiscountAccountID,
                   GetBonusAccID(Q1.t_FIID, Q1.t_Portfolio, Q1.t_PortfID, p_RepDate)        as t_BonusAccountID,
                   GetReserveAccID(Q1.t_FIID, Q1.t_Portfolio, Q1.t_PortfID, p_RepDate)      as t_ReserveAccountID,
                   GetPDDReserveAccID(Q1.t_FIID, Q1.t_Portfolio, Q1.t_PortfID, p_RepDate)   as t_PDDReserveAccountID,
                   GetCorrAccID_M(Q1.t_FIID, Q1.t_Portfolio, Q1.t_PortfID, p_RepDate)       as t_CorrAccountID_M,
                   GetCorrAccID_P(Q1.t_FIID, Q1.t_Portfolio, Q1.t_PortfID, p_RepDate)       as t_CorrAccountID_P,
                   GetCorrEstResAccID_M(Q1.t_FIID, Q1.t_Portfolio, Q1.t_PortfID, p_RepDate) as t_CorrEstResAccountID_M,
                   GetCorrEstResAccID_P(Q1.t_FIID, Q1.t_Portfolio, Q1.t_PortfID, p_RepDate) as t_CorrEstResAccountID_P,
                   GetUNKDAccID(Q1.t_FIID, Q1.t_Portfolio, Q1.t_PortfID, p_RepDate)         as t_UNKDAccountID
              FROM (SELECT T1.*,
                           DefinePortfID(T1.t_Portfolio, T1.t_State, T1.t_Buy_Sale, T1.t_DealID, T1.t_Amount, T1.t_AmountBD) as t_PortfID,
                           GetSetBppDate(T1.t_SumID) as SetDate,
                           RSB_SPREPFUN.GetLotCostAccountID(T1.t_SumID, p_RepDate) AS t_CostAccountID
                      FROM (SELECT /*+LEADING(wrtsum) cardinality(wrtsum 100) index(wrtsum DPMWRTSUM_DBT_IDX10)*/ V1.*,
                                   MAX(V1.t_Instance) OVER(PARTITION BY V1.t_SumID) as MaxInstance
                              FROM dpmwrtsum_dbt wrtsum, v_scwrthistex V1
                             WHERE wrtsum.t_Buy_Sale = RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY
                               AND wrtsum.t_Party    = -1
                               AND wrtsum.t_DocKind  = RSB_SECUR.DLDOC_PAYMENT
                               AND wrtsum.t_Kind     = RSB_PMWRTOFF.WRTSUM_KIND_RRWAB2
                               AND wrtsum.t_FIID     = p_FIID
                               AND (wrtsum.t_ChangeDate > p_RepDate OR
                                    (wrtsum.t_ChangeDate <= p_RepDate AND wrtsum.t_Amount > 0)
                                   )
                               AND V1.t_SumID        = wrtsum.t_SumID
                               AND V1.t_ChangeDate   <= p_RepDate
                               AND V1.t_Instance     = (CASE WHEN wrtsum.t_ChangeDate <= p_RepDate THEN wrtsum.t_Instance ELSE V1.t_Instance END)
                           ) T1
                     WHERE T1.t_Instance = T1.MaxInstance
                       --AND T1.t_Amount   > 0 --В исходном макросе этого условия не было
                       AND T1.t_State    = RSB_PMWRTOFF.PM_WRTSUM_SALE_BPP
                       AND T1.t_Portfolio IN (RSB_PMWRTOFF.KINDPORT_SSPU, RSB_PMWRTOFF.KINDPORT_SSSD, RSB_PMWRTOFF.KINDPORT_ASCB, RSB_PMWRTOFF.KINDPORT_CONTR)
                  ) Q1
             GROUP BY Q1.t_FIID, Q1.t_CostAccountID, Q1.t_PortfID, Q1.t_Portfolio
           ) Q;

      IF v_repreclots.COUNT > 0 THEN

        CorrecData(p_RepDate, v_repreclots);

        FORALL indx IN v_repreclots.FIRST .. v_repreclots.LAST
          INSERT INTO DREPRECLOTS_DBT
                VALUES v_repreclots(indx);
      END IF;
    END IF;



    IF p_SSPU <> 0 OR p_SSSD <> 0 OR p_ASCB <> 0 OR p_PKU <> 0 OR p_PDO <> 0 THEN

      SELECT
/*SessionID               */ p_SessionID,
/*FIID                    */ Q.t_FIID,
/*PortfID                 */ Q.t_PortfID,
/*Amount                  */ Q.SumAmount,
/*BuyCostCUR              */ 0,
/*BuyCostNMN              */ 0,
/*OutLay                  */ Q.OutLay,
/*NKDBuy                  */ Q.NKDBuy,
/*CostIn                  */ Q.CostIn,
/*CostInAccID             */ Q.t_CostAccountID,
/*CostInAccRest           */ ABS(NVL((SELECT NVL(RSI_RSB_FIInstr.ConvSum(rsb_account.restac(acc.t_Account, acc.t_Code_Currency, p_RepDate, acc.t_Chapter, null), acc.t_Code_Currency, v_FaceValueFI, p_RepDate, 1),0)
                                        FROM daccount_dbt acc
                                       WHERE acc.t_AccountID = Q.t_CostAccountID), 0)),
/*CostInAccRestRUB        */ 0,
/*OverAmount              */ Q.OverAmount,
/*OverAmountAccID         */ (CASE WHEN Q.OverAmount < 0 AND Q.t_OverAccountID_M > 0 THEN Q.t_OverAccountID_M
                                   WHEN Q.OverAmount > 0 AND Q.t_OverAccountID_P > 0 THEN Q.t_OverAccountID_P
                                   ELSE 0 END),
/*OverAmountAccRest       */ ABS(NVL((SELECT rsb_account.restac(acc.t_Account, acc.t_Code_Currency, p_RepDate, acc.t_Chapter, null)
                                        FROM daccount_dbt acc
                                       WHERE acc.t_AccountID = (CASE WHEN Q.OverAmount < 0 AND Q.t_OverAccountID_M > 0 THEN Q.t_OverAccountID_M
                                                                     WHEN Q.OverAmount > 0 AND Q.t_OverAccountID_P > 0 THEN Q.t_OverAccountID_P
                                                                     ELSE 0 END)), 0)),
/*OverAmountAccRestRUB    */ 0,
/*Income                  */ Q.Income,
/*IncomeAccID             */ Q.t_IncomeAccountID,
/*IncomeAccRest           */ ABS(NVL((SELECT rsb_account.restac(acc.t_Account, acc.t_Code_Currency, p_RepDate, acc.t_Chapter, null)
                                        FROM daccount_dbt acc
                                       WHERE acc.t_AccountID = Q.t_IncomeAccountID), 0)),
/*IncomeAccRestRUB        */ 0,
/*BonusRest               */ Q.BonusRest,
/*BonusRestAccID          */ Q.t_BonusAccountID,
/*BonusRestAccRest        */ ABS(NVL((SELECT rsb_account.restac(acc.t_Account, acc.t_Code_Currency, p_RepDate, acc.t_Chapter, null)
                                        FROM daccount_dbt acc
                                       WHERE acc.t_AccountID = Q.t_BonusAccountID), 0)),
/*BonusRestAccRestRUB     */ 0,
/*Discount                */ Q.Discount,
/*DiscountAccID           */ Q.t_DiscountAccountID,
/*DiscountAccRest         */ ABS(NVL((SELECT rsb_account.restac(acc.t_Account, acc.t_Code_Currency, p_RepDate, acc.t_Chapter, null)
                                        FROM daccount_dbt acc
                                       WHERE acc.t_AccountID = Q.t_DiscountAccountID), 0)),
/*DiscountAccRestRUB      */ 0,
/*CorrSum                 */ Q.CorrSum,
/*CorrAccID               */ (CASE WHEN Q.CorrSum < 0 AND Q.t_CorrAccountID_M > 0 THEN Q.t_CorrAccountID_M
                                   WHEN Q.CorrSum > 0 AND Q.t_CorrAccountID_P > 0 THEN Q.t_CorrAccountID_P
                                   ELSE 0 END),
/*CorrAccRest             */ ABS(NVL((SELECT rsb_account.restac(acc.t_Account, acc.t_Code_Currency, p_RepDate, acc.t_Chapter, null)
                                        FROM daccount_dbt acc
                                       WHERE acc.t_AccountID = (CASE WHEN Q.CorrSum < 0 AND Q.t_CorrAccountID_M > 0 THEN Q.t_CorrAccountID_M
                                                                     WHEN Q.CorrSum > 0 AND Q.t_CorrAccountID_P > 0 THEN Q.t_CorrAccountID_P
                                                                     ELSE 0 END)), 0)),
/*CorrAccRestRUB          */ 0,
/*ReserveSum              */ Q.ReserveSum,
/*ReserveSumAccID         */ Q.t_ReserveAccountID,
/*ReserveSumAccRest       */ ABS(NVL((SELECT rsb_account.restac(acc.t_Account, acc.t_Code_Currency, p_RepDate, acc.t_Chapter, null)
                                        FROM daccount_dbt acc
                                       WHERE acc.t_AccountID = Q.t_ReserveAccountID), 0)),
/*ReserveSumAccRestRUB    */ 0,
/*PDD_ReserveSum          */ Q.PDD_ReserveSum,
/*PDD_ReserveSumAccID     */ Q.t_PDDReserveAccountID,
/*PDD_ReserveSumAccRest   */ ABS(NVL((SELECT rsb_account.restac(acc.t_Account, acc.t_Code_Currency, p_RepDate, acc.t_Chapter, null)
                                        FROM daccount_dbt acc
                                       WHERE acc.t_AccountID = Q.t_PDDReserveAccountID), 0)),
/*PDD_ReserveSumAccRestRUB*/ 0,
/*CorrEstReserveSum       */ Q.CorrEstReserveSum,
/*CorrEstReserveAccID     */ (CASE WHEN Q.CorrEstReserveSum > 0 AND Q.t_CorrEstResAccountID_M > 0 THEN Q.t_CorrEstResAccountID_M
                                   WHEN Q.CorrEstReserveSum < 0 AND Q.t_CorrEstResAccountID_P > 0 THEN Q.t_CorrEstResAccountID_P
                                   ELSE 0 END),
/*CorrEstReserveAccRest   */ ABS(NVL((SELECT rsb_account.restac(acc.t_Account, acc.t_Code_Currency, p_RepDate, acc.t_Chapter, null)
                                        FROM daccount_dbt acc
                                       WHERE acc.t_AccountID = (CASE WHEN Q.CorrEstReserveSum > 0 AND Q.t_CorrEstResAccountID_M > 0 THEN Q.t_CorrEstResAccountID_M
                                                                     WHEN Q.CorrEstReserveSum < 0 AND Q.t_CorrEstResAccountID_P > 0 THEN Q.t_CorrEstResAccountID_P
                                                                     ELSE 0 END)), 0)),
/*CorrEstReserveAccRestRUB*/ 0,
/*UNKDSum                 */ Q.NKDBuy,
/*UNKDAccID               */ Q.t_UNKDAccountID,
/*UNKDAccRest             */ ABS(NVL((SELECT rsb_account.restac(acc.t_Account, acc.t_Code_Currency, p_RepDate, acc.t_Chapter, null)
                                        FROM daccount_dbt acc
                                       WHERE acc.t_AccountID = Q.t_UNKDAccountID), 0)),
/*UNKDAccRestRUB          */ 0,
/*CourseCBRF              */ v_CourseCBRF,
/*DateCourseCBRF          */ v_DateCourseCBRF,
/*BPP_OverAmount          */ 0,
/*BPP_CorrSum             */ 0,
/*BPP_CorrEstReserveSum   */ 0

      BULK COLLECT INTO v_repreclots
      FROM (SELECT Q1.t_FIID, Q1.t_CostAccountID, Q1.t_PortfID, Q1.t_Portfolio,
                   SUM(Q1.t_Amount) as SumAmount,
                   SUM(Q1.t_Outlay) as OutLay,
                   SUM(Q1.t_NKDAmount) as NKDBuy,
                   ROUND(SUM((CASE WHEN Q1.t_Portfolio = RSB_PMWRTOFF.KINDPORT_CONTR THEN NVL(RSI_RSB_FIInstr.ConvSum(Q1.t_Cost, RSI_RSB_FIInstr.NATCUR, v_FaceValueFI, p_RepDate, 1),0)
                                   ELSE Q1.t_Cost END
                                  ) + Q1.t_CostPFI - (Q1.t_BegBonus - Q1.t_Bonus + Q1.t_OldBonus)), 2) as CostIn,
                   SUM(Q1.t_OverAmount) as OverAmount,
                   SUM(Q1.t_InterestIncome) as Income,
                   SUM(Q1.t_DiscountIncome) as Discount,
                   SUM(Q1.t_BegBonus - Q1.t_Bonus + Q1.t_OldBonus) as BonusRest,
                   SUM(Q1.t_ReservAmount) as ReserveSum,
                   SUM(Q1.t_IncomeReserv) as PDD_ReserveSum,
                   SUM(Q1.t_CorrValue + Q1.t_CorrIntToEIR) as CorrSum,
                   SUM(Q1.t_CorrEstReserve + Q1.t_EstReserve) as CorrEstReserveSum,
                   GetOverAccID_M(Q1.t_FIID, Q1.t_Portfolio, Q1.t_PortfID, p_RepDate)       as t_OverAccountID_M,
                   GetOverAccID_P(Q1.t_FIID, Q1.t_Portfolio, Q1.t_PortfID, p_RepDate)       as t_OverAccountID_P,
                   GetIncomeAccID(Q1.t_FIID, Q1.t_Portfolio, Q1.t_PortfID, p_RepDate)       as t_IncomeAccountID,
                   GetDiscountAccID(Q1.t_FIID, Q1.t_Portfolio, Q1.t_PortfID, p_RepDate)     as t_DiscountAccountID,
                   GetBonusAccID(Q1.t_FIID, Q1.t_Portfolio, Q1.t_PortfID, p_RepDate)        as t_BonusAccountID,
                   GetReserveAccID(Q1.t_FIID, Q1.t_Portfolio, Q1.t_PortfID, p_RepDate)      as t_ReserveAccountID,
                   GetPDDReserveAccID(Q1.t_FIID, Q1.t_Portfolio, Q1.t_PortfID, p_RepDate)   as t_PDDReserveAccountID,
                   GetCorrAccID_M(Q1.t_FIID, Q1.t_Portfolio, Q1.t_PortfID, p_RepDate)       as t_CorrAccountID_M,
                   GetCorrAccID_P(Q1.t_FIID, Q1.t_Portfolio, Q1.t_PortfID, p_RepDate)       as t_CorrAccountID_P,
                   GetCorrEstResAccID_M(Q1.t_FIID, Q1.t_Portfolio, Q1.t_PortfID, p_RepDate) as t_CorrEstResAccountID_M,
                   GetCorrEstResAccID_P(Q1.t_FIID, Q1.t_Portfolio, Q1.t_PortfID, p_RepDate) as t_CorrEstResAccountID_P,
                   GetUNKDAccID(Q1.t_FIID, Q1.t_Portfolio, Q1.t_PortfID, p_RepDate)         as t_UNKDAccountID
              FROM (SELECT T1.*,
                           DefinePortfID(T1.t_Portfolio, T1.t_State, T1.t_Buy_Sale, T1.t_DealID, T1.t_Amount, T1.t_AmountBD) as t_PortfID,
                           RSB_SPREPFUN.GetLotCostAccountID(T1.t_SumID, p_RepDate) AS t_CostAccountID
                      FROM (SELECT /*+LEADING(wrtsum) cardinality(wrtsum 100) index(wrtsum DPMWRTSUM_DBT_IDX10)*/ V1.*,
                                   MAX(V1.t_Instance) OVER(PARTITION BY V1.t_SumID) as MaxInstance
                              FROM dpmwrtsum_dbt wrtsum, v_scwrthistex V1
                             WHERE wrtsum.t_Buy_Sale = RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY
                               AND wrtsum.t_Party    = -1
                               AND wrtsum.t_DocKind  IN (RSB_SECUR.DLDOC_PAYMENT, RSB_SECUR.DL_ISSUE_UNION, RSB_SECUR.DL_MOVINGDOC)
                               AND wrtsum.t_DealID   > 0
                               AND wrtsum.t_Kind     IN (RSB_PMWRTOFF.WRTSUM_KIND_B,
                                                         RSB_PMWRTOFF.WRTSUM_KIND_RRWAB2,
                                                         RSB_PMWRTOFF.WRTSUM_KIND_FB,
                                                         RSB_PMWRTOFF.WRTSUM_KIND_DB,
                                                         RSB_PMWRTOFF.WRTSUM_KIND_GB,
                                                         RSB_PMWRTOFF.WRTSUM_KIND_MB
                                                        )
                               AND wrtsum.t_FIID     = p_FIID
                               AND (wrtsum.t_ChangeDate > p_RepDate OR
                                    (wrtsum.t_ChangeDate <= p_RepDate AND wrtsum.t_Amount > 0)
                                   )
                               AND V1.t_SumID        = wrtsum.t_SumID
                               AND V1.t_ChangeDate   <= p_RepDate
                               AND V1.t_Instance     = (CASE WHEN wrtsum.t_ChangeDate <= p_RepDate THEN wrtsum.t_Instance ELSE V1.t_Instance END)
                           ) T1
                     WHERE T1.t_Instance = T1.MaxInstance
                       AND T1.t_Amount   > 0
                       AND T1.t_State    = RSB_PMWRTOFF.PM_WRTSUM_FORM
                       AND (   1 = (CASE WHEN p_SSPU <> 0 AND T1.t_Portfolio = RSB_PMWRTOFF.KINDPORT_SSPU       THEN 1 ELSE 0 END)
                            OR 1 = (CASE WHEN p_SSSD <> 0 AND T1.t_Portfolio = RSB_PMWRTOFF.KINDPORT_SSSD       THEN 1 ELSE 0 END)
                            OR 1 = (CASE WHEN p_ASCB <> 0 AND T1.t_Portfolio = RSB_PMWRTOFF.KINDPORT_ASCB       THEN 1 ELSE 0 END)
                            OR 1 = (CASE WHEN p_PKU <> 0  AND T1.t_Portfolio = RSB_PMWRTOFF.KINDPORT_CONTR      THEN 1 ELSE 0 END)
                            OR 1 = (CASE WHEN p_PDO <> 0  AND T1.t_Portfolio = RSB_PMWRTOFF.KINDPORT_PROMISSORY THEN 1 ELSE 0 END)
                           )
                  ) Q1
             GROUP BY Q1.t_FIID, Q1.t_CostAccountID, Q1.t_PortfID, Q1.t_Portfolio
           ) Q;

      IF v_repreclots.COUNT > 0 THEN

        CorrecData(p_RepDate, v_repreclots);

        FORALL indx IN v_repreclots.FIRST .. v_repreclots.LAST
          INSERT INTO DREPRECLOTS_DBT
                VALUES v_repreclots(indx);
      END IF;

    END IF;


    IF p_PVO <> 0 THEN

      SELECT
/*SessionID               */ p_SessionID,
/*FIID                    */ Q.t_FIID,
/*PortfID                 */ Q.t_PortfID,
/*Amount                  */ Q.SumAmount,
/*BuyCostCUR              */ Q.BuyCostCUR,
/*BuyCostNMN              */ Q.BuyCostNMN,
/*OutLay                  */ Q.OutLay,
/*NKDBuy                  */ Q.NKDBuy,
/*CostIn                  */ Q.CostIn,
/*CostInAccID             */ Q.t_CostAccountID,
/*CostInAccRest           */ ABS(NVL((SELECT rsb_account.restac(acc.t_Account, acc.t_Code_Currency, p_RepDate, acc.t_Chapter, null)
                                        FROM daccount_dbt acc
                                       WHERE acc.t_AccountID = Q.t_CostAccountID), 0)),
/*CostInAccRestRUB        */ 0,
/*OverAmount              */ Q.OverAmount,
/*OverAmountAccID         */ 0,
/*OverAmountAccRest       */ 0,
/*OverAmountAccRestRUB    */ 0,
/*Income                  */ Q.Income,
/*IncomeAccID             */ Q.t_IncomeAccountID,
/*IncomeAccRest           */ ABS(NVL((SELECT rsb_account.restac(acc.t_Account, acc.t_Code_Currency, p_RepDate, acc.t_Chapter, null)
                                        FROM daccount_dbt acc
                                       WHERE acc.t_AccountID = Q.t_IncomeAccountID), 0)),
/*IncomeAccRestRUB        */ 0,
/*BonusRest               */ Q.BonusRest,
/*BonusRestAccID          */ Q.t_BonusAccountID,
/*BonusRestAccRest        */ ABS(NVL((SELECT rsb_account.restac(acc.t_Account, acc.t_Code_Currency, p_RepDate, acc.t_Chapter, null)
                                        FROM daccount_dbt acc
                                       WHERE acc.t_AccountID = Q.t_BonusAccountID), 0)),
/*BonusRestAccRestRUB     */ 0,
/*Discount                */ Q.Discount,
/*DiscountAccID           */ Q.t_DiscountAccountID,
/*DiscountAccRest         */ ABS(NVL((SELECT rsb_account.restac(acc.t_Account, acc.t_Code_Currency, p_RepDate, acc.t_Chapter, null)
                                        FROM daccount_dbt acc
                                       WHERE acc.t_AccountID = Q.t_DiscountAccountID), 0)),
/*DiscountAccRestRUB      */ 0,
/*CorrSum                 */ Q.CorrSum,
/*CorrAccID               */ (CASE WHEN Q.CorrSum < 0 AND Q.t_CorrAccountID_M > 0 THEN Q.t_CorrAccountID_M
                                   WHEN Q.CorrSum > 0 AND Q.t_CorrAccountID_P > 0 THEN Q.t_CorrAccountID_P
                                   ELSE 0 END),
/*CorrAccRest             */ ABS(NVL((SELECT rsb_account.restac(acc.t_Account, acc.t_Code_Currency, p_RepDate, acc.t_Chapter, null)
                                        FROM daccount_dbt acc
                                       WHERE acc.t_AccountID = (CASE WHEN Q.CorrSum < 0 AND Q.t_CorrAccountID_M > 0 THEN Q.t_CorrAccountID_M
                                                                     WHEN Q.CorrSum > 0 AND Q.t_CorrAccountID_P > 0 THEN Q.t_CorrAccountID_P
                                                                     ELSE 0 END)), 0)),
/*CorrAccRestRUB          */ 0,
/*ReserveSum              */ Q.ReserveSum,
/*ReserveSumAccID         */ Q.t_ReserveAccountID,
/*ReserveSumAccRest       */ ABS(NVL((SELECT rsb_account.restac(acc.t_Account, acc.t_Code_Currency, p_RepDate, acc.t_Chapter, null)
                                        FROM daccount_dbt acc
                                       WHERE acc.t_AccountID = Q.t_ReserveAccountID), 0)),
/*ReserveSumAccRestRUB    */ 0,
/*PDD_ReserveSum          */ Q.PDD_ReserveSum,
/*PDD_ReserveSumAccID     */ Q.t_PDDReserveAccountID,
/*PDD_ReserveSumAccRest   */ ABS(NVL((SELECT rsb_account.restac(acc.t_Account, acc.t_Code_Currency, p_RepDate, acc.t_Chapter, null)
                                        FROM daccount_dbt acc
                                       WHERE acc.t_AccountID = Q.t_PDDReserveAccountID), 0)),
/*PDD_ReserveSumAccRestRUB*/ 0,
/*CorrEstReserveSum       */ Q.CorrEstReserveSum,
/*CorrEstReserveAccID     */ (CASE WHEN Q.CorrEstReserveSum > 0 AND Q.t_CorrEstResAccountID_M > 0 THEN Q.t_CorrEstResAccountID_M
                                   WHEN Q.CorrEstReserveSum < 0 AND Q.t_CorrEstResAccountID_P > 0 THEN Q.t_CorrEstResAccountID_P
                                   ELSE 0 END),
/*CorrEstReserveAccRest   */ ABS(NVL((SELECT rsb_account.restac(acc.t_Account, acc.t_Code_Currency, p_RepDate, acc.t_Chapter, null)
                                        FROM daccount_dbt acc
                                       WHERE acc.t_AccountID = (CASE WHEN Q.CorrEstReserveSum > 0 AND Q.t_CorrEstResAccountID_M > 0 THEN Q.t_CorrEstResAccountID_M
                                                                     WHEN Q.CorrEstReserveSum < 0 AND Q.t_CorrEstResAccountID_P > 0 THEN Q.t_CorrEstResAccountID_P
                                                                     ELSE 0 END)), 0)),
/*CorrEstReserveAccRestRUB*/ 0,
/*UNKDSum                 */ Q.NKDBuy,
/*UNKDAccID               */ Q.t_UNKDAccountID,
/*UNKDAccRest             */ ABS(NVL((SELECT rsb_account.restac(acc.t_Account, acc.t_Code_Currency, p_RepDate, acc.t_Chapter, null)
                                        FROM daccount_dbt acc
                                       WHERE acc.t_AccountID = Q.t_UNKDAccountID), 0)),
/*UNKDAccRestRUB          */ 0,
/*CourseCBRF              */ v_CourseCBRF,
/*DateCourseCBRF          */ v_DateCourseCBRF,
/*BPP_OverAmount          */ 0,
/*BPP_CorrSum             */ 0,
/*BPP_CorrEstReserveSum   */ 0
      BULK COLLECT INTO v_repreclots
      FROM (SELECT Q1.t_FIID, Q1.t_CostAccountID, Q1.t_PortfID, Q1.t_Portfolio,
                   SUM(Q1.t_Amount) as SumAmount,
                   SUM(Q1.t_Sum) as BuyCostCUR,
                   ROUND(SUM(NVL(RSI_RSB_FIInstr.ConvSum(Q1.t_Sum, Q1.t_Currency, v_FaceValueFI, Q1.SetDate, 1),0)),2) as BuyCostNMN,
                   SUM(Q1.t_Outlay) as OutLay,
                   SUM(Q1.t_NKDAmount) as NKDBuy,
                   SUM(Q1.t_BalanceCost) as CostIn,
                   SUM(Q1.t_OverAmount) as OverAmount,
                   SUM(Q1.t_InterestIncome) as Income,
                   SUM(Q1.t_DiscountIncome) as Discount,
                   SUM(Q1.t_BegBonus - Q1.t_Bonus + Q1.t_OldBonus) as BonusRest,
                   SUM(Q1.t_ReservAmount) as ReserveSum,
                   SUM(Q1.t_IncomeReserv) as PDD_ReserveSum,
                   SUM(Q1.t_CorrValue + Q1.t_CorrIntToEIR) as CorrSum,
                   SUM(Q1.t_CorrEstReserve + Q1.t_EstReserve) as CorrEstReserveSum,
                   GetIncomeAccID(Q1.t_FIID, Q1.t_Portfolio, Q1.t_PortfID, p_RepDate)       as t_IncomeAccountID,
                   GetDiscountAccID(Q1.t_FIID, Q1.t_Portfolio, Q1.t_PortfID, p_RepDate)     as t_DiscountAccountID,
                   GetBonusAccID(Q1.t_FIID, Q1.t_Portfolio, Q1.t_PortfID, p_RepDate)        as t_BonusAccountID,
                   GetReserveAccID(Q1.t_FIID, Q1.t_Portfolio, Q1.t_PortfID, p_RepDate)      as t_ReserveAccountID,
                   GetPDDReserveAccID(Q1.t_FIID, Q1.t_Portfolio, Q1.t_PortfID, p_RepDate)   as t_PDDReserveAccountID,
                   GetCorrAccID_M(Q1.t_FIID, Q1.t_Portfolio, Q1.t_PortfID, p_RepDate)       as t_CorrAccountID_M,
                   GetCorrAccID_P(Q1.t_FIID, Q1.t_Portfolio, Q1.t_PortfID, p_RepDate)       as t_CorrAccountID_P,
                   GetCorrEstResAccID_M(Q1.t_FIID, Q1.t_Portfolio, Q1.t_PortfID, p_RepDate) as t_CorrEstResAccountID_M,
                   GetCorrEstResAccID_P(Q1.t_FIID, Q1.t_Portfolio, Q1.t_PortfID, p_RepDate) as t_CorrEstResAccountID_P,
                   GetUNKDAccID(Q1.t_FIID, Q1.t_Portfolio, Q1.t_PortfID, p_RepDate)         as t_UNKDAccountID
              FROM (SELECT T1.*,
                           T1.t_Date as SetDate,
                           DefinePortfID(T1.t_Portfolio, T1.t_State, T1.t_Buy_Sale, T1.t_DealID, T1.t_Amount, T1.t_AmountBD) as t_PortfID,
                           RSB_SPREPFUN.GetLotCostAccountID(T1.t_SumID, p_RepDate) AS t_CostAccountID
                      FROM (SELECT /*+LEADING(wrtsum)*/ V1.*,
                                   MAX(V1.t_Instance) OVER(PARTITION BY V1.t_SumID) as MaxInstance
                              FROM dpmwrtsum_dbt wrtsum, v_scwrthistex V1
                             WHERE wrtsum.t_Party    = -1
                               AND wrtsum.t_DocKind  IN (RSB_SECUR.DLDOC_PAYMENT, RSB_SECUR.DL_ISSUE_UNION)
                               AND wrtsum.t_DealID   > 0
                               AND wrtsum.t_FIID     = p_FIID
                               AND (wrtsum.t_Kind IN (RSB_PMWRTOFF.WRTSUM_KIND_FB,
                                                      RSB_PMWRTOFF.WRTSUM_KIND_DB,
                                                      RSB_PMWRTOFF.WRTSUM_KIND_GB)
                                    OR (    wrtsum.t_Buy_Sale = RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY_BO
                                        AND wrtsum.t_DealID = (SELECT tick.t_DealID
                                                                 FROM ddl_tick_dbt tick
                                                                WHERE tick.t_DealID = wrtsum.t_DealID
                                                                  AND RSB_SECUR.IsTwoPart(RSB_SECUR.get_OperationGroup(RSB_SECUR.get_OperSysTypes(tick.t_DealType, tick.t_BofficeKind)))=1
                                                                  AND ( tick.t_DealStatus < 20 OR (tick.t_DealStatus = 20 AND tick.t_CloseDate > p_RepDate))
                                                              )
                                       )
                                   )
                               AND (wrtsum.t_ChangeDate > p_RepDate OR
                                    (wrtsum.t_ChangeDate <= p_RepDate AND wrtsum.t_Amount > 0)
                                   )
                               AND V1.t_SumID        = wrtsum.t_SumID
                               AND V1.t_Portfolio    = RSB_PMWRTOFF.KINDPORT_BACK
                               AND V1.t_ChangeDate   <= p_RepDate
                               AND V1.t_Instance     = (CASE WHEN wrtsum.t_ChangeDate <= p_RepDate THEN wrtsum.t_Instance ELSE V1.t_Instance END)
                           ) T1
                     WHERE T1.t_Instance = T1.MaxInstance
                       AND (T1.t_Kind IN (RSB_PMWRTOFF.WRTSUM_KIND_FB,
                                          RSB_PMWRTOFF.WRTSUM_KIND_DB,
                                          RSB_PMWRTOFF.WRTSUM_KIND_GB)
                            OR (T1.t_State = RSB_PMWRTOFF.PM_WRTSUM_FORM AND T1.t_Amount > 0 AND T1.t_AmountBD = 0)
                           )
                  ) Q1
             GROUP BY Q1.t_FIID, Q1.t_CostAccountID, Q1.t_PortfID, Q1.t_Portfolio
           ) Q;

      IF v_repreclots.COUNT > 0 THEN

        CorrecData(p_RepDate, v_repreclots);

        FORALL indx IN v_repreclots.FIRST .. v_repreclots.LAST
          INSERT INTO DREPRECLOTS_DBT
                VALUES v_repreclots(indx);
      END IF;

    END IF;


  END;


  --Подготовка данных для отчета
  PROCEDURE CreateAllData(p_RepDate       IN DATE,
                          p_FIID          IN NUMBER,
                          p_SSPU          IN NUMBER,
                          p_SSSD          IN NUMBER,
                          p_ASCB          IN NUMBER,
                          p_BPP           IN NUMBER,
                          p_PVO           IN NUMBER,
                          p_PKU           IN NUMBER,
                          p_PDO           IN NUMBER,
                          p_SessionID     IN NUMBER,
                          p_ParallelLevel IN NUMBER)
  IS
    v_task_name      VARCHAR2(30);
    v_sql_chunks     CLOB;
    v_sql_query      CLOB;
    v_sql_query_add  CLOB;
    v_sql_process    VARCHAR2(400);
    v_try            NUMBER(5) := 0;
    v_status         NUMBER;
    v_sign           NUMBER := 0;
    v_cnt            NUMBER := 0;
    v_FIID           NUMBER;
    v_ParallelLevel  NUMBER := p_ParallelLevel;
  BEGIN

    DELETE FROM DSCREPFI_TMP;

    IF p_FIID > 0 OR p_ParallelLevel <= 0 THEN --Без распараллеливания

      ProcessFI(p_RepDate, p_FIID, p_SSPU, p_SSSD, p_ASCB, p_BPP, p_PVO, p_PKU, p_PDO, p_SessionID);
    ELSE

      v_sql_query := 'INSERT INTO DSCREPFI_TMP (T_FIID) ' ||
                     'SELECT DISTINCT avr.t_FIID ' ||
                     '  FROM davoiriss_dbt avr ' ||
                     ' WHERE EXISTS (SELECT 1 ' ||
                     '                 FROM dpmwrtsum_dbt lot ' ||
                     '                WHERE lot.t_FIID = avr.t_FIID AND lot.t_Party = -1';


      v_sql_query_add := '';
      IF p_SSPU <> 0 OR p_SSSD <> 0 OR p_ASCB <> 0 OR p_BPP <> 0 OR p_PVO <> 0 OR p_PKU <> 0 OR p_PDO <> 0 THEN
        v_sql_query_add := '            AND (';

        IF p_BPP <> 0 THEN
          v_sign := 1;
          v_sql_query_add := v_sql_query_add || ' lot.t_Kind = ' || RSB_PMWRTOFF.WRTSUM_KIND_RRWAB2 ||
                                                ' AND ((     lot.t_ChangeDate <= TO_DATE('''||TO_CHAR(p_RepDate, 'DD.MM.YYYY')||''',''DD.MM.YYYY'') ' ||
                                                '        AND lot.t_State = ' || RSB_PMWRTOFF.PM_WRTSUM_SALE_BPP ||
                                                '        AND lot.t_Amount > 0 ' ||
                                                '      ) '||
                                                '   OR (     lot.t_ChangeDate > TO_DATE('''||TO_CHAR(p_RepDate, 'DD.MM.YYYY')||''',''DD.MM.YYYY'') ' ||
                                                '        AND EXISTS (SELECT 1 '||
                                                '                      FROM dpmwrtbc_dbt v ' ||
                                                '                     WHERE v.t_SumID = lot.t_SumID ' ||
                                                '                       AND v.t_Instance = (SELECT MAX(v1.t_Instance) ' ||
                                                '                                             FROM dpmwrtbc_dbt v1 ' ||
                                                '                                            WHERE v1.t_SumID = v.t_SumID ' ||
                                                '                                              AND v1.t_ChangeDate <= TO_DATE('''||TO_CHAR(p_RepDate, 'DD.MM.YYYY')||''',''DD.MM.YYYY'') ' ||
                                                '                                          ) ' ||
                                                '                       AND v.t_State = ' || RSB_PMWRTOFF.PM_WRTSUM_SALE_BPP ||
                                                '                       AND v.t_Amount > 0 ' ||
                                                '                   ) ' ||
                                                '      )' ||
                                                '     )';
        END IF;

        IF p_SSPU <> 0 THEN
          IF v_sign <> 0 THEN
            v_sql_query_add := v_sql_query_add || ' OR ';
          END IF;

          v_sign := 1;
          v_sql_query_add := v_sql_query_add || ' (     lot.t_ChangeDate <= TO_DATE('''||TO_CHAR(p_RepDate, 'DD.MM.YYYY')||''',''DD.MM.YYYY'') ' ||
                                                '   AND lot.t_Portfolio = ' || RSB_PMWRTOFF.KINDPORT_SSPU ||
                                                '   AND lot.t_State = ' || RSB_PMWRTOFF.PM_WRTSUM_FORM ||
                                                '   AND lot.t_Amount > 0 ' ||
                                                ' ) '||
                                                'OR (     lot.t_ChangeDate > TO_DATE('''||TO_CHAR(p_RepDate, 'DD.MM.YYYY')||''',''DD.MM.YYYY'') ' ||
                                                '     AND EXISTS (SELECT 1 '||
                                                '                   FROM dpmwrtbc_dbt v ' ||
                                                '                  WHERE v.t_SumID = lot.t_SumID ' ||
                                                '                    AND v.t_Instance = (SELECT MAX(v1.t_Instance) ' ||
                                                '                                          FROM dpmwrtbc_dbt v1 ' ||
                                                '                                         WHERE v1.t_SumID = v.t_SumID ' ||
                                                '                                           AND v1.t_ChangeDate <= TO_DATE('''||TO_CHAR(p_RepDate, 'DD.MM.YYYY')||''',''DD.MM.YYYY'') ' ||
                                                '                                       ) ' ||
                                                '                    AND v.t_Portfolio = ' || RSB_PMWRTOFF.KINDPORT_SSPU ||
                                                '                    AND v.t_State = ' || RSB_PMWRTOFF.PM_WRTSUM_FORM ||
                                                '                    AND v.t_Amount > 0 ' ||
                                                '                ) ' ||
                                                '   )';
        END IF;

        IF p_SSSD <> 0 THEN
          IF v_sign <> 0 THEN
            v_sql_query_add := v_sql_query_add || ' OR ';
          END IF;

          v_sign := 1;
          v_sql_query_add := v_sql_query_add || ' (     lot.t_ChangeDate <= TO_DATE('''||TO_CHAR(p_RepDate, 'DD.MM.YYYY')||''',''DD.MM.YYYY'') ' ||
                                                '   AND lot.t_Portfolio = ' || RSB_PMWRTOFF.KINDPORT_SSSD ||
                                                '   AND lot.t_State = ' || RSB_PMWRTOFF.PM_WRTSUM_FORM ||
                                                '   AND lot.t_Amount > 0 ' ||
                                                ' ) '||
                                                'OR (     lot.t_ChangeDate > TO_DATE('''||TO_CHAR(p_RepDate, 'DD.MM.YYYY')||''',''DD.MM.YYYY'') ' ||
                                                '     AND EXISTS (SELECT 1 '||
                                                '                   FROM dpmwrtbc_dbt v ' ||
                                                '                  WHERE v.t_SumID = lot.t_SumID ' ||
                                                '                    AND v.t_Instance = (SELECT MAX(v1.t_Instance) ' ||
                                                '                                          FROM dpmwrtbc_dbt v1 ' ||
                                                '                                         WHERE v1.t_SumID = v.t_SumID ' ||
                                                '                                           AND v1.t_ChangeDate <= TO_DATE('''||TO_CHAR(p_RepDate, 'DD.MM.YYYY')||''',''DD.MM.YYYY'') ' ||
                                                '                                       ) ' ||
                                                '                    AND v.t_Portfolio = ' || RSB_PMWRTOFF.KINDPORT_SSSD ||
                                                '                    AND v.t_State = ' || RSB_PMWRTOFF.PM_WRTSUM_FORM ||
                                                '                    AND v.t_Amount > 0 ' ||
                                                '                ) ' ||
                                                '   )';
        END IF;

        IF p_ASCB <> 0 THEN
          IF v_sign <> 0 THEN
            v_sql_query_add := v_sql_query_add || ' OR ';
          END IF;

          v_sign := 1;
          v_sql_query_add := v_sql_query_add || ' (     lot.t_ChangeDate <= TO_DATE('''||TO_CHAR(p_RepDate, 'DD.MM.YYYY')||''',''DD.MM.YYYY'') ' ||
                                                '   AND lot.t_Portfolio = ' || RSB_PMWRTOFF.KINDPORT_ASCB ||
                                                '   AND lot.t_State = ' || RSB_PMWRTOFF.PM_WRTSUM_FORM ||
                                                '   AND lot.t_Amount > 0 ' ||
                                                ' ) '||
                                                'OR (     lot.t_ChangeDate > TO_DATE('''||TO_CHAR(p_RepDate, 'DD.MM.YYYY')||''',''DD.MM.YYYY'') ' ||
                                                '     AND EXISTS (SELECT 1 '||
                                                '                   FROM dpmwrtbc_dbt v ' ||
                                                '                  WHERE v.t_SumID = lot.t_SumID ' ||
                                                '                    AND v.t_Instance = (SELECT MAX(v1.t_Instance) ' ||
                                                '                                          FROM dpmwrtbc_dbt v1 ' ||
                                                '                                         WHERE v1.t_SumID = v.t_SumID ' ||
                                                '                                           AND v1.t_ChangeDate <= TO_DATE('''||TO_CHAR(p_RepDate, 'DD.MM.YYYY')||''',''DD.MM.YYYY'') ' ||
                                                '                                       ) ' ||
                                                '                    AND v.t_Portfolio = ' || RSB_PMWRTOFF.KINDPORT_ASCB ||
                                                '                    AND v.t_State = ' || RSB_PMWRTOFF.PM_WRTSUM_FORM ||
                                                '                    AND v.t_Amount > 0 ' ||
                                                '                ) ' ||
                                                '   )';
        END IF;

        IF p_PKU <> 0 THEN
          IF v_sign <> 0 THEN
            v_sql_query_add := v_sql_query_add || ' OR ';
          END IF;

          v_sign := 1;
          v_sql_query_add := v_sql_query_add || ' (     lot.t_ChangeDate <= TO_DATE('''||TO_CHAR(p_RepDate, 'DD.MM.YYYY')||''',''DD.MM.YYYY'') ' ||
                                                '   AND lot.t_Portfolio = ' || RSB_PMWRTOFF.KINDPORT_CONTR ||
                                                '   AND lot.t_State = ' || RSB_PMWRTOFF.PM_WRTSUM_FORM ||
                                                '   AND lot.t_Amount > 0 ' ||
                                                ' ) '||
                                                'OR (     lot.t_ChangeDate > TO_DATE('''||TO_CHAR(p_RepDate, 'DD.MM.YYYY')||''',''DD.MM.YYYY'') ' ||
                                                '     AND EXISTS (SELECT 1 '||
                                                '                   FROM dpmwrtbc_dbt v ' ||
                                                '                  WHERE v.t_SumID = lot.t_SumID ' ||
                                                '                    AND v.t_Instance = (SELECT MAX(v1.t_Instance) ' ||
                                                '                                          FROM dpmwrtbc_dbt v1 ' ||
                                                '                                         WHERE v1.t_SumID = v.t_SumID ' ||
                                                '                                           AND v1.t_ChangeDate <= TO_DATE('''||TO_CHAR(p_RepDate, 'DD.MM.YYYY')||''',''DD.MM.YYYY'') ' ||
                                                '                                       ) ' ||
                                                '                    AND v.t_Portfolio = ' || RSB_PMWRTOFF.KINDPORT_CONTR ||
                                                '                    AND v.t_State = ' || RSB_PMWRTOFF.PM_WRTSUM_FORM ||
                                                '                    AND v.t_Amount > 0 ' ||
                                                '                ) ' ||
                                                '   )';
        END IF;

        IF p_PDO <> 0 THEN
          IF v_sign <> 0 THEN
            v_sql_query_add := v_sql_query_add || ' OR ';
          END IF;

          v_sign := 1;
          v_sql_query_add := v_sql_query_add || ' (     lot.t_ChangeDate <= TO_DATE('''||TO_CHAR(p_RepDate, 'DD.MM.YYYY')||''',''DD.MM.YYYY'') ' ||
                                                '   AND lot.t_Portfolio = ' || RSB_PMWRTOFF.KINDPORT_PROMISSORY ||
                                                '   AND lot.t_State = ' || RSB_PMWRTOFF.PM_WRTSUM_FORM ||
                                                '   AND lot.t_Amount > 0 ' ||
                                                ' ) '||
                                                'OR (     lot.t_ChangeDate > TO_DATE('''||TO_CHAR(p_RepDate, 'DD.MM.YYYY')||''',''DD.MM.YYYY'') ' ||
                                                '     AND EXISTS (SELECT 1 '||
                                                '                   FROM dpmwrtbc_dbt v ' ||
                                                '                  WHERE v.t_SumID = lot.t_SumID ' ||
                                                '                    AND v.t_Instance = (SELECT MAX(v1.t_Instance) ' ||
                                                '                                          FROM dpmwrtbc_dbt v1 ' ||
                                                '                                         WHERE v1.t_SumID = v.t_SumID ' ||
                                                '                                           AND v1.t_ChangeDate <= TO_DATE('''||TO_CHAR(p_RepDate, 'DD.MM.YYYY')||''',''DD.MM.YYYY'') ' ||
                                                '                                       ) ' ||
                                                '                    AND v.t_Portfolio = ' || RSB_PMWRTOFF.KINDPORT_PROMISSORY ||
                                                '                    AND v.t_State = ' || RSB_PMWRTOFF.PM_WRTSUM_FORM ||
                                                '                    AND v.t_Amount > 0 ' ||
                                                '                ) ' ||
                                                '   )';
        END IF;

        IF p_PVO <> 0 THEN
          IF v_sign <> 0 THEN
            v_sql_query_add := v_sql_query_add || ' OR ';
          END IF;

          v_sign := 1;
          v_sql_query_add := v_sql_query_add || ' (     lot.t_ChangeDate <= TO_DATE('''||TO_CHAR(p_RepDate, 'DD.MM.YYYY')||''',''DD.MM.YYYY'') ' ||
                                                '   AND lot.t_Portfolio = ' || RSB_PMWRTOFF.KINDPORT_BACK ||
                                                ' ) '||
                                                'OR (     lot.t_ChangeDate > TO_DATE('''||TO_CHAR(p_RepDate, 'DD.MM.YYYY')||''',''DD.MM.YYYY'') ' ||
                                                '     AND EXISTS (SELECT 1 '||
                                                '                   FROM dpmwrtbc_dbt v ' ||
                                                '                  WHERE v.t_SumID = lot.t_SumID ' ||
                                                '                    AND v.t_Instance = (SELECT MAX(v1.t_Instance) ' ||
                                                '                                          FROM dpmwrtbc_dbt v1 ' ||
                                                '                                         WHERE v1.t_SumID = v.t_SumID ' ||
                                                '                                           AND v1.t_ChangeDate <= TO_DATE('''||TO_CHAR(p_RepDate, 'DD.MM.YYYY')||''',''DD.MM.YYYY'') ' ||
                                                '                                       ) ' ||
                                                '                    AND v.t_Portfolio = ' || RSB_PMWRTOFF.KINDPORT_BACK ||
                                                '                ) ' ||
                                                '   )';
        END IF;

        v_sql_query_add := v_sql_query_add || ' ) ';
      ELSE
        v_sql_query_add := ' AND 1 <> 1 ';
      END IF;

      v_sql_query  := v_sql_query || v_sql_query_add ||
                      '               ) ';

      EXECUTE IMMEDIATE(v_sql_query);

      SELECT COUNT(1) INTO v_cnt FROM DSCREPFI_TMP;

      IF v_cnt < p_ParallelLevel THEN
        v_ParallelLevel := v_cnt;
      END IF;

      IF v_cnt > 0 THEN
        v_sql_chunks := 'SELECT DISTINCT t_FIID, ' || TO_CHAR(p_SessionID) || ' FROM DSCREPFI_TMP ';

        v_task_name := DBMS_PARALLEL_EXECUTE.generate_task_name;
        DBMS_PARALLEL_EXECUTE.create_task (task_name => v_task_name);





        DBMS_PARALLEL_EXECUTE.create_chunks_by_sql(task_name => v_task_name,
                                                   sql_stmt  => v_sql_chunks,
                                                   by_rowid  => FALSE);

        v_sql_process := 'CALL RSB_REPRECLOTS.ProcessFI(TO_DATE('''||TO_CHAR(p_RepDate, 'DD.MM.YYYY')||''',''DD.MM.YYYY''), '||
                                                        ':start_id, ' ||
                                                        TO_CHAR(p_SSPU) || ', '||
                                                        TO_CHAR(p_SSSD) || ', '||
                                                        TO_CHAR(p_ASCB) || ', '||
                                                        TO_CHAR(p_BPP ) || ', '||
                                                        TO_CHAR(p_PVO ) || ', '||
                                                        TO_CHAR(p_PKU ) || ', '||
                                                        TO_CHAR(p_PDO ) || ', '||
                                                        ':end_id '||')';

        DBMS_PARALLEL_EXECUTE.run_task(task_name => v_task_name,
                                       sql_stmt => v_sql_process,
                                       language_flag => DBMS_SQL.NATIVE,
                                       parallel_level => v_ParallelLevel);

        v_status := DBMS_PARALLEL_EXECUTE.task_status(v_task_name);
        WHILE(v_try < 2 AND v_status != DBMS_PARALLEL_EXECUTE.FINISHED)
        LOOP
          v_try := v_try + 1;
          DBMS_PARALLEL_EXECUTE.resume_task(v_task_name);
          v_status := DBMS_PARALLEL_EXECUTE.task_status(v_task_name);
        END LOOP;

        DBMS_PARALLEL_EXECUTE.drop_task(v_task_name);
      END IF;

    END IF;


  END CreateAllData;

END RSB_REPRECLOTS;
/
