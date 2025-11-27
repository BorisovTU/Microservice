-- Author  : IAl
  -- Created : 15.09.2005
  -- Purpose : Secur
CREATE OR REPLACE PACKAGE BODY Rsb_Secur
IS
  LastErrorMessage VARCHAR2(1024) := '';

  TYPE ArrDates_t IS TABLE OF DATE;
  TYPE arrtmp_t IS TABLE OF DXIRR_TMP%ROWTYPE;

  PROCEDURE RSI_InitError
  AS
  BEGIN
     LastErrorMessage := '';
  END;

  PROCEDURE RSI_SetError( ErrNum IN INTEGER, ErrMes IN VARCHAR2 DEFAULT NULL )
  AS
  BEGIN
     IF( ErrMes IS NULL ) THEN
        LastErrorMessage := '';
     ELSE
        LastErrorMessage := ErrMes;
     END IF;
     RAISE_APPLICATION_ERROR( ErrNum,'' );
  END;

  PROCEDURE RSI_GetLastErrorMessage( ErrMes OUT VARCHAR2 )
  AS
  BEGIN
     ErrMes := LastErrorMessage;
  END;

-----------------------------------------------------------------------------------------------------------------------
  FUNCTION SecurKind( AvoirKind IN NUMBER, DeposReceiptAsShare IN NUMBER DEFAULT 1 )
    RETURN  NUMBER DETERMINISTIC IS
     AvoirKind_v NUMBER;
  BEGIN

    IF( RSI_RSB_FIInstr.FI_AvrKindsGetRoot( 2, AvoirKind ) = RSI_RSB_FIInstr.AVOIRKIND_SHARE OR
        (DeposReceiptAsShare = 1 AND RSI_RSB_FIInstr.FI_AvrKindsGetRoot( 2, AvoirKind ) = AVOIRKIND_DEPOS_RECEIPT) ) THEN
      AvoirKind_v := AVOIRKIND_EQUITY_SHARE;
    ELSIF( RSI_RSB_FIInstr.FI_AvrKindsGetRoot( 2, AvoirKind ) = RSI_RSB_FIInstr.AVOIRKIND_BOND ) THEN
      AvoirKind_v := AVOIRKIND_ORDINARY_BOND;
    ELSE
      AvoirKind_v := AvoirKind;
    END IF;

    RETURN AvoirKind_v;

  END SecurKind;

-----------------------------------------------------------------------------------------------------------------------
  FUNCTION BondKind( in_FIID IN NUMBER )
    RETURN VARCHAR2 DETERMINISTIC
  IS
    v_AvoirKind NUMBER;
    v_ValCat davrkinds_dbt.t_NumList%TYPE;
  BEGIN

    v_ValCat := '';

    SELECT t_AvoirKind
      INTO v_AvoirKind
      FROM dfininstr_dbt
     WHERE t_FIID = in_FIID;

    IF v_AvoirKind <> 0 THEN
      IF SecurKind(v_AvoirKind) = AVOIRKIND_ORDINARY_BOND THEN

        SELECT t_NumList
          INTO v_ValCat
          FROM davrkinds_dbt
         WHERE t_FI_Kind = 2 and
               t_AvoirKind = v_AvoirKind;
      ELSE
        v_ValCat := '';
      END IF;
    ELSE
      v_ValCat := '';
    END IF;

    RETURN v_ValCat;

    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        RETURN '';

  END BondKind;

-----------------------------------------------------------------------------------------------------------------------
  FUNCTION AmortizationMethod( in_ClientID IN NUMBER, in_ContractID IN NUMBER DEFAULT 0 )
    RETURN NUMBER DETERMINISTIC
  IS
    v_MethodID NUMBER;
    v_ClientID NUMBER;
  BEGIN

    IF in_ClientID = -1 THEN
      v_ClientID := 0; -- наш банк
    ELSE
      v_ClientID := in_ClientID;
    END IF;

    SELECT t_MethodID
      INTO v_MethodID
      FROM dpmwrtmet_dbt
     WHERE t_Party = v_ClientID;

    RETURN v_MethodID;

    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        RETURN -1;

  END AmortizationMethod;

-----------------------------------------------------------------------------------------------------------------------
  FUNCTION get_RestAmount( DocKind IN NUMBER,
                           DocID   IN NUMBER,
                           PartNum IN NUMBER,
                           ReportDate IN DATE )
    RETURN  NUMBER IS
     SaleAmount    NUMBER;
     LotAmount     NUMBER;
  BEGIN
-- временная мера для избавления от лишних линков исключим погашения
-- правда потом нужно будет всетаки учитывать погашения выпусков
/*
     begin
       SELECT SUM(Lnk.T_AMOUNT) INTO SaleAmount
         FROM dpmwrtlnk_dbt Lnk
        WHERE Lnk.T_DOCIDBUY = DocID AND
              Lnk.T_DOCKINDBUY = DocKind AND
              Lnk.T_PARTNUMBUY = PartNum AND
              Lnk.T_DOCKINDSALE != 117 AND
              Lnk.T_CREATEDATE > ReportDate;
     exception
        when NO_DATA_FOUND then SaleAmount := 0;
     end;
--
     SELECT T_AMOUNT INTO LotAmount
       FROM dpmwrtsum_dbt
      WHERE T_DOCID = DocID AND
            T_DOCKIND = DocKind AND
            T_PARTNUM = PartNum AND
            T_BUY_SALE = 0;
--
     IF( SaleAmount > 0 ) THEN
         RETURN (LotAmount + SaleAmount);
      ELSE
         RETURN (LotAmount);
     END IF;
*/
     RETURN 0;

  END get_RestAmount;
-----------------------------------------------------------------------------------------------------------------------
  FUNCTION get_BalanceCost( DocKind IN NUMBER,
                            DocID   IN NUMBER,
                            PartNum IN NUMBER,
                            ReportDate IN DATE )
    RETURN  NUMBER IS
     BalanceCost   NUMBER;
     SaleBalCost   NUMBER;
     OverBalCost   NUMBER;
     InstanceOv    NUMBER;
     OvDate        DATE;
/*
     CURSOR WrtBc IS ( SELECT WrtBc.T_INSTANCE, WrtBc.T_OLDSUM, WrtBc.T_CHANGEDATE
                         FROM DPMWRTBC_DBT WrtBc
                        WHERE  WrtBc.T_DOCID = DocID AND WrtBc.T_DOCKIND = DocKind AND WrtBc.T_PARTNUM = PartNum
                               AND WrtBc.T_BUY_SALE = 0 AND WrtBc.T_ACTION = 3 AND WrtBc.T_CHANGEDATE > ReportDate );
     CURSOR BuyLot  IS ( SELECT Lot.T_BALANCECOST, Lot.T_SUM, Lot.T_PARTY
                           FROM DPMWRTSUM_DBT Lot
                          WHERE Lot.T_DOCKIND = DocKind AND Lot.T_DOCID = DocID AND Lot.T_PARTNUM = PartNum );
     CURSOR SaleLot IS ( SELECT Sale.T_BALANCECOST, Sale.T_INSTANCEBUY, Sale.T_CREATEDATE
                           FROM DPMWRTLNK_DBT Sale
                          WHERE Sale.T_CREATEDATE > ReportDate AND Sale.T_DOCKINDBUY = DocKind AND Sale.T_DOCIDBUY = DocID AND Sale.T_PARTNUMBUY = PartNum );
*/
  BEGIN
/*
    InstanceOv := -1;
--
    FOR Lot IN BuyLot LOOP
      IF( Lot.T_PARTY > 0 ) THEN
         BalanceCost := Lot.T_SUM;
      ELSE
         BalanceCost := Lot.T_BALANCECOST;
      END IF;
    END LOOP;
--
   FOR over IN WrtBc LOOP
     IF( (over.T_OLDSUM IS NOT NULL) AND (OvDate < over.T_CHANGEDATE) ) THEN
         OverBalCost := over.T_OLDSUM;
         InstanceOv := over.T_INSTANCE;
         OvDate := over.T_CHANGEDATE;
     END IF;
   END LOOP;

   IF( OverBalCost > 0 ) THEN
      BalanceCost := OverBalCost;
   END IF;
--
   FOR Sale IN SaleLot LOOP
     IF( InstanceOv > 0) THEN
        IF( (Sale.T_CREATEDATE <= OvDate) AND (Sale.T_INSTANCEBUY < InstanceOv) ) THEN
           BalanceCost := BalanceCost + Sale.T_BALANCECOST;
        END IF;
      ELSE
       BalanceCost := BalanceCost + Sale.T_BALANCECOST;
     END IF;
   END LOOP;
          */
   RETURN 0;

  END get_BalanceCost;
-----------------------------------------------------------------------------------------------------------------------
  FUNCTION get_RestNKD( DocKind IN NUMBER,
                        DocID   IN NUMBER,
                        PartNum IN NUMBER,
                        ReportDate IN DATE )
    RETURN  NUMBER IS
     PayNKD       NUMBER;
/*
     CURSOR BuyLot  IS ( SELECT Lot.T_PARTY, Lot.T_NKDAMOUNT
                           FROM DPMWRTSUM_DBT Lot
                          WHERE Lot.T_DOCKIND = DocKind AND Lot.T_DOCID = DocID AND Lot.T_PARTNUM = PartNum );
     CURSOR SaleLot IS ( SELECT Sale.T_NKDSALEAMOUNT, Sale.T_NKDBUYAMOUNT
                           FROM DPMWRTLNK_DBT Sale
                          WHERE Sale.T_CREATEDATE > ReportDate AND Sale.T_DOCKINDBUY = DocKind AND Sale.T_DOCIDBUY = DocID AND Sale.T_PARTNUMBUY = PartNum );
*/
  BEGIN
/*
--
    FOR Lot IN BuyLot LOOP
      PayNKD := Lot.T_NKDAMOUNT;
    END LOOP;
--
    FOR Sale IN SaleLot LOOP
      PayNKD := PayNKD + Sale.T_NKDBUYAMOUNT;
    END LOOP;
*/
    RETURN 0;

  END get_RestNKD;
-----------------------------------------------------------------------------------------------------------------------
  FUNCTION get_OperationGroup( oper IN doprkoper_dbt.T_SYSTYPES%TYPE )
    RETURN NUMBER IS
      OGroup NUMBER;
  BEGIN

    OGroup := 0;
    --Определяем тип операции
    IF( instr( oper, TYPEKOPER_CONVERS ) > 0 ) THEN
       OGroup := RSI_rsb_bitwise.b_or( OGroup, TO_NUMBER(DT_CONVERS,'XXXXXXXXXX'));
    ELSE IF( instr( oper, TYPEKOPER_SWAP ) > 0 ) THEN
            OGroup := RSI_rsb_bitwise.b_or( OGroup, TO_NUMBER(DT_SWAP,'XXXXXXXXXX'));
         ELSE IF( instr( oper, TYPEKOPER_OPTION ) > 0 ) THEN
                 OGroup := RSI_rsb_bitwise.b_or( OGroup, TO_NUMBER(DT_OPTION,'XXXXXXXXXX'));
              ELSE IF( instr( oper, TYPEKOPER_FUTURES ) > 0 ) THEN
                      OGroup := RSI_rsb_bitwise.b_or( OGroup, TO_NUMBER(DT_FUTURES,'XXXXXXXXXX'));
                   ELSE IF( instr( oper, TYPEKOPER_MOVING ) > 0 ) THEN
                           OGroup := RSI_rsb_bitwise.b_or( OGroup, TO_NUMBER(DT_MOVING,'XXXXXXXXXX'));
                        ELSE IF( instr( oper, TYPEKOPER_MMARK ) > 0 ) THEN
                                OGroup := RSI_rsb_bitwise.b_or( OGroup, TO_NUMBER(DT_MOVING,'XXXXXXXXXX'));
                             END IF;
                        END IF;
                   END IF;
              END IF;
         END IF;
    END IF;
    --Определяем направление либо (для ЦБ) тип погашения
    IF( instr( oper, TYPEKOPER_SALE ) > 0 ) THEN
       OGroup := RSI_rsb_bitwise.b_or( OGroup, TO_NUMBER(IS_SALE,'XXXXXXXXXX'));
    ELSE IF( instr( oper, TYPEKOPER_BUY ) > 0 ) THEN
            OGroup := RSI_rsb_bitwise.b_or( OGroup, TO_NUMBER(IS_BUY,'XXXXXXXXXX'));
         ELSE IF( instr( oper, TYPEKOPER_ISSUE ) > 0 ) THEN
                 OGroup := RSI_rsb_bitwise.b_or( OGroup, TO_NUMBER(IS_RET_ISSUE,'XXXXXXXXXX'));
              ELSE IF( instr( oper, TYPEKOPER_COUPON ) > 0 ) THEN
                      OGroup := RSI_rsb_bitwise.b_or( OGroup, TO_NUMBER(IS_RET_COUPON,'XXXXXXXXXX'));
                   ELSE IF( instr( oper, TYPEKOPER_AVRWRTIN ) > 0 ) THEN
                           OGroup := RSI_rsb_bitwise.b_or( OGroup, TO_NUMBER(IS_AVRWRTIN,'XXXXXXXXXX'));
                        ELSE IF( instr( oper, TYPEKOPER_AVRWRTOUT ) > 0 ) THEN
                                OGroup := RSI_rsb_bitwise.b_or( OGroup, TO_NUMBER(IS_AVRWRTOUT,'XXXXXXXXXX'));
                             ELSE IF( instr( oper, TYPEKOPER_CONV_SHARE ) > 0 ) THEN
                                     OGroup := RSI_rsb_bitwise.b_or( OGroup, TO_NUMBER(IS_CONV_SHARE,'XXXXXXXXXX'));
                                  ELSE IF( instr( oper, TYPEKOPER_CONV_RECEIPT ) > 0 ) THEN
                                          OGroup := RSI_rsb_bitwise.b_or( OGroup, TO_NUMBER(IS_CONV_RECEIPT,'XXXXXXXXXX'));
                                       END IF;
                                  END IF;
                             END IF;
                        END IF;
                   END IF;
              END IF;
         END IF;
    END IF;
    --Всякие разные признаки
    IF( instr( oper, TYPEKOPER_PUT ) > 0 ) THEN
       OGroup := RSI_rsb_bitwise.b_or( OGroup, TO_NUMBER(IS_PUT,'XXXXXXXXXX'));
    END IF;
    IF( instr( oper, TYPEKOPER_EXCHANGE ) > 0 ) THEN
       OGroup := RSI_rsb_bitwise.b_or( OGroup, TO_NUMBER(IS_EXCHANGE,'XXXXXXXXXX'));
    END IF;
    IF( instr( oper, TYPEKOPER_OUTEXCHANGE ) > 0 ) THEN
       OGroup := RSI_rsb_bitwise.b_or( OGroup, TO_NUMBER(IS_OUTEXCHANGE,'XXXXXXXXXX'));
    END IF;
    IF( instr( oper, TYPEKOPER_OTC ) > 0 ) THEN
       OGroup := RSI_rsb_bitwise.b_or( OGroup, TO_NUMBER(IS_OTC,'XXXXXXXXXX'));
    END IF;
    IF( instr( oper, TYPEKOPER_BROKER ) > 0 ) THEN
       OGroup := RSI_rsb_bitwise.b_or( OGroup, TO_NUMBER(IS_BROKER,'XXXXXXXXXX'));
    END IF;
    IF( instr( oper, TYPEKOPER_PARTLY ) > 0 ) THEN
       OGroup := RSI_rsb_bitwise.b_or( OGroup, TO_NUMBER(IS_RET_PARTLY,'XXXXXXXXXX'));
    END IF;
    IF( instr( oper, TYPEKOPER_ADDINCOME ) > 0 ) THEN
       OGroup := RSI_rsb_bitwise.b_or( OGroup, TO_NUMBER(IS_RET_ADDINCOME,'XXXXXXXXXX'));
    END IF;
    IF( instr( oper, TYPEKOPER_BACKSALE ) > 0 ) THEN
       OGroup := RSI_rsb_bitwise.b_or( OGroup, TO_NUMBER(IS_BACKSALE,'XXXXXXXXXX'));
    END IF;
    IF( instr( oper, TYPEKOPER_REPO ) > 0 ) THEN
       OGroup := RSI_rsb_bitwise.b_or( OGroup, TO_NUMBER(IS_REPO,'XXXXXXXXXX'));
    END IF;
    IF( instr( oper, TYPEKOPER_FLTRATE ) > 0 ) THEN
       OGroup := RSI_rsb_bitwise.b_or( OGroup, TO_NUMBER(IS_FLTRATE,'XXXXXXXXXX'));
    END IF;
    IF( instr( oper, TYPEKOPER_INDXFLTRATE ) > 0 ) THEN
       OGroup := RSI_rsb_bitwise.b_or( OGroup, TO_NUMBER(IS_INDXFLTRATE,'XXXXXXXXXX'));
    END IF;
    IF( instr( oper, TYPEKOPER_ONCALL ) > 0 ) THEN
       OGroup := RSI_rsb_bitwise.b_or( OGroup, TO_NUMBER(IS_ONCALL,'XXXXXXXXXX'));
    END IF;
    IF( instr( oper, TYPEKOPER_FULL ) > 0 ) THEN
       OGroup := RSI_rsb_bitwise.b_or( OGroup, TO_NUMBER(IS_FULL,'XXXXXXXXXX'));
    END IF;
    IF( instr( oper, TYPEKOPER_CALL ) > 0 ) THEN
       OGroup := RSI_rsb_bitwise.b_or( OGroup, TO_NUMBER(IS_CALL,'XXXXXXXXXX'));
    END IF;
    IF( instr( oper, TYPEKOPER_EXEC_DV ) > 0 ) THEN
       OGroup := RSI_rsb_bitwise.b_or( OGroup, TO_NUMBER(IS_EXEC_DV,'XXXXXXXXXX'));
    END IF;
    IF( instr( oper, TYPEKOPER_LOAN ) > 0 ) THEN
       OGroup := RSI_rsb_bitwise.b_or( OGroup, TO_NUMBER(IS_LOAN,'XXXXXXXXXX'));
    END IF;
    IF( instr( oper, TYPEKOPER_BASKET ) > 0 ) THEN
       OGroup := RSI_rsb_bitwise.b_or( OGroup, TO_NUMBER(IS_BASKET,'XXXXXXXXXX'));
    END IF;
    IF( instr( oper, TYPEKOPER_TODAY ) > 0 ) THEN
       OGroup := RSI_rsb_bitwise.b_or( OGroup, TO_NUMBER(IS_TODAY,'XXXXXXXXXX'));
    END IF;
    IF( instr( oper, TYPEKOPER_DEAL_KSU ) > 0 ) THEN
       OGroup := RSI_rsb_bitwise.b_or( OGroup, TO_NUMBER(IS_DEAL_KSU,'XXXXXXXXXX'));
    END IF;
    IF( instr( oper, TYPEKOPER_INTERDEALER_REPO ) > 0 ) THEN
       OGroup := RSI_rsb_bitwise.b_or( OGroup, TO_NUMBER(IS_INTERDEALER_REPO,'XXXXXXXXXX'));
    END IF;


    IF( instr( oper, TYPEKOPER_DIVIDEND_RETURN_REPO ) > 0 ) THEN
       OGroup := RSI_rsb_bitwise.b_or( OGroup, TO_NUMBER(IS_DIVIDEND_RETURN_REPO,'XXXXXXXXXX'));
    END IF;

    IF( instr( oper, TYPEKOPER_GET_DIVIDEND_REPO ) > 0 ) THEN
       OGroup := RSI_rsb_bitwise.b_or( OGroup, TO_NUMBER(IS_GET_DIVIDEND_REPO,'XXXXXXXXXX'));
    END IF;
        IF( instr( oper, TYPEKOPER_GET_DIVIDEND_EMITENT ) > 0 ) THEN
       OGroup := RSI_rsb_bitwise.b_or( OGroup, TO_NUMBER(IS_GET_DIVIDEND_EMITENT,'XXXXXXXXXX'));
    END IF;

    RETURN OGroup;

  END get_OperationGroup;

  FUNCTION get_OperationGroupDocKind( oper IN doprkoper_dbt.T_SYSTYPES%TYPE, DocKind IN NUMBER )
    RETURN NUMBER IS
      OGroup NUMBER;
  BEGIN
    OGroup := 0;

    IF( DocKind = DL_DVFXDEAL ) THEN
       IF( instr(oper, TYPEKOPER_DVFX_SWAP) > 0 ) THEN
          OGroup := RSI_rsb_bitwise.b_or(OGroup, TO_NUMBER(DT_DVFX_SWAP,'XXXXXXXXXX'));
       ELSIF( instr(oper, TYPEKOPER_DVFX_DEAL) > 0 ) THEN
          OGroup := RSI_rsb_bitwise.b_or(OGroup, TO_NUMBER(DT_DVFX_DEAL,'XXXXXXXXXX'));
       END IF;
    ELSE
       OGroup := get_OperationGroup(oper);
    END IF;

    RETURN OGroup;
  END get_OperationGroupDocKind;

-----------------------------------------------------------------------------------------------------------------------
  FUNCTION get_OperSysTypes( TypeID IN NUMBER, DocKind IN NUMBER )
    RETURN doprkoper_dbt.T_SYSTYPES%TYPE RESULT_CACHE
  IS
      OGroup doprkoper_dbt.T_SYSTYPES%TYPE;
  BEGIN
    SELECT T_SYSTYPES INTO OGroup FROM DOPRKOPER_DBT WHERE T_KIND_OPERATION = TypeID;
    RETURN OGroup;
  END get_OperSysTypes;
-----------------------------------------------------------------------------------------------------------------------
-- Анализ флагов группы видов операций, флаг задается числом
  FUNCTION check_Group( OGroup IN NUMBER,
                        Mask   IN NUMBER  )
    RETURN NUMBER IS
  BEGIN
    IF( RSI_rsb_bitwise.b_and(OGroup, Mask) > 0 ) THEN
       RETURN 1;
    ELSE
       RETURN 0;
    END IF;
  END check_Group;
-----------------------------------------------------------------------------------------------------------------------
-- Анализ флагов группы видов операций, флаг задается строкой в которой записано число в HEX формате
  FUNCTION check_GroupStr( OGroup IN NUMBER,
                           Mask   IN VARCHAR2  )
    RETURN NUMBER DETERMINISTIC IS
  BEGIN
    IF( RSI_rsb_bitwise.b_and(OGroup, TO_NUMBER(Mask,'XXXXXXXXXX')) > 0 ) THEN
       RETURN 1;
    ELSE
       RETURN 0;
    END IF;
  END check_GroupStr;
-----------------------------------------------------------------------------------------------------------------------
-- Анализ флагов группы видов операций
  FUNCTION IsBuy( OGroup IN NUMBER )
    RETURN NUMBER DETERMINISTIC IS
  BEGIN
    RETURN ( check_GroupStr(OGroup, IS_BUY));
  END IsBuy;
-----------------------------------------------------------------------------------------------------------------------
  FUNCTION IsSale( OGroup IN NUMBER )
    RETURN NUMBER DETERMINISTIC IS
  BEGIN
    RETURN ( check_GroupStr(OGroup, IS_SALE));
  END IsSale;
-----------------------------------------------------------------------------------------------------------------------
  FUNCTION IsRepo( OGroup IN NUMBER )
    RETURN NUMBER DETERMINISTIC IS
  BEGIN
    RETURN ( check_GroupStr(OGroup, IS_REPO));
  END IsRepo;
-----------------------------------------------------------------------------------------------------------------------
  FUNCTION IsFUTURES( OGroup IN NUMBER )
    RETURN NUMBER DETERMINISTIC IS
  BEGIN
    RETURN ( check_GroupStr(OGroup, DT_FUTURES));
  END IsFUTURES;
-----------------------------------------------------------------------------------------------------------------------
  FUNCTION IsDVFXSWAP( OGroup IN NUMBER )
    RETURN NUMBER DETERMINISTIC IS
  BEGIN
    RETURN ( check_GroupStr(OGroup, DT_DVFX_SWAP));
  END IsDVFXSWAP;

-----------------------------------------------------------------------------------------------------------------------
 FUNCTION IsDVFXDEAL( OGroup IN NUMBER )
 RETURN NUMBER DETERMINISTIC IS
 BEGIN
 RETURN ( check_GroupStr(OGroup, DT_DVFX_DEAL));
 END IsDVFXDEAL;

 FUNCTION IsSHORTSWAP( OGroup IN NUMBER )
 RETURN NUMBER DETERMINISTIC IS
 BEGIN
 RETURN ( check_GroupStr(OGroup, DT_SHORT_SWAP));
 END IsSHORTSWAP;

  FUNCTION DealIsRepo( DealID IN NUMBER )
    RETURN NUMBER DETERMINISTIC IS

    v_IsRepo NUMBER := 0;
  BEGIN

    SELECT RSB_SECUR.IsRepo(RSB_SECUR.get_OperationGroup(RSB_SECUR.get_OperSysTypes(t_DealType, t_BofficeKind)))
      INTO v_IsRepo
      FROM DDL_TICK_DBT
     WHERE T_DEALID = DealID;

    RETURN v_IsRepo;

    EXCEPTION
     WHEN OTHERS THEN RETURN 0;
  END DealIsRepo;

-----------------------------------------------------------------------------------------------------------------------
  FUNCTION IsBasket( OGroup IN NUMBER )
    RETURN NUMBER DETERMINISTIC IS
  BEGIN
    RETURN ( check_GroupStr(OGroup, IS_BASKET));
  END IsBasket;
-----------------------------------------------------------------------------------------------------------------------
  FUNCTION IsDealKSU( OGroup IN NUMBER )
    RETURN NUMBER DETERMINISTIC IS
  BEGIN
    RETURN ( check_GroupStr(OGroup, IS_DEAL_KSU));
  END IsDealKSU;
-----------------------------------------------------------------------------------------------------------------------
  FUNCTION IsInterDealerRepo( OGroup IN NUMBER )
    RETURN NUMBER DETERMINISTIC IS
  BEGIN
    RETURN ( check_GroupStr(OGroup, IS_INTERDEALER_REPO));
  END IsInterDealerRepo;
-----------------------------------------------------------------------------------------------------------------------
  FUNCTION IsBackSale( OGroup IN NUMBER )
    RETURN NUMBER DETERMINISTIC IS
  BEGIN
    RETURN ( check_GroupStr(OGroup, IS_BACKSALE));
  END IsBackSale;
-----------------------------------------------------------------------------------------------------------------------
  FUNCTION IsAvrWrtIn( OGroup IN NUMBER )
    RETURN NUMBER DETERMINISTIC IS
  BEGIN
    RETURN ( check_GroupStr(OGroup, IS_AVRWRTIN));
  END IsAvrWrtIn;
-----------------------------------------------------------------------------------------------------------------------
  FUNCTION IsAvrWrtOut( OGroup IN NUMBER )
    RETURN NUMBER DETERMINISTIC IS
  BEGIN
    RETURN ( check_GroupStr(OGroup, IS_AVRWRTOUT));
  END IsAvrWrtOut;
-----------------------------------------------------------------------------------------------------------------------
  FUNCTION IsExchange( OGroup IN NUMBER, isExcludeOTC IN NUMBER DEFAULT 0 )
    RETURN NUMBER DETERMINISTIC IS
  BEGIN
    IF check_GroupStr(OGroup, IS_EXCHANGE) = 1 AND ((isExcludeOTC = 0) OR (isExcludeOTC = 1 AND check_GroupStr(OGroup, IS_OTC) = 0)) THEN
      RETURN 1;
    END IF;
    RETURN 0;
  END;
-----------------------------------------------------------------------------------------------------------------------
  FUNCTION IsRet_Coupon( OGroup IN NUMBER )
    RETURN NUMBER DETERMINISTIC IS
  BEGIN
    RETURN ( check_GroupStr(OGroup, IS_RET_COUPON));
  END;
-----------------------------------------------------------------------------------------------------------------------
  FUNCTION IsRet_Partly( OGroup IN NUMBER )
    RETURN NUMBER DETERMINISTIC IS
  BEGIN
    RETURN ( check_GroupStr(OGroup, IS_RET_PARTLY));
  END;
  
-----------------------------------------------------------------------------------------------------------------------
  FUNCTION IsRet_ADDINCOME( OGroup IN NUMBER )
    RETURN NUMBER DETERMINISTIC IS
  BEGIN
    RETURN ( check_GroupStr(OGroup, IS_RET_ADDINCOME));
  END;
  
-----------------------------------------------------------------------------------------------------------------------
  FUNCTION IsRet_Issue( OGroup IN NUMBER )
    RETURN NUMBER DETERMINISTIC IS
  BEGIN
    RETURN ( check_GroupStr(OGroup, IS_RET_ISSUE));
  END;
-----------------------------------------------------------------------------------------------------------------------
  FUNCTION IsLoan( OGroup IN NUMBER )
    RETURN NUMBER DETERMINISTIC IS
  BEGIN
    RETURN ( check_GroupStr(OGroup, IS_LOAN));
  END;

  FUNCTION DealIsLoan( DealID IN NUMBER )
    RETURN NUMBER DETERMINISTIC IS

    v_IsLoan NUMBER := 0;
  BEGIN

    SELECT RSB_SECUR.IsLoan(RSB_SECUR.get_OperationGroup(RSB_SECUR.get_OperSysTypes(t_DealType, t_BofficeKind)))
      INTO v_IsLoan
      FROM DDL_TICK_DBT
     WHERE T_DEALID = DealID;

    RETURN v_IsLoan;

    EXCEPTION
     WHEN OTHERS THEN RETURN 0;
  END DealIsLoan;
-----------------------------------------------------------------------------------------------------------------------
  FUNCTION IsTwoPart( OGroup IN NUMBER )
    RETURN NUMBER DETERMINISTIC IS
  BEGIN
    IF((check_GroupStr(OGroup, IS_REPO) = 1) OR (check_GroupStr(OGroup, IS_LOAN) = 1)) THEN
       RETURN 1;
    ELSE
       RETURN 0;
    END IF;
  END IsTwoPart;
-----------------------------------------------------------------------------------------------------------------------
 FUNCTION IsConvShare( OGroup IN NUMBER )
 RETURN NUMBER DETERMINISTIC IS
 BEGIN
 RETURN ( check_GroupStr(OGroup, IS_CONV_SHARE));
 END;
-----------------------------------------------------------------------------------------------------------------------
 FUNCTION IsConvReceipt( OGroup IN NUMBER )
 RETURN NUMBER DETERMINISTIC IS
 BEGIN
 RETURN ( check_GroupStr(OGroup, IS_CONV_RECEIPT));
 END;
-----------------------------------------------------------------------------------------------------------------------
  FUNCTION IsBroker( OGroup IN NUMBER )
    RETURN NUMBER DETERMINISTIC IS
  BEGIN
    RETURN ( check_GroupStr(OGroup, IS_BROKER));
  END;
-----------------------------------------------------------------------------------------------------------------------
  FUNCTION IsOutExchange( OGroup IN NUMBER, isIncludeOTC IN NUMBER DEFAULT 0 )
    RETURN NUMBER DETERMINISTIC IS
  BEGIN
    IF check_GroupStr(OGroup, IS_OUTEXCHANGE) = 1 OR (isIncludeOTC = 1 AND check_GroupStr(OGroup, IS_OTC) = 1) THEN
      RETURN 1;
    END IF;
    RETURN 0;
  END;
-----------------------------------------------------------------------------------------------------------------------
  FUNCTION IsOTC( OGroup IN NUMBER )
    RETURN NUMBER DETERMINISTIC IS
  BEGIN
    RETURN ( check_GroupStr(OGroup, IS_OTC));
  END;
-----------------------------------------------------------------------------------------------------------------------
  FUNCTION IsToday( OGroup IN NUMBER )
    RETURN NUMBER DETERMINISTIC IS
  BEGIN
    RETURN ( check_GroupStr(OGroup, IS_TODAY));
  END;
  -----------------------------------------------------------------------------------------------------------------------
  FUNCTION IsDividendReturnRepo( OGroup IN NUMBER )
    RETURN NUMBER DETERMINISTIC IS
  BEGIN
    RETURN ( check_GroupStr(OGroup, IS_DIVIDEND_RETURN_REPO));
  END;
  -----------------------------------------------------------------------------------------------------------------------
  FUNCTION IsGetDividRepo( OGroup IN NUMBER )
    RETURN NUMBER DETERMINISTIC IS
  BEGIN
    RETURN ( check_GroupStr(OGroup, IS_GET_DIVIDEND_REPO));
  END;
  -----------------------------------------------------------------------------------------------------------------------
  FUNCTION IsGetDividEmitent( OGroup IN NUMBER )
    RETURN NUMBER DETERMINISTIC IS
  BEGIN
    RETURN ( check_GroupStr(OGroup, IS_GET_DIVIDEND_EMITENT));
  END;

-- Возвращает:
--      S - продажа
--      BS- покупка с обратной продажей (2 ч.)
--      SB - продажа с обратным выку-пом (1 ч.)
--      RBS - репо покупка (2 ч)
--      RSB - репо продажа. (1 ч.)
--      DI - погашение выпуска
--      DW - погашение купона
--      DP - частичное погашение
--      MS - Перемещение из портфеля
--      FS - Списание лота
--      FB - Зачисление лота
--      zS - Размещение займа
--      zB - Привлечение займа
--      CSD - Конвертация акция в ДР
--      CDS - Конвертация ДР в акции
  FUNCTION get_OperationType( DocKind IN NUMBER, KindOper IN NUMBER, REJECTDATE IN DATE DEFAULT NULL ) -- из ddl_tick`a
    RETURN VARCHAR2 IS
      OGroup  NUMBER;
      SysType doprkoper_dbt.T_SYSTYPES%TYPE;

  BEGIN

    SELECT T_SYSTYPES INTO SysType
      FROM DOPRKOPER_DBT
     WHERE T_KIND_OPERATION = KindOper AND T_DOCKIND = DocKind;

    OGroup:=get_OperationGroup(SysType);

    IF( (IsBuy(OGroup)=1) AND (IsAvrWrtIn(OGroup)!=1) ) THEN
       IF( IsRepo(OGroup)=1 AND REJECTDATE is NULL ) THEN RETURN 'RBS';
       ELSIF( IsBackSale(OGroup)=1 AND REJECTDATE is NULL ) THEN RETURN 'BS';
       ELSE RETURN 'B';
       END IF;
    ELSIF( (IsSale(OGroup)=1) AND (IsAvrWrtOut(OGroup)!=1) ) THEN
          IF( IsRepo(OGroup)=1 AND REJECTDATE is NULL ) THEN RETURN 'RSB';
          ELSIF( IsBackSale(OGroup)=1 AND REJECTDATE is NULL ) THEN RETURN 'SB';
          ELSE RETURN 'S';
          END IF;
    ELSIF( check_GroupStr(OGroup,IS_RET_ISSUE)=1) THEN RETURN 'DI';
    ELSIF( check_GroupStr(OGroup,IS_RET_COUPON)=1) THEN RETURN 'DC';
    ELSIF( check_GroupStr(OGroup,IS_RET_PARTLY)=1) THEN RETURN 'DP';
    ELSIF( check_GroupStr(OGroup,IS_AVRWRTIN)=1) THEN RETURN 'FB';
    ELSIF( check_GroupStr(OGroup,IS_AVRWRTOUT)=1) THEN RETURN 'FS';
    ELSIF( check_GroupStr(OGroup,IS_LOAN)=1 AND IsBuy(OGroup)=1) THEN RETURN 'zB';
    ELSIF( check_GroupStr(OGroup,IS_LOAN)=1 AND IsSale(OGroup)=1) THEN RETURN 'zS';
    ELSIF( check_GroupStr(OGroup,IS_CONV_SHARE)=1) THEN RETURN 'CSD';
    ELSIF( check_GroupStr(OGroup,IS_CONV_RECEIPT)=1) THEN RETURN 'CDS';
    END IF;

    RETURN 'U';

  END get_OperationType;

-----------------------------------------------------------------------------------------------------------------------

  FUNCTION get_DealPartBuyType( TypeID  IN NUMBER,
                                DocKind IN NUMBER,
                                IsBack  IN NUMBER )
    RETURN NUMBER IS
      OGroup  NUMBER;

  BEGIN

    OGroup := get_OperationGroup(get_OperSysTypes(TypeID,DocKind));
    IF( ( (IsSale(OGroup)=1) AND (IsBack=0) ) OR ( (IsBuy(OGroup)=1) AND (IsBack=1) ) OR ( IsAvrWrtOut(OGroup)=1 ) ) THEN
       RETURN 0;
    END IF;

    IF( ( (IsSale(OGroup)=1) AND (IsBack=1) ) OR ( (IsBuy(OGroup)=1) AND (IsBack=0) ) OR ( IsAvrWrtIn(OGroup)=1 ) ) THEN
       RETURN 1;
    END IF;

    RETURN -1;
  END get_DealPartBuyType;

-----------------------------------------------------------------------------------------------------------------------

  FUNCTION get_DrawingOperation( FIID IN NUMBER,
                                 NUM IN VARCHAR2,
                                 IsPartial IN VARCHAR2 )
    RETURN NUMBER IS
     CURSOR Ticket IS ( SELECT T_DEALID, OperType.T_SYSTYPES AS T_SYSTYPES, T_NUMBER_COUPON, T_NUMBER_PARTLY, T_DEALTYPE, T_BOFFICEKIND
                           FROM DDL_TICK_DBT
                           LEFT JOIN DOPRKOPER_DBT OperType ON T_DEALTYPE = OperType.T_KIND_OPERATION AND T_BOFFICEKIND = OperType.T_DOCKIND
                          WHERE T_BOFFICEKIND=117 );
     CURSOR DLLeg IS ( SELECT T_DEALID, T_PFI
                           FROM DDL_LEG_DBT
                          WHERE T_LEGKIND = 0 AND T_LEGID = 0 );
  BEGIN

    FOR tick IN Ticket LOOP
       IF( IsPartial = 'X' ) THEN
          IF( tick.T_NUMBER_PARTLY = NUM ) THEN
             FOR leg IN DLLeg LOOP
              IF( leg.T_PFI = FIID AND leg.T_DEALID = tick.T_DEALID ) THEN RETURN tick.T_DEALID; END IF;
             END LOOP;
          END IF;
       ELSE
          IF(tick.T_NUMBER_COUPON = NUM) THEN
             FOR leg IN DLLeg LOOP
              IF( leg.T_PFI = FIID AND leg.T_DEALID = tick.T_DEALID ) THEN RETURN tick.T_DEALID; END IF;
             END LOOP;
          END IF;
       END IF;
    END LOOP;

    FOR tick IN Ticket LOOP
       IF( check_GroupStr(get_OperationGroup(tick.T_SYSTYPES), IS_RET_ISSUE)=1 ) THEN
             FOR leg IN DLLeg LOOP
              IF( leg.T_PFI = FIID AND leg.T_DEALID = tick.T_DEALID ) THEN RETURN tick.T_DEALID; END IF;
             END LOOP;
       END IF;
    END LOOP;

    RETURN NULL;

  END get_DrawingOperation;
-----------------------------------------------------------------------------------------------------------------------
-- Возвращает ID шага операции если он имеется в данной операции
-- Можно задать либо BranchSymbol, либо ActionStep(KindAction) и DealPart
  FUNCTION get_OperStepID( OperationID IN NUMBER,
                           DocKind IN NUMBER,
                           KindOper IN NUMBER,
                           BranchSymbol IN doproblck_dbt.T_SYMBOL%TYPE,
                           ActionStep IN NUMBER,
                           DealPart IN NUMBER )
    RETURN NUMBER IS
      KindAction   NUMBER;
      NumberStep   NUMBER;
      StepID       NUMBER;
      BlockID      NUMBER;

  BEGIN

    IF( BranchSymbol IS NOT NULL ) THEN

--       SELECT T_KIND_ACTION, T_NUMBER_STEP INTO KindAction, NumberStep
--         FROM DOPROSTEP_DBT
--        WHERE T_KIND_OPERATION = KindOper AND
--              T_BLOCKID = ( SELECT T_BLOCKID FROM DOPROBLCK_DBT WHERE T_KIND_OPERATION = KindOper AND T_SYMBOL = BranchSymbol );
    SELECT T_BLOCKID INTO BlockID FROM DOPROBLCK_DBT WHERE T_KIND_OPERATION = KindOper AND T_SYMBOL = BranchSymbol;

    ELSE
       KindAction := ActionStep;

       IF( DealPart = 1 ) THEN
          SELECT MAX(T_ID_STEP) INTO StepID FROM DOPRSTEP_DBT WHERE T_ID_OPERATION = OperationID AND
                                                                       T_KIND_OPERATION = KindOper AND
                                                                       T_KIND_ACTION = KindAction AND
                                                                       T_NUMBER_STEP < 500;
       ELSE
          SELECT MAX(T_ID_STEP) INTO StepID FROM DOPRSTEP_DBT WHERE T_ID_OPERATION = OperationID AND
                                                                       T_KIND_OPERATION = KindOper AND
                                                                       T_KIND_ACTION = KindAction AND
                                                                       T_NUMBER_STEP > 500;
       END IF;

       RETURN StepID;
    END IF;

    SELECT MAX(T_ID_STEP) INTO StepID
      FROM DOPRSTEP_DBT
     WHERE T_ID_OPERATION = OperationID AND
           T_BLOCKID = BlockID;

    RETURN StepID;

  EXCEPTION
     WHEN OTHERS THEN RETURN NULL;
  END get_OperStepID;
-------------------------------------------------------
-- Находит документ по шагу операции если есть
  FUNCTION get_StepCarrySum( DealID IN NUMBER,                    --T_DEALID сделки в ddl_tick_dbt
                             DocKind IN NUMBER,                   --T_DEALTYPE сделки в ddl_tick_dbt
                             KindOper IN NUMBER,                  --T_BOFFICEKIND сделки в ddl_tick_dbt
                             ToFI     IN NUMBER,
                             BranchSymbol IN doproblck_dbt.T_SYMBOL%TYPE,
                             ActionStep IN NUMBER,
                             DealPart IN NUMBER )
    RETURN NUMBER IS
      DocID        NUMBER;
      OperationID  NUMBER;
      Summa        NUMBER(19,4);
      CarryFI      NUMBER;
      CarryDate    DATE;

  BEGIN
  -- получаем ID операции
    SELECT T_ID_OPERATION INTO OperationID
      FROM DOPROPER_DBT
     WHERE T_KIND_OPERATION = KindOper AND
           T_DOCKIND = DocKind AND
           T_DOCUMENTID = (TO_CHAR( DealID, 'FM0999999999999999999999999999999999' ));

    IF( OperationID IS NULL ) THEN
       RETURN NULL;
    END IF;
  -- получаем ID документа по операции
    SELECT T_AccTrnID INTO DocID
      FROM DOPRDOCS_DBT
     WHERE T_ID_OPERATION = OperationID AND
           T_ID_STEP = Rsb_Secur.get_OperStepID( OperationID, DocKind, KindOper, BranchSymbol, ActionStep, DealPart ) AND
           T_DOCKIND = 1;

  --Блок поиска по acctrn.dbt
    BEGIN
      SELECT T_SUM_PAYER, T_FIID_PAYER, T_DATE_CARRY INTO Summa, CarryFI, CarryDate
        FROM DACCTRN_DBT
       WHERE DocID  = T_AccTrnID;
     EXCEPTION
       WHEN NO_DATA_FOUND THEN Summa := NULL;
    END;

    Summa := RSI_RSB_FIInstr.ConvSum( Summa, CarryFI, ToFI, CarryDate);

    RETURN Summa;

   EXCEPTION
     WHEN OTHERS THEN RETURN NULL;
  END get_StepCarrySum;
-----------------------------------------------------------------------------------------------------------------------
-- Возвращает дату шага операции если IsExecute = 'X' то
-- Вернет дату выполненого шага если таковой имеет место быть
-- в противном случае вернет Плановую дату.
  FUNCTION get_OperStepDate( DealID IN NUMBER,                    --T_DEALID сделки в ddl_tick_dbt
                             DocKind IN NUMBER,                   --T_DEALTYPE сделки в ddl_tick_dbt
                             KindOper IN NUMBER,                  --T_BOFFICEKIND сделки в ddl_tick_dbt
                             BranchSymbol IN doproblck_dbt.T_SYMBOL%TYPE,
                             ActionStep IN NUMBER,
                             DealPart IN NUMBER )
    RETURN DATE IS
      OperationID  NUMBER;
      Summa        NUMBER(19,4);
      CarryFI      NUMBER;
      CarryDate    DATE;

  BEGIN
  -- получаем ID операции
    SELECT T_ID_OPERATION INTO OperationID
      FROM DOPROPER_DBT
     WHERE T_KIND_OPERATION = KindOper AND
           T_DOCKIND = DocKind AND
           T_DOCUMENTID = (TO_CHAR( DealID, 'FM0999999999999999999999999999999999' ));

    IF( OperationID IS NULL ) THEN
       RETURN NULL;
    END IF;

    BEGIN
      SELECT T_PLAN_DATE INTO CarryDate
        FROM DOPRSTEP_DBT
       WHERE T_ID_OPERATION = OperationID AND
             T_ID_STEP = Rsb_Secur.get_OperStepID( OperationID, DocKind, KindOper, BranchSymbol, ActionStep, DealPart ) AND
             T_ISEXECUTE = 'X';
     EXCEPTION
       WHEN NO_DATA_FOUND THEN CarryDate := NULL;
    END;


    RETURN CarryDate;

  exception
    when NO_DATA_FOUND then
         return NULL;
    when OTHERS then
         dbms_output.put_line('Ошибка' || SQLERRM);
         return NULL;
  END get_OperStepDate;
--------------------------------------------------------------------------------------------------------
  -- получить дату приема на обслуживание
  function get_StartServiceDate( EndPerion   IN DATE,
                                 FIID        IN NUMBER,
                                 OperDepartment IN NUMBER DEFAULT NULL ) RETURN DATE IS
  StartDate  DATE;
  EndDate    DATE;
  State      NUMBER;
  OperDprt   NUMBER;

  begin
    begin
      if OperDepartment IS NULL then
        OperDprt := RsbSessionData.OperDprt();
      else
        OperDprt := OperDepartment;
      end if;
      exception
        when OTHERS then
           dbms_output.put_line('Ошибка' || SQLERRM);
           return NULL;
    end;
    begin
      SELECT T_ServiceStartDate, T_ServiceState INTO StartDate, State FROM DAVOIRSRV_DBT WHERE T_FIID = FIID AND T_DEPARTMENT = OperDprt;
    exception
      when NO_DATA_FOUND then
           return NULL;
      when OTHERS then
           dbms_output.put_line('Ошибка' || SQLERRM);
           return NULL;
    end;

    if( ( State > 0 ) and (StartDate<=EndPerion) ) then return StartDate; end if;

    begin
      SELECT MAX(T_OperDate) INTO StartDate FROM DDEPOADMO_DBT WHERE T_OperDate <= EndPerion AND
                                                                     T_FIID = FIID AND
                                                                     T_Item = 70 AND
                                                                     T_OperKind = 10;
    exception
      when NO_DATA_FOUND then
           return NULL;
      when OTHERS then
           dbms_output.put_line('Ошибка' || SQLERRM);
           return NULL;
    end;

    return StartDate;

  exception
    when NO_DATA_FOUND then
         return NULL;
    when OTHERS then
         dbms_output.put_line('Ошибка' || SQLERRM);
         return NULL;
  end get_StartServiceDate;

--------------------------------------------------------------------------------------------------------
  -- получить дату снятия с обслуживания обслуживание
  FUNCTION get_EndServiceDate( EndPerion   IN DATE,
                               FIID        IN NUMBER,
                               OperDepartment IN NUMBER DEFAULT NULL ) RETURN DATE IS
  StartDate  DATE;
  EndDate    DATE;
  State      NUMBER;
  OperDprt   NUMBER;

  begin
    begin
      if OperDepartment IS NULL then
        OperDprt := RsbSessionData.OperDprt();
      else
        OperDprt := OperDepartment;
      end if;
      exception
        when OTHERS then
           dbms_output.put_line('Ошибка' || SQLERRM);
           return NULL;
    end;

    begin
      SELECT T_ServiceStateDate, T_ServiceState INTO EndDate, State FROM DAVOIRSRV_DBT WHERE T_FIID = FIID AND T_DEPARTMENT = OperDprt;
    exception
      when NO_DATA_FOUND then
           return NULL;
      when OTHERS then
           dbms_output.put_line('Ошибка' || SQLERRM);
           return NULL;
    end;

    if( ( State = 0 ) and (EndDate<=EndPerion) ) then return EndDate; end if;

    begin
      SELECT MAX(T_OperDate) INTO EndDate FROM DDEPOADMO_DBT WHERE T_OperDate <= EndPerion AND
                                                                   T_FIID = FIID AND
                                                                   T_Item = 70 AND
                                                                   T_OperKind = 30;
    exception
      when NO_DATA_FOUND then
           return NULL;
      when OTHERS then
           dbms_output.put_line('Ошибка' || SQLERRM);
           return NULL;
    end;

    return EndDate;

  exception
    when NO_DATA_FOUND then
         return NULL;
    when OTHERS then
         dbms_output.put_line('Ошибка' || SQLERRM);
         return NULL;
  end get_EndServiceDate;

--------------------------------------------------------------------------------------------------------
  -- расчитать резерв по сделке с цб

  FUNCTION get_CalculatedReserve( EndPerion   IN DATE,
                                  RP          IN NUMBER,  --процент риска
                                  DealID      IN NUMBER,
                                  DocKind     IN NUMBER,
                                  KindOper    IN NUMBER ) RETURN NUMBER is
  StepDate  DATE;
  SumDelay  NUMBER;

  begin




    return NULL;

  exception
    when NO_DATA_FOUND then
         return NULL;
    when OTHERS then
         dbms_output.put_line('Ошибка' || SQLERRM);
         return NULL;
  end get_CalculatedReserve;

  function GetOverdueDeal(DocKind IN NUMBER,
                          TypeOp  IN NUMBER,
                          DocID   IN NUMBER,
                          PayFIID IN NUMBER,
                          ReportDate IN DATE) return NUMBER is
    Summ         NUMBER;

  begin
    Summ := 0.0;
    Summ := Summ + GetOverduePart(DocKind, TypeOp, DocID, 1, PayFIID, ReportDate);
    Summ := Summ + GetOverduePart(DocKind, TypeOp, DocID, 2, PayFIID, ReportDate);

    return Summ;
  exception
    when NO_DATA_FOUND then
         return 0;
    when OTHERS then
         dbms_output.put_line('Ошибка' || SQLERRM);
         return 0;
  end GetOverdueDeal;

  function GetOverduePart(DocKind IN NUMBER,
                          TypeOp  IN NUMBER,
                          DocID   IN NUMBER,
                          Part    IN NUMBER,
                          PayFIID IN NUMBER,
                          ReportDate IN DATE) return NUMBER is
    PartType     NUMBER;
    Summ         NUMBER;
    Summ1        NUMBER;
    DelayDate1   DATE;
    DelayDate2   DATE;
    DelayDate3   DATE;
    RemovalDate1 DATE;
    RemovalDate2 DATE;
    RemovalDate3 DATE;

  begin
    Summ := 0.0;

    PartType := get_DealPartBuyType( TypeOp, DocKind, 0 );
    if( PartType = 1 ) then
    --- Если покупка то по первой части поставка, по второй части Аванс и оплата
       DelayDate1 := get_OperStepDate(DocID, DocKind, TypeOp, '>', 0, 0);
       DelayDate2 := get_OperStepDate(DocID, DocKind, TypeOp, '9', 0, 0);
       DelayDate3 := get_OperStepDate(DocID, DocKind, TypeOp, '*', 0, 0);

       if( (DelayDate1 is not NULL) and (Part=1) ) then
          RemovalDate1 := get_OperStepDate(DocID, DocKind, TypeOp, '{', 0, 0);
          if( RemovalDate1 >= ReportDate ) then
             Summ1 := get_StepCarrySum(DocID, DocKind, TypeOp, PayFIID,'>', 0, 0);
             if( Summ1 is not NULL ) then Summ := Summ + Summ1; end if;
          end if;
       end if;

       if( (DelayDate2 is not NULL) and (Part=2) ) then
          RemovalDate2 := get_OperStepDate(DocID, DocKind, TypeOp, '@', 0, 0);
          if( RemovalDate2 >= ReportDate ) then
             Summ1 := get_StepCarrySum(DocID, DocKind, TypeOp, PayFIID,'9', 0, 0);
             if( Summ1 is not NULL ) then Summ := Summ + Summ1; end if;
          end if;
       end if;

       if( (DelayDate3 is not NULL) and (Part=2) ) then
          RemovalDate3 := get_OperStepDate(DocID, DocKind, TypeOp, ')', 0, 0);
          if( RemovalDate3 >= ReportDate ) then
             Summ1 := get_StepCarrySum(DocID, DocKind, TypeOp, PayFIID,'*', 0, 0);
             if( Summ1 is not NULL ) then Summ := Summ + Summ1; end if;
          end if;
       end if;

    else
    --- Если продажа то по первой части Аванс и оплата по второй поставка
       DelayDate1 := get_OperStepDate(DocID, DocKind, TypeOp, '8', 0, 0);
       DelayDate2 := get_OperStepDate(DocID, DocKind, TypeOp, '&', 0, 0);
       DelayDate3 := get_OperStepDate(DocID, DocKind, TypeOp, '?', 0, 0);

       if( (DelayDate1 is not NULL) and (Part=1) ) then
          RemovalDate1 := get_OperStepDate(DocID, DocKind, TypeOp, '0', 0, 0);
          if( RemovalDate1 >= ReportDate ) then
             Summ1 := get_StepCarrySum(DocID, DocKind, TypeOp, PayFIID,'8', 0, 0);
             if( Summ1 is not NULL ) then Summ := Summ + Summ1; end if;
          end if;
       end if;

       if( (DelayDate2 is not NULL) and (Part=1) ) then
          RemovalDate2 := get_OperStepDate(DocID, DocKind, TypeOp, '(', 0, 0);
          if( RemovalDate2 >= ReportDate ) then
             Summ1 := get_StepCarrySum(DocID, DocKind, TypeOp, PayFIID,'&', 0, 0);
             if( Summ1 is not NULL ) then Summ := Summ + Summ1; end if;
          end if;
       end if;

       if( (DelayDate3 is not NULL) and (Part=2) ) then
          RemovalDate3 := get_OperStepDate(DocID, DocKind, TypeOp, '}', 0, 0);
          if( RemovalDate3 >= ReportDate ) then
             Summ1 := get_StepCarrySum(DocID, DocKind, TypeOp, PayFIID,'?', 0, 0);
             if( Summ1 is not NULL ) then Summ := Summ + Summ1; end if;
          end if;
       end if;
    end if;

    return Summ;
  exception
    when NO_DATA_FOUND then
         return 0;
    when OTHERS then
         dbms_output.put_line('Ошибка' || SQLERRM);
         return 0;
  end GetOverduePart;

  -- Функция возвращает большую из двух сумм
  function max_sum( Sum1 IN NUMBER,
                    Sum2 IN NUMBER ) return NUMBER is
  begin
    if( Sum1 > Sum2 ) then
       return Sum1;
    else
       return Sum2;
    end if;
  end;

  -- Функция возвращает меньшую из двух сумм
  function min_sum( Sum1 IN NUMBER,
                    Sum2 IN NUMBER ) return NUMBER is
  begin
    if( Sum1 < Sum2 ) then
       return Sum1;
    else
       return Sum2;
    end if;
  end;

--------------------------------------------------------------------------------------------------------
  -- Вычислить свободное количество ц/б в лоте покупки по состоянию на дату для отчетов
  FUNCTION TaxFreeAmountToDate( in_BuyLot IN dsctaxlot_dbt%ROWTYPE, in_Date IN DATE, in_IsDate IN BOOLEAN, in_Mode IN NUMBER DEFAULT TAXREG_ORDINARY, in_RID IN NUMBER DEFAULT 0 )
  RETURN NUMBER
  IS
    v_Amount NUMBER;
    v_S NUMBER;
    v_R NUMBER;
  BEGIN

    v_Amount := 0;

    IF in_Mode = TAXREG_ORDINARY THEN
      -- стандатный режим
      IF in_BuyLot.t_Type = TAXLOTS_BUY THEN
        IF NOT in_IsDate THEN
          SELECT NVL(SUM(lnk.t_Amount),0)
            INTO v_S -- сумма по всем связям с продажами
            FROM dsctaxlnk_dbt lnk
           WHERE (lnk.t_type = TAXLNK_BS OR lnk.t_Type = TAXLNK_CLPOS) and
                  lnk.t_BuyID = in_BuyLot.t_ID;
        ELSE
          SELECT NVL(SUM(lnk.t_Amount),0)
            INTO v_S -- сумма по всем связям с продажами
            FROM dsctaxlnk_dbt lnk, dsctaxlot_dbt lot
           WHERE (lnk.t_type = TAXLNK_BS OR lnk.t_Type = TAXLNK_CLPOS) and
                  lnk.t_BuyID = in_BuyLot.t_ID and
                 lot.t_ID = lnk.t_SaleID and
                 lot.t_Date <= in_Date;
        END IF;

        IF NOT in_IsDate THEN
          SELECT NVL(SUM(lnk.t_Amount),0)
            INTO v_R -- сумма списаний в прямых Репо за вычетом возвращенной суммы
            FROM dsctaxlnk_dbt lnk
           WHERE lnk.t_type = TAXLNK_REPO and
                 lnk.t_BuyID = in_BuyLot.t_ID and
                 Exists(SELECT repolot.t_ID
                          FROM dsctaxlot_dbt repolot
                         WHERE repolot.t_ID = lnk.t_SaleID and
                               (repolot.t_ID = in_RID or -- для учета однодневных РЕПО
                                repolot.t_Date2 > in_Date or
                                repolot.t_Date2 = TO_DATE('01-01-0001','DD-MM-YYYY')));
        ELSE
          SELECT NVL(SUM(lnk.t_Amount),0)
            INTO v_R -- сумма списаний в прямых Репо за вычетом возвращенной суммы
            FROM dsctaxlnk_dbt lnk
           WHERE lnk.t_type = TAXLNK_REPO and
                 lnk.t_BuyID = in_BuyLot.t_ID and
                 Exists(SELECT repolot.t_ID
                          FROM dsctaxlot_dbt repolot
                         WHERE repolot.t_ID = lnk.t_SaleID and
                               repolot.t_Date <= in_Date and
                               (repolot.t_ID = in_RID or -- для учета однодневных РЕПО
                                repolot.t_Date2 > in_Date or
                                repolot.t_Date2 = TO_DATE('01-01-0001','DD-MM-YYYY')));
        END IF;

        v_Amount := in_BuyLot.t_Amount - v_S - v_R;
      ELSIF in_BuyLot.t_Type = TAXLOTS_BACKREPO THEN
        IF in_BuyLot.t_Date2 > in_Date or in_BuyLot.t_Date2 = TO_DATE('01-01-0001','DD-MM-YYYY') THEN
          IF NOT in_IsDate THEN
            SELECT NVL(SUM(opnlnk.t_Amount -
                           (SELECT NVL(SUM(clslnk.t_Amount),0)
                              FROM dsctaxlnk_dbt clslnk
                             WHERE clslnk.t_type = TAXLNK_CLPOS and
                                   clslnk.t_SaleID = opnlnk.t_SaleID and
                                   clslnk.t_SourceID = in_BuyLot.t_ID and
                                   Exists(SELECT buylot2.t_ID
                                            FROM dsctaxlot_dbt buylot2
                                           WHERE buylot2.t_ID = clslnk.t_BuyID and
                                                 buylot2.t_Date <= in_Date))),0)
              INTO v_S -- сумма по всем связям с продажами за вычетом закрытых в покупках
              FROM dsctaxlnk_dbt opnlnk
             WHERE opnlnk.t_type = TAXLNK_OPSPOS and
                   opnlnk.t_BuyID = in_BuyLot.t_ID;
          ELSE
            SELECT NVL(SUM(opnlnk.t_Amount -
                           (SELECT NVL(SUM(clslnk.t_Amount),0)
                              FROM dsctaxlnk_dbt clslnk
                             WHERE clslnk.t_type = TAXLNK_CLPOS and
                                   clslnk.t_SaleID = opnlnk.t_SaleID and
                                   clslnk.t_SourceID = in_BuyLot.t_ID and
                                   Exists(SELECT buylot2.t_ID
                                            FROM dsctaxlot_dbt buylot2
                                           WHERE buylot2.t_ID = clslnk.t_BuyID and
                                                 buylot2.t_Date <= in_Date))),0)
              INTO v_S -- сумма по всем связям с продажами за вычетом закрытых в покупках
              FROM dsctaxlnk_dbt opnlnk, dsctaxlot_dbt lot
             WHERE opnlnk.t_type = TAXLNK_OPSPOS and
                   opnlnk.t_BuyID = in_BuyLot.t_ID and
                   lot.t_ID = opnlnk.t_SaleID and
                   lot.t_Date <= in_Date;
          END IF;

          IF NOT in_IsDate THEN
            SELECT NVL(SUM(lnk.t_Amount),0)
              INTO v_R -- сумма списаний в прямых Репо за вычетом возвращенной суммы
              FROM dsctaxlnk_dbt lnk
             WHERE lnk.t_type = TAXLNK_REPO and
                   lnk.t_BuyID = in_BuyLot.t_ID and
                   Exists(SELECT repolot.t_ID
                            FROM dsctaxlot_dbt repolot
                           WHERE repolot.t_ID = lnk.t_SaleID and
                                 (repolot.t_ID = in_RID or -- для учета однодневных РЕПО
                                  repolot.t_Date2 > in_Date or
                                  repolot.t_Date2 = TO_DATE('01-01-0001','DD-MM-YYYY')));
          ELSE
            SELECT NVL(SUM(lnk.t_Amount),0)
              INTO v_R -- сумма списаний в прямых Репо за вычетом возвращенной суммы
              FROM dsctaxlnk_dbt lnk
             WHERE lnk.t_type = TAXLNK_REPO and
                   lnk.t_BuyID = in_BuyLot.t_ID and
                   Exists(SELECT repolot.t_ID
                            FROM dsctaxlot_dbt repolot
                           WHERE repolot.t_ID = lnk.t_SaleID and
                                 repolot.t_Date <= in_Date and
                                 (repolot.t_ID = in_RID or -- для учета однодневных РЕПО
                                  repolot.t_Date2 > in_Date or
                                  repolot.t_Date2 = TO_DATE('01-01-0001','DD-MM-YYYY')));
          END IF;

          v_Amount := in_BuyLot.t_Amount - v_S - v_R;
        END IF;
      END IF;
    ELSIF in_Mode = TAXREG_NETTEXCL THEN
      -- с исключением неттинга
      IF in_BuyLot.t_Type = TAXLOTS_BUY THEN
        IF NOT in_IsDate THEN
          SELECT NVL(SUM(lnk.t_Amount),0)
            INTO v_S -- сумма по всем связям с продажами
            FROM dsctaxlnk_dbt lnk
           WHERE (lnk.t_type = TAXLNK_DELIVER OR lnk.t_Type = TAXLNK_CLPOS) and
                  lnk.t_BuyID = in_BuyLot.t_ID;
        ELSE
          SELECT NVL(SUM(lnk.t_Amount),0)
            INTO v_S -- сумма по всем связям с продажами
            FROM dsctaxlnk_dbt lnk, dsctaxlot_dbt lot
           WHERE (lnk.t_type = TAXLNK_DELIVER OR lnk.t_Type = TAXLNK_CLPOS) and
                  lnk.t_BuyID = in_BuyLot.t_ID and
                 lot.t_ID = lnk.t_SaleID and
                 lot.t_Date <= in_Date;
        END IF;

        IF NOT in_IsDate THEN
          SELECT NVL(SUM(lnk.t_Amount),0)
            INTO v_R -- сумма списаний в прямых Репо за вычетом возвращенной суммы
            FROM dsctaxlnk_dbt lnk
           WHERE lnk.t_type = TAXLNK_DELREPO and
                 lnk.t_BuyID = in_BuyLot.t_ID and
                 Exists(SELECT repolot.t_ID
                          FROM dsctaxlot_dbt repolot
                         WHERE repolot.t_ID = lnk.t_SaleID and
                               (repolot.t_ID = in_RID or -- для учета однодневных РЕПО
                                repolot.t_Date2 > in_Date or
                                repolot.t_Date2 = TO_DATE('01-01-0001','DD-MM-YYYY')));
        ELSE
          SELECT NVL(SUM(lnk.t_Amount),0)
            INTO v_R -- сумма списаний в прямых Репо за вычетом возвращенной суммы
            FROM dsctaxlnk_dbt lnk
           WHERE lnk.t_type = TAXLNK_DELREPO and
                 lnk.t_BuyID = in_BuyLot.t_ID and
                 Exists(SELECT repolot.t_ID
                          FROM dsctaxlot_dbt repolot
                         WHERE repolot.t_ID = lnk.t_SaleID and
                               repolot.t_Date <= in_Date and
                               (repolot.t_ID = in_RID or -- для учета однодневных РЕПО
                                repolot.t_Date2 > in_Date or
                                repolot.t_Date2 = TO_DATE('01-01-0001','DD-MM-YYYY')));
        END IF;

        v_Amount := in_BuyLot.t_Amount - in_BuyLot.t_Netting - v_S - v_R;
      ELSIF in_BuyLot.t_Type = TAXLOTS_BACKREPO THEN
        IF in_BuyLot.t_Date2 > in_Date or in_BuyLot.t_Date2 = TO_DATE('01-01-0001','DD-MM-YYYY') THEN
          IF NOT in_IsDate THEN
            SELECT NVL(SUM(opnlnk.t_Amount -
                           (SELECT NVL(SUM(clslnk.t_Amount),0)
                              FROM dsctaxlnk_dbt clslnk
                             WHERE clslnk.t_type = TAXLNK_CLPOS and
                                   clslnk.t_SaleID = opnlnk.t_SaleID and
                                   clslnk.t_SourceID = in_BuyLot.t_ID and
                                   Exists(SELECT buylot2.t_ID
                                            FROM dsctaxlot_dbt buylot2
                                           WHERE buylot2.t_ID = clslnk.t_BuyID and
                                                 buylot2.t_Date <= in_Date))),0)
              INTO v_S -- сумма по всем связям с продажами за вычетом закрытых в покупках
              FROM dsctaxlnk_dbt opnlnk
             WHERE opnlnk.t_type = TAXLNK_OPSPOS and
                   opnlnk.t_BuyID = in_BuyLot.t_ID;
          ELSE
            SELECT NVL(SUM(opnlnk.t_Amount -
                           (SELECT NVL(SUM(clslnk.t_Amount),0)
                              FROM dsctaxlnk_dbt clslnk
                             WHERE clslnk.t_type = TAXLNK_CLPOS and
                                   clslnk.t_SaleID = opnlnk.t_SaleID and
                                   clslnk.t_SourceID = in_BuyLot.t_ID and
                                   Exists(SELECT buylot2.t_ID
                                            FROM dsctaxlot_dbt buylot2
                                           WHERE buylot2.t_ID = clslnk.t_BuyID and
                                                 buylot2.t_Date <= in_Date))),0)
              INTO v_S -- сумма по всем связям с продажами за вычетом закрытых в покупках
              FROM dsctaxlnk_dbt opnlnk, dsctaxlot_dbt lot
             WHERE opnlnk.t_type = TAXLNK_OPSPOS and
                   opnlnk.t_BuyID = in_BuyLot.t_ID and
                   lot.t_ID = opnlnk.t_SaleID and
                   lot.t_Date <= in_Date;
          END IF;

          IF NOT in_IsDate THEN
            SELECT NVL(SUM(lnk.t_Amount),0)
              INTO v_R -- сумма списаний в прямых Репо за вычетом возвращенной суммы
              FROM dsctaxlnk_dbt lnk
             WHERE lnk.t_type = TAXLNK_DELREPO and
                   lnk.t_BuyID = in_BuyLot.t_ID and
                   Exists(SELECT repolot.t_ID
                            FROM dsctaxlot_dbt repolot
                           WHERE repolot.t_ID = lnk.t_SaleID and
                                 (repolot.t_ID = in_RID or -- для учета однодневных РЕПО
                                  repolot.t_Date2 > in_Date or
                                  repolot.t_Date2 = TO_DATE('01-01-0001','DD-MM-YYYY')));
          ELSE
            SELECT NVL(SUM(lnk.t_Amount),0)
              INTO v_R -- сумма списаний в прямых Репо за вычетом возвращенной суммы
              FROM dsctaxlnk_dbt lnk
             WHERE lnk.t_type = TAXLNK_DELREPO and
                   lnk.t_BuyID = in_BuyLot.t_ID and
                   Exists(SELECT repolot.t_ID
                            FROM dsctaxlot_dbt repolot
                           WHERE repolot.t_ID = lnk.t_SaleID and
                                 repolot.t_Date <= in_Date and
                                 (repolot.t_ID = in_RID or -- для учета однодневных РЕПО
                                  repolot.t_Date2 > in_Date or  -- для учета однодневных РЕПО неравенство нестрогое
                                  repolot.t_Date2 = TO_DATE('01-01-0001','DD-MM-YYYY')));
          END IF;

          v_Amount := in_BuyLot.t_Amount - v_S - v_R;
        END IF;
      END IF;
    END IF;

    RETURN v_Amount;

  END TaxFreeAmountToDate;

--------------------------------------------------------------------------------------------------------
  -- Вычислить свободное количество ц/б в лоте покупки по состоянию на дату
  FUNCTION TaxFreeAmount( in_BuyLot IN dsctaxlot_dbt%ROWTYPE, in_Date IN DATE, in_Mode IN NUMBER DEFAULT TAXREG_ORDINARY, in_RID IN NUMBER DEFAULT 0 )
  RETURN NUMBER
  IS
  BEGIN

    RETURN TaxFreeAmountToDate( in_BuyLot, in_Date, FALSE, in_Mode, in_RID );

  END TaxFreeAmount;

--------------------------------------------------------------------------------------------------------
  -- Вычислить свободное количество ц/б в лоте покупки по ИД по состоянию на дату/время для отчетов
  FUNCTION TaxFreeAmountByIDToDate(lotID IN dsctaxlot_dbt.t_ID%TYPE, in_Date IN DATE, in_Mode IN NUMBER DEFAULT TAXREG_ORDINARY, in_RID IN NUMBER DEFAULT 0)
  RETURN NUMBER
  IS
    r_lot dsctaxlot_dbt%ROWTYPE;
  BEGIN

    SELECT *
      INTO r_lot
      FROM dsctaxlot_dbt
     WHERE t_ID = lotID;

     RETURN TaxFreeAmountToDate(r_lot, in_Date, TRUE, in_Mode, in_RID);

  END;

--------------------------------------------------------------------------------------------------------
  -- Вычислить свободное количество ц/б в лоте покупки по ИД по состоянию на дату/время
  FUNCTION TaxFreeAmountByID(lotID IN dsctaxlot_dbt.t_ID%TYPE, in_Date IN DATE, in_Mode IN NUMBER DEFAULT TAXREG_ORDINARY, in_RID IN NUMBER DEFAULT 0)
  RETURN NUMBER
  IS
    r_lot dsctaxlot_dbt%ROWTYPE;
  BEGIN
    SELECT *
      INTO r_lot
      FROM dsctaxlot_dbt
     WHERE t_ID = lotID;

     RETURN TaxFreeAmountToDate(r_lot, in_Date, FALSE, in_Mode, in_RID);

  END;

--------------------------------------------------------------------------------------------------------
  -- Вычислить свободное количество ц/б в лоте репо прямого, связанного с лотом покупки
  FUNCTION TaxRepoFreeAmount( in_RepoID IN dsctaxlot_dbt.t_ID%TYPE,
                              in_BuyID IN dsctaxlot_dbt.t_ID%TYPE,
                              in_Mode IN NUMBER DEFAULT TAXREG_ORDINARY )
  RETURN NUMBER
  IS
  BEGIN

    RETURN TaxRepoFreeAmountToDate( in_RepoID, in_BuyID, TO_DATE('01-01-0001','DD-MM-YYYY'), in_Mode );

  END TaxRepoFreeAmount;

--------------------------------------------------------------------------------------------------------
  -- Вычислить свободное количество ц/б в лоте репо прямого, связанного с лотом покупки, до заданной даты
  FUNCTION TaxRepoFreeAmountToDate( in_RepoID IN dsctaxlot_dbt.t_ID%TYPE,
                                    in_BuyID IN dsctaxlot_dbt.t_ID%TYPE,
                                    in_EndDate IN DATE,
                                    in_Mode IN NUMBER DEFAULT TAXREG_ORDINARY )
  RETURN NUMBER
  IS
    v_Qr NUMBER;
    v_Qv NUMBER;
  BEGIN

    v_Qr := 0;
    v_Qv := 0;

    IF in_Mode = TAXREG_ORDINARY THEN
      -- стандартный режим
      SELECT NVL(SUM(lnk.t_Amount),0)
        INTO v_Qr
        FROM dsctaxlnk_dbt lnk
       WHERE lnk.t_type = TAXLNK_REPO and
             lnk.t_SaleID = in_RepoID and
             lnk.t_BuyID  = in_BuyID;

      IF in_EndDate <> TO_DATE('01-01-0001', 'DD-MM-YYYY') THEN
        SELECT NVL(SUM(lnk.t_Amount),0)
          INTO v_Qv
          FROM dsctaxlnk_dbt lnk, dsctaxlot_dbt lot
         WHERE lnk.t_type = TAXLNK_BREPO and
               lnk.t_BuyID    = in_RepoID and
               lnk.t_SourceID = in_BuyID and
               lot.t_ID = lnk.t_SaleID and
               lot.t_Date <= in_EndDate;
      ELSE
        SELECT NVL(SUM(lnk.t_Amount),0)
          INTO v_Qv
          FROM dsctaxlnk_dbt lnk
         WHERE lnk.t_type = TAXLNK_BREPO and
               lnk.t_BuyID    = in_RepoID and
               lnk.t_SourceID = in_BuyID;
      END IF;

    ELSIF in_Mode = TAXREG_NETTEXCL THEN
      -- с исключением неттинга
      SELECT NVL(SUM(lnk.t_Amount),0)
        INTO v_Qr
        FROM dsctaxlnk_dbt lnk
       WHERE lnk.t_type = TAXLNK_DELREPO and
             lnk.t_SaleID = in_RepoID and
             lnk.t_BuyID  = in_BuyID;

      IF in_EndDate <> TO_DATE('01-01-0001', 'DD-MM-YYYY') THEN
        SELECT NVL(SUM(lnk.t_Amount),0)
          INTO v_Qv
          FROM dsctaxlnk_dbt lnk, dsctaxlot_dbt lot
         WHERE lnk.t_type = TAXLNK_DELBREP and
               lnk.t_BuyID    = in_RepoID and
               lnk.t_SourceID = in_BuyID and
               lot.t_ID = lnk.t_SaleID and
               lot.t_Date <= in_EndDate;
      ELSE
        SELECT NVL(SUM(lnk.t_Amount),0)
          INTO v_Qv
          FROM dsctaxlnk_dbt lnk
         WHERE lnk.t_type = TAXLNK_DELBREP and
               lnk.t_BuyID    = in_RepoID and
               lnk.t_SourceID = in_BuyID;
      END IF;

    END IF;

    RETURN v_Qr - v_Qv;

  END TaxRepoFreeAmountToDate;

--------------------------------------------------------------------------------------------------------
  -- Вычислить свободное количество ц/б для связи "возврат 2"
  FUNCTION TaxRepoFreeAmount2( in_RepoID IN dsctaxlot_dbt.t_ID%TYPE,
                               in_RepoID1 IN dsctaxlot_dbt.t_ID%TYPE,
                               in_BuyID IN dsctaxlot_dbt.t_ID%TYPE,
                               in_Mode IN NUMBER DEFAULT TAXREG_ORDINARY )
  RETURN NUMBER
  IS
    v_Qr NUMBER;
    v_Qv NUMBER;
  BEGIN

    v_Qr := 0;
    v_Qv := 0;

    IF in_Mode = TAXREG_ORDINARY THEN
      -- стандартный режим
      SELECT NVL(SUM(t_Amount),0)
        INTO v_Qr
        FROM dsctaxlnk_dbt
       WHERE t_type = TAXLNK_BREPO and
             t_SaleID   = in_RepoID1 and
             t_BuyID    = in_RepoID and
             t_SourceID = in_BuyID;

      SELECT NVL(SUM(t_Amount),0)
        INTO v_Qv
        FROM dsctaxlnk_dbt
       WHERE t_type = TAXLNK_RETURN2 and
             t_SaleID   = in_RepoID1 and
             t_BuyID    = in_RepoID and
             t_SourceID = in_BuyID;

    ELSIF in_Mode = TAXREG_NETTEXCL THEN
      -- с исключением неттинга
      SELECT NVL(SUM(t_Amount),0)
        INTO v_Qr
        FROM dsctaxlnk_dbt
       WHERE t_type = TAXLNK_DELBREP and
             t_SaleID   = in_RepoID1 and
             t_BuyID    = in_RepoID and
             t_SourceID = in_BuyID;

      SELECT NVL(SUM(t_Amount),0)
        INTO v_Qv
        FROM dsctaxlnk_dbt
       WHERE t_type = TAXLNK_DELRET2 and
             t_SaleID   = in_RepoID1 and
             t_BuyID    = in_RepoID and
             t_SourceID = in_BuyID;
    END IF;

    RETURN v_Qr - v_Qv;

  END TaxRepoFreeAmount2;

--------------------------------------------------------------------------------------------------------
  -- Вычислить нераспределенное (доступное для связывания) возвращенное количество ц/б в лоте типа
  -- "Продажа" или "Репо прямое" из связи со сделкой покупки
  FUNCTION TaxRepoUnallottedAmount( in_SaleID IN dsctaxlot_dbt.t_ID%TYPE,
                                    in_BuyID IN dsctaxlot_dbt.t_ID%TYPE,
                                    in_Mode IN NUMBER DEFAULT TAXREG_ORDINARY )
  RETURN NUMBER
  IS
    v_Qr NUMBER;
    v_Qv NUMBER;
  BEGIN

    v_Qr := 0;
    v_Qv := 0;

    IF in_Mode = TAXREG_ORDINARY THEN
      -- стандартный режим
      SELECT NVL(SUM(t_Amount),0)
        INTO v_Qr
        FROM dsctaxlnk_dbt
       WHERE (t_type = TAXLNK_BS or t_type = TAXLNK_REPO) and
             t_SaleID = in_SaleID and
             t_BuyID  = in_BuyID;

      SELECT NVL(SUM(t_Amount),0)
        INTO v_Qv
        FROM dsctaxlnk_dbt
       WHERE t_type = TAXLNK_BREPO and
             t_SaleID   = in_SaleID and
             t_SourceID = in_BuyID;
    ELSIF in_Mode = TAXREG_NETTEXCL THEN
       -- с исключением неттинга
      SELECT NVL(SUM(t_Amount),0)
        INTO v_Qr
        FROM dsctaxlnk_dbt
       WHERE (t_type = TAXLNK_DELIVER or t_type = TAXLNK_DELREPO) and
             t_SaleID = in_SaleID and
             t_BuyID  = in_BuyID;

      SELECT NVL(SUM(t_Amount),0)
        INTO v_Qv
        FROM dsctaxlnk_dbt
       WHERE t_type = TAXLNK_DELBREP and
             t_SaleID   = in_SaleID and
             t_SourceID = in_BuyID;
    END IF;

    RETURN v_Qr - v_Qv;

  END TaxRepoUnallottedAmount;

--------------------------------------------------------------------------------------------------------
  -- Вычислить свободное количество ц/б для связи "возврат 2"
  FUNCTION TaxRepoFreeAmountSP( in_SourceID IN dsctaxlot_dbt.t_ID%TYPE,
                                in_BuyID IN dsctaxlot_dbt.t_ID%TYPE,
                                in_DestID IN dsctaxlot_dbt.t_ID%TYPE,
                                in_S IN dsctaxlot_dbt.t_Amount%TYPE )
  RETURN NUMBER
  IS
    v_Qv NUMBER;
  BEGIN
    v_Qv := 0;

    SELECT NVL(SUM(t_Amount),0)
      INTO v_Qv
      FROM dsctaxlnk_dbt
     WHERE t_SourceID = in_SourceID and
           t_BuyID = in_BuyID and
           t_DestID = in_DestID and
           t_type = TAXLNK_RETSPOS;

    RETURN in_S - v_Qv;

  END TaxRepoFreeAmountSP;

--------------------------------------------------------------------------------------------------------
  -- Вычислить нераспределенное (доступное для связывания) количество ц/б для связи "Возврат 2"
  FUNCTION TaxRepoUnallottedAmount2( in_RepoID IN dsctaxlot_dbt.t_ID%TYPE,
                                     in_SaleID IN dsctaxlot_dbt.t_ID%TYPE,
                                     in_BuyID IN dsctaxlot_dbt.t_ID%TYPE,
                                     in_Mode IN NUMBER DEFAULT TAXREG_ORDINARY )
  RETURN NUMBER
  IS
    v_Qin  NUMBER;
    v_Qout NUMBER;
  BEGIN

    v_Qin  := 0;
    v_Qout := 0;

    IF in_Mode = TAXREG_ORDINARY THEN
      -- стандартный режим
      SELECT NVL(SUM(t_Amount),0)
        INTO v_Qin
        FROM dsctaxlnk_dbt
       WHERE t_type = TAXLNK_RETURN2 and
             t_BuyID   = in_RepoID and
             t_SourceID = in_BuyID and
             t_DestID   = in_SaleID;

      SELECT NVL(SUM(t_Amount),0)
        INTO v_Qout
        FROM dsctaxlnk_dbt
       WHERE t_type = TAXLNK_RETURN2 and
             t_SaleID    = in_RepoID and
             t_SourceID = in_BuyID and
             t_DestID   = in_SaleID;
    ELSIF in_Mode = TAXREG_NETTEXCL THEN
      -- с исключением неттинга
      SELECT NVL(SUM(t_Amount),0)
        INTO v_Qin
        FROM dsctaxlnk_dbt
       WHERE t_type = TAXLNK_DELRET2 and
             t_BuyID   = in_RepoID and
             t_SourceID = in_BuyID and
             t_DestID   = in_SaleID;

      SELECT NVL(SUM(t_Amount),0)
        INTO v_Qout
        FROM dsctaxlnk_dbt
       WHERE t_type = TAXLNK_DELRET2 and
             t_SaleID    = in_RepoID and
             t_SourceID = in_BuyID and
             t_DestID   = in_SaleID;
    END IF;

    RETURN v_Qin - v_Qout;

  END TaxRepoUnallottedAmount2;

--------------------------------------------------------------------------------------------------------
  -- Вычислить нераспределенное (доступное для связывания) количество ц/б  для связи "Возврат в КП"
  FUNCTION TaxRepoUnallottedAmountSP( in_SourceID IN dsctaxlot_dbt.t_ID%TYPE,
                                      in_SaleID IN dsctaxlot_dbt.t_ID%TYPE,
                                      in_S IN dsctaxlot_dbt.t_Amount%TYPE )
  RETURN NUMBER
  IS
    v_Qv  NUMBER;
  BEGIN

    v_Qv := 0;

    SELECT NVL(SUM(t_Amount),0)
      INTO v_Qv
      FROM dsctaxlnk_dbt
     WHERE t_SourceID = in_SourceID and
           t_SaleID = in_SaleID and
           t_type = TAXLNK_RETSPOS;

    RETURN in_S - v_Qv;

  END TaxRepoUnallottedAmountSP;

--------------------------------------------------------------------------------------------------------
  -- Вычислить незакрытый остаток короткой позиции по лоту продажи, возникших по лоту обратного репо
  FUNCTION TaxSalePosition( in_LotID  IN dsctaxlot_dbt.t_ID%TYPE,
                            in_RepoID IN dsctaxlot_dbt.t_ID%TYPE )
  RETURN NUMBER
  IS
    v_O NUMBER;
    v_C NUMBER;
  BEGIN

    SELECT NVL(SUM(t_Amount),0)
      INTO v_O
      FROM dsctaxlnk_dbt
     WHERE t_type = TAXLNK_OPSPOS and
           t_SaleID = in_LotID and
           t_BuyID = in_RepoID;

    SELECT NVL(SUM(t_Amount),0)
      INTO v_C
      FROM dsctaxlnk_dbt
     WHERE t_type = TAXLNK_CLPOS and
           t_SaleID   = in_LotID and
           t_SourceID = in_RepoID;

    RETURN v_O - v_C;

  END TaxSalePosition;

--------------------------------------------------------------------------------------------------------
  -- Вычислить количество в неттинге для сделки по платежу по неттингу
  FUNCTION TaxGetNettingAmount( in_Tick IN ddl_tick_dbt%ROWTYPE, in_PM IN dpmlink_dbt%ROWTYPE )
  RETURN NUMBER
  IS
    v_ObjType NUMBER;
    v_res NUMBER;
    v_DealCode ddl_tick_dbt.t_DealCode%TYPE;

    FUNCTION CheckExistsNote( in_NoteKind IN dnotekind_dbt.t_NoteKind%TYPE,
                              in_ObjType IN dnotetext_dbt.t_ObjectType%TYPE,
                              in_DocID IN NUMBER,
                              in_IsText IN BOOLEAN,
                              out_Text OUT VARCHAR2 )
    RETURN BOOLEAN
    IS
      v_Text dnotetext_dbt.t_Text%TYPE;
      v_OutText ddl_tick_dbt.t_DealCode%TYPE;
      v_iCount NUMBER;
    BEGIN

      SELECT t_Text
        INTO v_Text
        FROM dnotetext_dbt
       WHERE t_ObjectType = in_ObjType and
             TO_NUMBER(t_DocumentID) = in_DocID and
             t_NoteKind   = in_NoteKind;

      IF in_IsText THEN
        v_OutText := substr(rsb_struct.getString(v_Text),1,30);

        v_iCount := 1;
        out_Text := '';
        WHILE v_iCount < Length(v_OutText) and v_iCount < 30 LOOP
          IF substr(v_OutText,v_iCount,1) > CHR(1) THEN
            out_Text := out_Text || substr(v_OutText,v_iCount,1);
          END IF;
          v_iCount := v_iCount + 1;
        END LOOP;
      END IF;

      RETURN TRUE;

      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          RETURN FALSE;
    END;

  BEGIN

    IF in_Tick.t_BofficeKind = 117 or in_Tick.t_BofficeKind = 130 THEN
      v_ObjType := OBJTYPE_RETIRE;
    ELSE
      v_ObjType := OBJTYPE_SECDEAL;
    END IF;

    IF CheckExistsNote(NOTEKIND_DEAL_REGULAT, v_ObjType, in_Tick.t_DealID, FALSE, v_DealCode) THEN
      v_res := 0;
    ELSIF CheckExistsNote(NOTEKIND_NETT_NUMDEAL, OBJTYPE_NETTING, in_PM.t_DocumentID, TRUE, v_DealCode) THEN
      IF v_DealCode = in_Tick.t_DealCode THEN

        SELECT t_Amount
          INTO v_res
          FROM dpmpaym_dbt
         WHERE t_PaymentID = in_PM.t_PurposePayment;

        v_res := in_PM.t_Amount - v_res;
      ELSE
        v_res := in_PM.t_Amount;
      END IF;
    ELSE
      v_res := in_PM.t_Amount;
    END IF;

    RETURN v_res;

    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        RETURN 0;

  END TaxGetNettingAmount;

  FUNCTION TaxGetNettingAmountById( in_TickID IN ddl_tick_dbt.t_DealID%TYPE, in_PMID IN dpmlink_dbt.t_PaymLinkID%TYPE )
  RETURN NUMBER
  IS
    r_tick ddl_tick_dbt%ROWTYPE;
    r_pmlnk dpmlink_dbt%ROWTYPE;
  BEGIN

    SELECT *
      INTO r_tick
      FROM ddl_tick_dbt
     WHERE t_DealID = in_TickID;

    SELECT *
      INTO r_pmlnk
      FROM dpmlink_dbt
     WHERE t_PaymLinkID = in_PMID;

    RETURN TaxGetNettingAmount(r_tick, r_pmlnk);

  END TaxGetNettingAmountById;

--------------------------------------------------------------------------------------------------------
  FUNCTION date_min( d1 IN DATE, d2 IN DATE )
  RETURN DATE
  IS
  BEGIN
    IF d1 < d2 THEN
      RETURN d1;
    END IF;

    RETURN d2;
  END;

--------------------------------------------------------------------------------------------------------
  FUNCTION date_max( d1 IN DATE, d2 IN DATE )
  RETURN DATE
  IS
  BEGIN
    IF d1 > d2 THEN
      RETURN d1;
    END IF;

    RETURN d2;
  END;

--------------------------------------------------------------------------------------------------------
  -- вывод сообщений об ошибке в протокол
  PROCEDURE InsertError( DealID IN NUMBER, FIID IN NUMBER, ErrorType IN VARCHAR2, ErrorStr IN VARCHAR2 )
  IS
  BEGIN
     EXECUTE IMMEDIATE (
       'INSERT INTO DSCTAXMES_TMP ErrLog
        (
           ErrLog.T_ID,
           ErrLog.T_DEALID,
           ErrLog.T_FIID,
           ErrLog.T_TYPE,
           ErrLog.T_MESSAGE,
           ErrLog.T_MESTIME
        )
        VALUES
        (
           0, :1, :2, :3, :4, :5
        )'
    ) USING IN DealID, IN FIID, IN ErrorType, IN ErrorStr, SYSDATE;

    COMMIT;
  END;

--------------------------------------------------------------------------------------------------------
  -- Выполнить рекурсивное построение связи "Возврат 2"
  PROCEDURE TaxLinkDirectRepo( in_RepoID IN dsctaxlot_dbt.t_ID%TYPE,
                               in_SaleID IN dsctaxlot_dbt.t_ID%TYPE,
                               in_BuyID IN dsctaxlot_dbt.t_ID%TYPE,
                               in_Mode IN NUMBER DEFAULT TAXREG_ORDINARY )
  AS
    v_LR  NUMBER DEFAULT TAXLNK_UNDEF;
    v_LR2 NUMBER DEFAULT TAXLNK_UNDEF;
    v_AM NUMBER;
    v_RepoUnllotAmount NUMBER;
    v_RepoFreeAmount NUMBER;
    v_Amount NUMBER;
    v_repolot2id dsctaxlot_dbt.t_ID%TYPE;
    r_dr2 dsctaxlnk_dbt%ROWTYPE;
    v_lotDate DATE;
    v_lotTime DATE;

  BEGIN

    IF in_Mode = TAXREG_ORDINARY THEN
      -- стандартный режим
      v_LR  := TAXLNK_BREPO;
      v_LR2 := TAXLNK_RETURN2;
    ELSIF in_Mode = TAXREG_NETTEXCL THEN
      -- с исключением неттинга
      v_LR  := TAXLNK_DELBREP;
      v_LR2 := TAXLNK_DELRET2;
    END IF;

    v_AM := AmortizationMethod(-1);

    v_RepoUnllotAmount := TaxRepoUnallottedAmount2(in_RepoID, in_SaleID, in_BuyID, in_Mode);
    WHILE v_RepoUnllotAmount > 0 LOOP
      v_lotDate    := NULL;
      v_lotTime    := NULL;
      v_repolot2id := NULL;

      BEGIN

        IF v_AM = PM_WRITEOFF_FIFO THEN
          SELECT MIN(lot.t_Date)
            INTO v_lotDate
            FROM dsctaxlot_dbt lot
           WHERE lot.t_Type = TAXLOTS_REPO and
                 Exists( SELECT lnk.t_Type
                           FROM dsctaxlnk_dbt lnk
                          WHERE lnk.t_Type = v_LR and
                                lnk.t_SaleID   = in_RepoID and
                                lnk.t_BuyID    = lot.t_ID and
                                lnk.t_SourceID = in_BuyID ) and
                 TaxRepoFreeAmount2(lot.t_ID, in_RepoID, in_BuyID, in_Mode) > 0;

          SELECT MIN(lot.t_Time)
            INTO v_lotTime
            FROM dsctaxlot_dbt lot
           WHERE lot.t_Type = TAXLOTS_REPO and
                 Exists( SELECT lnk.t_Type
                           FROM dsctaxlnk_dbt lnk
                          WHERE lnk.t_Type = v_LR and
                                lnk.t_SaleID   = in_RepoID and
                                lnk.t_BuyID    = lot.t_ID and
                                lnk.t_SourceID = in_BuyID ) and
                 lot.t_Date = v_lotDate and
                 TaxRepoFreeAmount2(lot.t_ID, in_RepoID, in_BuyID, in_Mode) > 0;

          SELECT MIN(lot.t_ID)
            INTO v_repolot2id
            FROM dsctaxlot_dbt lot
           WHERE lot.t_Type = TAXLOTS_REPO and
                 Exists( SELECT lnk.t_Type
                           FROM dsctaxlnk_dbt lnk
                          WHERE lnk.t_Type = v_LR and
                                lnk.t_SaleID   = in_RepoID and
                                lnk.t_BuyID    = lot.t_ID and
                                lnk.t_SourceID = in_BuyID ) and
                 lot.t_Date = v_lotDate and
                 lot.t_Time = v_lotTime and
                 TaxRepoFreeAmount2(lot.t_ID, in_RepoID, in_BuyID, in_Mode) > 0;
        ELSE
          SELECT MAX(lot.t_Date)
            INTO v_lotDate
            FROM dsctaxlot_dbt lot
           WHERE lot.t_Type = TAXLOTS_REPO and
                 Exists( SELECT lnk.t_Type
                           FROM dsctaxlnk_dbt lnk
                          WHERE lnk.t_Type = v_LR and
                                lnk.t_SaleID   = in_RepoID and
                                lnk.t_BuyID    = lot.t_ID and
                                lnk.t_SourceID = in_BuyID ) and
                 TaxRepoFreeAmount2(lot.t_ID, in_RepoID, in_BuyID, in_Mode) > 0;

          SELECT MAX(lot.t_Time)
            INTO v_lotTime
            FROM dsctaxlot_dbt lot
           WHERE lot.t_Type = TAXLOTS_REPO and
                 Exists( SELECT lnk.t_Type
                           FROM dsctaxlnk_dbt lnk
                          WHERE lnk.t_Type = v_LR and
                                lnk.t_SaleID   = in_RepoID and
                                lnk.t_BuyID    = lot.t_ID and
                                lnk.t_SourceID = in_BuyID ) and
                 lot.t_Date = v_lotDate and
                 TaxRepoFreeAmount2(lot.t_ID, in_RepoID, in_BuyID, in_Mode) > 0;

          SELECT MAX(lot.t_ID)
            INTO v_repolot2id
            FROM dsctaxlot_dbt lot
           WHERE lot.t_Type = TAXLOTS_REPO and
                 Exists( SELECT lnk.t_Type
                           FROM dsctaxlnk_dbt lnk
                          WHERE lnk.t_Type = v_LR and
                                lnk.t_SaleID   = in_RepoID and
                                lnk.t_BuyID    = lot.t_ID and
                                lnk.t_SourceID = in_BuyID ) and
                 lot.t_Date = v_lotDate and
                 lot.t_Time = v_lotTime and
                 TaxRepoFreeAmount2(lot.t_ID, in_RepoID, in_BuyID, in_Mode) > 0;
        END IF;

        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            NULL;
      END;

      EXIT WHEN v_LotDate IS NULL or
                v_LotTime IS NULL or
                v_repolot2id IS NULL;

      v_RepoFreeAmount := TaxRepoFreeAmount2(v_repolot2id, in_RepoID, in_BuyID, in_Mode);

      IF v_RepoFreeAmount < v_RepoUnllotAmount THEN
        v_Amount := v_RepoFreeAmount;
      ELSE
        v_Amount := v_RepoUnllotAmount;
      END IF;

      BEGIN

        SELECT *
          INTO r_dr2
          FROM dsctaxlnk_dbt lnk
         WHERE lnk.t_Type     = v_LR2 and
               lnk.t_SaleID   = in_RepoID and
               lnk.t_BuyID    = v_repolot2id and
               lnk.t_SourceID = in_BuyID and
               lnk.t_DestID   = in_SaleID;

        UPDATE dsctaxlnk_dbt lnk
           SET lnk.t_Amount = lnk.t_Amount + v_Amount
         WHERE lnk.t_Type     = r_dr2.t_Type and
               lnk.t_SaleID   = r_dr2.t_SaleID and
               lnk.t_BuyID    = r_dr2.t_BuyID and
               lnk.t_SourceID = r_dr2.t_SourceID and
               lnk.t_DestID   = r_dr2.t_DestID;

        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            BEGIN
              INSERT INTO dsctaxlnk_dbt ( T_SALEID,
                                          T_BUYID,
                                          T_TYPE,
                                          T_AMOUNT,
                                          T_SOURCEID,
                                          T_DESTID )
              VALUES ( in_RepoID,
                       v_repolot2id,
                       v_LR2,
                       v_Amount,
                       in_BuyID,
                       in_SaleID
                     );
            END;
      END;

      TaxLinkDirectRepo(v_repolot2id, in_SaleID, in_BuyID, in_Mode);

      v_RepoUnllotAmount := TaxRepoUnallottedAmount2(in_RepoID, in_SaleID, in_BuyID, in_Mode);

    END LOOP;

  END TaxLinkDirectRepo;

--------------------------------------------------------------------------------------------------------
  -- заполнение файла отчета налогового регистра №8
  PROCEDURE CreateTaxLots
            (
               BegDate_in IN DATE, -- Дата начала пересчета
               EndDate_in IN DATE, -- Дата конца пересчета
               ExclNetting_in IN BOOLEAN DEFAULT FALSE, -- Признак исключения неттинга
               DebugFIID_in IN NUMBER DEFAULT -1 -- Сбор данных по ц/б для отладки
            )
  AS

    v_BegDate DATE;
    v_ExclNettBegDate DATE;
    v_ExclNettEndDate DATE;
    r_repolot dsctaxlot_dbt%ROWTYPE;
    r_salelot dsctaxlot_dbt%ROWTYPE;
    v_repolot_id dsctaxlot_dbt.t_ID%TYPE;
    v_salelot_id dsctaxlot_dbt.t_ID%TYPE;
    v_KeyId NUMBER;

    CURSOR WrtSumRepo2 IS ( SELECT pws.*, get_OperationGroup(opr.t_SysTypes) oGroup
                              FROM dpmwrtsum_dbt pws, ddl_tick_dbt dl, doprkoper_dbt opr, dpmpaym_dbt pm, dfininstr_dbt fin
                             WHERE dl.T_DEALID = pws.T_DEALID and
                                   fin.t_FIID = pws.t_FIID and /*на всякий случай проверим наличие ФИ*/
                                   (DebugFIID_in = -1 or pws.t_FIID = DebugFIID_in) and
                                   opr.T_KIND_OPERATION = dl.t_DealType and
                                   pm.t_PaymentID = pws.t_DocID and
                                   pws.t_DocKind = 29 /*DLDOC_PAYMENT*/ and
                                   pws.t_Date >= v_BegDate and
                                   pws.t_Date <= EndDate_in and
                                   pws.t_State >= 40 /*PM_WRTSUM_FORM*/ and
                                   (pws.t_Party = 0 or pws.t_Party = -1) and
                                   ( (IsRepo(get_OperationGroup(opr.t_SysTypes))=1 AND IsSale(get_OperationGroup(opr.t_SysTypes))=1) or
                                     (IsRepo(get_OperationGroup(opr.t_SysTypes))=1 AND IsBuy(get_OperationGroup(opr.t_SysTypes))=1) ) and
                                   pm.t_Purpose=3 /*BRi*/ );

    TYPE RepoSaleLotCurTyp IS REF CURSOR;
    RepoSaleLot_cur RepoSaleLotCurTyp;

    -- Воспомогательные функции

    -- Вычислить нераспределенное количество ц/б в лоте продажи
    FUNCTION TaxUnallottedAmount( in_SaleLot IN dsctaxlot_dbt%ROWTYPE, in_Mode IN NUMBER DEFAULT TAXREG_ORDINARY )
    RETURN NUMBER
    IS
      v_B NUMBER;
    BEGIN

      IF in_Mode = TAXREG_ORDINARY THEN
        -- стандартный режим
        SELECT NVL(SUM(t_Amount),0)
          INTO v_B
          FROM dsctaxlnk_dbt
         WHERE (t_type = TAXLNK_BS OR t_Type = TAXLNK_OPSPOS OR t_Type = TAXLNK_REPO) and
               t_SaleID = in_SaleLot.t_ID;

        RETURN in_SaleLot.t_Amount - v_B;
      ELSIF in_Mode = TAXREG_NETTEXCL THEN
       -- с исключением неттинга
        SELECT NVL(SUM(t_Amount),0)
          INTO v_B
          FROM dsctaxlnk_dbt
         WHERE (t_type = TAXLNK_DELIVER OR t_Type = TAXLNK_OPSPOS OR t_Type = TAXLNK_DELREPO) and
               t_SaleID = in_SaleLot.t_ID;

        RETURN in_SaleLot.t_Amount - in_SaleLot.t_Netting - v_B;
      END IF;

      RETURN 0;
    END TaxUnallottedAmount;

    -- Вычислить незакрытый остаток короткой позиции по лоту репо обратного
    FUNCTION TaxRepoPosition( in_RepoID IN dsctaxlot_dbt.t_ID%TYPE )
    RETURN NUMBER
    IS
      v_Amount NUMBER;
    BEGIN

      SELECT NVL(SUM(TaxSalePosition(t_SaleID,in_RepoID)),0)
        INTO v_Amount
        FROM dsctaxlnk_dbt
       WHERE t_type = TAXLNK_OPSPOS and
             t_BuyID = in_RepoID;

      RETURN v_Amount;

    END TaxRepoPosition;


    -- Функции списаний
    -- Выполнить списание лота продажи из следок продажи
    PROCEDURE TaxLinkSaleToBuy( in_SaleLot IN dsctaxlot_dbt%ROWTYPE, in_Mode IN NUMBER DEFAULT TAXREG_ORDINARY )
    AS
      v_LS  NUMBER DEFAULT TAXLNK_UNDEF;
      v_LD  NUMBER DEFAULT TAXLNK_UNDEF;
      v_LR  NUMBER DEFAULT TAXLNK_UNDEF;
      v_LR2 NUMBER DEFAULT TAXLNK_UNDEF;
      v_AM NUMBER;
      v_FaceValueFI dfininstr_dbt.t_FaceValueFI%TYPE;
      v_buylotid dsctaxlot_dbt.t_ID%TYPE;
      v_repolotid dsctaxlot_dbt.t_ID%TYPE;
      v_Amount NUMBER;
      v_FreeAmount NUMBER;
      v_RepoFreeAmount NUMBER;
      v_UnlotAmount NUMBER;
      v_RepoUnlotAmount NUMBER;
      v_lotDate DATE;
      v_lotTime DATE;

    BEGIN

      IF in_Mode = TAXREG_ORDINARY THEN
        -- стандартный режим
        v_LS  := TAXLNK_BS;
        v_LD  := TAXLNK_REPO;
        v_LR  := TAXLNK_BREPO;
        v_LR2 := TAXLNK_RETURN2;
      ELSIF in_Mode = TAXREG_NETTEXCL THEN
        -- с исключением неттинга
        v_LS  := TAXLNK_DELIVER;
        v_LD  := TAXLNK_DELREPO;
        v_LR  := TAXLNK_DELBREP;
        v_LR2 := TAXLNK_DELRET2;
      END IF;

      v_AM := AmortizationMethod(-1);

      BEGIN
        SELECT t_FaceValueFI
          INTO v_FaceValueFI
          FROM dfininstr_dbt
         WHERE t_FIID = in_SaleLot.t_FIID;

        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            v_FaceValueFI := -1;
      END;

      v_UnlotAmount := TaxUnallottedAmount(in_SaleLot, in_Mode);
      WHILE v_UnlotAmount > 0 LOOP
        v_lotDate  := NULL;
        v_lotTime  := NULL;
        v_buylotid := NULL;

        BEGIN

          SELECT MAX(lot.t_Date)
            INTO v_lotDate
            FROM dsctaxlot_dbt lot
           WHERE lot.t_Type = TAXLOTS_BUY and
                 lot.t_FIID = in_SaleLot.t_FIID and
                 lot.t_Date <= in_SaleLot.t_Date and
                 TaxFreeAmountByID(lot.t_ID, in_SaleLot.t_Date, in_Mode, 0) > 0;

          SELECT MAX(lot.t_Time)
            INTO v_lotTime
            FROM dsctaxlot_dbt lot
           WHERE lot.t_Type = TAXLOTS_BUY and
                 lot.t_FIID = in_SaleLot.t_FIID and
                 lot.t_Date <= in_SaleLot.t_Date and
                 lot.t_Date = v_lotDate and
                 TaxFreeAmountByID(lot.t_ID, in_SaleLot.t_Date, in_Mode, 0) > 0;

          SELECT MAX(lot.t_ID)
            INTO v_buylotid
            FROM dsctaxlot_dbt lot
           WHERE lot.t_Type = TAXLOTS_BUY and
                 lot.t_FIID = in_SaleLot.t_FIID and
                 lot.t_Date <= in_SaleLot.t_Date and
                 lot.t_Date = v_lotDate and
                 lot.t_Time = v_lotTime and
                 TaxFreeAmountByID(lot.t_ID, in_SaleLot.t_Date, in_Mode, 0) > 0;
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              NULL;

        END;

        EXIT WHEN v_LotDate IS NULL or
                  v_LotTime IS NULL or
                  v_buylotid IS NULL;

        v_FreeAmount := TaxFreeAmountByID(v_buylotid, in_SaleLot.t_Date, in_Mode, 0);

        IF v_FreeAmount < v_UnlotAmount  THEN
          v_Amount := v_FreeAmount;
        ELSE
          v_Amount := v_UnlotAmount;
        END IF;

        INSERT INTO dsctaxlnk_dbt ( T_SALEID,
                                    T_BUYID,
                                    T_TYPE,
                                    T_AMOUNT,
                                    T_SOURCEID,
                                    T_DESTID )
        VALUES ( in_SaleLot.T_ID,
                 v_buylotid,
                 v_LS,
                 v_Amount,
                 0,
                 0
               );

        v_RepoUnlotAmount := TaxRepoUnallottedAmount(in_SaleLot.t_ID, v_buylotid, in_Mode);
        WHILE v_RepoUnlotAmount > 0 LOOP

          v_lotDate   := NULL;
          v_lotTime   := NULL;
          v_repolotid := NULL;

          BEGIN
            IF v_AM = PM_WRITEOFF_FIFO THEN
              SELECT MIN(lot.t_Date2)
                INTO v_lotDate
                FROM dsctaxlot_dbt lot
               WHERE lot.t_Type = TAXLOTS_REPO and
                     lot.t_Date2 <= in_SaleLot.t_Date and
                     lot.t_Date2 <> TO_DATE('01-01-0001','DD-MM-YYYY') and
                     Exists( SELECT lnk.t_Type
                               FROM dsctaxlnk_dbt lnk
                              WHERE lnk.t_Type = v_LD and
                                    lnk.t_SaleID = lot.t_ID and
                                    lnk.t_BuyID = v_buylotid ) and
                     TaxRepoFreeAmount(lot.t_ID, v_buylotid, in_Mode) > 0;

              SELECT MIN(lot.t_Time2)
                INTO v_lotTime
                FROM dsctaxlot_dbt lot
               WHERE lot.t_Type = TAXLOTS_REPO and
                     lot.t_Date2 <= in_SaleLot.t_Date and
                     lot.t_Date2 <> TO_DATE('01-01-0001','DD-MM-YYYY') and
                     Exists( SELECT lnk.t_Type
                               FROM dsctaxlnk_dbt lnk
                              WHERE lnk.t_Type = v_LD and
                                    lnk.t_SaleID = lot.t_ID and
                                    lnk.t_BuyID = v_buylotid ) and
                     lot.t_Date2 = v_lotDate and
                     TaxRepoFreeAmount(lot.t_ID, v_buylotid, in_Mode) > 0;

              SELECT MIN(lot.t_ID)
                INTO v_repolotid
                FROM dsctaxlot_dbt lot
               WHERE lot.t_Type = TAXLOTS_REPO and
                     lot.t_Date2 <= in_SaleLot.t_Date and
                     lot.t_Date2 <> TO_DATE('01-01-0001','DD-MM-YYYY') and
                     Exists( SELECT lnk.t_Type
                               FROM dsctaxlnk_dbt lnk
                              WHERE lnk.t_Type = v_LD and
                                    lnk.t_SaleID = lot.t_ID and
                                    lnk.t_BuyID = v_buylotid ) and
                     lot.t_Date2 = v_lotDate and
                     lot.t_Time2 = v_lotTime and
                     TaxRepoFreeAmount(lot.t_ID, v_buylotid, in_Mode) > 0;
            ELSE
              SELECT MAX(lot.t_Date2)
                INTO v_lotDate
                FROM dsctaxlot_dbt lot
               WHERE lot.t_Type = TAXLOTS_REPO and
                     lot.t_Date2 <= in_SaleLot.t_Date and
                     lot.t_Date2 <> TO_DATE('01-01-0001','DD-MM-YYYY') and
                     Exists( SELECT lnk.t_Type
                               FROM dsctaxlnk_dbt lnk
                              WHERE lnk.t_Type = v_LD and
                                    lnk.t_SaleID = lot.t_ID and
                                    lnk.t_BuyID = v_buylotid ) and
                     TaxRepoFreeAmount(lot.t_ID, v_buylotid, in_Mode) > 0;

              SELECT MAX(lot.t_Time2)
                INTO v_lotTime
                FROM dsctaxlot_dbt lot
               WHERE lot.t_Type = TAXLOTS_REPO and
                     lot.t_Date2 <= in_SaleLot.t_Date and
                     lot.t_Date2 <> TO_DATE('01-01-0001','DD-MM-YYYY') and
                     Exists( SELECT lnk.t_Type
                               FROM dsctaxlnk_dbt lnk
                              WHERE lnk.t_Type = v_LD and
                                    lnk.t_SaleID = lot.t_ID and
                                    lnk.t_BuyID = v_buylotid ) and
                     lot.t_Date2 = v_lotDate and
                     TaxRepoFreeAmount(lot.t_ID, v_buylotid, in_Mode) > 0;

              SELECT MAX(lot.t_ID)
                INTO v_repolotid
                FROM dsctaxlot_dbt lot
               WHERE lot.t_Type = TAXLOTS_REPO and
                     lot.t_Date2 <= in_SaleLot.t_Date and
                     lot.t_Date2 <> TO_DATE('01-01-0001','DD-MM-YYYY') and
                     Exists( SELECT lnk.t_Type
                               FROM dsctaxlnk_dbt lnk
                              WHERE lnk.t_Type = v_LD and
                                    lnk.t_SaleID = lot.t_ID and
                                    lnk.t_BuyID = v_buylotid ) and
                     lot.t_Date2 = v_lotDate and
                     lot.t_Time2 = v_lotTime and
                     TaxRepoFreeAmount(lot.t_ID, v_buylotid, in_Mode) > 0;
            END IF;

            EXCEPTION
              WHEN NO_DATA_FOUND THEN
                NULL;

          END;

          EXIT WHEN v_LotDate IS NULL or
                    v_LotTime IS NULL or
                    v_repolotid IS NULL;

          v_RepoFreeAmount := TaxRepoFreeAmount(v_repolotid, v_buylotid, in_Mode);

          IF v_RepoFreeAmount < v_RepoUnlotAmount  THEN
            v_Amount := v_RepoFreeAmount;
          ELSE
            v_Amount := v_RepoUnlotAmount;
          END IF;

          INSERT INTO dsctaxlnk_dbt ( T_SALEID,
                                      T_BUYID,
                                      T_TYPE,
                                      T_AMOUNT,
                                      T_SOURCEID,
                                      T_DESTID )
          VALUES ( in_SaleLot.T_ID,
                   v_repolotid,
                   v_LR,
                   v_Amount,
                   v_buylotid,
                   0
                 );

          INSERT INTO dsctaxlnk_dbt ( T_SALEID,
                                      T_BUYID,
                                      T_TYPE,
                                      T_AMOUNT,
                                      T_SOURCEID,
                                      T_DESTID )
          VALUES ( in_SaleLot.T_ID,
                   v_repolotid,
                   v_LR2,
                   v_Amount,
                   v_buylotid,
                   in_SaleLot.T_ID
                 );

          TaxLinkDirectRepo(v_repolotid, in_SaleLot.T_ID, v_buylotid, in_Mode);

          v_RepoUnlotAmount := TaxRepoUnallottedAmount(in_SaleLot.t_ID, v_buylotid, in_Mode);

        END LOOP;

        v_UnlotAmount := TaxUnallottedAmount(in_SaleLot, in_Mode);

      END LOOP;

    END TaxLinkSaleToBuy;

    -- Выполнить закрытие коротких позиций по лоту продажи
    PROCEDURE TaxCloseSalePos( in_SaleLot IN dsctaxlot_dbt%ROWTYPE,
                               in_RepoLot IN dsctaxlot_dbt%ROWTYPE )
    AS
      v_buylotid dsctaxlot_dbt.t_ID%TYPE;
      v_Amount NUMBER;
      v_FreeAmount NUMBER;
      v_SalePosAmount NUMBER;
      v_lotDate DATE;
      v_lotTime DATE;

    BEGIN

      v_SalePosAmount := TaxSalePosition(in_SaleLot.t_ID, in_RepoLot.t_ID);
      WHILE v_SalePosAmount > 0 LOOP

        v_lotDate  := NULL;
        v_lotTime  := NULL;
        v_buylotid := NULL;

        BEGIN

          SELECT MIN(lot.t_Date)
            INTO v_lotDate
            FROM dsctaxlot_dbt lot
           WHERE lot.t_Type = TAXLOTS_BUY and
                 lot.t_FIID = in_SaleLot.t_FIID and
                 lot.t_Date >= in_SaleLot.t_Date and
                 (in_RepoLot.t_Date2 = TO_DATE('01-01-0001','DD-MM-YYYY') or lot.t_Date <= in_RepoLot.t_Date2) and
                 TaxFreeAmountByID(lot.t_ID, in_SaleLot.t_Date, TAXREG_ORDINARY, 0) > 0;

          SELECT MIN(lot.t_Time)
            INTO v_lotTime
            FROM dsctaxlot_dbt lot
           WHERE lot.t_Type = TAXLOTS_BUY and
                 lot.t_FIID = in_SaleLot.t_FIID and
                 lot.t_Date >= in_SaleLot.t_Date and
                 (in_RepoLot.t_Date2 = TO_DATE('01-01-0001','DD-MM-YYYY') or lot.t_Date <= in_RepoLot.t_Date2) and
                 lot.t_Date = v_lotDate and
                 TaxFreeAmountByID(lot.t_ID, in_SaleLot.t_Date, TAXREG_ORDINARY, 0) > 0;

          SELECT MIN(lot.t_ID)
            INTO v_buylotid
            FROM dsctaxlot_dbt lot
           WHERE lot.t_Type = TAXLOTS_BUY and
                 lot.t_FIID = in_SaleLot.t_FIID and
                 lot.t_Date >= in_SaleLot.t_Date and
                 (in_RepoLot.t_Date2 = TO_DATE('01-01-0001','DD-MM-YYYY') or lot.t_Date <= in_RepoLot.t_Date2) and
                 lot.t_Date = v_lotDate and
                 lot.t_Time = v_lotTime and
                 TaxFreeAmountByID(lot.t_ID, in_SaleLot.t_Date, TAXREG_ORDINARY, 0) > 0;

          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              NULL;

        END;

        EXIT WHEN v_LotDate IS NULL or
                  v_LotTime IS NULL or
                  v_buylotid IS NULL;

        v_FreeAmount := TaxFreeAmountByID(v_buylotid, in_SaleLot.t_Date, TAXREG_ORDINARY, 0);

        IF v_FreeAmount < v_SalePosAmount  THEN
          v_Amount := v_FreeAmount;
        ELSE
          v_Amount := v_SalePosAmount;
        END IF;

        INSERT INTO dsctaxlnk_dbt ( T_SALEID,
                                    T_BUYID,
                                    T_TYPE,
                                    T_AMOUNT,
                                    T_SOURCEID,
                                    T_DESTID )
        VALUES ( in_SaleLot.T_ID,
                 v_buylotid,
                 TAXLNK_CLPOS,
                 v_Amount,
                 in_RepoLot.T_ID,
                 0
               );

        v_SalePosAmount := TaxSalePosition(in_SaleLot.t_ID, in_RepoLot.t_ID);

      END LOOP;

    END TaxCloseSalePos;

    -- Выполнить списание лота продажи из следок репо обр.
    PROCEDURE TaxLinkSaleToRepo( in_SaleLot IN dsctaxlot_dbt%ROWTYPE,
                                 in_ExID IN dsctaxlot_dbt.t_ID%TYPE)
    AS
      v_AM NUMBER;
      v_repolotid dsctaxlot_dbt.t_ID%TYPE;
      r_repolot1 dsctaxlot_dbt%ROWTYPE;
      v_Amount NUMBER;
      v_FreeAmount NUMBER;
      v_UnlotAmount NUMBER;
      v_UnlotRepSP NUMBER;
      v_lotDate DATE;
      v_lotTime DATE;

      v_buySPSourceID NUMBER;
      v_buySPBuyID    NUMBER;
      v_buySPDestID   NUMBER;
      v_buySPAmount   NUMBER;
      v_A             NUMBER;
    BEGIN

      v_AM := AmortizationMethod(-1);

      v_UnlotAmount := TaxUnallottedAmount(in_SaleLot, TAXREG_ORDINARY);
      WHILE v_UnlotAmount > 0 LOOP

        v_lotDate   := NULL;
        v_lotTime   := NULL;
        v_repolotid := NULL;

        BEGIN

          SELECT MAX(lot.t_Date)
            INTO v_lotDate
            FROM dsctaxlot_dbt lot
           WHERE lot.t_Type = TAXLOTS_BACKREPO and
                 lot.t_FIID = in_SaleLot.t_FIID and
                 lot.t_Date <= in_SaleLot.t_Date and
                 (lot.t_Date2 > in_SaleLot.t_Date or lot.t_Date2 = TO_DATE('01-01-0001','DD-MM-YYYY')) and
                 (in_ExID = 0 or in_ExID <> lot.t_ID) and
                 TaxFreeAmountByID(lot.t_ID, in_SaleLot.t_Date, TAXREG_ORDINARY, 0) > 0;

          SELECT MAX(lot.t_Time)
            INTO v_lotTime
            FROM dsctaxlot_dbt lot
           WHERE lot.t_Type = TAXLOTS_BACKREPO and
                 lot.t_FIID = in_SaleLot.t_FIID and
                 lot.t_Date <= in_SaleLot.t_Date and
                 (lot.t_Date2 > in_SaleLot.t_Date or lot.t_Date2 = TO_DATE('01-01-0001','DD-MM-YYYY')) and
                 (in_ExID = 0 or in_ExID <> lot.t_ID) and
                 lot.t_Date = v_lotDate and
                 TaxFreeAmountByID(lot.t_ID, in_SaleLot.t_Date, TAXREG_ORDINARY, 0) > 0;

          SELECT MAX(lot.t_ID)
            INTO v_repolotid
            FROM dsctaxlot_dbt lot
           WHERE lot.t_Type = TAXLOTS_BACKREPO and
                 lot.t_FIID = in_SaleLot.t_FIID and
                 lot.t_Date <= in_SaleLot.t_Date and
                 (lot.t_Date2 > in_SaleLot.t_Date or lot.t_Date2 = TO_DATE('01-01-0001','DD-MM-YYYY')) and
                 (in_ExID = 0 or in_ExID <> lot.t_ID) and
                 lot.t_Date = v_lotDate and
                 lot.t_Time = v_lotTime and
                 TaxFreeAmountByID(lot.t_ID, in_SaleLot.t_Date, TAXREG_ORDINARY, 0) > 0;

          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              NULL;

        END;

        EXIT WHEN v_LotDate IS NULL or
                  v_LotTime IS NULL or
                  v_repolotid IS NULL;

        SELECT *
          INTO r_repolot1
          FROM dsctaxlot_dbt
         WHERE t_ID = v_repolotid;

        v_FreeAmount := TaxFreeAmount(r_repolot1, in_SaleLot.t_Date, TAXREG_ORDINARY, 0);

        IF v_FreeAmount < v_UnlotAmount  THEN
          v_Amount := v_FreeAmount;
        ELSE
          v_Amount := v_UnlotAmount;
        END IF;

        INSERT INTO dsctaxlnk_dbt ( T_SALEID,
                                    T_BUYID,
                                    T_TYPE,
                                    T_AMOUNT,
                                    T_SOURCEID,
                                    T_DESTID )
        VALUES ( in_SaleLot.T_ID,
                 v_repolotid,
                 TAXLNK_OPSPOS,
                 v_Amount,
                 0,
                 0
               );

        v_UnlotRepSP := TaxRepoUnallottedAmountSP(v_repolotid, in_SaleLot.T_ID, v_Amount);
        WHILE v_UnlotRepSP > 0 LOOP
          BEGIN
            IF v_AM = PM_WRITEOFF_FIFO THEN
              SELECT *
                INTO v_buySPSourceID, v_buySPBuyID, v_buySPDestID, v_buySPAmount
                FROM ( SELECT buy.t_SourceID, buy.t_BuyID, buy.t_DestID, buy.t_Amount
                         FROM V_SCTAX_RETSPBUY buy
                        WHERE buy.t_SourceID = v_repolotid
                          AND buy.t_Date <= in_SaleLot.t_Date
                          AND buy.t_DestID <> in_SaleLot.t_ID
                          AND TaxRepoFreeAmountSP(buy.t_SourceID, buy.t_BuyID, buy.t_DestID, buy.t_Amount) > 0
                     ORDER BY buy.t_Date ASC,
                              buy.t_Time ASC,
                              buy.t_ID ASC )
               WHERE ROWNUM = 1;
            ELSE
              SELECT *
                INTO v_buySPSourceID, v_buySPBuyID, v_buySPDestID, v_buySPAmount
                FROM ( SELECT buy.t_SourceID, buy.t_BuyID, buy.t_DestID, buy.t_Amount
                         FROM V_SCTAX_RETSPBUY buy
                        WHERE buy.t_SourceID = v_repolotid
                          AND buy.t_Date <= in_SaleLot.t_Date
                          AND buy.t_DestID <> in_SaleLot.t_ID
                          AND TaxRepoFreeAmountSP(buy.t_SourceID, buy.t_BuyID, buy.t_DestID, buy.t_Amount) > 0
                     ORDER BY buy.t_Date DESC,
                              buy.t_Time DESC,
                              buy.t_ID DESC )
               WHERE ROWNUM = 1;
            END IF;

          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              BEGIN
                v_buySPSourceID := NULL;
                v_buySPBuyID    := NULL;
                v_buySPDestID   := NULL;
                v_buySPAmount   := NULL;
              END;
          END;

          EXIT WHEN v_buySPSourceID IS NULL or
                    v_buySPBuyID    IS NULL or
                    v_buySPDestID   IS NULL or
                    v_buySPAmount   IS NULL;

          v_A :=  min_sum(v_UnlotRepSP,
                          TaxRepoFreeAmountSP(v_buySPSourceID, v_buySPBuyID, v_buySPDestID, v_buySPAmount) );

          INSERT INTO dsctaxlnk_dbt ( T_SALEID,
                                      T_BUYID,
                                      T_TYPE,
                                      T_AMOUNT,
                                      T_SOURCEID,
                                      T_DESTID )
          VALUES ( in_SaleLot.T_ID,
                   v_buySPBuyID,
                   TAXLNK_RETSPOS,
                   v_A,
                   v_repolotid,
                   v_buySPDestID
                 );

          v_UnlotRepSP := TaxRepoUnallottedAmountSP(v_repolotid, in_SaleLot.T_ID, v_Amount);
        END LOOP;

        TaxCloseSalePos(in_SaleLot, r_repolot1);

        v_UnlotAmount := TaxUnallottedAmount(in_SaleLot, TAXREG_ORDINARY);

      END LOOP;

    END TaxLinkSaleToRepo;

    -- Выполнить списание лота репо прямого
    PROCEDURE TaxLinkRepo( in_RepoLot IN dsctaxlot_dbt%ROWTYPE, in_Mode IN NUMBER DEFAULT TAXREG_ORDINARY )
    AS
      v_LD  NUMBER DEFAULT TAXLNK_UNDEF;
      v_LR  NUMBER DEFAULT TAXLNK_UNDEF;
      v_AM NUMBER;
      v_buylotid dsctaxlot_dbt.t_ID%TYPE;
      v_repolotid dsctaxlot_dbt.t_ID%TYPE;
      v_buylottype dsctaxlot_dbt.t_Type%TYPE;
      v_Amount NUMBER;
      v_FreeAmount NUMBER;
      v_UnlotAmount NUMBER;
      v_RepoFreeAmount NUMBER;
      v_RepoUnlotAmount NUMBER;
      v_LotDate DATE;
      v_LotTime DATE;

      v_buySPSourceID NUMBER;
      v_buySPBuyID    NUMBER;
      v_buySPDestID   NUMBER;
      v_buySPAmount   NUMBER;
      v_A             NUMBER;
      v_UnlotRepoSP   NUMBER;
    BEGIN

      IF in_Mode = TAXREG_ORDINARY THEN
        -- стандартный режим
        v_LD  := TAXLNK_REPO;
        v_LR  := TAXLNK_BREPO;
      ELSIF in_Mode = TAXREG_NETTEXCL THEN
        -- с исключением неттинга
        v_LD  := TAXLNK_DELREPO;
        v_LR  := TAXLNK_DELBREP;
      END IF;

      v_AM := AmortizationMethod(-1);
      v_UnlotAmount := TaxUnallottedAmount(in_RepoLot, in_Mode);

      WHILE v_UnlotAmount > 0 LOOP

        v_LotDate  := NULL;
        v_LotTime  := NULL;
        v_buylotid := NULL;

        BEGIN

          IF v_AM = PM_WRITEOFF_FIFO THEN

            SELECT MIN(lot.t_Date)
              INTO v_LotDate
              FROM dsctaxlot_dbt lot
             WHERE (lot.t_Type = TAXLOTS_BUY or lot.t_Type = TAXLOTS_BACKREPO) and
                   lot.t_FIID = in_RepoLot.t_FIID and
                   lot.t_Date <= in_RepoLot.t_Date and
                   TaxFreeAmountByID(lot.t_ID, in_RepoLot.t_Date, in_Mode, in_RepoLot.t_ID) > 0;

            SELECT MIN(lot.t_Time)
              INTO v_LotTime
              FROM dsctaxlot_dbt lot
             WHERE (lot.t_Type = TAXLOTS_BUY or lot.t_Type = TAXLOTS_BACKREPO) and
                   lot.t_FIID = in_RepoLot.t_FIID and
                   lot.t_Date <= in_RepoLot.t_Date and
                   lot.t_Date = v_LotDate and
                   TaxFreeAmountByID(lot.t_ID, in_RepoLot.t_Date, in_Mode, in_RepoLot.t_ID) > 0;

             SELECT MIN(lot.t_ID)
               INTO v_buylotid
               FROM dsctaxlot_dbt lot
              WHERE (lot.t_Type = TAXLOTS_BUY or lot.t_Type = TAXLOTS_BACKREPO) and
                    lot.t_FIID = in_RepoLot.t_FIID and
                    lot.t_Date <= in_RepoLot.t_Date and
                    lot.t_Date = v_LotDate and
                    lot.t_Time = v_LotTime and
                    TaxFreeAmountByID(lot.t_ID, in_RepoLot.t_Date, in_Mode, in_RepoLot.t_ID) > 0;
          ELSE

            SELECT MAX(lot.t_Date)
              INTO v_LotDate
              FROM dsctaxlot_dbt lot
             WHERE (lot.t_Type = TAXLOTS_BUY or lot.t_Type = TAXLOTS_BACKREPO) and
                   lot.t_FIID = in_RepoLot.t_FIID and
                   lot.t_Date <= in_RepoLot.t_Date and
                   TaxFreeAmountByID(lot.t_ID, in_RepoLot.t_Date, in_Mode, in_RepoLot.t_ID) > 0;

            SELECT MAX(lot.t_Time)
              INTO v_LotTime
              FROM dsctaxlot_dbt lot
             WHERE (lot.t_Type = TAXLOTS_BUY or lot.t_Type = TAXLOTS_BACKREPO) and
                   lot.t_FIID = in_RepoLot.t_FIID and
                   lot.t_Date <= in_RepoLot.t_Date and
                   lot.t_Date = v_LotDate and
                   TaxFreeAmountByID(lot.t_ID, in_RepoLot.t_Date, in_Mode, in_RepoLot.t_ID) > 0;

             SELECT MAX(lot.t_ID)
               INTO v_buylotid
               FROM dsctaxlot_dbt lot
              WHERE (lot.t_Type = TAXLOTS_BUY or lot.t_Type = TAXLOTS_BACKREPO) and
                    lot.t_FIID = in_RepoLot.t_FIID and
                    lot.t_Date <= in_RepoLot.t_Date and
                    lot.t_Date = v_LotDate and
                    lot.t_Time = v_LotTime and
                    TaxFreeAmountByID(lot.t_ID, in_RepoLot.t_Date, in_Mode, in_RepoLot.t_ID) > 0;
          END IF;

          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              NULL;

        END;

        EXIT WHEN v_LotDate IS NULL or
                  v_LotTime IS NULL or
                  v_buylotid IS NULL;

        v_FreeAmount := TaxFreeAmountByID(v_buylotid, in_RepoLot.t_Date, in_Mode, in_RepoLot.t_ID);

        IF v_FreeAmount < v_UnlotAmount  THEN
          v_Amount := v_FreeAmount;
        ELSE
          v_Amount := v_UnlotAmount;
        END IF;

        INSERT INTO dsctaxlnk_dbt ( T_SALEID,
                                    T_BUYID,
                                    T_TYPE,
                                    T_AMOUNT,
                                    T_SOURCEID,
                                    T_DESTID )
        VALUES ( in_RepoLot.T_ID,
                 v_buylotid,
                 v_LD,
                 v_Amount,
                 0,
                 0
               );


        SELECT t_Type
          INTO v_buylottype
          FROM dsctaxlot_dbt
         WHERE t_ID = v_buylotid;

        IF v_buylottype = TAXLOTS_BACKREPO AND in_Mode = TAXREG_ORDINARY THEN

          v_UnlotRepoSP := TaxRepoUnallottedAmountSP(v_buylotid, in_RepoLot.T_ID, v_Amount);
          WHILE v_UnlotRepoSP > 0 LOOP

            BEGIN
              IF v_AM = PM_WRITEOFF_FIFO THEN
                SELECT *
                  INTO v_buySPSourceID, v_buySPBuyID, v_buySPDestID, v_buySPAmount
                  FROM ( SELECT buy.t_SourceID, buy.t_BuyID, buy.t_DestID, buy.t_Amount
                           FROM V_SCTAX_RETSPBUY buy
                          WHERE buy.t_SourceID = v_buylotid
                            AND buy.t_Date <= in_RepoLot.t_Date
                            AND buy.t_BuyID <> in_RepoLot.t_ID
                            AND TaxRepoFreeAmountSP(buy.t_SourceID, buy.t_BuyID, buy.t_DestID, buy.t_Amount) > 0
                       ORDER BY buy.t_Date ASC,
                                buy.t_Time ASC,
                                buy.t_ID ASC )
                 WHERE ROWNUM = 1;
              ELSE
                SELECT *
                  INTO v_buySPSourceID, v_buySPBuyID, v_buySPDestID, v_buySPAmount
                  FROM ( SELECT buy.t_SourceID, buy.t_BuyID, buy.t_DestID, buy.t_Amount
                           FROM V_SCTAX_RETSPBUY buy
                          WHERE buy.t_SourceID = v_buylotid
                            AND buy.t_Date <= in_RepoLot.t_Date
                            AND buy.t_BuyID <> in_RepoLot.t_ID
                            AND TaxRepoFreeAmountSP(buy.t_SourceID, buy.t_BuyID, buy.t_DestID, buy.t_Amount) > 0
                       ORDER BY buy.t_Date DESC,
                                buy.t_Time DESC,
                                buy.t_ID DESC )
                 WHERE ROWNUM = 1;
              END IF;

            EXCEPTION
              WHEN NO_DATA_FOUND THEN
                BEGIN
                  v_buySPSourceID := NULL;
                  v_buySPBuyID    := NULL;
                  v_buySPDestID   := NULL;
                  v_buySPAmount   := NULL;
                END;
            END;

            EXIT WHEN v_buySPSourceID IS NULL or
                      v_buySPBuyID    IS NULL or
                      v_buySPDestID   IS NULL or
                      v_buySPAmount   IS NULL;


            v_A :=  min_sum(v_UnlotRepoSP,
                            TaxRepoFreeAmountSP(v_buySPSourceID, v_buySPBuyID, v_buySPDestID, v_buySPAmount) );

            INSERT INTO dsctaxlnk_dbt ( T_SALEID,
                                        T_BUYID,
                                        T_TYPE,
                                        T_AMOUNT,
                                        T_SOURCEID,
                                        T_DESTID )
            VALUES ( in_RepoLot.T_ID,
                     v_buySPBuyID,
                     TAXLNK_RETSPOS,
                     v_A,
                     v_buylotid,
                     v_buySPDestID
                   );

            v_UnlotRepoSP := TaxRepoUnallottedAmountSP(v_buylotid, in_RepoLot.T_ID, v_Amount);
          END LOOP;

        ELSIF v_buylottype = TAXLOTS_BUY THEN
          v_RepoUnlotAmount := TaxRepoUnallottedAmount(in_RepoLot.t_ID, v_buylotid, in_Mode);
          WHILE v_RepoUnlotAmount > 0 LOOP

            v_LotDate   := NULL;
            v_LotTime   := NULL;
            v_repolotid := NULL;

            BEGIN

              IF v_AM = PM_WRITEOFF_FIFO THEN

                SELECT MIN(lot.t_Date)
                  INTO v_LotDate
                  FROM dsctaxlot_dbt lot
                 WHERE lot.t_Type = TAXLOTS_REPO and
                       lot.t_Date2 <= in_RepoLot.t_Date and
                       lot.t_Date2 <> TO_DATE('01-01-0001','DD-MM-YYYY') and
                       Exists( SELECT lnk.t_Type
                                 FROM dsctaxlnk_dbt lnk
                                WHERE lnk.t_Type = v_LD and
                                      lnk.t_SaleID = lot.t_ID and
                                      lnk.t_BuyID = v_buylotid ) and
                       TaxRepoFreeAmount(lot.t_ID, v_buylotid, in_Mode) > 0;

                SELECT MIN(lot.t_Time)
                  INTO v_LotTime
                  FROM dsctaxlot_dbt lot
                 WHERE lot.t_Type = TAXLOTS_REPO and
                       lot.t_Date2 <= in_RepoLot.t_Date and
                       lot.t_Date2 <> TO_DATE('01-01-0001','DD-MM-YYYY') and
                       Exists( SELECT lnk.t_Type
                                 FROM dsctaxlnk_dbt lnk
                                WHERE lnk.t_Type = v_LD and
                                      lnk.t_SaleID = lot.t_ID and
                                      lnk.t_BuyID = v_buylotid ) and
                       lot.t_Date = v_LotDate and
                       TaxRepoFreeAmount(lot.t_ID, v_buylotid, in_Mode) > 0;

                SELECT MIN(lot.t_ID)
                  INTO v_repolotid
                  FROM dsctaxlot_dbt lot
                 WHERE lot.t_Type = TAXLOTS_REPO and
                       lot.t_Date2 <= in_RepoLot.t_Date and
                       lot.t_Date2 <> TO_DATE('01-01-0001','DD-MM-YYYY') and
                       Exists( SELECT lnk.t_Type
                                 FROM dsctaxlnk_dbt lnk
                                WHERE lnk.t_Type = v_LD and
                                      lnk.t_SaleID = lot.t_ID and
                                      lnk.t_BuyID = v_buylotid ) and
                       lot.t_Date = v_LotDate and
                       lot.t_Time = v_LotTime and
                       TaxRepoFreeAmount(lot.t_ID, v_buylotid, in_Mode) > 0;
              ELSE
                SELECT MAX(lot.t_Date)
                  INTO v_LotDate
                  FROM dsctaxlot_dbt lot
                 WHERE lot.t_Type = TAXLOTS_REPO and
                       lot.t_Date2 <= in_RepoLot.t_Date and
                       lot.t_Date2 <> TO_DATE('01-01-0001','DD-MM-YYYY') and
                       Exists( SELECT lnk.t_Type
                                 FROM dsctaxlnk_dbt lnk
                                WHERE lnk.t_Type = v_LD and
                                      lnk.t_SaleID = lot.t_ID and
                                      lnk.t_BuyID = v_buylotid ) and
                       TaxRepoFreeAmount(lot.t_ID, v_buylotid, in_Mode) > 0;

                SELECT MAX(lot.t_Time)
                  INTO v_LotTime
                  FROM dsctaxlot_dbt lot
                 WHERE lot.t_Type = TAXLOTS_REPO and
                       lot.t_Date2 <= in_RepoLot.t_Date and
                       lot.t_Date2 <> TO_DATE('01-01-0001','DD-MM-YYYY') and
                       Exists( SELECT lnk.t_Type
                                 FROM dsctaxlnk_dbt lnk
                                WHERE lnk.t_Type = v_LD and
                                      lnk.t_SaleID = lot.t_ID and
                                      lnk.t_BuyID = v_buylotid ) and
                       lot.t_Date = v_LotDate and
                       TaxRepoFreeAmount(lot.t_ID, v_buylotid, in_Mode) > 0;

                SELECT MAX(lot.t_ID)
                  INTO v_repolotid
                  FROM dsctaxlot_dbt lot
                 WHERE lot.t_Type = TAXLOTS_REPO and
                       lot.t_Date2 <= in_RepoLot.t_Date and
                       lot.t_Date2 <> TO_DATE('01-01-0001','DD-MM-YYYY') and
                       Exists( SELECT lnk.t_Type
                                 FROM dsctaxlnk_dbt lnk
                                WHERE lnk.t_Type = v_LD and
                                      lnk.t_SaleID = lot.t_ID and
                                      lnk.t_BuyID = v_buylotid ) and
                       lot.t_Date = v_LotDate and
                       lot.t_Time = v_LotTime and
                       TaxRepoFreeAmount(lot.t_ID, v_buylotid, in_Mode) > 0;
              END IF;

              EXCEPTION
                WHEN NO_DATA_FOUND THEN
                  NULL;

            END;

            EXIT WHEN v_LotDate IS NULL or
                      v_LotTime IS NULL or
                      v_repolotid IS NULL;

            v_RepoFreeAmount := TaxRepoFreeAmount(v_repolotid, v_buylotid, in_Mode);

            IF v_RepoFreeAmount < v_RepoUnlotAmount THEN
              v_Amount := v_RepoFreeAmount;
            ELSE
              v_Amount := v_RepoUnlotAmount;
            END IF;

            INSERT INTO dsctaxlnk_dbt ( T_SALEID,
                                        T_BUYID,
                                        T_TYPE,
                                        T_AMOUNT,
                                        T_SOURCEID,
                                        T_DESTID )
            VALUES ( in_RepoLot.T_ID,
                     v_repolotid,
                     v_LR,
                     v_Amount,
                     v_buylotid,
                     0
                   );

            v_RepoUnlotAmount := TaxRepoUnallottedAmount(in_RepoLot.t_ID, v_buylotid, in_Mode);

          END LOOP;

        END IF;

        v_UnlotAmount := TaxUnallottedAmount(in_RepoLot, in_Mode);

      END LOOP;

      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          NULL;

    END TaxLinkRepo;

    -- Выполнить закрытие коротких позиций по лоту репо обратному 2 ч.
    PROCEDURE TaxCloseRepoPos( in_RepoLot IN dsctaxlot_dbt%ROWTYPE )
    AS
      r_vbuylot dsctaxlot_dbt%ROWTYPE;
      r_vsalelot dsctaxlot_dbt%ROWTYPE;
      v_salelotid dsctaxlot_dbt.t_ID%TYPE;
      v_S NUMBER;
      v_R NUMBER;
      v_Amount NUMBER;
      v_CountVirt NUMBER;
      v_ID NUMBER;

      TYPE SaleLotCurTyp IS REF CURSOR;
      SaleLot_cur SaleLotCurTyp;

    BEGIN

      v_S := 0;
      v_R := 0;

      v_S := TaxRepoPosition(in_RepoLot.t_ID);
      IF v_S > 0 THEN

        SELECT COUNT(1)
          INTO v_CountVirt
          FROM dsctaxlot_dbt
         WHERE t_Date = in_RepoLot.t_Date2 and
               t_IsVirtual = CHR(88);

        r_vbuylot.t_ID        := 0;
        r_vbuylot.T_DEALID    := 0;
        r_vbuylot.T_FIID      := in_RepoLot.t_FIID;
        r_vbuylot.T_TYPE      := TAXLOTS_BUY;
        r_vbuylot.T_ISVIRTUAL := CHR(88);
        r_vbuylot.T_NUMBER    := 'V'||TO_CHAR(in_RepoLot.t_Date2,'DDMMYY')||'/'||LTRIM(TO_CHAR(v_CountVirt+1,'0000'));
        r_vbuylot.T_BUYID     := 0;
        r_vbuylot.T_DATE      := in_RepoLot.t_Date2;
        r_vbuylot.T_TIME      := in_RepoLot.t_Time2;
        r_vbuylot.T_DATE2     := TO_DATE('01-01-0001', 'DD-MM-YYYY');
        r_vbuylot.T_TIME2     := TO_DATE('01-01-0001', 'DD-MM-YYYY');
        r_vbuylot.T_AMOUNT    := v_S;
        r_vbuylot.T_NETTING   := 0;

        INSERT INTO dsctaxlot_dbt VALUES r_vbuylot RETURNING t_ID INTO v_ID;

        BEGIN

          OPEN SaleLot_cur FOR SELECT lot.t_ID
                                 FROM dsctaxlot_dbt lot, dsctaxlnk_dbt lnk
                                WHERE lnk.t_BuyID = in_RepoLot.t_ID and
                                      lnk.t_Type = TAXLNK_OPSPOS and
                                      lnk.t_SaleID = lot.t_ID and
                                      TaxSalePosition(lot.t_ID, in_RepoLot.t_ID) > 0;

          LOOP

            FETCH SaleLot_cur INTO v_salelotid;
            EXIT WHEN SaleLot_cur%NOTFOUND OR
                      SaleLot_cur%NOTFOUND IS NULL OR
                      v_R > v_S;

            v_Amount := TaxSalePosition(v_salelotid, in_RepoLot.t_ID);
            v_R := v_R + v_Amount;

            INSERT INTO dsctaxlnk_dbt ( T_SALEID,
                                        T_BUYID,
                                        T_TYPE,
                                        T_AMOUNT,
                                        T_SOURCEID,
                                        T_DESTID )
            VALUES ( v_salelotid,
                     v_ID,
                     TAXLNK_CLPOS,
                     v_Amount,
                     in_RepoLot.t_ID,
                     0
                   );
          END LOOP;

          CLOSE SaleLot_cur;

        END;

--        IF v_R > v_S THEN
--          RAISE_APPLICATION_ERROR(-20500,'Недостаточно ц/б');
--        ELSIF v_R < v_S THEN
--          RAISE_APPLICATION_ERROR(-20501,'Развалилась база');
--        END IF;

        IF v_R > v_S THEN
          InsertError( 0,
                       in_RepoLot.t_FIID,
                       'Предупреждение',
                       'Недостаточно ц/б: сумма незакрытых остатков кор. позиций по лотам продажи, возникшим по лоту обр. РЕПО'||in_RepoLot.t_ID||
                       ' больше незакрытого остатка короткой позиции по лоту РЕПО обратного'||in_RepoLot.t_ID );
          COMMIT;
        END IF;

        IF v_R < v_S THEN
          InsertError( 0,
                       in_RepoLot.t_FIID,
                       'Ошибка',
                       'Развалилась база: сумма незакрытых остатков кор. позиций по лотам продажи, возникшим по лоту обр. РЕПО'||in_RepoLot.t_ID||
                       ' меньше незакрытого остатка короткой позиции по лоту РЕПО обратного'||in_RepoLot.t_ID );
          COMMIT;

          RAISE_APPLICATION_ERROR(-20501,'Развалилась база');
        END IF;

        IF v_R = v_S THEN

          SELECT COUNT(1) INTO v_CountVirt
            FROM dsctaxlot_dbt
           WHERE t_Date = in_RepoLot.t_Date2 and
                 t_IsVirtual = CHR(88);

          r_vsalelot.t_ID        := 0;
          r_vsalelot.T_DEALID    := 0;
          r_vsalelot.T_FIID      := in_RepoLot.t_FIID;
          r_vsalelot.T_TYPE      := TAXLOTS_SALE;
          r_vsalelot.T_ISVIRTUAL := CHR(88);
          r_vsalelot.T_NUMBER    := 'V'||TO_CHAR(in_RepoLot.t_Date2,'DDMMYY')||'/'||LTRIM(TO_CHAR(v_CountVirt+1,'0000'));
          r_vsalelot.T_BUYID     := v_ID;
          r_vsalelot.T_DATE      := in_RepoLot.t_Date2;
          r_vsalelot.T_TIME      := in_RepoLot.t_Time2;
          r_vsalelot.T_DATE2     := TO_DATE('01-01-0001', 'DD-MM-YYYY');
          r_vsalelot.T_TIME2     := TO_DATE('01-01-0001', 'DD-MM-YYYY');
          r_vsalelot.T_AMOUNT    := v_S;
          r_vsalelot.T_NETTING   := 0;

          INSERT INTO dsctaxlot_dbt VALUES r_vsalelot RETURNING t_ID INTO v_ID;

          SELECT *
            INTO r_vsalelot
            FROM dsctaxlot_dbt
           WHERE t_ID = v_ID;

          TaxLinkSaleToRepo(r_vsalelot, in_RepoLot.t_ID);

        END IF;

      END IF;

      EXCEPTION
        WHEN NO_DATA_FOUND
        THEN
          NULL;

    END TaxCloseRepoPos;

  BEGIN

    BEGIN
      IF DebugFIID_in <> -1 THEN
        EXECUTE IMMEDIATE 'DELETE FROM DSCTAXMES_TMP WHERE T_FIID = :1'
                    USING DebugFIID_in;
      ELSE
        EXECUTE IMMEDIATE 'TRUNCATE TABLE DSCTAXMES_TMP';
      END IF;

      EXCEPTION
        WHEN OTHERS
        THEN NULL;
    END;

    -- берем значения дат из настроек
    BEGIN
      v_KeyId := rsb_tools.find_regkey('SECUR\DATE_BUILD_TAXREG');

      SELECT TO_DATE(rsb_struct.getString(t_fmtblobdata_xxxx),'DD.MM.YYYY')
        INTO v_BegDate
       FROM dregval_dbt
       WHERE t_KeyID = v_KeyId;

      EXCEPTION
        WHEN OTHERS
        THEN v_BegDate := TO_DATE('01-01-0001','DD-MM-YYYY');
    END;

    BEGIN
      v_KeyId := rsb_tools.find_regkey('SECUR\DATE_BUILD_TAXREGEXCLNETT');

      SELECT TO_DATE(rsb_struct.getString(t_fmtblobdata_xxxx),'DD.MM.YYYY')
        INTO v_ExclNettBegDate
       FROM dregval_dbt
       WHERE t_KeyID = v_KeyId;

      EXCEPTION
        WHEN OTHERS
        THEN v_ExclNettBegDate := TO_DATE('01-01-0001','DD-MM-YYYY');
    END;

    -- проверяем и корректируем даты
    v_BegDate := date_min(v_BegDate, BegDate_in);
    v_ExclNettBegDate := date_min(v_ExclNettBegDate, v_BegDate);
    IF ExclNetting_in THEN
      v_ExclNettEndDate := EndDate_in;
    ELSE
      IF v_ExclNettBegDate <> TO_DATE('01-01-0001','DD-MM-YYYY') THEN
        v_ExclNettEndDate := date_min(v_ExclNettBegDate-1, EndDate_in);
      ELSE
        v_ExclNettEndDate := date_min(v_ExclNettBegDate, EndDate_in);
      END IF;
    END IF;

    -- чистим таблицы за период, начиная с v_BegDate и v_ExclNettBegDate
    DELETE
      FROM dsctaxlnk_dbt lnk
     WHERE ( (lnk.t_Type IN (TAXLNK_BS, TAXLNK_REPO, TAXLNK_OPSPOS, TAXLNK_CLPOS)) and
             ( Exists( SELECT lot.t_Id
                         FROM dsctaxlot_dbt lot
                        WHERE lot.t_Id = lnk.t_SaleID AND lot.t_Date >= v_BegDate
                          AND (DebugFIID_in = -1 or DebugFIID_in = lot.t_FIID) ) OR
               Exists( SELECT lot.t_Id
                         FROM dsctaxlot_dbt lot
                        WHERE lot.t_Id = lnk.t_BuyID AND lot.t_Date >= v_BegDate
                          AND (DebugFIID_in = -1 or DebugFIID_in = lot.t_FIID) ) ) ) or
           ( (lnk.t_Type IN (TAXLNK_BREPO, TAXLNK_RETURN2)) and
             ( Exists( SELECT lot.t_Id
                         FROM dsctaxlot_dbt lot
                        WHERE lot.t_Id = lnk.t_SaleID AND lot.t_Date >= v_BegDate AND (lot.t_Date2 >= v_BegDate OR lot.t_Date2 = TO_DATE('01.01.0001','DD.MM.YYYY'))
                          AND (DebugFIID_in = -1 or DebugFIID_in = lot.t_FIID) ) OR
               Exists( SELECT lot.t_Id
                         FROM dsctaxlot_dbt lot
                        WHERE lot.t_Id = lnk.t_BuyID AND lot.t_Date >= v_BegDate
                          AND (DebugFIID_in = -1 or DebugFIID_in = lot.t_FIID) ) ) ) or
           ( (lnk.t_Type IN (TAXLNK_DELIVER, TAXLNK_DELREPO)) and
             ( Exists( SELECT lot.t_Id
                         FROM dsctaxlot_dbt lot
                        WHERE lot.t_Id = lnk.t_SaleID AND lot.t_Date >= v_ExclNettBegDate
                          AND (DebugFIID_in = -1 or DebugFIID_in = lot.t_FIID) ) OR
               Exists( SELECT lot.t_Id
                         FROM dsctaxlot_dbt lot
                        WHERE lot.t_Id = lnk.t_BuyID AND lot.t_Date >= v_ExclNettBegDate
                          AND (DebugFIID_in = -1 or DebugFIID_in = lot.t_FIID) ) ) ) or
           ( (lnk.t_Type IN (TAXLNK_DELBREP, TAXLNK_DELRET2)) and
             ( Exists( SELECT lot.t_Id
                         FROM dsctaxlot_dbt lot
                        WHERE lot.t_Id = lnk.t_SaleID AND lot.t_Date >= v_BegDate AND (lot.t_Date2 >= v_ExclNettBegDate OR lot.t_Date2 = TO_DATE('01.01.0001','DD.MM.YYYY'))
                          AND (DebugFIID_in = -1 or DebugFIID_in = lot.t_FIID) ) OR
               Exists( SELECT lot.t_Id
                         FROM dsctaxlot_dbt lot
                        WHERE lot.t_Id = lnk.t_BuyID AND lot.t_Date >= v_ExclNettBegDate
                          AND (DebugFIID_in = -1 or DebugFIID_in = lot.t_FIID) ) ) ) or
           ( (lnk.t_Type IN (TAXLNK_RETURN2)) and
             ( Exists( SELECT lot.t_Id
                         FROM dsctaxlot_dbt lot
                        WHERE lot.t_Id = lnk.t_DestID AND lot.t_Date >= v_BegDate
                          AND (DebugFIID_in = -1 or DebugFIID_in = lot.t_FIID) ) ) ) or
           ( (lnk.t_Type IN (TAXLNK_DELRET2)) and
             ( Exists( SELECT lot.t_Id
                         FROM dsctaxlot_dbt lot
                        WHERE lot.t_Id = lnk.t_DestID AND lot.t_Date >= v_ExclNettBegDate
                          AND (DebugFIID_in = -1 or DebugFIID_in = lot.t_FIID) ) ) );

    DELETE
      FROM dsctaxlot_dbt
     WHERE t_Date >= v_BegDate and
           (DebugFIID_in = -1 or DebugFIID_in = t_FIID);

    UPDATE dsctaxlot_dbt
       SET t_Date2 = TO_DATE('01.01.0001','DD.MM.YYYY'),
           t_Time2 = TO_DATE('01.01.0001','DD.MM.YYYY')
     WHERE t_Date2 >= v_BegDate and
           (DebugFIID_in = -1 or DebugFIID_in = t_FIID);

    COMMIT;

    -- выполняем расчет только для еще нерасчитанного периода
    IF EndDate_in >= v_BegDate THEN

      -- Заносим лоты покупок/продаж, совершенных за период
      INSERT INTO dsctaxlot_dbt ( T_ID,
                                  T_DEALID,
                                  T_FIID,
                                  T_TYPE,
                                  T_ISVIRTUAL,
                                  T_NUMBER,
                                  T_BUYID,
                                  T_DATE,
                                  T_TIME,
                                  T_DATE2,
                                  T_TIME2,
                                  T_AMOUNT,
                                  T_NETTING )
        SELECT 0,
               pws.t_DealID f_DealID,
               pws.t_FIID f_FIID,
               (CASE WHEN (IsBuy(get_OperationGroup(opr.t_SysTypes))=1 AND
                           IsBackSale(get_OperationGroup(opr.t_SysTypes))=1) and
                          IsRepo(get_OperationGroup(opr.t_SysTypes))<>1
                          THEN (CASE WHEN pm.t_Purpose=1 /*BAi*/ THEN TAXLOTS_BUY
                                ELSE TAXLOTS_SALE END)
                     WHEN (IsSale(get_OperationGroup(opr.t_SysTypes))=1 AND
                           IsBackSale(get_OperationGroup(opr.t_SysTypes))=1) and
                          IsRepo(get_OperationGroup(opr.t_SysTypes))<>1
                          THEN (CASE WHEN pm.t_Purpose=1 /*BAi*/ THEN TAXLOTS_SALE
                                ELSE TAXLOTS_BUY END)
                     WHEN (IsBuy(get_OperationGroup(opr.t_SysTypes))=1 OR
                           IsAvrWrtIn(get_OperationGroup(opr.t_SysTypes))=1) and
                          IsRepo(get_OperationGroup(opr.t_SysTypes))<>1 and
                          IsBackSale(get_OperationGroup(opr.t_SysTypes))<>1
                          THEN TAXLOTS_BUY
                     WHEN (IsSale(get_OperationGroup(opr.t_SysTypes))=1 OR
                           IsAvrWrtOut(get_OperationGroup(opr.t_SysTypes))=1 OR
                           check_GroupStr(get_OperationGroup(opr.t_SysTypes),IS_RET_ISSUE)=1) and
                          IsRepo(get_OperationGroup(opr.t_SysTypes))<>1 and
                          IsBackSale(get_OperationGroup(opr.t_SysTypes))<>1
                          THEN TAXLOTS_SALE
                     WHEN IsRepo(get_OperationGroup(opr.t_SysTypes))=1 AND
                          IsBuy(get_OperationGroup(opr.t_SysTypes))=1
                          THEN TAXLOTS_BACKREPO
                     WHEN IsRepo(get_OperationGroup(opr.t_SysTypes))=1 AND
                          IsSale(get_OperationGroup(opr.t_SysTypes))=1
                          THEN TAXLOTS_REPO
                     ELSE TAXLOTS_UNDEF END) f_Type,
               CHR(0),
               dl.t_DealCode,
               0,
               pws.t_Date f_Date,
               pws.t_Time f_Time,
               TO_DATE('01-01-0001','DD-MM-YYYY'),
               TO_DATE('01-01-0001','DD-MM-YYYY'),
               pm.t_Amount Amount,
               (CASE WHEN (IsSale(get_OperationGroup(opr.t_SysTypes))=1 and IsRepo(get_OperationGroup(opr.t_SysTypes))=1) or
                          (IsBuy(get_OperationGroup(opr.t_SysTypes))=1 and IsRepo(get_OperationGroup(opr.t_SysTypes))=1)
                          THEN 0
                     ELSE (SELECT NVL(SUM(TaxGetNettingAmountById(dl.t_DealID, pmlnk.t_PaymLinkID)),0)
                             FROM dpmlink_dbt pmlnk
                            WHERE pmlnk.t_InitialPayment = pm.t_PaymentID and
                                  pmlnk.t_LinkKind = 1) END) Netting
          FROM dpmwrtsum_dbt pws, doprkoper_dbt opr, ddl_tick_dbt dl, dpmpaym_dbt pm, dfininstr_dbt fin
         WHERE dl.T_DEALID = pws.T_DEALID and
               fin.t_FIID = pws.t_FIID and
               (DebugFIID_in = -1 or pws.t_FIID = DebugFIID_in) and
               opr.T_KIND_OPERATION = dl.t_DealType and
               pm.t_PaymentID = pws.t_DocID and
               pws.t_DocKind = 29 /*DLDOC_PAYMENT*/ and
               pws.t_Date >= v_BegDate and
               pws.t_Date <= EndDate_in and
               pws.t_State >= 40 /*PM_WRTSUM_FORM*/ and
               (pws.t_GroupID != 0) and
               (pws.t_Party = 0 or pws.t_Party = -1) and
               ( (IsBuy(get_OperationGroup(opr.t_SysTypes))=1 and IsRepo(get_OperationGroup(opr.t_SysTypes))<>1 and IsBackSale(get_OperationGroup(opr.t_SysTypes))<>1) or -- покупка
                 (IsSale(get_OperationGroup(opr.t_SysTypes))=1 and IsRepo(get_OperationGroup(opr.t_SysTypes))<>1 and IsBackSale(get_OperationGroup(opr.t_SysTypes))<>1) or -- продажа
                 (IsRepo(get_OperationGroup(opr.t_SysTypes))=1 AND IsSale(get_OperationGroup(opr.t_SysTypes))=1) or  -- репо продажа
                 (IsRepo(get_OperationGroup(opr.t_SysTypes))=1 AND IsBuy(get_OperationGroup(opr.t_SysTypes))=1) or -- репо покупка
                 (check_GroupStr(get_OperationGroup(opr.t_SysTypes),IS_RET_ISSUE)=1 and IsRepo(get_OperationGroup(opr.t_SysTypes))<>1) or -- погашение выпуска
                 (IsBuy(get_OperationGroup(opr.t_SysTypes))=1 AND IsBackSale(get_OperationGroup(opr.t_SysTypes))=1 and IsRepo(get_OperationGroup(opr.t_SysTypes))<>1) or -- покупка с обратной продажей
                 (IsSale(get_OperationGroup(opr.t_SysTypes))=1 AND IsBackSale(get_OperationGroup(opr.t_SysTypes))=1 and IsRepo(get_OperationGroup(opr.t_SysTypes))<>1) or -- продажа с обратным выкупом
                 (IsAvrWrtIn(get_OperationGroup(opr.t_SysTypes))=1 and IsRepo(get_OperationGroup(opr.t_SysTypes))<>1 and IsBackSale(get_OperationGroup(opr.t_SysTypes))<>1) or -- зачисление
                 (IsAvrWrtOut(get_OperationGroup(opr.t_SysTypes))=1 and IsRepo(get_OperationGroup(opr.t_SysTypes))<>1 and IsBackSale(get_OperationGroup(opr.t_SysTypes))<>1) -- списание
               ) and
               ( pm.t_Purpose=1 /*BAi*/ or
                 ( pm.t_Purpose=3 /*BRi*/ and
                   not(IsRepo(get_OperationGroup(opr.t_SysTypes))=1 AND IsBuy(get_OperationGroup(opr.t_SysTypes))=1) and
                   not(IsRepo(get_OperationGroup(opr.t_SysTypes))=1 AND IsSale(get_OperationGroup(opr.t_SysTypes))=1)
                 )
               )
      ORDER BY f_Date,
               f_Time;

      COMMIT;

      -- установим дату лотов по 2-й части Репо, совершенных за период
      IF EndDate_in >= v_BegDate THEN
        FOR c IN WrtSumRepo2 LOOP

          UPDATE dsctaxlot_dbt taxl
             SET taxl.t_Date2 = c.t_Date,
                 taxl.t_Time2 = c.t_Time
           WHERE ( taxl.t_DealID = c.t_DealID and
                   ( (IsRepo(c.oGroup)=1 and IsSale(c.oGroup)=1 and
                      taxl.t_Type = TAXLOTS_REPO) or
                      taxl.t_Type = TAXLOTS_BACKREPO) );

        END LOOP;
      END IF;

      COMMIT;

      -- закрываем короткие позиции, которые были открыты за прошлые периоды, новыми сделками покупками
      IF EndDate_in >= v_BegDate THEN
        BEGIN
          OPEN RepoSaleLot_cur FOR SELECT repolot.t_ID, salelot.t_ID
                                     FROM dsctaxlot_dbt repolot, dsctaxlot_dbt salelot, dsctaxlnk_dbt lnk
                                    WHERE repolot.t_Type = TAXLOTS_BACKREPO and
                                          (repolot.t_Date2 >= v_BegDate or
                                           repolot.t_Date2 = TO_DATE('01-01-0001','DD-MM-YYYY')) and
                                          repolot.t_Date < v_BegDate and
                                          salelot.t_Type = TAXLOTS_SALE and
                                          salelot.t_Date < v_BegDate and
                                          (DebugFIID_in = -1 or salelot.t_FIID = DebugFIID_in) and
                                          lnk.t_SaleID = salelot.t_ID and
                                          lnk.t_BuyID = repolot.t_ID and
                                          lnk.t_Type = TAXLNK_OPSPOS
                                 ORDER BY salelot.t_Date ASC,
                                          salelot.t_Time ASC,
                                          salelot.t_ID ASC;
          LOOP

            FETCH RepoSaleLot_cur INTO v_repolot_id, v_salelot_id;
            EXIT WHEN RepoSaleLot_cur%NOTFOUND OR
                      RepoSaleLot_cur%NOTFOUND IS NULL;

            IF TaxSalePosition(v_salelot_id, v_repolot_id) > 0 THEN
              SELECT *
                INTO r_salelot
                FROM dsctaxlot_dbt
               WHERE t_ID = v_salelot_id;

              SELECT *
                INTO r_repolot
                FROM dsctaxlot_dbt
               WHERE t_ID = v_repolot_id;

              TaxCloseSalePos(r_salelot, r_repolot);
            END IF;

          END LOOP;

          CLOSE RepoSaleLot_cur;
        END;
      END IF;

      COMMIT;

      -- выполняем стандартное списание
      IF EndDate_in >= v_BegDate THEN
        BEGIN
          OPEN RepoSaleLot_cur FOR SELECT T_ID
                                     FROM ( (SELECT lot.t_id, lot.t_Date D, lot.t_Time T, 2 ORD
                                               FROM dsctaxlot_dbt lot
                                              WHERE (lot.t_Type = TAXLOTS_SALE or
                                                     lot.t_Type = TAXLOTS_REPO) and
                                                    lot.t_Date >= v_BegDate and
                                                    lot.t_Date <= EndDate_in and
                                                    (DebugFIID_in = -1 or lot.t_FIID = DebugFIID_in))
                                          UNION ALL
                                            (SELECT lot2.t_id, lot2.t_Date2 D, lot2.t_Time2 T, 1 ORD
                                               FROM dsctaxlot_dbt lot2
                                              WHERE lot2.t_Type = TAXLOTS_BACKREPO and
                                                    lot2.t_Date2 >= v_BegDate and
                                                    lot2.t_Date2 <= EndDate_in and
                                                    lot2.t_Date2 <> TO_DATE('01-01-0001','DD-MM-YYYY') and
                                                    (DebugFIID_in = -1 or lot2.t_FIID = DebugFIID_in)))
                                 ORDER BY D ASC,
                                          ORD ASC,
                                          T ASC,
                                          t_ID ASC;
          LOOP

            FETCH RepoSaleLot_cur INTO v_salelot_id;
            EXIT WHEN RepoSaleLot_cur%NOTFOUND OR
                      RepoSaleLot_cur%NOTFOUND IS NULL;

            SELECT *
              INTO r_salelot
              FROM dsctaxlot_dbt
             WHERE t_ID = v_salelot_id;

            IF r_salelot.t_Type = TAXLOTS_SALE THEN
              TaxLinkSaleToBuy(r_salelot, TAXREG_ORDINARY);
              TaxLinkSaleToRepo(r_salelot, 0);
            ELSIF r_salelot.t_Type = TAXLOTS_REPO THEN
              TaxLinkRepo(r_salelot, TAXREG_ORDINARY);
            ELSIF r_salelot.t_Type = TAXLOTS_BACKREPO THEN
              TaxCloseRepoPos(r_salelot);
            END IF;

            COMMIT;

          END LOOP;

          CLOSE RepoSaleLot_cur;
        END;
      END IF;

      COMMIT;

      -- выполняем списание с исключением неттинга
      IF v_ExclNettEndDate >= v_ExclNettBegDate THEN
        BEGIN
          OPEN RepoSaleLot_cur FOR SELECT lot.*
                                     FROM dsctaxlot_dbt lot
                                    WHERE (lot.t_Type = TAXLOTS_SALE or lot.t_Type = TAXLOTS_REPO) and
                                          lot.t_Date >= v_ExclNettBegDate and
                                          lot.t_Date <= v_ExclNettEndDate and
                                          lot.t_Amount > lot.t_Netting and
                                          (DebugFIID_in = -1 or lot.t_FIID = DebugFIID_in)
                                 ORDER BY lot.t_Date ASC,
                                          lot.t_Time ASC,
                                          lot.t_ID ASC;
          LOOP

            FETCH RepoSaleLot_cur INTO r_salelot;
            EXIT WHEN RepoSaleLot_cur%NOTFOUND OR
                      RepoSaleLot_cur%NOTFOUND IS NULL;

            IF r_salelot.t_Type = TAXLOTS_SALE THEN
              TaxLinkSaleToBuy(r_salelot, TAXREG_NETTEXCL);
            ELSIF r_salelot.t_Type = TAXLOTS_REPO THEN
              TaxLinkRepo(r_salelot, TAXREG_NETTEXCL);
            END IF;

            COMMIT;

          END LOOP;

          CLOSE RepoSaleLot_cur;
        END;
      END IF;

      COMMIT;

      -- заносим в реестр начало следующего периода только, если не указан DebugFIID_in
      IF( DebugFIID_in = -1 ) THEN
        v_KeyId := rsb_tools.find_regkey('SECUR\DATE_BUILD_TAXREG');

        IF v_KeyId <> 0 THEN
          UPDATE dregval_dbt
             SET t_fmtblobdata_xxxx = rsb_struct.putString( t_fmtblobdata_xxxx, TO_CHAR(date_max(EndDate_in+1,v_BegDate),'DD')||'.'||
                                                                                TO_CHAR(date_max(EndDate_in+1,v_BegDate),'MM')||'.'||
                                                                                TO_CHAR(date_max(EndDate_in+1,v_BegDate),'YYYY')||CHR(0))
           WHERE t_KeyID = v_KeyId;
        END IF;

        IF v_ExclNettEndDate <> TO_DATE('01-01-0001','DD-MM-YYYY') THEN
          v_KeyId := rsb_tools.find_regkey('SECUR\DATE_BUILD_TAXREGEXCLNETT');

          IF v_KeyId <> 0 THEN
            UPDATE dregval_dbt
               SET t_fmtblobdata_xxxx = rsb_struct.putString( t_fmtblobdata_xxxx, TO_CHAR(date_max(v_ExclNettEndDate+1,v_ExclNettBegDate),'DD')||'.'||
                                                                                  TO_CHAR(date_max(v_ExclNettEndDate+1,v_ExclNettBegDate),'MM')||'.'||
                                                                                  TO_CHAR(date_max(v_ExclNettEndDate+1,v_ExclNettBegDate),'YYYY')||CHR(0))
             WHERE t_KeyID = v_KeyId;
          END IF;
        END IF;
      END IF;

      InsertError( 0,
                   0,
                   'Сообщение',
                   'Формирование данных выполнено успешно' );

      -- фиксируем изменения
      COMMIT;

    END IF;

  END CreateTaxLots;


  PROCEDURE CreateReturnSP( BegDate_in IN DATE, -- Дата начала пересчета
                            EndDate_in IN DATE, -- Дата конца пересчета
                            DebugFIID_in IN NUMBER DEFAULT -1 -- Сбор данных по ц/б для отладки
                          )
  AS
    v_AM            NUMBER;
    v_A             NUMBER;
    v_buySPSourceID NUMBER;
    v_buySPBuyID    NUMBER;
    v_buySPDestID   NUMBER;
    v_buySPAmount   NUMBER;
    v_UnAmountSP    NUMBER;

    CURSOR cur_spbuy IS SELECT B.t_ID BID, S.t_ID SID, L.t_Amount,
                               S.t_Date SDate, S.t_Type SType
                          FROM dsctaxlot_dbt S, dsctaxlot_dbt B, dsctaxlnk_dbt L
                         WHERE B.t_Type = TAXLOTS_BACKREPO
                           AND L.t_SaleID = S.t_ID
                           AND L.t_BuyID = B.t_ID
                           AND ( (L.t_Type = TAXLNK_OPSPOS AND S.t_Type = TAXLOTS_SALE) OR
                                 (L.t_Type = TAXLNK_REPO AND S.t_Type = TAXLOTS_REPO) )
                           AND S.t_Date >= BegDate_in
                           AND S.t_Date < EndDate_in
                           AND (DebugFIID_in = -1 OR B.t_FIID = DebugFIID_in)
                      ORDER BY S.t_Date,
                               S.t_Time,
                               S.t_ID;


  BEGIN

    v_AM := AmortizationMethod(-1);

    FOR c IN cur_spbuy LOOP

      v_UnAmountSP := TaxRepoUnallottedAmountSP( c.BID, c.SID, c.T_AMOUNT );
      WHILE v_UnAmountSP > 0 LOOP

        BEGIN
          IF v_AM = PM_WRITEOFF_FIFO THEN
            SELECT *
              INTO v_buySPSourceID, v_buySPBuyID, v_buySPDestID, v_buySPAmount
              FROM ( SELECT buy.t_SourceID, buy.t_BuyID, buy.t_DestID, buy.t_Amount
                       FROM V_SCTAX_RETSPBUY buy
                      WHERE buy.t_SourceID = c.BID
                        AND buy.t_Date <= c.SDate
                        AND ( (c.SType <> TAXLOTS_SALE AND buy.t_DestID <> c.SID) OR
                              (c.SType = TAXLOTS_SALE AND buy.t_BuyID <> c.SID) )
                        AND TaxRepoFreeAmountSP(buy.t_SourceID, buy.t_BuyID, buy.t_DestID, buy.t_Amount) > 0
                   ORDER BY buy.t_Date ASC,
                            buy.t_Time ASC,
                            buy.t_ID ASC )
             WHERE ROWNUM = 1;
          ELSE
            SELECT *
              INTO v_buySPSourceID, v_buySPBuyID, v_buySPDestID, v_buySPAmount
              FROM ( SELECT buy.t_SourceID, buy.t_BuyID, buy.t_DestID, buy.t_Amount
                       FROM V_SCTAX_RETSPBUY buy
                      WHERE buy.t_SourceID = c.BID
                        AND buy.t_Date <= c.SDate
                        AND ( (c.SType <> TAXLOTS_SALE AND buy.t_DestID <> c.SID) OR
                              (c.SType = TAXLOTS_SALE AND buy.t_BuyID <> c.SID) )
                        AND TaxRepoFreeAmountSP(buy.t_SourceID, buy.t_BuyID, buy.t_DestID, buy.t_Amount) > 0
                   ORDER BY buy.t_Date DESC,
                            buy.t_Time DESC,
                            buy.t_ID DESC )
             WHERE ROWNUM = 1;
          END IF;

        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            BEGIN
              v_buySPSourceID := NULL;
              v_buySPBuyID    := NULL;
              v_buySPDestID   := NULL;
              v_buySPAmount   := NULL;
            END;
        END;

        EXIT WHEN v_buySPSourceID IS NULL or
                  v_buySPBuyID    IS NULL or
                  v_buySPDestID   IS NULL or
                  v_buySPAmount   IS NULL;


        v_A :=  min_sum(v_UnAmountSP,
                        TaxRepoFreeAmountSP(v_buySPSourceID, v_buySPBuyID, v_buySPDestID, v_buySPAmount) );

        INSERT INTO dsctaxlnk_dbt ( T_SALEID,
                                    T_BUYID,
                                    T_TYPE,
                                    T_AMOUNT,
                                    T_SOURCEID,
                                    T_DESTID )
        VALUES ( c.SID,
                 v_buySPBuyID,
                 TAXLNK_RETSPOS,
                 v_A,
                 c.BID,
                 v_buySPDestID
               );

        v_UnAmountSP := TaxRepoUnallottedAmountSP( c.BID, c.SID, c.T_AMOUNT );

      END LOOP;
    END LOOP;

    COMMIT;

  END CreateReturnSP;


  -- достроить возвраты 2 для госбумаг
  PROCEDURE CreateReturn2( BegDate_in IN DATE, -- Дата начала пересчета
                           EndDate_in IN DATE, -- Дата конца пересчета
                           DebugFIID_in IN NUMBER DEFAULT -1 -- Сбор данных по ц/б для отладки
                          )
  AS
    v_BegDate DATE;
    v_salelot_id dsctaxlot_dbt.t_ID%TYPE;
    r_salelot dsctaxlot_dbt%ROWTYPE;

    TYPE RepoSaleLotCurTyp IS REF CURSOR;
    RepoSaleLot_cur RepoSaleLotCurTyp;

    -- Выполнить достроение связей "возврат 2" по госбумагам, непостроенным в TaxLinkSaleToBuy
    -- процедура почти повторяет TaxLinkSaleToBuy
    PROCEDURE TaxDoLnkRet2( in_SaleLot IN dsctaxlot_dbt%ROWTYPE, in_Mode IN NUMBER DEFAULT TAXREG_ORDINARY )
    AS
      v_LS  NUMBER DEFAULT TAXLNK_UNDEF;
      v_LD  NUMBER DEFAULT TAXLNK_UNDEF;
      v_LR  NUMBER DEFAULT TAXLNK_UNDEF;
      v_LR2 NUMBER DEFAULT TAXLNK_UNDEF;
      v_AM NUMBER;
      v_F BOOLEAN;
      v_BondKind davrkinds_dbt.t_NumList%TYPE;
      v_lnk  dsctaxlnk_dbt%ROWTYPE;
      v_lnk2 dsctaxlnk_dbt%ROWTYPE;

      TYPE SaleLnkCurTyp IS REF CURSOR;
      lnk_cur SaleLnkCurTyp;
      lnk_cur2 SaleLnkCurTyp;

    BEGIN

      IF in_Mode = TAXREG_ORDINARY THEN
        -- стандартный режим
        v_LS  := TAXLNK_BS;
        v_LD  := TAXLNK_REPO;
        v_LR  := TAXLNK_BREPO;
        v_LR2 := TAXLNK_RETURN2;
      ELSIF in_Mode = TAXREG_NETTEXCL THEN
        -- с исключением неттинга
        v_LS  := TAXLNK_DELIVER;
        v_LD  := TAXLNK_DELREPO;
        v_LR  := TAXLNK_DELBREP;
        v_LR2 := TAXLNK_DELRET2;
      END IF;

      v_AM := AmortizationMethod(-1);
      v_BondKind := BondKind(in_SaleLot.t_FIID);

      IF (v_BondKind = '2.1.1.6' OR -- еврооблигации
         (substr(v_BondKind,3,1) <> '1' and substr(v_BondKind,3,1) <> '2')) THEN -- не лежит в ветках "государственные" или "муниципальные"
        v_F := TRUE;
      ELSE
        v_F := FALSE;
      END IF;

      OPEN lnk_cur FOR SELECT lnk.*
                         FROM dsctaxlnk_dbt lnk, dsctaxlot_dbt lot
                        WHERE lnk.t_SaleID = in_SaleLot.T_ID
                          AND lnk.t_Type = v_LS
                          AND lot.t_ID = lnk.t_BuyID
                          AND lot.t_Type = TAXLOTS_BUY
                          AND lot.t_FIID = in_SaleLot.t_FIID
                          AND lot.t_Date <= in_SaleLot.t_Date
                     ORDER BY lot.t_Date DESC,
                              lot.t_Time DESC,
                              lot.t_ID DESC;

      LOOP

        FETCH lnk_cur INTO v_lnk;
        EXIT WHEN lnk_cur%NOTFOUND OR
                  lnk_cur%NOTFOUND IS NULL;

        IF v_AM = PM_WRITEOFF_FIFO THEN
          OPEN lnk_cur2 FOR SELECT lnk.*
                              FROM dsctaxlnk_dbt lnk, dsctaxlot_dbt lot
                             WHERE lnk.t_SaleID = in_SaleLot.T_ID
                               AND lnk.t_Type = v_LR
                               AND lnk.t_SourceID = v_lnk.t_BuyID
                               AND lot.t_ID = lnk.t_BuyID
                               AND lot.t_Type = TAXLOTS_REPO
                               AND lot.t_FIID = in_SaleLot.t_FIID
                               AND lot.t_Date2 <= in_SaleLot.t_Date
                               AND lot.t_Date2 <> TO_DATE('01-01-0001','DD-MM-YYYY')
                               AND Exists( SELECT lnk2.t_Type
                                             FROM dsctaxlnk_dbt lnk2
                                            WHERE lnk2.t_Type = v_LD and
                                                  lnk2.t_SaleID = lot.t_ID and
                                                  lnk2.t_BuyID = lnk.t_SourceID )
                          ORDER BY lot.t_Date2 ASC,
                                   lot.t_Time2 ASC,
                                   lot.t_ID ASC;
        ELSE
          OPEN lnk_cur2 FOR SELECT lnk.*
                              FROM dsctaxlnk_dbt lnk, dsctaxlot_dbt lot
                             WHERE lnk.t_SaleID = in_SaleLot.T_ID
                               AND lnk.t_Type = v_LR
                               AND lnk.t_SourceID = v_lnk.t_BuyID
                               AND lot.t_ID = lnk.t_BuyID
                               AND lot.t_Type = TAXLOTS_REPO
                               AND lot.t_FIID = in_SaleLot.t_FIID
                               AND lot.t_Date2 <= in_SaleLot.t_Date
                               AND lot.t_Date2 <> TO_DATE('01-01-0001','DD-MM-YYYY')
                               AND Exists( SELECT lnk2.t_Type
                                             FROM dsctaxlnk_dbt lnk2
                                            WHERE lnk2.t_Type = v_LD and
                                                  lnk2.t_SaleID = lot.t_ID and
                                                  lnk2.t_BuyID = lnk.t_SourceID )
                          ORDER BY lot.t_Date2 DESC,
                                   lot.t_Time2 DESC,
                                   lot.t_ID DESC;
        END IF;

        LOOP

          FETCH lnk_cur2 INTO v_lnk2;
          EXIT WHEN lnk_cur2%NOTFOUND OR
                    lnk_cur2%NOTFOUND IS NULL;

--          IF v_F = TRUE THEN так было раньше, нам нужно наоборот
          IF v_F <> TRUE THEN
            InsertError( 0,
                         -1,
                         'Отладка',
                         'вствка связи В2! T_SALEID = '||in_SaleLot.T_ID||
                         ' T_BUYID = '||v_lnk2.t_BuyID||
                         ' T_TYPE = '||v_LR2||
                         ' T_AMOUNT = '||v_lnk2.t_Amount||
                         ' T_SOURCEID = '||v_lnk.t_BuyID||
                         ' T_DESTID = '||in_SaleLot.T_ID||
                         ' t_FIID = '||in_SaleLot.t_FIID );

            BEGIN
              INSERT INTO dsctaxlnk_dbt ( T_SALEID,
                                          T_BUYID,
                                          T_TYPE,
                                          T_AMOUNT,
                                          T_SOURCEID,
                                          T_DESTID )
              VALUES ( in_SaleLot.T_ID,
                       v_lnk2.t_BuyID,
                       v_LR2,
                       v_lnk2.t_Amount,
                       v_lnk.t_BuyID,
                       in_SaleLot.T_ID
                     );
            EXCEPTION
              WHEN OTHERS THEN
                InsertError( 0,
                             -1,
                             'Отладка',
                             'Дублирование lnk!T_SALEID = '||in_SaleLot.T_ID||
                             ' T_BUYID = '||v_lnk2.t_BuyID||
                             ' T_TYPE = '||v_LR2||
                             ' T_AMOUNT = '||v_lnk2.t_Amount||
                             ' T_SOURCEID = '||v_lnk.t_BuyID||
                             ' T_DESTID = '||in_SaleLot.T_ID||
                             ' t_FIID = '||in_SaleLot.t_FIID );
            END;

            TaxLinkDirectRepo(v_lnk.t_BuyID, in_SaleLot.T_ID, v_lnk2.t_BuyID, in_Mode);
          END IF;

        END LOOP;

        CLOSE lnk_cur2;

      END LOOP;

      CLOSE lnk_cur;

    END TaxDoLnkRet2;

  BEGIN

    BEGIN
      IF DebugFIID_in <> -1 THEN
        EXECUTE IMMEDIATE 'DELETE FROM DSCTAXMES_TMP WHERE T_FIID = :1'
                    USING DebugFIID_in;
      ELSE
        EXECUTE IMMEDIATE 'TRUNCATE TABLE DSCTAXMES_TMP';
      END IF;

      EXCEPTION
        WHEN OTHERS
        THEN NULL;
    END;

    -- берем значения дат из настроек
    BEGIN
      SELECT TO_DATE(rsb_struct.getString(t_fmtblobdata_xxxx),'DD.MM.YYYY')
        INTO v_BegDate
       FROM dregval_dbt
       WHERE t_KeyID = rsb_tools.find_regkey('SECUR\DATE_BUILD_TAXREG');

      EXCEPTION
        WHEN OTHERS
        THEN v_BegDate := TO_DATE('01-01-0001','DD-MM-YYYY');
    END;

    -- проверяем и корректируем даты
    v_BegDate := date_min(v_BegDate, BegDate_in);
    -- выполняем расчет только для еще нерасчитанного периода
    IF EndDate_in >= v_BegDate THEN

      OPEN RepoSaleLot_cur FOR SELECT T_ID
                                 FROM ( (SELECT lot.t_id, lot.t_Date D, lot.t_Time T, 2 ORD
                                           FROM dsctaxlot_dbt lot
                                          WHERE (lot.t_Type = TAXLOTS_SALE or
                                                 lot.t_Type = TAXLOTS_REPO) and
                                                lot.t_Date >= v_BegDate and
                                                lot.t_Date <= EndDate_in and
                                                (DebugFIID_in = -1 or lot.t_FIID = DebugFIID_in))
                                      UNION ALL
                                        (SELECT lot2.t_id, lot2.t_Date2 D, lot2.t_Time2 T, 1 ORD
                                           FROM dsctaxlot_dbt lot2
                                          WHERE lot2.t_Type = TAXLOTS_BACKREPO and
                                                lot2.t_Date2 >= v_BegDate and
                                                lot2.t_Date2 <= EndDate_in and
                                                lot2.t_Date2 <> TO_DATE('01-01-0001','DD-MM-YYYY') and
                                                (DebugFIID_in = -1 or lot2.t_FIID = DebugFIID_in)))
                             ORDER BY D ASC,
                                      ORD ASC,
                                      T ASC,
                                      t_ID ASC;
      LOOP

        FETCH RepoSaleLot_cur INTO v_salelot_id;
        EXIT WHEN RepoSaleLot_cur%NOTFOUND OR
                  RepoSaleLot_cur%NOTFOUND IS NULL;

        SELECT *
          INTO r_salelot
          FROM dsctaxlot_dbt
         WHERE t_ID = v_salelot_id;

        IF r_salelot.t_Type = TAXLOTS_SALE THEN
          TaxDoLnkRet2(r_salelot, TAXREG_ORDINARY);
        END IF;

        COMMIT;

      END LOOP;

      CLOSE RepoSaleLot_cur;

      -- достраиваем "возврат в КП"
      CreateReturnSP( v_BegDate, EndDate_in );
    END IF;

    COMMIT;

  END  CreateReturn2;

  function GetDealBuySale( DealType    IN NUMBER,
                           BofficeKind IN NUMBER,
                           IsBack      IN NUMBER ) return NUMBER is
     OGroup NUMBER;
     DEAL_TYPE_UNDEF      CONSTANT NUMBER := 0;       /*неизвестный вид*/
     DEAL_TYPE_SALE       CONSTANT NUMBER := 1;       /*продажи\погашения*/
     DEAL_TYPE_BUY        CONSTANT NUMBER := 2;       /*покупки*/
     DEAL_TYPE_RET_COUPON CONSTANT NUMBER := 3; /*погашения купонов*/
     DEAL_TYPE_RET_PARTLY CONSTANT NUMBER := 4; /*частичное погашение*/

  begin
       OGroup := get_OperationGroup(get_OperSysTypes(DealType,BofficeKind));
       if( IsRet_Coupon(OGroup) = 1 ) then
          return DEAL_TYPE_RET_COUPON;
       elsif( IsRet_Partly(OGroup) = 1 ) then
             return DEAL_TYPE_RET_PARTLY;
       elsif( ((IsSale(OGroup) = 1) and (IsBack = 0)) or
              ((IsBuy(OGroup) = 1) and (IsBack = 1)) or
              ((BofficeKind = 138) and (IsBack = 0)) or
              (IsRet_Issue(OGroup) = 1) or
              (IsAvrWrtOut(OGroup) = 1) ) then
             return DEAL_TYPE_SALE;
       elsif( ((IsBuy(OGroup) = 1) and (IsBack = 0)) or
              ((IsSale(OGroup) = 1) and (IsBack = 1)) or
              ((BofficeKind = 138) and (IsBack = 1)) or
              (IsAvrWrtIn(OGroup) = 1) ) then
             return DEAL_TYPE_BUY;
       end if;
       return DEAL_TYPE_UNDEF;
  end;

  FUNCTION GetDealTypeName(p_DealType IN NUMBER)
    RETURN DealTypeName_t DETERMINISTIC
  AS
    v_DealTypeName DealTypeName_t;
  BEGIN

    v_DealTypeName := CHR(1);

    SELECT t_Name INTO v_DealTypeName
      FROM doprkoper_dbt
     WHERE t_Kind_Operation = p_DealType;

    RETURN v_DealTypeName;

    EXCEPTION
      WHEN OTHERS THEN
        RETURN CHR(1);

  END GetDealTypeName;

  FUNCTION GetDealClientShortName(p_ClientID IN NUMBER)
    RETURN DealClientShortName_t DETERMINISTIC
  AS
    v_DealClientShortName DealClientShortName_t;
    v_ClientID NUMBER;
  BEGIN

    v_ClientID            := p_ClientID;
    v_DealClientShortName := CHR(1);

    IF v_ClientID < 0 THEN
      v_ClientID := RSBSESSIONDATA.OurBank;
    END IF;

    SELECT pt.t_ShortName INTO v_DealClientShortName
      FROM dparty_dbt pt
     WHERE pt.t_PartyID = v_ClientID;

    RETURN v_DealClientShortName;

    EXCEPTION
      WHEN OTHERS THEN
        RETURN CHR(1);

  END GetDealClientShortName;

  FUNCTION GetDealSfContrNumber(p_ClientContrID IN NUMBER)
    RETURN DealSfContrNumber_t DETERMINISTIC
  AS
    v_DealSfContrNumber DealSfContrNumber_t;
  BEGIN

    v_DealSfContrNumber := CHR(1);

    IF p_ClientContrID > 0 THEN
      SELECT sf.t_Number INTO v_DealSfContrNumber
        FROM dsfcontr_dbt sf
       WHERE sf.t_ID = p_ClientContrID;
    END IF;

    RETURN v_DealSfContrNumber;

    EXCEPTION
      WHEN OTHERS THEN
        RETURN CHR(1);

  END GetDealSfContrNumber;

  FUNCTION GetDealContrShortName(p_PartyID IN NUMBER)
    RETURN DealContrShortName_t DETERMINISTIC
  AS
    v_DealContrShortName DealContrShortName_t;
  BEGIN

    v_DealContrShortName := CHR(1);

    SELECT pt.t_ShortName INTO v_DealContrShortName
      FROM dparty_dbt pt
     WHERE pt.t_PartyID = p_PartyID;

    RETURN v_DealContrShortName;

    EXCEPTION
      WHEN OTHERS THEN
        RETURN CHR(1);

  END GetDealContrShortName;

  FUNCTION GetDealFICcy(p_FIID IN NUMBER)
    RETURN DealFICcy_t DETERMINISTIC
  AS
    v_Ccy DealFICcy_t;
  BEGIN

    v_Ccy := CHR(1);

    SELECT fin.t_Ccy INTO v_Ccy
      FROM dfininstr_dbt fin
     WHERE fin.t_FIID = p_FIID;

    RETURN v_Ccy;

    EXCEPTION
      WHEN OTHERS THEN
        RETURN CHR(1);

  END GetDealFICcy;

  FUNCTION GetDealDepName(p_DepartmentID IN NUMBER)
    RETURN DealDepName_t DETERMINISTIC
  AS
    v_DepName DealDepName_t;
  BEGIN

    v_DepName := CHR(1);

    SELECT dp.t_Name INTO v_DepName
      FROM ddp_dep_dbt dp
     WHERE dp.t_Code = p_DepartmentID;

    RETURN v_DepName;

    EXCEPTION
      WHEN OTHERS THEN
        RETURN CHR(1);

  END GetDealDepName;

  FUNCTION GetDealOperName(p_Oper IN NUMBER)
    RETURN DealOperName_t DETERMINISTIC
  AS
    v_DealOperName DealOperName_t;
  BEGIN

    v_DealOperName := CHR(1);

    SELECT op.t_Name INTO v_DealOperName
      FROM dperson_dbt op
     WHERE op.t_Oper = p_Oper;

    RETURN v_DealOperName;

    EXCEPTION
      WHEN OTHERS THEN
        RETURN CHR(1);

  END GetDealOperName;

  FUNCTION GetDealSupplyDate1(p_MaturityIsPrincipal IN CHAR,
                              p_Maturity IN DATE,
                              p_Expiry IN DATE
                             )
    RETURN DealSupplyDate_t DETERMINISTIC
  AS
    DealSupplyDate1 DealSupplyDate_t;
  BEGIN

    DealSupplyDate1 := p_Expiry;
    IF p_MaturityIsPrincipal = 'X' THEN
      DealSupplyDate1 := p_Maturity;
    END IF;

    RETURN DealSupplyDate1;

  END GetDealSupplyDate1;

  FUNCTION GetDealSupplyDate2(p_DealID IN NUMBER,
                              p_LegKind IN NUMBER
                             )
    RETURN DealSupplyDate_t DETERMINISTIC
  AS
    DealSupplyDate2 DealSupplyDate_t;
  BEGIN

    DealSupplyDate2 := TO_DATE('01.01.0001','DD.MM.YYYY');

    SELECT (CASE WHEN t_MaturityIsPrincipal = 'X' THEN t_Maturity ELSE t_Expiry END)
      INTO DealSupplyDate2
      FROM ddl_leg_dbt
     WHERE t_DealID = p_DealID
       AND t_LegKind = p_LegKind
       AND t_LegID = 0;

    RETURN DealSupplyDate2;

    EXCEPTION
      WHEN OTHERS THEN
        RETURN TO_DATE('01.01.0001','DD.MM.YYYY');

  END GetDealSupplyDate2;

  FUNCTION GetDealSupplyTime2(p_DealID IN NUMBER,
                              p_LegKind IN NUMBER
                             )
    RETURN DealSupplyTime_t DETERMINISTIC
  AS
    DealSupplyTime2 DealSupplyTime_t;
  BEGIN

    DealSupplyTime2 := TO_DATE('01.01.0001','DD.MM.YYYY');

    SELECT t_SupplyTime
      INTO DealSupplyTime2
      FROM ddl_leg_dbt
     WHERE t_DealID = p_DealID
       AND t_LegKind = p_LegKind
       AND t_LegID = 0;

    RETURN DealSupplyTime2;

    EXCEPTION
      WHEN OTHERS THEN
        RETURN TO_DATE('01.01.0001','DD.MM.YYYY');

  END GetDealSupplyTime2;

  FUNCTION GetDealPayDate1(p_MaturityIsPrincipal IN CHAR,
                              p_Maturity IN DATE,
                              p_Expiry IN DATE
                             )
    RETURN DealPayDate_t DETERMINISTIC
  AS
    DealPayDate1 DealPayDate_t;
  BEGIN

    DealPayDate1 := p_Maturity;
    IF p_MaturityIsPrincipal = 'X' THEN
      DealPayDate1 := p_Expiry;
    END IF;

    RETURN DealPayDate1;

  END GetDealPayDate1;

  FUNCTION GetDealPayDate2(p_DealID IN NUMBER,
                              p_LegKind IN NUMBER
                             )
    RETURN DealPayDate_t DETERMINISTIC
  AS
    DealPayDate2 DealPayDate_t;
  BEGIN

    DealPayDate2 := TO_DATE('01.01.0001','DD.MM.YYYY');

    SELECT (CASE WHEN t_MaturityIsPrincipal = 'X' THEN t_Expiry ELSE t_Maturity END)
      INTO DealPayDate2
      FROM ddl_leg_dbt
     WHERE t_DealID = p_DealID
       AND t_LegKind = p_LegKind
       AND t_LegID = 0;

    RETURN DealPayDate2;

    EXCEPTION
      WHEN OTHERS THEN
        RETURN TO_DATE('01.01.0001','DD.MM.YYYY');

  END GetDealPayDate2;

  FUNCTION GetDealIsNett(p_DealID IN NUMBER,
                         p_BOfficeKind IN NUMBER
                        )
    RETURN DealIsNett_t DETERMINISTIC
  AS
    v_IsNett DealIsNett_t;
  BEGIN

    SELECT Count(1) INTO v_IsNett
      FROM dpmlink_dbt pl, ddlrq_dbt rq
     WHERE pl.t_LinkKind = RSB_PAYMENT.PMLINK_KIND_RQNETTING
       AND pl.t_InitialPayment = rq.t_ID
       AND rq.t_DocKind = p_BOfficeKind
       AND rq.t_DocID = p_DealID;

    RETURN v_IsNett;
  END GetDealIsNett;

  FUNCTION GetDealRejectDate2(p_DealID IN NUMBER,
                              p_LegKind IN NUMBER
                             )
    RETURN DealRejectDate_t DETERMINISTIC
  AS
    DealRejectDate2 DealRejectDate_t;
  BEGIN

    DealRejectDate2 := TO_DATE('01.01.0001','DD.MM.YYYY');

    SELECT t_RejectDate INTO DealRejectDate2
      FROM ddl_leg_dbt
     WHERE t_DealID = p_DealID
       AND t_LegKind = p_LegKind
       AND t_LegID = 0;

    RETURN DealRejectDate2;

    EXCEPTION
      WHEN OTHERS THEN
        RETURN TO_DATE('01.01.0001','DD.MM.YYYY');

  END GetDealRejectDate2;

  --получение номера договора для начисления комиссий ПЗО по сделке\погашению
  --если плательщик задан - вернуть договор c этим плательщиком (клиентом или клиентом-контрагентом), для Нашего Банка может быть несколько (возвращаем главный)
  --если субъект не задан - вернуть "главный" договор по сделке (т.е. работает как и раньше)
  --если задан p_RegistrarByLeg - по какой части (legkind: 0 или 2) вернуть договор с регистратором, при этом p_PayerID указывать не надо
  FUNCTION RSI_GetSfContrID(p_DealID IN NUMBER, p_PayerID IN NUMBER DEFAULT 0, p_RegistrarByLeg IN NUMBER DEFAULT -1 )
    RETURN NUMBER
  AS
    v_tick     ddl_tick_dbt%rowtype;
    v_leg      ddl_leg_dbt%rowtype;
    v_ContrID  NUMBER := 0;
    v_ServKind NUMBER := 0;
    v_Group    NUMBER := 0;

    FUNCTION DL_GetSfContrIDbyPartyID( pDealDate IN DATE, pPartyID IN NUMBER, pServKind IN NUMBER )
      RETURN NUMBER
    AS
      v_ID NUMBER := 0;
    BEGIN
      select T_ID into v_ID
        from ( select T_ID
                 from dsfcontr_dbt
                where t_ServKind = pServKind
                  and t_partyID = RSBSESSIONDATA.OurBank
                  and t_ContractorID = pPartyID
                  and t_dateBegin <= pDealDate
                  and  (t_dateClose = TO_DATE('01.01.0001','DD.MM.YYYY') or t_dateClose >= pDealDate )
                  order by t_ServKind, t_ContractorID, t_partyID, t_DateConc
             )
       where rownum = 1;
       return v_ID;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        RETURN 0;
      WHEN OTHERS THEN
        RETURN 0;
    END;

  BEGIN

    SELECT * INTO v_tick
      FROM ddl_tick_dbt
     WHERE t_DealID = p_DealID;

    if( v_tick.t_BofficeKind = 131 OR v_tick.t_BofficeKind = 945 OR v_tick.t_BofficeKind = 940 )then
       v_ServKind := 17;
    else
       v_ServKind := 1;
    end if;

    v_Group := rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(v_tick.t_DealType, v_tick.t_BofficeKind));

    SELECT * INTO v_leg
      FROM ddl_leg_dbt
     WHERE t_LegKind = 0
       AND t_DealID = p_DealID
       AND t_LegID = 0;

    if( p_PayerID > 0 and p_PayerID != RSBSESSIONDATA.OurBank )then

       if( v_tick.t_ClientID = p_PayerID and v_tick.t_ClientContrID > 0)then--плательщик = клиент, получатель НБ
          v_ContrID := v_tick.t_ClientContrID;
       elsif( v_tick.t_ClientID > 0 and v_tick.t_IsPartyClient = 'X' and v_tick.t_PartyID = p_PayerID and v_tick.t_PartyContrID > 0)then--плательщик = клиент-контрагент, получатель НБ
          v_ContrID := v_tick.t_PartyContrID;
       end if;

       return v_ContrID;

    elsif( p_RegistrarByLeg != -1 )then

       if( p_RegistrarByLeg = 0 and v_leg.t_PayRegTax = 'X' and v_leg.t_RegistrarContrID > 0 )then--с регистратором по 1 ч
          v_ContrID := v_leg.t_RegistrarContrID;
       elsif( p_RegistrarByLeg = 2 )then--проверим регистратора по второй части(пе первой проверили выше)--плательщик = НБ, получатель = регистратор

          SELECT * INTO v_leg
            FROM ddl_leg_dbt
           WHERE t_LegKind = 2
             AND t_DealID = p_DealID
             AND t_LegID = 0;

          if( v_leg.t_PayRegTax = 'X' and v_leg.t_RegistrarContrID > 0 )then
             v_ContrID := v_leg.t_RegistrarContrID;
          end if;
       end if;

       return v_ContrID;

    end if;

    if( v_tick.t_ClientID > 0 )then
       if( v_tick.t_ClientContrID > 0 )then
          v_ContrID := v_tick.t_ClientContrID;
       end if;
    elsif( v_tick.t_BofficeKind = 117 OR v_tick.t_BofficeKind = 130 OR v_tick.t_BofficeKind = 940 )then
       if( v_tick.t_Flag1 = 'X' )then -- ОРЦБ
         -- получатель комиссии - БИРЖА (договор в панели не задается)
          v_ContrID := DL_GetSfContrIDbyPartyID( v_tick.t_DealDate, v_tick.t_MarketID, v_ServKind );
       elsif( v_leg.t_ID > 0 )then
         -- получатель комиссии - депозитарий (договор задается в панели погашения)
            if( v_leg.t_RegistrarContrID > 0 )then
               v_ContrID := v_leg.t_RegistrarContrID;
            end if;
         end if;
    elsif( v_tick.t_BrokerID > 0 and (IsExchange(v_Group) = 0 or (IsRepo(v_Group) = 1)) )then--для РЕПО (биржа\внебиржа) с указанным брокером берем договор с брокером
      if( v_tick.t_BrokerContrID > 0 )then
         v_ContrID := v_tick.t_BrokerContrID;
      end if;
    elsif( IsExchange(v_Group) = 1 )then
     --получим номер договора банка с биржей по сделке
      v_ContrID := DL_GetSfContrIDbyPartyID( v_tick.t_DealDate, v_tick.t_MarketID, v_ServKind );
    elsif( IsOUTEXCHANGE( v_Group ) = 0 )then
     --для всех кроме внебиржи
     --получим номер договора банка с контрагентом по сделке
      v_ContrID := DL_GetSfContrIDbyPartyID( v_tick.t_DealDate, v_tick.t_PartyID, v_ServKind );
    end if;

    RETURN v_ContrID;

    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        RETURN 0;
      WHEN OTHERS THEN
        RETURN 0;

  END RSI_GetSfContrID;

  --получение номера договора для начисления комиссий ПЗО по сделке\погашению
  --если плательщик задан - вернуть договор c этим плательщиком (клиентом или клиентом-контрагентом), для Нашего Банка может быть несколько (возвращаем главный)
  --если субъект не задан - вернуть "главный" договор по сделке (т.е. работает как и раньше)
  --если задан p_RegistrarByLeg - по какой части (legkind: 0 или 2) вернуть договор с регистратором, при этом p_PayerID указывать не надо
  FUNCTION GetSfContrID(p_DealID IN NUMBER, p_PayerID IN NUMBER DEFAULT 0, p_RegistrarByLeg IN NUMBER DEFAULT -1 )
    RETURN NUMBER
  AS
  BEGIN
    return RSI_GetSfContrID(p_DealID, p_PayerID, p_RegistrarByLeg);
  END GetSfContrID;

  --  дата закрытия должна определяться как максимальная из фактических дат выполнения оплаты, поставки, комиссии.
  --  Под датой комиссии понимается максимальная из дат оплаты комиссии
  FUNCTION GetTickCloseDate (p_DealID IN NUMBER)
     RETURN DATE
  IS
     v_CloseDate   DATE := TO_DATE ('01.01.0001', 'DD.MM.YYYY');
  BEGIN
     SELECT MAX (closedate)
      INTO v_CloseDate
       FROM (SELECT RQ.T_FACTDATE AS closedate
                FROM ddlrq_dbt rq
               WHERE     RQ.T_DOCID = p_DealID
                     AND RQ.T_DOCKIND IN (101, 117, 127, 4830, 4831)
                     AND RQ.T_TYPE IN (2, 6, 8));

     RETURN NVL (v_CloseDate, TO_DATE ('01.01.0001', 'DD.MM.YYYY'));

  END GetTickCloseDate;

  --получить код сделки
  FUNCTION GetDealCode(p_DocumentKind IN NUMBER, p_DocumentID IN NUMBER, p_IsInner IN NUMBER DEFAULT 1)
    RETURN VARCHAR2
  AS
    v_DealCode ddl_tick_dbt.t_DealCode%TYPE;
    v_DealCodeTS ddl_tick_dbt.t_DealCodeTS%TYPE;
  BEGIN

    v_DealCode := CHR(1);
    v_DealCodeTS := CHR(1);

    IF( p_DocumentKind = DL_SECURITYDOC OR
        p_DocumentKind = DL_RETIREMENT OR
        p_DocumentKind = DL_AVRWRT OR
        p_DocumentKind = DL_SECUROWN OR
        p_DocumentKind = DL_AVRWRTOWN OR
        p_DocumentKind = DL_RETIREMENT_OWN )
    THEN
      SELECT tk.t_DealCode, tk.t_DealCodeTS INTO v_DealCode, v_DealCodeTS
        FROM ddl_tick_dbt tk
       WHERE tk.t_BOfficeKind = p_DocumentKind
         AND tk.t_DealID = p_DocumentID;
    ELSIF p_DocumentKind = DL_SECURLEG THEN
      SELECT tk.t_DealCode, tk.t_DealCodeTS INTO v_DealCode, v_DealCodeTS
        FROM ddl_tick_dbt tk, ddl_leg_dbt leg
       WHERE leg.t_ID = p_DocumentID
         AND tk.t_DealID = leg.t_DealID;
    END IF;

    RETURN CASE p_IsInner
             WHEN 1 THEN v_DealCode
             ELSE v_DealCodeTS
           END;

    EXCEPTION
      WHEN OTHERS THEN
        RETURN CHR(1);
  END;

  --получить ID сделки
  FUNCTION GetDealID(p_DocumentKind IN NUMBER, p_DocumentID IN NUMBER)
    RETURN NUMBER
  AS
    v_DealID ddl_tick_dbt.t_DealID%TYPE;
  BEGIN

    v_DealID := 0;

    IF( p_DocumentKind = DL_SECURITYDOC OR
        p_DocumentKind = DL_RETIREMENT OR
        p_DocumentKind = DL_AVRWRT OR
        p_DocumentKind = DL_SECUROWN OR
        p_DocumentKind = DL_AVRWRTOWN OR
        p_DocumentKind = DL_RETIREMENT_OWN ) THEN
      v_DealID := p_DocumentID;
    ELSIF( p_DocumentKind = DL_SECURLEG ) THEN
      SELECT leg.t_DealID INTO v_DealID
        FROM ddl_leg_dbt leg
       WHERE leg.t_ID = p_DocumentID;
    ELSIF( p_DocumentKind = DL_TICK_ENS_DOC ) THEN
      SELECT ens.t_DealID INTO v_DealID
        FROM ddl_tick_ens_dbt ens
       WHERE ens.t_ID = p_DocumentID;
    END IF;

    RETURN v_DealID;

    EXCEPTION
      WHEN OTHERS THEN
        RETURN 0;
  END;

  --получить ID сделки учетом операции неттинга
  FUNCTION GetDealIdEx(p_DocumentKind IN NUMBER, p_DocumentID IN NUMBER)
    RETURN NUMBER
  AS
    v_DealID ddl_tick_dbt.t_DealID%TYPE;
  BEGIN

    v_DealID := 0;

    IF p_DocumentKind = OBJTYPE_NTGSEC THEN
      v_DealID := p_DocumentID;
    ELSE
      v_DealID := GetDealID(p_DocumentKind, p_DocumentID);
    END IF;

    RETURN v_DealID;

    EXCEPTION
      WHEN OTHERS THEN
        RETURN 0;
  END;

  --получить код сделки с учетом операции неттинга, операций НУ, операций перевода ДС ПА, комиссии ПА
  FUNCTION RSI_GetDealCodeEx( p_DocumentKind IN NUMBER, p_DocumentID IN NUMBER, p_IsInner IN NUMBER DEFAULT 1 )
    RETURN VARCHAR2
  AS
    v_DealCode VARCHAR2(30);
    v_DealCodeTS VARCHAR2(30);
  BEGIN

    v_DealCode := chr(1);
    v_DealCodeTS := chr(1);

    CASE
    
      WHEN p_DocumentKind = DL_NTGSEC THEN
        select ntg.t_DealNumber, NTG.T_EXTCODE  into v_DealCode, v_DealCodeTS
         from ddl_nett_dbt ntg
        where ntg.t_NettingID = p_DocumentID;

      WHEN p_DocumentKind IN ( DL_CALCNDFL, DL_CLOSENDFL, DL_WRTMONEY, DL_HOLDNDFL ) THEN
        select nptxop.t_Code into v_DealCode
          from dnptxop_dbt nptxop
          where nptxop.t_ID = p_DocumentID
            and nptxop.t_DocKind = p_DocumentKind;

      WHEN p_DocumentKind IN ( SP_TRANSFERPA, SP_COMISSION_PA ) THEN
        select comm.t_CommCode, comm.t_CommCodeTS into v_DealCode, v_DealCodeTS
          from ddl_comm_dbt comm
          where comm.t_DocumentID = p_DocumentID
            and comm.t_DocKind = p_DocumentKind;            
    
      ELSE 
        IF( p_IsInner = 1 ) THEN
          v_DealCode := GetDealCode(p_DocumentKind, p_DocumentID, p_IsInner);
        ELSE
          v_DealCodeTS := GetDealCode(p_DocumentKind, p_DocumentID, p_IsInner);
        END IF;

    END CASE;

    RETURN CASE p_IsInner
             WHEN 1 THEN v_DealCode
             ELSE v_DealCodeTS
           END; 

    EXCEPTION
      WHEN OTHERS THEN
        RETURN chr(1);
  END;

  --получить вид операции учетом операции неттинга
  FUNCTION RSI_GetDealKindName( p_DocumentKind IN NUMBER, p_DocumentID IN NUMBER )
    RETURN DealTypeName_t DETERMINISTIC
  AS
    v_Kind_Operation NUMBER;
  BEGIN

    v_Kind_Operation := 0;

    IF( p_DocumentKind = DL_NTGSEC ) THEN
       select ntg.t_Kind_Operation into v_Kind_Operation
         from ddl_nett_dbt ntg
        where ntg.t_NettingID = p_DocumentID;
    ELSE
       select tk.t_DealType into v_Kind_Operation
         from ddl_tick_dbt tk
        where tk.t_DealID = GetDealID(p_DocumentKind, p_DocumentID);
    END IF;

    RETURN GetDealTypeName(v_Kind_Operation);

    EXCEPTION
      WHEN OTHERS THEN
        RETURN chr(1);
  END;

  -- Процедура сохранения обеспечения
  PROCEDURE RSI_SC_SaveTickEns( p_DealID IN NUMBER DEFAULT -1 )
  IS
     v_Count NUMBER;

     TYPE TickEnsTmpRec IS TABLE OF DDL_TICK_ENS_TMP%ROWTYPE;
     TickEnsTmp TickEnsTmpRec;
  BEGIN

     SELECT COUNT(1) INTO v_Count
       FROM DDL_TICK_ENS_DBT dbt, DDL_TICK_ENS_TMP tmp
      WHERE tmp.t_EnsID = dbt.t_ID
        AND tmp.t_Version != dbt.t_Version;

     IF( v_Count > 0 ) THEN
        RSI_SetError(-20200, ''); -- Конфликт. Документ изменен другим операционистом
     END IF;

     -- При вставке сделки установим актуальный DealID
     BEGIN
        IF( p_DealID > 0 ) THEN
           UPDATE DDL_TICK_ENS_TMP SET t_DealID = p_DealID
            WHERE t_DealID <= 0;
        END IF;
        EXCEPTION WHEN OTHERS THEN NULL;
     END;

     -- Удаление записей
     BEGIN
        DELETE FROM DDL_TICK_ENS_DBT dbt
         WHERE EXISTS( SELECT tmp.t_ID FROM DDL_TICK_ENS_TMP tmp WHERE tmp.t_EnsID = dbt.t_ID AND tmp.t_Delete = CHR(88) );
        EXCEPTION WHEN OTHERS THEN NULL;
     END;

     -- Обновление записей
     BEGIN
        UPDATE DDL_TICK_ENS_DBT dbt SET( dbt.t_Date, dbt.t_Kind, dbt.t_Principal, dbt.t_TotalCost, dbt.t_CostFIID, dbt.t_NKD ) =
                                ( SELECT tmp.t_Date, tmp.t_Kind, tmp.t_Principal, tmp.t_TotalCost, tmp.t_CostFIID, tmp.t_NKD
                                    FROM DDL_TICK_ENS_TMP tmp
                                   WHERE tmp.t_EnsID = dbt.t_ID AND tmp.t_Delete != CHR(88) )
         WHERE EXISTS( SELECT tmp.t_ID FROM DDL_TICK_ENS_TMP tmp WHERE tmp.t_EnsID = dbt.t_ID AND tmp.t_Delete != CHR(88) );
        EXCEPTION WHEN OTHERS THEN NULL;
     END;

     -- Вставка новых записей
     BEGIN
        INSERT INTO DDL_TICK_ENS_DBT ( t_ID,
                                       t_DealID,
                                       t_FIID,
                                       t_Date,
                                       t_Kind,
                                       t_Principal,
                                       t_TotalCost,
                                       t_CostFIID,
                                       t_NKD )
                                SELECT 0,
                                       tmp.t_DealID,
                                       tmp.t_FIID,
                                       tmp.t_Date,
                                       tmp.t_Kind,
                                       tmp.t_Principal,
                                       tmp.t_TotalCost,
                                       tmp.t_CostFIID,
                                       tmp.t_NKD
                                  FROM DDL_TICK_ENS_TMP tmp
                                 WHERE tmp.t_EnsID <= 0 AND tmp.t_Delete != CHR(88) AND tmp.t_Parent != 0;
        EXCEPTION WHEN OTHERS THEN NULL;
     END;
  END; -- RSI_SC_SaveTickEns

  --Получить, есть ли строки графика нужного статуса по комиссии(для использования ф-и в макросах)
  FUNCTION CheckExistGrDealByCom(pDocKind IN NUMBER, pDocID IN NUMBER, pCONTRACT IN NUMBER, pPLANPAYDATE IN DATE, pState IN NUMBER, pTemplNum IN NUMBER DEFAULT 0) RETURN NUMBER DETERMINISTIC
  AS
   BEGIN
    return RSI_DLGR.CheckExistGrDealByCom(pDocKind, pDocID, pCONTRACT, pPLANPAYDATE, pState, pTemplNum);
  END CheckExistGrDealByCom;

  -- Получить ID ТО по строке графика (без генерации ошибок)
  FUNCTION SC_GetRQByGrDeal( pGrDealID IN NUMBER ) RETURN NUMBER
  AS
  BEGIN
     RETURN RSI_DLRQ.RSI_GetRQByGrDeal(pGrDealID);
  END;

  -- функция определяет, является ли сделка технической
  FUNCTION RSI_IsTechDeal (p_DealID IN NUMBER, p_OnDate IN DATE)
     RETURN NUMBER
     DETERMINISTIC
  IS
     m_OnDate   DATE;
     m_AttrID   dobjatcor_dbt.t_AttrID%TYPE;
  BEGIN
     IF NOT p_DealID > -1
     THEN
        RETURN 0;
     END IF;

     -- Получаем значение категории.

     m_OnDate :=
        CASE
           WHEN p_OnDate = TO_DATE('01.01.0001', 'DD.MM.YYYY') THEN RsbSessionData.curdate
           ELSE p_OnDate
        END;

     BEGIN
        SELECT ac.t_AttrID
          INTO m_AttrID
          FROM dobjatcor_dbt ac
         WHERE ac.t_ObjectType = 101                      -- OBJTYPE_SECDEAL
               AND ac.t_GroupID = 24                      -- Является технической сделкой
               AND ac.t_Object = lpad( p_DealID, 34, '0' )
               AND ac.t_ValidToDate >= m_OnDate;
     EXCEPTION
        WHEN OTHERS
        THEN
           RETURN 0;
     END;

     IF m_AttrID = 2                                       -- OBJATTR_QUOTED_YES
     THEN
        RETURN 1;
     END IF;

     RETURN 0;
  END;

  -- срок в днях в соответствии с базисом расчёта
  FUNCTION GetTermInDaysByTypeDuration( pDuration     IN NUMBER,
                                        pTypeDuration IN NUMBER,
                                        pBasis        IN NUMBER
                                      ) RETURN number
  IS
     TermInDays NUMBER := 0;
  BEGIN
     if( pBasis = cnst.BASIS_30360 ) then
        if( pTypeDuration = RSB_SECUR.CB_PERIOD_DAY ) then
           TermInDays := pDuration;
        elsif( pTypeDuration = RSB_SECUR.CB_PERIOD_WEEK ) then
           TermInDays := pDuration * 7;
        elsif( pTypeDuration = RSB_SECUR.CB_PERIOD_MONTH ) then
           TermInDays := pDuration * 30;
        elsif( pTypeDuration = RSB_SECUR.CB_PERIOD_YEAR ) then
           TermInDays := pDuration * 360;
        end if;
     else
        raise_application_error(-20001, 'GetTermInDaysByTypeDuration: unknown basis');
     end if;

     RETURN TermInDays;

  END GetTermInDaysByTypeDuration;

    -- определяет дату закрытия сделки
  FUNCTION RSI_GetOperCloseDate( p_dockind IN NUMBER, p_docid IN NUMBER) RETURN DATE
  IS
     v_CloseDate  DATE   := RsbSessionData.curdate;
     v_at_carry   NUMBER := 1; -- DLDOC_CARRY
     v_at_carryc  NUMBER := 7; -- DLDOC_CARRYC
  BEGIN

     BEGIN
       SELECT MAX (d) INTO v_CloseDate
        FROM (SELECT MAX (t_valuedate) d
                FROM DPMPAYM_DBT
               WHERE     t_dockind = p_dockind
                     AND t_documentid = p_docid
                     AND (t_paymstatus = PM_COMMON.PM_FINISHED or t_paymstatus = Rsb_Payment.PM_CLOSED_W_M_MOVEMENT)
              UNION ALL
              SELECT MAX (t_plan_date) d
                FROM DOPROPER_DBT oo JOIN DOPRSTEP_DBT os USING (t_id_operation)
               WHERE oo.t_dockind = p_dockind AND TO_NUMBER (t_documentid) = p_docid
              UNION ALL
              SELECT MAX (t_date_carry) d
                FROM DOPROPER_DBT oo
                     JOIN DOPRDOCS_DBT od
                        ON oo.t_id_operation = od.t_id_operation
                     JOIN dacctrn_dbt act
                        ON act.t_acctrnid = od.t_acctrnid
               WHERE     oo.t_dockind = p_dockind
                     AND TO_NUMBER (oo.t_documentid) = p_docid
                     AND od.t_dockind IN (v_at_carry, v_at_carryc));

          EXCEPTION WHEN OTHERS THEN RETURN TO_DATE ('01.01.0001', 'DD.MM.YYYY');
     END;

     RETURN NVL (v_CloseDate, RsbSessionData.curdate);

  END RSI_GetOperCloseDate;

  -- количество дней в году по базису
  FUNCTION DaysInYearByBasis(basis in int,  -- базис
                       d in date)   -- дата с искомым годом
  RETURN int
  AS
      y date;
  BEGIN
      IF    basis = cnst.BASIS_ACTACT THEN -- act/act
          y := trunc(d, 'year');
          RETURN add_months(y, 12) - y;
      ELSIF basis = cnst.BASIS_ACT365 THEN -- 365/act
          RETURN 365;
      ElSE             -- 360/30, 360/act, 360/31
          RETURN 360;
      END IF;
  END DaysInYearByBasis;

  -- количество дней в месяце по базису
  FUNCTION DaysInMonthByBasis(basis in int,  -- базис
                       d in date)   -- дата с искомым месяцем
  RETURN int
  AS
    m date;
  BEGIN
    if  basis = cnst.BASIS_ACTACT or
        basis = cnst.BASIS_ACT360 or
        basis = cnst.BASIS_ACT365 or
        basis = cnst.BASIS_ACT360NOLEAP
    then
        m := trunc(d, 'month');
        return add_months(m, 1) - m;
    else
        return 30;
    end if;
  END DaysInMonthByBasis;

  -- переменное количество дней в месяце
  -- аналог K_VarMonth из pcidc.c
  FUNCTION SC_VarMonth(
      start_dt    in DATE,    -- начало периода начисления
      end_dt      in DATE,    -- окончние периода начисления
      basis       in NUMBER)  -- базис ставки (cnst.BASIS_***)
  RETURN number
  AS
      l_start_dt date := start_dt + 1; -- исключение начальной даты
  BEGIN
      if trunc(l_start_dt, 'year') = trunc(end_dt, 'year') then
          return (trunc(end_dt) - trunc(l_start_dt) + 1) / DaysInYearByBasis(basis, start_dt);
      else
          return
              (trunc(end_dt) - trunc(end_dt, 'year') + 1) / DaysInYearByBasis(basis, end_dt) +
              (add_months(trunc(l_start_dt, 'year'), 12) - trunc(l_start_dt)) / DaysInYearByBasis(basis, l_start_dt) +
              extract(year from end_dt) - extract(year from l_start_dt) - 1;
      end if;
  END SC_VarMonth;

  -- постоянное количество дней в месяце
  -- аналог K_ConstMonth из pcidc.c
  FUNCTION SC_ConstMonth(
      start_dt    in DATE,    -- начало периода начисления
      end_dt      in DATE,    -- окончние периода начисления
      basis       in NUMBER)  -- базис ставки (cnst.BASIS_***)
  RETURN number
  AS
      dim         number := DaysInMonthByBasis(basis, start_dt);
      l_start_dt  date := start_dt + 1; -- исключение начальной даты
      start_day   number := extract(day   from l_start_dt);
      start_mon   number := extract(month from l_start_dt);
      start_year  number := extract(year  from l_start_dt);
      end_day     number := extract(day   from end_dt);
      end_mon     number := extract(month from end_dt);
      end_year    number := extract(year  from end_dt);
      res number;
  BEGIN
      start_day   := least(start_day, dim);
      end_day     := least(end_day, dim);

      if trunc(l_start_dt, 'year') = trunc(end_dt, 'year') then
          res := (end_day - start_day + 1 + (end_mon - start_mon) * dim) / DaysInYearByBasis(basis, l_start_dt);
      else
          res :=
              (end_day + (end_mon - 1) * dim) / DaysInYearByBasis(basis, end_dt) +
              (dim - start_day + 1 + (12 - start_mon) * dim) / DaysInYearByBasis(basis, l_start_dt) +
              end_year - start_year - 1;
      end if;

      if res > 0 then
          return res;
      else
          return 0;
      end if;
  END SC_ConstMonth;

  -- количество лет по базису
  -- аналог K_Universal из pcidc.c
  FUNCTION SC_Years(
      start_dt    in DATE,    -- начало периода начисления
      end_dt      in DATE,    -- окончние периода начисления
      basis       in NUMBER)  -- базис ставки (cnst.BASIS_***)
  RETURN number
  AS
  BEGIN
      if      basis = cnst.BASIS_ACTACT or
              basis = cnst.BASIS_ACT360 or
              basis = cnst.BASIS_ACT365
      then
          return SC_VarMonth(start_dt, end_dt, basis);
      elsif   basis = cnst.BASIS_30360 or
              basis = cnst.BASIS_31360
      then
          return SC_ConstMonth(start_dt, end_dt, basis);
      else
          raise_application_error(-20001, 'SC_Years: unknown basis');
      end if;

  END SC_Years;

  -- Процедура установки признака необходимости перенумерации сделок
  PROCEDURE RSI_SetDateCalc( pDate IN DATE )
  IS
     v_CalcState NUMBER;
  BEGIN

     SELECT NVL((SELECT t_CalcState
       FROM DDLDATECALC_DBT
                  WHERE t_Date = pDate), -1) INTO v_CalcState
       FROM dual;

     IF( v_CalcState > 0 ) THEN
        UPDATE DDLDATECALC_DBT SET t_CalcState = 0
         WHERE t_Date = pDate;
     ELSIF(v_CalcState < 0) THEN
        BEGIN
          INSERT INTO DDLDATECALC_DBT(t_ID, t_Date, t_CalcState) VALUES(0, pDate, 0);
        EXCEPTION
          WHEN DUP_VAL_ON_INDEX THEN
            UPDATE DDLDATECALC_DBT SET t_CalcState = 0
             WHERE t_Date = pDate;
        END;
     END IF;

  END; -- RSI_SetDateCalc

  FUNCTION GetDvnObjType( p_DocKind IN ddvndeal_dbt.t_DocKind%TYPE )
    RETURN dobjatcor_dbt.t_ObjectType%TYPE
  IS
    x_ObjectType dobjatcor_dbt.t_ObjectType%TYPE;
  BEGIN
     IF (p_DocKind = DL_DVFXDEAL) THEN
        x_ObjectType := OBJTYPE_FXOPER_DV;
     ELSE
        x_ObjectType := OBJTYPE_OUTOPER_DV;
     END IF;
     RETURN x_ObjectType;
  END;

  FUNCTION GetMainObjAttr( p_ObjectType IN dobjatcor_dbt.t_ObjectType%TYPE,
                           p_Object     IN dobjatcor_dbt.t_Object%TYPE,
                           p_GroupID    IN dobjatcor_dbt.t_GroupID%TYPE,
                           p_Date       IN dobjatcor_dbt.t_ValidFromDate%TYPE
                         )
    RETURN dobjattr_dbt.t_AttrID%TYPE
  IS
    p_AttrID dobjattr_dbt.t_AttrID%TYPE;
  BEGIN
    BEGIN
      SELECT AtCor.t_AttrID INTO p_AttrID
        FROM dobjatcor_dbt AtCor
       WHERE AtCor.t_ObjectType  = p_ObjectType
         AND AtCor.t_GroupID     = p_GroupID
         AND AtCor.t_Object      = p_Object
         AND AtCor.t_ValidToDate >= p_Date
         AND AtCor.t_ValidFromDate = (SELECT MAX(t.t_ValidFromDate)
                                        FROM dobjatcor_dbt t
                                       WHERE t.t_ObjectType     = p_ObjectType
                                         AND t.t_GroupID        = p_GroupID
                                         AND t.t_Object         = p_Object
                                         AND t.t_ValidFromDate <= p_Date
                                         AND t.t_ValidToDate    >= p_Date
                                     );
    EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
        p_AttrID := 0;
      WHEN OTHERS
      THEN
        p_AttrID := 0;
    END;

    RETURN p_AttrID;
  END; --GetMainObjAttr

  FUNCTION GetGeneralMainObjAttr( p_ObjectType IN dobjatcor_dbt.t_ObjectType%TYPE,
                                  p_Object     IN dobjatcor_dbt.t_Object%TYPE,
                                  p_GroupID    IN dobjatcor_dbt.t_GroupID%TYPE,
                                  p_Date       IN dobjatcor_dbt.t_ValidFromDate%TYPE
                                )
    RETURN dobjattr_dbt.t_AttrID%TYPE DETERMINISTIC
  IS
    p_AttrID dobjattr_dbt.t_AttrID%TYPE;
  BEGIN
    BEGIN
      SELECT AtCor.t_AttrID INTO p_AttrID
        FROM dobjatcor_dbt AtCor
       WHERE AtCor.t_ObjectType  = p_ObjectType
         AND AtCor.t_GroupID     = p_GroupID
         AND AtCor.t_Object      = p_Object
         AND AtCor.t_General     = 'X'
         AND AtCor.t_ValidToDate >= p_Date
         AND AtCor.t_ValidFromDate = (SELECT MAX(t.t_ValidFromDate)
                                        FROM dobjatcor_dbt t
                                       WHERE t.t_ObjectType     = p_ObjectType
                                         AND t.t_GroupID        = p_GroupID
                                         AND t.t_Object         = p_Object
                                         AND t.t_General        = 'X'
                                         AND t.t_ValidFromDate <= p_Date
                                         AND t.t_ValidToDate    >= p_Date
                                     );
    EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
        p_AttrID := 0;
      WHEN OTHERS
      THEN
        p_AttrID := 0;
    END;

    RETURN p_AttrID;
  END; --GetGeneralMainObjAttr


  FUNCTION GetMainObjAttrNoDate(p_ObjectType IN dobjatcor_dbt.t_ObjectType%TYPE,
                                p_Object     IN dobjatcor_dbt.t_Object%TYPE,
                                p_GroupID    IN dobjatcor_dbt.t_GroupID%TYPE
                                )
    RETURN dobjattr_dbt.t_AttrID%TYPE
  IS
    p_AttrID dobjattr_dbt.t_AttrID%TYPE;
  BEGIN
    BEGIN
      SELECT AtCor.t_AttrID INTO p_AttrID
        FROM dobjatcor_dbt AtCor
       WHERE AtCor.t_ObjectType  = p_ObjectType
         AND AtCor.t_GroupID     = p_GroupID
         AND AtCor.t_Object      = p_Object;
    EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
        p_AttrID := 0;
      WHEN OTHERS
      THEN
        p_AttrID := 0;
    END;

    RETURN p_AttrID;
  END; --GetMainObjAttrNoDate

  FUNCTION GetFICategoryAttrID( pFiID IN NUMBER, pGroupID IN dobjatcor_dbt.t_GroupID%TYPE )
     RETURN dobjattr_dbt.t_AttrID%TYPE
  IS
  BEGIN
     RETURN GetMainObjAttrNoDate(OBJTYPE_AVOIRISS, LPAD(pFIID, 10, '0'), pGroupID);
  END;

  function GetRegBoolValueAsInt(p_KeyPath IN VARCHAR2, p_Oper IN NUMBER)
    return NUMBER
  is
    parmval BOOLEAN;
    retval NUMBER;
  begin
    parmval := Rsb_Common.GetRegBoolValue(p_KeyPath, p_Oper);
    retval := 0;

    if (parmval = true) then
      retval := 1;
    end if;

    return retval;
  end;

  FUNCTION GetObjAttr(p_ObjectType IN dobjatcor_dbt.t_ObjectType%TYPE
                     ,p_GroupID IN dobjatcor_dbt.t_GroupID%TYPE
                     ,p_AttrID IN dobjatcor_dbt.t_AttrID%TYPE)
    RETURN dobjattr_dbt%ROWTYPE
  IS
    p_Attr dobjattr_dbt%ROWTYPE;
  BEGIN
    SELECT
      Attr.*
    INTO
      p_Attr
    FROM
      dobjattr_dbt Attr
     ,(SELECT
         p_ObjectType AS t_ObjectType
        ,p_GroupID AS t_GroupID
        ,p_AttrID AS t_AttrID
       FROM
         DUAL) q
    WHERE
      Attr.t_ObjectType(+) = q.t_ObjectType
    AND Attr.t_GroupID(+) = q.t_GroupID
    AND Attr.t_AttrID(+) = q.t_AttrID;

    RETURN p_Attr;
  END; --GetObjAttr

  FUNCTION GetObjAttrName(p_ObjectType IN dobjatcor_dbt.t_ObjectType%TYPE
                         ,p_GroupID IN dobjatcor_dbt.t_GroupID%TYPE
                         ,p_AttrID IN dobjatcor_dbt.t_AttrID%TYPE)
    RETURN dobjattr_dbt.t_Name%TYPE
  IS
  BEGIN
    RETURN NVL(GetObjAttr(p_ObjectType, p_GroupID, p_AttrID).t_Name, CHR(1));
  END; --GetObjAttrName

  FUNCTION GetObjAttrFullName(p_ObjectType IN dobjatcor_dbt.t_ObjectType%TYPE
                             ,p_GroupID IN dobjatcor_dbt.t_GroupID%TYPE
                             ,p_AttrID IN dobjatcor_dbt.t_AttrID%TYPE)
    RETURN dobjattr_dbt.t_FullName%TYPE
  IS
  BEGIN
    RETURN NVL(GetObjAttr(p_ObjectType, p_GroupID, p_AttrID).t_FullName, CHR(1));
  END; --GetObjAttrFullName

  FUNCTION GetObjAttrNumber(p_ObjectType IN dobjatcor_dbt.t_ObjectType%TYPE
                           ,p_GroupID IN dobjatcor_dbt.t_GroupID%TYPE
                           ,p_AttrID IN dobjatcor_dbt.t_AttrID%TYPE)
    RETURN dobjattr_dbt.t_NumInList%TYPE
  IS
  BEGIN
    RETURN NVL(GetObjAttr(p_ObjectType, p_GroupID, p_AttrID).t_NumInList, CHR(1));
  END; --GetObjAttrNumber
  
  FUNCTION GetObjAttrNameObject(p_ObjectType IN dobjatcor_dbt.t_ObjectType%TYPE
                               ,p_GroupID IN dobjatcor_dbt.t_GroupID%TYPE
                               ,p_AttrID IN dobjatcor_dbt.t_AttrID%TYPE)
    RETURN dobjattr_dbt.t_NameObject%TYPE DETERMINISTIC
  IS
  BEGIN
    RETURN NVL(GetObjAttr(p_ObjectType, p_GroupID, p_AttrID).t_NameObject, CHR(1));
  END; --GetObjAttrNameObject
  
  FUNCTION GetNameAlg(p_TypeAlg IN dNameAlg_dbt.t_iTypeAlg%TYPE
                     ,p_NumberAlg IN dNameAlg_dbt.t_iNumberAlg%TYPE)
    RETURN dNameAlg_dbt%ROWTYPE
  IS
    p_NA dNameAlg_dbt%ROWTYPE;
  BEGIN
    SELECT NA.* INTO p_NA
      FROM dNameAlg_dbt NA,
           (SELECT p_TypeAlg AS t_TypeAlg,
                   p_NumberAlg AS t_NumberAlg
              FROM DUAL) q
    WHERE NA.t_iTypeAlg(+) = q.t_TypeAlg
      AND NA.t_iNumberAlg(+) = q.t_NumberAlg;

    RETURN p_NA;
  END; --GetNameAlg
  
  FUNCTION GetNameAlgName(p_TypeAlg IN dNameAlg_dbt.t_iTypeAlg%TYPE
                         ,p_NumberAlg IN dNameAlg_dbt.t_iNumberAlg%TYPE)
    RETURN dNameAlg_dbt.t_szNameAlg%TYPE DETERMINISTIC
  IS
  BEGIN
    RETURN NVL(GetNameAlg(p_TypeAlg, p_NumberAlg).t_szNameAlg, CHR(1));
  END; --GetNameAlgName

  function SC_GetNominalCostByPeriod( pFIID              IN NUMBER,
                                      pBegDate           IN DATE,
                                      pEndDate           IN DATE
                                    ) return NUMBER is
    vNominalCost NUMBER := 0;
  begin

    BEGIN
      SELECT nvl(sum(t_FACEVALUE),0) INTO vNominalCost
        FROM DV_FI_FACEVALUE_HIST
       WHERE t_FIID = pFIID
         AND t_BegDate >= pBegDate
         AND t_BegDate <= pEndDate;
    EXCEPTION
       WHEN NO_DATA_FOUND THEN vNominalCost := 0;
    END;

    return vNominalCost;
  end;

  -- Получить курс НКД за период (используется в отчете)
  FUNCTION SC_GetNKDCourseByPeriod( p_FIID IN NUMBER,
                                    p_BegDate IN DATE,
                                    p_EndDate IN DATE,
                                    p_NKDCourseType IN NUMBER,
                                    p_FaceValueFI IN NUMBER,
                                    p_HandMode IN NUMBER DEFAULT -1 --отбирать курсы: -1 - все, 0 - репликация, 1 - ручной ввод
                                  )
    RETURN NUMBER
  IS
    v_Rate     NUMBER;
    v_RateID   NUMBER;
    v_RateDate DATE;
    v_CurDate  DATE   := p_BegDate;
    v_RateAll  NUMBER := 0;
  BEGIN

    LOOP
       v_Rate := RSI_RSB_FIInstr.FI_GetRate( p_FIID, p_FaceValueFI, p_NKDCourseType, v_CurDate, 0, 0, v_RateID, v_RateDate, False, null, null, null, null, null, p_HandMode);
       if( v_Rate > 0 ) then
          v_RateAll := v_RateAll + v_Rate;
       end if;
       v_CurDate := v_CurDate + 1;
    EXIT WHEN (v_CurDate > p_EndDate);
    END LOOP;

    return v_RateAll;
  END;

  -- Проверка для субъекта, что он является налоговым резидентом
  FUNCTION IsTaxResident( PartyID IN NUMBER, OperDate IN DATE ) RETURN NUMBER DETERMINISTIC
  IS
    CategoryValue dobjattr_dbt.t_NumInList % TYPE;
    legal_form    dparty_dbt.t_LegalForm % TYPE;
    not_resident  dparty_dbt.t_NotResident % TYPE;
  BEGIN
    BEGIN
      SELECT party.t_LegalForm, party.t_NoTResident INTO legal_form, not_resident
      FROM DPARTY_DBT party
      WHERE party.t_PartyID = PartyID;

      IF ( legal_form = 2 ) THEN
            -- -------- ФИЗЛИЦО ------------
              SELECT Attr.t_NumInList INTO CategoryValue
                FROM dobjatcor_dbt AtCor, dobjattr_dbt Attr
               WHERE AtCor.t_ObjectType = 3  -- OBJTYPE_PARTY
                 AND AtCor.t_GroupID    = 42 -- Является плательщиком НДФЛ
                 AND AtCor.t_Object     = LPAD( PartyID, 10, '0' )
                 AND AtCor.t_ValidFromDate  = ( SELECT MAX(t.T_ValidFromDate)
                                                  FROM DOBJATCOR_DBT t
                                                  WHERE t.T_ObjectType = AtCor.T_ObjectType
                                                    AND t.T_GroupID    = AtCor.T_GroupID
                                                    AND t.t_Object     = AtCor.t_Object
                                                    AND t.T_ValidFromDate <= OperDate
                                              )
                 AND Attr.t_AttrID      = AtCor.t_AttrID
                 AND Attr.t_ObjectType  = AtCor.t_ObjectType
                 AND Attr.t_GroupID     = AtCor.t_GroupID
                 AND AtCor.t_ValidToDate >= OperDate;

       ELSE
            -- --------- ЮРЛИЦО -----------
              SELECT Attr.t_NumInList INTO CategoryValue
                FROM dobjatcor_dbt AtCor, dobjattr_dbt Attr
               WHERE AtCor.t_ObjectType = 3  -- OBJTYPE_PARTY
                 AND AtCor.t_GroupID    = 84 -- имеет постоянное представительство
                 AND AtCor.t_Object     = LPAD( PartyID, 10, '0' )
                 AND AtCor.t_ValidFromDate  = ( SELECT MAX(t.T_ValidFromDate)
                                                 FROM DOBJATCOR_DBT t
                                                 WHERE t.T_ObjectType = AtCor.T_ObjectType
                                                   AND t.T_GroupID    = AtCor.T_GroupID
                                                   AND t.t_Object     = AtCor.t_Object
                                                   AND t.T_ValidFromDate <= OperDate
                                              )
                 AND Attr.t_AttrID      = AtCor.t_AttrID
                 AND Attr.t_ObjectType  = AtCor.t_ObjectType
                 AND Attr.t_GroupID     = AtCor.t_GroupID
                 AND Attr.t_NameObject  = '1'
                 AND AtCor.t_ValidToDate >= OperDate;
      END IF;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN CategoryValue := chr(0);
      WHEN OTHERS THEN
             return 0;
    END;

    -- ФизЛицо
    IF( legal_form = 2  AND CategoryValue = '1'  ) THEN
           return 1; -- Резидент
    ELSIF( legal_form = 2 and CategoryValue = chr(0) ) THEN
        if ( 'X' = not_resident ) then
           return 0; -- НЕ Резидент
        else
           return 1;  -- Резидент
        end if;
    END IF;

    -- ЮрЛицо
    IF ( legal_form = 1 and CategoryValue = chr(0) and not_resident = 'X' ) THEN
            return 0; -- НЕ Резидент
    ELSIF ( legal_form = 1 ) THEN
           return 1; -- Резидент
    END IF;

    RETURN 0;
  END; -- IsTaxResident


  -- Получить репозитарный статус сделки
   FUNCTION GetRStatus (dealID IN NUMBER, kindID IN NUMBER)
      RETURN NUMBER
   IS
      RSTAT_V    NUMBER DEFAULT -1;
      STAT_V     NUMBER;
      UPDATE_V CHAR;
      BULK_V     CHAR;
      kindID_V   NUMBER;
      v_KeyID    NUMBER;
      v_val      NUMERIC;
      v_qq       NUMERIC;
      v_ExpDep   NUMERIC;
      v_INIT     NUMERIC;
      v_reco     NUMERIC;
      v_pod      CHAR;
  BEGIN
      --Определение kindID
      SELECT (CASE
                 WHEN kindID = 101 THEN 101
                 WHEN kindID = 4815 THEN 4815
                 WHEN kindID = 4813 THEN 4813
                 ELSE 199
              END)
        INTO kindID_V
        FROM DUAL;
      --Определение сводных сообщений
      BEGIN
         SELECT T_RSTATUS, T_BULK,T_TEMPLNUM
           INTO STAT_V, BULK_V,UPDATE_V
           FROM (SELECT GINF.T_RSTATUS,
                        GINF.T_BULK,
                        decode(GDEAL.T_TEMPLNUM,47,'X',49,'X',chr(0)) T_TEMPLNUM,
                        ROW_NUMBER ()
                     OVER (ORDER BY GINF.T_PRDATE DESC, GINF.T_PRTIME DESC)
                           rn
                   FROM ddlgrdeal_dbt GDEAL,
                        ddlgrdoc_dbt GDOC,
                        dir_generalinf_dbt GINF
                  WHERE     GDOC.T_GRDEALID = GDEAL.T_ID
                        AND GDEAL.t_docid = dealID
                        AND GDEAL.T_DOCKIND = kindID_V
                        AND GINF.T_INTERNALMESSAGEID =
                               (CASE
                                WHEN GDOC.T_DOCKIND = RSB_SECUR.REPOS_CM023
                                   THEN
                                      (SELECT T_INTERNALMESSAGEID
                                         FROM dir_cm023_dbt
                                        WHERE T_ID = GDOC.T_DOCID)
                                WHEN GDOC.T_DOCKIND = RSB_SECUR.REPOS_CM021
                                   THEN
                                      (SELECT T_INTERNALMESSAGEID
                                         FROM dir_cm021_dbt
                                        WHERE T_ID = GDOC.T_DOCID)
                                WHEN GDOC.T_DOCKIND = RSB_SECUR.REPOS_CM022
                                   THEN
                                      (SELECT T_INTERNALMESSAGEID
                                         FROM dir_cm022_dbt
                                        WHERE T_ID = GDOC.T_DOCID)
                                WHEN GDOC.T_DOCKIND = RSB_SECUR.REPOS_CM032
                                   THEN
                                      (SELECT T_INTERNALMESSAGEID
                                         FROM dir_cm032_dbt
                                        WHERE T_ID = GDOC.T_DOCID)
                                WHEN GDOC.T_DOCKIND = RSB_SECUR.REPOS_CM041
                                   THEN
                                      (SELECT T_INTERNALMESSAGEID
                                         FROM dir_cm041_dbt
                                        WHERE T_ID = GDOC.T_DOCID)
                                WHEN GDOC.T_DOCKIND = RSB_SECUR.REPOS_CM042
                                   THEN
                                      (SELECT T_INTERNALMESSAGEID
                                         FROM dir_cm042_dbt
                                        WHERE T_ID = GDOC.T_DOCID)
                                WHEN GDOC.T_DOCKIND = RSB_SECUR.REPOS_CM043
                                   THEN
                                      (SELECT T_INTERNALMESSAGEID
                                         FROM dir_cm043_dbt
                                        WHERE T_ID = GDOC.T_DOCID)
                                WHEN GDOC.T_DOCKIND = RSB_SECUR.REPOS_CM044
                                   THEN
                                      (SELECT T_INTERNALMESSAGEID
                                         FROM dir_cm044_dbt
                                        WHERE T_ID = GDOC.T_DOCID)
                                WHEN GDOC.T_DOCKIND = RSB_SECUR.REPOS_CM045
                                   THEN
                                      (SELECT T_INTERNALMESSAGEID
                                         FROM dir_cm045_dbt
                                        WHERE T_ID = GDOC.T_DOCID)
                                WHEN GDOC.T_DOCKIND = RSB_SECUR.REPOS_CM046
                                   THEN
                                      (SELECT T_INTERNALMESSAGEID
                                         FROM dir_cm046_dbt
                                        WHERE T_ID = GDOC.T_DOCID)
                                WHEN GDOC.T_DOCKIND = RSB_SECUR.REPOS_CM047
                                   THEN
                                      (SELECT T_INTERNALMESSAGEID
                                         FROM dir_cm047_dbt
                                        WHERE T_ID = GDOC.T_DOCID)
                                WHEN GDOC.T_DOCKIND = RSB_SECUR.REPOS_CM048
                                   THEN
                                      (SELECT T_INTERNALMESSAGEID
                                         FROM dir_cm048_dbt
                                        WHERE T_ID = GDOC.T_DOCID)
                                WHEN GDOC.T_DOCKIND = RSB_SECUR.REPOS_CM083
                                   THEN
                                      (SELECT T_INTERNALMESSAGEID
                                         FROM dir_cm083_dbt
                                        WHERE T_ID = GDOC.T_DOCID)
                                WHEN GDOC.T_DOCKIND = RSB_SECUR.REPOS_CM093
                                   THEN
                                      (SELECT T_INTERNALMESSAGEID
                                         FROM dir_cm093_dbt
                                        WHERE T_ID = GDOC.T_DOCID)
                                WHEN GDOC.T_DOCKIND = RSB_SECUR.REPOS_CM093
                                   THEN
                                      (SELECT T_INTERNALMESSAGEID
                                         FROM dir_cm053_dbt
                                        WHERE T_ID = GDOC.T_DOCID)
                                 WHEN GDOC.T_DOCKIND = RSB_SECUR.REPOS_CM051
                                   THEN
                                      (SELECT T_INTERNALMESSAGEID
                                         FROM dir_cm051_dbt
                                        WHERE T_ID = GDOC.T_DOCID)
                                WHEN GDOC.T_DOCKIND = RSB_SECUR.REPOS_CM094
                                   THEN
                                      (SELECT T_INTERNALMESSAGEID
                                         FROM dir_cm094_dbt
                                        WHERE T_ID = GDOC.T_DOCID)

                                END))
          WHERE rn = 1;

         IF (STAT_V = 1 AND BULK_V <> 'X'and UPDATE_V<>'X')                                 --1 Подготовлено
         THEN
            RSTAT_V := 2;                                --Анкета подготовлена
         ELSIF (STAT_V = 2 AND BULK_V <> 'X'and UPDATE_V<>'X')                              --2   Ошибка подготовки
         THEN
            RSTAT_V := 7;                           --Ошибка подготовки анкеты
         ELSIF (STAT_V = 3 AND BULK_V <> 'X'and UPDATE_V<>'X')                              --3
         THEN
            RSTAT_V := 3;                                   --Анкета выгружена
         ELSIF (STAT_V = 4 AND BULK_V <> 'X'and UPDATE_V<>'X')                              --4
         THEN
            RSTAT_V := 8;                             --Ошибка выгрузки анкеты
         ELSIF (STAT_V = 1 AND BULK_V <> 'X' and UPDATE_V='X')                              --5  Подготовлено
         THEN
            RSTAT_V := 5;                      --Изменение анкеты подготовлено
         ELSIF (STAT_V = 2 AND BULK_V <> 'X' and UPDATE_V='X')                              --6
         THEN
            RSTAT_V := 9;                -- Ошибка подготовки изменения анкеты
         ELSIF (STAT_V = 3 AND BULK_V <> 'X' and UPDATE_V='X')                              --7
         THEN
            RSTAT_V := 6;                         --Изменение анкеты выгружено
         ELSIF (STAT_V = 4 AND BULK_V <> 'X' and UPDATE_V='X')                              --8
         THEN
            RSTAT_V := 10;                         --Ошибка выгрузки изменения
         END IF;

      /*      условие связи сводных сообщениий с графиком*/
         IF (STAT_V = 1 AND BULK_V = 'X')                                  --9  Подготовлено
         THEN
            RSTAT_V := 12;                         --Сводный отчет подготовлен
         ELSIF (STAT_V = 2 AND BULK_V = 'X')                              --10  Ошибка подготовки
         THEN
            RSTAT_V := 13;                 --Ошибка подготовки сводного отчета
         ELSIF (STAT_V = 4 AND BULK_V = 'X')                              --11
         THEN
            RSTAT_V := 14;                   --Ошибка выгрузки сводного отчета
         ELSIF (STAT_V = 3 AND BULK_V = 'X')                              --12
         THEN
            RSTAT_V := 15;                            --Сводный отчет выгружен
         END IF;

      IF (RSTAT_V = -1)
      THEN
         --Если нет сообщений СРС
         FOR item IN (SELECT GR.T_TEMPLNUM TEMPL
                        FROM ddlgrdeal_dbt gr
                       WHERE GR.T_DOCID = dealID AND GR.T_DOCKIND = kindID_V)
         LOOP
               IF (item.TEMPL = 48)
               THEN
                  RSTAT_V := 11;
               ELSIF (item.TEMPL = 46 OR item.TEMPL = 43 OR item.TEMPL = 72 )
            THEN
               RSTAT_V := 1;                               --Подготовка анкеты
            ELSIF (item.TEMPL = 47)
            THEN
               RSTAT_V := 4;                            --Подготовка изменения
            END IF;
         END LOOP;
         /* IF (RSTAT_V = -1)
         THEN
            v_KeyID :=
               rsb_common.
                RSI_GetRegParm (
                  'РЕПОЗИТАРИЙ\СРОК ОЖИД.ЗАПР. ПРИ КОМБ.ПОДТВ.');
            v_qq :=
               rsb_common.RSI_ProcessIntegerValue (v_val,
                                                   v_KeyID,
                                                   0,
                                                   0,
                                                   v_ExpDep);
          END IF;*/

         /*SELECT OGR.T_KEEPOLDVALUES
           INTO v_pod
           FROM dobjgroup_dbt ogr
          WHERE OGR.T_OBJECTTYPE =
                   (CASE WHEN kindID_V = 101 THEN kindID_V ELSE 145 END)
                AND OGR.T_GROUPID = 43;

         BEGIN
            SELECT T_INITIATOR, T_TYPE_RECONCILIATION
              INTO v_INIT, V_reco
              FROM ddl_repozdeal_dbt rep
             WHERE REP.T_DOCID = dealID AND REP.T_DOCKIND = kindID_V;

            IF (    (v_INIT = 2)
                AND (V_reco = 1)
                AND (v_val > 0)
                AND (v_pod <> 'X'))
            THEN
               RSTAT_V := 11;                   --Ожидание запроса репозитария
            END IF;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               IF (v_pod = 'X')
               THEN
                  RSTAT_V := 16;                                    --Квитовка
               END IF;

               NULL;
         END;*/
      END IF;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            --Если нет сообщений СРС
         FOR item IN (SELECT GR.T_TEMPLNUM TEMPL
                        FROM ddlgrdeal_dbt gr
                    WHERE GR.T_DOCID = dealID AND GR.T_DOCKIND = kindID_V)
            LOOP
               IF (item.TEMPL = 48)
               THEN
                  RSTAT_V := 11;
               ELSIF (item.TEMPL = 46 OR item.TEMPL = 43 OR item.TEMPL = 72 )
               THEN
                  RSTAT_V := 1;                            --Подготовка анкеты
               ELSIF (item.TEMPL = 47 or item.TEMPL = 49)
               THEN
                  RSTAT_V := 4;                         --Подготовка изменения
               END IF;
            END LOOP;
      /* IF (RSTAT_V = -1)
            THEN
               v_KeyID :=
                  rsb_common.
                   RSI_GetRegParm (
                     'РЕПОЗИТАРИЙ\СРОК ОЖИД.ЗАПР. ПРИ КОМБ.ПОДТВ.');
               v_qq :=
                  rsb_common.RSI_ProcessIntegerValue (v_val,
                                                      v_KeyID,
                                                      0,
                                                      0,
                                                      v_ExpDep);
            END IF;

            SELECT OGR.T_KEEPOLDVALUES
              INTO v_pod
              FROM dobjgroup_dbt ogr
             WHERE OGR.T_OBJECTTYPE =
                      (CASE WHEN kindID_V = 101 THEN kindID_V ELSE 145 END)
                   AND OGR.T_GROUPID = 43;

            BEGIN
               SELECT T_INITIATOR, T_TYPE_RECONCILIATION
                 INTO v_INIT, V_reco
                 FROM ddl_repozdeal_dbt rep
                WHERE REP.T_DOCID = dealID AND REP.T_DOCKIND = kindID_V;

               IF (    (v_INIT = 2)
                   AND (V_reco = 1)
                   AND (v_val > 0)
                   AND (v_pod <> 'X'))
               THEN
                  RSTAT_V := 11;                --Ожидание запроса репозитария
               END IF;
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  IF (v_pod = 'X')
                  THEN
                     RSTAT_V := 16;                                 --Квитовка
                  END IF;
          END;*/
      END;

      RETURN RSTAT_V;
  END GetRStatus;

  -- Получить репозитарный статус сделки
   FUNCTION StatusSRS_Is093 (dealID IN NUMBER, kindID IN NUMBER )
      RETURN NUMBER
   IS
      KIND_V NUMBER DEFAULT 0;
      kindID_V   NUMBER;
      is093      NUMBER DEFAULT 0;
  BEGIN
      --Определение kindID
      SELECT (CASE
                 WHEN kindID = 101 THEN 101
                 WHEN kindID = 4815 THEN 4815
                 WHEN kindID = 4813 THEN 4813
                 ELSE 199
              END)
        INTO kindID_V
        FROM DUAL;
      --Определение сводных сообщений
      BEGIN
         SELECT T_KIND
           INTO KIND_V
           FROM (SELECT GDOC.T_DOCKIND T_KIND,
                        ROW_NUMBER ()
                     OVER (ORDER BY GINF.T_PRDATE DESC, GINF.T_PRTIME DESC)
                           rn
                   FROM ddlgrdeal_dbt GDEAL,
                        ddlgrdoc_dbt GDOC,
                        dir_generalinf_dbt GINF
                  WHERE     GDOC.T_GRDEALID = GDEAL.T_ID
                        AND GDEAL.t_docid = dealID
                        AND GDEAL.T_DOCKIND = kindID_V
                        AND GINF.T_INTERNALMESSAGEID =
                               (CASE
                                WHEN GDOC.T_DOCKIND = RSB_SECUR.REPOS_CM023
                                   THEN
                                      (SELECT T_INTERNALMESSAGEID
                                         FROM dir_cm023_dbt
                                        WHERE T_ID = GDOC.T_DOCID)
                                WHEN GDOC.T_DOCKIND = RSB_SECUR.REPOS_CM021
                                   THEN
                                      (SELECT T_INTERNALMESSAGEID
                                         FROM dir_cm021_dbt
                                        WHERE T_ID = GDOC.T_DOCID)
                                WHEN GDOC.T_DOCKIND = RSB_SECUR.REPOS_CM022
                                   THEN
                                      (SELECT T_INTERNALMESSAGEID
                                         FROM dir_cm022_dbt
                                        WHERE T_ID = GDOC.T_DOCID)
                                WHEN GDOC.T_DOCKIND = RSB_SECUR.REPOS_CM032
                                   THEN
                                      (SELECT T_INTERNALMESSAGEID
                                         FROM dir_cm032_dbt
                                        WHERE T_ID = GDOC.T_DOCID)
                                WHEN GDOC.T_DOCKIND = RSB_SECUR.REPOS_CM041
                                   THEN
                                      (SELECT T_INTERNALMESSAGEID
                                         FROM dir_cm041_dbt
                                        WHERE T_ID = GDOC.T_DOCID)
                                 WHEN GDOC.T_DOCKIND = RSB_SECUR.REPOS_CM042
                                   THEN
                                      (SELECT T_INTERNALMESSAGEID
                                         FROM dir_cm042_dbt
                                        WHERE T_ID = GDOC.T_DOCID)
                                WHEN GDOC.T_DOCKIND = RSB_SECUR.REPOS_CM043
                                   THEN
                                      (SELECT T_INTERNALMESSAGEID
                                         FROM dir_cm043_dbt
                                        WHERE T_ID = GDOC.T_DOCID)
                                WHEN GDOC.T_DOCKIND = RSB_SECUR.REPOS_CM044
                                   THEN
                                      (SELECT T_INTERNALMESSAGEID
                                         FROM dir_cm044_dbt
                                        WHERE T_ID = GDOC.T_DOCID)
                                WHEN GDOC.T_DOCKIND = RSB_SECUR.REPOS_CM045
                                   THEN
                                      (SELECT T_INTERNALMESSAGEID
                                         FROM dir_cm045_dbt
                                        WHERE T_ID = GDOC.T_DOCID)
                                WHEN GDOC.T_DOCKIND = RSB_SECUR.REPOS_CM046
                                   THEN
                                      (SELECT T_INTERNALMESSAGEID
                                         FROM dir_cm046_dbt
                                        WHERE T_ID = GDOC.T_DOCID)
                                WHEN GDOC.T_DOCKIND = RSB_SECUR.REPOS_CM047
                                   THEN
                                      (SELECT T_INTERNALMESSAGEID
                                         FROM dir_cm047_dbt
                                        WHERE T_ID = GDOC.T_DOCID)
                                WHEN GDOC.T_DOCKIND = RSB_SECUR.REPOS_CM048
                                   THEN
                                      (SELECT T_INTERNALMESSAGEID
                                         FROM dir_cm048_dbt
                                        WHERE T_ID = GDOC.T_DOCID)
                                WHEN GDOC.T_DOCKIND = RSB_SECUR.REPOS_CM083
                                   THEN
                                      (SELECT T_INTERNALMESSAGEID
                                         FROM dir_cm083_dbt
                                        WHERE T_ID = GDOC.T_DOCID)
                                WHEN GDOC.T_DOCKIND = RSB_SECUR.REPOS_CM093
                                   THEN
                                      (SELECT T_INTERNALMESSAGEID
                                         FROM dir_cm093_dbt
                                        WHERE T_ID = GDOC.T_DOCID)
                                WHEN GDOC.T_DOCKIND = RSB_SECUR.REPOS_CM053
                                   THEN
                                      (SELECT T_INTERNALMESSAGEID
                                         FROM dir_cm053_dbt
                                        WHERE T_ID = GDOC.T_DOCID)
                                WHEN GDOC.T_DOCKIND = RSB_SECUR.REPOS_CM051
                                   THEN
                                      (SELECT T_INTERNALMESSAGEID
                                         FROM dir_cm051_dbt
                                        WHERE T_ID = GDOC.T_DOCID)
                                END))
          WHERE rn = 1;
      if(KIND_V = RSB_SECUR.REPOS_CM093)
      THEN
        is093 :=1;
      ELSE
        is093 :=0;
      END IF;

      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
         is093 :=0;

      END;
  RETURN  is093;

  END StatusSRS_Is093;

  -- Получить репозитарный статус сделки
   FUNCTION StatusSRS_Is094 (dealID IN NUMBER, kindID IN NUMBER )
      RETURN NUMBER
   IS
      KIND_V NUMBER DEFAULT 0;
      kindID_V   NUMBER;
      is094      NUMBER DEFAULT 0;
  BEGIN
      --Определение kindID
      SELECT (CASE
                 WHEN kindID = 101 THEN 101
                 WHEN kindID = 4815 THEN 4815
                 ELSE 199
              END)
        INTO kindID_V
        FROM DUAL;
      --Определение сводных сообщений
      BEGIN
         SELECT T_KIND
           INTO KIND_V
           FROM (SELECT GDOC.T_DOCKIND T_KIND,
                        ROW_NUMBER ()
                     OVER (ORDER BY GINF.T_PRDATE DESC, GINF.T_PRTIME DESC)
                           rn
                   FROM ddlgrdeal_dbt GDEAL,
                        ddlgrdoc_dbt GDOC,
                        dir_generalinf_dbt GINF
                  WHERE     GDOC.T_GRDEALID = GDEAL.T_ID
                        AND GDEAL.t_docid = dealID
                        AND GDEAL.T_DOCKIND = kindID_V
                        AND GINF.T_INTERNALMESSAGEID =
                               (CASE
                                WHEN GDOC.T_DOCKIND = RSB_SECUR.REPOS_CM023
                                   THEN
                                      (SELECT T_INTERNALMESSAGEID
                                         FROM dir_cm023_dbt
                                        WHERE T_ID = GDOC.T_DOCID)
                                WHEN GDOC.T_DOCKIND = RSB_SECUR.REPOS_CM021
                                   THEN
                                      (SELECT T_INTERNALMESSAGEID
                                         FROM dir_cm021_dbt
                                        WHERE T_ID = GDOC.T_DOCID)
                                WHEN GDOC.T_DOCKIND = RSB_SECUR.REPOS_CM022
                                   THEN
                                      (SELECT T_INTERNALMESSAGEID
                                         FROM dir_cm022_dbt
                                        WHERE T_ID = GDOC.T_DOCID)
                                WHEN GDOC.T_DOCKIND = RSB_SECUR.REPOS_CM032
                                   THEN
                                      (SELECT T_INTERNALMESSAGEID
                                         FROM dir_cm032_dbt
                                        WHERE T_ID = GDOC.T_DOCID)
                                WHEN GDOC.T_DOCKIND = RSB_SECUR.REPOS_CM041
                                   THEN
                                      (SELECT T_INTERNALMESSAGEID
                                         FROM dir_cm041_dbt
                                        WHERE T_ID = GDOC.T_DOCID)
                                WHEN GDOC.T_DOCKIND = RSB_SECUR.REPOS_CM042
                                   THEN
                                      (SELECT T_INTERNALMESSAGEID
                                         FROM dir_cm042_dbt
                                        WHERE T_ID = GDOC.T_DOCID)
                                WHEN GDOC.T_DOCKIND = RSB_SECUR.REPOS_CM043
                                   THEN
                                      (SELECT T_INTERNALMESSAGEID
                                         FROM dir_cm043_dbt
                                        WHERE T_ID = GDOC.T_DOCID)
                                WHEN GDOC.T_DOCKIND = RSB_SECUR.REPOS_CM044
                                   THEN
                                      (SELECT T_INTERNALMESSAGEID
                                         FROM dir_cm044_dbt
                                        WHERE T_ID = GDOC.T_DOCID)
                                WHEN GDOC.T_DOCKIND = RSB_SECUR.REPOS_CM045
                                   THEN
                                      (SELECT T_INTERNALMESSAGEID
                                         FROM dir_cm045_dbt
                                        WHERE T_ID = GDOC.T_DOCID)
                                WHEN GDOC.T_DOCKIND = RSB_SECUR.REPOS_CM046
                                   THEN
                                      (SELECT T_INTERNALMESSAGEID
                                         FROM dir_cm046_dbt
                                        WHERE T_ID = GDOC.T_DOCID)
                                WHEN GDOC.T_DOCKIND = RSB_SECUR.REPOS_CM047
                                   THEN
                                      (SELECT T_INTERNALMESSAGEID
                                         FROM dir_cm047_dbt
                                        WHERE T_ID = GDOC.T_DOCID)
                                WHEN GDOC.T_DOCKIND = RSB_SECUR.REPOS_CM048
                                   THEN
                                      (SELECT T_INTERNALMESSAGEID
                                         FROM dir_cm048_dbt
                                        WHERE T_ID = GDOC.T_DOCID)
                                WHEN GDOC.T_DOCKIND = RSB_SECUR.REPOS_CM083
                                   THEN
                                      (SELECT T_INTERNALMESSAGEID
                                         FROM dir_cm083_dbt
                                        WHERE T_ID = GDOC.T_DOCID)
                                WHEN GDOC.T_DOCKIND = RSB_SECUR.REPOS_CM094
                                   THEN
                                      (SELECT T_INTERNALMESSAGEID
                                         FROM dir_cm094_dbt
                                        WHERE T_ID = GDOC.T_DOCID)
                                END))
          WHERE rn = 1;
      if(KIND_V = RSB_SECUR.REPOS_CM094)
      THEN
        is094 :=1;
      ELSE
        is094 :=0;
      END IF;

      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
         is094 :=0;

      END;
  RETURN  is094;

  END StatusSRS_Is094;

   FUNCTION Get_GR_ID_SRS (dealID IN NUMBER, kindID IN NUMBER )
      RETURN NUMBER
   IS
      KIND_V NUMBER DEFAULT 0;
      kindID_V   NUMBER;
      GR_ID    NUMBER DEFAULT 0;
  BEGIN
      --Определение kindID
      SELECT (CASE
                 WHEN kindID = 101 THEN 101
                 WHEN kindID = 4815 THEN 4815
                 WHEN kindID = 4812 THEN 4812
                 WHEN kindID = 4813 THEN 4813
                 WHEN kindID = 4626 THEN 4626
                 ELSE 199
              END)
        INTO kindID_V
        FROM DUAL;
      --Определение сводных сообщений
      BEGIN
         SELECT T_ID
           INTO GR_ID
           FROM (SELECT GDEAL.T_ID T_ID,
                        ROW_NUMBER ()
                     OVER (ORDER BY GINF.T_PRDATE DESC, GINF.T_PRTIME DESC)
                           rn
                   FROM ddlgrdeal_dbt GDEAL,
                        ddlgrdoc_dbt GDOC,
                        dir_generalinf_dbt GINF
                  WHERE     GDOC.T_GRDEALID = GDEAL.T_ID
                        AND GDEAL.t_docid = dealID
                        AND GDEAL.T_DOCKIND = kindID_V
                        AND GINF.T_INTERNALMESSAGEID =GDOC.T_docid
                        and GDOC.T_dockind=RSB_SECUR.REPOS_GENERALINF)

                        /*
                               (CASE
                                WHEN GDOC.T_DOCKIND = RSB_SECUR.REPOS_CM023
                                   THEN
                                      (SELECT T_INTERNALMESSAGEID
                                         FROM dir_cm023_dbt
                                        WHERE T_ID = GDOC.T_DOCID)
                                WHEN GDOC.T_DOCKIND = RSB_SECUR.REPOS_CM022
                                   THEN
                                      (SELECT T_INTERNALMESSAGEID
                                         FROM dir_cm022_dbt
                                        WHERE T_ID = GDOC.T_DOCID)
                                WHEN GDOC.T_DOCKIND = RSB_SECUR.REPOS_CM041
                                   THEN
                                      (SELECT T_INTERNALMESSAGEID
                                         FROM dir_cm041_dbt
                                        WHERE T_ID = GDOC.T_DOCID)
                                WHEN GDOC.T_DOCKIND = RSB_SECUR.REPOS_CM083
                                   THEN
                                      (SELECT T_INTERNALMESSAGEID
                                         FROM dir_cm083_dbt
                                        WHERE T_ID = GDOC.T_DOCID)
                                WHEN GDOC.T_DOCKIND = RSB_SECUR.REPOS_CM093
                                   THEN
                                      (SELECT T_INTERNALMESSAGEID
                                         FROM dir_cm093_dbt
                                        WHERE T_ID = GDOC.T_DOCID)
                                END))*/
          WHERE rn = 1;

      END;
  RETURN  GR_ID;

  END Get_GR_ID_SRS;

  PROCEDURE SC_ConvSumTypeRepProc( SumB      IN NUMBER
                                  ,pFromFI   IN NUMBER
                                  ,pToFI     IN NUMBER
                                  ,pToVR     IN NUMBER
                                  ,pType     IN NUMBER
                                  ,pbdate    IN DATE
                                  ,pRate     OUT NUMBER
                                  ,pRateID   OUT NUMBER
                                  ,pRateDate OUT DATE
                                  ,pMarketId IN NUMBER
                                 )
  AS
    t_RD       DRATEDEF_DBT%ROWTYPE;
    v_Rate     NUMBER;
    v_RateID   NUMBER;
    v_RateDate DATE;
    v_ToFI     NUMBER := pToFI;
  BEGIN

    v_Rate := RSI_RSB_FIInstr.FI_GetRate( pFromFI, pToFI, pType, pbdate, pbdate-to_date('01.01.0001','DD.MM.YYYY'), 0, v_RateID, v_RateDate, false, null, 0, 0, 0, pMarketId  );
    if( v_Rate <= 0 ) then
       v_Rate := RSI_RSB_FIInstr.FI_GetRate( pFromFI, -1, pType, pbdate, pbdate-to_date('01.01.0001','DD.MM.YYYY'), 0, v_RateID, v_RateDate, false, null, 0, 0, 0, pMarketId  );
       begin
         select * into t_RD
           from dratedef_dbt
          where t_RateID = v_RateID;
       exception
         when OTHERS then v_Rate := 0;
       end;

       if(t_RD.t_OtherFI = pFromFI)then
          v_ToFI := t_RD.t_FIID;
       else
          v_ToFI := t_RD.t_OtherFI;
       end if;
    end if;

    if( v_Rate > 0 ) then
       v_Rate := SumB*v_Rate;
       v_Rate := RSB_FIInstr.ConvSum(v_Rate, v_ToFI, pToVR, pbdate);
    end if;

    pRate := v_Rate;
    pRateID := v_RateID;
    pRateDate := v_RateDate;

  END;

  -- Конвертация сумм  (используется в отчете)
  FUNCTION SC_ConvSumTypeRep( SumB     IN NUMBER
                             ,pFromFI IN NUMBER
                             ,pToFI    IN NUMBER
                             ,pToVR   IN NUMBER
                             ,pType    IN NUMBER
                             ,pbdate   IN DATE
                             ,pMarketId IN NUMBER
                            )
    RETURN NUMBER
  IS
    t_RD       DRATEDEF_DBT%ROWTYPE;
    v_Rate     NUMBER;
    v_RateID   NUMBER;
    v_RateDate DATE;
    v_ToFI     NUMBER := pToFI;
  BEGIN

    SC_ConvSumTypeRepProc( SumB      
                          ,pFromFI   
                          ,pToFI     
                          ,pToVR     
                          ,pType     
                          ,pbdate    
                          ,v_Rate     
                          ,v_RateID   
                          ,v_RateDate 
                          ,pMarketId
                         );
    return v_Rate;
  EXCEPTION
    when OTHERS then return 0.0;
  END;

  --Получить ставку налога для покупателя/продавца по 1 части РЕПО при выплате купона
  FUNCTION GetCoupREPO_TaxRate(p_PartyID IN NUMBER, p_FIID IN NUMBER, p_OnDate IN DATE) RETURN ddlrq_dbt.t_TaxRateBuy%type
  IS
    v_legalform   dparty_dbt.t_LegalForm % TYPE;
    v_isresident  NUMBER;

    v_Rate        ddlrq_dbt.t_TaxRateBuy%type;
    v_TaxGroup   NUMBER;

    v_BegPlacementDate DATE;
    v_FaceValueFI      NUMBER;
    v_AvoirKind        NUMBER;
  BEGIN

    SELECT party.t_LegalForm INTO v_legalform
      FROM DPARTY_DBT party
     WHERE party.t_PartyID = p_PartyID;

    SELECT iss.t_BegPlacementDate, fin.t_FaceValueFI, fin.t_AvoirKind INTO v_BegPlacementDate, v_FaceValueFI, v_AvoirKind
      FROM DAVOIRISS_DBT iss, DFININSTR_DBT fin
     WHERE iss.t_FIID = p_FIID
       AND fin.t_FIID = iss.t_FIID;

    v_isresident := IsTaxResident(p_PartyID, p_OnDate);

    v_Rate := 0;
    IF v_legalform = 1 THEN --Юрлицо
      IF v_isresident = 0 THEN

        v_TaxGroup := RSI_NUTX.GetNUTaxGroup(p_FIID, p_OnDate);

        IF v_TaxGroup = RSI_NUTX.NUTAXGROUP_20 OR v_TaxGroup = RSI_NUTX.NUTAXGROUP_15 THEN
          v_Rate := RSI_NUTX.GetNUTaxRate(RSI_NUTX.INCOME_KIND_PERC, p_OnDate, p_PartyID);
        END IF;

      END IF;

    ELSIF v_legalform = 2 THEN --Физлицо
      IF v_isresident = 1 THEN
        v_Rate := 13;

        v_TaxGroup := NPTO.GetPaperTaxGroupNPTX(p_FIID);

        IF v_TaxGroup = RSI_NPTXC.TXGROUP_40 OR v_TaxGroup = RSI_NPTXC.TXGROUP_50 THEN
          v_Rate := 0;
        /*ELSIF v_TaxGroup = RSI_NPTXC.TXGROUP_20 AND
              v_BegPlacementDate < TO_DATE('01.01.2017','DD.MM.YYYY') AND
              v_FaceValueFI = RSI_RSB_FIInstr.NATCUR AND
              RSI_rsb_fiinstr.FI_AvrKindsEQ(RSI_RSB_FIInstr.FIKIND_AVOIRISS, RSI_RSB_FIInstr.AVOIRKIND_BOND_CORPORATE, v_AvoirKind) = 1 AND
              NPTO.IfMarket(p_FIID, p_OnDate) = 'X'
              THEN*/
        ELSIF v_TaxGroup = RSI_NPTXC.TXGROUP_30 AND v_BegPlacementDate < TO_DATE('01.01.2017','DD.MM.YYYY') THEN
          v_Rate := 9;
        ELSIF v_TaxGroup = RSI_NPTXC.TXGROUP_20 AND NPTO.IsCorpBondAfter2018byDrawDate(p_FIID, p_OnDate) = 1 THEN
          v_Rate := 35;
        END IF;

      ELSE
        v_Rate := 30;
         begin
            select   rsb_struct.getDouble(note.t_Text ) into v_Rate
                from DNOTETEXT_DBT note
                where note.t_ObjectType = 3 /*OBJTYPE_PARTY*/
                      and note.t_NoteKind = 77 /*PARTY_NOTE_KIND_NPTX_RATE_NOTRES  Налоговая ставка для физическоголица-нерезидента по общим доходам*/
                      and note.t_DocumentID = LPAD( p_PartyID, 10, '0' )
                      and note.t_ValidToDate >= p_OnDate
                      and note.t_Date <= p_OnDate;

         exception
            when NO_DATA_FOUND then v_Rate := 30;
         end;

      END IF;

    END IF;

    RETURN v_Rate;

  END GetCoupREPO_TaxRate;

  --Получение стоимости бумаг по курсу для 712 формы
  FUNCTION SC_GetCostByCourse712(Amount IN NUMBER,
                                 pFIID IN NUMBER,
                                 pbdate IN DATE,
                                 pFI_Kind IN NUMBER,
                                 pAvoirKind IN NUMBER,
                                 pMarketId IN NUMBER) RETURN NUMBER
  IS
    v_Rate          NUMBER := 0.0;
    v_RateID        NUMBER := 0;
    v_RateDate      DATE;
    v_RateID_Med    NUMBER := 0;
    v_Rate_Med      NUMBER := 0.0;
    v_RateDate_Med  DATE;
    v_RateID_Cls    NUMBER := 0;
    v_RateDate_Cls  DATE;
    v_Rate_Cls      NUMBER := 0.0;
    v_FromFI        NUMBER;
    v_Type_Med      NUMBER := Rsb_Common.GetRegIntValue('SECUR\ВИД КУРСА СРЕДНЕВЗВЕШЕННАЯ ЦЕНА', 0);
    v_Type_Cls      NUMBER := Rsb_Common.GetRegIntValue('SECUR\ВИД КУРСА ЦЕНА ЗАКРЫТИЯ', 0);
    v_Cost          NUMBER := 0.0;
    v_Type_NKD      NUMBER := Rsb_Common.GetRegIntValue('SECUR\ВИД КУРСА НКД ДЛЯ ЦБ', 0);
    v_Rate_NKD      NUMBER := 0.0;
    v_RateID_NKD    NUMBER := 0.0;
    v_RateDate_NKD  DATE;
    /*USER*/
    v_Type_Mrkt      NUMBER := Rsb_Common.GetRegIntValue('SECUR\ВИД КУРСА РЫНОЧНАЯ ЦЕНА', 0);
    v_RateID_Mrkt    NUMBER := 0;
    v_RateDate_Mrkt  DATE;
    v_Rate_Mrkt     NUMBER := 0.0;
    v_Type_707      NUMBER := 1002;  /*Вид курса "Цена закрытия Bloomberg для ф. 707".Новое требование по котировкам. iSupport: 517026*/
    v_RateID_707    NUMBER := 0;
    v_RateDate_707  DATE;
    v_Rate_707     NUMBER := 0.0;    
  BEGIN

    v_Rate_Med := RSI_RSB_FIInstr.FI_GetRate( pFIID, -1, v_Type_Med, pbdate, pbdate - ADD_MONTHS(pbdate, -3), 0, v_RateID_Med, v_RateDate_Med, false, null, 0, 0, 0, pMarketId );
    v_Rate_Cls := RSI_RSB_FIInstr.FI_GetRate( pFIID, -1, v_Type_Cls, pbdate, pbdate - ADD_MONTHS(pbdate, -3), 0, v_RateID_Cls, v_RateDate_Cls, false, null, 0, 0, 0, pMarketId );
    v_Rate_Mrkt := RSI_RSB_FIInstr.FI_GetRate( pFIID, -1, v_Type_Mrkt, pbdate, pbdate - ADD_MONTHS(pbdate,-3), 0, v_RateID_Mrkt, v_RateDate_Mrkt, false, null, 0, 0, 0, pMarketId );
    v_Rate_707 := RSI_RSB_FIInstr.FI_GetRate( pFIID, -1, v_Type_707, pbdate, pbdate - ADD_MONTHS(pbdate,-3), 0, v_RateID_707, v_RateDate_707 );  /*iSupport: 517026*/

    if( v_RateID_Med > 0 or v_RateID_Cls > 0 or v_RateID_Mrkt > 0 or v_RateID_707 > 0)then

/*USER*/
       if ( v_RateID_Mrkt > 0 )then
          v_RateID := v_RateID_Mrkt;
          v_Rate := v_Rate_Mrkt;
          v_RateDate := v_RateDate_Mrkt;
       elsif ( v_RateID_Med > 0 )then
          v_RateID   := v_RateID_Med;
          v_Rate     := v_Rate_Med;
          v_RateDate := v_RateDate_Med;
       elsif ( v_RateID_Cls > 0 )then
          v_RateID   := v_RateID_Cls;
          v_Rate     := v_Rate_Cls;
          v_RateDate := v_RateDate_Cls;
       else 
          v_RateID := v_RateID_707;  /*iSupport: 517026*/
          v_Rate := v_Rate_707;
          v_RateDate := v_RateDate_707;                 
       end if;

       select case when t_OtherFI = pFIID then t_FIID else t_OtherFI end
         into v_FromFI
         from dratedef_dbt
        where t_RateID = v_RateID;

       IF (RSI_RSB_FIInstr.FI_AvrKindsGetRoot(pFI_Kind, pAvoirKind) = RSI_RSB_FIInstr.AVOIRKIND_BOND) THEN
          v_Rate_NKD := RSI_RSB_FIInstr.FI_GetRate(pFIID, -1, v_Type_NKD, pbdate, pbdate - ADD_MONTHS(pbdate, -3), 0, v_RateID_NKD, v_RateDate_NKD);
       END IF;

       v_Cost := Amount * (v_Rate + v_Rate_NKD);
       v_Cost := RSB_FIInstr.ConvSum(v_Cost, v_FromFI, RSI_RSB_FIInstr.NATCUR, pbdate);
    end if;

    return v_Cost;
  EXCEPTION
    when OTHERS then return 0.0;
  END SC_GetCostByCourse712;

  FUNCTION SC_HasNominalOnRateDate712(pFIID IN NUMBER,
                                      pDate IN DATE) RETURN NUMBER
  IS
     v_RateDate     DATE   := NULL;
     v_HasNominal   NUMBER := 0;
     v_HasIndexNom  CHAR   := CNST.UNSET_CHAR;

     v_Rate_Med     NUMBER := 0.0;
     v_Type_Med     NUMBER := Rsb_Common.GetRegIntValue('SECUR\ВИД КУРСА СРЕДНЕВЗВЕШЕННАЯ ЦЕНА', 0);
     v_RateID_Med   NUMBER := 0;
     v_RateDate_Med DATE   := NULL;

     v_Rate_Cls     NUMBER := 0.0;
     v_Type_Cls     NUMBER := Rsb_Common.GetRegIntValue('SECUR\ВИД КУРСА ЦЕНА ЗАКРЫТИЯ', 0);
     v_RateID_Cls   NUMBER := 0;
     v_RateDate_Cls DATE   := NULL;
  BEGIN
     SELECT avr.t_IndexNom
       INTO v_HasIndexNom
       FROM davoiriss_dbt avr
      WHERE avr.t_FIID = pFIID;

     IF (v_HasIndexNom <> CNST.SET_CHAR) THEN
        v_HasNominal := 1;
     ELSE
        v_Rate_Med := RSI_RSB_FIInstr.FI_GetRate(pFIID, -1, v_Type_Med, pDate, pDate - ADD_MONTHS(pDate, -3), 0, v_RateID_Med, v_RateDate_Med);
        v_Rate_Cls := RSI_RSB_FIInstr.FI_GetRate(pFIID, -1, v_Type_Cls, pDate, pDate - ADD_MONTHS(pDate, -3), 0, v_RateID_Cls, v_RateDate_Cls);

        IF (v_RateID_Med > 0) THEN
           v_RateDate := v_RateDate_Med;
        ELSIF (v_RateID_Cls > 0) THEN
           v_RateDate := v_RateDate_Cls;
        END IF;

        SELECT 1
          INTO v_HasNominal
          FROM DV_FI_FACEVALUE_HIST fv
         WHERE     fv.t_FIID    = pFIID
               AND fv.t_BegDate = v_RateDate;  
     END IF;

     RETURN v_HasNominal;
  EXCEPTION
     WHEN OTHERS THEN RETURN 0;
  END SC_HasNominalOnRateDate712;

  FUNCTION SC_GetNominalDate712(pFIID IN NUMBER,
                                pDate IN DATE) RETURN DATE
  IS
     v_NominalDate  DATE   := NULL;
     v_RateDate     DATE   := NULL;
     v_HasIndexNom  CHAR   := CNST.UNSET_CHAR;

     v_Rate_Med     NUMBER := 0.0;
     v_Type_Med     NUMBER := Rsb_Common.GetRegIntValue('SECUR\ВИД КУРСА СРЕДНЕВЗВЕШЕННАЯ ЦЕНА', 0);
     v_RateID_Med   NUMBER := 0;
     v_RateDate_Med DATE;

     v_Rate_Cls     NUMBER := 0.0;
     v_Type_Cls     NUMBER := Rsb_Common.GetRegIntValue('SECUR\ВИД КУРСА ЦЕНА ЗАКРЫТИЯ', 0);
     v_RateID_Cls   NUMBER := 0;
     v_RateDate_Cls DATE;
  BEGIN
     SELECT avr.t_IndexNom
       INTO v_HasIndexNom
       FROM davoiriss_dbt avr
      WHERE avr.t_FIID = pFIID;

     IF (v_HasIndexNom = CNST.SET_CHAR) THEN
        v_Rate_Med := RSI_RSB_FIInstr.FI_GetRate(pFIID, -1, v_Type_Med, pDate, pDate - ADD_MONTHS(pDate, -3), 0, v_RateID_Med, v_RateDate_Med);
        v_Rate_Cls := RSI_RSB_FIInstr.FI_GetRate(pFIID, -1, v_Type_Cls, pDate, pDate - ADD_MONTHS(pDate, -3), 0, v_RateID_Cls, v_RateDate_Cls);

        IF (v_RateID_Med > 0) THEN
           v_RateDate := v_RateDate_Med;
        ELSIF (v_RateID_Cls > 0) THEN
           v_RateDate := v_RateDate_Cls;
        END IF;

        SELECT fv.t_BegDate
          INTO v_NominalDate
          FROM DV_FI_FACEVALUE_HIST fv
         WHERE     fv.t_FIID     = pFIID
               AND fv.t_BegDate <= v_RateDate;
     END IF;

     RETURN v_NominalDate;
  EXCEPTION
     WHEN OTHERS THEN RETURN NULL;
  END SC_GetNominalDate712;

  --Получить количество ц/б в выписки на дату для строки регистра КДУ
  FUNCTION GetQRRestOnDate( p_QRID   IN NUMBER
                           ,p_OnDate IN DATE
                          )
    RETURN NUMBER
  IS
    v_Rest          NUMBER := 0;
  BEGIN
    SELECT T_REST INTO v_Rest FROM DSCQRREST_DBT rest WHERE rest.T_QRID = p_QRID AND rest.T_DATE = p_OnDate and rownum = 1 ORDER BY rest.T_DATE DESC; /*USER*/
    return v_Rest;
  EXCEPTION
    when OTHERS then return 0;
  END GetQRRestOnDate;


  --Получить количество ц/б в обеспечении на дату
  FUNCTION GetQntySecPledgedOnDate(p_SecAgrID IN NUMBER
                                  ,p_FIID IN NUMBER
                                  ,p_Department IN NUMBER
                                  ,p_OnDate IN DATE
                                  )
    RETURN NUMBER
  IS
    v_SumSec NUMBER := 0;
  BEGIN

    SELECT NVL(sum(T_HIDDEN_SUM * decode(DDL_COMM_DBT.T_OPERSUBKIND, 2, -1, 1)), 0 )
    INTO v_SumSec
    FROM DDL_COMM_DBT
    WHERE DDL_COMM_DBT.T_DOCKIND = SP_TRNCOLLAT /*Перевод обеспечения*/
      AND DDL_COMM_DBT.T_CONTRACTID = p_SecAgrID
      AND DDL_COMM_DBT.T_FIID = p_FIID
      AND DDL_COMM_DBT.T_DIVISION = p_Department
      AND DDL_COMM_DBT.T_COMMDATE <= p_OnDate
      AND DDL_COMM_DBT.T_COMMSTATUS = 2; /*Закрыта*/

    RETURN v_SumSec;

  EXCEPTION
    when OTHERS then return 0;
  END GetQntySecPledgedOnDate;

  --Получить количество заблокированных ц/б на дату
  FUNCTION GetQntySecBlockedOnDate(p_SecAgrID IN NUMBER
                                  ,p_FIID IN NUMBER
                                  ,p_Department IN NUMBER
                                  ,p_OnDate IN DATE
                                  )
    RETURN NUMBER
  IS
    v_SumSec NUMBER := 0;
  BEGIN

    SELECT NVL(sum(DSCBLCKLNK_DBT.T_AMOUNT * decode(DDL_COMM_DBT.T_OPERSUBKIND, 2, -1, 1)), 0 )
    INTO v_SumSec
    FROM DDL_COMM_DBT, DSCBLCKLNK_DBT
    WHERE DDL_COMM_DBT.T_DOCKIND = SP_BLOCKBATCH /*Блокировка партий*/
      AND DDL_COMM_DBT.T_CONTRACTID = p_SecAgrID
      AND DDL_COMM_DBT.T_FIID = p_FIID
      AND DDL_COMM_DBT.T_DIVISION = p_Department
      AND DDL_COMM_DBT.T_COMMDATE <= p_OnDate
      AND DDL_COMM_DBT.T_COMMSTATUS = 2 /*Закрыта*/
      AND DSCBLCKLNK_DBT.T_DOCKIND = DDL_COMM_DBT.T_DOCKIND
      AND DSCBLCKLNK_DBT.T_DOCID = DDL_COMM_DBT.T_DOCUMENTID;

    RETURN v_SumSec;

  EXCEPTION
    when OTHERS then return 0;
  END GetQntySecBlockedOnDate;

  --Получить количество заблокированных ц/б на дату по сделке
  FUNCTION GetQntySecBlockedOnDateByDeal(p_SecAgrID IN NUMBER
                                        ,p_FIID IN NUMBER
                                        ,p_DealID IN NUMBER
                                        ,p_OnDate IN DATE
                                        )
    RETURN NUMBER
  IS
    v_SumSec NUMBER := 0;
  BEGIN

    SELECT NVL(sum(DSCBLCKLNK_DBT.T_AMOUNT * decode(DDL_COMM_DBT.T_OPERSUBKIND, 2, -1, 1)), 0 )
    INTO v_SumSec
    FROM DDL_COMM_DBT, DSCBLCKLNK_DBT
    WHERE DDL_COMM_DBT.T_DOCKIND = SP_BLOCKBATCH /*Блокировка партий*/
      AND DDL_COMM_DBT.T_CONTRACTID = p_SecAgrID
      AND DDL_COMM_DBT.T_FIID = p_FIID
      AND DDL_COMM_DBT.T_COMMDATE <= p_OnDate
      AND DDL_COMM_DBT.T_COMMSTATUS = 2 /*Закрыта*/
      AND DSCBLCKLNK_DBT.T_DOCKIND = DDL_COMM_DBT.T_DOCKIND
      AND DSCBLCKLNK_DBT.T_DOCID = DDL_COMM_DBT.T_DOCUMENTID
      AND DSCBLCKLNK_DBT.T_DEALID = p_DealID;

    RETURN v_SumSec;

  EXCEPTION
    when OTHERS then return 0;
  END GetQntySecBlockedOnDateByDeal;


  FUNCTION GetDealObjType(p_BOfficeKind IN NUMBER) RETURN NUMBER
  IS
    v_ObjType NUMBER := 0;
  BEGIN

    IF p_BOfficeKind = DL_SECURITYDOC OR p_BOfficeKind = DL_AVRWRT THEN
      v_ObjType := OBJTYPE_SECDEAL;
    ELSIF p_BOfficeKind = DL_RETIREMENT THEN
      v_ObjType := OBJTYPE_RETIRE;
    ELSIF p_BOfficeKind = DL_SECUROWN OR p_BOfficeKind = DL_AVRWRTOWN THEN
      v_ObjType := OBJTYPE_SECUROWN;
    ELSIF p_BOfficeKind = DL_RETIREMENT_OWN THEN
      v_ObjType := OBJTYPE_RETIREOWN;
    END IF;

    RETURN v_ObjType;
  END GetDealObjType;

  FUNCTION GetDealMarketTestAttrID(p_BOfficeKind IN NUMBER, p_DealID IN NUMBER) RETURN NUMBER
  IS
  BEGIN
    return GetMainObjAttrNoDate(GetDealObjType(p_BOfficeKind), LPAD(p_DealID, 34, '0'), 47 /*Тест на рыночность пройден*/);
  END GetDealMarketTestAttrID;

  --Установить значение категории "Тест на рыночность" для сделки
  PROCEDURE SetDealMarketTestAttrID(p_DealID IN NUMBER, p_OnDate IN DATE, p_AttrID IN NUMBER)
  IS
    v_tick DDL_TICK_DBT%ROWTYPE;

    v_ObjType   NUMBER := 0;
    v_DealObjID DOBJATCOR_DBT.T_OBJECT%TYPE;
  BEGIN

    SELECT * INTO v_tick
      FROM ddl_tick_dbt
     WHERE t_DealID = p_DealID;

    v_ObjType   := GetDealObjType(v_tick.t_BOfficeKind);
    v_DealObjID := LPAD(v_tick.t_DealID, 34, '0');

    IF GetMainObjAttrNoDate(GetDealObjType(v_tick.t_BOfficeKind), v_DealObjID, 47 /*Тест на рыночность пройден*/) != 0 THEN
      IF p_AttrID > 0 THEN

        UPDATE DOBJATCOR_DBT
          SET T_ATTRID = p_AttrID
        WHERE T_OBJECTTYPE = v_ObjType
          AND T_OBJECT = v_DealObjID
          AND T_GROUPID = 47;

      ELSE

        --Значения не было, поэтому удаляем
        DELETE DOBJATCOR_DBT
         WHERE T_OBJECTTYPE = v_ObjType
           AND T_OBJECT = v_DealObjID
           AND T_GROUPID = 47;
      END IF;

    ELSIF p_AttrID > 0 THEN

      INSERT INTO DOBJATCOR_DBT ( T_OBJECTTYPE,
                                  T_GROUPID,
                                  T_ATTRID,
                                  T_OBJECT,
                                  T_GENERAL,
                                  T_VALIDFROMDATE,
                                  T_OPER,
                                  T_VALIDTODATE,
                                  T_SYSDATE,
                                  T_SYSTIME,
                                  T_ISAUTO,
                                  T_ID
                                )
                         VALUES ( v_ObjType,                           --T_OBJECTTYPE
                                  47, /*Тест на рыночность пройден*/   --T_GROUPID
                                  p_AttrID,                            --T_ATTRID
                                  v_DealObjID,                         --T_OBJECT
                                  'X',                                 --T_GENERAL
                                  p_OnDate,                            --T_VALIDFROMDATE
                                  RsbSessionData.Oper,                 --T_OPER
                                  TO_DATE('31-12-9999', 'DD-MM-YYYY'), --T_VALIDTODATE
                                  TRUNC(SYSDATE),                      --T_SYSDATE
                                  TO_DATE('01-01-0001:' || TO_CHAR(SYSDATE, 'HH24:MI:SS'), 'DD-MM-YYYY:HH24:MI:SS'),--T_SYSTIME
                                  'X',                                 --T_ISAUTO
                                  0                                    --T_ID
                                );

    END IF;

  END SetDealMarketTestAttrID;

  --Установить значение категории для ФИ
  PROCEDURE SetFICategoryAttrID(pGroupID IN NUMBER, pFIID IN NUMBER, pOnDate IN DATE, pAttrID IN NUMBER)
  IS
    v_FIObjID DOBJATCOR_DBT.T_OBJECT%TYPE;
  BEGIN
    v_FIObjID := LPAD(pFIID, 10, '0');

    IF GetMainObjAttrNoDate(OBJTYPE_AVOIRISS, v_FIObjID, pGroupID) != 0 THEN
      IF pAttrID > 0 THEN

        UPDATE DOBJATCOR_DBT
          SET T_ATTRID = pAttrID
        WHERE T_OBJECTTYPE = OBJTYPE_AVOIRISS
          AND T_OBJECT = v_FIObjID
          AND T_GROUPID = pGroupID;
      ELSE
        --Значения не было, поэтому удаляем
        DELETE DOBJATCOR_DBT
         WHERE T_OBJECTTYPE = OBJTYPE_AVOIRISS
           AND T_OBJECT = v_FIObjID
           AND T_GROUPID = pGroupID;
      END IF;

    ELSIF pAttrID > 0 THEN

      INSERT INTO DOBJATCOR_DBT ( T_OBJECTTYPE,
                                  T_GROUPID,
                                  T_ATTRID,
                                  T_OBJECT,
                                  T_GENERAL,
                                  T_VALIDFROMDATE,
                                  T_OPER,
                                  T_VALIDTODATE,
                                  T_SYSDATE,
                                  T_SYSTIME,
                                  T_ISAUTO,
                                  T_ID
                                )
                         VALUES ( OBJTYPE_AVOIRISS,                    --T_OBJECTTYPE
                                  pGroupID,                            --T_GROUPID
                                  pAttrID,                             --T_ATTRID
                                  v_FIObjID,                           --T_OBJECT
                                  'X',                                 --T_GENERAL
                                  pOnDate,                             --T_VALIDFROMDATE
                                  RsbSessionData.Oper,                 --T_OPER
                                  TO_DATE('31-12-9999', 'DD-MM-YYYY'), --T_VALIDTODATE
                                  TRUNC(SYSDATE),                      --T_SYSDATE
                                  TO_DATE('01-01-0001:' || TO_CHAR(SYSDATE, 'HH24:MI:SS'), 'DD-MM-YYYY:HH24:MI:SS'),--T_SYSTIME
                                  'X',                                 --T_ISAUTO
                                  0                                    --T_ID
                                );

    END IF;

  END SetFICategoryAttrID;

  PROCEDURE AddInArrTmp( p_date IN DATE, p_sum IN NUMBER, p_arrtmp IN OUT NOCOPY arrtmp_t )
  AS
    v_xirr DXIRR_TMP%rowtype;
  BEGIN
    IF p_date != UnknownDate THEN
      v_xirr        := NULL;
      v_xirr.t_Date := p_date;
      v_xirr.t_Sum  := p_sum;

      p_arrtmp.extend();
      p_arrtmp( p_arrtmp.last ) := v_xirr;
    END IF;
  END AddInArrTmp;

  PROCEDURE InsertArrToXIRR( p_arrtmp IN OUT NOCOPY arrtmp_t )
  AS
  BEGIN
    IF p_arrtmp IS NOT EMPTY THEN
      FORALL i IN p_arrtmp.FIRST .. p_arrtmp.LAST
           INSERT INTO DXIRR_TMP VALUES p_arrtmp (i);
      p_arrtmp.delete;
    END IF;
  END InsertArrToXIRR;

  --Заполнить массив для расчета данныых по купонам/чп на дату
  PROCEDURE AddFiWarntsInArrTmp(p_arrtmp IN OUT NOCOPY arrtmp_t,
                                p_FIID IN NUMBER,
                                p_BegDate IN DATE,
                                p_EndDate IN DATE,
                                p_Amount IN NUMBER,
                                p_AmountRate IN NUMBER DEFAULT 1,
                                p_OnlyDateRange IN BOOLEAN DEFAULT FALSE,
                                p_avance IN ddlrq_dbt%rowtype DEFAULT NULL,
                                p_IsOffer IN NUMBER DEFAULT 0)
  IS
    v_FaceValue     NUMBER  := 0;
    v_FVFI          NUMBER  := -1;
    v_DrawingDate   DATE    := UnknownDate;
    v_HasPart       BOOLEAN := FALSE;
    v_AddAvance     BOOLEAN := FALSE;
    v_Termless      DAVOIRISS_DBT.T_TERMLESS%TYPE;
  BEGIN

    SELECT FI.t_FaceValue, FI.t_FaceValueFI, RSI_RSB_FIInstr.FI_GetNominalDrawingDate(FI.t_FIID, AV.t_Termless), AV.t_Termless
      INTO v_FaceValue, v_FVFI, v_DrawingDate, v_Termless
      FROM DFININSTR_DBT FI, DAVOIRISS_DBT AV
     WHERE FI.t_FIID = p_FIID
       AND AV.t_FIID = FI.t_FIID;

    -- di - Дата купона или ч/п.
    -- Si - сумма купона или ч/п.
    -- dn - Дата последнего денежного потока =  Дате полного погашения из анкеты ц/б.
    -- Sn - Сумма последнего денежного потока = Сумма последнего частичного погашения для облигаций с амортизацией долга и номинал для остальных.
    FOR one_cm IN (SELECT (case when fw.t_DrawingDate > p_EndDate then p_EndDate else fw.t_DrawingDate end) t_DrawingDate,
                          CASE WHEN fw.t_IsPartial = CHR(0) THEN
                                 ROUND(RSI_RSB_FIInstr.FI_CalcIncomeValue(fw.t_FIID,
                                                                          (case when fw.t_DrawingDate > p_EndDate then p_EndDate else fw.t_DrawingDate end),
                                                                          p_Amount, 1, 0, 0, 1), fw.t_IncomePoint)
                               ELSE
                                 p_Amount * (CASE WHEN fw.t_RelativeIncome = CHR(0) THEN
                                               fw.t_IncomeVolume
                                             ELSE
                                               ROUND(v_FaceValue * fw.t_IncomeRate / GREATEST(1, fw.t_IncomeScale) / 100, fw.t_IncomePoint)
                                             END)
                               END t_Sum,
                          CASE WHEN EXISTS(SELECT 1 from DFIWARNTS_DBT WHERE t_FIID = fw.t_FIID AND t_IsPartial = CHR(88)) THEN
                                 1 ELSE 0
                               END t_HasPart
                     FROM DFIWARNTS_DBT fw
                    WHERE fw.t_FIID   = p_FIID
                      AND (fw.t_DrawingDate BETWEEN p_BegDate AND p_EndDate
                          or (p_IsOffer = 1 and fw.t_DrawingDate > p_EndDate and fw.t_FirstDate BETWEEN p_BegDate AND p_EndDate)
                          )
                      AND (CASE WHEN v_Termless = 'X' THEN
                                  (CASE WHEN fw.t_IsPartial = CHR(0) and (fw.t_IncomeRate > 0 or fw.t_IncomeVolume > 0) THEN 1 ELSE 0 END)
                                ELSE 1
                           END) = 1
                    ORDER BY fw.t_DrawingDate
                  )
    LOOP
      IF one_cm.t_HasPart = 1 THEN
        v_HasPart := TRUE;
      END IF;
      -- нужно для учета аванса 2ч в АС лин.м.
      IF p_avance.t_ID > 0 and v_AddAvance = false THEN
        IF p_avance.t_PlanDate <= one_cm.t_DrawingDate THEN
          AddInArrTmp( p_avance.t_PlanDate, p_avance.t_Amount * p_AmountRate, p_arrtmp );
          v_AddAvance := true;
        END IF;
      END IF;

      IF one_cm.t_Sum <> 0 THEN
        AddInArrTmp( one_cm.t_DrawingDate, one_cm.t_Sum, p_arrtmp );
      END IF;
    END LOOP;
    -- нужно для учета аванса 2ч в АС лин.м.
    IF p_avance.t_ID > 0 and v_AddAvance = false THEN
      AddInArrTmp( p_avance.t_PlanDate, p_avance.t_Amount * p_AmountRate, p_arrtmp );
    END IF;

    IF v_HasPart = FALSE and p_OnlyDateRange = FALSE THEN
      IF p_IsOffer = 1 THEN
        v_DrawingDate := p_EndDate; -- дате оферты
      END IF;
      v_FaceValue := RSI_RSB_FIInstr.FI_GetNominalOnDate(p_FIID, v_DrawingDate);

      AddInArrTmp( v_DrawingDate, v_FaceValue * p_Amount, p_arrtmp );
    END IF;

    EXCEPTION
      WHEN NO_DATA_FOUND THEN NULL;
  END AddFiWarntsInArrTmp;

  -- Комиссии, признанные как существенные.
  PROCEDURE AddComisInArrTmp( p_arrtmp IN OUT NOCOPY arrtmp_t,
                              p_CalcKind IN NUMBER,
                              p_tick IN ddl_tick_dbt%rowtype,
                              p_leg IN ddl_leg_dbt%rowtype,
                              p_AmountLot IN NUMBER,
                              p_TopCalcDate IN DATE )
  AS
    v_SignDeviation BOOLEAN := false;
    v_RateKind      NUMBER;
    v_RateVal       NUMBER;
    v_FaceFI        NUMBER;
    v_DoCompare     NUMBER  := 0;
    v_ToCompare     NUMBER  := 0;
    v_OGroup        NUMBER  := get_OperationGroup(get_OperSysTypes(p_tick.t_DealType, p_tick.t_BofficeKind));
    v_Portfolio     NUMBER  := case when IsREPO(v_OGroup)=1 then (case when p_CalcKind = CALCKIND_OREPO then RSB_PMWRTOFF.KINDPORT_CURR_AC_PVO 
                                                                       else RSB_PMWRTOFF.KINDPORT_CURR_AC_BPP 
                                                                  end)
                                    when p_CalcKind = CALCKIND_OWN then RSB_PMWRTOFF.KINDPORT_AC_OWN 
                                    else p_tick.t_PortfolioID 
                               end;
  BEGIN

    IF IsREPO(v_OGroup)=0 THEN
       SELECT t_FaceValueFI INTO v_FaceFI
         FROM DFININSTR_DBT
        WHERE T_FIID = p_tick.t_PFI;
    END IF;

    FOR one_cm IN (SELECT dlcomis.t_Sum - dlcomis.t_NDS ComSum, comis.t_FIID_Comm Currency, GREATEST(dlcomis.t_PlanPayDate, dlcomis.t_FactPayDate) PayDate
                     FROM ddlcomis_dbt dlcomis, dsfcomiss_dbt comis
                    WHERE dlcomis.t_DocKind   = p_tick.t_BofficeKind
                      AND dlcomis.t_DocID     = p_tick.t_DealID
                      AND dlcomis.t_FeeType   = comis.t_FeeType
                      AND dlcomis.t_ComNumber = comis.t_Number
                      AND GREATEST(dlcomis.t_PlanPayDate, dlcomis.t_FactPayDate) <= p_TopCalcDate
                  )
    LOOP
       v_DoCompare := RSI_RSB_FIInstr.ConvSum( one_cm.ComSum, one_cm.Currency, RSI_RSB_FIInstr.NATCUR, one_cm.PayDate, 1 );
       v_ToCompare := RSI_RSB_FIInstr.ConvSum( p_leg.t_Cost, p_leg.t_CFI, RSI_RSB_FIInstr.NATCUR, one_cm.PayDate, 1 );

       IF GetEssentialDev(LEVELESSENTIAL_CONTRACTCOSTS, --Затраты по договору
                          v_Portfolio,
                          one_cm.PayDate,
                          v_DoCompare,
                          v_ToCompare,
                          0,
                          0,
                          0,
                          v_SignDeviation, -- Да/нет
                          v_RateKind,
                          v_RateVal
                         ) <> 0 THEN
          dbms_output.put_line('Ошибка: неверные параметры при вызове ф-ции проверки на существенность отклонения ');
       END IF;

       IF v_SignDeviation = TRUE THEN
          IF IsREPO(v_OGroup)=0 OR p_CalcKind = CALCKIND_OWN THEN
             v_DoCompare := RSI_RSB_FIInstr.ConvSum( one_cm.ComSum, one_cm.Currency, v_FaceFI, one_cm.PayDate, 1 );
             IF p_CalcKind = CALCKIND_OWN THEN
                IF p_AmountLot > 0 THEN
                   v_DoCompare := v_DoCompare / p_leg.t_Principal * p_AmountLot;
                END IF;
             ELSE
                v_DoCompare := -v_DoCompare / p_leg.t_Principal;
             END IF;
          ELSE
             v_DoCompare := RSI_RSB_FIInstr.ConvSum( one_cm.ComSum, one_cm.Currency, p_leg.t_CFI, one_cm.PayDate, 1 );
             IF p_AmountLot > 0 THEN
                v_DoCompare:= v_DoCompare / p_leg.t_Principal * p_AmountLot;
             END IF;
             IF IsBuy(v_OGroup)=1 THEN -- ОРЕПО
                v_DoCompare := -v_DoCompare;
             END IF;
          END IF;
          AddInArrTmp( one_cm.PayDate, v_DoCompare, p_arrtmp );
       END IF;
    END LOOP;
  END AddComisInArrTmp;

  FUNCTION GetRQ(p_rq IN OUT NOCOPY ddlrq_dbt%rowtype, p_BofficeKind IN NUMBER, p_DealID IN NUMBER, p_SubKind IN NUMBER, p_DealPart IN NUMBER, p_Type IN NUMBER) RETURN NUMBER
  IS
    v_stat NUMBER := 0;
  BEGIN
     p_rq := NULL;
     BEGIN
       SELECT * INTO p_rq
         FROM ddlrq_dbt rq
        WHERE rq.t_DocKind  = p_BofficeKind
          AND rq.t_DocID    = p_DealID
          AND rq.t_SubKind  = p_SubKind
          AND rq.t_DealPart = p_DealPart
          AND rq.t_Type     = p_Type
          AND ROWNUM = 1;
     EXCEPTION
       WHEN NO_DATA_FOUND THEN
         v_stat := 1;
     END;

    RETURN v_stat;
  END GetRQ;

  -- ТО вида "Компенсационный платеж"
  PROCEDURE AddCompPaymRepoInArrTmp( p_arrtmp IN OUT NOCOPY arrtmp_t,
                                     p_tick IN ddl_tick_dbt%rowtype,
                                     p_BegDate IN DATE,
                                     p_EndDate IN DATE,
                                     p_ToFIID IN NUMBER,
                                     p_AmountRate IN NUMBER,
                                     p_Sum IN OUT NUMBER )
  AS
    v_D    DATE   := UnknownDate;
    v_S    NUMBER := 0;
    v_Kind NUMBER := RSI_DLRQ.DLRQ_KIND_UNKNOWN;
    v_OGroup        NUMBER  := get_OperationGroup(get_OperSysTypes(p_tick.t_DealType, p_tick.t_BofficeKind));
  BEGIN
    p_Sum := 0;
    FOR one_cm IN (SELECT rq.t_Kind, GREATEST(rq.t_PlanDate, rq.t_FactDate),
                          RSI_RSB_FIInstr.ConvSum( rq.t_Amount, rq.t_FIID, p_ToFIID, GREATEST(rq.t_PlanDate, rq.t_FactDate), 1 ) * p_AmountRate
                     INTO v_Kind, v_D, v_S
                     FROM ddlrq_dbt rq
                    WHERE rq.t_DocKind  = p_tick.t_BofficeKind
                      AND rq.t_DocID    = p_tick.t_DealID
                      AND rq.t_SubKind  = RSI_DLRQ.DLRQ_SUBKIND_CURRENCY
                      AND rq.t_Type     = RSI_DLRQ.DLRQ_TYPE_COMPPAYM
                      AND (rq.t_PlanDate BETWEEN p_BegDate AND p_EndDate)
                  )
    LOOP
       IF ((IsSale(v_OGroup)=1 AND v_Kind = RSI_DLRQ.DLRQ_KIND_REQUEST) OR
           (IsBuy(v_OGroup)=1 AND v_Kind = RSI_DLRQ.DLRQ_KIND_COMMIT)) THEN
          v_S := -v_S;
       END IF;
       p_Sum := p_Sum + v_S;

       AddInArrTmp( v_D, v_S, p_arrtmp );
    END LOOP;
  END AddCompPaymRepoInArrTmp;

  FUNCTION GetSumRqHistRepo(p_Rq IN ddlrq_dbt%rowtype, p_Date IN Date) RETURN NUMBER
  IS
    v_Ret NUMBER := 0;
  BEGIN
     if( p_Rq.t_ChangeDate <= p_Date )then
        v_Ret := p_Rq.t_Amount;
     else
        BEGIN
           SELECT h.t_Amount INTO v_Ret
             FROM dDlRqBc_dbt h
            WHERE h.t_RQID = p_Rq.t_ID
              AND h.t_Instance = (select max(t_Instance)
                                    from dDlRqBc_dbt
                                   where t_RQID = h.t_RQID
                                     AND h.t_ChangeDate < p_Date
                                 );
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            v_Ret := p_Rq.t_Amount;
        END;
     end if;

     RETURN v_Ret;
  END GetSumRqHistRepo;

  -- Sn для РЕПО
  PROCEDURE AddSnREPOInArrTmp( p_arrtmp IN OUT NOCOPY arrtmp_t,
                               p_tick IN ddl_tick_dbt%rowtype,
                               p_leg2 IN ddl_leg_dbt%rowtype,
                               p_D0 IN DATE,
                               p_Dn IN DATE,
                               p_AmountLot IN NUMBER )
  AS
    v_S           NUMBER := 0;
    v_P           NUMBER := 0;
    v_Ps          NUMBER := 0;
    v_R           NUMBER := 0;
    v_Rs          NUMBER := 0;
    v_B           NUMBER := -1;
    v_S2t0        NUMBER := 0;
    v_FaceValue   NUMBER := 0;
    v_FVFI        NUMBER := -1;
    v_rq_p        ddlrq_dbt%rowtype; -- оплата 2ч
    v_rq_inc      ddlrq_dbt%rowtype; -- проценты по РЕПО 2ч
    v_Amount      NUMBER := p_leg2.t_Principal;
    v_AmountRate  NUMBER := 1;
  BEGIN
    IF p_AmountLot > 0 THEN
      v_AmountRate := p_AmountLot / p_leg2.t_Principal;
      v_Amount := p_AmountLot;
    END IF;

    -- вместо Sn используем значение Sn'
    IF p_tick.t_Flag5 = 'X' AND p_tick.t_ReturnIncomeKind = 2/*SP_RETURNINCOME_WRTOFF*/ THEN
      SELECT t_FaceValue, t_FaceValueFI INTO v_FaceValue, v_FVFI
        FROM DFININSTR_DBT
       WHERE t_FIID = p_leg2.t_PFI;
      -- Получим предстоящие выплаты купонов и/или чп
      FOR one_cm IN (SELECT fw.t_DrawingDate,
                            CASE WHEN fw.t_IsPartial = CHR(0) THEN
                                   ROUND(RSI_RSB_FIInstr.FI_CalcIncomeValue(fw.t_FIID, fw.t_DrawingDate, v_Amount, 1, 0, 0), fw.t_IncomePoint)
                                 ELSE
                                   v_Amount * (CASE WHEN fw.t_RelativeIncome = CHR(0) THEN
                                                 fw.t_IncomeVolume
                                               ELSE
                                                 ROUND(v_FaceValue * fw.t_IncomeRate / GREATEST(1, fw.t_IncomeScale) / 100, fw.t_IncomePoint)
                                               END)
                                 END t_Sum
                       FROM DFIWARNTS_DBT fw
                      WHERE fw.t_FIID   = p_leg2.t_PFI
                        AND (fw.t_DrawingDate BETWEEN p_D0 AND p_Dn-1)
                        AND fw.t_IsClosed <> 'X'
                      ORDER BY fw.t_DrawingDate
                    )
      LOOP
        IF one_cm.t_Sum <> 0 THEN
          v_R  := RSI_RSB_FIInstr.ConvSum( one_cm.t_Sum, v_FVFI, p_leg2.t_CFI, one_cm.t_DrawingDate, 1 );
          -- ti/Ri
          AddInArrTmp( one_cm.t_DrawingDate, v_R, p_arrtmp );

          v_Rs := v_Rs + v_R;
/*!?*/    v_B  := RSI_RSB_FIInstr.FI_GetDaysInYearByBase( p_leg2.t_PFI, one_cm.t_DrawingDate );
          v_P  := v_R * p_leg2.t_IncomeRate/100 * (p_Dn - one_cm.t_DrawingDate) / v_B;
          v_Ps := v_Ps + v_P;
        END IF;
      END LOOP;

      -- Сумма по второй части сделки РЕПО в системе на дату расчета t0:
      -- ТО оплата 2ч
      IF GetRQ(v_rq_p, p_tick.t_BofficeKind, p_tick.t_DealID, RSI_DLRQ.DLRQ_SUBKIND_CURRENCY, 2, RSI_DLRQ.DLRQ_TYPE_PAYMENT) = 0 THEN
         v_S2t0 := GetSumRqHistRepo(v_rq_p, p_D0) * v_AmountRate;
      END IF;
      -- ТО проценты 2ч
      IF GetRQ(v_rq_inc, p_tick.t_BofficeKind, p_tick.t_DealID, RSI_DLRQ.DLRQ_SUBKIND_CURRENCY, 2, RSI_DLRQ.DLRQ_TYPE_INCREPO) = 0 THEN
         IF p_leg2.t_IncomeRate >= 0 THEN
            v_S2t0 := v_S2t0 + GetSumRqHistRepo(v_rq_inc, p_D0) * v_AmountRate;
         ELSE
            v_S2t0 := v_S2t0 - GetSumRqHistRepo(v_rq_inc, p_D0) * v_AmountRate;
         END IF;
      END IF;

      v_S := v_S2t0 - v_Ps - v_Rs;
    ELSE
      v_S := p_leg2.t_TotalCost * v_AmountRate;
    END IF;
    AddInArrTmp( p_Dn, v_S, p_arrtmp );

  END AddSnREPOInArrTmp;

  -- Получить объем выпуска бумаги на дату
  FUNCTION GetQtyFI(p_FIID IN NUMBER, p_Date IN DATE DEFAULT UnknownDate) RETURN NUMBER
  IS
    v_Qty NUMBER;
  BEGIN
     BEGIN
        IF p_Date != UnknownDate THEN
           SELECT qh.t_Qty INTO v_Qty
             FROM dv_fi_qty_hist qh
            WHERE qh.t_FIID = p_FIID
              AND qh.t_BegDate <= p_Date
              AND (qh.t_EndDate >= p_Date OR qh.t_EndDate = UnknownDate)
              AND ROWNUM = 1
           ORDER BY qh.t_FIID, qh.t_Sort, qh.t_EndDate DESC;
        END IF;
        IF v_Qty = 0 THEN
           SELECT t_QTY INTO v_Qty FROM davoiriss_dbt WHERE t_FIID = p_FIID;
        END IF;
     EXCEPTION
       WHEN NO_DATA_FOUND THEN v_Qty := 0;
     END;
     RETURN v_Qty;
  END GetQtyFI;

  -- Получить количество бумаг из лота
  FUNCTION GetWrtAmount(p_SumID IN NUMBER) RETURN NUMBER
  IS
     v_Amount NUMBER;
  BEGIN
     BEGIN
        SELECT t_Amount INTO v_Amount FROM DPMWRTSUM_DBT WHERE T_SUMID = p_SumID;
     EXCEPTION
       WHEN NO_DATA_FOUND THEN RETURN 0;
     END;
     RETURN v_Amount;
  END GetWrtAmount;

  FUNCTION GetWrtAmountOnDate(p_SumID IN NUMBER, p_Date IN DATE) RETURN NUMBER
  IS
     v_Amount NUMBER;
  BEGIN
     BEGIN
        SELECT t_Amount INTO v_Amount 
          FROM V_SCWRTHISTEX V
         WHERE V.T_SUMID = p_SumID
           AND V.T_INSTANCE = (SELECT MAX(V1.T_INSTANCE)
                                 FROM V_SCWRTHISTEX V1
                                WHERE V1.T_SUMID = V.T_SUMID
                                  AND V1.t_ChangeDate <= p_Date);
     EXCEPTION
       WHEN NO_DATA_FOUND THEN RETURN 0;
     END;
     RETURN v_Amount;
  END GetWrtAmountOnDate;

  -- Получить верхнюю дату расчета, если на бумаге задана категория Расчет корректировок до даты ближайшей оферты
  FUNCTION SC_GetTopCalcDate(pCalcKind IN NUMBER, pFiID IN NUMBER, pCalcDate IN DATE, pIsOffer OUT NUMBER, pTermless OUT CHAR, pOfferDate OUT DATE) RETURN DATE
  IS
     v_TopDate DATE;
     v_DrawingDate DATE;
  BEGIN
     pTermless := chr(0);
     pIsOffer := 0;
     pOfferDate := UnknownDate;

     SELECT RSI_RSB_FIInstr.FI_GetNominalDrawingDate(FI.T_FIID, AV.t_Termless), AV.t_Termless
       INTO v_DrawingDate, pTermless
       FROM DFININSTR_DBT FI, DAVOIRISS_DBT AV
      WHERE FI.T_FIID = pFiID
        AND AV.T_FIID = FI.T_FIID;

     IF pCalcKind IN (CALCKIND_AVR, CALCKIND_OWN) THEN
        IF GetFICategoryAttrID(pFiID, 63/*Расчет корректировок до даты ближайшей оферты*/) = 1/*Да*/ THEN
           pIsOffer := 1;

           SELECT NVL(MIN(t_DateRedemption), UnknownDate) INTO pOfferDate
             FROM DOFFERS_DBT ofs
            WHERE ofs.t_FIID = pFiID
              AND (ofs.t_DateRedemption BETWEEN pCalcDate AND (CASE WHEN pTermless = 'X' THEN MAX_DATE ELSE (v_DrawingDate-1) END));

           v_TopDate := pOfferDate;
           IF (v_TopDate = UnknownDate) THEN v_TopDate := v_DrawingDate; END IF;
        ELSE
           v_TopDate := v_DrawingDate;
        END IF;
     ELSE
        v_TopDate := v_DrawingDate;
     END IF;

     RETURN v_TopDate;
  END SC_GetTopCalcDate;

  PROCEDURE AddInArrTmpEx( pDate IN DATE, pSum IN NUMBER, pArrTmp IN OUT NOCOPY arrtmp_t, pTopDate IN DATE )
  AS
  BEGIN
     IF pDate <= pTopDate THEN
        AddInArrTmp( pDate, pSum, pArrTmp );
     END IF;
  END AddInArrTmpEx;

  --Заполнить временную таблицу для расчета ЭПС
  PROCEDURE FillXIRR_EPS(p_CalcKind IN NUMBER, p_DocID IN NUMBER, p_SumID IN NUMBER, p_CalcDate IN DATE DEFAULT UnknownDate)
  IS
    v_arrtmp        arrtmp_t := arrtmp_t();
    v_tick          ddl_tick_dbt%rowtype;
    v_leg           ddl_leg_dbt%rowtype;
    v_leg2          ddl_leg_dbt%rowtype;
    v_lnk           dvsordlnk_dbt%rowtype;
    v_dvnDeal       ddvndeal_dbt%rowtype;
    v_dvnFI         ddvnfi_dbt%rowtype;
    v_OGroup        doprkoper_dbt.T_SYSTYPES%TYPE;
    v_rq            ddlrq_dbt%rowtype;
    v_FaceFI        NUMBER;
    v_AvoirKind     NUMBER;
    v_TopCalcDate   DATE;
    v_DeliveryDate  DATE    := UnknownDate;
    v_PaymentDate   DATE    := UnknownDate;
    v_PaymentDate2  DATE    := UnknownDate;
    v_D             DATE    := UnknownDate;
    v_S             NUMBER  := 0;
    v_Sa            NUMBER  := 0;
    v_Sa2           NUMBER  := 0;
    v_Qty           NUMBER;
    v_Amount        NUMBER;
    v_AmountLot     NUMBER := 0;
    v_AmountRate    NUMBER := 1;
    v_N             NUMBER  := 0;
    v_C             NUMBER  := 0;
    v_Basis         NUMBER;
    v_dc            DATE;
    v_B             NUMBER;
    v_IsOffer       NUMBER;
    v_Termless      CHAR;
    v_OfferDate     DATE;
    v_BioPFI         NUMBER := 0;
    v_BioPFICurrency NUMBER := -1;
    v_BioPFIDate     DATE;
  BEGIN

    -- очистим временную таблицу
    DELETE FROM DXIRR_TMP;

    IF p_CalcKind IN (CALCKIND_VS,CALCKIND_VA) THEN
      SELECT *
        INTO v_lnk
        FROM dvsordlnk_dbt lnk
       WHERE     LNK.T_CONTRACTID = p_DocID
             AND LNK.T_BCID = p_SumID --За неимением других параметров
             AND LNK.T_LINKKIND = CASE WHEN p_CalcKind = CALCKIND_VS THEN 0 ELSE 1 END;

      SELECT *
        INTO v_leg
        FROM ddl_leg_dbt leg
       WHERE     leg.t_LegKind = 1
             AND leg.t_DealID = v_lnk.t_BCID
             AND t_LegID = 0;

       if (rsb_bill.GetBnrPlanRepayDate (v_leg.t_ID, v_D) != 0) THEN
         RAISE NO_DATA_FOUND;
       END IF;

       if p_CalcKind = CALCKIND_VA THEN
         SELECT *
           INTO v_leg2
           FROM ddl_leg_dbt leg
          WHERE     leg.t_LegKind = 0
                AND leg.t_DealID = p_DocID
                AND t_LegID = 0;
       END IF;

       v_PaymentDate := rsb_bill.GetBNRFirstDate(v_lnk.t_BCID, v_lnk.T_CONTRACTID);

       v_S := RSI_RSB_FIInstr.ConvSum(v_lnk.T_BCCOST, v_lnk.T_BCCFI, v_leg.t_PFI, v_PaymentDate, 2 );
       v_S := v_S * -1;
    END IF;

    IF p_CalcKind IN (CALCKIND_AVR, CALCKIND_OREPO, CALCKIND_PREPO, CALCKIND_OWN) THEN
      SELECT * INTO v_tick
        FROM ddl_tick_dbt
       WHERE t_DealID = p_DocID;

      SELECT * INTO v_leg
        FROM ddl_leg_dbt
       WHERE t_LegKind = 0
         AND t_DealID = p_DocID
         AND t_LegID = 0;

      SELECT t_FaceValueFI, t_AvoirKind INTO v_FaceFI, v_AvoirKind
        FROM DFININSTR_DBT
       WHERE t_FIID = v_tick.t_PFI;

      v_OGroup := get_OperationGroup(get_OperSysTypes(v_tick.t_DealType, v_tick.t_BofficeKind));

      -- фактическая дата оплаты
      IF GetRQ(v_rq, v_tick.t_BofficeKind, v_tick.t_DealID, RSI_DLRQ.DLRQ_SUBKIND_CURRENCY, 1, RSI_DLRQ.DLRQ_TYPE_PAYMENT) = 0 THEN
        v_PaymentDate := GREATEST(v_rq.t_PlanDate, v_rq.t_FactDate);
      END IF;

      v_Amount := v_leg.t_Principal;
      IF p_SumID > 0 THEN
        v_AmountLot := GetWrtAmountOnDate(p_SumID, p_CalcDate);
        v_AmountRate := v_AmountLot / v_Amount;
      END IF;

      v_TopCalcDate := SC_GetTopCalcDate(p_CalcKind, v_tick.t_PFI, p_CalcDate, v_IsOffer, v_Termless, v_OfferDate);
    END IF;

    -- ЦБ (покупка)
    IF p_CalcKind = CALCKIND_AVR THEN
      IF IsTwoPart(v_OGroup)=0 AND IsBuy(v_OGroup)=1 THEN
        -- фактическая дата поставки
        IF GetRQ(v_rq, v_tick.t_BofficeKind, v_tick.t_DealID, RSI_DLRQ.DLRQ_SUBKIND_AVOIRISS, 1, RSI_DLRQ.DLRQ_TYPE_DELIVERY) = 0 THEN
          v_DeliveryDate := GREATEST(v_rq.t_PlanDate, v_rq.t_FactDate);
        END IF;

        -- минус (Сумма начального денежного потока в пересчете на одну ценную бумагу + Sав):
          -- Sав;
          -- стоимость с учетом НКД;
          -- Существенные затраты:
            -- Значение поля <предварительные затраты> признаем как существенную комиссию;
            -- Комиссии, признанные как существенные.

        -- Если облигация покупается при исполнении опциона на покупку,
        -- то цена опциона должна приравниваться к сумме аванса, а дата оплаты опциона к дате аванса
        IF( v_tick.t_ParentID > 0 AND RSI_RSB_FIInstr.FI_AvrKindsGetRoot( 2, v_AvoirKind ) = RSI_RSB_FIInstr.AVOIRKIND_BOND AND v_tick.t_OriginID = 158) THEN
            BEGIN
              select nDeal.* into v_dvnDeal
                from ddvndeal_dbt nDeal
               where nDeal.t_ID = v_tick.t_ParentID
                 and nDeal.t_DvKind = 2/*DERIVATIVE_OPTION*/;

              select nFI.* into v_dvnFI
                from ddvnfi_dbt nFI
               where nFI.t_DealID = v_dvnDeal.t_ID
                 AND nFI.t_Type = 0;

              v_D := v_dvnFI.t_PayDate;
              v_S := -RSI_RSB_FIInstr.ConvSum(v_dvnFI.t_Cost, v_dvnFI.t_PriceFIID, v_FaceFI, v_dvnFI.t_PayDate, 1 ) / v_dvnFI.t_Amount;
              AddInArrTmpEx( v_D, v_S, v_arrtmp, v_TopCalcDate );

            EXCEPTION
              WHEN NO_DATA_FOUND THEN
                DBMS_OUTPUT.PUT_LINE('Не найден опцион ' || v_tick.t_ParentID);
            END;
        END IF;

        -- дата оплаты аванса
        -- минус сумма аванса в пересчете на одну бумагу
        IF GetRQ(v_rq, v_tick.t_BofficeKind, v_tick.t_DealID, RSI_DLRQ.DLRQ_SUBKIND_CURRENCY, 1, RSI_DLRQ.DLRQ_TYPE_AVANCE) = 0 THEN
          v_D := GREATEST(v_rq.t_PlanDate, v_rq.t_FactDate);
          v_Sa := RSI_RSB_FIInstr.ConvSum(v_rq.t_Amount, v_rq.t_FIID, v_FaceFI, v_D, 1 );
          v_S := -1 * v_Sa / v_Amount;
          AddInArrTmpEx( v_D, v_S, v_arrtmp, v_TopCalcDate );
        END IF;

        -- дата оплаты задатка
        -- минус сумма задатка в пересчете на одну бумагу
        IF GetRQ(v_rq, v_tick.t_BofficeKind, v_tick.t_DealID, RSI_DLRQ.DLRQ_SUBKIND_CURRENCY, 1, RSI_DLRQ.DLRQ_TYPE_DEPOSIT) = 0 THEN
          v_D := GREATEST(v_rq.t_PlanDate, v_rq.t_FactDate);
          v_S := -1 * RSI_RSB_FIInstr.ConvSum(v_rq.t_Amount, v_rq.t_FIID, v_FaceFI, v_D, 1 ) / v_Amount;
          AddInArrTmpEx( v_D, v_S, v_arrtmp, v_TopCalcDate );
        END IF;

        -- стоимость с учетом НКД
        v_S := -1 * (RSI_RSB_FIInstr.ConvSum(v_leg.t_TotalCost, v_leg.t_CFI, v_FaceFI, v_PaymentDate, 1 ) - v_Sa) / v_Amount;
        AddInArrTmpEx( v_PaymentDate, v_S, v_arrtmp, v_TopCalcDate );

        -- Значение поля <предварительные затраты> признаем как существенную комиссию
        IF v_tick.t_PreOutlay <> 0 THEN
          v_S := -1 * RSI_RSB_FIInstr.ConvSum(v_tick.t_PreOutlay, v_tick.t_PreOutlayFIID, v_FaceFI, v_tick.t_DealDate, 1 ) / v_Amount;
          AddInArrTmpEx( v_tick.t_DealDate, v_S, v_arrtmp, v_TopCalcDate );
        END IF;

        AddComisInArrTmp(v_arrtmp, p_CalcKind, v_tick, v_leg, v_AmountLot, v_TopCalcDate);

        AddFiWarntsInArrTmp(v_arrtmp, v_tick.t_PFI, v_DeliveryDate, v_TopCalcDate, 1, 1, false, NULL, v_IsOffer);
      END IF;

    -- ПРЕПО/ОРЕПО
    ELSIF p_CalcKind IN (CALCKIND_PREPO, CALCKIND_OREPO) THEN
      IF IsRepo(v_OGroup)=1 THEN
        SELECT * INTO v_leg2
          FROM ddl_leg_dbt
         WHERE t_LegKind = 2
           AND t_DealID = p_DocID
           AND t_LegID = 0;

        -- фактическая дата оплаты 2
        IF GetRQ(v_rq, v_tick.t_BofficeKind, v_tick.t_DealID, RSI_DLRQ.DLRQ_SUBKIND_CURRENCY, 2, RSI_DLRQ.DLRQ_TYPE_PAYMENT) = 0 THEN
          v_PaymentDate2 := GREATEST(v_rq.t_PlanDate, v_rq.t_FactDate);
        END IF;

        -- дата оплаты аванса/ минус сумма аванса
        IF GetRQ(v_rq, v_tick.t_BofficeKind, v_tick.t_DealID, RSI_DLRQ.DLRQ_SUBKIND_CURRENCY, 1, RSI_DLRQ.DLRQ_TYPE_AVANCE) = 0 THEN
          v_D  := GREATEST(v_rq.t_PlanDate, v_rq.t_FactDate);
          v_Sa := v_rq.t_Amount * v_AmountRate;
          v_S  := -1 * v_Sa;
          AddInArrTmp( v_D, v_S, v_arrtmp );
        END IF;

        -- дата оплаты задатка/ минус сумма задатка
        IF GetRQ(v_rq, v_tick.t_BofficeKind, v_tick.t_DealID, RSI_DLRQ.DLRQ_SUBKIND_CURRENCY, 1, RSI_DLRQ.DLRQ_TYPE_DEPOSIT) = 0 THEN
          v_D := GREATEST(v_rq.t_PlanDate, v_rq.t_FactDate);
          v_S := -1 * v_rq.t_Amount * v_AmountRate;
          AddInArrTmp( v_D, v_S, v_arrtmp );
        END IF;

        -- дата оплаты аванса 2/ сумма аванса 2
        IF GetRQ(v_rq, v_tick.t_BofficeKind, v_tick.t_DealID, RSI_DLRQ.DLRQ_SUBKIND_CURRENCY, 2, RSI_DLRQ.DLRQ_TYPE_AVANCE) = 0 THEN
          v_D   := GREATEST(v_rq.t_PlanDate, v_rq.t_FactDate);
          v_Sa2 := v_rq.t_Amount * v_AmountRate;
          AddInArrTmp( v_D, v_Sa2, v_arrtmp );
        END IF;

        -- дата оплаты задатка 2/ сумма задатка 2
        IF GetRQ(v_rq, v_tick.t_BofficeKind, v_tick.t_DealID, RSI_DLRQ.DLRQ_SUBKIND_CURRENCY, 2, RSI_DLRQ.DLRQ_TYPE_DEPOSIT) = 0 THEN
          v_D := GREATEST(v_rq.t_PlanDate, v_rq.t_FactDate);
          v_S := v_rq.t_Amount * v_AmountRate;
          AddInArrTmp( v_D, v_S, v_arrtmp );
        END IF;

        -- минус Сумма по договору (Сумма первой части сделки РЕПО) за вычетом аванса
        v_S := -1 * (v_leg.t_TotalCost * v_AmountRate - v_Sa);
        AddInArrTmp( v_PaymentDate, v_S, v_arrtmp );

        -- Комиссии, признанные как существенные.
        AddComisInArrTmp(v_arrtmp, p_CalcKind, v_tick, v_leg, v_AmountLot, v_TopCalcDate);

        AddCompPaymRepoInArrTmp(v_arrtmp, v_tick, v_PaymentDate, v_PaymentDate2, v_Leg.t_CFI, v_AmountRate, v_S);

        -- Sn/Sn'
        AddSnREPOInArrTmp( v_arrtmp, v_tick, v_leg2, p_CalcDate, v_PaymentDate2, v_leg.t_Principal );
      END IF;

    -- ОЭБ
    ELSIF p_CalcKind = CALCKIND_OWN THEN
      IF v_tick.t_BofficeKind IN (DL_SECUROWN, DL_AVRWRTOWN) THEN
        IF v_AmountLot > 0 THEN
          v_Amount := v_AmountLot;
        END IF;
        -- Предварительные затраты, согласно таблице комиссий на бумаге
        IF v_tick.t_Placement = 'X' THEN
          v_Qty := RSB_PMWRTOFF.WRTGetAmountOwn(v_tick.t_Department, v_tick.t_PFI, p_CalcDate, 1, 1);
          IF p_SumID = 0 THEN
            v_Qty := v_Qty + v_Amount;
          END IF;

          IF v_Qty = 0 THEN
            DBMS_OUTPUT.PUT_LINE('Ошибка в ОЭБ с FIID ' || v_tick.t_PFI || ' - нет размещенных ц/б');
          ELSE
          
            FOR cd IN (SELECT DLC.T_STARTAMORTDATE as PayDate,
                              RSB_FIInstr.ConvSum(DLC.T_SUM, CM.T_FIID_COMM, FI.T_FACEVALUEFI, DLC.T_PLANPAYDATE) as SumComis
                         FROM DDLCOMIS_DBT DLC, DSFCOMISS_DBT CM, DFININSTR_DBT FI
                        WHERE DLC.T_DOCKIND = DLDOC_ISSUE
                          AND DLC.T_DOCID = v_tick.t_PFI
                          AND DLC.T_STARTAMORTDATE > TO_DATE('01.01.0001','DD.MM.YYYY')
                          AND DLC.T_STARTAMORTDATE <= p_CalcDate
                          AND FI.T_FIID = DLC.T_DOCID
                          AND CM.T_FEETYPE = DLC.T_FEETYPE
                          AND CM.T_NUMBER = DLC.T_COMNUMBER
                      )
            LOOP
              v_S := cd.SumComis * v_Amount / v_Qty;
              AddInArrTmp( cd.PayDate, v_S, v_arrtmp );
            END LOOP;
          END IF;
        END IF;

        -- Дата/сумма поступления денежных средств
        v_S := -RSI_RSB_FIInstr.ConvSum(v_leg.t_TotalCost, v_leg.t_CFI, v_FaceFI, v_PaymentDate, 1) * v_AmountRate;
        AddInArrTmp( v_PaymentDate, v_S, v_arrtmp );

        -- сумма затрат
        AddComisInArrTmp(v_arrtmp, p_CalcKind, v_tick, v_leg, v_AmountLot, v_TopCalcDate);

        -- даты погашения и суммы из истории купонов и частичных погашений
        AddFiWarntsInArrTmp(v_arrtmp, v_tick.t_PFI, v_PaymentDate, v_TopCalcDate, v_Amount, 1, false, NULL, v_IsOffer);

        --По БИО добавить СС ПФИ
        BEGIN
          IF GetRQ(v_rq, v_tick.t_BofficeKind, v_tick.t_DealID, RSI_DLRQ.DLRQ_SUBKIND_AVOIRISS, 1, RSI_DLRQ.DLRQ_TYPE_DELIVERY) = 0 THEN
            v_DeliveryDate := GREATEST(v_rq.t_PlanDate, v_rq.t_FactDate);
          END IF;

          SELECT FR.T_ONEITEMFV, FR.T_CURRENCY, FR.T_DATE
            INTO v_BioPFI, v_BioPFICurrency, v_BioPFIDate
            FROM DBIOFRVAL_DBT FR
           WHERE FR.T_FIID = v_tick.t_PFI
             AND FR.T_DATE = (SELECT MAX(FR1.T_DATE)
                                FROM DBIOFRVAL_DBT FR1
                               WHERE FR1.T_FIID = FR.T_FIID
                                 AND FR1.T_DATE <= v_DeliveryDate
                             );
                                  
        EXCEPTION
             WHEN NO_DATA_FOUND THEN NULL;
        END;

        IF v_BioPFI <> 0 THEN
          v_BioPFI := RSI_RSB_FIInstr.ConvSum(v_BioPFI, v_BioPFICurrency, v_FaceFI, v_BioPFIDate, 1) * v_Amount;

          AddInArrTmp( v_BioPFIDate, v_BioPFI, v_arrtmp );
        END IF;

      END IF;
    ELSIF p_CalcKind IN (CALCKIND_VS,CALCKIND_VA) THEN
      v_Basis := v_leg.t_Basis;
      v_dc := v_leg.t_InterestStart;
      v_N := v_leg.t_Principal;
      v_C := v_leg.t_Price / 1000000;
      if (rsb_bill.GetDaysInYearByBasis (v_Basis,v_dc,TRUE,v_B) != 0) THEN
         RAISE NO_DATA_FOUND;
      END IF;
      v_Sa := v_N + (v_N * v_C * (v_D - v_dc) / v_B);
      AddInArrTmp( v_PaymentDate, v_S, v_arrtmp );
      AddInArrTmp( v_D, v_Sa, v_arrtmp );
    END IF;

    -- заполним временную таблицу
    InsertArrToXIRR(v_arrtmp);

    EXCEPTION
      WHEN NO_DATA_FOUND THEN NULL;
  END FillXIRR_EPS;

  --Рассчитать ЭПС по сделке
  FUNCTION CalcEPS(p_CalcKind IN NUMBER, p_DocID IN NUMBER, p_SumID IN NUMBER, p_CalcDate IN DATE DEFAULT UnknownDate) RETURN NUMBER
  IS
  BEGIN
    FillXIRR_EPS( p_CalcKind, p_DocID, p_SumID, p_CalcDate );

    RETURN CalcEPSNoFill();
  END CalcEPS;

  --Реализация расчета ЭПС взята у Лоанс
  -- Если вкратце по алгоритму, то использован алгоритм половинного деления.
  -- Ограничения и особенности:
  --   Ставки могут быть от 0% до 100%. Расширить диапазон не сложно (за диапазон отвечают параметры a и b)
  --   Условия достижения точности почему-то убрано и алгоритм проходит по всем итерациям. Возможно это и неплохо, т.к. точнее ставка
  FUNCTION CalcEPSNoFill1 RETURN NUMBER
  IS
     v_accuracy INTEGER := ABS(RSB_COMMON.GetRegIntValue('SECUR\МСФО\ЭПС_ТОЧНОСТЬ_РЕЗУЛЬТАТА'));
     v_iternum  INTEGER := RSB_COMMON.GetRegIntValue('SECUR\МСФО\ЭПС_ЧИСЛО_ИТЕРАЦИЙ');
     v_iter     NUMBER  := 0;
     v_epsval   NUMBER  := 0;
  BEGIN
     BEGIN
        EXECUTE IMMEDIATE
        'with t as '||
        '( '||
        '  SELECT t_date as dt, t_sum as summ FROM DXIRR_TMP '||
        ') '||
        '  SELECT decode(rate, 0, -1, rate)/*эффективная_ставка*/, iter /*количество_итераций*/ '||
        '    FROM (SELECT * '||
        '            FROM t '||
        '           model '||
        '    dimension by (row_number() over (order by dt) rn) '||
        '        measures (dt - first_value(dt) over (order by dt) dt, summ s, 0 sg, 1 sgch, 0 ss, 0 sss, 0 a, 200 b, 0 x, 0 rate, 0 iter) '||
        '           rules iterate(' || to_char(v_iternum) || ') '||
        '                 ( '||
        '                   x[1] = a[1] + (b[1] - a[1])/2, '||
        '                   ss[any] = s[CV()] / power(1 + x[1], decode(sign(22945-dt[CV()]), -1, 22945, dt[CV()])/365), '||
        '                   sss[1] = sum(ss)[any], '||
        '                   sgch[1] = decode(sign(abs(sss[1]) - power(10, ' || to_char(-(v_accuracy+2)) || ')), -1, -1, 1), '||
        '                   sg[1] = decode(sg[1], 0, decode(sign(sss[1]), -1, -1, 1), sg[1]), '||
        '                   a[1] = decode(sign(sss[1]), sg[1] * sgch[1], a[1], x[1]), '||
        '                   b[1] = decode(sign(sss[1]), sg[1] * sgch[1], x[1], b[1]), '||
        '                   rate[1] = decode(sign(abs(sss[1]) - power(10, ' || to_char(-(v_accuracy+2)) || ')), 1, rate[1], x[1]), '||
        '                   iter[1] = iteration_number + 1 '||
        '                 ) '||
        '         ) '||
        '   WHERE rn = 1 '
        INTO v_epsval, v_iter;
     EXCEPTION
       WHEN OTHERS THEN RETURN -1;
     END;

     RETURN round(v_epsval, v_accuracy);
  END CalcEPSNoFill1;

  --последовательно подобрать ставку
  FUNCTION CalcEPSNoFill2
     RETURN NUMBER
  IS
     v_accuracy        INTEGER:= ABS (RSB_COMMON.GetRegIntValue ('SECUR\МСФО\ЭПС_ТОЧНОСТЬ_РЕЗУЛЬТАТА'));
     v_iternum         INTEGER:= RSB_COMMON.GetRegIntValue ('SECUR\МСФО\ЭПС_ЧИСЛО_ИТЕРАЦИЙ');
     v_iter            NUMBER := 0;
     v_epsval          NUMBER := 0;
     v_Counter         INTEGER := 1;
     v_FirstDate       DATE;
     v_Estimate        NUMBER := 0;
     v_Rate            NUMBER := 0;
     v_DeltaEstimate   NUMBER := 1e-7;
     v_PrevEstimate    NUMBER;
     v_Direction       SMALLINT := 0;
     v_DeltaRate       NUMBER := 1e-2;
  BEGIN
     BEGIN
        SELECT MIN (T_DATE) INTO v_FirstDate FROM DXIRR_TMP;

        FOR v_Counter IN 1 .. v_iternum
        LOOP
           SELECT NVL(SUM (T_SUM / POWER (1 + v_Rate, (T_DATE - v_FirstDate) / 365)), 0)
             INTO v_Estimate
             FROM DXIRR_TMP;

           IF (ABS (v_Estimate) < v_DeltaEstimate)
           THEN
              EXIT;
           END IF;

           IF v_PrevEstimate IS NOT NULL
           THEN
              IF v_PrevEstimate > 0 AND v_Estimate > 0
              THEN
                 v_Direction := 1;
              END IF;

              IF v_PrevEstimate < 0 AND v_Estimate < 0
              THEN
                 v_Direction := -1;
              END IF;

              IF    (v_PrevEstimate < 0 AND v_Estimate > 0)
                 OR (v_PrevEstimate > 0 AND v_Estimate < 0)
              THEN
                 v_DeltaRate := v_DeltaRate / 2;
              END IF;
           END IF;

           v_PrevEstimate := v_Estimate;
           v_Rate := v_Rate + v_DeltaRate * v_Direction;
        END LOOP;

        IF v_Rate = 0 THEN
           v_Rate := -1;
        END IF;
     EXCEPTION
       WHEN OTHERS THEN RETURN -1;
     END;

     RETURN ROUND(v_Rate, v_accuracy);
  END CalcEPSNoFill2;

  FUNCTION CalcEPSNoFill RETURN NUMBER
  IS
     v_Ret    NUMBER;
     v_ExistP NUMBER;
     v_ExistM NUMBER;
  BEGIN
     select nvl((select 1 from dxirr_tmp where t_Sum >= 0 and rownum = 1),0),
            nvl((select 1 from dxirr_tmp where t_Sum < 0 and rownum = 1),0)
       into v_ExistP, v_ExistM
       from dual;

     -- должны быть хотя бы один положительный и отрицательный потоки
     IF v_ExistP = 1 AND v_ExistM = 1 THEN
        v_Ret := CalcEPSNoFill1();
        IF v_Ret = -1 THEN
           v_Ret := CalcEPSNoFill2();
        END IF;
     ELSE
        v_Ret := -1;
     END IF;

     RETURN v_Ret;
  END CalcEPSNoFill;

   -- Заполнить временную таблицу для расчета АС по ЭПС
   PROCEDURE FillXIRR_AS_EPS(p_CalcKind  IN NUMBER,
                             p_DocID     IN NUMBER,
                             p_SumID     IN NUMBER,
                             p_CalcDate  IN DATE,
                             p_EPS       IN NUMBER)
   IS
     v_arrtmp       arrtmp_t := arrtmp_t();
     v_OGroup       doprkoper_dbt.T_SYSTYPES%TYPE;
     v_tick         ddl_tick_dbt%rowtype;
     v_lnk          dvsordlnk_dbt%rowtype;
     v_leg          ddl_leg_dbt%rowtype;
     v_leg2         ddl_leg_dbt%rowtype;
     v_rq           ddlrq_dbt%rowtype;
     v_TopCalcDate  DATE;
     v_PaymentDate2 DATE := UnknownDate;
     v_D            DATE := UnknownDate;
     v_Sa2          NUMBER := 0;
     v_Amount       NUMBER;
     v_AmountLot    NUMBER := 0;
     v_AmountRate   NUMBER := 1;
     v_N            NUMBER := 0;
     v_C            NUMBER := 0;
     v_Basis        NUMBER;
     v_dc           DATE;
     v_B            NUMBER;
     v_IsOffer      NUMBER;
     v_Termless     CHAR;
     v_OfferDate    DATE;
   BEGIN
     -- очистим временную таблицу
     DELETE FROM DXIRR_TMP;

     IF p_CalcKind IN (CALCKIND_VS,CALCKIND_VA) THEN
       SELECT *
         INTO v_lnk
         FROM dvsordlnk_dbt lnk
        WHERE     LNK.T_CONTRACTID = p_DocID
              AND LNK.T_BCID = p_SumID --За неимением других параметров
              AND LNK.T_LINKKIND = CASE WHEN p_CalcKind = CALCKIND_VS THEN 0 ELSE 1 END;

       SELECT *
         INTO v_leg
         FROM ddl_leg_dbt leg
        WHERE     leg.t_LegKind = 1
              AND leg.t_DealID = v_lnk.t_BCID
              AND t_LegID = 0;

        if (rsb_bill.GetBnrPlanRepayDate (v_leg.t_ID, v_D) != 0) THEN
          RAISE NO_DATA_FOUND;
        END IF;
     END IF;

     IF p_CalcKind IN (CALCKIND_AVR, CALCKIND_PREPO, CALCKIND_OREPO, CALCKIND_OWN) THEN
       SELECT * INTO v_tick
         FROM ddl_tick_dbt
        WHERE t_DealID = p_DocID;

       SELECT * INTO v_leg
         FROM ddl_leg_dbt
        WHERE t_LegKind = 0
          AND t_DealID = p_DocID
          AND t_LegID = 0;

       v_OGroup := get_OperationGroup(get_OperSysTypes(v_tick.t_DealType, v_tick.t_BofficeKind));
       v_Amount := v_leg.t_Principal;
       IF p_SumID > 0 THEN
         v_AmountLot := GetWrtAmountOnDate(p_SumID, p_CalcDate);
         v_AmountRate := v_AmountLot / v_Amount;
         v_Amount := v_AmountLot;
       END IF;
     END IF;

     -- ЦБ (покупка), ОЭБ
     IF p_CalcKind IN (CALCKIND_AVR, CALCKIND_OWN) THEN
       IF IsTwoPart(v_OGroup)=0 and IsBuy(v_OGroup)=1 or p_CalcKind=CALCKIND_OWN THEN
         v_TopCalcDate := SC_GetTopCalcDate(p_CalcKind, v_tick.t_PFI, p_CalcDate, v_IsOffer, v_Termless, v_OfferDate);
         AddFiWarntsInArrTmp(v_arrtmp, v_tick.t_PFI, p_CalcDate, v_TopCalcDate, v_Amount, 1, false, NULL, v_IsOffer);
       END IF;

     -- ПРЕПО/ОРЕПО
     ELSIF p_CalcKind IN (CALCKIND_PREPO, CALCKIND_OREPO) THEN
       IF IsRepo(v_OGroup)=1 THEN
         SELECT * INTO v_leg2
           FROM ddl_leg_dbt
          WHERE t_LegKind = 2
            AND t_DealID = p_DocID
            AND t_LegID = 0;

         -- дата оплаты аванса 2/ сумма аванса 2
         IF GetRQ(v_rq, v_tick.t_BofficeKind, v_tick.t_DealID, RSI_DLRQ.DLRQ_SUBKIND_CURRENCY, 2, RSI_DLRQ.DLRQ_TYPE_AVANCE) = 0 THEN
           v_D   := GREATEST(v_rq.t_PlanDate, v_rq.t_FactDate);
           AddInArrTmp( v_D, v_rq.t_Amount*v_AmountRate, v_arrtmp );
         END IF;

         -- дата оплаты задатка 2/ сумма задатка 2
         IF GetRQ(v_rq, v_tick.t_BofficeKind, v_tick.t_DealID, RSI_DLRQ.DLRQ_SUBKIND_CURRENCY, 2, RSI_DLRQ.DLRQ_TYPE_DEPOSIT) = 0 THEN
           v_D := GREATEST(v_rq.t_PlanDate, v_rq.t_FactDate);
           AddInArrTmp( v_D, v_rq.t_Amount*v_AmountRate, v_arrtmp );
         END IF;

         -- фактическая дата оплаты 2
         IF GetRQ(v_rq, v_tick.t_BofficeKind, v_tick.t_DealID, RSI_DLRQ.DLRQ_SUBKIND_CURRENCY, 2, RSI_DLRQ.DLRQ_TYPE_PAYMENT) = 0 THEN
           v_PaymentDate2 := GREATEST(v_rq.t_PlanDate, v_rq.t_FactDate);
         END IF;

         -- D0/0
         AddInArrTmp( p_CalcDate, 0, v_arrtmp );

         -- Sn/Sn'
         AddSnREPOInArrTmp( v_arrtmp, v_tick, v_leg2, p_CalcDate, v_PaymentDate2, v_leg2.t_Principal );
       END IF;
     ELSIF p_CalcKind IN (CALCKIND_VS,CALCKIND_VA) THEN
       v_Basis := v_leg.t_Basis;
       v_dc    := v_leg.t_InterestStart;
       v_N     := v_leg.t_Principal;
       v_C     := v_leg.t_Price / 1000000;
       if (rsb_bill.GetDaysInYearByBasis (v_Basis,v_dc,TRUE,v_B) != 0) THEN
          RAISE NO_DATA_FOUND;
       END IF;
       v_Sa2 := v_N + (v_N * v_C * (v_D - v_dc) / v_B);
       AddInArrTmp( p_CalcDate, 0, v_arrtmp );
       AddInArrTmp( v_D, v_Sa2, v_arrtmp );
     END IF;

     -- заполним временную таблицу
     InsertArrToXIRR(v_arrtmp);

   EXCEPTION
     WHEN NO_DATA_FOUND THEN NULL;
   END FillXIRR_AS_EPS;

   -- Расчет АС по ЭПС (для ЦБ(покупка), ОЭБ в ВН, для РЕПО в ВЦ)
   FUNCTION CalcAS_EPS(p_CalcKind  IN NUMBER,
                       p_DocID     IN NUMBER,
                       p_SumID     IN NUMBER,
                       p_CalcDate  IN DATE,
                       p_EPS       IN NUMBER) RETURN NUMBER
   IS
     v_asval       NUMBER  := 0;
     v_accuracy    INTEGER := ABS(RSB_COMMON.GetRegIntValue('SECUR\МСФО\ЭПС_ТОЧНОСТЬ_РЕЗУЛЬТАТА'));
   BEGIN
     IF (p_EPS != -1) and p_CalcDate <> UnknownDate THEN
       FillXIRR_AS_EPS(p_CalcKind, p_DocID, p_SumID, p_CalcDate, p_EPS);

       with t as
       (
         SELECT t_date as dt, t_sum as summ FROM DXIRR_TMP WHERE t_date >= p_CalcDate
       )
       SELECT decode(sum(rate), null, 0, sum(rate)) into v_asval
         FROM (SELECT *
                 FROM t
                model
         dimension by (row_number() over (order by dt) rn)
             measures (dt, summ s, 0 rate)
                rules (rate[any] = s[CV()] / power(1 + p_EPS, (dt[CV()] - p_CalcDate)/365))
              );
     END IF;

     RETURN round(v_asval, v_accuracy);
   END CalcAS_EPS;

  -- Комиссии, признанные как существенные на дату.
  FUNCTION GetEssentialComis( p_CalcKind IN NUMBER,
                              p_tick IN ddl_tick_dbt%rowtype,
                              p_leg IN ddl_leg_dbt%rowtype,
                              p_BegDate IN DATE,
                              p_EndDate IN DATE,
                              p_ToFI IN NUMBER default -1,
                              p_IsAmortized IN BOOLEAN default false,
                              p_t0 IN DATE default UnknownDate,
                              p_d2repo IN DATE default UnknownDate)
    RETURN NUMBER
  AS
    v_SignDeviation BOOLEAN := false;
    v_RateKind      NUMBER;
    v_RateVal       NUMBER;
    v_DoCompare     NUMBER  := 0;
    v_ToCompare     NUMBER  := 0;
    v_OGroup        NUMBER  := get_OperationGroup(get_OperSysTypes(p_tick.t_DealType, p_tick.t_BofficeKind));
    v_Portfolio     NUMBER  := case when IsREPO(v_OGroup)=1 then (case when p_CalcKind = CALCKIND_OREPO 
                                                                       then RSB_PMWRTOFF.KINDPORT_CURR_AC_PVO 
                                                                       else RSB_PMWRTOFF.KINDPORT_CURR_AC_BPP 
                                                                  end) 
                                    when p_CalcKind = CALCKIND_OWN then RSB_PMWRTOFF.KINDPORT_AC_OWN 
                                    else p_tick.t_PortfolioID 
                               end;
    v_com           NUMBER;
    v_S             NUMBER  := 0;
    v_FaceFI        NUMBER;
    v_DrawingDate   DATE;
    v_t0            DATE;
    v_tn            DATE;
    v_ToFI          NUMBER;
  BEGIN
    SELECT F.t_FaceValueFI, RSI_RSB_FIInstr.FI_GetNominalDrawingDate(F.t_FIID, (select t_Termless from davoiriss_dbt where t_FIID = F.t_FIID))
      INTO v_FaceFI, v_DrawingDate
      FROM DFININSTR_DBT F
     WHERE F.t_FIID = p_tick.t_PFI;

    FOR one_cm IN (SELECT dlcomis.t_Sum - dlcomis.t_NDS ComSum, comis.t_FIID_Comm Currency, GREATEST(dlcomis.t_PlanPayDate, dlcomis.t_FactPayDate) PayDate
                     FROM ddlcomis_dbt dlcomis, dsfcomiss_dbt comis
                    WHERE dlcomis.t_DocKind   = p_tick.t_BofficeKind
                      AND dlcomis.t_DocID     = p_tick.t_DealID
                      AND dlcomis.t_FeeType   = comis.t_FeeType
                      AND dlcomis.t_ComNumber = comis.t_Number
                      AND GREATEST(dlcomis.t_PlanPayDate, dlcomis.t_FactPayDate) BETWEEN p_BegDate and p_EndDate
                  )
    LOOP
       v_DoCompare := RSI_RSB_FIInstr.ConvSum( one_cm.ComSum, one_cm.Currency, RSI_RSB_FIInstr.NATCUR, one_cm.PayDate, 1 );
       v_ToCompare := RSI_RSB_FIInstr.ConvSum( p_leg.t_Cost, p_leg.t_CFI, RSI_RSB_FIInstr.NATCUR, one_cm.PayDate, 1 );

       IF GetEssentialDev(LEVELESSENTIAL_CONTRACTCOSTS, --Затраты по договору
                          v_Portfolio,
                          one_cm.PayDate,
                          v_DoCompare,
                          v_ToCompare,
                          0,
                          0,
                          0,
                          v_SignDeviation, -- Да/нет
                          v_RateKind,
                          v_RateVal
                         ) <> 0 THEN
          dbms_output.put_line('Ошибка: неверные параметры при вызове ф-ции проверки на существенность отклонения');
       END IF;

       IF v_SignDeviation = TRUE THEN
         IF p_ToFI != -1 THEN
           v_ToFI := p_ToFI;
         ELSIF IsREPO(v_OGroup)=1 then
           v_ToFI := p_leg.t_CFI;
           v_tn := p_d2repo;
         else
           v_ToFI := v_FaceFI;
           v_tn := v_DrawingDate;
         end if;

         v_com := RSI_RSB_FIInstr.ConvSum( one_cm.ComSum, one_cm.Currency, v_ToFI, one_cm.PayDate, 1 );
         IF p_IsAmortized = FALSE THEN
           v_S := v_S + v_com;
         ELSE
           v_t0 := GREATEST(one_cm.PayDate, p_t0);
           v_S := v_S + v_com * (p_EndDate - v_t0) / (v_tn - v_t0);
         END IF;
       END IF;
    END LOOP;

    IF p_CalcKind = CALCKIND_OWN THEN
       v_S := -v_S;
    END IF;

    RETURN v_S;
  END GetEssentialComis;


  FUNCTION CalcAS_EPS0(p_CalcKind  IN NUMBER,
                       p_DocID     IN NUMBER,
                       p_SumID     IN NUMBER,
                       p_CalcDate  IN DATE,
                       p_FairValue IN NUMBER) RETURN NUMBER
   IS
     v_ASt0          NUMBER  := 0;
     v_lot           dpmwrtsum_dbt%rowtype;
     v_tick          ddl_tick_dbt%rowtype;
     v_leg           ddl_leg_dbt%rowtype;
     v_leg2          ddl_leg_dbt%rowtype;
     v_rq            ddlrq_dbt%rowtype;
     v_OGroup        doprkoper_dbt.T_SYSTYPES%TYPE;
     v_D             DATE    := UnknownDate;
     v_PrevCalcDate  DATE;
     v_FaceValue     NUMBER;
     v_FaceValueFI   NUMBER;
     v_AmountLot     NUMBER := 0;
     v_AmountRate    NUMBER := 1;
     v_Amount        NUMBER := 0;
     v_SignDeviation BOOLEAN;
     v_RateKind      NUMBER;
     v_RateVal       NUMBER;
     v_CostRub       NUMBER;
     v_FairValueRub  NUMBER;
     v_DealSum       NUMBER;
     v_Portfolio     NUMBER;
     v_ToFI          NUMBER;
     v_NKD           NUMBER := 0;
     v_ExistsRetCoup NUMBER := 0;
     v_Kt            NUMBER := 0;
     v_Ret           NUMBER := 0;
     v_PlacedAmount  NUMBER := 0;
     v_FICom         NUMBER := 0;
     v_BioPFI        NUMBER := 0;

   BEGIN

     IF p_CalcKind IN (CALCKIND_AVR, CALCKIND_PREPO, CALCKIND_OREPO, CALCKIND_OWN) THEN
       SELECT * INTO v_tick
         FROM ddl_tick_dbt
        WHERE t_DealID = p_DocID;

       SELECT t_FaceValue, t_FaceValueFI INTO v_FaceValue, v_FaceValueFI
         FROM DFININSTR_DBT
        WHERE t_FIID = v_tick.t_PFI;

       SELECT * INTO v_leg
         FROM ddl_leg_dbt
        WHERE t_LegKind = 0
          AND t_DealID = p_DocID
          AND t_LegID = 0;

       IF p_SumID > 0 THEN
         SELECT * INTO v_Lot
           FROM DPMWRTSUM_DBT
          WHERE t_SumID = p_SumID;
       END IF;

       v_OGroup := get_OperationGroup(get_OperSysTypes(v_tick.t_DealType, v_tick.t_BofficeKind));

       IF p_SumID > 0 THEN
         v_AmountLot := GetWrtAmountOnDate(p_SumID, p_CalcDate);
         v_AmountRate := v_AmountLot / v_leg.t_Principal;
       END IF;

       IF p_CalcKind IN (CALCKIND_AVR, CALCKIND_OWN) THEN
         v_ToFI := v_FaceValueFI;
         -- дата поставки
         IF GetRQ(v_rq, v_tick.t_BofficeKind, v_tick.t_DealID, RSI_DLRQ.DLRQ_SUBKIND_AVOIRISS, 1, RSI_DLRQ.DLRQ_TYPE_DELIVERY) = 0 THEN
           v_D := GREATEST(v_rq.t_PlanDate, v_rq.t_FactDate);
         END IF;

       ELSIF p_CalcKind IN (CALCKIND_PREPO, CALCKIND_OREPO) THEN
         v_ToFI := v_leg.t_CFI;
         -- дата оплаты 1ч
         IF GetRQ(v_rq, v_tick.t_BofficeKind, v_tick.t_DealID, RSI_DLRQ.DLRQ_SUBKIND_CURRENCY, 1, RSI_DLRQ.DLRQ_TYPE_PAYMENT) = 0 THEN
           v_D := GREATEST(v_rq.t_PlanDate, v_rq.t_FactDate);
         END IF;
       END IF;

       IF p_CalcKind = CALCKIND_AVR THEN
         v_Portfolio := RSB_PMWRTOFF.KINDPORT_ASCB; -- АС_ЦБ
       ELSIF p_CalcKind = CALCKIND_OWN THEN
         v_Portfolio := RSB_PMWRTOFF.KINDPORT_AC_OWN; -- АС_ОЭБ
       END IF;

       -- Фактическая цена сделки (с учетом сущ. комиссий)
       v_DealSum := (RSI_RSB_FIInstr.ConvSum(v_leg.t_TotalCost, v_leg.T_CFI, v_ToFI, v_D, 1)
                  + GetEssentialComis(p_CalcKind, v_tick, v_leg, v_tick.t_DealDate, v_D, v_ToFI)) * v_AmountRate;

       IF p_CalcKind IN (CALCKIND_AVR, CALCKIND_OWN) THEN
         v_CostRub := RSI_RSB_FIInstr.ConvSum(v_leg.t_TotalCost, v_leg.T_CFI, RSI_RSB_FIInstr.NATCUR, v_D, 1) * v_AmountRate;
         v_FairValueRub := RSI_RSB_FIInstr.ConvSum(p_FairValue, v_FaceValueFI, RSI_RSB_FIInstr.NATCUR, v_D, 1);

         IF RSB_SECUR.GetEssentialDev(
                RSB_SECUR.LEVELESSENTIAL_FACTPRICE, -- Вид уровня существенности = Отклонение фактической цены
                v_Portfolio,
                v_D,
                v_CostRub,
                v_FairValueRub,
                0,
                0,
                0,
                v_SignDeviation, -- Да/нет
                v_RateKind,      -- Наименование ставки используемой в дальнейшем
                v_RateVal        -- Значение ставки
               ) <> 0 THEN
            dbms_output.put_line('Ошибка: неверные параметры при вызове ф-ции проверки на существенность отклонения');
         END IF;

         IF p_CalcKind = CALCKIND_AVR THEN
           IF v_Lot.T_PORTFOLIO = RSB_PMWRTOFF.KINDPORT_ASCB THEN
             IF v_SignDeviation = TRUE AND
                (RSB_PMWRTOFF.RSI_AvoirAttrHSDataLevel(v_Lot.T_FIID) IN (1, 2) OR
                 RSB_PMWRTOFF.RSI_AvoirAttrObsBaseData(v_Lot.T_FIID) = 0/*Да*/) THEN
               v_ASt0 := p_FairValue;
             ELSE
               v_ASt0 := v_DealSum;
             END IF;
           ELSIF v_Lot.T_PORTFOLIO = RSB_PMWRTOFF.KINDPORT_SSSD THEN
             v_ASt0 := v_DealSum;
           END IF;
         ELSE
           IF v_SignDeviation = TRUE AND
              (RSB_PMWRTOFF.RSI_AvoirAttrHSDataLevel(v_Lot.T_FIID) IN (1, 2) OR
               RSB_PMWRTOFF.RSI_AvoirAttrObsBaseData(v_Lot.T_FIID) = 0/*Да*/) THEN
             v_ASt0 := p_FairValue;
           ELSE
             v_ASt0 := v_DealSum;
           END IF;
         END IF;

         IF v_ASt0 = 0 THEN
           v_ASt0 := v_DealSum;
         END IF;
         
         IF p_CalcKind = CALCKIND_OWN THEN
           IF p_SumID > 0 THEN
             v_Amount := v_AmountLot;
           ELSE
             v_Amount := v_leg.t_Principal;
           END IF;

           v_PlacedAmount := RSB_PMWRTOFF.WRTGetAmountOwn( v_tick.t_Department, v_tick.t_PFI, p_CalcDate, 1, 0);

           IF p_SumID = 0 THEN
             v_PlacedAmount := v_PlacedAmount  + v_Amount;
           END IF; 

           IF v_PlacedAmount > 0 THEN 
             SELECT NVL(SUM(RSB_FIINSTR.CONVSUM(DLC.T_SUM, CM.T_FIID_COMM, v_FaceValueFI, DLC.T_PLANPAYDATE)), 0) INTO v_FICom
               FROM DDLCOMIS_DBT DLC, DSFCOMISS_DBT CM
              WHERE DLC.T_DOCKIND = DLDOC_ISSUE
                AND DLC.T_DOCID = v_tick.t_PFI
                AND DLC.T_STARTAMORTDATE > TO_DATE('01.01.0001','DD.MM.YYYY')
                AND DLC.T_STARTAMORTDATE <= p_CalcDate
                AND CM.T_FEETYPE = DLC.T_FEETYPE 
                AND CM.T_NUMBER = DLC.T_COMNUMBER;
           
             v_ASt0 := v_ASt0 - v_FICom * v_Amount / v_PlacedAmount;


             SELECT NVL(SUM(RSB_FIINSTR.CONVSUM(FR.T_ONEITEMFV, FR.T_CURRENCY, v_FaceValueFI, FR.T_DATE)), 0) INTO v_BioPFI
               FROM DBIOFRVAL_DBT FR
              WHERE FR.T_FIID = v_tick.t_PFI
                AND FR.T_DATE = (SELECT MAX(FR1.T_DATE)
                                   FROM DBIOFRVAL_DBT FR1
                                  WHERE FR1.T_FIID = FR.T_FIID
                                    AND FR1.T_DATE <= v_D
                                );

             v_ASt0 := v_ASt0 - v_BioPFI * v_Amount;
           END IF;
           
         END IF;

       ELSIF p_CalcKind IN (CALCKIND_PREPO, CALCKIND_OREPO) THEN
         IF v_Lot.t_AmortCalcKind = AMORTCALCKIND_EPS THEN
           v_ASt0 := v_DealSum;
         ELSIF v_Lot.t_AmortCalcKind = AMORTCALCKIND_RPS THEN
           IF (RSB_PMWRTOFF.RSI_AvoirAttrHSDataLevel(v_Lot.T_FIID) IN (1, 2) OR
               RSB_PMWRTOFF.RSI_AvoirAttrObsBaseData(v_Lot.T_FIID) = 0/*Да*/) THEN
             v_ASt0 := p_FairValue;
           ELSE
             v_ASt0 := v_DealSum;
           END IF;
         ELSIF v_ASt0 = 0 THEN
           v_ASt0 := v_DealSum;
         END IF;
       END IF;

     END IF;

     RETURN v_ASt0;

   END CalcAS_EPS0;


  --Суммы купонов/ЧП на дату
  FUNCTION GetCoupPart(p_FIID IN NUMBER, p_IsPartial IN NUMBER, p_BegDate IN DATE, p_EndDate IN DATE, p_Amount IN NUMBER)
    RETURN NUMBER
  IS
    v_FaceValue     NUMBER  := 0;
    v_S             NUMBER  := 0;
  BEGIN
    SELECT t_FaceValue INTO v_FaceValue
      FROM DFININSTR_DBT
     WHERE t_FIID = p_FIID;

    BEGIN
      SELECT NVL(SUM(CASE WHEN fw.t_IsPartial = CHR(0) THEN
                            ROUND(RSI_RSB_FIInstr.FI_CalcIncomeValue(fw.t_FIID, fw.t_DrawingDate, p_Amount, 1, 0, 0), fw.t_IncomePoint)
                          ELSE
                            p_Amount * (CASE WHEN fw.t_RelativeIncome = CHR(0) THEN
                                          fw.t_IncomeVolume
                                        ELSE
                                          ROUND(v_FaceValue * fw.t_IncomeRate / GREATEST(1, fw.t_IncomeScale) / 100, fw.t_IncomePoint)
                                        END)
                          END),0)
        INTO v_S
        FROM DFIWARNTS_DBT fw
       WHERE fw.t_FIID   = p_FIID
         AND (fw.t_DrawingDate BETWEEN p_BegDate AND p_EndDate)
         AND fw.t_IsPartial = case when p_IsPartial = 1 then CHR(88) else fw.t_IsPartial end;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        v_S := 0;
    END;
    RETURN v_S;
  END GetCoupPart;

  -- получение ставки на дату из истории
  FUNCTION GetOldIncomeRate( p_DealID IN NUMBER, p_D IN DATE, p_OldIncomeRate IN OUT NUMBER )
    RETURN BOOLEAN
  IS
    v_RetVal BOOLEAN := FALSE;
  BEGIN
    p_OldIncomeRate := 0.0;

    BEGIN
      SELECT s.t_OldIncomeRate INTO p_OldIncomeRate
        FROM dsptkchng_dbt s
       WHERE s.t_DealID = p_DealID
         AND s.t_OldChangeDate <= p_D
         AND s.t_OldInstance = ( SELECT MAX (s1.t_OldInstance)
                                   FROM dsptkchng_dbt s1
                                  WHERE s1.t_DealID = s.t_DealID
                                   AND s1.t_OldChangeDate <= p_D );
      v_RetVal := TRUE;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN v_RetVal := FALSE;
    END;

    RETURN v_RetVal;
  END GetOldIncomeRate;

  -- получение ставки
  FUNCTION GetIncomeRate( p_Leg2 IN ddl_leg_dbt%rowtype, p_D IN DATE, p_Deal IN ddl_tick_dbt%rowtype )
    RETURN NUMBER
  IS
    v_TempDate DATE;
    v_RetVal   NUMBER := 0;
  BEGIN
    v_TempDate := p_Deal.t_ChangeDate + 1;
    IF p_Deal.t_ChangeDate = UnknownDate OR p_D >= v_TempDate THEN
      v_RetVal := p_Leg2.t_IncomeRate;
    ELSE
      IF GetOldIncomeRate( p_Deal.t_DealID, p_D, v_RetVal ) = FALSE THEN
        v_RetVal := p_Leg2.t_IncomeRate;
      END IF;
    END IF;

    RETURN v_RetVal;
  END GetIncomeRate;

  -- определение кол-ва дней в году по базису расчета РЕПО
  FUNCTION GetNDaysByBasis( p_Basis IN NUMBER, p_Date IN DATE )
    RETURN NUMBER
  IS
  BEGIN
    IF p_Basis = SP_KINDBASISCALC_365_CAL OR p_Basis = SP_KINDBASISCALC_365_30 THEN
      return 365;
    ELSIF p_Basis = SP_KINDBASISCALC_360_30 OR p_Basis = SP_KINDBASISCALC_360_CAL THEN
      return 360;
    ELSE
      return RSI_RSB_FIInstr.FI_GetDaysInYear(TO_NUMBER(TO_CHAR(p_Date, 'YYYY')));
    END IF;
  END GetNDaysByBasis;

   -- Расчет АС ФИ линейным методом (для ЦБ(покупка), ОЭБ в ВН, для РЕПО в ВЦ)
   FUNCTION CalcAS_Line(p_CalcKind  IN NUMBER,
                        p_DocID     IN NUMBER,
                        p_SumID     IN NUMBER,
                        p_CalcDate  IN DATE) RETURN NUMBER
   IS
     v_OGroup       doprkoper_dbt.T_SYSTYPES%TYPE;
     v_tick         ddl_tick_dbt%rowtype;
     v_leg          ddl_leg_dbt%rowtype;
     v_leg2         ddl_leg_dbt%rowtype;
     v_rq           ddlrq_dbt%rowtype;
     v_lnk          dvsordlnk_dbt%rowtype;
     v_FaceFI       NUMBER;
     v_DrawingDate  DATE;
     v_AvoirKind    NUMBER;
     v_FaceValue    NUMBER;
     v_PaymentDate  DATE := UnknownDate;
     v_t0           DATE := UnknownDate;
     v_DealPrice    NUMBER;
     v_PreOutlay    NUMBER;
     v_ACt          NUMBER := 0;
     v_AC0          NUMBER := 0;
     v_Sc3i         NUMBER;
     v_St           NUMBER;
     v_Spdt         NUMBER := 0;
     v_Sddt         NUMBER := 0;
     v_Sbt          NUMBER := 0;
     v_C3t          NUMBER := 0;
     v_d2repo       DATE;
     v_Dn           DATE;
     v_SpDn         NUMBER := 0;
     v_B            NUMBER;
     v_Va           NUMBER;
     v_VaMinus1     NUMBER;
     v_rj           NUMBER;
     v_Ta           NUMBER;
     v_Amount       NUMBER;
     v_AmountLot    NUMBER := 0;
     v_AmountRate   NUMBER := 1;
     v_arrtmp       arrtmp_t := arrtmp_t();
     v_PaymentDateFromLeg DATE := UnknownDate;
   BEGIN
     IF p_CalcKind IN (CALCKIND_VS,CALCKIND_VA) THEN
       SELECT *
         INTO v_lnk
         FROM dvsordlnk_dbt lnk
        WHERE     LNK.T_CONTRACTID = p_DocID
              AND LNK.T_BCID = p_SumID --За неимением других параметров
              AND LNK.T_LINKKIND = CASE WHEN p_CalcKind = CALCKIND_VS THEN 0 ELSE 1 END;

       SELECT *
         INTO v_leg
         FROM ddl_leg_dbt leg
        WHERE     leg.t_LegKind = 1
              AND leg.t_DealID = v_lnk.t_BCID
              AND t_LegID = 0;
       if p_CalcKind = CALCKIND_VA THEN
         SELECT *
           INTO v_leg2
           FROM ddl_leg_dbt leg
          WHERE     leg.t_LegKind = 0
                AND leg.t_DealID = p_DocID
                AND t_LegID = 0;
       END IF;
     END IF;

     IF p_CalcKind IN (CALCKIND_AVR, CALCKIND_PREPO, CALCKIND_OREPO, CALCKIND_OWN) THEN
       SELECT * INTO v_tick
         FROM ddl_tick_dbt
        WHERE t_DealID = p_DocID;

       v_OGroup := get_OperationGroup(get_OperSysTypes(v_tick.t_DealType, v_tick.t_BofficeKind));

       SELECT * INTO v_leg
         FROM ddl_leg_dbt
        WHERE t_LegKind = 0
          AND t_DealID = p_DocID
          AND t_LegID = 0;

       SELECT F.t_FaceValueFI, RSI_RSB_FIInstr.FI_GetNominalDrawingDate(F.t_FIID, (select t_Termless from davoiriss_dbt where t_FIID = F.t_FIID)), F.t_AvoirKind, F.t_FaceValue
         INTO v_FaceFI, v_DrawingDate, v_AvoirKind, v_FaceValue
         FROM DFININSTR_DBT F
        WHERE F.t_FIID = v_tick.t_PFI;

       v_DealPrice := RSB_FIInstr.ConvSum(v_leg.t_Price, v_leg.t_CFI, v_FaceFI, v_tick.t_DealDate, 1);

       -- фактическая дата оплаты
       IF GetRQ(v_rq, v_tick.t_BofficeKind, v_tick.t_DealID, RSI_DLRQ.DLRQ_SUBKIND_CURRENCY, 1, RSI_DLRQ.DLRQ_TYPE_PAYMENT) = 0 THEN
         v_PaymentDate := GREATEST(v_rq.t_PlanDate, v_rq.t_FactDate);
       END IF;

       -- Дата первоначального признания ФИ
       IF GetRQ(v_rq, v_tick.t_BofficeKind, v_tick.t_DealID, RSI_DLRQ.DLRQ_SUBKIND_AVOIRISS, 1, RSI_DLRQ.DLRQ_TYPE_DELIVERY) = 0 THEN
         v_t0 := GREATEST(v_rq.t_PlanDate, v_rq.t_FactDate);
       END IF;

       v_Amount := v_leg.t_Principal;
       IF p_SumID > 0 THEN
         v_AmountLot := GetWrtAmountOnDate(p_SumID, p_CalcDate);
         v_AmountRate := v_AmountLot / v_Amount;
         v_Amount := v_AmountLot;
       END IF;
     END IF;

     IF p_CalcKind = CALCKIND_AVR THEN
       IF v_tick.t_BofficeKind = DL_SECURITYDOC and IsTwoPart(v_OGroup)=0 and IsBuy(v_OGroup)=1 THEN
         -- Начальная амортизированная стоимость:
         --   Стоимость покупки с учетом НКД
         v_AC0 := RSI_RSB_FIInstr.ConvSum(v_leg.t_TotalCost, v_leg.t_CFI, v_FaceFI, v_PaymentDate, 1 ) * v_AmountRate;
         --   Существенные затраты до даты первоначального признания ФИ включительно
         IF v_tick.t_PreOutlay <> 0 THEN
           v_PreOutlay := RSI_RSB_FIInstr.ConvSum(v_tick.t_PreOutlay, v_tick.t_PreOutlayFIID, v_FaceFI, v_tick.t_DealDate, 1 ) * v_AmountRate;
         END IF;
         v_AC0 := v_AC0 + v_PreOutlay + GetEssentialComis(p_CalcKind, v_tick, v_leg, UnknownDate, v_t0) * v_AmountRate;

         -- Существенные затраты, понесенные после первоначального признания до даты t
         v_Sc3i := GetEssentialComis(p_CalcKind, v_tick, v_leg, v_t0+1, p_CalcDate) * v_AmountRate;

         -- Величина поступивших и/или ожидаемых средств частичному погашению за период [t0:t]
         v_St := GetCoupPart(v_tick.t_PFI, 1, v_t0, p_CalcDate, v_Amount);

         -- Сумма начисляемого процентного дохода за период [t0;t]
         IF RSI_RSB_FIInstr.FI_AvrKindsGetRoot( 2, v_AvoirKind ) = RSI_RSB_FIInstr.AVOIRKIND_BOND THEN
           BEGIN
             SELECT ROUND(RSI_RSB_FIInstr.CalcNKD_Ex(T_FIID, p_CalcDate, T_AMOUNT, 1), 2)
                    INTO v_Spdt
               FROM dpmwrtsum_dbt
              WHERE t_DealID = p_DocID
                AND rownum = 1;
           EXCEPTION
             WHEN NO_DATA_FOUND THEN v_Spdt := 0;
           END;
         END IF;

         IF v_DealPrice <> v_FaceValue THEN
           -- Сумма начисляемого дисконтного дохода за период [t0;t]
           IF v_DealPrice < v_FaceValue THEN
             BEGIN
               SELECT ROUND(RSB_PMWRTOFF.WRTCalcDiscountIncomeOnDate(v_t0, p_CalcDate, t_BegDiscountDate, T_FIID, t_BegDiscount, 1, RSB_PMWRTOFF.GetAmortizationMethod(t_Party, t_Contract)), 2)
                      INTO v_Sddt
                 FROM dpmwrtsum_dbt
                WHERE t_DealID = p_DocID
                  AND rownum = 1;
             EXCEPTION
               WHEN NO_DATA_FOUND THEN v_Sddt := 0;
             END;
           -- Сумма начисляемой премии за период [t0;t]
           ELSE
             BEGIN
               SELECT ROUND(RSB_PMWRTOFF.WRTCalcBonusOnDate(v_t0, p_CalcDate, t_BegBonusDate, T_FIID, t_BegBonus, 1, RSB_PMWRTOFF.GetAmortizationMethod(t_Party, t_Contract)), 2)
                      INTO v_Sbt
                 FROM dpmwrtsum_dbt
                WHERE t_DealID = p_DocID
                  AND rownum = 1;
             EXCEPTION
               WHEN NO_DATA_FOUND THEN v_Sbt := 0;
             END;
           END IF;
         END iF;

         -- Сумма самортизированных затрат на дату t
         v_C3t := (v_PreOutlay*v_AmountRate) * (p_CalcDate - v_t0) / (v_DrawingDate - v_t0);
         v_C3t := v_C3t + GetEssentialComis(p_CalcKind, v_tick, v_leg, UnknownDate, p_CalcDate, -1, true, v_t0) * v_AmountRate;

         -- Амортизированная стоимость на заданную дату t
         v_ACt := v_AC0 + v_Sc3i - v_St + v_Spdt + v_Sddt - v_Sbt - v_C3t;
       END IF;

     -- ОЭБ
     ELSIF p_CalcKind = CALCKIND_OWN THEN
       IF v_tick.t_BofficeKind IN (DL_SECUROWN, DL_AVRWRTOWN) THEN
         -- Начальная амортизированная стоимость:
         --   Стоимость продажи с учетом НКД
         v_AC0 := RSI_RSB_FIInstr.ConvSum(v_leg.t_TotalCost, v_leg.t_CFI, v_FaceFI, v_PaymentDate, 1 ) * v_AmountRate;
         --   Существенные затраты до даты первоначального признания ФИ включительно
         v_AC0 := v_AC0 + GetEssentialComis(p_CalcKind, v_tick, v_leg, UnknownDate, v_t0) * v_AmountRate;

         -- Существенные затраты, понесенные после первоначального признания до даты t
         v_Sc3i := GetEssentialComis(p_CalcKind, v_tick, v_leg, v_t0+1, p_CalcDate) * v_AmountRate;

         -- Величина уплаченных и/или предстоящих платежей по оплате частичных погашений и выплаты купонов за период [t0:t]
         v_St := GetCoupPart(v_tick.t_PFI, 1, v_t0, p_CalcDate, v_Amount);

         -- Сумма начисляемого процентного дохода за период [t0;t]
         IF RSI_RSB_FIInstr.FI_AvrKindsGetRoot( 2, v_AvoirKind ) = RSI_RSB_FIInstr.AVOIRKIND_BOND THEN
           BEGIN
             SELECT ROUND(RSI_RSB_FIInstr.CalcNKD_Ex(T_FIID, p_CalcDate, T_AMOUNT, 1), 2)
                    INTO v_Spdt
               FROM dpmwrtsum_dbt
              WHERE t_DealID = p_DocID
                AND rownum = 1;
           EXCEPTION
             WHEN NO_DATA_FOUND THEN v_Spdt := 0;
           END;
         END IF;

         IF v_DealPrice <> v_FaceValue THEN
           -- Сумма начисляемого дисконтного дохода за период [t0;t]
           IF v_DealPrice < v_FaceValue THEN
             BEGIN
               SELECT ROUND(RSB_PMWRTOFF.WRTCalcDiscountExpOwnOnDate(p_CalcDate, t_BegDiscountDate, T_FIID, t_BegDiscount), 2)
                      INTO v_Sddt
                 FROM dpmwrtsum_dbt
                WHERE t_DealID = p_DocID
                  AND rownum = 1;
             EXCEPTION
               WHEN NO_DATA_FOUND THEN v_Sddt := 0;
             END;
           -- Сумма списываемой премии за период [t0;t]
           ELSE
             BEGIN
               SELECT ROUND(RSB_PMWRTOFF.WRTCalcBonusOwnOnDate(p_CalcDate, t_BegBonusDate, T_FIID, t_BegBonus), 2)
                      INTO v_Sbt
                 FROM dpmwrtsum_dbt
                WHERE t_DealID = p_DocID
                  AND rownum = 1;
             EXCEPTION
               WHEN NO_DATA_FOUND THEN v_Sbt := 0;
             END;
           END IF;
         END iF;

         -- Сумма самортизированных затрат на дату t
         v_C3t := GetEssentialComis(p_CalcKind, v_tick, v_leg, UnknownDate, p_CalcDate, -1, true, v_t0) * v_AmountRate;

         -- Амортизированная стоимость на заданную дату t
         v_ACt := v_AC0 + v_Sc3i - v_St + v_Spdt + v_Sddt - v_Sbt - v_C3t;
       END IF;

     -- ПРЕПО/ОРЕПО
     ELSIF p_CalcKind IN (CALCKIND_PREPO, CALCKIND_OREPO) THEN
       IF IsRepo(v_OGroup)=1 THEN
         SELECT * INTO v_leg2
           FROM ddl_leg_dbt
          WHERE t_LegKind = 2
            AND t_DealID = p_DocID
            AND t_LegID = 0;

         -- 2ч по сделке РЕПО
        IF GetRQ(v_rq, v_tick.t_BofficeKind, v_tick.t_DealID, RSI_DLRQ.DLRQ_SUBKIND_CURRENCY, 2, RSI_DLRQ.DLRQ_TYPE_PAYMENT) = 0 THEN
          v_d2repo := GREATEST(v_rq.t_PlanDate, v_rq.t_FactDate);
        END IF;
        IF GetRQ(v_rq, v_tick.t_BofficeKind, v_tick.t_DealID, RSI_DLRQ.DLRQ_SUBKIND_AVOIRISS, 2, RSI_DLRQ.DLRQ_TYPE_DELIVERY) = 0 THEN
          v_d2repo := GREATEST(v_d2repo, GREATEST(v_rq.t_PlanDate, v_rq.t_FactDate));
        END IF;

         -- Начальная амортизированная стоимость = V_0 - СЗ_0
         v_AC0 := v_leg.t_TotalCost * v_AmountRate;
         --   Существенные затраты до даты первоначального признания ФИ включительно
         IF v_tick.t_PreOutlay <> 0 THEN
           v_PreOutlay := RSI_RSB_FIInstr.ConvSum(v_tick.t_PreOutlay, v_tick.t_PreOutlayFIID, v_leg.t_CFI, v_tick.t_DealDate, 1 ) * v_AmountRate;
         END IF;
         v_AC0 := v_AC0 - v_PreOutlay - GetEssentialComis(p_CalcKind, v_tick, v_leg, UnknownDate, v_t0) * v_AmountRate;

         -- Существенные затраты, понесенные после первоначального признания до даты t
         v_Sc3i := GetEssentialComis(p_CalcKind, v_tick, v_leg, v_t0+1, p_CalcDate) * v_AmountRate;

         -- Ожидаемые/Полученные контрагентом денежные потоки меняющие сумму займа
         AddCompPaymRepoInArrTmp(v_arrtmp, v_tick, v_t0, p_CalcDate, v_Leg.t_CFI, v_AmountRate, v_St);
         v_arrtmp.delete;
         IF v_tick.t_Flag5 = 'X' AND v_tick.t_ReturnIncomeKind = 2/*SP_RETURNINCOME_WRTOFF*/ THEN
           v_St := v_St + GetCoupPart(v_tick.t_PFI, 0, v_t0, p_CalcDate, v_Amount);
         END IF;

         -- Сумма начисляемого процентного расхода за период [t0;t]
         BEGIN
           SELECT s.t_Date, s.t_Sum INTO v_Dn, v_SpDn
             FROM ddlsum_dbt s
            WHERE s.t_DocKind = v_tick.t_BofficeKind
              AND s.t_DocID = v_tick.t_DealID
              AND s.t_Kind = DLSUM_SUM_TO_PERCENT_CFI
              AND s.T_DATE = (SELECT MAX (T_DATE)
                                FROM ddlsum_dbt
                               WHERE t_DocKind = s.t_DocKind
                                 AND t_DocID = s.t_DocID
                                 AND t_Kind = DLSUM_SUM_TO_PERCENT_CFI
                                 AND T_DATE <= p_CalcDate)
              AND ROWNUM = 1;
         EXCEPTION
           WHEN NO_DATA_FOUND THEN v_Dn := UnknownDate; v_SpDn := 0;
         END;

         IF v_Dn = v_t0 THEN
           v_SpDn := 0;
         END IF;
         v_Spdt := v_SpDn;

         IF v_Dn <> UnknownDate THEN
           IF v_tick.t_Flag5 = 'X' AND v_tick.t_ReturnIncomeKind = 2/*SP_RETURNINCOME_WRTOFF*/ THEN
             -- Сумма займа действующая на дату Дн
             IF GetRQ(v_rq, v_tick.t_BofficeKind, v_tick.t_DealID, RSI_DLRQ.DLRQ_SUBKIND_CURRENCY, 1, RSI_DLRQ.DLRQ_TYPE_PAYMENT) = 0 THEN
               v_VaMinus1 := GetSumRqHistRepo(v_rq, v_Dn) * v_AmountRate;
               v_Va := v_VaMinus1;
             END IF;

              -- нужно учесть аванс по 2ч
             IF GetRQ(v_rq, v_tick.t_BofficeKind, v_tick.t_DealID, RSI_DLRQ.DLRQ_SUBKIND_CURRENCY, 2, RSI_DLRQ.DLRQ_TYPE_AVANCE) <> 0 THEN
               v_rq := null;
             END IF;

             AddFiWarntsInArrTmp(v_arrtmp, v_tick.t_PFI, v_Dn, p_CalcDate, v_Amount, v_AmountRate, true, v_rq);
             FOR i IN v_arrtmp.First .. v_arrtmp.Last LOOP
               v_B := GetNDaysByBasis( v_leg.t_Basis, v_arrtmp(i).t_Date );
               v_rj := GetIncomeRate( v_Leg2, v_arrtmp(i).t_Date, v_tick ) / 100;

               v_Va := v_VaMinus1 - v_arrtmp(i).t_Sum;
               v_Ta := (v_arrtmp(i).t_Date - v_Dn) / v_B;

               v_Spdt := v_Spdt + v_Va * v_Ta * v_rj;

               v_VaMinus1 := v_Va;
               v_Dn := v_arrtmp(i).t_Date;
             END LOOP;
             -- включая дату t
             IF v_Dn < p_CalcDate THEN
               v_B := GetNDaysByBasis( v_leg.t_Basis, p_CalcDate );
               v_rj := GetIncomeRate( v_Leg2, p_CalcDate, v_tick ) / 100;
               v_Ta := (p_CalcDate - v_Dn) / v_B;
               v_Spdt := v_Spdt + v_Va * v_Ta * v_rj;
             END IF;

             IF v_Leg2.t_IncomeRate < 0 THEN
               v_Spdt := -v_Spdt;
             END IF;
           END IF;
         END IF;

         -- Сумма самортизированных затрат на дату t
         v_C3t := (v_PreOutlay*v_AmountRate) * (p_CalcDate - v_t0) / (v_d2repo - v_t0);
         v_C3t := v_C3t + GetEssentialComis(p_CalcKind, v_tick, v_leg, UnknownDate, p_CalcDate, -1, true, v_t0, v_d2repo) * v_AmountRate;

         -- Амортизированная стоимость на заданную дату t
         v_ACt := v_AC0 - v_Sc3i - v_St + v_Spdt + v_C3t;
       END IF;
     ELSIF p_CalcKind = CALCKIND_VA THEN
       IF rsb_bill.GetDiscountOnDateVA(v_leg.t_ID, v_LNK.T_CONTRACTID, p_CalcDate, v_Sddt) != 0 THEN
          RAISE NO_DATA_FOUND;
       END IF;

       IF rsb_bill.GetBonusOnDateVA(v_leg.t_ID, p_CalcDate, v_Sbt) != 0 THEN
          RAISE NO_DATA_FOUND;
       END IF;

       IF rsb_bill.GetPrecentOnDate(v_leg.t_ID, p_CalcDate, v_Spdt) != 0 THEN
          RAISE NO_DATA_FOUND;
       END IF;

       v_PaymentDateFromLeg := rsb_bill.GetBNRFirstDate(v_lnk.t_BCID, v_lnk.T_CONTRACTID);

       v_AC0 := RSI_RSB_FIInstr.ConvSum(v_lnk.T_BCCOST, v_lnk.T_BCCFI, v_leg.t_PFI, v_PaymentDateFromLeg, 2 );
       v_ACt := v_Sddt - v_Sbt + v_Spdt + v_AC0;
     ELSIF p_CalcKind = CALCKIND_VS THEN
       v_PaymentDateFromLeg := rsb_bill.GetBNRFirstDate(v_lnk.t_BCID, v_lnk.T_CONTRACTID);

       IF rsb_bill.GetDiscountOnDateVS(v_leg.t_ID, v_LNK.T_CONTRACTID, p_CalcDate, v_Sddt) != 0 THEN
          RAISE NO_DATA_FOUND;
       END IF;

       IF rsb_bill.GetPrecentOnDate(v_leg.t_ID, p_CalcDate, v_Spdt) != 0 THEN
          RAISE NO_DATA_FOUND;
       END IF;

       v_AC0 := RSI_RSB_FIInstr.ConvSum(v_lnk.T_BCCOST, v_lnk.T_BCCFI, v_leg.t_PFI, v_PaymentDateFromLeg, 2 );
       v_ACt := v_Sddt + v_Spdt + v_AC0;
     END IF;

     RETURN v_ACt;
   END CalcAS_Line;

  -- Сумма полученного купона
  FUNCTION GetIncomeRetCoupon( pFIID IN NUMBER, pCalcDate IN DATE, pAmount IN NUMBER )
    RETURN NUMBER
  IS
    v_NKDCourse NUMBER := 0;
    v_Income NUMBER := 0;
  BEGIN

    v_NKDCourse := RSI_RSB_FIInstr.FindNKDCource(pFIID, pCalcDate);

    select nvl((select (CASE WHEN v_NKDCourse > 0 THEN ROUND(v_NKDCourse*pAmount,2) ELSE ROUND(RSI_RSB_FIInstr.FI_CalcIncomeValue(fw.t_FIID, fw.t_DrawingDate, pAmount, 1, 0, 0), fw.t_IncomePoint) END)
                  from dfiwarnts_dbt fw
                 where FW.T_ID = (select t_ID
                                    from dfiwarnts_dbt
                                   where t_FIID = pFIID
                                     and t_IsPartial = chr(0)
                                     and t_SPIsClosed = chr(88)
                                     and t_DrawingDate = pCalcDate
                                     and rownum = 1)
              ), 0) into v_Income from dual;
    RETURN v_Income;
  END GetIncomeRetCoupon;

  -- Расчет корректировки % до ЭПС (для ЦБ(покупка), ОЭБ в ВН, для РЕПО в ВЦ)
  FUNCTION CalcCorrectPersentEPS(p_CalcKind IN NUMBER,
                                 p_DocID IN NUMBER,
                                 p_SumID IN NUMBER,
                                 p_CalcDate IN DATE,
                                 p_PrevCalcDate IN DATE,
                                 p_InterestIncome IN NUMBER,
                                 p_Bonus IN NUMBER,
                                 p_DiscountIncome IN NUMBER,
                                 p_Outlay IN NUMBER,
                                 p_FairValue IN NUMBER,
                                 p_EPS IN NUMBER) RETURN NUMBER
   IS
     v_ASt           NUMBER  := 0;
     v_ASt0          NUMBER  := 0;
     v_Vt            NUMBER  := 0;
     v_bnr           dvsbanner_dbt%rowtype;
     v_lot           dpmwrtsum_dbt%rowtype;
     v_tick          ddl_tick_dbt%rowtype;
     v_leg           ddl_leg_dbt%rowtype;
     v_leg2          ddl_leg_dbt%rowtype;
     v_rq            ddlrq_dbt%rowtype;
     v_lnk           dvsordlnk_dbt%rowtype;
     v_OGroup        doprkoper_dbt.T_SYSTYPES%TYPE;
     v_D             DATE    := UnknownDate;
     v_PrevCalcDate  DATE;
     v_FaceValue     NUMBER;
     v_FaceValueFI   NUMBER;
     v_AmountLot     NUMBER := 0;
     v_AmountRate    NUMBER := 1;
     v_DealSum       NUMBER;
     v_Portfolio     NUMBER;
     v_ToFI          NUMBER;
     v_InterestIncome NUMBER := p_InterestIncome;
     v_Outlay        NUMBER := p_Outlay;
     v_NKD           NUMBER := 0;
     v_ExistsRetCoup NUMBER := 0;
     v_Kt            NUMBER := 0;
     v_Ret           NUMBER := 0;
     v_CostPFI       NUMBER := 0;
     v_FICom         NUMBER := 0;
     v_AmortFICom    NUMBER := 0;
     v_AmortFIComTmp NUMBER := 0;
     v_PlacedAmount  NUMBER := 0;
     v_Rep                  DATE    := UnknownDate;
   BEGIN

     IF p_CalcKind IN (CALCKIND_VS,CALCKIND_VA) THEN
       SELECT *
         INTO v_lnk
         FROM dvsordlnk_dbt lnk
        WHERE     LNK.T_CONTRACTID = p_DocID
              AND LNK.T_BCID = p_SumID --За неимением других параметров
              AND LNK.T_LINKKIND = CASE WHEN p_CalcKind = CALCKIND_VS THEN 0 ELSE 1 END;

       SELECT *
         INTO v_leg
         FROM ddl_leg_dbt leg
        WHERE     leg.t_LegKind = 1
              AND leg.t_DealID = v_lnk.t_BCID
              AND t_LegID = 0;

        SELECT *
          INTO v_bnr
          FROM dvsbanner_dbt bnr
         WHERE     bnr.t_BCID = v_lnk.t_BCID;

         IF (p_CalcKind = CALCKIND_VA) THEN
            SELECT *
              INTO v_leg2
              FROM ddl_leg_dbt leg
             WHERE     leg.t_LegKind = 0
                   AND leg.t_DealID = p_DocID
                   AND t_LegID = 0;
         END IF;
     END IF;

     IF p_CalcKind IN (CALCKIND_AVR, CALCKIND_PREPO, CALCKIND_OREPO, CALCKIND_OWN) THEN
       SELECT * INTO v_tick
         FROM ddl_tick_dbt
        WHERE t_DealID = p_DocID;

       SELECT t_FaceValue, t_FaceValueFI INTO v_FaceValue, v_FaceValueFI
         FROM DFININSTR_DBT
        WHERE t_FIID = v_tick.t_PFI;

       SELECT * INTO v_leg
         FROM ddl_leg_dbt
        WHERE t_LegKind = 0
          AND t_DealID = p_DocID
          AND t_LegID = 0;

       IF p_SumID > 0 THEN
         SELECT * INTO v_Lot
           FROM DPMWRTSUM_DBT
          WHERE t_SumID = p_SumID;
       END IF;

       v_OGroup := get_OperationGroup(get_OperSysTypes(v_tick.t_DealType, v_tick.t_BofficeKind));

       IF p_SumID > 0 THEN
         v_AmountLot := GetWrtAmountOnDate(p_SumID, p_CalcDate);
         v_AmountRate := v_AmountLot / v_leg.t_Principal;
       END IF;

       IF p_CalcKind IN (CALCKIND_AVR, CALCKIND_OWN) THEN
         v_ToFI := v_FaceValueFI;
         -- дата поставки
         IF GetRQ(v_rq, v_tick.t_BofficeKind, v_tick.t_DealID, RSI_DLRQ.DLRQ_SUBKIND_AVOIRISS, 1, RSI_DLRQ.DLRQ_TYPE_DELIVERY) = 0 THEN
           v_D := GREATEST(v_rq.t_PlanDate, v_rq.t_FactDate);
         END IF;

       ELSIF p_CalcKind IN (CALCKIND_PREPO, CALCKIND_OREPO) THEN
         v_ToFI := v_leg.t_CFI;
         -- дата оплаты 1ч
         IF GetRQ(v_rq, v_tick.t_BofficeKind, v_tick.t_DealID, RSI_DLRQ.DLRQ_SUBKIND_CURRENCY, 1, RSI_DLRQ.DLRQ_TYPE_PAYMENT) = 0 THEN
           v_D := GREATEST(v_rq.t_PlanDate, v_rq.t_FactDate);
         END IF;
       END IF;

       -- Если функция запускается в дату t=дата ПП, то расчет не производить
       IF p_CalcDate = v_D THEN
         return 0;
       END IF;


       -- Рассчитываем АС ЭПС на дату t
       v_ASt := CalcAS_EPS(p_CalcKind, p_DocID, p_SumID, p_CalcDate, p_EPS);

       v_ASt0 := CalcAS_EPS0(p_CalcKind, p_DocID, p_SumID, p_CalcDate, p_FairValue);

       -- НКД уплаченный при покупке
       IF p_CalcKind = CALCKIND_AVR THEN
         BEGIN
           SELECT 1 INTO v_ExistsRetCoup
             FROM DFIWARNTS_DBT fw
            WHERE fw.t_FIID   = v_tick.t_PFI
              AND fw.t_IsPartial = CHR(0)
              AND fw.t_SPIsClosed = CHR(88)
              AND fw.t_DrawingDate > v_D
              AND fw.t_DrawingDate <= p_CalcDate
              AND ROWNUM = 1;
         EXCEPTION
           WHEN NO_DATA_FOUND THEN v_ExistsRetCoup := 0;
         END;

         IF v_ExistsRetCoup = 1 THEN
           v_NKD := RSI_RSB_FIInstr.ConvSum(v_leg.t_NKD, v_leg.T_NKDFIID, v_ToFI, v_D, 1) * v_AmountRate;
         END IF;
       END IF;

     END IF;

     -- ЦБ (покупка), ОЭБ
     IF p_CalcKind IN (CALCKIND_AVR, CALCKIND_OWN) THEN
       -- Определяем Vt
       BEGIN
         SELECT NVL(sum(v_AmountLot * (CASE WHEN fw.t_RelativeIncome = CHR(0) THEN
                                         fw.t_IncomeVolume
                                       ELSE
                                         ROUND(v_FaceValue * fw.t_IncomeRate / GREATEST(1, fw.t_IncomeScale) / 100, fw.t_IncomePoint)
                                       END)
                       ),0) t_Sum INTO v_Vt
           FROM DFIWARNTS_DBT fw
          WHERE fw.t_FIID   = v_tick.t_PFI
            AND fw.t_IsPartial = CHR(88)
            AND fw.t_SPIsClosed = CHR(88)
            AND fw.t_DrawingDate >= v_D
            AND fw.t_DrawingDate < p_CalcDate;
       EXCEPTION
         WHEN NO_DATA_FOUND THEN v_Vt := 0;
       END;

       v_Kt := GetIncomeRetCoupon(v_tick.t_PFI, p_CalcDate, v_AmountLot);

       v_CostPFI := 0;
       IF p_SumID > 0 THEN
         v_CostPFI := v_Lot.T_COSTPFI;
       END IF;

       IF p_CalcKind = CALCKIND_OWN THEN
         
         v_PlacedAmount := RSB_PMWRTOFF.WRTGetAmountOwn( v_tick.t_Department, v_tick.t_PFI, p_CalcDate, 1, 0);   

         IF v_PlacedAmount > 0 THEN

           --v_FICom вроде как получается не нужным, т.к. комиссии учлись в ЭПС и АС0. Поэтому не заполняем

           SELECT NVL(SUM(RSB_FIINSTR.CONVSUM(S.T_SUM + S.T_NDS, S.T_CURRENCY, v_FaceValueFI, DLC.T_PLANPAYDATE)), 0) INTO v_AmortFICom
             FROM DDLSUM_DBT S, DDLCOMIS_DBT DLC
            WHERE DLC.T_DOCKIND = DLDOC_ISSUE
              AND DLC.T_DOCID = v_tick.t_PFI
              AND S.T_DOCKIND = DL_SECURITYCOM
              AND S.T_DOCID = DLC.T_ID
              AND S.T_DATE <= p_CalcDate;

           SELECT NVL(SUM(RSB_FIINSTR.CONVSUM(S.T_SUM + S.T_NDS, S.T_CURRENCY, v_FaceValueFI, DLC.T_PLANPAYDATE)), 0) INTO v_AmortFIComTmp
             FROM DDLSUM_TMP S, DDLCOMIS_DBT DLC
            WHERE DLC.T_DOCKIND = DLDOC_ISSUE
              AND DLC.T_DOCID = v_tick.t_PFI
              AND S.T_DOCKIND = DL_SECURITYCOM
              AND S.T_DOCID = DLC.T_ID
              AND S.T_DATE = p_CalcDate;

              v_AmortFICom := ROUND((v_AmortFICom+v_AmortFIComTmp) * v_AmountLot / v_PlacedAmount, 2);
           
         END IF;
         
         v_Outlay := v_Outlay + v_AmortFICom;
         
         v_Outlay := -v_Outlay;
       END IF;

       -- Рассчитываем значение Корректировка%_ЭПС
       v_Ret := (v_ASt - v_ASt0 + v_Vt - v_Kt - v_CostPFI - v_FICom) - p_InterestIncome - p_DiscountIncome + p_Bonus + v_Outlay + v_NKD;

     -- ПРЕПО/ОРЕПО
     ELSIF p_CalcKind IN (CALCKIND_PREPO, CALCKIND_OREPO) THEN
       -- расход
       /*IF (v_leg.t_IncomeRate > 0.0 AND IsSale(v_OGroup)=1) OR
          (v_leg.t_IncomeRate < 0.0 AND IsBuy(v_OGroup)=1) THEN
         v_InterestIncome := -p_InterestIncome;
       END IF;*/

       -- Рассчитываем значение Корректировка%_ЭПС
       v_Ret := (v_ASt - v_ASt0) - v_InterestIncome + p_Outlay;

     ELSIF p_CalcKind in (CALCKIND_VS,CALCKIND_VA) THEN
       v_D := rsb_bill.GetBNRFirstDate(v_lnk.t_BCID, v_lnk.T_CONTRACTID);
       -- Если функция запускается в дату ПП, то расчет не производить
       IF p_CalcDate = v_D THEN
         return 0;
       END IF;
       if (rsb_bill.GetBnrPlanRepayDate (v_leg.t_ID, v_Rep) != 0) THEN
          RAISE NO_DATA_FOUND;
        END IF;
       v_ASt := CalcAS_EPS(p_CalcKind, p_DocID, p_SumID, (case when p_CalcDate > v_Rep then v_Rep else p_CalcDate end), p_EPS);
       IF v_leg.t_principaldiff = AMORTCALCKIND_EPS then
         v_ASt0 := RSI_RSB_FIInstr.ConvSum(v_lnk.T_BCCOST, v_lnk.T_BCCFI, v_leg.t_PFI, p_CalcDate, 2 );
       ELSIF v_leg.t_principaldiff = AMORTCALCKIND_RPS then
         IF (v_leg.t_PayRegTax = 'X') THEN
           v_ASt0 := RSI_RSB_FIInstr.ConvSum(p_FairValue, RSI_RSB_FIInstr.NATCUR, v_leg.t_PFI, v_D, 2);
         ELSE
           v_ASt0 := RSI_RSB_FIInstr.ConvSum(v_lnk.T_BCCOST, v_lnk.T_BCCFI, v_leg.t_PFI, p_CalcDate, 2 );
         END IF;
       END IF;
       v_Ret := (v_ASt - v_ASt0) - p_InterestIncome - p_DiscountIncome + p_Bonus + p_Outlay;
     END IF;

     RETURN v_Ret;
   END CalcCorrectPersentEPS;

  --Провести тест на рыночность для сделки размещения ОЭБ - результат возвращается
  FUNCTION CalcExecDealMarketTestOwn(p_DealID IN NUMBER, p_OnDate IN DATE) RETURN NUMBER
  IS
    v_tick DDL_TICK_DBT%ROWTYPE;
    v_fin  DFININSTR_DBT%ROWTYPE;
    v_avr  DAVOIRISS_DBT%ROWTYPE;

    v_ObjType   NUMBER := 0;
    v_DealObjID DOBJATCOR_DBT.T_OBJECT%TYPE;

    v_CircPeriodDays NUMBER := 0; --Срок обращения ц/б в днях

    v_MarketRateMin NUMBER   := 0; --РПСмин
    v_MarketRateMax NUMBER   := 0; --РПСмакс
    v_SourceDataLayer NUMBER := 0; --Уровень исходных данных

    v_EIR NUMBER := 0; --ЭПС

    v_SignDeviation BOOLEAN := FALSE; --Отклонение существенно?
    v_TestAttrID    NUMBER := 2; --Значение для категории "Тест на рыночность пройден". По умолчанию Нет

    v_RateKind NUMBER;
    v_RateVal NUMBER;

    v_EPS_Med NUMBER := Rsb_Common.GetRegIntValue('SECUR\МСФО\МЕТОД_РАСЧЕТА_ОТКЛ_ЭПС', 0);
    v_Val1 NUMBER;
    v_Val2 NUMBER;
    v_Val3 NUMBER;
    v_DrawingDate DATE;
  BEGIN

    SELECT * INTO v_tick
      FROM ddl_tick_dbt
     WHERE t_DealID = p_DealID;

    SELECT * INTO v_fin
      FROM dfininstr_dbt
     WHERE t_FIID = v_tick.t_PFI;

    SELECT * INTO v_avr
      FROM davoiriss_dbt
     WHERE t_FIID = v_fin.t_FIID;

    v_DrawingDate := RSI_RSB_FIInstr.FI_GetNominalDrawingDate(v_fin.t_FIID, v_avr.t_Termless);

    v_CircPeriodDays := v_DrawingDate - p_OnDate + 1;
    v_ObjType   := GetDealObjType(v_tick.t_BOfficeKind);
    v_DealObjID := LPAD(v_tick.t_DealID, 34, '0');

    IF v_CircPeriodDays > 0 AND v_ObjType > 0 THEN
      --определяется  ЭПС
      v_EIR := CalcEPS(CALCKIND_OWN, p_DealID, 0, p_OnDate);

      --Определяется минимальное и максимальное значение РПС (с учетом настройки SECUR\МСФО\МЕТОД_РАСЧЕТА_ОТКЛ_ЭПС)
      IF GetRPS_LevelDevEPS
         ( CALCKIND_OWN,                   -- Вид расчета
           1,                              -- Вид обслуживания - Фондовый дилинг
           v_tick.t_Department,            -- Подразделение ТС - ГО
           57,                             -- Вид банковского продукта - Привлечение ОЭБ
           0,                              -- Банковский продукт -не заполняем
           v_CircPeriodDays,               -- Срок в днях
           v_fin.t_FaceValue*v_avr.t_Qty,  -- Сумма договора - объем эмиссии (произведение номинала на количество ценных бумаг в выпуске)
           v_fin.t_FaceValueFI,            -- Валюта договора - валюта номинала
           0,                              -- Тип клиента - все клиенты
           p_OnDate,                       -- Дата - дата расчета
           v_MarketRateMin,                -- РПСмин
           v_MarketRateMax,                -- РПСмакс
           v_SourceDataLayer               -- Уровень исходных данных
         ) = 0 THEN
        -- Алгоритм 2
        IF v_EPS_Med = ALG_CALCDEV_EPS2 THEN
          v_Val1 := v_MarketRateMax; -- РПСmax
          v_Val2 := v_EIR; -- ЭПС
          v_Val3 := v_avr.t_IncomeRate / 100; -- ставка по договору
        -- Алгоритм 1
        ELSE
          v_Val1 := v_EIR; -- значение ЭПС (ЭПС)
          v_Val2 := v_MarketRateMin; -- минимальное  значение РПС (РПСmin)
          v_Val3 := v_MarketRateMax; -- максимальное значение РПС (РПСmax)
        END IF;

        --Определяется существенность отклонения ЭПС
        IF GetEssentialDev(LEVELESSENTIAL_EPS -- Вид уровня существенности = Отклонение ЭПС
                          ,RSB_PMWRTOFF.KINDPORT_AC_OWN -- Портфель = АС_ОЭБ
                          ,p_OnDate        -- Дата определения значения уровня существенности
                          ,v_Val1          -- Что сравниваем
                          ,v_Val2          -- С чем сравниваем 1
                          ,v_Val3          -- С чем сравниваем 2
                          ,v_tick.t_BOfficeKind -- Вид объекта (для настройки ALG_CALCDEV_EPS2)
                          ,v_tick.t_DealID -- Объект (для настройки ALG_CALCDEV_EPS2)
                          ,v_SignDeviation -- Да/нет
                          ,v_RateKind      -- Наименование ставки используемой в дальнейшем
                          ,v_RateVal       -- Значение ставки
                          ) = 0 THEN

          IF v_SignDeviation = FALSE THEN
            v_TestAttrID := 1; --Да
          END IF;
        END IF;
      END IF;
    END IF;

    RETURN v_TestAttrID;
  END CalcExecDealMarketTestOwn;


  PROCEDURE ExecDealMarketTestOwn(p_DealID IN NUMBER, p_OnDate IN DATE)
  IS
    v_tick DDL_TICK_DBT%ROWTYPE;

    v_TestAttrID NUMBER := 0;
  BEGIN

    SELECT * INTO v_tick
      FROM ddl_tick_dbt
     WHERE t_DealID = p_DealID;

    v_TestAttrID := RSB_SECUR.CalcExecDealMarketTestOwn(v_tick.t_DealID, v_tick.t_DealDate);

    RSB_SECUR.SetDealMarketTestAttrID(v_tick.t_DealID, v_tick.t_DealDate, v_TestAttrID);
  END ExecDealMarketTestOwn;


  PROCEDURE Mass_ExecDealMarketTestOwn
  IS
  BEGIN

     FOR UpdtDeal_rec IN (SELECT Deals.t_DealID, Deals.t_DealDate
                          FROM DV_MKDEAL_MASS_EXEC Deals
                       ) LOOP
       RSB_SECUR.SetDealMarketTestAttrID(UpdtDeal_rec.t_DealID, UpdtDeal_rec.t_DealDate, RSB_SECUR.CalcExecDealMarketTestOwn(UpdtDeal_rec.t_DealID, UpdtDeal_rec.t_DealDate));
     END LOOP;
  END Mass_ExecDealMarketTestOwn;

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

  --Получить значениия уровня существенности
  FUNCTION GetLevelEssential(p_LevelEssential IN NUMBER -- вид уровня
                            ,p_Portfolio IN NUMBER -- портфель
                            ,p_DateStart IN DATE -- дата значения
                            ,p_AbsoluteValue OUT NUMBER -- абсолютное значение уровня
                            ,p_RelativeValue OUT NUMBER -- относительное значение уровня
                            )
    RETURN NUMBER
  IS
     v_NumID NUMBER := -1;
  BEGIN
     p_AbsoluteValue := 0;
     p_RelativeValue := 0;

     BEGIN
       SELECT t_NumID INTO v_NumID
         FROM DDL_LEVELESSENTIAL_DBT
        WHERE t_PortfolioID = p_Portfolio
          AND t_LevelEssentialID = p_LevelEssential;
     EXCEPTION
       when OTHERS then v_NumID := -1;
     END;

     IF v_NumID < 0 THEN
        BEGIN
          SELECT t_NumID INTO v_NumID
            FROM DDL_LEVELESSENTIAL_DBT
           WHERE (t_PortfolioID IS NULL or t_PortfolioID < 0)
             AND t_LevelEssentialID = p_LevelEssential;
        EXCEPTION
          when OTHERS then return 1;
        END;
     END IF;

     IF v_NumID > 0 THEN
        BEGIN
           SELECT T_ABSOLUTEVALUE, T_RelativeValue/100 INTO p_AbsoluteValue, p_RelativeValue
             FROM DDL_VALUESLEVEL_DBT
            WHERE     t_NumID = v_NumID
                  AND t_DateStart = (SELECT MAX (t_DateStart)
                                       FROM DDL_VALUESLEVEL_DBT
                                      WHERE t_DateStart <= p_DateStart
                                        AND t_NumID = v_NumID
                                    )
                  AND ROWNUM = 1
            ORDER BY T_ID DESC;
        EXCEPTION
          when OTHERS then return 1;
        END;
     ELSE
        return 1;
     END IF;

     return 0;

  END GetLevelEssential;

  FUNCTION GetCalcKind(p_BofficeKind IN NUMBER, p_OGroup IN NUMBER)
    RETURN NUMBER
  IS
  BEGIN
    IF p_BofficeKind = DL_SECURITYDOC THEN
      IF IsTwoPart(p_OGroup)=0 AND IsBuy(p_OGroup)=1 THEN
        RETURN CALCKIND_AVR;
      ELSIF IsRepo(p_OGroup)=1 THEN
        IF IsBuy(p_OGroup)=1 THEN
          RETURN CALCKIND_OREPO;
        ELSE
          RETURN CALCKIND_PREPO;
        END IF;
      END IF;
    ELSIF p_BofficeKind IN (DL_SECUROWN, DL_AVRWRTOWN) THEN
      RETURN CALCKIND_OWN;
    END IF;

    RETURN -1;
  END GetCalcKind;

  --Получить значение РПС для уровня существенности отклонение ЭПС (с учетом настройки SECUR\МСФО\МЕТОД_РАСЧЕТА_ОТКЛ_ЭПС)
  FUNCTION GetRPS_LevelDevEPS(pCalcKind IN NUMBER, -- вид расчета (ПРЕПО/ОРЕПО, ОЭБ, УВ/СВ)
                              pServiseKind IN NUMBER, --Вид обслуживания
                              pBranchID IN NUMBER, --Подразделение ТС
                              pProductKindID IN NUMBER, --Вид банковского продукта
                              pProductID IN NUMBER, --Банковский продукт
                              pPeriodDays IN NUMBER, --Срок в днях
                              pContractSum IN NUMBER, --Сумма договора
                              pFiID IN NUMBER, --Валюта договора
                              pClientType IN NUMBER, --Тип клиента
                              pDate IN DATE, --Дата
                              pMarketRateMin IN OUT NUMBER,
                              pMarketRateMax IN OUT NUMBER,
                              pSourceDataLayer IN OUT NUMBER,
                              pIssuer IN NUMBER DEFAULT 0, --Эмитент
                              pSumID IN NUMBER DEFAULT 0 -- Id лота (0 необязательный)
                             )
     RETURN NUMBER
  IS
     v_ContractYears NUMBER;
     v_ContractMonths NUMBER;
     v_ContractDays NUMBER;
     v_MarketRateID NUMBER;
     v_Tt NUMBER;
     v_T NUMBER;
     v_Tt1 NUMBER;
     v_Ret NUMBER := 1;
     v_MarketRateMin2 NUMBER;
     v_MarketRateMax2 NUMBER;
     v_SourceDataLayer2 NUMBER;
     v_MarketRateID2 NUMBER;
     v_Ret2 NUMBER := 1;
     v_EPS_Med NUMBER := Rsb_Common.GetRegIntValue('SECUR\МСФО\МЕТОД_РАСЧЕТА_ОТКЛ_ЭПС', 0);
     v_PD_loan NUMBER := 0;
     v_LGD_loan NUMBER := 0;
     v_PD_RSHB NUMBER := 0;
     v_LGD_RSHB NUMBER := 0;
     v_FICERTID NUMBER := 0;
     v_InterestFrequency NUMBER := 0; --Периодичность выплаты
  BEGIN
     IF ((pCalcKind = CALCKIND_VA) or (pCalcKind = CALCKIND_VS)) THEN
        v_InterestFrequency := 5; -- в конце срока. тем самым поиск будет происходить по этой переодичности + остутсвующей переодичности
     END IF;
     IF pPeriodDays > 0 THEN
        --Определить количество целых лет срока (исходя из 360 дней)
        v_ContractYears  := FLOOR(pPeriodDays/360);
        v_ContractDays := MOD( pPeriodDays, 360 );
        --Определить количество целых месяцев срока свыше количества лет (исходя из 30 дней)
        v_ContractMonths := FLOOR(v_ContractDays/30);
        --Определить количество дней срока, свыше количества месяцев
        v_ContractDays := MOD( v_ContractDays, 30 );

        --Определить минимальное и максимальное значение РПС. Вызывается функция определения РПС
        v_Ret := RSI_MARKETRATE.SelectRangeMarketRateEx( pServiseKind, --Вид обслуживания
                                                         pBranchID, --Подразделение ТС
                                                         pProductKindID, --Вид банковского продукта
                                                         pProductID, --Банковский продукт
                                                         v_InterestFrequency, --Периодичность выплаты
                                                         v_ContractYears, --Срок договора - года
                                                         v_ContractMonths, --Срок договора - месяцы
                                                         v_ContractDays, --Срок договора - дни
                                                         pContractSum, --Сумма договора
                                                         pFiID, --Валюта договора
                                                         pClientType, --Тип клиента
                                                         pDate, --Дата
                                                         pMarketRateMin, --Мин. значение интервала РПС
                                                         pMarketRateMax, --Макс. значение интервала РПС
                                                         pSourceDataLayer,
                                                         v_MarketRateID
                                                       );
        IF v_Ret = 0 THEN
           pMarketRateMin := pMarketRateMin / 100;
           pMarketRateMax := pMarketRateMax / 100;

           -- Алгоритм 2
           IF v_EPS_Med = ALG_CALCDEV_EPS2 THEN
              IF pCalcKind IN (CALCKIND_VA, CALCKIND_PREPO, CALCKIND_OREPO, CALCKIND_OWN) THEN
                 v_T := pPeriodDays;

                 select (T_CONTRACTYEAR * 360 + T_CONTRACTMONTHS * 30 + T_CONTRACTDAYS) into v_Tt
                   from DMARKETRATE_DBT
                  where t_ID = v_MarketRateID;

                 -- Если срок соответствует не полностью, то рассчитаем РПС по формуле:
                 IF (v_Tt != v_T) and (v_Tt > 0 and v_T > 0) THEN
                    -- Определить мин и макс значение РПС (срок > T)
                    v_Ret2 := RSI_MARKETRATE.SelectRangeMarketRateEx_UpT( pServiseKind, --Вид обслуживания
                                                                          pBranchID, --Подразделение ТС
                                                                          pProductKindID, --Вид банковского продукта
                                                                          pProductID, --Банковский продукт
                                                                          v_InterestFrequency, --Периодичность выплаты
                                                                          v_ContractYears, --Срок договора - года
                                                                          v_ContractMonths, --Срок договора - месяцы
                                                                          v_ContractDays, --Срок договора - дни
                                                                          pContractSum, --Сумма договора
                                                                          pFiID, --Валюта договора
                                                                          pClientType, --Тип клиента
                                                                          pDate, --Дата
                                                                          v_MarketRateMin2, --Мин. значение интервала РПС
                                                                          v_MarketRateMax2, --Макс. значение интервала РПС
                                                                          v_SourceDataLayer2,
                                                                          v_MarketRateID2
                                                                        );
                    IF v_Ret2 = 0 THEN
                       v_MarketRateMin2 := v_MarketRateMin2 / 100;
                       v_MarketRateMax2 := v_MarketRateMax2 / 100;

                       select (T_CONTRACTYEAR * 360 + T_CONTRACTMONTHS * 30 + T_CONTRACTDAYS) into v_Tt1
                         from DMARKETRATE_DBT
                        where t_ID = v_MarketRateID2;

                       pMarketRateMax := pMarketRateMax + (v_T - v_Tt) * (v_MarketRateMax2 - pMarketRateMax) / (v_Tt1 - v_Tt);

                       IF pCalcKind = CALCKIND_VA THEN

                          SELECT ficert.t_ficertid 
                            INTO v_FICERTID 
                            FROM dficert_dbt ficert 
                           WHERE ficert.t_avoirkind = AVOIRKIND_BILL 
                             AND ficert.t_certid = pSumID;

                          v_PD_loan := Rsb_SCTX.GetNoteText(RSB_SECUR.OBJTYPE_FICERT, LPAD(v_FICERTID, 10, '0'), 17);
                          IF v_PD_loan IS NULL THEN
                            v_PD_loan := TO_NUMBER(NVL(Rsb_SCTX.GetNoteText(RSB_SECUR.OBJTYPE_PARTY, LPAD(pIssuer, 10, '0'), 80), 0));
                          END IF;
                          v_LGD_loan := Rsb_SCTX.GetNoteText(RSB_SECUR.OBJTYPE_FICERT, LPAD(v_FICERTID, 10, '0'), 18);
                          IF v_LGD_loan IS NULL THEN
                            v_LGD_loan := TO_NUMBER(NVL(Rsb_SCTX.GetNoteText(RSB_SECUR.OBJTYPE_PARTY, LPAD(pIssuer, 10, '0'), 81), 0));
                          END IF;
                          v_PD_RSHB := Rsb_SCTX.GetNoteText(RSB_SECUR.OBJTYPE_FICERT, LPAD(v_FICERTID, 10, '0'), 19);
                          IF v_PD_RSHB IS NULL THEN
                            v_PD_RSHB := TO_NUMBER(NVL(Rsb_SCTX.GetNoteText(RSB_SECUR.OBJTYPE_PARTY, LPAD(pIssuer, 10, '0'), 82), 0));
                          END IF;
                          v_LGD_RSHB := Rsb_SCTX.GetNoteText(RSB_SECUR.OBJTYPE_FICERT, LPAD(v_FICERTID, 10, '0'), 20);
                          IF v_LGD_RSHB IS NULL THEN
                            v_LGD_RSHB := TO_NUMBER(NVL(Rsb_SCTX.GetNoteText(RSB_SECUR.OBJTYPE_PARTY, LPAD(pIssuer, 10, '0'), 83), 0));
                          END IF;

                          pMarketRateMax := pMarketRateMax + (v_PD_loan*v_LGD_loan)-(v_PD_RSHB*v_LGD_RSHB);
                       END IF;

                       pMarketRateMin := pMarketRateMax;
                    END IF;

                    IF pMarketRateMax < 0 OR v_Ret2 != 0 THEN
                       pMarketRateMax := 0;
                       pMarketRateMin := 0;
                    END IF;
                 END IF;
              END IF;
           END IF;
        END IF;
     END IF;

     RETURN v_Ret;
  END GetRPS_LevelDevEPS;

  --Проверка на существенность отклонения
  FUNCTION GetEssentialDev(p_LevelEssential IN NUMBER -- вид уровня
                          ,p_Portfolio IN NUMBER -- портфель
                          ,p_CalcDate IN DATE -- дата расчета
                          ,p_DoCompare IN NUMBER -- что сравниваем
                          ,p_ToCompare1 IN NUMBER -- с чем сравниваем 1
                          ,p_ToCompare2 IN NUMBER -- с чем сравниваем 2
                          ,p_ObjectKind IN NUMBER  -- вид объекта
                          ,p_ObjectID IN NUMBER -- id объекта
                          ,p_IsEssential OUT BOOLEAN -- Да/нет
                          ,p_RateKind OUT NUMBER -- Наименование ставки используемой в дальнейшем
                          ,p_RateVal OUT NUMBER -- Значение ставки
                          )
    RETURN NUMBER
  IS
     v_AbsoluteValue NUMBER := 0;
     v_RelativeValue NUMBER := 0;
     v_AbsoluteValue2 NUMBER := 0;
     v_RelativeValue2 NUMBER := 0;
     v_FirstDate DATE := UnknownDate;
     v_LastDate DATE := UnknownDate;
     v_AC_LINi NUMBER := 0;
     v_AC_EPSi NUMBER := 0;
     v_CalcKind NUMBER := -1;
     v_EPS NUMBER := -1;
     v_OGroup doprkoper_dbt.T_SYSTYPES%TYPE;
     v_tick ddl_tick_dbt%rowtype;
     v_leg ddl_leg_dbt%rowtype;
     v_bnr dvsbanner_dbt%rowtype;
     v_ArrDates ArrDates_t;
     v_EPS_Med NUMBER := Rsb_Common.GetRegIntValue('SECUR\МСФО\МЕТОД_РАСЧЕТА_ОТКЛ_ЭПС', 0);
     v_FP_Med NUMBER := Rsb_Common.GetRegIntValue('SECUR\МСФО\МЕТОД_РАСЧЕТА_ОТКЛ_ФЦ', 0);
     v_FIID NUMBER := -1;
     v_Fin DFININSTR_DBT%ROWTYPE;
     v_Termless CHAR;
  BEGIN
     p_IsEssential := FALSE;
     p_RateKind    := RATE_KIND_UNDEF;
     p_RateVal     := 0;

     IF (p_LevelEssential = LEVELESSENTIAL_EPS AND v_EPS_Med = ALG_CALCDEV_EPS2) OR
        p_LevelEssential = LEVELESSENTIAL_AC THEN
        IF p_ObjectKind IN (DL_SECURITYDOC, DL_SECUROWN, DL_AVRWRTOWN) THEN
           SELECT tick.* INTO v_tick
             FROM ddl_tick_dbt tick
            WHERE tick.t_DealID = p_ObjectID;

           v_OGroup := get_OperationGroup(get_OperSysTypes(v_tick.t_DealType, v_tick.t_BofficeKind));
           v_CalcKind := GetCalcKind(v_tick.t_BofficeKind, v_OGroup);
           v_FIID := v_tick.t_PFI;

        ELSIF p_ObjectKind = DL_VSBANNER THEN
           SELECT leg.*
             INTO v_leg
             FROM ddl_leg_dbt leg
            WHERE     leg.t_LegKind = 1
                  AND leg.t_LegID = 0
                  AND leg.t_DealID = p_ObjectID;
           SELECT bnr.*
             INTO v_bnr
             FROM dvsbanner_dbt bnr
            WHERE     bnr.t_BCID = p_ObjectID;

           v_CalcKind := CASE WHEN rsb_bill.IsOurBanner(v_bnr.t_Issuer) THEN CALCKIND_VS ELSE CALCKIND_VA END;

        ELSIF p_ObjectKind = DLDOC_ISSUE THEN
           v_FIID := p_ObjectID;
        END IF;
     END IF;

     IF NOT (p_LevelEssential = LEVELESSENTIAL_EPS AND v_EPS_Med = ALG_CALCDEV_EPS2) THEN
        IF GetLevelEssential(p_LevelEssential,
                             p_Portfolio,
                             p_CalcDate,
                             v_AbsoluteValue,
                             v_RelativeValue
                            ) = 1 THEN
           return 1;
        END IF;
     END IF;

     -- Отклонение ЭПС
     IF p_LevelEssential = LEVELESSENTIAL_EPS THEN
        -- Алгоритм 1
        IF v_EPS_Med = ALG_CALCDEV_EPS1 THEN
           -- В данном уровне используется только абсолютное значение отклонения. Будет задаваться в процентных пунктах. Должно быть /100
           v_AbsoluteValue := v_AbsoluteValue / 100;

           IF (p_ToCompare1 <= p_DoCompare + v_AbsoluteValue) AND (p_DoCompare - v_AbsoluteValue <= p_ToCompare2) THEN
              p_IsEssential := FALSE;
              p_RateKind    := RATE_KIND_EPS;
              p_RateVal     := p_DoCompare;
           ELSIF (p_DoCompare + v_AbsoluteValue < p_ToCompare1) THEN
              p_IsEssential := TRUE;
              p_RateKind    := RATE_KIND_RPS;
              p_RateVal     := p_ToCompare1;
           ELSIF (p_ToCompare2 < p_DoCompare - v_AbsoluteValue) THEN
              p_IsEssential := TRUE;
              p_RateKind    := RATE_KIND_RPS;
              p_RateVal     := p_ToCompare2;
           END IF;

        -- Алгоритм 2
        ELSIF v_EPS_Med = ALG_CALCDEV_EPS2 THEN
           -- Что сравниваем (p_DoCompare)  - значение РПС
           -- С чем сравниваем (p_ToCompare1)  - ЭПС  (ЭПС)
           -- С чем сравниваем (p_ToCompare2)  - значение Ставка по договору

           -- Получим значения уровней
           IF GetLevelEssential(LEVELESSENTIAL_EPS_DOWN,
                                p_Portfolio,
                                p_CalcDate,
                                v_AbsoluteValue, -- Отклонение вниз ЭПСабс
                                v_RelativeValue -- Отклонение вниз ЭПСотн
                               ) = 1 THEN
              return 1;
           END IF;
           IF GetLevelEssential(LEVELESSENTIAL_EPS_UP,
                                p_Portfolio,
                                p_CalcDate,
                                v_AbsoluteValue2, -- Отклонение вверх ЭПСабс
                                v_RelativeValue2 -- Отклонение вверх ЭПСотн
                               ) = 1 THEN
              return 1;
           END IF;

           -- Для портфелей = ПРЕПО, ОРЕПО
           IF v_CalcKind IN (CALCKIND_PREPO, CALCKIND_OREPO) THEN
              IF p_ObjectKind = DL_SECURITYDOC THEN
                 SELECT * INTO v_leg
                   FROM ddl_leg_dbt
                  WHERE t_LegKind = 0
                    AND t_DealID = p_ObjectID
                    AND t_LegID = 0;

                 -- Если сделка РЕПО рублевая:
                 IF v_leg.t_CFI = RSI_RSB_FIInstr.NATCUR THEN
                    IF (p_DoCompare * (1 - v_RelativeValue) <= p_ToCompare2) AND (p_ToCompare2 <= p_DoCompare * (1 + v_RelativeValue2)) THEN
                       p_IsEssential := FALSE;
                       p_RateKind    := RATE_KIND_SPD;
                       p_RateVal     := p_ToCompare2;
                    ELSIF p_ToCompare2 < (p_DoCompare * (1 - v_RelativeValue)) THEN
                       p_IsEssential := TRUE;
                       p_RateKind    := RATE_KIND_RPS;
                       p_RateVal     := p_DoCompare;
                    ELSIF p_DoCompare * (1 + v_RelativeValue2) < p_ToCompare2 THEN
                       p_IsEssential := TRUE;
                       p_RateKind    := RATE_KIND_RPS;
                       p_RateVal     := p_DoCompare;
                    END IF;
                 -- Если сделка РЕПО валютная:
                 ELSE
                    v_AbsoluteValue := v_AbsoluteValue / 100;
                    v_AbsoluteValue2 := v_AbsoluteValue2 / 100;

                    IF ((p_DoCompare - v_AbsoluteValue) <= p_ToCompare2) AND (p_ToCompare2 <= (p_DoCompare + v_AbsoluteValue2)) THEN
                       p_IsEssential := FALSE;
                       p_RateKind    := RATE_KIND_SPD;
                       p_RateVal     := p_ToCompare2;
                    ELSIF p_ToCompare2 < (p_DoCompare - v_AbsoluteValue) THEN
                       p_IsEssential := TRUE;
                       p_RateKind    := RATE_KIND_RPS;
                       p_RateVal     := p_DoCompare;
                    ELSIF (p_DoCompare + v_AbsoluteValue2) < p_ToCompare2 THEN
                       p_IsEssential := TRUE;
                       p_RateKind    := RATE_KIND_RPS;
                       p_RateVal     := p_DoCompare;
                    END IF;
                 END IF;
              END IF;

           -- Для портфеля = ОЭБ
           ELSIF v_CalcKind = CALCKIND_OWN THEN
              SELECT * INTO v_Fin FROM DFININSTR_DBT WHERE T_FIID = v_FIID;

              -- Если облигация рублевая
              IF RSI_RSB_FIInstr.FI_AvrKindsGetRoot( 2, v_Fin.t_AvoirKind ) = RSI_RSB_FIInstr.AVOIRKIND_BOND THEN
                 IF p_DoCompare * (1 - v_RelativeValue) <= p_ToCompare1 AND p_ToCompare1 <= p_DoCompare * (1 + v_RelativeValue2) THEN
                       p_IsEssential := FALSE;
                       p_RateKind    := RATE_KIND_EPS;
                       p_RateVal     := p_ToCompare1;
              ELSIF p_ToCompare1 < p_DoCompare * (1 - v_RelativeValue) THEN
                 IF p_ToCompare2 < p_DoCompare * (1 - v_RelativeValue) THEN
                    p_IsEssential := TRUE;
                    p_RateKind    := RATE_KIND_RPS;
                    p_RateVal     := p_DoCompare;
                 ELSE
                    p_IsEssential := FALSE;
                    p_RateKind    := RATE_KIND_SPD;
                    p_RateVal     := p_ToCompare2;
                 END IF;
              ELSIF p_DoCompare * (1 + v_RelativeValue2) < p_ToCompare1 THEN
                 IF p_DoCompare * (1 + v_RelativeValue2) < p_ToCompare2 THEN
                    p_IsEssential := TRUE;
                    p_RateKind    := RATE_KIND_RPS;
                    p_RateVal     := p_DoCompare;
                 ELSE
                    p_IsEssential := FALSE;
                    p_RateKind    := RATE_KIND_SPD;
                    p_RateVal     := p_ToCompare2;
                 END IF;
              END IF;
           END IF;

           -- Для портфелей = СВ, УВ
           ELSIF v_CalcKind IN (CALCKIND_VS, CALCKIND_VA) THEN
              -- Если вексель рублей
              IF v_leg.t_PFI = RSI_RSB_FIInstr.NATCUR THEN
                 IF p_DoCompare * (1 - v_RelativeValue) <= p_ToCompare1 AND p_ToCompare1 <= p_DoCompare * (1 + v_RelativeValue2) THEN
                    p_IsEssential := FALSE;
                    p_RateKind    := RATE_KIND_EPS;
                    p_RateVal     := p_ToCompare1;
                 ELSIF p_ToCompare1 < p_DoCompare * (1 - v_RelativeValue) THEN
                    IF p_ToCompare2 < p_DoCompare * (1 - v_RelativeValue) THEN
                       p_IsEssential := TRUE;
                       p_RateKind    := RATE_KIND_RPS;
                       p_RateVal     := p_DoCompare;
                    ELSE
                       p_IsEssential := FALSE;
                       p_RateKind    := RATE_KIND_EPS;
                       p_RateVal     := p_ToCompare1;
                    END IF;
                 ELSIF p_DoCompare * (1 + v_RelativeValue2) < p_ToCompare1 THEN
                    IF p_DoCompare * (1 + v_RelativeValue2) < p_ToCompare2 THEN
                       p_IsEssential := TRUE;
                       p_RateKind    := RATE_KIND_RPS;
                       p_RateVal     := p_DoCompare;
                    ELSE
                       p_IsEssential := FALSE;
                       p_RateKind    := RATE_KIND_EPS;
                       p_RateVal     := p_ToCompare1;
                    END IF;
                 END IF;

              -- Если вексель валютный
              ELSE
                 v_AbsoluteValue := v_AbsoluteValue / 100;
                 v_AbsoluteValue2 := v_AbsoluteValue2 / 100;

                 IF ((p_DoCompare - v_AbsoluteValue) <= p_ToCompare1) AND (p_ToCompare1 <= (p_DoCompare + v_AbsoluteValue2)) THEN
                    p_IsEssential := FALSE;
                    p_RateKind    := RATE_KIND_EPS;
                    p_RateVal     := p_ToCompare1;
                 ELSIF p_ToCompare1 < (p_DoCompare - v_AbsoluteValue) THEN
                    IF p_ToCompare2 < (p_DoCompare - v_AbsoluteValue) THEN
                       p_IsEssential := TRUE;
                       p_RateKind    := RATE_KIND_RPS;
                       p_RateVal     := p_DoCompare;
                    ELSE
                       p_IsEssential := FALSE;
                       p_RateKind    := RATE_KIND_EPS;
                       p_RateVal     := p_ToCompare1;
                    END IF;
                 ELSIF (p_DoCompare + v_AbsoluteValue2) < p_ToCompare1 THEN
                    IF (p_DoCompare + v_AbsoluteValue2) < p_ToCompare2 THEN
                       p_IsEssential := TRUE;
                       p_RateKind    := RATE_KIND_RPS;
                       p_RateVal     := p_DoCompare;
                    ELSE
                       p_IsEssential := FALSE;
                       p_RateKind    := RATE_KIND_EPS;
                       p_RateVal     := p_ToCompare1;
                    END IF;
                 END IF;
              END IF;
           END IF;
        END IF;

     -- Отклонение АС
     ELSIF p_LevelEssential = LEVELESSENTIAL_AC THEN
        IF v_RelativeValue <> 0 THEN
           v_FirstDate := LAST_DAY(p_CalcDate);

           IF p_ObjectKind = DLDOC_ISSUE THEN
              SELECT t_Termless INTO v_Termless from davoiriss_dbt where t_FIID = p_ObjectID;
              v_LastDate := RSI_RSB_FIInstr.FI_GetNominalDrawingDate(p_ObjectID, v_Termless);

           ELSIF p_ObjectKind = DL_VSBANNER THEN
              IF rsb_bill.GetBnrPlanRepayDate (v_leg.t_ID, v_LastDate) != 0 THEN
                 RAISE NO_DATA_FOUND;
              END IF;

           ELSIF p_ObjectKind IN (DL_SECURITYDOC, DL_SECUROWN, DL_AVRWRTOWN) THEN
              IF IsRepo( v_OGroup ) = 1 THEN
                 v_LastDate := GetDealSupplyDate2(p_ObjectID, 2);
              ELSE
                 SELECT t_Termless INTO v_Termless from davoiriss_dbt where t_FIID = v_FIID;
                 v_LastDate := RSI_RSB_FIInstr.FI_GetNominalDrawingDate(v_FIID, v_Termless);
              END IF;
           END IF;

           IF v_FirstDate > v_LastDate THEN
              return 0;
           END IF;

           -- Определяем последовательность предстоящих отчетных дат
           v_ArrDates := GetSeqRepDates(v_FirstDate, v_LastDate);

           IF v_ArrDates IS NOT EMPTY THEN
              IF p_ObjectKind IN (DL_SECURITYDOC, DL_SECUROWN, DL_AVRWRTOWN) THEN
                 -- Проверка на относительную разницу
                 FOR i IN v_ArrDates.First .. v_ArrDates.Last LOOP
                    -- АС ФИ рассчитанная линейным методом на i-тый период
                    v_AC_LINi := CalcAS_Line(v_CalcKind, p_ObjectID, 0, v_ArrDates(i)); --(для ЦБ(покупка), ОЭБ в ВН, для РЕПО в ВЦ)
                    -- АС ФИ рассчитанная методом ЭПС на i-тый период
                    v_EPS := CalcEPS(v_CalcKind, p_ObjectID, 0, v_ArrDates(i));
                    v_AC_EPSi := CalcAS_EPS(v_CalcKind, p_ObjectID, 0, v_ArrDates(i), v_EPS); --(для ЦБ(покупка), ОЭБ в ВН, для РЕПО в ВЦ)

                    IF v_AC_EPSi = 0 THEN
                       return 1;
                    END IF;

                    IF ABS(v_AC_LINi - v_AC_EPSi) / v_AC_EPSi > v_RelativeValue THEN
                       p_IsEssential := TRUE;
                       return 0;
                    END IF;
                 END LOOP;

                 -- Проверка на абсолютную разницу
                 IF v_AbsoluteValue <> 0 THEN
                    FOR i IN v_ArrDates.First .. v_ArrDates.Last LOOP
                       -- АС ФИ рассчитанная линейным методом на i-тый период
                       v_AC_LINi := CalcAS_Line(v_CalcKind, p_ObjectID, 0, v_ArrDates(i)); --(для ЦБ(покупка), ОЭБ в ВН, для РЕПО в ВЦ)
                       -- АС ФИ рассчитанная методом ЭПС на i-тый период
                       v_EPS := CalcEPS(v_CalcKind, p_ObjectID, 0, v_ArrDates(i));
                       v_AC_EPSi := CalcAS_EPS(v_CalcKind, p_ObjectID, 0, v_ArrDates(i), v_EPS); --(для ЦБ(покупка), ОЭБ в ВН, для РЕПО в ВЦ)

                       IF ABS(v_AC_LINi - v_AC_EPSi) > v_AbsoluteValue THEN
                          p_IsEssential := TRUE;
                          return 0;
                       END IF;
                    END LOOP;
                 END IF;
              ELSIF p_ObjectKind = DL_VSBANNER THEN
                 -- Проверка на относительную разницу
                 FOR i IN v_ArrDates.First .. v_ArrDates.Last LOOP
                    -- АС ФИ рассчитанная линейным методом на i-тый период
                    v_AC_LINi := CalcAS_Line(v_CalcKind, p_DoCompare, v_bnr.t_BCID, v_ArrDates(i));
                    -- АС ФИ рассчитанная методом ЭПС на i-тый период
                    v_EPS := CalcEPS(v_CalcKind, p_DoCompare, v_bnr.t_BCID, v_ArrDates(i));
                    v_AC_EPSi := CalcAS_EPS(v_CalcKind, p_DoCompare, v_bnr.t_BCID, v_ArrDates(i), v_EPS);

                    IF v_AC_EPSi = 0 THEN
                       return 1;
                    END IF;

                    IF ABS(v_AC_LINi - v_AC_EPSi) / v_AC_EPSi > v_RelativeValue THEN
                       p_IsEssential := TRUE;
                       return 0;
                    END IF;
                 END LOOP;

                 -- Проверка на абсолютную разницу
                 IF v_AbsoluteValue <> 0 THEN
                    FOR i IN v_ArrDates.First .. v_ArrDates.Last LOOP
                       -- АС ФИ рассчитанная линейным методом на i-тый период
                       v_AC_LINi := CalcAS_Line(v_CalcKind, p_DoCompare, v_bnr.t_BCID, v_ArrDates(i));
                       -- АС ФИ рассчитанная методом ЭПС на i-тый период
                       v_EPS := CalcEPS(v_CalcKind, p_DoCompare, v_bnr.t_BCID, v_ArrDates(i));
                       v_AC_EPSi := CalcAS_EPS(v_CalcKind, p_DoCompare, v_bnr.t_BCID, v_ArrDates(i), v_EPS);

                       IF ABS(v_AC_LINi - v_AC_EPSi) > v_AbsoluteValue THEN
                          p_IsEssential := TRUE;
                          return 0;
                       END IF;
                    END LOOP;
                 END IF;
              END IF;
           END IF;
        END IF;

     -- Отклонение СС
     ELSIF p_LevelEssential = LEVELESSENTIAL_CC THEN
        -- Проверка на относительную разницу
        IF p_DoCompare = 0 THEN
           return 1;
        END IF;

        IF NOT (ABS((p_ToCompare1 - p_DoCompare) / p_DoCompare) > v_RelativeValue) THEN
           return 0;
        END IF;

        -- Проверка на абсолютную разницу
        IF v_AbsoluteValue <> 0 AND ABS(p_ToCompare1 - p_DoCompare) > v_AbsoluteValue THEN
           p_IsEssential := TRUE;
        END IF;

     -- Отклонение Фактической Цены
     ELSIF p_LevelEssential = LEVELESSENTIAL_FACTPRICE THEN
        -- Алгоритм 1
        IF v_FP_Med = ALG_CALCDEV_FP1 THEN
           -- Проверка на относительную разницу
           IF p_ToCompare1 = 0 THEN
              return 1;
           END IF;

           IF NOT (ABS((p_DoCompare - p_ToCompare1) / p_ToCompare1) > v_RelativeValue) THEN
              return 0;
           END IF;

           -- Проверка на абсолютную разницу
           IF ABS(p_DoCompare - p_ToCompare1) > v_AbsoluteValue THEN
              p_IsEssential := TRUE;
           END IF;

        -- Алгоритм 2
        ELSIF v_FP_Med = ALG_CALCDEV_FP2 THEN
           -- Что сравниваем (p_DoCompare)  - Стоимость покупки (ФЦ)
           -- С чем сравниваем (p_ToCompare1)  - Справедливая стоимость на дату первоначального признания (СС)

           -- Получим значения уровней
           IF GetLevelEssential(LEVELESSENTIAL_FP_DOWN,
                                p_Portfolio,
                                p_CalcDate,
                                v_AbsoluteValue,
                                v_RelativeValue -- СущФЦотн_вниз
                               ) = 1 THEN
              return 1;
           END IF;
           IF GetLevelEssential(LEVELESSENTIAL_FP_UP,
                                p_Portfolio,
                                p_CalcDate,
                                v_AbsoluteValue2,
                                v_RelativeValue2 -- СущФЦотн_вверх
                               ) = 1 THEN
              return 1;
           END IF;

           -- Проверка на относительную разницу
           IF (p_ToCompare1 + p_ToCompare1 * v_RelativeValue2) < p_DoCompare OR
              (p_ToCompare1 - p_ToCompare1 * v_RelativeValue) > p_DoCompare THEN
              p_IsEssential := TRUE;
           END IF;
        END IF;

     -- Отклонение Затрат по договору
     ELSIF p_LevelEssential = LEVELESSENTIAL_CONTRACTCOSTS THEN
        -- Проверка на относительную разницу
        IF p_ToCompare1 = 0 THEN
           return 1;
        END IF;

        IF NOT (p_DoCompare / p_ToCompare1 > v_RelativeValue) THEN
           return 0;
        END IF;

        -- Проверка на абсолютную разницу
        IF p_DoCompare > v_AbsoluteValue THEN
           p_IsEssential := TRUE;
        END IF;
     END IF;

     return 0;

  EXCEPTION
    when OTHERS then return 1;
  END GetEssentialDev;

  --Проверка на существенность отклонения
  FUNCTION GetEssentialDevInt(p_LevelEssential IN NUMBER -- вид уровня
                             ,p_Portfolio IN NUMBER -- портфель
                             ,p_CalcDate IN DATE -- дата расчета
                             ,p_DoCompare IN NUMBER -- что сравниваем
                             ,p_ToCompare1 IN NUMBER -- с чем сравниваем 1
                             ,p_ToCompare2 IN NUMBER -- с чем сравниваем 2
                             ,p_ObjectKind IN NUMBER  -- вид объекта
                             ,p_ObjectID IN NUMBER -- id объекта
                             ,p_IsEssential OUT NUMBER -- Да/нет
                             ,p_RateKind OUT NUMBER -- Наименование ставки используемой в дальнейшем
                             ,p_RateVal OUT NUMBER -- Значение ставки
                             )
    RETURN NUMBER
  IS
    v_stat          NUMBER := 0;
    v_IsEssential   BOOLEAN := FALSE;
  BEGIN
    v_stat := GetEssentialDev(p_LevelEssential,p_Portfolio,p_CalcDate,p_DoCompare,
    p_ToCompare1,p_ToCompare2,p_ObjectKind,p_ObjectID,v_IsEssential,p_RateKind,p_RateVal);
    p_IsEssential := CASE WHEN v_IsEssential THEN 1 ELSE 0 END;
    RETURN v_stat;
  EXCEPTION
    when OTHERS then return 1;
  END GetEssentialDevInt;

  FUNCTION IsEssentialSumByContrCost(p_CalcDate DATE, p_PortfolioID NUMBER, p_DoCompareRUB NUMBER, p_ToCompareRUB NUMBER) RETURN NUMBER
  AS
    v_SignDeviation BOOLEAN;
    v_RateKind      NUMBER;
    v_RateVal       NUMBER;
    v_stat          NUMBER;
  BEGIN
    v_stat := GetEssentialDev( LEVELESSENTIAL_CONTRACTCOSTS, --Затраты по договору
                             p_PortfolioID,
                             p_CalcDate,
                             p_DoCompareRUB,
                             p_ToCompareRUB,
                             0,
                             0,
                             0,
                             v_SignDeviation, -- Да/нет
                             v_RateKind,      -- Наименование ставки используемой в дальнейшем
                             v_RateVal        -- Значение ставки
                           );

    if( v_SignDeviation = TRUE )then
       RETURN 1;
    end if;

    RETURN 0;

    EXCEPTION
      WHEN OTHERS THEN RETURN 0;
  END IsEssentialSumByContrCost;

  FUNCTION DealIsEssentialByOverTPlus( p_DealID IN NUMBER ,  p_OperDate IN DATE ) RETURN NUMBER
  AS
    v_Flag             NUMBER := 0;
    v_FVCourseType     NUMBER := Rsb_Common.GetRegIntValue('SECUR\ВИД КУРСА СПРАВЕДЛ. СТОИМОСТЬ', 0);
    v_Portfolio        NUMBER;
    v_Course           NUMBER;
    v_CalcNKD          NUMBER;
    v_DealDate         DATE;
    v_FaceFI           NUMBER;
    v_DealFIID         NUMBER; -- FIID бумаги
    v_DealTSSrub       NUMBER;
    v_CurrTSSrub       NUMBER;
    v_SignDeviation    BOOLEAN;
    v_RateKind         NUMBER;
    v_RateVal          NUMBER;
    v_stat             NUMBER;



  BEGIN

    SELECT FI.T_FACEVALUEFI, TK.T_PFI, TK.T_DEALDATE, TK.T_PORTFOLIOID INTO v_FaceFI, v_DealFIID, v_DealDate, v_Portfolio
    FROM DDL_TICK_DBT TK, DFININSTR_DBT FI
    WHERE TK.T_DEALID = p_DealID
    AND TK.T_PFI =  FI.T_FIID
    AND FI.T_FI_KIND = 2;

    v_Course     := NVL(SC_ConvSumTypeRep(1, v_DealFIID, v_FaceFI, RSI_RSB_FIInstr.NATCUR, v_FVCourseType, v_DealDate), 0); -- уже в нац.вал
    v_CalcNKD    := NVL(RSI_RSB_FIInstr.FI_CalcNKD(v_DealFIID, v_DealDate, 1, 0), 0);
    v_DealTSSrub := v_Course - RSB_FIInstr.ConvSum(v_CalcNKD, v_CalcNKD, RSI_RSB_FIInstr.NATCUR, v_DealDate);

    v_Course     := NVL(SC_ConvSumTypeRep(1, v_DealFIID, v_FaceFI, RSI_RSB_FIInstr.NATCUR, v_FVCourseType, p_OperDate), 0); -- уже в нац.вал
    v_CalcNKD    := NVL(RSI_RSB_FIInstr.FI_CalcNKD(v_DealFIID, p_OperDate, 1, 0), 0);
    v_CurrTSSrub := v_Course - RSB_FIInstr.ConvSum(v_CalcNKD, v_CalcNKD, RSI_RSB_FIInstr.NATCUR, p_OperDate);


    v_stat :=  GetEssentialDev(
                               LEVELESSENTIAL_CC, -- Вид уровня существенности = Отклонение СС
                               v_Portfolio,
                               p_OperDate,
                               v_DealTSSrub,
                               v_CurrTSSrub,
                               0,
                               0,
                               0,
                               v_SignDeviation, -- OUT Да/нет
                               v_RateKind ,     -- OUT Наименование ставки используемой в дальнейшем
                               v_RateVal       -- OUT Значение ставки
                               );
    IF(v_SignDeviation = true) THEN
       v_Flag := 1;
    END IF;

    return v_Flag;
  END DealIsEssentialByOverTPlus;


  FUNCTION GetDealSumOverTPlusOnDate(p_DealID IN NUMBER, p_Kind IN NUMBER, p_OnDate IN DATE, p_ExcludeWrt IN NUMBER DEFAULT 0) RETURN NUMBER
  AS
      v_OverSum   NUMBER := 0;

  BEGIN

     BEGIN 
        SELECT VL.T_SUM INTO v_OverSum
          FROM DDL_VALUE_DBT VL
         WHERE VL.T_DOCKIND = DL_SECURITYDOC 
           AND VL.T_DOCID = p_DealID 
           AND VL.T_KIND IN (RSI_DLGR.DL_VALUE_KIND_PLUSDEALTOVER, RSI_DLGR.DL_VALUE_KIND_MINUSDEALTOVER) 
           AND (p_Kind = 0 OR VL.T_KIND = p_Kind)
           AND VL.T_DATE <= p_OnDate
           AND VL.T_SUM <> 0
           AND (p_ExcludeWrt = 0 OR VL.T_ID_STEP <> -1)
           AND VL.T_DATE =
             (SELECT MAX (VL1.T_DATE)
                FROM DDL_VALUE_DBT VL1
               WHERE VL1.T_DOCKIND = VL.T_DOCKIND
                 AND VL1.T_DOCID   = VL.T_DOCID
                 AND VL1.T_KIND IN (RSI_DLGR.DL_VALUE_KIND_PLUSDEALTOVER, RSI_DLGR.DL_VALUE_KIND_MINUSDEALTOVER)
                 AND (p_Kind = 0 OR VL1.T_KIND = p_Kind)
                 AND VL1.T_DATE <= p_OnDate
                 AND (p_ExcludeWrt = 0 OR VL1.T_ID_STEP <> -1)
             );
     EXCEPTION
        WHEN NO_DATA_FOUND THEN v_OverSum := 0;

     END;
      return v_OverSum;
  END GetDealSumOverTPlusOnDate;

  -- Найти курс
  FUNCTION FindRate( pSum      IN NUMBER
                    ,pFromFI   IN NUMBER
                    ,pToFI     IN NUMBER
                    ,pToVR     IN NUMBER
                    ,pType     IN NUMBER
                    ,pbdate    IN DATE
                    ,pNDays    IN NUMBER
                    ,pRateID   IN OUT NUMBER
                    ,pRateDate IN OUT DATE
                   )
     RETURN NUMBER
  IS
     v_RD     DRATEDEF_DBT%ROWTYPE;
     v_Rate   NUMBER := 0;
     v_ToFI   NUMBER := pToFI;
  BEGIN

     v_Rate := RSI_RSB_FIInstr.FI_GetRate( pFromFI, pToFI, pType, pbdate, pNDays, 0, pRateID, pRateDate );
     if( v_Rate <= 0 ) then
        v_Rate := RSI_RSB_FIInstr.FI_GetRate( pFromFI, -1, pType, pbdate, pNDays, 0, pRateID, pRateDate );
        begin
           select * into v_RD
             from dratedef_dbt
            where t_RateID = pRateID;
        exception
           when OTHERS then return 0;
        end;

        if(v_RD.t_OtherFI = pFromFI)then
            v_ToFI := v_RD.t_FIID;
        else
            v_ToFI := v_RD.t_OtherFI;
        end if;
     end if;

     if( v_Rate > 0 ) then
        v_Rate := pSum*v_Rate;
        v_Rate := RSB_FIInstr.ConvSum(v_Rate, v_ToFI, pToVR, pbdate);
     end if;

    return v_Rate;
  EXCEPTION
     when OTHERS then return 0;
  END FindRate;

  -- Определить СС ФИ используя курсы
  FUNCTION GetFairValueFromRates( pFIID IN NUMBER,
                                  pCalcDate IN DATE,
                                  pRateID OUT NUMBER,
                                  pRateDate OUT DATE ) RETURN NUMBER
  AS
     v_FaceFI       NUMBER;
     v_CT1          NUMBER;
     v_CT4          NUMBER;
     v_CT10         NUMBER;
     v_CT18         NUMBER;
     v_CT23         NUMBER;
     v_Rate         NUMBER;
     v_RateNext     NUMBER;
     v_RateIDNext   NUMBER;
     v_RateDateNext DATE;
     v_SSDay        NUMBER := Rsb_Common.GetRegIntValue('SECUR\МСФО\СС_ДНЕЙ', 0);
     v_WDep         DATE := RSI_RsbCalendar.GetDateAfterWorkDay(pCalcDate,-v_SSDay);
  BEGIN
     SELECT FI.T_FACEVALUEFI
       INTO v_FaceFI
       FROM DFININSTR_DBT FI,
            davoiriss_dbt av
      WHERE FI.T_FIID = pFIID
        AND av.T_FIID = FI.T_FIID;

     IF v_FaceFI = RSI_RSB_FIInstr.NATCUR THEN
        -- в порядке приоритета ищутся курсы видов
        v_CT4  := nvl(Rsb_Common.GetRegIntValue('SECUR\ВИД КУРСА СРЕДНЕВЗВЕШЕННАЯ ЦЕНА', 0), 4); --RATETYPE_AVR_PRICE
        v_CT1  := nvl(Rsb_Common.GetRegIntValue('SECUR\ВИД КУРСА РЫНОЧНАЯ ЦЕНА', 0), 1); --RATETYPE_MARKET_PRICE
        v_CT18 := nvl(Rsb_Common.GetRegIntValue('SECUR\ВИД КУРСА ЦЕНА ЗАКРЫТИЯ', 0), 18);

        v_Rate := FindRate(1, pFIID, v_FaceFI, v_FaceFI, v_CT4, pCalcDate, pCalcDate-v_WDep, pRateID, pRateDate);
        v_RateNext := FindRate(1, pFIID, v_FaceFI, v_FaceFI, v_CT1, pCalcDate, pCalcDate-v_WDep, v_RateIDNext, v_RateDateNext);

        IF v_RateDateNext > pRateDate AND v_RateNext > 0 THEN
           pRateID    := v_RateIDNext;
           v_Rate     := v_RateNext;
           pRateDate  := v_RateDateNext;
        END IF;

        v_RateNext := FindRate(1, pFIID, v_FaceFI, v_FaceFI, v_CT18, pCalcDate, pCalcDate-v_WDep, v_RateIDNext, v_RateDateNext);

        IF v_RateDateNext > pRateDate AND v_RateNext > 0 THEN
           pRateID    := v_RateIDNext;
           v_Rate     := v_RateNext;
           pRateDate  := v_RateDateNext;
        END IF;

     ELSE
        -- в порядке приоритета ищутся курсы видов
        v_CT23 := nvl(Rsb_Common.GetRegIntValue('SECUR\ВИД КУРСА ЦЕНА ЗАКРЫТ. БЛУМБЕРГ', 0), 23);
        v_CT10 := nvl(Rsb_Common.GetRegIntValue('SECUR\ВИД КУРСА СРЕДНЯЯ ЦЕНА РЕЙТЕР', 0), 10);

        v_Rate := FindRate(1, pFIID, v_FaceFI, v_FaceFI, v_CT23, pCalcDate, pCalcDate-v_WDep, pRateID, pRateDate);
        v_RateNext := FindRate(1, pFIID, v_FaceFI, v_FaceFI, v_CT10, pCalcDate, pCalcDate-v_WDep, v_RateIDNext, v_RateDateNext);

        IF v_RateDateNext > pRateDate AND v_RateNext > 0 THEN
           pRateID    := v_RateIDNext;
           v_Rate     := v_RateNext;
           pRateDate  := v_RateDateNext;
        END IF;
     END IF;

     return v_Rate;
  END GetFairValueFromRates;

  /*GAA */
-- Определить СС ФИ используя курсы, с учетом инсталяции РСХБ
--Т.к. кризисная переоценка учитавает дату поставки лота, то курс ТСС используемый в кризисной переоценке нельзя задать на выпуске в целом.
--Поэтому данный курс будет каждый раз опеределяться для конкретного лота, с учетом пользовательского расчета ТСС в банке при выполнении переоценки
--ТСС на выходе без НКД!!!   
    --ПРИ АДАПТАЦИИ НЕ ЗАБЫВАТЬ ПЕРЕНОСИТЬ ИЗМЕНЕНИЯ В ДИСТИРУБИВНОЙ GetFairValueFromRates СУДА!!!
   FUNCTION GetFairValueFromRates_RSHB( pFIID IN NUMBER,
                                  pCalcDate IN DATE,
                                  pRateID OUT NUMBER,
                                  pRateDate OUT DATE ) RETURN NUMBER
  AS
     v_FaceFI       NUMBER;
     v_CT1          NUMBER;
     v_CT4          NUMBER;
     v_CT10         NUMBER;
     v_CT18         NUMBER;
     v_CT23         NUMBER;
     v_Rate         NUMBER;
     v_RateNext     NUMBER;
     v_RateIDNext   NUMBER;
     v_RateDateNext DATE;
/*GAA*/     
--     v_SSDay        NUMBER := Rsb_Common.GetRegIntValue('SECUR\МСФО\СС_ДНЕЙ', 0);
     v_WDep         DATE := RSI_RsbCalendar.GetDateAfterWorkDay(pCalcDate,-90/*v_SSDay*/);--т.к. в макросе sp_calccost.mac именно данное значение!!!

     v_CT1001       NUMBER;
     v_WD30         DATE := RSI_RsbCalendar.GetDateAfterWorkDay(pCalcDate,-30);
/*GAA*/
     
  BEGIN
     SELECT FI.T_FACEVALUEFI
       INTO v_FaceFI
       FROM DFININSTR_DBT FI,
            davoiriss_dbt av
      WHERE FI.T_FIID = pFIID
        AND av.T_FIID = FI.T_FIID;

     IF v_FaceFI = RSI_RSB_FIInstr.NATCUR THEN
        -- в порядке приоритета ищутся курсы видов
        v_CT1001 := 1001; /*Мотивированное суждение*/           
        v_CT4  := nvl(Rsb_Common.GetRegIntValue('SECUR\ВИД КУРСА СРЕДНЕВЗВЕШЕННАЯ ЦЕНА', 0), 4); --RATETYPE_AVR_PRICE
        v_CT1  := nvl(Rsb_Common.GetRegIntValue('SECUR\ВИД КУРСА РЫНОЧНАЯ ЦЕНА', 0), 1); --RATETYPE_MARKET_PRICE
        v_CT23 := nvl(Rsb_Common.GetRegIntValue('SECUR\ВИД КУРСА ЦЕНА ЗАКРЫТ. БЛУМБЕРГ', 0), 23);        
           
        v_Rate := FindRate(1, pFIID, v_FaceFI, v_FaceFI, v_CT1001, pCalcDate, pCalcDate-v_WD30, pRateID, pRateDate);
        v_RateNext := FindRate(1, pFIID, v_FaceFI, v_FaceFI, v_CT4, pCalcDate, pCalcDate-v_WDep, v_RateIDNext, v_RateDateNext); 
        
        IF v_Rate > 0 and pRateDate!= UnknownDate Then
           return v_Rate;
        ELSE
           pRateID    := v_RateIDNext;
           v_Rate     := v_RateNext;
           pRateDate  := v_RateDateNext;
        END IF;

        v_RateNext := FindRate(1, pFIID, v_FaceFI, v_FaceFI, v_CT1, pCalcDate, pCalcDate-v_WDep, v_RateIDNext, v_RateDateNext);

        IF v_Rate > 0 and pRateDate!= UnknownDate Then
           return v_Rate;
        ELSE
           pRateID    := v_RateIDNext;
           v_Rate     := v_RateNext;
           pRateDate  := v_RateDateNext;           
        END IF; 
             
        v_RateNext := FindRate(1, pFIID, v_FaceFI, v_FaceFI, v_CT23, pCalcDate, pCalcDate-v_WDep, v_RateIDNext, v_RateDateNext);

        IF v_Rate > 0 and pRateDate!= UnknownDate Then
--insert into d_GAA_DBT (T_MESSAGE) values ('3 v_CT1. v_Rate= '||v_Rate||', pRateDate = '||pRateDate);          
           return v_Rate;
        ELSE
           pRateID    := v_RateIDNext;
           v_Rate     := v_RateNext;
           pRateDate  := v_RateDateNext; 
--insert into d_GAA_DBT (T_MESSAGE) values ('4 v_CT23. v_Rate= '||v_Rate||', pRateDate = '||pRateDate);                       
        END IF;         

     ELSE
        -- в порядке приоритета ищутся курсы видов
        v_CT1001 := 1001; /*Мотивированное суждение*/
        v_CT23 := nvl(Rsb_Common.GetRegIntValue('SECUR\ВИД КУРСА ЦЕНА ЗАКРЫТ. БЛУМБЕРГ', 0), 23);                    
        v_CT4  := nvl(Rsb_Common.GetRegIntValue('SECUR\ВИД КУРСА СРЕДНЕВЗВЕШЕННАЯ ЦЕНА', 0), 4); --RATETYPE_AVR_PRICE
        v_CT1  := nvl(Rsb_Common.GetRegIntValue('SECUR\ВИД КУРСА РЫНОЧНАЯ ЦЕНА', 0), 1); --RATETYPE_MARKET_PRICE
 
        v_Rate := FindRate(1, pFIID, v_FaceFI, v_FaceFI, v_CT1001, pCalcDate, pCalcDate-v_WD30, pRateID, pRateDate);
        v_RateNext := FindRate(1, pFIID, v_FaceFI, v_FaceFI, v_CT23, pCalcDate, pCalcDate-v_WDep, v_RateIDNext, v_RateDateNext);

        IF v_Rate > 0 and pRateDate!= UnknownDate Then
           return v_Rate;
        ELSE
           pRateID    := v_RateIDNext;
           v_Rate     := v_RateNext;
           pRateDate  := v_RateDateNext;           
        END IF;    

        v_RateNext := FindRate(1, pFIID, v_FaceFI, v_FaceFI, v_CT4, pCalcDate, pCalcDate-v_WDep, v_RateIDNext, v_RateDateNext);

        IF v_Rate > 0 and pRateDate!= UnknownDate Then
           return v_Rate;
        ELSE
           pRateID    := v_RateIDNext;
           v_Rate     := v_RateNext;
           pRateDate  := v_RateDateNext;           
        END IF;  

        v_RateNext := FindRate(1, pFIID, v_FaceFI, v_FaceFI, v_CT1, pCalcDate, pCalcDate-v_WDep, v_RateIDNext, v_RateDateNext);

        IF v_Rate > 0 and pRateDate!= UnknownDate Then
           return v_Rate;
        ELSE
           pRateID    := v_RateIDNext;
           v_Rate     := v_RateNext;
           pRateDate  := v_RateDateNext; 
        END IF;
     END IF;

     return v_Rate;
  END GetFairValueFromRates_RSHB;
  /*GAA*/

/* КД*/
 -- Определить СС ФИ используя курс "Мотивированное суждение"
  FUNCTION GetFairValueFromRatesMS( pFIID IN NUMBER, pCalcDate IN DATE, pRateDate IN OUT DATE ) RETURN NUMBER
  AS
     v_FaceFI       NUMBER;
     v_CT1          NUMBER;
     v_CT4          NUMBER;
     v_CT10         NUMBER;
     v_CT18         NUMBER;
     v_CT23         NUMBER;
     v_CT1001       NUMBER;
     v_Rate         NUMBER;
     v_RateID       NUMBER;
     v_RateNext     NUMBER;
     v_RateIDNext   NUMBER;
     v_RateDateNext DATE;
     v_IsBond       NUMBER;
     v_NKDRKind     NUMBER;
     v_WD30         DATE := RSI_RsbCalendar.GetDateAfterWorkDay(pCalcDate,-30);     
  BEGIN
     SELECT FI.T_FACEVALUEFI, av.T_NKDRound_Kind, /*FI.T_Name,*/
            DECODE(RSI_RSB_FIInstr.FI_AvrKindsGetRoot( 2, FI.t_AvoirKind ), RSI_RSB_FIInstr.AVOIRKIND_BOND,1, 0)
       INTO v_FaceFI, v_IsBond, /*v_AvName,*/ v_NKDRKind
       FROM DFININSTR_DBT FI,
            davoiriss_dbt av
      WHERE FI.T_FIID = pFIID
        AND av.T_FIID = FI.T_FIID;

     IF v_FaceFI = RSI_RSB_FIInstr.NATCUR THEN
        -- в порядке приоритета ищутся курсы видов
        v_CT1001 := 1001;
        v_Rate := FindRate(1, pFIID, v_FaceFI, v_FaceFI, v_CT1001, pCalcDate, pCalcDate-v_WD30, v_RateID, pRateDate);
     ELSE
        -- в порядке приоритета ищутся курсы видов
        v_CT1001 := 1001;
        v_Rate := FindRate(1, pFIID, v_FaceFI, v_FaceFI, v_CT1001, pCalcDate, pCalcDate-v_WD30, v_RateID, pRateDate);
     END IF;

     -- Если курс найден
     IF v_Rate > 0 AND pRateDate != UnknownDate THEN
        IF v_IsBond = 1 THEN
           IF v_NKDRKind = AVOIRISSNKDROUND_SUM THEN
              v_Rate := ROUND(v_Rate + RSI_RSB_FIInstr.CalcNKD_Ex_NoRound(pFIID, pCalcDate, 1, 0), 9);
           ELSE
              v_Rate := v_Rate + RSI_RSB_FIInstr.FI_CalcNKD(pFIID, pCalcDate, 1, 0);
           END IF;
        END IF;
     END IF;

     return v_Rate;
  END GetFairValueFromRatesMS;


  FUNCTION GetNKD( pFIID IN NUMBER,
                   pCalcDate IN DATE ) RETURN NUMBER
  AS
     v_IsBond       NUMBER;
     v_NKDRKind     NUMBER;
     v_NKD          NUMBER := 0;
  BEGIN
     SELECT av.T_NKDRound_Kind,
            DECODE(RSI_RSB_FIInstr.FI_AvrKindsGetRoot( 2, FI.t_AvoirKind ), RSI_RSB_FIInstr.AVOIRKIND_BOND,1, 0)
       INTO v_NKDRKind, v_IsBond
       FROM DFININSTR_DBT FI,
            davoiriss_dbt av
      WHERE FI.T_FIID = pFIID
        AND av.T_FIID = FI.T_FIID;

     IF v_IsBond = 1 THEN
        IF v_NKDRKind = AVOIRISSNKDROUND_SUM THEN
           v_NKD := ROUND(RSI_RSB_FIInstr.CalcNKD_Ex_NoRound(pFIID, pCalcDate, 1, 1), 9);
        ELSE
           v_NKD := RSI_RSB_FIInstr.CalcNKD_Ex(pFIID, pCalcDate, 1, 1);
        END IF;
     END IF;

     return v_NKD;
  END GetNKD;

  -- Алгоритм поиска рыночной котировки
  PROCEDURE AlgFindMarketRate( pFin DFININSTR_DBT%ROWTYPE,
                               pCalcDate IN DATE,
                               pEndCoupDate IN DATE,
                               pFairValue OUT NUMBER,
                               pRateID OUT NUMBER,
                               pFairValueDate OUT DATE,
                               pCat55 OUT NUMBER,
                               pCat60 OUT NUMBER,
                               pMsg OUT VARCHAR2 )
  AS
     v_SSDay NUMBER := Rsb_Common.GetRegIntValue('SECUR\МСФО\СС_ДНЕЙ', 0);
     v_WDep DATE := RSI_RsbCalendar.GetDateAfterWorkDay(pCalcDate,-v_SSDay);
     v_QuoteTerm NUMBER;
  BEGIN
     pFairValue := GetFairValueFromRates( pFin.t_FIID, pCalcDate, pRateID, pFairValueDate );

     -- Если СС определена
     IF pFairValue != 0 AND pFairValueDate != UnknownDate THEN
        pFairValue := pFairValue + GetNKD(pFin.t_FIID, pEndCoupDate);

        -- "Уровень исходных данных иерархии СС МСФО 13"
        pCat55 := 1;
        -- "Наблюдаемые исходные данные"
        pCat60 := 1;
        -- вид курса "Справедливая стоимость" устанавливается в процедуре ОТСС (sp_calccost.mac)

        IF pFairValueDate != pCalcDate THEN
           pMsg := 'Для выпуска ' || pFin.t_Name ||' за дату расчета не задан рыночный курс. Для расчета СС используется курс от ' || TO_CHAR(pFairValueDate, 'dd.mm.yy');

           v_QuoteTerm := RSI_RsbCalendar.getWorkDayCount(v_WDep, pFairValueDate);
           IF v_QuoteTerm <= 10 THEN
              pMsg := pMsg || '; ВНИМАНИЕ! Для выпуска ' || pFin.t_Name || ' через ' || TO_CHAR(v_QuoteTerm) || ' дней будет невозможно определить СС используя котировки рынка';
           END IF;
        END IF;
     ELSE
/* КД*/    pFairValue := GetFairValueFromRatesMS( pFin.t_FIID, pCalcDate, pFairValueDate );
/*insert into eps1 values('pFairValue2 = ', pFairValue);   */
     -- Если СС определена
         IF pFairValue != 0 AND pFairValueDate != UnknownDate THEN
            -- "Уровень исходных данных иерархии СС МСФО 13"
            pCat55 := 3;
            -- "Наблюдаемые исходные данные"
            pCat60 := 2;
         ELSE
        --DBMS_OUTPUT.PUT_LINE('Для выпуска ' || pFin.t_Name ||' не задан рыночный курс');
        pMsg := 'Для выпуска ' || pFin.t_Name ||' не задан рыночный курс';
     END IF;
     END IF;

  END AlgFindMarketRate;

  -- Алгоритм котировки сопоставимого выпуска (СС в ВН)
  PROCEDURE AlgCompareFIRate( pFin DFININSTR_DBT%ROWTYPE,
                              pCalcDate IN DATE,
                              pEndCoupDate IN DATE,
                              pFairValue OUT NUMBER,
                              pRateID OUT NUMBER,
                              pFairValueDate OUT DATE,
                              pCat55 OUT NUMBER,
                              pCat60 OUT NUMBER,
                              pMsg OUT VARCHAR2 )
  AS
     CURSOR cCompFI IS (SELECT t_AttrID
                          FROM dobjlink_dbt
                         WHERE T_GroupID = 13 -- сопоставимый выпуск
                           AND T_OBJECTTYPE = OBJTYPE_AVOIRISS
                           AND T_ATTRTYPE = OBJTYPE_AVOIRISS
                           AND t_ObjectID=LPAD( pFin.t_FIID, 10, '0' )
                       );
  BEGIN
     IF RSI_RSB_FIInstr.FI_AvrKindsGetRoot( 2, pFin.t_AvoirKind )
     IN (RSI_RSB_FIInstr.AVOIRKIND_BOND, RSI_RSB_FIInstr.AVOIRKIND_BILL) THEN
        FOR c IN cCompFI LOOP
           pFairValue := GetFairValueFromRates( c.t_AttrID, pCalcDate, pRateID, pFairValueDate );
           Exit WHEN pFairValue != 0 AND pFairValueDate <> UnknownDate;
        END LOOP;

        -- Если СС определена
        IF pFairValue != 0 AND pFairValueDate != UnknownDate THEN
           pFairValue := pFairValue + GetNKD(pFin.t_FIID, pEndCoupDate);

           -- "Уровень исходных данных иерархии СС МСФО 13"
           pCat55 := 2;
           -- "Наблюдаемые исходные данные"
           pCat60 := 1;
           -- вид курса "Справедливая стоимость" устанавливается в процедуре ОТСС (sp_calccost.mac)

           pMsg := 'Для выпуска ' || pFin.t_Name ||' не заданы рыночные курсы. Для расчета СС используется курс Сопоставимого выпуска от ' || TO_CHAR(pFairValueDate, 'dd.mm.yy');
        ELSE
           pMsg := 'Для выпуска ' || pFin.t_Name ||' не задан рыночный курс';
        END IF;

     END IF;

  END AlgCompareFIRate;

  -- Алгоритм дисконтирование денежных потоков (для ЦБ(покупка), ОЭБ в ВН, для РЕПО в ВЦ)
  PROCEDURE AlgDiscCostStream( pCalcKind IN NUMBER,
                               pDealID IN NUMBER,
                               pSumID IN NUMBER,
                               pCalcDate IN DATE,
                               pFin DFININSTR_DBT%ROWTYPE,
                               pFairValue OUT NUMBER,
                               pCat55 OUT NUMBER,
                               pCat60 OUT NUMBER,
                               pRateVal OUT NUMBER,
                               pAmortCalcKind OUT NUMBER
                             )
  AS
     v_tick            ddl_tick_dbt%rowtype;
     v_leg             ddl_leg_dbt%rowtype;
     v_leg2            ddl_leg_dbt%rowtype;
     v_lnk             dvsordlnk_dbt%rowtype;
     v_bnr             dvsbanner_dbt%rowtype;
     v_avr             DAVOIRISS_DBT%ROWTYPE;
     v_rq              ddlrq_dbt%rowtype;
     v_rq2             ddlrq_dbt%rowtype;
     v_OGroup          doprkoper_dbt.T_SYSTYPES%TYPE;
     v_BegDate         DATE;
     v_EndDate         DATE;
     v_Portfolio       NUMBER;
     v_RateKind        NUMBER;
     v_ContractSum     NUMBER;
     v_ContractFiID    NUMBER;
     p_ObjectKind      NUMBER;
     v_ToFI            NUMBER;
     v_Department      NUMBER;
     v_ProductKindID   NUMBER := 0;
     v_CircPeriodDays  NUMBER := 0;
     v_ServKind        NUMBER := 1;
     v_EPS             NUMBER := 0;
     v_MarketRateMin   NUMBER := 0; --РПСмин
     v_MarketRateMax   NUMBER := 0; --РПСмакс
     v_SourceDataLayer NUMBER := 0; --Уровень исходных данных
     v_SignDeviation   BOOLEAN := FALSE; --Отклонение существенно?
     v_EPS_Med         NUMBER := Rsb_Common.GetRegIntValue('SECUR\МСФО\МЕТОД_РАСЧЕТА_ОТКЛ_ЭПС', 0);
     v_Val1            NUMBER;
     v_Val2            NUMBER;
     v_Val3            NUMBER;
     v_Issuer          NUMBER := 0;
     v_DrawingDate     DATE;
     v_Termless        CHAR;
  BEGIN
     IF pCalcKind IN (CALCKIND_VS, CALCKIND_VA) THEN
        SELECT *
          INTO v_lnk
          FROM dvsordlnk_dbt lnk
         WHERE     LNK.T_CONTRACTID = pDealID
               AND LNK.T_BCID = pSumID --За неимением других параметров
               AND LNK.T_LINKKIND = CASE WHEN pCalcKind = CALCKIND_VS THEN 0 ELSE 1 END;

        SELECT *
          INTO v_leg
          FROM ddl_leg_dbt leg
         WHERE     leg.t_LegKind = 1
               AND leg.t_DealID = v_lnk.t_BCID
               AND t_LegID = 0;
        SELECT *
          INTO v_bnr
          FROM dvsbanner_dbt bnr
         WHERE     bnr.t_BCID = v_lnk.t_BCID;
         IF (pCalcKind = CALCKIND_VA) THEN
            SELECT *
              INTO v_leg2
              FROM ddl_leg_dbt leg
             WHERE     leg.t_LegKind = 0
                   AND leg.t_DealID = pDealID
                   AND t_LegID = 0;
         END IF;
         v_BegDate := rsb_bill.GetBNRFirstDate(v_lnk.t_BCID, v_lnk.T_CONTRACTID);
         v_Department := v_bnr.t_Department;
         p_ObjectKind := DL_VSBANNER;
         v_ServKind := CASE WHEN pCalcKind = CALCKIND_VA THEN 14 ELSE 8 END;
         v_Issuer := v_bnr.t_Issuer;
     ELSE
        SELECT * INTO v_tick
          FROM ddl_tick_dbt
         WHERE t_DealID = pDealID;
        SELECT * INTO v_leg
          FROM ddl_leg_dbt
         WHERE t_LegKind = 0
           AND t_DealID = v_tick.t_DealID
           AND t_LegID = 0;
        SELECT t_Termless INTO v_Termless
          FROM davoiriss_dbt
         WHERE t_FIID = pFin.t_FIID;
        p_ObjectKind := v_tick.t_BOfficeKind;
        v_Department := v_tick.t_Department;
        v_ServKind := 1;
        v_OGroup := get_OperationGroup(get_OperSysTypes(v_tick.t_DealType, v_tick.t_BofficeKind));
        v_DrawingDate := RSI_RSB_FIInstr.FI_GetNominalDrawingDate(pFin.t_FIID, v_Termless);
     END IF;

     pFairValue := 0;

     IF pCalcKind IN (CALCKIND_AVR, CALCKIND_OWN) THEN
        v_ToFI := pFin.t_FaceValueFI;
        IF GetRQ(v_rq, v_tick.t_BofficeKind, v_tick.t_DealID, RSI_DLRQ.DLRQ_SUBKIND_AVOIRISS, 1, RSI_DLRQ.DLRQ_TYPE_DELIVERY) = 0 THEN
           v_BegDate := GREATEST(v_rq.t_PlanDate, v_rq.t_FactDate);
        END IF;

        v_CircPeriodDays := v_DrawingDate - pCalcDate + 1;
        v_ContractSum    := pFin.t_FaceValue*v_avr.t_Qty; -- объем эмиссии (произведение номинала на количество ценных бумаг в выпуске)
        v_ContractFiID   := pFin.t_FaceValueFI; -- валюта номинала
        IF pCalcKind = CALCKIND_AVR THEN
           v_ProductKindID  := 60; -- Приобретенные облигации
           v_Portfolio      := v_tick.t_PortfolioID;
        ELSE
           v_ProductKindID  := 57; -- Привлечение ОЭБ
           v_Portfolio      := RSB_PMWRTOFF.KINDPORT_AC_OWN; -- Портфель = АС_ОЭБ
        END IF;

     ELSIF pCalcKind IN (CALCKIND_PREPO, CALCKIND_OREPO) THEN
        v_ToFI := v_leg.t_CFI;
        v_ProductKindID := case when IsSale(v_OGroup)=1 then 56 else 55 end; --Если сделка ОРЕПО, то 55 /*Вид банковского продукта - Размещение РЕПО */, иначе 56 /*Вид банковского продукта - Привлечение РЕПО*/
        IF GetRQ(v_rq, v_tick.t_BofficeKind, v_tick.t_DealID, RSI_DLRQ.DLRQ_SUBKIND_CURRENCY, 1, RSI_DLRQ.DLRQ_TYPE_PAYMENT) = 0 THEN
           v_BegDate := GREATEST(v_rq.t_PlanDate, v_rq.t_FactDate);
           v_ContractSum    := v_rq.t_Amount;
           v_ContractFiID   := v_rq.t_FIID;
        END IF;
        --Определить количество дней РЕПО
        IF GetRQ(v_rq2, v_tick.t_BofficeKind, v_tick.t_DealID, RSI_DLRQ.DLRQ_SUBKIND_CURRENCY, 2, RSI_DLRQ.DLRQ_TYPE_PAYMENT) = 0 THEN
           v_CircPeriodDays := v_rq2.t_PlanDate - v_rq.t_PlanDate;
        END IF;
        if( IsSale(v_OGroup)=1 )then
           v_Portfolio := rsb_pmwrtoff.KINDPORT_CURR_AC_BPP;
        else
           v_Portfolio := rsb_pmwrtoff.KINDPORT_CURR_AC_PVO;
        end if;
     ELSIF pCalcKind IN (CALCKIND_VA, CALCKIND_VS) THEN
       v_ProductKindID := case when pCalcKind = CALCKIND_VA then 59 else 58 end;
       v_ContractSum := v_leg.t_Principal;
       v_ContractFiID := v_leg.t_PFI;
       if (rsb_bill.GetBnrPlanRepayDate (v_leg.t_ID, v_EndDate) != 0) THEN
         RAISE NO_DATA_FOUND;
       END IF;
       v_CircPeriodDays := v_EndDate - pCalcDate;
       v_Portfolio := v_bnr.t_PortfolioID;
     END IF;

     IF v_CircPeriodDays > 0 AND v_ProductKindID > 0 THEN
        --определяется  ЭПС
        v_EPS := CalcEPS(pCalcKind, pDealID, pSumID, pCalcDate);

        --Определяется минимальное и максимальное значение РПС (с учетом настройки SECUR\МСФО\МЕТОД_РАСЧЕТА_ОТКЛ_ЭПС)
        IF GetRPS_LevelDevEPS
           ( pCalcKind,                      -- Вид расчета
             v_ServKind,                     -- Вид обслуживания
             v_Department,                   -- Подразделение ТС - ГО
             v_ProductKindID,                -- Вид банковского продукта
             0,                              -- Банковский продукт -не заполняем
             v_CircPeriodDays,               -- Срок в днях
             v_ContractSum,                  -- Сумма договора
             v_ContractFiID,                 -- Валюта договора
             0,                              -- Тип клиента - все клиенты
             pCalcDate,                      -- Дата - дата расчета
             v_MarketRateMin,                -- РПСмин
             v_MarketRateMax,                -- РПСмакс
             v_SourceDataLayer,              -- Уровень исходных данных
             v_Issuer,                       -- Эмитент
             pSumID                          -- ID лота (0 необязательный)
           ) = 0 THEN
           -- Алгоритм 2
           IF v_EPS_Med = ALG_CALCDEV_EPS2 THEN
             v_Val1 := v_MarketRateMax; -- РПСmax
             v_Val2 := v_EPS; -- ЭПС
             -- ставка по договору
             IF pCalcKind IN (CALCKIND_AVR, CALCKIND_OWN) THEN
               v_Val3 := v_avr.t_IncomeRate / 100;
             ELSIF pCalcKind IN (CALCKIND_PREPO, CALCKIND_OREPO) THEN
               v_Val3 := v_leg.t_IncomeRate / 100;
             ELSIF pCalcKind IN (CALCKIND_VA, CALCKIND_VS) THEN
               v_Val3 := rsb_bill.GetBnrRateOnDate(v_bnr.t_BCID, pCalcDate) / 1000000.0;
             END IF;
           -- Алгоритм 1
           ELSE
             v_Val1 := v_EPS; -- значение ЭПС (ЭПС)
             v_Val2 := v_MarketRateMin; -- минимальное  значение РПС (РПСmin)
             v_Val3 := v_MarketRateMax; -- максимальное значение РПС (РПСmax)
           END IF;

           --Определяется существенность отклонения ЭПС
           IF GetEssentialDev(LEVELESSENTIAL_EPS -- Вид уровня существенности = Отклонение ЭПС
                             ,v_Portfolio     -- Портфель
                             ,pCalcDate       -- Дата определения значения уровня существенности
                             ,v_Val1          -- Что сравниваем
                             ,v_Val2          -- С чем сравниваем 1
                             ,v_Val3          -- С чем сравниваем 2
                             ,p_ObjectKind    -- Вид объекта (для настройки ALG_CALCDEV_EPS2)
                             ,case when p_ObjectKind = DL_VSBANNER then v_bnr.t_BCID else v_tick.t_DealID end -- Объект (для настройки ALG_CALCDEV_EPS2)
                             ,v_SignDeviation -- Да/нет
                             ,v_RateKind      -- Наименование ставки используемой в дальнейшем
                             ,pRateVal        -- Значение ставки
                             ) = 0 THEN

              -- Расчет в дату Первоначального признания
              IF pCalcDate = v_BegDate THEN
                 -- сравнение по уровню существенности
                 IF      v_SignDeviation = FALSE
                     AND v_RateKind = RATE_KIND_EPS
                     AND (   (    pRateVal NOT IN (-1)
                             AND pCalcKind IN (CALCKIND_VS, CALCKIND_VA)) --Для бездоходных векселей 0 ЭПС не является ошибкой
                         OR (pRateVal NOT IN (-1, 0)))
                 THEN
                    IF pCalcKind IN (CALCKIND_VS, CALCKIND_VA) THEN
                       pFairValue := RSI_RSB_FIInstr.ConvSum(v_lnk.T_BCCOST, v_lnk.T_BCCFI, v_leg.t_PFI, pCalcDate, 2 );
                    ELSE
                       IF GetRQ(v_rq, v_tick.t_BofficeKind, v_tick.t_DealID, RSI_DLRQ.DLRQ_SUBKIND_CURRENCY, 1, RSI_DLRQ.DLRQ_TYPE_PAYMENT) = 0 THEN
                          pFairValue := RSI_RSB_FIInstr.ConvSum( v_rq.t_Amount, v_rq.t_FIID, v_ToFI, pCalcDate)
                                        + GetEssentialComis(pCalcKind, v_tick, v_leg, v_tick.t_DealDate, pCalcDate, v_ToFI);
                       END IF;
                    END IF;
                 END IF;
              END IF;

              IF v_SignDeviation = TRUE AND v_RateKind = RATE_KIND_RPS AND pRateVal != 0 THEN
                 pAmortCalcKind := RSB_SECUR.AMORTCALCKIND_RPS;
                 pFairValue := CalcAS_EPS(pCalcKind, pDealID, pSumID, pCalcDate, pRateVal);
              END IF;

              -- если СС определена
              IF pFairValue != 0 THEN
                 pCat55 := v_SourceDataLayer;
                 IF v_SourceDataLayer = 2 THEN
                    pCat60 := 1; --да
                 ELSIF v_SourceDataLayer = 3 THEN
                    pCat60 := 2; --нет
                 END IF;
              END IF;
           END IF;
        END IF;
     END IF;

  END AlgDiscCostStream;

  FUNCTION GetEndCoupDate( pFIID IN NUMBER, pCalcDate IN DATE ) RETURN DATE
  AS
    v_EndCoupDate DATE := UnknownDate;
    v_LastDayMonth DATE := LAST_DAY(pCalcDate);
    v_AvrKindsRoot NUMBER;
  BEGIN
    SELECT RSI_RSB_FIInstr.FI_AvrKindsGetRoot( 2, FI.t_AvoirKind ) INTO v_AvrKindsRoot
      FROM DFININSTR_DBT FI
     WHERE FI.T_FIID = pFIID;

    IF v_AvrKindsRoot = RSI_RSB_FIInstr.AVOIRKIND_BOND THEN
      IF pCalcDate = RSI_RsbCalendar.GetDateAfterWorkDay(v_LastDayMonth+1, -1) AND pCalcDate != v_LastDayMonth THEN
        v_EndCoupDate := v_LastDayMonth;
      ELSE
        v_EndCoupDate := pCalcDate;
      END IF;
    END IF;
    RETURN v_EndCoupDate;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN return UnknownDate;
  END;

  -- Определить справедливую стоимость (Порядок использования алгоритмов)
  --   ф-я внутри себя категории и др. объекты не изменяет, возвращает значения наружу
  --   если pAlgUsed=ALG_DISCCOSTSTREAM, то СС на кол-во бумаг из сделки (для ЦБ(покупка), ОЭБ в ВН, для РЕПО в ВЦ)
  FUNCTION SC_CalcFairValue( pFIID IN NUMBER, -- идентификатор ц/б
                             pCalcDate IN DATE, -- дата расчета
                             pCalcKind IN NUMBER, -- вид расчета (0 необязательный)
                             pDealID IN NUMBER, -- id сделки (0 необязательный)
                             pSumID IN NUMBER, -- id лота (0 необязательный)
                             pEndCoupDate IN DATE, -- Дата окончания периода расчета купона
                             pFairValue OUT NUMBER, -- сумма СС (в ВН). Если pAlgUsed=ALG_DISCCOSTSTREAM, то СС на кол-во бумаг из сделки (для ЦБ(покупка), ОЭБ в ВН, для РЕПО в ВЦ)
                             pMsg OUT VARCHAR2, -- сообщение
                             pRateID OUT NUMBER, -- курс, использованный для вычисления СС
                             pCat55 OUT NUMBER, -- значение для категории "Уровень исходных данных иерархии СС МСФО 13"
                             pCat60 OUT NUMBER, -- значение для категории "Наблюдаемые исходные данные"
                             pAlgUsed OUT NUMBER, -- примененный алгоритм
                             pEIR OUT NUMBER, -- ставка (может быть изменена, если применен алгоритм ALG_DISCCOSTSTREAM)
                             pAmortCalcKind OUT NUMBER -- Вид расчета АС (может быть изменен, если применен алгоритм ALG_DISCCOSTSTREAM)
                           ) RETURN NUMBER
  AS
     v_tick          DDL_TICK_DBT%ROWTYPE;
     v_fin           DFININSTR_DBT%ROWTYPE;
     v_avr           DAVOIRISS_DBT%ROWTYPE;
     v_OGroup        doprkoper_dbt.T_SYSTYPES%TYPE;
     v_FairValueDate DATE := UnknownDate;
     v_stat          NUMBER := 0;
  BEGIN
     SELECT * INTO v_fin
       FROM dfininstr_dbt
      WHERE t_FIID = pFIID;

     SELECT * INTO v_avr
       FROM davoiriss_dbt
      WHERE t_FIID = pFIID;

     pAlgUsed := 0;
     pCat55   := 0;
     pCat60   := 0;
     pEIR     := -1;
     pAmortCalcKind := 0;

     IF pDealID != 0 AND pCalcKind NOT IN (CALCKIND_VA,CALCKIND_VS) THEN
        SELECT * INTO v_tick
          FROM ddl_tick_dbt
         WHERE t_DealID = pDealID;

        v_OGroup := get_OperationGroup(get_OperSysTypes(v_tick.t_DealType, v_tick.t_BofficeKind));
     END IF;

     -- СС_РЕПО
     IF pCalcKind != 0 AND pDealID != 0 AND IsRepo(v_OGroup)=1 THEN
        AlgDiscCostStream( pCalcKind, v_tick.t_DealID, pSumID, pCalcDate, v_fin,
                           pFairValue, pCat55, pCat60, pEIR, pAmortCalcKind);
        IF pFairValue != 0 THEN
           pAlgUsed := ALG_DISCCOSTSTREAM;
        END IF;

     ELSE
        -- СС_Облигация
        IF RSI_RSB_FIInstr.FI_AvrKindsGetRoot( 2, v_fin.t_AvoirKind ) = RSI_RSB_FIInstr.AVOIRKIND_BOND THEN
           AlgFindMarketRate(v_fin, pCalcDate, pEndCoupDate, pFairValue, pRateID, v_FairValueDate, pCat55, pCat60, pMsg);
           IF pFairValue != 0 AND v_FairValueDate != UnknownDate THEN
              pAlgUsed := ALG_FINDMARKETRATE;
           ELSE
              AlgCompareFIRate(v_fin, pCalcDate, pEndCoupDate, pFairValue, pRateID, v_FairValueDate, pCat55, pCat60, pMsg);
              IF pFairValue != 0 AND v_FairValueDate != UnknownDate THEN
                 pAlgUsed := ALG_COMPAREFIRATE;
              ELSIF pCalcKind != 0 AND pDealID != 0 THEN
                 AlgDiscCostStream( pCalcKind, v_tick.t_DealID, pSumID, pCalcDate, v_fin,
                                    pFairValue, pCat55, pCat60, pEIR, pAmortCalcKind);
                 IF pFairValue != 0 THEN
                    pMsg := 'Для выпуска ' || v_fin.t_Name ||' не заданы рыночные курсы. Для расчета СС используется расчетный метод';
                    pAlgUsed := ALG_DISCCOSTSTREAM;
                 ELSE
                    pMsg := 'Для выпуска ' || v_fin.t_Name ||' невозможно определить Справедливую стоимость';
                 END IF;
              END IF;
           END IF;

        -- СС_Вексель
        ELSIF RSI_RSB_FIInstr.FI_AvrKindsGetRoot( 2, v_fin.t_AvoirKind ) = RSI_RSB_FIInstr.AVOIRKIND_BILL THEN
           AlgCompareFIRate(v_fin, pCalcDate, UnknownDate, pFairValue, pRateID, v_FairValueDate, pCat55, pCat60, pMsg);
           IF pFairValue != 0 AND v_FairValueDate != UnknownDate THEN
              pAlgUsed := ALG_COMPAREFIRATE;
           ELSIF pCalcKind != 0 AND pDealID != 0 THEN
              AlgDiscCostStream( pCalcKind, pDealID, pSumID, pCalcDate, v_fin,
                                 -- выходные
                                 pFairValue, pCat55, pCat60, pEIR, pAmortCalcKind);
              IF pFairValue != 0 THEN
                 pMsg := 'Для выпуска ' || v_fin.t_Name ||' не заданы рыночные курсы. Для расчета СС используется расчетный метод';
                 pAlgUsed := ALG_DISCCOSTSTREAM;
              ELSE
                 pMsg := 'Для выпуска ' || v_fin.t_Name ||' невозможно определить Справедливую стоимость';
              END IF;
           END IF;

        -- СС_Акция и пр.
        ELSE
           AlgFindMarketRate(v_fin, pCalcDate, UnknownDate, pFairValue, pRateID, v_FairValueDate, pCat55, pCat60, pMsg);
           IF pFairValue != 0 AND v_FairValueDate != UnknownDate THEN
              pAlgUsed := ALG_FINDMARKETRATE;
           END IF;
        END IF;
     END IF;

     -- если СС не определена
     IF pFairValue = 0 OR pAlgUsed = 0 THEN
        IF nvl(Length(pMsg),0) = 0 THEN
           pMsg := 'Для выпуска ' || v_fin.t_Name ||' невозможно определить Справедливую стоимость';
        END IF;
        v_stat := 1;
     END IF;

     RETURN v_stat;
  END SC_CalcFairValue;

  PROCEDURE LoadSubAcc(p_dlcontrid number)
  AS
    mproot DDLSUBACCMP_TMP%rowtype;
    subacc DDLSUBACCMP_TMP%rowtype;
    root_id number;
  begin
    delete from DDLSUBACCMP_TMP;

    for Curr in (select distinct sf.t_servkind serv_kind
                            from ddlcontrmp_dbt mp, dsfcontr_dbt sf
                           where mp.t_sfcontrid = sf.t_id
                             and mp.t_dlcontrid = p_dlcontrid)
    loop
        mproot.t_id := 0;
        mproot.t_parentID := 0;
        mproot.t_mpID := 0;
        mproot.t_accountID := 0;
        mproot.t_servkind := Curr.serv_kind;

        insert into DDLSUBACCMP_TMP values mproot returning t_id into mproot.t_id;

        for rs in (select MP.t_ID MP_ID, AC.t_ACCOUNTID ACC_ID
                     from DDLCONTRMP_DBT MP, DSFCONTR_DBT SF, DACCOUNT_DBT AC, DSETTACC_DBT SA, DSFSSI_DBT SI
                    where MP.T_DLCONTRID = p_dlcontrid
                      AND SF.T_ID = MP.T_SFCONTRID
                      AND SF.T_SERVKIND = Curr.serv_kind
                      AND SI.T_OBJECTTYPE = 659
                      AND SI.T_OBJECTID = LPAD(SF.T_ID, 10, '0')
                      AND SA.T_SETTACCID = SI.T_SETACCID
                      AND AC.T_ACCOUNT = SA.T_ACCOUNT
                      AND AC.T_CHAPTER = SA.T_CHAPTER
                      AND AC.T_CODE_CURRENCY = SA.T_FIID
                      AND AC.T_BALANCE IN (30601, 30606))
        loop
            subacc.t_id := 0;
            subacc.t_parentID := mproot.t_id;
            subacc.t_mpID := rs.MP_ID;
            subacc.t_accountID := rs.ACC_ID;
            subacc.t_servkind := Curr.serv_kind;

            insert into DDLSUBACCMP_TMP values subacc;
        end loop;
    end loop;

  end LoadSubAcc;

  PROCEDURE LoadRepoAccForTransf(p_Kind IN NUMBER, p_Currency IN NUMBER, p_Date IN DATE)
  AS
    TYPE acc_t IS TABLE OF DDLSUBACCMP_TMP%ROWTYPE INDEX BY BINARY_INTEGER;
    v_acc acc_t;
    v_IsBuyREPO NUMBER := case when p_Kind = 15 then 1 else 0 end;
  BEGIN

    EXECUTE IMMEDIATE 'TRUNCATE TABLE DDLSUBACCMP_TMP';

    select 0, 0, 0, ACC.T_ACCOUNTID, 0 BULK COLLECT INTO v_acc
      from ddl_tick_dbt tick, ddl_leg_dbt leg, dmccateg_dbt cat, dmcaccdoc_dbt mc, daccount_dbt acc
     where Rsb_Secur.IsRepo(Rsb_Secur.get_OperationGroupDocKind(Rsb_Secur.get_OperSysTypes(tick.t_DealType, tick.t_BofficeKind), tick.t_BofficeKind)) = 1
       and 1 = case when v_IsBuyREPO = 1
                    then Rsb_Secur.IsBuy(Rsb_Secur.get_OperationGroupDocKind(Rsb_Secur.get_OperSysTypes(tick.t_DealType, tick.t_BofficeKind), tick.t_BofficeKind))
                    else Rsb_Secur.IsSale(Rsb_Secur.get_OperationGroupDocKind(Rsb_Secur.get_OperSysTypes(tick.t_DealType, tick.t_BofficeKind), tick.t_BofficeKind))
               end
       and TICK.T_CLIENTID <= 0
       and LEG.T_DEALID = TICK.T_DEALID
       and MC.T_DOCID = LEG.T_ID
       and MC.T_DOCKIND = 176
       and MC.T_CURRENCY = p_Currency
       and MC.T_CATID = CAT.T_ID
       and CAT.T_CODE = case when v_IsBuyREPO = 1 then '-Расчеты' else '+Расчеты' end
       and CAT.T_LEVELTYPE = 1
       and MC.T_CONTRACTOR = (case when TICK.T_MARKETID > 0 and TICK.T_PARTYID > 0 then  TICK.T_PARTYID
                                   when TICK.T_MARKETID > 0 and TICK.T_BROKERID > 0  then TICK.T_BROKERID
                                   when TICK.T_MARKETID > 0 then  TICK.T_MARKETID
                                   else TICK.T_PARTYID
                              end
                             )
        and ACC.T_ACCOUNT = MC.T_ACCOUNT
        and ACC.T_CODE_CURRENCY = MC.T_CURRENCY
        and ACC.T_CHAPTER = MC.T_CHAPTER
        and ACC.T_OPEN_DATE <= p_Date
        and (ACC.T_CLOSE_DATE > p_Date or ACC.T_CLOSE_DATE = to_date('01.01.0001','DD.MM.YYYY'))
        group by ACC.T_ACCOUNTID;

    IF v_acc.COUNT > 0 THEN
       FORALL indx IN v_acc.FIRST .. v_acc.LAST
          INSERT INTO DDLSUBACCMP_TMP
               VALUES v_acc (indx);
    END IF;

  end LoadRepoAccForTransf;

  -- Добавить сообщение во временную таблицу лога
  PROCEDURE AddWarningLogTmp( pDealCode IN VARCHAR2, pErrCode IN NUMBER, pMsg IN VARCHAR2 )
  AS
  BEGIN
     INSERT INTO DSCREPLOG_TMP (T_WARNING) VALUES (pDealCode || '~' || TO_CHAR(pErrCode) || '~' || pMsg);
  END AddWarningLogTmp;

  -- Транслитерация кириллицы по ИКАО 9303
  FUNCTION Translit(pStr IN VARCHAR2) RETURN VARCHAR2
  AS
    ret VARCHAR2(32000) := pStr;
    l_upper BOOLEAN := FALSE;
  BEGIN
    IF upper(pStr) = pStr THEN
      l_upper := TRUE;
    END IF;
    ret := translate(ret, 'АБВГДЕЁЗИЙКЛМНОПРСТУФЫЭ', 'ABVGDEEZIIKLMNOPRSTUFYE');
    ret := REPLACE(ret, 'Ж', 'Zh');
    ret := REPLACE(ret, 'Х', 'Kh');
    ret := REPLACE(ret, 'Ц', 'Ts');
    ret := REPLACE(ret, 'Ч', 'Ch');
    ret := REPLACE(ret, 'Ш', 'Sh');
    ret := REPLACE(ret, 'Щ', 'Shch');
    ret := REPLACE(ret, 'Ъ', 'Ie');
    ret := REPLACE(ret, 'Ь', '');
    ret := REPLACE(ret, 'Ю', 'Iu');
    ret := REPLACE(ret, 'Я', 'Ia');
    IF l_upper THEN
      ret := upper(ret);
    ELSE
      ret := translate(ret, 'абвгдеёзийклмнопрстуфыэ', 'abvgdeeziiklmnoprstufye');
      ret := REPLACE(RET, 'ж', 'zh');
      ret := REPLACE(ret, 'х', 'kh');
      ret := REPLACE(RET, 'ц', 'ts');
      ret := REPLACE(RET, 'ч', 'ch');
      ret := REPLACE(RET, 'ш', 'sh');
      ret := REPLACE(ret, 'щ', 'shch');
      ret := REPLACE(ret, 'ъ', 'ie');
      ret := REPLACE(ret, 'ь', '');
      ret := REPLACE(ret, 'ю', 'iu');
      ret := REPLACE(ret, 'я', 'ia');
    END IF;

    RETURN ret;
  END;

  PROCEDURE SaveOwnTaxFromTMP( p_ID_Operation IN NUMBER,  -- Операция
                               p_ID_Step      IN NUMBER   -- Шаг операции
                             )
  IS
  BEGIN

     INSERT INTO DSCOWNTAX_DBT(T_ID, T_SHID, T_FACTRECEIVERID, T_PAYSUM, T_PAYCUR, T_TAXRATE, T_TAXSUM, T_ID_OPERATION, T_ID_STEP, T_TAXBASESUM, T_NOMSUM, T_COUPSUM, T_TAXBASESUM15, T_TAXSUM15)
     SELECT 0, TMP.T_SHID, TMP.T_FACTRECEIVERID, TMP.T_PAYSUM, TMP.T_PAYCUR, TMP.T_TAXRATE, TMP.T_TAXSUM, p_ID_Operation, p_ID_Step, TMP.T_TAXBASESUM, TMP.T_NOMSUM, TMP.T_COUPSUM, TMP.T_TAXBASESUM15, TMP.T_TAXSUM15
       FROM DSCOWNTAX_TMP TMP;

     DELETE FROM DSCOWNTAX_TMP;

  END; --SaveOwnTaxFromTMP

  PROCEDURE BackOutOwnTax( p_ID_Operation IN NUMBER,  -- Операция
                           p_ID_Step      IN NUMBER   -- Шаг операции
                         )
  IS
  BEGIN

     DELETE FROM DSCOWNTAX_DBT WHERE T_ID_OPERATION = p_ID_Operation AND T_ID_STEP = p_ID_Step;

  END; --BackOutOwnTax

   --Простановка признака "Учтен" на записях СС ПФИ в дату операции переоценки ПФИ
   PROCEDURE RSI_SetAccountedFrVal( pDocID IN NUMBER, pDocKind IN NUMBER, pOperDate  IN DATE, pID_Operation IN NUMBER, pID_Step IN NUMBER, pGrpID IN NUMBER )
   AS
   BEGIN
     update ddvnfrval_dbt fv
          set FV.T_ID_OPERATION = pID_Operation,
               FV.T_ID_STEP = pID_Step,
               FV.T_GRPID = pGrpID,
               FV.T_ACCOUNTED = 'X'
     where FV.T_DEALID = pDocID
        and  FV.T_DOCKIND = pDocKind
        and FV.T_DATE between NVL(( select max(FV_1.T_DATE)
                                                        from ddvnfrval_dbt fv_1
                                                     where FV_1.T_DEALID = pDocID
                                                         and  FV_1.T_DOCKIND = pDocKind
                                                         and FV_1.T_ACCOUNTED = 'X'
                                                         and FV_1.T_DATE < pOperDate
                                                    ),pOperDate) and  pOperDate
        and FV.T_ACCOUNTED != 'X';
   END RSI_SetAccountedFrVal;

   --Выполняет откат простановки признака "Учтен" при откате переоценки ПФИ
   PROCEDURE RSI_RestoreSetAccountedFrVal( pID_Operation IN NUMBER, pID_Step IN NUMBER, pGrpID IN NUMBER )
   AS
     v_Count NUMBER := 0;
   BEGIN
     SELECT count(1) into v_Count
       FROM DDVNFRVAL_DBT FV, DDVNFRVAL_DBT FV_1
     WHERE FV.T_ID_OPERATION = pID_Operation
          and FV.T_ID_STEP = pID_Step
          and FV.T_GRPID = pGrpID
          and FV.T_DEALID = FV_1.T_DEALID
          and FV.T_DOCKIND = FV_1.T_DOCKIND
          and FV.T_DATE <= FV_1.T_DATE
          and (FV.T_ID_OPERATION != FV_1.T_ID_OPERATION or
                  FV.T_ID_STEP != FV_1.T_ID_STEP or
                  FV.T_GRPID != FV_1.T_GRPID)
          and FV.T_ID < FV_1.T_ID
          and FV_1.T_ACCOUNTED = 'X';

     IF( v_Count > 0 ) THEN
        RSI_SetError(-20258, ''); -- Откатываемая операция по учету СС ПФИ не является последней
     END IF;

     update ddvnfrval_dbt fv
          set FV.T_ID_OPERATION = 0,
               FV.T_ID_STEP = 0,
               FV.T_GRPID = 0,
               FV.T_ACCOUNTED = chr(0)
     where FV.T_ID_OPERATION = pID_Operation
         and FV.T_ID_STEP = pID_Step
         and FV.T_GRPID = pGrpID;
   END RSI_RestoreSetAccountedFrVal;

  --Функция получения кода на заданную дату
  FUNCTION SC_GetObjCodeOnDate( p_ObjectType IN NUMBER,
                                p_CodeKind   IN NUMBER,
                                p_ObjectID   IN NUMBER,
                                p_Date       IN DATE DEFAULT NULL
                              ) RETURN VARCHAR2
  AS
    m_Code dobjcode_dbt.t_code%TYPE;
  BEGIN
    IF p_Date IS NULL OR p_Date = TO_DATE('01.01.0001', 'DD.MM.YYYY') THEN
      BEGIN
        SELECT t_code INTO m_Code
          FROM dobjcode_dbt
         WHERE t_objectid = p_ObjectID
           AND t_objecttype = p_ObjectType
           AND t_codekind = p_CodeKind
           AND t_bankclosedate = TO_DATE('01.01.0001', 'DD.MM.YYYY')
           AND ROWNUM = 1;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN NULL;
      END;
    ELSE
      BEGIN
        SELECT t_code INTO m_Code
          FROM dobjcode_dbt
         WHERE t_objectid = p_ObjectID
           AND t_objecttype = p_ObjectType
           AND t_codekind = p_CodeKind
           AND p_Date >= t_bankdate
           AND p_Date < DECODE(t_bankclosedate, TO_DATE('01.01.0001', 'DD.MM.YYYY'), TO_DATE('31.12.9999', 'DD.MM.YYYY'), t_bankclosedate)
           AND ROWNUM = 1;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN NULL;
      END;
    END IF;
    RETURN m_Code;
  END;

  /*Проверить соответствие схемы расчетов виду рынка и принадлежности средств (используется в листалке схем расчетов в панели расчетов с биржей и обработки итогв вал торгов)*/
  FUNCTION DL_IsSuitMarketScheme( pSchemeID IN NUMBER, pMoneySource IN NUMBER, pMarketKind IN NUMBER ) RETURN NUMBER
  AS
    v_IsPool NUMBER := Rsb_Common.GetRegIntValue('SECUR\БИРЖЕВЫЕ ОПЕРАЦИИ\КЛИРИНГ.ОБЕСП.ДЛЯ СОБСТВ.СДЕЛОК', 0);
    v_RetVal NUMBER := 0;
  BEGIN
    select 1 into v_RetVal
      from ddlmarket_dbt mt
     where mt.t_ID = pSchemeID
       and exists( select OFFICE.T_OFFICEID
                     from dptoffice_dbt office
                    where office.t_PartyID = mt.t_Centr
                      and office.T_OFFICEID = mt.t_CentrOffice
                      and ( (v_IsPool = 1 and pMoneySource = 1 and upper(OFFICE.T_OFFICENAME) like '%РЫНОК_Т+%') or
                            (pMarketKind = DV_MARKETKIND_STOCK and mt.t_FI_Kind = 2) or
                               ((v_IsPool = 0 or pMoneySource = 2) and pMarketKind = DV_MARKETKIND_DERIV and upper(OFFICE.T_OFFICENAME) like '%СРОЧН%') or
                               ((v_IsPool = 0 or pMoneySource = 2) and pMarketKind = DV_MARKETKIND_CURRENCY and upper(OFFICE.T_OFFICENAME) like '%ВАЛЮТН%') or
                               ((v_IsPool = 0 or pMoneySource = 2) and pMarketKind = DV_MARKETKIND_SPFIMARKET and upper(OFFICE.T_OFFICENAME) like '%СПФИ%')
                          )
                 )
       and exists( select MARKETACC.T_ACCOUNT
                     from ddlmarketacc_dbt marketacc
                    where marketacc.t_MarketID = mt.t_ID
                      and MARKETACC.T_TYPEOWN = pMoneySource
                 );
    RETURN v_RetVal;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN return 0;
  END DL_IsSuitMarketScheme;

  /*Проверить соответствие схемы расчетов расчетному коду заданного рынка и принадлежности средств */
  FUNCTION DL_IsSuitMarketSchemeInSettleCode( pSchemeID IN NUMBER, pMarketKind IN NUMBER, pMoneySource IN NUMBER ) RETURN NUMBER
  AS
    v_RetVal NUMBER := 0;
  BEGIN
    select 1 into v_RetVal
      from ddlmarket_dbt mt
     where mt.t_ID = pSchemeID
       and (   exists( select 1
                         from ddl_extsettlecode_dbt extcode
                        where extcode.t_MarketSchemeID = mt.t_ID
                          and extcode.t_CodeType = pMoneySource
                          and extcode.t_ParentID = 0
                          and (    (extcode.t_MarketKind = Rsb_Secur.DL_MARKETKIND_SETTLE_STOCK_TPLUS and extcode.t_InPool = chr(88)) 
                                or (extcode.t_MarketKind = pMarketKind and extcode.t_InPool <> chr(88))
                              )
                          and not exists( select 1
                                            from ddl_extscanalytics_dbt analytics
                                           where analytics.t_MarketKind = pMarketKind
                                             and analytics.t_CodeID = extcode.t_ID )                                           
                     )
            or exists( select 1
                      from ddl_extscanalytics_dbt analytics, ddl_extsettlecode_dbt extcode
                     where analytics.t_MarketSchemeID = mt.t_ID
                       and analytics.t_MarketKind = pMarketKind
                       and analytics.t_CodeID = extcode.t_ID
                       and extcode.t_CodeType = pMoneySource
                       and extcode.t_MarketKind = Rsb_Secur.DL_MARKETKIND_SETTLE_STOCK_TPLUS and extcode.t_InPool = chr(88)
                      )
                 );
    RETURN v_RetVal;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN return 0;
  END DL_IsSuitMarketSchemeInSettleCode;
  
  /*Проверить соответствие схемы расчетов заданному сектору расчетного центра и принадлежности средств */
  FUNCTION DL_IsSuitMarketSchemeByOffice( pSchemeID IN NUMBER, pCentrID IN NUMBER, pCentrOfficeID IN NUMBER, pMoneySource IN NUMBER ) RETURN NUMBER
  AS
    v_RetVal NUMBER := 0;
  BEGIN
    select 1 into v_RetVal
      from ddlmarket_dbt mt
     where mt.t_ID = pSchemeID
       and mt.t_Centr = pCentrID
       and mt.t_CentrOffice = pCentrOfficeID
       and exists( select MARKETACC.T_ACCOUNT
                     from ddlmarketacc_dbt marketacc
                    where marketacc.t_MarketID = mt.t_ID
                      and MARKETACC.T_TYPEOWN = pMoneySource
                 );
    RETURN v_RetVal;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN return 0;
  END DL_IsSuitMarketSchemeByOffice;

  function get_BA_Kind(FI_Kind number, AvoirKind number, root number) return number DETERMINISTIC
  AS
  FIKIND_DERIVATIVE CONSTANT NUMBER := RSI_RSB_FIINSTR.FIKIND_DERIVATIVE;
  FIKIND_AVOIRISS CONSTANT NUMBER := RSI_RSB_FIINSTR.FIKIND_AVOIRISS;
  FIKIND_CURRENCY CONSTANT NUMBER := RSI_RSB_FIINSTR.FIKIND_CURRENCY;
  FIKIND_INDEX CONSTANT NUMBER := RSI_RSB_FIINSTR.FIKIND_INDEX;
  FIKIND_METAL CONSTANT NUMBER := RSI_RSB_FIINSTR.FIKIND_METAL;
  FIKIND_ARTICLE CONSTANT NUMBER := RSI_RSB_FIINSTR.FIKIND_ARTICLE;
  DERIVATIVE_FUTURES CONSTANT NUMBER := RSB_DERIVATIVES.DV_DERIVATIVE_FUTURES;
  DERIVATIVE_FORWARD CONSTANT NUMBER := 3;
  AVOIRISSKIND_INVESTMENT_SHARE CONSTANT NUMBER := RSI_RSB_fiinstr.AVOIRKIND_INVESTMENT_SHARE;
  AVOIRISSKIND_DEPO_RECEIPT CONSTANT NUMBER := RSI_RSB_FIINSTR.AVOIRKIND_DEPOSITORY_RECEIPT;
  AVOIRISSKIND_SHARE CONSTANT NUMBER := RSI_RSB_FIINSTR.AVOIRKIND_SHARE;
  AVOIRISSKIND_COUPON_BOND CONSTANT NUMBER := RSI_RSB_FIINSTR.AVOIRKIND_BOND;
  AVOIRISSKIND_BASKET CONSTANT NUMBER := RSI_RSB_FIINSTR.AVOIRISSKIND_BASKET;
  
  ba number;
  nvr number;
  begin
    nvr := NVL(root, 0);
    CASE 
        WHEN FI_Kind=FIKIND_DERIVATIVE AND AvoirKind=DERIVATIVE_FUTURES THEN ba := DVBASEACT_FUTURES; 
        WHEN FI_Kind=FIKIND_DERIVATIVE AND AvoirKind=DERIVATIVE_FORWARD THEN ba := DVBASEACT_FORWARD; 
        WHEN FI_Kind=FIKIND_AVOIRISS AND nvr=AVOIRISSKIND_INVESTMENT_SHARE THEN ba := DVBASEACT_INVESTMENT_SHARE; 
        WHEN FI_Kind=FIKIND_AVOIRISS AND nvr=AVOIRISSKIND_DEPO_RECEIPT THEN ba := DVBASEACT_DEPOSITORY_RECEIPT; 
        WHEN FI_Kind=FIKIND_AVOIRISS AND nvr=AVOIRISSKIND_SHARE THEN ba := DVBASEACT_SHARE; 
        WHEN FI_Kind=FIKIND_AVOIRISS AND nvr=AVOIRISSKIND_COUPON_BOND THEN ba := DVBASEACT_BOND; 
        WHEN FI_Kind=FIKIND_AVOIRISS AND AvoirKind=AVOIRISSKIND_BASKET THEN ba := DVBASEACT_BASKET; 
        WHEN FI_Kind=FIKIND_CURRENCY THEN ba := DVBASEACT_CURRENCY; 
        WHEN FI_Kind=FIKIND_INDEX THEN ba := DVBASEACT_INDEX; 
        WHEN FI_Kind=FIKIND_METAL THEN ba := DVBASEACT_METAL; 
        WHEN FI_Kind=FIKIND_ARTICLE THEN ba := DVBASEACT_ARTICLE; 
        ELSE ba := DVBASEACT_NOTDEF; 
    END case;
  return ba;
  end get_BA_Kind;

  function getObjCode(ObjectID number, ObjectType number, CodeKind number) return varchar2
  AS
  
  set_char CONSTANT char := 'X';
  unset_char CONSTANT char := chr(0);
  BDATE_ZERO CONSTANT DATE := TO_DATE('01.01.0001', 'DD.MM.YYYY');
  
  code varchar2(35) := NULL;
  dup char;
  state number;
  cldate date;
  begin
    select T.T_DUPLICATE into dup from dobjkcode_dbt t where T.T_CODEKIND = CodeKind and T.T_OBJECTTYPE = ObjectType;
    case
        when dup=unset_char then
            select T.T_CODE into code 
                from dobjcode_dbt t 
                where 
                    T.T_OBJECTTYPE = ObjectType 
                    and T.T_CODEKIND = CodeKind
                    and T.T_OBJECTID = ObjectID
                    and T.T_BANKCLOSEDATE = BDATE_ZERO;
        when dup=set_char then
            select T.T_CODE into code
                from dobjcode_dbt t 
                where 
                    T.T_OBJECTTYPE = ObjectType 
                    and T.T_CODEKIND = CodeKind
                    and T.T_OBJECTID = ObjectID
                    and T.T_STATE = 0;
        else
            code := NULL;
    end case;
    return code;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN return NULL;
  end getObjCode;
  
  
  
  function get_FI_Code(FIID number, ObjectType number, CodeKind number) return varchar2
  AS
  
  code varchar2(35) := NULL;
  
  begin
    CASE 
        WHEN CodeKind=CODE_FI_Code or CodeKind = 0 THEN
            select T.T_FI_CODE into code from dfininstr_dbt t where T.T_FIID = FIID;
        WHEN CodeKind=CODE_ISO_Number THEN
            select T.T_ISO_NUMBER into code from dfininstr_dbt t where T.T_FIID = FIID;
        WHEN CodeKind=CODE_Ccy THEN
            select T.T_CCY into code from dfininstr_dbt t where T.T_FIID = FIID;
        WHEN CodeKind=CODE_CodeInAccount THEN
            select T.T_CODEINACCOUNT into code from dfininstr_dbt t where T.T_FIID = FIID;
        WHEN CodeKind=CODE_LSIN THEN
            select T.T_LSIN into code from davoiriss_dbt t where T.T_FIID = FIID;
        WHEN CodeKind=CODE_ISIN THEN
            select T.T_ISIN into code from davoiriss_dbt t where T.T_FIID = FIID;
        ELSE
            if CodeKind > MAX_INNER_SYSTEM_CODEKIND then
                code := getObjCode(FIID, ObjectType, CodeKind);
            else
                code := NULL;
            end if;
    END case;
    code := NVL(code, '');
    return code;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN return '';
  end get_FI_Code;

  FUNCTION GetPmWrtGrpCntFI(p_GrpID IN NUMBER)
    RETURN NUMBER DETERMINISTIC
  AS
    v_cnt NUMBER := 0;
  BEGIN

    SELECT Count(1) INTO v_cnt
      FROM dpmwrtgrpfi_dbt
     WHERE t_GrpID = p_GrpID;

    RETURN v_cnt;

  END GetPmWrtGrpCntFI;


  FUNCTION GetPmWrtGrpAvrName(p_GrpID IN NUMBER) RETURN PmWrtGrpAvrName_t DETERMINISTIC
  AS
    v_grp     dpmwrtgrp_dbt%ROWTYPE;
    v_dl_comm ddl_comm_dbt%ROWTYPE;
    v_AvrName PmWrtGrpAvrName_t;
    v_DefStr  PmWrtGrpAvrName_t := 'Все';
  BEGIN

    v_AvrName := v_DefStr;

    SELECT * INTO v_grp FROM DPMWRTGRP_DBT WHERE T_ID = p_GrpID;

    IF v_grp.t_Party = -1 OR GetPmWrtGrpCntFI(p_GrpID) = 1 THEN
      SELECT avr.t_Name INTO v_AvrName
        FROM davrkinds_dbt avr, dpmwrtgrpfi_dbt grpfi, dfininstr_dbt fin
       WHERE grpfi.t_GrpID = p_GrpID
         AND fin.t_FIID = grpfi.t_FIID
         AND avr.t_FI_Kind = fin.t_FI_Kind
         AND avr.t_AvoirKind = fin.t_AvoirKind;
    ELSE
      SELECT * INTO v_dl_comm FROM DDL_COMM_DBT WHERE T_DOCKIND = v_grp.t_DocKind AND T_DOCUMENTID = v_grp.t_DocID;

      IF v_dl_comm.t_AvoirKind > 0 AND v_dl_comm.t_Flag1 = CHR(0) THEN
        SELECT avr.t_Name INTO v_AvrName
          FROM davrkinds_dbt avr
         WHERE avr.t_FI_Kind = RSI_RSB_FIInstr.FIKIND_AVOIRISS
           AND avr.t_AvoirKind = v_dl_comm.t_AvoirKind;
      END IF;
    END IF;

    RETURN v_AvrName;

    EXCEPTION
      WHEN OTHERS THEN
        RETURN v_DefStr;

  END GetPmWrtGrpAvrName;

  FUNCTION GetPmWrtGrpFiCode(p_GrpID IN NUMBER) RETURN PmWrtGrpFiCode_t DETERMINISTIC
  AS
    v_grp     dpmwrtgrp_dbt%ROWTYPE;
    v_dl_comm ddl_comm_dbt%ROWTYPE;
    v_FiCode  PmWrtGrpFiCode_t;
    v_DefStr  PmWrtGrpFiCode_t := 'Все';
  BEGIN

    v_FiCode := v_DefStr;

    SELECT * INTO v_grp FROM DPMWRTGRP_DBT WHERE T_ID = p_GrpID;

    IF v_grp.t_Party = -1 OR GetPmWrtGrpCntFI(p_GrpID) = 1 THEN
      SELECT fin.t_FI_Code INTO v_FiCode
        FROM dpmwrtgrpfi_dbt grpfi, dfininstr_dbt fin
       WHERE grpfi.t_GrpID = p_GrpID
         AND fin.t_FIID = grpfi.t_FIID;
    ELSE
      SELECT * INTO v_dl_comm FROM DDL_COMM_DBT WHERE T_DOCKIND = v_grp.t_DocKind AND T_DOCUMENTID = v_grp.t_DocID;

      IF v_dl_comm.t_FIID > 0 AND v_dl_comm.t_Flag1 = CHR(0) THEN
        SELECT fin.t_FI_Code INTO v_FiCode
          FROM dfininstr_dbt fin
         WHERE fin.t_FIID = v_dl_comm.t_FIID;
      END IF;
    END IF;

    RETURN v_FiCode;

    EXCEPTION
      WHEN OTHERS THEN
        RETURN v_DefStr;

  END GetPmWrtGrpFiCode;

  FUNCTION GetPmWrtGrpFiName(p_GrpID IN NUMBER) RETURN PmWrtGrpFiName_t DETERMINISTIC
  AS
    v_grp     dpmwrtgrp_dbt%ROWTYPE;
    v_dl_comm ddl_comm_dbt%ROWTYPE;
    v_FiName  PmWrtGrpFiName_t;
    v_DefStr  PmWrtGrpFiName_t := 'Все';
  BEGIN

    v_FiName := v_DefStr;

    SELECT * INTO v_grp FROM DPMWRTGRP_DBT WHERE T_ID = p_GrpID;

    IF v_grp.t_Party = -1 OR GetPmWrtGrpCntFI(p_GrpID) = 1 THEN
      SELECT fin.t_Name INTO v_FiName
        FROM dpmwrtgrpfi_dbt grpfi, dfininstr_dbt fin
       WHERE grpfi.t_GrpID = p_GrpID
         AND fin.t_FIID = grpfi.t_FIID;
    ELSE
      SELECT * INTO v_dl_comm FROM DDL_COMM_DBT WHERE T_DOCKIND = v_grp.t_DocKind AND T_DOCUMENTID = v_grp.t_DocID;

      IF v_dl_comm.t_FIID > 0 AND v_dl_comm.t_Flag1 = CHR(0) THEN
        SELECT fin.t_Name INTO v_FiName
          FROM dfininstr_dbt fin
         WHERE fin.t_FIID = v_dl_comm.t_FIID;
      END IF;
    END IF;

    RETURN v_FiName;

    EXCEPTION
      WHEN OTHERS THEN
        RETURN v_DefStr;

  END GetPmWrtGrpFiName;

  FUNCTION GetPmWrtGrpFiFvCode(p_GrpID IN NUMBER) RETURN PmWrtGrpFiFvCode_t DETERMINISTIC
  AS
    v_grp       dpmwrtgrp_dbt%ROWTYPE;
    v_dl_comm   ddl_comm_dbt%ROWTYPE;
    v_FiFvCode  PmWrtGrpFiFvCode_t;
    v_DefStr    PmWrtGrpFiFvCode_t := 'Все';
  BEGIN

    v_FiFvCode := v_DefStr;

    SELECT * INTO v_grp FROM DPMWRTGRP_DBT WHERE T_ID = p_GrpID;

    IF v_grp.t_Party = -1 OR GetPmWrtGrpCntFI(p_GrpID) = 1 THEN
      SELECT curr.t_ISO_Number INTO v_FiFvCode
        FROM dpmwrtgrpfi_dbt grpfi, dfininstr_dbt fin, dfininstr_dbt curr
       WHERE grpfi.t_GrpID = p_GrpID
         AND fin.t_FIID = grpfi.t_FIID
         AND curr.t_FIID = fin.t_FaceValueFI;
    ELSE
      SELECT * INTO v_dl_comm FROM DDL_COMM_DBT WHERE T_DOCKIND = v_grp.t_DocKind AND T_DOCUMENTID = v_grp.t_DocID;

      IF v_dl_comm.t_Currency > -1 AND v_dl_comm.t_Flag1 = CHR(0) THEN
        SELECT fin.t_ISO_Number INTO v_FiFvCode
          FROM dfininstr_dbt fin
         WHERE fin.t_FIID = v_dl_comm.t_Currency;
      END IF;
    END IF;

    RETURN v_FiFvCode;

    EXCEPTION
      WHEN OTHERS THEN
        RETURN v_DefStr;

  END GetPmWrtGrpFiFvCode;

  PROCEDURE FillItogClirAccTrn( p_DocID IN NUMBER, p_DocKind IN NUMBER )                                                   
  IS                                                                          
    v_dl_comm ddl_comm_dbt%ROWTYPE;
    TYPE acctrn_t IS TABLE OF DDL_ITOGCLIRACCTRN_TMP%ROWTYPE INDEX BY BINARY_INTEGER;
    v_acctrn acctrn_t;                                                        
  BEGIN

    EXECUTE IMMEDIATE 'TRUNCATE TABLE DDL_ITOGCLIRACCTRN_TMP';

    if( p_DocKind != DL_SETTLEMENTFCURM ) then
       return;
    end if;

    begin
       SELECT * INTO v_dl_comm FROM DDL_COMM_DBT WHERE T_DocumentID = p_DocID and T_DocKind = p_DocKind;
    exception
      when OTHERS then return;
    end;

    if( v_dl_comm.T_ContractKind != DL_MARKETKIND_SETTLE_STOCK_T0 and v_dl_comm.T_ContractKind != DL_MARKETKIND_SETTLE_STOCK_TPLUS )then
       return;
    end if;

    with doc as( SELECT comm.t_DocumentID t_id, comm.t_DocKind
                   FROM DDL_COMM_DBT comm
                  WHERE comm.t_CommDate = v_dl_comm.t_CommDate
                    AND  comm.t_DocKind  = DL_SCACCOUNTING
                    AND comm.t_Flag4    = 'X' --Признак "Расчеты с биржей"
                    AND comm.t_CommStatus = 2
               )
    SELECT DISTINCT trans.T_AcctrnID BULK COLLECT INTO v_acctrn
      FROM doc, ddlgrdoc_dbt grdoc, dacctrn_dbt trans,
           (SELECT acc.t_Account
              FROM dmccateg_dbt cat, dmcaccdoc_dbt acc, dmctempl_dbt templ  
             WHERE cat.t_LevelType = 1 
               AND (cat.t_Code = '+Биржа' or cat.t_Code = '-Биржа')
               AND acc.t_CatNum = cat.t_Number
               AND acc.t_IsCommon = 'X'
               AND acc.t_MarketPlaceID = v_dl_comm.T_TraderID
               AND acc.t_MarketPlaceOfficeID = v_dl_comm.T_PartyOfficeID 
               AND acc.t_CatID = templ.t_CatID 
               AND acc.t_TemplNum = templ.t_Number 
               AND templ.t_Value1 = case when v_dl_comm.t_OperSubKind = RSB_Derivatives.ALG_SP_MONEY_SOURCE_OWN then 1 else 2 end 
          GROUP BY acc.t_Account) acc_market,
           (SELECT acc.t_Account
              FROM dmccateg_dbt cat, dmcaccdoc_dbt acc, dmctempl_dbt templ  
             WHERE cat.t_LevelType = 1 
   --            AND (cat.t_Code = 'Клиринговый счет' or cat.t_Code = '+Обеспечение' or cat.t_Code = '-Обеспечение')
               AND acc.t_CatNum = cat.t_Number
               AND acc.t_IsCommon = 'X'
               AND acc.t_MarketPlaceID = v_dl_comm.T_TraderID
               AND acc.t_MarketPlaceOfficeID = v_dl_comm.T_PartyOfficeID
               AND acc.t_CatID = templ.t_CatID 
               AND acc.t_TemplNum = templ.t_Number 
               AND (   (cat.t_Code = 'Клиринговый счет' AND templ.t_Value1 = case when v_dl_comm.t_OperSubKind = RSB_Derivatives.ALG_SP_MONEY_SOURCE_OWN then 1 else 2 end)
                     OR(v_dl_comm.t_Flag1 = 'X' AND (cat.t_Code = '+Обеспечение' or cat.t_Code = '-Обеспечение') AND templ.t_Value2 = case when v_dl_comm.t_OperSubKind = RSB_Derivatives.ALG_SP_MONEY_SOURCE_OWN then 1 else 2 end)
               )
          GROUP BY acc.t_Account) acc_clearing         
     WHERE grdoc.T_ServDocID = doc.t_ID
       AND grdoc.T_ServDocKind = doc.t_DocKind
       AND grdoc.t_DocKind = 1
       AND trans.T_AcctrnID = grdoc.T_DocID
       AND (    (trans.T_Account_Payer = acc_market.t_Account and trans.T_Account_Receiver = acc_clearing.t_Account)
             or (trans.T_Account_Payer = acc_clearing.t_Account and trans.T_Account_Receiver = acc_market.t_Account)
           );

    IF v_acctrn.COUNT > 0 THEN                                                
       FORALL indx IN v_acctrn.FIRST .. v_acctrn.LAST                         
          INSERT INTO DDL_ITOGCLIRACCTRN_TMP                                         
               VALUES v_acctrn (indx);                                        
    END IF;                                                                   

    if( v_dl_comm.T_ContractKind = DL_MARKETKIND_SETTLE_STOCK_TPLUS and v_dl_comm.T_Flag1 = 'X' )then
    
        with doc as( SELECT comm.t_DocumentID t_id, comm.t_DocKind
                       FROM DDL_COMM_DBT comm 
                      WHERE comm.t_CommDate = v_dl_comm.t_CommDate 
                        AND comm.t_DocKind  = DL_DVCURMARKET
                        AND comm.t_OperSubKind  = v_dl_comm.t_OperSubKind
                        AND comm.t_CommStatus = 2
                     UNION ALL
                     SELECT oper.t_id t_id, oper.t_dockind
                       FROM DDVOPER_DBT oper, DOPRKOPER_DBT kop
                      WHERE oper.t_Date = v_dl_comm.t_CommDate
                        AND  oper.t_OperKind = kop.t_Kind_Operation
                        AND  (kop.t_SysTypes = chr(1) or kop.t_SysTypes = 'S')
                        AND  oper.T_Flag1 = v_dl_comm.t_OperSubKind
                        AND  oper.T_State = 2
                        /*PNV*/
                     UNION ALL 
                     SELECT comm2.T_NETTINGID  id, comm2.t_DocKind  
                       FROM DDL_NETT_DBT comm2 
                      WHERE comm2.t_valueDate = v_dl_comm.t_CommDate 
                        and COMM2.T_CONTRACTOR = 1181 
                        and COMM2.T_IDENTPROGRAM = 12   
                        /*PNV*/                        
                   )
        SELECT DISTINCT trans.T_AcctrnID BULK COLLECT INTO v_acctrn
          FROM doc, doprdocs_dbt oprdocs, dacctrn_dbt trans, doproper_dbt oper,
               (SELECT acc.t_Account
                  FROM dmccateg_dbt cat, dmcaccdoc_dbt acc, dmctempl_dbt templ  
                 WHERE cat.t_LevelType = 1 
                   AND (cat.t_Code = '+Биржа' or cat.t_Code = '-Биржа')
                   AND acc.t_CatNum = cat.t_Number
                   AND acc.t_IsCommon = 'X'
                   AND acc.t_MarketPlaceID = v_dl_comm.T_TraderID
                   AND (   acc.t_MarketPlaceOfficeID = v_dl_comm.T_PartyOfficeID 
                        or acc.t_MarketPlaceOfficeID IN (SELECT Analytics.t_CentrOfficeID
                                                           FROM DDL_EXTSETTLECODE_DBT ExtCode, DDL_EXTSCANALYTICS_DBT Analytics 
                                                          WHERE ExtCode.T_MarketSchemeID = v_dl_comm.T_MarketSchemeID
                                                            AND ExtCode.T_ParentID = 0 
                                                            AND Analytics.T_CodeID = ExtCode.T_ID
                                                        )
                       ) 
                   AND acc.t_CatID = templ.t_CatID 
                   AND acc.t_TemplNum = templ.t_Number 
                   AND templ.t_Value1 = case when v_dl_comm.t_OperSubKind = RSB_Derivatives.ALG_SP_MONEY_SOURCE_OWN then 1 else 2 end 
              GROUP BY acc.t_Account) acc_market,
               (SELECT acc.t_Account
                  FROM dmccateg_dbt cat, dmcaccdoc_dbt acc, dmctempl_dbt templ  
                 WHERE cat.t_LevelType = 1 
                   AND cat.t_Code = 'Клиринговый счет'
                   AND acc.t_CatNum = cat.t_Number
                   AND acc.t_IsCommon = 'X'
                   AND acc.t_MarketPlaceID = v_dl_comm.T_TraderID
                   AND acc.t_MarketPlaceOfficeID = v_dl_comm.T_PartyOfficeID
                   AND acc.t_CatID = templ.t_CatID 
                   AND acc.t_TemplNum = templ.t_Number 
                   AND templ.t_Value1 = case when v_dl_comm.t_OperSubKind = RSB_Derivatives.ALG_SP_MONEY_SOURCE_OWN then 1 else 2 end 
              GROUP BY acc.t_Account) acc_clearing
         WHERE ltrim(oper.T_DocumentID) = doc.t_ID
           AND oper.T_DocKind = doc.t_DocKind
           AND oprdocs.T_ID_Operation = oper.T_ID_Operation
           AND trans.T_AcctrnID = oprdocs.T_AcctrnID
           AND trans.T_FIID_Payer = RSI_RSB_FIInstr.NATCUR
           AND (    (trans.T_Account_Payer = acc_market.t_Account and trans.T_Account_Receiver = acc_clearing.t_Account)
                 or (trans.T_Account_Payer = acc_clearing.t_Account and trans.T_Account_Receiver = acc_market.t_Account)
               );

        IF v_acctrn.COUNT > 0 THEN                                                
           FORALL indx IN v_acctrn.FIRST .. v_acctrn.LAST                         
              INSERT INTO DDL_ITOGCLIRACCTRN_TMP                                         
                   VALUES v_acctrn (indx);                                        
        END IF;                                                                   
    end if;
    -- проводки шага "Завершение расчетов с биржей по ФИ"
    SELECT trans.T_AcctrnID BULK COLLECT INTO v_acctrn
      FROM doprdocs_dbt oprdocs, dacctrn_dbt trans, doproper_dbt oper
     WHERE ltrim(oper.T_DocumentID) = v_dl_comm.t_DocumentID
       AND oper.T_DocKind = v_dl_comm.t_DocKind
       AND oprdocs.T_ID_Operation = oper.T_ID_Operation
       AND trans.T_AcctrnID = oprdocs.T_AcctrnID
       AND trans.T_FIID_Payer != RSI_RSB_FIInstr.NATCUR
       AND substr(trans.T_Account_Payer,1,5) in('47404','47403','30118') AND substr(trans.T_Account_Receiver,1,5) in('47404','47403','30118'); --Тут только проводки нужной принадлежности с нужным сектором

    IF v_acctrn.COUNT > 0 THEN                                                
       FORALL indx IN v_acctrn.FIRST .. v_acctrn.LAST                         
          INSERT INTO DDL_ITOGCLIRACCTRN_TMP                                         
               VALUES v_acctrn (indx);                                        
    END IF;                                                                   

  END FillItogClirAccTrn;                                                         

  PROCEDURE FillCashFlowsTmpForAC(pSumID IN NUMBER, pDate IN DATE)
  IS
    v_LotView v_scwrthistex%ROWTYPE;
  BEGIN
    DELETE FROM dcashflows_tmp;
    
    SELECT *
      INTO v_LotView
      FROM v_scwrthistex lot
     WHERE     lot.t_SumID    = pSumID
           AND lot.t_Instance = (SELECT MAX(v.t_Instance)
                                   FROM v_scwrthistex v
                                  WHERE     v.t_SumID       = lot.t_SumID
                                        AND v.t_ChangeDate <= pDate);

    IF (v_LotView.t_Date = pDate) THEN
        RSB_SECUR.FillXIRR_AS_EPS(1, v_LotView.t_DealID, v_LotView.t_SumID, pDate, v_LotView.t_FairValue);
    ELSE
        RSB_SECUR.FillXIRR_AS_EPS(1, v_LotView.t_DealID, v_LotView.t_SumID, pDate, v_LotView.t_EffectInterestRate);
    END IF;

    INSERT INTO dcashflows_tmp(t_Date,
                               t_SumDuty,
                               t_SumPerc,
                               t_SumKomiss,
                               t_Sum,
                               t_PaySum,
                               t_RestSum,
                               t_ObjID,
                               t_ObjN)
    SELECT xirr.t_Date,
           xirr.t_SumDuty,
           xirr.t_SumPerc,
           xirr.t_SumKomiss,
           xirr.t_Sum,
           xirr.t_PaySum,
           xirr.t_RestSum,
           xirr.t_ObjID,
           xirr.t_ObjN
      FROM dxirr_tmp xirr;

    DELETE FROM dxirr_tmp;
  END FillCashFlowsTmpForAC;
  
  PROCEDURE FillCashFlowsTmpForEIR(pSumID IN NUMBER, pDate IN DATE)
  IS
    v_LotView v_scwrthistex%ROWTYPE;
  BEGIN
    DELETE FROM dcashflows_tmp;

    SELECT *
      INTO v_LotView
      FROM v_scwrthistex lot
     WHERE     lot.t_SumID    = pSumID
           AND lot.t_Instance = (SELECT MAX(v.t_Instance)
                                   FROM v_scwrthistex v
                                  WHERE     v.t_SumID       = lot.t_SumID
                                        AND v.t_ChangeDate <= pDate);

    RSB_SECUR.FillXIRR_EPS(1, v_LotView.t_DealID, v_LotView.t_SumID, pDate);

    INSERT INTO dcashflows_tmp(t_Date,
                               t_SumDuty,
                               t_SumPerc,
                               t_SumKomiss,
                               t_Sum,
                               t_PaySum,
                               t_RestSum,
                               t_ObjID,
                               t_ObjN)
    SELECT xirr.t_Date,
           xirr.t_SumDuty,
           xirr.t_SumPerc,
           xirr.t_SumKomiss,
           xirr.t_Sum,
           xirr.t_PaySum,
           xirr.t_RestSum,
           xirr.t_ObjID,
           xirr.t_ObjN
      FROM dxirr_tmp xirr;

    DELETE FROM dxirr_tmp;
  END FillCashFlowsTmpForEIR;

  PROCEDURE InsertDLMes(p_DocKind IN INTEGER, p_DocID IN INTEGER, p_Status IN INTEGER, p_Condition IN INTEGER) 
  IS
    v_Status INTEGER := p_Status;
  BEGIN
    IF p_Status <= 0 THEN
      v_Status := p_Condition;
    END IF;
    INSERT INTO ddlmes_dbt VALUES (p_DocKind, p_DocID, v_Status, p_Condition);
    EXCEPTION 
      WHEN DUP_VAL_ON_INDEX THEN
        UPDATE ddlmes_dbt SET T_Status = v_Status, T_Condition = p_Condition
          WHERE T_DocKind = p_DocKind AND T_DocID = p_DocID;
      --WHEN OTHERS THEN NULL;
  END InsertDLMes;

  FUNCTION DL_GetNotResidentForBrokClientRep (p_PartyID IN NUMBER, p_Date IN DATE)
     RETURN CHAR
  AS
     v_NotResident    CHAR;
     v_NRCountry      dparty_dbt.t_NRCountry%TYPE;
     v_LegalForm      NUMBER;
     v_HasResidPerm   NUMBER;
  BEGIN
     SELECT t_LegalForm, t_NotResident, t_NRCountry
       INTO v_LegalForm, v_NotResident, v_NRCountry
       FROM dparty_dbt
      WHERE t_PartyID = p_PartyID;

     IF v_LegalForm = 2
     THEN
      BEGIN
            SELECT /*+ index(hist DPTPRMHIST_DBT_IDX1)*/
               CASE WHEN hist.t_ValueBefore = CHR (1) THEN CHR (0) ELSE CHR (88) END
            INTO v_NotResident
            FROM dptprmhist_dbt hist
            WHERE  hist.t_PartyID = p_PartyID
                AND hist.t_ParamKindID = RSI_RSBPARTY.PARTY_NOTRESIDENT
                AND hist.t_BankDate > p_Date
                AND ROWNUM < 2
            ORDER BY hist.t_BankDate;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN  NULL;
      END;

        IF v_NotResident = CHR (88)
        THEN
           BEGIN
              SELECT /*+ index(hist DPTPRMHIST_DBT_IDX1)*/ 
                 hist.t_ValueBefore
                INTO v_NRCountry
                  FROM dptprmhist_dbt hist
                 WHERE     hist.t_PartyID = p_PartyID
                       AND hist.t_ParamKindID = RSI_RSBPARTY.PARTY_NRCOUNTRY
                       AND hist.t_BankDate > p_Date
                       AND ROWNUM < 2
              ORDER BY hist.t_BankDate;
           EXCEPTION
              WHEN NO_DATA_FOUND THEN NULL;
           END;
           
           IF v_NRCountry = 'RUS'
           THEN
              v_NotResident := CHR (0);
           ELSE
              SELECT CASE
                        WHEN EXISTS
                                (SELECT 1
                                   FROM dpersnidc_dbt
                                  WHERE     t_PersonID = p_PartyID
                                        AND t_PaperKind = 5
                                      AND (   t_ValidToDate = TO_DATE ('01.01.0001', 'DD.MM.YYYY')
                                           OR t_ValidToDate >= p_Date))
                        THEN 1
                        ELSE 0
                     END
                INTO v_HasResidPerm
                FROM DUAL;

              IF v_HasResidPerm = 1
              THEN
                 v_NotResident := CHR (0);
              END IF;
           END IF;
        END IF;
     END IF;

     RETURN v_NotResident;
  END DL_GetNotResidentForBrokClientRep;

  FUNCTION DL_GetPaymStatusOnDate( p_PaymID IN NUMBER, p_Date IN DATE ) RETURN NUMBER
  AS
    v_PaymStatus NUMBER := -1;
  BEGIN
     SELECT *
     INTO v_PaymStatus
     FROM (SELECT hist.t_StatusIDFrom
             FROM dpmhist_dbt hist
            WHERE hist.t_PaymentID = p_PaymID
              AND hist.t_Date > p_Date
         ORDER BY hist.t_Date, hist.t_AutoKey )
     WHERE ROWNUM = 1;

     RETURN v_PaymStatus;

     EXCEPTION
        WHEN NO_DATA_FOUND THEN
           BEGIN
              SELECT t_PaymStatus
              INTO v_PaymStatus
              FROM dpmpaym_dbt
              WHERE t_PaymentID = p_PaymID;

              RETURN v_PaymStatus;
              
              EXCEPTION
                 WHEN NO_DATA_FOUND THEN RETURN -1;
           END;
  END DL_GetPaymStatusOnDate;

  FUNCTION GetOwnTaxRateStr(p_SHID IN NUMBER) RETURN VARCHAR2
  AS
    v_StrRate VARCHAR2(14);
    v_owntax  dscowntax_dbt%rowtype;
  BEGIN
    v_StrRate := '';


    IF p_SHID IS NOT NULL AND p_SHID > 0 THEN
      BEGIN
        SELECT * INTO v_owntax
          FROM dscowntax_dbt
         WHERE t_SHID = p_SHID;

        IF v_owntax.t_TaxRate > 0 THEN
          v_StrRate := TO_CHAR(v_owntax.t_TaxRate)||'%';
          IF v_owntax.t_TaxSum15 > 0 THEN
            v_StrRate := v_StrRate || ', '|| TO_CHAR(15.0)||'%';
          END IF;
        ELSIF v_owntax.t_TaxSum15 > 0 THEN
          v_StrRate := TO_CHAR(15.0)||'%';
        END IF;

        EXCEPTION
             WHEN OTHERS THEN v_StrRate := '';

      END;
    END IF;

    RETURN v_StrRate;

  END GetOwnTaxRateStr;

  FUNCTION NeedDlContrMark(p_DlContrID IN NUMBER, p_OnDate IN DATE) RETURN NUMBER deterministic
  IS
    v_NeedMark NUMBER := 0;
  BEGIN

    SELECT COUNT(1) INTO v_NeedMark
      FROM ddlcontr_dbt dlc, dsfcontr_dbt sf
     WHERE dlc.t_DlCOntrID = p_DlContrID
       AND sf.t_ID = dlc.t_SfContrID
       AND RSB_SECUR.GetMainObjAttr(OBJTYPE_PARTY, LPAD( sf.t_PartyID, 10, '0' ), 26 /*PARTY_ATTR_GROUP_OBSERVEDCLIENT372 Наблюдаемый клиент по 372-ФЗ*/, p_OnDate) = 1 /*Да*/;

    RETURN v_NeedMark;
  END NeedDlContrMark;

function SelectNeedDlContrMark(p_OnDate in date) return tt_DLCONTRID
  pipelined is
  v_res tr_DLCONTRID;
begin
  for cur in (select /*+ result_cache */ distinct dlc.t_dlcontrid
                from (select t_Attrid
                            ,t_Object
                            ,t_ValidFromDate
                            ,max(t_ValidFromDate) over(partition by t_Object) as MaxValidFromDate
                        from dobjatcor_dbt
                       where t_ObjectType = OBJTYPE_PARTY
                         and t_GroupID = 26 /*PARTY_ATTR_GROUP_OBSERVEDCLIENT372 Наблюдаемый клиент по 372-ФЗ*/
                         and t_ValidFromDate <= p_OnDate
                         and t_ValidToDate > p_OnDate) AtCor
                join dsfcontr_dbt sf
                  on sf.t_PartyID = to_number(AtCor.t_Object default null on CONVERSION ERROR)
                join ddlcontr_dbt dlc
                  on sf.t_ID = dlc.t_SfContrID
               where AtCor.t_Attrid = 1
                 and AtCor.t_ValidFromDate = AtCor.MaxValidFromDate)
  loop
    v_res.T_DLCONTRID := cur.T_DLCONTRID;
    pipe row(v_res);
  end loop;
end SelectNeedDlContrMark;



  FUNCTION GetDlContrTaxBase(p_DlContrID IN NUMBER, p_OnDate IN DATE, p_TaxBaseKind IN NUMBER DEFAULT 0) RETURN NUMBER deterministic
  IS
    v_TaxBase NUMBER := 0;
    v_BegDate DATE;
    v_EndDate DATE;
  BEGIN

    v_BegDate := TRUNC(p_OnDate, 'year');
    v_EndDate := p_OnDate;  

    WITH cntr AS (SELECT dlc.t_IIS, sf.t_PartyID
                    FROM ddlcontr_dbt dlc, dsfcontr_dbt sf
                   WHERE dlc.t_DlContrID = p_DlContrID
                     AND sf.t_ID = dlc.t_SfContrID
                 )
    SELECT NVL(SUM(nobj.t_Sum0), 0) INTO v_TaxBase
      FROM dnptxobj_dbt nobj, cntr, dnptxkind_dbt txk
     WHERE nobj.t_Client = cntr.t_PartyID
       AND nobj.t_Date >= v_BegDate
       AND nobj.t_Date <= v_EndDate
       AND nobj.t_Level = 7
       AND 1 = (CASE WHEN cntr.t_IIS = 'X' AND nobj.t_analitickind6 = rsi_nptxc.txobj_kind6020 AND nobj.t_analitic6 in (select mp.t_sfcontrid from ddlcontrmp_dbt mp where mp.t_dlcontrid = p_dlcontrid) 
                                           AND nobj.t_Kind = RSI_NPTXC.TXOBJ_BASEG5 THEN 1 WHEN cntr.t_IIS = CHR(0) AND nobj.t_Kind IN (RSI_NPTXC.TXOBJ_BASEG1, 
                                                                                                                                        RSI_NPTXC.TXOBJ_BASEG2, 
                                                                                                                                        RSI_NPTXC.TXOBJ_BASEG3, 
                                                                                                                                        RSI_NPTXC.TXOBJ_BASEG4, 
                                                                                                                                        RSI_NPTXC.TXOBJ_BASEG6, 
                                                                                                                                        RSI_NPTXC.TXOBJ_BASEG7, 
                                                                                                                                        RSI_NPTXC.TXOBJ_BASEG8, 
                                                                                                                                        RSI_NPTXC.TXOBJ_BASEG9) THEN 1 ELSE 0 END)
       AND txk.t_Element = nobj.t_Kind
       AND (p_TaxBaseKind = 0 OR txk.t_TaxBaseKind = p_TaxBaseKind);

    RETURN v_TaxBase;
  END GetDlContrTaxBase;


  PROCEDURE CalcPayerTaxDivRet(p_PartyID IN NUMBER,
                               p_FIID IN NUMBER,
                               p_CalcDate IN DATE,
                               p_DivDeal IN NUMBER,
                               p_DivCurr IN NUMBER,
                               p_RatePay IN NUMBER,
                               p_D1 IN NUMBER,
                               p_D2 IN NUMBER,
                               p_TaxCurr IN NUMBER,
                               p_Tax OUT NUMBER,
                               p_IncrTax OUT NUMBER
                              )
  AS
    v_LegalForm NUMBER;
    v_NotResident CHAR;

    v_Div_Sec NUMBER;

    v_Rg NUMBER := 0.13;
    v_Rp NUMBER := 0.15;

    v_Znp NUMBER := 0;
  BEGIN

    p_Tax := 0;
    p_IncrTax := 0;

    BEGIN
      SELECT pt.t_LegalForm, pt.t_NotResident INTO v_LegalForm, v_NotResident
        FROM dparty_dbt pt
       WHERE pt.t_PartyID = p_PartyID;

      EXCEPTION
      WHEN NO_DATA_FOUND THEN
        RETURN;
    END;

    IF v_LegalForm = 2 AND v_NotResident <> 'X' AND p_CalcDate >= to_date('01.01.2021', 'dd.mm.yyyy') THEN --Физлицо-резидент
      v_Div_Sec := RSI_RSB_FIInstr.ConvSum(p_DivDeal, p_DivCurr, RSI_RSB_FIInstr.NATCUR, p_CalcDate, 2);

      IF v_Div_Sec <> 0 AND p_D1 <> 0 AND p_D2 <> 0 THEN
        v_Znp := v_Rg * LEAST(v_Div_Sec, v_Div_Sec/p_D1*p_D2);
      END IF;

      IF v_Div_Sec <= RSI_NPTXC.BASE_MAX_15 THEN
        p_Tax     := v_Rg * v_Div_Sec - v_Znp;
        p_IncrTax := 0;
      ELSIF v_Div_Sec > RSI_NPTXC.BASE_MAX_15 AND v_Znp < v_Rg * RSI_NPTXC.BASE_MAX_15 THEN
        p_Tax     := v_Rg * RSI_NPTXC.BASE_MAX_15 - v_Znp;
        p_IncrTax := v_Rp * (v_Div_Sec - RSI_NPTXC.BASE_MAX_15);
      ELSIF v_Div_Sec > RSI_NPTXC.BASE_MAX_15 AND v_Znp > v_Rg * RSI_NPTXC.BASE_MAX_15 THEN
        p_Tax := 0;
        p_IncrTax := v_Rp * (v_Div_Sec - RSI_NPTXC.BASE_MAX_15) - (v_Znp - v_Rg * RSI_NPTXC.BASE_MAX_15);
      END IF;

      p_Tax     := RSI_RSB_FIInstr.ConvSum(p_Tax, RSI_RSB_FIInstr.NATCUR, p_TaxCurr, p_CalcDate, 2);
      p_IncrTax := RSI_RSB_FIInstr.ConvSum(p_IncrTax, RSI_RSB_FIInstr.NATCUR, p_TaxCurr, p_CalcDate, 2); 

    ELSIF v_LegalForm = 1 AND v_NotResident <> 'X' AND p_D1 > 0 THEN --Юрлицо-резидент
       p_Tax := GREATEST(0, p_RatePay * p_DivDeal * (p_D1 - p_D2)/(p_D1*100)); 

       p_Tax := RSI_RSB_FIInstr.ConvSum(p_Tax, p_DivCurr, p_TaxCurr, p_CalcDate, 2);

    ELSE
       p_Tax := p_RatePay * p_DivDeal / 100;

       p_Tax := RSI_RSB_FIInstr.ConvSum(p_Tax, p_DivCurr, p_TaxCurr, p_CalcDate, 2);
    END IF;

    p_Tax     := ROUND(p_Tax, 2);
    p_IncrTax := ROUND(p_IncrTax, 2);

  END CalcPayerTaxDivRet;

  PROCEDURE CalcReceiverTaxDivRet(p_PartyID IN NUMBER,
                                  p_FIID IN NUMBER,
                                  p_CalcDate IN DATE,
                                  p_RetDivDealID IN NUMBER,
                                  p_DivDeal IN NUMBER,
                                  p_DivCurr IN NUMBER,
                                  p_RateGet IN NUMBER,
                                  p_D1 IN NUMBER,
                                  p_D2 IN NUMBER,
                                  p_TaxCurr IN NUMBER,
                                  p_Tax OUT NUMBER,
                                  p_IncrTax OUT NUMBER,
                                  p_ParmStr OUT VARCHAR2
                                 )
  AS

    v_LegalForm NUMBER;
    v_NotResident CHAR;

  BEGIN

    p_Tax := 0;
    p_IncrTax := 0;
    p_ParmStr := CHR(1);

    BEGIN
      SELECT pt.t_LegalForm, pt.t_NotResident INTO v_LegalForm, v_NotResident
        FROM dparty_dbt pt
       WHERE pt.t_PartyID = p_PartyID;

      EXCEPTION
      WHEN NO_DATA_FOUND THEN
        RETURN;
    END;

    IF v_LegalForm = 2 AND v_NotResident <> 'X' AND p_CalcDate >= to_date('01.01.2021', 'dd.mm.yyyy') THEN --Физлицо-резидент

      RSB_DEPO.CalcDivTaxAmount2021(p_DivDeal, 
                                    p_DivCurr, 
                                    p_PartyID, 
                                    p_FIID, 
                                    0,
                                    p_RetDivDealID,
                                    p_CalcDate,
                                    p_D1,
                                    p_D2,
                                    p_TaxCurr,
                                    p_Tax,
                                    p_IncrTax,
                                    p_ParmStr
                                   );
    ELSIF v_LegalForm = 1 AND v_NotResident <> 'X' AND p_D1 > 0 THEN --Юрлицо-резидент
      p_Tax := GREATEST(0, p_RateGet * p_DivDeal * (p_D1 - p_D2)/(p_D1*100)); 

      p_Tax := RSI_RSB_FIInstr.ConvSum(p_Tax, p_DivCurr, p_TaxCurr, p_CalcDate, 2);
    
    ELSE
      p_Tax := p_RateGet * p_DivDeal / 100;

      p_Tax := RSI_RSB_FIInstr.ConvSum(p_Tax, p_DivCurr, p_TaxCurr, p_CalcDate, 2);
    END IF;

    p_Tax     := ROUND(p_Tax, 2);
    p_IncrTax := ROUND(p_IncrTax, 2);

  END CalcReceiverTaxDivRet;

  FUNCTION GetFullNameASTSTable(p_MarketKind IN NUMBER) RETURN VARCHAR2 DETERMINISTIC
  AS
    v_Scheme    VARCHAR2(80) := chr(1);
    v_ShortName VARCHAR2(20) := chr(1);
    v_FullName  VARCHAR2(100):= chr(0);  
    v_IsExecute NUMBER := 0;
  BEGIN
    IF p_MarketKind = DV_MARKETKIND_CURRENCY THEN
       v_Scheme := trim(RSB_COMMON.GetRegStrValue('RG\ASTS\SCRSCHEMA_CURR', 0));
       v_ShortName := 'DASTSCURR_TRADES_DBT';
    ELSE
    v_Scheme := trim(RSB_COMMON.GetRegStrValue('RG\ASTS\SCRSCHEMA', 0));
       v_ShortName := 'DASTS_TRADES_DBT';
    END IF;
    
    SELECT 1 INTO v_IsExecute
      FROM (SELECT * 
              FROM ALL_TABLES 
             WHERE TABLE_NAME = v_ShortName
               AND OWNER = (CASE WHEN v_Scheme <> chr(1) THEN v_Scheme ELSE (SELECT user FROM dual) END))
     WHERE ROWNUM = 1;
       
    IF v_IsExecute = 1 THEN
       IF v_Scheme <> chr(1) THEN 
          v_FullName := v_Scheme || '.';
       END IF;
       v_FullName := v_FullName || v_ShortName;
    END IF;
    
    RETURN v_FullName;

  END GetFullNameASTSTable;

  FUNCTION SC_GetAvrHdCode( p_ObjKind IN NUMBER,
                            p_ObjID   IN NUMBER
                          ) RETURN VARCHAR2
  AS

    v_ObjectType NUMBER := 0;
    v_ObjectID   NUMBER := 0;
    v_CodeKind   NUMBER := 0;

    v_Code dobjcode_dbt.t_code%TYPE := CHR(1);

  BEGIN
    IF p_ObjKind = OBJTYPE_AVOIRISS THEN
      v_ObjectType := OBJTYPE_FININSTR;
      v_ObjectID   := p_ObjID;
      v_CodeKind   := 21; --Код объекта хеджирования
    END IF;

    IF v_ObjectID > 0 THEN
      v_Code := SC_GetObjCodeOnDate(v_ObjectType, v_CodeKind, v_ObjectID);
    END IF;

    RETURN v_Code;
  END;

  --Функция получения кода на заданную дату
  FUNCTION SC_GetDlObjCodeOnDate( p_ObjectType IN NUMBER,
                                  p_CodeKind   IN NUMBER,
                                  p_ObjectID   IN NUMBER,
                                  p_Date       IN DATE DEFAULT NULL
                                ) RETURN VARCHAR2
  AS
    m_Code ddlobjcode_dbt.t_code%TYPE;
  BEGIN
    IF p_Date IS NULL OR p_Date = TO_DATE('01.01.0001', 'DD.MM.YYYY') THEN
      BEGIN
        SELECT t_code INTO m_Code
          FROM ddlobjcode_dbt
         WHERE t_objectid = p_ObjectID
           AND t_objecttype = p_ObjectType
           AND t_codekind = p_CodeKind
           AND t_bankclosedate = TO_DATE('01.01.0001', 'DD.MM.YYYY')
           AND ROWNUM = 1;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN NULL;
      END;
    ELSE
      BEGIN
        SELECT t_code INTO m_Code
          FROM ddlobjcode_dbt
         WHERE t_objectid = p_ObjectID
           AND t_objecttype = p_ObjectType
           AND t_codekind = p_CodeKind
           AND p_Date >= t_bankdate
           AND p_Date < DECODE(t_bankclosedate, TO_DATE('01.01.0001', 'DD.MM.YYYY'), TO_DATE('31.12.9999', 'DD.MM.YYYY'), t_bankclosedate)
           AND ROWNUM = 1;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN NULL;
      END;
    END IF;
    RETURN m_Code;
  END;

  FUNCTION GetAllGrDealIdsByParam (
      p_PlanDate         IN DATE,
      p_FIID             IN NUMBER DEFAULT NULL,
      p_docKindsInClause IN VARCHAR2 DEFAULT NULL)
      RETURN GRDEALID_T
      DETERMINISTIC
  IS
      v_RetVals          GRDEALID_T;
      v_Cursor           SYS_REFCURSOR;

      TYPE grDealRec IS RECORD
      (
          T_DOCID  ddlgrdeal_dbt.T_DOCID%TYPE
      );

      TYPE grDealArr IS TABLE OF grDealRec;

      v_grDealArr        grDealArr;

      v_FIIDStr          VARCHAR2 (2000);
      v_docKindsInClause VARCHAR2 (2000);
  BEGIN
      v_RetVals := GRDEALID_T ();

      IF ((p_FIID IS NOT NULL) AND (p_FIID > 0))
      THEN
          v_FIIDStr :=
              ' AND grdeal.t_fiid = ' || TO_CHAR (p_FIID, '99999999999999');
      END IF;

      IF (p_docKindsInClause IS NOT NULL)
      THEN
          v_docKindsInClause :=
                 ' AND grdeal.t_DocKind IN ( '
              || TO_CHAR (p_docKindsInClause)
              || ' ) ';
      END IF;

      OPEN v_Cursor FOR
             'SELECT /*+ INDEX(grdeal,DDLGRDEAL_DBT_IDX5) */ DISTINCT grdeal.t_docid
                FROM ddlgrdeal_dbt grdeal
               WHERE grdeal.t_PlanDate = :planDate '
          || v_FIIDStr
          || v_docKindsInClause
          USING p_PlanDate;

      LOOP
          FETCH v_Cursor BULK COLLECT INTO v_grDealArr LIMIT 1000;

          FOR indx IN 1 .. v_grDealArr.COUNT
          LOOP
              IF (v_grDealArr (indx).T_DOCID IS NOT NULL)
              THEN
                  v_RetVals.EXTEND;
                  v_RetVals (v_RetVals.LAST) := v_grDealArr (indx).T_DOCID;
              END IF;
          END LOOP;

          EXIT WHEN v_Cursor%NOTFOUND;
      END LOOP;

      CLOSE v_Cursor;

      RETURN v_RetVals;
  END;

  PROCEDURE CheckWorkedDepoLastDate (p_BeginDate         IN DATE,
                                     p_EndDate           IN DATE,
                                     p_dlComId           IN NUMBER,
                                     p_tableName         IN VARCHAR2)
  AS

    v_dlcomm     DDL_COMM_DBT%rowtype;
    v_query VARCHAR2 (2000);
  BEGIN
    SELECT *
      INTO v_dlcomm
      FROM DDL_COMM_DBT
     WHERE T_DOCUMENTID = p_dlComId;

    v_query := 
      ' INSERT INTO '||p_tableName||' SELECT /*+ leading(gracc) cardinality(gracc 100) */ DISTINCT grdeal.t_PlanDate 
          from ddl_tick_dbt tk, ddlgrdeal_dbt grdeal, ddlgracc_dbt gracc 
         where grdeal.t_PlanDate BETWEEN :begDate and :endDate 
           and gracc.t_GrDealID = grdeal.t_ID 
           and gracc.t_AccNum = 4 /*DLGR_ACCKIND_CUSTODY*/
           and gracc.t_State = 1 /*DLGRACC_STATE_PLAN*/  
           and tk.t_DealID = grdeal.t_DocID
           and tk.t_BOfficeKind = grdeal.t_DocKind
           and tk.t_Department = :department
           and tk.t_BOfficeKind IN ('||DL_SECURITYDOC||','||DL_RETIREMENT||','||DL_AVRWRT||','||DL_AVRWRTOWN||','||DL_SECUROWN||')';

    IF((v_dlcomm.t_Deal1 is not null) and (v_dlcomm.t_Deal1 > 0)) then
      v_query := v_query || ' and tk.t_DealID = ' || TO_CHAR(v_dlcomm.t_Deal1);
    END IF;

    IF((v_dlcomm.t_MarketSchemeID is not null) and (v_dlcomm.t_MarketSchemeID > 0)) then
      v_query := v_query || ' and tk.t_DepSetID = ' || TO_CHAR(v_dlcomm.t_MarketSchemeID);
    END IF;

    IF((v_dlcomm.t_ClientID is not null) and (v_dlcomm.t_ClientID > -1)) then
      IF (v_dlcomm.t_ClientID = RSBSESSIONDATA.OurBank) THEN
        v_query := v_query || ' and tk.t_ClientID = -1';
      else
        v_query := v_query || ' and (tk.t_ClientID = '||TO_CHAR(v_dlcomm.t_ClientID)||' or (tk.t_PartyID = '||TO_CHAR(v_dlcomm.t_ClientID)||' and tk.t_IsPartyClient = CHR(88)))';
      END if;
    end if;

    IF((v_dlcomm.t_ContractID is not null) and (v_dlcomm.t_ContractID > -1)) then
      v_query := v_query || ' and (tk.t_ClientContrID = '||TO_CHAR(v_dlcomm.t_ContractID)||' or (tk.t_IsPartyClient = CHR(88) and tk.t_PartyContrID = '||TO_CHAR(v_dlcomm.t_ContractID)||')) ';
    end if;


    EXECUTE IMMEDIATE v_query USING p_BeginDate, p_EndDate, v_dlcomm.t_Division;
  END;

  PROCEDURE CheckWorkedDepoLastDateByYear (p_BeginYear         IN NUMBER,
                                           p_EndYear           IN NUMBER,
                                           p_dlComId           IN NUMBER,
                                           p_tableName         IN VARCHAR2)
  AS
  BEGIN
    CheckWorkedDepoLastDate(TO_DATE('01.01.'||TO_CHAR(p_BeginYear),'DD.MM.YYYY'),TO_DATE('31.12.'||TO_CHAR(p_BeginYear),'DD.MM.YYYY'),p_dlComId, p_tableName);
  END;
  
  PROCEDURE GetPaymentsForHdgDP (  pRealtionID IN NUMBER,
                                   pFIID       IN NUMBER,
                                   DocKind     IN NUMBER,
                                   ObjType     IN NUMBER,
                                   pDate0      IN DATE,
                                   pDate1      IN DATE)
  IS
       v_LotView     v_scwrthistex%ROWTYPE;
       v_xirr        dxirr_tmp%ROWTYPE;
       v_ExistDate   NUMBER;
       CURR          NUMBER:=0;
  BEGIN
       DELETE FROM dcashflows_tmp;

       FOR v_LotView
          IN (SELECT lot.*
                FROM v_scwrthistex lot, dfininstr_dbt fin
               WHERE     lot.t_Amount > 0
                     AND lot.t_DocKind IN (29, 135)
                     AND lot.t_DocID > 0
                     AND lot.t_FIID = pFIID
                     AND lot.t_Instance =
                            (SELECT MAX (bc.t_Instance)
                               FROM v_scwrthistex bc
                              WHERE     bc.t_SumID = lot.t_SumID)
                     AND fin.t_FIID = lot.t_FIID
                     AND fin.t_FI_Kind = 2)
       LOOP
          RSB_SECUR.FillXIRR_EPS (1, v_LotView.t_DealID, v_LotView.t_SumID);

          FOR v_xirr IN (SELECT xirr.t_Date,
                                xirr.t_SumDuty,
                                xirr.t_SumPerc,
                                xirr.t_SumKomiss,
                                xirr.t_Sum,
                                xirr.t_PaySum,
                                xirr.t_RestSum,
                                xirr.t_ObjID,
                                xirr.t_ObjN
                           FROM dxirr_tmp xirr
                          WHERE xirr.t_Date BETWEEN pDate0 AND pDate1)
          LOOP
             SELECT NVL (COUNT (1), 0)
               INTO v_ExistDate
               FROM dcashflows_tmp 
              WHERE t_Date = v_xirr.T_date;

             IF v_ExistDate = 1
             THEN
                UPDATE dcashflows_tmp
                   SET T_SUM = (T_SUM + v_xirr.t_Sum)
                 WHERE T_DATE = v_xirr.T_DATE;
             ELSE
                INSERT INTO dcashflows_tmp (t_Date,
                                            t_SumDuty,
                                            t_SumPerc,
                                            t_SumKomiss,
                                            t_Sum,
                                            t_PaySum,
                                            t_RestSum,
                                            t_ObjID,
                                            t_ObjN)
                     VALUES (v_xirr.t_Date,
                             v_xirr.t_SumDuty,
                             v_xirr.t_SumPerc,
                             v_xirr.t_SumKomiss,
                             v_xirr.t_Sum,
                             v_xirr.t_PaySum,
                             v_xirr.t_RestSum,
                             v_xirr.t_ObjID,
                             v_xirr.t_ObjN);
             END IF;
          END LOOP;

          DELETE FROM dxirr_tmp;
       END LOOP;
       
       DELETE FROM ddv_amort_rhdp_dbt WHERE T_OBJID = pFIID AND T_OBJDOCKIND = DocKind AND T_OBJTYPE = ObjType AND T_RELATIONID = pRealtionID;
       begin
       SELECT finin.t_facevaluefi INTO CURR FROM dfininstr_dbt finin where finin.t_fiid = pFIID;
       exception
        when NO_DATA_FOUND then CURR := 0;
       end;
       
       INSERT INTO ddv_amort_rhdp_dbt (T_OBJID, T_OBJDOCKIND , T_OBJTYPE , T_DATE , T_SUM , T_CURR, T_RELATIONID) SELECT pFIID, DocKind, ObjType, cash.T_DATE, cash.t_SUM, CURR, pRealtionID FROM dcashflows_tmp cash;
  END GetPaymentsForHdgDP;

  --Установить значение категории "Зачисление НДФЛ" для сделки
  PROCEDURE SetDealInTaxAttrID(p_DealID IN NUMBER, p_OnDate IN DATE, p_AttrID IN NUMBER)
  IS
    v_tick DDL_TICK_DBT%ROWTYPE;

    v_ObjType   NUMBER := 0;
    v_DealObjID DOBJATCOR_DBT.T_OBJECT%TYPE;
  BEGIN

    SELECT * INTO v_tick
      FROM ddl_tick_dbt
     WHERE t_DealID = p_DealID;

    v_ObjType   := GetDealObjType(v_tick.t_BOfficeKind);
    v_DealObjID := LPAD(v_tick.t_DealID, 34, '0');

    IF GetMainObjAttrNoDate(GetDealObjType(v_tick.t_BOfficeKind), v_DealObjID, 210 /*Зачисление НДФЛ*/) != 0 THEN
      IF p_AttrID > 0 THEN

        UPDATE DOBJATCOR_DBT
          SET T_ATTRID = p_AttrID
        WHERE T_OBJECTTYPE = v_ObjType
          AND T_OBJECT = v_DealObjID
          AND T_GROUPID = 210;

      ELSE

        --Значения не было, поэтому удаляем
        DELETE DOBJATCOR_DBT
         WHERE T_OBJECTTYPE = v_ObjType
           AND T_OBJECT = v_DealObjID
           AND T_GROUPID = 210;
      END IF;

    ELSIF p_AttrID > 0 THEN

      INSERT INTO DOBJATCOR_DBT ( T_OBJECTTYPE,
                                  T_GROUPID,
                                  T_ATTRID,
                                  T_OBJECT,
                                  T_GENERAL,
                                  T_VALIDFROMDATE,
                                  T_OPER,
                                  T_VALIDTODATE,
                                  T_SYSDATE,
                                  T_SYSTIME,
                                  T_ISAUTO,
                                  T_ID
                                )
                         VALUES ( v_ObjType,                           --T_OBJECTTYPE
                                  210, /*Зачисление НДФЛ*/             --T_GROUPID
                                  p_AttrID,                            --T_ATTRID
                                  v_DealObjID,                         --T_OBJECT
                                  'X',                                 --T_GENERAL
                                  p_OnDate,                            --T_VALIDFROMDATE
                                  RsbSessionData.Oper,                 --T_OPER
                                  TO_DATE('31-12-9999', 'DD-MM-YYYY'), --T_VALIDTODATE
                                  TRUNC(SYSDATE),                      --T_SYSDATE
                                  TO_DATE('01-01-0001:' || TO_CHAR(SYSDATE, 'HH24:MI:SS'), 'DD-MM-YYYY:HH24:MI:SS'),--T_SYSTIME
                                  'X',                                 --T_ISAUTO
                                  0                                    --T_ID
                                );

    END IF;

  END SetDealInTaxAttrID;

  PROCEDURE Mass_ExecDealInTaxAttr
  IS
  BEGIN

     FOR UpdtDeal_rec IN (SELECT Deals.t_DealID, Deals.t_DealDate
                          FROM DV_MKDEAL_MASS_EXEC Deals WHERE INSTR(RSB_SECUR.get_OperSysTypes( Deals.T_KIND_OPERATION, NULL ), 'T') > 0
                          AND Deals.T_BOFFICEKIND = RSB_SECUR.OBJTYPE_AVRWRT
                       ) LOOP
       RSB_SECUR.SetDealInTaxAttrID(UpdtDeal_rec.t_DealID, UpdtDeal_rec.t_DealDate, 1);
     END LOOP;
  END Mass_ExecDealInTaxAttr;


  --╙ёЄрэютшЄ№ чэрўхэшх ърЄхуюЁшш фы  ёфхыъш
  --┼ёыш фрээр  яЁюЎхфєЁр чряєёърхЄё  шч шэЄхуЁрЎшш (Єю хёЄ№ эх шч юъЁєцхэш  RS), 
  --  Єю цхырЄхы№эю шэшЎшрышчшЁютрЄ№ RsbSessionData.SetOper
  --╧рЁрьхЄЁ√: 
  -- p_DealID - ъюф ёфхыъш - Єю хёЄ№ ddl_tick_dbt.t_DealID
  -- p_OnDate - фрЄр эрўрыр фхщёЄтш  ърЄхуюЁшш
  -- p_AttrID - ъюф єёЄрэютыштрхьюую рЄЁшсєЄр (яЁшчэрър). 
  --   ═ряЁшьхЁ "─р" - 1, "═хЄ" - 2. ╧ЁютхЁ Є№ яю ЄрсышЎх DOBJATTR_DBT 
  -- p_GroupId - шфхэЄшЇшърЄюЁ ърЄхуюЁшш
  --╧Ёшьхўрэшх: юсЁрЄэр  get-ЇєэъЎш , ўЄюс√ яюыєўшЄ№ чэрўхэшх рЄЁшсєЄр - GetGeneralMainObjAttr 
  PROCEDURE SetDealAttrID(p_DealID IN NUMBER, p_OnDate IN DATE, 
    p_AttrID IN NUMBER, p_GroupId IN NUMBER )
  IS
    v_tick DDL_TICK_DBT%ROWTYPE;

    v_ObjType   NUMBER := 0;
    v_DealObjID DOBJATCOR_DBT.T_OBJECT%TYPE;
  BEGIN

    SELECT * INTO v_tick
      FROM ddl_tick_dbt
     WHERE t_DealID = p_DealID;

    v_ObjType   := GetDealObjType(v_tick.t_BOfficeKind);
    v_DealObjID := LPAD(v_tick.t_DealID, 34, '0');

    IF GetMainObjAttrNoDate(GetDealObjType(v_tick.t_BOfficeKind), v_DealObjID, p_GroupId ) != 0 THEN
      IF p_AttrID > 0 THEN

        UPDATE DOBJATCOR_DBT
          SET T_ATTRID = p_AttrID
        WHERE T_OBJECTTYPE = v_ObjType
          AND T_OBJECT = v_DealObjID
          AND T_GROUPID = p_GroupId;

      ELSE

        --╟эрўхэш  эх с√ыю, яю¤Єюьє єфры хь
        DELETE DOBJATCOR_DBT
         WHERE T_OBJECTTYPE = v_ObjType
           AND T_OBJECT = v_DealObjID
           AND T_GROUPID = p_GroupId;
      END IF;

    ELSIF p_AttrID > 0 THEN

      INSERT INTO DOBJATCOR_DBT ( T_OBJECTTYPE,
                                  T_GROUPID,
                                  T_ATTRID,
                                  T_OBJECT,
                                  T_GENERAL,
                                  T_VALIDFROMDATE,
                                  T_OPER,
                                  T_VALIDTODATE,
                                  T_SYSDATE,
                                  T_SYSTIME,
                                  T_ISAUTO,
                                  T_ID
                                )
                         VALUES ( v_ObjType,                           --T_OBJECTTYPE
                                  p_GroupId,                           --T_GROUPID
                                  p_AttrID,                            --T_ATTRID
                                  v_DealObjID,                         --T_OBJECT
                                  'X',                                 --T_GENERAL
                                  p_OnDate,                            --T_VALIDFROMDATE
                                  RsbSessionData.Oper,                 --T_OPER
                                  TO_DATE('31-12-9999', 'DD-MM-YYYY'), --T_VALIDTODATE
                                  TRUNC(SYSDATE),                      --T_SYSDATE
                                  TO_DATE('01-01-0001:' || TO_CHAR(SYSDATE, 'HH24:MI:SS'), 'DD-MM-YYYY:HH24:MI:SS'),--T_SYSTIME
                                  'X',                                 --T_ISAUTO
                                  0                                    --T_ID
                                );

    END IF;

  END SetDealAttrID;

  FUNCTION GetSCSrvRepContrNumber (p_scsrvrepID   IN NUMBER,
                                   p_lineLimit    IN NUMBER DEFAULT 100)
     RETURN VARCHAR2
  AS
     m_bufVal   VARCHAR2 (32000);
  BEGIN
    SELECT LISTAGG (repcntr.T_DLCONTRNUMBER, ', ')
              WITHIN GROUP (ORDER BY repcntr.T_DLCONTRNUMBER)
              INTO m_bufVal
      FROM (  SELECT *
                FROM DSCSRVREPCNTR_DBT
               WHERE T_SERVDOCID = p_scsrvrepID
            ORDER BY T_DLCONTRNUMBER
            FETCH FIRST 150 ROWS ONLY) repcntr;
  
     RETURN CASE
               WHEN m_bufVal IS NULL
               THEN
                  'Все'
               WHEN LENGTH (m_bufVal) > p_lineLimit
               THEN
                  SUBSTR (m_bufVal, 1, p_lineLimit - 4) || ' ...'
               ELSE
                  m_bufVal
            END;
  END GetSCSrvRepContrNumber;

  FUNCTION GetSCSrvRepContrNumberTmp (p_scsrvrepID   IN NUMBER,
                                      p_lineLimit    IN NUMBER DEFAULT 100)
     RETURN VARCHAR2
  AS
     m_bufVal   VARCHAR2 (32000);
  BEGIN
    SELECT LISTAGG (repcntr.T_DLCONTRNUMBER, ', ')
              WITHIN GROUP (ORDER BY repcntr.T_DLCONTRNUMBER)
              INTO m_bufVal
      FROM (  SELECT *
                FROM DSCSRVREPCNTR_TMP
               WHERE T_SERVDOCID = p_scsrvrepID
            ORDER BY T_DLCONTRNUMBER
            FETCH FIRST 150 ROWS ONLY) repcntr;
  
     RETURN CASE
               WHEN m_bufVal IS NULL
               THEN
                  'Все'
               WHEN LENGTH (m_bufVal) > p_lineLimit
               THEN
                  SUBSTR (m_bufVal, 1, p_lineLimit - 4) || ' ...'
               ELSE
                  m_bufVal
            END;
  END GetSCSrvRepContrNumberTmp;

  PROCEDURE FillSCSrvRepContrTmp (p_scsrvrepID IN NUMBER)
  IS
  BEGIN
     DELETE FROM DSCSRVREPCNTR_TMP;
  
     INSERT INTO DSCSRVREPCNTR_TMP (T_SERVDOCID,
                                    T_DLCONTRID,
                                    T_DLCONTRNUMBER,
                                    T_CLIENTNAME,
                                    T_CLIENTCODE,
                                    T_EKK,
                                    T_GROUPNUMBER,
                                    T_ROWNUM)
        SELECT T_SERVDOCID,
               T_DLCONTRID,
               T_DLCONTRNUMBER,
               T_CLIENTNAME,
               T_CLIENTCODE,
               T_EKK,
               T_GROUPNUMBER,
               T_ROWNUM
          FROM DSCSRVREPCNTR_DBT
         WHERE T_SERVDOCID = p_scsrvrepID;
  END FillSCSrvRepContrTmp;

  PROCEDURE SyncSCSrvRepContrTmp (p_scsrvrepID IN NUMBER)
  IS
  BEGIN
     MERGE INTO DSCSRVREPCNTR_DBT c
          USING (SELECT DISTINCT p_scsrvrepID AS T_SERVDOCID,
                                 T_DLCONTRID,
                                 T_DLCONTRNUMBER,
                                 T_CLIENTNAME,
                                 T_CLIENTCODE,
                                 T_EKK,
                                 T_GROUPNUMBER,
                                 T_ROWNUM
                   FROM DSCSRVREPCNTR_TMP) d
             ON (c.T_SERVDOCID = d.T_SERVDOCID
                 AND c.T_DLCONTRID = d.T_DLCONTRID)
     WHEN MATCHED
     THEN
        UPDATE SET c.T_DLCONTRNUMBER = d.T_DLCONTRNUMBER,
                   c.T_CLIENTNAME = d.T_CLIENTNAME,
                   c.T_CLIENTCODE = d.T_CLIENTCODE,
                   c.T_EKK = d.T_EKK,
                   c.T_GROUPNUMBER = d.T_GROUPNUMBER,
                   c.T_ROWNUM = T_ROWNUM
     WHEN NOT MATCHED
     THEN
        INSERT     (c.T_SERVDOCID,
                    c.T_DLCONTRID,
                    c.T_DLCONTRNUMBER,
                    c.T_CLIENTNAME,
                    c.T_CLIENTCODE,
                    c.T_EKK,
                    c.T_GROUPNUMBER,
                    c.T_ROWNUM)
            VALUES (p_scsrvrepID,
                    d.T_DLCONTRID,
                    d.T_DLCONTRNUMBER,
                    d.T_CLIENTNAME,
                    d.T_CLIENTCODE,
                    d.T_EKK,
                    d.T_GROUPNUMBER,
                    d.T_ROWNUM);
  
     DELETE FROM DSCSRVREPCNTR_DBT dbt
           WHERE dbt.T_SERVDOCID = p_scsrvrepID
                 AND NOT EXISTS
                        (SELECT 1
                           FROM DSCSRVREPCNTR_TMP tmp
                          WHERE tmp.T_DLCONTRID = dbt.T_DLCONTRID);
  END SyncSCSrvRepContrTmp;
  
-- Возвращает ID шага операции если он имеется в данной операции
-- Ищет шаг Получение средств от платежного агента 115. Не возможно искать через символ, т.к. у шага его нет
  FUNCTION CheckNotExistStepReceivingFunds( DealID IN NUMBER,
                           DocKind IN NUMBER)
    RETURN NUMBER IS
      ID_STEP   NUMBER;
  BEGIN

    SELECT step.t_id_step INTO ID_STEP
             FROM doprstep_dbt step, doproper_dbt oper 
           WHERE oper.t_DocKind      = DocKind AND 
                  oper.t_DocumentID   = LPAD(DealID, 34, '0') AND 
                  step.t_ID_Operation = oper.t_ID_Operation AND 
                  step.t_symbol = '#' AND 
                  step.t_number_step = 115;
                  
     RETURN ID_STEP;
   EXCEPTION 
    WHEN NO_DATA_FOUND THEN RETURN -1;
  END CheckNotExistStepReceivingFunds;

  PROCEDURE SendBrokerContractDepoMessAtChngQInv(p_DlContrID IN NUMBER)
  IS
  BEGIN
    --Отправим сообщения по всем открытым договорам клиента, включая переданный (если он открыт, конечно)
    FOR Contr_rec IN (SELECT DISTINCT dl1.t_DlContrID 
                        FROM ddlcontr_dbt dl, dsfcontr_dbt sf, dsfcontr_dbt sf1, ddlcontr_dbt dl1
                       WHERE dl.t_DlContrID = p_DlContrID
                         AND sf.t_ID = dl.t_SfContrID
                         AND sf1.t_PartyID = sf.t_PartyID 
                         AND sf1.t_DateClose = to_date('01.01.0001','dd.mm.yyyy')
                         AND dl1.t_SfContrID = sf1.t_ID
                     ) LOOP
      IT_DIASOFT.SendBrokerContractDepo(Contr_rec.t_DlContrID);
    END LOOP;
  END SendBrokerContractDepoMessAtChngQInv;

  --Проверить, что субъект-эмитент зарегистрирован в государстве - члене ЕАЭС на дату
  FUNCTION IsIssuerEAEU(p_PartyID IN NUMBER, p_OnDate IN DATE) RETURN NUMBER
  IS
    v_AttrID dobjattr_dbt.t_AttrID % TYPE;
  BEGIN

    v_AttrID := GetMainObjAttr(RSB_SECUR.OBJTYPE_PARTY, LPAD(p_PartyID, 10, '0' ), 112 /*Зарегистрирован в государстве - члене ЕАЭС*/, p_OnDate);

    IF v_AttrID = 1 THEN
      RETURN 1;
    END IF;

    RETURN 0;
  END;
  
   FUNCTION GetIncRateOnDate(p_DealId IN NUMBER, p_DocKind IN NUMBER, p_OnDate DATE) RETURN NUMBER
  IS
    v_IncomeRate NUMBER := 0;
  BEGIN

      SELECT incr.T_INCOMERATE INTO v_IncomeRate 
        FROM DDL_INCRATE_HST_DBT incr
       WHERE incr.T_DEALID = p_DealId 
         AND incr.T_DOCKIND = p_DocKind 
         AND incr.T_FROMDATE = (SELECT MAX(tmp.T_FROMDATE) 
                                  FROM DDL_INCRATE_HST_DBT tmp 
                                 WHERE tmp.T_DEALID = p_DealId 
                                   AND tmp.T_DOCKIND = p_DocKind 
                                   AND tmp.T_FROMDATE <= p_OnDate);
    RETURN v_IncomeRate;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN RETURN 0;
  END GetIncRateOnDate;

END Rsb_Secur;
/
