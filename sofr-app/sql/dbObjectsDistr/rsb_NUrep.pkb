CREATE OR REPLACE PACKAGE BODY rsb_NUrep
IS
  UnknownDate CONSTANT DATE := TO_DATE('01.01.0001', 'DD.MM.YYYY');

  LLCLASS_KINDCB_PORTF CONSTANT NUMBER := 100;    --Вид ц/б в портфеле
  LLCLASS_KINDPORT     CONSTANT NUMBER := 474;    --Вид портфеля
  LLCLASS_IS_AVOIR_BPP CONSTANT NUMBER := 1653;   --Признак передачи без прекращения признания
  LLCLASS_KIND_ACC_PDD CONSTANT NUMBER := 1654;   --Назначения счетов ПДД

  UPDATE_INTERVAL  CONSTANT NUMBER := 0.03; -- 3 сек.
  m_ReqId          VARCHAR2(255);
  m_MaxValue       NUMBER;
  m_CurValue       NUMBER;
  m_Text           VARCHAR2(255);
  m_lastUpdateTime NUMBER;

  TYPE nunprep_t IS TABLE OF DNUNPREP_TMP%ROWTYPE;
  TYPE nusvodrep_t IS TABLE OF DNUSVODREP_DBT%ROWTYPE;
  TYPE SVODRepList_t IS TABLE OF nusvodrep_t INDEX BY PLS_INTEGER;

  TYPE AccRec_t IS RECORD ( t_Account VARCHAR2(25),
                            t_Currency NUMBER(10),
                            t_Chapter NUMBER(5)
                          );
  TYPE ListAcc_t IS TABLE OF AccRec_t;

  TYPE TrnSumAcc_t IS RECORD ( t_Account VARCHAR2(25),
                               t_Sum NUMBER(32,12)
                             );
  TYPE ListTrnSumAcc_t IS TABLE OF TrnSumAcc_t;

  TYPE ListAccCheck_t IS TABLE OF VARCHAR2(25) INDEX BY VARCHAR2(25);

  GlobalSVODRepList SVODRepList_t;

  --настройки
  BPP_ACCOUNT_METHOD NUMBER;
  V13 CHAR(1);

  IsReadedSettings BOOLEAN := false;

  -- Константы признаков pPrm для ф-и GetTrnSumAcc_SVOD
  --101          0001 1
  --101+repo     0011 3
  --101+repo+ens 0111 7
  --101+4815     1001 9
  SVODTRNSUMACC_101          CONSTANT NUMBER := 1;
  SVODTRNSUMACC_101_REPO     CONSTANT NUMBER := 3;
  SVODTRNSUMACC_101_REPO_ENS CONSTANT NUMBER := 7;
  SVODTRNSUMACC_101_4815     CONSTANT NUMBER := 9;

  PROCEDURE AddNURepError(pSessionID IN NUMBER, pMessage IN VARCHAR2)
  IS
  BEGIN
    INSERT INTO DNUREPERR_DBT VALUES (pSessionID, pMessage);
  END;

  ---------------------------------------------------------------------------------------------------
  -- Для стандартного (из RsFloatingWindow) индикатора прогресса АРНУ (веб)
  PROCEDURE WebInsertProcessState
  IS
  BEGIN
    INSERT INTO DWEB_PROCSTATE_DBT (T_ID, T_OPER, T_VALUE, T_MAXVALUE, T_TEXT)
                            VALUES (m_ReqId,
                                    RsbSessionData.oper(),
                                    m_CurValue,
                                    m_MaxValue,
                                    m_Text
                                   );
  END;

  PROCEDURE WebUpdateProcessState
  IS
    v_sql VARCHAR2(650);
  BEGIN
    v_sql := 'UPDATE DWEB_PROCSTATE_DBT SET T_VALUE = ' || TO_CHAR(m_CurValue);

    if( m_MaxValue != 0 ) then
      v_sql := v_sql || ', T_MAXVALUE = ' || TO_CHAR(m_MaxValue);
    end if;
    if( NVL(LENGTH(m_Text),0) != 0 ) then
      v_sql := v_sql || ', T_TEXT = ''' || m_Text || '''';
    end if;

    v_sql := v_sql || ' WHERE T_ID = ''' || m_ReqId || '''';

    EXECUTE IMMEDIATE (v_sql);

    COMMIT;
  END;

  FUNCTION WebFindProcessState(pState OUT DWEB_PROCSTATE_DBT%rowtype)
    RETURN NUMBER
  IS
  BEGIN
    SELECT * INTO pState
      FROM DWEB_PROCSTATE_DBT WHERE T_ID = m_reqId;
    RETURN 1;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN RETURN 0;
  END;

  PROCEDURE WebUpdateProcessStateByReqId
  IS
    v_state DWEB_PROCSTATE_DBT%rowtype;
  BEGIN
    if( NVL(LENGTH(m_ReqId),0) = 0 ) then
      DBMS_OUTPUT.PUT_LINE('Ошибка PL/SQL WebProgressIndicator: Не задан reqId');
      RETURN;
    end if;

    if( m_CurValue = 0 and m_MaxValue = 0 and NVL(LENGTH(m_Text),0) = 0 ) then
      DBMS_OUTPUT.PUT_LINE('Ошибка PL/SQL WebProgressIndicator: Не задан ни один обновляемый параметр');
      RETURN;
    end if;

    IF WebFindProcessState(v_state) = 1 THEN
      if( (m_CurValue != v_state.t_Value) or
          (m_MaxValue != 0 and m_MaxValue != v_state.t_MaxValue) or
          (NVL(LENGTH(m_Text),0) != 0 and m_Text != v_state.t_Text)
      ) then
        WebUpdateProcessState;
      end if;
    ELSE
      WebInsertProcessState;
    END IF;

  END;

  -- !!! Учитывать, что данная процедура использует COMMIT !!!
  PROCEDURE WebProgressIndicator_Update(curValue IN NUMBER, maxValue IN NUMBER DEFAULT 0, text IN VARCHAR2 DEFAULT '')
  IS
  BEGIN
    if( curValue >= 0 ) then
      m_CurValue := curValue;
    end if;

    if( maxValue > 0 ) then
      m_MaxValue := maxValue;
    end if;

    if( NVL(LENGTH(text),0) != 0 ) then
      m_Text := text;
    end if;

    if( m_lastUpdateTime = 0 or (dbms_utility.get_time - m_lastUpdateTime) > UPDATE_INTERVAL ) then
      m_lastUpdateTime := dbms_utility.get_time;
      WebUpdateProcessStateByReqId();
    end if;
  END;

  -- !!! Учитывать, что данная процедура использует COMMIT !!!
  PROCEDURE WebProgressIndicator_Start(reqId IN VARCHAR2, maxValue IN NUMBER, text IN VARCHAR2)
  IS
  BEGIN
    m_ReqId := ReqId;
    m_MaxValue := 0;
    m_CurValue := 0;
    m_Text := text;
    m_lastUpdateTime := 0;

    if( maxValue >= 0 ) then
      m_MaxValue := maxValue;
    end if;

    WebProgressIndicator_Update(m_CurValue, m_MaxValue, m_Text);
  END;

  -- !!! Учитывать, что данная процедура использует COMMIT !!!
  PROCEDURE WebProgressIndicator_Stop
  IS
  BEGIN
    m_lastUpdateTime := 0;
    WebProgressIndicator_Update(m_MaxValue);
  END;

  -- Увеличить прогресс индикатора
  PROCEDURE WebProcessState_IncreaseByReqID(reqId IN VARCHAR2)
  IS
    v_sql VARCHAR2(650);
  BEGIN
    v_sql := 'UPDATE DWEB_PROCSTATE_DBT SET T_VALUE = T_VALUE + 1 WHERE T_ID = ''' || reqId || '''';
    EXECUTE IMMEDIATE (v_sql);
    COMMIT;
  END;
  ---------------------------------------------------------------------------------------------------

  FUNCTION GetRestAcc(pAccount IN VARCHAR2, pCurrency IN NUMBER, pChapter IN NUMBER, pDate IN DATE)
    RETURN NUMBER
  IS
  BEGIN
    RETURN NVL(ABS(rsb_account.restac(pAccount, pCurrency, pDate, pChapter, null)), 0);
  END;

  --Получить для ФИ по КУ остатки по счетам (строка промежуточных итогов)
  FUNCTION GetRestAccByCode(pFIID IN NUMBER, pCode IN VARCHAR2, pDate IN DATE, pToFIID IN NUMBER)
    RETURN NUMBER
  IS
    v_Sum NUMBER := 0;
  BEGIN
    SELECT NVL(SUM( RSI_RSB_FIInstr.ConvSum( rsb_account.restac(acc.t_Account, acc.t_Currency, pDate, acc.t_Chapter, null),
                                             acc.t_Currency, pToFIID, pDate )
                  ), 0)
          INTO v_Sum
      FROM ( SELECT accdoc.t_Account, accdoc.t_Currency, accdoc.t_Chapter
               FROM DMCACCDOC_DBT accdoc, DMCCATEG_DBT categ
              WHERE categ.t_ID = accdoc.t_catID
                AND categ.t_Code = pCode
                AND CATEG.T_LEVELTYPE = 1
                AND accdoc.t_FIID = pFIID
              GROUP BY accdoc.t_Account, accdoc.t_Currency, accdoc.t_Chapter
           ) acc ;

    RETURN v_Sum;
  END;

  FUNCTION Where_AccEqByParm( LClass IN INTEGER, LValue IN INTEGER ) RETURN VARCHAR2
  AS
  BEGIN
    RETURN 'DECODE( '||LCLASS||','||
                                'cat.t_Class1, tpl.t_Value1,
                                 cat.t_Class2, tpl.t_Value2,
                                 cat.t_Class3, tpl.t_Value3,
                                 cat.t_Class4, tpl.t_Value4,
                                 cat.t_Class5, tpl.t_Value5,
                                 cat.t_Class6, tpl.t_Value6,
                                 cat.t_Class7, tpl.t_Value7,
                                 cat.t_Class8, tpl.t_Value8,
                                 -1
                              ) = '||LValue;
  END;

  FUNCTION Where_AccNoEqByParm( LClass IN INTEGER, LValue IN INTEGER ) RETURN VARCHAR2
  AS
  BEGIN
    RETURN 'DECODE( '||LCLASS||','||
                                'cat.t_Class1, tpl.t_Value1,
                                 cat.t_Class2, tpl.t_Value2,
                                 cat.t_Class3, tpl.t_Value3,
                                 cat.t_Class4, tpl.t_Value4,
                                 cat.t_Class5, tpl.t_Value5,
                                 cat.t_Class6, tpl.t_Value6,
                                 cat.t_Class7, tpl.t_Value7,
                                 cat.t_Class8, tpl.t_Value8,
                                 -1
                              ) != '||LValue;
  END;

  --Получить счет, параметризованный по ц/б
  PROCEDURE GetAccountByParm( pListAcc IN OUT NOCOPY ListAcc_t,
                              pFIID IN NUMBER,
                              pCode IN VARCHAR2,
                              pWhereQuery IN VARCHAR2 default '' )
  IS
  BEGIN
    EXECUTE IMMEDIATE ( ' SELECT acd.t_Account, acd.t_Currency, acd.t_Chapter ' ||
                          ' FROM dmcaccdoc_dbt acd, DMCCATEG_DBT cat, dmctempl_dbt tpl ' ||
                         ' WHERE acd.t_FIID = '||pFIID||
                           ' AND acd.t_Chapter = 1 ' ||
                           ' AND acd.t_CatID = cat.t_ID ' ||
                           ' AND cat.T_CODE IN ('||pCode||') ' ||
                           ' AND CAT.T_LEVELTYPE = 1 ' ||
                           ' AND tpl.t_CatID = cat.t_ID ' ||
                           ' AND tpl.t_Number = acd.t_TemplNum '||pWhereQuery||
                         ' GROUP BY acd.t_Account, acd.t_Currency, acd.t_Chapter '
                      )
       BULK COLLECT INTO pListAcc;
  EXCEPTION
    WHEN OTHERS THEN NULL;
  END;

  --Получить счет, параметризованный по сделке
  PROCEDURE GetAccountByParmDeal( pListAcc IN OUT NOCOPY ListAcc_t,
                                  pDealID IN NUMBER,
                                  pCode IN VARCHAR2,
                                  pWhereQuery IN VARCHAR2 default '' )
  IS
  BEGIN
    EXECUTE IMMEDIATE ( ' SELECT acd.t_Account, acd.t_Currency, acd.t_Chapter ' ||
                          ' FROM dmcaccdoc_dbt acd, DMCCATEG_DBT cat, dmctempl_dbt tpl ' ||
                         ' WHERE acd.T_DocKind = '||rsb_secur.DL_SECURITYDOC||
                           ' AND acd.T_DocID = '||pDealID||
                           ' AND acd.t_Chapter = 1 ' ||
                           ' AND acd.t_CatID = cat.t_ID ' ||
                           ' AND cat.T_CODE IN ('||pCode||') ' ||
                           ' AND CAT.T_LEVELTYPE = 1 ' ||
                           ' AND tpl.t_CatID = cat.t_ID ' ||
                           ' AND tpl.t_Number = acd.t_TemplNum '||pWhereQuery||
                         ' GROUP BY acd.t_Account, acd.t_Currency, acd.t_Chapter '
                      )
       BULK COLLECT INTO pListAcc;
  EXCEPTION
    WHEN OTHERS THEN NULL;
  END;

  --Получить посделочные счета ПРЕПО
  PROCEDURE GetPREPOAccountByParm( pListAcc IN OUT NOCOPY ListAcc_t,
                                   pFIID IN NUMBER,
                                   pCode IN VARCHAR2,
                                   pWhereQuery IN VARCHAR2 default '' )
  IS
  BEGIN
    EXECUTE IMMEDIATE ( ' SELECT acd.t_Account, acd.t_Currency, acd.t_Chapter ' ||
                          ' FROM dmcaccdoc_dbt acd, DMCCATEG_DBT cat, dmctempl_dbt tpl, ddl_leg_dbt leg, ddl_tick_dbt tick ' ||
                         ' WHERE acd.T_DocKind = '||rsb_secur.DL_SECURLEG||
                           ' AND acd.T_DocID = leg.t_ID '||
                           ' AND tick.t_DealID = leg.t_DealID '||
                           ' AND tick.t_PFI = '||pFIID||
                           ' AND rsb_secur.IsRepo(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(tick.t_DealType, tick.t_BofficeKind))) = 1 '||
                           ' AND rsb_secur.IsSale(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(tick.t_DealType, tick.t_BofficeKind))) = 1 '||
                           ' AND rsb_secur.IsBasket(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(tick.t_DealType, tick.t_BofficeKind))) = 0 '||
                           ' AND acd.t_Chapter = 1 ' ||
                           ' AND acd.t_CatID = cat.t_ID ' ||
                           ' AND cat.T_CODE IN ('||pCode||') ' ||
                           ' AND CAT.T_LEVELTYPE = 1 ' ||
                           ' AND tpl.t_CatID = cat.t_ID ' ||
                           ' AND tpl.t_Number = acd.t_TemplNum '||pWhereQuery||
                         ' GROUP BY acd.t_Account, acd.t_Currency, acd.t_Chapter '
                      )
       BULK COLLECT INTO pListAcc;
  EXCEPTION
    WHEN OTHERS THEN NULL;
  END;

  --Заполнить поля, связанные с остатками по счетам (для промежуточных строк)
  PROCEDURE SetFldsRestAcc(pNUNPRec IN OUT NOCOPY DNUNPREP_TMP%rowtype, pDate IN DATE)
  IS
    v_ListAcc ListAcc_t;
    v_rest NUMBER := 0;
  BEGIN

    -- получим данные, которые ранее получились посделочно: Б1(Б6), Б2(Б7)
    SELECT NVL(SUM(t_BalSumVN_DH), 0), NVL(SUM(t_BalSumRub_DH), 0), NVL(SUM(t_BalNkdBVN_DH), 0), NVL(SUM(t_BalNkdBrub_DH), 0)
      INTO pNUNPRec.t_BalSumVN_DH, pNUNPRec.t_BalSumRub_DH, pNUNPRec.t_BalNkdBVN_DH, pNUNPRec.t_BalNkdBrub_DH
      FROM DNUNPREP_TMP
     WHERE t_FIID         = pNUNPRec.t_FIID
       AND t_PortfID      = pNUNPRec.t_PortfID
       AND t_IsItogPortf  = chr(0)
       AND t_IsItog       = chr(0);

    --Б1, Б6
    GetAccountByParm( v_ListAcc, pNUNPRec.t_FIID,
                      '''Наш портфель ц/б'', ''Ц/б, Корзина БПП''',
                      'AND '||Where_AccEqByParm(LLCLASS_KINDPORT, pNUNPRec.t_PortfID) );

    IF v_ListAcc.COUNT > 0 THEN
      FOR i IN v_ListAcc.FIRST .. v_ListAcc.LAST
      LOOP
        v_rest := GetRestAcc(v_ListAcc(i).t_Account, v_ListAcc(i).t_Currency, v_ListAcc(i).t_Chapter, pDate);

        IF v_rest != 0 THEN
          pNUNPRec.t_BalSumVN_DH := pNUNPRec.t_BalSumVN_DH + NVL(v_rest, 0);
          pNUNPRec.t_BalSumRub_DH := pNUNPRec.t_BalSumRub_DH + NVL(RSI_RSB_FIInstr.ConvSum(v_rest, v_ListAcc(i).t_Currency, RSI_RSB_FIInstr.NATCUR, pDate), 0);
        END IF;
      END LOOP;
      v_ListAcc.delete;
    END IF;

    GetPREPOAccountByParm( v_ListAcc, pNUNPRec.t_FIID,
                           '''Ц/б, БПП''',
                           'AND '||Where_AccEqByParm(LLCLASS_KINDPORT, pNUNPRec.t_PortfID) );

    IF v_ListAcc.COUNT > 0 THEN
      FOR i IN v_ListAcc.FIRST .. v_ListAcc.LAST
      LOOP
        v_rest := GetRestAcc(v_ListAcc(i).t_Account, v_ListAcc(i).t_Currency, v_ListAcc(i).t_Chapter, pDate);

        IF v_rest != 0 THEN
          pNUNPRec.t_BalSumVN_DH := pNUNPRec.t_BalSumVN_DH + NVL(v_rest, 0);
          pNUNPRec.t_BalSumRub_DH := pNUNPRec.t_BalSumRub_DH + NVL(RSI_RSB_FIInstr.ConvSum(v_rest, v_ListAcc(i).t_Currency, RSI_RSB_FIInstr.NATCUR, pDate), 0);
        END IF;
      END LOOP;
      v_ListAcc.delete;
    END IF;

    -- для КУ 'Наш портфель ПКУ, ц/б' нет параметра 'ВидПортф'
    GetAccountByParm( v_ListAcc, pNUNPRec.t_FIID,
                      '''Наш портфель ПКУ, ц/б''' );

    IF v_ListAcc.COUNT > 0 THEN
      FOR i IN v_ListAcc.FIRST .. v_ListAcc.LAST
      LOOP
        v_rest := GetRestAcc(v_ListAcc(i).t_Account, v_ListAcc(i).t_Currency, v_ListAcc(i).t_Chapter, pDate);

        IF v_rest != 0 THEN
          pNUNPRec.t_BalSumVN_DH := pNUNPRec.t_BalSumVN_DH + NVL(v_rest, 0);
          pNUNPRec.t_BalSumRub_DH := pNUNPRec.t_BalSumRub_DH + NVL(RSI_RSB_FIInstr.ConvSum(v_rest, v_ListAcc(i).t_Currency, RSI_RSB_FIInstr.NATCUR, pDate), 0);
        END IF;
      END LOOP;
      v_ListAcc.delete;
    END IF;

    --Б2, Б7
    GetAccountByParm( v_ListAcc, pNUNPRec.t_FIID,
                      '''Уплаченный НКД'', ''Уплач. НКД, Корзина БПП''',
                      'AND '||Where_AccEqByParm(LLCLASS_KINDPORT, pNUNPRec.t_PortfID) );

    IF v_ListAcc.COUNT > 0 THEN
      FOR i IN v_ListAcc.FIRST .. v_ListAcc.LAST
      LOOP
        v_rest := GetRestAcc(v_ListAcc(i).t_Account, v_ListAcc(i).t_Currency, v_ListAcc(i).t_Chapter, pDate);

        IF v_rest != 0 THEN
          pNUNPRec.t_BalNkdBVN_DH := pNUNPRec.t_BalNkdBVN_DH + NVL(v_rest, 0);
          pNUNPRec.t_BalNkdBrub_DH := pNUNPRec.t_BalNkdBrub_DH + NVL(RSI_RSB_FIInstr.ConvSum(v_rest, v_ListAcc(i).t_Currency, RSI_RSB_FIInstr.NATCUR, pDate), 0);
        END IF;
      END LOOP;
      v_ListAcc.delete;
    END IF;

    GetPREPOAccountByParm( v_ListAcc, pNUNPRec.t_FIID,
                           '''Уплаченный НКД, БПП''',
                           'AND '||Where_AccEqByParm(LLCLASS_KINDPORT, pNUNPRec.t_PortfID) );

    IF v_ListAcc.COUNT > 0 THEN
      FOR i IN v_ListAcc.FIRST .. v_ListAcc.LAST
      LOOP
        v_rest := GetRestAcc(v_ListAcc(i).t_Account, v_ListAcc(i).t_Currency, v_ListAcc(i).t_Chapter, pDate);

        IF v_rest != 0 THEN
          pNUNPRec.t_BalNkdBVN_DH := pNUNPRec.t_BalNkdBVN_DH + NVL(v_rest, 0);
          pNUNPRec.t_BalNkdBrub_DH := pNUNPRec.t_BalNkdBrub_DH + NVL(RSI_RSB_FIInstr.ConvSum(v_rest, v_ListAcc(i).t_Currency, RSI_RSB_FIInstr.NATCUR, pDate), 0);
        END IF;
      END LOOP;
      v_ListAcc.delete;
    END IF;

    --Б3, Б8
    pNUNPRec.t_B3 := 0;
    pNUNPRec.t_B8 := 0;

    GetAccountByParm( v_ListAcc, pNUNPRec.t_FIID,
                      '''Начисл.ПДД, ц/б''',
                      'AND '||Where_AccEqByParm(LLCLASS_KINDPORT, pNUNPRec.t_PortfID) ||
                    --'AND '||Where_AccEqByParm(LLCLASS_IS_AVOIR_BPP, 1) ||
                      'AND '||Where_AccEqByParm(LLCLASS_KIND_ACC_PDD, 1) );

    IF v_ListAcc.COUNT > 0 THEN
      FOR i IN v_ListAcc.FIRST .. v_ListAcc.LAST
      LOOP
        v_rest := GetRestAcc(v_ListAcc(i).t_Account, v_ListAcc(i).t_Currency, v_ListAcc(i).t_Chapter, pDate);

        IF v_rest != 0 THEN
          pNUNPRec.t_B3 := pNUNPRec.t_B3 + NVL(v_rest, 0);
          pNUNPRec.t_B8 := pNUNPRec.t_B8 + NVL(RSI_RSB_FIInstr.ConvSum(v_rest, v_ListAcc(i).t_Currency, RSI_RSB_FIInstr.NATCUR, pDate), 0);
        END IF;
      END LOOP;
      v_ListAcc.delete;
    END IF;

    --Б4, Б9
    pNUNPRec.t_B4 := 0;
    pNUNPRec.t_B9 := 0;

    GetAccountByParm( v_ListAcc, pNUNPRec.t_FIID,
                      '''Начисл.ПДД, ц/б''',
                      'AND '||Where_AccEqByParm(LLCLASS_KINDPORT, pNUNPRec.t_PortfID) ||
                    --'AND '||Where_AccEqByParm(LLCLASS_IS_AVOIR_BPP, 1) ||
                      'AND '||Where_AccEqByParm(LLCLASS_KIND_ACC_PDD, 2) );

    IF v_ListAcc.COUNT > 0 THEN
      FOR i IN v_ListAcc.FIRST .. v_ListAcc.LAST
      LOOP
        v_rest := GetRestAcc(v_ListAcc(i).t_Account, v_ListAcc(i).t_Currency, v_ListAcc(i).t_Chapter, pDate);

        IF v_rest != 0 THEN
          pNUNPRec.t_B4 := pNUNPRec.t_B4 + NVL(v_rest, 0);
          pNUNPRec.t_B9 := pNUNPRec.t_B9 + NVL(RSI_RSB_FIInstr.ConvSum(v_rest, v_ListAcc(i).t_Currency, RSI_RSB_FIInstr.NATCUR, pDate), 0);
        END IF;
      END LOOP;
      v_ListAcc.delete;
    END IF;

    --Б5, Б10
    pNUNPRec.t_B5 := 0;
    pNUNPRec.t_B10 := 0;

    GetAccountByParm( v_ListAcc, pNUNPRec.t_FIID,
                      '''Премия, ц/б''',
                    --'AND '||Where_AccEqByParm(LLCLASS_IS_AVOIR_BPP, 1) ||
                      'AND '||Where_AccEqByParm(LLCLASS_KINDPORT, pNUNPRec.t_PortfID) );

    IF v_ListAcc.COUNT > 0 THEN
      FOR i IN v_ListAcc.FIRST .. v_ListAcc.LAST
      LOOP
        v_rest := GetRestAcc(v_ListAcc(i).t_Account, v_ListAcc(i).t_Currency, v_ListAcc(i).t_Chapter, pDate);

        IF v_rest != 0 THEN
          pNUNPRec.t_B5 := pNUNPRec.t_B5 + NVL(v_rest, 0);
          pNUNPRec.t_B10 := pNUNPRec.t_B10 + NVL(RSI_RSB_FIInstr.ConvSum(v_rest, v_ListAcc(i).t_Currency, RSI_RSB_FIInstr.NATCUR, pDate), 0);
        END IF;
      END LOOP;
      v_ListAcc.delete;
    END IF;

    --Б11
    pNUNPRec.t_B11 := 0;

    GetAccountByParm( v_ListAcc, pNUNPRec.t_FIID,
                      '''-Корректировка, ц/б''',
                      'AND '||Where_AccEqByParm(LLCLASS_KINDPORT, pNUNPRec.t_PortfID) ||
                      'AND '||Where_AccEqByParm(LLCLASS_KINDCB_PORTF, 102) );

    IF v_ListAcc.COUNT > 0 THEN
      FOR i IN v_ListAcc.FIRST .. v_ListAcc.LAST
      LOOP
        v_rest := GetRestAcc(v_ListAcc(i).t_Account, v_ListAcc(i).t_Currency, v_ListAcc(i).t_Chapter, pDate);

        IF v_rest != 0 THEN
          pNUNPRec.t_B11 := pNUNPRec.t_B11 + NVL(RSI_RSB_FIInstr.ConvSum(v_rest, v_ListAcc(i).t_Currency, RSI_RSB_FIInstr.NATCUR, pDate), 0);
        END IF;
      END LOOP;
      v_ListAcc.delete;
    END IF;

    --Б12
    pNUNPRec.t_B12 := 0;

    GetAccountByParm( v_ListAcc, pNUNPRec.t_FIID,
                      '''+Корректировка, ц/б''',
                      'AND '||Where_AccEqByParm(LLCLASS_KINDPORT, pNUNPRec.t_PortfID) ||
                      'AND '||Where_AccEqByParm(LLCLASS_KINDCB_PORTF, 102) );

    IF v_ListAcc.COUNT > 0 THEN
      FOR i IN v_ListAcc.FIRST .. v_ListAcc.LAST
      LOOP
        v_rest := GetRestAcc(v_ListAcc(i).t_Account, v_ListAcc(i).t_Currency, v_ListAcc(i).t_Chapter, pDate);

        IF v_rest != 0 THEN
          pNUNPRec.t_B12 := pNUNPRec.t_B12 + NVL(RSI_RSB_FIInstr.ConvSum(v_rest, v_ListAcc(i).t_Currency, RSI_RSB_FIInstr.NATCUR, pDate), 0);
        END IF;
      END LOOP;
      v_ListAcc.delete;
    END IF;

    --Б13
    pNUNPRec.t_B13 := 0;

    GetAccountByParm( v_ListAcc, pNUNPRec.t_FIID,
                      '''+Переоценка, ц/б ССПУ_ЦБ'', ''+Переоценка, ц/б СССД_ЦБ''',
                      'AND '||Where_AccEqByParm(LLCLASS_KINDPORT, pNUNPRec.t_PortfID) );

    IF v_ListAcc.COUNT > 0 THEN
      FOR i IN v_ListAcc.FIRST .. v_ListAcc.LAST
      LOOP
        v_rest := GetRestAcc(v_ListAcc(i).t_Account, v_ListAcc(i).t_Currency, v_ListAcc(i).t_Chapter, pDate);

        IF v_rest != 0 THEN
          pNUNPRec.t_B13 := pNUNPRec.t_B13 + NVL(RSI_RSB_FIInstr.ConvSum(v_rest, v_ListAcc(i).t_Currency, RSI_RSB_FIInstr.NATCUR, pDate), 0);
        END IF;
      END LOOP;
      v_ListAcc.delete;
    END IF;

    --Б14
    pNUNPRec.t_B14 := 0;

    GetAccountByParm( v_ListAcc, pNUNPRec.t_FIID,
                      '''-Переоценка, ц/б ССПУ_ЦБ'', ''-Переоценка, ц/б СССД_ЦБ''',
                      'AND '||Where_AccEqByParm(LLCLASS_KINDPORT, pNUNPRec.t_PortfID) );

    IF v_ListAcc.COUNT > 0 THEN
      FOR i IN v_ListAcc.FIRST .. v_ListAcc.LAST
      LOOP
        v_rest := GetRestAcc(v_ListAcc(i).t_Account, v_ListAcc(i).t_Currency, v_ListAcc(i).t_Chapter, pDate);

        IF v_rest != 0 THEN
          pNUNPRec.t_B14 := pNUNPRec.t_B14 + NVL(RSI_RSB_FIInstr.ConvSum(v_rest, v_ListAcc(i).t_Currency, RSI_RSB_FIInstr.NATCUR, pDate), 0);
        END IF;
      END LOOP;
      v_ListAcc.delete;
    END IF;

    --Б15
    pNUNPRec.t_B15 := 0;

    GetAccountByParm( v_ListAcc, pNUNPRec.t_FIID,
                      '''+Корректировка, ц/б''',
                      'AND '||Where_AccEqByParm(LLCLASS_KINDPORT, pNUNPRec.t_PortfID) ||
                      'AND '||Where_AccEqByParm(LLCLASS_KINDCB_PORTF, 3) );

    IF v_ListAcc.COUNT > 0 THEN
      FOR i IN v_ListAcc.FIRST .. v_ListAcc.LAST
      LOOP
        v_rest := GetRestAcc(v_ListAcc(i).t_Account, v_ListAcc(i).t_Currency, v_ListAcc(i).t_Chapter, pDate);

        IF v_rest != 0 THEN
          pNUNPRec.t_B15 := pNUNPRec.t_B15 + NVL(RSI_RSB_FIInstr.ConvSum(v_rest, v_ListAcc(i).t_Currency, RSI_RSB_FIInstr.NATCUR, pDate), 0);
        END IF;
      END LOOP;
      v_ListAcc.delete;
    END IF;

    --Б16
    pNUNPRec.t_B16 := 0;

    GetAccountByParm( v_ListAcc, pNUNPRec.t_FIID,
                      '''-Корректировка, ц/б''',
                      'AND '||Where_AccEqByParm(LLCLASS_KINDPORT, pNUNPRec.t_PortfID) ||
                      'AND '||Where_AccEqByParm(LLCLASS_KINDCB_PORTF, 3) );

    IF v_ListAcc.COUNT > 0 THEN
      FOR i IN v_ListAcc.FIRST .. v_ListAcc.LAST
      LOOP
        v_rest := GetRestAcc(v_ListAcc(i).t_Account, v_ListAcc(i).t_Currency, v_ListAcc(i).t_Chapter, pDate);

        IF v_rest != 0 THEN
          pNUNPRec.t_B16 := pNUNPRec.t_B16 + NVL(RSI_RSB_FIInstr.ConvSum(v_rest, v_ListAcc(i).t_Currency, RSI_RSB_FIInstr.NATCUR, pDate), 0);
        END IF;
      END LOOP;
      v_ListAcc.delete;
    END IF;

    --Б17
    pNUNPRec.t_B17 := 0;

    GetAccountByParm( v_ListAcc, pNUNPRec.t_FIID,
                      '''Резерв ц/б''',
                      'AND '||Where_AccEqByParm(LLCLASS_KINDPORT, pNUNPRec.t_PortfID) ||
                      'AND NOT ('||Where_AccEqByParm(LLCLASS_KINDPORT, 2) ||
                           'AND '||Where_AccEqByParm(LLCLASS_KINDCB_PORTF, 3) || ')' );

    IF v_ListAcc.COUNT > 0 THEN
      FOR i IN v_ListAcc.FIRST .. v_ListAcc.LAST
      LOOP
        v_rest := GetRestAcc(v_ListAcc(i).t_Account, v_ListAcc(i).t_Currency, v_ListAcc(i).t_Chapter, pDate);

        IF v_rest != 0 THEN
          pNUNPRec.t_B17 := pNUNPRec.t_B17 + NVL(RSI_RSB_FIInstr.ConvSum(v_rest, v_ListAcc(i).t_Currency, RSI_RSB_FIInstr.NATCUR, pDate), 0);
        END IF;
      END LOOP;
      v_ListAcc.delete;
    END IF;

    --Б17.1
    pNUNPRec.t_B17_1 := 0;

    GetAccountByParm( v_ListAcc, pNUNPRec.t_FIID,
                      '''Резерв ц/б''',
                      'AND '||Where_AccEqByParm(LLCLASS_KINDPORT, pNUNPRec.t_PortfID) ||
                      'AND '||Where_AccEqByParm(LLCLASS_KINDPORT, 2) ||
                      'AND '||Where_AccEqByParm(LLCLASS_KINDCB_PORTF, 3) );

    IF v_ListAcc.COUNT > 0 THEN
      FOR i IN v_ListAcc.FIRST .. v_ListAcc.LAST
      LOOP
        v_rest := GetRestAcc(v_ListAcc(i).t_Account, v_ListAcc(i).t_Currency, v_ListAcc(i).t_Chapter, pDate);

        IF v_rest != 0 THEN
          pNUNPRec.t_B17_1 := pNUNPRec.t_B17_1 + NVL(RSI_RSB_FIInstr.ConvSum(v_rest, v_ListAcc(i).t_Currency, RSI_RSB_FIInstr.NATCUR, pDate), 0);
        END IF;
      END LOOP;
      v_ListAcc.delete;
    END IF;

    --Б18
    pNUNPRec.t_B18 := 0;

    GetAccountByParm( v_ListAcc, pNUNPRec.t_FIID,
                      '''+Кор_Резерв, ЦБ''',
                      'AND '||Where_AccEqByParm(LLCLASS_KINDPORT, pNUNPRec.t_PortfID) ||
                      'AND NOT ('||Where_AccEqByParm(LLCLASS_KINDPORT, 2) ||
                           'AND '||Where_AccEqByParm(LLCLASS_KINDCB_PORTF, 3) || ')' );

    IF v_ListAcc.COUNT > 0 THEN
      FOR i IN v_ListAcc.FIRST .. v_ListAcc.LAST
      LOOP
        v_rest := GetRestAcc(v_ListAcc(i).t_Account, v_ListAcc(i).t_Currency, v_ListAcc(i).t_Chapter, pDate);

        IF v_rest != 0 THEN
          pNUNPRec.t_B18 := pNUNPRec.t_B18 + NVL(RSI_RSB_FIInstr.ConvSum(v_rest, v_ListAcc(i).t_Currency, RSI_RSB_FIInstr.NATCUR, pDate), 0);
        END IF;
      END LOOP;
      v_ListAcc.delete;
    END IF;

    --Б18.1
    pNUNPRec.t_B18_1 := 0;

    GetAccountByParm( v_ListAcc, pNUNPRec.t_FIID,
                      '''-Кор_Резерв, ЦБ''',
                      'AND '||Where_AccEqByParm(LLCLASS_KINDPORT, pNUNPRec.t_PortfID) ||
                      'AND '||Where_AccEqByParm(LLCLASS_KINDPORT, 2) ||
                      'AND '||Where_AccEqByParm(LLCLASS_KINDCB_PORTF, 3) );

    IF v_ListAcc.COUNT > 0 THEN
      FOR i IN v_ListAcc.FIRST .. v_ListAcc.LAST
      LOOP
        v_rest := GetRestAcc(v_ListAcc(i).t_Account, v_ListAcc(i).t_Currency, v_ListAcc(i).t_Chapter, pDate);

        IF v_rest != 0 THEN
          pNUNPRec.t_B18_1 := pNUNPRec.t_B18_1 + NVL(RSI_RSB_FIInstr.ConvSum(v_rest, v_ListAcc(i).t_Currency, RSI_RSB_FIInstr.NATCUR, pDate), 0);
        END IF;
      END LOOP;
      v_ListAcc.delete;
    END IF;

    --Б19
    pNUNPRec.t_B19 := 0;

    GetAccountByParm( v_ListAcc, pNUNPRec.t_FIID,
                      '''-Кор_Резерв, ЦБ''',
                      'AND '||Where_AccEqByParm(LLCLASS_KINDPORT, pNUNPRec.t_PortfID) ||
                      'AND NOT ('||Where_AccEqByParm(LLCLASS_KINDPORT, 2) ||
                           'AND '||Where_AccEqByParm(LLCLASS_KINDCB_PORTF, 3) || ')' );

    IF v_ListAcc.COUNT > 0 THEN
      FOR i IN v_ListAcc.FIRST .. v_ListAcc.LAST
      LOOP
        v_rest := GetRestAcc(v_ListAcc(i).t_Account, v_ListAcc(i).t_Currency, v_ListAcc(i).t_Chapter, pDate);

        IF v_rest != 0 THEN
          pNUNPRec.t_B19 := pNUNPRec.t_B19 + NVL(RSI_RSB_FIInstr.ConvSum(v_rest, v_ListAcc(i).t_Currency, RSI_RSB_FIInstr.NATCUR, pDate), 0);
        END IF;
      END LOOP;
      v_ListAcc.delete;
    END IF;

    --Б19.1
    pNUNPRec.t_B19_1 := 0;

    GetAccountByParm( v_ListAcc, pNUNPRec.t_FIID,
                      '''+Кор_Резерв, ЦБ''',
                      'AND '||Where_AccEqByParm(LLCLASS_KINDPORT, pNUNPRec.t_PortfID) ||
                      'AND '||Where_AccEqByParm(LLCLASS_KINDPORT, 2) ||
                      'AND '||Where_AccEqByParm(LLCLASS_KINDCB_PORTF, 3) );

    IF v_ListAcc.COUNT > 0 THEN
      FOR i IN v_ListAcc.FIRST .. v_ListAcc.LAST
      LOOP
        v_rest := GetRestAcc(v_ListAcc(i).t_Account, v_ListAcc(i).t_Currency, v_ListAcc(i).t_Chapter, pDate);

        IF v_rest != 0 THEN
          pNUNPRec.t_B19_1 := pNUNPRec.t_B19_1 + NVL(RSI_RSB_FIInstr.ConvSum(v_rest, v_ListAcc(i).t_Currency, RSI_RSB_FIInstr.NATCUR, pDate), 0);
        END IF;
      END LOOP;
      v_ListAcc.delete;
    END IF;

  END;

  --Получить счет в формате формате: ХХХХХ-ХХХ-Х-ХХХХ-ХХХХХХХ
  FUNCTION GetFormattedAccount(pAccount IN VARCHAR2)
    RETURN VARCHAR2
  IS
  BEGIN
    IF NVL(LENGTH(pAccount), 0) > 19 THEN
      RETURN substr(pAccount, 0, 5) || '-' ||
             substr(pAccount, 6, 3) || '-' ||
             substr(pAccount, 9, 1) || '-' ||
             substr(pAccount, 10, 4) || '-' ||
             substr(pAccount, 14/*, 7*/);
    ELSE
      RETURN pAccount;
    END IF;
  END;

  PROCEDURE SetComBRub(pNUNPRec IN OUT NOCOPY DNUNPREP_TMP%rowtype)
  IS
  BEGIN
    pNUNPRec.t_V13 := V13;

    SELECT NVL(SUM( RSI_RSB_FIInstr.ConvSum( CASE WHEN pNUNPRec.t_V13 = CHR(88) THEN dlcomis.t_Sum ELSE (dlcomis.t_Sum - dlcomis.t_NDS) END,
                                             comis.t_FIID_Comm, 0/*RSI_RSB_FIInstr.NATCUR*/, GREATEST(dlcomis.t_PlanPayDate, dlcomis.t_FactPayDate) )
              ), 0)
      INTO pNUNPRec.t_ComBRubSum
      FROM ddlcomis_dbt dlcomis, dsfcomiss_dbt comis
     WHERE dlcomis.t_DocKind   = pNUNPRec.t_BofficeKind
       AND dlcomis.t_DocID     = pNUNPRec.t_DealID
       AND dlcomis.t_FeeType   = comis.t_FeeType
       AND dlcomis.t_ComNumber = comis.t_Number;

    BEGIN
      pNUNPRec.t_ComRub := NVL(RSB_STRUCT.getMoney(rsi_rsb_kernel.GetNote(RSB_SECUR.OBJTYPE_SECDEAL, LPAD(pNUNPRec.t_DealID, 34, '0'), 25/*Прочие комиссии*/, pNUNPRec.t_DDB)), 0);
    EXCEPTION
      WHEN OTHERS THEN pNUNPRec.t_ComRub :=0;
    END;
  END;

  PROCEDURE SetSettings
  IS
  BEGIN
    V13 := CASE WHEN Rsb_Common.GetRegIntValue('SECUR\НАЛОГОВЫЙ УЧЕТ\V13') = 0 THEN CHR(88) ELSE CHR(0) END;
    BPP_ACCOUNT_METHOD := Rsb_Common.GetRegIntValue('SECUR\СПОСОБ ВЕДЕНИЯ СЧЕТОВ БПП');
    IsReadedSettings := true;
  END;


  --Формирование данных для отчета НУНП
  PROCEDURE CreateNUNPData( pBegDate IN DATE,
                            pEndDate IN DATE,
                            pSessionID IN NUMBER,
                            pReqID IN VARCHAR2 DEFAULT NULL )
  IS
    v_nunprep nunprep_t := nunprep_t();
    v_nunprec DNUNPREP_TMP%rowtype;
    v_rate NUMBER;
    v_DrawingDate DATE;
    v_NomDate DATE;
    v_startPI BOOLEAN := false; -- запущен ли индикатор прогресса для веб
    v_usePI BOOLEAN := CASE WHEN NVL(LENGTH(pReqID),0) > 0 THEN TRUE ELSE FALSE END;
    v_RowNum NUMBER := 0;
    v_ListAcc ListAcc_t;
    v_rest NUMBER := 0;
  BEGIN

    SetSettings;

    --Отобрать строки отчета
    FOR cData IN ( SELECT count(1) over() CNT,
                          ROWNUM RN,
                          FI.t_FIID,
                          FI.t_FaceValueFI,
                          FI.t_FaceValue,
                          FI.t_FI_CODE,
                          FI.t_Name AS t_SecName,
                          RSI_RSB_FIInstr.FI_AvrKindsGetRoot( 2, FI.t_AvoirKind ) AS t_RootAvrKind,
                          AV.t_ISIN,
                          AV.t_LSIN AS t_NumReg,
                          AV.t_IndexNom,
                          AVK.t_Name AS t_Code,
                          TXBuyLot.T_DEALID,
                          TXBuyLot.T_DEALDATE AS t_DR,
                          ( SELECT t_ID
                              FROM ddlrq_dbt
                             WHERE t_DocKind  = DealBuy.t_BofficeKind
                               AND t_DocID    = DealBuy.t_DealID
                               AND t_SubKind  = RSI_DLRQ.DLRQ_SUBKIND_AVOIRISS
                               AND t_DealPart = 1
                               AND t_Type     = RSI_DLRQ.DLRQ_TYPE_DELIVERY
                               AND ROWNUM     < 2
                          ) AS t_RqID_DELIVERY,
                          Leg.t_Principal AS t_QntyB,
                          ( SELECT nvl(sum(Lnk.t_Amount), 0)
                              FROM dsctxlnk_dbt Lnk
                             WHERE Lnk.t_FIID = FI.t_FIID
                               AND Lnk.t_Type IN (RSB_SCTXC.TXLNK_DELIVER)
                               AND (Lnk.t_Date <= pEndDate)
                               AND (Lnk.t_BegDate = UnknownDate OR Lnk.t_BegDate <= pEndDate)
                               AND Lnk.t_BuyID = TXBuyLot.t_ID
                          ) AS t_QntyS,
                          ( SELECT T_CCY FROM DFININSTR_DBT WHERE T_FIID = FI.t_FaceValueFI ) AS t_VN,
                          RSI_RSB_FIInstr.FI_GetNominalOnDate( FI.t_FIID, pEndDate ) AS t_NomVN_DH,
                          Leg.t_RelativePrice,
                          Leg.t_Price,
                          Leg.t_CFI,
                          Leg.t_NKDFIID,
                          Leg.t_NKD,
                          ( SELECT NVL(MAX(t_DrawingDate), UnknownDate)
                              FROM DFIWARNTS_DBT
                             WHERE t_FIID = FI.t_FIID
                               AND t_IsPartial = CHR(0)
                               AND t_SPIsClosed = CHR(88)
                               AND t_DrawingDate <= pEndDate
                          ) AS t_LastCoupDate,
                          DealBuy.t_DealCodeTS AS t_CodeB,
                          DealBuy.t_BofficeKind,
                          TXBuyLot.t_Portfolio AS t_PortfID,
                          ( CASE WHEN TXBuyLot.t_Portfolio = RSB_PMWRTOFF.KINDPORT_TRADE THEN 1
                                 WHEN TXBuyLot.t_Portfolio = RSB_PMWRTOFF.KINDPORT_SALE THEN 2
                                 WHEN TXBuyLot.t_Portfolio IN (RSB_PMWRTOFF.KINDPORT_CONTR, RSB_PMWRTOFF.KINDPORT_PROMISSORY, RSB_PMWRTOFF.KINDPORT_RETIRE ) THEN 3
                                 ELSE -1
                            END
                          ) AS t_O1,
                          DealBuy.t_PartyID AS t_ContB_ID,
                          NVL( (SELECT t_Name FROM DPARTY_DBT WHERE t_PartyID = DealBuy.t_PartyID), CHR(0) ) AS t_ContB_Name

                     FROM ( SELECT NVL(TXRest.T_SourceID,0) AS SourceID, NVL(sum(TXRest.T_AMOUNT),0) AS Amount
                              FROM DSCTXREST_DBT TXRest
                             WHERE TXRest.T_AMOUNT > 0
                               AND TXRest.t_BUYDATE <= pEndDate
                               AND TXRest.t_BUYDATE <> UnknownDate
                               AND ( TXRest.t_SALEDATE IS NULL  OR
                                     TXRest.t_SALEDATE = UnknownDate OR
                                     TXRest.t_SALEDATE > pEndDate
                                   OR (    TXRest.t_SALEDATE <= pEndDate
                                       AND TXRest.T_TYPE = RSB_SCTXC.TXREST_B_DR
                                       AND EXISTS (SELECT 1
                                                     FROM DSCTXREST_DBT
                                                    WHERE T_SOURCEID = TXRest.T_SOURCEID
                                                      AND T_TYPE = RSB_SCTXC.TXREST_DR_U
                                                      AND t_BUYDATE > pEndDate)
                                      )
                                   )
                             GROUP BY TXRest.T_SourceID
                          ) QueryRest,
                          DSCTXLOT_DBT currTXBuyLot, DSCTXLOT_DBT TXBuyLot, DDL_TICK_DBT DealBuy, DFININSTR_DBT FI, DAVOIRISS_DBT AV, DAVRKINDS_DBT AVK, DDL_LEG_DBT Leg
                    WHERE currTXBuyLot.t_ID = QueryRest.SourceID
                      AND TXBuyLot.t_ID = currTXBuyLot.t_BegLotID
                      AND TXBuyLot.t_Type = RSB_SCTXC.TXLOTS_BUY
                      AND FI.t_FIID = TXBuyLot.t_FIID
                      AND DealBuy.t_DealID = TXBuyLot.t_DealID
                      AND AV.t_FIID = FI.t_FIID
                      AND AVK.t_AVOIRKIND = FI.t_AVOIRKIND
                      AND AVK.t_FI_KIND = FI.t_FI_KIND
                      AND Leg.t_LegKind = 0 --LEG_KIND_DL_TICK
                      AND Leg.t_DealID = DealBuy.t_DealID
                      AND Leg.t_LegID = 0
                 )
   LOOP
      if( v_usePI = true and v_startPI = false ) then -- индикатор прогресса для веб
        v_startPI := true;
        WebProgressIndicator_Start(pReqID, cData.Cnt, 'НУНП: обработка отобранных строк');
      end if;

      v_nunprec := NULL;

      v_nunprec.t_IsItog       := chr(0);
      v_nunprec.t_IsItogPortf  := chr(0);
      v_nunprec.t_FIID         := cData.t_FIID;
      v_nunprec.t_FaceValueFI  := cData.t_FaceValueFI;
      v_nunprec.t_FaceValue    := cData.t_FaceValue;
      v_nunprec.t_FI_CODE      := cData.t_FI_CODE;
      v_nunprec.t_SecName      := cData.t_SecName;
      v_nunprec.t_RootAvrKind  := cData.t_RootAvrKind;
      v_nunprec.t_DealID       := cData.t_DealID;
      v_nunprec.t_BofficeKind  := cData.t_BofficeKind;
      v_nunprec.t_PortfID      := cData.t_PortfID;
      v_nunprec.t_O1           := cData.t_O1;
      v_nunprec.t_NumReg       := cData.t_NumReg;
      v_nunprec.t_ISIN         := cData.t_ISIN;
      v_nunprec.t_Code         := cData.t_Code;
      v_nunprec.t_DR           := cData.t_DR;

      SELECT GREATEST(t_FactDate, t_PlanDate)
        INTO v_nunprec.t_DDB
        FROM ddlrq_dbt
       WHERE t_ID  =  cData.t_RqID_DELIVERY;

      v_nunprec.t_QntyB        := cData.t_QntyB;
      v_nunprec.t_QntyS        := cData.t_QntyS;
      v_nunprec.t_QntyOst      := v_nunprec.t_QntyB - v_nunprec.t_QntyS;
      v_nunprec.t_VN           := cData.t_VN;

      v_rate := RSI_RSB_FIInstr.FI_GetPartialPersent( v_nunprec.t_FIID, v_nunprec.t_DDB );

      SELECT MAX(t_DrawingDate) INTO v_DrawingDate
      FROM dfiwarnts_dbt
      WHERE t_FIID = v_nunprec.t_FIID
        AND t_IsPartial = chr(88);

      IF v_nunprec.t_DDB >= v_DrawingDate AND v_rate = 100 THEN
         v_NomDate := v_DrawingDate - 1;
      ELSE
         v_NomDate := v_nunprec.t_DDB;
      END IF;
      v_nunprec.t_NomVN_DDB    := RSI_RSB_FIInstr.FI_GetNominalOnDate( v_nunprec.t_FIID, v_NomDate );

      IF cData.t_IndexNom = chr(88) AND v_nunprec.t_NomVN_DDB = 0
      THEN
         AddNURepError(pSessionID, 'Для ц/б ' || v_nunprec.t_FI_CODE || ' не задан номинал на дату ' || to_char(v_NomDate, 'dd.mm.yyyy'));
         CONTINUE;
      END IF;

      v_nunprec.t_NomVN_DH     := cData.t_NomVN_DH;

      IF v_nunprec.t_FaceValueFI <> RSI_RSB_FIInstr.NATCUR THEN
        v_nunprec.t_CrVNrub_DDB := NVL(RSI_RSB_FIInstr.ConvSum(1, v_nunprec.t_FaceValueFI, RSI_RSB_FIInstr.NATCUR, v_nunprec.t_DDB), 0);
        v_nunprec.t_CrVNrub_DH := NVL(RSI_RSB_FIInstr.ConvSum(1, v_nunprec.t_FaceValueFI, RSI_RSB_FIInstr.NATCUR, pEndDate), 0);
      ELSE
        v_nunprec.t_CrVNrub_DDB := 1.0;
        v_nunprec.t_CrVNrub_DH := 1.0;
      END IF;

      --для облигаций
      IF v_nunprec.t_RootAvrKind = RSI_RSB_FIInstr.AVOIRKIND_BOND THEN
        IF cData.t_RelativePrice = chr(88) THEN
          v_nunprec.t_PrBnom := cData.t_Price;
        ELSE
          v_nunprec.t_PrBnom := cData.t_Price * 100 / v_nunprec.t_NomVN_DDB;
        END IF;
        v_nunprec.t_NkdBVN_DDB := NVL(RSI_RSB_FIInstr.ConvSum(cData.t_NKD, cData.t_NKDFIID, v_nunprec.t_FaceValueFI, v_nunprec.t_DDB), 0);
        v_nunprec.t_LastCoupDate := cData.t_LastCoupDate;
        v_nunprec.t_SumBVN_DH := v_nunprec.t_QntyOst * v_nunprec.t_NomVN_DH * v_nunprec.t_PrBnom / 100;

        v_nunprec.t_PrBVN := 0;

      --для прочих ц/б
      ELSE
        IF cData.t_RelativePrice = chr(88) THEN
          v_nunprec.t_PrBVN := v_nunprec.t_NomVN_DDB * cData.t_Price / 100;
        ELSE
          v_nunprec.t_PrBVN := cData.t_Price;
        END IF;
        v_nunprec.t_PrBVN := NVL(RSI_RSB_FIInstr.ConvSum(v_nunprec.t_PrBVN, cData.t_CFI, v_nunprec.t_FaceValueFI, v_nunprec.t_DDB), 0);
        v_nunprec.t_SumBVN_DH := v_nunprec.t_QntyOst * v_nunprec.t_PrBVN;

        v_nunprec.t_PrBnom := 0;
        v_nunprec.t_NkdBVN_DDB := 0;
      END IF;

      v_nunprec.t_SumBrub_DDB := v_nunprec.t_SumBVN_DH * v_nunprec.t_CrVNrub_DDB;
      v_nunprec.t_SumBrub_DH := v_nunprec.t_SumBVN_DH * v_nunprec.t_CrVNrub_DH;

      IF v_nunprec.t_LastCoupDate < v_nunprec.t_DDB THEN
        v_nunprec.t_NkdBVN_DH := CASE WHEN v_nunprec.t_QntyB = 0 THEN 0 ELSE v_nunprec.t_NkdBVN_DDB / v_nunprec.t_QntyB * v_nunprec.t_QntyOst END;
      ELSE
        v_nunprec.t_NkdBVN_DH := 0;
      END IF;

      v_nunprec.t_NkdBrub_DDB := v_nunprec.t_NkdBVN_DH * v_nunprec.t_CrVNrub_DDB;
      v_nunprec.t_NkdBrub_DH := v_nunprec.t_NkdBVN_DH * v_nunprec.t_CrVNrub_DH;

      v_nunprec.t_CodeB            := cData.t_CodeB;
      SetComBRub(v_nunprec);
      v_nunprec.t_ContB_Name       := cData.t_ContB_Name;

      IF cData.t_ContB_ID > 0 THEN
        v_nunprec.t_ContB_VZL := CASE WHEN RSB_SECUR.GetMainObjAttr( RSB_SECUR.OBJTYPE_PARTY, LPAD(cData.t_ContB_ID, 10, '0'), 58/*Вид взаимозависимости*/, v_nunprec.t_DDB)
                                         IN (1/*0*/, 2/*1*/, 3/*2*/, 4/*3*/) THEN 'да' ELSE 'нет' END;
      ELSE
        v_nunprec.t_ContB_VZL := 'нет';
      END IF;

      --Б1(Б6) (по КУ с характеристикой только по Сделке)
      v_nunprec.t_BalSumVN_DH := 0;
      v_nunprec.t_BalSumRub_DH := 0;

      GetAccountByParmDeal( v_ListAcc, v_nunprec.t_DealID,
                            '''Ц/б, БПП''',
                            'AND '||Where_AccEqByParm(LLCLASS_KINDPORT, v_nunprec.t_PortfID) );

      IF v_ListAcc.COUNT > 0 THEN
        FOR i IN v_ListAcc.FIRST .. v_ListAcc.LAST
        LOOP
          v_rest := GetRestAcc(v_ListAcc(i).t_Account, v_ListAcc(i).t_Currency, v_ListAcc(i).t_Chapter, pEndDate);

          IF v_rest != 0 THEN
            v_nunprec.t_BalSumVN_DH := v_nunprec.t_BalSumVN_DH + NVL(v_rest, 0);
            v_nunprec.t_BalSumRub_DH := v_nunprec.t_BalSumRub_DH + NVL(RSI_RSB_FIInstr.ConvSum(v_rest, v_ListAcc(i).t_Currency, RSI_RSB_FIInstr.NATCUR, pEndDate), 0);
          END IF;
        END LOOP;
        v_ListAcc.delete;
      END IF;

      --Б2(Б7) (по КУ с характеристикой только по Сделке)
      v_nunprec.t_BalNkdBVN_DH := 0;
      v_nunprec.t_BalNkdBrub_DH := 0;

      GetAccountByParmDeal( v_ListAcc, v_nunprec.t_DealID,
                            '''Уплаченный НКД, БПП''',
                            'AND '||Where_AccEqByParm(LLCLASS_KINDPORT, v_nunprec.t_PortfID) );

      IF v_ListAcc.COUNT > 0 THEN
        FOR i IN v_ListAcc.FIRST .. v_ListAcc.LAST
        LOOP
          v_rest := GetRestAcc(v_ListAcc(i).t_Account, v_ListAcc(i).t_Currency, v_ListAcc(i).t_Chapter, pEndDate);

          IF v_rest != 0 THEN
            v_nunprec.t_BalNkdBVN_DH := v_nunprec.t_BalNkdBVN_DH + NVL(v_rest, 0);
            v_nunprec.t_BalNkdBrub_DH := v_nunprec.t_BalNkdBrub_DH + NVL(RSI_RSB_FIInstr.ConvSum(v_rest, v_ListAcc(i).t_Currency, RSI_RSB_FIInstr.NATCUR, pEndDate), 0);
          END IF;
        END LOOP;
        v_ListAcc.delete;
      END IF;

      v_nunprec.t_VR2 := (v_nunprec.t_SumBVN_DH + v_nunprec.t_NkdBVN_DH) * (v_nunprec.t_CrVNrub_DH - v_nunprec.t_CrVNrub_DDB);
      v_nunprec.t_ComBRub := CASE WHEN v_nunprec.t_QntyB = 0 THEN 0 ELSE v_nunprec.t_ComBRubSum * v_nunprec.t_QntyOst / v_nunprec.t_QntyB + v_nunprec.t_ComRub END;

      v_nunprep.Extend();
      v_nunprep( v_nunprep.last ) := v_nunprec;

      if( v_usePI = true ) then -- индикатор прогресса для веб
        WebProgressIndicator_Update(cData.RN);
      end if;

    END LOOP;

    IF v_nunprep.COUNT > 0 THEN
      FORALL i IN v_nunprep.FIRST .. v_nunprep.LAST
           INSERT INTO DNUNPREP_TMP VALUES v_nunprep(i);
      v_nunprep.delete;
    END IF;

    if( v_usePI = true ) then -- индикатор прогресса для веб
      --WebProgressIndicator_Stop;
      v_startPI := false;
    end if;


    --Создать строки промежуточных итогов по портфелям внутри выпусков
    FOR cData IN ( SELECT count(1) over() CNT,
                          t_FIID, t_FaceValueFI, t_SecName, t_O1, t_PortfID, t_ISIN, t_NumReg, t_Code, t_FI_CODE
                     FROM DNUNPREP_TMP
                    WHERE t_IsItogPortf = chr(0)
                      AND t_IsItog = chr(0)
                 GROUP BY t_FIID, t_FaceValueFI, t_SecName, t_O1, t_PortfID, t_ISIN, t_NumReg, t_Code, t_FI_CODE )
    LOOP
      if( v_usePI = true and v_startPI = false ) then -- индикатор прогресса для веб
        v_startPI := true;
        v_RowNum := 0;
        WebProgressIndicator_Update(0, cData.Cnt, 'НУНП: создание строк промежуточных итогов по портфелям внутри выпусков');
      end if;

      v_nunprec := NULL;

      v_nunprec.t_IsItogPortf  := chr(88);
      v_nunprec.t_IsItog       := chr(0);
      v_nunprec.t_FIID         := cData.t_FIID;
      v_nunprec.t_SecName      := cData.t_SecName;
      v_nunprec.t_O1           := cData.t_O1;
      v_nunprec.t_ISIN         := cData.t_ISIN;
      v_nunprec.t_NumReg       := cData.t_NumReg;
      v_nunprec.t_Code         := cData.t_Code;
      v_nunprec.t_FI_Code      := cData.t_FI_Code;
      v_nunprec.t_FaceValueFI  := cData.t_FaceValueFI;
      v_nunprec.t_PortfID      := cData.t_PortfID;

      SELECT NVL(SUM(t_SumBVN_DH), 0) t_SumBVN_DH
        INTO v_nunprec.t_SumBVN_DH
        FROM DNUNPREP_TMP
       WHERE t_FIID = cData.t_FIID
         AND t_IsItogPortf = CHR(0)
         AND t_IsItog = CHR(0)
         AND t_PortfID = cData.t_PortfID;

      SELECT t_CrVNrub_DH
        INTO v_nunprec.t_CrVNrub_DH
        FROM DNUNPREP_TMP
       WHERE T_FIID = v_nunprec.t_FIID
         AND t_IsItog = chr(0)
         AND t_IsItogPortf = chr(0)
         AND rownum < 2;

      SetFldsRestAcc(v_nunprec, pEndDate);

      v_nunprec.t_B20 := (v_nunprec.t_BalSumRub_DH + v_nunprec.t_BalNkdBrub_DH + v_nunprec.t_B8 + v_nunprec.t_B9 + v_nunprec.t_B10 + v_nunprec.t_B13 + v_nunprec.t_B15 + v_nunprec.t_B18 + v_nunprec.t_B18_1)
                       - (v_nunprec.t_B14 + v_nunprec.t_B16 + v_nunprec.t_B17 + v_nunprec.t_B19 + v_nunprec.t_B19_1);

      v_nunprec.t_VR1 := v_nunprec.t_BalSumRub_DH + v_nunprec.t_B9 + v_nunprec.t_B10 - v_nunprec.t_SumBVN_DH * v_nunprec.t_CrVNrub_DH;

      v_nunprep.Extend();
      v_nunprep( v_nunprep.last ) := v_nunprec;

      if( v_usePI = true ) then -- индикатор прогресса для веб
        v_RowNum := v_RowNum + 1;
        WebProgressIndicator_Update(v_RowNum);
      end if;

    END LOOP;

    IF v_nunprep.COUNT > 0 THEN
      FORALL i IN v_nunprep.FIRST .. v_nunprep.LAST
         INSERT INTO DNUNPREP_TMP VALUES v_nunprep(i);
      v_nunprep.delete;
    END IF;

    if( v_usePI = true ) then -- индикатор прогресса для веб
      --WebProgressIndicator_Stop;
      v_startPI := false;
    end if;


    --Создать строки промежуточных итогов по выпускам
    FOR cData IN ( SELECT count(1) over() CNT,
                          t_FIID, t_SecName, t_ISIN, t_NumReg, t_Code, t_FI_CODE
                     FROM DNUNPREP_TMP
                    WHERE t_IsItog = chr(0)
                      AND t_IsItogPortf = chr(0)
                 GROUP BY t_FIID, t_SecName, t_ISIN, t_NumReg, t_Code, t_FI_CODE )
    LOOP
      if( v_usePI = true and v_startPI = false ) then -- индикатор прогресса для веб
        v_startPI := true;
        v_RowNum := 0;
        WebProgressIndicator_Update(0, cData.Cnt, 'НУНП: создание строк промежуточных итогов по выпускам');
      end if;

      v_nunprec := NULL;

      v_nunprec.t_IsItog       := chr(88);
      v_nunprec.t_IsItogPortf  := chr(0);
      v_nunprec.t_FIID         := cData.t_FIID;
      v_nunprec.t_SecName      := cData.t_SecName;
      v_nunprec.t_ISIN         := cData.t_ISIN;
      v_nunprec.t_NumReg       := cData.t_NumReg;
      v_nunprec.t_Code         := cData.t_Code;
      v_nunprec.t_FI_Code      := cData.t_FI_Code;

      SELECT NVL(SUM(t_QntyB), 0) t_QntyB,
             NVL(SUM(t_QntyS), 0) t_QntyS,
             NVL(SUM(t_QntyOst), 0) t_QntyOst,
             NVL(SUM(t_QntyB_OR), 0) t_QntyB_OR,
             NVL(SUM(t_QntyS_PR), 0) t_QntyS_PR,
             NVL(SUM(t_SumBrub_DDB), 0) t_SumBrub_DDB,
             NVL(SUM(t_NkdBrub_DDB), 0) t_NkdBrub_DDB,
             NVL(SUM(t_SumBrub_DH), 0) t_SumBrub_DH,
             NVL(SUM(t_NkdBrub_DH), 0) t_NkdBrub_DH,
             NVL(SUM(t_VR2), 0) t_VR2,
             NVL(SUM(t_ComBRub), 0) t_ComBRub
        INTO v_nunprec.t_QntyB,
             v_nunprec.t_QntyS,
             v_nunprec.t_QntyOst,
             v_nunprec.t_QntyB_OR,
             v_nunprec.t_QntyS_PR,
             v_nunprec.t_SumBrub_DDB,
             v_nunprec.t_NkdBrub_DDB,
             v_nunprec.t_SumBrub_DH,
             v_nunprec.t_NkdBrub_DH,
             v_nunprec.t_VR2,
             v_nunprec.t_ComBRub
        FROM DNUNPREP_TMP
       WHERE t_FIID = cData.t_FIID
         AND t_IsItogPortf = CHR(0)
         AND t_IsItog = CHR(0);

      SELECT NVL(SUM(t_BalSumRub_DH), 0) t_BalSumRub_DH,
             NVL(SUM(t_BalNkdBrub_DH), 0) t_BalNkdBrub_DH,
             NVL(SUM(t_B8), 0) t_B8,
             NVL(SUM(t_B9), 0) t_B9,
             NVL(SUM(t_B10), 0) t_B10,
             NVL(SUM(t_B11), 0) t_B11,
             NVL(SUM(t_B12), 0) t_B12,
             NVL(SUM(t_B13), 0) t_B13,
             NVL(SUM(t_B14), 0) t_B14,
             NVL(SUM(t_B15), 0) t_B15,
             NVL(SUM(t_B16), 0) t_B16,
             NVL(SUM(t_B17), 0) t_B17,
             NVL(SUM(t_B17_1), 0) t_B17_1,
             NVL(SUM(t_B18), 0) t_B18,
             NVL(SUM(t_B18_1), 0) t_B18_1,
             NVL(SUM(t_B19), 0) t_B19,
             NVL(SUM(t_B19_1), 0) t_B19_1,
             NVL(SUM(t_B20), 0) t_B20,
             NVL(SUM(t_VR1), 0) t_VR1
        INTO v_nunprec.t_BalSumRub_DH,
             v_nunprec.t_BalNkdBrub_DH,
             v_nunprec.t_B8,
             v_nunprec.t_B9,
             v_nunprec.t_B10,
             v_nunprec.t_B11,
             v_nunprec.t_B12,
             v_nunprec.t_B13,
             v_nunprec.t_B14,
             v_nunprec.t_B15,
             v_nunprec.t_B16,
             v_nunprec.t_B17,
             v_nunprec.t_B17_1,
             v_nunprec.t_B18,
             v_nunprec.t_B18_1,
             v_nunprec.t_B19,
             v_nunprec.t_B19_1,
             v_nunprec.t_B20,
             v_nunprec.t_VR1
        FROM DNUNPREP_TMP
       WHERE t_FIID = cData.t_FIID
         AND t_IsItogPortf = CHR(88);

      SELECT NVL(SUM(sv.T_NKDREPRUB3), 0) T_NKDREPRUB3,
             NVL(SUM(sv.T_NKDREPCHGNUMRUB), 0) T_NKDREPCHGNUMRUB,
             NVL(SUM(sv.T_NKDREPVN), 0) T_NKDREPVN
        INTO v_nunprec.T_NKDREPRUB3,
             v_nunprec.T_NKDREPCHGNUMRUB,
             v_nunprec.T_NKDREPVN
        FROM DSCTXTOTAL_DBT sv LEFT JOIN DFININSTR_DBT fin on sv.t_FI_CODE = fin.t_FI_CODE
       WHERE fin.t_fiid = cData.t_FIID ;

      --QntyB_OR
      BEGIN
        SELECT NVL(SUM(q.Amount), 0) INTO v_nunprec.t_QntyB_OR
          FROM ( SELECT leg.t_Principal AS Amount
                   FROM ddl_tick_dbt tick, ddl_leg_dbt leg
                  WHERE tick.t_BofficeKind = RSB_SECUR.DL_SECURITYDOC
                    AND tick.t_DealStatus >= 10 --DL_READIED
                    AND tick.t_PFI = cData.t_FIID
                    AND tick.t_ClientID = 0
                    AND RSB_SECUR.IsRepo( RSB_SECUR.get_OperationGroup( RSB_SECUR.get_OperSysTypes( tick.t_DealType, tick.t_BofficeKind ) ) ) = 1
                    AND RSB_SECUR.IsBasket( RSB_SECUR.get_OperationGroup( RSB_SECUR.get_OperSysTypes( tick.t_DealType, tick.t_BofficeKind ) ) ) = 0
                    AND RSB_SECUR.IsBuy( RSB_SECUR.get_OperationGroup( RSB_SECUR.get_OperSysTypes( tick.t_DealType, tick.t_BofficeKind ) ) ) = 1
                    AND exists ( SELECT 1
                                   FROM DDLRQ_DBT
                                  WHERE t_DocKind  = tick.t_BofficeKind
                                    AND t_DocID    = tick.t_DealID
                                    AND t_SubKind  = RSI_DLRQ.DLRQ_SUBKIND_AVOIRISS
                                    AND t_DealPart = 2
                                    AND t_Type     = RSI_DLRQ.DLRQ_TYPE_DELIVERY
                                    AND (t_FactDate = UnknownDate or t_FactDate > pEndDate)
                               )
                    AND leg.t_LegKind = 0 --LEG_KIND_DL_TICK
                    AND leg.t_DealID = tick.t_DealID
                    AND leg.t_LegID = 0
                 UNION
                 SELECT ens.t_Principal AS Amount
                   FROM ddl_tick_ens_dbt ens
                  WHERE ens.t_FIID = cData.t_FIID
                    AND ens.t_DealID IN ( SELECT t_DealID
                                            FROM ddl_tick_dbt
                                           WHERE t_BofficeKind = RSB_SECUR.DL_SECURITYDOC
                                             AND t_DealStatus >= 10 --DL_READIED
                                             AND RSB_SECUR.IsBasket( RSB_SECUR.get_OperationGroup( RSB_SECUR.get_OperSysTypes( t_DealType, t_BofficeKind ) ) ) = 1
                                             AND RSB_SECUR.IsBuy( RSB_SECUR.get_OperationGroup( RSB_SECUR.get_OperSysTypes( t_DealType, t_BofficeKind ) ) ) = 1
                                        )
               ) q;

      END;

      --QntyS_PR
      BEGIN
        SELECT NVL(SUM(qs.t_Amount), 0) INTO v_nunprec.t_QntyS_PR
          FROM ( SELECT (q.t_Amount - NVL( (SELECT SUM(rsb_sctx.TXGetSumSCTXLSOnDate( RLnk.t_ID, pEndDate))
                                              FROM dsctxlnk_dbt RLnk,
                                                   dsctxlot_dbt Rcurrsalelot
                                             WHERE RLnk.t_BuyID = q.BuyLot_ID
                                               AND RLnk.t_Type = q.LinkType
                                               AND Rcurrsalelot.t_ID = RLnk.t_SaleID
                                               AND RLnk.t_Date <= pEndDate
                                               AND (RLnk.t_BegDate = UnknownDate or RLnk.t_BegDate <= UnknownDate)
                                               AND (RLnk.t_EndDate = UnknownDate or RLnk.t_EndDate > pEndDate)
                                               AND (    Rcurrsalelot.t_BuyDate = UnknownDate
                                                     or Rcurrsalelot.t_BuyDate > pEndDate
                                                   )
                                           ), 0)
                        ) AS t_Amount
                   FROM ( SELECT TXBuyLot.t_ID AS BuyLot_ID, lnk.t_Type AS LinkType, NVL(SUM(lnk.T_AMOUNT), 0) AS t_Amount
                            FROM dsctxlnk_dbt lnk,
                                 dsctxlot_dbt TXBuyLot,
                                 dsctxlot_dbt currbuylot,
                                 dsctxlot_dbt currsalelot
                           WHERE lnk.t_Type in (RSB_SCTXC.TXLNK_DELREPO, RSB_SCTXC.TXLNK_SUBSTREPO) and
                                 Lnk.t_FIID = cData.t_FIID and
                                 currbuylot.t_ID = lnk.t_BuyID and
                                 TXBuyLot.t_ID = currbuylot.t_BegLotID and
                                 TXBuyLot.t_Type = RSB_SCTXC.TXLOTS_BUY and
                                 currsalelot.t_ID = lnk.t_SaleID and
                                 lnk.t_Date <= pEndDate and
                                 (lnk.t_BegDate = UnknownDate or lnk.t_BegDate <= pEndDate) and
                                 (lnk.t_EndDate = UnknownDate or lnk.t_EndDate > pEndDate) and
                                 (    currsalelot.t_BuyDate = UnknownDate
                                   or currsalelot.t_BuyDate > pEndDate
                                 )
                           GROUP BY TXBuyLot.t_ID, lnk.t_Type
                        ) q
               ) qs
         WHERE qs.t_Amount > 0;
      END;

      --CrVNrub_DH
      BEGIN
        SELECT t_CrVNrub_DH
          INTO v_nunprec.t_CrVNrub_DH
          FROM DNUNPREP_TMP
         WHERE T_FIID = v_nunprec.t_FIID
           AND t_IsItog = chr(0)
           AND t_IsItogPortf = chr(0)
           AND rownum < 2;
      END;

      v_nunprep.Extend();
      v_nunprep( v_nunprep.last ) := v_nunprec;

      if( v_usePI = true ) then -- индикатор прогресса для веб
        v_RowNum := v_RowNum + 1;
        WebProgressIndicator_Update(v_RowNum);
      end if;

    END LOOP;

    IF v_nunprep.COUNT > 0 THEN
      FORALL i IN v_nunprep.FIRST .. v_nunprep.LAST
           INSERT INTO DNUNPREP_TMP VALUES v_nunprep(i);
      v_nunprep.delete;
    END IF;

    if( v_usePI = true ) then -- индикатор прогресса для веб
      --WebProgressIndicator_Stop;
      v_startPI := false;
    end if;

  END CreateNUNPData;


    -- Ставка налога в отношении процентных доходов по облигациям
    FUNCTION GetStRate_15_20_9_0( v_TaxGroup IN NUMBER )RETURN VARCHAR2
    IS
    BEGIN
      IF (v_TaxGroup = Rsb_SCTX.STATE_BOND_FED_PERC    or
          v_TaxGroup = Rsb_SCTX.STATE_BOND_SUBFED_PERC or
          v_TaxGroup = Rsb_SCTX.MOUN_BOND_15_PERC      or
          v_TaxGroup = Rsb_SCTX.BOND_INDEXNOM          or
          v_TaxGroup = Rsb_SCTX.KORP_BOND_IP15
         ) THEN
         RETURN '15%';
      ELSIF
         (v_TaxGroup = Rsb_SCTX.STATE_BOND_IN_LOAN
         ) THEN
         RETURN '0%';
      ELSIF
         (v_TaxGroup = Rsb_SCTX.MOUN_BOND_9_PERC or
          v_TaxGroup = Rsb_SCTX.KORP_BOND_IP9
         ) THEN
         RETURN '9%';
      ELSE
         RETURN '20%';
      END IF;
    END;

    -- Ставка налога в отношении процентных доходов по облигациям
    FUNCTION GetStRateFor17( pFI_CODE IN VARCHAR2 )RETURN VARCHAR2
    AS
       vStRate VARCHAR2(8) := '';
    BEGIN
       SELECT case when (sc.T_NKDREPRUB1 > 0 or sc.T_NKDALLRUB1  > 0) and (sc.T_NKDREPRUB2 > 0 or sc.T_NKDALLRUB2  > 0) then '15%/20%'
                          when  (sc.T_NKDREPRUB1 > 0 or sc.T_NKDALLRUB1  > 0) then '15%'
                          when  (sc.T_NKDREPRUB2 > 0 or sc.T_NKDALLRUB2  > 0) then '20%'
                   else null end into vStRate
         FROM DSCTXTOTAL_DBT sc
       WHERE sc.t_FI_CODE = pFI_CODE;
       RETURN vStRate;
    EXCEPTION WHEN OTHERS THEN
       RETURN NULL;
    END;

  /*Поиск счетов, параметризованных по ц\б*/
  FUNCTION GetAccountByFIID_SVOD( pFIID IN NUMBER,
                                  pCode IN VARCHAR2,
                                  pWhereQuery IN VARCHAR2 default '') RETURN VARCHAR2
  AS
    v_Accounts VARCHAR2(2048) := '';
  BEGIN
     EXECUTE IMMEDIATE(
        ' select LISTAGG(substr(t_Account, 0, 5) ||''-'' ||
                 substr(t_Account, 6, 3) || ''-'' ||
                 substr(t_Account, 9, 1) || ''-'' ||
                 substr(t_Account, 10, 4) || ''-'' ||
                 substr(t_Account, 14),'', '')  WITHIN GROUP (ORDER BY t_Account)
             from ( SELECT acd.t_Account, acd.t_Currency, acd.t_Chapter
                         FROM dmcaccdoc_dbt acd, DMCCATEG_DBT cat, dmctempl_dbt tpl
                        WHERE acd.t_FIID = '||pFIID||'
                          AND acd.t_Chapter = 1
                          AND acd.t_CatID = cat.t_ID
                          AND cat.T_CODE in ('||pCode||')'||'
                          AND cat.T_LevelType = 1
                          AND tpl.t_CatID = cat.t_ID
                          AND tpl.t_Number = acd.t_TemplNum '||pWhereQuery||'
                       GROUP BY acd.t_Account, acd.t_Currency, acd.t_Chapter
                      )'
     ) INTO v_Accounts;
     RETURN v_Accounts;
  EXCEPTION WHEN OTHERS THEN
     RETURN '';
  END;

  /*Поиск счетов, параметризованных по сделке с данной ц\б*/
  FUNCTION GetAccountByDEAL_SVOD( pFIID IN NUMBER,
                                  pCode IN VARCHAR2) RETURN VARCHAR2
  AS
    v_Accounts VARCHAR2(2048) := '';
  BEGIN
     EXECUTE IMMEDIATE(
        ' select LISTAGG(substr(t_Account, 0, 5) ||''-'' ||
                 substr(t_Account, 6, 3) || ''-'' ||
                 substr(t_Account, 9, 1) || ''-'' ||
                 substr(t_Account, 10, 4) || ''-'' ||
                 substr(t_Account, 14),'', '')  WITHIN GROUP (ORDER BY t_Account)
             from ( SELECT acd.t_Account, acd.t_Currency, acd.t_Chapter
                         FROM dmcaccdoc_dbt acd, DMCCATEG_DBT cat
                        WHERE acd.t_DocKind in( '||rsb_secur.DL_SECURITYDOC||', '||rsb_secur.DL_SECURLEG||', '||rsb_secur.DL_TICK_ENS_DOC||' )
                          AND acd.t_Chapter = 1
                          AND acd.t_CatID = cat.t_ID
                          AND cat.T_CODE in ('||pCode||')'||'
                          AND cat.T_LevelType = 1
                          AND ((acd.t_DocKind = '||rsb_secur.DL_SECURITYDOC||' and exists( select TICK.T_PFI  from ddl_tick_dbt tick where TICK.T_BOFFICEKIND = ACD.T_DOCKIND and TICK.T_DEALID = ACD.T_DOCID and TICK.T_PFI = '||pFIID||' ))
                                    or  (acd.t_DocKind = '||rsb_secur.DL_SECURLEG||' and exists( select LEG.T_PFI  from ddl_leg_dbt leg where LEG.T_ID = ACD.T_DOCID and LEG.T_PFI = '||pFIID||' ))
                                    or  (acd.t_DocKind = '||rsb_secur.DL_TICK_ENS_DOC||' and exists( select ENS.T_FIID from ddl_tick_ens_dbt ens where ENS.T_ID = ACD.T_DOCID and ENS.T_FIID = '||pFIID||' ))
                                 )
                       GROUP BY acd.t_Account, acd.t_Currency, acd.t_Chapter
                      )'
     ) INTO v_Accounts;
     RETURN v_Accounts;
  EXCEPTION WHEN OTHERS THEN
     RETURN '';
  END;

  -- Категория параметризована по ФИ (MCCATEG_ATTRMASK_FIID)
  FUNCTION IsCatParameterizedByFI( pCode IN VARCHAR2 ) RETURN NUMBER
  AS
    v_IsParamByFI NUMBER := 0;
  BEGIN
    SELECT 1 INTO v_IsParamByFI
      FROM DMCCATEG_DBT cat
     WHERE cat.T_CODE = pCode
       AND cat.T_LEVELTYPE = 1
       AND MOD(cat.t_AttrMask, 2) = 1;
    RETURN v_IsParamByFI;
  EXCEPTION
    WHEN OTHERS THEN RETURN 0;
  END;

  --Получить суммы проводок и счета, параметризованные по ФИ/сделке с данной ц/б
  PROCEDURE GetTrnSumAcc_SVOD( pListTrnSumAcc IN OUT NOCOPY ListTrnSumAcc_t,
                               pIsCreditMain IN NUMBER,
                               pByDeal IN NUMBER, -- по счетам, параметризованным по сделке
                               pPrm IN NUMBER, -- признаки (БОЦБ, Т+3, РЕПО и др.,)
                               pFIID IN NUMBER,
                               pBegDate IN DATE,
                               pEndDate IN DATE,
                               pCode1 IN VARCHAR2, pBalance1 IN VARCHAR2,
                               pCode2 IN VARCHAR2,
                               pWhereQuery IN VARCHAR2 default '')
  IS
    v_FldAccName1 VARCHAR2(18);
    v_FldAccName2 VARCHAR2(18);
    v_FldAccFIID1 VARCHAR2(15);
    v_FldAccFIID2 VARCHAR2(15);
    v_AND VARCHAR2(1024);
    v_AndENS VARCHAR2(256) := '';
    v_And4815 VARCHAR2(256) := '';
    v_IsRepo NUMBER := 0;
    v_SQL    VARCHAR2(30676) ;
  BEGIN
    IF pIsCreditMain = 1 THEN
      v_FldAccName1 := 'T_ACCOUNT_RECEIVER';
      v_FldAccName2 := 'T_ACCOUNT_PAYER';
      v_FldAccFIID1 := 'T_FIID_RECEIVER';
      v_FldAccFIID2 := 'T_FIID_PAYER';
    ELSE
      v_FldAccName1 := 'T_ACCOUNT_PAYER';
      v_FldAccName2 := 'T_ACCOUNT_RECEIVER';
      v_FldAccFIID1 := 'T_FIID_PAYER';
      v_FldAccFIID2 := 'T_FIID_RECEIVER';
    END IF;

    IF pByDeal = 0 THEN
      v_AND := ' AND acd.t_FIID = '||pFIID;
    ELSE
      v_AND := ' AND ( ';

      IF BITAND(pPrm, SVODTRNSUMACC_101) = SVODTRNSUMACC_101 THEN
        IF BITAND(pPrm, SVODTRNSUMACC_101_REPO) = SVODTRNSUMACC_101_REPO THEN
          v_IsRepo := 1;
          IF BITAND(pPrm, SVODTRNSUMACC_101_REPO_ENS) = SVODTRNSUMACC_101_REPO_ENS THEN
            v_AndENS := ' or (acd.t_DocKind = '||rsb_secur.DL_TICK_ENS_DOC||
                           '  and exists( select 1 '||
                                          ' from ddl_tick_ens_dbt ens '||
                                         ' where ENS.T_ID = ACD.T_DOCID '||
                                           ' and ENS.T_FIID = '||pFIID||' )) ';
          END IF;
        END IF;
        IF BITAND(pPrm, SVODTRNSUMACC_101_4815) = SVODTRNSUMACC_101_4815 THEN
          v_And4815 := ' or (acd.t_DocKind = '||rsb_secur.DL_DVDEALT3||
                           ' and exists( select 1 ' ||
                                         ' from ddvndeal_dbt DVNDeal, ddvnfi_dbt nFI ' ||
                                        ' where DVNDeal.t_ID = ACD.T_DOCID '||
                                          ' and nFI.t_DealID = DVNDeal.t_ID '||
                                          ' and nFI.t_FIID = '||pFIID||
                                     ' ) ' ||
                          ' ) ';
        END IF;
        v_AND := v_AND ||
                 ' (acd.t_DocKind = '||(case when v_IsRepo = 1 then rsb_secur.DL_SECURLEG else rsb_secur.DL_SECURITYDOC end)||
                  ' and exists( select 1 ' ||
                                ' from ddl_tick_dbt tick ' ||
                               ' where tick.T_DEALID = rsb_secur.GetDealID(ACD.t_DocKind, ACD.t_DocID) ' ||
                                 ' and tick.T_PFI = '||pFIID||
                                 ' and RSB_SECUR.DealIsRepo(tick.T_DEALID) = '||v_IsRepo||
                            ' ) ' ||
                 ' ) '||v_AndENS ||v_And4815;
      END IF;

      v_AND := v_AND || ' ) ';
    END IF;
    v_SQL := ' with acc_cat1 as (select /*+ ordered use_nl(acd) result_cache */ ' ||
                                             ' distinct acd.t_Currency ' ||
                                             ' ,acd.t_Account ' ||
                                       ' from DMCCATEG_DBT  cat ' ||
                                           ' ,dmcaccdoc_dbt acd ' ||
                                      ' where cat.T_CODE in ('||pCode1||') ' ||
                                        ' and cat.T_LEVELTYPE = 1 ' ||
                                        ' and acd.t_CatID = cat.t_ID ' ||
                                        ' and acd.t_Chapter = 1), ' ||
                            ' acc_cat2 as (select /*+ ordered use_nl(acd) result_cache */ ' ||
                                       ' distinct acd.t_Currency ' ||
                                               ' ,acd.t_Account ' ||
                                         ' from DMCCATEG_DBT  cat ' ||
                                             ' ,dmcaccdoc_dbt acd ' ||
                                        ' where cat.T_CODE in ('||pCode2||') ' ||
                                          ' and cat.T_LEVELTYPE = 1 ' ||
                                          ' and acd.t_CatID = cat.t_ID ' ||
                                          ' and acd.t_Chapter = 1) ' ||
                       'SELECT /*+ leading(acc_cat1) use_nl(trn) */ trn.'||v_FldAccName1||' as t_Account, ' ||
                               ' trn.T_SUM_NATCUR as t_SumRub ' ||
                          ' FROM acc_cat1 ' ||
                          '  join DACCTRN_DBT trn  ' ||
                            '   on acc_cat1.t_Currency  = trn.'||v_FldAccFIID1||' ' ||
                            '  and acc_cat1.t_Account = trn.'||v_FldAccName1||' ' ||
                         ' WHERE trn.t_Chapter = 1 ' ||
                           ' AND trn.t_State = 1 ' ||
                           ' AND trn.t_Date_Carry BETWEEN :pBegDate1 and :pEndDate1 '||
                           ' AND SUBSTR(trn.'||v_FldAccName1||', 1, 5 ) IN ('||pBalance1||') ' ||
                          /* ' AND EXISTS( SELECT 1 ' ||
                                         ' FROM dmcaccdoc_dbt acd, DMCCATEG_DBT cat ' ||
                                        ' WHERE acd.t_Chapter = trn.t_Chapter ' ||
                                          ' AND acd.t_Currency = trn.'||v_FldAccFIID1||' ' ||
                                          ' AND acd.t_Account = trn.'||v_FldAccName1||' ' ||
                                          ' AND cat.T_LEVELTYPE = 1 ' ||
                                          ' AND acd.t_CatID = cat.t_ID ' ||
                                          ' AND cat.T_CODE IN ('||pCode1||') ' ||
                                     ' ) ' || */
                           ' and (trn.'||v_FldAccFIID2||', trn.'||v_FldAccName2||') in (select t_Currency ' ||
                                                                                   ' ,t_Account ' ||
                                                                               ' from acc_cat2) ' ||
                           ' AND case when EXISTS( SELECT 1 ' ||
                                         ' FROM dmcaccdoc_dbt acd, DMCCATEG_DBT cat, dmctempl_dbt tpl ' ||
                                        ' WHERE acd.t_Chapter = trn.t_Chapter ' ||
                                          ' AND acd.t_Currency = trn.'||v_FldAccFIID2||' ' ||
                                          ' AND acd.t_Account = trn.'||v_FldAccName2||' ' ||
                                          v_AND||
                                          ' AND acd.t_CatID = cat.t_ID ' ||
                                          ' AND cat.T_CODE IN ('||pCode2||') ' ||
                                          ' AND cat.T_LEVELTYPE = 1 ' ||
                                          ' AND tpl.t_CatID = cat.t_ID ' ||
                                          ' AND tpl.t_Number = acd.t_TemplNum '||pWhereQuery||
                                     ' ) then 1 else 0 end = 1 ' ||
                        ' UNION ALL ' ||
                        ' SELECT /*+ leading(acc_cat1) use_nl(trn) */ trn.'||v_FldAccName2||' as t_Account, ' ||
                               ' -trn.T_SUM_NATCUR as t_SumRub ' ||
                          ' FROM acc_cat1  ' ||
                          '  join  DACCTRN_DBT trn ' ||
                            '   on acc_cat1.t_Currency  = trn.'||v_FldAccFIID2||' ' ||
                            '  and acc_cat1.t_Account = trn.'||v_FldAccName2||' ' ||
                         ' WHERE trn.t_Chapter = 1 ' ||
                           ' AND trn.t_State = 1 ' ||
                           ' AND trn.t_Date_Carry BETWEEN :pBegDate2 and :pEndDate2 '||
                           ' AND SUBSTR(trn.'||v_FldAccName2||', 1, 5 ) IN ('||pBalance1||') ' ||
                          /* ' AND EXISTS( SELECT 1 ' ||
                                         ' FROM dmcaccdoc_dbt acd, DMCCATEG_DBT cat ' ||
                                        ' WHERE acd.t_Chapter = trn.t_Chapter ' ||
                                          ' AND acd.t_Currency = trn.'||v_FldAccFIID2||' ' ||
                                          ' AND acd.t_Account = trn.'||v_FldAccName2||' ' ||
                                          ' AND cat.T_LEVELTYPE = 1 ' ||
                                          ' AND acd.t_CatID = cat.t_ID ' ||
                                          ' AND cat.T_CODE IN ('||pCode1||') ' ||
                                     ' ) ' || */
                           ' and (trn.'||v_FldAccFIID1||', trn.'||v_FldAccName1||') in (select t_Currency ' ||
                                                                                   ' ,t_Account ' ||
                                                                               ' from acc_cat2) ' ||
                           ' AND case when EXISTS( SELECT 1 ' ||
                                         ' FROM dmcaccdoc_dbt acd, DMCCATEG_DBT cat, dmctempl_dbt tpl ' ||
                                        ' WHERE acd.t_Chapter = trn.t_Chapter ' ||
                                          ' AND acd.t_Currency = trn.'||v_FldAccFIID1||' ' ||
                                          ' AND acd.t_Account = trn.'||v_FldAccName1||' ' ||
                                          v_AND||
                                          ' AND acd.t_CatID = cat.t_ID ' ||
                                          ' AND cat.T_CODE IN ('||pCode2||') ' ||
                                          ' AND CAT.T_LEVELTYPE = 1 ' ||
                                          ' AND tpl.t_CatID = cat.t_ID ' ||
                                          ' AND tpl.t_Number = acd.t_TemplNum '||pWhereQuery||
                                     ' )  then 1 else 0 end = 1 ';
    EXECUTE IMMEDIATE (v_SQL)
       BULK COLLECT INTO pListTrnSumAcc
           USING IN pBegDate, pEndDate, pBegDate, pEndDate;
  EXCEPTION
     WHEN OTHERS THEN 
        it_log.log(p_msg  => 'Error:'||sqlerrm ,
                p_msg_type => it_log.C_MSG_TYPE__ERROR,
                p_msg_clob => v_SQL) ;
  END;

  FUNCTION GetTrnSumAccREPO_SVOD( pFIID IN NUMBER,
                                  pBegDate IN DATE,
                                  pEndDate IN DATE,
                                  pIsCreditMain IN NUMBER,
                                  pCode1 IN VARCHAR2,
                                  pBalance2 IN VARCHAR2 ) RETURN NUMBER
  IS
    v_SumRub NUMBER := 0;
    v_FldAccName1 VARCHAR2(18);
    v_FldAccName2 VARCHAR2(18);
    v_FldAccFIID1 VARCHAR2(15);
    v_FldAccFIID2 VARCHAR2(15);
    v_SQL    VARCHAR2(32600);
  BEGIN
    IF pIsCreditMain = 1 THEN
      v_FldAccName1 := 'T_ACCOUNT_RECEIVER';
      v_FldAccName2 := 'T_ACCOUNT_PAYER';
      v_FldAccFIID1 := 'T_FIID_RECEIVER';
      v_FldAccFIID2 := 'T_FIID_PAYER';
    ELSE
      v_FldAccName1 := 'T_ACCOUNT_PAYER';
      v_FldAccName2 := 'T_ACCOUNT_RECEIVER';
      v_FldAccFIID1 := 'T_FIID_PAYER';
      v_FldAccFIID2 := 'T_FIID_RECEIVER';
    END IF;
    v_SQL := ' with acc_cat1 as (select /*+ ordered use_nl(acd) result_cache */ ' ||
                                             ' distinct acd.t_Currency ' ||
                                             ' ,acd.t_Account ' ||
                                       ' from DMCCATEG_DBT  cat ' ||
                                           ' ,dmcaccdoc_dbt acd ' ||
                                      ' where cat.T_CODE in ('||pCode1||') ' ||
                                        ' and cat.T_LEVELTYPE = 1 ' ||
                                        ' and acd.t_CatID = cat.t_ID ' ||
                                        ' and acd.t_Chapter = 1) ' ||
               ' SELECT NVL(SUM(T_SUM_NATCUR), 0) '||
                          ' FROM ( '||
                                ' SELECT /*+ leading(grdeal) */ NVL(SUM(T_SUM_NATCUR), 0) T_SUM_NATCUR '||
                                  ' FROM ddlgrdeal_dbt grdeal, ddlgrdoc_dbt grdoc, DACCTRN_DBT trn , acc_cat1 '||
                                 ' WHERE grdeal.t_DocKind = '||rsb_secur.DL_SECURITYDOC||
                                   ' AND grdeal.t_FIID = '||pFIID||
                                   ' AND grdeal.t_TemplNum = '||RSI_DLGR.DLGR_TEMPL_PAYMENT2|| -- !!! (возможно потребуются доп.виды)
                                   ' AND grdeal.t_PlanDate BETWEEN :pBegDate1 and :pEndDate1 '||
                                   ' AND grdoc.t_DocKind = 1 '|| -- проводки
                                   ' AND grdoc.t_GrDealID = grdeal.t_ID '||
                                   ' AND trn.t_AccTrnID = grdoc.t_DocID '||
                                   ' AND trn.t_Chapter = 1 '||
                                   ' AND trn.t_State = 1 '||
                                   ' and acc_cat1.t_Currency =  trn.'||v_FldAccFIID1||' '||
                                   ' and acc_cat1.t_Account =  trn.'||v_FldAccName1||' '|| 
                                   --' AND trn.t_Date_Carry = grdeal.t_PlanDate '||
                                   /*' AND EXISTS( SELECT 1 '||
                                                 ' FROM dmcaccdoc_dbt acd, DMCCATEG_DBT cat '||
                                                ' WHERE acd.t_Chapter = trn.t_Chapter '||
                                                  ' AND acd.t_Currency = trn.'||v_FldAccFIID1||' '||
                                                  ' AND acd.t_Account = trn.'||v_FldAccName1||' '||
                                                  ' AND acd.t_CatID = cat.t_ID '||
                                                  ' AND cat.T_CODE IN ('||pCode1||') '||
                                                  ' AND cat.T_LEVELTYPE = 1 '||
                                             ' ) '|| */
                                   ' AND case when EXISTS( SELECT 1 '||
                                                 ' FROM dmcaccdoc_dbt acd, DMCCATEG_DBT cat '||
                                                ' WHERE acd.t_Chapter = trn.t_Chapter '||
                                                  ' AND acd.t_Currency = trn.'||v_FldAccFIID2||' '||
                                                  ' AND acd.t_Account = trn.'||v_FldAccName2||' '||
                                                  ' AND acd.t_CatID = cat.t_ID '||
                                                  ' AND SUBSTR(trn.'||v_FldAccName2||', 1, 5 ) IN ('||pBalance2||') '||
                                                  ' AND cat.T_LEVELTYPE = 1 '||
                                             ' ) then 1 else 0 end = 1 '||
                                ' UNION ALL '||
                                ' SELECT /*+ leading(acc_cat1) use_nl(trn) */ NVL(SUM(T_SUM_NATCUR), 0) T_SUM_NATCUR '||
                                  ' FROM doprdocs_dbt docs, doproper_dbt opr, ddl_tick_dbt tick, dacctrn_dbt trn, acc_cat1 '||
                                 ' WHERE docs.t_DocKind = 1 '|| -- проводки
                                   ' AND docs.t_ServDocKind = '||rsb_secur.DL_GET_INCOME||
                                   ' AND opr.t_DocKind = '||rsb_secur.DL_SECURITYDOC||
                                   ' AND TO_NUMBER(opr.t_DocumentID) = tick.t_dealid '||
                                   ' AND docs.t_ID_Operation = opr.t_ID_Operation '||
                                   ' AND tick.t_PFI = '||pFIID||
                                   ' AND RSB_SECUR.IsRepo( RSB_SECUR.get_OperationGroup( RSB_SECUR.get_OperSysTypes( tick.t_DealType, tick.t_BofficeKind ) ) ) = 1 '||
                                   ' AND trn.t_AccTrnID = docs.t_AccTrnID '||
                                   ' AND trn.t_Chapter = 1 '||
                                   ' AND trn.t_State = 1 '||
                                   ' AND trn.t_Date_Carry BETWEEN :pBegDate2 and :pEndDate2 '||
                                   ' and acc_cat1.t_Currency =  trn.'||v_FldAccFIID1||' '||
                                   ' and acc_cat1.t_Account =  trn.'||v_FldAccName1||' '|| 
                                  /* ' AND EXISTS( SELECT 1 '||
                                                 ' FROM dmcaccdoc_dbt acd, DMCCATEG_DBT cat '||
                                                ' WHERE acd.t_Chapter = trn.t_Chapter '||
                                                  ' AND acd.t_Currency = trn.'||v_FldAccFIID1||' '||
                                                  ' AND acd.t_Account = trn.'||v_FldAccName1||' '||
                                                  ' AND acd.t_CatID = cat.t_ID '||
                                                  ' AND cat.T_CODE IN ('||pCode1||') '||
                                                  ' AND cat.T_LEVELTYPE = 1 '||
                                             ' ) '|| */
                                   ' AND case when EXISTS( SELECT 1 '||
                                                 ' FROM dmcaccdoc_dbt acd, DMCCATEG_DBT cat '||
                                                ' WHERE acd.t_Chapter = trn.t_Chapter '||
                                                  ' AND acd.t_Currency = trn.'||v_FldAccFIID2||' '||
                                                  ' AND acd.t_Account = trn.'||v_FldAccName2||' '||
                                                  ' AND acd.t_CatID = cat.t_ID '||
                                                  ' AND SUBSTR(trn.'||v_FldAccName2||', 1, 5 ) IN ('||pBalance2||') '||
                                                  ' AND cat.T_LEVELTYPE = 1 '||
                                             ' )  then 1 else 0 end = 1 '||
                                --Если найдены аналогичные проводки, но со счетами по дебету и кредиту "наоборот", то суммы в них учитываются со знаком минус
                                ' UNION ALL '||
                                ' SELECT /*+ leading(grdeal) */ NVL(SUM(-T_SUM_NATCUR), 0) T_SUM_NATCUR '||
                                  ' FROM ddlgrdeal_dbt grdeal, ddlgrdoc_dbt grdoc, DACCTRN_DBT trn, acc_cat1  '||
                                 ' WHERE grdeal.t_DocKind = '||rsb_secur.DL_SECURITYDOC||
                                   ' AND grdeal.t_FIID = '||pFIID||
                                   ' AND grdeal.t_TemplNum = '||RSI_DLGR.DLGR_TEMPL_PAYMENT2|| -- !!! (возможно потребуются доп.виды)
                                   ' AND grdeal.t_PlanDate BETWEEN :pBegDate3 and :pEndDate3 '||
                                   ' AND grdoc.t_DocKind = 1 '|| -- проводки
                                   ' AND grdoc.t_GrDealID = grdeal.t_ID '||
                                   ' AND trn.t_AccTrnID = grdoc.t_DocID '||
                                   ' AND trn.t_Chapter = 1 '||
                                   ' AND trn.t_State = 1 '||
                                   ' and acc_cat1.t_Currency =  trn.'||v_FldAccFIID2||' '||
                                   ' and acc_cat1.t_Account =  trn.'||v_FldAccName2||' '|| 
                                   --' AND trn.t_Date_Carry = grdeal.t_PlanDate '||
                                 /*  ' AND EXISTS( SELECT 1 '||
                                                 ' FROM dmcaccdoc_dbt acd, DMCCATEG_DBT cat '||
                                                ' WHERE acd.t_Chapter = trn.t_Chapter '||
                                                  ' AND acd.t_Currency = trn.'||v_FldAccFIID2||' '||
                                                  ' AND acd.t_Account = trn.'||v_FldAccName2||' '||
                                                  ' AND acd.t_CatID = cat.t_ID '||
                                                  ' AND cat.T_CODE IN ('||pCode1||') '||
                                                  ' AND cat.T_LEVELTYPE = 1 '||
                                             ' ) '|| */
                                   ' AND case when EXISTS( SELECT 1 '||
                                                 ' FROM dmcaccdoc_dbt acd, DMCCATEG_DBT cat '||
                                                ' WHERE acd.t_Chapter = trn.t_Chapter '||
                                                  ' AND acd.t_Currency = trn.'||v_FldAccFIID1||' '||
                                                  ' AND acd.t_Account = trn.'||v_FldAccName1||' '||
                                                  ' AND acd.t_CatID = cat.t_ID '||
                                                  ' AND SUBSTR(trn.'||v_FldAccName1||', 1, 5 ) IN ('||pBalance2||') '||
                                                  ' AND cat.T_LEVELTYPE = 1 '||
                                             ' ) then 1 else 0 end = 1'||
                                ' UNION ALL '||
                                ' SELECT /*+ leading(acc_cat1) use_nl(trn) */ NVL(SUM(-T_SUM_NATCUR), 0) T_SUM_NATCUR '||
                                  ' FROM doprdocs_dbt docs, doproper_dbt opr, ddl_tick_dbt tick, dacctrn_dbt trn, acc_cat1 '||
                                 ' WHERE docs.t_DocKind = 1 '|| -- проводки
                                   ' AND docs.t_ServDocKind = '||rsb_secur.DL_GET_INCOME||
                                   ' AND opr.t_DocKind = '||rsb_secur.DL_SECURITYDOC||
                                   ' AND TO_NUMBER(opr.t_DocumentID) = tick.t_dealid '||
                                   ' AND docs.t_ID_Operation = opr.t_ID_Operation '||
                                   ' AND tick.t_PFI = '||pFIID||
                                   ' AND RSB_SECUR.IsRepo( RSB_SECUR.get_OperationGroup( RSB_SECUR.get_OperSysTypes( tick.t_DealType, tick.t_BofficeKind ) ) ) = 1 '||
                                   ' AND docs.t_AccTrnID = trn.t_AccTrnID '||
                                   ' AND trn.t_Chapter = 1 '||
                                   ' AND trn.t_State = 1 '||
                                   ' AND trn.t_Date_Carry BETWEEN :pBegDate4 and :pEndDate4 '||
                                   ' and acc_cat1.t_Currency =  trn.'||v_FldAccFIID2||' '||
                                   ' and acc_cat1.t_Account =  trn.'||v_FldAccName2||' '|| 
                                 /*  ' AND EXISTS( SELECT 1 '||
                                                 ' FROM dmcaccdoc_dbt acd, DMCCATEG_DBT cat '||
                                                ' WHERE acd.t_Chapter = trn.t_Chapter '||
                                                  ' AND acd.t_Currency = trn.'||v_FldAccFIID2||' '||
                                                  ' AND acd.t_Account = trn.'||v_FldAccName2||' '||
                                                  ' AND acd.t_CatID = cat.t_ID '||
                                                  ' AND cat.T_CODE IN ('||pCode1||') '||
                                                  ' AND cat.T_LEVELTYPE = 1 '||
                                             ' ) '|| */
                                   ' AND case when EXISTS( SELECT 1 '||
                                                 ' FROM dmcaccdoc_dbt acd, DMCCATEG_DBT cat '||
                                                ' WHERE acd.t_Chapter = trn.t_Chapter '||
                                                  ' AND acd.t_Currency = trn.'||v_FldAccFIID1||' '||
                                                  ' AND acd.t_Account = trn.'||v_FldAccName1||' '||
                                                  ' AND acd.t_CatID = cat.t_ID '||
                                                  ' AND SUBSTR(trn.'||v_FldAccName1||', 1, 5 ) IN ('||pBalance2||') '||
                                                  ' AND cat.T_LEVELTYPE = 1 '||
                                             ' ) then 1 else 0 end = 1 '||
                               ' ) ' ;
    EXECUTE IMMEDIATE (v_SQL)
       INTO v_SumRub
       USING IN pBegDate, pEndDate, pBegDate, pEndDate, pBegDate, pEndDate, pBegDate, pEndDate;
    RETURN v_SumRub;
  EXCEPTION
    WHEN OTHERS THEN 
      it_log.log(p_msg      => 'Error:'||sqlerrm ,
                 p_msg_type => it_log.C_MSG_TYPE__ERROR,
                 p_msg_clob => v_SQL);
      RETURN 0;
  END;

  --Б21 СВОД
  FUNCTION GetB21_SVOD( pFIID IN NUMBER,
                        pBegDate IN DATE,
                        pEndDate IN DATE,
                        RootAvrKind IN NUMBER ) RETURN NUMBER
  IS
    v_B21_SVOD NUMBER := 0;
  BEGIN
    SELECT NVL(SUM(trn.T_SUM_NATCUR), 0)
      INTO v_B21_SVOD
      FROM ddlgrdeal_dbt grdeal, ddlgrdoc_dbt grdoc, DACCTRN_DBT trn
     WHERE grdeal.t_DocKind = rsb_secur.DL_SECURITYDOC
       AND grdeal.t_FIID = pFIID
       AND grdeal.t_TemplNum = RSI_DLGR.DLGR_TEMPL_RECDELIVERY  -- !!! (возможно потребуются доп.виды)
       AND grdeal.t_PlanDate BETWEEN pBegDate and pEndDate
       AND grdoc.t_DocKind = 1 -- проводки
       AND grdoc.t_GrDealID = grdeal.t_ID
       AND trn.t_AccTrnID = grdoc.t_DocID
       AND trn.t_Chapter = 1
       AND trn.t_State = 1
       AND trn.t_Date_Carry = grdeal.t_PlanDate
       AND ((EXISTS( SELECT 1
                       FROM dmcaccdoc_dbt acd, DMCCATEG_DBT cat
                      WHERE acd.t_Chapter = trn.t_Chapter
                        AND acd.t_Currency = trn.t_FIID_PAYER
                        AND acd.t_CatID = cat.t_ID
                        AND cat.T_CODE IN ('-КВ, затраты, ц/б')
                        AND cat.T_LEVELTYPE = 1
                        AND acd.t_Account = trn.T_ACCOUNT_PAYER
                   ) AND
             EXISTS( SELECT 1
                       FROM dmcaccdoc_dbt acd, DMCCATEG_DBT cat
                      WHERE acd.t_Chapter = trn.t_Chapter
                        AND acd.t_Currency = trn.t_FIID_RECEIVER
                        AND acd.t_CatID = cat.t_ID
                        AND (cat.T_CODE IN ('+Биржа', '-Биржа')
                             OR (    RootAvrKind = RSI_RSB_FIINSTR.AVOIRKIND_BOND
                                 AND cat.t_CODE = 'Предв.затраты, ц/б'))
                        AND cat.T_LEVELTYPE = 1
                        AND acd.t_Account = trn.T_ACCOUNT_RECEIVER
                   )
            )
            OR
            (EXISTS( SELECT 1
                       FROM dmcaccdoc_dbt acd, DMCCATEG_DBT cat
                      WHERE acd.t_Chapter = trn.t_Chapter
                        AND acd.t_Currency = trn.t_FIID_PAYER
                        AND acd.t_CatID = cat.t_ID
                        AND cat.t_CODE IN ('Прочие расходы')
                        AND cat.t_LEVELTYPE = 1
                        AND acd.t_Account = trn.t_ACCOUNT_PAYER
                    ) AND
             EXISTS( SELECT 1
                       FROM dmcaccdoc_dbt acd, dmccateg_dbt cat
                      WHERE acd.t_Chapter = trn.t_Chapter
                        AND acd.t_Currency = trn.t_FIID_RECEIVER
                        AND acd.t_CatID = cat.t_ID
                        AND cat.t_CODE IN ('Предв.затраты, ц/б')
                        AND cat.t_LEVELTYPE = 1
                        AND acd.t_Account = trn.t_ACCOUNT_RECEIVER)
            )
           );
    RETURN v_B21_SVOD;
  EXCEPTION
    WHEN OTHERS THEN RETURN 0;
  END;

  --Заполнить поля по счетам и остаткам
  PROCEDURE SetAccFlds_SVOD(pnurec IN OUT NOCOPY DNUSVODREP_DBT%rowtype, pBegDate IN DATE, pEndDate IN DATE)
  IS
    v_ListTrn ListTrnSumAcc_t;
    v_ListAcc ListAcc_t;
    v_ListAccCheck ListAccCheck_t;
    v_ACC_REPREST VARCHAR(2048) := '';
    v_ACC_REPCOUPB VARCHAR(2048) := '';
  BEGIN
    --Б1/С1
    pnurec.t_BalDoh_Rest := 0;
    pnurec.t_AccDoh_Rest := '';
    --Б2/С2
    pnurec.t_BalRash_Rest := 0;
    pnurec.t_AccRash_Rest := '';
    --Б21
    pnurec.t_B21_SVOD := 0;

    -- Если не старый выпуск из глобальной операции
    IF pnurec.t_DGO = UnknownDate THEN
      --Б1/С1
      GetTrnSumAcc_SVOD( v_ListTrn, 1, 0, 0, pnurec.t_FIID, pBegDate, pEndDate,
                         '''+Маржа, ц/б''', '''70601''',
                       /*GAA:506871, old: '''+ПО ДК'', ''-ПО ДК'', ''+ПО ДК 2014'', ''-ПО ДК 2014'', ''Реализация, ц/б'''*/
                       '''+ПО ДК'', ''-ПО ДК'', ''+ПО ДК 2014'', ''-ПО ДК 2014'', ''Реализация, ц/б'', ''+Переоценка, ц/б СССД_ЦБ'',''-Переоценка, ц/б СССД_ЦБ'', ''Предв.затраты, ц/б'', ''-Корректировка, ц/б''' /*GAA:523373, new:'-Корректировка, ц/б'*/
                       );
      IF v_ListTrn.COUNT > 0 THEN
        pnurec.t_BalDoh_Rest := v_ListTrn(1).t_Sum;
        pnurec.t_AccDoh_Rest := GetFormattedAccount(v_ListTrn(1).t_Account);
        v_ListAccCheck(v_ListTrn(1).t_Account) := v_ListTrn(1).t_Account; -- для проверки на уникальность счетов
        FOR i IN 2 .. v_ListTrn.LAST
        LOOP
          pnurec.t_BalDoh_Rest := pnurec.t_BalDoh_Rest + v_ListTrn(i).t_Sum;
          IF v_ListAccCheck.EXISTS(v_ListTrn(i).t_Account) = FALSE THEN -- проверка на уникальность счетов
            pnurec.t_AccDoh_Rest := pnurec.t_AccDoh_Rest || ', ' || GetFormattedAccount(v_ListTrn(i).t_Account);
            v_ListAccCheck(v_ListTrn(i).t_Account) := v_ListTrn(i).t_Account; -- для проверки на уникальность счетов
          END IF;
        END LOOP;
        v_ListTrn.delete;
        v_ListAccCheck.delete;
      END IF;
      GetTrnSumAcc_SVOD( v_ListTrn, 1, 1, SVODTRNSUMACC_101_REPO, pnurec.t_FIID, pBegDate, pEndDate,
                         '''+Маржа, ц/б''', '''70601''',
                         '''-ОД'''
                       );
      IF v_ListTrn.COUNT > 0 THEN
        FOR i IN v_ListTrn.FIRST .. v_ListTrn.LAST
        LOOP
          pnurec.t_BalDoh_Rest := pnurec.t_BalDoh_Rest + v_ListTrn(i).t_Sum;
          IF v_ListAccCheck.EXISTS(v_ListTrn(i).t_Account) = FALSE THEN -- проверка на уникальность счетов
            pnurec.t_AccDoh_Rest := pnurec.t_AccDoh_Rest || ', ' || GetFormattedAccount(v_ListTrn(i).t_Account);
            v_ListAccCheck(v_ListTrn(i).t_Account) := v_ListTrn(i).t_Account; -- для проверки на уникальность счетов
          END IF;
        END LOOP;
        v_ListTrn.delete;
        v_ListAccCheck.delete;
      END IF;

      --Б2/С2
      GetTrnSumAcc_SVOD( v_ListTrn, 0, 0, 0, pnurec.t_FIID, pBegDate, pEndDate,
                         '''-Маржа, ц/б''', '''70606''',
                       /*GAA:506871, old:'''+ПО ДК'', ''-ПО ДК'', ''+ПО ДК 2014'', ''-ПО ДК 2014'', ''Реализация, ц/б'''*/
                       '''+ПО ДК'', ''-ПО ДК'', ''+ПО ДК 2014'', ''-ПО ДК 2014'', ''Реализация, ц/б'', ''+Переоценка, ц/б СССД_ЦБ'',''-Переоценка, ц/б СССД_ЦБ'', ''Предв.затраты, ц/б'',''+Корректировка, ц/б''' /*GAA:523373, new:'-Корректировка, ц/б'*/                       
                       );
      IF v_ListTrn.COUNT > 0 THEN
        pnurec.t_BalRash_Rest := v_ListTrn(1).t_Sum;
        pnurec.t_AccRash_Rest := GetFormattedAccount(v_ListTrn(1).t_Account);
        v_ListAccCheck(v_ListTrn(1).t_Account) := v_ListTrn(1).t_Account; -- для проверки на уникальность счетов
        FOR i IN 2 .. v_ListTrn.LAST
        LOOP
          pnurec.t_BalRash_Rest := pnurec.t_BalRash_Rest + v_ListTrn(i).t_Sum;
          IF v_ListAccCheck.EXISTS(v_ListTrn(i).t_Account) = FALSE THEN -- проверка на уникальность счетов
            pnurec.t_AccRash_Rest := pnurec.t_AccRash_Rest || ', ' || GetFormattedAccount(v_ListTrn(i).t_Account);
            v_ListAccCheck(v_ListTrn(i).t_Account) := v_ListTrn(i).t_Account; -- для проверки на уникальность счетов
          END IF;
        END LOOP;
        v_ListTrn.delete;
        v_ListAccCheck.delete;
      END IF;
      GetTrnSumAcc_SVOD( v_ListTrn, 0, 1, SVODTRNSUMACC_101_REPO, pnurec.t_FIID, pBegDate, pEndDate,
                         '''-Маржа, ц/б''', '''70606''',
                         '''-ОД'''
                       );
      IF v_ListTrn.COUNT > 0 THEN
        FOR i IN v_ListTrn.FIRST .. v_ListTrn.LAST
        LOOP
          pnurec.t_BalRash_Rest := pnurec.t_BalRash_Rest + v_ListTrn(i).t_Sum;
          IF v_ListAccCheck.EXISTS(v_ListTrn(i).t_Account) = FALSE THEN -- проверка на уникальность счетов
            pnurec.t_AccRash_Rest := pnurec.t_AccRash_Rest || ', ' || GetFormattedAccount(v_ListTrn(i).t_Account);
            v_ListAccCheck(v_ListTrn(i).t_Account) := v_ListTrn(i).t_Account; -- для проверки на уникальность счетов
          END IF;
        END LOOP;
        v_ListTrn.delete;
        v_ListAccCheck.delete;
      END IF;

      --Б21
      pnurec.t_B21_SVOD := GetB21_SVOD(pnurec.t_FIID, pBegDate, pEndDate, pnurec.t_RootAvrKind);
    END IF;

    --Б3/С3
    pnurec.t_BalDoh_Coup := 0;
    pnurec.t_AccDoh_Coup := '';
    GetTrnSumAcc_SVOD( v_ListTrn, 1, 0, 0, pnurec.t_FIID, pBegDate, pEndDate,
                       '''+%ДЦ/б''', '''70601''',
                       '''Начисл.ПДД, ц/б''',
                       'AND '||Where_AccEqByParm(LLCLASS_KIND_ACC_PDD, 1)
                     );
    IF v_ListTrn.COUNT > 0 THEN
      pnurec.t_BalDoh_Coup := v_ListTrn(1).t_Sum;
      pnurec.t_AccDoh_Coup := GetFormattedAccount(v_ListTrn(1).t_Account);
      v_ListAccCheck(v_ListTrn(1).t_Account) := v_ListTrn(1).t_Account; -- для проверки на уникальность счетов
      FOR i IN 2 .. v_ListTrn.LAST
      LOOP
        pnurec.t_BalDoh_Coup := pnurec.t_BalDoh_Coup + v_ListTrn(i).t_Sum;
        IF v_ListAccCheck.EXISTS(v_ListTrn(i).t_Account) = FALSE THEN -- проверка на уникальность счетов
          pnurec.t_AccDoh_Coup := pnurec.t_AccDoh_Coup || ', ' || GetFormattedAccount(v_ListTrn(i).t_Account);
          v_ListAccCheck(v_ListTrn(i).t_Account) := v_ListTrn(i).t_Account; -- для проверки на уникальность счетов
        END IF;
      END LOOP;
      v_ListTrn.delete;
      v_ListAccCheck.delete;
    END IF;

    --Б4/С4
    pnurec.t_BalDoh_Disk := 0;
    pnurec.t_AccDoh_Disk := '';
    GetTrnSumAcc_SVOD( v_ListTrn, 1, 0, 0, pnurec.t_FIID, pBegDate, pEndDate,
                       '''+%ДЦ/б''', '''70601''',
                       '''Начисл.ПДД, ц/б''',
                       'AND '||Where_AccEqByParm(LLCLASS_KIND_ACC_PDD, 2)
                     );
    IF v_ListTrn.COUNT > 0 THEN
      pnurec.t_BalDoh_Disk := v_ListTrn(1).t_Sum;
      pnurec.t_AccDoh_Disk := GetFormattedAccount(v_ListTrn(1).t_Account);
      v_ListAccCheck(v_ListTrn(1).t_Account) := v_ListTrn(1).t_Account; -- для проверки на уникальность счетов
      FOR i IN 2 .. v_ListTrn.LAST
      LOOP
        pnurec.t_BalDoh_Disk := pnurec.t_BalDoh_Disk + v_ListTrn(i).t_Sum;
        IF v_ListAccCheck.EXISTS(v_ListTrn(i).t_Account) = FALSE THEN -- проверка на уникальность счетов
          pnurec.t_AccDoh_Disk := pnurec.t_AccDoh_Disk || ', ' || GetFormattedAccount(v_ListTrn(i).t_Account);
          v_ListAccCheck(v_ListTrn(i).t_Account) := v_ListTrn(i).t_Account; -- для проверки на уникальность счетов
        END IF;
      END LOOP;
      v_ListTrn.delete;
      v_ListAccCheck.delete;
    END IF;

    --Б5/С5
    pnurec.t_B5_SVOD := 0;
    pnurec.t_S5_SVOD := '';
    GetTrnSumAcc_SVOD( v_ListTrn, 0, 0, 0, pnurec.t_FIID, pBegDate, pEndDate,
                       '''-Премия, ц/б''', '''70606''',
                       '''Премия, ц/б'''
                     );
    IF v_ListTrn.COUNT > 0 THEN
      pnurec.t_B5_SVOD := v_ListTrn(1).t_Sum;
      pnurec.t_S5_SVOD := GetFormattedAccount(v_ListTrn(1).t_Account);
      v_ListAccCheck(v_ListTrn(1).t_Account) := v_ListTrn(1).t_Account; -- для проверки на уникальность счетов
      FOR i IN 2 .. v_ListTrn.LAST
      LOOP
        pnurec.t_B5_SVOD := pnurec.t_B5_SVOD + v_ListTrn(i).t_Sum;
        IF v_ListAccCheck.EXISTS(v_ListTrn(i).t_Account) = FALSE THEN -- проверка на уникальность счетов
          pnurec.t_S5_SVOD := pnurec.t_S5_SVOD || ', ' || GetFormattedAccount(v_ListTrn(i).t_Account);
          v_ListAccCheck(v_ListTrn(i).t_Account) := v_ListTrn(i).t_Account; -- для проверки на уникальность счетов
        END IF;
      END LOOP;
      v_ListTrn.delete;
      v_ListAccCheck.delete;
    END IF;

    --Б6
    pnurec.t_DohPrcRep := GetTrnSumAccREPO_SVOD( pnurec.t_FIID, pBegDate, pEndDate, 1, '''+%Пкр''', '''47427''' );

    --Б7
    pnurec.t_RashPrcRep := GetTrnSumAccREPO_SVOD( pnurec.t_FIID, pBegDate, pEndDate, 0, '''-%Пкр''', '''47426''' );

    --Б8
    pnurec.t_Doh_Deriv := 0;
    GetTrnSumAcc_SVOD( v_ListTrn, 1, 1, SVODTRNSUMACC_101_4815, pnurec.t_FIID, pBegDate, pEndDate,
                       '''Доходы ПФИ''', '''70613''',
                       '''+ПФИ'', ''-ПФИ'''
                     );
    IF v_ListTrn.COUNT > 0 THEN
      FOR i IN v_ListTrn.FIRST .. v_ListTrn.LAST
      LOOP
        pnurec.t_Doh_Deriv := pnurec.t_Doh_Deriv + v_ListTrn(i).t_Sum;
      END LOOP;
      v_ListTrn.delete;
    END IF;

    --Б9
    pnurec.t_Rash_Deriv := 0;
    GetTrnSumAcc_SVOD( v_ListTrn, 0, 1, SVODTRNSUMACC_101_4815, pnurec.t_FIID, pBegDate, pEndDate,
                       '''Расходы ПФИ''', '''70614''',
                       '''+ПФИ'', ''-ПФИ'''
                     );
    IF v_ListTrn.COUNT > 0 THEN
      FOR i IN v_ListTrn.FIRST .. v_ListTrn.LAST
      LOOP
        pnurec.t_Rash_Deriv := pnurec.t_Rash_Deriv + v_ListTrn(i).t_Sum;
      END LOOP;
      v_ListTrn.delete;
    END IF;

    --Б10
    pnurec.t_PlusCurrPereoc := 0;
    GetTrnSumAcc_SVOD( v_ListTrn, 1, 0, 0, pnurec.t_FIID, pBegDate, pEndDate,
                       '''+ПереоценкаА''', '''70603''',
                       '''Наш портфель ц/б'', ''Начисл.ПДД, ц/б'', ''Премия, ц/б'', ''Уплаченный НКД'''
                     );
    IF v_ListTrn.COUNT > 0 THEN
      FOR i IN v_ListTrn.FIRST .. v_ListTrn.LAST
      LOOP
        pnurec.t_PlusCurrPereoc := pnurec.t_PlusCurrPereoc + v_ListTrn(i).t_Sum;
      END LOOP;
      v_ListTrn.delete;
    END IF;
    GetTrnSumAcc_SVOD( v_ListTrn, 1, 1, SVODTRNSUMACC_101_REPO_ENS, pnurec.t_FIID, pBegDate, pEndDate,
                       '''+ПереоценкаА''', '''70603''',
                       '''Ц/б, БПП'', ''Уплаченный НКД, БПП'''
                     );
    IF v_ListTrn.COUNT > 0 THEN
      FOR i IN v_ListTrn.FIRST .. v_ListTrn.LAST
      LOOP
        pnurec.t_PlusCurrPereoc := pnurec.t_PlusCurrPereoc + v_ListTrn(i).t_Sum;
      END LOOP;
      v_ListTrn.delete;
    END IF;

    --Б11
    pnurec.t_MinusCurrPereoc := 0;
    GetTrnSumAcc_SVOD( v_ListTrn, 0, 0, 0, pnurec.t_FIID, pBegDate, pEndDate,
                       '''-ПереоценкаА''', '''70608''',
                       '''Наш портфель ц/б'', ''Начисл.ПДД, ц/б'', ''Премия, ц/б'', ''Уплаченный НКД'''
                     );
    IF v_ListTrn.COUNT > 0 THEN
      FOR i IN v_ListTrn.FIRST .. v_ListTrn.LAST
      LOOP
        pnurec.t_MinusCurrPereoc := pnurec.t_MinusCurrPereoc + v_ListTrn(i).t_Sum;
      END LOOP;
      v_ListTrn.delete;
    END IF;
    GetTrnSumAcc_SVOD( v_ListTrn, 0, 1, SVODTRNSUMACC_101_REPO_ENS, pnurec.t_FIID, pBegDate, pEndDate,
                       '''-ПереоценкаА''', '''70608''',
                       '''Ц/б, БПП'', ''Уплаченный НКД, БПП'''
                     );
    IF v_ListTrn.COUNT > 0 THEN
      FOR i IN v_ListTrn.FIRST .. v_ListTrn.LAST
      LOOP
        pnurec.t_MinusCurrPereoc := pnurec.t_MinusCurrPereoc + v_ListTrn(i).t_Sum;
      END LOOP;
      v_ListTrn.delete;
    END IF;

    --Б12/С6
    pnurec.t_DohPerMarkt := 0;
    pnurec.t_AccDoh_Pereoc := '';
    -- в дистрибутиве нет параметра КУ - выпуск, настройка только для РСХБ
    IF IsCatParameterizedByFI('+МаржаП, ц/б') = 1 THEN
      GetAccountByParm( v_ListAcc, pnurec.t_FIID,
                        '''+МаржаП, ц/б'''
                      );
      IF v_ListAcc.COUNT > 0 THEN
        pnurec.t_DohPerMarkt := GetRestAcc(v_ListAcc(1).t_Account, v_ListAcc(1).t_Currency, v_ListAcc(1).t_Chapter, pEndDate);
        pnurec.t_AccDoh_Pereoc := GetFormattedAccount(v_ListAcc(1).t_Account);
        FOR i IN 2 .. v_ListAcc.LAST
        LOOP
          pnurec.t_DohPerMarkt := pnurec.t_DohPerMarkt + GetRestAcc(v_ListAcc(i).t_Account, v_ListAcc(i).t_Currency, v_ListAcc(i).t_Chapter, pEndDate);
          pnurec.t_AccDoh_Pereoc := pnurec.t_AccDoh_Pereoc || ', ' || GetFormattedAccount(v_ListAcc(1).t_Account);
        END LOOP;
        v_ListAcc.delete;
      END IF;
    END IF;

    --Б13/С7
    pnurec.t_RashPerMarkt := 0;
    pnurec.t_AccRash_Pereoc := '';
    -- в дистрибутиве нет параметра КУ - выпуск, настройка только для РСХБ
    IF IsCatParameterizedByFI('-МаржаП, ц/б') = 1 THEN
      GetAccountByParm( v_ListAcc, pnurec.t_FIID,
                        '''-МаржаП, ц/б'''
                      );
      IF v_ListAcc.COUNT > 0 THEN
        pnurec.t_RashPerMarkt := GetRestAcc(v_ListAcc(1).t_Account, v_ListAcc(1).t_Currency, v_ListAcc(1).t_Chapter, pEndDate);
        pnurec.t_AccRash_Pereoc := GetFormattedAccount(v_ListAcc(1).t_Account);
        FOR i IN 2 .. v_ListAcc.LAST
        LOOP
          pnurec.t_RashPerMarkt := pnurec.t_RashPerMarkt + GetRestAcc(v_ListAcc(i).t_Account, v_ListAcc(i).t_Currency, v_ListAcc(i).t_Chapter, pEndDate);
          pnurec.t_AccRash_Pereoc := pnurec.t_AccRash_Pereoc || ', ' || GetFormattedAccount(v_ListAcc(1).t_Account);
        END LOOP;
        v_ListAcc.delete;
      END IF;
    END IF;

    --Б14/С8
    pnurec.t_B14_SVOD := 0;
    pnurec.t_S8_SVOD := '';
    GetTrnSumAcc_SVOD( v_ListTrn, 1, 0, 0, pnurec.t_FIID, pBegDate, pEndDate,
                       '''+КорЭПС_%, ц/б''', '''70601''',
                       '''+Корректировка, ц/б'', ''-Корректировка, ц/б'''
                     );
    IF v_ListTrn.COUNT > 0 THEN
      pnurec.t_B14_SVOD := v_ListTrn(1).t_Sum;
      pnurec.t_S8_SVOD := GetFormattedAccount(v_ListTrn(1).t_Account);
      v_ListAccCheck(v_ListTrn(1).t_Account) := v_ListTrn(1).t_Account; -- для проверки на уникальность счетов
      FOR i IN 2 .. v_ListTrn.LAST
      LOOP
        pnurec.t_B14_SVOD := pnurec.t_B14_SVOD + v_ListTrn(i).t_Sum;
        IF v_ListAccCheck.EXISTS(v_ListTrn(i).t_Account) = FALSE THEN -- проверка на уникальность счетов
          pnurec.t_S8_SVOD := pnurec.t_S8_SVOD || ', ' || GetFormattedAccount(v_ListTrn(i).t_Account);
          v_ListAccCheck(v_ListTrn(i).t_Account) := v_ListTrn(i).t_Account; -- для проверки на уникальность счетов
        END IF;
      END LOOP;
      v_ListTrn.delete;
      v_ListAccCheck.delete;
    END IF;

    --Б15/С9
    pnurec.t_B15_SVOD := 0;
    pnurec.t_S9_SVOD := '';
    GetTrnSumAcc_SVOD( v_ListTrn, 0, 0, 0, pnurec.t_FIID, pBegDate, pEndDate,
                       '''-КорЭПС_%, ц/б''', '''70606''',
                       '''+Корректировка, ц/б'', ''-Корректировка, ц/б'''
                     );
    IF v_ListTrn.COUNT > 0 THEN
      pnurec.t_B15_SVOD := v_ListTrn(1).t_Sum;
      pnurec.t_S9_SVOD := GetFormattedAccount(v_ListTrn(1).t_Account);
      v_ListAccCheck(v_ListTrn(1).t_Account) := v_ListTrn(1).t_Account; -- для проверки на уникальность счетов
      FOR i IN 2 .. v_ListTrn.LAST
      LOOP
        pnurec.t_B15_SVOD := pnurec.t_B15_SVOD + v_ListTrn(i).t_Sum;
        IF v_ListAccCheck.EXISTS(v_ListTrn(i).t_Account) = FALSE THEN -- проверка на уникальность счетов
          pnurec.t_S9_SVOD := pnurec.t_S9_SVOD || ', ' || GetFormattedAccount(v_ListTrn(i).t_Account);
          v_ListAccCheck(v_ListTrn(i).t_Account) := v_ListTrn(i).t_Account; -- для проверки на уникальность счетов
        END IF;
      END LOOP;
      v_ListTrn.delete;
      v_ListAccCheck.delete;
    END IF;

    --Б16
    pnurec.t_B16_SVOD := 0;
    GetTrnSumAcc_SVOD( v_ListTrn, 1, 0, 0, pnurec.t_FIID, pBegDate, pEndDate,
                       '''+РС,ц/б'', ''+РС, д/с'', ''+КорОР_ЦБ''', '''70601''',
                       '''+Кор_Резерв, ЦБ'', ''-Кор_Резерв, ЦБ'', ''Резерв ц/б'''
                     );
    IF v_ListTrn.COUNT > 0 THEN
      FOR i IN v_ListTrn.FIRST .. v_ListTrn.LAST
      LOOP
        pnurec.t_B16_SVOD := pnurec.t_B16_SVOD + v_ListTrn(i).t_Sum;
      END LOOP;
      v_ListTrn.delete;
    END IF;

    --Б17
    pnurec.t_B17_SVOD := 0;
    GetTrnSumAcc_SVOD( v_ListTrn, 0, 0, 0, pnurec.t_FIID, pBegDate, pEndDate,
                       '''-РС,ц/б'', ''-РС, д/с'', ''-КорОР_ЦБ''', '''70606''',
                       '''+Кор_Резерв, ЦБ'', ''-Кор_Резерв, ЦБ'', ''Резерв ц/б'''
                     );
    IF v_ListTrn.COUNT > 0 THEN
      FOR i IN v_ListTrn.FIRST .. v_ListTrn.LAST
      LOOP
        pnurec.t_B17_SVOD := pnurec.t_B17_SVOD + v_ListTrn(i).t_Sum;
      END LOOP;
      v_ListTrn.delete;
    END IF;

    --Б19/С24
    pnurec.t_PLUS_PEREOC_ACCOUNT   := GetAccountByFIID_SVOD(pnurec.t_FIID, '''+ПО ДК 2014''');
    if ( (pnurec.t_PLUS_PEREOC_ACCOUNT is NULL) or (pnurec.t_PLUS_PEREOC_ACCOUNT = '') )then
      pnurec.t_PLUS_PEREOC_DIFF := 0;
    else
      pnurec.t_PLUS_PEREOC_DIFF := GetRestAccByCode(pnurec.t_FIID, '+ПО ДК 2014', pEndDate, RSI_RSB_FIInstr.NATCUR) - GetRestAccByCode(pnurec.t_FIID, '+ПО ДК 2014', pBegDate, RSI_RSB_FIInstr.NATCUR);
    end if;

    --Б20/С25
    pnurec.t_MINUS_PEREOC_ACCOUNT := GetAccountByFIID_SVOD(pnurec.t_FIID, '''-ПО ДК 2014''');
    if ( (pnurec.t_MINUS_PEREOC_ACCOUNT is NULL) or (pnurec.t_MINUS_PEREOC_ACCOUNT = '') )then
      pnurec.t_MINUS_PEREOC_DIFF := 0;
    else
      pnurec.t_MINUS_PEREOC_DIFF := GetRestAccByCode(pnurec.t_FIID, '-ПО ДК 2014', pEndDate, RSI_RSB_FIInstr.NATCUR) - GetRestAccByCode(pnurec.t_FIID, '-ПО ДК 2014', pBegDate, RSI_RSB_FIInstr.NATCUR);
    end if;

    --С10
    pnurec.t_ACC_REST := GetAccountByFIID_SVOD( pnurec.t_FIID,
                                                '''Наш портфель ц/б'''
                                              );

    --С11
    pnurec.t_ACC_COUPS := GetAccountByFIID_SVOD( pnurec.t_FIID,
                                                 '''Начисл.ПДД, ц/б''',
                                                 'AND '||Where_AccEqByParm(LLCLASS_KIND_ACC_PDD,1)||
                                                 'AND '||Where_AccEqByParm(LLCLASS_IS_AVOIR_BPP,0)
                                               );

    --С12
    pnurec.t_ACC_DISK := GetAccountByFIID_SVOD( pnurec.t_FIID,
                                                '''Начисл.ПДД, ц/б''',
                                                'AND '||Where_AccEqByParm(LLCLASS_KIND_ACC_PDD,2)||
                                                'AND '||Where_AccEqByParm(LLCLASS_IS_AVOIR_BPP,0)
                                              );

    --С13
    pnurec.t_S13_SVOD := GetAccountByFIID_SVOD( pnurec.t_FIID,
                                                '''Премия, ц/б''',
                                                'AND '||Where_AccEqByParm(LLCLASS_IS_AVOIR_BPP,0)
                                              );

    --С14
    pnurec.t_ACC_COUPB := GetAccountByFIID_SVOD(pnurec.t_FIID, '''Уплаченный НКД''');

    --С15
    pnurec.t_ACC_REPREST := GetAccountByDEAL_SVOD(pnurec.t_FIID, '''Ц/б, БПП'', ''Ц/б, Корзина БПП''');

    --С16
    pnurec.t_ACC_REPCOUPS := GetAccountByFIID_SVOD( pnurec.t_FIID,
                                                    '''Начисл.ПДД, ц/б''',
                                                    'AND '||Where_AccEqByParm(LLCLASS_KIND_ACC_PDD,1)||
                                                    'AND '||Where_AccEqByParm(LLCLASS_IS_AVOIR_BPP,1)
                                                  );

    --С17
    pnurec.t_ACC_REPDISK := GetAccountByFIID_SVOD( pnurec.t_FIID,
                                                   '''Начисл.ПДД, ц/б''',
                                                   'AND '||Where_AccEqByParm(LLCLASS_KIND_ACC_PDD,2)||
                                                   'AND '||Where_AccEqByParm(LLCLASS_IS_AVOIR_BPP,1)
                                                 );

    --С18
    pnurec.t_S18_SVOD := GetAccountByFIID_SVOD( pnurec.t_FIID,
                                                '''Премия, ц/б''',
                                                'AND '||Where_AccEqByParm(LLCLASS_IS_AVOIR_BPP,1)
                                              );

    --С19
    pnurec.t_ACC_REPCOUPB := GetAccountByDEAL_SVOD(pnurec.t_FIID, '''Уплаченный НКД, БПП'', ''Уплач. НКД, Корзина БПП''');

    --С20
    pnurec.t_S20_SVOD := GetAccountByFIID_SVOD(pnurec.t_FIID, '''+Переоценка, ц/б ССПУ_ЦБ'', ''+Переоценка, ц/б СССД_ЦБ''');

    --С21
    pnurec.t_S21_SVOD := GetAccountByFIID_SVOD(pnurec.t_FIID, '''-Переоценка, ц/б ССПУ_ЦБ'', ''-Переоценка, ц/б СССД_ЦБ''');

    --С22
    pnurec.t_S22_SVOD := GetAccountByFIID_SVOD(pnurec.t_FIID, '''+Корректировка, ц/б''');

    --С23
    pnurec.t_S23_SVOD := GetAccountByFIID_SVOD(pnurec.t_FIID, '''-Корректировка, ц/б''');

    if (BPP_ACCOUNT_METHOD = 1) then
       --С15
       v_ACC_REPREST := GetAccountByFIID_SVOD( pnurec.t_FIID,
                                               '''Наш портфель ц/б''',
                                               'AND '||Where_AccEqByParm(LLCLASS_IS_AVOIR_BPP,1)
                                             );
       if (v_ACC_REPREST != '') then
          pnurec.t_ACC_REPREST := pnurec.t_ACC_REPREST || (case when pnurec.t_ACC_REPREST != '' then ', ' else '' end) || v_ACC_REPREST;
       end if;

       --С19
       v_ACC_REPCOUPB := GetAccountByFIID_SVOD( pnurec.t_FIID,
                                                '''Уплаченный НКД''',
                                                'AND '||Where_AccEqByParm(LLCLASS_IS_AVOIR_BPP,1)
                                              );
       if (v_ACC_REPCOUPB != '') then
          pnurec.t_ACC_REPCOUPB := pnurec.t_ACC_REPCOUPB || (case when pnurec.t_ACC_REPCOUPB != '' then ', ' else '' end) || v_ACC_REPCOUPB;
       end if;
    end if;

  END;

  -- Обработка ФИ для НУСВОД
  PROCEDURE ProcessFI_SVOD(pFIID IN NUMBER, pSessionID IN NUMBER, pBegDate IN DATE, pEndDate IN DATE, pReqID IN VARCHAR2 DEFAULT NULL, pIsParallel IN NUMBER DEFAULT 1)
  IS
    v_nusvodrep nusvodrep_t := nusvodrep_t();
    v_nurec DNUSVODREP_DBT%rowtype;
  BEGIN
    SELECT * BULK COLLECT INTO v_nusvodrep
      FROM DNUSVODREP_DBT
     WHERE t_SessionID = pSessionID
       AND t_FIID = pFIID;

    IF (v_nusvodrep.COUNT > 0) THEN
      v_nurec := v_nusvodrep(1);

      IF (IsReadedSettings = false) THEN
        SetSettings;
      END IF;

      if( v_nurec.t_TaxGroup = 17 )then
        v_nurec.t_StRate := GetStRateFor17(v_nurec.T_FI_CODE);
      else
        v_nurec.t_StRate := GetStRate_15_20_9_0(v_nurec.t_TaxGroup);
      end if;

      SetAccFlds_SVOD(v_nurec, pBegDate, pEndDate);

      v_nurec.t_PRTFCATEG := '';
      if(instr(v_nurec.t_ACC_REST,'501') = 1 or instr(v_nurec.t_ACC_REST,', 501') > 0 or instr(v_nurec.t_ACC_REST,'506') = 1 or instr(v_nurec.t_ACC_REST,', 506') > 0)then
        v_nurec.t_PRTFCATEG := '1-я категория';
      end if;
      if(instr(v_nurec.t_ACC_REST,'502') = 1 or instr(v_nurec.t_ACC_REST,', 502') > 0 or instr(v_nurec.t_ACC_REST,'507') = 1 or instr(v_nurec.t_ACC_REST,', 507') > 0)then
        if(NVL(LENGTH(v_nurec.t_PRTFCATEG), 0) > 0)then
           v_nurec.t_PRTFCATEG := v_nurec.t_PRTFCATEG||' ';
        end if;
        v_nurec.t_PRTFCATEG := v_nurec.t_PRTFCATEG||'2-я категория';
      end if;
      if(instr(v_nurec.t_ACC_REST,'504') = 1 or instr(v_nurec.t_ACC_REST,', 504') > 0 or instr(v_nurec.t_ACC_REST,'601') = 1 or instr(v_nurec.t_ACC_REST,', 601') > 0)then
        if(NVL(LENGTH(v_nurec.t_PRTFCATEG), 0) > 0)then
           v_nurec.t_PRTFCATEG := v_nurec.t_PRTFCATEG||' ';
        end if;
        v_nurec.t_PRTFCATEG := v_nurec.t_PRTFCATEG||'3-я категория';
      end if;

      IF (pIsParallel = 1) THEN
        UPDATE DNUSVODREP_DBT
           SET ROW = v_nurec
         WHERE t_SessionID = pSessionID
           AND t_FIID = v_nurec.t_FIID;
      ELSE
        GlobalSVODRepList(pSessionID).extend();
        GlobalSVODRepList(pSessionID)(GlobalSVODRepList(pSessionID).last) := v_nurec;
      END IF;
    END IF;

    IF( pReqID IS NOT NULL ) THEN -- индикатор прогресса для веб
      WebProcessState_IncreaseByReqID(pReqID);
    END IF;

  EXCEPTION  
    WHEN OTHERS THEN
      BEGIN
        AddNURepError(pSessionID, 'При обработке ц/б ' || v_nurec.t_FI_CODE || ' в СВОД, произошла ошибка ');
        RAISE;
      END;
  END;

  --Формирование данных НУСвод
  PROCEDURE CreateNU_SVOD_Data( pBegDate IN DATE,
                                pEndDate IN DATE,
                                pSessionID IN NUMBER,
                                pParallelLevel IN NUMBER,
                                pReqID IN VARCHAR2 DEFAULT NULL )
  IS
    v_Cnt NUMBER;
    v_task_name VARCHAR2(30);
    v_sql_chunks CLOB;
    v_sql_process VARCHAR2(400);
    v_try NUMBER(5) := 0;
    v_status NUMBER;
  BEGIN

    INSERT INTO DNUSVODREP_DBT (t_SessionID,
                                t_FIID,
                                t_FI_CODE,
                                t_FaceValueFI,
                                t_FaceValue,
                                t_SecName,
                                t_ISIN,
                                t_NumReg,
                                t_TaxGroup,
                                t_Code,
                                t_RootAvrKind,
                                t_VN,
                                t_IssName,
                                t_DGO
                               )
                (SELECT pSessionID,
                        FI.t_FIID,
                        FI.T_FI_CODE,
                        FI.t_FaceValueFI,
                        FI.t_FaceValue,
                        FI.t_Name AS t_SecName,
                        AV.t_ISIN,
                        AV.t_LSIN AS t_NumReg,
                        AV.T_TaxGroup,
                        AVK.t_ShortName AS t_Code,
                        RSI_RSB_FIInstr.FI_AvrKindsGetRoot( 2, FI.t_AvoirKind ) AS t_RootAvrKind,
                        (SELECT T_CCY FROM DFININSTR_DBT WHERE T_FIID = FI.t_FaceValueFI) AS t_VN,
                        (SELECT t_Name FROM DPARTY_DBT WHERE t_PartyID = FI.t_Issuer) AS t_IssName,
                        TXFI.t_DGO
                   FROM ( WITH FIIDS AS (
                          -- ДАННЫЕ НАЛОГОВОГО УЧЕТА:
                          SELECT TXBuyLot.t_FIID
                            FROM ( SELECT NVL(TXRest.T_SourceID,0) AS SourceID
                                     FROM DSCTXREST_DBT TXRest
                                    WHERE TXRest.T_AMOUNT > 0 AND
                                         (TXRest.t_BUYDATE <= pEndDate) AND
                                         TXRest.t_BUYDATE <> UnknownDate AND
                                        ( TXRest.t_SALEDATE IS NULL  OR
                                           TXRest.t_SALEDATE = UnknownDate OR 
                                           TXRest.t_SALEDATE > pEndDate
                                         ) 
                                   GROUP BY TXRest.T_SourceID
                                 ) QueryRest, DSCTXLOT_DBT currTXBuyLot, DSCTXLOT_DBT TXBuyLot
                           WHERE currTXBuyLot.t_ID = QueryRest.SourceID AND
                                 TXBuyLot.t_ID = currTXBuyLot.t_BegLotID AND
                                 TXBuyLot.t_Type = RSB_SCTXC.TXLOTS_BUY
                           GROUP BY TXBuyLot.t_FIID
                          UNION
                          -- ПЛЮС БУМАГИ ИЗ РЕГИСТРОВ ДЛЯ СВОД:
                          SELECT FI.T_FIID
                            FROM DFININSTR_DBT FI, DSCTXTOTAL_DBT total
                           WHERE FI.T_FI_CODE = total.t_FI_CODE
                          UNION
                          -- ПЛЮС БУМАГИ ПО СДЕЛКАМ ПФИ, КОТОРЫЕ МОГУТ ОТСУТСТВОВАТЬ НА БАЛАНСЕ
                          SELECT DISTINCT tick.t_PFI
                            FROM doprdocs_dbt docs, doproper_dbt opr, ddl_tick_dbt tick, dacctrn_dbt trn
                           WHERE opr.t_DocKind = RSB_SECUR.DL_SECURITYDOC
                                AND TO_NUMBER (opr.t_DocumentID) = tick.t_dealid
                                AND docs.t_ID_Operation = opr.t_ID_Operation
                                AND docs.t_DocKind = 1
                                AND tick.T_ISPFI = chr(88)
                                AND docs.t_AccTrnID = trn.t_AccTrnID
                                AND trn.t_Chapter = 1
                                AND trn.t_State = 1
                                AND (trn.t_Date_Carry BETWEEN  pBegDate and pEndDate)
                                AND EXISTS
                                   (SELECT 1
                                      FROM dmcaccdoc_dbt acd, DMCCATEG_DBT cat
                                     WHERE acd.t_Chapter = trn.t_Chapter
                                       AND acd.t_Currency = trn.T_FIID_RECEIVER
                                       AND acd.t_Account = trn.T_ACCOUNT_RECEIVER
                                       AND acd.t_CatID = cat.t_ID
                                       AND cat.T_CODE IN ('Расходы ПФИ','Доходы ПФИ','+ПФИ','-ПФИ')
                                       AND cat.T_LEVELTYPE = 1)
                                AND NOT EXISTS (select 1 from ddl_comm_dbt com where com.t_DocKind = RSB_SECUR.DL_ISSUE_UNION and com.t_FIID = tick.t_PFI)
                          )
                          SELECT t_FIID, UnknownDate t_DGO
                            FROM FIIDS
                          UNION
                          -- ПЛЮС СТАРЫЕ ВЫПУСКИ В ГЛОБАЛЬНЫХ ОПЕРАЦИЯХ
                          SELECT t_FIID, t_CommDate t_DGO
                            FROM ddl_comm_dbt
                           WHERE t_DocumentID IN (select t_DealID from DSCDLFI_DBT dlfi, FIIDS fi where dlfi.t_DealKind = RSB_SECUR.DL_ISSUE_UNION and dlfi.t_NewFIID =  fi.t_FIID)
                        ) TXFI, DFININSTR_DBT FI, DAVOIRISS_DBT AV, DAVRKINDS_DBT AVK
                  WHERE FI.t_FIID = TXFI.t_FIID
                    AND AV.t_FIID = FI.t_FIID
                    AND AVK.t_AVOIRKIND = FI.t_AVOIRKIND
                    AND AVK.t_FI_KIND = FI.t_FI_KIND
                );

    v_Cnt := SQL%ROWCOUNT;

    IF(v_Cnt > 0) THEN
      IF( pReqID IS NOT NULL) THEN -- индикатор прогресса для веб
        WebProgressIndicator_Start(pReqID, v_Cnt, 'НУ СВОД: обработка отобранных строк');
      END IF;

      IF(pParallelLevel > 0) THEN
        v_task_name := DBMS_PARALLEL_EXECUTE.generate_task_name;
        DBMS_PARALLEL_EXECUTE.create_task (task_name => v_task_name);

        v_sql_chunks := 'SELECT t_FIID, '||to_char(pSessionID)||' FROM DNUSVODREP_DBT WHERE t_SessionID = '||to_char(pSessionID);

        DBMS_PARALLEL_EXECUTE.create_chunks_by_sql(task_name => v_task_name,
                                                   sql_stmt  => v_sql_chunks,
                                                   by_rowid  => FALSE);

        v_sql_process := 'CALL RSB_NUREP.ProcessFI_SVOD(:start_id, :end_id, '||
                                                        'TO_DATE('''||TO_CHAR(pBegDate, 'DD.MM.YYYY')||''',''DD.MM.YYYY''), '||
                                                        'TO_DATE('''||TO_CHAR(pEndDate, 'DD.MM.YYYY')||''',''DD.MM.YYYY''), '||
                                                        ''''||pReqID||''')';

        DBMS_PARALLEL_EXECUTE.run_task(task_name => v_task_name,
                                       sql_stmt => v_sql_process,
                                       language_flag => DBMS_SQL.NATIVE,
                                       parallel_level => pParallelLevel);

        v_status := DBMS_PARALLEL_EXECUTE.task_status(v_task_name);
        WHILE(v_try < 2 and v_status != DBMS_PARALLEL_EXECUTE.FINISHED)
        LOOP
          v_try := v_try + 1;
          DBMS_PARALLEL_EXECUTE.resume_task(v_task_name);
          v_status := DBMS_PARALLEL_EXECUTE.task_status(v_task_name);
        END LOOP;

        DBMS_PARALLEL_EXECUTE.drop_task(v_task_name);

      ELSE -- для отладки
        GlobalSVODRepList(pSessionID) := nusvodrep_t();

        FOR cData IN (SELECT T_FIID FROM DNUSVODREP_DBT WHERE t_SessionID = pSessionID)
        LOOP
          ProcessFI_SVOD(cData.t_FIID, pSessionID, pBegDate, pEndDate, pReqID, 0);
        END LOOP;

        IF (GlobalSVODRepList(pSessionID).COUNT > 0) THEN
          FORALL i IN GlobalSVODRepList(pSessionID).FIRST .. GlobalSVODRepList(pSessionID).LAST
            UPDATE DNUSVODREP_DBT
               SET ROW = GlobalSVODRepList(pSessionID)(i)
             WHERE t_SessionID = pSessionID
               AND t_FIID = GlobalSVODRepList(pSessionID)(i).t_FIID;

          GlobalSVODRepList(pSessionID).DELETE;
          GlobalSVODRepList.DELETE(pSessionID);
        END IF;
      END IF;

      --IF( pReqID IS NOT NULL ) THEN -- индикатор прогресса для веб
      --  WebProgressIndicator_Stop;
      --END IF;
    END IF;

    EXCEPTION
      WHEN OTHERS THEN
        BEGIN
          IF ((pParallelLevel < 1) AND
              (GlobalSVODRepList IS NOT NULL) AND
              (GlobalSVODRepList.COUNT > 0) AND
              (GlobalSVODRepList.EXISTS(pSessionID))) THEN
            GlobalSVODRepList(pSessionID).DELETE;
            GlobalSVODRepList.DELETE(pSessionID);
          END IF;
          DELETE FROM DNUSVODREP_DBT WHERE t_SessionID = pSessionID;
          RAISE;
        END;
  END CreateNU_SVOD_Data;

END rsb_NUrep;
/
