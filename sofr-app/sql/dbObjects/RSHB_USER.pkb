CREATE OR REPLACE PACKAGE BODY RSHB_USER
AS
   FUNCTION DetermineOp (p_FIID           IN     INTEGER,
                         p_CalcDiscount      OUT INTEGER,
                         p_CalcBonus         OUT INTEGER,
                         p_CalcOutlay        OUT INTEGER)
      RETURN INTEGER
   AS
      v_Count       INTEGER;
      v_SumComiss   NUMBER;
      v_IsClosed    INTEGER;
   BEGIN
      SELECT COUNT (t_isclosed)
        INTO v_IsClosed
        FROM dfininstr_dbt
       WHERE t_fiid = p_fiid AND t_isClosed = CHR (88);

      IF v_IsClosed = 0
      THEN
         --начисление дисконта
         SELECT COUNT (*)
           INTO v_Count
           FROM dpmwrtsum_dbt pm, dfininstr_dbt fi
          WHERE     PM.T_FIID = fi.t_fiid
                AND pm.t_party = -1
                AND RSI_RSB_FIInstr.
                     FI_AvrKindsGetRoot (fi.t_fi_kind, fi.t_AvoirKind) = 17
                AND pm.t_fiid = p_Fiid
                AND PM.T_BEGDISCOUNT > 0;

         IF v_Count > 0
         THEN
            p_CalcDiscount := 1;
         ELSE
            p_CalcDiscount := 0;
         END IF;

         --начисление премии
         SELECT COUNT (*)
           INTO v_Count
           FROM dpmwrtsum_dbt pm, dfininstr_dbt fi
          WHERE     PM.T_FIID = fi.t_fiid
                AND pm.t_party = -1
                AND RSI_RSB_FIInstr.
                     FI_AvrKindsGetRoot (fi.t_fi_kind, fi.t_AvoirKind) = 17
                AND pm.t_fiid = p_Fiid
                AND PM.T_BEGBONUS > 0;

         IF v_Count > 0
         THEN
            p_CalcBONUS := 1;
         ELSE
            p_CalcBONUS := 0;
         END IF;

         --начисление расходов
         SELECT NVL (SUM (RSB_FIInstr.ConvSum (DLC.T_SUM,
                                               CM.T_FIID_COMM,
                                               0,
                                               DLC.T_PLANPAYDATE)),
                     0)
           INTO v_SUmComiss
           FROM DDLCOMIS_DBT DLC, DSFCOMISS_DBT CM
          WHERE     DLC.T_DOCKIND = 5
                AND DLC.T_DOCID = p_FIID
                --AND DLC.T_PLANPAYDATE <= ?
                AND CM.T_FEETYPE = DLC.T_FEETYPE
                AND CM.T_NUMBER = DLC.T_COMNUMBER;

         IF v_SumComiss > 0
         THEN
            p_CalcOutlay := 1;
         ELSE
            p_CalcOutlay := 0;
         END IF;

         RETURN 1;
      ELSE
         RETURN 0;
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         RETURN 0;
   END;

   -- Обновляет таблицу остатков по КДУ перед проведением сверки с вышестоящим депозитарием
   PROCEDURE UpdateSCRest (pStorID    IN INTEGER,
                           pDepoAcc   IN VARCHAR2,
                           pDate      IN DATE)
   IS
      pQRid     INTEGER;
      nRestID   INTEGER;
   BEGIN
      FOR QR IN (SELECT t_qrid
                   FROM dscqracc_dbt
                  WHERE t_storageid = pStorID AND t_depoAccCode = pDepoAcc)
      LOOP
         BEGIN
            SELECT t_id
              INTO nRestID
              FROM dscqrrest_dbt
             WHERE t_qrid = qr.t_qrid AND t_date = pDate;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               --Создание строки с остатком
               INSERT INTO dscqrrest_dbt (t_qrid, t_rest, t_date)
                    VALUES (QR.t_qrid, 0, pDate);
         END;

         UPDATE dscqrrest_dbt
            SET t_rest = 0
          WHERE t_ID = nRestID;
      END LOOP;

      COMMIT;
   END;



   FUNCTION GetKDURestOnDate (p_FIID IN NUMBER, p_Date IN DATE)
      RETURN NUMBER
   AS
      v_retVal   NUMBER;
   BEGIN
        SELECT SUM (ROUND (ABS (RSI_RSB_ACCOUNT.RestAC (ac.T_ACCOUNT,
                                                        ac.T_CODE_CURRENCY,
                                                        p_Date,
                                                        ac.T_CHAPTER,
                                                        0))))
          INTO v_retval
          FROM dscqracc_dbt qrac, daccount_dbt ac
         WHERE QRAC.T_ACCOUNTID = AC.T_ACCOUNTID AND qrac.t_fiid = p_FIID
      GROUP BY qrac.t_fiid;

      RETURN v_retval;
   END;



   FUNCTION EuroImpNeedKdu (P_Date IN DATE, P_TXT OUT VARCHAR2)
      RETURN INTEGER
   AS
      p_count   INTEGER;
   BEGIN
      SELECT COUNT (T.T_DEALID)
        INTO p_count
        FROM ddlgrdeal_dbt d, ddlgracc_dbt ac, ddl_tick_dbt t
       WHERE     d.t_docid = t.t_dealid
             AND d.t_dockind = t.t_bofficekind
             AND d.t_id = ac.t_grdealid
             AND AC.T_ACCNUM = 4
             AND T.T_DEALSTATUS = 10
             AND d.t_plandate <= p_Date
             AND ac.t_state = 1;

      IF p_count > 0
      THEN
         p_Txt :=
            'Найдены неисполненные строки графика по сделкам исполнение которых может повлиять на остаток ЦБ. | Рекомендуется выполнить формирование поручений Депо за указанные даты: |';

         FOR c_Deal
            IN (  SELECT COUNT (T.T_DEALID) cnt, d.t_plandate dealdate
                    FROM ddlgrdeal_dbt d, ddlgracc_dbt ac, ddl_tick_dbt t
                   WHERE     d.t_docid = t.t_dealid
                         AND d.t_dockind = t.t_bofficekind
                         AND d.t_id = ac.t_grdealid
                         AND AC.T_ACCNUM = 4
                         AND T.T_DEALSTATUS = 10
                         AND d.t_plandate <= p_Date
                         AND ac.t_state = 1
                GROUP BY d.t_plandate
                ORDER BY dealdate)
         LOOP
            p_TXT :=
                  p_TXT
               || TO_CHAR (c_Deal.cnt)
               || ' за '
               || TO_CHAR (c_Deal.dealdate, 'dd.mm.yyyy')
               || '  |  ';
         END LOOP;

         RETURN 1;
      ELSE
         RETURN 0;
      END IF;
   -- DBMS_OUTPUT.PUT_LINE(p_TXT);

   END EuroImpNeedKdu;

   FUNCTION Bond_HasZeroCoupons (FIID IN NUMBER, CalcDate IN DATE)
      RETURN NUMBER
   IS
      v_RetVal   NUMBER := 0;
   BEGIN
      SELECT COUNT (1)
        INTO v_RetVal
        FROM dfiwarnts_dbt
       WHERE     T_IsPartial = CHR (0)
             AND T_FIID = FIID
             AND CalcDate BETWEEN T_FirstDate AND T_DrawingDate;

      IF v_RetVal > 0
      THEN
         SELECT COUNT (1)
           INTO v_RetVal
           FROM dfiwarnts_dbt
          WHERE     T_IsPartial = CHR (0)
                AND T_FIID = FIID
                AND CalcDate BETWEEN T_FirstDate AND T_DrawingDate
                AND T_INCOMERATE = 0
                AND T_INCOMEVOLUME = 0;

         IF v_RetVal > 0
         THEN
            v_RetVal := 1;
         ELSE
            v_RetVal := 3;
         END IF;
      END IF;


      RETURN v_RetVal;
   END;                                                   -- FI_HasZeroCoupons

   FUNCTION GA_GETPARTYID (PTCODE IN VARCHAR2)
      RETURN NUMBER
   IS
      v_RetVal   NUMBER := 0;
   BEGIN
      IF PTCODE = CHR (0)
      THEN
         V_RETVAL := 0;
         RETURN V_RETVAL;
      END IF;

      CASE
         WHEN SUBSTR (PTCODE, 1, 3) = 'INN'
         THEN                                                            --INN
            BEGIN
               SELECT T_OBJECTID
                 INTO V_RETVAL
                 FROM DOBJCODE_DBT
                WHERE     t_objectid NOT IN (127951, 127690)
                      AND T_OBJECTTYPE = 3
                      AND T_CODEKIND = 16
                      AND INSTR (T_CODE, REPLACE (PTCODE, 'INN_')) > 0
                      AND t_bankclosedate = TO_DATE ('01010001', 'ddmmyyyy');
            EXCEPTION
               WHEN TOO_MANY_ROWS
               THEN
                  V_RETVAL := -2;
               WHEN NO_DATA_FOUND
               THEN
                  V_RETVAL := 0;
            END;
         ELSE                                                            --RIK
            BEGIN
               SELECT T_OBJECTID
                 INTO V_RETVAL
                 FROM DOBJCODE_DBT
                WHERE     T_OBJECTTYPE = 3
                      AND T_CODEKIND = 70
                      AND T_CODE = PTCODE;
            EXCEPTION
               WHEN TOO_MANY_ROWS
               THEN
                  V_RETVAL := -2;
               WHEN NO_DATA_FOUND
               THEN
                  V_RETVAL := -1;
            END;
      END CASE;

      RETURN V_RETVAL;
   END;                                                       -- GA_GETPARTYID


   PROCEDURE CheckCoupon (pCheckCouponAmount   IN NUMBER,
                          pCheckCouponNumber   IN NUMBER,
                          pCheckCouponDate     IN NUMBER,
                          pCheckCouponRate     IN NUMBER,
                          pOnlyOwnPortfolio    IN NUMBER,
                          pCurDate             IN DATE,
                          pOnlyNewCoupon       IN NUMBER)
   IS
      vFintoolID          NUMBER;
      vRDCouponAmount     NUMBER;
      vSOFRCouponAmount   NUMBER;
      vComment            CLOB := '';
      vLostCoupon         CLOB := '';
      vIs_Exist           NUMBER;
      vSofrIncVol         NUMBER;
      vSofrIncRate        NUMBER;
      vRELATIVEINCOME     CHAR (1);
      l_Error             EXCEPTION;
      findFintoolID BOOLEAN;
      PRAGMA EXCEPTION_INIT (l_Error, -942);
      
   BEGIN
      -- почистим таблицу перед новой вставкой
      DELETE FROM CouponVerification_dbt;
      
--      FOR m_I
--         IN (SELECT av.t_fiid,
--                    fi.t_definition,
--                    av.t_isin,
--                    av.t_lsin,
--                    (SELECT c.t_code
--                       FROM dobjcode_dbt c
--                     WHERE     c.t_objecttype = 9
--                            AND c.t_codekind = 104
--                            AND c.t_objectid = av.t_fiid
--                            AND c.t_state = 0)
--                       RuDataCode
--               FROM davoiriss_dbt av, dfininstr_dbt fi
--              WHERE av.t_fiid = fi.t_fiid
--                    AND RSI_RSB_FIInstr.
--                         FI_AvrKindsGetRoot (fi.t_fi_kind, fi.t_AvoirKind) =
--                           17
--                    AND FI.T_ISCLOSED <> CHR (88)
--                   -- AND FI.T_DRAWINGDATE >= TO_DATE (' 7.08.2019', 'dd.mm.yyyy') SVE
--                   -- AND FI.T_DRAWINGDATE <= TO_DATE (' 17.04.2020', 'dd.mm.yyyy')   
--                   AND FI.T_DRAWINGDATE >= pCurDate
--                    --              AND av.t_fiid = 1639
--                    AND av.t_fiid IN
--                           (SELECT DISTINCT v.t_fiid
--                              FROM v_scwrthistex v
--                             WHERE t_state IN (1, 3, 20, 21) AND t_amount > 0
--                                   AND t_instance =
--                                          (SELECT MAX (t_instance)
--                                             FROM v_scwrthistex
--                                            WHERE v.t_sumid = t_sumid
--                                                  AND v.t_changedate =
--                                                         t_changedate)
--                                   AND t_party =
--                                          CASE
--                                             WHEN pOnlyOwnPortfolio = 1
--                                             THEN
--                                                -1
--                                             ELSE
--                                                t_party
--                                          END
--                                   AND v.t_changedate =
--                                          (SELECT MAX (t.t_changedate)
--                                             FROM v_scwrthistex t
--                                            WHERE v.t_sumid = t_sumid
--                                                  --AND t.t_changedate <= TO_DATE (' 7.08.2019', 'dd.mm.yyyy'))))  SVE
--                                                  --AND t.t_changedate <= TO_DATE (' 17.04.2020', 'dd.mm.yyyy') )))
--                                                  AND t.t_changedate <= pCurDate )))
      FOR m_I
        IN (SELECT av.t_fiid,
                         fi.t_definition,
                         av.t_isin,
                         av.t_lsin,
                         (SELECT c.t_code
                             FROM dobjcode_dbt c
                           WHERE c.t_objecttype = 9
                              AND c.t_codekind = 104
                              AND c.t_objectid = av.t_fiid
                              AND c.t_state = 0)    RuDataCode
               FROM davoiriss_dbt av, dfininstr_dbt fi
             WHERE av.t_fiid = fi.t_fiid
                  AND RSI_RSB_FIInstr.FI_AvrKindsGetRoot (fi.t_fi_kind, fi.t_AvoirKind) = 17
                  AND FI.T_ISCLOSED <> CHR (88)
                  AND FI.T_DRAWINGDATE >= pCurDate
                  AND (rsb_pmwrtoff.WRTGetPortfolioAmount (1, fi.t_fiid, CASE WHEN pOnlyOwnPortfolio = 1 THEN -1 ELSE 0 END, -1, -1, -1, pCurDate, 1, 0, 1) != 0
                    OR RSB_PMWRTOFF.WRTGETAMOUNTOWN (1, fi.t_fiid, pCurDate, 1, 1) != 0))
 
      LOOP
         IF m_i.RuDataCode IS NULL
         THEN
            vComment :=
               vComment
               || 'Для выпуска не указан идентификатор в RuDATA. ';
         END IF;

         --определяем fintoolid
         begin
         SELECT SIF.FINTOOLID
           INTO vFintoolID
           FROM sofr_info_fintoolreferencedata sif
          WHERE sif.ISINCODEBASE_NRD = m_I.t_Isin
                OR sif.ISINCODE = m_I.t_isin;
                findFintoolID := TRUE;
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
             vComment :=
               vComment
               || 'Для выпуска не найдено соответствие в таблицах RuDATA. ';
             findFintoolID := FALSE ;        
          end;      

         --Проверка купонов по количеству
         IF pCheckCouponAmount = 1 and findFintoolID
         THEN
            SELECT COUNT (*)
              INTO vRDCouponAmount
              FROM SOFR_BOND_COUPONS sbc
             WHERE SBC.ID_FINTOOL = vFintoolID
                   AND sbc.end_period >=
                          CASE
                             WHEN pOnlyNewCoupon = 1 THEN pCurDate
                             ELSE sbc.end_period
                          END;

            SELECT COUNT (*)
              INTO vSOFRCouponAmount
              FROM dfiwarnts_dbt
             WHERE t_fiid = m_I.t_FIID AND T_ISPARTIAL != CHR (88)
                   AND t_drawingdate >=
                          CASE
                             WHEN pOnlyNewCoupon = 1 THEN pCurDate
                             ELSE t_drawingdate
                          END;

            IF vRDCouponAmount <> vSOFRCouponAmount
            THEN
               vComment :=
                  vComment
                  || 'Имеются расхождения в количестве купонов: СОФР('
                  || vSOFRCouponAmount
                  || '), RuDATA('
                  || vRDCouponAmount
                  || ').'
                  || CHR (13)
                  || CHR (10);
            END IF;
         END IF;

         --Проверка купонов по номерам
         IF pCheckCouponNumber = 1
         THEN
            FOR c_I
               IN (SELECT *
                     FROM SOFR_BOND_COUPONS sbc
                    WHERE SBC.ID_FINTOOL = vFintoolID
                          AND sbc.end_period >=
                                 CASE
                                    WHEN pOnlyNewCoupon = 1 THEN pCurDate
                                    ELSE sbc.end_period
                                 END)
            LOOP
               BEGIN
                  SELECT 1
                    INTO vIs_Exist
                    FROM dfiwarnts_dbt
                   WHERE     t_fiid = m_I.t_fiid
                         AND t_ispartial != CHR (88)
                         AND t_number = c_i.id_coupon;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     vIs_Exist := 0;
               END;

               IF vIs_Exist = 0
               THEN
                  vLostCoupon := vLostCoupon || ' ' || c_i.id_coupon || ';';
               END IF;
            END LOOP;

            IF LENGTH (vLostCoupon) > 0
            THEN
               vComment :=
                  vComment
                  || 'Номера купонов, отсутствующие в СОФР:'
                  || vLostCoupon
                  || CHR (13)
                  || CHR (10);
               vLostCoupon := '';
            END IF;
         END IF;

         vLostCoupon := '';

         --Проверка купонов по датам
         IF pCheckCouponDate = 1 and findFintoolID
         THEN
            FOR c_I
               IN (  SELECT *
                       FROM SOFR_BOND_COUPONS sbc
                      WHERE SBC.ID_FINTOOL = vFintoolID
                            AND sbc.end_period >=
                                   CASE
                                      WHEN pOnlyNewCoupon = 1 THEN pCurDate
                                      ELSE sbc.end_period
                                   END
                   ORDER BY begin_period)
            LOOP
               BEGIN
                  SELECT 1
                    INTO vIs_Exist
                    FROM dfiwarnts_dbt
                   WHERE     t_fiid = m_I.t_fiid
                         AND t_ispartial != CHR (88)
                         AND t_firstdate - 1 = c_I.Begin_Period
                         AND t_drawingdate = c_I.End_period;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     vIs_Exist := 0;
               END;

               IF vIs_Exist = 0
               THEN
                  vLostCoupon :=
                        vLostCoupon
                     || CHR (13)
                     || CHR (10)
                     || '          -  с '
                     || TO_CHAR (c_I.Begin_period, 'dd.mm.yyyy')
                     || ' по '
                     || TO_CHAR (c_I.End_Period, 'dd.mm.yyyy')
                     || ' ;'
                     || CHR (13)
                     || CHR (10);
               END IF;
            END LOOP;

            IF LENGTH (vLostCoupon) > 0
            THEN
               vComment :=
                  vComment
                  || 'Купонные периоды соответствие которым не найдено в СОФР:'
                  || vLostCoupon
                  || CHR (13)
                  || CHR (10);
               vLostCoupon := '';
            END IF;
         END IF;

         --Проверка купонов по ставкам
         IF pCheckCouponRate = 1 and findFintoolID
         THEN
            FOR c_I
               IN (SELECT *
                     FROM SOFR_BOND_COUPONS sbc
                    WHERE SBC.ID_FINTOOL = vFintoolID
                          AND end_period >= pCurDate
                          AND sbc.end_period >=
                                 CASE
                                    WHEN pOnlyNewCoupon = 1 THEN pCurDate
                                    ELSE sbc.end_period
                                 END)
            LOOP
               BEGIN
                  SELECT 1,
                         ROUND (t_incomevolume, 10),
                         ROUND (t_incomerate, 10),
                         t_RELATIVEINCOME
                    INTO vIs_Exist,
                         vSofrIncVol,
                         vSofrIncRate,
                         vRELATIVEINCOME
                    FROM dfiwarnts_dbt
                   WHERE     t_fiid = m_I.t_fiid
                         AND t_ispartial != CHR (88)
                         AND t_firstdate - 1 = c_I.Begin_Period
                         AND t_drawingdate = c_I.End_period;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     vIs_Exist := 0;
                     vSofrIncVol := 0;
                     vSofrIncRate := 0;
                     vRELATIVEINCOME := CHR (0);
               END;

               /*DBMS_OUTPUT.
                put_line (
                     vSofrIncVol
                  || '   '
                  || c_I.pay_per_bond
                  || '   '
                  || vSofrIncRate
                  || '   '
                  || c_I.coupon_rate);*/

               IF vIs_Exist = 0
                  OR (vSofrIncVol != c_I.pay_per_bond
                      AND vRELATIVEINCOME = CHR (0))
                  OR (vSofrIncRate != c_I.coupon_rate
                      AND vRELATIVEINCOME = CHR (88))
               THEN
                  vLostCoupon :=
                        vLostCoupon
                     || CHR (13)
                     || CHR (10)
                     || '     - ( '
                     || TO_CHAR (c_I.Begin_period, 'dd.mm.yyyy')
                     || ' по '
                     || TO_CHAR (c_I.End_Period, 'dd.mm.yyyy')
                     || ')  '
                     || CHR (13)
                     || CHR (10)
                     || '          Выплата на 1 цб СОФР ( '
                     || vSofrIncVol
                     || ')   RuDATA('
                     || c_I.pay_per_bond
                     || ')'
                     || CHR (13)
                     || CHR (10)
                     || '          Ставка СОФР ( '
                     || vSofrIncRate
                     || ')   RuDATA('
                     || c_I.coupon_rate
                     || ')'
                     || CHR (13)
                     || CHR (10);
               END IF;
            END LOOP;

            IF LENGTH (vLostCoupon) > 0
            THEN
               vComment :=
                  vComment
                  || 'Найдены расхождения в ставках:  '
                  || vLostCoupon;
               vLostCoupon := '';
            END IF;
         END IF;

         vLostCoupon := '';

         INSERT INTO CouponVerification_dbt
              VALUES (m_I.t_fiid,
                      m_I.t_ISIN,
                      m_I.t_LSIN,
                      m_I.RuDataCode,
                      m_I.t_definition,
                      vComment);

         vComment := '';
      END LOOP;

      COMMIT;
   END;
   
     FUNCTION uGetMainObjAttrRecID(p_ObjectType IN dobjatcor_dbt.t_ObjectType%TYPE,
                                p_Object     IN dobjatcor_dbt.t_Object%TYPE,
                                p_GroupID    IN dobjatcor_dbt.t_GroupID%TYPE,
                                p_Date       IN dobjatcor_dbt.t_ValidFromDate%TYPE)
    RETURN number IS
    p_AttrRecID number;
  BEGIN
    BEGIN
      SELECT AtCor.t_ID
        INTO p_AttrRecID
        FROM dobjatcor_dbt AtCor
       WHERE AtCor.t_ObjectType = p_ObjectType
         AND AtCor.t_GroupID = p_GroupID
         AND AtCor.t_Object = p_Object
         AND AtCor.t_ValidToDate >= p_Date
         AND AtCor.t_ValidFromDate <= p_date /*(SELECT MAX(t.t_ValidFromDate)
                                                                            FROM dobjatcor_dbt t
                                                                           WHERE t.t_ObjectType     = p_ObjectType
                                                                             AND t.t_GroupID        = p_GroupID
                                                                             AND t.t_Object         = p_Object
                                                                             AND t.t_ValidFromDate <= p_Date
                                                                             AND t.t_ValidToDate    > p_Date
                                                                         )*/
      ;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        p_AttrRecID := 0;
      WHEN OTHERS THEN
        p_AttrRecID := 0;
    END;
  
    RETURN p_AttrRecID;
  END;

  function uGetChangeAtCorDate(CurAtRecID in integer) return date is
    FunctionResult date;
  
    CurAtCorRec  dobjatcor_dbt%ROWTYPE;
    PrevAtCorRec dobjatcor_dbt%ROWTYPE;
    PrevAtRecID  number;
  begin
    begin
      FunctionResult := to_date('01 01 0001', 'dd mm yyyy');
      select t.*
        into CurAtCorRec
        from dobjatcor_dbt t
       where t.t_id = CurAtRecID;
      dbms_output.put_line(CurAtCorRec.t_id);
      select nvl(max(t.t_id), 0)
        into PrevAtRecID
        from dobjatcor_dbt t
       where t.t_objecttype = CurAtCorRec.t_objecttype
         and t.t_groupid = CurAtCorRec.t_groupid
         and t.t_object = CurAtCorRec.t_object
         and t.t_validfromdate <= CurAtCorRec.t_validfromdate
         and t.t_id != CurAtCorRec.t_id;
    
      if PrevAtRecID != 0 then
        begin
          select t.*
            into PrevAtCorRec
            from dobjatcor_dbt t
           where t.t_id = PrevAtRecID;
          -- FunctionResult := PrevAtCorRec.t_validtodate;
          FunctionResult := CurAtCorRec.t_Validfromdate;
        exception
          when NO_DATA_FOUND then
            return(FunctionResult);
          when OTHERS then
            return(FunctionResult);
        end;
      
      end if;
    
    exception
      when NO_DATA_FOUND then
        FunctionResult := to_date('01 01 0001', 'dd mm yyyy');
      
      when OTHERS then
        FunctionResult := to_date('01 01 0001', 'dd mm yyyy');
    end;
    return FunctionResult;
  end uGetChangeAtCorDate;

  function uGetDateExclusion(p_partyID in integer) return date is
    i              integer;
    all_count      integer;
    close_count    integer;
    Date_esclusion date := to_date('01 01 0001', 'dd mm yyyy');
  begin
  
    select count(1)
      into all_count
      from dobjatcor_dbt
     where t_objecttype = 3
       and t_groupid = 95
       and t_object = LPAD(p_partyID, 10, '0');
  
    select count(1)
      into close_count
      from dobjatcor_dbt
     where t_objecttype = 3
       and t_groupid = 95
       and t_object = LPAD(p_partyID, 10, '0')
       and T_VALIDTODATE != to_date('31.12.9999', 'dd.mm.yyyy');
  
    if all_count = close_count and all_count != 0 then
      select max(T_VALIDTODATE)
        into Date_esclusion
        from dobjatcor_dbt
       where t_objecttype = 3
         and t_groupid = 95
         and t_object = LPAD(p_partyID, 10, '0');
    end if;
    return Date_esclusion;
  end;

  procedure insertDateLog(pOperation in varchar2) is
    pDateTime varchar2(100);
  begin
    select to_char(sysdate, 'DD MONTH YYYY HH24:MI:SS')
      into pDateTime
      from dual;
    insert into dlog values (pOperation, pDateTime);
  end;

  function ConvertNumToBase(pNum  in integer,
                            pBase in varchar2,
                            vStr  out varchar2) return number is
    n      integer;
    rest   integer;
    tmpStr varchar2(10);
  begin
    n      := pNum;
    tmpStr := '';
    loop
      rest   := mod(n, length(pBase));
      n      := trunc(n / length(pBase), 0);
      tmpStr := substr(pBase, rest + 1, 1) || tmpStr;
      exit when n = 0;
    end loop;
    vStr := tmpStr;
    return 1;
  end;
  function uGenerateClientCode_FX(pRefVal in integer, vCode out varchar2)
    return integer is
    pTmpCode varchar2(100);
    pBase_36 varchar2(36) := '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ';
    i        integer;
  begin
    i        := ConvertNumToBase(pRefVal, pBase_36, ptmpCode);
    pTmpCode := 'F502' || case
                  when length(pTmpCode) > 3 then
                   pTmpCode
                  else
                   lpad(pTmpCode, 3, '0')
                end;
  
    vCode := pTmpCode;
    if i = 1 then
      select count(1) into i from ddlcontrmp_dbt where t_mpcode = ptmpCode;
      if i = 0 then
        vCode := pTmpCode;
        return 1;
      end if;
    end if;
    return 0;
  end;

  function uGenerateUnicClientCode_FX(pRefNum       in integer,
                                      pOperDprt     in integer,
                                      pOperDprtNode in integer,
                                      pDate         in date,
                                      vCode         out varchar2)
    return integer is
    nextRefVal integer;
    lCode      varchar2(100) := '';
    i          integer := 0;
    stat       integer := 1;
  
  begin
    RsbSessionData.SetCurdate(TRUNC(pDate));
    RsbSessionData.SetOperDprt(pOperDprt); -- департамент
    RsbSessionData.SetOperDprtNode(pOperDprtNode); -- подразделение
    loop
      stat := RSI_RSB_REFER.WldGenerateReference(nextRefVal,
                                                 pRefNum,
                                                 0,
                                                 0,
                                                 TRUNC(SYSDATE));
      stat := uGenerateClientCode_FX(nextRefVal, lCode);
      exit when stat = 1;
    end loop;
    vCode := lCode;
    return i;
  end;

   
END;
/
