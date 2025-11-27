CREATE OR REPLACE PACKAGE BODY it_vekselrepchod IS
  DATA_NOT_FOUND_ERR CONSTANT NUMBER(5) := -20003;
  INCORRECT_DATA_ERR CONSTANT NUMBER(5) := -20004;
  PAWN_TRANSFERTRATE_DEFAULT_REGVAL_NAME VARCHAR2(50) := 'ВЕКСЕЛЯ БАНКА\РСХБ\МАРЖА ВЕКСЕЛЯ В ЗАЛОГЕ';
  TRANSFERTRATE_CHANGE_DATE_REGVAL_NAME VARCHAR2(50) := 'ВЕКСЕЛЯ БАНКА\РСХБ\ДАТА ИЗМ. ТС';

  -- Подсчитать коэффициент дней для расчета ставки
  FUNCTION GetDaysCoef(p_startdate DATE, p_enddate DATE) RETURN NUMBER AS
    v_calcdatestart DATE;
    v_calcdateend DATE;
    v_currentyear NUMBER;
    v_days365 NUMBER := 0;
    v_days366 NUMBER := 0;
  BEGIN
    v_calcdatestart := p_startdate + 1;
    v_currentyear := EXTRACT(year FROM v_calcdatestart);

     IF EXTRACT(year FROM p_enddate) > v_currentyear THEN
      v_calcdateend := to_date('31.12.'||to_char(v_currentyear), 'DD.MM.YYYY');
    ELSE
      v_calcdateend := p_enddate;
    END IF;

    WHILE v_calcdatestart <= v_calcdateend LOOP
      IF MOD(v_currentyear, 4) = 0 and (MOD(v_currentyear, 100) != 0 or MOD(v_currentyear, 400) = 0) THEN
        v_days366 := v_days366 + v_calcdateend - v_calcdatestart + 1;
      ELSE
         v_days365 := v_days365 + v_calcdateend - v_calcdatestart + 1;
      END IF;

      v_currentyear := v_currentyear + 1;
      v_calcdatestart := to_date('01.01.'||to_char(v_currentyear), 'DD.MM.YYYY');
      v_calcdateend := to_date('31.12.'||to_char(v_currentyear), 'DD.MM.YYYY');

      IF v_calcdateend > p_enddate THEN
        v_calcdateend := p_enddate;
      END IF;
    END LOOP;

    RETURN v_days365/365 + v_days366/366;
  END;

  -- Определить тип субъекта
  FUNCTION GetPartyTypeName(p_partyID dparty_dbt.t_partyid%type) RETURN VARCHAR2 DETERMINISTIC IS
    v_legalform dparty_dbt.t_legalform%type;
    v_isemployer dpersn_dbt.t_isemployer%type;
    v_cnt NUMBER(5);
    v_result VARCHAR2(25);
  BEGIN
    SELECT party.t_legalform
      INTO v_legalform
      FROM dparty_dbt party
     WHERE party.t_partyid = p_partyID;

    IF v_legalform = 2 THEN -- ФЛ
      SELECT persn.t_isemployer
        INTO v_isemployer
        FROM dpersn_dbt persn
       WHERE persn.t_personid = p_partyID;

      IF v_isemployer = 'X' THEN
        v_result := 'ИП';
      ELSE
        v_result := 'Физическое лицо';
      END IF;
    ELSE
      SELECT count(1)
        INTO v_cnt
        FROM dpartyown_dbt po
       WHERE po.t_partyid = p_partyID
         AND po.t_partykind = 2; -- PTK_BANK

      IF v_cnt = 0 THEN
        v_result := 'Юридическое лицо';
      ELSE
        v_result := 'Кредитная организация';
      END IF;
    END IF;

    RETURN v_result;

  EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
  END GetPartyTypeName;

  -- Получить ближайшее к дате значение трансфертной ставки
  PROCEDURE GetLastTransfertRateData(p_marketRateID NUMBER
                                    ,p_searchDate DATE
                                    ,o_transfertRate OUT NUMBER
                                    ,o_transfertRateDate OUT DATE) IS

  BEGIN
    SELECT (val.t_minValue + val.t_maxValue)/2, val.t_beginDate
      INTO o_transfertRate, o_transfertRateDate
      FROM dValIntMarketRate_dbt val
     WHERE val.t_marketRateID = p_marketRateID
       AND val.t_beginDate = (SELECT MAX(t_beginDate)
                                FROM dValIntMarketRate_dbt
                               WHERE t_marketRateID = p_marketRateID
                                 AND t_beginDate <= p_searchDate);
  EXCEPTION WHEN NO_DATA_FOUND THEN
    o_transfertRate := NULL;
    o_transfertRateDate := NULL;
  END;

  -- Получить актуальную зону ответственности для субъекта
  FUNCTION GetPartyResponsibilityZone(p_partyID dparty_dbt.t_partyid%type) RETURN VARCHAR2 DETERMINISTIC IS
    p_result dobjattr_dbt.t_name%type;
  BEGIN
    SELECT oa.t_name
      INTO p_result
      FROM dobjatcor_dbt oac
     INNER JOIN dobjattr_dbt oa
        ON   oa.t_objecttype = oac.t_objecttype
         AND oa.t_groupid = oac.t_groupid
         AND oa.t_attrid = oac.t_attrid
     WHERE oac.t_objecttype = 3
       AND oac.t_groupid = 116
       AND oac.t_object = LPAD(p_partyID, 10, '0');

    RETURN p_result;

  EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
  END GetPartyResponsibilityZone;

  -- Запуск отчета "Отчет ЧОД"
  PROCEDURE ReportRun(p_fileName   IN VARCHAR2
                     ,p_startDate  IN DATE
                     ,p_endDate    IN DATE
                     ,o_GUID       OUT VARCHAR2
                     ,o_errorCode  OUT NUMBER
                     ,o_errorDesc  OUT VARCHAR2) IS
    v_pawnArr JSON_ARRAY_T := JSON_ARRAY_T();
    v_sendedArr JSON_ARRAY_T := JSON_ARRAY_T();
    v_rec JSON_OBJECT_T;
    v_clob CLOB;
    v_clob2 CLOB;
    v_daysCoef NUMBER :=0;
    v_yield NUMBER;
    v_transfertRate NUMBER;
    v_pawnTRDefaultValue FLOAT;
    v_transfertRateChangeDate DATE;

    v_rateSearchDate DATE;
    v_rateDateLowBound DATE;
    v_rateDateHighBound DATE;
    v_rateLowBound NUMBER;
    v_rateHighBound NUMBER;
    v_daysLowBound NUMBER;
    v_daysHighBound NUMBER;
    
    v_MessMeta XMLType;
    v_MsgID itt_q_message_log.msgid%type := it_q_message.get_sys_guid;
  BEGIN
    o_errorCode := 0;
    o_errorDesc := '';
    v_pawnTRDefaultValue := NVL(Rsb_Common.GetRegDoubleValue(PAWN_TRANSFERTRATE_DEFAULT_REGVAL_NAME, 0), 0);
    
    BEGIN
      v_transfertRateChangeDate := to_date(NVL(Rsb_Common.GetRegStrValue(TRANSFERTRATE_CHANGE_DATE_REGVAL_NAME, 0), '01.01.0001'), 'DD.MM.YYYY');
    EXCEPTION WHEN OTHERS THEN
      RAISE_APPLICATION_ERROR(INCORRECT_DATA_ERR, 'Некорректное значение настройки '||TRANSFERTRATE_CHANGE_DATE_REGVAL_NAME);
    END;

    FOR rec IN (WITH marketRates AS (SELECT t_ID, t_FIID, t_contractDays, 0 as t_isNew
                                       FROM dMarketRate_dbt 
                                      WHERE UPPER(t_name) LIKE 'СВ%' AND t_contractDays > 0 AND t_contractDays != 45
                                     UNION
                                     SELECT MAX(t_ID), t_FIID, t_contractDays, 1 as t_isNew
                                       FROM dMarketRate_dbt
                                      WHERE t_contractDays > 0 AND t_interestFrequency = 5 GROUP BY t_fiid, t_contractDays)
                SELECT issuer.t_name t_issuerName, -- A1
                       bnr.t_holderName, -- A2
                       bnr.t_holder,
                       bnr.t_bcseries,   -- A7
                       bnr.t_bcnumber,   -- A8
                       bnr.t_issueDate,
                       leg.t_principal,  -- A10
                       CASE WHEN leg.t_duration = 0 THEN 1
                            ELSE leg.t_duration
                            END t_duration,   -- A14
                       leg.t_formula,
                       leg.t_price,
                       leg.t_point,
                       leg.t_receiptAmount,
                       repay.t_changedate t_repayDate, -- A13.1
                       it_vekselrepchod.GetPartyTypeName(bnr.t_holder) t_holderTypeName, -- A5
                       it_vekselrepchod.GetPartyResponsibilityZone(bnr.t_holder) t_responsibilityZone, -- A6
                       (SELECT t_ID
                          FROM marketRates
                         WHERE t_FIID = leg.t_pfi
                           AND t_isNew = CASE WHEN bnr.t_issuedate < v_transfertRateChangeDate THEN 0 ELSE 1 END
                           AND t_contractDays = (SELECT MAX(t_contractDays)
                                                   FROM marketRates
                                                  WHERE t_FIID = leg.t_pfi
                                                    AND t_isNew = CASE WHEN bnr.t_issuedate < v_transfertRateChangeDate THEN 0 ELSE 1 END
                                                    AND t_contractDays <= DECODE(leg.t_duration, 0, 1, leg.t_duration))) t_daysLowBoundID,
                       (SELECT t_ID
                          FROM marketRates
                         WHERE t_FIID = leg.t_pfi
                           AND t_isNew = CASE WHEN bnr.t_issuedate < v_transfertRateChangeDate THEN 0 ELSE 1 END
                           AND t_contractDays = (SELECT MIN(t_contractDays)
                                                   FROM marketRates
                                                  WHERE t_FIID = leg.t_pfi
                                                    AND t_isNew = CASE WHEN bnr.t_issuedate < v_transfertRateChangeDate THEN 0 ELSE 1 END
                                                    AND t_contractDays >= DECODE(leg.t_duration, 0, 1, leg.t_duration))) t_daysHighBoundID,
                       CASE WHEN holder.t_notresident = 'X' THEN ''
                            ELSE rsi_rsbparty.getpartycode(holder.t_partyid, 16)
                            END t_holderINN, -- A3
                       CASE WHEN holder.t_notresident = 'X' THEN 'Нерезидент'
                            ELSE 'Резидент'
                            END t_holderStatus, -- A4
                       (SELECT t_ccy FROM dfininstr_dbt WHERE t_fiid = leg.t_pfi) t_currency, -- A11
                       CASE WHEN leg.t_pfi = 0 THEN lnk.t_bccost
                            ELSE RSI_RSB_FIInstr.ConvSum(lnk.t_bccost, leg.t_pfi, 0, bnr.t_issueDate, 2)
                            END t_placementSum, -- A12
                       CASE WHEN bnr.t_BCTermFormula = rsb_bill.VS_TERMF_FIXEDDAY
                              OR bnr.t_BCTermFormula = rsb_bill.VS_TERMF_INATIME
                              OR (bnr.t_BCTermFormula = rsb_bill.VS_TERMF_ATSIGHT AND leg.t_maturity >= leg.t_start)
                                 THEN leg.t_maturity
                            WHEN bnr.t_BCTermFormula = rsb_bill.VS_TERMF_DURING AND bnr.t_bcPresentationDate > to_date('01.01.0001', 'DD.MM.YYYY')
                                 THEN bnr.t_bcPresentationDate + leg.t_diff
                            ELSE leg.t_start + rsb_bill_rshb.GetDaysInYearByBasis(leg.t_basis, leg.t_start, 1)
                            END t_planRepayDate, --A13.2
                       holder.t_legalform t_holderlegalform,
                       (SELECT t_newabcstatus
                          FROM dvsbnrbck_dbt WHERE t_id = (
                           SELECT MAX(t_id)FROM (SELECT row_number() OVER(partition by t_bcid, t_changedate ORDER BY t_id DESC) AS rowno, b.*
                                                   FROM dvsbnrbck_dbt b
                                                  WHERE b.t_bcid = bnr.t_bcid
                                                    AND t_bcstatus = 'X'
                                                    AND b.t_changedate <= p_endDate)
                             WHERE rowno = 1 AND t_newabcstatus in (20, 25))) t_bcReportStatus,
                       (SELECT t_newbcstate
                          FROM dvsbnrbck_dbt
                         WHERE t_id = (SELECT MAX(b.t_id) FROM dvsbnrbck_dbt b
                                        WHERE b.t_bcid = bnr.t_bcid
                                          AND b.t_changedate <= p_startDate)) t_bcInState
                  FROM dvsbanner_dbt bnr
                 INNER JOIN dparty_dbt issuer
                    ON issuer.t_partyid = bnr.t_issuer
                 INNER JOIN dparty_dbt holder
                    ON holder.t_partyid = bnr.t_holder
                 INNER JOIN ddl_leg_dbt leg
                    ON leg.t_dealid = bnr.t_bcid AND leg.t_legkind = 1 --LEG_KIND_VSBANNER
                 INNER JOIN dvsordlnk_dbt lnk
                    ON lnk.t_bcid = bnr.t_bcid AND lnk.t_dockind = 109 AND lnk.t_linkkind = 0
                 LEFT JOIN dvsbnrbck_dbt repay
                    ON repay.t_bcid = bnr.t_bcid AND repay.t_newabcstatus = 30
                 WHERE bnr.t_issuedate <= p_endDate
                   AND (bnr.t_repaymentdate = to_date('01.01.0001', 'DD.MM.YYYY')
                     OR bnr.t_repaymentdate > p_startDate
                     OR repay.t_changedate IS null)
                 ORDER BY NVL(t_repayDate, t_planRepayDate), bnr.t_issueDate, bnr.t_bcid)
    LOOP
      IF INSTR(rec.t_bcInState, 'П') > 0 THEN -- Состояние на p_startDate - 'Предъявлен'
        CONTINUE;
      END IF;
      
      -- Доходность при последнем размещении
      v_yield := 0;

      IF rec.t_formula = 1 THEN    -- VS_IN_S_PC простые проценты
        v_yield := rec.t_price / POWER(10, rec.t_point);
      ELSIF rec.t_formula = 0 THEN -- VS_IN_DISCONT дисконт
        v_daysCoef := GetDaysCoef(rec.t_issueDate, NVL(rec.t_repayDate, rec.t_planRepayDate));

        IF v_daysCoef > 0 AND rec.t_receiptAmount > 0 THEN
          v_yield := (rec.t_principal - rec.t_receiptAmount)/rec.t_receiptAmount/v_daysCoef*100;
        END IF;
      END IF;

      -- Трансфертная ставка
      IF rec.t_bcReportStatus = 25 THEN
        v_transfertRate := v_pawnTRDefaultValue;
      ELSIF rec.t_daysLowBoundID = rec.t_daysHighBoundID THEN
        GetLastTransfertRateData(rec.t_daysLowBoundID, rec.t_issueDate, v_transfertRate, v_rateSearchDate);
      ELSE
        BEGIN
          v_rateSearchDate := rec.t_issueDate;

          SELECT t_contractDays INTO v_daysLowBound FROM dMarketRate_dbt WHERE t_ID = rec.t_daysLowBoundID;
          SELECT t_contractDays INTO v_daysHighBound FROM dMarketRate_dbt WHERE t_ID = rec.t_daysHighBoundID;

          LOOP
            GetLastTransfertRateData(rec.t_daysLowBoundID, v_rateSearchDate, v_rateLowBound, v_rateDateLowBound);
            GetLastTransfertRateData(rec.t_daysHighBoundID, LEAST(v_rateSearchDate, v_rateDateLowBound), v_rateHighBound, v_rateDateHighBound);

            EXIT WHEN v_rateDateLowBound = v_rateDateHighBound
                   OR v_rateDateLowBound IS NULL
                   OR v_rateDateHighBound IS NULL;

            v_rateSearchDate := LEAST(v_rateDateLowBound, v_rateDateHighBound);
          END LOOP;
        EXCEPTION WHEN OTHERS THEN
          v_rateLowBound := NULL;
          v_rateHighBound := NULL;
        END;

        IF v_rateLowBound IS NULL OR v_rateHighBound IS NULL THEN
          v_transfertRate := NULL;
        ELSE
          v_transfertRate := v_rateLowBound + (v_rateHighBound - v_rateLowBound)/(v_daysHighBound - v_daysLowBound)*(rec.t_duration - v_daysLowBound);
        END IF;
      END IF;

      SELECT JSON_OBJECT('Branch' IS JSON_OBJECT('Name' IS rec.t_issuerName),
                         'FirstHolder' IS JSON_OBJECT('Name' IS rec.t_holderName,
                                                      'INN' IS '"'||DECODE(rec.t_holderINN, CHR(1), '', rec.t_holderINN)||'"' FORMAT JSON,
                                                      'Status' IS rec.t_holderStatus,
                                                      'Type' IS rec.t_holderTypeName),
                         'ResponsibilityZone' IS JSON_OBJECT('Name' IS '"'||rec.t_responsibilityZone||'"' FORMAT JSON),
                         'Bill' IS JSON_OBJECT('Series' IS rec.t_bcSeries,
                                               'Number' IS '"'||TRUNC(rec.t_bcNumber)||'"' FORMAT JSON,
                                               'Date' IS to_char(rec.t_issueDate, 'DD.MM.YYYY'),
                                               'Nominal' IS JSON_OBJECT('Amount' IS RTRIM(to_char(rec.t_principal, 'FM999999999999990.9999'), '.'),
                                                                        'Currency' IS rec.t_currency),
                                               'LastPlacement' IS JSON_OBJECT('Amount' IS RTRIM(to_char(rec.t_placementSum, 'FM999999999999990.9999'), '.'),
                                                                              'Yield' IS RTRIM(to_char(v_yield, 'FM999999999999990.9999'), '.')),
                                               'Repayment' IS JSON_OBJECT('Date' IS to_char(NVL(rec.t_repayDate, rec.t_planRepayDate), 'DD.MM.YYYY'),
                                                                          'Term' IS rec.t_duration),
                                               'Pledged' IS DECODE(rec.t_bcReportStatus, 20, 'В обращении', 'В залоге'),
                                               'TransferRate' IS DECODE(v_transfertRate, NULL, 'Не найдена', RTRIM(to_char(v_transfertRate, 'FM999999999999990.99'), '.')) )
                         )
        INTO v_clob
        FROM dual;

      v_rec := JSON_OBJECT_T.parse(v_clob);
      
      IF rec.t_bcReportStatus = 20 THEN
        v_sendedArr.append(v_rec);
      ELSE
        v_pawnArr.append(v_rec);
      END IF;
    END LOOP;

    IF v_sendedArr.get_size() = 0 AND v_pawnArr.get_size() = 0 THEN
      RAISE_APPLICATION_ERROR(DATA_NOT_FOUND_ERR, 'Нет векселей за указанный период');
    END IF;
    
    v_clob := v_sendedArr.to_clob();
    v_clob2 := v_pawnArr.to_clob();

    SELECT JSON_OBJECT('DateStart' IS to_char(p_startDate, 'DD.MM.YYYY'),
                       'DateEnd' IS to_char(p_endDate, 'DD.MM.YYYY'),
                       'VekselInfoList' IS JSON_OBJECT('VekselInfoInCirculation' IS v_clob FORMAT JSON,
                                                       'VekselInfoPledged' IS v_clob2 FORMAT JSON)
                       RETURNING CLOB) 
      INTO v_clob
      FROM DUAL;
    
    v_MessMeta := IT_IPS_DFactory.add_KafkaHeader_Xmessmeta( p_List_dllvalues_dbt => 4194
                                                            ,p_traceid => it_q_message.get_sys_guid
                                                            ,p_requestid => v_MsgID
                                                            ,p_templateparams => NULL
                                                            ,p_outputfilename => p_fileName);
    IT_Kafka.load_msg_S3(p_msgid => v_MsgID
                        ,p_message_type => it_q_message.C_C_MSG_TYPE_R
                        ,p_ServiceName => 'IPS_DFACTORY.SendPromissoryReportInfo' 
                        ,p_Receiver => IT_IPS_DFactory.C_C_SYSTEM_NAME
                        ,p_CORRmsgid => NULL
                        ,p_MESSBODY => v_clob  -- JSON c данными   
                        ,p_MessMETA => v_MessMETA
                        ,p_isquery => 0
                        ,o_ErrorCode => o_errorCode
                        ,o_ErrorDesc => o_errorDesc);
    o_GUID := v_MsgID;
  EXCEPTION WHEN OTHERS THEN
    o_errorCode := ABS(sqlcode);
    o_errorDesc := sqlerrm;
  END;

END it_vekselrepchod;
/