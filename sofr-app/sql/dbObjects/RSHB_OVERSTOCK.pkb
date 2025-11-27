CREATE OR REPLACE PACKAGE BODY RSHB_OVERSTOCK IS
/******************************************************************************
   NAME:       RSHB_OVERSTOCK
   PURPOSE:    Формирование остатков по внебирже

   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ------------------------------------
   1.0        25.01.2023   Geraskina-TV       1. Created this package body.
   1.1        31.01.2023   Geraskina-TV       2. Накопительное заполнение таблицы
   1.2        06.02.2023   Geraskina-TV       3. Добавлен параметр p_attribute
   2.0        28.02.2023   Geraskina-TV       4. Изменен порядок получения цены и валюты цены
   2.1        02.03.2023   Geraskina-TV       5. Изменена таблица логов
   2.2        09.03.2023   Geraskina-TV       6. Изменен формат даты для OTCInstrument c 2003-07-22T00:00:00 на 2003-07-22
   3.0        16.03.2023   Geraskina-TV       7. Изменено логирование, добавлено изменение статуса события (перенесено из ws_synch_sofr.mac)
   4.0        30.03.2023   Geraskina-TV       8. Добавлены праметры для OTCInstrument
   5.0        03.05.2023   Geraskina-TV       9. Поменяны местами Definition и Name в OTCInstruments
******************************************************************************/

  FI_TYPE               CONSTANT NUMBER(10) := 9;
  PARTY_TYPE            CONSTANT NUMBER(10) := 3;
  FI_CODEKIND_TICKER    CONSTANT NUMBER(10) := 11;
  PARTY_CODEKIND_ABS    CONSTANT NUMBER(10) := 101;
  PARTY_CODEKIND_MAIN   CONSTANT NUMBER(10) := 1;
  
  STATUS_ERROR CONSTANT NUMBER(10) := 5;
  STATUS_OK CONSTANT NUMBER(10) := 4;

  g_sessionid     NUMBER            := USERENV ('sessionid');
  g_guid          VARCHAR2(32 byte) := SYS_GUID();


  PROCEDURE GatherStat ( ptabname IN VARCHAR2 ) 
  IS
  BEGIN
   dbms_stats.gather_table_stats(ownname => SYS_CONTEXT('USERENV','CURRENT_SCHEMA'),
                                 tabname => ptabname,
                                 estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE,
                                 cascade=>TRUE, 
                                 method_opt=>'FOR ALL COLUMNS SIZE AUTO');
  END;


  PROCEDURE FixTimeStamp ( p_Label_    IN VARCHAR2,
                           p_CalcDate  IN DATE,
                           p_timestart IN TIMESTAMP,
                           p_timeend   IN TIMESTAMP )
  IS
   PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
   INSERT INTO DFILLOVERSTOCKLOG_DBT ( T_SESSIONID,
                                       T_DATE,
                                       T_LABEL,
                                       T_START,
                                       T_END,
                                       T_GUID )
        VALUES ( g_sessionid,
                 p_CalcDate ,
                 substr(p_Label_,1,250) ,
                 NVL (p_timestart, NVL (p_timeend, SYSTIMESTAMP)),
                 NVL (p_timeend,   NVL (p_timestart, SYSTIMESTAMP)),
                 g_guid );
   COMMIT;
  END;
  
    PROCEDURE FixStatusProcessevent ( p_recid in NUMBER,
                           p_status    IN NUMBER, 
                           p_Message in VARCHAR2 
                           )
  IS
   PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
     UPDATE uTableProcessEvent_dbt 
           SET t_status = p_status, 
                  t_lastupdate = sysdate, 
                  t_resulttext = substr(p_Message,1,1000), 
                  t_resultcode = g_guid
        WHERE t_recid = p_recid;       
   COMMIT;
  END;
  

  FUNCTION GetFi_ByRateFin( p_fiid     IN NUMBER,
                            p_DateRate IN DATE,
                            p_mp_mmvb  IN NUMBER,
                            p_mp_spb   IN NUMBER )
  RETURN NUMBER IS
   v_cnt             NUMBER;
   v_valuerate_mmvb  NUMBER;
   v_valuerate_spb   NUMBER;
   v_rateid          NUMBER;
   v_ratedate        DATE;
   v_days            NUMBER;
   v_true_mp         NUMBER := -1;
   t_RD              DRATEDEF_DBT%ROWTYPE;
   v_toFI            NUMBER(10) := -1;
   v_rate            NUMBER;

   c_type_value_mmvb       CONSTANT NUMBER(10) := 17;    -- объем торгов ММВБ
   c_type_value_spb        CONSTANT NUMBER(10) := 35;    -- объем торгов СПБ
   c_type_price            CONSTANT NUMBER(10) := 1;     -- рыночная цена
   c_type_price_bloomberg  CONSTANT NUMBER(10) := 23;    -- цена закрытия Блумберг
  BEGIN
   SELECT count(*) INTO v_cnt
     FROM dratedef_dbt
    WHERE t_otherfi = p_fiid
      AND t_type = 1;

   v_days := p_DateRate - to_date('01.01.0001','DD.MM.YYYY');

   IF v_cnt > 1 THEN
      v_valuerate_mmvb  :=  RSI_RSB_FIINSTR.FI_GetRate(p_fiid, -1, c_type_value_mmvb, p_DateRate, v_days, 0, v_rateid, v_ratedate);
      v_valuerate_spb   :=  RSI_RSB_FIINSTR.FI_GetRate(p_fiid, -1, c_type_value_spb, p_DateRate, v_days, 0, v_rateid, v_ratedate);
      IF v_valuerate_mmvb >= v_valuerate_spb THEN
         v_true_mp := p_mp_mmvb;
      ELSE 
         v_true_mp := p_mp_spb;
      END IF;
   ELSE
      v_true_mp := -1;
   END IF;

   v_rate :=  RSI_RSB_FIINSTR.FI_GetRate(p_fiid, -1, c_type_price, p_DateRate, v_days, 0, v_rateid, v_ratedate, 
                                         false, null, null, null, null, v_true_mp);
   IF v_rate = 0 THEN
      v_rate :=  RSI_RSB_FIINSTR.FI_GetRate(p_fiid, -1, c_type_price_bloomberg, p_DateRate, v_days, 0, v_rateid, v_ratedate, 
                                            false, null, null, null, null, null);
   END IF;

   IF v_rate > 0 THEN 
      SELECT * INTO t_RD
        FROM dratedef_dbt
       WHERE t_RateID = v_rateid;

      IF t_RD.t_OtherFI = p_fiid THEN
         v_toFI := t_RD.t_FIID;
      ELSE
         v_toFI := t_RD.t_OtherFI;
      END IF;
   ELSE 
      v_toFI := -1;
   END IF;
      
   RETURN v_toFI;
   -- а если exception, значит будет exception
  END;
  
  
  FUNCTION GetPartyidByCode( p_code IN VARCHAR2,
                             p_codekind IN NUMBER )
  RETURN NUMBER is
   res NUMBER;
  BEGIN
   SELECT t_objectid INTO res
     FROM dobjcode_dbt
    WHERE t_objecttype = PARTY_TYPE
      AND t_codekind = p_codekind
      AND t_code = p_code 
      AND t_state = 0;
    
   RETURN res;
  EXCEPTION
   WHEN others THEN RETURN -1;
  END;


  FUNCTION GetPriceByPartyANDFiid( p_fiid IN NUMBER,
                                   p_partyid IN NUMBER,
                                   p_PriceFIID IN OUT NUMBER,
                                   p_CalcDate IN DATE )
  RETURN NUMBER IS
   v_DealDate  DATE;
   v_isBond    NUMBER := 0;
   sum_tmp     NUMBER := 0;
   price_tmp   NUMBER := 0;
   amount_tmp  NUMBER := 0;
   sum_all     NUMBER := 0;
   amount_all  NUMBER := 0;
   v_FaceValueFI NUMBER := -1;
   v_relativeprice char(1) ;
  BEGIN
     SELECT t_facevaluefi
        INTO v_FaceValueFI
        FROM dfininstr_dbt
       WHERE t_fiid = p_FIID;

    FOR rec IN (
      SELECT * FROM v_scwrthistex v
       WHERE v.t_state = 1
         AND v.t_amount > 0
         AND v.t_buy_sale IN (0, 3)
         AND v.t_fiid = p_fiid
         AND v.t_party = p_partyid
         AND v.t_INstance =
            (SELECT MAX(bc.t_INstance)
               FROM v_scwrthistex bc
              WHERE bc.t_SumID = v.t_SumID
                AND bc.t_ChangeDATE <= p_CalcDate )
       ) LOOP

      IF rec.t_dockind = 29 THEN
         SELECT tk.t_dealdate INTO v_DealDate
           FROM ddlrq_dbt rq, ddl_tick_dbt tk
          WHERE rq.t_ID = rec.t_DocID
            AND tk.t_DealID = rq.t_docid
            AND tk.t_bofficekind = rq.t_dockind;
      ELSE
         v_DealDate := rec.t_DATE;
         IF p_PriceFIID = -1 THEN
            p_PriceFIID := rec.t_currency;
         END IF;
      END IF;

      IF RSI_RSB_FIINSTR.FI_AvrKindsGetRootByFiID (p_fiid) = RSI_RSB_FIINSTR.AVOIRKIND_BOND THEN
         v_isBond := 1;
      END IF;

      IF rec.t_dockind = 135 AND rec.t_sum = 0 THEN
         -- конвертация
         BEGIN
           SELECT NVL(RSI_RSB_FIINSTR.ConvSum(t.t_price * s.t_denominator / s.t_numerator, 
                                          t.t_cfi, 
                                          p_PriceFIID, 
                                          tt.t_dealDate,
                                          1), 0),
                  t.t_principal
             INTO price_tmp, amount_tmp
             FROM ddl_tick_dbt tt 
           JOIN ddl_leg_dbt t --цена приобретения
             ON t.t_dealid = rec.t_dealid AND t.t_legkind = 0 AND t.t_legid = 0
           JOIN ddl_comm_dbt c --операция конвертации
             ON c.t_documentid = rec.t_docid AND c.t_dockind = rec.t_dockind
           JOIN dscdlfi_dbt s --параметры конвертации
             ON s.t_dealid = c.t_documentid AND s.t_dealkind = c.t_dockind
           WHERE tt.t_dealid = rec.t_dealid;
           EXCEPTION
             WHEN OTHERS THEN
               price_tmp := 0;
               amount_tmp := 0;
         END;
         sum_tmp := price_tmp * amount_tmp;
      ELSIF rec.t_changeDATE = to_date ('31122018', 'ddmmyyyy') AND rec.t_sum = 0 THEN
         --НДФЛ зачисления миграции
         BEGIN
            SELECT sum(price), sum(t_principal)
              INTO price_tmp, amount_tmp
              FROM (SELECT l.t_Cost,
                           l.t_principal,
                           CASE
                              WHEN  v_isBond = 1 AND l.t_relativeprice = CHR (0)
                              THEN
                                 RSI_RSB_FIINSTR.ConvSum (l.t_price,
                                                          l.t_cfi,
                                                          v_FaceValueFI,--p_PriceFIID,
                                                          L.T_START,
                                                          1)
                                 / RSI_RSB_FIINSTR.FI_GETNOMINALONDATE (t.t_pfi, t.t_dealdate)
                                 * 100
                              ELSE
                                 CASE
                                    WHEN v_isBond = 1 THEN l.t_price
                                    ELSE RSI_RSB_FIINSTR.ConvSum (l.t_price,
                                                                  l.t_cfi,
                                                                  p_PriceFIID,
                                                                  L.T_START,
                                                                  1)
                                 END
                           END price
                      FROM v_scwrthistex v,
                           ddlrq_dbt rq,
                           ddl_tick_dbt t,
                           ddl_tick_dbt t2,
                           ddl_leg_dbt l
                     WHERE t_sumid = rec.t_sumid
                       AND v.t_INstance = 0
                       AND rq.t_id = v.t_docid
                       AND rq.t_docid = t.t_dealid
                       AND t.t_clientcontrid = t2.t_clientcontrid
                       AND t.t_pfi = t2.t_pfi
                       AND t2.t_flag3 = CHR (88)
                       AND l.t_dealid = t2.t_dealid
                       AND l.t_legid = 0
                       AND l.t_legkINd = 0);
            IF price_tmp IS NULL THEN
               price_tmp := 0;
               amount_tmp := 0;
            END IF;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               price_tmp := 0;
               amount_tmp := 0;
         END;

         sum_tmp := price_tmp*amount_tmp;

      ELSIF rec.t_sum = 0 THEN
         --НДФЛ зачисление не миграция (ДЕПО-зачисление)
         BEGIN
            SELECT 
               CASE
                  WHEN v_isBond = 1 AND l.t_relativeprice = CHR (0)
                  THEN
                     CASE v_FaceValueFI --p_PriceFIID
                        WHEN -1 THEN l.t_price
                                     / RSI_RSB_FIINSTR.FI_GETNOMINALONDATE (t.t_pfi, t.t_dealdate)
                                     * 100
                        ELSE 
                           RSI_RSB_FIINSTR.ConvSum (l.t_price,
                                                    l.t_cfi,
                                                    v_FaceValueFI,--p_PriceFIID,
                                                    v_DealDate,
                                                    1)
                           / RSI_RSB_FIINSTR.FI_GETNOMINALONDATE (t.t_pfi, t.t_dealdate)
                           * 100
                     END
                  ELSE
                     CASE
                        WHEN v_isBond = 1 THEN l.t_price
                        ELSE 
                           CASE p_PriceFIID
                              WHEN -1 THEN l.t_price
                              ELSE  RSI_RSB_FIINSTR.ConvSum (l.t_price,
                                                             l.t_cfi,
                                                             p_PriceFIID,
                                                             v_DealDate,
                                                             1)
                           END
                     END
               END price,
               CASE p_PriceFIID 
                  WHEN -1 THEN  l.t_cfi
                  ELSE p_PriceFIID
               END pricefiid,
               l.t_principal
              INTO price_tmp, p_PriceFIID, amount_tmp
              FROM v_scwrthistex v,
                   ddlrq_dbt rq,
                   ddl_tick_dbt t,
                   ddl_leg_dbt l
             WHERE t_sumid = rec.t_sumid
               AND v.t_INstance = 0
               AND rq.t_id = v.t_docid
               AND rq.t_docid = t.t_dealid
               AND l.t_dealid = t.t_dealid
               AND l.t_legid = 0
               AND l.t_legkINd = 0;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               price_tmp := 0;
               amount_tmp := 0;
         END;

         sum_tmp := price_tmp*amount_tmp;

      ELSE
         IF p_PriceFIID = -1 THEN
            p_PriceFIID := rec.t_currency;
         END IF;
         amount_tmp := rec.t_amount;

         IF v_isBond = 1 THEN
           BEGIN
               select l.t_relativeprice into v_relativeprice
                  FROM v_scwrthistex v,
                             ddlrq_dbt rq,
                             ddl_tick_dbt t,
                             ddl_leg_dbt l
                       WHERE  t_sumid = rec.t_sumid                 --v_sumid_t0
                             AND v.t_instance = 0
                             AND rq.t_id = v.t_docid
                             AND rq.t_docid = t.t_dealid
                             AND l.t_dealid = t.t_dealid
                             AND l.t_legid = 0
                             AND l.t_legkind = 0 ;
               IF  v_relativeprice = CHR (0) THEN 
                 sum_tmp := RSI_RSB_FIINSTR.ConvSum (rec.t_sum,
                                                    rec.t_currency,
                                                    v_FaceValueFI,
                                                    v_DealDate,
                                                    1)
                           / RSI_RSB_FIINSTR.FI_GETNOMINALONDATE (p_fiid, rec.t_date)
                           * 100;
               ELSE
                 sum_tmp := RSI_RSB_FIINSTR.ConvSum (rec.t_sum,
                                                    rec.t_currency,
                                                    p_PriceFIID,
                                                    v_DealDate,
                                                    1)
                           / RSI_RSB_FIINSTR.FI_GETNOMINALONDATE (p_fiid, rec.t_date)
                           * 100;
               END IF;
          EXCEPTION
          WHEN NO_DATA_FOUND
            THEN
               sum_tmp := 0;
          END;

         ELSE
            sum_tmp := RSI_RSB_FIINSTR.ConvSum (rec.t_sum,
                                                rec.t_currency,
                                                p_PriceFIID,
                                                v_DealDate,
                                                1);
         END IF;
      END IF;

      amount_all := amount_all + amount_tmp;
      sum_all := sum_all + sum_tmp;

   END LOOP;

   IF amount_all > 0 AND sum_all > 0 THEN
      price_tmp := round(sum_all/amount_all,6);
   ELSE 
      price_tmp := 0;
   END IF;

   RETURN price_tmp;
  exception
    when others then 
        FixTimeStamp (
             'Цены p_fiid='||p_fiid||' p_partyid='||p_partyid||' p_PriceFIID='||p_PriceFIID||' '|| sqlerrm||' '|| DBMS_UTILITY.Format_Error_Stack || ' ' || DBMS_UTILITY.Format_Error_Backtrace,
             p_CalcDate,
             SYSTIMESTAMP,
             SYSTIMESTAMP);
        raise;
  END;


  FUNCTION OTCPortfolio( p_party IN VARCHAR2 default null, 
                         p_asset IN VARCHAR2 default null ) 
  RETURN CLOB IS
   res            CLOB;
   sql_statement  VARCHAR2(4000);
   v_partyid      NUMBER(10);
   v_fiid         NUMBER(10);
   v_cur         NUMBER(10);
  BEGIN

   IF p_party IS NOT NULL AND length(p_party) > 1 THEN 
      -- поиск клиента по коду 101 (код АБС)
      v_partyid := GetPartyidByCode(p_party, PARTY_CODEKIND_ABS);
      IF v_partyid = -1 THEN
         RAISE_APPLICATION_ERROR (-20001, 'Не найден клиент по коду '||PARTY_CODEKIND_ABS||' = '||p_party);
      END IF;
   ELSE
      v_partyid := 0;
   END IF;

   -- попробуем определить фин.инструмент
   IF p_asset IS NOT NULL AND length(p_asset) > 1 THEN
      -- поиск валюты 
      v_cur := -1;
      BEGIN
         SELECT t_fiid INTO v_cur
           FROM dfininstr_dbt 
          WHERE t_ccy = p_asset
               AND t_fi_kind = 1;
      EXCEPTION
         WHEN no_data_found THEN 
            v_cur := -1;
      END;
      
      if v_cur = -1 then
          v_fiid := 0;
          -- поиск по ISIN
          BEGIN
             SELECT t_fiid INTO v_fiid
               FROM davoiriss_dbt 
              WHERE t_isIN = p_asset;
          EXCEPTION
             WHEN no_data_found THEN 
                v_fiid := 0;
          END;

          IF v_fiid = 0 THEN
             -- поиск по номеру гос.регистрации
             BEGIN
                SELECT t_fiid INTO v_fiid
                  FROM davoiriss_dbt 
                 WHERE t_lsIN = p_asset;
             EXCEPTION
                WHEN no_data_found THEN 
                   v_fiid := 0;
             END;
          END IF;

          IF v_fiid = 0 THEN
             -- поиск по коду ФИ в кодировке ММВБ
             BEGIN
                SELECT t_objectid INTO v_fiid
                  FROM dobjcode_dbt
                 WHERE t_objecttype = FI_TYPE 
                   AND t_codekind = FI_CODEKIND_TICKER
                   AND t_code = p_asset
                   AND t_state = 0;
             EXCEPTION
                WHEN no_data_found THEN 
                   v_fiid := 0;
             END;
          END IF;
      END IF;

      IF v_fiid = 0 and v_cur = -1 THEN
         RAISE_APPLICATION_ERROR (-20002, 'Не найден финансовый инструмент = '||p_asset);
      END IF;
   ELSE 
      v_fiid := 0;
   END IF;

   /* json_query ( ... , '$' RETURNING CLOB PRETTY ) используется для структурирования json, иначе он выглядит как длинная строка,
      использовать при отладке, для штатной работы не нужен */ 
   sql_statement := 
      'SELECT 
      /*json_query (  */
         replace(
            json_arrayagg (
               JSON_OBJECT(
                  key ''ClientID''  value info.t_partyid,
                  key ''ekk''       value info.t_ekk,
                  key ''cftID''     value info.t_cftid,
                  key ''ISIN''      value r.t_isIN,
                  key ''Regnum''    value r.t_lsIN,
                  key ''Ticker''    value r.t_ticker,
                  key ''ccyCD''     value r.t_ccy,
                  key ''Amount''    value (CASE WHEN r.t_ccy IS NOT NULL THEN r.t_money306 ELSE r.t_amount END),
                  key ''Price''     value r.t_price,
                  key ''PriceCCY''  value r.t_priceccy
                  ) format json RETURNING CLOB
               )
            , ''\u0001'','''')
            /*, ''$'' RETURNING CLOB PRETTY )*/
             as json_doc
        FROM DOVERSTOCK_REST_DBT r, 
             DOVERSTOCK_CLIENTINFO_DBT info
       WHERE r.t_dlcontrid = info.t_dlcontrid AND r.t_sfcontrid = info.t_sfcontrid ';
   IF v_partyid > 0 THEN
      sql_statement := sql_statement || ' AND info.t_partyid = '||v_partyid;
   END IF;
   IF v_cur > -1 THEN
      sql_statement := sql_statement || ' AND r.t_code_currency = '||v_cur;
   END IF;

   IF v_fiid > 0 THEN
      sql_statement := sql_statement || ' AND r.t_fiid = '||v_fiid;
   END IF;
   

   EXECUTE IMMEDIATE sql_statement INTO res;

   IF nvl(DBMS_LOB.GETLENGTH(res),0) = 0 THEN
      RAISE_APPLICATION_ERROR (-20003, 'Нет данных');
   END IF;

   RETURN res;
  /*EXCEPTION
      WHEN others THEN 
         v_code := SQLCODE;
         v_text := SQLERRM;
         EXECUTE IMMEDIATE 'SELECT json_object ( key ''error'' value :c||'' | ''||:t ) FROM dual' INTO res usINg v_code, v_text;
         RETURN res; */
  END;


  /* Ценные бумаги */
  FUNCTION OTCINstruments( p_asset     IN VARCHAR2 default null,
                           p_attribute IN VARCHAR2 default null) 
  RETURN CLOB IS
   res CLOB;
   sql_statement VARCHAR2(4000);
   sql_buf VARCHAR2(4000);
   sql_list VARCHAR2(4000);
   v_fiid NUMBER(10);
  BEGIN
   -- попробуем определить фин.инструмент
   IF p_asset IS NOT NULL AND length(p_asset) > 1 THEN
      -- поиск по ISIN
      BEGIN
         SELECT t_fiid INTO v_fiid
           FROM davoiriss_dbt 
          WHERE t_isIN = p_asset;
      EXCEPTION
         WHEN no_data_found THEN 
            RAISE_APPLICATION_ERROR (-20002, 'Не найден финансовый инструмент по ISIN = '||p_asset);
      END;
   ELSE 
      v_fiid := 0;
   END IF;

   IF UPPER(p_attribute) NOT IN ('ISSUER','NAME','DEFINITION','FI_KIND','AVOIRKIND',
                                 'SUMPRECISION','FACEVALUE','FACEVALUEFI','FIQUOTE','ISSUEDDATE',
                                 'DRAWINGDATE') THEN
      RAISE_APPLICATION_ERROR (-20004, 'Неизвестный атрибут = '||p_attribute);
   END IF;

   /* отбор инструментов */
   sql_statement := 
        'SELECT av.t_fiid, av.t_ISIN, av.t_LSIN, fin.t_issuer,
               (SELECT t_shortname FROM dparty_dbt p WHERE p.t_partyid = fin.t_issuer) issuer,
               (SELECT t_code FROM dobjcode_dbt 
                 WHERE t_objecttype = :FI_TYPE AND t_codekind = :FI_CODEKIND_TICKER
                   AND t_objectid = av.t_fiid AND t_state = 0 ) ticker,
               fin.t_definition, fin.t_name, fin.t_fi_kind, 
               (SELECT t_name FROM dfikinds_dbt k WHERE k.t_fi_kind =  fin.t_fi_kind) fi_kind,
               fin.t_avoirkind,
               (SELECT t_name FROM davrkinds_dbt avk WHERE avk.t_fi_kind =  fin.t_fi_kind AND avk.t_avoirkind =  fin.t_avoirkind ) avoirkind,
               fin.t_sumprecision, fin.t_facevalue, 
               (SELECT t_ccy FROM dfininstr_dbt f WHERE t_fiid = fin.t_facevaluefi) facevaluefi,
               to_char(fin.t_issued,''yyyy-mm-dd'') issued, to_char(fin.t_drawingdate,''yyyy-mm-dd'') drawingdate
           FROM davoiriss_dbt av, dfininstr_dbt fin
          WHERE fin.t_fiid = av.t_fiid 
            AND fin.t_fi_kind = 2
            AND fin.t_isclosed = chr(0)
            AND fin.t_issys = chr(0)';

   sql_list := 
   '           key ''Issuer''        value issuer,
               key ''Name''          value t_name,
               key ''Definition''    value t_definition,
               key ''Fi_Kind''       value fi_kind,
               key ''AvoirKind''     value avoirkind,
               key ''Sumprecision''  value t_sumprecision,
               key ''FaceValue''     value t_facevalue,
               key ''FaceValuefi''   value facevaluefi,
               key ''FiQuote''       value facevaluefi,
               key ''IssuedDate''    value issued,
               key ''DrawingDate''   value drawingdate';
   IF v_fiid > 0 THEN
      sql_statement := sql_statement || ' AND fin.t_fiid = '||v_fiid;
      -- Будем использовать ключи в точности, как описано в ТЗ, поэтому длинное условие
      IF p_attribute IS NOT NULL AND length(p_attribute) > 1 THEN
         CASE UPPER(p_attribute)
            WHEN 'ISSUER'        THEN sql_list := 'key ''Issuer''        value issuer ';
            WHEN 'NAME'          THEN sql_list := 'key ''Name''          value t_name';
            WHEN 'DEFINITION'    THEN sql_list := 'key ''Definition''    value t_definition';
            WHEN 'FI_KIND'       THEN sql_list := 'key ''Fi_Kind''       value fi_kind';
            WHEN 'AVOIRKIND'     THEN sql_list := 'key ''AvoirKind''     value avoirkind';
            WHEN 'SUMPRECISION'  THEN sql_list := 'key ''Sumprecision''  value t_sumprecision';
            WHEN 'FACEVALUE'     THEN sql_list := 'key ''FaceValue''     value t_facevalue';
            WHEN 'FACEVALUEFI'   THEN sql_list := 'key ''FaceValuefi''   value facevaluefi';
            WHEN 'FIQUOTE'       THEN sql_list := 'key ''FiQuote''       value facevaluefi';
            WHEN 'ISSUEDDATE'    THEN sql_list := 'key ''IssuedDate''    value issued';
            WHEN 'DRAWINGDATE'   THEN sql_list := 'key ''DrawingDate''   value drawingdate';
         END CASE;
      END IF;
   ELSE
      sql_buf := q'[
         MERGE INTO DOVERSTOCK_METRICS_DBT m
              USING (SELECT TRUNC (SYSDATE) AS t_DATE,
                            'INSTRUMENT_COUNT' AS T_METRICNAME,
                            (SELECT COUNT (*)
                               FROM (]' || sql_statement || q'[))
                               AS t_Count
                       FROM DUAL) d
                 ON (m.t_DATE = d.t_DATE AND m.T_METRICNAME = d.T_METRICNAME)
         WHEN MATCHED
         THEN
            UPDATE SET m.T_NUMBERVALUE = d.t_Count
         WHEN NOT MATCHED
         THEN
            INSERT     (m.T_DATE, m.T_METRICNAME, m.T_NUMBERVALUE)
                VALUES (d.t_DATE, d.T_METRICNAME, d.t_Count)
      ]';
      EXECUTE IMMEDIATE sql_buf using FI_TYPE, FI_CODEKIND_TICKER;
   END IF;

   /* json_query ( ... , '$' RETURNINg clob pretty ) используется для структурирования json, иначе он выглядит как длинная строка,
      использовать при отладке, для штатной работы не нужен */ 
   sql_statement := 
      'SELECT 
     /*json_query ( */
         replace(
            json_arrayagg (
               JSON_OBJECT(
                  key ''ISIN''          value t_ISIN,
                  key ''Regnum''    value t_LSIN,
                  key ''Ticker''    value ticker,'
                  ||sql_list||
               ') format json RETURNINg clob 
             )
          , ''\u0001'','''')
         /*, ''$'' RETURNINg clob pretty ) */
          as json_doc 
         FROM (' ||sql_statement||' )';

   EXECUTE IMMEDIATE sql_statement INTO res using FI_TYPE, FI_CODEKIND_TICKER;

   IF nvl(DBMS_LOB.GETLENGTH(res),0) = 0 THEN
      RAISE_APPLICATION_ERROR (-20003, 'Нет данных');
   END IF;

   RETURN res;

 /* EXCEPTION
      WHEN others THEN 
         v_code := SQLCODE;
         v_text := SQLERRM;
         EXECUTE IMMEDIATE 'SELECT json_object ( key ''error'' value :c||'' | ''||:t ) FROM dual' INTO res usINg v_code, v_text;
         RETURN res; */
  END;


  /* Заполнение служебной таблицы данными по договорам */
  PROCEDURE FillContrTable ( p_CalcDate IN DATE, p_recid in NUMBER ) 
  IS
   v_Restdate     DATE;
   v_Restbegin    DATE;
   v_cnt          NUMBER         := 0;
   v_lastUpdate   DATE           := SYSDATE;
   v_sql          VARCHAR2(4000);
   v_sql_where    VARCHAR2(4000);
   v_ts           TIMESTAMP;
   v_MMVB_Code    VARCHAR2(35);
   v_SPB_Code     VARCHAR2(35);
   v_mp_mmvb      NUMBER;
   v_mp_spb       NUMBER;
   
   TYPE rests_temp_rec IS RECORD (
      t_fiid      NUMBER, 
      t_partyid   NUMBER, 
      t_begdate_w DATE, 
      t_price_currency NUMBER,
      t_ccy       VARCHAR2(3 char),
      t_price     NUMBER
   );
   
   TYPE rests_t IS TABLE OF rests_temp_rec
   INDEX BY PLS_INTEGER;
   v_rests rests_t;

  BEGIN
   g_sessionid   := USERENV ('sessionid');
   g_guid          := SYS_GUID();

   v_Restdate := p_CalcDate;
   --DELETE FROM DOVERSTOCK_CLIENTINFO_DBT;
   -- будем накапливать данные 
   FixTimeStamp (
      'Начало заполнения таблицы DOVERSTOCK_CLIENTINFO_DBT',
      p_CalcDate,
      SYSTIMESTAMP,
      SYSTIMESTAMP);

   SELECT count(*) INTO v_cnt 
     FROM DOVERSTOCK_CLIENTINFO_DBT
    WHERE rownum = 1;

   v_ts := SYSTIMESTAMP;
   v_sql := 
   'INSERT INTO DOVERSTOCK_CLIENTINFO_DBT
    SELECT sfcontr.t_id,
           sfcontr.t_partyid,
           p.t_legalform,
           dlc.t_dlcontrid,
           sfcontr.t_ServKINd,
           sfcontr.t_ServKINdSub,
           chr(1) as T_EKK,
           chr(1) as T_CFTID,
           SYSDATE
      FROM dsfcontr_dbt sfcontr,
           ddlcontrmp_dbt mp,
           ddlcontr_dbt dlc,
           dparty_Dbt p
     WHERE sfcontr.t_ID > 0
       AND sfcontr.t_ServKINd = 1
       AND sfcontr.t_ServKINdSub = 9
       AND sfcontr.t_partyid = p.t_partyid
       AND mp.t_SfContrID = sfcontr.t_ID
       AND dlc.t_DlContrID = mp.t_DlContrID
       AND p.t_legalform = 2
       AND (sfcontr.t_DATEclose = to_date(''01010001'',''ddmmyyyy'') or sfcontr.t_DATEclose > :p_CalcDate)';

   v_sql_WHERE := 
   '   AND not exists (SELECT 1 FROM DOVERSTOCK_CLIENTINFO_DBT WHERE t_sfcontrid = sfcontr.t_ID)';

   IF v_cnt = 0 THEN
      EXECUTE IMMEDIATE v_sql USING p_CalcDate;

      FixTimeStamp (
         'Заполнена полностью DOVERSTOCK_CLIENTINFO_DBT',
         p_CalcDate,
         v_ts,
         SYSTIMESTAMP);
      v_ts := SYSTIMESTAMP;
      GatherStat('DOVERSTOCK_CLIENTINFO_DBT');
      FixTimeStamp (
         'Собрана статистика DOVERSTOCK_CLIENTINFO_DBT',
         p_CalcDate,
         v_ts,
         SYSTIMESTAMP);
   ELSE 
      v_ts := SYSTIMESTAMP;
      SELECT nvl(max (t_lastupDATE), SYSDATE)
        INTO v_lastUpdate
        FROM DOVERSTOCK_CLIENTINFO_DBT;
        
      v_lastUpdate := trunc(v_lastUpdate);
      v_lastUpdate := add_months(v_lastUpdate,-1);
      
      -- удаление закрытых субдоговоров
      DELETE FROM DOVERSTOCK_CLIENTINFO_DBT 
       WHERE t_sfcontrid IN (
         SELECT t_id 
           FROM dsfcontr_dbt sf,
                DOVERSTOCK_CLIENTINFO_DBT dl
          WHERE sf.t_servkind = 1 
            AND sf.t_servkindsub = 9
            AND sf.t_dateclose >= v_lastUpdate
            AND sf.t_id = dl.t_sfcontrid
       );

      FixTimeStamp (
         'Удалены старые субдоговора',
         p_CalcDate,
         v_ts,
         SYSTIMESTAMP);
      
      v_ts := SYSTIMESTAMP;
      -- удаление закрытых договоров БО, хотя субдоговора тоже должны быть закрыты и должны попадать в предыдущий запрос
      DELETE FROM DOVERSTOCK_CLIENTINFO_DBT 
       WHERE t_dlcontrid IN (
         SELECT dlc.t_dlcontrid
           FROM dsfcontr_dbt sfcontr,
                ddlcontr_dbt dlc
          WHERE sfcontr.t_ID = dlc.t_sfcontrid  
            AND sfcontr.t_DATEclose >= v_lastUpdate
            );
      FixTimeStamp (
         'Удалены старые договора БО',
         p_CalcDate,
         v_ts,
         SYSTIMESTAMP); 

      v_ts := SYSTIMESTAMP;

      -- вставка новых договоров
      EXECUTE IMMEDIATE v_sql||v_sql_WHERE USING p_CalcDate;
      
      FixTimeStamp (
         'Вставлены новые договора',
         p_CalcDate,
         v_ts,
         SYSTIMESTAMP); 
               
      v_ts := SYSTIMESTAMP;
      GatherStat('DOVERSTOCK_CLIENTINFO_DBT');
      FixTimeStamp (
         'Собрана статистика DOVERSTOCK_CLIENTINFO_DBT',
         p_CalcDate,
         v_ts,
         SYSTIMESTAMP); 
   END IF;

   v_ts := SYSTIMESTAMP;
   /* ЕКК, CFTID */
   MERGE INTO DOVERSTOCK_CLIENTINFO_DBT c
     USING (  SELECT (SELECT T_CODE
                        FROM DDLOBJCODE_DBT
                       WHERE t_codekind = 1 AND T_OBJECTTYPE = 207
                         AND (T_BANKCLOSEDATE = to_date ('01010001', 'ddmmyyyy') OR T_BANKCLOSEDATE >= p_CalcDate)
                         AND T_OBJECTID = c.T_DLCONTRID) ekk,
                     c.t_dlcontrid,
                     c.t_partyid,
                     (SELECT T_CODE
                        FROM dobjcode_dbt obj
                       WHERE obj.t_objecttype = 3 AND obj.t_objectid = c.t_partyid 
                         AND obj.t_codekind = 101 AND obj.t_state = 0) cftid
                FROM DOVERSTOCK_CLIENTINFO_DBT c 
            GROUP BY c.t_dlcontrid, c.t_partyid ) d
        ON (c.t_dlcontrid = d.t_dlcontrid)
   WHEN MATCHED THEN
      UPDATE SET c.t_ekk = d.ekk,
                 c.t_cftid = d.cftid;
   FixTimeStamp (
      'Доп.информация по договорам',
      p_CalcDate,
      v_ts,
      SYSTIMESTAMP);

   /* Счета 306* */
   v_ts := SYSTIMESTAMP;
   /* Удаление закрытых*/
   DELETE FROM DOVERSTOCK_REST_DBT 
    WHERE t_accountid IN (
         SELECT acc.t_accountid
           FROM daccount_dbt acc
          WHERE acc.t_Chapter = 1
            AND acc.t_Account LIKE '306%'
            AND acc.t_close_DATE  BETWEEN v_lastUpdate AND  p_CalcDate
            AND acc.t_client NOT IN (SELECT d.t_PartyID FROM ddp_dep_dbt d));
   FixTimeStamp (
      'Удаление закрытых счетов 306*',
      p_CalcDate,
      v_ts,
      SYSTIMESTAMP); 

   v_ts := SYSTIMESTAMP;
   /* Вставка новых счетов */
   MERGE INTO DOVERSTOCK_REST_DBT c
   USING ( 
      SELECT /*+ ordered parallel(4) index(accdoc DMCACCDOC_DBT_IDXC)*/   DISTINCT doc.t_sfcontrid,
            doc.t_partyid,
            doc.t_dlcontrid,
            acc.t_AccountID,
            acc.t_Client,
            acc.t_Account,
            acc.t_Code_Currency,
            acc.t_chapter,
            0  AS t_Money306,
            curr.t_CCY,
            SYSDATE as t_lastupdate
        FROM  DOVERSTOCK_CLIENTINFO_DBT doc
          join dmcaccdoc_dbt accdoc on doc.t_sfcontrid = accdoc.t_ClientContrID AND doc.t_partyid = accdoc.t_owner 
          join dfininstr_dbt curr on  curr.t_FIID = accdoc.t_Currency AND curr.t_fi_kind = 1
          join daccount_dbt acc on accdoc.t_Chapter = acc.t_Chapter AND accdoc.t_Account = acc.t_Account AND accdoc.t_Currency = acc.t_Code_Currency
       WHERE accdoc.t_Chapter = 1
         AND accdoc.t_catid = 70 /*ДС Клиента*/
         AND accdoc.t_Account LIKE '306%'
         AND (accdoc.t_disablingdate = to_date ('01.01.0001', 'DD.MM.YYYY') OR accdoc.t_disablingdate >= p_CalcDate)
         AND acc.t_client NOT IN (SELECT d.t_PartyID FROM ddp_dep_dbt d)
         AND acc.t_open_date <= v_Restdate
         AND (acc.t_close_date = to_date ('01.01.0001', 'DD.MM.YYYY') OR acc.t_close_date >= p_CalcDate)
         ) s
      ON (c.t_sfcontrid = s.t_sfcontrid AND c.t_dlcontrid = s.t_dlcontrid AND  c.t_accountid = s.t_accountid) 
   WHEN NOT MATCHED THEN
      INSERT (c.t_sfcontrid, c.t_partyid, c.t_dlcontrid, c.t_accountid, c.t_client, 
              c.t_account, c.t_code_currency, c.t_chapter, c.t_money306, c.t_ccy, 
              c.t_lastupdate)
      VALUES (s.t_sfcontrid, s.t_partyid, s.t_dlcontrid, s.t_accountid, s.t_client, 
              s.t_account, s.t_code_currency, s.t_chapter, s.t_money306, s.t_ccy, 
              s.t_lastupdate);
   FixTimeStamp (
      'Вставка новых счетов 306*',
      p_CalcDate,
      v_ts,
      SYSTIMESTAMP); 

   v_ts := SYSTIMESTAMP;
   GatherStat('DOVERSTOCK_REST_DBT');
   FixTimeStamp (
      'Собрана статистика DOVERSTOCK_REST_DBT',
      p_CalcDate,
      v_ts,
      SYSTIMESTAMP);
      
   SELECT count(*) INTO v_cnt 
    FROM DOVERSTOCK_REST_DBT
   WHERE t_Money306 > 0 
     AND rownum = 1;

   IF v_cnt = 0 THEN
      v_Restbegin := to_date('01011970','ddmmyyyy');
   ELSE 
      v_Restbegin := add_months(v_Restdate,-3);
   END IF;
   
   v_ts := SYSTIMESTAMP;
   /* Остатки по счетам 306 */
   MERGE INTO DOVERSTOCK_REST_DBT c
     USING (SELECT c.t_accountid, c.t_code_currency, r.t_rest, c.t_sfcontrid
              FROM DOVERSTOCK_REST_DBT c, drestdate_dbt r
             WHERE c.t_accountid = r.t_accountid
               AND c.t_code_currency = r.t_restcurrency  
               AND t_restdate = (SELECT MAX (t_restdate)
                                   FROM drestdate_dbt t
                                  WHERE t.t_accountid = r.t_accountid
                                    AND t.t_restcurrency = r.t_restcurrency
                                    AND t_restdate BETWEEN v_Restbegin AND v_Restdate)) s
        ON (c.t_accountid = s.t_accountid AND c.t_code_currency = s.t_code_currency AND c.t_sfcontrid = s.t_sfcontrid ) 
   WHEN MATCHED THEN
      UPDATE SET c.t_Money306 = s.t_rest, c.t_lastupDATE = SYSDATE;

   FixTimeStamp (
      'Остатки по счетам 306*',
      p_CalcDate,
      v_ts,
      SYSTIMESTAMP);

   v_ts := SYSTIMESTAMP;
   /* Лоты */
   MERGE INTO DOVERSTOCK_REST_DBT c
     USING ( 
        SELECT info.t_sfcontrid,
               info.t_partyid,
               info.t_dlcontrid,
               wrtcl.t_fiid,
               av.t_isin,
               av.t_lsin,
               (SELECT o.t_code FROM dobjcode_dbt o 
                 WHERE o.t_objecttype = FI_TYPE AND o.t_codekind = FI_CODEKIND_TICKER 
                   AND o.t_objectid = fin.t_fiid AND o.t_state = 0) as t_ticker,
               wrtcl.t_amount,
               null as t_price,
               fin.t_facevaluefi as t_price_currency,
               chr(1) as t_priceccy,
               wrtcl.t_begdate as t_begdate_w,
               SYSDATE as t_lastupdate
          FROM DPMWRTCL_DBT wrtcl,
               DOVERSTOCK_CLIENTINFO_DBT info,
               davoiriss_dbt av, 
               dfininstr_dbt fin
         WHERE wrtcl.t_contract = info.t_sfcontrid
           AND wrtcl.t_party = info.t_partyid
           AND wrtcl.t_fiid = fin.t_fiid
           AND fin.t_fiid = av.t_fiid 
           AND fin.t_fi_kind = 2
           AND fin.t_isclosed = chr(0)
           AND fin.t_issys = chr(0)
           AND wrtcl.t_begdate <= p_CalcDate 
           AND wrtcl.t_enddate >= p_CalcDate
           ) s
      ON (c.t_sfcontrid = s.t_sfcontrid AND c.t_dlcontrid = s.t_dlcontrid AND c.t_fiid = s.t_fiid) 
   WHEN NOT MATCHED THEN
      INSERT (c.t_sfcontrid, c.t_partyid, c.t_dlcontrid, c.t_fiid, c.t_isin, 
              c.t_lsin, c.t_ticker, c.t_amount, c.t_price, c.t_price_currency,
              c.t_priceccy, c.t_begdate_w, c.t_lastupdate)
      VALUES (s.t_sfcontrid, s.t_partyid, s.t_dlcontrid, s.t_fiid, s.t_isin, 
              s.t_lsin, s.t_ticker, s.t_amount, s.t_price, s.t_price_currency, 
              s.t_priceccy, s.t_begdate_w, s.t_lastupdate)
   WHEN MATCHED THEN 
         UPDATE SET c.t_amount = s.t_amount, 
                    c.t_lastupdate = SYSDATE,
                    c.t_begdate_w_prev = (case 
                                             when s.t_begdate_w <> c.t_begdate_w then c.t_begdate_w
                                             else c.t_begdate_w_prev
                                          end), 
                    c.t_begdate_w = s.t_begdate_w;

   --для отсутствующих клиентских лотов обнулим остаток
   UPDATE DOVERSTOCK_REST_DBT rest
      SET REST.T_AMOUNT = 0,REST.t_Price = 0, REST.T_LASTUPDATE = SYSDATE
    WHERE NOT EXISTS
                 (SELECT 1
                    FROM DPMWRTCL_DBT wrtcl
                   WHERE     WRTCL.T_FIID = REST.T_FIID
                         AND WRTCL.T_CONTRACT = REST.T_SFCONTRID
                         AND WRTCL.T_PARTY = REST.T_PARTYID
                         AND WRTCL.T_BEGDATE <= p_CalcDate
                         AND WRTCL.T_ENDDATE >= p_CalcDate)
          AND REST.T_FIID IS NOT NULL;

   FixTimeStamp (
      'Лоты',
      p_CalcDate,
      v_ts,
      SYSTIMESTAMP);
      
   SELECT count(*) INTO v_cnt 
    FROM DOVERSTOCK_REST_DBT
   WHERE t_amount IS NOT NULL 
      AND rownum = 1;

   IF v_cnt = 0 THEN
      v_Restbegin := to_date('01011970','ddmmyyyy');
   ELSE 
      v_Restbegin := add_months(p_CalcDate,-3);
   END IF;

   /* Цена лотов и валюта */
   v_ts := SYSTIMESTAMP;
   v_MMVB_Code := trim(rsb_common.GetRegStrValue('SECUR\MICEX_CODE', 0));
   v_SPB_Code  := trim(rsb_common.GetRegStrValue('SECUR\SPBEX_CODE', 0));
    
   v_mp_mmvb := GetPartyidByCode(v_MMVB_Code, PARTY_CODEKIND_MAIN);
   v_mp_spb :=  GetPartyidByCode(v_SPB_Code, PARTY_CODEKIND_MAIN);
   
   SELECT distinct t_fiid, t_partyid, t_begdate_w, t_price_currency, null, null
   BULK COLLECT INTO v_rests
   FROM (
      SELECT t_fiid, t_partyid, t_begdate_w, t_price_currency
        FROM DOVERSTOCK_REST_DBT 
       WHERE t_fiid IS NOT NULL AND t_price IS NULL
         union
      SELECT t_fiid, t_partyid, t_begdate_w, t_price_currency
        FROM DOVERSTOCK_REST_DBT 
       WHERE t_fiid IS NOT NULL AND t_begdate_w <> nvl(t_begdate_w_prev, v_Restbegin)
         union
      SELECT t_fiid, t_partyid, t_begdate_w, t_price_currency
        FROM DOVERSTOCK_REST_DBT 
       WHERE t_fiid IS NOT NULL AND t_begdate_w = v_Restbegin
       );

   if v_rests.count > 0 then
       FOR i IN v_rests.FIRST .. v_rests.LAST LOOP
          v_rests(i).t_price_currency := GetFi_ByRateFin( v_rests(i).t_fiid, 
                                                          v_rests(i).t_begdate_w, 
                                                          v_mp_mmvb, 
                                                          v_mp_spb);
          v_rests(i).t_price := GetPriceByPartyAndFiid(v_rests(i).t_fiid, 
                                                       v_rests(i).t_partyid, 
                                                       v_rests(i).t_price_currency, 
                                                       p_CalcDate);

          IF v_rests(i).t_price_currency >= 0 THEN
             SELECT t_ccy INTO v_rests(i).t_ccy
               FROM dfininstr_dbt curr 
              WHERE curr.t_FIID = v_rests(i).t_price_currency AND curr.t_fi_kind = 1;
          END IF;
       END LOOP;

       FORALL i IN v_rests.FIRST .. v_rests.LAST 
          UPDATE DOVERSTOCK_REST_DBT 
             SET t_price = v_rests(i).t_price,
                 t_price_currency = v_rests(i).t_price_currency, 
                 t_priceccy = v_rests(i).t_ccy, 
                 t_begdate_w_prev = t_begdate_w,
                 t_lastupdate = SYSDATE
          WHERE t_fiid = v_rests(i).t_fiid 
            AND t_partyid = v_rests(i).t_partyid;
   end if;

   FixTimeStamp (
      'Цены',
      p_CalcDate,
      v_ts,
      SYSTIMESTAMP);

      FixStatusProcessevent(p_recid, STATUS_OK, 'OK');
     
   COMMIT;
  EXCEPTION
   WHEN others THEN 
      FixTimeStamp (
         sqlerrm||' '|| DBMS_UTILITY.Format_Error_Stack || ' ' || DBMS_UTILITY.Format_Error_Backtrace,
         p_CalcDate,
         v_ts,
         SYSTIMESTAMP);
      FixStatusProcessevent(p_recid, STATUS_ERROR, sqlerrm);
      ROLLBACK; 
      
  END;

END RSHB_OVERSTOCK;
/
