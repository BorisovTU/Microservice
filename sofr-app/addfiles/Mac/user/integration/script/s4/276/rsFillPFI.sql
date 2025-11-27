CREATE OR REPLACE PROCEDURE rsFillPFI (pdate        IN DATE,
                                       clear_date   IN BOOLEAN DEFAULT TRUE)
IS
   rec_PFI                   tRSHB_PFI_PKL_2CHD%ROWTYPE;
   RepDate                   DATE;

   fininstr                  dfininstr_dbt%ROWTYPE;
   prev_fininstr             NUMBER;
   avoiriss                  davoiriss_dbt%ROWTYPE;
   dl_tick                   ddl_tick_dbt%ROWTYPE;
   dl_leg                    ddl_leg_dbt%ROWTYPE;

   ZeroDate         CONSTANT DATE := TO_DATE ('01.01.0001', 'dd.mm.yyyy');
   vid                       NUMBER;

   dealtype                  VARCHAR2 (25);
   dealkind                  VARCHAR2 (25);

   ost_tr                    NUMBER;
   ost_ob                    NUMBER;

   ost_tr_rur                NUMBER;
   ost_ob_rur                NUMBER;

   PPFI_1                    DDVNFI_dbt%ROWTYPE;
   PPFI_2                    DDVNFI_dbt%ROWTYPE;
   pFrVal                    DDVNFRVAL_dbt%ROWTYPE;
   PDeal                     DDVNDEAL_dbt%ROWTYPE;
   Deal_rec                  DDVNDEAL_dbt%ROWTYPE;

   usertbl                   user_dvprevdata%ROWTYPE;

   nFI1                      ddvnfi_dbt%ROWTYPE;
   nFI2                      ddvnfi_dbt%ROWTYPE;
   RS                        ddvnpmgr_dbt%ROWTYPE;

   vder1                     NUMBER;
   vder2                     NUMBER;

   vob                       NUMBER;
   vtreb                     NUMBER;

   FI_PRICE_POINT   CONSTANT NUMBER := -10;

   TSS1                      NUMBER;
   TSS2                      NUMBER;
   basefi                    VARCHAR2 (150);

   AMOUNT                    NUMBER;
   DEALSUM                   NUMBER;
   rate_deal                 NUMBER;
   treb                      NUMBER;
   ob                        NUMBER;

   TSS1_ACCOUNT              VARCHAR2 (20);
   TSS2_ACCOUNT              VARCHAR2 (20);

   TSS1ost                   NUMBER;
   TSS2ost                   NUMBER;
   TSS_VAL                   NUMBER;
   TSS_ACC                   VARCHAR2 (20);

   TR_ACCOUNT                VARCHAR2 (20);
   TR_CURR                   NUMBER (10);
   OB_ACCOUNT                VARCHAR2 (20);
   OB_CURR                   NUMBER (10);

   vcountrycode              VARCHAR2 (5);

   FUNCTION GetISOCur_byCode (pcur_code IN NUMBER)
      RETURN NUMBER
   IS
      res   NUMBER;
   BEGIN
      IF pcur_code IS NOT NULL
      THEN
         BEGIN
            SELECT TO_NUMBER (t_iso_number)
              INTO res
              FROM dfininstr_dbt
             WHERE t_fiid = pcur_code;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               res := -1;
            WHEN INVALID_NUMBER
            THEN
               res := 0;
         END;
      ELSE
         RETURN NULL;
      END IF;

      RETURN res;
   END;

  FUNCTION SubjectType (subjectid IN dparty_dbt.T_PARTYID%TYPE)
      RETURN tRSHB_PFI_PKL_2CHD.SUBJECT_TYPE%TYPE
   IS
      res   tRSHB_PFI_PKL_2CHD.SUBJECT_TYPE%TYPE;
      tmp   NUMBER;
   BEGIN
      --IF subjectid IS NOT NULL
      --THEN
         BEGIN
            SELECT COUNT (*)
              INTO tmp
              FROM dpartyown_dbt ptown
             WHERE PTOWN.T_PARTYKIND = 2 AND ptown.t_partyid = subjectid;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               tmp := 0;
         END;
      --END IF;

      IF tmp = 1
      THEN
         res := 'КО';
      ELSE
         res := 'ЮЛ';
      END IF;

      RETURN res;
   END;
   
FUNCTION GetFixDate (dealid IN ddvndeal_dbt.t_id%TYPE, inDate in Date)
      RETURN date
   IS
      res   date;
   BEGIN
      IF dealid IS NOT NULL and indate is not null
      THEN
         BEGIN
         select min(t_fixdate) into res from DDVNPMGR_DBT where t_fixdate > inDate and t_dealid = dealid;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               res := null;
         END;
      END IF;
      RETURN res;
   END;
   
FUNCTION GetDealtype (exectype IN ddvnfi_dbt.t_exectype%TYPE)
      RETURN tRSHB_PFI_PKL_2CHD.deal_supply%TYPE
   IS
      res   tRSHB_PFI_PKL_2CHD.deal_supply%TYPE;
      begin
      IF exectype = 1
      THEN
         res := 'Поставочная';
      elsif exectype = 0
      then res:='Беспоставочная';
      ELSE
         res := null;
      END IF;

      RETURN res;
end;
     
BEGIN
   RepDate := pdate;

   IF clear_date
   THEN
      DELETE FROM tRSHB_PFI_PKL_2CHD
            WHERE report_dt = pdate;
   END IF;

   FOR pfi_data
      IN (SELECT tabl.*,
                 a.t_account tr_account,
                 a.t_currency tr_curr,
                 a.t_chapter tr_chapt,
                 b.t_account ob_account,
                 b.t_currency ob_curr,
                 b.t_chapter ob_chapt
            FROM (  SELECT l.t_id,
                           t.t_bofficekind bokind,
                           t.t_typedoc typedoc,
                           l.t_legid legid,
                           l.t_legkind legkind,
                           t_dealtype dealtype,
                           t.t_dealdate startdate,
                           l.t_maturity matdate,
                           t.t_dealcode code_in,
                           t.t_dealcodets code_out,
                           t.t_dealid dealid,
                           TRIM (c.t_shortname) contr_name,
                           c.t_partyid SUBJECT_TYPE,
                           c.t_nrcountry contr_country,
                           (CASE f.t_fi_kind
                               WHEN 1 THEN t_ccy
                               ELSE f.t_name
                            END)
                              fi_name,
                           f.t_fi_code,
                           f.t_fi_kind fi_kind,
                           f.t_fiid fiid,
                           f.t_facevalue,
                           (CASE t.t_bofficekind
                               WHEN 100
                               THEN
                                  (SELECT MAX (acco.t_id)
                                     FROM dmcaccdoc_dbt acco, dmccateg_dbt
                                    WHERE     t_dockind = 175
                                          AND t_docid = l.t_id
                                          AND t_catnum = t_number
                                          AND t_code =
                                                 '+Форвард, дрейф'
                                          AND (EXISTS
                                                  (SELECT *
                                                     FROM drestdate_dbt rest,
                                                          daccount_dbt ac
                                                    WHERE     acco.t_account =
                                                                 ac.t_account
                                                          AND rest.t_accountid =
                                                                 ac.t_accountid
                                                          AND rest.t_rest <> 0
                                                          AND rest.t_restcurrency =
                                                                 ac.t_code_currency
                                                          AND rest.t_restdate <=
                                                                 RepDate)))
                               WHEN 4813
                               THEN
                                  (SELECT MAX (acco.t_id)
                                     FROM dmcaccdoc_dbt acco, dmccateg_dbt
                                    WHERE     t_dockind = 4813
                                          AND t_docid = l.t_id
                                          AND t_catnum = t_number
                                          AND t_code =
                                                 '+Форвард, дрейф'
                                          AND (EXISTS
                                                  (SELECT *
                                                     FROM drestdate_dbt rest,
                                                          daccount_dbt ac
                                                    WHERE     acco.t_account =
                                                                 ac.t_account
                                                          AND rest.t_accountid =
                                                                 ac.t_accountid
                                                          AND rest.t_rest <> 0
                                                          AND rest.t_restcurrency =
                                                                 ac.t_code_currency
                                                          AND rest.t_restdate <=
                                                                 RepDate)))
                               WHEN 101
                               THEN
                                  (SELECT MAX (acco.t_id)
                                     FROM dmcaccdoc_dbt acco, dmccateg_dbt
                                    WHERE     t_dockind = 101
                                          AND t_docid = l.t_dealid
                                          AND t_catnum = t_number
                                          AND t_code =
                                                 '+Форвард, дрейф'
                                          AND (EXISTS
                                                  (SELECT *
                                                     FROM drestdate_dbt rest,
                                                          daccount_dbt ac
                                                    WHERE     acco.t_account =
                                                                 ac.t_account
                                                          AND rest.t_accountid =
                                                                 ac.t_accountid
                                                          AND rest.t_rest <> 0
                                                          AND rest.t_restcurrency =
                                                                 ac.t_code_currency
                                                          AND rest.t_restdate <=
                                                                 RepDate)))
                            END)
                              tr_acc,
                           (CASE t.t_bofficekind
                               WHEN 100
                               THEN
                                  (SELECT MAX (acco.t_id)
                                     FROM dmcaccdoc_dbt acco, dmccateg_dbt
                                    WHERE     t_dockind = 175
                                          AND t_docid = l.t_id
                                          AND t_catnum = t_number
                                          AND t_code =
                                                 '-Форвард, дрейф'
                                          AND (EXISTS
                                                  (SELECT *
                                                     FROM drestdate_dbt rest,
                                                          daccount_dbt ac
                                                    WHERE     acco.t_account =
                                                                 ac.t_account
                                                          AND rest.t_accountid =
                                                                 ac.t_accountid
                                                          AND rest.t_rest <> 0
                                                          AND rest.t_restcurrency =
                                                                 ac.t_code_currency
                                                          AND rest.t_restdate <=
                                                                 RepDate)))
                               WHEN 4813
                               THEN
                                  (SELECT MAX (acco.t_id)
                                     FROM dmcaccdoc_dbt acco, dmccateg_dbt
                                    WHERE     t_dockind = 4813
                                          AND t_docid = l.t_id
                                          AND t_catnum = t_number
                                          AND t_code =
                                                 '-Форвард, дрейф'
                                          AND (EXISTS
                                                  (SELECT *
                                                     FROM drestdate_dbt rest,
                                                          daccount_dbt ac
                                                    WHERE     acco.t_account =
                                                                 ac.t_account
                                                          AND rest.t_accountid =
                                                                 ac.t_accountid
                                                          AND rest.t_rest <> 0
                                                          AND rest.t_restcurrency =
                                                                 ac.t_code_currency
                                                          AND rest.t_restdate <=
                                                                 RepDate)))
                               WHEN 101
                               THEN
                                  (SELECT MAX (acco.t_id)
                                     FROM dmcaccdoc_dbt acco, dmccateg_dbt
                                    WHERE     t_dockind = 101
                                          AND t_docid = l.t_dealid
                                          AND t_catnum = t_number
                                          AND t_code =
                                                 '-Форвард, дрейф'
                                          AND (EXISTS
                                                  (SELECT *
                                                     FROM drestdate_dbt rest,
                                                          daccount_dbt ac
                                                    WHERE     acco.t_account =
                                                                 ac.t_account
                                                          AND rest.t_accountid =
                                                                 ac.t_accountid
                                                          AND rest.t_rest <> 0
                                                          AND rest.t_restcurrency =
                                                                 ac.t_code_currency
                                                          AND rest.t_restdate <=
                                                                 RepDate)))
                            END)
                              ob_acc,
                           l.t_principal amount,
                           (CASE l.t_totalcost
                               WHEN 0 THEN l.t_cost
                               ELSE l.t_totalcost
                            END)
                              dealsum
                      FROM ddl_tick_dbt t,
                           ddl_leg_dbt l,
                           dparty_dbt c,
                           dfininstr_dbt f
                     WHERE     t.t_bofficekind IN (101, 100, 4813)
                           AND t.t_dealtype NOT IN (2121, 2126, 2131, 2136, 22121, 2122, 2127, 2132, 2137, 2139) 
                           AND t.t_dealid = l.t_dealid
                           AND t_maturity = t_expiry
                           AND t.t_partyid = c.t_partyid
                           AND l.t_pfi = f.t_fiid
                           AND l.t_maturity >
                                  RSI_RsbCalendar.GetDateAfterWorkDay (
                                     t.t_dealdate,
                                     3)                                 /*ИЕ*/
                           AND t.t_dealdate <= RepDate
                  AND l.t_maturity > RepDate
                  ORDER BY t.t_dealcode) tabl,
                 dmcaccdoc_dbt a,
                 dmcaccdoc_dbt b
           WHERE tabl.tr_acc = a.t_id(+) AND tabl.ob_acc = b.t_id(+))
   LOOP
      -- проверка на перекв. ПФИ
      vid := 0;

      BEGIN
         SELECT objatcor.t_AttrID ID
           INTO vid
           FROM dobjatcor_dbt objatcor
          WHERE     (CASE WHEN pfi_data.BOKIND = 101 THEN 101 ELSE 102 END) =
                       objatcor.t_ObjectType
                AND objatcor.t_GroupID = 101
                AND objatcor.t_Object = LPAD (pfi_data.DealID, 34, '0');
      EXCEPTION
         WHEN OTHERS
         THEN
            vid := 0;
      END;

      IF vid NOT IN (1, 2)
      THEN                                          -- проверка на перекв. ПФИ
         IF pfi_data.DEALTYPE IN (1020, 10020)
         THEN
            dealtype := 'SWAP';
         ELSE
            dealtype := 'FW';
         END IF;

         -- Вид сделки - для бумаг и валюты по отдельности
         IF pfi_data.FI_KIND = 2
         THEN
            IF    pfi_data.DEALTYPE IN (2140,
                                        2141,
                                        2142,
                                        2143,
                                        2144,
                                        2160,
                                        2161,
                                        2162,
                                        2180,
                                        2182,
                                        2183,
                                        12182,
                                        22140)
               OR (    pfi_data.LEGKIND = 0
                   AND pfi_data.DEALTYPE IN (2121,
                                             2122,
                                             2131,
                                             2132,
                                             22121))
               OR (    pfi_data.LEGKIND = 2
                   AND pfi_data.DEALTYPE IN (2126,
                                             2127,
                                             2136,
                                             2137,
                                             2139))
            THEN
               dealkind := 'BUY';
            ELSE
               dealkind := 'SELL';
            END IF;
         ELSE
            IF pfi_data.TYPEDOC = 'B'
            THEN
               IF pfi_data.LEGID = 0
               THEN
                  dealkind := 'BUY';
               ELSE
                  dealkind := 'SELL';
               END IF;
            ELSE
               IF pfi_data.LEGID = 0
               THEN
                  dealkind := 'SELL';
               ELSE
                  dealkind := 'BUY';
               END IF;
            END IF;
         END IF;

         -- валюта счетов требований и обязательств
         TR_CURR := GetISOCur_byCode (pfi_data.TR_CURR);
         OB_CURR := GetISOCur_byCode (pfi_data.OB_CURR);

         -- остаток на счете требований
         IF pfi_data.TR_ACC IS NOT NULL
         THEN
            ost_tr :=
               rsi_rsb_account.resta (pfi_data.TR_ACCOUNT,
                                      RepDate,
                                      4,
                                      NULL,
                                      pfi_data.TR_CURR);
            ost_tr_rur :=
               RSB_FIInstr.ConvSum (ost_tr,
                                    pfi_data.TR_CURR,
                                    PM_COMMON.NATCUR,
                                    RepDate);
         ELSE
            ost_tr := 0;
            ost_tr_rur := 0;
         END IF;

         IF pfi_data.OB_ACC IS NOT NULL
         THEN
            ost_ob :=
               rsi_rsb_account.resta (pfi_data.OB_ACCOUNT,
                                      RepDate,
                                      4,
                                      NULL,
                                      pfi_data.OB_CURR);
            ost_ob_rur :=
               RSB_FIInstr.ConvSum (ost_ob,
                                    pfi_data.OB_CURR,
                                    PM_COMMON.NATCUR,
                                    RepDate);
         ELSE
            ost_ob := 0;
            ost_ob_rur := 0;
         END IF;

         -- страна контрагента
         BEGIN
            rec_PFI.COUNTRY := '';
            rec_PFI.COUNTRY_RATING := '';                  -- Страновая оценка

            IF pfi_data.contr_country = CHR (1)
            THEN
               vcountrycode := 'RUS';                -- не указано, значит, РФ
            ELSE
               vcountrycode := pfi_data.contr_country;
            END IF;

            SELECT SUBSTR (t_fullname, 1, 30), T_RISKCLASS
              INTO rec_PFI.COUNTRY, rec_PFI.COUNTRY_RATING
              FROM dcountry_dbt
             WHERE t_codelat3 = vcountrycode;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               rec_PFI.COUNTRY := vcountrycode;
            WHEN OTHERS
            THEN
               rec_PFI.COUNTRY := '';
         END;

         rec_PFI.REPORT_DT := pdate;                           -- Дата отчета;
         rec_PFI.DEAL_NUMBER := pfi_data.code_in;               --Номер сделки
         rec_PFI.SUBJECT := pfi_data.CONTR_NAME;                  --Контрагент
         rec_PFI.SUBJECT_TYPE := subjecttype(pfi_data.SUBJECT_TYPE);      --тип контрагента
         rec_PFI.BASE_ACTIVE := pfi_data.FI_NAME;              --Базовый актив
         rec_PFI.FINSTR_NAME := pfi_data.t_fi_code; --Наименование инструмента
         rec_PFI.DEAL_EXCHANGE := dealtype; --Вид сделки (биржевая/ внебиржевая)
         rec_PFI.DEAL_DIRECTION := dealkind; --Направление сделки  (покупка/продажа)
         rec_PFI.BEGIN_DT := pfi_data.STARTDATE;      --Дата заключения сделки
         rec_PFI.END_DT := pfi_data.MATDATE;           --Дата окончания сделки
         rec_PFI.ACCOUNT_DEMAND := pfi_data.TR_ACCOUNT; --Лицевой счет требований
         rec_PFI.CURRENCY_DEMAND := TR_CURR;         --Валюта счета требований
         rec_PFI.AMOUNT_DEMAND_VAL := ost_tr; --Сумма требований (в ин.валюте)
         rec_PFI.AMOUNT_DEMAND_RUR := ost_tr_rur; --Сумма требований (в руб.экв-те)
         rec_PFI.ACCOUNT_LIABILITY := pfi_data.OB_ACCOUNT; --Лицевой счет обязательств
         rec_PFI.CURRENCY_LIABILITY := OB_CURR;    --Валюта счета обязательств
         rec_PFI.AMOUNT_LIABILITY_VAL := ost_ob; --Сумма обязательств (в ин.валюте)
         rec_PFI.AMOUNT_LIABILITY_RUR := ost_ob_rur; --Сумма обязательств (в руб.эквиваленте)
         rec_PFI.SOURCE := 'RSBANK'; --Наименование системы источника (DIASOFT, FORTS, CFT и т.д.).

         -- не заполняются
         rec_PFI.AGREEMENT_TYPE := NULL; --Соглашение на основание которого заключена сделка (RISDA/ISDA)
         rec_PFI.IS_LIQ_NETTING := NULL; --Наличие в соглашении по сделке условия ликвидационного неттинга (ДА/НЕТ)
         rec_PFI.IS_CLC_NETTING := NULL; --Наличие в соглашении по сделке условия расчетного неттинга (ДА/НЕТ)
         rec_PFI.SWAP_BAL_ACC_FLOAT_RATE := NULL; --Процентные свопы. № счета 2-го порядка позиции с плавающей процентной ставкой.

         INSERT INTO tRSHB_PFI_PKL_2CHD
              VALUES rec_PFI;
      END IF;
   END LOOP;

   FOR pfi_data
      IN (SELECT tabl.*,
                 a.t_account tr_account,
                 a.t_currency tr_curr,
                 a.t_chapter tr_chapt,
                 b.t_account ob_account,
                 b.t_currency ob_curr,
                 b.t_chapter ob_chapt,
                 aa.t_account tss1_account,
                 aa.t_currency tss1_curr,
                 aa.t_chapter tss1_chapt,
                 bb.t_account tss2_account,
                 bb.t_currency tss2_curr,
                 bb.t_chapter tss2_chapt
            FROM (  SELECT l.t_id,
                           (SELECT t_fiid
                              FROM ddvnfi_dbt
                             WHERE t_dealid = t.t_id AND t_type = 0)
                              lf1,
                           (SELECT t_fiid
                              FROM ddvnfi_dbt
                             WHERE t_dealid = t.t_id AND t_type = 2)
                              lf2,
                           l.t_exectype exec,
                           l.t_type legkind,
                           t_kind dealtype,
                           t.t_type TYPE,
                           t.t_date startdate,
                           l.t_execdate matdate,
                           t.t_code code_in,
                           t.t_extcode code_out,
                           t.t_id dealid,
                           TRIM (c.t_shortname) contr_name,
                           c.t_partyid SUBJECT_TYPE,
                           c.t_nrcountry contr_country,
                           (CASE f.t_fi_kind
                               WHEN 1 THEN t_ccy
                               ELSE f.t_name
                            END)
                              fi_name,
                           f.t_fi_code,
                           f.t_fi_kind fi_kind,
                           f.t_fiid fiid,
                           f.t_facevalue,
                           CASE
                              WHEN t.t_type IN (1,
                                                2,
                                                5,
                                                6)
                              THEN
                                 l.t_pricefiid
                              ELSE
                                 l.t_fiid
                           END
                              fiid_contr,
                           (SELECT MAX (acco.t_id)
                              FROM dmcaccdoc_dbt acco, dmccateg_dbt
                             WHERE     t_dockind = 199
                                   AND t_docid = l.t_dealid
                                   AND t_catnum = t_number
                                   AND t_code =
                                          '+Форвард, дрейф внебирж'
                                   AND (EXISTS
                                           (SELECT *
                                              FROM drestdate_dbt rest,
                                                   daccount_dbt ac
                                             WHERE     acco.t_account =
                                                          ac.t_account
                                                   AND rest.t_accountid =
                                                          ac.t_accountid
                                                   AND rest.t_rest <> 0
                                                   AND rest.t_restcurrency =
                                                          ac.t_code_currency
                                                   AND rest.t_restdate <=
                                                          RepDate)))
                              tr_acc,
                           ( (SELECT MAX (acco.t_id)
                                FROM dmcaccdoc_dbt acco, dmccateg_dbt
                               WHERE     t_dockind = 199
                                     AND t_docid = l.t_dealid
                                     AND t_catnum = t_number
                                     AND t_code =
                                            '-Форвард, дрейф внебирж'
                                     AND (EXISTS
                                             (SELECT *
                                                FROM drestdate_dbt rest,
                                                     daccount_dbt ac
                                               WHERE     acco.t_account =
                                                            ac.t_account
                                                     AND rest.t_accountid =
                                                            ac.t_accountid
                                                     AND rest.t_rest <> 0
                                                     AND rest.t_restcurrency =
                                                            ac.t_code_currency
                                                     AND rest.t_restdate <=
                                                            RepDate))))
                              ob_acc,
                           (SELECT MAX (acco.t_id)
                              FROM dmcaccdoc_dbt acco, dmccateg_dbt
                             WHERE     t_dockind = 199
                                   AND t_docid = l.t_dealid
                                   AND t_catnum = t_number
                                   AND t_code = '+ПФИ'
                                   AND (EXISTS
                                           (SELECT *
                                              FROM drestdate_dbt rest,
                                                   daccount_dbt ac
                                             WHERE     acco.t_account =
                                                          ac.t_account
                                                   AND rest.t_accountid =
                                                          ac.t_accountid
                                                   AND rest.t_rest <> 0
                                                   AND rest.t_restcurrency =
                                                          ac.t_code_currency
                                                   AND rest.t_restdate <=
                                                          RepDate)))
                              tss1_acc,
                           ( (SELECT MAX (acco.t_id)
                                FROM dmcaccdoc_dbt acco, dmccateg_dbt
                               WHERE     t_dockind = 199
                                     AND t_docid = l.t_dealid
                                     AND t_catnum = t_number
                                     AND t_code = '-ПФИ'
                                     AND (EXISTS
                                             (SELECT *
                                                FROM drestdate_dbt rest,
                                                     daccount_dbt ac
                                               WHERE     acco.t_account =
                                                            ac.t_account
                                                     AND rest.t_accountid =
                                                            ac.t_accountid
                                                     AND rest.t_rest <> 0
                                                     AND rest.t_restcurrency =
                                                            ac.t_code_currency
                                                     AND rest.t_restdate <=
                                                            RepDate))))
                              tss2_acc,
                           l.t_amount amount,
                           (CASE l.t_cost
                               WHEN 0 THEN l.t_amount
                               ELSE l.t_cost
                            END)
                              dealsum
                      FROM ddvndeal_dbt t,
                           ddvnfi_dbt l,
                           dparty_dbt c,
                           dfininstr_dbt f
                     WHERE /*t.t_kind NOT IN (2695)
                       AND*/
                          t    .t_type NOT IN (9, 10)
                           AND t.t_id = l.t_dealid
                           AND t.t_contractor = c.t_partyid
                           AND l.t_fiid = f.t_fiid
                           AND (CASE
                                   WHEN t.t_kind IN (2690, 2695, 12690) THEN 0
                                   ELSE 2
                                END) = l.t_type
                                    AND l.t_execdate >= RSI_RsbCalendar.GetDateAfterWorkDay( t.t_date, 3 )    --ИЕ
                           AND t.t_date <= RepDate
                                    AND l.t_execdate > RepDate
                           AND t.t_state <> 0
                           AND t.t_state <> 2
                  ORDER BY t.t_code) tabl,
                 dmcaccdoc_dbt a,
                 dmcaccdoc_dbt b,
                 dmcaccdoc_dbt aa,
                 dmcaccdoc_dbt bb
           WHERE     tabl.tr_acc = a.t_id(+)
                 AND tabl.ob_acc = b.t_id(+)
                 AND tabl.tss1_acc = aa.t_id(+)
                 AND tabl.tss2_acc = bb.t_id(+))
   LOOP
      TSS1 := 0;
      TSS2 := 0;
      ost_ob := 0;
      ost_tr := 0;
      ost_ob_rur := 0;
      ost_tr_rur := 0;

      basefi := pfi_data.FI_NAME;

      IF pfi_data.TYPE IN (7, 8)
      THEN
         basefi := '%%ставка';
      END IF;

      IF pfi_data.TYPE IN (1, 2)
      THEN
         IF pfi_data.dealType = 2695
         THEN
            dealtype := 'OPT';
         ELSE
            dealtype := 'FW';
         END IF;
      ELSIF pfi_data.TYPE IN (5, 6)
      THEN
         dealtype := 'SWAP';
      ELSIF pfi_data.TYPE IN (7, 8)
      THEN
         IF pfi_data.LF1 = pfi_data.LF2
         THEN
            dealtype := 'IRS';
         ELSE
            dealtype := 'CCYIRS';
         END IF;
      END IF;

      IF pfi_data.dealType IN (2690, 12690)
      THEN
         IF pfi_data.TYPE = 1
         THEN
            dealkind := 'BUY';
         ELSE
            dealkind := 'SELL';
         END IF;
      ELSIF pfi_data.dealType = 2710
      THEN
         IF pfi_data.TYPE = 5
         THEN
            IF pfi_data.LegKind = 0
            THEN
               dealkind := 'BUY';
            ELSE
               dealkind := 'SELL';
            END IF;
         ELSE
            IF pfi_data.LegKind = 2
            THEN
               dealkind := 'BUY';
            ELSE
               dealkind := 'SELL';
            END IF;
         END IF;
      ELSE
         dealkind := 'BUY';
      END IF;

      IF pfi_data.TYPE IN (7, 8)
      THEN
         SELECT *
           INTO PDeal
           FROM ddvndeal_dbt t
          WHERE t_Id = pfi_data.DealID;

         SELECT *
           INTO PPFI_1
           FROM DDVNFI_DBT t
          WHERE t.t_type = 0 AND t.t_dealid = pDeal.t_Id;

         SELECT *
           INTO PPFI_2
           FROM DDVNFI_DBT t
          WHERE t.t_type = 2 AND t.t_dealid = pDeal.t_Id;

         DELETE FROM user_dvprevdata
               WHERE dvid = pDeal.t_Id;

         SELECT *
           INTO nFI1
           FROM ddvnfi_dbt
          WHERE t_type = 0 AND t_dealid = pDeal.t_Id;

         SELECT *
           INTO nFI2
           FROM ddvnfi_dbt
          WHERE t_type = 2 AND t_dealid = pDeal.t_Id;

         IF nFI1.t_fiid != nFI2.t_fiid
         THEN                               /*CCY заполняем обмен номиналами*/
            usertbl.dvid := 0;
            usertbl.dvside := 0;
            usertbl.dvdatebegin := NULL;
            usertbl.dvdateend := NULL;

            usertbl.dvdatepay := NULL;
            usertbl.dvrate1 := 0.0;
            usertbl.dvrate2 := 0.0;

            usertbl.dvspread1 := 0.0;
            usertbl.dvspread2 := 0.0;
            usertbl.dvramount := 0.0;
            usertbl.dvpamount := 0.0;
            usertbl.dvnetamount := 0.0;
            usertbl.dvsort := 0;
            usertbl.dvnum := 0;

            usertbl.dvid := pDeal.t_Id;
            usertbl.dvside := 'T';

            --var BegDate = ReadNoteForObject(145, UniID(Deal, 145),101, date(31,12,9999));
            IF usertbl.dvdatepay = ZeroDate
            THEN
               usertbl.dvdatepay := pDeal.t_BeginDate;
            END IF;

            usertbl.dvnetamount := nFI1.t_amount;
            usertbl.dvsort := 1;

            -- Первый обмен
            INSERT INTO user_dvprevdata
                 VALUES usertbl;

            usertbl.dvnetamount := -nFI2.t_amount;
            usertbl.dvside := 'O';

            INSERT INTO user_dvprevdata
                 VALUES usertbl;

            -- Второй обмен
            usertbl := NULL;
            usertbl.dvdatepay := nFI1.t_ExecDate;
            usertbl.dvsort := 3;
            usertbl.dvnetamount := -nFI1.t_amount;
            usertbl.dvside := 'O';

            INSERT INTO user_dvprevdata
                 VALUES usertbl;

            usertbl.dvnetamount := nFI2.t_amount;
            usertbl.dvside := 'T';

            INSERT INTO user_dvprevdata
                 VALUES usertbl;
         END IF;

         /*Цикл по графику платежей*/
         FOR RS IN (SELECT t_side,
                           t_begdate,
                           t_enddate,
                           t_fixdate,
                           t_paydate
                      FROM ddvnpmgr_dbt
                     WHERE t_dealid = pDeal.t_Id)
         LOOP
            usertbl.dvrate1 := 0.0;
            usertbl.dvrate2 := 0.0;

            usertbl.dvspread1 := 0.0;
            usertbl.dvspread2 := 0.0;
            usertbl.dvramount := 0.0;
            usertbl.dvpamount := 0.0;
            usertbl.dvnetamount := 0.0;

            usertbl.dvid := pDeal.t_Id;
            usertbl.dvside := 'T';

            usertbl.dvnum := NVL (usertbl.dvnum, 0) + 1;
            usertbl.dvid := pDeal.t_Id;
            usertbl.dvsort := 2;
            usertbl.dvdatebegin := RS.t_begdate;
            usertbl.dvdateend := RS.t_enddate;
            usertbl.dvdatepay := RS.t_paydate;

            IF nFI1.t_RateID > 0
            THEN /*Задана плавающая ставка - пытаемся получить ставку на дату фиксации*/
               IF RepDate > RS.t_FixDate
               THEN
                  usertbl.dvrate1 :=
                     NVL (RSB_FIInstr.ConvSumType (1.0,
                                                   nFI1.t_RateID,
                                                   FI_PRICE_POINT,
                                                   0,
                                                   RS.t_FixDate),
                          0.0);
               ELSE
                  usertbl.dvrate1 :=
                     NVL (RSB_FIInstr.ConvSumType (1.0,
                                                   nFI1.t_RateID,
                                                   FI_PRICE_POINT,
                                                   0,
                                                   RepDate),
                          0.0);
               END IF;

               usertbl.dvspread1 := nFI1.t_spread;
            ELSE          /*иначе фиксированная - берем из параметров сделки*/
               usertbl.dvrate1 := nFI1.t_rate;
            END IF;

            IF nFI2.t_RateID > 0
            THEN /*Задана плавающая ставка - пытаемся получить ставку на дату фиксации*/
               IF RepDate > RS.t_FixDate
               THEN
                  usertbl.dvrate2 :=
                     NVL (RSB_FIInstr.ConvSumType (1.0,
                                                   nFI2.t_RateID,
                                                   FI_PRICE_POINT,
                                                   0,
                                                   RS.t_FixDate),
                          0.0);
               ELSE
                  usertbl.dvrate2 :=
                     NVL (RSB_FIInstr.ConvSumType (1.0,
                                                   nFI2.t_RateID,
                                                   FI_PRICE_POINT,
                                                   0,
                                                   RepDate),
                          0.0);
               END IF;

               usertbl.dvspread2 := nFI2.t_spread;
            ELSE         /*иначе фиксированная - берем из параметров сделки */
               usertbl.dvrate2 := nFI2.t_rate;
            END IF;

            IF Rs.t_Side = 1
            THEN
               usertbl.dvside := 'O';
               usertbl.dvrate2 := 0;
            ELSIF Rs.t_Side = 2
            THEN
               usertbl.dvside := 'T';
               usertbl.dvrate1 := 0;
            ELSE                                                   /* Любое */
               usertbl.dvside := ' ';
            END IF;

            vder1 :=
               RSB_Derivatives.DV_Years (usertbl.dvdatebegin,
                                         usertbl.dvdateend,
                                         nFI1.t_Basis);
            vder2 :=
               RSB_Derivatives.DV_Years (usertbl.dvdatebegin,
                                         usertbl.dvdateend,
                                         nFI2.t_Basis);

            IF usertbl.dvrate1 != 0
            THEN /*Если на дату фиксации известна ставка, то рассчитываем сумму обязательств*/
               usertbl.dvramount :=
                    nFI1.t_Amount
                  * (usertbl.dvrate1 + usertbl.dvspread1)
                  * vder1;
            END IF;

            IF usertbl.dvrate2 != 0
            THEN /*Если на дату фиксации известна ставка, то рассчитываем сумму требований*/
               usertbl.dvpamount :=
                    nFI2.t_Amount
                  * (usertbl.dvrate2 + usertbl.dvspread2)
                  * vder2;
            END IF;

            IF usertbl.dvramount != 0 AND usertbl.dvpamount != 0
            THEN /*Если известно обе суммы, то рассчитываем сумму итогового снеттингованного платежа*/
               usertbl.dvnetamount := usertbl.dvpamount - usertbl.dvramount;
            END IF;

            IF Rs.t_Side = 1
            THEN
               usertbl.dvnetamount := -usertbl.dvramount;
            ELSIF Rs.t_Side = 2
            THEN
               usertbl.dvnetamount := usertbl.dvpamount;
            END IF;

            IF Rs.t_Side != 0
            THEN
               usertbl.dvramount := 0;
               usertbl.dvpamount := 0;
            END IF;

            INSERT INTO user_dvprevdata
                 VALUES usertbl;
         END LOOP;

         AMOUNT := 0;
         DEALSUM := 0;

         IF pfi_data.FIID_Contr = PM_COMMON.NATCUR
         THEN
            rate_deal :=
               RSB_FIInstr.ConvSum (100.00,
                                    pfi_data.FIID,
                                    PM_COMMON.NATCUR,
                                    RepDate);
            rate_deal := rate_deal / 100;
         ELSE
            rate_deal :=
               RSB_FIInstr.ConvSum (100.00,
                                    pfi_data.FIID_Contr,
                                    PM_COMMON.NATCUR,
                                    RepDate);
            rate_deal := rate_deal / 100;
         END IF;

         IF dealtype = 'IRS'
         THEN
            BEGIN
               SELECT NVL (SUM (t.dvramount), 0) treb,
                      NVL (SUM (t.dvpamount), 0) ob
                 INTO vob, vtreb
                 FROM USER_DVPREVDATA t
                WHERE t.dvid = pfi_data.DealID AND dvdatepay > RepDate;
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  AMOUNT := 0;
                  DEALSUM := 0;
            END;

            IF pfi_data.TYPE = 7
            THEN
               AMOUNT := treb * rate_deal;
               DEALSUM := ob;
            ELSIF pfi_data.TYPE = 8
            THEN
               AMOUNT := ob * rate_deal;
               DEALSUM := treb;
            END IF;
         ELSIF dealtype = 'CCYIRS'
         THEN
            FOR sqlRes
               IN (SELECT NVL (t.dvnetamount, 0) treb,
                          NVL (t2.dvnetamount, 0) ob
                     INTO vob, vtreb
                     FROM USER_DVPREVDATA t, USER_DVPREVDATA t2
                    WHERE     t.dvid = pfi_data.DealID
                          AND t2.dvid = pfi_data.DealID
                          AND t.dvside = 'T'
                          AND t2.dvside = 'O'
                          AND t.dvdatepay = t2.dvdatepay
                          AND t.dvsort IN (2, 3)
                          AND t.dvsort = t2.dvsort
                          AND t.dvdatepay > RepDate
                          AND t2.dvdatepay > RepDate)
            LOOP
               IF pfi_data.TYPE = 7
               THEN
                  AMOUNT := AMOUNT + ABS (ROUND (TREB, 2) * rate_deal);
                  DEALSUM := DEALSUM + ABS (ROUND (OB, 2));
               ELSIF pfi_data.TYPE = 8
               THEN
                  AMOUNT := AMOUNT + ABS (ROUND (OB, 2) * rate_deal);
                  DEALSUM := DEALSUM + ABS (ROUND (TREB, 2));
               END IF;
            END LOOP;
         END IF;
      ELSE
         AMOUNT := pfi_data.AMOUNT;
         DEALSUM := pfi_data.DEALSUM;
      END IF;

      -- остаток на счете требований
      IF pfi_data.TSS1_ACC IS NOT NULL
      THEN
         TSS1_ACCOUNT := pfi_data.TSS1_ACCOUNT;
         TSS1ost :=
            rsi_rsb_account.resta (TSS1_ACCOUNT,
                                   RepDate,
                                   1,
                                   NULL,
                                   pfi_data.TSS1_CURR);
      ELSE
         TSS1ost := -1;
      END IF;

      IF pfi_data.TSS2_ACC IS NOT NULL
      THEN
         TSS2_ACCOUNT := pfi_data.TSS2_ACCOUNT;
         TSS2ost :=
            rsi_rsb_account.resta (TSS2_ACCOUNT,
                                   RepDate,
                                   1,
                                   NULL,
                                   pfi_data.TSS2_CURR);
      ELSE
         TSS2ost := -1;
      END IF;

      IF TSS1ost = -1 AND TSS2ost = -1
      THEN
         TSS_val := NULL;
         TSS_acc := NULL;
         TSS1ost := 0;
         TSS2ost := 0;
      END IF;

      IF TSS1ost = 0 AND TSS2ost = 0
      THEN
         TSS_val := 0;
         TSS_acc := '0';
      END IF;

      IF TSS1ost > 0 AND TSS2ost > 0
      THEN
         TSS_val := 0;
         TSS_acc := 'Ошибка';
         TSS1ost := 0;
         TSS2ost := 0;
      END IF;

      IF TSS1ost NOT IN (0, -1) AND (TSS2ost IN (-1, 0) OR TSS2ost < 0)
      THEN
         TSS_val := TSS1ost;
         TSS_acc := TSS1_ACCOUNT;
         TSS1 := TSS_val;
      END IF;

      IF TSS2ost NOT IN (0, -1) AND (TSS1ost NOT IN (-1, 0) OR TSS1ost < 0)
      THEN
         TSS_val := TSS2ost;
         TSS_acc := TSS2_ACCOUNT;
         TSS2 := TSS_val;
      END IF;

      TSS_val := ABS (TSS_val);
      TSS1 := ABS (TSS1);
      TSS2 := ABS (TSS2);

      ost_tr := 0;
      ost_ob := 0;
      ost_tr_rur := 0;
      ost_ob_rur := 0;

      -- валюта счетов требований и обязательств
      TR_CURR := GetISOCur_byCode (pfi_data.TR_CURR);
      OB_CURR := GetISOCur_byCode (pfi_data.OB_CURR);

      IF pfi_data.Exec = 1
      THEN
         IF pfi_data.TR_ACC IS NOT NULL
         THEN
            TR_ACCOUNT := pfi_data.TR_ACCOUNT;
            ost_tr :=
               rsi_rsb_account.resta (TR_ACCOUNT,
                                      RepDate,
                                      4,
                                      NULL,
                                      pfi_data.TR_CURR);
            ost_tr_rur :=
               RSB_FIInstr.ConvSum (ost_tr,
                                    pfi_data.TR_CURR,
                                    PM_COMMON.NATCUR,
                                    RepDate);
         ELSE
            ost_tr := 0;
            ost_tr_rur := 0;
         END IF;
      ELSE
         IF dealkind = 'BUY' AND pfi_data.TYPE > 6
         THEN
            ost_tr := AMOUNT;
         END IF;

         IF dealkind = 'SELL' AND pfi_data.TYPE > 6
         THEN
            ost_tr := DealSum;
         END IF;

         IF dealkind = 'BUY' AND pfi_data.TYPE < 7
         THEN
            ost_tr := TSS_val;
         END IF;

         IF dealkind = 'SELL' AND pfi_data.TYPE < 7
         THEN
            ost_tr := pfi_data.DEALSUM;
         END IF;

         TR_ACCOUNT := '';
         ost_tr_rur :=
            RSB_FIInstr.ConvSum (ost_tr,
                                 pfi_data.TR_CURR,
                                 PM_COMMON.NATCUR,
                                 RepDate);
      END IF;

      IF pfi_data.Exec = 1
      THEN
         IF pfi_data.OB_ACC IS NOT NULL
         THEN
            OB_ACCOUNT := pfi_data.OB_ACCOUNT;
            ost_ob :=
               rsi_rsb_account.resta (OB_ACCOUNT,
                                      RepDate,
                                      4,
                                      NULL,
                                      pfi_data.OB_CURR);
            ost_ob_rur :=
               RSB_FIInstr.ConvSum (ost_ob,
                                    pfi_data.OB_CURR,
                                    PM_COMMON.NATCUR,
                                    RepDate);
         ELSE
            ost_ob := 0;
            ost_ob_rur := 0;
         END IF;
      ELSE
         IF dealkind = 'BUY' AND pfi_data.TYPE > 6
         THEN
            ost_ob := DealSum;
         END IF;

         IF dealkind = 'SELL' AND pfi_data.TYPE > 6
         THEN
            ost_ob := AMOUNT;
         END IF;

         IF dealkind = 'BUY' AND pfi_data.TYPE < 7
         THEN
            ost_ob := pfi_data.DEALSUM;
         END IF;

         IF dealkind = 'SELL' AND pfi_data.TYPE < 7
         THEN
            ost_ob := TSS_val;
         END IF;

         ost_ob_rur :=
            RSB_FIInstr.ConvSum (ost_ob,
                                 pfi_data.OB_CURR,
                                 PM_COMMON.NATCUR,
                                 RepDate);
      END IF;

      ost_ob := NVL (ost_ob, 0);

      -- страна контрагента
      BEGIN
         rec_PFI.COUNTRY := '';
         rec_PFI.COUNTRY_RATING := '';                     -- Страновая оценка

         IF pfi_data.contr_country = CHR (1)
         THEN
            vcountrycode := 'RUS';                   -- не указано, значит, РФ
         ELSE
            vcountrycode := pfi_data.contr_country;
         END IF;

         SELECT SUBSTR (t_fullname, 1, 30), T_RISKCLASS
           INTO rec_PFI.COUNTRY, rec_PFI.COUNTRY_RATING
           FROM dcountry_dbt
          WHERE t_codelat3 = vcountrycode;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            rec_PFI.COUNTRY := vcountrycode;
         WHEN OTHERS
         THEN
            rec_PFI.COUNTRY := '';
      END;

      rec_PFI.REPORT_DT := pdate;                              -- Дата отчета;
      rec_PFI.DEAL_NUMBER := pfi_data.CODE_IN;                  --Номер сделки
      rec_PFI.SUBJECT := pfi_data.CONTR_NAME;                     --Контрагент
      rec_PFI.SUBJECT_TYPE := subjecttype(pfi_data.SUBJECT_TYPE); --Вид контрагента (КО/ЮЛ)
      rec_PFI.DEAL_TYPE := 'PFI';                   --Тип сделки (ПФИ/срочная)
      rec_PFI.SUBJ_RATING := NULL;      --Рейтинг контрагента на отчетную дату
      rec_PFI.SUBJ_RATING_3453U := NULL; --Рейтинг контрагента в соответствии с  Указанием 3453-У
      rec_PFI.FINSTR_NAME := pfi_data.t_fi_code;    --Наименование инструмента
      rec_PFI.BASE_ACTIVE := basefi;                           --Базовый актив
      rec_PFI.DEAL_SUPPLY := GetDealtype(pfi_data.exec);  --Вид сделки (поставочная/ беспоставочная)
      rec_PFI.DEAL_EXCHANGE := dealtype;  --Вид сделки (биржевая/ внебиржевая)
      rec_PFI.DEAL_DIRECTION := dealkind; --Направление сделки  (покупка/продажа)
      rec_PFI.BEGIN_DT := pfi_data.STARTDATE;         --Дата заключения сделки
      rec_PFI.END_DT := pfi_data.MATDATE;              --Дата окончания сделки
      rec_PFI.ACCOUNT_DEMAND := pfi_data.TR_ACCOUNT; --Лицевой счет требований
      rec_PFI.CURRENCY_DEMAND := TR_CURR;            --Валюта счета требований
      rec_PFI.AMOUNT_DEMAND_VAL := ost_tr;    --Сумма требований (в ин.валюте)
      rec_PFI.AMOUNT_DEMAND_RUR := ost_tr_rur; --Сумма требований (в руб.экв-те)
      rec_PFI.ACCOUNT_LIABILITY := pfi_data.OB_ACCOUNT; --Лицевой счет обязательств
      rec_PFI.CURRENCY_LIABILITY := OB_CURR;       --Валюта счета обязательств
      rec_PFI.AMOUNT_LIABILITY_VAL := ost_ob; --Сумма обязательств (в ин.валюте)
      rec_PFI.AMOUNT_LIABILITY_RUR := ost_ob_rur; --Сумма обязательств (в руб.эквиваленте)
      rec_PFI.ACCOUNT_SS := TSS_Acc; --Лицевой счет по учету справедливой стоимости ПФИ
      rec_PFI.AMOUNT_SS_ACTIVE := TSS1;        --Справедливая стоимость актива
      rec_PFI.AMOUNT_SS_LIABILITY := TSS2; --Справедливая стоимость обязательства
      rec_PFI.IS_REPOSITORY := NULL; --Сделка зарегистрирована в репозитарии (ДА/НЕТ)
      rec_PFI.SWAP_PRC_CHANGE_DT := GetFixDate(pfi_data.t_id, pdate); --Процентные свопы. Ближайшая дата пересмотра процентной ставки
      rec_PFI.SOURCE := 'RSBANK'; --Наименование системы источника (DIASOFT, FORTS, CFT и т.д.).

      -- не заполняются
      rec_PFI.AGREEMENT_TYPE := NULL; --Соглашение на основание которого заключена сделка (RISDA/ISDA)
      rec_PFI.IS_LIQ_NETTING := NULL; --Наличие в соглашении по сделке условия ликвидационного неттинга (ДА/НЕТ)
      rec_PFI.IS_CLC_NETTING := NULL; --Наличие в соглашении по сделке условия расчетного неттинга (ДА/НЕТ)
      rec_PFI.SWAP_BAL_ACC_FLOAT_RATE := NULL; --Процентные свопы. № счета 2-го порядка позиции с плавающей процентной ставкой.

      INSERT INTO tRSHB_PFI_PKL_2CHD
           VALUES rec_PFI;
   END LOOP;

   COMMIT;
END;
/