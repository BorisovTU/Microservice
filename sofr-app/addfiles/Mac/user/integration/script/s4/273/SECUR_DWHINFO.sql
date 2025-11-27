/* Formatted on 26/09/2018 14:52:22 (QP5 v5.277) */
CREATE OR REPLACE PACKAGE SECUR_DWHINFO
AS
   --Номер категории ОКВЭД
   CATEG_OKVED      CONSTANT INTEGER := 17;

   --Номер вида субъекта 'является Банком'
   PARTYKIND_BANK   CONSTANT INTEGER := 2;

   /*
    * Получить ISO-код валюты
    * @param     fiid - РС-Банковский ID фин.инструмента
   */
   FUNCTION GetCurrencyISO (fiid IN dfininstr_dbt.t_fiid%TYPE)
      RETURN VARCHAR2;

   /*
    * Получить остаток по лицевому счету
    * @param     accountnum - номер ЛС
    * @param     currence      - код валюты ЛС
    * @param     restdate      - дата, на к-рую надо получить остаток
   */
   FUNCTION GetAccountRest (
      accountnum   IN daccount_dbt.t_account%TYPE,
      currency     IN daccount_dbt.t_code_currency%TYPE,
      restdate     IN drestdate_dbt.t_restdate%TYPE)
      RETURN NUMBER;

   /*
    * Определить признак ФО / НФО
    * @param     partyid
   */
   FUNCTION DefineIsNFO (partyid IN dparty_dbt.t_partyid%TYPE)
      RETURN VARCHAR2;

   /*
    * Подготовка данных - отчет РЕПО
    * @param     DateRep1 - дата 'с'
    * @param     DateRep2 - дата 'по'
   */

   FUNCTION GetRate (Tid     IN dobjatcor_dbt.T_OBJECT%TYPE,
                     ObId    IN dobjattr_dbt.t_objecttype%TYPE,
                     GrId    IN dobjattr_dbt.t_groupid%TYPE,
                     ParId   IN dobjattr_dbt.t_parentid%TYPE,
                     Ndate   IN DATE)
      RETURN dobjattr_dbt.t_name%TYPE;

   PROCEDURE PrepareREPO (DateRep1 IN DATE, DateRep2 IN DATE);
END SECUR_DWHINFO;


CREATE OR REPLACE PACKAGE BODY SECUR_DWHINFO
IS
   FUNCTION GetCurrencyISO (fiid IN dfininstr_dbt.t_fiid%TYPE)
      RETURN VARCHAR2
   IS
      iso   dfininstr_dbt.t_fiid%TYPE;
   BEGIN
      BEGIN
         SELECT t_iso_number
           INTO iso
           FROM dfininstr_dbt
          WHERE t_fiid = fiid;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            iso := CHR (1);
      END;

      RETURN iso;
   END GetCurrencyISO;


   FUNCTION GetAccountRest (
      accountnum   IN daccount_dbt.t_account%TYPE,
      currency     IN daccount_dbt.t_code_currency%TYPE,
      restdate     IN drestdate_dbt.t_restdate%TYPE)
      RETURN NUMBER
   IS
      rest   drestdate_dbt.t_rest%TYPE;
   BEGIN
      BEGIN
         SELECT t_rest
           INTO rest
           FROM drestdate_dbt r, daccount_dbt a
          WHERE     a.t_account = accountnum
                AND r.t_restcurrency = currency
                AND a.t_open_close NOT IN ('З', 'з')
                AND r.t_accountid = a.t_accountid
                AND r.t_restdate =
                       (SELECT MAX (t_restdate)
                          FROM drestdate_dbt
                         WHERE     t_accountid = r.t_accountid
                               AND t_restcurrency = r.t_restcurrency
                               AND t_restdate <= restdate);
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            rest := 0;
      END;

      RETURN rest;
   END GetAccountRest;


   FUNCTION DefineIsNFO (partyid IN dparty_dbt.t_partyid%TYPE)
      RETURN VARCHAR2
   IS
      islegalform     CHAR (1);
      isresident      CHAR (1);
      isbank          CHAR (1);
      isokved6group   CHAR (1);
   BEGIN
      BEGIN
         SELECT DECODE (p.t_legalform, 1, 'Y', 0),
                DECODE (p.t_notresident, CHR (88), 'N', 'Y'),
                DECODE (o.t_partykind, PARTYKIND_BANK, 'Y', 'N')
           INTO islegalform, isresident, isbank
           FROM dparty_dbt p,
                (SELECT t_partyid, t_partykind
                   FROM dpartyown_dbt
                  WHERE t_partykind = PARTYKIND_BANK) o
          WHERE p.t_partyid = o.t_partyid(+) AND p.t_partyid = partyid;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            islegalform := 'N';
            isresident := 'N';
            isbank := 'N';
      END;

      IF (isbank = 'Y')
      THEN
         RETURN 'ФО';
      END IF;

      IF (isresident = 'Y' AND islegalform = 'Y')
      THEN
         BEGIN
            SELECT 'Y'
              INTO isokved6group
              FROM dobjattr_dbt a, dobjatcor_dbt c
             WHERE     a.t_objecttype = c.t_objecttype
                   AND a.t_groupid = c.t_groupid
                   AND a.t_attrid = c.t_attrid
                   AND c.t_objecttype = 3
                   AND c.t_groupid = CATEG_OKVED
                   AND c.t_object = LPAD (partyid, 10, '0')
                   AND c.t_validtodate = TO_DATE ('31.12.9999', 'dd.mm.yyyy')
                   AND (   a.t_nameobject LIKE '64%'
                        OR a.t_nameobject LIKE '65%'
                        OR a.t_nameobject LIKE '66%');
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               isokved6group := 'N';
         END;

         IF (isokved6group = 'Y')
         THEN
            RETURN 'ФО';
         ELSE
            RETURN 'НФО';
         END IF;
      --ELSE
      --необходимо проанализировать случай нерезидента не ЮЛ

      END IF;

      RETURN CHR (1);
   END;


   /*
   Функция для определения рейтинга из категорий объекта
   на входе:
   Tid    -  Ид объекта, в процедуре использовать переменную IssueId для объкта ц/б или IssuerId для эмитента ц/б
   ObId   -  тип объекта в процедуре использовать константу ObIdIssue для ц/б или ObIdIssuer для эмитента ц/б
   GrId   -  ид категории в процедуре использовать  константу GrIdIssue для ц/б или GrIdIssuer для эмитента ц/б
   ParId  -  ид вида рейтинга в процедуре использовать константу SaPRat для Рейтинга Standard and Poor's, MoodyRat для рейтинга Moody или  FitchRat для рейтинга Fitch IBCA
   Ndate  -  на какую дату используется параметр, в процедуре используется входящий параметр DateRep2*/
   FUNCTION GetRate (Tid     IN dobjatcor_dbt.T_OBJECT%TYPE,
                     ObId    IN dobjattr_dbt.t_objecttype%TYPE,
                     GrId    IN dobjattr_dbt.t_groupid%TYPE,
                     ParId   IN dobjattr_dbt.t_parentid%TYPE,
                     Ndate   IN DATE)
      RETURN dobjattr_dbt.t_name%TYPE
   IS
      out_Name   dobjattr_dbt.t_name%TYPE;
      DateTmp    dobjatcor_dbt.T_sysdate%TYPE;
      TimeTmp    dobjatcor_dbt.t_systime%TYPE;
   BEGIN
      BEGIN
         SELECT MAX (cor.T_sysdate)
           INTO DateTmp
           FROM dobjatcor_dbt cor
                LEFT JOIN dobjattr_dbt attr
                   ON     cor.t_objecttype = attr.t_objecttype
                      AND cor.t_groupid = attr.t_groupid
                      AND cor.t_attrid = attr.t_attrid
          WHERE     TO_NUMBER (cor.T_OBJECT) = Tid
                AND cor.t_groupid = GrId
                AND cor.t_objecttype = ObId
                AND attr.t_parentid = ParId
                AND ndate BETWEEN cor.t_validfromdate AND cor.t_validtodate;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            DateTmp := NULL;
      END;

      BEGIN
         SELECT MAX (cor.T_systime)
           INTO TimeTmp
           FROM dobjatcor_dbt cor
                LEFT JOIN dobjattr_dbt attr
                   ON     cor.t_objecttype = attr.t_objecttype
                      AND cor.t_groupid = attr.t_groupid
                      AND cor.t_attrid = attr.t_attrid
          WHERE     TO_NUMBER (cor.T_OBJECT) = Tid
                AND cor.t_groupid = GrId
                AND cor.t_objecttype = ObId
                AND attr.t_parentid = ParId
                AND ndate BETWEEN cor.t_validfromdate AND cor.t_validtodate
                AND COR.T_SYSDATE = DateTmp;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            TimeTmp := NULL;
      END;

      BEGIN
         SELECT attr.t_name
           INTO out_name
           FROM dobjatcor_dbt cor
                LEFT JOIN dobjattr_dbt attr
                   ON     cor.t_objecttype = attr.t_objecttype
                      AND cor.t_groupid = attr.t_groupid
                      AND cor.t_attrid = attr.t_attrid
          WHERE     TO_NUMBER (cor.T_OBJECT) = Tid
                AND cor.t_groupid = GrId
                AND cor.t_objecttype = ObId
                AND attr.t_parentid = ParId
                AND ndate BETWEEN cor.t_validfromdate AND cor.t_validtodate
                AND COR.T_SYSDATE = DateTmp
                AND cor.T_systime = TimeTmp;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            out_name := NULL;
      END;

      RETURN out_name;
   END;

   PROCEDURE PrepareREPO (DateRep1 IN DATE, DateRep2 IN DATE)
   IS
      repo_rec                tRSHB_REPO_PKL_2CHD%ROWTYPE; --структура, в которую будем собирать инфу из разных селектов
      fiid_acc_money          dfininstr_dbt.t_fiid%TYPE;
      fiid_acc_percnt         dfininstr_dbt.t_fiid%TYPE;
      acc_repo_in_repo        daccount_dbt.t_account%TYPE;
      acc_fiid_repo_in_repo   dfininstr_dbt.t_fiid%TYPE;
      IssueId                 dparty_dbt.t_partyid%TYPE;    --ид ценной бумаги
      IssuerId                dparty_dbt.t_partyid%TYPE;         --ид эмитента
      SaPRat         CONSTANT dobjattr_dbt.t_parentid%TYPE := 110; --Ид Рейтинга Standard and Poor's
      MoodyRat       CONSTANT dobjattr_dbt.t_parentid%TYPE := 210; --ид рейтинга Moody
      FitchRat       CONSTANT dobjattr_dbt.t_parentid%TYPE := 310; --ид рейтинга Fitch IBCA
      ObIdIssue      CONSTANT dobjattr_dbt.t_objecttype%TYPE := 12; -- Ид типа объекта для ц/б
      ObIdIssuer     CONSTANT dobjattr_dbt.t_objecttype%TYPE := 3; -- Ид типа объекта для эмитента ц/б
      GrIdIssue      CONSTANT dobjattr_dbt.t_objecttype%TYPE := 53; -- Ид категории для ц/б
      GrIdIssuer     CONSTANT dobjattr_dbt.t_objecttype%TYPE := 19; -- Ид категории для эмитента ц/б
      subjectid dparty_dbt.t_partyid%type;
   BEGIN
      DELETE FROM tRSHB_REPO_PKL_2CHD;                              --временно

      --COMMIT;

      --основной select по сделке
      FOR deal_rec
         IN (  SELECT DISTINCT
                      deal.t_dealid AS DealID,
                      deal.t_dealcodets AS DealCodeTS,
                      deal.t_dealdate AS DealDate,
                      deal.t_dealtype AS DealType,
                      deal.t_partyid AS DealContragent,
                      deal.t_PFI AS DealFI,
                      leg1.t_maturity AS L1Date,
                      leg1.t_principal AS L1Amount,
                      (SELECT DECODE (
                                 t_factdate,
                                 TO_DATE ('01010001', 'ddmmyyyy'), t_plandate,
                                 t_factdate)
                         FROM ddlrq_dbt
                        WHERE     t_docid = deal.t_dealid
                              AND t_type = 2
                              AND t_dealpart = 1
                              AND t_dockind = 101)
                         AS DatePay1,
                      deal.t_PFI ISIN,
                      leg1.t_id AccBound,
                      deal.t_BOfficeKind DealKind
                 FROM ddlrq_dbt pm,
                      ddl_tick_dbt deal,
                      ddl_leg_dbt leg1,
                      dfininstr_dbt curr
                WHERE     pm.t_dockind = 101
                      AND pm.t_type IN (0, 2, 8)
                      AND (   pm.t_plandate > DateRep1
                           OR (    pm.t_plandate <= DateRep2
                               AND pm.t_state IN (0, 2)
                               AND deal.t_dealstatus = 10))
                      AND deal.t_dealdate <= DateRep2
                      AND deal.t_dealid = pm.t_docid
                      AND deal.t_dealid = leg1.t_dealid
                      AND leg1.t_legkind = 0
                      AND leg1.t_cfi = curr.t_fiid
             ORDER BY t_dealdate)
      LOOP
         repo_rec.DEAL_NUMBER := deal_rec.DealCodeTS;
         repo_rec.DEAL_DT := deal_rec.DealDate;
         repo_rec.PART_1_EX_DT := deal_rec.DatePay1;
         repo_rec.ISIN := deal_rec.ISIN;

         BEGIN
            SELECT CASE
                      WHEN     INSTR (LOWER (t_name), 'прям') > 0
                           AND INSTR (LOWER (t_name), 'репо') > 0
                      THEN
                         'прямое РЕПО'
                      WHEN     INSTR (LOWER (t_name), 'обр') > 0
                           AND INSTR (LOWER (t_name), 'репо') > 0
                      THEN
                         'обратное РЕПО'
                   END
              INTO repo_rec.REPO_DIRECTION
              FROM doprkoper_dbt t
             WHERE t_kind_operation = deal_rec.DealType;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               repo_rec.REPO_DIRECTION := CHR (1);
         END;

         IF (Rsb_Secur.IsOutExchange (
                Rsb_Secur.get_OperationGroup (
                   Rsb_Secur.get_OperSysTypes (deal_rec.DealType,
                                               deal_rec.DealKind))) > 0)
         THEN
            repo_rec.REPO_EXCHANGE := 'внебиржевая';
            repo_rec.IS_LIQ_NETTING := CHR (1);
            repo_rec.IS_CLC_NETTING := CHR (1);
         --если сделка внебиржевая, читаем параметр на ген соглашении, чтобы решить, что передать в IS_REPOSITORY (ДА или НЕТ)                                   !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
         --(под чтением параметра на ген соглашении подразумевается чтение примечания)
         --repo_rec.IS_REPOSITORY  :=
         ELSE
            repo_rec.REPO_EXCHANGE := 'биржевая';
            repo_rec.IS_LIQ_NETTING := 'ДА';
            repo_rec.IS_CLC_NETTING := 'ДА';
            repo_rec.IS_REPOSITORY := 'НЕТ';
         END IF;

         BEGIN
            SELECT t_account, t_currency
              INTO acc_repo_in_repo, acc_fiid_repo_in_repo
              FROM dmcaccdoc_dbt
             WHERE t_id =
                      (SELECT MAX (acc.t_id)
                         FROM dmcaccdoc_dbt acc, dmccateg_dbt cat
                        WHERE     acc.t_catnum = cat.t_number
                              AND acc.t_dockind = 176
                              AND acc.t_docid = deal_rec.AccBound
                              AND cat.t_code IN
                                     ('Ц/б, ПВО_БПП, Корзина',
                                      'Ц/б, ПВО_БПП'));
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               acc_repo_in_repo := CHR (1);
               acc_fiid_repo_in_repo := -1;
         END;

         IF (GetAccountRest (acc_repo_in_repo,
                             acc_fiid_repo_in_repo,
                             repo_rec.DEAL_DT) != 0)
         THEN
            repo_rec.REPO_IN_REPO := 'РЕПО в РЕПО';
         ELSE
            repo_rec.REPO_IN_REPO := CHR (1);
         END IF;

         BEGIN
            SELECT subj.t_shortname AS ContragentName,
                   subj.t_nrcountry AS ContragentCountry,
                   subj.t_partyid
              INTO repo_rec.SUBJECT, repo_rec.SUBJ_COUNTRY, subjectid
              FROM dparty_dbt subj
             WHERE subj.t_partyid = deal_rec.DealContragent;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               repo_rec.SUBJECT := CHR (1);
               repo_rec.SUBJ_COUNTRY := CHR (1);
               subjectid := 0;
         END;

         repo_rec.IS_NFO := DefineIsNFO (subjectid);

         SELECT CASE
                   WHEN EXISTS
                           (SELECT 1
                              FROM dobjatcor_dbt o, dobjattr_dbt a
                             WHERE     o.t_objecttype = 3
                                   AND o.t_groupid = 19
                                   /* смотрим наличие только высоких рейтингов: */
                                   AND ASCII (
                                          SUBSTR (UPPER (a.t_name), 1, 1)) IN
                                          (65                       /*англ A*/
                                             , 53392)               /*русс А*/
                                   AND o.t_object =
                                          LPAD (deal_rec.DealContragent,
                                                10,
                                                CHR (48))
                                   AND o.t_validtodate =
                                          TO_DATE ('31.12.9999',
                                                   'dd.mm.yyyy')
                                   AND a.t_objecttype = o.t_objecttype
                                   AND a.t_groupid = o.t_groupid
                                   AND a.t_attrid = o.t_attrid)
                   THEN
                      'Выше "В"'
                   ELSE
                      'нет'
                END
           INTO repo_rec.SUBJ_RATING
           FROM DUAL;

         BEGIN
            SELECT leg2.t_principal AS L2Amount,
                   leg2.t_incomerate AS L2PercentsREPO,
                   (SELECT DECODE (
                              t_factdate,
                              TO_DATE ('01010001', 'ddmmyyyy'), t_plandate,
                              t_factdate)
                      FROM ddlrq_dbt
                     WHERE     t_docid = deal_rec.DealID
                           AND t_type = 2
                           AND t_dealpart = 2
                           AND t_dockind = 101)
                      AS DatePay2
              INTO repo_rec.QUANTITY, repo_rec.RATE, repo_rec.PART_2_EX_DT
              FROM ddl_leg_dbt leg2
             WHERE leg2.t_dealid = deal_rec.DealID AND leg2.t_legkind = 2;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               repo_rec.QUANTITY := 0;
               repo_rec.RATE := 0;
               repo_rec.PART_2_EX_DT := TO_DATE ('01010001', 'ddmmyyyy');
         END;

         repo_rec.QUANTITY := repo_rec.QUANTITY + deal_rec.L1Amount;

         BEGIN
            SELECT SUBSTR (p.t_shortname, 1, 60),
                   SUBSTR (fi.t_name, 1, 25),
                   p.t_nrcountry                  /*nvl(a.t_country, chr(1))*/
                                ,
                   v.t_incirculationdate,
                   fi.t_fiid,
                   fi.t_issuer
              INTO repo_rec.ISSUER,
                   repo_rec.ISSUE_NAME,
                   repo_rec.ISSUER_COUNTRY,
                   repo_rec.DEPLOY_DT,
                   issueid,                          -- правка З узнаем ид ц/б
                   issuerid                 -- правка З узнаем ид эмитента ц/б
              FROM dfininstr_dbt fi,
                   dfininstr_dbt curr2,
                   davoiriss_dbt v,
                   dparty_dbt p,
                   (SELECT *
                      FROM dadress_dbt
                     WHERE t_type = 1) a
             WHERE     fi.t_fiid = deal_rec.DealFI
                   AND fi.t_facevaluefi = curr2.t_fiid
                   AND fi.t_fiid = v.t_fiid
                   AND fi.t_issuer = p.t_partyid
                   AND a.t_partyid(+) = p.t_partyid;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               repo_rec.ISSUER := CHR (1);
               repo_rec.ISSUE_NAME := CHR (1);
               repo_rec.ISSUER_COUNTRY := CHR (1);
               repo_rec.DEPLOY_DT := TO_DATE ('01.01.0001', 'dd.mm.yyyy');
         END;

         BEGIN
            SELECT TO_CHAR (t_riskclass)
              INTO repo_rec.COUNTRY_RATING
              FROM dcountry_dbt
             WHERE     t_codelat3 = repo_rec.ISSUER_COUNTRY
                   AND repo_rec.ISSUER_COUNTRY != CHR (1);
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               repo_rec.COUNTRY_RATING := CHR (1);
         END;

         BEGIN
            SELECT a.t_name
              INTO repo_rec.SECURITY_TYPE
              FROM dobjatcor_dbt o, dobjattr_dbt a
             WHERE     o.t_objecttype = 12
                   AND o.t_groupid = 1
                   AND o.t_object = LPAD (deal_rec.DealID, 34, CHR (48))
                   AND o.t_validtodate = TO_DATE ('31.12.9999', 'dd.mm.yyyy')
                   AND a.t_objecttype = o.t_objecttype
                   AND a.t_groupid = o.t_groupid
                   AND a.t_attrid = o.t_attrid;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               repo_rec.SECURITY_TYPE := CHR (1);
         END;

         BEGIN
            SELECT raccount.t_account AccBond,
                   GetCurrencyISO (raccount.t_currency) CurrBond,
                   oaccount.t_account AccMoney,
                   oaccount.t_currency,
                   GetCurrencyISO (oaccount.t_currency) CurrMoney
              INTO repo_rec.ACCOUNT_BOND,
                   repo_rec.CURRENCY_BOND,
                   repo_rec.ACCOUNT_MONEY,
                   fiid_acc_money,
                   repo_rec.CURRENCY_MONEY
              FROM (SELECT t_account, t_currency
                      FROM dmcaccdoc_dbt
                     WHERE t_id =
                              (SELECT MAX (acc.t_id)
                                 FROM dmcaccdoc_dbt acc, dmccateg_dbt cat
                                WHERE     acc.t_catnum = cat.t_number
                                      AND acc.t_dockind = 176
                                      AND acc.t_docid = deal_rec.AccBound
                                      AND cat.t_code IN
                                             ('+ОД', 'Ц/б, БПП')))
                   raccount,
                   (SELECT t_account, t_currency
                      FROM dmcaccdoc_dbt
                     WHERE t_id =
                              (SELECT MAX (acc.t_id)
                                 FROM dmcaccdoc_dbt acc, dmccateg_dbt cat
                                WHERE     acc.t_catnum = cat.t_number
                                      AND acc.t_dockind = 176
                                      AND acc.t_docid = deal_rec.AccBound
                                      AND cat.t_code IN
                                             ('-ОД', 'Ц/б, ПВО')))
                   oaccount;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               repo_rec.ACCOUNT_BOND := CHR (1);
               repo_rec.CURRENCY_BOND := CHR (1);
               repo_rec.ACCOUNT_MONEY := CHR (1);
               repo_rec.CURRENCY_MONEY := CHR (1);
               fiid_acc_money := -1;
         END;

         repo_rec.AMOUNT_MONEY :=
            GetAccountRest (repo_rec.ACCOUNT_MONEY,
                            fiid_acc_money,
                            repo_rec.DEAL_DT);


         BEGIN
            SELECT t_account, t_currency
              INTO repo_rec.ACCOUNT_RERC, fiid_acc_percnt
              FROM dmcaccdoc_dbt
             WHERE t_id =
                      (SELECT MAX (acc.t_id)
                         FROM dmcaccdoc_dbt acc, dmccateg_dbt cat
                        WHERE     acc.t_catnum = cat.t_number
                              AND acc.t_dockind = 176
                              AND acc.t_docid = deal_rec.AccBound
                              AND cat.t_code IN ('+% к погашению'));
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               repo_rec.ACCOUNT_RERC := CHR (1);
               fiid_acc_percnt := -1;
         END;

         repo_rec.AMOUNT_PERC :=
            GetAccountRest (repo_rec.ACCOUNT_RERC,
                            fiid_acc_percnt,
                            repo_rec.DEAL_DT);

         --            repo_rec.REPORT_DT :=
         repo_rec.ISSUER_RATING_SP :=
            GetRate (IssuerId,
                     ObIdIssuer,
                     GrIdIssuer,
                     SaPRat,
                     DateRep2);                                     --Правка З
         repo_rec.ISSUE_RATING_SP :=
            GetRate (IssueId,
                     ObIdIssue,
                     GrIdIssue,
                     SaPRat,
                     DateRep2);                                     --Правка З
         --            repo_rec.ISSUER_RATING_3453U_SP :=
         --            repo_rec.ISSUE_RATING_3453U_SP :
         repo_rec.ISSUER_RATING_MOODYS :=
            GetRate (IssuerId,
                     ObIdIssuer,
                     GrIdIssuer,
                     MoodyRat,
                     DateRep2);                                    -- Правка З
         repo_rec.ISSUE_RATING_MOODYS :=
            GetRate (IssueId,
                     ObIdIssue,
                     GrIdIssue,
                     MoodyRat,
                     DateRep2);                                    -- Правка З
         --            repo_rec.ISSUER_RATING_3453U_MOODYS :=
         --            repo_rec.ISSUE_RATING_3453U_MOODYS :=
         repo_rec.ISSUER_RATING_FITCH :=
            GetRate (IssuerId,
                     ObIdIssuer,
                     GrIdIssuer,
                     FitchRat,
                     DateRep2);                                    -- Правка З
         repo_rec.ISSUE_RATING_FITCH :=
            GetRate (IssueId,
                     ObIdIssue,
                     GrIdIssue,
                     FitchRat,
                     DateRep2);                                    -- Правка З

         --            repo_rec.ISSUER_RATING_3453U_FITCH :=
         --            repo_rec.ISSUE_RATING_3453U_FITCH :=
         --            repo_rec.IS_SPV :=
         --            repo_rec.IS_HYPO_COVER :=
         --            repo_rec.BALANCE_PRICE :=
         --            repo_rec.OVERPRICE :=
         --            repo_rec.TSS_AMOUNT :=
         --            repo_rec.IS_MARG_PAYMENT :=
         --            repo_rec.IS_NO_SELL_LIMIT :=
         --            repo_rec.IS_NO_RETURN_IN_30_DAYS :=
         --            repo_rec.IS_SUBORD_BOND :=
         --            repo_rec.IS_USE_FOR_INDEX :=
         --            repo_rec.DEVALUATION :=

         INSERT INTO tRSHB_REPO_PKL_2CHD
              VALUES repo_rec;

         COMMIT;
      END LOOP;
   END;
END SECUR_DWHINFO;


/* Примеры чтения примечаний разного типа:
 
    select RSB_STRUCT.GETSTRING(t_text) from dnotetext_dbt where t_objecttype = 101 and t_notekind = ? and t_documentid = lpad(?, 34, chr(48));
    select RSB_STRUCT.GETDATE(t_text) from dnotetext_dbt where t_objecttype = 101 and t_notekind = ? and t_documentid = lpad(?, 34, chr(48));
    select RSB_STRUCT.GETMONEY(t_text) from dnotetext_dbt where t_objecttype = 101 and t_notekind = ? and t_documentid = lpad(?, 34, chr(48));
    select RSB_STRUCT.GETDOUBLE(t_text) from dnotetext_dbt where t_objecttype = 101 and t_notekind = ? and t_documentid = lpad(?, 34, chr(48));
*/
/