CREATE OR REPLACE PACKAGE BODY SECUR_DWHINFO
IS
   FUNCTION GetCurrencyISO (fiid IN dfininstr_dbt.t_fiid%TYPE)
      RETURN VARCHAR2 deterministic
   IS
      iso   dfininstr_dbt.t_fiid%TYPE;
   BEGIN
      BEGIN
         SELECT t_codeinaccount
           INTO iso
           FROM dfininstr_dbt
          WHERE t_fiid = fiid;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            iso := null;
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

   FUNCTION GetBalanceCost (accin   IN V_SCWRTHISTEX.T_DEALCODE%TYPE,
                            Ndate   IN DATE, coursedate in date)
      RETURN V_SCWRTHISTEX.t_balancecost%TYPE

   is   BalanceCost V_SCWRTHISTEX.t_balancecost%TYPE;
      begin
      BEGIN
      select RSB_FIInstr.ConvSumtype(rest.t_rest,REST.T_RESTCURRENCY, 0,7,coursedate)  into BalanceCost  from daccount_dbt acct left join drestdate_dbt rest on acct.t_accountid = rest.t_accountid
       where rest.t_restdate =(select max( rest2.t_restdate) from drestdate_dbt rest2 where REST2.T_ACCOUNTID = acct.t_accountid and rest2.t_restdate<=   Ndate) and acct.t_account = accin and t_restcurrency = 0;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            BalanceCost := 0;
           --BalanceCost := GetBalanceCost (accin, Ndate-1, coursedate);
      END;
      return BalanceCost;
      end;

FUNCTION GetOveramount (accin in V_SCWRTHISTEX.T_DEALCODE%type,
                       Ndate in Date, REPO_DIRECTION in trshb_repo_pkl_2chd.REPO_DIRECTION%type )
      return V_SCWRTHISTEX.T_OVERAMOUNT%type
      is   Overamount V_SCWRTHISTEX.T_OVERAMOUNT%type;
      begin
      BEGIN
      SELECT sum(hist.T_OVERAMOUNT) into Overamount
  FROM V_SCWRTHISTEX hist
 WHERE    hist.T_DEALCODE = accin
       --AND hist.T_CHANGEDATE = Ndate
       AND hist.t_instance = (SELECT MAX (hist2.t_instance)
                          FROM V_SCWRTHISTEX hist2
                         WHERE HIST2.T_SUMID = HIST.T_SUMID AND hist2.T_CHANGEDATE <= Ndate);
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            Overamount := 0;
      END;
            case when REPO_DIRECTION = 'прямое РЕПО' then Overamount := Overamount + GetOveramountRep(accin, Ndate);
      else Overamount := 0 ;
      end case;
      return Overamount;
      end;
FUNCTION GetOveramountRep (accin in V_SCWRTHISTEX.T_DEALCODE%type,
                       Ndate in Date)
      return V_SCWRTHISTEX.T_OVERAMOUNT%type
      is   Overamount V_SCWRTHISTEX.T_OVERAMOUNT%type;
      begin
      BEGIN
       SELECT  nvl(sum(hist.T_OVERAMOUNT),0)into Overamount
  FROM V_SCWRTHISTEX hist 
  WHERE  HIST.T_PARENT in (select distinct(hist1.t_sumid) from V_SCWRTHISTEX hist1 where hist1.T_DEALCODE = accin and hist1.t_portfolio <> 45)
      AND hist.t_instance = (SELECT MAX (hist2.t_instance)
                          FROM V_SCWRTHISTEX hist2
                         WHERE HIST2.T_SUMID = HIST.T_SUMID AND hist2.T_CHANGEDATE <= Ndate)
                         AND HIST.T_STATE = 3;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            Overamount := 0;
      END;
      return Overamount;
      end;
      
FUNCTION GetAmount (Paper   IN dfininstr_dbt.T_FIID%TYPE,
                           Ndate   IN DATE)
      RETURN TRSHB_REPO_PKL_2CHD.TSS_AMOUNT%TYPE
      is
      Amount TRSHB_REPO_PKL_2CHD.TSS_AMOUNT%TYPE;
            begin 
      begin 
      SELECT hist.t_rate / hist.t_scale / POWER (10, hist.t_point) into Amount
  FROM (  SELECT d.t_rateid,
          t_type,
          d.t_sincedate,
          d.t_scale,
          t_fiid,
          t_otherfi,
          d.t_inputdate,
          d.t_rate,
          d.t_point,
          d.t_isinverse,
          CHR (0) AS t_isdef,
          nvl((SELECT MIN (t_sincedate)
             FROM (SELECT t_rateid, t_sincedate FROM dratedef_dbt
                   UNION
                   SELECT t_rateid, t_sincedate FROM dratehist_dbt) s
            WHERE s.t_rateid = d.t_RateID AND s.t_sincedate > d.t_SinceDate), to_date('31129999', 'ddmmyyyy'))
             AS T_TODATE from dratedef_dbt d
   UNION
   SELECT h.t_rateid,
          t_type,
          h.t_sincedate,
          h.t_scale,
          t_fiid,
          t_otherfi,
          h.t_inputdate,
          h.t_rate,
          h.t_point,
          h.t_isinverse,
          CHR (0) AS t_isdef,
          nvl((SELECT MIN (t_sincedate)
             FROM (SELECT t_rateid, t_sincedate FROM dratedef_dbt
                   UNION
                   SELECT t_rateid, t_sincedate FROM dratehist_dbt) s
            WHERE s.t_rateid = h.t_RateID AND s.t_sincedate > h.t_SinceDate), to_date('31129999', 'ddmmyyyy'))
             AS T_TODATE
     FROM dratehist_dbt h JOIN dratedef_dbt d ON (h.t_rateid = d.t_rateid)) hist
 WHERE HIST.T_rateid =
          (SELECT t_rateid
             FROM dratedef_dbt def
            WHERE     DEF.T_TYPE = 12
                  AND DEF.T_OTHERFI = Paper
                  AND DEF.T_FIID = (select t_facevaluefi from dfininstr_dbt where t_fiid = Paper) )
       AND hist.T_SINCEDATE =
              (SELECT min(T_SINCEDATE)
                 FROM dratehist_dbt
                WHERE T_rateid =
                         (SELECT t_rateid
                            FROM dratedef_dbt def
                           WHERE     DEF.T_TYPE = 12
                                 AND DEF.T_OTHERFI = Paper
                                 AND DEF.T_FIID = (select t_facevaluefi from dfininstr_dbt where t_fiid = Paper))
                      AND hist.T_SINCEDATE >= Ndate );
               EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
           Amount:= 0;
      END;
      return Amount;
      end;
      
       FUNCTION GetSPV (partyid IN dparty_dbt.t_partyid%TYPE)
      RETURN TRSHB_REPO_PKL_2CHD.IS_SPV%type
      is
      OUTNAME TRSHB_REPO_PKL_2CHD.IS_SPV%type;
      begin 
      begin
      select a.t_name  INTO OUTNAME  
               from dobjattr_dbt a 
               join dobjatcor_dbt c on a.t_objecttype = c.t_objecttype and a.t_groupid = c.t_groupid and a.t_attrid = c.t_attrid
                where c.t_objecttype = 3 
                 and c.t_groupid = 110 
                 and c.t_object = lpad( Partyid, 10, '0') 
                  and c.t_validtodate = to_date('31.12.9999', 'dd.mm.yyyy');
EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         OUTNAME := NULL;
   END;
   return OUTNAME;
end;      
   FUNCTION GetHypoCover (fiid IN dparty_dbt.t_partyid%TYPE)
      RETURN TRSHB_REPO_PKL_2CHD.IS_HYPO_COVER%type
      is
      OUTNAME TRSHB_REPO_PKL_2CHD.IS_HYPO_COVER%type;
      begin 
      begin
      select a.t_name  INTO OUTNAME  
               from dobjattr_dbt a 
               join dobjatcor_dbt c on a.t_objecttype = c.t_objecttype and a.t_groupid = c.t_groupid and a.t_attrid = c.t_attrid
                where c.t_objecttype = 12
                 and c.t_groupid = 102
                 and c.t_object = lpad( fiid, 10, '0') 
                  and c.t_validtodate = to_date('31.12.9999', 'dd.mm.yyyy');
EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         OUTNAME := NULL;
   END;
   return OUTNAME;
end; 
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
   RETURN dobjattr_dbt.t_name%TYPE  RESULT_CACHE
IS
   out_Name   dobjattr_dbt.t_name%TYPE;
   DateTmp    dobjatcor_dbt.T_sysdate%TYPE;
   TimeTmp    dobjatcor_dbt.t_systime%TYPE;
BEGIN
   BEGIN
      SELECT t_name                                                   
        INTO out_name
        from ( select  attr.t_name
                 ,COR.T_SYSDATE
                 ,max(cor.T_sysdate) over() max_sysdate
                 ,cor.T_systime
                 ,max(cor.T_systime) over(partition by cor.T_sysdate) max_systime
                 FROM    dobjatcor_dbt cor
                 LEFT JOIN  dobjattr_dbt attr
               ON  cor.t_objecttype = attr.t_objecttype
                AND cor.t_groupid = attr.t_groupid
                AND cor.t_attrid = attr.t_attrid
                WHERE  TO_NUMBER (cor.T_OBJECT) = Tid
                    AND cor.t_groupid = GrId
                    AND cor.t_objecttype = ObId
                    AND attr.t_parentid = ParId
                    AND ndate BETWEEN cor.t_validfromdate AND cor.t_validtodate
               )
             where  T_SYSDATE = max_SYSDATE
                   AND T_systime = max_systime; 
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         out_name := NULL;
   END;
 
 /*  BEGIN
      SELECT MAX (cor.T_sysdate)
        INTO DateTmp
        FROM    dobjatcor_dbt cor
             LEFT JOIN
                dobjattr_dbt attr
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
        FROM    dobjatcor_dbt cor
             LEFT JOIN
                dobjattr_dbt attr
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
        FROM    dobjatcor_dbt cor
             LEFT JOIN
                dobjattr_dbt attr
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
   END;*/

   RETURN out_name;
END;

PROCEDURE PrepareREPO (DateRep1 IN DATE)
IS
   repo_rec                tRSHB_REPO_PKL_2CHD%ROWTYPE; --структура, в которую будем собирать инфу из разных селектов
   fiid_acc_money          dfininstr_dbt.t_fiid%TYPE;
   fiid_Paper                  dfininstr_dbt.t_fiid%TYPE;
   fiid_acc_percnt         dfininstr_dbt.t_fiid%TYPE;
   acc_repo_in_repo        daccount_dbt.t_account%TYPE;
   acc_fiid_repo_in_repo   dfininstr_dbt.t_fiid%TYPE;
   IssueId                 dparty_dbt.t_partyid%TYPE;       --ид ценной бумаги
   IssuerId                dparty_dbt.t_partyid%TYPE;            --ид эмитента
   SaPRat         CONSTANT dobjattr_dbt.t_parentid%TYPE := 110; --Ид Рейтинга Standard and Poor's
   MoodyRat       CONSTANT dobjattr_dbt.t_parentid%TYPE := 210; --ид рейтинга Moody
   FitchRat       CONSTANT dobjattr_dbt.t_parentid%TYPE := 310; --ид рейтинга Fitch IBCA
   ObIdIssue      CONSTANT dobjattr_dbt.t_objecttype%TYPE := 12; -- Ид типа объекта для ц/б
   ObIdIssuer     CONSTANT dobjattr_dbt.t_objecttype%TYPE := 3; -- Ид типа объекта для эмитента ц/б
   GrIdIssue      CONSTANT dobjattr_dbt.t_objecttype%TYPE := 53; -- Ид категории для ц/б
   GrIdIssuer     CONSTANT dobjattr_dbt.t_objecttype%TYPE := 19; -- Ид категории для эмитента ц/б
   subjectid               dparty_dbt.t_partyid%TYPE;
   tmp              number;
   --GAA: 540515
   cost_         V_SCWRTHISTEX.T_COST%type;
   --GAA
   nkd          V_SCWRTHISTEX.T_NKDAMOUNT%type;
   interest    V_SCWRTHISTEX.t_interestincome%type;
   discount   V_SCWRTHISTEX.T_DISCOUNTINCOME%type;
   begbonus V_SCWRTHISTEX.T_BEGBONUS%type;
   bonus      v_scwrthistex.t_bonus%type;
   issubord davoiriss_dbt.t_subordinated%Type;
   changedate    date;
   valuefi           DFININSTR_DBT.T_FACEVALUEFI%type;
   spread constant NUMBER   :=1; --1 - размазать переоценку и корректировку пропорционально количеству бумаг 0 - не размазывать
   acc_sold          daccount_dbt.t_account%TYPE;
   acc_left          daccount_dbt.t_account%TYPE;
   bal_price_sold   tRSHB_REPO_PKL_2CHD.balance_price%TYPE;
   bal_price_left   tRSHB_REPO_PKL_2CHD.balance_price%TYPE;
   quantity_sold    tRSHB_REPO_PKL_2CHD.quantity%TYPE;
   quantity_left    tRSHB_REPO_PKL_2CHD.quantity%TYPE;
   Amount_money_sold  tRSHB_REPO_PKL_2CHD.amount_money%TYPE;
   Amount_money_left  tRSHB_REPO_PKL_2CHD.amount_money%TYPE; 
BEGIN
--   DELETE FROM tRSHB_REPO_PKL_2CHD;                                 --временно

   --COMMIT;

   --основной select по сделке
   --осознанно откидываем сделки с портфелями, они будут обрабатываться отдельно
 FOR deal_rec
      IN (   SELECT DISTINCT
                   deal.t_dealid AS DealID,
                   deal.t_dealcode AS DealCode,
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
                   avo.t_isin ISIN,
                   leg1.t_id AccBound,
                   deal.t_BOfficeKind DealKind
              FROM ddlrq_dbt pm,
                   ddl_tick_dbt deal,
                   ddl_leg_dbt leg1,
                   dfininstr_dbt curr,
                   davoiriss_dbt avo,
                   doprkoper_dbt opr,
                   dfininstr_dbt curr1
             WHERE pm.t_dockind = 101 AND pm.t_type IN (0, 2, 8)
                   AND (pm.t_plandate > DateRep1
                        OR (    pm.t_plandate <= DateRep1
                            AND pm.t_state IN (0, 2)
                            AND deal.t_dealstatus = 10))
                   AND deal.t_dealdate <= DateRep1
                   AND deal.t_dealid = pm.t_docid
                   AND deal.t_dealid = leg1.t_dealid
                   AND leg1.t_legkind = 0
                   AND leg1.t_cfi = curr.t_fiid
                   AND deal.t_pfi = avo.t_fiid
                   and deal.t_pfi = curr1.t_fiid
                   AND opr.t_dockind = 101
                   AND deal.t_dealtype = opr.t_kind_operation
                   and deal.t_clientid = -1
                   AND INSTR (opr.t_systypes, 't') > 0
                   and CURR1.T_AVOIRKIND <> 48
--                   AND deal.t_dealid in (3133050 /*3133044, 3133042, 3133052, 3133054, 3133056, 3133052,  3133056*/)
          ORDER BY t_dealdate)
   LOOP
      repo_rec.DEAL_NUMBER := deal_rec.DealCode;
      repo_rec.DEAL_DT := deal_rec.DealDate;
      repo_rec.PART_1_EX_DT := deal_rec.DatePay1;
      repo_rec.ISIN := deal_rec.ISIN;

      BEGIN
         SELECT CASE
                   WHEN INSTR (LOWER (t_name), 'прям') > 0
                        AND INSTR (LOWER (t_name), 'репо') > 0
                   THEN
                      'прямое РЕПО'
                   WHEN INSTR (LOWER (t_name), 'обр') > 0
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

      IF (Rsb_Secur.
           IsOutExchange (
             Rsb_Secur.
              get_OperationGroup (
                Rsb_Secur.
                 get_OperSysTypes (deal_rec.DealType, deal_rec.DealKind))) >
             0)
      THEN
         repo_rec.REPO_EXCHANGE := 'внебиржевая';
         repo_rec.IS_LIQ_NETTING := CHR (1);
         repo_rec.IS_CLC_NETTING := CHR (1);
    
      ELSE
         repo_rec.REPO_EXCHANGE := 'биржевая';
         repo_rec.IS_LIQ_NETTING := 'ДА';
         repo_rec.IS_CLC_NETTING := 'ДА';
         repo_rec.IS_REPOSITORY := 'НЕТ';
      END IF;

     
      
      select case when repo_rec.repo_direction  = 'обратное РЕПО' 
                       and instr((select t_sysTypes  
                                from doprkoper_dbt
                                where t_kind_operation 
                                      in (select tick.t_dealtype from ddl_tick_dbt tick
                                            where tick.t_dealid 
                                                   in (select hist1.t_dealid
                                                         from V_SCWRTHISTEX hist1
                                                         where hist1.t_sumid 
                                                               in (SELECT hist.t_parent
                                                                   FROM V_SCWRTHISTEX hist 
                                                                   WHERE hist.t_dealcode = deal_rec.DealCode and HIST.T_PARENT <> 0
                                                                         and hist.t_instance 
                                                                            = (SELECT MAX (hist2.t_instance) FROM V_SCWRTHISTEX hist2
                                                                              WHERE HIST2.T_SUMID = HIST.T_SUMID AND hist2.T_CHANGEDATE <= repo_rec.DEAL_DT)
                                                                         
                                                                ) and HIST1.T_DEALCODE <> deal_rec.DealCode
                                                               and hist1.t_instance 
                                                               =  (SELECT MAX (hist3.t_instance)
                                                                    FROM V_SCWRTHISTEX hist3
                                                                    WHERE HIST3.T_SUMID = HIST1.T_SUMID AND hist3.T_CHANGEDATE <= repo_rec.DEAL_DT
                                                                   )
                                                          )
                                             )
                                      ), 't') <> 0
                              
             then 'да'
             else 'нет'
             end case into repo_rec.REPO_IN_REPO from dual;

      BEGIN
         SELECT subj.t_shortname AS ContragentName,
                (SELECT t_name
                   FROM dcountry_dbt
                  WHERE t_codelat3 = subj.t_nrcountry)
                   AS ContragentCountry,
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

      -- KS 11.04.2022 I-sup 541347 Признак финансовой организации необходимо определять для эмитента ценной бумаги по сделке (dfininstr_dbt.t_issuer)
      --               repo_rec.IS_NFO определю ниже
      --repo_rec.IS_NFO := DefineIsNFO (subjectid);

      SELECT CASE
                WHEN EXISTS
                        (SELECT 1
                           FROM dobjatcor_dbt o, dobjattr_dbt a
                          WHERE o.t_objecttype = 3 AND o.t_groupid = 19
                                /* смотрим наличие только высоких рейтингов: */
                                AND ASCII (SUBSTR (UPPER (a.t_name), 1, 1)) IN
                                       (65                          /*англ A*/
                                          , 53392)                  /*русс А*/
                                AND o.t_object =
                                       LPAD (deal_rec.DealContragent,
                                             10,
                                             CHR (48))
                                AND o.t_validtodate =
                                       TO_DATE ('31.12.9999', 'dd.mm.yyyy')
                                AND a.t_objecttype = o.t_objecttype
                                AND a.t_groupid = o.t_groupid
                                AND a.t_attrid = o.t_attrid)
                THEN
                   'Выше "В"'
                ELSE
                   null
             END
        INTO repo_rec.SUBJ_RATING
        FROM DUAL;

      select nvl(min(t_changedate), to_date('01/01/0001', 'dd/mm/yyyy')) into changedate from ddl_tick_dbt  where  t_dealid = deal_rec.dealid and t_changedate >= DateRep1;
      if changedate > DateRep1
        then 
             begin
                select chng.t_oldprincipal2, chng.t_oldincomerate,
                (SELECT DECODE (t_factdate,
                                TO_DATE ('01010001', 'ddmmyyyy'), t_plandate,
                                t_factdate)
                   FROM ddlrq_dbt
                  WHERE     t_docid = deal_rec.DealID
                        AND t_type = 2
                        AND t_dealpart = 2
                        AND t_dockind = 101)
                   AS DatePay2
               INTO repo_rec.QUANTITY, repo_rec.RATE, repo_rec.PART_2_EX_DT
               from dsptkchng_dbt chng
               where chng.t_dealid = deal_rec.dealid 
               and chng.t_oldchangedate = (select max(t_oldchangedate) from dsptkchng_dbt where t_dealid = deal_rec.dealid and t_oldchangedate<=DateRep1);
             exception
                      WHEN NO_DATA_FOUND
               THEN
                repo_rec.QUANTITY := 0;
                repo_rec.RATE := 0;
                repo_rec.PART_2_EX_DT := TO_DATE ('01010001', 'ddmmyyyy');
             end;
        else
              BEGIN
                SELECT 
                leg2.t_principal AS L2Amount,
                leg2.t_incomerate AS L2PercentsREPO,
                (SELECT DECODE (t_factdate,
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
     end if; 

      repo_rec.QUANTITY := (repo_rec.QUANTITY); /* + deal_rec.L1Amount)/2;  */ /*PDV 524544 */

      BEGIN
         SELECT SUBSTR (p.t_shortname, 1, 60),
                SUBSTR (fi.t_name, 1, 25),
                (SELECT t_name
                   FROM dcountry_dbt
                  WHERE t_codelat3 = p.t_nrcountry) /*nvl(a.t_country, chr(1))*/
                                                   ,
                v.t_incirculationdate,
                fi.t_fiid,
                fi.t_issuer
           INTO repo_rec.ISSUER,
                repo_rec.ISSUE_NAME,
                repo_rec.ISSUER_COUNTRY,
                repo_rec.DEPLOY_DT,
                issueid,                             -- правка З узнаем ид ц/б
                issuerid                    -- правка З узнаем ид эмитента ц/б
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

      -- KS 11.04.2022 I-sup 541347 Признак финансовой организации необходимо определять для эмитента ценной бумаги по сделке (dfininstr_dbt.t_issuer)
      repo_rec.IS_NFO := DefineIsNFO (issuerid);

      BEGIN
         SELECT TO_CHAR (t_riskclass)
           INTO repo_rec.COUNTRY_RATING
           FROM dcountry_dbt
          WHERE t_codelat3 = repo_rec.ISSUER_COUNTRY
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
                AND o.t_object = LPAD (deal_rec.DealFI, 10, CHR (48))
                AND o.t_validtodate = TO_DATE ('31.12.9999', 'dd.mm.yyyy')
                AND a.t_objecttype = o.t_objecttype
                AND a.t_groupid = o.t_groupid
                AND a.t_attrid = o.t_attrid;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            repo_rec.SECURITY_TYPE := CHR (1);
      END;

      declare
        v_operationgroup number ;
      BEGIN
         select rsb_secur.get_operationgroup(rsb_secur.get_opersystypes((select t_dealtype 
                                                                         from ddl_tick_dbt 
                                                                          where t_dealid 
                                                                              = ( select t_dealid 
                                                                                   from ddl_leg_dbt 
                                                                                    where t_id = deal_rec.AccBound)) , 101)
                                             ) into v_operationgroup from dual;
         SELECT raccount.t_account AccBond,
                abs(GetCurrencyISO (nvl(raccount.t_currency,-1))) CurrBond,
                oaccount.t_account AccMoney,
                nvl(oaccount.t_currency, 0),
                abs(GetCurrencyISO (nvl(oaccount.t_currency, -1))) CurrMoney
           INTO repo_rec.ACCOUNT_MONEY,
                repo_rec.CURRENCY_BOND,
                repo_rec.ACCOUNT_BOND,
                fiid_acc_money,
                repo_rec.CURRENCY_MONEY
            FROM (SELECT t_account, t_currency,CASE WHEN t_docid != 0 THEN t_docid ELSE  deal_rec.AccBound END t_docid
                   FROM dmcaccdoc_dbt
                  WHERE t_id = (select max(tid) KEEP (DENSE_RANK FIRST ORDER BY t_pr)
                                from 
                                     ((SELECT MAX (acc.t_id) tid,  1 t_pr
                                      FROM dmcaccdoc_dbt acc, dmccateg_dbt cat
                                      WHERE     acc.t_catnum = cat.t_number
                                            AND acc.t_dockind = 176
                                            AND acc.t_docid = deal_rec.AccBound
                                            AND cat.t_code IN
                                                ('Ц/б, БПП', 'Ц/б, ПВО', 'Ц/б, ПВО_БПП'))
                                union
                                SELECT MAX (acc.t_id) tid, 1 t_pr
                                FROM dmcaccdoc_dbt acc, dmccateg_dbt cat, dmctempl_dbt templ
                                WHERE     acc.t_catnum = cat.t_number
                                      AND acc.t_dockind = 176
                                      AND acc.t_docid = deal_rec.AccBound
                                      AND acc.t_catid = templ.t_catid
                                      AND templ.t_number = acc.t_templnum
                                      AND templ.t_value4 = 1
                                      AND cat.t_code IN ('Наш портфель ц/б')
                                      AND rsb_account.restac (ACC.T_ACCOUNT, ACC.T_CURRENCY, DateRep1, ACC.T_CHAPTER, ACC.T_CURRENCY) != 0
                                UNION
                                SELECT /*+ ordered */  MAX (acc.t_id) tid, 2 t_pr /*MVA:546918 ищем общесистемный счет по выпуску (с признаком БПП), если не найден счет по сделке*/
                                FROM dmccateg_dbt cat,
                                     dmcaccdoc_dbt acc,
                                     dmctempl_dbt templ
                                WHERE    acc.t_catid = cat.t_id  --acc.t_catnum = cat.t_number
                                     AND acc.t_iscommon = CHR(88)
                                     AND acc.t_dockind = 0
                                     AND acc.t_docid = 0
                                     AND acc.t_catid = templ.t_catid
                                     AND templ.t_number = acc.t_templnum
                                     AND templ.t_value4 = 1
                                     AND cat.t_code IN ('Наш портфель ц/б')
                                     AND acc.t_fiid = deal_rec.DealFI
                                     AND rsb_account.restac (ACC.T_ACCOUNT, ACC.T_CURRENCY, DateRep1, ACC.T_CHAPTER, ACC.T_CURRENCY) != 0)
                                WHERE tid IS NOT NULL)) oaccount
            left join (SELECT t_account, t_currency, t_docid
                         FROM dmcaccdoc_dbt
                         WHERE t_id =
                           (SELECT acc.t_id
                              FROM dmcaccdoc_dbt acc, dmccateg_dbt cat
                             WHERE     acc.t_catnum = cat.t_number
                                   AND acc.t_dockind = 176
                                   AND acc.t_docid =deal_rec.AccBound
                                   AND( (cat.t_code IN ('+ОД') and rsb_secur.isbuy (v_operationgroup) <> 0 ) OR
                                   (cat.t_code IN ('-ОД') AND rsb_secur.issale(v_operationgroup) <> 0 ) ))) raccount 
             on raccount.t_docid = oaccount.t_docid;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            repo_rec.ACCOUNT_BOND := CHR (1);
            repo_rec.CURRENCY_BOND := CHR (1);
            repo_rec.ACCOUNT_MONEY := CHR (1);
            repo_rec.CURRENCY_MONEY := CHR (1);
            fiid_acc_money := -1;
      END;
      
      BEGIN
         SELECT nvl(t_account, chr(1)), nvl(t_currency, -1)
           INTO repo_rec.ACCOUNT_RERC, fiid_acc_percnt
           FROM dmcaccdoc_dbt
          WHERE t_id =
                   (SELECT MAX (acc.t_id)
                      FROM dmcaccdoc_dbt acc, dmccateg_dbt cat
                     WHERE     acc.t_catnum = cat.t_number
                           AND acc.t_dockind = 176
                           AND acc.t_docid in (select t_id from ddl_leg_dbt where t_dealid = (select t_dealid from ddl_leg_dbt where t_id = deal_rec.AccBound))
                           AND cat.t_code IN ('+% к погашению', '-% к погашению'));
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            repo_rec.ACCOUNT_RERC := CHR (1);
            fiid_acc_percnt := -1;
      END;
     
      repo_rec.REPORT_DT := DateRep1;
      
     repo_rec.AMOUNT_PERC :=abs(Getbalancecost(repo_rec.ACCOUNT_RERC, repo_rec.REPORT_DT,repo_rec.REPORT_DT ));
    
      repo_rec.ISSUER_RATING_SP :=
         GetRate (IssuerId,
                  ObIdIssuer,
                  GrIdIssuer,
                  SaPRat,
                  DateRep1);                                        --Правка З
      repo_rec.ISSUE_RATING_SP :=
         GetRate (IssueId,
                  ObIdIssue,
                  GrIdIssue,
                  SaPRat,
                  DateRep1);                                        --Правка З
                  repo_rec.ISSUER_RATING_3453U_SP :=GetRate (IssuerId,
                  ObIdIssuer,
                  GrIdIssuer,
                  459,
                  DateRep1); 
                  repo_rec.ISSUE_RATING_3453U_SP :=GetRate (IssueId,
                  ObIdIssue,
                  GrIdIssue,
                  459,
                  DateRep1);
      repo_rec.ISSUER_RATING_MOODYS :=
         GetRate (IssuerId,
                  ObIdIssuer,
                  GrIdIssuer,
                  MoodyRat,
                  DateRep1);                                       -- Правка З
      repo_rec.ISSUE_RATING_MOODYS :=
         GetRate (IssueId,
                  ObIdIssue,
                  GrIdIssue,
                  MoodyRat,
                  DateRep1);                                       -- Правка З
                  repo_rec.ISSUER_RATING_3453U_MOODYS :=GetRate (IssuerId,
                  ObIdIssuer,
                  GrIdIssuer,
                  483,
                  DateRep1); 
                  repo_rec.ISSUE_RATING_3453U_MOODYS :=GetRate (IssueId,
                  ObIdIssue,
                  GrIdIssue,
                  483,
                  DateRep1);  
      repo_rec.ISSUER_RATING_FITCH :=
         GetRate (IssuerId,
                  ObIdIssuer,
                  GrIdIssuer,
                  FitchRat,
                  DateRep1);                                       -- Правка З
      repo_rec.ISSUE_RATING_FITCH :=
         GetRate (IssueId,
                  ObIdIssue,
                  GrIdIssue,
                  FitchRat,
                  DateRep1);                                       -- Правка З

      repo_rec.ISSUER_RATING_3453U_FITCH :=GetRate (IssuerId,
                  ObIdIssuer,
                  GrIdIssuer,
                  505,
                  DateRep1);
                 repo_rec.ISSUE_RATING_3453U_FITCH :=GetRate (IssueId,
                  ObIdIssue,
                  GrIdIssue,
                  505,
                  DateRep1);
       repo_rec.IS_SPV := getSPV(IssuerId);
       repo_rec.IS_HYPO_COVER :=GetHypoCover(deal_rec.DealFI);

             case when repo_rec.repo_direction  = 'обратное РЕПО'
             then  repo_rec.OVERPRICE := 0;
             else  repo_rec.OVERPRICE := GetOveramount(deal_rec.DealCode, repo_rec.REPORT_DT, repo_rec.REPO_DIRECTION);
             end case;
-- GAA:540515. Закоментировал данный код, т.к. при наличии двух и более сделок с одной ц/б балансовая стоимость по отдельным сделкам вычисляется не верно
-- Будем брать информацию с лотов.               
             repo_rec.BALANCE_PRICE :=  abs(Getbalancecost(repo_rec.ACCOUNT_BOND, repo_rec.REPORT_DT, repo_rec.REPORT_DT));  
--             repo_rec.BALANCE_PRICE :=  0;
 
             if SUBSTR(repo_rec.ACCOUNT_BOND, 1,3) != '914' THEN
             case when repo_rec.repo_direction  = 'прямое РЕПО' 
             then 
             repo_rec.BALANCE_PRICE :=  0; /*GAA:540515*/
             SELECT   nvl(sum(t_cost),0),/*GAA:540515*/
             nvl(sum(t_nkdamount),0),nvl(sum(t_interestincome),0),nvl(sum(t_discountincome),0),nvl(sum(t_begbonus),0), nvl(sum(t_bonus),0) 
               into cost_,  /*GAA:540515*/
               nkd, interest, discount, begbonus, bonus
                  FROM V_SCWRTHISTEX hist 
                    WHERE  HIST.T_PARENT in (select distinct(hist1.t_sumid) from V_SCWRTHISTEX hist1 where hist1.T_DEALCODE = deal_rec.DealCode and hist1.t_portfolio <> 45)
                        AND hist.t_instance = (SELECT MAX (hist2.t_instance)
                          FROM V_SCWRTHISTEX hist2
                         WHERE HIST2.T_SUMID = HIST.T_SUMID AND hist2.T_CHANGEDATE <= repo_rec.REPORT_DT)
                         AND (HIST.T_STATE = 3 OR HIST.T_PORTFOLIO = 8); /*MVA:546918 при списании из ПВО созданный лот по второй части препо находится в +ОД со статусом "не поставлен"*/
              select T_FACEVALUEFI into valuefi from dfininstr_dbt where t_fiid = issueid;
/*GAA:540515*/              
              repo_rec.BALANCE_PRICE := repo_rec.BALANCE_PRICE   +  RSB_FIInstr.ConvSum (cost_ + nkd + interest + discount,
                                                valuefi,
                                                0,
                                                repo_rec.REPORT_DT);              
/*GAA old^              
              repo_rec.BALANCE_PRICE := repo_rec.BALANCE_PRICE   +  RSB_FIInstr.ConvSum (nkd + interest + discount + begbonus -bonus,
                                                valuefi,
                                                0,
                                                repo_rec.REPORT_DT);*/
              else repo_rec.BALANCE_PRICE := repo_rec.BALANCE_PRICE ;
              end case;
            END IF;
         repo_rec.ISSUER_RATING_AKRA := GetRate (IssuerId,
                  ObIdIssuer,
                  GrIdIssuer,
                  436,
                  DateRep1);     
        repo_rec.ISSUE_RATING_AKRA   := GetRate (IssueId,
                  ObIdIssue,
                  GrIdIssue,
                  436,
                  DateRep1); 
                  repo_rec.ISSUER_RATING_EXPERT := GetRate (IssuerId,
                  ObIdIssuer,
                  GrIdIssuer,
                  367,
                  DateRep1);     
        repo_rec.ISSUE_RATING_EXPERT   := GetRate (IssueId,
                  ObIdIssue,
                  GrIdIssue,
                  367,
                  DateRep1);


                  repo_rec.AMOUNT_MONEY :=abs(Getbalancecost(repo_rec.ACCOUNT_MONEY, repo_rec.REPORT_DT, repo_rec.REPORT_DT));
                  select t_FaceValueFI into fiid_Paper  from dfininstr_dbt where t_fiid =deal_rec.DealFI;
                  repo_rec.TSS_AMOUNT := repo_rec.BALANCE_PRICE + repo_rec.OVERPRICE;--nvl(RSB_FIInstr.ConvSumtype(1, deal_rec.DealFI,fiid_Paper,12, repo_rec.REPORT_DT), 0);
         case when( GetRate (IssueId, 12, 103, 0, DateRep1) = 'Да' or GetRate (IssueId, 12, 104, 0, DateRep1) = 'Да')
         then repo_rec.IS_USE_FOR_INDEX := 'Да';
         else repo_rec.IS_USE_FOR_INDEX := 'Нет';
         end case;
         select t_subordinated into issubord  from davoiriss_dbt where t_fiid = IssueId;
         case when issubord = 'X'
         then repo_rec.IS_SUBORD_BOND := 'Да';
         else repo_rec.IS_SUBORD_BOND := 'Нет';
         end case;
        
        begin
                select i.t_indicator into repo_rec.devaluation from f657_ind_dep i 
                where i.t_isin = repo_rec.ISIN 
                and i.t_date_load = (select max(di.t_date_load) from f657_ind_dep di 
                where di.t_isin = repo_rec.ISIN 
                and di.t_date_load <= repo_rec.report_dt)
                 and rownum = 1;
      exception
                WHEN NO_DATA_FOUND
                THEN
                repo_rec.devaluation := 0;
      END;

      /*ZYY Рассматриваем ситуацию, когда бумаги по обратному репо были проданы*/  
      case when repo_rec.repo_direction  = 'обратное РЕПО'
      then 
      acc_sold := '';            
           BEGIN
            SELECT t_account
             into acc_sold
                   FROM dmcaccdoc_dbt
                  WHERE t_id =
                           (SELECT acc.t_id
                              FROM dmcaccdoc_dbt acc, dmccateg_dbt cat
                             WHERE     acc.t_catnum = cat.t_number
                                   AND acc.t_dockind = 176
                                   AND acc.t_docid =deal_rec.AccBound
                                   AND cat.t_code IN ('-ОД')
                                   AND rsb_account.restac (ACC.T_ACCOUNT, ACC.T_CURRENCY, repo_rec.REPORT_DT, ACC.T_CHAPTER, ACC.T_CURRENCY) != 0 );
            EXCEPTION
                WHEN NO_DATA_FOUND
                THEN  acc_sold := '';  
            END;

           IF LENGTH(acc_sold) is not NULL THEN
               repo_rec.ACCOUNT_BOND:= acc_sold;                    
               repo_rec.BALANCE_PRICE :=  abs(Getbalancecost(repo_rec.ACCOUNT_BOND, repo_rec.REPORT_DT, repo_rec.REPORT_DT)); 
               repo_rec.TSS_AMOUNT := repo_rec.BALANCE_PRICE + repo_rec.TSS_AMOUNT;
               INSERT INTO tRSHB_REPO_PKL_2CHD  VALUES repo_rec;
               COMMIT;
               
           else
             INSERT INTO tRSHB_REPO_PKL_2CHD  VALUES repo_rec;
                COMMIT;
           END IF;
           --Проданы не все бумаги - выводим в 2 строчки
            /*else
            SELECT t_account
             into acc_sold
                   FROM dmcaccdoc_dbt
                  WHERE t_id =
                           (SELECT acc.t_id
                              FROM dmcaccdoc_dbt acc, dmccateg_dbt cat
                             WHERE     acc.t_catnum = cat.t_number
                                   AND acc.t_dockind = 176
                                   AND acc.t_docid =deal_rec.AccBound
                                   AND cat.t_code IN ('-ОД') );
           quantity_left :=repo_rec.quantity - quantity_sold;
           Amount_money_sold  := repo_rec.Amount_money / repo_rec.quantity * quantity_sold;
           Amount_money_left    := repo_rec.Amount_money / repo_rec.quantity * quantity_left;
           repo_rec.quantity :=quantity_left;
           repo_rec.Amount_money :=     Amount_money_left;
            INSERT INTO tRSHB_REPO_PKL_2CHD  VALUES repo_rec;
           COMMIT;
           repo_rec.ACCOUNT_BOND:= acc_sold;                    
           repo_rec.BALANCE_PRICE :=  abs(Getbalancecost(repo_rec.ACCOUNT_BOND, repo_rec.REPORT_DT, repo_rec.REPORT_DT)); 
           repo_rec.TSS_AMOUNT := repo_rec.BALANCE_PRICE + repo_rec.TSS_AMOUNT;
           repo_rec.Amount_money :=     Amount_money_sold; 
           INSERT INTO tRSHB_REPO_PKL_2CHD  VALUES repo_rec;
           commit;*/        
     else
        INSERT INTO tRSHB_REPO_PKL_2CHD  VALUES repo_rec;
       COMMIT;
     end case;
      END LOOP;



      --обработка сделок с портфелями, немного другой алгоритм, чтобы не ломать все что уже работает делается отдельно 
     FOR deal_rec
      IN (SELECT DISTINCT
                   deal.t_dealid AS DealID,
                   deal.t_dealcode AS DealCode,
                   deal.t_dealdate AS DealDate,
                   deal.t_dealtype AS DealType,
                   deal.t_partyid AS DealContragent,
                   sel.t_fiid  AS DealFI,
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
                   avo.t_isin ISIN,
                   leg1.t_id AccBound,
                   deal.t_BOfficeKind DealKind,
                   sel.korz
              FROM ddlrq_dbt pm
                   left join ddl_tick_dbt deal on deal.t_dealid = pm.t_docid
                   left join ddl_leg_dbt leg1 on deal.t_dealid = leg1.t_dealid
                   left join dfininstr_dbt curr on leg1.t_cfi = curr.t_fiid
                   left join doprkoper_dbt opr on deal.t_dealtype = opr.t_kind_operation
                   left join (select min(t_id) as Korz,t_dealid, t_fiid from ddl_tick_ens_dbt group by t_dealid, t_fiid) sel on  deal.t_dealid = sel.t_dealid
                   left join davoiriss_dbt avo on SEL.T_FIID = avo.t_fiid
                   left join dfininstr_dbt curr1 on deal.t_pfi = curr1.t_fiid
             WHERE pm.t_dockind = 101 AND pm.t_type IN (0, 2, 8)
                   AND (pm.t_plandate > DateRep1
                        OR (    pm.t_plandate <= DateRep1
                            AND pm.t_state IN (0, 2)
                            AND deal.t_dealstatus = 10))
                   AND deal.t_dealdate <= DateRep1
                   AND leg1.t_legkind = 0
                   AND opr.t_dockind = 101
                   and deal.t_clientid = -1
                   AND INSTR (opr.t_systypes, 't') > 0
                   and CURR1.T_AVOIRKIND = 48
          ORDER BY t_dealdate)
   LOOP
      repo_rec.DEAL_NUMBER := deal_rec.DealCode;
      repo_rec.DEAL_DT := deal_rec.DealDate;
      repo_rec.PART_1_EX_DT := deal_rec.DatePay1;
      repo_rec.ISIN := deal_rec.ISIN;

      BEGIN
         SELECT CASE
                   WHEN INSTR (LOWER (t_name), 'прям') > 0
                        AND INSTR (LOWER (t_name), 'репо') > 0
                   THEN
                      'прямое РЕПО'
                   WHEN INSTR (LOWER (t_name), 'обр') > 0
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

      IF (Rsb_Secur.
           IsOutExchange (
             Rsb_Secur.
              get_OperationGroup (
                Rsb_Secur.
                 get_OperSysTypes (deal_rec.DealType, deal_rec.DealKind))) >
             0)
      THEN
         repo_rec.REPO_EXCHANGE := 'внебиржевая';
         repo_rec.IS_LIQ_NETTING := CHR (1);
         repo_rec.IS_CLC_NETTING := CHR (1);
    
      ELSE
         repo_rec.REPO_EXCHANGE := 'биржевая';
         repo_rec.IS_LIQ_NETTING := 'ДА';
         repo_rec.IS_CLC_NETTING := 'ДА';
         repo_rec.IS_REPOSITORY := 'НЕТ';
      END IF;

     
      
      select case when instr((select t_sysTypes  
            from doprkoper_dbt
            where t_kind_operation in (select tick.t_dealtype from 
ddl_tick_dbt tick
where
tick.t_dealid in (select hist1.t_dealid
from V_SCWRTHISTEX hist1
where hist1.t_sumid in (SELECT hist.t_parent
  FROM V_SCWRTHISTEX hist 
  WHERE hist.t_instance = (SELECT MAX (hist2.t_instance)
                          FROM V_SCWRTHISTEX hist2
                         WHERE HIST2.T_SUMID = HIST.T_SUMID AND hist2.T_CHANGEDATE <= repo_rec.DEAL_DT)
        and hist.t_dealcode = deal_rec.DealCode
        and HIST.T_PARENT <> 0)
        and HIST1.T_DEALCODE <> deal_rec.DealCode
        and hist1.t_instance = (SELECT MAX (hist3.t_instance)
                          FROM V_SCWRTHISTEX hist3
                         WHERE HIST3.T_SUMID = HIST1.T_SUMID AND hist3.T_CHANGEDATE <= repo_rec.DEAL_DT)))), 't') <> 0
                         and repo_rec.repo_direction  = 'обратное РЕПО' 
                         then 'да'
                         else 'нет'
           end case into repo_rec.REPO_IN_REPO from dual;

      BEGIN
         SELECT subj.t_shortname AS ContragentName,
                (SELECT t_name
                   FROM dcountry_dbt
                  WHERE t_codelat3 = subj.t_nrcountry)
                   AS ContragentCountry,
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
                          WHERE o.t_objecttype = 3 AND o.t_groupid = 19
                                /* смотрим наличие только высоких рейтингов: */
                                AND ASCII (SUBSTR (UPPER (a.t_name), 1, 1)) IN
                                       (65                          /*англ A*/
                                          , 53392)                  /*русс А*/
                                AND o.t_object =
                                       LPAD (deal_rec.DealContragent,
                                             10,
                                             CHR (48))
                                AND o.t_validtodate =
                                       TO_DATE ('31.12.9999', 'dd.mm.yyyy')
                                AND a.t_objecttype = o.t_objecttype
                                AND a.t_groupid = o.t_groupid
                                AND a.t_attrid = o.t_attrid)
                THEN
                   'Выше "В"'
                ELSE
                   null
             END
        INTO repo_rec.SUBJ_RATING
        FROM DUAL;


      --проверяем были ли изменения, т.к. может быть вызов на архивную дату
      
      select nvl(min(t_changedate), to_date('01/01/0001', 'dd/mm/yyyy')) into changedate from ddl_tick_dbt  where  t_dealid = deal_rec.dealid and t_changedate >= DateRep1;
      if changedate > DateRep1
        then 
             begin
                select chng.t_oldprincipal2, chng.t_oldincomerate,
                (SELECT DECODE (t_factdate,
                                TO_DATE ('01010001', 'ddmmyyyy'), t_plandate,
                                t_factdate)
                   FROM ddlrq_dbt
                  WHERE     t_docid = deal_rec.DealID
                        AND t_type = 2
                        AND t_dealpart = 2
                        AND t_dockind = 101)
                   AS DatePay2
               INTO repo_rec.QUANTITY, repo_rec.RATE, repo_rec.PART_2_EX_DT
               from dsptkchng_dbt chng
               where chng.t_dealid = deal_rec.dealid 
               and chng.t_oldchangedate = (select max(t_oldchangedate) from dsptkchng_dbt where t_dealid = deal_rec.dealid and t_oldchangedate<=DateRep1);
             exception
                      WHEN NO_DATA_FOUND
               THEN
                repo_rec.QUANTITY := 0;
                repo_rec.RATE := 0;
                repo_rec.PART_2_EX_DT := TO_DATE ('01010001', 'ddmmyyyy');
             end;
        else
              BEGIN
                SELECT 
                leg2.t_principal AS L2Amount,
                leg2.t_incomerate AS L2PercentsREPO,
                (SELECT DECODE (t_factdate,
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
     end if; 
      --repo_rec.QUANTITY := (repo_rec.QUANTITY + deal_rec.L1Amount)/2;

      BEGIN
         SELECT SUBSTR (p.t_shortname, 1, 60),
                SUBSTR (fi.t_name, 1, 25),
                (SELECT t_name
                   FROM dcountry_dbt
                  WHERE t_codelat3 = p.t_nrcountry) /*nvl(a.t_country, chr(1))*/
                                                   ,
                v.t_incirculationdate,
                fi.t_fiid,
                fi.t_issuer
           INTO repo_rec.ISSUER,
                repo_rec.ISSUE_NAME,
                repo_rec.ISSUER_COUNTRY,
                repo_rec.DEPLOY_DT,
                issueid,                             -- правка З узнаем ид ц/б
                issuerid                    -- правка З узнаем ид эмитента ц/б
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
          WHERE t_codelat3 = repo_rec.ISSUER_COUNTRY
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
                AND o.t_object = LPAD (deal_rec.DealFI, 10, CHR (48))
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
                abs(GetCurrencyISO (nvl(raccount.t_currency,-1))) CurrBond,
                oaccount.t_account AccMoney,
                nvl(oaccount.t_currency, 0),
                abs(GetCurrencyISO (nvl(oaccount.t_currency, -1))) CurrMoney
           INTO repo_rec.ACCOUNT_MONEY,
                repo_rec.CURRENCY_BOND,
                repo_rec.ACCOUNT_BOND,
                fiid_acc_money,
                repo_rec.CURRENCY_MONEY
            FROM (SELECT t_account, t_currency,t_docid
                   FROM dmcaccdoc_dbt
                  WHERE t_id = (SELECT MAX (T_ID)
                                FROM 
                                     (SELECT MAX (acc.t_id) t_id /*09.10.2020 RAS 517096 добавил t_id */
                                      FROM dmcaccdoc_dbt acc, dmccateg_dbt cat
                                      WHERE     acc.t_catnum = cat.t_number
                                            and acc.t_dockind = 4620 
                                            and acc.t_docid = deal_rec.korz AND 
                                                cat.t_code IN ('Ц/б, Корзина БПП')
                                      union
                                      SELECT MAX (acc.t_id) t_id
                                      FROM dmcaccdoc_dbt acc, dmccateg_dbt cat, dmctempl_dbt templ
                                      WHERE     acc.t_catnum = cat.t_number
                                            AND acc.t_dockind = 4620
                                            AND acc.t_docid = deal_rec.korz
                                            AND acc.t_catid = templ.t_catid
                                            AND templ.t_number = acc.t_templnum
                                            AND templ.t_value4 = 1
                                            AND cat.t_code IN ('Наш портфель ц/б'))
                                   )) oaccount,
                   (SELECT t_account, t_currency, t_docid
                   FROM dmcaccdoc_dbt
                  WHERE t_id =
                           (SELECT acc.t_id
                              FROM dmcaccdoc_dbt acc, dmccateg_dbt cat
                             WHERE     acc.t_catnum = cat.t_number
                                   AND acc.t_dockind = 176
                                   AND acc.t_docid =deal_rec.AccBound
                                   AND( (cat.t_code IN ('+ОД') and rsb_secur.isbuy (rsb_secur.get_operationgroup(rsb_secur.get_opersystypes((select t_dealtype from ddl_tick_dbt where t_dealid = ( select t_dealid from ddl_leg_dbt where t_id = deal_rec.AccBound)) , 101))) = 0 ) OR
                                   (cat.t_code IN ('-ОД') AND rsb_secur.issale(rsb_secur.get_operationgroup(rsb_secur.get_opersystypes((select t_dealtype from ddl_tick_dbt where t_dealid = ( select t_dealid from ddl_leg_dbt where t_id = deal_rec.AccBound)) , 101))) <> 0 ) ))) raccount;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            repo_rec.ACCOUNT_BOND := CHR (1);
            repo_rec.CURRENCY_BOND := CHR (1);
            repo_rec.ACCOUNT_MONEY := CHR (1);
            repo_rec.CURRENCY_MONEY := CHR (1);
            fiid_acc_money := -1;
      END;
      
      BEGIN
         SELECT nvl(t_account, chr(1)), nvl(t_currency, -1)
           INTO repo_rec.ACCOUNT_RERC, fiid_acc_percnt
           FROM dmcaccdoc_dbt
          WHERE t_id =
                   (SELECT MAX (acc.t_id)
                      FROM dmcaccdoc_dbt acc, dmccateg_dbt cat
                     WHERE     acc.t_catnum = cat.t_number
                           AND acc.t_dockind = 176
                           AND acc.t_docid in (select t_id from ddl_leg_dbt where t_dealid = (select t_dealid from ddl_leg_dbt where t_id = deal_rec.AccBound))
                           AND cat.t_code IN ('+% к погашению', '-% к погашению'));
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            repo_rec.ACCOUNT_RERC := CHR (1);
            fiid_acc_percnt := -1;
      END;
     
      repo_rec.REPORT_DT := DateRep1;
      
     repo_rec.AMOUNT_PERC :=abs(Getbalancecost(repo_rec.ACCOUNT_RERC, repo_rec.REPORT_DT,repo_rec.REPORT_DT ));
       
      repo_rec.ISSUER_RATING_SP :=
         GetRate (IssuerId,
                  ObIdIssuer,
                  GrIdIssuer,
                  SaPRat,
                  DateRep1);                                        --Правка З
      repo_rec.ISSUE_RATING_SP :=
         GetRate (IssueId,
                  ObIdIssue,
                  GrIdIssue,
                  SaPRat,
                  DateRep1);                                        --Правка З
                  repo_rec.ISSUER_RATING_3453U_SP :=GetRate (IssuerId,
                  ObIdIssuer,
                  GrIdIssuer,
                  459,
                  DateRep1); 
                  repo_rec.ISSUE_RATING_3453U_SP :=GetRate (IssueId,
                  ObIdIssue,
                  GrIdIssue,
                  459,
                  DateRep1);
      repo_rec.ISSUER_RATING_MOODYS :=
         GetRate (IssuerId,
                  ObIdIssuer,
                  GrIdIssuer,
                  MoodyRat,
                  DateRep1);                                       -- Правка З
      repo_rec.ISSUE_RATING_MOODYS :=
         GetRate (IssueId,
                  ObIdIssue,
                  GrIdIssue,
                  MoodyRat,
                  DateRep1);                                       -- Правка З
                  repo_rec.ISSUER_RATING_3453U_MOODYS :=GetRate (IssuerId,
                  ObIdIssuer,
                  GrIdIssuer,
                  483,
                  DateRep1); 
                  repo_rec.ISSUE_RATING_3453U_MOODYS :=GetRate (IssueId,
                  ObIdIssue,
                  GrIdIssue,
                  483,
                  DateRep1);  
      repo_rec.ISSUER_RATING_FITCH :=
         GetRate (IssuerId,
                  ObIdIssuer,
                  GrIdIssuer,
                  FitchRat,
                  DateRep1);                                       -- Правка З
      repo_rec.ISSUE_RATING_FITCH :=
         GetRate (IssueId,
                  ObIdIssue,
                  GrIdIssue,
                  FitchRat,
                  DateRep1);                                       -- Правка З

      repo_rec.ISSUER_RATING_3453U_FITCH :=GetRate (IssuerId,
                  ObIdIssuer,
                  GrIdIssuer,
                  505,
                  DateRep1);
                 repo_rec.ISSUE_RATING_3453U_FITCH :=GetRate (IssueId,
                  ObIdIssue,
                  GrIdIssue,
                  505,
                  DateRep1);
       repo_rec.IS_SPV := getSPV(IssuerId);
       repo_rec.IS_HYPO_COVER :=GetHypoCover(deal_rec.DealFI);

             case when repo_rec.repo_direction  = 'обратное РЕПО'
             then  repo_rec.OVERPRICE := 0;
             else  repo_rec.OVERPRICE := GetOveramount(deal_rec.DealCode, repo_rec.REPORT_DT, repo_rec.REPO_DIRECTION);
             end case;
     
              SELECT  nvl(sum(t_nkdamount),0),sum(t_interestincome),sum(t_discountincome),sum(t_begbonus),sum(t_bonus) 
               into nkd, interest, discount, begbonus, bonus
                  FROM V_SCWRTHISTEX hist 
                    WHERE  HIST.T_PARENT in (select distinct(hist1.t_sumid) from V_SCWRTHISTEX hist1 where hist1.T_DEALCODE = deal_rec.DealCode and hist1.t_portfolio <> 45)
                        AND hist.t_instance = (SELECT MAX (hist2.t_instance)
                          FROM V_SCWRTHISTEX hist2
                         WHERE HIST2.T_SUMID = HIST.T_SUMID AND hist2.T_CHANGEDATE <= repo_rec.REPORT_DT)
                         AND HIST.T_STATE = 3;
             repo_rec.BALANCE_PRICE :=  abs(Getbalancecost(repo_rec.ACCOUNT_BOND, repo_rec.REPORT_DT, repo_rec.REPORT_DT)) + nkd + interest + discount + begbonus -bonus;
         repo_rec.ISSUER_RATING_AKRA := GetRate (IssuerId,
                  ObIdIssuer,
                  GrIdIssuer,
                  436,
                  DateRep1);     
        repo_rec.ISSUE_RATING_AKRA   := GetRate (IssueId,
                  ObIdIssue,
                  GrIdIssue,
                  436,
                  DateRep1); 
                  repo_rec.ISSUER_RATING_EXPERT := GetRate (IssuerId,
                  ObIdIssuer,
                  GrIdIssuer,
                  367,
                  DateRep1);     
        repo_rec.ISSUE_RATING_EXPERT   := GetRate (IssueId,
                  ObIdIssue,
                  GrIdIssue,
                  367,
                  DateRep1);


                  repo_rec.AMOUNT_MONEY :=abs(Getbalancecost(repo_rec.ACCOUNT_MONEY, repo_rec.REPORT_DT, repo_rec.REPORT_DT));
                  select t_FaceValueFI into fiid_Paper  from dfininstr_dbt where t_fiid =deal_rec.DealFI;
                  repo_rec.TSS_AMOUNT := repo_rec.BALANCE_PRICE + repo_rec.OVERPRICE;--nvl(RSB_FIInstr.ConvSumtype(1, deal_rec.DealFI,fiid_Paper,12, repo_rec.REPORT_DT), 0);
         case when( GetRate (IssueId, 12, 103, 0, DateRep1) = 'Да' or GetRate (IssueId, 12, 104, 0, DateRep1) = 'Да')
         then repo_rec.IS_USE_FOR_INDEX := 'Да';
         else repo_rec.IS_USE_FOR_INDEX := 'Нет';
         end case;
         select t_subordinated into issubord  from davoiriss_dbt where t_fiid = IssueId;
         case when issubord = 'X'
         then repo_rec.IS_SUBORD_BOND := 'Да';
         else repo_rec.IS_SUBORD_BOND := 'Нет';
         end case;
        
        begin
                select i.t_indicator into repo_rec.devaluation from f657_ind_dep i 
                where i.t_isin = repo_rec.ISIN 
                and i.t_date_load = (select max(di.t_date_load) from f657_ind_dep di 
                where di.t_isin = repo_rec.ISIN 
                and di.t_date_load <= repo_rec.report_dt);
      exception
                WHEN NO_DATA_FOUND
                THEN
                repo_rec.devaluation := 0;
      END;

        
        
         INSERT INTO tRSHB_REPO_PKL_2CHD  VALUES repo_rec;
         COMMIT;
      END LOOP; 
      
    /*размазывание переоценки в Прямом Репо пропорционально количеству/ . Искренне надеюсь, что заказчик передумает и уберем это*/   
     
IF (spread = 1) THEN           
        UPDATE trshb_repo_pkl_2chd m
           SET overprice =
                     (SELECT round(t1.avgovervalue * t2.avramount ,2) spreadoveramount
                        FROM (  SELECT SUM (quantity) avramount,
                                       SUM (overprice) overvalue,
                                       SUM (overprice) / SUM (quantity) avgovervalue, isin
                                  FROM trshb_repo_pkl_2chd t
                                 WHERE repo_direction = 'прямое РЕПО' AND report_dt = DateRep1
                              GROUP BY isin) t1, (SELECT SUM (quantity) avramount,
                                       SUM (overprice) overvalue,
                                       SUM (overprice) / SUM (quantity) avgovervalue,
                                       deal_number ,
                                      isin
                                  FROM trshb_repo_pkl_2chd t
                                 WHERE repo_direction = 'прямое РЕПО' AND report_dt = DateRep1
                              GROUP BY isin, deal_number ) t2 where t1.isin = t2.isin and t2.deal_number = m.deal_number );
           END IF; 
         COMMIT;        
           
      
      
   END;
END SECUR_DWHINFO;
/
