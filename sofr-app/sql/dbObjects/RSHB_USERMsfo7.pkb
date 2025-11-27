CREATE OR REPLACE PACKAGE BODY RSHB_USERMsfo7
AS
   -- Заполоняет основную таблицу данных отчета

   PROCEDURE InsIntoTmpTable (BegDate IN DATE, EndDate IN DATE)
   IS
        BegMoveDate DATE;
        BegDatePereoc DATE;
   BEGIN
      /*Наш портфель*/

      EXECUTE IMMEDIATE 'TRUNCATE TABLE DMsfo7_DBT';
--      DELETE FROM DMsfo7_DBT;

        IF (BegDate = to_date('01.01.2019','dd.mm.yyyy')) then /*отсекаем переводы по мсфо в 01.01.2019*/
            BegMoveDate:=BegDate +1;
        ELSE
            BegMoveDate:=BegDate;
        END IF;   
        
        
      INSERT INTO DMsfo7_DBT (FIID,
                              AvrName,
                              AvrType,
                              ISIN,
                              begportfcode,
                              portfaccount,
                              code_currency,
                              begportfcost 
                              )
         SELECT t_fiid,
                t_definition,
                t_name,
                t_isin,
                t_value1,
                accountPortf,
                t_currency,
                NVL (sum(restbeg), 0)
           FROM (SELECT accdoc.t_fiid,
                        (case when fin.t_definition != chr(1) Then fin.t_definition else fin.t_name end) t_definition,
                        kind.t_name,
                        avr.t_isin,
                        templ.t_value1,
                        chr(0)  accountPortf,
                        accdoc.t_currency,
                        abs(ROUND (RSI_Rsb_FIInstr.ConvSum (RSI_RSB_ACCOUNT.
                                                         RestAC (
                                                           accdoc.t_account,
                                                           accdoc.t_currency,
                                                           BegDate,
                                                           1,
                                                           NULL),
                                                        accdoc.t_currency,
                                                        0,
                                                        BegDate), 2))
                           RestBeg,
                        RSI_RSB_ACCOUNT.kreditac (accdoc.t_account,
                                                  1,
                                                  accdoc.t_currency,
                                                  BegMoveDate/*BegDate*/,
                                                  EndDate)
                           Kred,
                        RSI_RSB_ACCOUNT.debetac (accdoc.t_account,
                                                 1,
                                                 accdoc.t_currency,
                                                 BegMoveDate/*BegDate*/,
                                                 EndDate)
                           Deb
                   FROM dmcaccdoc_dbt accdoc,
                        dmctempl_dbt templ,
                        dfininstr_dbt fin,
                        davoiriss_dbt avr,
                        davrkinds_dbt kind
                  WHERE     accdoc.t_catid  in (select t_id from DMCCATEG_DBT t where t_number IN ( 231)) -- t_catnum = 231
                        AND accdoc.t_iscommon = 'X'
                        AND templ.t_catid = accdoc.t_catid
                        AND templ.t_number = accdoc.t_templnum
                        AND SUBSTR (accdoc.t_account, 1, 3) != '503'
                        AND fin.t_fiid = accdoc.t_fiid
                        AND avr.t_fiid = accdoc.t_fiid
                        AND kind.t_avoirkind = fin.t_avoirkind
                        AND kind.t_fi_kind = 2)
          WHERE Restbeg <> 0 OR DEB <> 0 OR Kred <> 0
group by t_fiid,
                t_definition,
                t_name,
                t_isin,
                t_value1,
                accountPortf,
                t_currency;
      /* ПКУ  */

      INSERT INTO DMsfo7_DBT (FIID,
                              AvrName,
                              AvrType,
                              ISIN,
                              begportfcode,
                              portfaccount,
                              code_currency,
                              begportfcost)
         SELECT t_fiid,
                t_definition,
                t_name,
                t_isin,
                t_value1,
                t_account,
                t_currency,
                restbeg
           FROM (SELECT accdoc.t_fiid,
                        (case when fin.t_definition != chr(1) Then fin.t_definition else fin.t_name end) t_definition,
                        kind.t_name,
                        avr.t_isin,
                        7 t_value1,
                        accdoc.t_account,
                        accdoc.t_currency,
                        abs(ROUND (RSI_Rsb_FIInstr.ConvSum (RSI_RSB_ACCOUNT.
                                                         RestAC (
                                                           accdoc.t_account,
                                                           accdoc.t_currency,
                                                           BegDate,
                                                           1,
                                                           NULL),
                                                        accdoc.t_currency,
                                                        0,
                                                        BegDate), 2))
                           RestBeg,
                        RSI_RSB_ACCOUNT.kreditac (accdoc.t_account,
                                                  1,
                                                  accdoc.t_currency,
                                                  BegDate,
                                                  EndDate)
                           Kred,
                        RSI_RSB_ACCOUNT.debetac (accdoc.t_account,
                                                 1,
                                                 accdoc.t_currency,
                                                 BegDate,
                                                 EndDate)
                           Deb
                   FROM dmcaccdoc_dbt accdoc,
                        dfininstr_dbt fin,
                        davoiriss_dbt avr,
                        davrkinds_dbt kind
                  WHERE     accdoc.t_catid  in (select t_id from DMCCATEG_DBT t where t_number IN ( 1253)) --t_catnum = 1253
                        AND accdoc.t_iscommon = 'X'
                        AND fin.t_fiid = accdoc.t_fiid
                        AND avr.t_fiid = accdoc.t_fiid
                        AND kind.t_avoirkind = fin.t_avoirkind
                        AND kind.t_fi_kind = 2)
          WHERE Restbeg <> 0 OR DEB <> 0 OR Kred <> 0;



      /* К остатку портфельных счетов добавляем остатки по счетам в репо ( БПП) */


      MERGE INTO DMsfo7_DBT fo7
           USING ( SELECT t_pfi, t_value1,  
                           NVL (SUM (abs(ROUND (RSI_Rsb_FIInstr.
                                             ConvSum (RSI_RSB_ACCOUNT.
                                                       RestAC (
                                                         t_account,
                                                         t_currency,
                                                         BegDate,
                                                         1,
                                                         NULL),
                                                      t_currency,
                                                      0,
                                                      BegDate), 2))),
                                0)
                              RestBeg
  from (
SELECT leg.t_pfi,
                           templ.t_value1,
                           accdoc.t_account, accdoc.t_currency
                      FROM dmcaccdoc_dbt accdoc,
                           ddl_leg_dbt leg,
                           dmctempl_dbt templ
                     WHERE     accdoc.t_catid  in (select t_id from DMCCATEG_DBT t where t_number IN ( 233, 1237, 1298, 1299)) -- t_catnum IN (233, 1237, 1298, 1299)
                           AND accdoc.t_dockind = 176
                           AND leg.t_id = accdoc.t_docid
                           AND templ.t_catid = accdoc.t_catid
                           AND templ.t_number = accdoc.t_templnum
                  GROUP BY leg.t_pfi, templ.t_value1, accdoc.t_account, accdoc.t_currency)
                  group by t_pfi, t_value1) r
              ON (r.t_pfi = fo7.fiid AND r.t_value1 = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.begportfcost = fo7.begportfcost + r.restbeg;

/*

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT leg.t_pfi,
                           templ.t_value1,
                           NVL (SUM (abs(ROUND (RSI_Rsb_FIInstr.
                                             ConvSum (RSI_RSB_ACCOUNT.
                                                       RestAC (
                                                         accdoc.t_account,
                                                         accdoc.t_currency,
                                                         BegDate,
                                                         1,
                                                         NULL),
                                                      accdoc.t_currency,
                                                      0,
                                                      BegDate), 2))),
                                0)
                              RestBeg
                      FROM dmcaccdoc_dbt accdoc,
                           ddl_leg_dbt leg,
                           dmctempl_dbt templ
                     WHERE     accdoc.t_catnum IN (233, 1237, 1298, 1299)
                           AND accdoc.t_dockind = 176
                           AND leg.t_id = accdoc.t_docid
                           AND templ.t_catid = accdoc.t_catid
                           AND templ.t_number = accdoc.t_templnum
                  GROUP BY leg.t_pfi, templ.t_value1) r
              ON (r.t_pfi = fo7.fiid AND r.t_value1 = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.begportfcost = fo7.begportfcost + r.restbeg;
*/
/* К остатку портфельных счетов добавляем остатки по счетам в Корзине репо ( БПП) */

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT Ens.t_fiid,
                           templ.t_value1,
                           NVL (SUM (abs(ROUND (RSI_Rsb_FIInstr.
                                             ConvSum (RSI_RSB_ACCOUNT.
                                                       RestAC (
                                                         accdoc.t_account,
                                                         accdoc.t_currency,
                                                         BegDate,
                                                         1,
                                                         NULL),
                                                      accdoc.t_currency,
                                                      0,
                                                      BegDate), 2))),
                                0)
                              RestBeg
                      FROM dmcaccdoc_dbt accdoc,
                           ddl_tick_ens_dbt ens,
                           dmctempl_dbt templ
                     WHERE     accdoc.t_catid  in (select t_id from DMCCATEG_DBT t where t_number IN ( 1237)) --t_catnum IN ( 1237)
                           AND accdoc.t_dockind = 4620
                           AND ens.t_id = accdoc.t_docid
                           AND templ.t_catid = accdoc.t_catid
                           AND templ.t_number = accdoc.t_templnum
                  GROUP BY ens.t_fiid, templ.t_value1) r
              ON (r.t_fiid = fo7.fiid AND r.t_value1 = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.begportfcost = fo7.begportfcost + r.restbeg;


      /*  ПОЛЕ BUYCOST по Наш портфель */

      MERGE INTO DMsfo7_DBT fo7
           USING (     with accd as
                         (select /*+ index(ACCDOC DMCACCDOC_DBT_IDXC) materialize */
                          distinct acc.t_accountid,acc.t_account,accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                           where categ.t_code in ('Наш портфель ц/б')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account)
                     SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) BuyCostRur
                      FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX2 )*/ 
                                   distinct trn.t_acctrnid, accd.t_account,
                                   /*(SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = AccDeb.t_account)*/
                                     accd.t_fiid AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = accd.t_account -- accdeb.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM accd,
                                   dacctrn_dbt trn, 
                                   --daccount_dbt AccDeb,
                                   daccount_dbt AccCred
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   and trn.t_accountid_payer = accd.t_accountid
                                  /* AND AccDeb.t_accountid =
                                          trn.t_accountid_payer */
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                  /* AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Наш портфель ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id) */
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('+Форвард, расчеты',
                                                      '+Расчеты',
                                                      '-Расчеты',
                                                      'Треб сн.с',
                                                      'Пред. Затраты ц/б',
                                                      'Брокер')
                                                  AND accdoc.t_catid =
                                                         categ.t_id))
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.buycost = fo7.buycost + r.BuyCostRur;


      /*  ПОЛЕ BUYCOST по Наш портфель ПКУ, ц/б */

      MERGE INTO DMsfo7_DBT fo7
           USING (      with accd as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                          distinct acc.t_accountid,acc.t_account,accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                           where categ.t_code in
                                     ('Наш портфель ПКУ, ц/б')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account)
                     SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) BuyCostRur
                      FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX2 )*/ 
                                   distinct trn.t_acctrnid, accd.t_account,
                                   /*(SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = accdeb.t_account)*/
                                   accd.t_fiid   AS fiid,
                                   7 AS portfnum,
                                   trn.t_sum_natcur
                              FROM accd,
                                   dacctrn_dbt trn, 
                                   --daccount_dbt AccDeb,
                                   daccount_dbt AccCred

                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   and trn.t_accountid_payer = accd.t_accountid
                                   /*AND AccDeb.t_accountid =
                                          trn.t_accountid_payer */
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   /*AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Наш портфель ПКУ, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id) */
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('+Форвард, расчеты',
                                                      '+Расчеты',
                                                      '-Расчеты',
                                                      'Треб сн.с',
                                                      'Пред. Затраты ц/б',
                                                      'Брокер')
                                                  AND accdoc.t_catid =
                                                         categ.t_id))
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.buycost = fo7.buycost + r.BuyCostRur;


      /*   ПОЛЕ   BUYYNKD  по КУ Уплаченный НКД  */


      MERGE INTO DMsfo7_DBT fo7
           USING (       with accd as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                          distinct acc.t_accountid,acc.t_account,accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                           where categ.t_code in
                                                    ('Уплаченный НКД')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account),
                        acck as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                            distinct acc.t_accountid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                            where categ.t_code in
                                                     ('+Форвард, расчеты',
                                                      'Треб сн.с')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account )
                     SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) BuyYnkdRur
                      FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX3 )*/ 
                                   distinct trn.t_acctrnid, accd.t_account,
                                   /*(SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = accdeb.t_account)*/
                                   accd.t_fiid   AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = accd.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM acck,
                                   dacctrn_dbt trn, 
                                   --daccount_dbt AccDeb,
                                   --daccount_dbt AccCred
                                   accd
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   and trn.t_accountid_payer = accd.t_accountid
                                   and trn.t_accountid_receiver = acck.t_accountid 
                                   /*AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Уплаченный НКД')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('+Форвард, расчеты',
                                                      'Треб сн.с')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)*/)
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.buyYnkd = fo7.buyYnkd + r.BuyYnkdRur;


      /*   ПОЛЕ   BUYBONUS по КУ Премия, ц/б  */


      MERGE INTO DMsfo7_DBT fo7
           USING (  with accd as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                          distinct acc.t_accountid,acc.t_account,accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                           where categ.t_code in
                                                     ('Премия, ц/б')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account),
                        acck as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                            distinct acc.t_accountid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                            where categ.t_code in
                                                     ('+Форвард, расчеты',
                                                      'Треб сн.с')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account )
                       SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) BuyBonusRur
                      FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX2 )*/ 
                                   distinct trn.t_acctrnid, accd.t_account,
                                   /*(SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = accdeb.t_account)*/
                                   accd.t_fiid   AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = accd.t_account --accdeb.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM accd,
                                   dacctrn_dbt trn 
                                   --daccount_dbt AccDeb,
                                   --daccount_dbt AccCred
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   and trn.t_accountid_payer = accd.t_accountid
                                   and trn.t_accountid_receiver in (select t_accountid from acck) 
                                   /*AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Премия, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('+Форвард, расчеты',
                                                      'Треб сн.с')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)*/)
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.buyBonus = fo7.buyBonus + r.BuyBonusRur;


      /* Остатки по КУ Уплаченный НКД  на начало периода */

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT                              /*accdoc.t_account, */
                          accdoc.t_fiid,
                           templ.t_value1,
                           NVL (SUM (abs(ROUND (RSI_Rsb_FIInstr.
                                             ConvSum (RSI_RSB_ACCOUNT.
                                                       RestAC (
                                                         accdoc.t_account,
                                                         accdoc.t_currency,
                                                         BegDate,
                                                         1,
                                                         NULL),
                                                      accdoc.t_currency,
                                                      0,
                                                      BegDate), 2))),
                                0)
                              RestYnkd
                      FROM dmcaccdoc_dbt accdoc,
                           dmctempl_dbt templ,
                           dmccateg_dbt categ
                     WHERE     accdoc.t_catid = categ.t_id
                           AND categ.t_code IN ('Уплаченный НКД')
                           AND accdoc.t_iscommon = 'X'
                           AND templ.t_catid = accdoc.t_catid
                           AND templ.t_number = accdoc.t_templnum
                  GROUP BY                              /*accdoc.t_account, */
                          accdoc.t_fiid,
                           templ.t_value1) r
              ON (r.t_fiid = fo7.fiid AND r.t_value1 = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.begYnkd = fo7.begYnkd + r.restYnkd; /*, fo7.YnkdAccount = r.t_account*/


      /* Прибавляем остатки по КУ Уплаченный НКД, БПП на начало периода */

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT leg.t_pfi,
                           templ.t_value1,
                           NVL (SUM (abs(ROUND (RSI_Rsb_FIInstr.
                                             ConvSum (RSI_RSB_ACCOUNT.
                                                       RestAC (
                                                         accdoc.t_account,
                                                         accdoc.t_currency,
                                                         BegDate,
                                                         1,
                                                         NULL),
                                                      accdoc.t_currency,
                                                      0,
                                                      BegDate), 2))),
                                0)
                              RestYnkdBpp
                      FROM dmcaccdoc_dbt accdoc,
                           ddl_leg_dbt leg,
                           dmctempl_dbt templ,
                           dmccateg_dbt categ
                     WHERE accdoc.t_catid = categ.t_id
                           AND categ.t_code IN
                                  ('Уплаченный НКД, БПП')
                           AND accdoc.t_dockind = 176
                           AND leg.t_id = accdoc.t_docid
                           AND templ.t_catid = accdoc.t_catid
                           AND templ.t_number = accdoc.t_templnum
                  GROUP BY leg.t_pfi, templ.t_value1) r
              ON (r.t_pfi = fo7.fiid AND r.t_value1 = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.begYnkd = fo7.begYnkd + r.RestYnkdBpp;

/* Прибавляем остатки по КУ Уплач. НКД, Корзина БПП на начало периода */

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT Ens.t_fiid,
                           templ.t_value1,
                           NVL (SUM (abs(ROUND (RSI_Rsb_FIInstr.
                                             ConvSum (RSI_RSB_ACCOUNT.
                                                       RestAC (
                                                         accdoc.t_account,
                                                         accdoc.t_currency,
                                                         BegDate,
                                                         1,
                                                         NULL),
                                                      accdoc.t_currency,
                                                      0,
                                                      BegDate), 2))),
                                0)
                              begYnkd
                      FROM dmcaccdoc_dbt accdoc,
                           ddl_tick_ens_dbt ens,
                           dmctempl_dbt templ
                     WHERE     accdoc.t_catid  in (select t_id from DMCCATEG_DBT t where t_number IN ( 1238)) --t_catnum IN ( 1238)
                           AND accdoc.t_dockind = 4620
                           AND ens.t_id = accdoc.t_docid
                           AND templ.t_catid = accdoc.t_catid
                           AND templ.t_number = accdoc.t_templnum
                           AND not exists(select 1 from dmcaccdoc_dbt where t_catid  in (select t_id from DMCCATEG_DBT t where t_number IN ( 1237)) --t_catnum = 1237 
                                                                            and t_account = accdoc.t_account)
                  GROUP BY ens.t_fiid, templ.t_value1) r
              ON (r.t_fiid = fo7.fiid AND r.t_value1 = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.begYnkd = fo7.begYnkd + r.begYnkd;




      /* Остатки по КУ Премия, ц/б  на начало периода */

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT accdoc.t_fiid,
                           templ.t_value1,
                           NVL (SUM (abs(ROUND (RSI_Rsb_FIInstr.
                                             ConvSum (RSI_RSB_ACCOUNT.
                                                       RestAC (
                                                         accdoc.t_account,
                                                         accdoc.t_currency,
                                                         BegDate,
                                                         1,
                                                         NULL),
                                                      accdoc.t_currency,
                                                      0,
                                                      BegDate), 2))),
                                0)
                              RestBonus
                      FROM dmcaccdoc_dbt accdoc,
                           dmctempl_dbt templ,
                           dmccateg_dbt categ
                     WHERE     accdoc.t_catid = categ.t_id
                           AND categ.t_code IN ('Премия, ц/б')
                           AND accdoc.t_iscommon = 'X'
                           AND templ.t_catid = accdoc.t_catid
                           AND templ.t_number = accdoc.t_templnum
                  GROUP BY accdoc.t_fiid, templ.t_value1) r
              ON (r.t_fiid = fo7.fiid AND r.t_value1 = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.begBonuscost = fo7.begBonuscost + r.restBonus;


      /* Остатки по КУ Начисл.ПДД, ц/б по начисленному купону  на начало периода */

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT accdoc.t_fiid,
                           templ.t_value1,
                           NVL (SUM (abs(ROUND (RSI_Rsb_FIInstr.
                                             ConvSum (RSI_RSB_ACCOUNT.
                                                       RestAC (
                                                         accdoc.t_account,
                                                         accdoc.t_currency,
                                                         BegDate,
                                                         1,
                                                         NULL),
                                                      accdoc.t_currency,
                                                      0,
                                                      BegDate), 2))),
                                0)
                              RestNKD
                      FROM dmcaccdoc_dbt accdoc,
                           dmctempl_dbt templ,
                           dmccateg_dbt categ
                     WHERE     accdoc.t_catid = categ.t_id
                           AND categ.t_code IN ('Начисл.ПДД, ц/б')
                           AND accdoc.t_iscommon = 'X'
                           AND templ.t_catid = accdoc.t_catid
                           AND templ.t_number = accdoc.t_templnum
                           AND templ.t_value4 = 1
                  GROUP BY accdoc.t_fiid, templ.t_value1) r
              ON (r.t_fiid = fo7.fiid AND r.t_value1 = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.begNKD = fo7.begNKD + r.restNKD;


      /* Остатки по КУ Начисл.ПДД, ц/б по начисленному дисконту  на начало периода */

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT accdoc.t_fiid,
                           templ.t_value1,
                           NVL (SUM (abs(ROUND (RSI_Rsb_FIInstr.
                                             ConvSum (RSI_RSB_ACCOUNT.
                                                       RestAC (
                                                         accdoc.t_account,
                                                         accdoc.t_currency,
                                                         BegDate,
                                                         1,
                                                         NULL),
                                                      accdoc.t_currency,
                                                      0,
                                                      BegDate), 2))),
                                0)
                              RestDiscount
                      FROM dmcaccdoc_dbt accdoc,
                           dmctempl_dbt templ,
                           dmccateg_dbt categ
                     WHERE     accdoc.t_catid = categ.t_id
                           AND categ.t_code IN ('Начисл.ПДД, ц/б')
                           AND accdoc.t_iscommon = 'X'
                           AND templ.t_catid = accdoc.t_catid
                           AND templ.t_number = accdoc.t_templnum
                           AND templ.t_value4 = 2
                  GROUP BY accdoc.t_fiid, templ.t_value1) r
              ON (r.t_fiid = fo7.fiid AND r.t_value1 = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.begDiscount = fo7.begDiscount + r.restDiscount;

      /*  Валютная переоценка в части номинала за предыдущий год  PrevYearCost  +  */

      MERGE INTO DMsfo7_DBT fo7
           USING ( with accd as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                          distinct acc.t_accountid,acc.t_account,accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                           where categ.t_code in
                                                     ('Наш портфель ц/б')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account),
                        acck as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                            distinct acc.t_accountid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                            where categ.t_code in
                                                     ('+Маржа, ИВ и ДМ', '+ПереоценкаА')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account )
                       SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) PrevCostRur
                        FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX2 )*/ 
                                   distinct trn.t_acctrnid, accd.t_account,
                                   /*(SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = accdeb.t_account)*/
                                   accd.t_fiid   AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = accd.t_account --accdeb.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                            FROM accd,
                                   dacctrn_dbt trn 
                                   --daccount_dbt AccDeb,
                                   --daccount_dbt AccCred
                             WHERE     trn.t_date_carry >= add_months(BegDate, -12)
                                   AND trn.t_date_carry <= BegDate
                                   AND trn.t_state = 1
                                   and trn.t_accountid_payer = accd.t_accountid
                                   and trn.t_accountid_receiver in (select t_accountid from acck) 
                                   /*AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Наш портфель ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('+Маржа, ИВ и ДМ', '+ПереоценкаА')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)*/)
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.PrevYearCost = fo7.PrevYearCost + r.PrevCostRur;

      /*   Валютная переоценка в части номинала за предыдущий год PrevYearCost  -  */

      MERGE INTO DMsfo7_DBT fo7
           USING (    with accd as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                          distinct acc.t_accountid,acc.t_account,accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                           where categ.t_code in
                                                     ('-Маржа, ИВ и ДМ', '-ПереоценкаА')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account),
                        acck as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                            distinct acc.t_accountid,acc.t_account, accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                            where categ.t_code in
                                                     ('Наш портфель ц/б')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account )
                       SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) PrevCostRur
                        FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX3 )*/ 
                                   distinct trn.t_acctrnid, acck.t_account ,
                                   /*(SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = acccred.t_account)*/
                                   acck.t_fiid   AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = acck.t_account --acccred.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                            FROM acck,
                                  dacctrn_dbt trn, 
                                   --daccount_dbt AccDeb,
                                   --daccount_dbt AccCred
                                  accd
                             WHERE     trn.t_date_carry >= add_months(BegDate, -12)
                                   AND trn.t_date_carry <= BegDate
                                   AND trn.t_state = 1
                                   and trn.t_accountid_payer = accd.t_accountid
                                   and trn.t_accountid_receiver = acck.t_accountid 
                                  /*AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Наш портфель ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('-Маржа, ИВ и ДМ', '-ПереоценкаА')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)*/)
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.PrevYearCost = fo7.PrevYearCost - r.PrevCostRur;


/*     Валютная переоценка за предыдущий год в части номинала по БПП PrevYearCost  + БПП  */

      MERGE INTO DMsfo7_DBT fo7
           USING (   with accd as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                          distinct acc.t_accountid,acc.t_account,accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                           where categ.t_code in
                                                     ('Ц/б, БПП')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account),
                        acck as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                            distinct acc.t_accountid, accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                            where categ.t_code in
                                                     ('+Маржа, ИВ и ДМ', '+ПереоценкаА')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account )
                      SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) PrevCostBPPRur
                      FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX2 )*/ 
                                   distinct trn.t_acctrnid, accd.t_account,
                                   (SELECT DISTINCT leg.t_pfi
                                      FROM dmcaccdoc_dbt c, ddl_leg_dbt leg
                                     WHERE c.t_account = accd.t_account --accdeb.t_account 
                                     and c.t_dockind = 176 and leg.t_id = c.t_docid and leg.t_legkind = 0) as fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = accd.t_account -- accdeb.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                             FROM accd,
                                   dacctrn_dbt trn 
                                   --daccount_dbt AccDeb,
                                   --daccount_dbt AccCred
                             WHERE     trn.t_date_carry >= add_months(BegDate, -12)
                                   AND trn.t_date_carry <= BegDate
                                   AND trn.t_state = 1
                                   and trn.t_accountid_payer = accd.t_accountid
                                   and trn.t_accountid_receiver in (select t_accountid from acck) 
                                  /*AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Ц/б, БПП')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('+Маржа, ИВ и ДМ', '+ПереоценкаА')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)*/)
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.PrevYearCost = fo7.PrevYearCost + r.PrevCostBPPRur;

/*   End  PrevYearCost  + БПП  */

/*     Валютная переоценка за предыдущий год в части номинала по Корзине БПП PrevYearCost  + Корзина БПП  */

      MERGE INTO DMsfo7_DBT fo7
           USING ( with accd as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                          distinct acc.t_accountid,acc.t_account,accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                           where categ.t_code in
                                                     ('Ц/б, Корзина БПП')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account),
                        acck as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                            distinct acc.t_accountid,acc.t_account, accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                            where categ.t_code in
                                                    ('+Маржа, ИВ и ДМ', '+ПереоценкаА')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account )
                      SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) PrevCostBPPRur
                        FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX2 )*/ 
                                   distinct trn.t_acctrnid ,
                                   (SELECT DISTINCT Ens.t_fiid
                                      FROM dmcaccdoc_dbt c, ddl_tick_ens_dbt Ens
                                     WHERE c.t_account = accd.t_account -- accdeb.t_account 
                                          and c.t_dockind = 4620 and Ens.t_id = c.t_docid) as fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = accd.t_account -- accdeb.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                             FROM accd,
                                   dacctrn_dbt trn 
                                   --daccount_dbt AccDeb,
                                   --daccount_dbt AccCred
                             WHERE     trn.t_date_carry >= add_months(BegDate, -12)
                                   AND trn.t_date_carry <= BegDate
                                   AND trn.t_state = 1
                                   and trn.t_accountid_payer = accd.t_accountid
                                   and trn.t_accountid_receiver in (select t_accountid from acck) 
                                   /*AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Ц/б, Корзина БПП')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                    ('+Маржа, ИВ и ДМ', '+ПереоценкаА')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)*/)
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.PrevYearCost = fo7.PrevYearCost + r.PrevCostBPPRur;



/*     Валютная переоценка в части номинала БПП за предыдущий год PrevYearCost  - БПП  */

      MERGE INTO DMsfo7_DBT fo7
           USING (  with accd as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                          distinct acc.t_accountid,acc.t_account,accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                           where categ.t_code in
                                                     ('-Маржа, ИВ и ДМ', '-ПереоценкаА')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account),
                        acck as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                            distinct acc.t_accountid,acc.t_account, accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                            where categ.t_code in
                                                     ('Ц/б, БПП')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account )
                      SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) PrevCostBPPRur
                        FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX3 )*/ 
                                   distinct trn.t_acctrnid ,
                                   (SELECT DISTINCT leg.t_pfi
                                      FROM dmcaccdoc_dbt c, ddl_leg_dbt leg
                                     WHERE c.t_account = acck.t_account --acccred.t_account 
                                     and c.t_dockind = 176 and leg.t_id = c.t_docid and leg.t_legkind = 0) as fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = acck.t_account -- acccred.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                            FROM acck,
                                   dacctrn_dbt trn, 
                                   --daccount_dbt AccDeb,
                                   --daccount_dbt AccCred
                                  accd
                             WHERE     trn.t_date_carry >= add_months(BegDate, -12)
                                   AND trn.t_date_carry <= BegDate
                                   AND trn.t_state = 1
                                   and trn.t_accountid_payer = accd.t_accountid
                                   and trn.t_accountid_receiver = acck.t_accountid 
                                   /*AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Ц/б, БПП')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('-Маржа, ИВ и ДМ', '-ПереоценкаА')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)*/)
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.PrevYearCost = fo7.PrevYearCost - r.PrevCostBPPRur;

/*   End  PrevYearCost  - БПП  */

/********************/
/*     Валютная переоценка за предыдущий год в части номинала по Корзине БПП PrevYearCost  - Корзина БПП  */

      MERGE INTO DMsfo7_DBT fo7
           USING ( with accd as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                          distinct acc.t_accountid,acc.t_account,accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                           where categ.t_code in
                                                    ('-Маржа, ИВ и ДМ', '-ПереоценкаА')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account),
                        acck as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                            distinct acc.t_accountid,acc.t_account, accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                            where categ.t_code in
                                                     ('Ц/б, Корзина БПП')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account )
                       SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) PrevCostBPPRur
                       FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX3 )*/ 
                                   distinct trn.t_acctrnid,
                                   (SELECT DISTINCT Ens.t_fiid
                                      FROM dmcaccdoc_dbt c, ddl_tick_ens_dbt Ens
                                     WHERE c.t_account = acck.t_account --acccred.t_account 
                                      and c.t_dockind = 4620 and Ens.t_id = c.t_docid) as fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = acck.t_account -- acccred.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                            FROM acck,
                                   dacctrn_dbt trn, 
                                   --daccount_dbt AccDeb,
                                   --daccount_dbt AccCred
                                   accd
                             WHERE     trn.t_date_carry >= add_months(BegDate, -12)
                                   AND trn.t_date_carry <= BegDate
                                   AND trn.t_state = 1
                                   and trn.t_accountid_payer = accd.t_accountid
                                   and trn.t_accountid_receiver = acck.t_accountid 
                                   /*AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Ц/б, Корзина БПП')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                    ('-Маржа, ИВ и ДМ', '-ПереоценкаА')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)*/)
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.PrevYearCost = fo7.PrevYearCost - r.PrevCostBPPRur;



/**/

      /*   Валютная переоценка в части УНКД за предыдущий год PrevYNKD  +  */

      MERGE INTO DMsfo7_DBT fo7
           USING ( with accd as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                          distinct acc.t_accountid,acc.t_account,accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                           where categ.t_code in
                                                     ('Уплаченный НКД')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account),
                        acck as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                            distinct acc.t_accountid,acc.t_account, accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                            where categ.t_code in
                                                     ('+Маржа, ИВ и ДМ', '+ПереоценкаА')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account )
                     SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) PrevYNKDRUR
                        FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX2 )*/ 
                                   distinct trn.t_acctrnid,
                                   /*(SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = accdeb.t_account)*/
                                   accd.t_fiid   AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = accd.t_account --accdeb.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                            FROM accd,
                                   dacctrn_dbt trn, 
                                   --daccount_dbt AccDeb,
                                   --daccount_dbt AccCred
                                   acck
                             WHERE     trn.t_date_carry >= add_months(BegDate, -12)
                                   AND trn.t_date_carry <= BegDate
                                   AND trn.t_state = 1
                                    and trn.t_accountid_payer = accd.t_accountid
                                   and trn.t_accountid_receiver = acck.t_accountid 
                                  /*AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Уплаченный НКД')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('+Маржа, ИВ и ДМ', '+ПереоценкаА')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)*/)
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.PrevYNKD = fo7.PrevYNKD + r.PrevYNKDRUR;

      /*   Валютная переоценка в части УНКД за предыдущий гогд PrevYNKD  -  */

      MERGE INTO DMsfo7_DBT fo7
           USING ( with accd as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                          distinct acc.t_accountid,acc.t_account,accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                           where categ.t_code in
                                                     ('-Маржа, ИВ и ДМ', '-ПереоценкаА')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account),
                        acck as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                            distinct acc.t_accountid,acc.t_account, accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                            where categ.t_code in
                                                     ('Уплаченный НКД')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account )
                      SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) PrevYNKDRur
                        FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX2 )*/ 
                                   distinct trn.t_acctrnid,
                                   /*(SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = acccred.t_account)*/
                                   acck.t_fiid   AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = acck.t_account --acccred.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                            FROM accd,
                                   dacctrn_dbt trn, 
                                   --daccount_dbt AccDeb,
                                   --daccount_dbt AccCred
                                   acck
                             WHERE     trn.t_date_carry >= add_months(BegDate, -12)
                                   AND trn.t_date_carry <= BegDate
                                   AND trn.t_state = 1
                                   and trn.t_accountid_payer = accd.t_accountid
                                   and trn.t_accountid_receiver = acck.t_accountid 
                                  /* AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Уплаченный НКД')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('-Маржа, ИВ и ДМ', '-ПереоценкаА')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)*/)
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.PrevYNKD = fo7.PrevYNKD - r.PrevYNkdRur;

/*    Валютная переоценка в части  УНКД БПП за предыдущий год PrevYNKD  + БПП  */

      MERGE INTO DMsfo7_DBT fo7
           USING ( with accd as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                          distinct acc.t_accountid,acc.t_account,accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                           where categ.t_code in
                                                     ('Уплаченный НКД, БПП')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account),
                        acck as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                            distinct acc.t_accountid,acc.t_account, accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                            where categ.t_code in
                                                     ('+Маржа, ИВ и ДМ', '+ПереоценкаА')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account )
                       SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) PrevYNKDBPPRur
                        FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX2 )*/ 
                                   distinct trn.t_acctrnid,
                                   (SELECT DISTINCT leg.t_pfi
                                      FROM dmcaccdoc_dbt c, ddl_leg_dbt leg
                                     WHERE c.t_account = accd.t_account -- accdeb.t_account 
                                       and c.t_dockind = 176 and leg.t_id = c.t_docid and leg.t_legkind = 0) as fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = accd.t_account -- accdeb.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                             FROM accd,
                                   dacctrn_dbt trn, 
                                   --daccount_dbt AccDeb,
                                   --daccount_dbt AccCred
                                   acck
                             WHERE     trn.t_date_carry >= add_months(BegDate, -12)
                                   AND trn.t_date_carry <= BegDate
                                   AND trn.t_state = 1
                                   and trn.t_accountid_payer = accd.t_accountid
                                   and trn.t_accountid_receiver = acck.t_accountid 
                                  /*AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Уплаченный НКД, БПП')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('+Маржа, ИВ и ДМ', '+ПереоценкаА')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)*/)
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.PrevYNKD = fo7.PrevYNKD + r.PrevYNKDBPPRur;

/*   End  PrevYNKD + БПП  */

/**********************/
/*    Валютная переоценка в части  Корзины УНКД БПП за предыдущий год PrevYNKD  + Корзина БПП  */

      MERGE INTO DMsfo7_DBT fo7
           USING ( with accd as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                          distinct acc.t_accountid,acc.t_account,accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                           where categ.t_code in
                                                     ('Уплач. НКД, Корзина БПП')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account
                             and accdoc.t_catnum != 1237),
                        acck as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                            distinct acc.t_accountid,acc.t_account, accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                            where categ.t_code in
                                                    ('+Маржа, ИВ и ДМ', '+ПереоценкаА')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account )
                      SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) PrevYNKDBPPRur
                      FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX2 )*/ 
                                   distinct trn.t_acctrnid,
                                   (SELECT DISTINCT Ens.t_fiid
                                      FROM dmcaccdoc_dbt c, ddl_tick_ens_dbt Ens
                                     WHERE c.t_account = acck.t_account  --accdeb.t_account 
                                     and c.t_dockind = 4620 and Ens.t_id = c.t_docid) as fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = accd.t_account --accdeb.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                            FROM accd,
                                   dacctrn_dbt trn, 
                                   --daccount_dbt AccDeb,
                                   --daccount_dbt AccCred
                                   acck
                             WHERE     trn.t_date_carry >= add_months(BegDate, -12)
                                   AND trn.t_date_carry <= BegDate
                                   AND trn.t_state = 1
                                   and trn.t_accountid_payer = accd.t_accountid
                                   and trn.t_accountid_receiver = acck.t_accountid 
                                  /*AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Уплач. НКД, Корзина БПП')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)
                                         AND  not exists(select 1 from dmcaccdoc_dbt where t_catid  in (select t_id from DMCCATEG_DBT t where t_number IN ( 1237))-- t_catnum = 1237 
                                                       and t_account = accdeb.t_account)
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                    ('+Маржа, ИВ и ДМ', '+ПереоценкаА')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)*/)
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.PrevYNKD = fo7.PrevYNKD + r.PrevYNKDBPPRur;




/*    Валютная переоценка в части УНКД БПП за предыдущий период  PrevYNKD  - БПП  */

      MERGE INTO DMsfo7_DBT fo7
           USING (  with accd as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                          distinct acc.t_accountid,acc.t_account,accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                           where categ.t_code in
                                                     ('-Маржа, ИВ и ДМ', '-ПереоценкаА')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account),
                        acck as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                            distinct acc.t_accountid,acc.t_account, accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                            where categ.t_code in
                                                     ('Уплаченный НКД, БПП')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account )
                      SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) PrevYNKDBPPRur
                        FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX2 )*/ 
                                   distinct trn.t_acctrnid,
                                   (SELECT DISTINCT leg.t_pfi
                                      FROM dmcaccdoc_dbt c, ddl_leg_dbt leg
                                     WHERE c.t_account = acck.t_account --- acccred.t_account 
                                       and c.t_dockind = 176 and leg.t_id = c.t_docid and leg.t_legkind = 0) as fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = acck.t_account --- acccred.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                             FROM accd,
                                   dacctrn_dbt trn, 
                                   --daccount_dbt AccDeb,
                                   --daccount_dbt AccCred
                                   acck
                             WHERE     trn.t_date_carry >= add_months(BegDate, -12)
                                   AND trn.t_date_carry <= BegDate
                                   AND trn.t_state = 1
                                   and trn.t_accountid_payer = accd.t_accountid
                                   and trn.t_accountid_receiver = acck.t_accountid 
                                  /*AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Уплаченный НКД, БПП')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('-Маржа, ИВ и ДМ', '-ПереоценкаА')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)*/)
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.PrevYNKD = fo7.PrevYNKD - r.PrevYNKDBPPRur;

/*   End  PrevYNKD  - БПП  */


/*****************/

/*    Валютная переоценка в части  Корзины УНКД БПП за предыдущий год PrevYNKD  - Корзина БПП  */

      MERGE INTO DMsfo7_DBT fo7
           USING ( with accd as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                          distinct acc.t_accountid,acc.t_account,accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                           where categ.t_code in
                                                     ('-Маржа, ИВ и ДМ', '-ПереоценкаА')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account),
                        acck as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                            distinct acc.t_accountid,acc.t_account, accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                            where categ.t_code in
                                                     ('Уплач. НКД, Корзина БПП')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account
                             and accdoc.t_catnum != 1237 )
                      SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) PrevYNKDBPPRur
                        FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX2 )*/ 
                                   distinct trn.t_acctrnid,
                                   (SELECT DISTINCT Ens.t_fiid
                                      FROM dmcaccdoc_dbt c, ddl_tick_ens_dbt Ens
                                     WHERE c.t_account = acck.t_account -- acccred.t_account 
                                       and c.t_dockind = 4620 and Ens.t_id = c.t_docid) as fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = acck.t_account -- acccred.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                             FROM accd,
                                   dacctrn_dbt trn, 
                                   --daccount_dbt AccDeb,
                                   --daccount_dbt AccCred
                                   acck
                             WHERE     trn.t_date_carry >= add_months(BegDate, -12)
                                   AND trn.t_date_carry <= BegDate
                                   AND trn.t_state = 1
                                   and trn.t_accountid_payer = accd.t_accountid
                                   and trn.t_accountid_receiver = acck.t_accountid 
                                   /*AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Уплач. НКД, Корзина БПП')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)
                                          AND  not exists(select 1 from dmcaccdoc_dbt where t_catid  in (select t_id from DMCCATEG_DBT t where t_number IN ( 1237)) --t_catnum = 1237
                                           and t_account = acccred.t_account)
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('-Маржа, ИВ и ДМ', '-ПереоценкаА')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)*/)
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.PrevYNKD = fo7.PrevYNKD - r.PrevYNKDBPPRur;




/*     Валютная переоценка в части премии за предыдущий год PrevBonus    +     */

      MERGE INTO DMsfo7_DBT fo7
           USING ( with accd as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                          distinct acc.t_accountid,acc.t_account,accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                           where categ.t_code in
                                                     ('Премия, ц/б')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account),
                        acck as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                            distinct acc.t_accountid,acc.t_account, accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                            where categ.t_code in
                                                     ('+Маржа, ИВ и ДМ', '+ПереоценкаА')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account )
                      SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) PrevBonus
                     FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX2 )*/ 
                                   distinct trn.t_acctrnid,
                                   /*(SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = accdeb.t_account)*/
                                   accd.t_fiid   AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = accd.t_account -- accdeb.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM accd,
                                   dacctrn_dbt trn, 
                                   --daccount_dbt AccDeb,
                                   --daccount_dbt AccCred
                                   acck
                             WHERE     trn.t_date_carry >= add_months(BegDate, -12)
                                   AND trn.t_date_carry <= BegDate
                                   AND trn.t_state = 1
                                   and trn.t_accountid_payer = accd.t_accountid
                                   and trn.t_accountid_receiver = acck.t_accountid 
                                  /*AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Премия, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('+Маржа, ИВ и ДМ', '+ПереоценкаА')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)*/)
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.PrevBonus = fo7.PrevBonus + r.PrevBonus;

/*    Валютная переоценка в части премии за предыдущий год PrevBonus    -     */

      MERGE INTO DMsfo7_DBT fo7
           USING ( with accd as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                          distinct acc.t_accountid,acc.t_account,accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                           where categ.t_code in
                                                     ('-Маржа, ИВ и ДМ', '-ПереоценкаА')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account),
                        acck as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                            distinct acc.t_accountid,acc.t_account, accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                            where categ.t_code in
                                                     ('Премия, ц/б')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account )
                       SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) PrevBonus
                       FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX2 )*/ 
                                   distinct trn.t_acctrnid,
                                   /*(SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = acccred.t_account)*/
                                   acck.t_fiid   AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = acck.t_account --acccred.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                             FROM accd,
                                   dacctrn_dbt trn, 
                                   --daccount_dbt AccDeb,
                                   --daccount_dbt AccCred
                                   acck
                             WHERE     trn.t_date_carry >= add_months(BegDate, -12)
                                   AND trn.t_date_carry <= BegDate
                                   AND trn.t_state = 1
                                   and trn.t_accountid_payer = accd.t_accountid
                                   and trn.t_accountid_receiver = acck.t_accountid 
                                   /*AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Премия, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('-Маржа, ИВ и ДМ', '-ПереоценкаА')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)*/)
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.PrevBonus = fo7.PrevBonus - r.PrevBonus;


/* Валютная переоценка в части начисленного купона за предыдущий год  PrevIncome    +  */

 MERGE INTO DMsfo7_DBT fo7
           USING ( with accd as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                          distinct acc.t_accountid,acc.t_account,accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc, dmctempl_dbt templ
                           where categ.t_code in
                                                     ('Начисл.ПДД, ц/б')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account
                             and templ.t_catid = categ.t_id and accdoc.t_templnum = templ.t_number and templ.t_value4 = 1 /*купон*/  and accdoc.t_iscommon = 'X'),
                        acck as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                            distinct acc.t_accountid,acc.t_account, accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                            where categ.t_code in
                                                     ('+Маржа, ИВ и ДМ', '+ПереоценкаА')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account ) 
                       SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) PrevIncome
                       FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX2 )*/ 
                                   distinct trn.t_acctrnid,
                                   /*(SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = accdeb.t_account and t_iscommon = 'X')*/
                                   accd.t_fiid   AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = accd.t_account --accdeb.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                            FROM accd,
                                   dacctrn_dbt trn, 
                                   --daccount_dbt AccDeb,
                                   --daccount_dbt AccCred
                                   acck
                             WHERE     trn.t_date_carry >=add_months(BegDate, -12)
                                   AND trn.t_date_carry <= BegDate
                                   AND trn.t_state = 1
                                   and trn.t_accountid_payer = accd.t_accountid
                                   and trn.t_accountid_receiver = acck.t_accountid 
                                  /*AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND accdeb.t_account IN
                                          (SELECT  accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc, dmctempl_dbt templ
                                            WHERE categ.t_code IN
                                                     ('Начисл.ПДД, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id and templ.t_catid = categ.t_id and accdoc.t_templnum = templ.t_number and templ.t_value4 = 1 \*купон*\  and accdoc.t_iscommon = 'X' group by accdoc.t_account )
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('+Маржа, ИВ и ДМ', '+ПереоценкаА')
                                                      and accdoc.t_catid = categ.t_id
                                                  )*/
)
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.PrevIncome = fo7.PrevIncome + r.PrevIncome;


/*   Валютная переоценка в части накопленного купона за предыдущий год PrevIncome    -  */

 MERGE INTO DMsfo7_DBT fo7
           USING ( with accd as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                          distinct acc.t_accountid,acc.t_account,accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                           where categ.t_code in
                                                     ('-Маржа, ИВ и ДМ', '-ПереоценкаА')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account),
                        acck as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                            distinct acc.t_accountid,acc.t_account, accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc, dmctempl_dbt templ
                            where categ.t_code in
                                                     ('Начисл.ПДД, ц/б')
                             and accdoc.t_catid = categ.t_id
                             and templ.t_catid = categ.t_id and accdoc.t_templnum = templ.t_number 
                             and templ.t_value4 = 1 /*купон*/  and accdoc.t_iscommon = 'X'
                             and acc.t_account = accdoc.t_account )
                       SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) PrevIncome
                       FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX2 )*/ 
                                   distinct trn.t_acctrnid,
                                  (SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = acck.t_account -- acccred.t_account 
                                             and t_iscommon = 'X')
                                      AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = acck.t_account --  acccred.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                            FROM accd,
                                   dacctrn_dbt trn, 
                                   --daccount_dbt AccDeb,
                                   --daccount_dbt AccCred
                                   acck
                             WHERE     trn.t_date_carry >= add_months(BegDate, -12)
                                   AND trn.t_date_carry <= BegDate
                                   AND trn.t_state = 1
                                   and trn.t_accountid_payer = accd.t_accountid
                                   and trn.t_accountid_receiver = acck.t_accountid 
                                  /*AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND acccred.t_account IN
                                          (SELECT  accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc, dmctempl_dbt templ
                                            WHERE categ.t_code IN
                                                     ('Начисл.ПДД, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id and templ.t_catid = categ.t_id and accdoc.t_templnum = templ.t_number and templ.t_value4 = 1 \*купон*\  and accdoc.t_iscommon = 'X' group by accdoc.t_account )
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('-Маржа, ИВ и ДМ', '-ПереоценкаА')
                                                      and accdoc.t_catid = categ.t_id
                                                  )*/
)
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.PrevIncome = fo7.PrevIncome - r.PrevIncome;


/*PrevDiscount*/

/* Валютная переоценка в части начисленного дисконта за предыдущий год  PrevDiscount    +  */

 MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) PrevDiscount
                      FROM (SELECT accdeb.t_account,acccred.t_account,
                                   (SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = accdeb.t_account and t_iscommon = 'X')
                                      AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = accdeb.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM dacctrn_dbt trn,
                                   daccount_dbt AccDeb,
                                   daccount_dbt AccCred
                             WHERE     trn.t_date_carry >=add_months(BegDate, -12)
                                   AND trn.t_date_carry <= BegDate
                                   AND trn.t_state = 1
                                   AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND accdeb.t_account IN
                                          (SELECT  accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc, dmctempl_dbt templ
                                            WHERE categ.t_code IN
                                                     ('Начисл.ПДД, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id and templ.t_catid = categ.t_id and accdoc.t_templnum = templ.t_number and templ.t_value4 = 2 /*дисконт*/  and accdoc.t_iscommon = 'X' group by accdoc.t_account )
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('+Маржа, ИВ и ДМ', '+ПереоценкаА')
                                                      and accdoc.t_catid = categ.t_id
                                                  )
)
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.PrevDiscount = fo7.PrevDiscount + r.PrevDiscount;


/*   Валютная переоценка в части накопленного дисконта за предыдущий год PrevDiscount    -  */

 MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) PrevDiscount
                      FROM (SELECT acccred.t_account,
                                   (SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = acccred.t_account and t_iscommon = 'X')
                                      AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = acccred.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM dacctrn_dbt trn,
                                   daccount_dbt AccDeb,
                                   daccount_dbt AccCred
                             WHERE     trn.t_date_carry >= add_months(BegDate, -12)
                                   AND trn.t_date_carry <= BegDate
                                   AND trn.t_state = 1
                                   AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND acccred.t_account IN
                                          (SELECT  accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc, dmctempl_dbt templ
                                            WHERE categ.t_code IN
                                                     ('Начисл.ПДД, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id and templ.t_catid = categ.t_id and accdoc.t_templnum = templ.t_number and templ.t_value4 = 2 /*дисконт*/  and accdoc.t_iscommon = 'X' group by accdoc.t_account )
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('-Маржа, ИВ и ДМ', '-ПереоценкаА')
                                                      and accdoc.t_catid = categ.t_id
                                                  )
)
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.PrevDiscount = fo7.PrevDiscount - r.PrevDiscount;

/* Остаток по счетам переоценки на начало перида  BegOverValue + */

/*Особая ситуация по дате 01.01.2019. Из-за переводов МСФО начальные остатки берем на 01.01.2019, но переоценка нужна на 31.12.2018 , так ка в 01.01.2019 есть операции переоценки */
        IF (BegDate = to_date('01.01.2019','dd.mm.yyyy')) then /*отсекаем переводы по мсфо в 01.01.2019*/
            BegDatePereoc:=BegDate -1;
        ELSE
            BegDatePereoc:=BegDate;
        END IF;   

      /* Положительная переоценка по 2 портфелю */

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT                              /*accdoc.t_account, */
                          accdoc.t_fiid,
                           NVL (SUM (abs(ROUND (RSI_Rsb_FIInstr.
                                             ConvSum (RSI_RSB_ACCOUNT.
                                                       RestAC (
                                                         accdoc.t_account,
                                                         accdoc.t_currency,
                                                         BegDatePereoc,
                                                         1,
                                                         NULL),
                                                      accdoc.t_currency,
                                                      0,
                                                      BegDatePereoc), 2))),
                                0)
                              BegOverValue
                      FROM dmcaccdoc_dbt accdoc,
                           dmccateg_dbt categ
                     WHERE     accdoc.t_catid = categ.t_id
                           AND categ.t_code IN ('+Переоценка, ц/б СССД_ЦБ')
                           AND accdoc.t_iscommon = 'X'
                  GROUP BY                              /*accdoc.t_account, */
                          accdoc.t_fiid) r
              ON (r.t_fiid = fo7.fiid AND  2/*СССД_ЦБ*/ = fo7.begportfcode and  r.BegOverValue != 0)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.BegOverValue = fo7.BegOverValue + r.BegOverValue; 
         
         
         
      /* отрицательная переоценка по 2 портфелю ( с минусом) */

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT                              /*accdoc.t_account, */
                          accdoc.t_fiid,
                           NVL (SUM (abs(ROUND (RSI_Rsb_FIInstr.
                                             ConvSum (RSI_RSB_ACCOUNT.
                                                       RestAC (
                                                         accdoc.t_account,
                                                         accdoc.t_currency,
                                                         BegDatePereoc,
                                                         1,
                                                         NULL),
                                                      accdoc.t_currency,
                                                      0,
                                                      BegDatePereoc), 2))),
                                0)*(-1)
                              BegOverValue
                      FROM dmcaccdoc_dbt accdoc,
                           dmccateg_dbt categ
                     WHERE     accdoc.t_catid = categ.t_id
                           AND categ.t_code IN ('-Переоценка, ц/б СССД_ЦБ')
                           AND accdoc.t_iscommon = 'X'
                  GROUP BY                              /*accdoc.t_account, */
                          accdoc.t_fiid) r
              ON (r.t_fiid = fo7.fiid AND  2/*СССД_ЦБ*/ = fo7.begportfcode and  r.BegOverValue != 0)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.BegOverValue = fo7.BegOverValue + r.BegOverValue;          


      /* Положительная переоценка по 1 портфелю */

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT                              /*accdoc.t_account, */
                          accdoc.t_fiid,
                           NVL (SUM (abs(ROUND (RSI_Rsb_FIInstr.
                                             ConvSum (RSI_RSB_ACCOUNT.
                                                       RestAC (
                                                         accdoc.t_account,
                                                         accdoc.t_currency,
                                                         BegDatePereoc,
                                                         1,
                                                         NULL),
                                                      accdoc.t_currency,
                                                      0,
                                                      BegDatePereoc), 2))),
                                0)
                              BegOverValue
                      FROM dmcaccdoc_dbt accdoc,
                           dmccateg_dbt categ
                     WHERE     accdoc.t_catid = categ.t_id
                           AND categ.t_code IN ('+Переоценка, ц/б ССПУ_ЦБ')
                           AND accdoc.t_iscommon = 'X'
                  GROUP BY                              /*accdoc.t_account, */
                          accdoc.t_fiid) r
              ON (r.t_fiid = fo7.fiid AND  1/*ССПУ_ЦБ*/ = fo7.begportfcode and  r.BegOverValue != 0)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.BegOverValue = fo7.BegOverValue + r.BegOverValue; 



      /* Отрицательная  переоценка по 1 портфелю( с минусом)  */

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT                              /*accdoc.t_account, */
                          accdoc.t_fiid,
                           NVL (SUM (abs(ROUND (RSI_Rsb_FIInstr.
                                             ConvSum (RSI_RSB_ACCOUNT.
                                                       RestAC (
                                                         accdoc.t_account,
                                                         accdoc.t_currency,
                                                         BegDatePereoc,
                                                         1,
                                                         NULL),
                                                      accdoc.t_currency,
                                                      0,
                                                      BegDatePereoc), 2))),
                                0)*(-1)
                              BegOverValue
                      FROM dmcaccdoc_dbt accdoc,
                           dmccateg_dbt categ
                     WHERE     accdoc.t_catid = categ.t_id
                           AND categ.t_code IN ('-Переоценка, ц/б ССПУ_ЦБ')
                           AND accdoc.t_iscommon = 'X'
                  GROUP BY                              /*accdoc.t_account, */
                          accdoc.t_fiid) r
              ON (r.t_fiid = fo7.fiid AND  1/*ССПУ_ЦБ*/ = fo7.begportfcode and  r.BegOverValue != 0)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.BegOverValue = fo7.BegOverValue + r.BegOverValue; 


/*Добавим переоценку по переводам в 01.01.2019 . Были переводы отр переоценки из 2 в 1 портфель*/

  IF (BegDate = to_date('01.01.2019','dd.mm.yyyy')) then 
      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) *(-1) BegOverValue
                      FROM (SELECT acccred.t_account,
                                   (SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = acccred.t_account)
                                      AS fiid,
                              (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = acccred.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM dacctrn_dbt trn,
                                   daccount_dbt AccDeb,
                                   daccount_dbt AccCred
                             WHERE    
                                    trn.t_date_carry = BegDate
                                   AND trn.t_state = 1
                                   AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('-Переоценка, ц/б СССД_ЦБ' )
                                                  AND accdoc.t_catid =
                                                         categ.t_id)
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('-Переоценка, ц/б ССПУ_ЦБ')
                                                  AND accdoc.t_catid =
                                                         categ.t_id))
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.BegOverValue = fo7.BegOverValue + r.BegOverValue;

/*Добавим в остаток корректирующую проводку  по переоценки выпуска RU0009029540*/

      MERGE INTO DMsfo7_DBT fo7
           USING (   SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) *(-1) BegOverValue
                      FROM (SELECT acccred.t_account,
                                   (SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = acccred.t_account)
                                      AS fiid,
                              (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = acccred.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM dacctrn_dbt trn,
                                   daccount_dbt AccDeb,
                                   daccount_dbt AccCred
                             WHERE    
                                   trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND accdeb.t_account IN ('50720810800000000004')
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('+Переоценка, ц/б СССД_ЦБ')
                                                  AND accdoc.t_catid =
                                                         categ.t_id and accdoc.t_fiid = 3808))
                  GROUP BY fiid, portfnum
) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.BegOverValue = fo7.BegOverValue + r.BegOverValue;


  End IF;



/* END Остаток по счетам переоценки на начало перида  BegOverValue + */



/*   Остатки по счетам корректировок на начало периода  BegIntToEir */

/* Остатки по КУ +Корректировка, ц/б  на начало периода */

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT                              /*accdoc.t_account, */
                          accdoc.t_fiid,
                           templ.t_value1,
                           NVL (SUM (abs(ROUND (RSI_Rsb_FIInstr.
                                             ConvSum (RSI_RSB_ACCOUNT.
                                                       RestAC (
                                                         accdoc.t_account,
                                                         accdoc.t_currency,
                                                         BegDate,
                                                         1,
                                                         NULL),
                                                      accdoc.t_currency,
                                                      0,
                                                      BegDate), 2))),
                                0)
                              BegIntToEir
                      FROM dmcaccdoc_dbt accdoc,
                           dmctempl_dbt templ,
                           dmccateg_dbt categ
                     WHERE     accdoc.t_catid = categ.t_id
                           AND categ.t_code IN ('+Корректировка, ц/б')
                           AND accdoc.t_iscommon = 'X'
                           AND templ.t_catid = accdoc.t_catid
                           AND templ.t_number = accdoc.t_templnum
                  GROUP BY                              /*accdoc.t_account, */
                          accdoc.t_fiid,
                           templ.t_value1) r
              ON (r.t_fiid = fo7.fiid AND r.t_value1 = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.BegIntToEir = fo7.BegIntToEir + r.BegIntToEir; 

/* Остатки по КУ -Корректировка, ц/б  на начало периода ( с минусом) */

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT                              /*accdoc.t_account, */
                          accdoc.t_fiid,
                           templ.t_value1,
                           NVL (SUM (abs(ROUND (RSI_Rsb_FIInstr.
                                             ConvSum (RSI_RSB_ACCOUNT.
                                                       RestAC (
                                                         accdoc.t_account,
                                                         accdoc.t_currency,
                                                         BegDate,
                                                         1,
                                                         NULL),
                                                      accdoc.t_currency,
                                                      0,
                                                      BegDate), 2))),
                                0)*(-1)
                              BegIntToEir
                      FROM dmcaccdoc_dbt accdoc,
                           dmctempl_dbt templ,
                           dmccateg_dbt categ
                     WHERE     accdoc.t_catid = categ.t_id
                           AND categ.t_code IN ('-Корректировка, ц/б')
                           AND accdoc.t_iscommon = 'X'
                           AND templ.t_catid = accdoc.t_catid
                           AND templ.t_number = accdoc.t_templnum
                  GROUP BY                              /*accdoc.t_account, */
                          accdoc.t_fiid,
                           templ.t_value1) r
              ON (r.t_fiid = fo7.fiid AND r.t_value1 = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.BegIntToEir = fo7.BegIntToEir + r.BegIntToEir; 

/*   END Остатки по счетам корректировок на начало периода  BegIntToEir */

/*Итого за начало периода BegItog*/

update DMsfo7_DBT set BegItog = begportfcost + begYnkd + begBonuscost+ begNKD +   begDiscount +PrevYearCost+PrevYNKD+PrevBonus+PrevIncome+PrevDiscount+BegOverValue + BegIntToEir;



/*  ДАННЫЕ ПО РЕКЛАССУ    */

--select * from dmcaccdoc_dbt where t_fiid = 2000 and t_catnum = 231  

--select * from dmcaccdoc_dbt where

/*Данные по реклассу Номинальная стоимость. ReclCost  по конечному портфелю  с плюсом*/  

      MERGE INTO DMsfo7_DBT fo7
           USING ( with accdk as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                          distinct acc.t_accountid,acc.t_account,accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                           where categ.t_code in
                                                     ('Наш портфель ц/б',
                                                      'Наш портфель ПКУ, ц/б')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account)
                        SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) ReclCost
                      FROM (SELECT  /*+ ordered index(trn DACCTRN_DBT_IDX2 )*/ 
                                   distinct trn.t_acctrnid,
                                   /*(SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = accdeb.t_account)*/
                                   accd.t_fiid   AS fiid,
                                  (Case When substr(accd.t_account -- accdeb.t_account
                                       , 1, 3) = '601'  Then 7 else (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = accd.t_account -- accdeb.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number) end)
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM accdk accd,
                                   dacctrn_dbt trn, 
                                   --daccount_dbt AccDeb,
                                   --daccount_dbt AccCred
                                   accdk acck,
                                   doprdocs_dbt docs,
                                   doproper_dbt oper 
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_date_carry != to_date('01.01.2019','dd.mm.yyyy')
                                   AND trn.t_state = 1
                                   and trn.t_accountid_payer = accd.t_accountid
                                   and trn.t_accountid_receiver = acck.t_accountid 
                                   and docs.t_acctrnid= trn.t_acctrnid
                                   and oper.t_id_operation = docs.t_id_operation
                                   and oper.t_dockind = 105
                                   /*AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Наш портфель ц/б',
                                                      'Наш портфель ПКУ, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Наш портфель ц/б',
                                                      'Наш портфель ПКУ, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)*/)
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.ReclCost = fo7.ReclCost + r.ReclCost;


/*Данные по реклассу Номинальная стоимость. ReclCost  по исходному портфелю  с минусом*/  

      MERGE INTO DMsfo7_DBT fo7
           USING ( with accdk as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                          distinct acc.t_accountid,acc.t_account,accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                           where categ.t_code in
                                                     ('Наш портфель ц/б',
                                                      'Наш портфель ПКУ, ц/б')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account)
                       SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) ReclCost
                     FROM (SELECT  /*+ ordered index(trn DACCTRN_DBT_IDX2 )*/ 
                                   distinct trn.t_acctrnid,
                                   /*(SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = accdeb.t_account)*/
                                   accd.t_fiid   AS fiid,
                                  (Case When substr(acck.t_account --acccred.t_account
                                           , 1, 3) = '601'  Then 7 else (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = acck.t_account -- acccred.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number) end)
                                      AS portfnum,
                                   trn.t_sum_natcur
                             FROM accdk accd,
                                   dacctrn_dbt trn, 
                                   --daccount_dbt AccDeb,
                                   --daccount_dbt AccCred
                                   accdk acck,
                                    doprdocs_dbt docs,
                                     doproper_dbt oper 
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_date_carry != to_date('01.01.2019','dd.mm.yyyy')
                                   AND trn.t_state = 1
                                   and trn.t_accountid_payer = accd.t_accountid
                                   and trn.t_accountid_receiver = acck.t_accountid 
                                   and docs.t_acctrnid= trn.t_acctrnid
                                   and oper.t_id_operation = docs.t_id_operation
                                   and oper.t_dockind = 105
                                   /*AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Наш портфель ц/б',
                                                      'Наш портфель ПКУ, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Наш портфель ц/б',
                                                      'Наш портфель ПКУ, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)*/)
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.ReclCost = fo7.ReclCost - r.ReclCost;

/*******************************/
/*Данные по реклассу Премия. ReclBonus  по конечному портфелю  с плюсом*/  

      MERGE INTO DMsfo7_DBT fo7
           USING ( with accdk as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                          distinct acc.t_accountid,acc.t_account,accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                           where categ.t_code in
                                                     ('Премия, ц/б')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account)
                      SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) ReclBonus
                      FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX2 )*/ 
                                   distinct trn.t_acctrnid,
                                   /*(SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = accdeb.t_account)*/
                                   accd.t_fiid   AS fiid,
                              (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = accd.t_account -- accdeb.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                            FROM accdk accd,
                                   dacctrn_dbt trn, 
                                   --daccount_dbt AccDeb,
                                   --daccount_dbt AccCred
                                  accdk acck,
                                  doprdocs_dbt docs,
                                   doproper_dbt oper 
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_date_carry != to_date('01.01.2019','dd.mm.yyyy')
                                   AND trn.t_state = 1
                                   and trn.t_accountid_payer = accd.t_accountid
                                   and trn.t_accountid_receiver = acck.t_accountid 
                                   and docs.t_acctrnid= trn.t_acctrnid
                                   and oper.t_id_operation = docs.t_id_operation
                                   and oper.t_dockind = 105
                                   /*AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Премия, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Премия, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)*/)
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.ReclBonus = fo7.ReclBonus + r.ReclBonus;


/*Данные по реклассу по премии. ReclBonus  по исходному портфелю  с минусом*/  

      MERGE INTO DMsfo7_DBT fo7
           USING ( with accdk as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                          distinct acc.t_accountid,acc.t_account,accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                           where categ.t_code in
                                                     ('Премия, ц/б')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account)
                      SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) ReclBonus
                       FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX2 )*/ 
                                   distinct trn.t_acctrnid,
                                   /*(SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = accdeb.t_account)*/
                                   accd.t_fiid   AS fiid,
                                  (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = acck.t_account -- acccred.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number) 
                                      AS portfnum,
                                   trn.t_sum_natcur
                            FROM accdk accd,
                                   dacctrn_dbt trn, 
                                   --daccount_dbt AccDeb,
                                   --daccount_dbt AccCred
                                  accdk acck,
                                    doprdocs_dbt docs,
                                     doproper_dbt oper 
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_date_carry != to_date('01.01.2019','dd.mm.yyyy')
                                   AND trn.t_state = 1
                                   and trn.t_accountid_payer = accd.t_accountid
                                   and trn.t_accountid_receiver = acck.t_accountid 
                                   and docs.t_acctrnid= trn.t_acctrnid
                                   and oper.t_id_operation = docs.t_id_operation
                                   and oper.t_dockind = 105
                                  /*AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Премия, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Премия, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)*/)
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.ReclBonus = fo7.ReclBonus - r.ReclBonus;


/*******************************/
/*Данные по реклассу Уплаченного НКД. ReclYnkd  по конечному портфелю  с плюсом*/  

      MERGE INTO DMsfo7_DBT fo7
           USING ( with accdk as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                          distinct acc.t_accountid,acc.t_account,accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                           where categ.t_code in
                                                     ('Уплаченный НКД')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account)
                        SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) ReclYnkd
                       FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX2 )*/ 
                                   distinct trn.t_acctrnid,
                                   /*(SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = accdeb.t_account)*/
                                   accd.t_fiid   AS fiid,
                              (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = accd.t_account -- accdeb.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                             FROM accdk accd,
                                   dacctrn_dbt trn, 
                                   --daccount_dbt AccDeb,
                                   --daccount_dbt AccCred
                                   accdk  acck,
                                    doprdocs_dbt docs,
                                     doproper_dbt oper 
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_date_carry != to_date('01.01.2019','dd.mm.yyyy')
                                   AND trn.t_state = 1
                                   and trn.t_accountid_payer = accd.t_accountid
                                   and trn.t_accountid_receiver = acck.t_accountid 
                                   and docs.t_acctrnid= trn.t_acctrnid
                                   and oper.t_id_operation = docs.t_id_operation
                                   and oper.t_dockind = 105
                                   /*AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Уплаченный НКД')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Уплаченный НКД')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)*/)
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.ReclYnkd = fo7.ReclYnkd + r.ReclYnkd;


/*Данные по реклассу по Уплаченному НКД. ReclYnkd  по исходному портфелю  с минусом*/  

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) ReclYnkd
                      FROM (SELECT accdeb.t_account,
                                   (SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = accdeb.t_account)
                                      AS fiid,
                                  (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = acccred.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number) 
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM dacctrn_dbt trn,
                                   daccount_dbt AccDeb,
                                   daccount_dbt AccCred,
                                    doprdocs_dbt docs,
                                     doproper_dbt oper 
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_date_carry != to_date('01.01.2019','dd.mm.yyyy')
                                   AND trn.t_state = 1
                                   and docs.t_acctrnid= trn.t_acctrnid
                                   and oper.t_id_operation = docs.t_id_operation
                                   and oper.t_dockind = 105
                                   AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Уплаченный НКД')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Уплаченный НКД')
                                                  AND accdoc.t_catid =
                                                         categ.t_id))
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.ReclYnkd = fo7.ReclYnkd - r.ReclYnkd;


/*******************************/
/*Данные по реклассу Начисленного НКД. ReclIncome по конечному портфелю  с плюсом*/  

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) ReclIncome
                      FROM (SELECT accdeb.t_account,
                                   (SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = accdeb.t_account)
                                      AS fiid,
                              (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = accdeb.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM dacctrn_dbt trn,
                                   daccount_dbt AccDeb,
                                   daccount_dbt AccCred,
                                    doprdocs_dbt docs,
                                     doproper_dbt oper 
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_date_carry != to_date('01.01.2019','dd.mm.yyyy')
                                   AND trn.t_state = 1
                                   and docs.t_acctrnid= trn.t_acctrnid
                                   and oper.t_id_operation = docs.t_id_operation
                                   and oper.t_dockind = 105
                                   AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND accdeb.t_account IN
                                          (SELECT  accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc, dmctempl_dbt templ
                                            WHERE categ.t_code IN
                                                     ('Начисл.ПДД, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id and templ.t_catid = categ.t_id and accdoc.t_templnum = templ.t_number and templ.t_value4 = 1 /*купон*/  and accdoc.t_iscommon = 'X' group by accdoc.t_account )
                                   AND acccred.t_account IN
                                          (SELECT  accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc, dmctempl_dbt templ
                                            WHERE categ.t_code IN
                                                     ('Начисл.ПДД, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id and templ.t_catid = categ.t_id and accdoc.t_templnum = templ.t_number and templ.t_value4 = 1 /*купон*/  and accdoc.t_iscommon = 'X' group by accdoc.t_account )
                                                         )
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.ReclIncome = fo7.ReclIncome + r.ReclIncome;


/*Данные по реклассу по Начисленному НКД. ReclIncome  по исходному портфелю  с минусом*/  

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) ReclIncome
                      FROM (SELECT accdeb.t_account,
                                   (SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = accdeb.t_account)
                                      AS fiid,
                                  (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = acccred.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number) 
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM dacctrn_dbt trn,
                                   daccount_dbt AccDeb,
                                   daccount_dbt AccCred,
                                    doprdocs_dbt docs,
                                     doproper_dbt oper 
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_date_carry != to_date('01.01.2019','dd.mm.yyyy')
                                   AND trn.t_state = 1
                                   and docs.t_acctrnid= trn.t_acctrnid
                                   and oper.t_id_operation = docs.t_id_operation
                                   and oper.t_dockind = 105
                                   AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND accdeb.t_account IN
                                          (SELECT  accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc, dmctempl_dbt templ
                                            WHERE categ.t_code IN
                                                     ('Начисл.ПДД, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id and templ.t_catid = categ.t_id and accdoc.t_templnum = templ.t_number and templ.t_value4 = 1 /*купон*/  and accdoc.t_iscommon = 'X' group by accdoc.t_account )
                                   AND acccred.t_account IN
                                          (SELECT  accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc, dmctempl_dbt templ
                                            WHERE categ.t_code IN
                                                     ('Начисл.ПДД, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id and templ.t_catid = categ.t_id and accdoc.t_templnum = templ.t_number and templ.t_value4 = 1 /*купон*/  and accdoc.t_iscommon = 'X' group by accdoc.t_account )
                                                         )
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.ReclIncome = fo7.ReclIncome - r.ReclIncome;

/*******************************/
/*Данные по реклассу Начисленного Дисконту. ReclDiscount по конечному портфелю  с плюсом*/  

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) ReclDiscount
                      FROM (SELECT accdeb.t_account,
                                   (SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = accdeb.t_account)
                                      AS fiid,
                              (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = accdeb.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM dacctrn_dbt trn,
                                   daccount_dbt AccDeb,
                                   daccount_dbt AccCred,
                                    doprdocs_dbt docs,
                                     doproper_dbt oper 
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_date_carry != to_date('01.01.2019','dd.mm.yyyy')
                                   AND trn.t_state = 1
                                   and docs.t_acctrnid= trn.t_acctrnid
                                   and oper.t_id_operation = docs.t_id_operation
                                   and oper.t_dockind = 105
                                   AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND accdeb.t_account IN
                                          (SELECT  accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc, dmctempl_dbt templ
                                            WHERE categ.t_code IN
                                                     ('Начисл.ПДД, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id and templ.t_catid = categ.t_id and accdoc.t_templnum = templ.t_number and templ.t_value4 = 2 /*дисконт*/  and accdoc.t_iscommon = 'X' group by accdoc.t_account )
                                   AND acccred.t_account IN
                                          (SELECT  accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc, dmctempl_dbt templ
                                            WHERE categ.t_code IN
                                                     ('Начисл.ПДД, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id and templ.t_catid = categ.t_id and accdoc.t_templnum = templ.t_number and templ.t_value4 = 2 /*дисконт*/  and accdoc.t_iscommon = 'X' group by accdoc.t_account )
                                                         )
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.ReclDiscount = fo7.ReclDiscount + r.ReclDiscount;


/*Данные по реклассу по Начисленному Дисконту. ReclDiscount  по исходному портфелю  с минусом*/  

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) ReclDiscount
                      FROM (SELECT accdeb.t_account,
                                   (SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = accdeb.t_account)
                                      AS fiid,
                                  (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = acccred.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number) 
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM dacctrn_dbt trn,
                                   daccount_dbt AccDeb,
                                   daccount_dbt AccCred,
                                    doprdocs_dbt docs,
                                     doproper_dbt oper 
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_date_carry != to_date('01.01.2019','dd.mm.yyyy')
                                   AND trn.t_state = 1
                                   and docs.t_acctrnid= trn.t_acctrnid
                                   and oper.t_id_operation = docs.t_id_operation
                                   and oper.t_dockind = 105
                                   AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND accdeb.t_account IN
                                          (SELECT  accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc, dmctempl_dbt templ
                                            WHERE categ.t_code IN
                                                     ('Начисл.ПДД, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id and templ.t_catid = categ.t_id and accdoc.t_templnum = templ.t_number and templ.t_value4 = 2 /*дисконта*/  and accdoc.t_iscommon = 'X' group by accdoc.t_account )
                                   AND acccred.t_account IN
                                          (SELECT  accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc, dmctempl_dbt templ
                                            WHERE categ.t_code IN
                                                     ('Начисл.ПДД, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id and templ.t_catid = categ.t_id and accdoc.t_templnum = templ.t_number and templ.t_value4 = 2 /*дисконт*/  and accdoc.t_iscommon = 'X' group by accdoc.t_account )
                                                         )
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.ReclDiscount = fo7.ReclDiscount - r.ReclDiscount;


/*******************************/
/*Данные по реклассу Переоценки . ReclOverValue  по конечному портфелю  с плюсом*/  

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) ReclOverValue
                      FROM (SELECT accdeb.t_account,
                                   (SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = accdeb.t_account)
                                      AS fiid,
                              (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = accdeb.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM dacctrn_dbt trn,
                                   daccount_dbt AccDeb,
                                   daccount_dbt AccCred,
                                    doprdocs_dbt docs,
                                     doproper_dbt oper 
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_date_carry != to_date('01.01.2019','dd.mm.yyyy')
                                   AND trn.t_state = 1
                                   and docs.t_acctrnid= trn.t_acctrnid
                                   and oper.t_id_operation = docs.t_id_operation
                                   and oper.t_dockind = 105
                                   AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('+Переоценка, ц/б ССПУ_ЦБ', '-Переоценка, ц/б ССПУ_ЦБ', '+Переоценка, ц/б СССД_ЦБ', '-Переоценка, ц/б СССД_ЦБ')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('+Переоценка, ц/б ССПУ_ЦБ', '-Переоценка, ц/б ССПУ_ЦБ', '+Переоценка, ц/б СССД_ЦБ', '-Переоценка, ц/б СССД_ЦБ')
                                                  AND accdoc.t_catid =
                                                         categ.t_id))
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.ReclOverValue = fo7.ReclOverValue + r.ReclOverValue;


/*Данные по реклассу по Переоценки. ReclOverValue  по исходному портфелю  с минусом*/  

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) ReclOverValue
                      FROM (SELECT accdeb.t_account,
                                   (SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = accdeb.t_account)
                                      AS fiid,
                                  (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = acccred.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number) 
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM dacctrn_dbt trn,
                                   daccount_dbt AccDeb,
                                   daccount_dbt AccCred,
                                    doprdocs_dbt docs,
                                     doproper_dbt oper 
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_date_carry != to_date('01.01.2019','dd.mm.yyyy')
                                   AND trn.t_state = 1
                                   and docs.t_acctrnid= trn.t_acctrnid
                                   and oper.t_id_operation = docs.t_id_operation
                                   and oper.t_dockind = 105
                                   AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('+Переоценка, ц/б ССПУ_ЦБ', '-Переоценка, ц/б ССПУ_ЦБ', '+Переоценка, ц/б СССД_ЦБ', '-Переоценка, ц/б СССД_ЦБ')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('+Переоценка, ц/б ССПУ_ЦБ', '-Переоценка, ц/б ССПУ_ЦБ', '+Переоценка, ц/б СССД_ЦБ', '-Переоценка, ц/б СССД_ЦБ')
                                                  AND accdoc.t_catid =
                                                         categ.t_id))
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.ReclOverValue = fo7.ReclOverValue - r.ReclOverValue;


/*Данные по реклассу по Переоценки. ReclOverValue при списании переоценки, а не при переносе (с минусом)*/  

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) ReclOverValue
                      FROM (SELECT accdeb.t_account,
                                   (SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = accdeb.t_account)
                                      AS fiid,
                                  (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = acccred.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number) 
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM dacctrn_dbt trn,
                                   daccount_dbt AccDeb,
                                   daccount_dbt AccCred,
                                    doprdocs_dbt docs,
                                     doproper_dbt oper 
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_date_carry != to_date('01.01.2019','dd.mm.yyyy')
                                   AND trn.t_state = 1
                                   and docs.t_acctrnid= trn.t_acctrnid
                                   and oper.t_id_operation = docs.t_id_operation
                                   and oper.t_dockind = 105
                                   AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND accdeb.t_account not IN /* НЕ ВХОДИТ*/
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('+Переоценка, ц/б ССПУ_ЦБ', '-Переоценка, ц/б ССПУ_ЦБ', '+Переоценка, ц/б СССД_ЦБ', '-Переоценка, ц/б СССД_ЦБ')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('+Переоценка, ц/б ССПУ_ЦБ', '-Переоценка, ц/б ССПУ_ЦБ', '+Переоценка, ц/б СССД_ЦБ', '-Переоценка, ц/б СССД_ЦБ')
                                                  AND accdoc.t_catid =
                                                         categ.t_id))
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.ReclOverValue = fo7.ReclOverValue - r.ReclOverValue;




/* END ДАННЫЕ ПО РЕКЛАССУ    */


/*  Перевод в ДУ/ ЗПИФ/ ПИФ  */

    /*  Перевод в ДУ. ПОЛЕ TODOCOST по Наш портфель */

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) TODUCOST
                      FROM (SELECT acccred.t_account,
                                   (SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = acccred.t_account)
                                      AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = acccred.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM dacctrn_dbt trn,
                                   daccount_dbt AccDeb,
                                   daccount_dbt AccCred
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Наш портфель ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id and accdoc.t_iscommon = 'X')
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT ac.t_account
                                             FROM daccount_dbt ac
                                            WHERE ac.t_balance = '47901'))
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.ToDUCost = fo7.ToDUCost + r.TODUCOST;
         
             /*  Перевод в ДУ. ПОЛЕ TODOCOST по Наш портфель ПКУ, ц/б */

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) TODUCOST
                      FROM (SELECT acccred.t_account,
                                   (SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = acccred.t_account)
                                      AS fiid,
                                 7
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM dacctrn_dbt trn,
                                   daccount_dbt AccDeb,
                                   daccount_dbt AccCred
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Наш портфель ПКУ, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id and accdoc.t_iscommon = 'X')
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT ac.t_account
                                             FROM daccount_dbt ac
                                            WHERE ac.t_balance = '47901'))
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.ToDUCost = fo7.ToDUCost + r.TODUCOST;



  /*  Перевод в ДУ. ПОЛЕ TODUYNKD по КУ Уплаченный НКД */

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) TODUYNKD
                      FROM (SELECT acccred.t_account,
                                   (SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = acccred.t_account)
                                      AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = acccred.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM dacctrn_dbt trn,
                                   daccount_dbt AccDeb,
                                   daccount_dbt AccCred
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Уплаченный НКД')
                                                  AND accdoc.t_catid =
                                                         categ.t_id and accdoc.t_iscommon = 'X')
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT ac.t_account
                                             FROM daccount_dbt ac
                                            WHERE ac.t_balance = '47901'))
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.TODUYNKD = fo7.TODUYNKD + r.TODUYNKD;
         


  /*  Перевод в ДУ. ПОЛЕ TODUBONUS по КУ Премия, ц/б */

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) TODUBONUS
                      FROM (SELECT acccred.t_account,
                                   (SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = acccred.t_account)
                                      AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = acccred.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM dacctrn_dbt trn,
                                   daccount_dbt AccDeb,
                                   daccount_dbt AccCred
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Премия, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id and accdoc.t_iscommon = 'X')
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT ac.t_account
                                             FROM daccount_dbt ac
                                            WHERE ac.t_balance = '47901'))
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.TODUBONUS = fo7.TODUBONUS + r.TODUBONUS;       
         
         
  /*  Перевод в ДУ. ПОЛЕ TODUINCOME по КУ  Начисл.ПДД, ц/б */

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) TODUINCOME
                      FROM (SELECT acccred.t_account,
                                   (SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = acccred.t_account)
                                      AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = acccred.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM dacctrn_dbt trn,
                                   daccount_dbt AccDeb,
                                   daccount_dbt AccCred
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                                FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc, dmctempl_dbt templ
                                            WHERE categ.t_code IN
                                                     ('Начисл.ПДД, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id and templ.t_catid = categ.t_id and accdoc.t_templnum = templ.t_number and templ.t_value4 = 1 /*купон*/  and accdoc.t_iscommon = 'X' group by accdoc.t_account )                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT ac.t_account
                                             FROM daccount_dbt ac
                                            WHERE ac.t_balance = '47901'))
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.TODUINCOME = fo7.TODUINCOME + r.TODUINCOME;            
  
  
    /*  Перевод в ДУ. ПОЛЕ TODUDISCOUNT по КУ  Начисл.ПДД, ц/б */

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) TODUDISCOUNT
                      FROM (SELECT acccred.t_account,
                                   (SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = acccred.t_account)
                                      AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = acccred.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM dacctrn_dbt trn,
                                   daccount_dbt AccDeb,
                                   daccount_dbt AccCred
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                                FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc, dmctempl_dbt templ
                                            WHERE categ.t_code IN
                                                     ('Начисл.ПДД, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id and templ.t_catid = categ.t_id and accdoc.t_templnum = templ.t_number and templ.t_value4 = 2 /*дисконт*/  and accdoc.t_iscommon = 'X' group by accdoc.t_account )                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT ac.t_account
                                             FROM daccount_dbt ac
                                            WHERE ac.t_balance = '47901'))
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.TODUDISCOUNT = fo7.TODUDISCOUNT + r.TODUDISCOUNT;            

  /*  Перевод в ДУ. ПОЛЕ TODUOVERVALUE по переоценке */

/*   -Переоценка, ц/б СССД_ЦБ   */

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) TODUOVERVALUE
                      FROM (SELECT acccred.t_account,
                                   (SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = acccred.t_account)
                                      AS fiid,
                                 2
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM dacctrn_dbt trn,
                                   daccount_dbt AccDeb,
                                   daccount_dbt AccCred
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('-Переоценка, ц/б СССД_ЦБ')
                                                  AND accdoc.t_catid =
                                                         categ.t_id and accdoc.t_iscommon = 'X')
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT ac.t_account
                                             FROM daccount_dbt ac
                                            WHERE ac.t_balance = '47901'))
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND 2 = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.TODUOVERVALUE = fo7.TODUOVERVALUE + r.TODUOVERVALUE;       
 
/*   -Переоценка, ц/б ССПУ_ЦБ   */

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) TODUOVERVALUE
                      FROM (SELECT acccred.t_account,
                                   (SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = acccred.t_account)
                                      AS fiid,
                                 1
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM dacctrn_dbt trn,
                                   daccount_dbt AccDeb,
                                   daccount_dbt AccCred
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('-Переоценка, ц/б ССПУ_ЦБ')
                                                  AND accdoc.t_catid =
                                                         categ.t_id and accdoc.t_iscommon = 'X')
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT ac.t_account
                                             FROM daccount_dbt ac
                                            WHERE ac.t_balance = '47901'))
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND 1 = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.TODUOVERVALUE = fo7.TODUOVERVALUE + r.TODUOVERVALUE;             


/*   +Переоценка, ц/б СССД_ЦБ   */

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) TODUOVERVALUE
                      FROM (SELECT accdeb.t_account,
                                   (SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = accdeb.t_account)
                                      AS fiid,
                                 2
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM dacctrn_dbt trn,
                                   daccount_dbt AccDeb,
                                   daccount_dbt AccCred
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('+Переоценка, ц/б СССД_ЦБ')
                                                  AND accdoc.t_catid =
                                                         categ.t_id and accdoc.t_iscommon = 'X')
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT ac.t_account
                                             FROM daccount_dbt ac
                                            WHERE ac.t_balance = '47901'))
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND 2 = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.TODUOVERVALUE = fo7.TODUOVERVALUE - r.TODUOVERVALUE;       
 
/*   +Переоценка, ц/б ССПУ_ЦБ   */

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) TODUOVERVALUE
                      FROM (SELECT accdeb.t_account,
                                   (SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = accdeb.t_account)
                                      AS fiid,
                                 1
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM dacctrn_dbt trn,
                                   daccount_dbt AccDeb,
                                   daccount_dbt AccCred
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('+Переоценка, ц/б ССПУ_ЦБ')
                                                  AND accdoc.t_catid =
                                                         categ.t_id and accdoc.t_iscommon = 'X')
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT ac.t_account
                                             FROM daccount_dbt ac
                                            WHERE ac.t_balance = '47901'))
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND 1 = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.TODUOVERVALUE = fo7.TODUOVERVALUE - r.TODUOVERVALUE;
                
/*  End Перевод в ДУ/ ЗПИФ/ ПИФ  */

/*!!!*/
 /*  Перевод ИЗ ДУ. ПОЛЕ FROMDUCOST по Наш портфель */

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) FROMDUCOST
                      FROM (SELECT accdeb.t_account,
                                   (SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account =  accdeb.t_account)
                                      AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account =  accdeb.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM dacctrn_dbt trn,
                                   daccount_dbt AccDeb,
                                   daccount_dbt AccCred
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND  accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Наш портфель ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id and accdoc.t_iscommon = 'X')
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT ac.t_account
                                             FROM daccount_dbt ac
                                            WHERE ac.t_balance = '47901'))
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.FROMDUCost = fo7.FROMDUCost + r.FROMDUCOST;
         
             /*  Перевод в ДУ. ПОЛЕ FROMDOCOST по Наш портфель ПКУ, ц/б */

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) FROMDUCOST
                      FROM (SELECT  accdeb.t_account,
                                   (SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account =  accdeb.t_account)
                                      AS fiid,
                                 7
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM dacctrn_dbt trn,
                                   daccount_dbt AccDeb,
                                   daccount_dbt AccCred
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND  accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Наш портфель ПКУ, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id and accdoc.t_iscommon = 'X')
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT ac.t_account
                                             FROM daccount_dbt ac
                                            WHERE ac.t_balance = '47901'))
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.FROMDUCost = fo7.FROMDUCost + r.FROMDUCOST;



  /*  Перевод ИЗ ДУ. ПОЛЕ FROMDUYNKD по КУ Уплаченный НКД */

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) FROMDUYNKD
                      FROM (SELECT  accdeb.t_account,
                                   (SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account =  accdeb.t_account)
                                      AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account =  accdeb.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM dacctrn_dbt trn,
                                   daccount_dbt AccDeb,
                                   daccount_dbt AccCred
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND  accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Уплаченный НКД')
                                                  AND accdoc.t_catid =
                                                         categ.t_id and accdoc.t_iscommon = 'X')
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT ac.t_account
                                             FROM daccount_dbt ac
                                            WHERE ac.t_balance = '47901'))
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.FROMDUYNKD = fo7.FROMDUYNKD + r.FROMDUYNKD;
         


  /*  Перевод ИЗ ДУ. ПОЛЕ FROMDUBONUS по КУ Премия, ц/б */

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) FROMDUBONUS
                      FROM (SELECT  accdeb.t_account,
                                   (SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account =  accdeb.t_account)
                                      AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account =  accdeb.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM dacctrn_dbt trn,
                                   daccount_dbt AccDeb,
                                   daccount_dbt AccCred
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND  accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Премия, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id and accdoc.t_iscommon = 'X')
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT ac.t_account
                                             FROM daccount_dbt ac
                                            WHERE ac.t_balance = '47901'))
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.FROMDUBONUS = fo7.FROMDUBONUS + r.FROMDUBONUS;       
         
         
  /*  Перевод ИЗ ДУ. ПОЛЕ FROMDUINCOME по КУ  Начисл.ПДД, ц/б */

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) FROMDUINCOME
                      FROM (SELECT  accdeb.t_account,
                                   (SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account =  accdeb.t_account)
                                      AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account =  accdeb.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM dacctrn_dbt trn,
                                   daccount_dbt AccDeb,
                                   daccount_dbt AccCred
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND  accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                                FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc, dmctempl_dbt templ
                                            WHERE categ.t_code IN
                                                     ('Начисл.ПДД, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id and templ.t_catid = categ.t_id and accdoc.t_templnum = templ.t_number and templ.t_value4 = 1 /*купон*/  and accdoc.t_iscommon = 'X' group by accdoc.t_account )
                                                           AND acccred.t_account IN
                                          (SELECT DISTINCT ac.t_account
                                             FROM daccount_dbt ac
                                            WHERE ac.t_balance = '47901'))
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.FROMDUINCOME = fo7.FROMDUINCOME + r.FROMDUINCOME;            
  
  
    /*  Перевод ИЗ ДУ. ПОЛЕ FROMDUDISCOUNT по КУ  Начисл.ПДД, ц/б */

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) FROMDUDISCOUNT
                      FROM (SELECT  accdeb.t_account,
                                   (SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account =  accdeb.t_account)
                                      AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account =  accdeb.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM dacctrn_dbt trn,
                                   daccount_dbt AccDeb,
                                   daccount_dbt AccCred
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND  accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                                FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc, dmctempl_dbt templ
                                            WHERE categ.t_code IN
                                                     ('Начисл.ПДД, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id and templ.t_catid = categ.t_id and accdoc.t_templnum = templ.t_number and templ.t_value4 = 2 /*дисконт*/  and accdoc.t_iscommon = 'X' group by accdoc.t_account )  
                                                     AND acccred.t_account IN
                                          (SELECT DISTINCT ac.t_account
                                             FROM daccount_dbt ac
                                            WHERE ac.t_balance = '47901'))
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.FROMDUDISCOUNT = fo7.FROMDUDISCOUNT + r.FROMDUDISCOUNT;            

  /*  Перевод ИЗ ДУ. ПОЛЕ FROMDUOVERVALUE по переоценке */

/*   -Переоценка, ц/б СССД_ЦБ   */

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) FROMDUOVERVALUE
                      FROM (SELECT accdeb.t_account,
                                   (SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = accdeb.t_account)
                                      AS fiid,
                                 2
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM dacctrn_dbt trn,
                                   daccount_dbt AccDeb,
                                   daccount_dbt AccCred
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('-Переоценка, ц/б СССД_ЦБ')
                                                  AND accdoc.t_catid =
                                                         categ.t_id and accdoc.t_iscommon = 'X')
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT ac.t_account
                                             FROM daccount_dbt ac
                                            WHERE ac.t_balance = '47901'))
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND 2 = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.FROMDUOVERVALUE = fo7.FROMDUOVERVALUE + r.FROMDUOVERVALUE;       
 
/*   -Переоценка, ц/б ССПУ_ЦБ   */

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) FROMDUOVERVALUE
                      FROM (SELECT accdeb.t_account,
                                   (SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = accdeb.t_account)
                                      AS fiid,
                                 1
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM dacctrn_dbt trn,
                                   daccount_dbt AccDeb,
                                   daccount_dbt AccCred
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('-Переоценка, ц/б ССПУ_ЦБ')
                                                  AND accdoc.t_catid =
                                                         categ.t_id and accdoc.t_iscommon = 'X')
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT ac.t_account
                                             FROM daccount_dbt ac
                                            WHERE ac.t_balance = '47901'))
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND 1 = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.FROMDUOVERVALUE = fo7.FROMDUOVERVALUE + r.FROMDUOVERVALUE;             


/*   +Переоценка, ц/б СССД_ЦБ   */

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) FROMDUOVERVALUE
                      FROM (SELECT acccred.t_account,
                                   (SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = acccred.t_account)
                                      AS fiid,
                                 2
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM dacctrn_dbt trn,
                                   daccount_dbt AccDeb,
                                   daccount_dbt AccCred
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('+Переоценка, ц/б СССД_ЦБ')
                                                  AND accdoc.t_catid =
                                                         categ.t_id and accdoc.t_iscommon = 'X')
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT ac.t_account
                                             FROM daccount_dbt ac
                                            WHERE ac.t_balance = '47901'))
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND 2 = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.FROMDUOVERVALUE = fo7.FROMDUOVERVALUE - r.FROMDUOVERVALUE;       
 
/*   +Переоценка, ц/б ССПУ_ЦБ   */

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) FROMDUOVERVALUE
                      FROM (SELECT acccred.t_account,
                                   (SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = acccred.t_account)
                                      AS fiid,
                                 1
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM dacctrn_dbt trn,
                                   daccount_dbt AccDeb,
                                   daccount_dbt AccCred
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('+Переоценка, ц/б ССПУ_ЦБ')
                                                  AND accdoc.t_catid =
                                                         categ.t_id and accdoc.t_iscommon = 'X')
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT ac.t_account
                                             FROM daccount_dbt ac
                                            WHERE ac.t_balance = '47901'))
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND 1 = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.FROMDUOVERVALUE = fo7.FROMDUOVERVALUE - r.FROMDUOVERVALUE;
                
/*  End Перевод ИЗ ДУ/ ЗПИФ/ ПИФ  */


/*  Начисленный КД за отчетный период   */

 MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) PeriodIncome
                      FROM (SELECT accdeb.t_account,acccred.t_account,
                                   (SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = accdeb.t_account and t_iscommon = 'X')
                                      AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = accdeb.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM dacctrn_dbt trn,
                                   daccount_dbt AccDeb,
                                   daccount_dbt AccCred
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND accdeb.t_account IN
                                          (SELECT  accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc, dmctempl_dbt templ
                                            WHERE categ.t_code IN
                                                     ('Начисл.ПДД, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id and templ.t_catid = categ.t_id and accdoc.t_templnum = templ.t_number and templ.t_value4 = 1 /*купон*/  and accdoc.t_iscommon = 'X' group by accdoc.t_account )
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('+%ДЦ/б')
                                                      and accdoc.t_catid = categ.t_id
                                                  )
)
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.PeriodIncome = fo7.PeriodIncome + r.PeriodIncome;


/*Учет исправительных проводок по купону*/
IF(Begdate<=to_date('01.01.2019','dd.mm.yyyy')) Then
    MERGE INTO DMsfo7_DBT fo7
           USING (    SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) PeriodIncome
                      FROM (SELECT accdeb.t_account,acccred.t_account,
                                   (SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = acccred.t_account and t_iscommon = 'X')
                                      AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = acccred.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM dacctrn_dbt trn,
                                   daccount_dbt AccDeb,
                                   daccount_dbt AccCred
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   AND trn.t_ground like 'Исп%'
                                   AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND acccred.t_account IN
                                          (SELECT  accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc, dmctempl_dbt templ
                                            WHERE categ.t_code IN
                                                     ('Начисл.ПДД, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id and templ.t_catid = categ.t_id and accdoc.t_templnum = templ.t_number and templ.t_value4 = 1 /*купон*/  and accdoc.t_iscommon = 'X' group by accdoc.t_account )
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('+%ДЦ/б')
                                                      and accdoc.t_catid = categ.t_id
                                                  )
)
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.PeriodIncome = fo7.PeriodIncome - r.PeriodIncome;

End If;

/*  Начисленный Дисконт  за отчетный период   */

 MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) PeriodDiscount
                      FROM (SELECT accdeb.t_account,acccred.t_account,
                                   (SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = accdeb.t_account and t_iscommon = 'X')
                                      AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = accdeb.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM dacctrn_dbt trn,
                                   daccount_dbt AccDeb,
                                   daccount_dbt AccCred
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND accdeb.t_account IN
                                          (SELECT  accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc, dmctempl_dbt templ
                                            WHERE categ.t_code IN
                                                     ('Начисл.ПДД, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id and templ.t_catid = categ.t_id and accdoc.t_templnum = templ.t_number and templ.t_value4 = 2 /*дисконт*/  and accdoc.t_iscommon = 'X' group by accdoc.t_account )
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('+%ДЦ/б')
                                                      and accdoc.t_catid = categ.t_id
                                                  )
)
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.PeriodDiscount = fo7.PeriodDiscount + r.PeriodDiscount;


/*Учет корректирующих проводок  по дисконту */

IF(Begdate<=to_date('01.01.2019','dd.mm.yyyy')) Then

 MERGE INTO DMsfo7_DBT fo7
           USING (    SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) PeriodDiscount
                      FROM (SELECT accdeb.t_account,acccred.t_account,
                                   (SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = acccred.t_account and t_iscommon = 'X')
                                      AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = acccred.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM dacctrn_dbt trn,
                                   daccount_dbt AccDeb,
                                   daccount_dbt AccCred
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   AND trn.t_ground like 'Кор%'
                                   AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND acccred.t_account IN
                                          (SELECT  accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc, dmctempl_dbt templ
                                            WHERE categ.t_code IN
                                                     ('Начисл.ПДД, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id and templ.t_catid = categ.t_id and accdoc.t_templnum = templ.t_number and templ.t_value4 = 2 /*дисконт*/  and accdoc.t_iscommon = 'X' group by accdoc.t_account )
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('+%ДЦ/б')
                                                      and accdoc.t_catid = categ.t_id
                                                  )
)
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.PeriodDiscount = fo7.PeriodDiscount - r.PeriodDiscount;

/*Учет исправительных проводок по дисконту*/
 MERGE INTO DMsfo7_DBT fo7
           USING (    SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) PeriodDiscount
                      FROM (SELECT accdeb.t_account,acccred.t_account,
                                   (SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = acccred.t_account and t_iscommon = 'X')
                                      AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = acccred.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM dacctrn_dbt trn,
                                   daccount_dbt AccDeb,
                                   daccount_dbt AccCred
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   AND trn.t_ground like 'Исп%'
                                   AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND acccred.t_account IN
                                          (SELECT  accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc, dmctempl_dbt templ
                                            WHERE categ.t_code IN
                                                     ('Начисл.ПДД, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id and templ.t_catid = categ.t_id and accdoc.t_templnum = templ.t_number and templ.t_value4 = 2 /*дисконт*/  and accdoc.t_iscommon = 'X' group by accdoc.t_account )
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('+%ДЦ/б')
                                                      and accdoc.t_catid = categ.t_id
                                                  )
)
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.PeriodDiscount = fo7.PeriodDiscount - r.PeriodDiscount;

End If;         

/* Списанная премия за отчетный период   */

 MERGE INTO DMsfo7_DBT fo7
           USING (with accd as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                          distinct acc.t_accountid,acc.t_account,accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                           where categ.t_code in
                                                     ('-Премия, ц/б')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account),
                        acck as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                            distinct acc.t_accountid,acc.t_account, accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                            where categ.t_code in
                                                     ('Премия, ц/б')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account )
                      SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0)*(-1) PeriodBonus
                     FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX2 )*/ 
                                   distinct trn.t_acctrnid,--accdeb.t_account,acccred.t_account,
                                   (SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = acck.t_account--acccred.t_account
                                        and t_iscommon = 'X')
                                      AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = acck.t_account --acccred.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                             FROM accd,
                                   dacctrn_dbt trn, 
                                   --daccount_dbt AccDeb,
                                   --daccount_dbt AccCred
                                   acck
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   and trn.t_accountid_payer = accd.t_accountid
                                   and trn.t_accountid_receiver = acck.t_accountid  
                                   /*AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND accdeb.t_account IN
                                          (SELECT  accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc, dmctempl_dbt templ
                                            WHERE categ.t_code IN
                                                     ('-Премия, ц/б')  and accdoc.t_catid = categ.t_id
                                                   )
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Премия, ц/б')
                                                      and accdoc.t_catid = categ.t_id
                                                  )*/
)
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.PeriodBonus = fo7.PeriodBonus + r.PeriodBonus;


/*Учет корректирующих проводок  по премии */

IF(Begdate<=to_date('01.01.2019','dd.mm.yyyy')) Then

 MERGE INTO DMsfo7_DBT fo7
           USING (    SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) PeriodBonus
                      FROM (SELECT accdeb.t_account,acccred.t_account,
                                   (SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = accdeb.t_account and t_iscommon = 'X')
                                      AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = accdeb.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM dacctrn_dbt trn,
                                   daccount_dbt AccDeb,
                                   daccount_dbt AccCred
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   AND trn.t_ground like 'Код%'
                                   AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND acccred.t_account IN
                                          (SELECT  accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc, dmctempl_dbt templ
                                            WHERE categ.t_code IN
                                                     ('-Премия, ц/б')
                                                  and accdoc.t_catid = categ.t_id)
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Премия, ц/б')
                                                      and accdoc.t_catid = categ.t_id
                                                  )
)
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.PeriodBonus = fo7.PeriodBonus + r.PeriodBonus;

End IF;

   /*   Полученный КД за отчетный период - в части погашения УНКД  PeriodDrawYnkd  ( со знаком минус) */

/*Уплаченный НКД*/
      MERGE INTO DMsfo7_DBT fo7
           USING (with  acck as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                            distinct acc.t_accountid,acc.t_account, accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                            where categ.t_code in
                                                     ('Уплаченный НКД')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account )
                      SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) PeriodDrawYnkd
                       FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX3 )*/ 
                                   distinct trn.t_acctrnid,
                                    (SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = acck.t_account -- acccred.t_account
                                     )
                                      AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = acck.t_account -- acccred.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                             FROM acck,
                                   dacctrn_dbt trn, 
                                   daccount_dbt AccDeb
                                   --daccount_dbt AccCred
                                   --acck
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   --and trn.t_accountid_payer = accd.t_accountid
                                   and trn.t_accountid_receiver = acck.t_accountid 
                                  AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                  /* AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Уплаченный НКД')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)*/
                                   AND accDeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('+Биржа', '+РасчетыПогаш', '+Расчеты', '+РасчетыПогОР', 
                                                      '-ОД')
                                                  AND accdoc.t_catid =
                                                         categ.t_id))
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.PeriodDrawYnkd = fo7.PeriodDrawYnkd - r.PeriodDrawYnkd;

/*Уплаченный НКД, БПП*/

      MERGE INTO DMsfo7_DBT fo7
           USING ( with acck as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                            distinct acc.t_accountid,acc.t_account, accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                            where categ.t_code in
                                                     ('Уплаченный НКД, БПП')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account )
                       SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) PeriodDrawYnkd
                        FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX3 )*/ 
                                   distinct trn.t_acctrnid,
                                   (SELECT DISTINCT leg.t_pfi
                                      FROM dmcaccdoc_dbt c, ddl_leg_dbt leg
                                     WHERE c.t_account = acck.t_account --acccred.t_account
                                       and c.t_dockind = 176 and leg.t_id = c.t_docid and leg.t_legkind = 0) as fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = acck.t_account --acccred.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                             FROM acck,
                                   dacctrn_dbt trn, 
                                   daccount_dbt AccDeb
                                   --daccount_dbt AccCred
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   and trn.t_accountid_receiver = acck.t_accountid 
                                   AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   /*AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Уплаченный НКД, БПП')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)*/
                                   AND accDeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('+Биржа', '+РасчетыПогаш', '+Расчеты', '+РасчетыПогОР', 
                                                      '-ОД')
                                                  AND accdoc.t_catid =
                                                         categ.t_id))
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.PeriodDrawYnkd = fo7.PeriodDrawYnkd - r.PeriodDrawYnkd;
         
/***************/

/*Уплаченный НКД, Корзина БПП*/

      MERGE INTO DMsfo7_DBT fo7
           USING ( with  acck as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                            distinct acc.t_accountid,acc.t_account, accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                            where categ.t_code in
                                                     ('Уплач. НКД, Корзина БПП')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account
                             and accdoc.t_catnum != 1237 )
                      SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) PeriodDrawYnkd
                     FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX3 )*/ 
                                   distinct trn.t_acctrnid, --acccred.t_account,
                                   (SELECT DISTINCT Ens.t_fiid
                                      FROM dmcaccdoc_dbt c, ddl_tick_ens_dbt Ens
                                     WHERE c.t_account = acck.t_account --acccred.t_account 
                                        and c.t_dockind = 4620 and Ens.t_id = c.t_docid) as fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = acck.t_account --acccred.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                             FROM acck,
                                   dacctrn_dbt trn, 
                                   daccount_dbt AccDeb
                                   --daccount_dbt AccCred
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   and trn.t_accountid_receiver = acck.t_accountid 
                                   AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   /*AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Уплач. НКД, Корзина БПП')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)
                                       AND  not exists(select 1 from dmcaccdoc_dbt where t_catid  in (select t_id from DMCCATEG_DBT t where t_number IN (1237)) -- t_catnum = 1237 
                                                                                      and t_account = acccred.t_account) */
                                   AND accDeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('+Биржа', '+РасчетыПогаш', '+Расчеты', '+РасчетыПогОР', 
                                                      '-ОД')
                                                  AND accdoc.t_catid =
                                                         categ.t_id))
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.PeriodDrawYnkd = fo7.PeriodDrawYnkd - r.PeriodDrawYnkd;

   /*   Полученный КД за отчетный период - в части погашения  купона PeriodDrawIncome */

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) PeriodDrawIncome
                      FROM (SELECT acccred.t_account,
                                   (SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = acccred.t_account)
                                      AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = acccred.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM dacctrn_dbt trn,
                                   daccount_dbt AccDeb,
                                   daccount_dbt AccCred
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND acccred.t_account IN
                                          (SELECT  accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc, dmctempl_dbt templ
                                            WHERE categ.t_code IN
                                                     ('Начисл.ПДД, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id and templ.t_catid = categ.t_id and accdoc.t_templnum = templ.t_number and templ.t_value4 = 1 /*купон*/  and accdoc.t_iscommon = 'X' group by accdoc.t_account ) 
                                                        AND accDeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('+Биржа', '+РасчетыПогаш', '+Расчеты', '+РасчетыПогОР',
                                                      '-ОД')
                                                  AND accdoc.t_catid =
                                                         categ.t_id))
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.PeriodDrawIncome = fo7.PeriodDrawIncome - r.PeriodDrawIncome;
         


   /*   Полученный КД за отчетный период - в части погашения  дисконта PeriodDrawDiscount */

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) PeriodDrawDiscount
                      FROM (SELECT acccred.t_account,
                                   (SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = acccred.t_account)
                                      AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = acccred.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM dacctrn_dbt trn,
                                   daccount_dbt AccDeb,
                                   daccount_dbt AccCred
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND acccred.t_account IN
                                          (SELECT  accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc, dmctempl_dbt templ
                                            WHERE categ.t_code IN
                                                     ('Начисл.ПДД, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id and templ.t_catid = categ.t_id and accdoc.t_templnum = templ.t_number and templ.t_value4 = 2 /*дисконт*/  and accdoc.t_iscommon = 'X' group by accdoc.t_account ) 
                                                        AND accDeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('+Биржа', '+РасчетыПогаш', '+Расчеты', '+РасчетыПогОР',
                                                      '-ОД')
                                                  AND accdoc.t_catid =
                                                         categ.t_id))
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.PeriodDrawDiscount = fo7.PeriodDrawDiscount - r.PeriodDrawDiscount;
         

/*   +-Переоценка, ц/б СССД_ЦБ   PERIODOVERVALUE  по дебету с плюсом*/

      MERGE INTO DMsfo7_DBT fo7
           USING ( with accd as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                          distinct acc.t_accountid,acc.t_account,accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                           where categ.t_code in
                                                     ('-Переоценка, ц/б СССД_ЦБ', '+Переоценка, ц/б СССД_ЦБ')
                             and accdoc.t_iscommon = 'X'
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account),
                        acck as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                            distinct acc.t_accountid,acc.t_account, accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                            where categ.t_code in
                                                     ('+МаржаП, ц/б', '-МаржаП, ц/б', '+ПО ДК 2014','-ПО ДК 2014','-ПО ДК 2014', '+ПО ДК','-ПО ДК')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account )
                      SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) PERIODOVERVALUE
                      FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX2 )*/ 
                                   distinct trn.t_acctrnid,
                                   /*(SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = accdeb.t_account)*/
                                   accd.t_fiid   AS fiid,
                                 2
                                      AS portfnum,
                                   trn.t_sum_natcur
                             FROM accd,
                                   dacctrn_dbt trn, 
                                   --daccount_dbt AccDeb,
                                   --daccount_dbt AccCred
                                   acck
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   and trn.t_accountid_payer = accd.t_accountid
                                   and trn.t_accountid_receiver = acck.t_accountid 
                                   /*AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('-Переоценка, ц/б СССД_ЦБ', '+Переоценка, ц/б СССД_ЦБ')
                                                  AND accdoc.t_catid =
                                                         categ.t_id and accdoc.t_iscommon = 'X')
                                   AND acccred.t_account IN
                                         (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('+МаржаП, ц/б', '-МаржаП, ц/б', '+ПО ДК 2014','-ПО ДК 2014','-ПО ДК 2014', '+ПО ДК','-ПО ДК')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)*/)
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND 2 = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.PERIODOVERVALUE = fo7.PERIODOVERVALUE + r.PERIODOVERVALUE;     

/*   +-Переоценка, ц/б ССПУ_ЦБ  PERIODOVERVALUE  по дебету с плюсом*/

/*

10199516.82

*/

      MERGE INTO DMsfo7_DBT fo7
           USING ( with accd as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                          distinct acc.t_accountid,acc.t_account,accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                           where categ.t_code in
                                                     ('+Переоценка, ц/б ССПУ_ЦБ', '-Переоценка, ц/б ССПУ_ЦБ')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account),
                        acck as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                            distinct acc.t_accountid,acc.t_account, accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                            where categ.t_code in
                                                     ('+МаржаП, ц/б', '-МаржаП, ц/б', '+ПО ДК 2014','-ПО ДК 2014')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account )
                      SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) PERIODOVERVALUE
                        FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX2 )*/ 
                                   distinct trn.t_acctrnid, accd.t_account,
                                   /*(SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = acccred.t_account)*/
                                   accd.t_fiid   AS fiid,
                                 1
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM accd,
                                   dacctrn_dbt trn, 
                                   --daccount_dbt AccDeb,
                                   --daccount_dbt AccCred
                                   acck
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   and trn.t_accountid_payer = accd.t_accountid
                                   and trn.t_accountid_receiver = acck.t_accountid 
                                  /*AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('+Переоценка, ц/б ССПУ_ЦБ', '-Переоценка, ц/б ССПУ_ЦБ')
                                                  AND accdoc.t_catid =
                                                         categ.t_id\* and accdoc.t_iscommon = 'X'*\)
                                   AND acccred.t_account IN
                                         (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('+МаржаП, ц/б', '-МаржаП, ц/б', '+ПО ДК 2014','-ПО ДК 2014')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)*/ )
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND 1 = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.PERIODOVERVALUE = fo7.PERIODOVERVALUE + r.PERIODOVERVALUE;     

/*   +-Переоценка, ц/б СССД_ЦБ PERIODOVERVALUE  по кредиту с минусом*/

      MERGE INTO DMsfo7_DBT fo7
           USING ( with accd as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                          distinct acc.t_accountid,acc.t_account,accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                           where categ.t_code in
                                                     ('+МаржаП, ц/б', '-МаржаП, ц/б', '+ПО ДК 2014','-ПО ДК 2014', '+ПО ДК','-ПО ДК')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account),
                        acck as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                            distinct acc.t_accountid,acc.t_account, accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                            where categ.t_code in
                                                     ('+Переоценка, ц/б СССД_ЦБ', '-Переоценка, ц/б СССД_ЦБ')
                             and accdoc.t_catid = categ.t_id
                             and accdoc.t_iscommon = 'X'
                             and acc.t_account = accdoc.t_account )
                     SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) PERIODOVERVALUE
                       FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX3 )*/ 
                                   distinct trn.t_acctrnid,
                                   /*(SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = acccred.t_account)*/
                                   acck.t_fiid   AS fiid,
                                 2
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM acck,
                                   dacctrn_dbt trn, 
                                   --daccount_dbt AccDeb,
                                   --daccount_dbt AccCred
                                   accd
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   and trn.t_accountid_payer = accd.t_accountid
                                   and trn.t_accountid_receiver = acck.t_accountid 
                                   /*AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('+Переоценка, ц/б СССД_ЦБ', '-Переоценка, ц/б СССД_ЦБ')
                                                  AND accdoc.t_catid =
                                                         categ.t_id and accdoc.t_iscommon = 'X')
                                   AND accdeb.t_account IN
                                         (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('+МаржаП, ц/б', '-МаржаП, ц/б', '+ПО ДК 2014','-ПО ДК 2014', '+ПО ДК','-ПО ДК')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)*/)
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND 2 = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.PERIODOVERVALUE = fo7.PERIODOVERVALUE - r.PERIODOVERVALUE;   


/*   +-Переоценка, ц/б ССПУ_ЦБ  PERIODOVERVALUE  по кредиту с минусом*/

      MERGE INTO DMsfo7_DBT fo7
           USING (with acck as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                            distinct acc.t_accountid,acc.t_account, accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                            where categ.t_code in
                                                     ('+Переоценка, ц/б ССПУ_ЦБ', '-Переоценка, ц/б ССПУ_ЦБ')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account )
                      SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) PERIODOVERVALUE
                        FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX3 )*/ 
                                   distinct trn.t_acctrnid,
                                   /*(SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = acccred.t_account)*/
                                   acck.t_fiid   AS fiid,
                                 1
                                      AS portfnum,
                                   trn.t_sum_natcur
                             FROM acck,
                                   dacctrn_dbt trn, 
                                   daccount_dbt AccDeb
                                   --daccount_dbt AccCred
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   and trn.t_accountid_receiver = acck.t_accountid 
                                   AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   /*AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('+Переоценка, ц/б ССПУ_ЦБ', '-Переоценка, ц/б ССПУ_ЦБ')
                                                  AND accdoc.t_catid =
                                                         categ.t_id \*and accdoc.t_iscommon = 'X'*\)*/
                                   AND accdeb.t_account IN
                                         (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('+МаржаП, ц/б', '-МаржаП, ц/б', '+ПО ДК 2014','-ПО ДК 2014')
                                                  AND accdoc.t_catid =
                                                         categ.t_id))
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND 1 = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.PERIODOVERVALUE = fo7.PERIODOVERVALUE - r.PERIODOVERVALUE;     


/*  Списание при обесценении. Номинал. ObescCost  */

/* ПКУ */

      MERGE INTO DMsfo7_DBT fo7
           USING ( with acck as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                            distinct acc.t_accountid,acc.t_account, accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                            where categ.t_code in
                                                     ('Наш портфель ПКУ, ц/б')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account )
                      SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) ObescCost
                      FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX2 )*/ 
                                   distinct trn.t_acctrnid,
                                   /*(SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = acccred.t_account)*/
                                   acck.t_fiid   AS fiid,
                                   7 AS portfnum,
                                   trn.t_sum_natcur
                             FROM acck,
                                   dacctrn_dbt trn, 
                                   daccount_dbt AccDeb
                                   --daccount_dbt AccCred
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   and trn.t_accountid_receiver = acck.t_accountid 
                                   AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   /*AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Наш портфель ПКУ, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)*/
                                    AND accdeb.t_account IN
                                          (SELECT DISTINCT ac.t_account
                                             FROM daccount_dbt ac
                                            WHERE ac.t_balance in( '50507', '50427', '60105', '50908', '10630', '50719' )
                                            ))
                               GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.ObescCost = fo7.ObescCost + r.ObescCost;



/* Наш портфель */

      MERGE INTO DMsfo7_DBT fo7
           USING ( with acck as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                            distinct acc.t_accountid,acc.t_account, accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                            where categ.t_code in
                                                     ('Наш портфель ц/б')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account )
                       SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) ObescCost
                       FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX2 )*/ 
                                   distinct trn.t_acctrnid,
                                   /*(SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = acccred.t_account)*/
                                   acck.t_fiid   AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = acck.t_account --acccred.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                             FROM acck,
                                   dacctrn_dbt trn, 
                                   daccount_dbt AccDeb
                                   --daccount_dbt AccCred
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   and trn.t_accountid_receiver = acck.t_accountid 
                                   AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   /*AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Наш портфель ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)*/
                                    AND accdeb.t_account IN
                                          (SELECT DISTINCT ac.t_account
                                             FROM daccount_dbt ac
                                            WHERE ac.t_balance in( '50507', '50427', '60105', '50908', '10630', '50719' )
                                            ))
                               GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.ObescCost = fo7.ObescCost + r.ObescCost;


/*  Списание при обесценении. Уплаченный НКД . ObescYnkd  */

      MERGE INTO DMsfo7_DBT fo7
           USING ( with acck as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                            distinct acc.t_accountid,acc.t_account, accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                            where categ.t_code in
                                                     ('Уплаченный НКД')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account )
                       SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) ObescYnkd
                      FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX3 )*/ 
                                   distinct trn.t_acctrnid,
                                   /*(SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = acccred.t_account)*/
                                   acck.t_fiid   AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = acck.t_account -- acccred.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                             FROM acck,
                                   dacctrn_dbt trn, 
                                   daccount_dbt AccDeb
                                   --daccount_dbt AccCred
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   and trn.t_accountid_receiver = acck.t_accountid 
                                   AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   /*AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Уплаченный НКД')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)*/
                                    AND accdeb.t_account IN
                                          (SELECT DISTINCT ac.t_account
                                             FROM daccount_dbt ac
                                            WHERE ac.t_balance in( '50507', '50427', '60105', '50908', '10630', '50719' )
                                            ))
                               GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.ObescYnkd = fo7.ObescYnkd + r.ObescYnkd;


/*  Списание при обесценении. Премия . ObescBonus  */

      MERGE INTO DMsfo7_DBT fo7
           USING ( with acck as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                            distinct acc.t_accountid,acc.t_account, accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                            where categ.t_code in
                                                     ('Премия, ц/б')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account )
                       SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) ObescBonus
                      FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX3 )*/ 
                                   distinct trn.t_acctrnid,
                                   /*(SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = acccred.t_account)*/
                                   acck.t_fiid   AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = acck.t_account --acccred.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                             FROM acck,
                                   dacctrn_dbt trn, 
                                   daccount_dbt AccDeb
                                   --daccount_dbt AccCred
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   and trn.t_accountid_receiver = acck.t_accountid 
                                   AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   /*AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Премия, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)*/
                                    AND accdeb.t_account IN
                                          (SELECT DISTINCT ac.t_account
                                             FROM daccount_dbt ac
                                            WHERE ac.t_balance in( '50507', '50427', '60105', '50908', '10630', '50719' )
                                            ))
                               GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.ObescBonus = fo7.ObescBonus + r.ObescBonus;

/*  Списание при обесценении. Начисленный купон . ObescIncome  */

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) ObescIncome
                      FROM (SELECT acccred.t_account,
                                   (SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = acccred.t_account)
                                      AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = acccred.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM dacctrn_dbt trn,
                                   daccount_dbt AccDeb,
                                   daccount_dbt AccCred
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND acccred.t_account IN
                                          (SELECT  accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc, dmctempl_dbt templ
                                            WHERE categ.t_code IN
                                                     ('Начисл.ПДД, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id and templ.t_catid = categ.t_id and accdoc.t_templnum = templ.t_number and templ.t_value4 = 1 /*купон*/  and accdoc.t_iscommon = 'X' group by accdoc.t_account )
                                                  AND accdeb.t_account IN
                                          (SELECT DISTINCT ac.t_account
                                             FROM daccount_dbt ac
                                            WHERE ac.t_balance in( '50507', '50427', '60105', '50908', '10630', '50719' )
                                            ))
                               GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.ObescIncome = fo7.ObescIncome + r.ObescIncome;


/*  Списание при обесценении. Начисленный дисконт . ObescDiscount  */

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) ObescDiscount
                      FROM (SELECT acccred.t_account,
                                   (SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = acccred.t_account)
                                      AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = acccred.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM dacctrn_dbt trn,
                                   daccount_dbt AccDeb,
                                   daccount_dbt AccCred
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND acccred.t_account IN
                                          (SELECT  accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc, dmctempl_dbt templ
                                            WHERE categ.t_code IN
                                                     ('Начисл.ПДД, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id and templ.t_catid = categ.t_id and accdoc.t_templnum = templ.t_number and templ.t_value4 = 2 /*дисконт*/  and accdoc.t_iscommon = 'X' group by accdoc.t_account )
                                                  AND accdeb.t_account IN
                                          (SELECT DISTINCT ac.t_account
                                             FROM daccount_dbt ac
                                            WHERE ac.t_balance in( '50507', '50427', '60105', '50908', '10630', '50719' )
                                            ))
                               GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.ObescDiscount = fo7.ObescDiscount + r.ObescDiscount;

/*  Финансовый результат от продажи (ОПУ)  SALEFINREZ   ( с минусом ) */

  
      MERGE INTO DMsfo7_DBT fo7
           USING ( with accd as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                          distinct acc.t_accountid,acc.t_account,accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                           where categ.t_code in
                                                     ('+Маржа, ц/б', '-Маржа, ц/б')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account),
                        acck as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                            distinct acc.t_accountid,acc.t_account, accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                            where categ.t_code in
                                                     ('Реализация, ц/б')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account )
                       SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) SALEFINREZ
                       FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX2 )*/ 
                                   distinct trn.t_acctrnid,
                                   /*(SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = acccred.t_account)*/
                                   acck.t_fiid   AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc,
                                           dmccateg_dbt categ -- Golovkin 25.10.2021 ID : 534663
                                     WHERE accdoc.t_account = accd.t_account --accdeb.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum = templ.t_number
                                           AND accdoc.t_catid = categ.t_id
                                           AND categ.t_code = 'Реализация, ц/б')
                                      AS portfnum,
                                   trn.t_sum_natcur
                             FROM accd,
                                   dacctrn_dbt trn, 
                                   --daccount_dbt AccDeb,
                                   --daccount_dbt AccCred
                                   acck
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   and trn.t_accountid_payer = accd.t_accountid
                                   and trn.t_accountid_receiver = acck.t_accountid 
                                   /*AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Реализация, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)
                                    AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('+Маржа, ц/б', '-Маржа, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id
                                            )*/)
                               GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.SALEFINREZ = fo7.SALEFINREZ - r.SALEFINREZ;  


/*  Финансовый результат от продажи (ОПУ)  SALEFINREZ   ( с плюсом ) */

  
      MERGE INTO DMsfo7_DBT fo7
           USING ( with accd as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                          distinct acc.t_accountid,acc.t_account,accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                           where categ.t_code in
                                                     ('Реализация, ц/б')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account),
                        acck as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                            distinct acc.t_accountid,acc.t_account, accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                            where categ.t_code in
                                                     ('+Маржа, ц/б', '-Маржа, ц/б')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account )
                      SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) SALEFINREZ
                       FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX2 )*/ 
                                   distinct trn.t_acctrnid,
                                   /*(SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = accdeb.t_account)*/
                                   accd.t_fiid   AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = acck.t_account --acccred.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM accd,
                                   dacctrn_dbt trn, 
                                   --daccount_dbt AccDeb,
                                   --daccount_dbt AccCred
                                   acck
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   and trn.t_accountid_payer = accd.t_accountid
                                   and trn.t_accountid_receiver = acck.t_accountid 
                                  /*AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Реализация, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)
                                    AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('+Маржа, ц/б', '-Маржа, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id
                                            )*/)
                               GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.SALEFINREZ = fo7.SALEFINREZ + r.SALEFINREZ;  


/* Продажа в части номинала SALECOST*/

  MERGE INTO DMsfo7_DBT fo7
           USING ( with accd as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                          distinct acc.t_accountid,acc.t_account,accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                           where categ.t_code in
                                                     ('Реализация, ц/б')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account),
                        acck as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                            distinct acc.t_accountid,acc.t_account, accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                            where categ.t_code in
                                                     ('Наш портфель ц/б')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account )
                      SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) SALECOST
                      FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX2 )*/ 
                                   distinct trn.t_acctrnid,
                                   /*(SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = acccred.t_account)*/
                                   acck.t_fiid   AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = acck.t_account -- acccred.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                             FROM accd,
                                   dacctrn_dbt trn, 
                                   --daccount_dbt AccDeb,
                                   --daccount_dbt AccCred
                                   acck
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   and trn.t_accountid_payer = accd.t_accountid
                                   and trn.t_accountid_receiver = acck.t_accountid 
                                   /*AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Реализация, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)
                                    AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Наш портфель ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id
                                            )*/)
                               GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.SALECOST = fo7.SALECOST + r.SALECOST;  


/* Продажа в части уплаченного НКД SALEYNKD*/

  MERGE INTO DMsfo7_DBT fo7
           USING ( with accd as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                          distinct acc.t_accountid,acc.t_account,accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                           where categ.t_code in
                                                     ('Реализация, ц/б')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account),
                        acck as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                            distinct acc.t_accountid,acc.t_account, accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                            where categ.t_code in
                                                     ('Уплаченный НКД')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account )
                       SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) SALEYNKD
                       FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX2 )*/ 
                                   distinct trn.t_acctrnid,
                                   /*(SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = acccred.t_account)*/
                                   acck.t_fiid   AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = acck.t_account --acccred.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM accd,
                                   dacctrn_dbt trn, 
                                   --daccount_dbt AccDeb,
                                   --daccount_dbt AccCred
                                   acck
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   and trn.t_accountid_payer = accd.t_accountid
                                   and trn.t_accountid_receiver = acck.t_accountid 
                                 /* AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Реализация, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)
                                    AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Уплаченный НКД')
                                                  AND accdoc.t_catid =
                                                         categ.t_id
                                            )*/)
                               GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.SALEYNKD = fo7.SALEYNKD + r.SALEYNKD;  


/* Продажа в части начисленного НКД SALEINCOME */

  MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) SALEINCOME
                      FROM (SELECT acccred.t_account,
                                   (SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = acccred.t_account)
                                      AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = acccred.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM dacctrn_dbt trn,
                                   daccount_dbt AccDeb,
                                   daccount_dbt AccCred
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Реализация, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)
                                    AND acccred.t_account IN
                                          (SELECT  accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc, dmctempl_dbt templ
                                            WHERE categ.t_code IN
                                                     ('Начисл.ПДД, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id and templ.t_catid = categ.t_id and accdoc.t_templnum = templ.t_number and templ.t_value4 = 1 /*купон*/  and accdoc.t_iscommon = 'X' group by accdoc.t_account )                                            
                                                         )
                               GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.SALEINCOME = fo7.SALEINCOME + r.SALEINCOME; 


/* Продажа в части начисленного дисконта SALEDISCOUNT */

  MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) SALEDISCOUNT
                      FROM (SELECT acccred.t_account,
                                   (SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = acccred.t_account)
                                      AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = acccred.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM dacctrn_dbt trn,
                                   daccount_dbt AccDeb,
                                   daccount_dbt AccCred
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Реализация, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)
                                    AND acccred.t_account IN
                                          (SELECT  accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc, dmctempl_dbt templ
                                            WHERE categ.t_code IN
                                                     ('Начисл.ПДД, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id and templ.t_catid = categ.t_id and accdoc.t_templnum = templ.t_number and templ.t_value4 = 2 /*дисконт*/  and accdoc.t_iscommon = 'X' group by accdoc.t_account )                                            
                                                         )
                               GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.SALEDISCOUNT = fo7.SALEDISCOUNT + r.SALEDISCOUNT; 


/* Продажа в части премии SALEBONUS*/

  MERGE INTO DMsfo7_DBT fo7
           USING ( with accd as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                          distinct acc.t_accountid,acc.t_account,accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                           where categ.t_code in
                                                     ('Реализация, ц/б')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account),
                        acck as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                            distinct acc.t_accountid,acc.t_account, accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                            where categ.t_code in
                                                     ('Премия, ц/б')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account )
                     SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) SALEBONUS
                        FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX2 )*/ 
                                   distinct trn.t_acctrnid,
                                   /*(SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = acccred.t_account)*/
                                   acck.t_fiid   AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = acck.t_account -- acccred.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                             FROM accd,
                                   dacctrn_dbt trn, 
                                   --daccount_dbt AccDeb,
                                   --daccount_dbt AccCred
                                   acck
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   and trn.t_accountid_payer = accd.t_accountid
                                   and trn.t_accountid_receiver = acck.t_accountid 
                                  /*AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Реализация, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)
                                    AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Премия, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id
                                            )*/)
                               GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.SALEBONUS = fo7.SALEBONUS + r.SALEBONUS;  


/* Продажа в части переоценки SALEOVERVALUE*/

/*  -+Переоценка, ц/б СССД_ЦБ  с плюсом */


  MERGE INTO DMsfo7_DBT fo7
           USING ( with accd as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                          distinct acc.t_accountid,acc.t_account,accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                           where categ.t_code in
                                                     ('Реализация, ц/б')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account),
                        acck as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                            distinct acc.t_accountid,acc.t_account, accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                            where categ.t_code in
                                                     ('+Переоценка, ц/б СССД_ЦБ', '-Переоценка, ц/б СССД_ЦБ')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account )
                       SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) SALEOVERVALUE
                     FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX2 )*/ 
                                   distinct trn.t_acctrnid,
                                   /*(SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = acccred.t_account)*/
                                   acck.t_fiid   AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = acck.t_account -- acccred.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                            FROM accd,
                                   dacctrn_dbt trn, 
                                   --daccount_dbt AccDeb,
                                   --daccount_dbt AccCred
                                   acck
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   and trn.t_accountid_payer = accd.t_accountid
                                   and trn.t_accountid_receiver = acck.t_accountid 
                                 /*AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Реализация, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)
                                    AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('+Переоценка, ц/б СССД_ЦБ', '-Переоценка, ц/б СССД_ЦБ')
                                                  AND accdoc.t_catid =
                                                         categ.t_id
                                            )*/)
                               GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.SALEOVERVALUE = fo7.SALEOVERVALUE + r.SALEOVERVALUE;  

/*  -+Переоценка, ц/б СССД_ЦБ  с минусом */


  MERGE INTO DMsfo7_DBT fo7
           USING ( with accd as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                          distinct acc.t_accountid,acc.t_account,accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                           where categ.t_code in
                                                     ('+Переоценка, ц/б СССД_ЦБ', '-Переоценка, ц/б СССД_ЦБ')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account),
                        acck as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                            distinct acc.t_accountid,acc.t_account, accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                            where categ.t_code in
                                                     ('Реализация, ц/б')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account )
                       SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) SALEOVERVALUE
                      FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX2 )*/ 
                                   distinct trn.t_acctrnid,
                                   /*(SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = accdeb.t_account)*/
                                   accd.t_fiid   AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = accd.t_account -- accdeb.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                             FROM accd,
                                   dacctrn_dbt trn, 
                                   --daccount_dbt AccDeb,
                                   --daccount_dbt AccCred
                                   acck
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   and trn.t_accountid_payer = accd.t_accountid
                                   and trn.t_accountid_receiver = acck.t_accountid 
                                  /*AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Реализация, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)
                                    AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('+Переоценка, ц/б СССД_ЦБ', '-Переоценка, ц/б СССД_ЦБ')
                                                  AND accdoc.t_catid =
                                                         categ.t_id
                                            )*/)
                               GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.SALEOVERVALUE = fo7.SALEOVERVALUE - r.SALEOVERVALUE;  


/***/

/*  -+Переоценка, ц/б ССПУ_ЦБ с плюсом */


  MERGE INTO DMsfo7_DBT fo7
           USING ( with accd as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                          distinct acc.t_accountid,acc.t_account,accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                           where categ.t_code in
                                                     ('Реализация, ц/б')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account),
                        acck as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                            distinct acc.t_accountid,acc.t_account, accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                            where categ.t_code in
                                                     ('+Переоценка, ц/б ССПУ_ЦБ', '-Переоценка, ц/б ССПУ_ЦБ')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account )
                      SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) SALEOVERVALUE
                       FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX2 )*/ 
                                   distinct trn.t_acctrnid,
                                   /*(SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = acccred.t_account)*/
                                   acck.t_fiid   AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = acck.t_account --acccred.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                             FROM accd,
                                   dacctrn_dbt trn, 
                                   --daccount_dbt AccDeb,
                                   --daccount_dbt AccCred
                                   acck
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                    and trn.t_accountid_payer = accd.t_accountid
                                   and trn.t_accountid_receiver = acck.t_accountid 
                                  /*AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Реализация, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)
                                    AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('+Переоценка, ц/б ССПУ_ЦБ', '-Переоценка, ц/б ССПУ_ЦБ')
                                                  AND accdoc.t_catid =
                                                         categ.t_id
                                            )*/)
                               GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.SALEOVERVALUE = fo7.SALEOVERVALUE + r.SALEOVERVALUE;  

/*  -+Переоценка, ц/б ССПУ_ЦБ с минусом */


  MERGE INTO DMsfo7_DBT fo7
           USING ( with accd as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                          distinct acc.t_accountid,acc.t_account,accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                           where categ.t_code in
                                                     ('+Переоценка, ц/б ССПУ_ЦБ', '-Переоценка, ц/б ССПУ_ЦБ')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account),
                        acck as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                            distinct acc.t_accountid,acc.t_account, accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                            where categ.t_code in
                                                     ('Реализация, ц/б')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account )
                       SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) SALEOVERVALUE
                       FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX2 )*/ 
                                   distinct trn.t_acctrnid,
                                   /*(SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = accdeb.t_account)*/
                                   accd.t_fiid   AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = accd.t_account --accdeb.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                             FROM accd,
                                   dacctrn_dbt trn, 
                                   --daccount_dbt AccDeb,
                                   --daccount_dbt AccCred
                                   acck
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                    and trn.t_accountid_payer = accd.t_accountid
                                   and trn.t_accountid_receiver = acck.t_accountid 
                                 /*AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Реализация, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)
                                    AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('+Переоценка, ц/б ССПУ_ЦБ', '-Переоценка, ц/б ССПУ_ЦБ')
                                                  AND accdoc.t_catid =
                                                         categ.t_id
                                            )*/)
                               GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.SALEOVERVALUE = fo7.SALEOVERVALUE - r.SALEOVERVALUE;  


/*  Продажа в части корректировок SALEEPS*/

/* С плюсом*/

  MERGE INTO DMsfo7_DBT fo7
           USING ( with accd as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                          distinct acc.t_accountid,acc.t_account,accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                           where categ.t_code in
                                                     ('Реализация, ц/б')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account),
                        acck as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                            distinct acc.t_accountid,acc.t_account, accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                            where categ.t_code in
                                                     ('-Корректировка, ц/б', '+Корректировка, ц/б')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account )
                      SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) SALEEPS
                      FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX2 )*/ 
                                   distinct trn.t_acctrnid,
                                   /*(SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = acccred.t_account)*/
                                   acck.t_fiid   AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = acck.t_account -- acccred.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM accd,
                                   dacctrn_dbt trn, 
                                   --daccount_dbt AccDeb,
                                   --daccount_dbt AccCred
                                   acck
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   and trn.t_accountid_payer = accd.t_accountid
                                   and trn.t_accountid_receiver = acck.t_accountid 
                                   /*AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Реализация, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)
                                    AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('-Корректировка, ц/б', '+Корректировка, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id
                                            )*/)
                               GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.SALEEPS = fo7.SALEEPS + r.SALEEPS;  


/* с минусом ( Пока тоже с плючом, чтобы было как в отчете банка). Но это будет уточнено*/


  MERGE INTO DMsfo7_DBT fo7
           USING ( with accd as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                          distinct acc.t_accountid,acc.t_account,accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                           where categ.t_code in
                                                     ('-Корректировка, ц/б', '+Корректировка, ц/б')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account),
                        acck as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                            distinct acc.t_accountid,acc.t_account, accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                            where categ.t_code in
                                                     ('Реализация, ц/б')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account )
                       SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) SALEEPS
                       FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX2 )*/ 
                                   distinct trn.t_acctrnid,
                                   /*(SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = accdeb.t_account)*/
                                   accd.t_fiid   AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = accd.t_account -- accdeb.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                            FROM accd,
                                   dacctrn_dbt trn, 
                                   --daccount_dbt AccDeb,
                                   --daccount_dbt AccCred
                                   acck
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   and trn.t_accountid_payer = accd.t_accountid
                                   and trn.t_accountid_receiver = acck.t_accountid 
                                  /*AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Реализация, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)
                                    AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('-Корректировка, ц/б', '+Корректировка, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id
                                            )*/)
                               GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.SALEEPS = fo7.SALEEPS +/*-*/ r.SALEEPS;  


/*Портфель на конец периода endportf */

update DMsfo7_DBT o7 set endportf = begportfcode;

update DMsfo7_DBT o7 set o7.endportf = (select to_char(t_destportofolio) from ddl_comm_dbt where t_dockind = 105 and 
t_commdate >= begdate and t_commdate <= enddate and t_fiid = o7.fiid) where exists(select t_destportofolio from ddl_comm_dbt where t_dockind = 105 and 
t_commdate >= begdate and t_commdate <= enddate and t_fiid = o7.fiid);


      /* Остатки по КУ Наш портфель ц/б  на конец периода ENDCOST*/

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT                              /*accdoc.t_account, */
                          accdoc.t_fiid,
                           templ.t_value1,
                           NVL (SUM (abs(ROUND (RSI_Rsb_FIInstr.
                                             ConvSum (RSI_RSB_ACCOUNT.
                                                       RestAC (
                                                         accdoc.t_account,
                                                         accdoc.t_currency,
                                                         EndDate,
                                                         1,
                                                         NULL),
                                                      accdoc.t_currency,
                                                      0,
                                                      EndDate), 2))),
                                0)
                              ENDCOST
                      FROM dmcaccdoc_dbt accdoc,
                           dmctempl_dbt templ,
                           dmccateg_dbt categ
                     WHERE     accdoc.t_catid = categ.t_id
                           AND categ.t_code IN ('Наш портфель ц/б')
                           AND accdoc.t_iscommon = 'X'
                           AND templ.t_catid = accdoc.t_catid
                           AND templ.t_number = accdoc.t_templnum
                  GROUP BY                              /*accdoc.t_account, */
                          accdoc.t_fiid,
                           templ.t_value1) r
              ON (r.t_fiid = fo7.fiid AND r.t_value1 = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.ENDCOST = fo7.ENDCOST + r.ENDCOST;


      /* Остатки по КУ Наш портфель ПКУ, ц/б на конец периода ENDCOST*/

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT                              /*accdoc.t_account, */
                          accdoc.t_fiid,
                           7,
                           NVL (SUM (abs(ROUND (RSI_Rsb_FIInstr.
                                             ConvSum (RSI_RSB_ACCOUNT.
                                                       RestAC (
                                                         accdoc.t_account,
                                                         accdoc.t_currency,
                                                         EndDate,
                                                         1,
                                                         NULL),
                                                      accdoc.t_currency,
                                                      0,
                                                      EndDate), 2))),
                                0)
                              ENDCOST
                      FROM dmcaccdoc_dbt accdoc,
                           dmctempl_dbt templ,
                           dmccateg_dbt categ
                     WHERE     accdoc.t_catid = categ.t_id
                           AND categ.t_code IN ('Наш портфель ПКУ, ц/б')
                           AND accdoc.t_iscommon = 'X'
                           AND templ.t_catid = accdoc.t_catid
                           AND templ.t_number = accdoc.t_templnum
                  GROUP BY                              /*accdoc.t_account, */
                          accdoc.t_fiid,
                           7) r
              ON (r.t_fiid = fo7.fiid AND 7 = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.ENDCOST = fo7.ENDCOST + r.ENDCOST;


      /* К остатку портфельных счетов добавляем остатки по счетам в репо ( БПП) */

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT leg.t_pfi,
                           templ.t_value1,
                           NVL (SUM (abs(ROUND (RSI_Rsb_FIInstr.
                                             ConvSum (RSI_RSB_ACCOUNT.
                                                       RestAC (
                                                         accdoc.t_account,
                                                         accdoc.t_currency,
                                                         EndDate,
                                                         1,
                                                         NULL),
                                                      accdoc.t_currency,
                                                      0,
                                                      EndDate), 2))),
                                0)
                              ENDCOST
                      FROM dmcaccdoc_dbt accdoc,
                           ddl_leg_dbt leg,
                           dmctempl_dbt templ
                     WHERE     accdoc.t_catid  in (select t_id from DMCCATEG_DBT t where t_number IN ( 233, 1237, 1298, 1299)) --t_catnum IN (233, 1237, 1298, 1299)
                           AND accdoc.t_dockind = 176
                           AND leg.t_id = accdoc.t_docid
                           AND templ.t_catid = accdoc.t_catid
                           AND templ.t_number = accdoc.t_templnum
                  GROUP BY leg.t_pfi, templ.t_value1) r
              ON (r.t_pfi = fo7.fiid AND r.t_value1 = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.ENDCOST = fo7.ENDCOST + r.ENDCOST;
         
         
/********************/         

/* К остатку портфельных счетов добавляем остатки по счетам в Корзине репо ( БПП) */

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT Ens.t_fiid,
                           templ.t_value1,
                           NVL (SUM (abs(ROUND (RSI_Rsb_FIInstr.
                                             ConvSum (RSI_RSB_ACCOUNT.
                                                       RestAC (
                                                         accdoc.t_account,
                                                         accdoc.t_currency,
                                                         EndDate,
                                                         1,
                                                         NULL),
                                                      accdoc.t_currency,
                                                      0,
                                                      EndDate), 2))),
                                0)
                              ENDCOST
                      FROM dmcaccdoc_dbt accdoc,
                           ddl_tick_ens_dbt ens,
                           dmctempl_dbt templ
                     WHERE     accdoc.t_catid  in (select t_id from DMCCATEG_DBT t where t_number IN ( 1237)) -- t_catnum IN ( 1237)
                           AND accdoc.t_dockind = 4620
                           AND ens.t_id = accdoc.t_docid
                           AND templ.t_catid = accdoc.t_catid
                           AND templ.t_number = accdoc.t_templnum
                  GROUP BY ens.t_fiid, templ.t_value1) r
              ON (r.t_fiid = fo7.fiid AND r.t_value1 = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.ENDCOST = fo7.ENDCOST + r.ENDCOST;

         

/*****************/

      /* Остатки по КУ Уплаченный НКД  на конец периода ENDYNKD */

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT                              /*accdoc.t_account, */
                          accdoc.t_fiid,
                           templ.t_value1,
                           NVL (SUM (abs(ROUND (RSI_Rsb_FIInstr.
                                             ConvSum (RSI_RSB_ACCOUNT.
                                                       RestAC (
                                                         accdoc.t_account,
                                                         accdoc.t_currency,
                                                         EndDate,
                                                         1,
                                                         NULL),
                                                      accdoc.t_currency,
                                                      0,
                                                      EndDate), 2))),
                                0)
                              ENDYNKD
                      FROM dmcaccdoc_dbt accdoc,
                           dmctempl_dbt templ,
                           dmccateg_dbt categ
                     WHERE     accdoc.t_catid = categ.t_id
                           AND categ.t_code IN ('Уплаченный НКД')
                           AND accdoc.t_iscommon = 'X'
                           AND templ.t_catid = accdoc.t_catid
                           AND templ.t_number = accdoc.t_templnum
                  GROUP BY                              /*accdoc.t_account, */
                          accdoc.t_fiid,
                           templ.t_value1) r
              ON (r.t_fiid = fo7.fiid AND r.t_value1 = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.ENDYNKD = fo7.ENDYNKD + r.ENDYNKD; 


      /* Прибавляем остатки по КУ Уплаченный НКД, БПП на конец периода */

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT leg.t_pfi,
                           templ.t_value1,
                           NVL (SUM (abs(ROUND (RSI_Rsb_FIInstr.
                                             ConvSum (RSI_RSB_ACCOUNT.
                                                       RestAC (
                                                         accdoc.t_account,
                                                         accdoc.t_currency,
                                                         EndDate,
                                                         1,
                                                         NULL),
                                                      accdoc.t_currency,
                                                      0,
                                                      EndDate), 2))),
                                0)
                              ENDYNKDBpp
                      FROM dmcaccdoc_dbt accdoc,
                           ddl_leg_dbt leg,
                           dmctempl_dbt templ,
                           dmccateg_dbt categ
                     WHERE accdoc.t_catid = categ.t_id
                           AND categ.t_code IN
                                  ('Уплаченный НКД, БПП')
                           AND accdoc.t_dockind = 176
                           AND leg.t_id = accdoc.t_docid
                           AND templ.t_catid = accdoc.t_catid
                           AND templ.t_number = accdoc.t_templnum
                  GROUP BY leg.t_pfi, templ.t_value1) r
              ON (r.t_pfi = fo7.fiid AND r.t_value1 = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.ENDYNKD = fo7.ENDYNKD + r.ENDYNKDBpp;


/********************/

/* Прибавляем остатки по КУ Уплач. НКД, Корзина БПП на конец периода */

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT Ens.t_fiid,
                           templ.t_value1,
                           NVL (SUM (abs(ROUND (RSI_Rsb_FIInstr.
                                             ConvSum (RSI_RSB_ACCOUNT.
                                                       RestAC (
                                                         accdoc.t_account,
                                                         accdoc.t_currency,
                                                         EndDate,
                                                         1,
                                                         NULL),
                                                      accdoc.t_currency,
                                                      0,
                                                      EndDate), 2))),
                                0)
                              ENDYNKD
                      FROM dmcaccdoc_dbt accdoc,
                           ddl_tick_ens_dbt ens,
                           dmctempl_dbt templ
                     WHERE     accdoc.t_catid  in (select t_id from DMCCATEG_DBT t where t_number IN ( 1238)) --t_catnum IN ( 1238)
                           AND accdoc.t_dockind = 4620
                           AND ens.t_id = accdoc.t_docid
                           AND templ.t_catid = accdoc.t_catid
                           AND templ.t_number = accdoc.t_templnum
                           AND not exists(select 1 from dmcaccdoc_dbt where t_catid  in (select t_id from DMCCATEG_DBT t where t_number IN ( 1237)) --t_catnum = 1237 
                                                                            and t_account = accdoc.t_account)
                  GROUP BY ens.t_fiid, templ.t_value1) r
              ON (r.t_fiid = fo7.fiid AND r.t_value1 = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.ENDYNKD = fo7.ENDYNKD + r.ENDYNKD;


      /* Остатки по КУ Премия, ц/б  на конец периода  ENDBONUS*/

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT accdoc.t_fiid,
                           templ.t_value1,
                           NVL (SUM (abs(ROUND (RSI_Rsb_FIInstr.
                                             ConvSum (RSI_RSB_ACCOUNT.
                                                       RestAC (
                                                         accdoc.t_account,
                                                         accdoc.t_currency,
                                                         EndDate,
                                                         1,
                                                         NULL),
                                                      accdoc.t_currency,
                                                      0,
                                                      EndDate), 2))),
                                0)
                              ENDBONUS
                      FROM dmcaccdoc_dbt accdoc,
                           dmctempl_dbt templ,
                           dmccateg_dbt categ
                     WHERE     accdoc.t_catid = categ.t_id
                           AND categ.t_code IN ('Премия, ц/б')
                           AND accdoc.t_iscommon = 'X'
                           AND templ.t_catid = accdoc.t_catid
                           AND templ.t_number = accdoc.t_templnum
                  GROUP BY accdoc.t_fiid, templ.t_value1) r
              ON (r.t_fiid = fo7.fiid AND r.t_value1 = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.ENDBONUS = fo7.ENDBONUS + r.ENDBONUS;


      /* Остатки по КУ Начисл.ПДД, ц/б по начисленному купону  на конец периода ENDINCOME */

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT accdoc.t_fiid,
                           templ.t_value1,
                           NVL (SUM (abs(ROUND (RSI_Rsb_FIInstr.
                                             ConvSum (RSI_RSB_ACCOUNT.
                                                       RestAC (
                                                         accdoc.t_account,
                                                         accdoc.t_currency,
                                                         EndDate,
                                                         1,
                                                         NULL),
                                                      accdoc.t_currency,
                                                      0,
                                                      EndDate), 2))),
                                0)
                              ENDINCOME
                      FROM dmcaccdoc_dbt accdoc,
                           dmctempl_dbt templ,
                           dmccateg_dbt categ
                     WHERE     accdoc.t_catid = categ.t_id
                           AND categ.t_code IN ('Начисл.ПДД, ц/б')
                           AND accdoc.t_iscommon = 'X'
                           AND templ.t_catid = accdoc.t_catid
                           AND templ.t_number = accdoc.t_templnum
                           AND templ.t_value4 = 1
                  GROUP BY accdoc.t_fiid, templ.t_value1) r
              ON (r.t_fiid = fo7.fiid AND r.t_value1 = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.ENDINCOME = fo7.ENDINCOME + r.ENDINCOME;


      /* Остатки по КУ Начисл.ПДД, ц/б по начисленному дисконту  на конец периода ENDDISCOUNT */

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT accdoc.t_fiid,
                           templ.t_value1,
                           NVL (SUM (abs(ROUND (RSI_Rsb_FIInstr.
                                             ConvSum (RSI_RSB_ACCOUNT.
                                                       RestAC (
                                                         accdoc.t_account,
                                                         accdoc.t_currency,
                                                         EndDate,
                                                         1,
                                                         NULL),
                                                      accdoc.t_currency,
                                                      0,
                                                      EndDate), 2))),
                                0)
                              ENDDISCOUNT
                      FROM dmcaccdoc_dbt accdoc,
                           dmctempl_dbt templ,
                           dmccateg_dbt categ
                     WHERE     accdoc.t_catid = categ.t_id
                           AND categ.t_code IN ('Начисл.ПДД, ц/б')
                           AND accdoc.t_iscommon = 'X'
                           AND templ.t_catid = accdoc.t_catid
                           AND templ.t_number = accdoc.t_templnum
                           AND templ.t_value4 = 2
                  GROUP BY accdoc.t_fiid, templ.t_value1) r
              ON (r.t_fiid = fo7.fiid AND r.t_value1 = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.ENDDISCOUNT = fo7.ENDDISCOUNT + r.ENDDISCOUNT;


/*   Остатки по счетам корректировок на конец периода  EndIntToEir */

/* Остатки по КУ +Корректировка, ц/б  на конец периода */

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT                              /*accdoc.t_account, */
                          accdoc.t_fiid,
                           templ.t_value1,
                           NVL (SUM (abs(ROUND (RSI_Rsb_FIInstr.
                                             ConvSum (RSI_RSB_ACCOUNT.
                                                       RestAC (
                                                         accdoc.t_account,
                                                         accdoc.t_currency,
                                                         EndDate,
                                                         1,
                                                         NULL),
                                                      accdoc.t_currency,
                                                      0,
                                                      EndDate), 2))),
                                0)
                              EndIntToEir
                      FROM dmcaccdoc_dbt accdoc,
                           dmctempl_dbt templ,
                           dmccateg_dbt categ
                     WHERE     accdoc.t_catid = categ.t_id
                           AND categ.t_code IN ('+Корректировка, ц/б')
                           AND accdoc.t_iscommon = 'X'
                           AND templ.t_catid = accdoc.t_catid
                           AND templ.t_number = accdoc.t_templnum
                  GROUP BY                              /*accdoc.t_account, */
                          accdoc.t_fiid,
                           templ.t_value1) r
              ON (r.t_fiid = fo7.fiid AND r.t_value1 = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.EndIntToEir = fo7.EndIntToEir + r.EndIntToEir; 

/* Остатки по КУ -Корректировка, ц/б  на конец периода ( с минусом) EndIntToEir*/

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT                              /*accdoc.t_account, */
                          accdoc.t_fiid,
                           templ.t_value1,
                           NVL (SUM (abs(ROUND (RSI_Rsb_FIInstr.
                                             ConvSum (RSI_RSB_ACCOUNT.
                                                       RestAC (
                                                         accdoc.t_account,
                                                         accdoc.t_currency,
                                                         EndDate,
                                                         1,
                                                         NULL),
                                                      accdoc.t_currency,
                                                      0,
                                                      EndDate), 2))),
                                0)*(-1)
                              EndIntToEir
                      FROM dmcaccdoc_dbt accdoc,
                           dmctempl_dbt templ,
                           dmccateg_dbt categ
                     WHERE     accdoc.t_catid = categ.t_id
                           AND categ.t_code IN ('-Корректировка, ц/б')
                           AND accdoc.t_iscommon = 'X'
                           AND templ.t_catid = accdoc.t_catid
                           AND templ.t_number = accdoc.t_templnum
                  GROUP BY                              /*accdoc.t_account, */
                          accdoc.t_fiid,
                           templ.t_value1) r
              ON (r.t_fiid = fo7.fiid AND r.t_value1 = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.EndIntToEir = fo7.EndIntToEir + r.EndIntToEir; 

/*   END Остатки по счетам корректировок на конец периода  BegIntToEir */


/***************************/

      /*  Валютная переоценка в части номинала за текуший период  VALOVERCOST  +  */

      MERGE INTO DMsfo7_DBT fo7
           USING ( with accd as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                          distinct acc.t_accountid,acc.t_account,accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                           where categ.t_code in
                                                     ('Наш портфель ц/б')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account),
                        acck as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                            distinct acc.t_accountid,acc.t_account, accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                            where categ.t_code in
                                                     ('+Маржа, ИВ и ДМ', '+ПереоценкаА')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account )
                      SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) VALOVERCOST
                     FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX2 )*/ 
                                   distinct trn.t_acctrnid,
                                   /*(SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = accdeb.t_account)*/
                                   accd.t_fiid   AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = accd.t_account -- accdeb.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                             FROM accd,
                                   dacctrn_dbt trn, 
                                   --daccount_dbt AccDeb,
                                   --daccount_dbt AccCred
                                   acck
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   and trn.t_accountid_payer = accd.t_accountid
                                   and trn.t_accountid_receiver = acck.t_accountid 
                                  /*AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Наш портфель ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('+Маржа, ИВ и ДМ', '+ПереоценкаА')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)*/)
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.VALOVERCOST = fo7.VALOVERCOST + r.VALOVERCOST;

      /*   Валютная переоценка в части номинала за текущий периол VALOVERCOST  -  */

      MERGE INTO DMsfo7_DBT fo7
           USING (  with accd as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                          distinct acc.t_accountid,acc.t_account,accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                           where categ.t_code in
                                                     ('-Маржа, ИВ и ДМ', '-ПереоценкаА')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account),
                        acck as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                            distinct acc.t_accountid,acc.t_account, accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                            where categ.t_code in
                                                     ('Наш портфель ц/б')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account )
                       SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) VALOVERCOST
                       FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX2 )*/ 
                                   distinct trn.t_acctrnid,
                                   /*(SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = acccred.t_account)*/
                                   acck.t_fiid   AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = acck.t_account -- acccred.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                             FROM accd,
                                   dacctrn_dbt trn, 
                                   --daccount_dbt AccDeb,
                                   --daccount_dbt AccCred
                                   acck
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   and trn.t_accountid_payer = accd.t_accountid
                                   and trn.t_accountid_receiver = acck.t_accountid 
                                  /*AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Наш портфель ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('-Маржа, ИВ и ДМ', '-ПереоценкаА')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)*/)
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.VALOVERCOST = fo7.VALOVERCOST - r.VALOVERCOST;


/*     Валютная переоценка за текущий период в части номинала по БПП VALOVERCOST  + БПП  */

      MERGE INTO DMsfo7_DBT fo7
           USING ( with accd as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                          distinct acc.t_accountid,acc.t_account,accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                           where categ.t_code in
                                                     ('Ц/б, БПП')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account),
                        acck as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                            distinct acc.t_accountid,acc.t_account, accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                            where categ.t_code in
                                                     ('+Маржа, ИВ и ДМ', '+ПереоценкаА')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account )
                      SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) VALOVERCOST
                     FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX2 )*/ 
                                   distinct trn.t_acctrnid,
                                   (SELECT DISTINCT leg.t_pfi
                                      FROM dmcaccdoc_dbt c, ddl_leg_dbt leg
                                     WHERE c.t_account = accd.t_account -- accdeb.t_account 
                                            and c.t_dockind = 176 and leg.t_id = c.t_docid and leg.t_legkind = 0) as fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = accd.t_account -- accdeb.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                             FROM accd,
                                   dacctrn_dbt trn, 
                                   --daccount_dbt AccDeb,
                                   --daccount_dbt AccCred
                                   acck
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   and trn.t_accountid_payer = accd.t_accountid
                                   and trn.t_accountid_receiver = acck.t_accountid 
                                   /*AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Ц/б, БПП')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('+Маржа, ИВ и ДМ', '+ПереоценкаА')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)*/)
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.VALOVERCOST = fo7.VALOVERCOST + r.VALOVERCOST;

/*   End  VALOVERCOST  + БПП  */


/*     Валютная переоценка в части номинала БПП за текущий период VALOVERCOST  - БПП  */

      MERGE INTO DMsfo7_DBT fo7
           USING ( with accd as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                          distinct acc.t_accountid,acc.t_account,accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                           where categ.t_code in
                                                     ('-Маржа, ИВ и ДМ', '-ПереоценкаА')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account),
                        acck as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                            distinct acc.t_accountid,acc.t_account, accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                            where categ.t_code in
                                                     ('Ц/б, БПП')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account )
                         SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) VALOVERCOST
                     FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX2 )*/ 
                                   distinct trn.t_acctrnid,
                                   (SELECT DISTINCT leg.t_pfi
                                      FROM dmcaccdoc_dbt c, ddl_leg_dbt leg
                                     WHERE c.t_account = acck.t_account ---acccred.t_account 
                                        and c.t_dockind = 176 and leg.t_id = c.t_docid and leg.t_legkind = 0) as fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = acck.t_account --- acccred.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                             FROM accd,
                                   dacctrn_dbt trn, 
                                   --daccount_dbt AccDeb,
                                   --daccount_dbt AccCred
                                   acck
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   and trn.t_accountid_payer = accd.t_accountid
                                   and trn.t_accountid_receiver = acck.t_accountid 
                                  /*AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Ц/б, БПП')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('-Маржа, ИВ и ДМ', '-ПереоценкаА')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)*/)
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.VALOVERCOST = fo7.VALOVERCOST - r.VALOVERCOST;

/*   End  VALOVERCOST  - БПП  */


/***********************/
/*     Валютная переоценка за текущий период в части номинала по Корзине БПП VALOVERCOST  + Корзина БПП  */

      MERGE INTO DMsfo7_DBT fo7
           USING ( with accd as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                          distinct acc.t_accountid,acc.t_account,accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                           where categ.t_code in
                                                     ('Ц/б, Корзина БПП')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account),
                        acck as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                            distinct acc.t_accountid,acc.t_account, accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                            where categ.t_code in
                                                    ('+Маржа, ИВ и ДМ', '+ПереоценкаА')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account )
                      SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) VALOVERCOST
                        FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX2 )*/ 
                                   (SELECT DISTINCT Ens.t_fiid
                                      FROM dmcaccdoc_dbt c, ddl_tick_ens_dbt Ens
                                     WHERE c.t_account = accd.t_account -- accdeb.t_account 
                                       and c.t_dockind = 4620 and Ens.t_id = c.t_docid) as fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = accd.t_account -- accdeb.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                            FROM accd,
                                   dacctrn_dbt trn, 
                                   --daccount_dbt AccDeb,
                                   --daccount_dbt AccCred
                                   acck
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   and trn.t_accountid_payer = accd.t_accountid
                                   and trn.t_accountid_receiver = acck.t_accountid 
                                   /*AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Ц/б, Корзина БПП')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                    ('+Маржа, ИВ и ДМ', '+ПереоценкаА')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)*/)
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.VALOVERCOST = fo7.VALOVERCOST + r.VALOVERCOST;

/*     Валютная переоценка за текущий период в части номинала по Корзине БПП VALOVERCOST  - Корзина БПП  */

      MERGE INTO DMsfo7_DBT fo7
           USING ( with accd as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                          distinct acc.t_accountid,acc.t_account,accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                           where categ.t_code in
                                                    ('-Маржа, ИВ и ДМ', '-ПереоценкаА')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account),
                        acck as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                            distinct acc.t_accountid,acc.t_account, accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                            where categ.t_code in
                                                     ('Ц/б, Корзина БПП')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account )
                       SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) VALOVERCOST
                     FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX2 )*/ 
                                   distinct trn.t_acctrnid,
                                   (SELECT DISTINCT Ens.t_fiid
                                      FROM dmcaccdoc_dbt c, ddl_tick_ens_dbt Ens
                                     WHERE c.t_account = acck.t_account --acccred.t_account 
                                          and c.t_dockind = 4620 and Ens.t_id = c.t_docid) as fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = acck.t_account --acccred.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                             FROM accd,
                                   dacctrn_dbt trn, 
                                   --daccount_dbt AccDeb,
                                   --daccount_dbt AccCred
                                   acck
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   and trn.t_accountid_payer = accd.t_accountid
                                   and trn.t_accountid_receiver = acck.t_accountid 
                                  /*AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Ц/б, Корзина БПП')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                    ('-Маржа, ИВ и ДМ', '-ПереоценкаА')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)*/)
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.VALOVERCOST = fo7.VALOVERCOST - r.VALOVERCOST;


/**/

      /*   Валютная переоценка в части УНКД за текущий период VALOVERYNKD +  */

      MERGE INTO DMsfo7_DBT fo7
           USING ( with accd as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                          distinct acc.t_accountid,acc.t_account,accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                           where categ.t_code in
                                                     ('Уплаченный НКД')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account),
                        acck as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                            distinct acc.t_accountid,acc.t_account, accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                            where categ.t_code in
                                                     ('+Маржа, ИВ и ДМ', '+ПереоценкаА')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account )
                       SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) VALOVERYNKD
                      FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX2 )*/ 
                                   distinct trn.t_acctrnid,
                                   /*(SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = accdeb.t_account)*/
                                   accd.t_fiid   AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = accd.t_account -- accdeb.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                            FROM accd,
                                   dacctrn_dbt trn, 
                                   --daccount_dbt AccDeb,
                                   --daccount_dbt AccCred
                                   acck
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                    and trn.t_accountid_payer = accd.t_accountid
                                   and trn.t_accountid_receiver = acck.t_accountid 
                                /* AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Уплаченный НКД')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('+Маржа, ИВ и ДМ', '+ПереоценкаА')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)*/)
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.VALOVERYNKD = fo7.VALOVERYNKD + r.VALOVERYNKD;

      /*   Валютная переоценка в части УНКД за текущий период VALOVERYNKD -  */

      MERGE INTO DMsfo7_DBT fo7
           USING ( with accd as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                          distinct acc.t_accountid,acc.t_account,accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                           where categ.t_code in
                                                     ('-Маржа, ИВ и ДМ', '-ПереоценкаА')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account),
                        acck as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                            distinct acc.t_accountid,acc.t_account, accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                            where categ.t_code in
                                                     ('Уплаченный НКД')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account )
                       SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) VALOVERYNKD
                     FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX2 )*/ 
                                   distinct trn.t_acctrnid,
                                   /*(SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = acccred.t_account)*/
                                   acck.t_fiid   AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account =  acck.t_account -- acccred.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                             FROM accd,
                                   dacctrn_dbt trn, 
                                   --daccount_dbt AccDeb,
                                   --daccount_dbt AccCred
                                   acck
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   and trn.t_accountid_payer = accd.t_accountid
                                   and trn.t_accountid_receiver = acck.t_accountid 
                                 /* AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Уплаченный НКД')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('-Маржа, ИВ и ДМ', '-ПереоценкаА')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)*/)
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.VALOVERYNKD = fo7.VALOVERYNKD - r.VALOVERYNKD;

/*    Валютная переоценка в части  УНКД БПП за текущий периол VALOVERYNKD  + БПП  */

      MERGE INTO DMsfo7_DBT fo7
           USING ( with accd as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                          distinct acc.t_accountid,acc.t_account,accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                           where categ.t_code in
                                                     ('Уплаченный НКД, БПП')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account),
                        acck as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                            distinct acc.t_accountid,acc.t_account, accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                            where categ.t_code in
                                                     ('+Маржа, ИВ и ДМ', '+ПереоценкаА')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account )
                      SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) VALOVERYNKD
                         FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX2 )*/ 
                                   distinct trn.t_acctrnid, accd.t_account,
                                   (SELECT DISTINCT leg.t_pfi
                                      FROM dmcaccdoc_dbt c, ddl_leg_dbt leg
                                     WHERE c.t_account = accd.t_account -- accdeb.t_account 
                                      and c.t_dockind = 176 and leg.t_id = c.t_docid and leg.t_legkind = 0) as fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = accd.t_account -- accdeb.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                             FROM accd,
                                   dacctrn_dbt trn 
                                   --daccount_dbt AccDeb,
                                   --daccount_dbt AccCred
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   and trn.t_accountid_payer = accd.t_accountid
                                   and trn.t_accountid_receiver in (select t_accountid from acck) 
                                   /*AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Уплаченный НКД, БПП')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('+Маржа, ИВ и ДМ', '+ПереоценкаА')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)*/)
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.VALOVERYNKD = fo7.VALOVERYNKD + r.VALOVERYNKD;

/*   End  PrevYNKD + БПП  */


/*    Валютная переоценка в части УНКД БПП за текущий период  VALOVERYNKD  - БПП  */

      MERGE INTO DMsfo7_DBT fo7
           USING ( with accd as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                          distinct acc.t_accountid,acc.t_account,accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                           where categ.t_code in
                                                     ('-Маржа, ИВ и ДМ', '-ПереоценкаА')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account),
                        acck as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                            distinct acc.t_accountid,acc.t_account, accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                            where categ.t_code in
                                                     ('Уплаченный НКД, БПП')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account )
                      SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) VALOVERYNKD
                      FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX2 )*/ 
                                   distinct trn.t_acctrnid,
                                   (SELECT DISTINCT leg.t_pfi
                                      FROM dmcaccdoc_dbt c, ddl_leg_dbt leg
                                     WHERE c.t_account = acck.t_account --acccred.t_account 
                                           and c.t_dockind = 176 and leg.t_id = c.t_docid and leg.t_legkind = 0) as fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = acck.t_account --acccred.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                             FROM accd,
                                   dacctrn_dbt trn, 
                                   --daccount_dbt AccDeb,
                                   --daccount_dbt AccCred
                                   acck
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   and trn.t_accountid_payer = accd.t_accountid
                                   and trn.t_accountid_receiver = acck.t_accountid 
                                  /*AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Уплаченный НКД, БПП')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('-Маржа, ИВ и ДМ', '-ПереоценкаА')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)*/)
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.VALOVERYNKD = fo7.VALOVERYNKD - r.VALOVERYNKD;

/*   End  VALOVERYNKD  - БПП  */

/*********************/

/*    Валютная переоценка в части  Корзины УНКД БПП за текущий период VALOVERYNKD  + Корзина БПП  */

      MERGE INTO DMsfo7_DBT fo7
           USING ( with accd as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                          distinct acc.t_accountid,acc.t_account,accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                           where categ.t_code in
                                                     ('Уплач. НКД, Корзина БПП')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account
                             and accdoc.t_catnum != 1237),
                        acck as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                            distinct acc.t_accountid,acc.t_account, accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                            where categ.t_code in
                                                    ('+Маржа, ИВ и ДМ', '+ПереоценкаА')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account )
                      SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) VALOVERYNKD
                       FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX2 )*/ 
                                   distinct trn.t_acctrnid,
                                   (SELECT DISTINCT Ens.t_fiid
                                      FROM dmcaccdoc_dbt c, ddl_tick_ens_dbt Ens
                                     WHERE c.t_account = accd.t_account --accdeb.t_account 
                                     and c.t_dockind = 4620 and Ens.t_id = c.t_docid) as fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = accd.t_account -- accdeb.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM accd,
                                   dacctrn_dbt trn 
                                   --daccount_dbt AccDeb,
                                   --daccount_dbt AccCred
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   and trn.t_accountid_payer = accd.t_accountid
                                   and trn.t_accountid_receiver in (select t_accountid from acck) 
                                  /*AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Уплач. НКД, Корзина БПП')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)
                                         AND  not exists(select 1 from dmcaccdoc_dbt where t_catid  in (select t_id from DMCCATEG_DBT t where t_number IN ( 1237)) --t_catnum = 1237 
                                                                                          and t_account = accdeb.t_account)
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                    ('+Маржа, ИВ и ДМ', '+ПереоценкаА')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)*/)
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.VALOVERYNKD = fo7.VALOVERYNKD + r.VALOVERYNKD;

/*    Валютная переоценка в части  Корзины УНКД БПП за текущий период VALOVERYNKD  - Корзина БПП  */

      MERGE INTO DMsfo7_DBT fo7
           USING (  with accd as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                          distinct acc.t_accountid,acc.t_account,accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                           where categ.t_code in
                                                     ('-Маржа, ИВ и ДМ', '-ПереоценкаА')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account),
                        acck as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                            distinct acc.t_accountid,acc.t_account, accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                            where categ.t_code in
                                                     ('Уплач. НКД, Корзина БПП')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account
                             and accdoc.t_catnum != 1237 )
                      SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) VALOVERYNKD
                      FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX2 )*/ 
                                   distinct trn.t_acctrnid,
                                   (SELECT DISTINCT Ens.t_fiid
                                      FROM dmcaccdoc_dbt c, ddl_tick_ens_dbt Ens
                                     WHERE c.t_account = acck.t_account -- acccred.t_account
                                         and c.t_dockind = 4620 and Ens.t_id = c.t_docid) as fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = acck.t_account -- acccred.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                             FROM accd,
                                   dacctrn_dbt trn, 
                                   --daccount_dbt AccDeb,
                                   --daccount_dbt AccCred
                                   acck
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   and trn.t_accountid_payer = accd.t_accountid
                                   and trn.t_accountid_receiver = acck.t_accountid 
                                   /*AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Уплач. НКД, Корзина БПП')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)
                                          AND  not exists(select 1 from dmcaccdoc_dbt where t_catid  in (select t_id from DMCCATEG_DBT t where t_number IN ( 1237))--t_catnum = 1237 
                                                                                           and t_account = acccred.t_account)
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('-Маржа, ИВ и ДМ', '-ПереоценкаА')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)*/)
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.VALOVERYNKD = fo7.VALOVERYNKD - r.VALOVERYNKD;



/*     Валютная переоценка в части премии за текущий период VALOVERBONUS    +     */

      MERGE INTO DMsfo7_DBT fo7
           USING ( with accd as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                          distinct acc.t_accountid,acc.t_account,accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                           where categ.t_code in
                                                     ('Премия, ц/б')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account),
                        acck as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                            distinct acc.t_accountid,acc.t_account, accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                            where categ.t_code in
                                                     ('+Маржа, ИВ и ДМ', '+ПереоценкаА')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account )
                      SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) VALOVERBONUS
                         FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX2 )*/ 
                                   distinct trn.t_acctrnid, 
                                   /*(SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = accdeb.t_account)*/
                                   accd.t_fiid   AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = accd.t_account --accdeb.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                            FROM accd,
                                   dacctrn_dbt trn, 
                                   --daccount_dbt AccDeb,
                                   --daccount_dbt AccCred
                                   acck
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   and trn.t_accountid_payer = accd.t_accountid
                                   and trn.t_accountid_receiver = acck.t_accountid 
                                   /*AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Премия, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('+Маржа, ИВ и ДМ', '+ПереоценкаА')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)*/)
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.VALOVERBONUS = fo7.VALOVERBONUS + r.VALOVERBONUS;

/*    Валютная переоценка в части премии за текущий период VALOVERBONUS    -     */

      MERGE INTO DMsfo7_DBT fo7
           USING ( with accd as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                          distinct acc.t_accountid,acc.t_account,accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                           where categ.t_code in
                                                     ('-Маржа, ИВ и ДМ', '-ПереоценкаА')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account),
                        acck as
                         (select /*+ index(accdoc dmcaccdoc_dbt_idxc) materialize */
                            distinct acc.t_accountid,acc.t_account, accdoc.t_fiid
                            from dmccateg_dbt  categ
                                ,dmcaccdoc_dbt accdoc
                                ,daccount_dbt acc
                            where categ.t_code in
                                                     ('Премия, ц/б')
                             and accdoc.t_catid = categ.t_id
                             and acc.t_account = accdoc.t_account )
                      SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) VALOVERBONUS
                        FROM (SELECT  /*+ index(trn DACCTRN_DBT_IDX2 )*/ 
                                   distinct trn.t_acctrnid,
                                   /*(SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = acccred.t_account)*/
                                   acck.t_fiid   AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = acck.t_account --acccred.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                             FROM accd,
                                   dacctrn_dbt trn, 
                                   --daccount_dbt AccDeb,
                                   --daccount_dbt AccCred
                                   acck
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                    and trn.t_accountid_payer = accd.t_accountid
                                   and trn.t_accountid_receiver = acck.t_accountid 
                                  /*AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('Премия, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('-Маржа, ИВ и ДМ', '-ПереоценкаА')
                                                  AND accdoc.t_catid =
                                                         categ.t_id)*/)
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.VALOVERBONUS = fo7.VALOVERBONUS - r.VALOVERBONUS;


/* Валютная переоценка в части начисленного купона за текущий год  VALOVERINCOME    +  */

 MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) VALOVERINCOME
                      FROM (SELECT accdeb.t_account,acccred.t_account,
                                   (SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = accdeb.t_account and t_iscommon = 'X')
                                      AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = accdeb.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM dacctrn_dbt trn,
                                   daccount_dbt AccDeb,
                                   daccount_dbt AccCred
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND accdeb.t_account IN
                                          (SELECT  accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc, dmctempl_dbt templ
                                            WHERE categ.t_code IN
                                                     ('Начисл.ПДД, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id and templ.t_catid = categ.t_id and accdoc.t_templnum = templ.t_number and templ.t_value4 = 1 /*купон*/  and accdoc.t_iscommon = 'X' group by accdoc.t_account )
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('+Маржа, ИВ и ДМ', '+ПереоценкаА')
                                                      and accdoc.t_catid = categ.t_id
                                                  )
)
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.VALOVERINCOME = fo7.VALOVERINCOME + r.VALOVERINCOME;


/*   Валютная переоценка в части накопленного купона за текущий период VALOVERINCOME    -  */

 MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) VALOVERINCOME
                      FROM (SELECT acccred.t_account,
                                   (SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = acccred.t_account and t_iscommon = 'X')
                                      AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = acccred.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM dacctrn_dbt trn,
                                   daccount_dbt AccDeb,
                                   daccount_dbt AccCred
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND acccred.t_account IN
                                          (SELECT  accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc, dmctempl_dbt templ
                                            WHERE categ.t_code IN
                                                     ('Начисл.ПДД, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id and templ.t_catid = categ.t_id and accdoc.t_templnum = templ.t_number and templ.t_value4 = 1 /*купон*/  and accdoc.t_iscommon = 'X' group by accdoc.t_account )
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('-Маржа, ИВ и ДМ', '-ПереоценкаА')
                                                      and accdoc.t_catid = categ.t_id
                                                  )
)
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.VALOVERINCOME = fo7.VALOVERINCOME - r.VALOVERINCOME;


/*PrevDiscount*/

/* Валютная переоценка в части начисленного дисконта за текущий период  VALOVERDISCOUNT    +  */

 MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) VALOVERDISCOUNT
                      FROM (SELECT accdeb.t_account,acccred.t_account,
                                   (SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = accdeb.t_account and t_iscommon = 'X')
                                      AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = accdeb.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM dacctrn_dbt trn,
                                   daccount_dbt AccDeb,
                                   daccount_dbt AccCred
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND accdeb.t_account IN
                                          (SELECT  accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc, dmctempl_dbt templ
                                            WHERE categ.t_code IN
                                                     ('Начисл.ПДД, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id and templ.t_catid = categ.t_id and accdoc.t_templnum = templ.t_number and templ.t_value4 = 2 /*дисконт*/  and accdoc.t_iscommon = 'X' group by accdoc.t_account )
                                   AND acccred.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('+Маржа, ИВ и ДМ', '+ПереоценкаА')
                                                      and accdoc.t_catid = categ.t_id
                                                  )
)
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.VALOVERDISCOUNT = fo7.VALOVERDISCOUNT + r.VALOVERDISCOUNT;


/*   Валютная переоценка в части накопленного дисконта за  текущий период VALOVERDISCOUNT    -  */

 MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT fiid,
                           portfnum,
                           NVL (SUM (t_sum_natcur), 0) VALOVERDISCOUNT
                      FROM (SELECT acccred.t_account,
                                   (SELECT DISTINCT t_fiid
                                      FROM dmcaccdoc_dbt
                                     WHERE t_account = acccred.t_account and t_iscommon = 'X')
                                      AS fiid,
                                   (SELECT DISTINCT templ.t_value1
                                      FROM dmctempl_dbt templ,
                                           dmcaccdoc_dbt accdoc
                                     WHERE accdoc.t_account = acccred.t_account
                                           AND templ.t_catid = accdoc.t_catid
                                           AND accdoc.t_templnum =
                                                  templ.t_number)
                                      AS portfnum,
                                   trn.t_sum_natcur
                              FROM dacctrn_dbt trn,
                                   daccount_dbt AccDeb,
                                   daccount_dbt AccCred
                             WHERE     trn.t_date_carry >= BegDate
                                   AND trn.t_date_carry <= EndDate
                                   AND trn.t_state = 1
                                   AND AccDeb.t_accountid =
                                          trn.t_accountid_payer
                                   AND AccCred.t_accountid =
                                          trn.t_accountid_receiver
                                   AND acccred.t_account IN
                                          (SELECT  accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc, dmctempl_dbt templ
                                            WHERE categ.t_code IN
                                                     ('Начисл.ПДД, ц/б')
                                                  AND accdoc.t_catid =
                                                         categ.t_id and templ.t_catid = categ.t_id and accdoc.t_templnum = templ.t_number and templ.t_value4 = 2 /*дисконт*/  and accdoc.t_iscommon = 'X' group by accdoc.t_account )
                                   AND accdeb.t_account IN
                                          (SELECT DISTINCT accdoc.t_account
                                             FROM dmccateg_dbt categ,
                                                  dmcaccdoc_dbt accdoc
                                            WHERE categ.t_code IN
                                                     ('-Маржа, ИВ и ДМ', '-ПереоценкаА')
                                                      and accdoc.t_catid = categ.t_id
                                                  )
)
                  GROUP BY fiid, portfnum) r
              ON (r.fiid = fo7.fiid AND r.portfnum = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.VALOVERDISCOUNT = fo7.VALOVERDISCOUNT - r.VALOVERDISCOUNT;

/*******************/

/* Остаток по счетам переоценки на конец перида  EndOverValue + */

      /* Положительная переоценка по 2 портфелю */

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT                              /*accdoc.t_account, */
                          accdoc.t_fiid,
                          CAST( NVL (SUM (ROUND (RSI_Rsb_FIInstr.
                                             ConvSum (RSI_RSB_ACCOUNT.
                                                       RestAC (
                                                         accdoc.t_account,
                                                         accdoc.t_currency,
                                                         EndDate,
                                                         1,
                                                         NULL),
                                                      accdoc.t_currency,
                                                      0,
                                                      EndDate), 2)),
                                0)*(-1) AS number(32,12) )
                              EndOverValue
                      FROM dmcaccdoc_dbt accdoc,
                           dmccateg_dbt categ
                     WHERE     accdoc.t_catid = categ.t_id
                           AND categ.t_code IN ('+Переоценка, ц/б СССД_ЦБ')
                           AND accdoc.t_iscommon = 'X'
                  GROUP BY                              /*accdoc.t_account, */
                          accdoc.t_fiid) r
              ON (r.t_fiid = fo7.fiid AND  2/*СССД_ЦБ*/ = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.EndOverValue = fo7.EndOverValue + r.EndOverValue; 
         
         
         
      /* отрицательная переоценка по 2 портфелю ( с минусом) */

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT                              /*accdoc.t_account, */
                          accdoc.t_fiid,
                          CAST( NVL (SUM (ROUND (RSI_Rsb_FIInstr.
                                             ConvSum (RSI_RSB_ACCOUNT.
                                                       RestAC (
                                                         accdoc.t_account,
                                                         accdoc.t_currency,
                                                         EndDate,
                                                         1,
                                                         NULL),
                                                      accdoc.t_currency,
                                                      0,
                                                      EndDate), 2)),
                                0)*(-1) AS number(32,12) )
                              EndOverValue
                      FROM dmcaccdoc_dbt accdoc,
                           dmccateg_dbt categ
                     WHERE     accdoc.t_catid = categ.t_id
                           AND categ.t_code IN ('-Переоценка, ц/б СССД_ЦБ')
                           AND accdoc.t_iscommon = 'X'
                  GROUP BY                              /*accdoc.t_account, */
                          accdoc.t_fiid) r
              ON (r.t_fiid = fo7.fiid AND  2/*СССД_ЦБ*/ = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.EndOverValue = fo7.EndOverValue + r.EndOverValue;          


      /* Положительная переоценка по 1 портфелю */

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT                              /*accdoc.t_account, */
                          accdoc.t_fiid,
                          CAST( NVL (SUM (ROUND (RSI_Rsb_FIInstr.
                                             ConvSum (RSI_RSB_ACCOUNT.
                                                       RestAC (
                                                         accdoc.t_account,
                                                         accdoc.t_currency,
                                                         EndDate,
                                                         1,
                                                         NULL),
                                                      accdoc.t_currency,
                                                      0,
                                                      EndDate), 2)),
                                0)*(-1) AS number(32,12) )
                              EndOverValue
                      FROM ( select t_account, t_fiid, t_catid, t_currency from dmcaccdoc_dbt, dmccateg_dbt categ
                     WHERE     t_catid = categ.t_id
                           AND categ.t_code IN ('+Переоценка, ц/б ССПУ_ЦБ') and t_chapter = 1 group by t_account,t_fiid, t_catid, t_currency )accdoc
                  GROUP BY                              /*accdoc.t_account, */
                          accdoc.t_fiid) r
              ON (r.t_fiid = fo7.fiid AND  1/*ССПУ_ЦБ*/ = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.EndOverValue = fo7.EndOverValue + r.EndOverValue; 



      /* Отрицательная  переоценка по 1 портфелю( с минусом)  */

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT                              /*accdoc.t_account, */
                          accdoc.t_fiid,
                          CAST( NVL (SUM (ROUND (RSI_Rsb_FIInstr.
                                             ConvSum (RSI_RSB_ACCOUNT.
                                                       RestAC (
                                                         accdoc.t_account,
                                                         accdoc.t_currency,
                                                         EndDate,
                                                         1,
                                                         NULL),
                                                      accdoc.t_currency,
                                                      0,
                                                      EndDate), 2)),
                                0)*(-1) AS number(32,12) )
                              EndOverValue
                      FROM ( select t_account, t_fiid, t_catid, t_currency from dmcaccdoc_dbt, dmccateg_dbt categ
                     WHERE     t_catid = categ.t_id
                           AND categ.t_code IN ('-Переоценка, ц/б ССПУ_ЦБ') and t_chapter = 1 group by t_account,t_fiid, t_catid, t_currency )accdoc
                  GROUP BY                              /*accdoc.t_account, */
                          accdoc.t_fiid) r
              ON (r.t_fiid = fo7.fiid AND  1/*ССПУ_ЦБ*/ = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.EndOverValue = fo7.EndOverValue + r.EndOverValue;
                 
/* END Остаток по счетам переоценки на конец периода  EndOverValue  */

/*Правим для бумаги ЕвразБанкРазвития-001Р-02 id 4122 и ОАО "Национальное бюро кредитных историй" id 432*/
      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT                              /*accdoc.t_account, */
                          accdoc.t_fiid,
                          CAST( NVL (SUM (ROUND (RSI_Rsb_FIInstr.
                                             ConvSum (RSI_RSB_ACCOUNT.
                                                       RestAC (
                                                         accdoc.t_account,
                                                         accdoc.t_currency,
                                                         EndDate,
                                                         1,
                                                         NULL),
                                                      accdoc.t_currency,
                                                      0,
                                                      EndDate), 2)),
                                0)*(-1) AS number(32,12) )
                              EndOverValue
                      FROM dmcaccdoc_dbt accdoc,
                           dmccateg_dbt categ
                     WHERE     accdoc.t_catid = categ.t_id
                           AND categ.t_code IN ('+ПО ДК 2014')
                           AND accdoc.t_iscommon = 'X'
                           AND accdoc.t_fiid IN (4122, 432)
                  GROUP BY                              /*accdoc.t_account, */
                          accdoc.t_fiid) r
              ON (r.t_fiid = fo7.fiid AND  2/*СССД_ЦБ*/ = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.EndOverValue = fo7.EndOverValue + r.EndOverValue; 
         
         
         
      /* отрицательная переоценка по 2 портфелю ( с минусом) */

      MERGE INTO DMsfo7_DBT fo7
           USING (  SELECT                              /*accdoc.t_account, */
                          accdoc.t_fiid,
                          CAST( NVL (SUM (ROUND (RSI_Rsb_FIInstr.
                                             ConvSum (RSI_RSB_ACCOUNT.
                                                       RestAC (
                                                         accdoc.t_account,
                                                         accdoc.t_currency,
                                                         EndDate,
                                                         1,
                                                         NULL),
                                                      accdoc.t_currency,
                                                      0,
                                                      EndDate), 2)),
                                0)*(-1) AS number(32,12) )
                              EndOverValue
                      FROM dmcaccdoc_dbt accdoc,
                           dmccateg_dbt categ
                     WHERE     accdoc.t_catid = categ.t_id
                           AND categ.t_code IN ('-ПО ДК 2014')
                           AND accdoc.t_iscommon = 'X'
                           AND accdoc.t_fiid IN (4122, 432)
                  GROUP BY                              /*accdoc.t_account, */
                          accdoc.t_fiid) r
              ON (r.t_fiid = fo7.fiid AND  2/*СССД_ЦБ*/ = fo7.begportfcode)
      WHEN MATCHED
      THEN
         UPDATE SET fo7.EndOverValue = fo7.EndOverValue + r.EndOverValue;  
/*END Правим для бумаги ЕвразБанкРазвития-001Р-02 id 4122 и ОАО "Национальное бюро кредитных историй" id 432*/

/*Итого за конец периода EndItog*/
/*Так по формуле от банка*/
   update DMsfo7_DBT set EndItog = Endcost + EndYnkd + EndBonus + EndIncome +   EndDiscount + ValOverCost+ValOverYNKD+ValOverBonus+ValOverIncome+ValOverDiscount+EndOverValue + EndIntToEir /**/ - ( ValOverCost+ValOverYNKD+ValOverBonus+ValOverIncome+ValOverDiscount);

--   update DMsfo7_DBT set EndItog = 0 where Endcost=0;

commit;

   END;
END;
/
