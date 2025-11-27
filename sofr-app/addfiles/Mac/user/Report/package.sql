CREATE OR REPLACE package body RSHB_SOFR.user_f657 is


--   rec_rep                                  dportfolio657_1%ROWTYPE;
   rec_rep                                  DPORTFOLIO2_TMP%ROWTYPE;
   RepDate                                  DATE;

   fininstr                                 dfininstr_dbt%ROWTYPE;
   prev_fininstr                            NUMBER;
   avoiriss                                 davoiriss_dbt%ROWTYPE;
   dl_tick                                  ddl_tick_dbt%ROWTYPE;
   dl_leg                                   ddl_leg_dbt%ROWTYPE;

   vSystypes                                VARCHAR2 (16);
   vDocKind                                 NUMBER;
   vGroup                                   NUMBER;
   LegKind                                  NUMBER;

   tss_prev_FIID                            NUMBER := -1;
   tss_prev_NumInList                       NUMBER := 0;

   vIsQuoted                                NUMBER;
   IsQuoted                                 NUMBER;

   PortfID                                  NUMBER;

   account_rec                              daccount_dbt%ROWTYPE;
   vpartyid                                 NUMBER;
   AvKindShName                             VARCHAR2 (35);

   vscwrthist                               v_scwrthistex%ROWTYPE;

   /* Идентификаторы учетных портфелей. */
   PortfID_Undef                   CONSTANT NUMBER := 0;
   PortfID_Trade                   CONSTANT NUMBER := 1;              /* ТП */
   PortfID_Sale                    CONSTANT NUMBER := 2;             /* ППР */
   PortfID_Retire                  CONSTANT NUMBER := 5;            /* ПУДП */
   PortfID_Unadmitted              CONSTANT NUMBER := 7;             /* БПП */
   PortfID_Back                    CONSTANT NUMBER := 6;             /* ПВО */
   PortfID_OD_req                  CONSTANT NUMBER := 8;             /* +ОД */
   PortfID_Contr                   CONSTANT NUMBER := 3;             /* ПКУ */
   PortfID_Promissory              CONSTANT NUMBER := 4;             /* ПДО */
   PortfID_OD_com                  CONSTANT NUMBER := 9;             /* -ОД */
   PortfID_Back_KSU                CONSTANT NUMBER := 10;        /* ПВО_КСУ */
   PortfID_Back_BPP_KSU            CONSTANT NUMBER := 11;    /* ПВО_БПП_КСУ */

   AVOIRISSKIND_INVESTMENT_SHARE   CONSTANT NUMBER := 16;

   FIROLE_BA_TP                    CONSTANT NUMBER := 80;
   FIROLE_BA_PPR                   CONSTANT NUMBER := 81;
   FIROLE_BA_PUDP                  CONSTANT NUMBER := 90;
   FIROLE_BAINCONTR                CONSTANT NUMBER := 15;
   FIROLE_DSTA                     CONSTANT NUMBER := 12;
   FIROLE_BA_BACK                  CONSTANT NUMBER := 36;
   FIROLE_BA                       CONSTANT NUMBER := 3;
   FIROLE_BAINPROMISSORY           CONSTANT NUMBER := 18;

   ISSUE_RATING                    CONSTANT NUMBER := 53;
   ISSUER_RATING                   CONSTANT NUMBER := 19;

   ZeroDate                        CONSTANT DATE
      := TO_DATE ('01.01.0001', 'dd.mm.yyyy') ;
   IsBond                                   BOOLEAN;

   vcountrycode                             VARCHAR2 (5);
   vcountry_rating                          CHAR (1);

   SetDate                                  DATE;
   facevalue                                NUMBER;
   CostInCUR                                NUMBER := 0;
   CostInRUR                                NUMBER := 0;
   BalCostCUR                               NUMBER := 0;
   BalCostRUR                               NUMBER := 0;
   BuyCostRUR                               NUMBER := 0;
   DiscountIncomeRUR                        NUMBER := 0;
   DiscountIncomeCUR                        NUMBER := 0;
   InterestIncomeCUR                        NUMBER := 0;
   InterestIncomeRUR                        NUMBER := 0;
   NKD                                      NUMBER := 0;
   OverAmountRUR                            NUMBER := 0;
   ReservAmountRUR                          NUMBER := 0;
   BonusRUR                                 NUMBER := 0;
   BonusCUR                                 NUMBER := 0;


FUNCTION GetAccountNumberByLot (p_sumid IN NUMBER, p_date IN DATE)
      RETURN varchar2
   iS
      acc    varchar2(25);
      prtf   NUMBER (5) := 0;

   BEGIN
      SELECT s.t_portfolio
        INTO prtf
        FROM v_scwrthistex s
       WHERE  s.t_changedate =
                          (SELECT MAX (t_changedate)
                             FROM v_scwrthistex
                            WHERE t_sumid = s.t_sumid
                                  AND t_changedate <= p_date)
                   AND s.t_sumid = p_sumid
                   AND s.t_instance =
                          (SELECT MAX (t_instance)
                             FROM v_scwrthistex
                            WHERE t_sumid = s.t_sumid
                                  AND t_changedate = s.t_changedate) ;

      IF (prtf = 3 /*ПКУ*/)
      THEN
         BEGIN
            SELECT ac.t_account
              INTO acc
              FROM       dmcaccdoc_dbt a
                      LEFT JOIN
                         dmctempl_dbt mctempl
                      ON mctempl.t_catid = a.t_catid
                         AND mctempl.t_number = a.t_templnum
                         and mctempl.t_isclose = chr(0)
                   LEFT JOIN
                      dllvalues_dbt llval
                   ON llval.t_list = 1100
                      AND llval.t_element = MCTEMPL.T_VALUE1,
                   v_scwrthistex s,
                   daccount_dbt ac
             WHERE     ac.t_account = a.t_account
             and( (ac.t_close_date >= p_date) or (ac.t_close_date = to_date('01010001','ddmmyyyy')))
                   AND ac.t_chapter = a.t_chapter     /*and s.t_fiid = 1130 */
                   AND s.t_changedate <= p_date
                   AND s.t_instance =
                          (SELECT MAX (t_instance)
                             FROM v_scwrthistex
                            WHERE t_sumid = s.t_sumid
                                  AND t_changedate = s.t_changedate)
                   AND s.t_changedate =
                          (SELECT MAX (t_changedate)
                             FROM v_scwrthistex
                            WHERE t_sumid = s.t_sumid
                                  AND t_changedate <= p_date)
                   AND s.t_sumid = p_sumid
                   --       in (
                   --       select t_sumid from  v_scwrthistex s where s.t_changedate <= TO_DATE('09012019','ddmmyyyy')
                   -- and s.t_instance = (select MAX(t_instance) from v_scwrthistex where t_sumid = s.t_sumid and t_changedate = s.t_changedate)
                   --              )
                   AND a.t_catnum = 1253             /*Наш портфель ПКУ, ц/б*/
                   AND a.t_fiid = s.t_fiid
                   AND s.t_portfolio = 3
                   AND a.t_iscommon = CHR (88)
                   AND ROWNUM = 1;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
--               DBMS_OUTPUT.put_line (p_sumid || '   ' || p_date);
               acc := 'Счет не указан';

         END;
      ELSIF (prtf IN (1, 2, 4, 5)) /*ССПУ_ЦБ, СССД_ЦБ, ПДО,АС_ЦБ*/
      THEN
         BEGIN
            --SELECT ac.t_account,s.t_sumid --into acc
            SELECT ac.t_account
              INTO acc
              FROM       dmcaccdoc_dbt a
                      LEFT JOIN
                         dmctempl_dbt mctempl
                      ON mctempl.t_catid = a.t_catid
                         AND mctempl.t_number = a.t_templnum
                         and mctempl.t_isclose = chr(0)
                   LEFT JOIN
                      dllvalues_dbt llval
                   ON llval.t_list = 1100
                      AND llval.t_element = MCTEMPL.T_VALUE1,
                   v_scwrthistex s,
                   daccount_dbt ac
             WHERE     ac.t_account = a.t_account
             and( (ac.t_close_date >= p_date) or (ac.t_close_date = to_date('01010001','ddmmyyyy')))
                   AND ac.t_chapter = a.t_chapter     /*and s.t_fiid = 1130 */
                   AND s.t_changedate <= p_date
                   AND s.t_instance =
                          (SELECT MAX (t_instance)
                             FROM v_scwrthistex
                            WHERE t_sumid = s.t_sumid
                                  AND t_changedate = s.t_changedate)
                   AND s.t_changedate =
                          (SELECT MAX (t_changedate)
                             FROM v_scwrthistex
                            WHERE t_sumid = s.t_sumid
                                  AND t_changedate <= p_date)
                   AND s.t_sumid = p_sumid
                   --       in (
                   --       select t_sumid from  v_scwrthistex s where s.t_changedate <= TO_DATE('09012019','ddmmyyyy')
                   -- and s.t_instance = (select MAX(t_instance) from v_scwrthistex where t_sumid = s.t_sumid and t_changedate = s.t_changedate)
                   --              )
                   AND ( (a.t_catnum = 231                    /*Наш портфель*/
                                          AND a.t_fiid = s.t_fiid AND a.t_iscommon = CHR (88)
                          AND ( (s.t_portfolio = 1
                                 AND llval.t_code = 'ССПУ_ЦБ')
                               OR (s.t_portfolio = 2
                                   AND llval.t_code = 'СССД_ЦБ')
                               OR (s.t_portfolio = 4
                                   AND llval.t_code = 'ПДО')
                               OR (s.t_portfolio = 5
                                   AND llval.t_code = 'АС_ЦБ')))
                        --            OR (a.t_catnum = 234 /*ПВО*/
                        --             AND a.t_iscommon = CHR (0)
                        --               and a.t_docid in (select t_id from ddl_leg_dbt where t_dealid = s.t_dealid)
                        --            )
                        --            OR ((a.t_catnum = 1253) /*Наш портфель ПКУ, ц/б*/
                        --                   AND a.t_fiid = s.t_fiid
                        --                    AND s.t_portfolio = 3
                        --                   AND a.t_iscommon = CHR (88)
                        --               )
                        OR ( (    a.t_catnum = 233                     /*БПП*/
                              AND a.t_fiid = s.t_fiid
                              AND a.t_iscommon = CHR (88)
                              AND ( (s.t_portfolio = 1
                                     AND llval.t_code = 'ССПУ_ЦБ')
                                   OR (s.t_portfolio = 2
                                       AND llval.t_code = 'СССД_ЦБ')
                                   OR (s.t_portfolio = 5
                                       AND llval.t_code = 'АС_ЦБ')))))
                   AND ROWNUM = 1; /*с ходу не удалось найти дубль, но с этим надо разобраться*/
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
--               DBMS_OUTPUT.put_line (p_sumid || '   ' || p_date);
               acc := 'Счет не указан';
         END;
      ELSIF (prtf = 6) /*ПВО*/
      THEN
         BEGIN
            SELECT ac.t_account
              INTO acc
              FROM       dmcaccdoc_dbt a
                      LEFT JOIN
                         dmctempl_dbt mctempl
                      ON mctempl.t_catid = a.t_catid
                         AND mctempl.t_number = a.t_templnum
                         and mctempl.t_isclose = chr(0)
                   LEFT JOIN
                      dllvalues_dbt llval
                   ON llval.t_list = 1100
                      AND llval.t_element = MCTEMPL.T_VALUE1,
                   v_scwrthistex s,
                   daccount_dbt ac
             WHERE     ac.t_account = a.t_account
             and( (ac.t_close_date >= p_date) or (ac.t_close_date = to_date('01010001','ddmmyyyy')))
                   AND ac.t_chapter = a.t_chapter     /*and s.t_fiid = 1130 */
                   AND s.t_changedate <= p_date
                   AND s.t_instance =
                          (SELECT MAX (t_instance)
                             FROM v_scwrthistex
                            WHERE t_sumid = s.t_sumid
                                  AND t_changedate = s.t_changedate)
                   AND s.t_changedate =
                          (SELECT MAX (t_changedate)
                             FROM v_scwrthistex
                            WHERE t_sumid = s.t_sumid
                                  AND t_changedate <= p_date)
                   AND s.t_sumid = p_sumid
                   AND ( (    a.t_catnum = 234                         /*ПВО*/
                          AND a.t_iscommon = CHR (0)
                          AND a.t_docid IN (SELECT t_id
                                              FROM ddl_leg_dbt
                                             WHERE t_dealid = s.t_dealid))
                        );
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
--               DBMS_OUTPUT.put_line (p_sumid || '   ' || p_date);
               acc := 'Счет не указан';

         END;
      ELSE /* КСУшные портфели и если что-то потерялось в предыдущих, то идем сюда*/
         BEGIN
--               DBMS_OUTPUT.put_line (' ---------------------------------------');
            --SELECT ac.t_account,s.t_sumid --into acc
            SELECT ac.t_account
              INTO acc
              FROM       dmcaccdoc_dbt a
                      LEFT JOIN
                         dmctempl_dbt mctempl
                      ON mctempl.t_catid = a.t_catid
                         AND mctempl.t_number = a.t_templnum
                   LEFT JOIN
                      dllvalues_dbt llval
                   ON llval.t_list = 1100
                      AND llval.t_element = MCTEMPL.T_VALUE1,
                   v_scwrthistex s,
                   daccount_dbt ac
             WHERE     ac.t_account = a.t_account
                   AND ac.t_chapter = a.t_chapter     /*and s.t_fiid = 1130 */
                   AND s.t_changedate <= p_date
                   AND s.t_instance =
                          (SELECT MAX (t_instance)
                             FROM v_scwrthistex
                            WHERE t_sumid = s.t_sumid
                                  AND t_changedate = s.t_changedate)
                   AND s.t_changedate =
                          (SELECT MAX (t_changedate)
                             FROM v_scwrthistex
                            WHERE t_sumid = s.t_sumid
                                  AND t_changedate <= p_date)
                   AND s.t_sumid = p_sumid
                   --       in (
                   --       select t_sumid from  v_scwrthistex s where s.t_changedate <= TO_DATE('09012019','ddmmyyyy')
                   -- and s.t_instance = (select MAX(t_instance) from v_scwrthistex where t_sumid = s.t_sumid and t_changedate = s.t_changedate)
                   --              )
                   AND ( (a.t_catnum = 231                    /*Наш портфель*/
                                          AND a.t_fiid = s.t_fiid AND a.t_iscommon = CHR (88)
                          AND ( (s.t_portfolio = 1
                                 AND llval.t_code = 'ССПУ_ЦБ')
                               OR (s.t_portfolio = 2
                                   AND llval.t_code = 'СССД_ЦБ')
                               OR (s.t_portfolio = 4
                                   AND llval.t_code = 'ПДО')
                               OR (s.t_portfolio = 5
                                   AND llval.t_code = 'АС_ЦБ')))
                        OR (    a.t_catnum = 234                       /*ПВО*/
                            AND a.t_iscommon = CHR (0)
                            AND a.t_docid IN (SELECT t_id
                                                FROM ddl_leg_dbt
                                               WHERE t_dealid = s.t_dealid))
                        OR (    (a.t_catnum = 1253)  /*Наш портфель ПКУ, ц/б*/
                            AND a.t_fiid = s.t_fiid
                            AND s.t_portfolio = 3
                            AND a.t_iscommon = CHR (88))
                        OR ( (    a.t_catnum = 233                     /*БПП*/
                              AND a.t_fiid = s.t_fiid
                              AND a.t_iscommon = CHR (88)
                              AND ( (s.t_portfolio = 1
                                     AND llval.t_code = 'ССПУ_ЦБ')
                                   OR (s.t_portfolio = 2
                                       AND llval.t_code = 'СССД_ЦБ')
                                   OR (s.t_portfolio = 5
                                       AND llval.t_code = 'АС_ЦБ')))))
                   AND ROWNUM = 1; /*с ходу не удалось найти дубль, но с этим надо разобраться*/
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
--               DBMS_OUTPUT.put_line (p_sumid || '   ' || p_date);
               acc := 'Счет не указан';
         END;
      END IF;

      RETURN Acc;
   END;

 FUNCTION GetAccountByLot (p_sumid IN NUMBER, p_date IN DATE)
      RETURN daccount_dbt%ROWTYPE
   AS
      acc    daccount_dbt%ROWTYPE;
      prtf   NUMBER (5) := 0;
   BEGIN
      SELECT s.t_portfolio
        INTO prtf
        FROM v_scwrthistex s
       WHERE  s.t_changedate =
                          (SELECT MAX (t_changedate)
                             FROM v_scwrthistex
                            WHERE t_sumid = s.t_sumid
                                  AND t_changedate <= p_date)
                   AND s.t_sumid = p_sumid
                   AND s.t_instance =
                          (SELECT MAX (t_instance)
                             FROM v_scwrthistex
                            WHERE t_sumid = s.t_sumid
                                  AND t_changedate = s.t_changedate) ;

      IF (prtf = 3 /*ПКУ*/)
      THEN
         BEGIN
            SELECT ac.*
              INTO acc
              FROM       dmcaccdoc_dbt a
                      LEFT JOIN
                         dmctempl_dbt mctempl
                      ON mctempl.t_catid = a.t_catid
                         AND mctempl.t_number = a.t_templnum
                         and mctempl.t_isclose = chr(0)
                   LEFT JOIN
                      dllvalues_dbt llval
                   ON llval.t_list = 1100
                      AND llval.t_element = MCTEMPL.T_VALUE1,
                   v_scwrthistex s,
                   daccount_dbt ac
             WHERE     ac.t_account = a.t_account
             and( (ac.t_close_date >= p_date) or (ac.t_close_date = to_date('01010001','ddmmyyyy')))
                   AND ac.t_chapter = a.t_chapter     /*and s.t_fiid = 1130 */
                   AND s.t_changedate <= p_date
                   AND s.t_instance =
                          (SELECT MAX (t_instance)
                             FROM v_scwrthistex
                            WHERE t_sumid = s.t_sumid
                                  AND t_changedate = s.t_changedate)
                   AND s.t_changedate =
                          (SELECT MAX (t_changedate)
                             FROM v_scwrthistex
                            WHERE t_sumid = s.t_sumid
                                  AND t_changedate <= p_date)
                   AND s.t_sumid = p_sumid
                   --       in (
                   --       select t_sumid from  v_scwrthistex s where s.t_changedate <= TO_DATE('09012019','ddmmyyyy')
                   -- and s.t_instance = (select MAX(t_instance) from v_scwrthistex where t_sumid = s.t_sumid and t_changedate = s.t_changedate)
                   --              )
                   AND a.t_catnum = 1253             /*Наш портфель ПКУ, ц/б*/
                   AND a.t_fiid = s.t_fiid
                   AND s.t_portfolio = 3
                   AND a.t_iscommon = CHR (88)
                   AND ROWNUM = 1;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
--               DBMS_OUTPUT.put_line (p_sumid || '   ' || p_date);
               acc.t_account := 'Счет не указан';
               acc.t_chapter := -999;
         END;
      ELSIF (prtf IN (1, 2, 4, 5)) /*ССПУ_ЦБ, СССД_ЦБ, ПДО,АС_ЦБ*/
      THEN
         BEGIN
            --SELECT ac.t_account,s.t_sumid --into acc
            SELECT ac.*
              INTO acc
              FROM       dmcaccdoc_dbt a
                      LEFT JOIN
                         dmctempl_dbt mctempl
                      ON mctempl.t_catid = a.t_catid
                         AND mctempl.t_number = a.t_templnum
                         and mctempl.t_isclose = chr(0)
                   LEFT JOIN
                      dllvalues_dbt llval
                   ON llval.t_list = 1100
                      AND llval.t_element = MCTEMPL.T_VALUE1,
                   v_scwrthistex s,
                   daccount_dbt ac
             WHERE     ac.t_account = a.t_account
             and( (ac.t_close_date >= p_date) or (ac.t_close_date = to_date('01010001','ddmmyyyy')))
                   AND ac.t_chapter = a.t_chapter     /*and s.t_fiid = 1130 */
                   AND s.t_changedate <= p_date
                   AND s.t_instance =
                          (SELECT MAX (t_instance)
                             FROM v_scwrthistex
                            WHERE t_sumid = s.t_sumid
                                  AND t_changedate = s.t_changedate)
                   AND s.t_changedate =
                          (SELECT MAX (t_changedate)
                             FROM v_scwrthistex
                            WHERE t_sumid = s.t_sumid
                                  AND t_changedate <= p_date)
                   AND s.t_sumid = p_sumid
                   --       in (
                   --       select t_sumid from  v_scwrthistex s where s.t_changedate <= TO_DATE('09012019','ddmmyyyy')
                   -- and s.t_instance = (select MAX(t_instance) from v_scwrthistex where t_sumid = s.t_sumid and t_changedate = s.t_changedate)
                   --              )
                   AND ( (a.t_catnum = 231                    /*Наш портфель*/
                                          AND a.t_fiid = s.t_fiid AND a.t_iscommon = CHR (88)
                          AND ( (s.t_portfolio = 1
                                 AND llval.t_code = 'ССПУ_ЦБ')
                               OR (s.t_portfolio = 2
                                   AND llval.t_code = 'СССД_ЦБ')
                               OR (s.t_portfolio = 4
                                   AND llval.t_code = 'ПДО')
                               OR (s.t_portfolio = 5
                                   AND llval.t_code = 'АС_ЦБ')))
                        --            OR (a.t_catnum = 234 /*ПВО*/
                        --             AND a.t_iscommon = CHR (0)
                        --               and a.t_docid in (select t_id from ddl_leg_dbt where t_dealid = s.t_dealid)
                        --            )
                        --            OR ((a.t_catnum = 1253) /*Наш портфель ПКУ, ц/б*/
                        --                   AND a.t_fiid = s.t_fiid
                        --                    AND s.t_portfolio = 3
                        --                   AND a.t_iscommon = CHR (88)
                        --               )
                        OR ( (    a.t_catnum = 233                     /*БПП*/
                              AND a.t_fiid = s.t_fiid
                              AND a.t_iscommon = CHR (88)
                              AND ( (s.t_portfolio = 1
                                     AND llval.t_code = 'ССПУ_ЦБ')
                                   OR (s.t_portfolio = 2
                                       AND llval.t_code = 'СССД_ЦБ')
                                   OR (s.t_portfolio = 5
                                       AND llval.t_code = 'АС_ЦБ')))))
                   AND ROWNUM = 1; /*с ходу не удалось найти дубль, но с этим надо разобраться*/
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
--               DBMS_OUTPUT.put_line (p_sumid || '   ' || p_date);
               acc.t_account := 'Счет не указан';
               acc.t_chapter := -999;
         END;
      ELSIF (prtf = 6) /*ПВО*/
      THEN
         BEGIN
            SELECT ac.*
              INTO acc
              FROM       dmcaccdoc_dbt a
                      LEFT JOIN
                         dmctempl_dbt mctempl
                      ON mctempl.t_catid = a.t_catid
                         AND mctempl.t_number = a.t_templnum
                         and mctempl.t_isclose = chr(0)
                   LEFT JOIN
                      dllvalues_dbt llval
                   ON llval.t_list = 1100
                      AND llval.t_element = MCTEMPL.T_VALUE1,
                   v_scwrthistex s,
                   daccount_dbt ac
             WHERE     ac.t_account = a.t_account
             and( (ac.t_close_date >= p_date) or (ac.t_close_date = to_date('01010001','ddmmyyyy')))
                   AND ac.t_chapter = a.t_chapter     /*and s.t_fiid = 1130 */
                   AND s.t_changedate <= p_date
                   AND s.t_instance =
                          (SELECT MAX (t_instance)
                             FROM v_scwrthistex
                            WHERE t_sumid = s.t_sumid
                                  AND t_changedate = s.t_changedate)
                   AND s.t_changedate =
                          (SELECT MAX (t_changedate)
                             FROM v_scwrthistex
                            WHERE t_sumid = s.t_sumid
                                  AND t_changedate <= p_date)
                   AND s.t_sumid = p_sumid
                   AND ( (    a.t_catnum = 234                         /*ПВО*/
                          AND a.t_iscommon = CHR (0)
                          AND a.t_docid IN (SELECT t_id
                                              FROM ddl_leg_dbt
                                             WHERE t_dealid = s.t_dealid))
                        );
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
--               DBMS_OUTPUT.put_line (p_sumid || '   ' || p_date);
               acc.t_account := 'Счет не указан';
               acc.t_chapter := -999;
         END;
      ELSE /* КСУшные портфели и если что-то потерялось в предыдущих, то идем сюда*/
         BEGIN
--               DBMS_OUTPUT.put_line (' ---------------------------------------');
            --SELECT ac.t_account,s.t_sumid --into acc
            SELECT ac.*
              INTO acc
              FROM       dmcaccdoc_dbt a
                      LEFT JOIN
                         dmctempl_dbt mctempl
                      ON mctempl.t_catid = a.t_catid
                         AND mctempl.t_number = a.t_templnum
                   LEFT JOIN
                      dllvalues_dbt llval
                   ON llval.t_list = 1100
                      AND llval.t_element = MCTEMPL.T_VALUE1,
                   v_scwrthistex s,
                   daccount_dbt ac
             WHERE     ac.t_account = a.t_account
                   AND ac.t_chapter = a.t_chapter     /*and s.t_fiid = 1130 */
                   AND s.t_changedate <= p_date
                   AND s.t_instance =
                          (SELECT MAX (t_instance)
                             FROM v_scwrthistex
                            WHERE t_sumid = s.t_sumid
                                  AND t_changedate = s.t_changedate)
                   AND s.t_changedate =
                          (SELECT MAX (t_changedate)
                             FROM v_scwrthistex
                            WHERE t_sumid = s.t_sumid
                                  AND t_changedate <= p_date)
                   AND s.t_sumid = p_sumid
                   --       in (
                   --       select t_sumid from  v_scwrthistex s where s.t_changedate <= TO_DATE('09012019','ddmmyyyy')
                   -- and s.t_instance = (select MAX(t_instance) from v_scwrthistex where t_sumid = s.t_sumid and t_changedate = s.t_changedate)
                   --              )
                   AND ( (a.t_catnum = 231                    /*Наш портфель*/
                                          AND a.t_fiid = s.t_fiid AND a.t_iscommon = CHR (88)
                          AND ( (s.t_portfolio = 1
                                 AND llval.t_code = 'ССПУ_ЦБ')
                               OR (s.t_portfolio = 2
                                   AND llval.t_code = 'СССД_ЦБ')
                               OR (s.t_portfolio = 4
                                   AND llval.t_code = 'ПДО')
                               OR (s.t_portfolio = 5
                                   AND llval.t_code = 'АС_ЦБ')))
                        OR (    a.t_catnum = 234                       /*ПВО*/
                            AND a.t_iscommon = CHR (0)
                            AND a.t_docid IN (SELECT t_id
                                                FROM ddl_leg_dbt
                                               WHERE t_dealid = s.t_dealid))
                        OR (    (a.t_catnum = 1253)  /*Наш портфель ПКУ, ц/б*/
                            AND a.t_fiid = s.t_fiid
                            AND s.t_portfolio = 3
                            AND a.t_iscommon = CHR (88))
                        OR ( (    a.t_catnum = 233                     /*БПП*/
                              AND a.t_fiid = s.t_fiid
                              AND a.t_iscommon = CHR (88)
                              AND ( (s.t_portfolio = 1
                                     AND llval.t_code = 'ССПУ_ЦБ')
                                   OR (s.t_portfolio = 2
                                       AND llval.t_code = 'СССД_ЦБ')
                                   OR (s.t_portfolio = 5
                                       AND llval.t_code = 'АС_ЦБ')))))
                   AND ROWNUM = 1; /*с ходу не удалось найти дубль, но с этим надо разобраться*/
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
--               DBMS_OUTPUT.put_line (p_sumid || '   ' || p_date);
               acc.t_account := 'Счет не указан';
               acc.t_chapter := -999;
         END;
      END IF;

      RETURN Acc;
   END;

   /* Определение даты поставки */
   FUNCTION GetSetBppDate (pSumID IN NUMBER)
      RETURN DATE
   IS
      res   DATE;
   BEGIN
      SELECT t.t_changedate
        INTO res
        FROM v_scwrthistex t
       WHERE t.t_SumID = pSumID
             AND t.t_instance =
                    (SELECT MIN (t_instance)
                       FROM v_scwrthistex
                      WHERE t_sumid = t.t_sumid AND t_action = 50);

      RETURN res;
   EXCEPTION
      WHEN OTHERS
      THEN
         RETURN NULL;
   END GetSetBppDate;

   /* значение категории для объекта */
   FUNCTION GetObjAttrValue (pobjtype    IN NUMBER,
                             pobjectid   IN NUMBER,
                             pgroupid    IN NUMBER,
                             pdate       IN DATE)
      RETURN VARCHAR2
   IS
      vnum   VARCHAR2 (35);
   BEGIN
      SELECT Attr.t_NumInList
        INTO vnum
        FROM dobjatcor_dbt AtCor, dobjattr_dbt Attr
       WHERE     AtCor.t_ObjectType = pobjtype
             AND AtCor.t_GroupID = pgroupid
             AND AtCor.t_Object = LPAD (pobjectid, 10, '0')
             AND AtCor.t_ValidFromDate =
                    (SELECT MAX (t.T_ValidFromDate)
                       FROM DOBJATCOR_DBT t
                      WHERE     t.T_ObjectType = AtCor.T_ObjectType
                            AND t.T_GroupID = AtCor.T_GroupID
                            AND t.t_Object = AtCor.t_Object
                            AND t.T_ValidFromDate <= pdate)
             AND Attr.t_AttrID = AtCor.t_AttrID
             AND Attr.t_ObjectType = AtCor.t_ObjectType
             AND Attr.t_GroupID = AtCor.t_GroupID;

      RETURN vnum;
   EXCEPTION
      WHEN OTHERS
      THEN
         RETURN NULL;
   END;


   FUNCTION sectype (fiid IN NUMBER)
      RETURN VARCHAR2
   IS
      result   VARCHAR2 (35);
   BEGIN
      SELECT a.t_name
        INTO result
        FROM dobjatcor_dbt o, dobjattr_dbt a
       WHERE     o.t_objecttype = 12
             AND o.t_groupid = 1
             AND o.t_object = LPAD (fiid, 10, CHR (48))
             AND o.t_validtodate = TO_DATE ('31.12.9999', 'dd.mm.yyyy')
             AND a.t_objecttype = o.t_objecttype
             AND a.t_groupid = o.t_groupid
             AND a.t_attrid = o.t_attrid;

      RETURN result;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RETURN NULL;
   END;

   FUNCTION DefineIsNFO (partyid IN dparty_dbt.t_partyid%TYPE)
      RETURN VARCHAR2
   IS
      islegalform               CHAR (1);
      isresident                CHAR (1);
      isbank                    CHAR (1);
      isokved6group             CHAR (1);
      PARTYKIND_BANK   CONSTANT INTEGER := 2;
      CATEG_OKVED      CONSTANT INTEGER := 17;
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

   /* значение категории для объекта с учетом иерархии*/
   FUNCTION GetObjAttrValueParent (pobjtype     IN NUMBER,
                                   pobjectid    IN NUMBER,
                                   pgroupid     IN NUMBER,
                                   pnuminlist   IN VARCHAR2,
                                   pdate        IN DATE)
      RETURN VARCHAR2
   IS
      vname   VARCHAR2 (4);
   BEGIN
      SELECT SUBSTR (Attr.t_Name, 1, 4)
        INTO vname
        FROM dobjatcor_dbt AtCor, dobjattr_dbt Attr
       WHERE     AtCor.t_ObjectType = pobjtype
             AND AtCor.t_GroupID = pgroupid
             AND AtCor.t_Object = LPAD (pobjectid, 10, '0')
             AND AtCor.t_ValidFromDate =
                    (SELECT MAX (t.T_ValidFromDate)
                       FROM DOBJATCOR_DBT t
                      WHERE     t.T_ObjectType = AtCor.T_ObjectType
                            AND t.T_GroupID = AtCor.T_GroupID
                            AND t.t_Object = AtCor.t_Object
                            AND t.T_ValidFromDate <= pdate)
             AND Attr.t_AttrID = AtCor.t_AttrID
             AND Attr.t_ObjectType = AtCor.t_ObjectType
             AND Attr.t_GroupID = AtCor.t_GroupID
             AND ATTR.T_PARENTID =
                    (SELECT t_attrid
                       FROM dobjattr_dbt pAttr
                      WHERE     pAttr.t_objecttype = Attr.t_ObjectType
                            AND pAttr.t_groupid = Attr.t_GroupID
                            AND pAttr.t_numinlist = pnuminlist);

      RETURN vname;
   EXCEPTION
      WHEN OTHERS
      THEN
         RETURN NULL;
   END;

/*
   /*Заполнить таблицу информацией по портфелям*/
   FUNCTION FetchPortfolioData ( pdate IN DATE, p_infiid IN NUMBER)
      RETURN number
   IS
      rez number := 0;
   BEGIN
     RepDate := pdate;

     EXECUTE IMMEDIATE 'TRUNCATE TABLE dportfolio657_1'; -- очистка временой на всякий случай

     /*Отбираются :
        а) не проданные по состоянию на начало отчетной даты лоты обычных покупок и операций зачисления,
           статус которых равен "поставлен", относящиеся к данному "учетному портфелю".
        б) по незакрытым на дату отчета сделкам РЕПО отбираются лоты статусом "поставлен" или "не по-ставлен".
     */

     prev_fininstr := NULL;

     FOR department IN (SELECT t_Code FROM ddp_dep_dbt)
     LOOP                                              -- по всем подразделениям
        FOR pmwrtsum
           IN (/*БПП*/
               SELECT *
                 FROM (SELECT s.t_fiid,
                              s.t_dealid,
                              s.t_buy_sale,
                              s.t_parent,
                              s.t_sumid,
                              s.t_instance,
                              s.t_amount,
                              s.t_amountbd,
                              s.t_changedate,
                              s.t_portfolio,
                              s.t_state,
                              s.t_enterdate,
                              s.t_docid,
                              s.t_dockind,
                              s.t_partnum,
                              s.t_kind
                         FROM dpmwrtsum_dbt s
                        WHERE s.t_Department = department.t_code
                              AND s.t_buy_sale =
                                     RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY
                              AND s.t_party = -1
                              AND s.t_dockind =
                                     PM_COMMON.DLDOC_PAYMENT
                              AND s.t_kind = 210
                              AND s.t_portfolio IN
                                     (RSB_PMWRTOFF.KINDPORT_TRADE,
                                      RSB_PMWRTOFF.KINDPORT_SALE,
                                      RSB_PMWRTOFF.KINDPORT_RETIRE,
                                      RSB_PMWRTOFF.KINDPORT_CONTR)
                              AND s.t_state =
                                     RSB_PMWRTOFF.PM_WRTSUM_SALE_BPP
                              AND s.t_changedate <= RepDate
                              AND (p_infiid = -1 OR p_infiid = s.t_fiid)
                       UNION
                       SELECT m.t_fiid,
                              m.t_dealid,
                              m.t_buy_sale,
                              m.t_parent,
                              t.t_sumid,
                              t.t_instance,
                              t.t_amount,
                              t.t_amountbd,
                              t.t_changedate,
                              m.t_portfolio,
                              t.t_state,
                              m.t_enterdate,
                              m.t_docid,
                              m.t_dockind,
                              m.t_partnum,
                              m.t_kind
                         FROM dpmwrtbc_dbt t, dpmwrtsum_dbt m
                        WHERE t.t_sumid IN
                                 (SELECT s.t_sumid
                                    FROM dpmwrtsum_dbt s
                                   WHERE s.t_Department =
                                            department.t_code
                                         AND s.t_buy_sale =
                                                RSB_PMWRTOFF.
                                                 PM_WRITEOFF_SUM_BUY
                                         AND s.t_party = -1
                                         AND s.t_dockind =
                                                PM_COMMON.
                                                 DLDOC_PAYMENT
                                         AND s.t_kind = 210
                                         AND s.t_portfolio IN
                                                (RSB_PMWRTOFF.
                                                  KINDPORT_TRADE,
                                                 RSB_PMWRTOFF.
                                                  KINDPORT_SALE,
                                                 RSB_PMWRTOFF.
                                                  KINDPORT_RETIRE,
                                                 RSB_PMWRTOFF.
                                                  KINDPORT_CONTR)
                                         AND s.t_changedate > RepDate)
                              AND t.t_state =
                                     RSB_PMWRTOFF.PM_WRTSUM_SALE_BPP
                              AND t.t_instance =
                                     (SELECT MAX (v.t_instance)
                                        FROM dpmwrtbc_dbt v
                                       WHERE v.t_sumid = t.t_sumid
                                             AND v.t_changedate <=
                                                    RepDate)
                              AND m.t_sumid = t.t_sumid
                              AND (p_infiid = -1
                                   OR p_infiid = t.t_fiid))
               /*"ТП" /"ППР" / "ПУДП" / "ПДО" / "ПКУ"*/
               UNION
               /*по kind = 210 отберем лоты, вернувшиеся из БПП
                           20 обычные покупки
                           40,110, 130 зачисления, зачисления при конвертации, зачисления в глоб. опции
                           90  перемещения в портфель*/
               SELECT *
                 FROM (SELECT s.t_fiid,
                              s.t_dealid,
                              s.t_buy_sale,
                              s.t_parent,
                              s.t_sumid,
                              s.t_instance,
                              s.t_amount,
                              s.t_amountbd,
                              s.t_changedate,
                              s.t_portfolio,
                              s.t_state,
                              s.t_enterdate,
                              s.t_docid,
                              s.t_dockind,
                              s.t_partnum,
                              s.t_kind
                         FROM dpmwrtsum_dbt s
                        WHERE s.t_Department = department.t_code
                              AND s.t_buy_sale =
                                     RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY
                              AND s.t_party = -1
                              AND s.t_dockind IN
                                     (PM_COMMON.DLDOC_PAYMENT,
                                      Rsb_Secur.DL_ISSUE_UNION,
                                      Rsb_Secur.DL_MOVINGDOC)
                              AND s.t_dealid > 0
                              AND s.t_kind IN
                                     (20, 210, 40, 110, 130, 90)
                              AND s.t_Portfolio IN
                                     (RSB_PMWRTOFF.KINDPORT_TRADE,
                                      RSB_PMWRTOFF.KINDPORT_SALE,
                                      RSB_PMWRTOFF.KINDPORT_CONTR,
                                      RSB_PMWRTOFF.KINDPORT_RETIRE,
                                      RSB_PMWRTOFF.
                                       KINDPORT_PROMISSORY)
                              AND s.t_state =
                                     RSB_PMWRTOFF.PM_WRTSUM_FORM
                              AND s.t_Amount != 0
                              AND s.t_changedate <= RepDate
                              AND ( (p_infiid = -1)
                                   OR (p_infiid = s.t_fiid))
                       UNION
                       SELECT m.t_fiid,
                              m.t_dealid,
                              m.t_buy_sale,
                              m.t_parent,
                              t.t_sumid,
                              t.t_instance,
                              t.t_amount,
                              t.t_amountbd,
                              t.t_changedate,
                              m.t_portfolio,
                              t.t_state,
                              m.t_enterdate,
                              m.t_docid,
                              m.t_dockind,
                              m.t_partnum,
                              m.t_kind
                         FROM dpmwrtbc_dbt t, dpmwrtsum_dbt m
                        WHERE t.t_sumid IN
                                 (SELECT s.t_sumid
                                    FROM dpmwrtsum_dbt s
                                   WHERE s.t_Department =
                                            department.t_code
                                         AND s.t_buy_sale =
                                                RSB_PMWRTOFF.
                                                 PM_WRITEOFF_SUM_BUY
                                         AND s.t_party = -1
                                         AND s.t_dockind IN
                                                (PM_COMMON.
                                                  DLDOC_PAYMENT,
                                                 Rsb_Secur.
                                                  DL_ISSUE_UNION,
                                                 Rsb_Secur.
                                                  DL_MOVINGDOC)
                                         AND s.t_dealid > 0
                                         AND s.t_kind IN
                                                (20,
                                                 210,
                                                 40,
                                                 110,
                                                 130,
                                                 90)
                                         AND s.t_Portfolio IN
                                                (RSB_PMWRTOFF.
                                                  KINDPORT_TRADE,
                                                 RSB_PMWRTOFF.
                                                  KINDPORT_SALE,
                                                 RSB_PMWRTOFF.
                                                  KINDPORT_CONTR,
                                                 RSB_PMWRTOFF.
                                                  KINDPORT_RETIRE,
                                                 RSB_PMWRTOFF.
                                                  KINDPORT_PROMISSORY)
                                         AND s.t_changedate > RepDate)
                              AND t.t_state =
                                     RSB_PMWRTOFF.PM_WRTSUM_FORM
                              AND t.t_Amount != 0
                              AND t.t_instance =
                                     (SELECT MAX (v.t_instance)
                                        FROM dpmwrtbc_dbt v
                                       WHERE v.t_sumid = t.t_sumid
                                             AND v.t_changedate <=
                                                    RepDate)
                              AND m.t_sumid = t.t_sumid
                              AND ( (p_infiid = -1)
                                   OR (p_infiid = t.t_fiid)))
               /* ПВО (РЕПО/зачисления) */
               UNION
               SELECT *
                 FROM (SELECT s.t_fiid,
                              s.t_dealid,
                              s.t_buy_sale,
                              s.t_parent,
                              s.t_sumid,
                              s.t_instance,
                              s.t_amount,
                              s.t_amountbd,
                              s.t_changedate,
                              s.t_portfolio,
                              s.t_state,
                              s.t_enterdate,
                              s.t_docid,
                              s.t_dockind,
                              s.t_partnum,
                              s.t_kind
                         FROM dpmwrtsum_dbt s
                        WHERE s.t_Department = department.t_code
                              AND s.t_Party = -1
                              AND s.t_dockind IN
                                     (PM_COMMON.DLDOC_PAYMENT,
                                      Rsb_Secur.DL_ISSUE_UNION)
                              AND ( (s.t_State =
                                        RSB_PMWRTOFF.PM_WRTSUM_FORM
                                     AND s.t_DealID =
                                            (SELECT tick.t_DealID
                                               FROM ddl_tick_dbt tick
                                              WHERE tick.t_DealID =
                                                       s.t_DealID
                                                    AND rsb_secur.
                                                         IsRepo (
                                                           rsb_secur.
                                                            get_OperationGroup (
                                                              rsb_secur.
                                                               get_OperSysTypes (
                                                                 tick.
                                                                  t_DealType,
                                                                 tick.
                                                                  t_BofficeKind))) =
                                                           1
                                                    AND (tick.
                                                          t_DealStatus <
                                                            20
                                                         OR (tick.
                                                              t_DealStatus =
                                                                20
                                                             AND tick.
                                                                  t_CloseDate >
                                                                    RepDate)))
                                     AND s.t_Buy_Sale =
                                            RSB_PMWRTOFF.
                                             PM_WRITEOFF_SUM_BUY_BO)
                                   OR s.t_kind IN (40, 110, 130))
                              AND s.t_Portfolio =
                                     RSB_PMWRTOFF.KINDPORT_BACK
                              AND s.t_Amount != 0
                              AND s.t_AmountBD = 0
                              AND s.t_changedate <= RepDate
                              AND ( (p_infiid = -1)
                                   OR (p_infiid = s.t_fiid))
                       UNION
                       SELECT m.t_fiid,
                              m.t_dealid,
                              m.t_buy_sale,
                              m.t_parent,
                              t.t_sumid,
                              t.t_instance,
                              t.t_amount,
                              t.t_amountbd,
                              t.t_changedate,
                              m.t_portfolio,
                              t.t_state,
                              m.t_enterdate,
                              m.t_docid,
                              m.t_dockind,
                              m.t_partnum,
                              m.t_kind
                         FROM dpmwrtbc_dbt t, dpmwrtsum_dbt m
                        WHERE t.t_sumid IN
                                 (SELECT s.t_sumid
                                    FROM dpmwrtsum_dbt s
                                   WHERE s.t_Department =
                                            department.t_code
                                         AND s.t_Party = -1
                                         AND s.t_dockind IN
                                                (PM_COMMON.
                                                  DLDOC_PAYMENT,
                                                 Rsb_Secur.
                                                  DL_ISSUE_UNION)
                                         AND ( (s.t_State =
                                                   RSB_PMWRTOFF.
                                                    PM_WRTSUM_FORM
                                                AND s.t_DealID =
                                                       (SELECT tick.
                                                                t_DealID
                                                          FROM ddl_tick_dbt tick
                                                         WHERE tick.
                                                                t_DealID =
                                                                  s.
                                                                   t_DealID
                                                               AND rsb_secur.
                                                                    IsRepo (
                                                                      rsb_secur.
                                                                       get_OperationGroup (
                                                                         rsb_secur.
                                                                          get_OperSysTypes (
                                                                            tick.
                                                                             t_DealType,
                                                                            tick.
                                                                             t_BofficeKind))) =
                                                                      1
                                                               AND (tick.
                                                                     t_DealStatus <
                                                                       20
                                                                    OR (tick.
                                                                         t_DealStatus =
                                                                           20
                                                                        AND tick.
                                                                             t_CloseDate >
                                                                               RepDate)))
                                                AND s.t_Buy_Sale =
                                                       RSB_PMWRTOFF.
                                                        PM_WRITEOFF_SUM_BUY_BO)
                                              OR s.t_kind IN
                                                    (40, 110, 130))
                                         AND s.t_Portfolio =
                                                RSB_PMWRTOFF.
                                                 KINDPORT_BACK
                                         AND s.t_changedate > RepDate)
                              AND t.t_Amount != 0
                              AND t.t_AmountBD = 0
                              AND t.t_instance =
                                     (SELECT MAX (v.t_instance)
                                        FROM dpmwrtbc_dbt v
                                       WHERE v.t_sumid = t.t_sumid
                                             AND v.t_changedate <=
                                                    RepDate)
                              AND m.t_sumid = t.t_sumid
                              AND ( (p_infiid = -1)
                                   OR (p_infiid = t.t_fiid)))
               /* +\- ОД */
               UNION
               SELECT *
                 FROM (SELECT s.t_fiid,
                              s.t_dealid,
                              s.t_buy_sale,
                              s.t_parent,
                              s.t_sumid,
                              s.t_instance,
                              s.t_amount,
                              s.t_amountbd,
                              s.t_changedate,
                              s.t_portfolio,
                              s.t_state,
                              s.t_enterdate,
                              s.t_docid,
                              s.t_dockind,
                              s.t_partnum,
                              s.t_kind
                         FROM dpmwrtsum_dbt s
                        WHERE s.t_Department = department.t_code
                              AND s.t_Party = -1
                              AND s.t_dockind =
                                     PM_COMMON.DLDOC_PAYMENT
                              AND s.t_DealID =
                                     (SELECT tick.t_DealID
                                        FROM ddl_tick_dbt tick
                                       WHERE tick.t_DealID =
                                                s.t_DealID
                                             AND rsb_secur.
                                                  IsRepo (
                                                    rsb_secur.
                                                     get_OperationGroup (
                                                       rsb_secur.
                                                        get_OperSysTypes (
                                                          tick.
                                                           t_DealType,
                                                          tick.
                                                           t_BofficeKind))) =
                                                    1
                                             AND (tick.t_DealStatus <
                                                     20
                                                  OR (tick.
                                                       t_DealStatus =
                                                         20
                                                      AND tick.
                                                           t_CloseDate >
                                                             RepDate)))
                              AND (s.t_Portfolio =
                                      RSB_PMWRTOFF.KINDPORT_BASICDEBT
                                   OR s.t_Buy_Sale =
                                         RSB_PMWRTOFF.
                                          PM_WRITEOFF_SUM_SALE)
                              AND s.t_AmountBD != 0
                              AND s.t_state =
                                     RSB_PMWRTOFF.PM_WRTSUM_NOTFORM
                              AND s.t_changedate <= RepDate
                              AND ( (p_infiid = -1)
                                   OR (p_infiid = s.t_fiid))
                       UNION
                       SELECT m.t_fiid,
                              m.t_dealid,
                              m.t_buy_sale,
                              m.t_parent,
                              t.t_sumid,
                              t.t_instance,
                              t.t_amount,
                              t.t_amountbd,
                              t.t_changedate,
                              m.t_portfolio,
                              t.t_state,
                              m.t_enterdate,
                              m.t_docid,
                              m.t_dockind,
                              m.t_partnum,
                              m.t_kind
                         FROM dpmwrtbc_dbt t, dpmwrtsum_dbt m
                        WHERE t.t_sumid IN
                                 (SELECT s.t_sumid
                                    FROM dpmwrtsum_dbt s
                                   WHERE s.t_Department =
                                            department.t_code
                                         AND s.t_Party = -1
                                         AND s.t_dockind =
                                                PM_COMMON.
                                                 DLDOC_PAYMENT
                                         AND s.t_DealID =
                                                (SELECT tick.t_DealID
                                                   FROM ddl_tick_dbt tick
                                                  WHERE tick.t_DealID =
                                                           s.t_DealID
                                                        AND rsb_secur.
                                                             IsRepo (
                                                               rsb_secur.
                                                                get_OperationGroup (
                                                                  rsb_secur.
                                                                   get_OperSysTypes (
                                                                     tick.
                                                                      t_DealType,
                                                                     tick.
                                                                      t_BofficeKind))) =
                                                               1
                                                        AND (tick.
                                                              t_DealStatus <
                                                                20
                                                             OR (tick.
                                                                  t_DealStatus =
                                                                    20
                                                                 AND tick.
                                                                      t_CloseDate >
                                                                        RepDate)))
                                         AND (s.t_Portfolio =
                                                 RSB_PMWRTOFF.
                                                  KINDPORT_BASICDEBT
                                              OR s.t_Buy_Sale =
                                                    RSB_PMWRTOFF.
                                                     PM_WRITEOFF_SUM_SALE)
                                         AND s.t_changedate > RepDate)
                              AND t.t_AmountBD != 0
                              AND t.t_state =
                                     RSB_PMWRTOFF.PM_WRTSUM_NOTFORM
                              AND t.t_instance =
                                     (SELECT MAX (v.t_instance)
                                        FROM dpmwrtbc_dbt v
                                       WHERE v.t_sumid = t.t_sumid
                                             AND v.t_changedate <=
                                                    RepDate)
                              AND m.t_sumid = t.t_sumid
                              AND ( (p_infiid = -1)
                                   OR (p_infiid = t.t_fiid)))
               /* ПВО_КСУ (РЕПО/зачисления) */
               UNION
               SELECT *
                 FROM (SELECT s.t_fiid,
                              s.t_dealid,
                              s.t_buy_sale,
                              s.t_parent,
                              s.t_sumid,
                              s.t_instance,
                              s.t_amount,
                              s.t_amountbd,
                              s.t_changedate,
                              s.t_portfolio,
                              s.t_state,
                              s.t_enterdate,
                              s.t_docid,
                              s.t_dockind,
                              s.t_partnum,
                              s.t_kind
                         FROM dpmwrtsum_dbt s
                        WHERE s.t_Department = department.t_code
                              AND s.t_Party = -1
                              AND s.t_dockind =
                                     PM_COMMON.DLDOC_PAYMENT
                              AND s.t_State =
                                     RSB_PMWRTOFF.PM_WRTSUM_FORM
                              AND s.t_Portfolio =
                                     RSB_PMWRTOFF.KINDPORT_BACK_KSU
                              AND s.t_Amount != 0
                              AND s.t_AmountBD = 0
                              AND s.t_changedate <= RepDate
                              AND ( (p_infiid = -1)
                                   OR (p_infiid = s.t_fiid))
                       UNION
                       SELECT m.t_fiid,
                              m.t_dealid,
                              m.t_buy_sale,
                              m.t_parent,
                              t.t_sumid,
                              t.t_instance,
                              t.t_amount,
                              t.t_amountbd,
                              t.t_changedate,
                              m.t_portfolio,
                              t.t_state,
                              m.t_enterdate,
                              m.t_docid,
                              m.t_dockind,
                              m.t_partnum,
                              m.t_kind
                         FROM dpmwrtbc_dbt t, dpmwrtsum_dbt m
                        WHERE t.t_sumid IN
                                 (SELECT s.t_sumid
                                    FROM dpmwrtsum_dbt s
                                   WHERE s.t_Department =
                                            department.t_code
                                         AND s.t_Party = -1
                                         AND s.t_dockind =
                                                PM_COMMON.
                                                 DLDOC_PAYMENT
                                         AND s.t_State =
                                                RSB_PMWRTOFF.
                                                 PM_WRTSUM_FORM
                                         AND s.t_Portfolio =
                                                RSB_PMWRTOFF.
                                                 KINDPORT_BACK_KSU
                                         AND s.t_changedate > RepDate)
                              AND t.t_Amount != 0
                              AND t.t_AmountBD = 0
                              AND t.t_instance =
                                     (SELECT MAX (v.t_instance)
                                        FROM dpmwrtbc_dbt v
                                       WHERE v.t_sumid = t.t_sumid
                                             AND v.t_changedate <=
                                                    RepDate)
                              AND m.t_sumid = t.t_sumid
                              AND ( (p_infiid = -1)
                                   OR (p_infiid = t.t_fiid)))
               /* ПВО_БПП_КСУ */
               UNION
               SELECT s.t_fiid,
                      s.t_dealid,
                      s.t_buy_sale,
                      s.t_parent,
                      s.t_sumid,
                      s.t_instance,
                      s.t_amount,
                      s.t_amountbd,
                      s.t_changedate,
                      s.t_portfolio,
                      s.t_state,
                      s.t_enterdate,
                      s.t_docid,
                      s.t_dockind,
                      s.t_partnum,
                      s.t_kind
                 FROM v_scwrthistex s, dpmwrtsum_dbt buy1
                WHERE     s.t_Department = department.t_code
                      AND s.t_Party = -1
                      AND s.t_dockind = PM_COMMON.DLDOC_PAYMENT
                      AND s.t_State = RSB_PMWRTOFF.PM_WRTSUM_NOTFORM
                      AND s.t_DealID =
                             (SELECT tick.t_DealID
                                FROM ddl_tick_dbt tick
                               WHERE tick.t_DealID = s.t_DealID
                                     AND (tick.t_DealStatus < 20
                                          OR (tick.t_DealStatus = 20
                                              AND (tick.t_CloseDate > RepDate
                                                   OR rsb_secur.
                                                       DealIsRepo (
                                                         tick.t_DealID) = 0))))
                      AND s.t_Amount > 0
                      AND s.t_instance =
                             (SELECT MAX (t_instance)
                                FROM v_scwrthistex
                               WHERE t_sumid = s.t_sumid
                                     AND t_changedate <= RepDate)
                      AND s.t_Portfolio = RSB_PMWRTOFF.KINDPORT_BACK_BPP_KSU
                      AND s.t_Buy_Sale = RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY
                      AND buy1.t_SumID = s.t_Source
                      AND buy1.t_Portfolio = RSB_PMWRTOFF.KINDPORT_BACK_KSU
                      AND ( (p_infiid = -1) OR (p_infiid = s.t_fiid)))
        LOOP
           BEGIN
              IF prev_fininstr IS NULL OR pmwrtsum.t_FIID <> prev_fininstr
              THEN
                 SELECT *
                   INTO fininstr
                   FROM dfininstr_dbt
                  WHERE t_fiid = pmwrtsum.t_FIID;

                 prev_fininstr := fininstr.t_FIID;
              END IF;

              IF fininstr.t_AvoirKind = AVOIRISSKIND_INVESTMENT_SHARE
              THEN
                 BEGIN
                    SELECT t_formvaluefiid
                      INTO fininstr.t_FaceValueFI
                      FROM davrinvst_dbt
                     WHERE t_fiid = fininstr.t_FIID;
                 EXCEPTION
                    WHEN NO_DATA_FOUND
                    THEN
                       NULL;
                 END;
              END IF;

              SELECT *
                INTO avoiriss
                FROM davoiriss_dbt
               WHERE t_fiid = pmwrtsum.t_FIID;

              SELECT *
                INTO dl_tick
                FROM ddl_tick_dbt
               WHERE t_dealid = pmwrtsum.t_dealid;

              -- группа операций
              SELECT t_sysTypes, t_DocKind
                INTO vSystypes, vDocKind
                FROM doprkoper_dbt
               WHERE t_kind_operation = dl_tick.t_DealType;

              vGroup := Rsb_Secur.get_OperationGroup (vSystypes);

              IsQuoted := 0;

              IF tss_prev_FIID = avoiriss.t_FIID
              THEN
                 IsQuoted := tss_prev_NumInList;
              ELSE
                 tss_prev_FIID := avoiriss.t_FIID;
                 tss_prev_NumInList := 0;

                 vIsQuoted :=
                    GetObjAttrValue (cnst.OBJTYPE_AVOIRISS,
                                     tss_prev_FIID,
                                     27,
                                     pdate);

                 IF vIsQuoted IS NULL
                 THEN
                    IsQuoted := 0;
                 ELSE
                    IsQuoted := TO_NUMBER (vIsQuoted);
                 END IF;

                 tss_prev_NumInList := IsQuoted;
              END IF;


              /* Получим учетный портфель. */
              IF     pmwrtsum.t_Portfolio = RSB_PMWRTOFF.KINDPORT_TRADE
                 AND pmwrtsum.t_State = RSB_PMWRTOFF.PM_WRTSUM_FORM
                 AND pmwrtsum.t_Amount != 0
                 AND pmwrtsum.t_Buy_Sale = RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY
              THEN
                 PortfID := PortfID_Trade;
              ELSIF     pmwrtsum.t_Portfolio = RSB_PMWRTOFF.KINDPORT_SALE
                    AND pmwrtsum.t_State = RSB_PMWRTOFF.PM_WRTSUM_FORM
                    AND pmwrtsum.t_Amount != 0
                    AND pmwrtsum.t_Buy_Sale = RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY
              THEN
                 PortfID := PortfID_Sale;
              ELSIF     pmwrtsum.t_Portfolio = RSB_PMWRTOFF.KINDPORT_RETIRE
                    AND pmwrtsum.t_State = RSB_PMWRTOFF.PM_WRTSUM_FORM
                    AND pmwrtsum.t_Amount != 0
                    AND pmwrtsum.t_Buy_Sale = RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY
              THEN
                 PortfID := PortfID_Retire;
              ELSIF     pmwrtsum.t_Portfolio = RSB_PMWRTOFF.KINDPORT_CONTR
                    AND pmwrtsum.t_State = RSB_PMWRTOFF.PM_WRTSUM_FORM
                    AND pmwrtsum.t_Amount != 0
                    AND pmwrtsum.t_Buy_Sale = RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY
              THEN
                 PortfID := PortfID_Contr;
              ELSIF     pmwrtsum.t_Portfolio = RSB_PMWRTOFF.KINDPORT_PROMISSORY
                    AND pmwrtsum.t_State = RSB_PMWRTOFF.PM_WRTSUM_FORM
                    AND pmwrtsum.t_Amount != 0
                    AND pmwrtsum.t_Buy_Sale = RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY
              THEN
                 PortfID := PortfID_Promissory;
              /*В портфель ПВО отбираются лоты, изначально купленные по сделкам обратного РЕПО БПП,
                не проданные впоследствии*/
              ELSIF ( (Rsb_Secur.IsREPO (vGroup) = 1
                       AND pmwrtsum.t_Buy_Sale =
                              RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY_BO)
                     OR Rsb_Secur.IsAVRWRTIN (vGroup) = 1)
                    AND (    pmwrtsum.t_Portfolio = RSB_PMWRTOFF.KINDPORT_BACK
                         AND pmwrtsum.t_State = RSB_PMWRTOFF.PM_WRTSUM_FORM
                         AND pmwrtsum.t_Amount != 0
                         AND pmwrtsum.t_AmountBD = 0)
              THEN
                 PortfID := PortfID_Back;
              /*Для портфеля БПП в отчет отбираются лоты, которые проданы из портфелей "ТП", "ППР", "ПУДП", "ПКУ" и имеют признак "БПП"*/
              ELSIF (   (pmwrtsum.t_Portfolio = RSB_PMWRTOFF.KINDPORT_SALE)
                     OR (pmwrtsum.t_Portfolio = RSB_PMWRTOFF.KINDPORT_TRADE)
                     OR (pmwrtsum.t_Portfolio = RSB_PMWRTOFF.KINDPORT_RETIRE)
                     OR (pmwrtsum.t_Portfolio = RSB_PMWRTOFF.KINDPORT_CONTR))
                    AND pmwrtsum.t_State = RSB_PMWRTOFF.PM_WRTSUM_SALE_BPP
              THEN
                 PortfID := PortfID_Unadmitted;
              /*Для портфеля ОД в отчет отбираются лоты, которые проданы из портфеля ПВО и представляющие
                собой требования/обязательства по возврату ц/б:
                a. учтенные на счетах "- ОД" (или "Обяз.сн.с.") обязательства по обратной поставке ц/б 2 частей сделок
                   обратного РЕПО, возникающие в результате продажи ценных бумаг, полученных по 1 части сделки обр. РЕПО или
                   привлечения займа (портфель ПВО);
                b. учтенные на счетах "+ОД" (или "Треб.сн.с) требования по поставке ц/б 2 частей сделок прямого РЕПО или
                   2 частей размещения займа, возникшие при продаже в этих сделках ц/б, полученных в 1-х частях сделок
                   обратного РЕПО или привлечения займа.*/
              ELSIF pmwrtsum.t_Buy_Sale = RSB_PMWRTOFF.PM_WRITEOFF_SUM_SALE
                    AND pmwrtsum.t_AmountBD != 0
                    AND (Rsb_Secur.IsREPO (vGroup) = 1
                         AND (pmwrtsum.t_State = RSB_PMWRTOFF.PM_WRTSUM_NOTFORM))
              THEN
                 PortfID := PortfID_OD_com;                             /* -ОД*/
              ELSIF     pmwrtsum.t_Portfolio = RSB_PMWRTOFF.KINDPORT_BASICDEBT
                    AND pmwrtsum.t_AmountBD != 0
                    AND pmwrtsum.t_State = RSB_PMWRTOFF.PM_WRTSUM_NOTFORM
              THEN
                 PortfID := PortfID_OD_req;                              /*+ОД*/
              ELSIF ( (Rsb_Secur.IsREPO (vGroup) = 1
                       AND pmwrtsum.t_Buy_Sale =
                              RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY_BO)
                     OR Rsb_Secur.IsAVRWRTIN (vGroup) = 1)
                    AND (pmwrtsum.t_Portfolio = RSB_PMWRTOFF.KINDPORT_BACK_KSU
                         AND pmwrtsum.t_State = RSB_PMWRTOFF.PM_WRTSUM_FORM
                         AND pmwrtsum.t_Amount != 0
                         AND pmwrtsum.t_AmountBD = 0)
              THEN
                 PortfID := PortfID_Back_KSU;
              ELSIF pmwrtsum.t_Portfolio = RSB_PMWRTOFF.KINDPORT_BACK_BPP_KSU
                    AND pmwrtsum.t_State = RSB_PMWRTOFF.PM_WRTSUM_NOTFORM
              THEN
                 PortfID := PortfID_Back_BPP_KSU;
              ELSE
                 PortfID := PortfID_Undef;
              END IF;

              IF PortfID != PortfID_Undef
              THEN
                 /* Получаем номер лицевого счета*/

                 account_rec := GetAccountByLot (pmwrtsum.t_sumid, RepDate);
                 /*if account_rec.t_Account = 'Счет не указан' then
                    dbms_output.put_line('Всё тлен '||pmwrtsum.t_dealid||'-'||pmwrtsum.t_sumid);
                 end if;*/

                 vpartyid :=
                    RSI_RSB_FIInstr.FI_GetIssuerOnDate (pmwrtsum.t_FIID, pdate);

                 /* A3а - Тип ценной бумаги - Краткое наименование вида ц/б
                    A3б - в. Номер выпуска */
                 SELECT t_name
                   INTO AvKindShName
                   FROM davrkinds_dbt t
                  WHERE t.t_fi_kind = fininstr.t_fi_kind
                        AND t.t_avoirkind = fininstr.t_AvoirKind;

                 --               AvKindShName := Substr(AvKindShName,1,5);

                 /* Добавляем запись во временную базу */
                 rec_rep := NULL;

                 rec_rep.t_PortfID := PortfID;
                 rec_rep.t_IsQuoted := IsQuoted;
                 rec_rep.t_DealID := dl_tick.t_DealID;
                 rec_rep.T_SUMID := pmwrtsum.t_sumid;
                 rec_rep.t_FIID := fininstr.t_FIID;
                 rec_rep.T_PARTYID := vpartyid;
                 rec_rep.t_AvKindShName := AvKindShName;
                 rec_rep.t_Account := account_rec.t_Account;
                 rec_rep.T_BALANCE := account_rec.t_balance;
                 rec_rep.t_DocKind := pmwrtsum.t_DocKind;
                 rec_rep.t_DocID := pmwrtsum.t_DocID;
                 rec_rep.t_PartNum := pmwrtsum.t_PartNum;
                 rec_rep.T_DEPARTMENT := department.t_code;

                 -- числовые параметры
                 SELECT t.*
                   INTO vscwrthist
                   FROM v_scwrthistex t
                  WHERE     t.t_DocKind = pmwrtsum.t_DocKind
                        AND t.t_DocID = pmwrtsum.t_DocID
                        AND t.t_PartNum = pmwrtsum.t_PartNum
                        AND t.t_instance =
                               (SELECT MAX (t_instance)
                                  FROM v_scwrthistex
                                 WHERE t_sumid = t.t_sumid
                                       AND t_changedate <= RepDate);

                 IsBond :=
                    RSI_RSB_FIInstr.FI_IsAvrKindBond (fininstr.t_avoirkind);

                 IF PortfID NOT IN (PortfID_OD_req, PortfID_OD_com)
                 THEN   /*для портфелей +\-ОД колонки по НКД и Затратам пустые*/
                    IF IsBond
                    THEN
                       NKD := vscwrthist.t_NKDAmount;

                       IF fininstr.t_FaceValueFI != PM_COMMON.NATCUR
                       THEN
                          NKD :=
                             RSB_FIInstr.ConvSum (NKD,
                                                  fininstr.t_FaceValueFI,
                                                  PM_COMMON.NATCUR,
                                                  RepDate);
                       END IF;
                    END IF;

                    BuyCostRUR := vscwrthist.t_Sum;

                    /*Сначала пересчитывается в валюту номинала по курсу на дату поставки, а затем уже в рубли*/
                    IF vscwrthist.t_Currency != PM_COMMON.NATCUR
                    THEN
                       IF PortfID = PortfID_Unadmitted
                       THEN                   /*для БПП ищем дату поставки БПП*/
                          SetDate := GetSetBppDate (vscwrthist.t_SumID);
                       ELSE
                          SetDate := vscwrthist.t_Date;
                       END IF;

                       BuyCostRUR :=
                          RSB_FIInstr.ConvSum (BuyCostRUR,
                                               vscwrthist.t_Currency,
                                               fininstr.t_FaceValueFI,
                                               SetDate);

                       IF fininstr.t_FaceValueFI != PM_COMMON.NATCUR
                       THEN
                          BuyCostRUR :=
                             RSB_FIInstr.ConvSum (BuyCostRUR,
                                                  fininstr.t_FaceValueFI,
                                                  PM_COMMON.NATCUR,
                                                  RepDate);
                       END IF;
                    END IF;
                 ELSE                                               /* +\- ОД */
                    IF fininstr.t_FaceValueFI != PM_COMMON.NATCUR
                    THEN
                       CostInCUR := vscwrthist.t_BalanceCostBD;
                       CostInRUR :=
                          RSB_FIInstr.ConvSum (CostInCUR,
                                               fininstr.t_FaceValueFI,
                                               PM_COMMON.NATCUR,
                                               RepDate);
                    ELSE
                       CostInCUR := vscwrthist.t_BalanceCostBD;
                       CostInRUR := CostInCUR;
                    END IF;

                    BuyCostRUR := CostInRUR - vscwrthist.t_OverAmountBD;
                 END IF;

                 IF fininstr.t_FaceValueFI != PM_COMMON.NATCUR
                 THEN
                    IF IsBond
                    THEN
                       /*ПД для УП "ТП", "ППР", "ПУДП" "БПП", "ПВО"*/
                       /*ДД для УП "ТП", "ППР", "ПУДП" "БПП"*/

                       IF PortfID IN
                             (PortfID_Trade, PortfID_Sale, PortfID_Retire)
                          OR (PortfID = PortfID_Unadmitted
                              AND vscwrthist.t_Portfolio !=
                                     RSB_PMWRTOFF.KINDPORT_CONTR)
                       THEN
                          DiscountIncomeCUR := vscwrthist.t_DiscountIncome;
                          DiscountIncomeRUR :=
                             RSB_FIInstr.ConvSum (DiscountIncomeCUR,
                                                  fininstr.t_FaceValueFI,
                                                  PM_COMMON.NATCUR,
                                                  RepDate);

                          InterestIncomeCUR := vscwrthist.t_InterestIncome;
                          InterestIncomeRUR :=
                             RSB_FIInstr.ConvSum (InterestIncomeCUR,
                                                  fininstr.t_FaceValueFI,
                                                  PM_COMMON.NATCUR,
                                                  RepDate);
                       END IF;

                       BonusCUR := vscwrthist.t_NotWrtBonus;
                       BonusRUR :=
                          RSB_FIInstr.ConvSum (BonusCUR,
                                               fininstr.t_FaceValueFI,
                                               PM_COMMON.NATCUR,
                                               RepDate);
                    END IF;

                    /*Под стоимостью вложений будем понимать:
                      ТП, ППР, ПДО, ТП/ППР БПП и т.д. - чистая стоимость (cost) + НКД (NKDAMOUNT)
                      ПВО - проще всего текущую стоимость  - balancecost
                      Под стоимостью в +/-ОД - стоимость в ОД (balancecostbd)
                      Затраты и НКД в +/-ОД не выводятся (по факту затраты в ПВО тоже 0)*/
                    IF PortfID NOT IN
                          (PortfID_OD_req, PortfID_OD_com, PortfID_Back)
                    THEN
                       IF PortfID = PortfID_Contr
                          OR (PortfID = PortfID_Unadmitted
                              AND vscwrthist.t_Portfolio =
                                     RSB_PMWRTOFF.KINDPORT_CONTR)
                       THEN
                          BalCostRUR := vscwrthist.t_BalanceCost;
                       ELSE
                          BalCostCUR :=
                             vscwrthist.t_BalanceCost
                             - (vscwrthist.t_begbonus - vscwrthist.t_bonus);
                       END IF;
                    ELSIF PortfID = PortfID_Back
                    THEN
                       BalCostCUR :=
                          vscwrthist.t_BalanceCost
                          - (vscwrthist.t_begbonus - vscwrthist.t_bonus);
                    ELSIF PortfID IN (PortfID_OD_req, PortfID_OD_com)
                    THEN
                       BalCostCUR := vscwrthist.t_BalanceCostBD;
                    END IF;

                    IF NOT (PortfID = PortfID_Contr
                            OR (PortfID = PortfID_Unadmitted
                                AND vscwrthist.t_Portfolio =
                                       RSB_PMWRTOFF.KINDPORT_CONTR))
                    THEN
                       /* Валюта номинала не рубли. */
                       BalCostRUR :=
                          RSB_FIInstr.ConvSum (BalCostCUR,
                                               fininstr.t_FaceValueFI,
                                               PM_COMMON.NATCUR,
                                               RepDate);
                    END IF;
                 ELSE
                    IF IsBond
                    THEN
                       /*ПД для УП "ТП", "ППР", "ПУДП" "БПП", "ПВО"*/
                       /*ДД для УП "ТП", "ППР", "ПУДП" "БПП"*/
                       IF PortfID IN
                             (PortfID_Trade,
                              PortfID_Sale,
                              PortfID_Retire,
                              PortfID_Back)
                          OR (PortfID = PortfID_Unadmitted
                              AND vscwrthist.t_Portfolio !=
                                     RSB_PMWRTOFF.KINDPORT_CONTR)
                       THEN
                          InterestIncomeRUR := vscwrthist.t_InterestIncome;

                          IF PortfID != PortfID_Back
                          THEN
                             DiscountIncomeRUR := vscwrthist.t_DiscountIncome;
                          END IF;
                       END IF;

                       BonusRUR := vscwrthist.t_NotWrtBonus;
                    END IF;

                    IF PortfID NOT IN
                          (PortfID_OD_req, PortfID_OD_com, PortfID_Back)
                    THEN
                       BalCostRUR :=
                          vscwrthist.t_BalanceCost
                          - (vscwrthist.t_begbonus - vscwrthist.t_bonus);
                    ELSIF PortfID = PortfID_Back
                    THEN
                       BalCostRUR :=
                          vscwrthist.t_BalanceCost
                          - (vscwrthist.t_begbonus - vscwrthist.t_bonus);
                    ELSE
                       BalCostRUR := vscwrthist.t_BalanceCostBD;
                    END IF;
                 END IF;

                 /*if( not SmartConvertSumDbl(ReservAmountRUR, , Data.RepDate, dlleg.CFI, NATCUR) )
                    Data.UniqStr.AddString(GetCurrencyConvertErrorMsg(dlleg.CFI, NATCUR, Data.RepDate));
                 end;*/
                 -- надо добавить конвертацию

                 /* Получить LegKind по лоту и сделке. Позволяет определить к какой части
                 сделки относится лот. */
                 IF dl_tick.t_BOfficeKind = Rsb_Secur.DL_CONVAVR
                 THEN
                    IF pmwrtsum.t_Buy_Sale = RSB_PMWRTOFF.PM_WRITEOFF_SUM_SALE
                    THEN
                       LegKind := RSB_BILL.LEG_KIND_DL_TICK;
                    ELSE
                       LegKind := RSB_BILL.LEG_KIND_DL_TICK_BACK;
                    END IF;
                 ELSE
                    IF (Rsb_Secur.IsREPO (vGroup) = 1
                        OR Rsb_Secur.IsBackSale (vGroup) = 1)
                       AND ( (Rsb_Secur.IsBuy (vGroup) = 1
                              AND pmwrtsum.t_Buy_Sale =
                                     RSB_PMWRTOFF.PM_WRITEOFF_SUM_SALE)
                            OR (Rsb_Secur.IsSale (vGroup) = 1
                                AND (pmwrtsum.t_Buy_Sale =
                                        RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY
                                     OR pmwrtsum.t_Buy_Sale =
                                           RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY_BO)))
                    THEN
                       LegKind := RSB_BILL.LEG_KIND_DL_TICK_BACK;
                    ELSE
                       LegKind := RSB_BILL.LEG_KIND_DL_TICK;
                    END IF;
                 END IF;

                 SELECT *
                   INTO dl_leg
                   FROM ddl_leg_dbt t
                  WHERE     t.t_legkind = LegKind
                        AND t.t_dealid = pmwrtsum.t_dealid
                        AND t.t_legid = 0;

                 ReservAmountRUR :=
                    RSB_FIInstr.ConvSum (vscwrthist.t_ReservAmount,
                                         dl_leg.t_CFI,
                                         PM_COMMON.NATCUR,
                                         RepDate);

                 IF (PortfID IN
                        (PortfID_Sale, PortfID_Unadmitted, PortfID_Back) /* and IsQuoted = 1*/
                                                                        )
                    OR PortfID = PortfID_Trade
                 THEN
                    OverAmountRUR := vscwrthist.t_OverAmount;
                 ELSIF PortfID IN (PortfID_OD_req, PortfID_OD_com) /*and  IsQuoted = 1 */
                 THEN
                    OverAmountRUR := vscwrthist.t_OverAmountBD;
                 END IF;

                 CASE
                    WHEN vscwrthist.t_begbonus <> 0
                    THEN
                       SELECT t_facevalue
                         INTO facevalue
                         FROM dfininstr_dbt
                        WHERE t_fiid = vscwrthist.t_fiid;

                       rec_rep.AMOUNT := vscwrthist.t_amount * RSB_FIINSTR.FI_GETNOMINALONDATE(vscwrthist.t_fiid, RepDate,2);
                    ELSE
                       rec_rep.Amount := vscwrthist.t_cost;
                 END CASE;


                 rec_rep.AMOUNT                     :=  vscwrthist.t_amount;
                 rec_rep.UNKD_WITH_COUPON := NKD;
                 rec_rep.COUPON_AMOUNT := InterestIncomeRUR;
                 rec_rep.DISCOUNT_AMOUNT := DiscountIncomeRUR;
                 rec_rep.PREMIUM_AMOUNT := BonusRUR;
                 rec_rep.BALANCE_PRICE := BalCostRUR;
                 rec_rep.OVERVALUE := OverAmountRUR;
                 rec_rep.RESERVE_AMOUNT := ReservAmountRUR;

                 INSERT INTO dportfolio657_1
                      VALUES rec_rep;
              END IF;
           /*exception
              when others then null;*/

           END;
        END LOOP;
     END LOOP;
*/


--     COMMIT;



     return rez;
   END;

/* Получим учетный портфель. */
  FUNCTION getPortfolioID(
    v_dealid IN NUMBER,
    v_Portfolio IN NUMBER,
    v_State IN NUMBER,
    v_Amount IN NUMBER,
    v_Buy_Sale IN NUMBER,
    v_AmountBD IN number
    )
        RETURN number
     IS
        PortfID number := 0;

       BEGIN
                 SELECT *
                  INTO dl_tick
                  FROM ddl_tick_dbt
                 WHERE t_dealid = v_dealid;

                -- группа операций
                SELECT t_sysTypes, t_DocKind
                  INTO vSystypes, vDocKind
                  FROM doprkoper_dbt
                 WHERE t_kind_operation = dl_tick.t_DealType;

                vGroup := Rsb_Secur.get_OperationGroup (vSystypes);

                /* Получим учетный портфель. */
                IF     v_Portfolio = RSB_PMWRTOFF.KINDPORT_TRADE
                   AND v_State = RSB_PMWRTOFF.PM_WRTSUM_FORM
                   AND v_Amount != 0
                   AND v_Buy_Sale = RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY
                THEN
                   PortfID := PortfID_Trade;
                ELSIF     v_Portfolio = RSB_PMWRTOFF.KINDPORT_SALE
                      AND v_State = RSB_PMWRTOFF.PM_WRTSUM_FORM
                      AND v_Amount != 0
                      AND v_Buy_Sale = RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY
                THEN
                   PortfID := PortfID_Sale;
                ELSIF     v_Portfolio = RSB_PMWRTOFF.KINDPORT_RETIRE
                      AND v_State = RSB_PMWRTOFF.PM_WRTSUM_FORM
                      AND v_Amount != 0
                      AND v_Buy_Sale = RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY
                THEN
                   PortfID := PortfID_Retire;
                ELSIF     v_Portfolio = RSB_PMWRTOFF.KINDPORT_CONTR
                      AND v_State = RSB_PMWRTOFF.PM_WRTSUM_FORM
                      AND v_Amount != 0
                      AND v_Buy_Sale = RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY
                THEN
                   PortfID := PortfID_Contr;
                ELSIF     v_Portfolio = RSB_PMWRTOFF.KINDPORT_PROMISSORY
                      AND v_State = RSB_PMWRTOFF.PM_WRTSUM_FORM
                      AND v_Amount != 0
                      AND v_Buy_Sale = RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY
                THEN
                   PortfID := PortfID_Promissory;
                /*В портфель ПВО отбираются лоты, изначально купленные по сделкам обратного РЕПО БПП,
                  не проданные впоследствии*/
                ELSIF ( (Rsb_Secur.IsREPO (vGroup) = 1
                         AND v_Buy_Sale =
                                RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY_BO)
                       OR Rsb_Secur.IsAVRWRTIN (vGroup) = 1)
                      AND (    v_Portfolio = RSB_PMWRTOFF.KINDPORT_BACK
                           AND v_State = RSB_PMWRTOFF.PM_WRTSUM_FORM
                           AND v_Amount != 0
                           AND v_AmountBD = 0)
                THEN
                   PortfID := PortfID_Back;
                /*Для портфеля БПП в отчет отбираются лоты, которые проданы из портфелей "ТП", "ППР", "ПУДП", "ПКУ" и имеют признак "БПП"*/
                ELSIF (   (v_Portfolio = RSB_PMWRTOFF.KINDPORT_SALE)
                       OR (v_Portfolio = RSB_PMWRTOFF.KINDPORT_TRADE)
                       OR (v_Portfolio = RSB_PMWRTOFF.KINDPORT_RETIRE)
                       OR (v_Portfolio = RSB_PMWRTOFF.KINDPORT_CONTR))
                      AND v_State = RSB_PMWRTOFF.PM_WRTSUM_SALE_BPP
                THEN
                   PortfID := PortfID_Unadmitted;
                /*Для портфеля ОД в отчет отбираются лоты, которые проданы из портфеля ПВО и представляющие
                  собой требования/обязательства по возврату ц/б:
                  a. учтенные на счетах "- ОД" (или "Обяз.сн.с.") обязательства по обратной поставке ц/б 2 частей сделок
                     обратного РЕПО, возникающие в результате продажи ценных бумаг, полученных по 1 части сделки обр. РЕПО или
                     привлечения займа (портфель ПВО);
                  b. учтенные на счетах "+ОД" (или "Треб.сн.с) требования по поставке ц/б 2 частей сделок прямого РЕПО или
                     2 частей размещения займа, возникшие при продаже в этих сделках ц/б, полученных в 1-х частях сделок
                     обратного РЕПО или привлечения займа.*/
                ELSIF v_Buy_Sale = RSB_PMWRTOFF.PM_WRITEOFF_SUM_SALE
                      AND v_AmountBD != 0
                      AND (Rsb_Secur.IsREPO (vGroup) = 1
                           AND (v_State = RSB_PMWRTOFF.PM_WRTSUM_NOTFORM))
                THEN
                   PortfID := PortfID_OD_com;                             /* -ОД*/
                ELSIF     v_Portfolio = RSB_PMWRTOFF.KINDPORT_BASICDEBT
                      AND v_AmountBD != 0
                      AND v_State = RSB_PMWRTOFF.PM_WRTSUM_NOTFORM
                THEN
                   PortfID := PortfID_OD_req;                              /*+ОД*/
                ELSIF ( (Rsb_Secur.IsREPO (vGroup) = 1
                         AND v_Buy_Sale =
                                RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY_BO)
                       OR Rsb_Secur.IsAVRWRTIN (vGroup) = 1)
                      AND (v_Portfolio = RSB_PMWRTOFF.KINDPORT_BACK_KSU
                           AND v_State = RSB_PMWRTOFF.PM_WRTSUM_FORM
                           AND v_Amount != 0
                           AND v_AmountBD = 0)
                THEN
                   PortfID := PortfID_Back_KSU;
                ELSIF v_Portfolio = RSB_PMWRTOFF.KINDPORT_BACK_BPP_KSU
                      AND v_State = RSB_PMWRTOFF.PM_WRTSUM_NOTFORM
                THEN
                   PortfID := PortfID_Back_BPP_KSU;
                ELSE
                   PortfID := PortfID_Undef;
                END IF;
                return PortfID;
  END;
  /*Вернуть номнал на дату*/
  FUNCTION getNominalOnDate(
      v_fiid IN NUMBER,
      v_RepDate IN DATE
      )
          RETURN number
       IS
          rez number(16,2) := 0;
  BEGIN
    rez := RSB_FIINSTR.FI_GETNOMINALONDATE(v_fiid, v_RepDate,2);
    return rez;
  END;

  /*Проверить на облигацию*/
  FUNCTION checkBond(
      v_AvoirKind IN NUMBER
      )
          RETURN NUMBER
       IS
          isBond boolean:= false;
  BEGIN

    isBond := RSI_RSB_FIInstr.FI_IsAvrKindBond(v_AvoirKind);
      IF isBond
      THEN
        return 1;
      ElSE
        return 0;
      END IF;
  END;


  /*Получить сумму резерва*/
  FUNCTION getReservAmountRUR(
      BOfficeKind IN NUMBER,
      v_Buy_Sale IN NUMBER,
      v_dealid IN NUMBER,
      v_ReservAmount IN NUMBER,
      v_RepDate IN DATE
      )
          RETURN NUMBER
       IS
         ReservAmountRUR number(16,2):= 0;
         LegKind number;
  BEGIN

       IF v_ReservAmount = 0.00 THEN
          return v_ReservAmount;
       END IF;

         /*if( not SmartConvertSumDbl(ReservAmountRUR, , Data.RepDate, dlleg.CFI, NATCUR) )
          Data.UniqStr.AddString(GetCurrencyConvertErrorMsg(dlleg.CFI, NATCUR, Data.RepDate));
       end;*/
       -- надо добавить конвертацию

       /* Получить LegKind по лоту и сделке. Позволяет определить к какой части
       сделки относится лот. */
       IF BOfficeKind = Rsb_Secur.DL_CONVAVR
       THEN
          IF v_Buy_Sale = RSB_PMWRTOFF.PM_WRITEOFF_SUM_SALE
          THEN
             LegKind := RSB_BILL.LEG_KIND_DL_TICK;
          ELSE
             LegKind := RSB_BILL.LEG_KIND_DL_TICK_BACK;
          END IF;
       ELSE
          IF (Rsb_Secur.IsREPO (vGroup) = 1
              OR Rsb_Secur.IsBackSale (vGroup) = 1)
             AND ( (Rsb_Secur.IsBuy (vGroup) = 1
                    AND v_Buy_Sale =
                           RSB_PMWRTOFF.PM_WRITEOFF_SUM_SALE)
                  OR (Rsb_Secur.IsSale (vGroup) = 1
                      AND (v_Buy_Sale =
                              RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY
                           OR v_Buy_Sale =
                                 RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY_BO)))
          THEN
             LegKind := RSB_BILL.LEG_KIND_DL_TICK_BACK;
          ELSE
             LegKind := RSB_BILL.LEG_KIND_DL_TICK;
          END IF;
       END IF;

       SELECT *
         INTO dl_leg
         FROM ddl_leg_dbt t
        WHERE     t.t_legkind = LegKind
              AND t.t_dealid = v_dealid
              AND t.t_legid = 0;
       DBMS_OUTPUT.PUT_LINE(dl_leg.t_CFI);

       ReservAmountRUR :=
          RSB_FIInstr.ConvSum (v_ReservAmount,
                               dl_leg.t_CFI,
                               PM_COMMON.NATCUR,
                               v_RepDate);
--         DBMS_OUTPUT.PUT_LINE(dl_leg.t_CFI);
       return ReservAmountRUR;
    END;
end user_f657;
/