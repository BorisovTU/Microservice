CREATE OR REPLACE PROCEDURE rsFillPortfolio (
   pdate        IN DATE,
   p_infiid     IN NUMBER DEFAULT -1, -- fiid для загрузки по одной бумаге, -1 по всем
   spread IN NUMBER DEFAULT 1, --1 - размазать переоценку и корректировку пропорционально количеству бумаг 0 - не размазывать
   clear_date   IN BOOLEAN DEFAULT TRUE)
IS
   rec_portfolio                            tRSHB_Portfolio_PKL_2CHD%ROWTYPE;
   rec_rep                                  DPORTFOLIO_TMP%ROWTYPE;
   RepDate                                  DATE;

   fininstr                                 dfininstr_dbt%ROWTYPE;
   prev_fininstr                            NUMBER;
   prev_portfolio                           NUMBER;
   prev_parent                              NUMBER;

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

   vscwrthist                               v_scwrthistex2%ROWTYPE;
   vscwrthist_fields                        varchar2(4000);

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
   Date108Note                              DATE;
   DateForConv                              DATE;

debug_tm number :=0; --1 on 0 off
tm_getaccountbylot interval day to second;
tm_convsum interval day to second;
tm_global interval day to second;
tm_start timestamp := systimestamp;

Function COnvSum (sm in number, fromfi in number, tofi in number, dat in date) return number as
tm timestamp;
retval number(32,12);
begin
tm := systimestamp;
retval := RSB_FIInstr.ConvSum (sm,
                                                fromfi,
                                                tofi,
                                                dat);
tm_convsum := tm_convsum + (systimestamp - tm);
return retval;                                                
end;                                                

   FUNCTION GetAccountByLot (p_sumid IN NUMBER, p_date IN DATE)
      RETURN daccount_dbt%ROWTYPE deterministic
   AS
      acc       daccount_dbt%ROWTYPE;
      --wrthist    v_scwrthistex2%ROWTYPE;
      prtf      NUMBER (5) := 0;
      f         NUMBER (5) := 0;   /*флаг успешности получения счета из кэша*/
      state     NUMBER (5) := 0;
      docid     NUMBER (10) := 0;
      dockind   NUMBER (10) := 0;
      parent    NUMBER (10) := 0;
      tm timestamp;
   BEGIN
   tm := systimestamp;
   select t_portfolio
      ,t_state
      ,t_docid
      ,t_dockind
      ,t_parent
        INTO prtf,
             state,
             docid,
             dockind,
             parent
    from (select t_portfolio
              ,t_state
              ,t_docid
              ,t_dockind
              ,t_parent
              ,t_changedate
              ,max(t_changedate) over() as max_changedate
              ,t_instance
              ,max(t_instance) over(partition by t_changedate) as max_t_instance
          from v_scwrthistex2
         where t_sumid = p_sumid
           and t_changedate <= p_date)
    where t_changedate = max_changedate
        and t_instance = max_t_instance ;

 /*     SELECT s.t_portfolio,
             s.t_state,
             s.t_docid,
             s.t_dockind,
             s.t_parent
        FROM v_scwrthistex2 s
       WHERE s.t_changedate =
                (SELECT MAX (t_changedate)
                   FROM v_scwrthistex2
                  WHERE t_sumid = s.t_sumid AND t_changedate <= p_date)
             AND s.t_sumid = p_sumid
             AND s.t_instance =
                    (SELECT MAX (t_instance)
                       FROM v_scwrthistex2
                      WHERE t_sumid = s.t_sumid
                            AND t_changedate = s.t_changedate);
*/
      /* скрипт получения счетов очень медленный и требует оптимизации, поэтому пробуем организовать некий кэш pkl_portfolio_accounts*/
      /* Считаем, что каждому sumid на протяжении его жизни строго соответствует один счет */
      /*upd как аказалось при некоторых перемещениях sumid не меняется, что портит нам всю малину */
      /*исключение -  счета БПП.  Они могут менять счет хоть каждый день. Поэтому по лотам со статусом 3 получаем счет за дату */
      IF state = 3                                                     /*БПП*/
      THEN
         BEGIN
                     --DBMS_OUTPUT.put_line ('1  ' || p_sumid || '   ' || acc.t_account || '   ' || p_date);

            SELECT a.*
              INTO acc
              FROM daccount_dbt a, pkl_portfolio_accounts ppa
             WHERE     ppa.t_account = a.t_account
                   AND ppa.t_sumid = p_sumid
                   AND ppa.t_date = p_date
                   AND ppa.t_state = state;

         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               f := 1;
         END;
      ELSE
         BEGIN
                     --DBMS_OUTPUT.put_line ('2  ' || p_sumid || '   ' || acc.t_account || '   ' || p_date);

            SELECT a.*
              INTO acc
              FROM daccount_dbt a
             WHERE a.t_account = (SELECT t_account 
                                    FROM (SELECT ppa.* 
                                            FROM pkl_portfolio_accounts ppa
                                           WHERE ppa.t_sumid = p_sumid
                                             AND ppa.t_state <> 3
                                             AND ppa.t_date <= p_date
                                           ORDER BY ppa.t_date DESC)
                                   WHERE ROWNUM = 1);

         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               f := 1;
         END;
      END IF;

      IF f = 0 THEN -- Проверка актуальности найденного счета по категории
        SELECT CASE WHEN (SELECT COUNT(1)
                            FROM dmcaccdoc_dbt mcaccdoc
                           WHERE mcaccdoc.t_account = acc.t_account
                             AND mcaccdoc.t_activatedate <= p_date
                             AND (mcaccdoc.t_disablingdate = TO_DATE('01.01.0001', 'DD.MM.YYYY') OR mcaccdoc.t_disablingdate > p_date)
                         ) = 0 THEN 1
                               ELSE 0 END
          INTO f FROM DUAL;
      END IF;

      IF f = 1         /*Не удалось получить счет, запускаем этот адский код*/
      THEN
         IF (prtf = 3                                                  /*ПКУ*/
                     )
         THEN
            BEGIN
               SELECT  *
                 INTO acc
                 FROM (
                 SELECT /*+ LEADING(s) */ ac.*
                   FROM       dmcaccdoc_dbt a,
                      v_scwrthistex2 s,
                      daccount_dbt ac
                WHERE ac.t_account = a.t_account
                      AND ( (ac.t_close_date >= p_date)
                           OR (ac.t_close_date =
                                  TO_DATE ('01010001', 'ddmmyyyy')))
                      AND ac.t_chapter = a.t_chapter  /*and s.t_fiid = 1130 */
                      AND s.t_changedate <= p_date
                      AND s.t_sumid = p_sumid
                      --       in (
                      --       select t_sumid from  v_scwrthistex2 s where s.t_changedate <= TO_DATE('09012019','ddmmyyyy')
                      -- and s.t_instance = (select MAX(t_instance) from v_scwrthistex2 where t_sumid = s.t_sumid and t_changedate = s.t_changedate)
                      --              )
                      AND a.t_catid = 495          /*Наш портфель ПКУ, ц/б*/
                      AND a.t_fiid = s.t_fiid
                      AND s.t_portfolio = 3
                      AND a.t_iscommon = CHR (88)
                      ORDER BY s.t_changedate DESC, s.t_instance DESC
                     )
                     WHERE ROWNUM = 1;
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  --               DBMS_OUTPUT.put_line (p_sumid || '   ' || p_date);
                  acc.t_account := 'Счет не указан';
                  acc.t_chapter := -999;
            END;
         ELSIF (prtf IN (1, 2, 4, 5))          /*ССПУ_ЦБ, СССД_ЦБ, ПДО,АС_ЦБ*/
         THEN
            BEGIN
               --SELECT ac.t_account,s.t_sumid --into acc
               IF state = 3  /*если разделить на БПП и не БПП, то работает примерно в три раза быстрее. Пока это самое быстрое решение. Но желание переписать не пропадает*/
               THEN
                  SELECT *
                    INTO acc
                    FROM (
                  SELECT /*+ LEADING(s) */ ac.*
                    FROM       dmcaccdoc_dbt a
                            LEFT JOIN
                               dmctempl_dbt mctempl
                            ON     mctempl.t_catid = a.t_catid
                               AND mctempl.t_number = a.t_templnum
                               AND mctempl.t_isclose = CHR (0)
                         LEFT JOIN
                            dllvalues_dbt llval
                         ON llval.t_list = 1100
                            AND llval.t_element = MCTEMPL.T_VALUE1 and MCTEMPL.T_VALUE4 <> 1,
                          v_scwrthistex2 s,
                         daccount_dbt ac
                   WHERE ac.t_account = a.t_account
                         AND ( (ac.t_close_date >= p_date)
                              OR (ac.t_close_date =
                                     TO_DATE ('01010001', 'ddmmyyyy')))
                         AND ac.t_chapter = a.t_chapter /*and s.t_fiid = 1130 */
                         AND s.t_changedate <= p_date
                         AND s.t_sumid = p_sumid
                         AND (s.t_state = 3 AND a.t_catid IN (384, 478)   /*БПП и Корзина БПП*/
                         AND a.t_dockind in (RSB_SECUR.DL_SECURLEG, RSB_SECUR.DL_TICK_ENS_DOC)
                              --AND a.t_fiid = s.t_fiid
            AND a.t_docid =
                   CASE /* по корзине БПП счета привязаны к  документу Обеспечение по цю 4620*/
                      WHEN EXISTS
                              (SELECT 1
                                 FROM ddl_tick_ens_dbt
                                WHERE t_dealid =
                                         (SELECT t_dealid
                                            FROM dpmwrtsum_dbt
                                           WHERE t_sumid = s.t_parent))
                      THEN
                         (SELECT t_id
                            FROM ddl_tick_ens_dbt
                           WHERE     t_kind = 0
                                 AND t_fiid = s.t_fiid
                                 AND t_dealid = (SELECT t_dealid
                                                   FROM dpmwrtsum_dbt
                                                  WHERE t_sumid = s.t_parent)
                                 AND t_id =
                                        (SELECT MIN (t_id)
                                           FROM ddl_tick_ens_dbt
                                          WHERE t_kind = 0
                                                AND t_fiid = s.t_fiid
                                                AND t_dealid =
                                                       (SELECT t_dealid
                                                          FROM dpmwrtsum_dbt
                                                         WHERE t_sumid =
                                                                  s.t_parent)))
                      ELSE
                         (SELECT t_id
                            FROM ddl_leg_dbt
                           WHERE t_legkind = 0
                                 AND t_dealid = (SELECT t_dealid
                                                   FROM dpmwrtsum_dbt
                                                  WHERE t_sumid = s.t_parent))
                   END
                              AND a.t_iscommon = CHR (0)
                              AND ( (s.t_portfolio = 1
                                     AND llval.t_code = 'ССПУ_ЦБ')
                                   OR (s.t_portfolio = 2
                                       AND llval.t_code = 'СССД_ЦБ')
                                   OR (s.t_portfolio = 5
                                       AND llval.t_code = 'АС_ЦБ')))
                         AND a.t_activatedate <= p_date
                         AND (a.t_disablingdate > p_date
                           OR a.t_disablingdate = to_date('01.01.0001', 'DD.MM.YYYY'))
                        ORDER BY s.t_changedate DESC, s.t_instance DESC
                           )
                         WHERE ROWNUM = 1 /*с ходу не удалось найти дубль, но с этим надо разобраться*/
/*KD*/          UNION ALL
                      SELECT *
                         /*  INTO acc*/
                         FROM (
                          SELECT /*+ LEADING(s) */ ac.*
                          FROM       dmcaccdoc_dbt a
                            LEFT JOIN
                               dmctempl_dbt mctempl
                            ON     mctempl.t_catid = a.t_catid
                               AND mctempl.t_number = a.t_templnum
                               AND mctempl.t_isclose = CHR (0)
                         LEFT JOIN
                            dllvalues_dbt llval
                         ON llval.t_list = 1100
                            AND llval.t_element = MCTEMPL.T_VALUE1 and MCTEMPL.T_VALUE4 = 1,
                         v_scwrthistex2 s,
                         daccount_dbt ac
                   WHERE ac.t_account = a.t_account
                         AND ( (ac.t_close_date >= p_date)
                              OR (ac.t_close_date =
                                     TO_DATE ('01010001', 'ddmmyyyy')))
                         AND ac.t_chapter = a.t_chapter /*and s.t_fiid = 1130 */
                         AND s.t_changedate <= p_date
                         AND s.t_sumid = p_sumid
                         --       in (
                         --       select t_sumid from  v_scwrthistex2 s where s.t_changedate <= TO_DATE('09012019','ddmmyyyy')
                         -- and s.t_instance = (select MAX(t_instance) from v_scwrthistex2 where t_sumid = s.t_sumid and t_changedate = s.t_changedate)
                         --              )
                         AND (    s.t_state = 3
                              AND a.t_catid = 31            /*Наш портфель*/
                              AND a.t_fiid = s.t_fiid
                              AND a.t_iscommon = CHR (88)
                              AND ( (s.t_portfolio = 1 /*RAS 516769 Было указано s.t_portfolio = 3*/
                                     AND llval.t_code = 'ССПУ_ЦБ')
                                   OR (s.t_portfolio = 2
                                       AND llval.t_code = 'СССД_ЦБ')
                                   OR (s.t_portfolio = 4
                                       AND llval.t_code = 'ПДО')
                                   OR (s.t_portfolio = 5
                                       AND llval.t_code = 'АС_ЦБ')))
                         AND a.t_activatedate <= p_date
                         AND (a.t_disablingdate > p_date
                           OR a.t_disablingdate = to_date('01.01.0001', 'DD.MM.YYYY'))
                        ORDER BY s.t_changedate DESC, s.t_instance DESC
                           )
                         WHERE ROWNUM = 1;                         
/* END KD*/
               ELSE
                  SELECT *
                    INTO acc
                    FROM (
                    SELECT /*+ LEADING(s) */ ac.*
                    FROM       dmcaccdoc_dbt a
                            LEFT JOIN
                               dmctempl_dbt mctempl
                            ON     mctempl.t_catid = a.t_catid
                               AND mctempl.t_number = a.t_templnum
                               AND mctempl.t_isclose = CHR (0)
                         LEFT JOIN
                            dllvalues_dbt llval
                         ON llval.t_list = 1100
                            AND llval.t_element = MCTEMPL.T_VALUE1 and MCTEMPL.T_VALUE4 <> 1,
                         v_scwrthistex2 s,
                         daccount_dbt ac
                   WHERE ac.t_account = a.t_account
                         AND ( (ac.t_close_date >= p_date)
                              OR (ac.t_close_date =
                                     TO_DATE ('01010001', 'ddmmyyyy')))
                         AND ac.t_chapter = a.t_chapter /*and s.t_fiid = 1130 */
                         AND s.t_changedate <= p_date
                         AND s.t_sumid = p_sumid
                         --       in (
                         --       select t_sumid from  v_scwrthistex2 s where s.t_changedate <= TO_DATE('09012019','ddmmyyyy')
                         -- and s.t_instance = (select MAX(t_instance) from v_scwrthistex2 where t_sumid = s.t_sumid and t_changedate = s.t_changedate)
                         --              )
                         AND (    s.t_state = 1
                              AND a.t_catid = 31            /*Наш портфель*/
                              AND a.t_fiid = s.t_fiid
                              AND a.t_iscommon = CHR (88)
                              AND ( (s.t_portfolio = 1
                                     AND llval.t_code = 'ССПУ_ЦБ')
                                   OR (s.t_portfolio = 2
                                       AND llval.t_code = 'СССД_ЦБ')
                                   OR (s.t_portfolio = 4
                                       AND llval.t_code = 'ПДО')
                                   OR (s.t_portfolio = 5
                                       AND llval.t_code = 'АС_ЦБ')))
                         AND a.t_activatedate <= p_date
                         AND (a.t_disablingdate > p_date
                           OR a.t_disablingdate = to_date('01.01.0001', 'DD.MM.YYYY'))
                         ORDER BY s.t_changedate DESC, s.t_instance DESC
                        )
                        WHERE ROWNUM = 1;
               END IF;
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  --               DBMS_OUTPUT.put_line (p_sumid || '   ' || p_date);
                  acc.t_account := 'Счет не указан';
                  acc.t_chapter := -999;
            END;
         ELSIF (prtf = 6)                                              /*ПВО*/
         THEN
            BEGIN
            --DBMS_OUTPUT.put_line (p_sumid || '   ' || p_date);
               SELECT * INTO acc FROM (
                  SELECT /*+ LEADING(s) INDEX( A DMCACCDOC_DBT_IDX4)*/ ac.*
                  FROM       dmcaccdoc_dbt a,
                        v_scwrthistex2 s,
                        daccount_dbt ac
                  WHERE ac.t_account = a.t_account
                        AND ( (ac.t_close_date >= p_date)
                             OR (ac.t_close_date =
                                  TO_DATE ('01010001', 'ddmmyyyy')))
                        AND ac.t_chapter = a.t_chapter  /*and s.t_fiid = 1130 */
                        AND s.t_changedate <= p_date
                        AND s.t_sumid = p_sumid
                        AND ( (    a.t_catid = 385                      /*ПВО*/
                               AND a.t_iscommon = CHR (0)
                               AND a.t_dockind = RSB_SECUR.DL_SECURLEG
                               AND a.t_docid IN (SELECT t_id
                                                   FROM ddl_leg_dbt
                                                  WHERE t_dealid = s.t_dealid)))
                  ORDER BY s.t_changedate DESC, s.t_instance DESC
                )
                WHERE rownum = 1; /*zy - !!!Пока делаем так, по корзине тело и уплаченный НКД учитываются на одном счете. Так быть не должно!!!*/
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  --               DBMS_OUTPUT.put_line (p_sumid || '   ' || p_date);
                  acc.t_account := 'Счет не указан';
                  acc.t_chapter := -999;
            END;
         ELSE /* КСУшные портфели и если что-то потерялось в предыдущих, то идем сюда*/
            BEGIN
               -- DBMS_OUTPUT.put_line (p_sumid || '   ' || p_date);
               --SELECT ac.t_account,s.t_sumid --into acc
               SELECT *
                 INTO acc
                 FROM (
                 SELECT /*+ leading(s)*/ ac.*
                 FROM  dmcaccdoc_dbt a
                         LEFT JOIN
                            dmctempl_dbt mctempl
                         ON mctempl.t_catid = a.t_catid
                            AND mctempl.t_number = a.t_templnum
                      LEFT JOIN
                         dllvalues_dbt llval
                      ON llval.t_list = 1100
                         AND llval.t_element = MCTEMPL.T_VALUE1,
                      v_scwrthistex2 s,
                      daccount_dbt ac
                WHERE     ac.t_account = a.t_account
                      AND ac.t_chapter = a.t_chapter  /*and s.t_fiid = 1130 */
                      AND s.t_changedate <= p_date
                      AND s.t_sumid = p_sumid
                      --       in (
                      --       select t_sumid from  v_scwrthistex2 s where s.t_changedate <= TO_DATE('09012019','ddmmyyyy')
                      -- and s.t_instance = (select MAX(t_instance) from v_scwrthistex2 where t_sumid = s.t_sumid and t_changedate = s.t_changedate)
                      --              )
                      and a.t_catid in (495, 385, 384, 31) -- /*Наш портфель*/ t_catnum in (1253, 234, 233, 231)
                      AND ( (    a.t_catid = 31             
                             AND a.t_fiid = s.t_fiid
                             AND a.t_iscommon = CHR (88)
                             AND ( (s.t_portfolio = 1
                                    AND llval.t_code = 'ССПУ_ЦБ')
                                  OR (s.t_portfolio = 2
                                      AND llval.t_code = 'СССД_ЦБ')
                                  OR (s.t_portfolio = 4
                                      AND llval.t_code = 'ПДО')
                                  OR (s.t_portfolio = 5
                                      AND llval.t_code = 'АС_ЦБ')))
                           OR (a.t_catid = 385                        /*ПВО*/
                                               AND a.t_iscommon = CHR (0)
                               AND a.t_dockind = RSB_SECUR.DL_SECURLEG
                               AND a.t_docid IN
                                      (SELECT t_id
                                         FROM ddl_leg_dbt
                                        WHERE t_dealid = s.t_dealid))
                           OR (    (a.t_catid = 495) /*Наш портфель ПКУ, ц/б*/
                               AND a.t_fiid = s.t_fiid
                               AND s.t_portfolio = 3
                               AND a.t_iscommon = CHR (88))
                           OR ( (    a.t_catid = 384                  /*БПП*/
                                 AND a.t_fiid = s.t_fiid
                                 AND a.t_iscommon = CHR (88)
                                 AND ( (s.t_portfolio = 1
                                        AND llval.t_code = 'ССПУ_ЦБ')
                                      OR (s.t_portfolio = 2
                                          AND llval.t_code = 'СССД_ЦБ')
                                      OR (s.t_portfolio = 5
                                          AND llval.t_code = 'АС_ЦБ')))))
                          ORDER BY s.t_changedate DESC, s.t_instance DESC
                      )
                      WHERE ROWNUM = 1; /*с ходу не удалось найти дубль, но с этим надо разобраться*/
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  --               DBMS_OUTPUT.put_line (p_sumid || '   ' || p_date);
                  acc.t_account := 'Счет не указан';
                  acc.t_chapter := -999;
            END;
         END IF;

         IF acc.t_chapter <> -999         /*если счет нашли - сохраним в кэш*/
         THEN
            INSERT INTO pkl_portfolio_accounts
                 VALUES (p_sumid,
                         acc.t_Account,
                         p_date,
                         state,
                         parent);

            --DBMS_OUTPUT.put_line (p_sumid || '   ' || acc.t_account || '   ' || p_date);
         END IF;
      END IF;

tm_getaccountbylot := tm_getaccountbylot + (systimestamp - tm);
      RETURN Acc;
   END;

   /* Определение даты поставки */
   FUNCTION GetSetBppDate (pSumID IN NUMBER)
      RETURN DATE deterministic
   IS
      res   DATE ;
   BEGIN
      SELECT t.t_changedate
        INTO res
        FROM v_scwrthistex2 t
       WHERE t.t_SumID = pSumID
             AND t.t_instance =
                    (SELECT MIN (t_instance)
                       FROM v_scwrthistex2
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
      RETURN VARCHAR2 deterministic
   IS
      vnum   VARCHAR2 (35);
   BEGIN
      SELECT Attr.t_NumInList
        INTO vnum
        FROM dobjatcor_dbt AtCor, dobjattr_dbt Attr
       WHERE     AtCor.t_ObjectType = pobjtype
             AND AtCor.t_GroupID = pgroupid
             AND AtCor.t_Object = LPAD (pobjectid, 10, '0')
             AND AtCor.t_General = CHR(88)
             AND (AtCor.t_ValidFromDate =
                    (SELECT MAX (t.T_ValidFromDate)
                       FROM DOBJATCOR_DBT t
                      WHERE     t.T_ObjectType = AtCor.T_ObjectType
                            AND t.T_GroupID = AtCor.T_GroupID
                            AND t.t_Object = AtCor.t_Object
                            AND t.T_ValidFromDate <= pdate)
                            or AtCor.t_ValidFromDate =to_date('01010001','DDMMYYYY'))
             AND Attr.t_AttrID = AtCor.t_AttrID
             AND Attr.t_ObjectType = AtCor.t_ObjectType
             AND Attr.t_GroupID = AtCor.t_GroupID;

      RETURN vnum;
   EXCEPTION
      WHEN OTHERS
      THEN
         RETURN NULL;
   END;

-- Получение котировки
FUNCTION GetRate( pFromFI IN NUMBER
                 ,pToFI    IN NUMBER
                 ,pType    IN NUMBER
                 ,pbdate   IN DATE
                 ,PercAsIs in NUMBER DEFAULT 0 -- 1 - проценты как проценты 0 - проценты в абсолютном выражении 
                )
  RETURN NUMBER
IS
  v_Rate     NUMBER;
  v_IsRelative char;
  v_RateID   NUMBER;
  v_RateDate DATE;
BEGIN
  v_Rate := RSI_RSB_FIInstr.FI_GetRate( pFromFI, pToFI, pType, pbdate, pbdate-to_date('01.01.0001','DD.MM.YYYY'), 0, v_RateID, v_RateDate );
  if( v_Rate <= 0 ) then
     v_Rate := RSI_RSB_FIInstr.FI_GetRate( pFromFI, -1, pType, pbdate, pbdate-to_date('01.01.0001','DD.MM.YYYY'), 0, v_RateID, v_RateDate );
  end if;
  /* begin DEF-37852 06.04.2023 */
  if( v_Rate <= 0 ) then
     -- Мотивированное суждение
     v_Rate := RSI_RSB_FIInstr.FI_GetRate( pFromFI, -1, 1001, pbdate, pbdate-to_date('01.01.0001','DD.MM.YYYY'), 0, v_RateID, v_RateDate );
  end if;
  if( v_Rate <= 0 ) then
     -- Цена закрытия Bloomberg
     v_Rate := RSI_RSB_FIInstr.FI_GetRate( pFromFI, -1, 23, pbdate, pbdate-to_date('01.01.0001','DD.MM.YYYY'), 0, v_RateID, v_RateDate );
  end if;
  /* end DEF-37852 06.04.2023 */
  if(v_RateID > 0)then
    begin
      select t_rate/power(10,t_point)/t_scale, t_IsRelative  
      into v_Rate, v_IsRelative
      from dratedef_dbt
      where t_rateid = v_RateID and
            t_isinverse != chr(88) and
            t_sincedate = v_RateDate;
    exception when others then
      begin
        select t_rate/power(10,t_point)/t_scale
        into v_Rate
        from dratehist_dbt t
        where t_rateid = v_RateID and
              t_isinverse != chr(88) and
              t_sincedate = (select max(v_RateDate) from dratehist_dbt where t_rateid = t.t_rateid and t_sincedate <= v_ratedate);
      select t_IsRelative  
      into v_IsRelative
      from dratedef_dbt
      where t_rateid = v_RateID;
      exception when others then
        null;
      end;
    end;
    if (PercAsIS = 0) and (v_IsRelative = chr(88)) then
       v_Rate := RSB_FIINSTR.FI_GETNOMINALONDATE(pFromFI, v_RateDate)*v_Rate/100;
    end if;
  end if;

  return v_Rate;
EXCEPTION
  when OTHERS then return 0.0;
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
      RETURN VARCHAR2 deterministic
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
            SELECT distinct( 'Y')
              INTO isokved6group
              FROM dobjattr_dbt a, dobjatcor_dbt c
             WHERE     a.t_objecttype = c.t_objecttype
                   AND a.t_groupid = c.t_groupid
                   AND a.t_attrid = c.t_attrid
                   AND c.t_objecttype = 3
                   AND c.t_groupid = 64
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
FUNCTION GetRating (
                  ObId    IN dobjattr_dbt.t_objecttype%TYPE,
                  Tid     IN dobjatcor_dbt.T_OBJECT%TYPE,
                  GrId    IN dobjattr_dbt.t_groupid%TYPE,
                  ParId   IN dobjattr_dbt.t_parentid%TYPE,
                  Ndate   IN DATE)
   RETURN dobjattr_dbt.t_name%TYPE deterministic
IS
   out_Name   dobjattr_dbt.t_name%TYPE;
   DateTmp    dobjatcor_dbt.T_sysdate%TYPE;
   TimeTmp    dobjatcor_dbt.t_systime%TYPE;
BEGIN
   BEGIN
      SELECT MAX (cor.T_sysdate)
        INTO DateTmp
        FROM    dobjatcor_dbt cor
             LEFT JOIN
                dobjattr_dbt attr
             ON     cor.t_objecttype = attr.t_objecttype
                AND cor.t_groupid = attr.t_groupid
                AND cor.t_attrid = attr.t_attrid
--       WHERE     TO_NUMBER (cor.T_OBJECT) = Tid
         WHERE cor.T_OBJECT = LPAD(Tid, 10, '0')
             AND cor.t_groupid = GrId
             AND cor.t_objecttype = ObId
             AND attr.t_parentid = ParId
             AND ndate BETWEEN cor.t_validfromdate AND cor.t_validtodate;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         DateTmp := NULL;
   END;
  
  IF DateTmp is NOT NULL THEN 
    BEGIN
      SELECT MAX (cor.T_systime)
        INTO TimeTmp
        FROM    dobjatcor_dbt cor
             LEFT JOIN
                dobjattr_dbt attr
             ON     cor.t_objecttype = attr.t_objecttype
                AND cor.t_groupid = attr.t_groupid
                AND cor.t_attrid = attr.t_attrid
--       WHERE     TO_NUMBER (cor.T_OBJECT) = Tid
         WHERE cor.T_OBJECT = LPAD(Tid, 10, '0')
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
       --WHERE     TO_NUMBER (cor.T_OBJECT) = Tid
         WHERE cor.T_OBJECT = LPAD(Tid, 10, '0')
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
   
   END IF;
   
   RETURN out_name;
END;
   FUNCTION GetSPV (partyid IN dparty_dbt.t_partyid%TYPE)
      RETURN TRSHB_REPO_PKL_2CHD.IS_SPV%TYPE
   IS
      OUTNAME   TRSHB_REPO_PKL_2CHD.IS_SPV%TYPE;
   BEGIN
      BEGIN
         SELECT a.t_name
           INTO OUTNAME
           FROM    dobjattr_dbt a
                JOIN
                   dobjatcor_dbt c
                ON     a.t_objecttype = c.t_objecttype
                   AND a.t_groupid = c.t_groupid
                   AND a.t_attrid = c.t_attrid
          WHERE     c.t_objecttype = 3
                AND c.t_groupid = 110
                AND c.t_object = LPAD (Partyid, 10, '0')
                AND c.t_validtodate = TO_DATE ('31.12.9999', 'dd.mm.yyyy');
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            OUTNAME := NULL;
      END;

      RETURN OUTNAME;
   END;

   FUNCTION GetHypoCover (fiid IN dparty_dbt.t_partyid%TYPE)
      RETURN TRSHB_REPO_PKL_2CHD.IS_HYPO_COVER%TYPE deterministic
   IS
      OUTNAME   TRSHB_REPO_PKL_2CHD.IS_HYPO_COVER%TYPE;
   BEGIN
      BEGIN
         SELECT a.t_name
           INTO OUTNAME
           FROM    dobjattr_dbt a
                JOIN
                   dobjatcor_dbt c
                ON     a.t_objecttype = c.t_objecttype
                   AND a.t_groupid = c.t_groupid
                   AND a.t_attrid = c.t_attrid
          WHERE     c.t_objecttype = 12
                AND c.t_groupid = 102
                AND c.t_object = LPAD (fiid, 10, '0')
                AND c.t_validtodate = TO_DATE ('31.12.9999', 'dd.mm.yyyy');
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            OUTNAME := NULL;
      END;

      RETURN OUTNAME;
   END;
   
   -- Для исключения блокировки    
   FUNCTION clear_pkl_portfolio_accounts return boolean
     is
     pragma autonomous_transaction;
     TYPE tt_sum_id IS TABLE OF number;
     tsum_id tt_sum_id := tt_sum_id();
     begin
       /* Если выпускать отчет промежуточным вариантом - по всем выпускам выполнять отдельный запуск процедуры, то итоговый набор даных может быть некорректным */
       /* В таком случае очистку оставляем на совести внешней реализации */

       /*Очистка кэша счетов. При откате сделок id лотов могут меняться и периодически появляются "пустышки"*/
       select  t.t_sumid 
            BULK COLLECT into tsum_id 
            from pkl_portfolio_accounts t
            WHERE case when NOT EXISTS (SELECT 1 FROM v_scwrthistex2 WHERE t_sumid = t.t_sumid) then 1 else 0 end = 1 for update wait 60;
       
       FORALL i IN tsum_id.FIRST..tsum_id.LAST
       DELETE FROM pkl_portfolio_accounts t
              WHERE t_sumid = tsum_id(i) ;
       commit;
       --EXECUTE IMMEDIATE 'DELETE FROM pkl_portfolio_accounts t WHERE case when NOT EXISTS (SELECT 1 FROM v_scwrthistex2 WHERE t_sumid = t.t_sumid) then 1 else 0 end = 1';
       
       /*чистим записи с лотами, по которым менялся портфель. Включение такого условие в селекты сильно замелдяло работу, поэтому действуем более грубо*/


       select t_sumid 
            BULK COLLECT into tsum_id 
        FROM pkl_portfolio_accounts
          WHERE t_sumid IN (with pkl as
                               (select /*+ materialize */ 
                                 t.t_sumid
                                  from pkl_portfolio_accounts t
                                  join dpmwrtsum_dbt s
                                    on t.t_sumid = s.t_sumid
                                    group by t.t_sumid )
                              select t_sumid
                                from (select /*+ cardinality(pkl 100) */ t.t_sumid
                                            ,count(distinct v.t_portfolio) - 1 as cnt
                                        from pkl t
                                        join v_scwrthistex v
                                          on t.t_sumid = v.t_sumid
                                       group by t.t_sumid)
                               where cnt > 0 ) for update wait 60;

       FORALL i IN tsum_id.FIRST..tsum_id.LAST
       DELETE FROM pkl_portfolio_accounts t
              WHERE t_sumid = tsum_id(i) ;

       /*EXECUTE IMMEDIATE 'DELETE FROM pkl_portfolio_accounts
          WHERE t_sumid IN (SELECT  t.t_sumid
                              FROM    pkl_portfolio_accounts t
                                   INNER JOIN
                                      dpmwrtsum_dbt s
                                   ON t.t_sumid = s.t_sumid
                            WHERE (SELECT COUNT (distinct t_portfolio) - 1
                                      FROM v_scwrthistex
                                               WHERE t.t_sumid = t_sumid) > 0) ';*/



    /*   EXECUTE IMMEDIATE 'DELETE FROM pkl_portfolio_accounts
          WHERE t_sumid IN (SELECT t.t_sumid
                              FROM    pkl_portfolio_accounts t
                                   INNER JOIN
                                      dpmwrtsum_dbt s
                                   ON t.t_sumid = s.t_sumid
                             WHERE (SELECT COUNT (1) - 1
                                      FROM (  SELECT 1
                                                FROM v_scwrthistex
                                               WHERE t.t_sumid = t_sumid
                                            GROUP BY t_portfolio)) > 0)';*/

       commit;
       return true;
     exception
       when others then
         rollback;
         return false;
     end;
BEGIN
   RepDate := pdate;
 
--tm_getaccountbylot := systimestamp-systimestamp;
--tm_convsum:= systimestamp-systimestamp;
--tm_global:= systimestamp-systimestamp;
--IF debug_tm = 1 then
select  systimestamp-systimestamp into tm_getaccountbylot           from dual;
select  systimestamp-systimestamp into tm_convsum           from dual;
select  systimestamp-systimestamp into tm_global           from dual;
tm_global := tm_global + (systimestamp - tm_start);
tm_start := systimestamp;
dbms_output.put_line('1 перед подготовкой '||tm_global);
--end if;
/*сбор статистики*/
--DBMS_STATS.GATHER_TABLE_STATS(OWNNAME =>sys_context('USERENV','CURRENT_USER') , TABNAME =>'DPMWRTSUM_DBT' , CASCADE => true);
--DBMS_STATS.GATHER_TABLE_STATS(OWNNAME =>sys_context('USERENV','CURRENT_USER') , TABNAME =>'DPMWRTBC_DBT' , CASCADE => true);
--DBMS_STATS.GATHER_TABLE_STATS(OWNNAME =>'RSHB_SOFR' , TABNAME =>'DPMWRTSUM_DBT' , CASCADE => true, no_invalidate => false);

   -- Для искдючения взаимной блокировки из qb_dwh_export_secur
   loop
     exit when clear_pkl_portfolio_accounts ;
   end loop;

   IF p_infiid = -1
   THEN
     -- EXECUTE IMMEDIATE 'TRUNCATE TABLE DPORTFOLIO_TMP DROP STORAGE'; -- очистка если запуск по всем бумагам
     EXECUTE IMMEDIATE 'DELETE FROM DPORTFOLIO_TMP WHERE  REPORT_DT = :1'
         USING RepDate; -- очистка  данных за дату
   ELSE
      EXECUTE IMMEDIATE 'DELETE FROM DPORTFOLIO_TMP WHERE T_FIID = :1 AND REPORT_DT = :2'
         USING p_infiid, RepDate; -- очистка  по той бумаге, по которой выполнен запуск
   END IF;
   
   
   /*Отбираются :
      а) не проданные по состоянию на начало отчетной даты лоты обычных покупок и операций зачисления,
         статус которых равен "поставлен", относящиеся к данному "учетному портфелю".
      б) по незакрытым на дату отчета сделкам РЕПО отбираются лоты статусом "поставлен" или "не по-ставлен".
   */

   prev_fininstr := NULL;
   prev_portfolio := NULL;
   prev_parent := NULL;
   IF debug_tm = 1 then
      tm_global := tm_global + (systimestamp - tm_start);
      tm_start := systimestamp;
      dbms_output.put_line('2 перед запуском главного отбора '||tm_global);
   end if;
   FOR department IN (SELECT t_Code FROM ddp_dep_dbt where t_partyid = 1)
   LOOP                                              -- по всем подразделениям
      FOR pmwrtsum IN (select *
                      from ( /*БПП*/
                            select /*+ index(s DPMWRTSUM_DBT_IDX6)*/
                             s.t_fiid
                            ,s.t_dealid
                            ,s.t_buy_sale
                            ,s.t_parent
                            ,s.t_sumid
                            ,s.t_instance
                            ,s.t_amount
                            ,s.t_amountbd
                            ,s.t_changedate
                            ,s.t_portfolio
                            ,s.t_state
                            ,s.t_enterdate
                            ,s.t_docid
                            ,s.t_dockind
                            ,s.t_partnum
                            ,s.t_kind
                              from dpmwrtsum_dbt s
                             where s.t_Department = department.t_code
                               and s.t_buy_sale = RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY
                               and s.t_party = -1
                               and s.t_dockind = PM_COMMON.DLDOC_PAYMENT
                               and s.t_kind = 210
                               and s.t_portfolio in (RSB_PMWRTOFF.KINDPORT_TRADE, RSB_PMWRTOFF.KINDPORT_SALE, RSB_PMWRTOFF.KINDPORT_RETIRE, RSB_PMWRTOFF.KINDPORT_CONTR)
                               and s.t_amount > 0
                               and s.t_state = RSB_PMWRTOFF. PM_WRTSUM_SALE_BPP
                               and s.t_changedate <= RepDate
                               and (p_infiid = -1 or p_infiid = s.t_fiid)
                            union
                            select /*+ orderd */
                             m.t_fiid
                            ,m.t_dealid
                            ,m.t_buy_sale
                            ,m.t_parent
                            ,t.t_sumid
                            ,t.t_instance
                            ,t.t_amount
                            ,t.t_amountbd
                            ,t.t_changedate
                            ,t.t_portfolio
                            ,t.t_state
                            ,m.t_enterdate
                            ,m.t_docid
                            ,m.t_dockind
                            ,m.t_partnum
                            ,m.t_kind
                              from (select /*+ index(s DPMWRTSUM_DBT_IDX11)*/ distinct
                                      s.t_sumid
                                       from dpmwrtsum_dbt s
                                      where s.t_Department = department.t_code
                                        and s.t_buy_sale = RSB_PMWRTOFF. PM_WRITEOFF_SUM_BUY
                                        and s.t_party = -1
                                        and s.t_dockind = PM_COMMON.
                                      DLDOC_PAYMENT
                                        and s.t_kind = 210
                                        and s.t_portfolio in
                                            (RSB_PMWRTOFF. KINDPORT_TRADE, RSB_PMWRTOFF. KINDPORT_SALE, RSB_PMWRTOFF. KINDPORT_RETIRE, RSB_PMWRTOFF. KINDPORT_CONTR)
                                        and s.t_changedate > RepDate) s
                                   ,dpmwrtbc_dbt t
                                   ,dpmwrtsum_dbt m
                             where t.t_sumid = s.t_sumid
                               and t.t_state = RSB_PMWRTOFF. PM_WRTSUM_SALE_BPP
                               and t.t_amount > 0
                               and decode(t.t_instance
                                         ,(select max(v.t_instance)
                                            from dpmwrtbc_dbt v
                                           where v.t_sumid = t.t_sumid
                                             and v. t_changedate <= RepDate)
                                         ,1
                                         ,0) = 1
                               and m.t_sumid = t.t_sumid
                               and (p_infiid = -1 or p_infiid = t.t_fiid)
                            /*"ТП" /"ППР" / "ПУДП" / "ПДО" / "ПКУ"*/
                            union
                            /*по kind = 210 отберем лоты, вернувшиеся из БПП
                            20 обычные покупки
                            40,110, 130 зачисления, зачисления при конвертации, зачисления в глоб. опции
                            90  перемещения в портфель*/
                            select /*+ index(s DPMWRTSUM_DBT_IDX6) */
                             s.t_fiid
                            ,s.t_dealid
                            ,s.t_buy_sale
                            ,s.t_parent
                            ,s.t_sumid
                            ,s.t_instance
                            ,s.t_amount
                            ,s.t_amountbd
                            ,s.t_changedate
                            ,s.t_portfolio
                            ,s.t_state
                            ,s.t_enterdate
                            ,s.t_docid
                            ,s.t_dockind
                            ,s.t_partnum
                            ,s.t_kind
                              from dpmwrtsum_dbt s
                             where s.t_Department = department.t_code
                               and s.t_buy_sale = RSB_PMWRTOFF. PM_WRITEOFF_SUM_BUY
                               and s.t_party = -1
                               and s.t_dockind in (PM_COMMON.DLDOC_PAYMENT, Rsb_Secur. DL_ISSUE_UNION, Rsb_Secur.DL_MOVINGDOC)
                               and s.t_dealid > 0
                               and s.t_kind in (20, 210, 40, 110, 130, 90)
                               and s.t_Portfolio in (RSB_PMWRTOFF. KINDPORT_TRADE
                                                    ,RSB_PMWRTOFF. KINDPORT_SALE
                                                    ,RSB_PMWRTOFF. KINDPORT_CONTR
                                                    ,RSB_PMWRTOFF. KINDPORT_RETIRE
                                                    ,RSB_PMWRTOFF. KINDPORT_PROMISSORY)
                               and s.t_state = RSB_PMWRTOFF. PM_WRTSUM_FORM
                               and s.t_Amount != 0
                               and s.t_changedate <= RepDate
                               and ((p_infiid = -1) or (p_infiid = s.t_fiid))
                            union
                            select /*+ orderd */
                             m.t_fiid
                            ,m.t_dealid
                            ,m.t_buy_sale
                            ,m.t_parent
                            ,t.t_sumid
                            ,t.t_instance
                            ,t.t_amount
                            ,t.t_amountbd
                            ,t.t_changedate
                            ,t.t_portfolio
                            ,t.t_state
                            ,m.t_enterdate
                            ,m.t_docid
                            ,m.t_dockind
                            ,m.t_partnum
                            ,m.t_kind
                              from (select /*+ index(s DPMWRTSUM_DBT_IDX11) */ distinct
                                      s.t_sumid
                                       from dpmwrtsum_dbt s
                                      where s.t_Department = department.t_code
                                        and s.t_buy_sale = RSB_PMWRTOFF. PM_WRITEOFF_SUM_BUY
                                        and s.t_party = -1
                                        and s.t_dockind in (PM_COMMON. DLDOC_PAYMENT, Rsb_Secur. DL_ISSUE_UNION, Rsb_Secur. DL_MOVINGDOC)
                                        and s.t_dealid > 0
                                        and s.t_kind in (20, 210, 40, 110, 130, 90)
                                        and s.t_Portfolio in (RSB_PMWRTOFF. KINDPORT_TRADE
                                                             ,RSB_PMWRTOFF. KINDPORT_SALE
                                                             ,RSB_PMWRTOFF. KINDPORT_CONTR
                                                             ,RSB_PMWRTOFF. KINDPORT_RETIRE
                                                             ,RSB_PMWRTOFF. KINDPORT_PROMISSORY)
                                        and s.t_changedate > RepDate) s
                                   ,dpmwrtbc_dbt t
                                   ,dpmwrtsum_dbt m
                             where t.t_sumid = s.t_sumid
                               and t.t_state = RSB_PMWRTOFF. PM_WRTSUM_FORM
                               and t.t_Amount != 0
                               and decode(t.t_instance
                                         ,(select max(v.t_instance)
                                            from dpmwrtbc_dbt v
                                           where v.t_sumid = t.t_sumid
                                             and v. t_changedate <= RepDate)
                                         ,1
                                         ,0) = 1
                               and m.t_sumid = t.t_sumid
                               and ((p_infiid = -1) or (p_infiid = t.t_fiid))
                            /* ПВО (РЕПО/зачисления) */ /*Белохонов П. Убрано по замечаниям из письма Марины Галямовой 21.03.2019 12:19 91314 не должны попадать в отчет*/
                    /*        union
                            select *
                              from (select s.t_fiid
                                           ,s.t_dealid
                                           ,s.t_buy_sale
                                           ,s.t_parent
                                           ,s.t_sumid
                                           ,s.t_instance
                                           ,s.t_amount
                                           ,s.t_amountbd
                                           ,s.t_changedate
                                           ,s.t_portfolio
                                           ,s.t_state
                                           ,s.t_enterdate
                                           ,s.t_docid
                                           ,s.t_dockind
                                           ,s.t_partnum
                                           ,s.t_kind
                                       from dpmwrtsum_dbt s
                                      where s.t_Department = department.t_code
                                        and s.t_Party = -1
                                        and s.t_dockind in (PM_COMMON.DLDOC_PAYMENT, Rsb_Secur. DL_ISSUE_UNION)
                                        and ((s.t_State = RSB_PMWRTOFF.
                                             PM_WRTSUM_FORM and
                                             s.t_DealID =
                                             (select tick. t_DealID
                                                 from ddl_tick_dbt tick
                                                where tick. t_DealID = s. t_DealID
                                                  and rsb_secur. IsRepo(rsb_secur. get_OperationGroup(rsb_secur. get_OperSysTypes(tick. t_DealType, tick. t_BofficeKind))) = 1
                                                  and (tick. t_DealStatus < 20 or (tick. t_DealStatus = 20 and tick. t_CloseDate > RepDate))) and
                                             s.t_Buy_Sale = RSB_PMWRTOFF. PM_WRITEOFF_SUM_BUY_BO) or s.t_kind in (40, 110, 130))
                                        and s.t_Portfolio = RSB_PMWRTOFF. KINDPORT_BACK
                                        and s.t_Amount != 0
                                        and s.t_AmountBD = 0
                                        and s.t_changedate <= RepDate
                                        and ((p_infiid = -1) or (p_infiid = s.t_fiid))
                                     union
                                     select m.t_fiid
                                           ,m.t_dealid
                                           ,m.t_buy_sale
                                           ,m.t_parent
                                           ,t.t_sumid
                                           ,t.t_instance
                                           ,t.t_amount
                                           ,t.t_amountbd
                                           ,t.t_changedate
                                           ,m.t_portfolio
                                           ,t.t_state
                                           ,m.t_enterdate
                                           ,m.t_docid
                                           ,m.t_dockind
                                           ,m.t_partnum
                                           ,m.t_kind
                                       from dpmwrtbc_dbt  t
                                           ,dpmwrtsum_dbt m
                                      where t.t_sumid in
                                            (select s.t_sumid
                                               from dpmwrtsum_dbt s
                                              where s.t_Department = department.t_code
                                                and s.t_Party = -1
                                                and s.t_dockind in (PM_COMMON. DLDOC_PAYMENT, Rsb_Secur. DL_ISSUE_UNION)
                                                and ((s.t_State = RSB_PMWRTOFF. PM_WRTSUM_FORM and s.
                                                     t_DealID =
                                                     (select tick. t_DealID
                                                         from ddl_tick_dbt tick
                                                        where tick. t_DealID = s. t_DealID
                                                          and rsb_secur.
                                                        IsRepo(rsb_secur. get_OperationGroup(rsb_secur. get_OperSysTypes(tick. t_DealType, tick. t_BofficeKind))) = 1
                                                          and (tick. t_DealStatus < 20 or (tick. t_DealStatus = 20 and tick. t_CloseDate > RepDate))) and s.
                                                     t_Buy_Sale = RSB_PMWRTOFF. PM_WRITEOFF_SUM_BUY_BO) or s.t_kind in (40, 110, 130))
                                                and s.t_Portfolio = RSB_PMWRTOFF. KINDPORT_BACK
                                                and s.t_changedate > RepDate)
                                        and t.t_Amount != 0
                                        and t.t_AmountBD = 0
                                        and t.t_instance = (select max(v.t_instance)
                                                              from dpmwrtbc_dbt v
                                                             where v.t_sumid = t.t_sumid
                                                               and v. t_changedate <= RepDate)
                                        and m.t_sumid = t.t_sumid
                                        and ((p_infiid = -1) or (p_infiid = t.t_fiid)))*/
                            /* +\- ОД */
                            union
                            select /*+ index(s DPMWRTSUM_DBT_IDX6) */
                             s.t_fiid
                            ,s.t_dealid
                            ,s.t_buy_sale
                            ,s.t_parent
                            ,s.t_sumid
                            ,s.t_instance
                            ,s.t_amount
                            ,s.t_amountbd
                            ,s.t_changedate
                            ,s.t_portfolio
                            ,s.t_state
                            ,s.t_enterdate
                            ,s.t_docid
                            ,s.t_dockind
                            ,s.t_partnum
                            ,s.t_kind
                              from dpmwrtsum_dbt s
                             where s.t_Department = department.t_code
                               and s.t_Party = -1
                               and s.t_dockind = PM_COMMON.DLDOC_PAYMENT
                               and decode(s.t_DealID
                                         ,(select tick.t_DealID
                                            from ddl_tick_dbt tick
                                           where tick.t_DealID = s.t_DealID
                                             and rsb_secur. IsRepo(rsb_secur. get_OperationGroup(rsb_secur. get_OperSysTypes(tick. t_DealType, tick. t_BofficeKind))) = 1
                                             and (tick. t_DealStatus < 20 or (tick. t_DealStatus = 20 and tick. t_CloseDate > RepDate)))
                                         ,1
                                         ,0) = 1
                               and (s.t_Portfolio = RSB_PMWRTOFF. KINDPORT_BASICDEBT or s.t_Buy_Sale = RSB_PMWRTOFF. PM_WRITEOFF_SUM_SALE)
                               and s.t_AmountBD != 0
                               and s.t_state = RSB_PMWRTOFF. PM_WRTSUM_NOTFORM
                               and s.t_changedate <= RepDate
                               and ((p_infiid = -1) or (p_infiid = s.t_fiid))
                            union
                            select /*+ orderd */
                             m.t_fiid
                            ,m.t_dealid
                            ,m.t_buy_sale
                            ,m.t_parent
                            ,t.t_sumid
                            ,t.t_instance
                            ,t.t_amount
                            ,t.t_amountbd
                            ,t.t_changedate
                            ,t.t_portfolio
                            ,t.t_state
                            ,m.t_enterdate
                            ,m.t_docid
                            ,m.t_dockind
                            ,m.t_partnum
                            ,m.t_kind
                              from (select /*+ index(s DPMWRTSUM_DBT_IDX6) */ distinct
                                      s.t_sumid
                                       from dpmwrtsum_dbt s
                                      where s.t_Department = department.t_code
                                        and s.t_Party = -1
                                        and s.t_dockind = PM_COMMON. DLDOC_PAYMENT
                                        and decode(s.t_DealID
                                                  ,(select tick. t_DealID
                                                     from ddl_tick_dbt tick
                                                    where tick. t_DealID = s. t_DealID
                                                      and rsb_secur.
                                                    IsRepo(rsb_secur. get_OperationGroup(rsb_secur. get_OperSysTypes(tick. t_DealType, tick. t_BofficeKind))) = 1
                                                      and (tick. t_DealStatus < 20 or (tick. t_DealStatus = 20 and tick. t_CloseDate > RepDate)))
                                                  ,1
                                                  ,0) = 1
                                        and (s.t_Portfolio = RSB_PMWRTOFF. KINDPORT_BASICDEBT or s.t_Buy_Sale = RSB_PMWRTOFF. PM_WRITEOFF_SUM_SALE)
                                        and s.t_changedate > RepDate) s
                                   ,dpmwrtbc_dbt t
                                   ,dpmwrtsum_dbt m
                             where t.t_sumid = s.t_sumid
                               and t.t_AmountBD != 0
                               and t.t_state = RSB_PMWRTOFF. PM_WRTSUM_NOTFORM
                               and decode(t.t_instance
                                         ,(select max(v.t_instance)
                                            from dpmwrtbc_dbt v
                                           where v.t_sumid = t.t_sumid
                                             and v. t_changedate <= RepDate)
                                         ,1
                                         ,0) = 1
                               and m.t_sumid = t.t_sumid
                               and ((p_infiid = -1) or (p_infiid = t.t_fiid))
                            /* ПВО_КСУ (РЕПО/зачисления) */
                            union
                            select /*+ index(s DPMWRTSUM_DBT_IDX6) */
                             s.t_fiid
                            ,s.t_dealid
                            ,s.t_buy_sale
                            ,s.t_parent
                            ,s.t_sumid
                            ,s.t_instance
                            ,s.t_amount
                            ,s.t_amountbd
                            ,s.t_changedate
                            ,s.t_portfolio
                            ,s.t_state
                            ,s.t_enterdate
                            ,s.t_docid
                            ,s.t_dockind
                            ,s.t_partnum
                            ,s.t_kind
                              from dpmwrtsum_dbt s
                             where s.t_Department = department.t_code
                               and s.t_Party = -1
                               and s.t_dockind = PM_COMMON.DLDOC_PAYMENT
                               and s.t_State = RSB_PMWRTOFF. PM_WRTSUM_FORM
                               and s.t_Portfolio = RSB_PMWRTOFF. KINDPORT_BACK_KSU
                               and s.t_Amount != 0
                               and s.t_AmountBD = 0
                               and s.t_changedate <= RepDate
                               and ((p_infiid = -1) or (p_infiid = s.t_fiid))
                            union
                            select /*+ orderd */
                             m.t_fiid
                            ,m.t_dealid
                            ,m.t_buy_sale
                            ,m.t_parent
                            ,t.t_sumid
                            ,t.t_instance
                            ,t.t_amount
                            ,t.t_amountbd
                            ,t.t_changedate
                            ,t.t_portfolio
                            ,t.t_state
                            ,m.t_enterdate
                            ,m.t_docid
                            ,m.t_dockind
                            ,m.t_partnum
                            ,m.t_kind
                              from (select /*+ index(s DPMWRTSUM_DBT_IDX11) */ distinct
                                      s.t_sumid
                                       from dpmwrtsum_dbt s
                                      where s.t_Department = department.t_code
                                        and s.t_Party = -1
                                        and s.t_dockind = PM_COMMON. DLDOC_PAYMENT
                                        and s.t_State = RSB_PMWRTOFF. PM_WRTSUM_FORM
                                        and s.t_Portfolio = RSB_PMWRTOFF. KINDPORT_BACK_KSU
                                        and s.t_changedate > RepDate) s
                                   ,dpmwrtbc_dbt t
                                   ,dpmwrtsum_dbt m
                             where t.t_sumid = s.t_sumid
                               and t.t_Amount != 0
                               and t.t_AmountBD = 0
                               and decode(t.t_instance
                                         ,(select max(v.t_instance)
                                            from dpmwrtbc_dbt v
                                           where v.t_sumid = t.t_sumid
                                             and v. t_changedate <= RepDate)
                                         ,1
                                         ,0) = 1
                               and m.t_sumid = t.t_sumid
                               and ((p_infiid = -1) or (p_infiid = t.t_fiid))
                            /* ПВО_БПП_КСУ */
                            union
                            select /*+ leading(buy1) cardinality(buy1,100) */
                             s.t_fiid
                            ,s.t_dealid
                            ,s.t_buy_sale
                            ,s.t_parent
                            ,s.t_sumid
                            ,s.t_instance
                            ,s.t_amount
                            ,s.t_amountbd
                            ,s.t_changedate
                            ,s.t_portfolio
                            ,s.t_state
                            ,s.t_enterdate
                            ,s.t_docid
                            ,s.t_dockind
                            ,s.t_partnum
                            ,s.t_kind
                              from v_scwrthistex2 s
                                   ,dpmwrtsum_dbt  buy1
                             where decode(s.t_Department, department.t_code, 1, 0) = 1
                               and decode(s.t_Party, -1, 1, 0) = 1
                               and decode(s.t_dockind, PM_COMMON.DLDOC_PAYMENT, 1, 0) = 1
                               and decode(s.t_State, RSB_PMWRTOFF.PM_WRTSUM_NOTFORM, 1, 0) = 1
                               and decode(s.t_DealID
                                         ,(select tick.t_DealID
                                            from ddl_tick_dbt tick
                                           where tick.t_DealID = s.t_DealID
                                             and (tick.t_DealStatus < 20 or
                                                 (tick. t_DealStatus = 20 and (tick. t_CloseDate > RepDate or rsb_secur. DealIsRepo(tick. t_DealID) = 0))))
                                         ,1
                                         ,0) = 1
                               and s.t_Amount > 0
                               and decode(s.t_instance
                                         ,(select max(t_instance)
                                            from v_scwrthistex2
                                           where t_sumid = s.t_sumid
                                             and t_changedate <= RepDate)
                                         ,1
                                         ,0) = 1
                               and decode(s.t_Portfolio, RSB_PMWRTOFF. KINDPORT_BACK_BPP_KSU, 1, 0) = 1
                               and decode(s.t_Buy_Sale, RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY, 1, 0) = 1
                               and buy1.t_SumID = s.t_Source
                               and buy1.t_Portfolio = RSB_PMWRTOFF.KINDPORT_BACK_KSU
                               and ((p_infiid = -1) or (p_infiid = s.t_fiid)))
                     order by t_fiid
                             ,t_portfolio
                             ,t_state
                             ,t_parent ) /*сортировка строго такая для минимизации вызова поиска счета*/
      LOOP
         BEGIN
         IF debug_tm = 1 THEN
         tm_global := tm_global + (systimestamp - tm_start);
         tm_start := systimestamp;
         dbms_output.put_line('3 итерация '||tm_global);
         end if;
            CostInCUR := 0;
            CostInRUR := 0;
            BalCostCUR := 0;
            BalCostRUR := 0;
            BuyCostRUR := 0;
            DiscountIncomeRUR := 0;
            DiscountIncomeCUR := 0;
            InterestIncomeCUR := 0;
            InterestIncomeRUR := 0;
            NKD := 0;
            OverAmountRUR := 0;
            ReservAmountRUR := 0;
            BonusRUR := 0;
            BonusCUR := 0;


            IF prev_fininstr IS NULL OR pmwrtsum.t_FIID <> prev_fininstr
            THEN
               SELECT *
                 INTO fininstr
                 FROM dfininstr_dbt
                WHERE t_fiid = pmwrtsum.t_FIID;
            END IF;
            
            Date108Note := CASE WHEN rsi_rsb_kernel.GetNote(RSB_SECUR.OBJTYPE_AVOIRISS, LPAD(fininstr.T_FIID, 10, '0'), 108, RepDate) IS NULL 
                                THEN TO_DATE ('01.01.0001', 'dd.mm.yyyy')
                                ELSE rsb_struct.getDate(rsi_rsb_kernel.GetNote(RSB_SECUR.OBJTYPE_AVOIRISS, LPAD(fininstr.T_FIID, 10, '0'), 108, RepDate)) 
                            END;

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

               /*Получаем только если в этом есть необходимость - изменилась цб, портфель*/
               IF    prev_fininstr IS NULL
                  OR prev_portfolio IS NULL
                  OR pmwrtsum.t_FIID <> prev_fininstr
                  OR PortfID <> prev_portfolio
                  OR PortfID IN (6, 8, 9, 10, 11) /* по посделочным без вариантов вызываем каждый раз. GetAccountByLot  по этим лотам нужно оптимизировать в первую очередь*/
                  OR (PortfID = 7
                      AND (prev_parent IS NULL
                           OR prev_parent <> pmwrtsum.t_parent)
                           ) /*в БПП по одному счету могло быть продано очень много лотов, поэтому получаем счет тоьлко один раз в разрезе t_parent*/
               THEN
                  account_rec := GetAccountByLot (pmwrtsum.t_sumid, RepDate);
               --                dbms_output.put_line('fiid'||'   ' ||prev_fininstr||'  '||pmwrtsum.t_FIID);
               --                dbms_output.put_line('portolio'||'   ' ||prev_portfolio||'    '||pmwrtsum.t_portfolio);
               --                dbms_output.put_line('t_account    '||account_rec.t_account);
               ELSE
                  NULL;
               --dbms_output.put_line(account_rec.t_account);
               --не обнуляем т.к. получили все на предыдущем шаге
               END IF;

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
               IF debug_tm = 1 THEN
                   tm_global := tm_global + (systimestamp - tm_start);
                   tm_start := systimestamp;
                   dbms_output.put_line('1.1 добавили мелочевку и начинаем считать числовые параметры  '||tm_global);
                end if;
               -- числовые параметры
              --  dbms_output.put_line ('1--pmwrtsum.t_DocKind:'||pmwrtsum.t_DocKind||'pmwrtsum.t_DocID:'||pmwrtsum.t_DocID||'pmwrtsum.t_PartNum:'||pmwrtsum.t_PartNum||'pmwrtsum.t_Portfolio:'||pmwrtsum.t_Portfolio);
              if vscwrthist_fields is null then
                  select listagg(c.column_name,',') within group (order by c.column_id) into vscwrthist_fields
                  from user_tab_columns c where c.table_name = 'V_SCWRTHISTEX2';
              end if;  
               begin
               execute immediate
               'select '||vscwrthist_fields||'
                from ( SELECT t.*,
                         max(t_changedate) over ( partition by t_sumid) as max_changedate,
                         max(t_instance) over ( partition by t_sumid,t_changedate) as max_instance
                      FROM v_scwrthistex2 t
                      WHERE  t.t_DocKind = :1                  --pmwrtsum.t_DocKind
                         AND t.t_DocID = :2                    --pmwrtsum.t_DocID
                         AND decode(t.t_PartNum,:3,1,0) = 1    --pmwrtsum.t_PartNum
                         AND decode(t.t_Portfolio,:4,1,0) = 1  --pmwrtsum.t_Portfolio
                         and t.t_changedate <= :5              --RepDate
                       )
                 where max_changedate = t_changedate
                       and max_instance = t_instance '
                 into vscwrthist
                 using pmwrtsum.t_DocKind,pmwrtsum.t_DocID, pmwrtsum.t_PartNum,pmwrtsum.t_Portfolio, RepDate  ;
               /* SELECT t.*
                 INTO vscwrthist
                 FROM v_scwrthistex2 t
                WHERE     t.t_DocKind = pmwrtsum.t_DocKind
                      AND t.t_DocID = pmwrtsum.t_DocID
                      AND t.t_PartNum = pmwrtsum.t_PartNum
                      AND t.t_Portfolio = pmwrtsum.t_Portfolio
                      AND t.t_changedate =
                          (SELECT MAX (v2.t_changedate)
                             FROM v_scwrthistex2 v2
                            WHERE t.t_sumid = v2.t_sumid AND v2.t_changedate <= RepDate)
                      AND t.t_instance =
                          (SELECT MAX (v.t_instance)
                             FROM v_scwrthistex2 v
                            WHERE t.t_sumid = v.t_sumid AND t.t_changedate = v.t_changedate);*/

                 exception when no_data_found then
                 dbms_output.put_line ('no_data_found pmwrtsum.t_DocKind:'||pmwrtsum.t_DocKind||'pmwrtsum.t_DocID:'||pmwrtsum.t_DocID||'pmwrtsum.t_PartNum:'||pmwrtsum.t_PartNum||'pmwrtsum.t_Portfolio:'||pmwrtsum.t_Portfolio);
                 WHEN OTHERS THEN
                 dbms_output.put_line ('OTHERS pmwrtsum.t_DocKind:'||pmwrtsum.t_DocKind||'pmwrtsum.t_DocID:'||pmwrtsum.t_DocID||'pmwrtsum.t_PartNum:'||pmwrtsum.t_PartNum||'pmwrtsum.t_Portfolio:'||pmwrtsum.t_Portfolio);
                 end;
                 IF debug_tm = 1  THEN                     
                     tm_global := tm_global + (systimestamp - tm_start);
                      tm_start := systimestamp;
                      dbms_output.put_line('1.2 отобрали из хиста '||tm_global);
                 end if;
               IsBond :=
                  RSI_RSB_FIInstr.FI_IsAvrKindBond (fininstr.t_avoirkind);

               IF PortfID NOT IN (PortfID_OD_req, PortfID_OD_com)
               THEN   /*для портфелей +\-ОД колонки по НКД и Затратам пустые*/
                  IF IsBond
                  THEN
                     NKD := vscwrthist.t_NKDAmount;

                     IF fininstr.t_FaceValueFI != PM_COMMON.NATCUR
                     THEN
                        DateForConv := CASE WHEN Date108Note != TO_DATE ('01.01.0001', 'dd.mm.yyyy') THEN Date108Note ELSE RepDate END;
                        NKD :=
                           ConvSum (NKD,
                                                fininstr.t_FaceValueFI,
                                                PM_COMMON.NATCUR,
                                                DateForConv);
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
                        ConvSum (BuyCostRUR,
                                             vscwrthist.t_Currency,
                                             fininstr.t_FaceValueFI,
                                             SetDate);

                     IF fininstr.t_FaceValueFI != PM_COMMON.NATCUR
                     THEN
                        BuyCostRUR :=
                           ConvSum (BuyCostRUR,
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
                        ConvSum (CostInCUR,
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
                     DateForConv := CASE WHEN Date108Note != TO_DATE ('01.01.0001', 'dd.mm.yyyy') THEN Date108Note ELSE RepDate END;
                     IF PortfID IN
                           (PortfID_Trade,
                            PortfID_Sale,
                            PortfID_Retire,
                            PortfID_Promissory)
                        OR (PortfID = PortfID_Unadmitted
                            AND vscwrthist.t_Portfolio !=
                                   RSB_PMWRTOFF.KINDPORT_CONTR)
                     THEN
                        DiscountIncomeCUR := vscwrthist.t_DiscountIncome;
                        DiscountIncomeRUR :=
                           ConvSum (DiscountIncomeCUR,
                                                fininstr.t_FaceValueFI,
                                                PM_COMMON.NATCUR,
                                                DateForConv);

                        InterestIncomeCUR := vscwrthist.t_InterestIncome;
                        InterestIncomeRUR :=
                           ConvSum (InterestIncomeCUR,
                                                fininstr.t_FaceValueFI,
                                                PM_COMMON.NATCUR,
                                                DateForConv);
                     END IF;

                     BonusCUR := vscwrthist.t_BegBonus - vscwrthist.t_Bonus; --vscwrthist.t_NotWrtBonus;
                     BonusRUR :=
                        ConvSum (BonusCUR,
                                             fininstr.t_FaceValueFI,
                                             PM_COMMON.NATCUR,
                                             DateForConv);
                  END IF;
               /*Под стоимостью вложений будем понимать:
                 ТП, ППР, ПДО, ТП/ППР БПП и т.д. - чистая стоимость (cost) + НКД (NKDAMOUNT)
                 ПВО - проще всего текущую стоимость  - balancecost
                 Под стоимостью в +/-ОД - стоимость в ОД (balancecostbd)
                 Затраты и НКД в +/-ОД не выводятся (по факту затраты в ПВО тоже 0)*/
               /* код отжил свое, удалим в следующий раз . BalCostRUR больше не нужна
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
                      BalCostRUR :=
                         ConvSum (BalCostCUR,
                                              fininstr.t_FaceValueFI,
                                              PM_COMMON.NATCUR,
                                              RepDate);
                   END IF;*/
               ELSE
                  IF IsBond
                  THEN
                     /*ПД для УП "ТП", "ППР", "ПУДП" "БПП", "ПВО"*/
                     /*ДД для УП "ТП", "ППР", "ПУДП" "БПП"*/
                     IF PortfID IN
                           (PortfID_Trade,
                            PortfID_Sale,
                            PortfID_Retire,
                            PortfID_Back,
                            PortfID_Promissory)
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

                     BonusRUR := vscwrthist.t_BegBonus - vscwrthist.t_Bonus; --vscwrthist.t_NotWrtBonus;
                  END IF;
       /*
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
                                 END IF;*/
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
               IF debug_tm = 1  THEN
tm_global := tm_global + (systimestamp - tm_start);
tm_start := systimestamp;
dbms_output.put_line('1.3 собрали все из хиста и полезем сейчас в лег '||tm_global);
end if;
begin  
             SELECT *
                 INTO dl_leg
                 FROM ddl_leg_dbt t
                WHERE     t.t_legkind = LegKind
                      AND t.t_dealid = pmwrtsum.t_dealid
                      AND t.t_legid = 0;
 -- dbms_output.put_line(dl_leg);                    
exception when no_data_found then
 dbms_output.put_line('ничего ненашли');
end;                      
IF debug_tm = 1  THEN
tm_global := tm_global + (systimestamp - tm_start);
tm_start := systimestamp;
dbms_output.put_line('1.4  забрали инфу из лега '||tm_global);
end if;
               ReservAmountRUR := vscwrthist.t_ReservAmount + vscwrthist.t_incomeReserv;

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

                     rec_rep.AMOUNT :=
                          vscwrthist.t_cost
                        - (vscwrthist.t_begbonus - vscwrthist.t_bonus)
                        + vscwrthist.t_costpfi;
                  --vscwrthist.t_amount * RSB_FIINSTR.FI_GETNOMINALONDATE(vscwrthist.t_fiid, RepDate,2);
                  WHEN vscwrthist.t_portfolio = 6                      /*ПВО*/
                  THEN
                     rec_rep.AMOUNT :=
                        rsi_rsb_account.resta (account_rec.t_Account,
                                               RepDate,
                                               3,
                                               NULL,
                                               fininstr.t_FaceValueFI);
                     rec_rep.AMOUNT :=
                        ConvSum (rec_rep.AMOUNT,
                                             fininstr.t_FaceValueFI,
                                             PM_COMMON.NATCUR,
                                             RepDate);
                  ELSE
                     rec_rep.Amount :=
                        vscwrthist.t_cost + vscwrthist.t_costpfi;
               END CASE;
               IF debug_tm = 1  THEN
tm_global := tm_global + (systimestamp - tm_start);
tm_start := systimestamp;
dbms_output.put_line('1.5 все собрали '||tm_global);
end if;

               --rec_rep.AMOUNT                     :=  vscwrthist.t_amount;
               rec_rep.UNKD_WITH_COUPON := NKD;
               rec_rep.COUPON_AMOUNT := InterestIncomeRUR;
               rec_rep.DISCOUNT_AMOUNT := DiscountIncomeRUR;
               rec_rep.PREMIUM_AMOUNT := BonusRUR;
               --               rec_rep.BALANCE_PRICE :=  rec_rep.AMOUNT + rec_rep.UNKD_WITH_COUPON
               --                                                                                   + rec_rep.COUPON_AMOUNT + rec_rep.DISCOUNT_AMOUNT + rec_rep.PREMIUM_AMOUNT;--BalCostRUR;
               rec_rep.OVERVALUE := OverAmountRUR;
               rec_rep.RESERVE_AMOUNT := ReservAmountRUR;

               IF PortfID IN (PortfID_OD_req, PortfID_OD_com)
               THEN
                  rec_rep.T_AVRAMOUNT := vscwrthist.t_amountBD;
               ELSE
                  rec_rep.T_AVRAMOUNT := vscwrthist.t_amount;
               END IF;
               rec_rep.ESTRESERVE:= vscwrthist.t_estreserve;
               rec_rep.CORRINTTOEIR := vscwrthist.t_corrinttoeir;
               rec_rep.REPORT_DT := RepDate;

               rec_rep.CORRESTRESERVE:= vscwrthist.t_correstreserve;

               INSERT INTO DPORTFOLIO_TMP
                    VALUES rec_rep;
                    IF debug_tm = 1 then
                    tm_global := tm_global + (systimestamp - tm_start);
                    tm_start := systimestamp;
                    dbms_output.put_line('1.7 заинсертили в табличку '||tm_global);
                    end if;
            END IF;
         /*exception
            when others then null;*/

         END;

         prev_fininstr := fininstr.t_FIID;
         prev_portfolio := PortfID;
         prev_parent := vscwrthist.t_parent;
      END LOOP;
      
   END LOOP;
   
   
   /*bpv отдельным циклом добавим данные по открытым сделкам 280419*/
   /*BIQ-4441 START*/
/*надо заполнить    
 T_PORTFID, T_ISQUOTED, T_DEALID, 
   T_SUMID, T_FIID, T_PARTYID, 
   T_AVKINDSHNAME, T_ACCOUNT, T_BALANCE, 
   T_DOCKIND, T_DOCID, T_PARTNUM, 
   T_DEPARTMENT, T_AVRAMOUNT, AMOUNT, 
   UNKD_WITH_COUPON, COUPON_AMOUNT, DISCOUNT_AMOUNT, 
   PREMIUM_AMOUNT, BALANCE_PRICE, OVERVALUE, 
   RESERVE_AMOUNT, ESTRESERVE, CORRINTTOEIR, 
   REPORT_DT, CORRESTRESERVE
*/
IF debug_tm = 1  THEN
tm_global := tm_global + (systimestamp - tm_start);
tm_start := systimestamp;
dbms_output.put_line('4  перед внебалансом '||tm_global);
end if;
   INSERT INTO DPORTFOLIO_TMP
   (T_PORTFID, T_ISQUOTED, T_DEALID, 
   T_SUMID, T_FIID, T_PARTYID, 
   T_AVKINDSHNAME, T_ACCOUNT, T_BALANCE, 
   T_DOCKIND, T_DOCID, T_PARTNUM, 
   T_DEPARTMENT, T_AVRAMOUNT, AMOUNT, 
   UNKD_WITH_COUPON, COUPON_AMOUNT, DISCOUNT_AMOUNT, 
   PREMIUM_AMOUNT, BALANCE_PRICE, OVERVALUE, 
   RESERVE_AMOUNT, ESTRESERVE, CORRINTTOEIR, 
   REPORT_DT, CORRESTRESERVE)
   SELECT 
   t.t_portfolioid, 
     (     SELECT Attr.t_NumInList
        FROM dobjatcor_dbt AtCor, dobjattr_dbt Attr
       WHERE     AtCor.t_ObjectType = cnst.OBJTYPE_AVOIRISS
             AND AtCor.t_GroupID = 27
             AND AtCor.t_Object = LPAD (t.t_pfi, 10, '0')
             AND AtCor.t_ValidFromDate =
                    (SELECT MAX (t.T_ValidFromDate)
                       FROM DOBJATCOR_DBT t
                      WHERE     t.T_ObjectType = AtCor.T_ObjectType
                            AND t.T_GroupID = AtCor.T_GroupID
                            AND t.t_Object = AtCor.t_Object
                            AND t.T_ValidFromDate <= RepDate)
             AND Attr.t_AttrID = AtCor.t_AttrID
             AND Attr.t_ObjectType = AtCor.t_ObjectType
             AND Attr.t_GroupID = AtCor.t_GroupID ) isquoted, 
   t.t_dealid,
   0 t_sumid,
   t.t_pfi,
   RSI_RSB_FIInstr.FI_GetIssuerOnDate (t.t_pfi, RepDate) partyid,
    (SELECT t_name
                 FROM davrkinds_dbt 
                WHERE t_fi_kind = fininstr.t_fi_kind
                      AND t_avoirkind = fininstr.t_AvoirKind ) avrkind,
   a.t_account,
   (select t_balance from daccount_dbt dac where dac.t_account = a.t_account) balance,
    t.t_bofficekind,
    t.t_dealid,
    0 t_partnum,
    t.t_department,
    l.t_principal,
    ABS (rsb_account.restall (a.t_account, a.t_chapter, a.t_currency, RepDate)) rest,
    0, 0, 0, 0, 0, 0, 0, 0, 0, RepDate, 0
  FROM dfininstr_dbt fininstr , ddl_leg_dbt l, ddl_tick_dbt t
       LEFT JOIN dmcaccdoc_dbt a
          ON     a.t_dockind = t.t_bofficekind
             AND a.t_docid = t.t_dealid
             AND a.t_catnum IN (318, 317)
       INNER JOIN dmctempl_dbt m
          ON     a.t_catid = m.t_catid
             AND a.t_templnum = m.t_number
             AND m.t_value2 = 3
 WHERE     t_dealdate <= RepDate
       AND t_clientid = -1
       AND l.t_dealid = t.t_dealid 
       AND l.t_legkind = 0
       AND t.t_pfi = fininstr.t_fiid
       AND (p_infiid = -1 OR p_infiid = t.t_pfi)
       AND rsb_account.restall (a.t_account, a.t_chapter, a.t_currency, RepDate) <> 0
       AND EXISTS
              (SELECT 1 FROM ddlrq_dbt r
                WHERE     t.t_dealid = r.t_docid
                      AND t.t_bofficekind = r.t_dockind
                      AND DECODE ( r.t_factdate, TO_DATE ('01010001', 'ddmmyyyy'), r.t_plandate, r.t_factdate) >= RepDate);

  /*BIQ-4441 END*/ 
   IF debug_tm = 1  THEN
tm_global := tm_global + (systimestamp - tm_start);
tm_start := systimestamp;
dbms_output.put_line('5 перед вторым циклом агрегации  '||tm_global);
end if;
   IF (clear_date) AND (p_infiid <> -1)
   THEN
      SELECT *
        INTO avoiriss
        FROM davoiriss_dbt
       WHERE t_fiid = p_inFIID;

      DELETE FROM tRSHB_Portfolio_PKL_2CHD
            WHERE     report_dt = pdate
                  AND reg_number = avoiriss.t_lsin
                  AND isin = avoiriss.t_isin;
   ELSE
      DELETE FROM tRSHB_Portfolio_PKL_2CHD
            WHERE report_dt = pdate;
   END IF;
--dbms_output.put_line('!!10!!');
   FOR TempFile IN (  SELECT T_PORTFID,
                             T_FIID,
                             T_PARTYID,
                             T_AVKINDSHNAME,
                             T_ACCOUNT,
                             T_BALANCE,
                             SUM (NVL (T_AVRAMOUNT, 0)) AVRAMOUNT,
                             SUM (NVL (AMOUNT, 0)) AMOUNT,
                             ROUND(SUM (NVL (UNKD_WITH_COUPON, 0)),2) UNKD_WITH_COUPON,
                             ROUND(SUM (NVL (COUPON_AMOUNT, 0)),2) COUPON_AMOUNT,
                             ROUND(SUM (NVL (DISCOUNT_AMOUNT, 0)),2) DISCOUNT_AMOUNT,
                             ROUND(SUM (NVL (PREMIUM_AMOUNT, 0)),2) PREMIUM_AMOUNT,
                             SUM (NVL (BALANCE_PRICE, 0)) BALANCE_PRICE,
                             SUM (NVL (OVERVALUE, 0)) OVERVALUE,
                             SUM (NVL (RESERVE_AMOUNT, 0)) RESERVE_AMOUNT,
                             SUM (NVL (ESTRESERVE, 0)) ESTRESERVE,
                             SUM (NVL (CORRESTRESERVE, 0)) CORRESTRESERVE,
                             SUM (NVL (CORRINTTOEIR, 0)) CORRINTTOEIR
                        FROM DPORTFOLIO_TMP
                       WHERE t_portfid NOT IN (3 /*ПКУ   слегка костыльно убираем по просьбе Демидова В. все бумаги учитываемые на счетах 6*  */
                                                ) and REPORT_DT = RepDate
                                                and ((p_infiid = -1) or (p_infiid = t_fiid))
                    GROUP BY T_PORTFID,
                             T_FIID,
                             T_PARTYID,
                             T_AVKINDSHNAME,
                             T_ACCOUNT,
                             T_BALANCE)
   LOOP
      SELECT *
        INTO fininstr
        FROM dfininstr_dbt
       WHERE t_fiid = TempFile.t_FIID;

      SELECT *
        INTO avoiriss
        FROM davoiriss_dbt
       WHERE t_fiid = TempFile.t_FIID;

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

      /* Валюта номинала
         ISO-код валюты номинала */
      BEGIN
         SELECT REPLACE (t_iso_number, '643', '810')
           INTO rec_portfolio.CURRENCY
           FROM dfininstr_dbt
          WHERE t_fiid = fininstr.t_FaceValueFI;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            NULL;
      END;

      --      dbms_output.put_line('sdfsdf  '||rec_portfolio.CURRENCY);

      IsBond := RSI_RSB_FIInstr.FI_IsAvrKindBond (fininstr.t_avoirkind);

      IF IsBond AND RSI_RSB_FIInstr.FI_IsCouponAvoiriss (fininstr.t_FIID) = 1
      THEN
         BEGIN
            SELECT MAX (t_DrawingDate)
              INTO rec_portfolio.COUPON_REPAY_DT
              FROM dfiwarnts_dbt
             WHERE     t_fiid = fininstr.t_FIID
                   AND t_IsPartial != 'X'
                   AND t_FirstDate <= pdate;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               rec_portfolio.COUPON_REPAY_DT := NULL;
         END;

         IF rec_portfolio.COUPON_REPAY_DT = ZeroDate
            OR rec_portfolio.COUPON_REPAY_DT < pdate
         THEN
            rec_portfolio.COUPON_REPAY_DT := NULL;
         END IF;
      ELSE
         rec_portfolio.COUPON_REPAY_DT := NULL;
      END IF;

      /* Дата погашения облигации
         Плановая дата погашения выпуска (выводится только для облигаций) */
      IF IsBond
      THEN
         rec_portfolio.REPAY_DT := fininstr.t_DrawingDate;
         rec_portfolio.ISSUE_START_DT := fininstr.t_Issued;
      ELSE
         rec_portfolio.REPAY_DT := NULL;
         rec_portfolio.ISSUE_START_DT := NULL;
      END IF;

      rec_portfolio.PortFolio := TempFile.t_PortfID;
      rec_portfolio.REPORT_DT := pdate;

      SELECT SUBSTR (t.t_shortname, 1, 30), t.t_nrcountry
        INTO rec_portfolio.ISSUER, vcountrycode
        FROM dparty_dbt t
       WHERE t.t_partyid = TempFile.T_PARTYID;

      -- Страна Эмитента
      BEGIN
         rec_portfolio.COUNTRY_RATING := '';               -- Страновая оценка

         IF vcountrycode = CHR (1)
         THEN
            --rec_portfolio.ISSUER_COUNTRY             := '';
            vcountrycode := 'RUS';                   -- не указано, значит, РФ
         END IF;

         SELECT SUBSTR (t_fullname, 1, 30), T_RISKCLASS
           INTO rec_portfolio.ISSUER_COUNTRY, rec_portfolio.COUNTRY_RATING
           FROM dcountry_dbt
          WHERE t_codelat3 = vcountrycode;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            rec_portfolio.ISSUER_COUNTRY := vcountrycode;
         WHEN OTHERS
         THEN
            rec_portfolio.ISSUER_COUNTRY := '';
      END;

      rec_portfolio.fi_group := TempFile.t_AvKindShName;
      rec_portfolio.security_type := sectype (fininstr.t_FIID);
      rec_portfolio.SECURITY_NAME := SUBSTR (fininstr.t_Name, 1, 25);
      rec_portfolio.ACCOUNT_NUMBER_2 := TempFile.t_Account;
      rec_portfolio.ACCOUNT_NUMBER_1 := TempFile.t_balance;  --Балансовый счет

      rec_portfolio.REG_NUMBER := avoiriss.t_LSIN;
      rec_portfolio.ISIN := avoiriss.t_ISIN;
      Date108Note := CASE WHEN rsi_rsb_kernel.GetNote(RSB_SECUR.OBJTYPE_AVOIRISS, LPAD(fininstr.T_FIID, 10, '0'), 108, RepDate) IS NULL 
                                THEN TO_DATE ('01.01.0001', 'dd.mm.yyyy')
                                ELSE rsb_struct.getDate(rsi_rsb_kernel.GetNote(RSB_SECUR.OBJTYPE_AVOIRISS, LPAD(fininstr.T_FIID, 10, '0'), 108, RepDate)) 
                            END;
      DateForConv := CASE WHEN Date108Note != TO_DATE ('01.01.0001', 'dd.mm.yyyy') THEN Date108Note ELSE pdate END;
      rec_portfolio.AMOUNT :=
         ConvSum ( (TempFile.AMOUNT),
                              fininstr.t_facevaluefi,
                              PM_COMMON.NATCUR,
                              DateForConv);
      --dbms_output.put_line(nvl(rec_portfolio.AMOUNT,'ffff'));
      --dbms_output.put_line(nvl(TempFile.AMOUNT,'2ffff'));
      --                              dbms_output.put_line( '1 '|| fininstr.t_facevaluefi  );
      --                              dbms_output.put_line(  ' 2 '||PM_COMMON.NATCUR  );
      --                              dbms_output.put_line( pdate  );
      --                              dbms_output.put_line(  '  *----*  '  );
      rec_portfolio.UNKD_WITH_COUPON := TempFile.UNKD_WITH_COUPON;
      rec_portfolio.COUPON_AMOUNT := TempFile.COUPON_AMOUNT;
      rec_portfolio.DISCOUNT_AMOUNT := TempFile.DISCOUNT_AMOUNT;
      rec_portfolio.PREMIUM_AMOUNT := TempFile.PREMIUM_AMOUNT;
      rec_portfolio.BALANCE_PRICE :=
           rec_portfolio.AMOUNT
         + rec_portfolio.UNKD_WITH_COUPON
         + rec_portfolio.COUPON_AMOUNT
         + rec_portfolio.DISCOUNT_AMOUNT
         + rec_portfolio.PREMIUM_AMOUNT;

      /*BEGIN                              --получаем переоценку Т+ по выпуску
         IF TempFile.t_PortfID <> 7
         THEN
            SELECT SUM (DECODE (t_kind, 10, -1, 1) * t_sum)
              INTO rec_portfolio.OVERVALUE
              FROM ddl_value_dbt
             WHERE t_dockind = 101 AND t_kind IN (9, 10) AND t_date = RepDate --TO_DATE ('300119', 'ddmmyy')
                   AND t_docid IN
                          (SELECT t_dealid
                             FROM ddl_tick_dbt
                            WHERE t_clientid = -1 AND t_pfi = fininstr.t_fiid);
         ELSE
            rec_portfolio.OVERVALUE := NULL;
         END IF;

         IF rec_portfolio.OVERVALUE IS NULL
         THEN
            rec_portfolio.OVERVALUE := TempFile.OVERVALUE;
         ELSE
            rec_portfolio.OVERVALUE :=
               rec_portfolio.OVERVALUE + TempFile.OVERVALUE;
         END IF;
         

         DBMS_OUTPUT.
          put_line ('rec_portfolio.OVERVALUE  ' || rec_portfolio.OVERVALUE);
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            rec_portfolio.OVERVALUE := TempFile.OVERVALUE;
      END;
*/
      rec_portfolio.OVERVALUE := TempFile.OVERVALUE;
      
      rec_portfolio.RESERVE_AMOUNT := TempFile.RESERVE_AMOUNT;
      rec_portfolio.ESTRESERVE := TempFile.ESTRESERVE; --скоро могут попросить добавить
      rec_portfolio.CORRESTRESERVE := TempFile.CORRESTRESERVE; --скоро могут попросить добавить
      --rec_portfolio.CORRINTTOEIR := TempFile.CORRINTTOEIR; скоро могут попросить добавить
      
      rec_portfolio.TSS_AMOUNT := rec_portfolio.BALANCE_PRICE + rec_portfolio.OVERVALUE;
      --rec_portfolio.TSS_AMOUNT := NVL (ConvSumtype (1,fininstr.t_FIID,fininstr.t_FaceValueFI,12,RepDate),0);

      rec_portfolio.IS_NFO := DefineIsNFO (TempFile.T_PARTYID);      -- ФО/НФО
      rec_portfolio.IS_SPV := getSPV (TempFile.T_PARTYID); -- SPV (да/нет)   VARCHAR2(3)
      rec_portfolio.IS_HYPO_COVER := gethypocover (fininstr.t_FIID); -- Ипотечное покрытие (да/нет)    VARCHAR2(3)

      -- категории субъекта
      rec_portfolio.ISSUER_OKVED :=
         GetObjAttrValue (cnst.OBJTYPE_PARTY,
                          TempFile.T_PARTYID,
                          64,
                          pdate);                            -- ОКВЭД эмитента

      rec_portfolio.ISSUER_RATING_SP :=
         GetRating (cnst.OBJTYPE_PARTY,
                                TempFile.T_PARTYID,
                                ISSUER_RATING,
                                110,
                                pdate); -- Рейтинг Эмитента (Standart and Poors)   VARCHAR2(4)
      rec_portfolio.ISSUER_RATING_3453U_SP :=
         GetRating (cnst.OBJTYPE_PARTY,
                                TempFile.T_PARTYID,
                                ISSUER_RATING,
                                459,
                                pdate); -- Рейтинг Эмитента в соответствии с  Указанием 3453-У (Standart and Poors)    VARCHAR2(4)
      rec_portfolio.ISSUER_RATING_MOODYS :=
         GetRating (cnst.OBJTYPE_PARTY,
                                TempFile.T_PARTYID,
                                ISSUER_RATING,
                                210,
                                pdate); -- Рейтинг Эмитента (Moodys )       VARCHAR2(4)
      rec_portfolio.ISSUER_RATING_3453U_MOODYS :=
         GetRating (cnst.OBJTYPE_PARTY,
                                TempFile.T_PARTYID,
                                ISSUER_RATING,
                                483,
                                pdate); -- Рейтинг Эмитента в соответствии с  Указанием 3453-У (Moodys )     VARCHAR2(4)
      rec_portfolio.ISSUER_RATING_FITCH :=
         GetRating (cnst.OBJTYPE_PARTY,
                                TempFile.T_PARTYID,
                                ISSUER_RATING,
                                310,
                                pdate); -- Рейтинг Эмитента (Fitch Ratings) VARCHAR2(4)
      rec_portfolio.ISSUER_RATING_3453U_FITCH :=
         GetRating (cnst.OBJTYPE_PARTY,
                                TempFile.T_PARTYID,
                                ISSUER_RATING,
                                505,
                                pdate); -- Рейтинг Эмитента в соответствии с  Указанием 3453-У (Fitch Ratings)  VARCHAR2(4)
      rec_portfolio.ISSUER_RATING_EXPERT :=
         GetRating (cnst.OBJTYPE_PARTY,
                                TempFile.T_PARTYID,
                                ISSUER_RATING,
                                367,
                                pdate); -- Рейтинг Эмитента Эксперт РА  VARCHAR2(4)
      rec_portfolio.ISSUER_RATING_AKRA :=
         GetRating (cnst.OBJTYPE_PARTY,
                                TempFile.T_PARTYID,
                                ISSUER_RATING,
                                436,
                                pdate);   -- Рейтинг Эмитента АКРА VARCHAR2(4)

      -- категории ценной бумаги
      rec_portfolio.ISSUE_RATING_SP :=
         GetRating (cnst.OBJTYPE_AVOIRISS,
                                TempFile.t_FIID,
                                ISSUE_RATING,
                                110,
                                pdate); -- Рейтинг эмиссии (Standart and Poors)    VARCHAR2(4)
      rec_portfolio.ISSUE_RATING_3453U_SP :=
         GetRating (cnst.OBJTYPE_AVOIRISS,
                                TempFile.t_FIID,
                                ISSUE_RATING,
                                459,
                                pdate); -- Рейтинг эмиссии в соответствии с  Указанием 3453-У (Standart and Poors)     VARCHAR2(4)
      rec_portfolio.ISSUE_RATING_MOODYS :=
         GetRating (cnst.OBJTYPE_AVOIRISS,
                                TempFile.t_FIID,
                                ISSUE_RATING,
                                210,
                                pdate); -- Рейтинг эмиссии (Moodys)         VARCHAR2(4)
      rec_portfolio.ISSUE_RATING_3453U_MOODYS :=
         GetRating (cnst.OBJTYPE_AVOIRISS,
                                TempFile.t_FIID,
                                ISSUE_RATING,
                                483,
                                pdate); -- Рейтинг эмиссии в соответствии с  Указанием 3453-У (Moodys )      VARCHAR2(4)
      rec_portfolio.ISSUE_RATING_FITCH :=
         GetRating (cnst.OBJTYPE_AVOIRISS,
                                TempFile.t_FIID,
                                ISSUE_RATING,
                                310,
                                pdate); -- Рейтинг эмиссии (Fitch Ratings)  VARCHAR2(4)
      rec_portfolio.ISSUE_RATING_3453U_FITCH :=
         GetRating (cnst.OBJTYPE_AVOIRISS,
                                TempFile.t_FIID,
                                ISSUE_RATING,
                                505,
                                pdate); -- Рейтинг эмиссии в соответствии с  Указанием 3453-У (Fitch Ratings)   VARCHAR2(4)
      rec_portfolio.ISSUE_RATING_EXPERT :=
         GetRating (cnst.OBJTYPE_AVOIRISS,
                                TempFile.t_FIID,
                                ISSUE_RATING,
                                367,
                                pdate); -- Рейтинг Эмитента Эксперт РА  VARCHAR2(4)
      rec_portfolio.ISSUE_RATING_AKRA :=
         GetRating (cnst.OBJTYPE_AVOIRISS,
                                TempFile.t_FIID,
                                ISSUE_RATING,
                                436,
                                pdate);   -- Рейтинг Эмитента АКРА VARCHAR2(4)
                                
   begin
      select i.t_indicator into rec_portfolio.devaluation from f657_ind_dep i 
      where i.t_isin = rec_portfolio.ISIN 
      and i.t_date_load = (select max(di.t_date_load) from f657_ind_dep di 
      where di.t_isin = rec_portfolio.ISIN 
      and di.t_date_load <= rec_portfolio.report_dt) 
      and rownum = 1;/*bpv есть два варианта t_ownership  "собственные"  и "РЕПО"- необходимо уточинть какой и как нужно использовать*/
   exception
         WHEN NO_DATA_FOUND
         THEN
            rec_portfolio.devaluation := 0;
      END;
 

      -- параметры не заполняются и не должны
      
      --rec_portfolio.IS_INTERNATIONAL_ORG := ''; -- Признак международной организации   не заполняется
      begin
        select 'ДА' into rec_portfolio.IS_INTERNATIONAL_ORG from dpartyown_dbt py where PY.T_PARTYID = TempFile.T_PARTYID  and PY.T_PARTYKIND = 32;
        exception 
      when no_data_found then
         rec_portfolio.IS_INTERNATIONAL_ORG := 'НЕТ';
      END;   
      
      rec_portfolio.IS_OESR_EUROZONE := ''; -- Признак ОЭСР, Еврозона              не заполняется
      rec_portfolio.IS_STATE_BORROW := ''; -- Признак права заимствования от имени государства   не заполняется
      --rec_portfolio.IS_IN_MMWB_INDEX_50 := '';  --'Включение биржей в списки для расчета Индекса ММВБ 50 (да/нет)';
      BEGIN
        SELECT upper(a.t_name) INTO rec_portfolio.IS_IN_MMWB_INDEX_50 
          FROM    dobjattr_dbt a
               JOIN
                  dobjatcor_dbt c
               ON     a.t_objecttype = c.t_objecttype
                  AND a.t_groupid = c.t_groupid
                  AND a.t_attrid = c.t_attrid
         WHERE     c.t_objecttype = 12
               AND c.t_groupid = 103
               AND c.t_object = LPAD (TempFile.t_fiid, 10, '0')
               AND c.t_validtodate = TO_DATE ('31.12.9999', 'dd.mm.yyyy');
      EXCEPTION
        When no_data_found then rec_portfolio.IS_IN_MMWB_INDEX_50 := 'НЕТ';
      END;       

      --rec_portfolio.IS_IN_RTS_INDEX_50 := '';  --'Включение биржей в списки для расчета Индекса РТС 50 (да/нет)';
      BEGIN
        SELECT upper(a.t_name) INTO rec_portfolio.IS_IN_RTS_INDEX_50 
          FROM    dobjattr_dbt a
               JOIN
                  dobjatcor_dbt c
               ON     a.t_objecttype = c.t_objecttype
                  AND a.t_groupid = c.t_groupid
                  AND a.t_attrid = c.t_attrid
         WHERE     c.t_objecttype = 12
               AND c.t_groupid = 104
               AND c.t_object = LPAD (TempFile.t_fiid, 10, '0')
               AND c.t_validtodate = TO_DATE ('31.12.9999', 'dd.mm.yyyy');
      EXCEPTION
        When no_data_found then rec_portfolio.IS_IN_RTS_INDEX_50 := 'НЕТ';
      END;

      IF substr(TempFile.t_balance,1,3) = '504' THEN
      --'Рыночная стоимость ценной бумаги (в валюте бумаги)';      
        --rec_portfolio.SEC_MARKET_PRICE_VAL := TempFile.AVRAMOUNT *(RSB_FIINSTR.FI_GETNOMINALONDATE(TempFile.t_FIID, RepDate))* (GetRate(TempFile.t_FIID, fininstr.t_facevaluefi, 4/*avgprice*/, RepDate)/100) + TempFile.COUPON_AMOUNT + TempFile.UNKD_WITH_COUPON;
        rec_portfolio.SEC_MARKET_PRICE_VAL := TempFile.AVRAMOUNT * (GetRate(TempFile.t_FIID, fininstr.t_facevaluefi, 4/*avgprice*/, RepDate, 0)) + TempFile.COUPON_AMOUNT + TempFile.UNKD_WITH_COUPON;
      --'Рыночная стоимость ценной бумаги (в рублевом эквиваленте)';      
       rec_portfolio.SEC_MARKET_PRICE_RUR := ConvSum (rec_portfolio.SEC_MARKET_PRICE_VAL, fininstr.t_FaceValueFI, PM_COMMON.NATCUR, RepDate);      
      ELSE
        rec_portfolio.SEC_MARKET_PRICE_VAL := '';
        rec_portfolio.SEC_MARKET_PRICE_RUR := '';       
      END IF;

      INSERT INTO tRSHB_Portfolio_PKL_2CHD
           VALUES rec_portfolio;
           
   END LOOP;


/*размазывание переоценки и корректировки по лотам в Прямом Репо пропорционально количеству/ . Искренне надеюсь, что заказчик передумает и уберем это*/
IF (spread = 1) THEN           
dbms_output.put_line('spread');
IF debug_tm = 1  THEN
tm_global := tm_global + (systimestamp - tm_start);
tm_start := systimestamp;
dbms_output.put_line('6 перед спредом  '||tm_global);
end if;
  update TRSHB_PORTFOLIO_PKL_2CHD a set overvalue = 
  (select ovr from (
  select cor/amnt*amnt1 cor, overvl/amnt*amnt1 ovr,  account_number_2 acc, reg_number reg from 
   (  select account_number_2, (select sum(t_avramount) from dportfolio_tmp m where (select t_isin from davoiriss_dbt where m.t_fiid = t_fiid ) = t.isin and t_portfid = 7 and report_dt = repdate ) amnt, 
   (select sum(overvalue) from dportfolio_tmp m where (select t_isin from davoiriss_dbt where m.t_fiid = t_fiid ) = t.isin and t_portfid = 7 and report_dt = repdate ) overvl,
   (select sum(corrinttoeir) from dportfolio_tmp m where (select t_isin from davoiriss_dbt where m.t_fiid = t_fiid ) = t.isin and t_portfid = 7 and report_dt = repdate ) cor,
  (select sum(t_avramount) from dportfolio_tmp m where (select t_isin from davoiriss_dbt where m.t_fiid = t_fiid ) = t.isin and t_account = account_number_2 and t_portfid = 7 and report_dt = repdate ) amnt1, 
  isin, overvalue, reg_number  from TRSHB_PORTFOLIO_PKL_2CHD t where report_dt = repdate and portfolio = 7)) q where a.account_number_2 = q.acc and  a.reg_number = q.reg)
  where report_dt = repdate and portfolio = 7;

  update dportfolio_tmp a set corrinttoeir = 
  (select cor from (
  select cor/amnt*amnt1 cor, overvl/amnt*amnt1 ovr,  t_sumid  from 
   (  select  (select sum(t_avramount) from dportfolio_tmp m where t_fiid = t.t_fiid and t_portfid = 7 and report_dt = repdate ) amnt, 
   (select sum(overvalue) from dportfolio_tmp m where t_fiid = t.t_fiid and t_portfid = 7 and report_dt = repdate ) overvl,
   (select sum(corrinttoeir) from dportfolio_tmp m where t_fiid = t.t_fiid and t_portfid = 7 and report_dt = repdate ) cor,
  (select sum(t_avramount) from dportfolio_tmp m where t_fiid = t.t_fiid and t_sumid = t.t_sumid and t_portfid = 7 and report_dt = repdate ) amnt1, 
  t_fiid, t_sumid , overvalue  from dportfolio_tmp t where report_dt = repdate and t_portfid = 7)) q where a.t_sumid = q.t_sumid)
  where report_dt = repdate and t_portfid = 7;
  /*
        UPDATE dportfolio_tmp m
           SET overvalue =
                     (SELECT round(t1.avgovervalue * t2.avramount ,2) spreadoveramount
                        FROM (  SELECT SUM (t_avramount) avramount,
                                       SUM (overvalue) overvalue,
                                       SUM (overvalue) / SUM (t_avramount) avgovervalue, t_fiid
                                  FROM dportfolio_tmp t
                                 WHERE t_portfid = 7 AND report_dt = RepDate
                              GROUP BY t_fiid) t1, (SELECT SUM (t_avramount) avramount,
                                       SUM (overvalue) overvalue,
                                       SUM (overvalue) / SUM (t_avramount) avgovervalue,
                                       t_account ,
                                      t_fiid
                                  FROM dportfolio_tmp t
                                 WHERE t_portfid = 7 AND report_dt = RepDate
                              GROUP BY t_fiid, t_account ) t2 where t1.t_fiid = t2.t_fiid and t2.t_account = m.t_account ),
             CORRINTTOEIR =
                    (SELECT round(t1.avgCORRINTTOEIR * t2.avramount ,2) spreadoverCORRINTTOEIR
                        FROM (  SELECT SUM (t_avramount) avramount,
                                       SUM (CORRINTTOEIR) CORRINTTOEIR,
                                       SUM (CORRINTTOEIR) / SUM (t_avramount) avgCORRINTTOEIR,
                                     t_fiid
                                  FROM dportfolio_tmp t
                                 WHERE t_portfid = 7 AND report_dt = RepDate
                              GROUP BY t_fiid) t1, (SELECT SUM (t_avramount) avramount,
                                       SUM (CORRINTTOEIR) CORRINTTOEIR,
                                       SUM (CORRINTTOEIR) / SUM (t_avramount) avgCORRINTTOEIR,
                                       t_account ,
                                      t_fiid
                                  FROM dportfolio_tmp t
                                 WHERE t_portfid = 7 AND report_dt = RepDate 
                              GROUP BY t_fiid, t_account ) t2 where t1.t_fiid = t2.t_fiid and M.T_ACCOUNT = t2.t_account )                
         WHERE report_dt = RepDate  and t_portfid = 7  ;     */
END IF;
--IF debug_tm = 1  THEN    
tm_global := tm_global + (systimestamp - tm_start);
tm_start := systimestamp;
dbms_output.put_line(' закончили  '||tm_global);           
--end if;
dbms_output.put_line('tm_getaccountbylot   '||tm_getaccountbylot);
dbms_output.put_line('tm_convsum  '||tm_convsum);
   COMMIT;
END;
/