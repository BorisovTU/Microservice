CREATE OR REPLACE PACKAGE BODY cft_utils IS

  --Encoding: Win 866

  /**
   @file    	cft_utils.pkb
   @brief       Утилиты для сверки проводок между ЦФТ и СОФР  

# changelog
 |date       |autor          |tasks                                           |note
 |-----------|---------------|------------------------------------------------|-------------------------------------------------------------
 |11.08.2025 |Велигжанин А.В.|DEF-95548                                       |Наименование получателя для проводок на 306 и 603
 |14.07.2025 |Велигжанин А.В.|DEF-95283                                       |getSqlSofr00(), запрос без фильтра 306-306
 |14.07.2025 |Велигжанин А.В.|DEF-95281                                       |Доработка определения наименования плательщика
 |           |               |                                                |При переводах t_sofr_payer_name может не определиться, берем из буферной таблицы
 |14.07.2025 |Велигжанин А.В.|DEF-95385                                       |Доработка getSqlCft306OnReqID(), из ЦФТ не забираем проводки по ЮЛ
 |11.07.2025 |Велигжанин А.В.|DEF-95283                                       |Исправление ошибки применения фильтра x_Filter306
 |11.07.2025 |Велигжанин А.В.|DEF-95281                                       |Доработка для наименования плательщика в переводах
 |10.07.2025 |Велигжанин А.В.|DEF-95283                                       |Настраиваемый фильтр в отчете-сверке по 306
 |10.07.2025 |Велигжанин А.В.|DEF-95217                                       |не показываем в отчете проводку по налогам (306-603), позднее требование
 |07.07.2025 |Велигжанин А.В.|DEF-94420                                       |CFT_GetSofr306OnDate(), получения проводок СОФР по 306-ым счетам за дату
 |27.05.2025 |Велигжанин А.В.|DEF-87040                                       |CFT_CheckLegal2(), для отбора проводок для ФЛ
 |08.04.2025 |Велигжанин А.В.|DEF-87040                                       |правка для CFT_CheckLegal(), определяется по 306-му счету
 |07.04.2025 |Велигжанин А.В.|DEF-87040                                       |правка для CFT_CheckLegal()
 |04.04.2025 |Велигжанин А.В.|DEF-87040                                       |правка для CFT_CheckLegal()
 |03.04.2025 |Велигжанин А.В.|DEF-87040                                       |правка для CFT_CheckLegal()
 |31.03.2025 |Велигжанин А.В.|DEF-84337                                       |при списании для внешнего платежа и для проводки, данные определяются по-разному
 |10.03.2025 |Велигжанин А.В.|DEF-82701                                       |CFT_GetValidName(), функция для получения имени строчки до '//'
 |21.02.2025 |Велигжанин А.В.|DEF-82701                                       |CFT_CheckName(), функция проверки строк
 |11.02.2025 |Велигжанин А.В.|DEF-81831                                       |Поправлена сверка t_check_acc
 |10.02.2025 |Велигжанин А.В.|DEF-81803                                       |Сумма в валюте счета для отчета-сверки по 306-ым
 |24.01.2025 |Велигжанин А.В.|DEF-80372                                       |t_amtcur = 0 для рублевых проводок
 |17.12.2024 |Велигжанин А.В.|DEF-76827                                       |Заполнение t_sofr_receiver_acc, t_sofr_receiver_payname,
 |           |               |                                                |t_check_sum в uentcomparetest_tmp.
 |21.11.2024 |Велигжанин А.В.|BOSS-4204_BOSS-6720                             |Заполнение t_cftcur и t_cftrub в uentcomparetest_tmp.
 |12.11.2024 |Велигжанин А.В.|DEF-76032                                       |CFT_CheckLegal(), для отбора проводок для ФЛ
 |15.10.2024 |Велигжанин А.В.|BOSS-1266_BOSS-5751                             |CFT_CheckCorrespondence(), проверки для проводок по 306-ым
 |11.10.2024 |Велигжанин А.В.|BOSS-1266_BOSS-5723                             |CFT_CreateCompare306(), проводки по 306-ым счетам
 |18.01.2024 |Велигжанин А.В.|DEF-51036                                       |Создан
  */

  x_Filter306 VARCHAR2(256) := RSB_COMMON.GetRegStrValue( 'РСХБ\БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ\ФИЛЬТР_ДЛЯ_СВЕРКИ_ПО_306' );

  /**
   @brief    Функция для журналирования времени выполнения.
  */
  FUNCTION ElapsedTime ( p_time IN pls_integer ) return varchar2 
  IS
  BEGIN
    RETURN to_char((dbms_utility.get_time - p_time) / 100, 'fm9999999990D00');
  END ElapsedTime;

  /**
   @brief    Функция для журналирования времени выполнения.
  */
  PROCEDURE LogDone ( p_Prefix IN varchar2, p_StartTime IN OUT pls_integer, p_Rows IN OUT number )
  IS
  BEGIN
    it_log.log( p_msg => p_Prefix||'Done: '||ElapsedTime(p_StartTime)||', rows: '||to_char(p_Rows), p_msg_type => it_log.c_msg_type__debug );
    p_StartTime := dbms_utility.get_time;
    p_Rows := 0;
  END LogDone;

  /** Возвращает Sql-выражение для получения данных из ЦФТ для сверки
  */
  FUNCTION getSqlCft0
     RETURN CLOB
  IS
  BEGIN
     RETURN q'[
       SELECT --+ INDEX(a ULOADENTCOMPARE_IDX1)
         a.t_autokey, a.t_counter, a.t_userid , a.t_opdate
         , substr(a.T_ACCT_DB, 1, 5) bal_db
         , substr(a.T_ACCT_CR, 1, 5) bal_cr
         , substr(a.T_ACCT_DB, 6, 3) val_db
         , substr(a.T_ACCT_CR, 6, 3) val_cr
         , a.t_docdate, a.t_acct_db, a.t_acct_cr, a.t_currency, a.t_amtcur, a.t_amtrub
         , a.t_details, a.t_docnum, a.t_acctrnid, a.t_idbiscotto, a.t_reqid, a.not_loaded
       FROM
         uloadentforcompare_dbt a 
       WHERE a.t_reqid = :c_ReqID     -- это идентификатор запроса в ЦФТ
         and not (substr(a.T_ACCT_DB, 1, 4) ='3023' and substr(a.T_ACCT_CR, 1, 3)='706')
         and not (substr(a.T_ACCT_CR, 1, 4) ='3023' and substr(a.T_ACCT_DB, 1, 3)='706')
         and not (substr(a.T_ACCT_DB, 1, 3) ='306' and substr(a.T_ACCT_CR, 1, 3)='306')
         and not a.t_details = 'Переоценка' 
         and not a.t_details = 'Курсовая разница' --не будем делать сверку курсовых переоценок
         and not a.t_details = 'Приведение в соответствие остатков на парных лицевых счетах'
     ]';
  END;

  /** Возвращает Sql-выражение для получения данных из ЦФТ для сверки (на основе данных uEntCft_tmp)
  */
  FUNCTION getSqlCft0FromTmp
     RETURN CLOB
  IS
  BEGIN
     RETURN q'[
       SELECT r.t_autokey, r.t_counter, r.t_userid, r.t_opdate
         , r.bal_db, r.bal_cr, r.val_db, r.val_cr
         , r.t_docdate, r.t_acct_db, r.t_acct_cr, r.t_currency, r.t_amtcur, r.t_amtrub
         , r.t_details, r.t_docnum, r.t_acctrnid, r.t_idbiscotto, r.t_reqid, r.not_loaded
       FROM uEntCft_tmp r WHERE r.matched_date = '1' 
     ]';
  END;

  /** Возвращает Sql-выражение для применения фильтра для данных отчета-сверки
  */
  FUNCTION getParamCond(p_UseParam IN NUMBER DEFAULT 0, p_Part IN NUMBER DEFAULT 1)
     RETURN CLOB
  IS
    x_Cond VARCHAR2(1000);
  BEGIN
     IF(p_UseParam = 1) THEN
       -- режим применения параметров доп.фильтра
       -- получаем полное выражение для доп.фильтра
       x_Cond := CFT_GetExtCondition(3, p_Part); 
       IF(x_Cond IS not null) THEN
          RETURN ' AND '||x_Cond;
       END IF;
     END IF;
     RETURN '';
  END getParamCond;

  /** Возвращает Sql-выражение для получения данных из СОФРа для сверки (на основе данных uEntSofr_tmp)
  */
  FUNCTION getSqlSofr0FromTmp
     RETURN CLOB
  IS
  BEGIN
     RETURN q'[
       SELECT r.t_acctrnid
         , r.t_account_payer, r.t_account_receiver, r.t_sync_db, r.t_sync_cr
         , r.payer_form, r.receiver_form, r.future_db, r.future_cr
         , r.bal_db, r.bal_cr, r.val_db, r.val_cr, r.t_idbiscotto
         , r.t_sum_payer, r.t_sum_natcur, r.t_date_carry, r.t_numb_document, r.t_ground
         , r.from_cft, r.from_sofr_pay, r.from_sofr_trn, r.t_currency, r.t_docnum
       FROM uEntSofr_tmp r WHERE r.matched_date = '1'
     ]';
  END;

  /** Возвращает Sql-выражение для получения данных из СОФРа для сверки
  */
  FUNCTION getSqlSofr00
     RETURN CLOB
  IS
  BEGIN
     RETURN q'[
       SELECT --+ INDEX (B DACCTRN_DBT_IDX6)
         b.t_acctrnid
         , b.t_account_payer, b.t_fiid_payer
         , b.t_account_receiver, b.t_fiid_receiver
         , cft_utils.CFT_ClientLegalForm(b.t_accountid_payer) payer_form
         , cft_utils.CFT_ClientLegalForm(b.t_accountid_receiver) receiver_form 
         , nvl(p.t_futurepayeraccount, '') future_db, nvl(p.t_futurereceiveraccount, '') future_cr
         , substr(b.t_account_payer, 1, 5)  bal_db, substr(b.t_account_receiver, 1, 5) bal_cr
         , substr(b.t_account_payer, 6, 3)  val_db, substr(b.t_account_receiver, 6, 3) val_cr
         -- попытаемся найти соответствующий id цФТ, он может быть в разных местах
         , coalesce ( substr(b.t_userfield4, 1, instr( b.t_userfield4,'#')-1), substr(p.t_userfield4, 1, instr( p.t_userfield4,'#')-1), ub.t_biscottoid ) t_idbiscotto
         , b.t_sum_payer, b.t_sum_natcur, b.t_date_carry , b.t_numb_document, b.t_ground
         , nvl2(ub.t_pmid, 1, 0) from_cft
         , sign(nvl(instr( p.t_userfield4, '#'), 0)) from_sofr_pay
         , sign(nvl(instr( b.t_userfield4, '#'), 0)) from_sofr_trn
         , ( SELECT F.T_CODEINACCOUNT FROM dfininstr_dbt F WHERE F.T_FIID = B.T_FIID_PAYER ) AS T_CURRENCY
         , b.T_NUMB_DOCUMENT AS T_DOCNUM
       FROM 
         dacctrn_dbt B 
         left join  dpmdocs_dbt D on d.t_acctrnid = b.t_acctrnid 
         left join  DPMPAYM_DBT p on p.t_paymentid = d.t_paymentid 
         left join  usr_acc306enroll_trn_dbt ep on  ep.t_acctrnid = b.t_acctrnid 
         left join upmbiscotto_dbt ub on (ub.t_pmid = ep.t_enrollid and ub.t_dockind = 5002)
       WHERE B.T_DATE_CARRY BETWEEN :c_StartDate AND :c_EndDate
           and not b.t_Result_Carry in ( 18, 82, 83, 84, 46) -- если будет делаться сверка проводок по курсовым разницам, нужно будет исправить это условие
           and b.t_state = 1
           and b.t_chapter in (1, 3, 4) 
     ]'
     ;
  END;

  /** Возвращает Sql-выражение для получения данных из СОФРа для сверки
  */
  FUNCTION getSqlSofr0(p_UseParam IN NUMBER DEFAULT 0, p_Part IN NUMBER DEFAULT 1)
     RETURN CLOB
  IS
  BEGIN
     RETURN getSqlSofr00()
       ||q'[ and not (substr(b.t_account_payer, 1, 3) ='306' and substr(b.t_account_receiver, 1, 3) ='306' )]'
       ||getParamCond(p_UseParam, p_Part)
     ;
  END;

  /** Возвращает Sql-выражение для курсора для табличной функции сравнения проводок СОФР и ЦФТ,
      у которых полностью совпадают номера счетов
  */
  FUNCTION getSqlSofrCftEnts
     RETURN CLOB
  IS
     x_sql CLOB;
  BEGIN
     x_sql := '
      --получим массив данных из ЦФТ для сверки
      WITH cft0 AS ('
         ||getSqlCft0()
      ||'),

      --получим массив данных из СОФРа для сверки
      sofr0 as ('
         ||getSqlSofr0() 
      ||'),

      -- сопоставим по id ЦФТ и одновременно выполним сравнение реквизитов
      matched1 as (
         select sofr.t_acctrnid , cft.t_autokey
           , cft_utils.CFT_MatchAccount(
               cft.t_acct_db, cft.bal_db, cft.val_db
               , sofr.t_account_payer, sofr.future_db, sofr.bal_db, sofr.val_db, sofr.payer_form
             ) AS matched_debet
           , cft_utils.CFT_MatchAccount(
               cft.t_acct_cr, cft.bal_cr, cft.val_cr
               , sofr.t_account_receiver, sofr.future_cr, sofr.bal_cr, sofr.val_cr, sofr.receiver_form
             ) AS matched_credit
           , cft_utils.CFT_MatchSum( cft.t_amtrub, sofr.t_sum_natcur ) AS matched_sum
           , 1 AS matched_details
           , cft_utils.CFT_MatchDate( cft.t_opdate, cft.t_docdate, sofr.t_date_carry ) AS matched_date
           , sofr.t_acctrnid AS SofrID
           , sofr.t_idbiscotto AS CftID
           , sofr.t_date_carry AS SofrDate
           , sofr.t_account_payer AS db
           , sofr.t_account_receiver AS cr
         from sofr0 sofr 
         inner join cft0 cft on sofr.t_idbiscotto = cft.t_idbiscotto    
      )
      select m1.SofrID, m1.CftID, m1.SofrDate, m1.db, m1.cr
         from matched1 m1 where matched_debet = 1 AND matched_credit = 1
      '
     ;

     return x_sql;
  END;

  /** Возвращает Sql-выражение для курсора для табличной функции сравнения проводок СОФР и ЦФТ,
      у которых совпадают балансовые счета и валюты, 
      а также балансовые счета соответствуют заданным маскам
  */
  FUNCTION getSqlSofrCftEntsByMask 
     RETURN CLOB
  IS
     x_sql CLOB;
  BEGIN
     x_sql := '
      --получим массив данных из ЦФТ для сверки
      WITH cft0 AS ('
         ||getSqlCft0()
      ||'),

      --получим массив данных из СОФРа для сверки
      sofr0 as ('
         ||getSqlSofr0() 
      ||'),

      -- сопоставим по id ЦФТ и одновременно выполним сравнение реквизитов
      matched1 as (
         select sofr.t_acctrnid , cft.t_autokey
           , case when cft.bal_db = sofr.bal_db then 1 else 0 end AS BalDb_matched
           , case when cft.bal_cr = sofr.bal_cr then 1 else 0 end AS BalCr_matched
           , case when cft.val_db = sofr.val_db then 1 else 0 end AS ValDb_matched
           , case when cft.val_cr = sofr.val_cr then 1 else 0 end AS ValCr_matched
           , sofr.t_acctrnid AS SofrID
           , sofr.t_idbiscotto AS CftID
           , sofr.t_date_carry AS SofrDate
           , sofr.t_account_payer AS SofrDb
           , sofr.t_account_receiver AS SofrCr
           , cft.t_acct_db AS CftDb
           , cft.t_acct_cr AS CftCr
         from sofr0 sofr 
         inner join cft0 cft on sofr.t_idbiscotto = cft.t_idbiscotto    
      )
      select m1.SofrID, m1.CftID, m1.SofrDate, m1.SofrDb, m1.SofrCr, m1.CftDb, m1.CftCr
         from matched1 m1 
         where BalDb_matched = 1 AND BalCr_matched = 1 and ValDb_matched = 1 and ValCr_matched = 1
         and regexp_like(m1.SofrDb, :c_MaskDb)
         and regexp_like(m1.SofrCr, :c_MaskCr)
      '
     ;

     return x_sql;
  END;

  /** Возвращает Sql-выражение для получения данных СОФР за дату
  */
  FUNCTION getSqlSofrOnDate( p_UseParam IN NUMBER DEFAULT 0, p_Part IN NUMBER DEFAULT 1 )
     RETURN CLOB
  IS
     x_sql CLOB;
  BEGIN
     x_sql := '
      INSERT INTO uEntSofr_tmp r (
         r.t_acctrnid, r.t_account_payer, r.t_fiid_payer, r.t_account_receiver, r.t_fiid_receiver
         , r.payer_form, r.receiver_form, r.future_db, r.future_cr, r.bal_db, r.bal_cr, r.val_db, r.val_cr
         , r.t_idbiscotto, r.t_sum_payer, r.t_sum_natcur, r.t_date_carry, r.t_numb_document, r.t_ground
         , r.from_cft, r.from_sofr_pay, r.from_sofr_trn, r.t_currency, r.t_docnum
      )

      --получим массив данных из СОФРа для сверки
      WITH sofr0 AS ('
         ||getSqlSofr0( p_UseParam, p_Part ) 
      ||q'[)

       SELECT
         r.t_acctrnid, r.t_account_payer, r.t_fiid_payer, r.t_account_receiver, r.t_fiid_receiver
         , r.payer_form, r.receiver_form, r.future_db, r.future_cr, r.bal_db, r.bal_cr, r.val_db, r.val_cr
         , r.t_idbiscotto, r.t_sum_payer, r.t_sum_natcur, r.t_date_carry, r.t_numb_document, r.t_ground
         , r.from_cft, r.from_sofr_pay, r.from_sofr_trn, r.t_currency, r.t_docnum
         FROM sofr0 r 
      ]'
     ;

     return x_sql;

  END getSqlSofrOnDate;


  /** Возвращает Sql-выражение для получения списаний с 306-го СОФР за дату
  */
  FUNCTION getSql306WriteOffOnDate
     RETURN CLOB
  IS
     x_sql CLOB;
  BEGIN
     x_sql := '
      INSERT INTO uEntSofr_tmp r (
         r.t_acctrnid, r.t_account_payer, r.t_fiid_payer, r.t_account_receiver, r.t_fiid_receiver
         , r.payer_form, r.receiver_form, r.future_db, r.future_cr, r.bal_db, r.bal_cr, r.val_db, r.val_cr
         , r.t_idbiscotto, r.t_sum_payer, r.t_sum_natcur, r.t_date_carry, r.t_numb_document, r.t_ground
         , r.from_cft, r.from_sofr_pay, r.from_sofr_trn, r.t_currency, r.t_docnum
      )

      --получим массив данных из СОФРа для сверки
      WITH sofr0 AS ('
         ||getSqlSofr00() 
      ||q'[
         AND ((RSB_MASK.CompareStringWithMask( '306*', B.T_ACCOUNT_PAYER ) > 0) 
         AND (RSB_MASK.CompareStringWithMask( :x_Filter306, B.T_ACCOUNT_RECEIVER ) = 0))
      )

       SELECT
         r.t_acctrnid, r.t_account_payer, r.t_fiid_payer, r.t_account_receiver, r.t_fiid_receiver
         , r.payer_form, r.receiver_form, r.future_db, r.future_cr, r.bal_db, r.bal_cr, r.val_db, r.val_cr
         , r.t_idbiscotto, r.t_sum_payer, r.t_sum_natcur, r.t_date_carry, r.t_numb_document, r.t_ground
         , r.from_cft, r.from_sofr_pay, r.from_sofr_trn, r.t_currency, r.t_docnum
         FROM sofr0 r 
         WHERE r.t_acctrnid NOT IN (SELECT t_acctrnid FROM uEntSofr_tmp)
      ]'
     ;

     return x_sql;

  END getSql306WriteOffOnDate;


  /** Возвращает Sql-выражение для получения зачислений на 306-ой СОФР за дату
  */
  FUNCTION getSql306EnrollOnDate
     RETURN CLOB
  IS
     x_sql CLOB;
  BEGIN
     x_sql := q'[
      INSERT INTO uEntSofr_tmp r (
         r.t_acctrnid, r.t_account_payer, r.t_fiid_payer, r.t_account_receiver, r.t_fiid_receiver
         , r.payer_form, r.receiver_form, r.future_db, r.future_cr, r.bal_db, r.bal_cr, r.val_db, r.val_cr
         , r.t_idbiscotto, r.t_sum_payer, r.t_sum_natcur, r.t_date_carry, r.t_numb_document, r.t_ground
         , r.from_cft, r.from_sofr_pay, r.from_sofr_trn, r.t_currency, r.t_docnum
      )
      SELECT
        sofr.T_ACCTRNID
          , sofr.T_ACCTSEND AS t_account_payer, sofr.t_fiid_payer, sofr.T_ACCOUNT_RECEIVER, sofr.t_fiid_receiver
          , sofr.payer_form, sofr.receiver_form, sofr.future_db, sofr.future_cr, sofr.bal_db, sofr.bal_cr, sofr.val_db, sofr.val_cr
          , sofr.t_idbiscotto, sofr.t_sum_payer, sofr.t_sum_natcur, sofr.t_date_carry, sofr.t_numb_document, sofr.t_ground
          , sofr.from_cft, sofr.from_sofr_pay, sofr.from_sofr_trn, sofr.t_currency, sofr.t_docnum
          from (
        SELECT 
          T.T_DEBETACCOUNT, T.T_CREDITACCOUNT, o.t_code
          , TRN.T_ACCTRNID, TRN.T_ACCOUNT_PAYER, TRN.T_ACCOUNT_RECEIVER
          , cft_utils.CFT_ClientLegalForm(T.T_ACCTSEND) payer_form
          , cft_utils.CFT_ClientLegalForm(trn.t_accountid_receiver) receiver_form
          , nvl(p.t_futurepayeraccount, '') future_db, nvl(p.t_futurereceiveraccount, '') future_cr
          , substr(trn.t_account_payer, 1, 5)  bal_db, substr(trn.t_account_receiver, 1, 5) bal_cr
          , substr(trn.t_account_payer, 6, 3)  val_db, substr(trn.t_account_receiver, 6, 3) val_cr
          , trn.t_sum_payer
          , trn.t_sum_natcur
          , trn.t_date_carry, trn.t_numb_document, trn.t_ground
          , trn.t_fiid_payer, trn.t_fiid_receiver
          , coalesce ( 
             substr(trn.t_userfield4, 1, instr( trn.t_userfield4,'#')-1)
             , substr(p.t_userfield4, 1, instr( p.t_userfield4,'#')-1)
             , ub.t_biscottoid 
            ) t_idbiscotto
          , TRN2.T_ACCOUNT_PAYER AS TRN2_ACCOUNT_PAYER, TRN2.T_ACCOUNT_RECEIVER AS TRN2_ACCOUNT_RECEIVER, T.T_ACCTSEND
          , nvl2(ub.t_pmid, 1, 0) from_cft
          , sign(nvl(instr( p.t_userfield4, '#'), 0)) from_sofr_pay
          , sign(nvl(instr( trn.t_userfield4, '#'), 0)) from_sofr_trn
          , ( SELECT F.T_CODEINACCOUNT FROM dfininstr_dbt F WHERE F.T_FIID = trn.T_FIID_PAYER ) AS T_CURRENCY
          , trn.T_NUMB_DOCUMENT AS T_DOCNUM
          FROM USR_ACC306ENROLL_DBT t 
          join USR_ACC306ENROLL_TRN_DBT tt on (TT.T_ENROLLID = T.T_ENROLLID)
          join DACCTRN_DBT trn2 on (TRN2.T_ACCTRNID = TT.T_ACCTRNID)
          join dnptxop_dbt o on (o.t_id = T.T_NPTXOPID)
          join doproper_dbt op on (OP.T_DOCUMENTID = o.t_id and OP.T_DOCKIND = O.T_DOCKIND)
          join doprdocs_dbt d on (D.T_ID_OPERATION = OP.T_ID_OPERATION)
          join DACCTRN_DBT trn on (TRN.T_ACCTRNID = D.T_ACCTRNID)
          left join  dpmdocs_dbt D on (d.t_acctrnid = trn2.t_acctrnid)
          left join  DPMPAYM_DBT p on (p.t_paymentid = d.t_paymentid)
          left join  usr_acc306enroll_trn_dbt ep on  (ep.t_acctrnid = trn2.t_acctrnid)
          left join  upmbiscotto_dbt ub on (ub.t_pmid = ep.t_enrollid and ub.t_dockind = 5002)  
          WHERE 
            trn.t_date_carry BETWEEN :c_StartDate AND :c_EndDate
            AND (substr(trn.t_account_receiver, 1, 3) = '306')
            AND (RSB_MASK.CompareStringWithMask( :x_Filter306, T.T_ACCTSEND ) = 0)
            AND trn.t_state = 1
            AND trn.t_chapter in (1, 3, 4)
          ) sofr
      ]'
     ;

     return x_sql;

  END getSql306EnrollOnDate;


  /** Возвращает Sql-выражение для получения данных ЦФТ для соответствующей пачки
  */
  FUNCTION getSqlCftOnReqID
     RETURN CLOB
  IS
     x_sql CLOB;
  BEGIN
     x_sql := 'INSERT INTO uEntCft_tmp r (
         r.t_autokey, r.t_counter, r.t_userid, r.t_opdate
         , r.bal_db, r.bal_cr, r.val_db, r.val_cr
         , r.t_docdate, r.t_acct_db, r.t_acct_cr, r.t_currency, r.t_amtcur, r.t_amtrub
         , r.t_details, r.t_docnum, r.t_acctrnid, r.t_idbiscotto, r.t_reqid, r.not_loaded
      )

      --получим массив данных из ЦФТ для сверки
      WITH cft0 AS ('
         ||getSqlCft0() 
      ||q'[)

       SELECT
         r.t_autokey, r.t_counter, r.t_userid, r.t_opdate
         , r.bal_db, r.bal_cr, r.val_db, r.val_cr
         , r.t_docdate, r.t_acct_db, r.t_acct_cr, r.t_currency, r.t_amtcur, r.t_amtrub
         , r.t_details, r.t_docnum, r.t_acctrnid, r.t_idbiscotto, r.t_reqid, r.not_loaded
         FROM cft0 r 
      ]'
     ;

     return x_sql;

  END getSqlCftOnReqID;

  /** Возвращает Sql-выражение для получения данных ЦФТ по 306-ым счетам
        DEF-95385 В запросе производится отсекание проводок по ЮЛ 
                  (они попадают в upmbiscotto_dbt с t_dockind == '4607')
  */
  FUNCTION getSqlCft306OnReqID
     RETURN CLOB
  IS
  BEGIN
     return q'[
       INSERT INTO uEntCft_tmp r (
         r.t_autokey, r.t_counter, r.t_userid, r.t_opdate
         , r.bal_db, r.bal_cr, r.val_db, r.val_cr
         , r.t_docdate, r.t_acct_db, r.t_acct_cr, r.t_currency, r.t_amtcur, r.t_amtrub
         , r.t_details, r.t_docnum, r.t_acctrnid, r.t_idbiscotto, r.t_reqid, r.not_loaded
         , r.t_payername, r.t_receivername, r.t_payeraccount, r.t_receiveraccount 
       )
       SELECT 
         a.t_autokey, a.t_counter, a.t_userid , a.t_opdate
         , substr(a.T_ACCT_DB, 1, 5) bal_db, substr(a.T_ACCT_CR, 1, 5) bal_cr
         , substr(a.T_ACCT_DB, 6, 3) val_db, substr(a.T_ACCT_CR, 6, 3) val_cr
         , a.t_docdate, a.t_acct_db, a.t_acct_cr, a.t_currency, a.t_amtcur, a.t_amtrub
         , a.t_details, a.t_docnum, a.t_acctrnid, a.t_idbiscotto, a.t_reqid, chr(1) AS not_loaded
         , a.t_payername, a.t_receivername, a.t_payeraccount, a.t_receiveraccount 
       FROM
         uload306accforcompare_dbt a 
         LEFT JOIN upmbiscotto_dbt ub ON (UB.T_BISCOTTOID = A.T_IDBISCOTTO)
       WHERE 
         a.t_reqid = :c_ReqID     -- это идентификатор запроса в ЦФТ
         AND nvl(ub.t_dockind, '5002') != '4607' 
     ]';
  END getSqlCft306OnReqID;

  /** Возвращает Sql-выражение для заполнения таблицы сравнения
  */
  FUNCTION getSqlCreateCompare
     RETURN CLOB
  IS
     x_sql CLOB;
  BEGIN
     x_sql := '
      INSERT INTO UENTCOMPARETEST_TMP r (
        r.t_acctrnid, r.t_acct_db, r.t_acct_cr, r.t_amtcur, r.t_amtrub
        , r.t_details, r.t_opdate, r.t_idbiscotto, r.t_err
        , r.t_currency, r.t_docnum, r.t_sync_db, r.t_sync_cr
        , r.t_cftcur, r.t_cftrub 						-- BOSS-4204_BOSS-6720
      )
      --получим массив данных из ЦФТ для сверки
      WITH cft0 AS ('
         ||getSqlCft0FromTmp()
      ||'),

      --получим массив данных из СОФРа для сверки
      sofr0 as ('
         ||getSqlSofr0FromTmp() 
      ||q'[),

      -- сопоставим по id ЦФТ и одновременно выполним сравнение реквизитов
      matched1 as (
         select 
           sofr.t_acctrnid , cft.t_autokey
           , cft_utils.CFT_MatchAccount(
               cft.t_acct_db, cft.bal_db, cft.val_db
               , sofr.t_account_payer, coalesce(sofr.t_sync_db, sofr.future_db), sofr.bal_db, sofr.val_db, sofr.payer_form
             ) AS matched_debet
           , cft_utils.CFT_MatchAccount(
               cft.t_acct_cr, cft.bal_cr, cft.val_cr
               , sofr.t_account_receiver, coalesce( sofr.t_sync_cr, sofr.future_cr), sofr.bal_cr, sofr.val_cr, sofr.receiver_form
             ) AS matched_credit
           , cft_utils.CFT_MatchSum( cft.t_amtrub, sofr.t_sum_natcur ) AS matched_sum
           , 1 AS matched_details
           , cft_utils.CFT_MatchDate( cft.t_opdate, cft.t_docdate, sofr.t_date_carry ) AS matched_date
           , cft.t_amtrub AS t_cftrub, cft.t_amtcur AS t_cftcur
         from sofr0 sofr 
         inner join cft0 cft on sofr.t_idbiscotto = cft.t_idbiscotto    
      ), 

      -- выявим оставшиеся после сопоставления проводки из ЦФТ    
      cft1 as (
         select cft.t_autokey, cft.t_counter , cft.t_userid , cft.t_opdate,cft.bal_db, cft.bal_cr,cft.val_db, cft.val_cr,
           cft.t_docdate, cft.t_acct_db, cft.t_acct_cr, cft.t_currency, cft.t_amtcur, cft.t_amtrub, cft.t_details, cft.t_docnum, 
           cft.t_acctrnid, cft.t_idbiscotto, cft.t_reqid, cft.not_loaded
         from cft0 cft where cft.t_autokey not in (select t_autokey from  matched1)
      ),  

      -- выявим оставшиеся после сопоставления проводки из СОФР 
      sofr1 as (
         select sofr.t_acctrnid , sofr.t_account_payer, sofr.t_account_receiver,  sofr.payer_form, sofr.receiver_form
           , sofr.bal_db, sofr.bal_cr, sofr.val_db, sofr.val_cr, sofr.t_idbiscotto, sofr.t_sum_payer
           , sofr.t_sum_natcur, sofr.t_date_carry , sofr.future_cr, sofr.future_db, sofr.t_ground
           , sofr.t_currency, sofr.t_docnum, sofr.t_sync_db, sofr.t_sync_cr
         from sofr0 sofr
         where sofr.t_acctrnid  not in (select t_acctrnid from  matched1) 
           and sofr.T_DATE_CARRY = :x_EndDate
      ),

      -- теперь сопоставим по реквизитам те проводки, которые не удалось сопоставить по id
      matched2 as (
         select 
           sofr.t_acctrnid, cft.t_autokey, cft.t_idbiscotto, 1 AS matched_details
           , cft.t_amtrub AS t_cftrub, cft.t_amtcur AS t_cftcur
         from sofr1 sofr inner join cft1 cft on (
             cft_utils.CFT_MatchAccount(
               cft.t_acct_db, cft.bal_db, cft.val_db
               , sofr.t_account_payer, coalesce(sofr.t_sync_db, sofr.future_db), sofr.bal_db, sofr.val_db, sofr.payer_form
             ) = 1
             and cft_utils.CFT_MatchAccount(
               cft.t_acct_cr, cft.bal_cr, cft.val_cr
               , sofr.t_account_receiver, coalesce( sofr.t_sync_cr, sofr.future_cr), sofr.bal_cr, sofr.val_cr, sofr.receiver_form
             ) = 1
             and cft_utils.CFT_MatchSum( cft.t_amtrub, sofr.t_sum_natcur ) = 1
             and cft_utils.CFT_MatchDate( cft.t_opdate, cft.t_docdate, sofr.t_date_carry ) = 1
         )
      ),

      -- теперь выведем в другую таблицу проводки, которые есть в одной системе и нет в другой
      all_diff_prov as (
        select 
          cft.t_autokey autokey, null AS acctrnid, cft.t_acct_db acct_db, cft.t_acct_cr acct_cr
          , 0 sumcur, 0 sumrub, cft.t_details descr, cft.t_opdate prov_date
          , cft.t_idbiscotto idbiscotto, 2 err_type, 'Нет в СОФР, есть в ЦФТ' err_desc
          , cft.t_currency, cft.t_docnum, '' AS t_sync_db, '' AS t_sync_cr
          , cft.t_amtcur t_cftcur, cft.t_amtrub t_cftrub
        from cft1 cft 
        where cft.t_autokey not in (select t_autokey from matched1 union all select t_autokey from matched2)

        union all

        select 
          0 autokey, sofr.t_acctrnid acctrnid, sofr.t_account_payer acct_db, sofr.t_account_receiver acct_cr
          , sofr.t_sum_payer sumcur, sofr.t_sum_natcur sumrub, sofr.t_ground descr, sofr.t_date_carry prov_date
          , '' AS idbiscotto, 3 err_type, 'Есть в СОФР, нет в ЦФТ' err_desc
          , sofr.t_currency, sofr.t_docnum, sofr.t_sync_db, sofr.t_sync_cr
          , 0 t_cftcur, 0 t_cftrub
        from sofr1 sofr 
        where sofr.t_acctrnid not in (select t_acctrnid from matched1 union all select t_acctrnid from matched2)

        union all

        select 
          0 autokey, sofr.t_acctrnid acctrnid, sofr.t_account_payer acct_db, sofr.t_account_receiver acct_cr
          , sofr.t_sum_payer sumcur, sofr.t_sum_natcur sumrub, sofr.t_ground descr, sofr.t_date_carry prov_date
          , sofr.t_idbiscotto
          , case when m.matched_debet = 1 and m.matched_credit = 1 and m.matched_sum = 1 and m.matched_date = 1 
                 then -1
            when m.matched_sum = 0 then 4
            when m.matched_debet = 0 then 5
            when m.matched_credit = 0 then 6
            when m.matched_date = 0 then 7
            end AS err_type
          , case when m.matched_debet = 1 and m.matched_credit = 1 and m.matched_sum = 1 and m.matched_date = 1 
                 then 'matched1'
            when m.matched_sum = 0 then 'Сумма проводки не совпадает'
            when m.matched_debet = 0 then 'Счет по дебету не совпадает' 
            when m.matched_credit = 0 then 'Счет по кредиту не совпадает'
            when m.matched_date = 0 then 'Даты не совпадают'
            end AS err_desc
          , sofr.t_currency, sofr.t_docnum, sofr.t_sync_db, sofr.t_sync_cr
          , m.t_cftcur, m.t_cftrub
        from sofr0 sofr, matched1 m
        where sofr.t_acctrnid = m.t_acctrnid

        union all

        select 
          0 autokey, sofr.t_acctrnid acctrnid, sofr.t_account_payer acct_db, sofr.t_account_receiver acct_cr
          , sofr.t_sum_payer sumcur, sofr.t_sum_natcur sumrub, sofr.t_ground descr, sofr.t_date_carry prov_date
          , m.t_idbiscotto, -2 AS err_type, 'matched2' AS err_desc
          , sofr.t_currency, sofr.t_docnum, sofr.t_sync_db, sofr.t_sync_cr
          , m.t_cftcur, m.t_cftrub
        from sofr1 sofr, matched2 m
        where sofr.t_acctrnid = m.t_acctrnid
      )
       select r.acctrnid, r.acct_db, r.acct_cr
         , case when r.t_currency = '810' then 0 else r.sumcur end AS sumcur			-- DEF-80372
         , r.sumrub, r.descr, r.prov_date
         , r.idbiscotto, r.err_type, r.t_currency, r.t_docnum, r.t_sync_db, r.t_sync_cr
         , r.t_cftcur, r.t_cftrub 								-- BOSS-4204_BOSS-6720
         from all_diff_prov r 
      ]'
     ;

     return x_sql;
  END;

  /** Возвращает наименование клиента счета
  */
  FUNCTION CFT_GetAccountPartyName(p_Account IN VARCHAR2)
     RETURN dparty_dbt.t_name%type DETERMINISTIC
  IS
     x_Name dparty_dbt.t_name%type;
  BEGIN
     IF(p_Account IS NOT NULL) THEN
       SELECT p.t_name INTO x_Name FROM dparty_dbt p, daccount_dbt a 
         WHERE a.t_account = p_Account AND p.t_partyid = a.t_client AND ROWNUM = 1
     ; 
     END IF;
     RETURN x_Name;
  END;

  /** BOSS-1266_BOSS-5751
      Функция проверки соответствия валюты
      Если полученные валюты равны, то 1, иначе 0
  */
  FUNCTION CFT_CheckCurrency(p_CurA IN VARCHAR2, p_CurB IN VARCHAR2)
     RETURN number DETERMINISTIC
  IS
  BEGIN
     IF(p_CurA = p_CurB) THEN
       RETURN 1;
     END IF;
     RETURN 0;
  END;

  /** BOSS-1266_BOSS-5751
      Функция проверки корректности корреспонденции
      Если корреспонденция счетов идет в соответствии с резиденством (Д 30601 К 40817, Д 30606 К 40820 и наоборот), тогда  "успешно".
  */
  FUNCTION CFT_CheckCorrespondence(p_Debit IN VARCHAR2, p_Credit IN VARCHAR2)
     RETURN number
  IS
    x_Debit VARCHAR2(5) := substr(p_Debit, 1, 5);
    x_Credit VARCHAR2(5) := substr(p_Credit, 1, 5);
  BEGIN
     IF( (x_Debit NOT IN ('30601','40817','30606','40820')) AND (x_Credit NOT IN ('30601','40817','30606','40820')) ) THEN
       -- это не те счета, которые необходимо проверять
       RETURN 1;
     ELSIF(x_Debit = '30601' AND x_Credit = '40817') THEN
       RETURN 1;
     ELSIF(x_Debit = '30606' AND x_Credit = '40820') THEN
       RETURN 1;
     ELSIF(x_Debit = '40817' AND x_Credit = '30601') THEN
       RETURN 1;
     ELSIF(x_Debit = '40820' AND x_Credit = '30606') THEN
       RETURN 1;
     END IF;
     RETURN 0;
  END;

  /** Функция для извлечения токена
  */
  FUNCTION getToken ( p_Str IN OUT varchar2, p_Delim IN varchar2 ) RETURN varchar2 
  IS
    x_Pos number;
    x_Token varchar2(200);
  BEGIN
    x_Pos := instr(p_Str, p_Delim);
    if(x_Pos = 0) then
      x_Token := p_Str;
      p_Str := '';
    else 
      x_Token := substr(p_Str, 1, x_Pos-1);
      p_Str := substr(p_Str, x_Pos+1);
    end if;
    RETURN x_Token;
  END getToken;


  /** DEF-82701
      Возвращает валидное имя строки (первую часть до '//')
  */
  FUNCTION CFT_GetValidName(p_Name IN VARCHAR2)
     RETURN varchar2
  IS
    x_ValidName varchar2(200) := p_Name;
  BEGIN
    RETURN trim(getToken(x_ValidName, '//'));
  END CFT_GetValidName;


  /** DEF-82701
      Возвращает 1, если наименования совпадают.
  */
  FUNCTION CFT_CheckName(p_CftName IN VARCHAR2, p_SofrName IN VARCHAR2)
     RETURN number
  IS
     x_CftName varchar2(200) := p_CftName;
     x_SofrName varchar2(200) := p_SofrName;
  BEGIN
     IF(CFT_GetValidName(x_CftName) = CFT_GetValidName(x_SofrName)) THEN
       RETURN 1;
     END IF;
     RETURN 0;
  END CFT_CheckName;


  /** DEF-76032
      Возвращает 2, если полученный счет является счетом ФЛ.
  */
  FUNCTION CFT_CheckLegal(p_Account IN VARCHAR2)
     RETURN number
  IS
    x_LegalForm dparty_dbt.t_legalform%type;
    x_IsEmployer dpersn_dbt.t_isemployer%type;
    x_AttrID dobjatcor_dbt.t_attrid%type;
  BEGIN
     IF( p_Account = 'xxx' ) THEN
       RETURN 2;
     END IF;

     SELECT p.t_legalform, n.t_isemployer, ac.t_attrid 
       INTO x_LegalForm, x_IsEmployer, x_AttrID
       FROM daccount_dbt a 
       LEFT JOIN dparty_dbt p ON (p.t_partyid = a.t_client)
       LEFT JOIN dpersn_dbt n ON (n.t_personid = a.t_client)
       LEFT JOIN dobjatcor_dbt ac ON (ac.t_ObjectType = 3 AND ac.t_GroupID = 53 AND ac.t_Object = lpad(p.t_partyid,10,'0') )
       WHERE a.t_account = p_Account AND ROWNUM = 1
     ; 

     -- Для ФЛ доп. проверка. Из ТЗ
     -- если dpersn_dbt.t_isemployer или  код ОКОПФ  (категория 51) начинается с '5 01', то 'ИП' (attrID = 109, 110, 111)
     -- если код ОКОПФ  (категория 51) начинается с '5 02', то 'ФЛЧП' (attrID = 112, 113, 114)
     IF( x_LegalForm = 2 ) THEN
       IF( (x_IsEmployer = 'X') OR x_AttrID IN (109, 110, 111, 112, 113, 114)) THEN
         x_LegalForm := 0; 
       END IF;
     END IF;

     RETURN x_LegalForm;

  EXCEPTION WHEN others THEN
     RETURN 2;
  END;

  /** DEF-76032
      Возвращает 2, если полученный счет является счетом ФЛ.
  */
  FUNCTION CFT_CheckLegal2(p_Account IN VARCHAR2, p_Account2 IN VARCHAR2)
     RETURN number
  IS
    x_Ret NUMBER;
  BEGIN
    IF((SUBSTR(p_Account, 1, 5) IN ('40701', '40702')) OR (SUBSTR(p_Account2, 1, 5) IN ('40701', '40702'))) THEN
      -- DEF-87040 Проверка не проходит
--      it_log.log( p_msg => 'p_Account: '||p_Account||', p_Account2: '||p_Account2, p_msg_type => it_log.c_msg_type__debug );
      RETURN 0;
    END IF;
    x_Ret := CFT_CheckLegal(p_Account);
    IF(x_Ret <> 2 AND p_Account2 IS NOT NULL) THEN
      x_Ret := CFT_CheckLegal(p_Account2);
    END IF;
--    IF(x_Ret <> 2) THEN
--      it_log.log( p_msg => 'p_Account: '||p_Account||', p_Account2: '||p_Account2, p_msg_type => it_log.c_msg_type__debug );
--    END IF;
    RETURN x_Ret;
  END;


  /** Возвращает Sql-выражение для заполнения таблицы сравнения по 306-ым счетам
  */
  FUNCTION getSqlCreateCompare306
     RETURN CLOB
  IS
     x_sql CLOB;
  BEGIN
     x_sql := q'[
      INSERT INTO UENT306COMPARE_TMP r (
         r.t_cft_date_carry, r.t_cft_doc_number, r.t_cft_sum, r.t_cft_dbt_acc, r.t_cft_dbt_cur
         , r.t_cft_crd_acc, r.t_cft_crd_cur, r.t_cft_payer_acc, r.t_cft_receiver_acc
         , r.t_cft_payer_name, r.t_cft_receiver_name, r.t_cft_pay_purpose
         , r.t_sofr_date_carry, r.t_sofr_doc_number, r.t_sofr_sum, r.t_sofr_dbt_acc, r.t_sofr_dbt_cur
         , r.t_sofr_crd_acc, r.t_sofr_crd_cur, r.t_sofr_payer_name, r.t_sofr_receiver_name, r.t_sofr_ground
         , r.t_check_pay, r.t_check_cur, r.t_check_name, r.t_check_acc, r.t_check_all
         -- DEF-76827 заполнение t_sofr_receiver_acc, t_sofr_receiver_payname, t_check_sum
         , r.t_sofr_receiver_acc, r.t_sofr_receiver_payname, r.t_check_sum
      )
       SELECT 
         c.t_cft_date_carry, c.t_cft_doc_number, c.t_cft_sum, c.t_cft_dbt_acc, c.t_cft_dbt_cur
         , c.t_cft_crd_acc, c.t_cft_crd_cur, c.t_cft_payer_acc, c.t_cft_receiver_acc
         , c.t_cft_payer_name, c.t_cft_receiver_name, c.t_cft_pay_purpose
         , c.t_sofr_date_carry, c.t_sofr_doc_number, c.t_sofr_sum, c.t_sofr_dbt_acc, c.t_sofr_dbt_cur
         , c.t_sofr_crd_acc, c.t_sofr_crd_cur, c.t_sofr_payer_name, c.t_sofr_receiver_name, c.t_sofr_ground
         , c.t_check_pay, c.t_check_cur, c.t_check_name, c.t_check_acc
         , case when c.t_check_pay = 1 
           AND c.t_check_cur = 1 
           AND c.t_check_name = 1 
           AND c.t_check_sum = 1 
           AND c.t_check_acc = 1 then 1 else 0 end 
           AS t_check_all                                  -- Итог сверки
         -- DEF-76827 заполнение t_sofr_receiver_acc, t_sofr_receiver_payname, t_check_sum
         , c.t_sofr_receiver_acc, c.t_sofr_receiver_payname, c.t_check_sum
       FROM (
       SELECT 
         b.t_cft_date_carry, b.t_cft_doc_number, b.t_cft_sum, b.t_cft_dbt_acc, b.t_cft_dbt_cur
         , b.t_cft_crd_acc, b.t_cft_crd_cur, b.t_cft_payer_acc, b.t_cft_receiver_acc
         , cft_utils.CFT_GetValidName(b.t_cft_payer_name) AS t_cft_payer_name
         , cft_utils.CFT_GetValidName(b.t_cft_receiver_name) AS t_cft_receiver_name
         , b.t_cft_pay_purpose
         , b.t_sofr_date_carry, b.t_sofr_doc_number, b.t_sofr_sum, b.t_sofr_dbt_acc, b.t_sofr_dbt_cur
         , b.t_sofr_crd_acc, b.t_sofr_crd_cur, b.t_sofr_payer_name, b.t_sofr_receiver_name, b.t_sofr_ground
         , b.t_check_pay
         , case when b.t_cft_dbt_cur = b.t_cft_crd_cur then 1 else 0 end AS t_check_cur  		-- Проверка соответствия валюты
         -- DEF-82701 Поправлена сверка t_check_name
         , case when b.t_check_pay = 0 then 0
           when (substr(b.t_sofr_crd_acc, 1, 3) = '306') then cft_utils.CFT_CheckName (b.t_cft_payer_name, b.t_sofr_receiver_payname)       -- зачисление
           when (substr(b.t_sofr_dbt_acc, 1, 3) = '306') then cft_utils.CFT_CheckName (b.t_cft_receiver_name, b.t_sofr_receiver_payname)    -- списание
           else 0 end AS t_check_name  									-- Проверка корректности ФИО
         -- DEF-81831 Поправлена сверка t_check_acc
         , case when b.t_check_pay = 0 then 0
           when ((substr(b.t_sofr_crd_acc, 1, 3) = '306') and (b.t_sofr_receiver_acc = b.t_cft_payer_acc)) then 1        -- зачисление
           when ((substr(b.t_sofr_dbt_acc, 1, 3) = '306') and (b.t_sofr_receiver_acc = b.t_cft_receiver_acc)) then 1     -- списание
           else 0 end AS t_check_acc  									-- Проверка корректности корреспонденции
         -- DEF-76827 заполнение t_sofr_receiver_acc
         , b.t_sofr_receiver_acc
         -- DEF-95548 заполнение t_sofr_receiver_payname
         , case when b.t_sofr_receiver_payname is null then cft_utils.CFT_GetAccountPartyName(b.t_sofr_receiver_acc)
           else cft_utils.CFT_GetValidName(b.t_sofr_receiver_payname) end AS t_sofr_receiver_payname
         , case when b.t_cft_sum = b.t_sofr_sum then 1 else 0 end AS t_check_sum  		-- Проверка суммы проводки
         -- DEF-76032 проверка юр.формы
         --           Нужно оставить проводки только для ФЛ
         --           для определения ФЛ передается счет противоположной стороны проводки по 306-му
         -- DEF-87040 поправлен счет, который передается в функцию проверки
         , cft_utils.CFT_CheckLegal2(
              case when substr(NVL(b.t_sofr_dbt_acc, b.t_cft_payer_acc), 1, 3) = '306' then 
                 NVL(b.t_sofr_dbt_acc, 'xxx')
              when substr(NVL(b.t_sofr_crd_acc, b.t_cft_receiver_acc), 1, 3) = '306' then 
                 NVL(b.t_sofr_crd_acc, 'xxx')
              else 'xxx' end
              , b.t_sofr_receiver_acc
         ) AS t_check_legal
       FROM (
       SELECT 
         a.t_cft_date_carry, a.t_cft_doc_number, a.t_cft_sum, a.t_cft_dbt_acc, a.t_cft_dbt_cur
         , a.t_cft_crd_acc, a.t_cft_crd_cur, a.t_cft_payer_acc, a.t_cft_receiver_acc
         , a.t_cft_payer_name, a.t_cft_receiver_name, a.t_cft_pay_purpose
         , a.t_sofr_date_carry, a.t_sofr_doc_number, a.t_sofr_sum, a.t_sofr_dbt_acc, a.t_sofr_dbt_cur
         , a.t_sofr_crd_acc, a.t_sofr_crd_cur
         -- DEF-95281 При переводах t_sofr_payer_name может не определиться, берем из буферной таблицы
         , NVL(a.t_sofr_payer_name, usrEnr.t_payername) AS t_sofr_payer_name
         , a.t_sofr_receiver_name, a.t_sofr_ground
         , a.t_check_pay
         -- DEF-76827 заполнение t_sofr_receiver_acc
         -- DEF-84337 при списании для внешнего платежа и для проводки, данные определяются по-разному
         , case 
           when ((substr(a.t_sofr_dbt_acc, 1, 3) = '306') and (substr(a.t_sofr_crd_acc, 1, 3) = '408')) then     -- списание, проводка через ГО
             a.t_sofr_crd_acc    
           when (substr(a.t_sofr_dbt_acc, 1, 3) = '306') then              					 -- списание, внешний платеж
             coalesce(p.t_receiveraccount, usrEnr.t_creditaccount, a.t_sofr_crd_acc) 				 -- DEF-95548  +a.t_sofr_crd_acc
           when (substr(a.t_sofr_crd_acc, 1, 3) = '306') then              					 -- зачисление
             a.t_sofr_dbt_acc
           else '' end AS t_sofr_receiver_acc
         -- DEF-76827 заполнение t_sofr_receiver_payname
         -- DEF-84337 при списании для внешнего платежа и для проводки, данные определяются по-разному
         , case 
           when ((substr(a.t_sofr_dbt_acc, 1, 3) = '306') and (substr(a.t_sofr_crd_acc, 1, 3) = '408')) then     -- списание, проводка через ГО
             a.t_sofr_receiver_name
           when (substr(a.t_sofr_dbt_acc, 1, 3) = '306') then              					 -- списание, внешний платеж
             NVL(cc.t_receivername, usrEnr.t_payername)
           when (substr(a.t_sofr_crd_acc, 1, 3) = '306') then              					 -- зачисление
             NVL(usrEnr.t_payername, '')
           else '' end AS t_sofr_receiver_payname
       FROM (
       SELECT 
         -- Данные ЦФТ
          cft.t_opdate AS t_cft_date_carry              -- Дата проводки
         , cft.t_idbiscotto AS t_cft_doc_number         -- Номер документа
         , case when cft.val_db = '810' 
                 and cft.val_cr = '810'
                then cft.t_amtrub 
                else cft.t_amtcur end AS t_cft_sum      -- DEF-81803 Сумма в валюте счета
         , cft.t_acct_db AS t_cft_dbt_acc               -- Счет дебета
         , cft.val_db AS t_cft_dbt_cur                  -- Валюта счета дебета
         , cft.t_acct_cr AS t_cft_crd_acc               -- Счет кредита
         , cft.val_cr AS t_cft_crd_cur                  -- Валюта счета кредита
         , cft.t_payeraccount AS t_cft_payer_acc        -- Счет плательщика
         , cft.t_receiveraccount AS t_cft_receiver_acc  -- Счет получателя
         , cft.t_payername AS t_cft_payer_name          -- Наименование плательщика
         , cft.t_receivername AS t_cft_receiver_name    -- Наименование получателя
         , cft.t_details AS t_cft_pay_purpose           -- Назначение платежа
         -- Данные СОФР
         , sofr.t_date_carry AS t_sofr_date_carry       -- Дата проводки
         , sofr.t_acctrnid AS t_sofr_doc_number         -- Номер документа
         , sofr.t_sum_payer AS t_sofr_sum               -- Сумма в валюте счета
         , sofr.t_account_payer AS t_sofr_dbt_acc       -- Счет дебета
         , sofr.val_db AS t_sofr_dbt_cur                -- Валюта счета дебета
         , sofr.t_account_receiver AS t_sofr_crd_acc    -- Счет кредита
         , sofr.val_cr AS t_sofr_crd_cur                -- Валюта счета кредита
         , cft_utils.CFT_GetAccountPartyName(
           sofr.t_account_payer
           ) AS t_sofr_payer_name                       -- Наименование плательщика
         , cft_utils.CFT_GetAccountPartyName(
           sofr.t_account_receiver
           ) AS t_sofr_receiver_name                    -- Наименование получателя
         , sofr.t_ground AS t_sofr_ground                -- Основание
         -- Сверка
         , case when cft.t_idbiscotto is not null 
           AND sofr.t_idbiscotto is not null 
           THEN 1 ELSE 0 END AS t_check_pay              -- Квитовка платежей
         FROM uEntCft_tmp cft 
         FULL OUTER JOIN uEntSofr_tmp sofr ON sofr.t_idbiscotto = cft.t_idbiscotto
         ) a
         LEFT JOIN dacctrn_dbt s ON (s.t_acctrnid = a.t_sofr_doc_number)
         LEFT JOIN  dpmdocs_dbt pmD ON (pmD.t_acctrnid = s.t_acctrnid) 
         LEFT JOIN  DPMPAYM_DBT p ON (p.t_paymentid = pmD.t_paymentid) 
         LEFT JOIN dpmrmprop_dbt cc ON (cc.t_paymentid = P.t_paymentid)
         LEFT JOIN doprdocs_dbt d ON (D.T_ACCTRNID = s.T_ACCTRNID)
         LEFT JOIN doproper_dbt op ON (OP.T_ID_OPERATION = D.T_ID_OPERATION)
         LEFT JOIN dnptxop_dbt o ON (o.t_id = OP.T_DOCUMENTID and O.T_DOCKIND = OP.T_DOCKIND)
         LEFT JOIN USR_ACC306ENROLL_DBT usrEnr ON (usrEnr.T_NPTXOPID = o.t_id)
       ) b
       ) c
       WHERE c.t_check_legal = 2
     ]';
     return x_sql;
  END;

  /** Возвращает номер пачки для сопоставления проводок ЦФТ и СОФР
  */
  FUNCTION CFT_GetPersNumberBisquit (
    p_NumberPack IN number
    , p_Oper IN Number
    , p_Department IN number
    , p_DateCarry IN Date
  ) 
    return varchar2
  IS
    x_PersNumberBisquit varchar2(32767);
  BEGIN
    SELECT coalesce( Y.T_PERS_NUMBER_BISQUIT, Y.CNTRPERSNUMBER, T_name_by_BRANCH, '9090909') 
      INTO x_PersNumberBisquit
    FROM ( 
      SELECT ( SELECT U.T_NAME FROM dllvalues_dbt U WHERE U.T_LIST = 5005 AND U.T_ELEMENT = p_NumberPack ) AS CNTRPERSNUMBER
      , ( SELECT RSB_STRUCT.getString(U.T_TEXT) FROM dnotetext_dbt U  
          WHERE U.T_ID = ( SELECT MAX(V.T_ID) FROM dnotetext_dbt V 
                           WHERE V.T_OBJECTTYPE = 92 AND V.T_NOTEKIND = 200 
                           AND V.T_DOCUMENTID = LPAD( TO_CHAR( p_Oper ), 10, '0') 
                           AND V.T_DATE <= p_DateCarry ) 
      ) AS T_PERS_NUMBER_BISQUIT
      , (SELECT '00100900' FROM ddp_dep_dbt W WHERE W.T_CODE = p_Department and W.T_NAME = '6300' ) AS T_name_by_BRANCH
      FROM DUAL 
    ) Y;
      
    RETURN x_PersNumberBisquit;
  END;

  /** Возвращает код формы для счета: 1 - юр, 2 - физ, 0 - банк
  */
  FUNCTION CFT_ClientLegalForm (
      p_AccountID IN number
    ) 
    RETURN number
  IS
    x_LegalForm NUMBER;
  BEGIN
    SELECT case when t_client = 1 then 0 else NVL(p.t_legalform, 1) end As LegalForm
      INTO x_LegalForm
      FROM daccount_dbt r 
      LEFT JOIN dparty_dbt p ON p.t_partyid = r.t_client
      WHERE r.t_accountid = p_AccountID
    ;
    RETURN x_LegalForm;
  END;

  /** Проверяет, есть ли полученный счет в таблице dbrokacc_dbt. 
      Возвращает 1, если есть. 
  */
  FUNCTION CFT_InDbrokacc( p_Account IN varchar2 )
    RETURN number deterministic
  IS
    x_Result NUMBER := 0;
  BEGIN
    SELECT count(1) INTO x_Result FROM dbrokacc_dbt WHERE t_account = p_Account;
    RETURN x_Result;
  END;

  /** Сравнивает счета ЦФТ и СОФР. 
      Возвращает 1, если совпадают. 
      0, если не совпадают.
  */
  FUNCTION CFT_MatchAccount(
      p_CftAccount IN varchar2, p_CftBal IN varchar2, p_CftVal IN varchar2
      , p_SofrAccount IN varchar2, p_SofrFuture IN varchar2, p_SofrBal IN varchar2, p_SofrVal IN varchar2, p_SofrLegalForm IN number
    )
    RETURN number
  IS
    x_Matched NUMBER := 0;
  BEGIN
    IF (p_SofrBal = '55555') THEN
      x_Matched := 1;
    ELSIF (p_CftAccount = p_SofrAccount) THEN
      x_Matched := 1;
    ELSIF (p_CftAccount = p_SofrFuture) THEN 
      x_Matched := 1;
    ELSIF(p_CftBal = p_SofrBal and p_CftVal = p_SofrVal and  p_SofrBal in ('40701', '40702', '40817')) THEN
      --здесь могут использоваться  сводные счета
      x_Matched := 1;
    ELSIF(p_CftBal = p_SofrBal and p_CftVal = p_SofrVal and p_SofrBal in ( '45815', '45817') and p_SofrLegalForm = 2) THEN
      -- можно сделать функцию для --а вообще, в дальнейшем хорошо бы сводные счета  45815 / 45817 включить в список сводных счетов dbrokacc_dbt
      x_Matched := 1;
    ELSIF(p_CftVal = p_SofrVal And p_CftBal in ('30102', '30109', '30110', '30111', '30232', '30233', '30302') 
                  and p_SofrBal in ('30102', '30109', '30110', '30111', '30232', '30233', '30302')
                  ) THEN
        -- здесь может быть подстановка других счетов (межфилиальные расчеты, корсчета и т.д.
      x_Matched := 1;
    ELSIF(p_CftBal = p_SofrBal and p_CftVal = p_SofrVal and CFT_InDbrokacc( p_CftAccount ) = 1 and p_SofrLegalForm = 2 ) THEN
      x_Matched := 1;
    END IF;
    RETURN x_Matched;
  END;

  /** Сравнивает даты проводок ЦФТ и СОФР. 
      Возвращает 1, если совпадают. 
      0, если не совпадают.
  */
  FUNCTION CFT_MatchDate(
      p_CftOpDate IN date, p_CftDocDate IN date, p_SofrDate IN date
    )
    RETURN number DETERMINISTIC
  IS
  BEGIN
    IF(p_SofrDate = p_CftOpDate or p_SofrDate = p_CftDocDate) THEN
      RETURN 1;
    END IF;
    RETURN 0;
  END;

  /** Сравнивает суммы проводок ЦФТ и СОФР. 
      Возвращает 1, если совпадают. 
      0, если не совпадают.
  */
  FUNCTION CFT_MatchSum(
      p_CftSum IN number, p_SofrSum IN number
    )
    RETURN number DETERMINISTIC
  IS
  BEGIN
    -- можно добавить проверку на сумму валютную, только нужно иметь в виду, 
    -- что в ЦФТ она 0 для проводок по переоценке и для рублевых проводок,
    -- а в СОФР - хранится отдельно для счета дебета и счета кредита
    IF(p_SofrSum - p_CftSum <= 0.01 and p_CftSum - p_SofrSum <= 0.01) THEN
      RETURN 1;
    END IF;
    RETURN 0;
  END;

  /** Сравнивает основание проводок ЦФТ и СОФР. 
      Возвращает 1, если совпадают. 
      0, если не совпадают.
      Из сравниваемых строк рекомендуется исключить все спецсимволы (в идеале - всё, кроме букв и цифр), 
      привести строки к одному регистру, определить меньшую длину получившихся строк и 
      сравнить на совпадение по этому количеству символов с начала строки. 
  */
  FUNCTION CFT_MatchText(
      p_CftText IN varchar2, p_SofrText IN varchar2, p_Flag IN number
    )
    RETURN number DETERMINISTIC
  IS
    l_CftText  varchar2(4000) := lower(p_CftText);
    l_SofrText varchar2(4000) := lower(p_SofrText);
  BEGIN
    IF(p_Flag = 0) THEN
      -- получен флаг 'не сравнивать основание'
      -- считаются, что сроки совпали
      RETURN 1;
    ELSE
      -- иначе, выполняется честное сравнение строк
      l_CftText  := regexp_replace(l_CftText, '[^[:alnum:]]', '');
      l_SofrText := regexp_replace(l_SofrText, '[^[:alnum:]]', '');

      IF regexp_like(l_CftText, l_SofrText) or regexp_like(l_SofrText, l_CftText) THEN
        RETURN 1;
      ELSE
        RETURN 0;
      END IF;
    END IF;
  END CFT_MatchText;

  /** Функция изменения счета по дебету для сводных счетов
  */
  FUNCTION CFT_UpdateSyncPayer RETURN number
  IS
    x_AccountPayer varchar2(25);
    x_SyncAcc varchar2(25);
    x_Count number := 0;
  BEGIN
    for i in (
       select s.t_acctrnid, s.t_account_payer, s.t_fiid_payer 
       from UENTSOFR_TMP s
       where substr(s.t_account_payer, 1, 8) in (select distinct substr(b.t_account,1,8) from dbrokacc_dbt b)
       order by s.t_account_payer
    ) loop
      if(x_AccountPayer is null or x_AccountPayer <> i.t_account_payer) then
         x_AccountPayer := i.t_account_payer;   
         x_SyncAcc := cft_utils.CFT_getSyncAcc ( i.t_account_payer, i.t_fiid_payer );
      end if;
      if(x_SyncAcc <> i.t_account_payer) then
        update UENTSOFR_TMP set t_sync_db = x_SyncAcc where t_acctrnid = i.t_acctrnid;
        x_Count := x_Count + SQL%ROWCOUNT;
      end if;
    end loop;
    COMMIT;
    RETURN x_Count;
  EXCEPTION
    WHEN others THEN
      RETURN -1;
  END;

  /** Функция изменения счета по крудиту для сводных счетов
  */
  FUNCTION CFT_UpdateSyncReceiver RETURN number
  IS
    x_AccountReceiver varchar2(25);
    x_SyncAcc varchar2(25);
    x_Count number := 0;
  BEGIN
    for i in (
       select s.t_acctrnid, s.t_account_receiver, s.t_fiid_receiver 
       from UENTSOFR_TMP s
       where substr(s.t_account_receiver, 1, 8) in (select distinct substr(b.t_account,1,8) from dbrokacc_dbt b)
       order by s.t_account_receiver
    ) loop
      if(x_AccountReceiver is null or x_AccountReceiver <> i.t_account_receiver) then
         x_AccountReceiver := i.t_account_receiver;   
         x_SyncAcc := cft_utils.CFT_getSyncAcc ( i.t_account_receiver, i.t_fiid_receiver );
      end if;
      if(x_SyncAcc <> i.t_account_receiver) then
        update UENTSOFR_TMP set t_sync_cr = x_SyncAcc where t_acctrnid = i.t_acctrnid;
        x_Count := x_Count + SQL%ROWCOUNT;
      end if;
    end loop;
    COMMIT;
    RETURN x_Count;
  EXCEPTION
    WHEN others THEN
      RETURN -1;
  END;

  /** Функция для сопоставления основания проводок
  */
  FUNCTION CFT_MatchTextLoop
    RETURN number
  IS
    x_Count number := 0;
  BEGIN
    FOR i IN (
      select r.t_acctrnid, r.t_idbiscotto from UENTCOMPARETEST_TMP r
        inner join uEntCft_tmp cft on (cft.t_idbiscotto = r.t_idbiscotto)
        inner join uEntSofr_tmp sofr on (sofr.t_acctrnid = r.t_acctrnid)
        where r.t_err < 0
        and cft_utils.CFT_MatchText( cft.t_details, sofr.t_ground, 1 ) = 0
    ) LOOP
      update UENTCOMPARETEST_TMP c 
        set c.t_err = 8 
        where c.t_acctrnid = i.t_acctrnid and c.t_idbiscotto = i.t_idbiscotto
      ;
      x_Count := x_Count + SQL%ROWCOUNT;
    END LOOP;
    COMMIT;
    RETURN x_Count;
  EXCEPTION
    WHEN others THEN
      RETURN -1;
  END;

  /** Функция получения проводок ЦФТ для соответствующего запроса. 
      Возвращает кол-во записей.
  */
  FUNCTION CFT_GetCftOnReqID(
      p_ReqID IN varchar2, p_Mode IN number
    )
    RETURN number
  IS
    x_Prefix VARCHAR2(64) := '';
    x_Result NUMBER := 0;
    x_StartTime pls_integer := dbms_utility.get_time;
    x_Sql CLOB;
  BEGIN
    it_log.log( 
      p_msg => x_Prefix||'Start, p_ReqID: '||p_ReqID
      , p_msg_type => it_log.c_msg_type__debug 
    );

    -- Удаление предыдущих данных
    it_log.log( p_msg => x_Prefix||'Удаление предыдущих данных', p_msg_type => it_log.c_msg_type__debug );
    EXECUTE IMMEDIATE 'DELETE FROM uEntCft_tmp';
    x_Result := SQL%ROWCOUNT;
    LogDone(x_Prefix, x_StartTime, x_Result);

    -- Получение данных ЦФТ для соответствующего запроса
    IF(p_Mode = 1) THEN
      x_Sql := getSqlCft306OnReqID();
      it_log.log( p_msg => x_Prefix||'Получение данных ЦФТ по 306-ым счетам', p_msg_type => it_log.c_msg_type__debug, p_msg_clob => x_Sql );
    ELSE
      x_Sql := getSqlCftOnReqID();
      it_log.log( p_msg => x_Prefix||'Получение данных ЦФТ', p_msg_type => it_log.c_msg_type__debug, p_msg_clob => x_Sql );
    END IF;
    EXECUTE IMMEDIATE x_Sql USING p_ReqID;
    x_Result := SQL%ROWCOUNT;
    COMMIT;
    LogDone(x_Prefix, x_StartTime, x_Result);

    RETURN x_Result;
  EXCEPTION
    WHEN others THEN
      it_error.put_error_in_stack;
      it_log.log(p_msg => 'Error'||SQLERRM, p_msg_type => it_log.c_msg_type__error);
      it_error.clear_error_stack;
      RAISE;
  END CFT_GetCftOnReqID;

  /** Функция возвращает количество итераций получения данных CОФР за дату. 
  */
  FUNCTION GetParamParts( p_UseParam IN NUMBER DEFAULT 0 )
    RETURN number
  IS
    x_Count NUMBER;
  BEGIN
    IF(p_UseParam = 0) THEN
      RETURN 1;
    ELSE
      SELECT COUNT(*) INTO x_Count FROM uEntCompareParam_tmp;
      IF(x_Count < 1) THEN
        x_Count := 1;
      END IF;
      RETURN x_Count;
    END IF;
  END GetParamParts;


  /** Функция получения проводок СОФР за дату. 
      Возвращает кол-во записей.
  */
  FUNCTION CFT_GetSofrOnDate(
      p_ReqStart IN date, p_ReqEnd IN date, p_UseParam IN NUMBER DEFAULT 0
    )
    RETURN number
  IS
    x_Prefix VARCHAR2(64) := '';
    x_Result NUMBER := 0;
    x_StartTime pls_integer := dbms_utility.get_time;
    x_Sql CLOB;
    x_Parts NUMBER := GetParamParts(p_UseParam);
  BEGIN
    it_log.log( 
      p_msg => x_Prefix||'Start, p_ReqStart: '||to_char (p_ReqStart, 'DD.MM.YYYY')||', p_ReqEnd: '||to_char (p_ReqEnd, 'DD.MM.YYYY')
      , p_msg_type => it_log.c_msg_type__debug 
    );

    -- Удаление предыдущих данных
    it_log.log( p_msg => x_Prefix||'Удаление предыдущих данных', p_msg_type => it_log.c_msg_type__debug );
    EXECUTE IMMEDIATE 'DELETE FROM uEntSofr_tmp';
    x_Result := SQL%ROWCOUNT;
    LogDone(x_Prefix, x_StartTime, x_Result);

    -- Получение данных СОФРа на дату
    FOR i IN 1..x_Parts LOOP
       x_Sql := getSqlSofrOnDate( p_UseParam, i );
       it_log.log( p_msg => x_Prefix||'Получение данных СОФР', p_msg_type => it_log.c_msg_type__debug, p_msg_clob => x_Sql );
       EXECUTE IMMEDIATE x_Sql USING p_ReqStart, p_ReqEnd;
       x_Result := x_Result + SQL%ROWCOUNT;
    END LOOP;
    COMMIT;
    LogDone(x_Prefix, x_StartTime, x_Result);

    it_log.log( p_msg => x_Prefix||'Корректировка для сводных счетов по дебету', p_msg_type => it_log.c_msg_type__debug );
    x_Result := CFT_UpdateSyncPayer();
    LogDone(x_Prefix, x_StartTime, x_Result);

    it_log.log( p_msg => x_Prefix||'Корректировка для сводных счетов по кредиту', p_msg_type => it_log.c_msg_type__debug );
    x_Result := CFT_UpdateSyncReceiver();
    LogDone(x_Prefix, x_StartTime, x_Result);

    RETURN x_Result;
  EXCEPTION
    WHEN others THEN
      it_error.put_error_in_stack;
      it_log.log(p_msg => 'Error'||SQLERRM, p_msg_type => it_log.c_msg_type__error);
      it_error.clear_error_stack;
      RAISE;
  END CFT_GetSofrOnDate;

  /** Функция получения проводок СОФР по 306-ым счетам за дату. 
      Возвращает кол-во записей.
  */
  FUNCTION CFT_GetSofr306OnDate(
      p_ReqStart IN date, p_ReqEnd IN date
    )
    RETURN number
  IS
    x_Prefix VARCHAR2(64) := '';
    x_Result NUMBER := 0;
    x_StartTime pls_integer := dbms_utility.get_time;
    x_Sql CLOB;
  BEGIN
    it_log.log( 
      p_msg => x_Prefix||'Start, p_ReqStart: '||to_char (p_ReqStart, 'DD.MM.YYYY')||', p_ReqEnd: '||to_char (p_ReqEnd, 'DD.MM.YYYY')
      , p_msg_type => it_log.c_msg_type__debug 
    );

    -- Удаление предыдущих данных
    it_log.log( p_msg => x_Prefix||'Удаление предыдущих данных', p_msg_type => it_log.c_msg_type__debug );
    EXECUTE IMMEDIATE 'DELETE FROM uEntSofr_tmp';
    x_Result := SQL%ROWCOUNT;
    LogDone(x_Prefix, x_StartTime, x_Result);

    -- Получение зачислений на 306-ой СОФРа на дату
    x_Sql := getSql306EnrollOnDate();
    it_log.log( p_msg => x_Prefix||'Получение зачислений на 306-ой СОФР', p_msg_type => it_log.c_msg_type__debug, p_msg_clob => x_Sql );
    EXECUTE IMMEDIATE x_Sql USING p_ReqStart, p_ReqEnd, x_Filter306;
    x_Result := x_Result + SQL%ROWCOUNT;
    LogDone(x_Prefix, x_StartTime, x_Result);
    COMMIT;

    -- Получение списаний с 306-го СОФРа на дату
    x_Sql := getSql306WriteOffOnDate();
    it_log.log( p_msg => x_Prefix||'Получение списаний с 306-го СОФР', p_msg_type => it_log.c_msg_type__debug, p_msg_clob => x_Sql );
    EXECUTE IMMEDIATE x_Sql USING p_ReqStart, p_ReqEnd, x_Filter306;
    x_Result := x_Result + SQL%ROWCOUNT;
    LogDone(x_Prefix, x_StartTime, x_Result);
    COMMIT;

    it_log.log( p_msg => x_Prefix||'Корректировка для сводных счетов по дебету', p_msg_type => it_log.c_msg_type__debug );
    x_Result := CFT_UpdateSyncPayer();
    LogDone(x_Prefix, x_StartTime, x_Result);

    it_log.log( p_msg => x_Prefix||'Корректировка для сводных счетов по кредиту', p_msg_type => it_log.c_msg_type__debug );
    x_Result := CFT_UpdateSyncReceiver();
    LogDone(x_Prefix, x_StartTime, x_Result);

    RETURN x_Result;
  EXCEPTION
    WHEN others THEN
      it_error.put_error_in_stack;
      it_log.log(p_msg => 'Error'||SQLERRM, p_msg_type => it_log.c_msg_type__error);
      it_error.clear_error_stack;
      RAISE;
  END CFT_GetSofr306OnDate;

  /** Функция возвращает флаг, нужно ли сравнивать основания проводок.
      Если 0, сравнивать основания не нужно.
      Если 1, сравнивать основания нужно.
  */
  FUNCTION CFT_GetMatchTextFlag RETURN number
  IS
     x_MatchTextFlag NUMBER := 0; -- по умолчанию, основания проводок не сравниваются.
   BEGIN
      IF(Rsb_Common.GetRegBoolValue('РСХБ\ИНТЕГРАЦИЯ\СВЕРКА_ОСНОВАНИЯ_ПРОВОДОК')) THEN
        x_MatchTextFlag := 1;
      END IF;
      RETURN x_MatchTextFlag;
  END CFT_GetMatchTextFlag;


  /** Функция сопостовления дат для выборки СОФР и ЦФТ
      Возвращает кол-во записей.
  */
  FUNCTION CFT_MathDates
    RETURN number
  IS
    x_Rows NUMBER := 0;
    x_Prefix VARCHAR2(64) := '';
    x_Result NUMBER := 0;
    x_StartTime pls_integer := dbms_utility.get_time;
  BEGIN
    it_log.log( p_msg => x_Prefix||'Размечаем даты cft.t_opdate, которые есть в СОФР', p_msg_type => it_log.c_msg_type__debug );
    UPDATE uEntCft_tmp cft
      SET cft.matched_date = '1' 
      WHERE cft.t_opdate in (select distinct t_date_carry from uEntSofr_tmp)
    ;
    x_Result := SQL%ROWCOUNT;
    LogDone(x_Prefix, x_StartTime, x_Result);

    it_log.log( p_msg => x_Prefix||'Размечаем даты cft.t_docdate, которые есть в СОФР', p_msg_type => it_log.c_msg_type__debug );
    UPDATE uEntCft_tmp cft
      SET cft.matched_date = '1' 
      WHERE cft.t_docdate in (select distinct t_date_carry from uEntSofr_tmp)
    ;
    x_Result := SQL%ROWCOUNT;
    LogDone(x_Prefix, x_StartTime, x_Result);

    it_log.log( p_msg => x_Prefix||'Получение расхождений по датам из ЦФТ, которых нет в СОФР', p_msg_type => it_log.c_msg_type__debug );
    INSERT INTO UENTCOMPARETEST_TMP r (
      r.t_acctrnid, r.t_acct_db, r.t_acct_cr, r.t_amtcur, r.t_amtrub
      , r.t_details, r.t_opdate, r.t_idbiscotto, r.t_err
      , r.t_currency, r.t_docnum, r.t_sync_db, r.t_sync_cr
    )
    SELECT 
      null, cft.t_acct_db, cft.t_acct_cr, cft.t_amtcur, cft.t_amtrub
      , cft.t_details, cft.t_opdate, cft.t_idbiscotto, 2 err_type
      , cft.t_currency, cft.t_docnum, '' AS t_sync_db, '' AS t_sync_cr
    FROM uEntCft_tmp cft  
    WHERE cft.matched_date = '0'
    ;
    x_Result := SQL%ROWCOUNT;
    x_Rows := x_Rows + x_Result;
    LogDone(x_Prefix, x_StartTime, x_Result);

    it_log.log( p_msg => x_Prefix||'Размечаем даты sofr.t_date_carry, которые есть в ЦФТ', p_msg_type => it_log.c_msg_type__debug );
    UPDATE uEntSofr_tmp r 
      SET matched_date = '1' 
      WHERE r.t_date_carry IN (
         select distinct cft.t_opdate from uEntCft_tmp cft
         union 
         select distinct cft.t_docdate from uEntCft_tmp cft)
    ;
    x_Result := SQL%ROWCOUNT;
    LogDone(x_Prefix, x_StartTime, x_Result);

    it_log.log( p_msg => x_Prefix||'Получение расхождений по датам из СОФР, которых нет в ЦФТ', p_msg_type => it_log.c_msg_type__debug );
    INSERT INTO UENTCOMPARETEST_TMP r (
      r.t_acctrnid, r.t_acct_db, r.t_acct_cr, r.t_amtcur, r.t_amtrub
      , r.t_details, r.t_opdate, r.t_idbiscotto, r.t_err
      , r.t_currency, r.t_docnum, r.t_sync_db, r.t_sync_cr
    )
    SELECT 
      sofr.t_acctrnid, sofr.t_account_payer, sofr.t_account_receiver, sofr.t_sum_payer, sofr.t_sum_natcur
      , sofr.t_ground, sofr.t_date_carry prov_date, '' AS idbiscotto, 3 err_type
      , sofr.t_currency, sofr.t_docnum, sofr.t_sync_db, sofr.t_sync_cr
    FROM uEntSofr_tmp sofr  
    WHERE sofr.matched_date = '0'
    ;
    x_Result := SQL%ROWCOUNT;
    x_Rows := x_Rows + x_Result;
    LogDone(x_Prefix, x_StartTime, x_Result);

    COMMIT;
    RETURN x_Rows;
  END CFT_MathDates;


  /** Функция создает записи сравнения проводок ЦФТ и СОФР. 
      Возвращает кол-во записей.
  */
  FUNCTION CFT_CreateCompare(
      p_ReqID IN varchar2, p_ReqDate IN date
    )
    RETURN number
  IS
    x_Prefix VARCHAR2(64) := '';
    x_Result NUMBER := 0;
    x_Result0 NUMBER := 0;
    c_EndDate DATE := p_ReqDate;
    x_StartTime pls_integer := dbms_utility.get_time;
    x_Sql CLOB;
    x_Rows number;
    x_MatchTextFlag NUMBER := 0; -- по умолчанию, основания проводок не сравниваются.
  BEGIN
    it_log.log( 
      p_msg => x_Prefix||'Start, p_ReqID: '||p_ReqID||', p_ReqDate: '||to_char (p_ReqDate, 'DD.MM.YYYY')
      , p_msg_type => it_log.c_msg_type__debug 
    );

    -- Удаление предыдущих данных
    it_log.log( p_msg => x_Prefix||'Удаление данных в UENTCOMPARETEST_TMP', p_msg_type => it_log.c_msg_type__debug );
    DELETE FROM UENTCOMPARETEST_TMP;
    x_Result := SQL%ROWCOUNT;
    LogDone(x_Prefix, x_StartTime, x_Result);

    -- подготовительная часть, проводки СОФРа собираются в темперную таблицу uEntSofr_tmp
    -- последующей подменой счетов СОФРа на сводные
    x_Rows := cft_utils.CFT_GetSofrOnDate( p_ReqDate, p_ReqDate, 1 );  

    -- проводки ЦФТ собираются в темперную таблицу uEntCft_tmp
    x_Rows := cft_utils.CFT_GetCftOnReqID( p_ReqID, 0 );  
    COMMIT;

    -- Сопоставление дат
    x_Rows := cft_utils.CFT_MathDates();

    -- Получение данных сравнения проводок
    x_Sql := getSqlCreateCompare();
    it_log.log( p_msg => x_Prefix||'Получение расхождений', p_msg_type => it_log.c_msg_type__debug, p_msg_clob => x_Sql );
    EXECUTE IMMEDIATE x_Sql USING c_EndDate;
    x_Result := SQL%ROWCOUNT;
    LogDone(x_Prefix, x_StartTime, x_Result);
    COMMIT;

    -- Флаг сравнения оснований проводок
    x_MatchTextFlag := CFT_GetMatchTextFlag();
    IF(x_MatchTextFlag = 0) THEN
      it_log.log( p_msg => x_Prefix||'Флаг сравнения оснований проводок -- Нет', p_msg_type => it_log.c_msg_type__debug );
    ELSE
      it_log.log( p_msg => x_Prefix||'Флаг сравнения оснований проводок -- Да', p_msg_type => it_log.c_msg_type__debug );
      x_Rows := CFT_MatchTextLoop();
      LogDone(x_Prefix, x_StartTime, x_Rows);
    END IF;
 
    RETURN x_Result0;
  EXCEPTION
    WHEN others THEN
      it_error.put_error_in_stack;
      it_log.log(p_msg => 'Error'||SQLERRM, p_msg_type => it_log.c_msg_type__error);
      it_error.clear_error_stack;
      RAISE;
  END;

  /** Процедура для добавления записей в темперную таблицу для фильтрации данных отчета-сверки проводок ЦФТ и СОФР. 
      При добавлении p_Id меньше 1, предварительно будут удаляться предыдущие данные
  */
  PROCEDURE CFT_AddEntCompareParam (p_Id IN number, p_PersN IN varchar2, p_AccD IN varchar2, p_AccC IN varchar2)
  IS
  BEGIN
    IF( p_Id < 1 ) THEN 
      DELETE FROM uEntCompareParam_tmp;
    END IF;
    INSERT INTO uEntCompareParam_tmp (
      Id, PersN, Acc_D, Acc_C
    ) VALUES (
      p_Id, p_PersN, p_AccD, p_AccC
    );
    COMMIT;
  END;

  /** BOSS-1266_BOSS-5723
      Процедура для добавления записей в темперную таблицу для фильтрации данных отчета-сверки проводок ЦФТ и СОФР. 
      При добавлении p_Id меньше 1, предварительно будут удаляться предыдущие данные
  */
  PROCEDURE CFT_AddEntCompareParam1 ( p_Id IN number, p_EqDebit IN varchar2, p_NonDebit IN varchar2, p_EqCredit IN varchar2, p_NonCredit IN varchar2)
  IS
  BEGIN
    IF( p_Id < 1 ) THEN 
      DELETE FROM uEntCompareParam_tmp;
    END IF;
    INSERT INTO uEntCompareParam_tmp r (
      r.Id, r.PersN, r.Acc_D, r.Acc_C, r.t_eq_debit, r.t_non_debit, r.t_eq_credit, r.t_non_credit
    ) VALUES (
      p_Id, chr(1), chr(1), chr(1), nvl(p_EqDebit, chr(1)), nvl(p_NonDebit, chr(1)), nvl(p_EqCredit, chr(1)), nvl(p_NonCredit, chr(1))
    );
    COMMIT;
  END;

  /** Табличная функция, возвращает результат сравнения проводок СОФР и ЦФТ,
      если полностью совпадают номера счетов
  */
  FUNCTION MatchSofrCftEnts (
    p_ReqID IN varchar2			-- номер запроса ЦФТ
    , p_StartDate IN date		-- начальная дата диапазона
    , p_EndDate IN date			-- конечная дата диапазона
  ) 
    RETURN tab_matchA pipelined
  IS
    x_Rec rec_matchA;
    x_Cursor SYS_REFCURSOR;
    x_Sql CLOB;
  BEGIN
    x_Sql := getSqlSofrCftEnts();
    OPEN x_Cursor FOR x_Sql USING p_ReqID, p_StartDate, p_EndDate;
    LOOP
      FETCH x_Cursor INTO x_Rec;
      EXIT WHEN x_Cursor%NOTFOUND;
      PIPE ROW (x_Rec);
    END LOOP;
    CLOSE x_Cursor;
  END MatchSofrCftEnts;

  /** Табличная функция, возвращает результат сравнения проводок СОФР и ЦФТ,
      у которых совпадают балансовые счета и валюты, 
      а также балансовые счета соответствуют заданным маскам
  */
  FUNCTION MatchSofrCftEntsByMask (
    p_ReqID IN varchar2			-- номер запроса ЦФТ
    , p_StartDate IN date		-- начальная дата диапазона
    , p_EndDate IN date			-- конечная дата диапазона
    , p_MaskDb IN varchar2  		-- маска для счета по дебету
    , p_MaskCr IN varchar2 		-- маска для счета по кредиту
  ) 
    RETURN tab_matchB pipelined
  IS
    x_Rec rec_matchB;
    x_Cursor SYS_REFCURSOR;
    x_Sql CLOB;
    x_Rows number;
  BEGIN
    x_Sql := getSqlSofrCftEntsByMask();
    OPEN x_Cursor FOR x_Sql USING p_ReqID, p_StartDate, p_EndDate, p_MaskDb, p_MaskCr;
    LOOP
      FETCH x_Cursor INTO x_Rec;
      EXIT WHEN x_Cursor%NOTFOUND;
      PIPE ROW (x_Rec);
    END LOOP;
    CLOSE x_Cursor;
  END MatchSofrCftEntsByMask;

  /** Функция определения ID выгрузки отчета-сверки проводок.
      Если получен параметр, то возвращается он.
      Если параметр не задан, он определяется по последней выгрузке.
  */
  FUNCTION CFT_GetReqID (p_ReqID IN VARCHAR2) RETURN varchar2
  IS
    x_ReqID VARCHAR2(40);
  BEGIN
    IF(p_ReqID is not null) THEN
      x_ReqID := p_ReqID;
    ELSE
      SELECT r.t_reqid INTO x_ReqID FROM uloadentforcompare_dbt r 
        WHERE r.t_autokey = (SELECT max(t_autokey) FROM uloadentforcompare_dbt r);
    END IF;
    RETURN x_ReqID;
  END CFT_GetReqID;

  /** Функция определения даты отчета-сверки проводок
  */
  FUNCTION CFT_GetStartDate (p_ReqID IN VARCHAR2, p_StartDate IN DATE) RETURN date
  IS
    x_Param VARCHAR2(40);
    x_Toc1 VARCHAR2(40);
    x_Toc2 VARCHAR2(40);
    x_Toc3 VARCHAR2(40);
    x_StartDate DATE;
    x_EndDate DATE;
  BEGIN
    IF(p_StartDate is not null) THEN
      x_StartDate := p_StartDate;
    ELSE
      SELECT r.t_param INTO x_Param FROM utableprocessevent_dbt r WHERE r.t_recid = p_ReqID;
      x_Toc1 := getToken(x_Param, ';');
      x_Toc2 := getToken(x_Param, ';');
      x_Toc3 := getToken(x_Param, ';');
      x_StartDate := to_date(x_Toc2,'dd.mm.yyyy');
      x_EndDate := to_date(x_Toc3,'dd.mm.yyyy');
    END IF;
    RETURN x_StartDate;
  END CFT_GetStartDate;

  /** Функция собирает данные по последней выгрузке сравнения проводок СОФР и ЦФТ.
      Используется для функционала авто-тестирования
  */
  FUNCTION PrepareEntCompareTestTmp(p_ReqID IN VARCHAR2, p_StartDate IN DATE)
    RETURN NUMBER
  IS
    x_rows number;
    x_ReqID VARCHAR2(40);
    x_StartDate DATE;
    x_Sql CLOB;
  BEGIN
    x_ReqID := CFT_GetReqID(p_ReqID);
    x_StartDate := CFT_GetStartDate(x_ReqID, p_StartDate);
    cft_utils.CFT_AddEntCompareParam (-1);
    x_rows := cft_utils.CFT_CreateCompare(x_ReqID, x_StartDate);
    COMMIT;
    RETURN 0;
  EXCEPTION
    WHEN others THEN
      it_error.put_error_in_stack;
      it_log.log(p_msg => 'ERROR:'||SQLERRM, p_msg_type => it_log.c_msg_type__error);
      it_error.clear_error_stack;
      RETURN 1;
  END PrepareEntCompareTestTmp;

  /** Если возможно, функция возвратит синтетический счет для полученного аналитического
  */
  FUNCTION CFT_getSyncAcc ( p_TestAcc IN varchar2, p_FIID IN number ) 
    RETURN varchar2 deterministic
  IS
    x_UserTypeAccount varchar2(16);
    x_SyncAcc varchar2(25);
    x_ServKind number := 0;
    x_ServKindSub number := 0;
  BEGIN
    -- входной тест: если счет не может быть сводным, то ничего потом и не делаем 
    -- (можно потерять время на просмотре dmcaccdoc_dbt)
    SELECT r.t_account INTO x_SyncAcc FROM dbrokacc_dbt r 
      WHERE substr(r.t_account, 1, 8) = substr(p_TestAcc, 1, 8)
      AND rownum = 1
    ;
    SELECT a.t_usertypeaccount INTO x_UserTypeAccount FROM daccount_dbt a WHERE a.t_account = p_TestAcc;
    IF(instr(x_UserTypeAccount, chr(133)) != 0) THEN
       -- ЕДП, используем x_ServKind := 0, x_ServKindSub := 0;
       NULL;
    ELSE
       -- не ЕДП;
       SELECT sf.t_servkind, sf.t_servkindsub INTO x_ServKind, x_ServKindSub
       FROM dsfcontr_dbt sf
       WHERE sf.t_id = ( SELECT m.t_clientcontrid FROM dmcaccdoc_dbt m
          WHERE m.t_account = p_TestAcc AND ((m.T_ISCOMMON = 'X') 
              or ( ( m.T_ISCOMMON = chr(0) ) and (m.t_disablingdate <> to_date('01010001','ddmmyyyy')) and (m.t_dockind = 0) )
              or ( ( m.T_ISCOMMON = chr(0) ) and (m.t_dockind = 3001) ) 
          )
          AND rownum = 1
       );
    END IF;
    SELECT r.t_account INTO x_SyncAcc
      FROM dbrokacc_dbt r 
      WHERE substr(r.t_account, 1, 8) = substr(p_TestAcc, 1, 8)
      AND r.t_servkind = x_ServKind 
      AND r.t_servkindsub = x_ServKindSub 
      AND r.t_currency = p_FIID
      ;
    RETURN x_SyncAcc;
  EXCEPTION
      WHEN others THEN
        -- не нашли, значит, синтетического счета нет
        RETURN p_TestAcc;
  END CFT_getSyncAcc;

  /**
   @brief    Функция возвращает строку ошибки.
  */
  FUNCTION GetStrEventErr( p_Mode IN INTEGER, p_Objecttype IN NUMBER, p_ObjectId IN NUMBER ) RETURN VARCHAR2
  IS                                                                                          
   v_StrErr   VARCHAR2(1000);
  BEGIN
    IF( p_Mode IN (2,3) ) THEN
      v_StrErr := USR_PKG_IMPORT_SOFR.GetStrEventErr( p_Mode, p_Objecttype, p_ObjectId );
    ELSIF (p_Mode = 4) THEN 
      v_StrErr := 'Из ЦФТ в СОФР. Сумма проводки не совпадает';
    ELSIF (p_Mode = 5) THEN 
      v_StrErr := 'Из ЦФТ в СОФР. Счет по дебету не совпадает';
    ELSIF (p_Mode = 6) THEN 
      v_StrErr := 'Из ЦФТ в СОФР. Счет по кредиту не совпадает';
    ELSIF (p_Mode = 7) THEN 
      v_StrErr := 'Из ЦФТ в СОФР. Даты не совпадают';
    ELSIF (p_Mode = 8) THEN 
      v_StrErr := 'Из ЦФТ в СОФР. Основание проводки не совпадает';
    END IF;
    RETURN v_StrErr;
   EXCEPTION
    WHEN OTHERS THEN
     RETURN '';
   END;

  /** BOSS-1266_BOSS-5723
      Функция получает условие доп.фильтра на выборку проводок из СОФР. 
      По данным файла uEntCompareParam_tmp
      Если p_Mode = 0, то возвращается полное условие
      Если p_Mode = 1, то возвращается условие для маски счетов дебета
      Если p_Mode = 2, то возвращается условие для маски счетов кредита
      Если p_Mode = 3, то возвращается условие по параметрам t_eq_debit, t_non_debit, t_eq_credit, t_non_creedit
  */
  FUNCTION CFT_GetExtCondition( p_Mode IN number, p_part IN number ) RETURN VARCHAR2
  IS                                                                                          
    x_DbtCond varchar2(1000);
    x_CrdCond varchar2(1000);
    x_Delim varchar2(1);
    x_And varchar2(5);
    x_Or varchar2(5);
    x_Cond varchar2(1000);
    x_AllCond varchar2(1000);
    x_Part NUMBER;
  BEGIN
    IF (p_Mode = 0) THEN
      -- полное условие
      x_DbtCond := CFT_GetExtCondition(1);
      x_CrdCond := CFT_GetExtCondition(2);
      IF((x_DbtCond IS NOT NULL) AND (x_CrdCond IS NOT NULL)) THEN
         RETURN '('||x_DbtCond||' OR '||x_CrdCond||')';
      ELSIF (x_DbtCond IS NOT NULL) THEN 
         RETURN x_DbtCond;
      ELSIF (x_CrdCond IS NOT NULL) THEN 
         RETURN x_CrdCond;
      END IF;
    ELSIF (p_Mode = 1) THEN 
      -- условие по дебету
      x_DbtCond := NULL;
      FOR i IN (select distinct acc_d from uEntCompareParam_tmp where acc_d is not null and acc_d != chr(1)) LOOP
        x_DbtCond := x_DbtCond||x_Delim||i.acc_d||'*';
        x_Delim := '|';
      END LOOP;
      IF(x_DbtCond is not null) THEN
        RETURN '(RSB_MASK.CompareStringWithMask( '''||x_DbtCond||''', B.T_ACCOUNT_PAYER ) > 0)';
      END IF;
    ELSIF (p_Mode = 2) THEN 
      -- условие по кредиту
      x_CrdCond := NULL;
      FOR i IN (select distinct acc_c from uEntCompareParam_tmp where acc_c is not null and acc_c != chr(1)) LOOP
        x_CrdCond := x_Delim||i.acc_c||'*';
        x_Delim := '|';
      END LOOP;
      IF(x_CrdCond is not null) THEN
        RETURN '(RSB_MASK.CompareStringWithMask( '''||x_CrdCond||''', B.T_ACCOUNT_RECEIVER ) > 0)';
      END IF;
    ELSIF (p_Mode = 3) THEN 
      -- условие по параметрам t_eq_debit, t_non_debit, t_eq_credit, t_non_credit
      x_Or := null;
      x_AllCond := null;
      x_Part := 1;
      FOR i IN (select * from uEntCompareParam_tmp order by id) LOOP
        x_Cond := null;
        x_And := null;
        IF (x_Part = p_Part) THEN
          IF(NVL(i.t_eq_debit, chr(1)) != chr(1)) THEN
            x_Cond := x_Cond||x_And||'(RSB_MASK.CompareStringWithMask( '''||i.t_eq_debit||''', B.T_ACCOUNT_PAYER ) > 0)';
            x_And := ' AND ';
          END IF;
          IF(NVL(i.t_non_debit, chr(1)) != chr(1)) THEN
            x_Cond := x_Cond||x_And||'(RSB_MASK.CompareStringWithMask( '''||i.t_non_debit||''', B.T_ACCOUNT_PAYER ) = 0)';
            x_And := ' AND ';
          END IF;
          IF(NVL(i.t_eq_credit, chr(1)) != chr(1)) THEN
            x_Cond := x_Cond||x_And||'(RSB_MASK.CompareStringWithMask( '''||i.t_eq_credit||''', B.T_ACCOUNT_RECEIVER ) > 0)';
            x_And := ' AND ';
          END IF;
          IF(NVL(i.t_non_credit, chr(1)) != chr(1)) THEN
            x_Cond := x_Cond||x_And||'(RSB_MASK.CompareStringWithMask( '''||i.t_non_credit||''', B.T_ACCOUNT_RECEIVER ) = 0)';
            x_And := ' AND ';
          END IF;
          IF(x_Cond IS NOT NULL) THEN
            x_AllCond := x_AllCond||x_Or||'('||x_Cond||')';
            x_Or := ' OR ';
          END IF;
        END IF;
        x_Part := x_Part + 1;
      END LOOP;
      RETURN x_AllCond;
    END IF;
    RETURN NULL;
   END;

  /** BOSS-1266_BOSS-5723
      Функция создает записи сравнения зачисления-списания ДС ЦФТ и СОФР. 
      Проводки по 306-ым счетам.
      Возвращает кол-во записей.
  */
  FUNCTION CFT_CreateCompare306(
    p_ReqID IN varchar2, p_ReqStart IN date, p_ReqEnd IN date
    )
    RETURN number
  IS
    x_Prefix VARCHAR2(64) := '';
    x_Result NUMBER := 0;
    x_Result0 NUMBER := 0;
    c_EndDate DATE := p_ReqEnd;
    x_StartTime pls_integer := dbms_utility.get_time;
    x_Sql CLOB;
    x_Rows number;
    x_MatchTextFlag NUMBER := 0; -- по умолчанию, основания проводок не сравниваются.
  BEGIN
    it_log.log( 
      p_msg => x_Prefix||'Start, p_ReqID: '||p_ReqID||', p_ReqStart: '||to_char (p_ReqStart, 'DD.MM.YYYY')
      , p_msg_type => it_log.c_msg_type__debug 
    );

    -- Удаление предыдущих данных
    it_log.log( p_msg => x_Prefix||'Удаление данных в uent306compare_tmp', p_msg_type => it_log.c_msg_type__debug );
    DELETE FROM uent306compare_tmp;
    x_Result := SQL%ROWCOUNT;
    LogDone(x_Prefix, x_StartTime, x_Result);

    -- подготовительная часть, проводки СОФРа собираются в темперную таблицу uEntSofr_tmp
    -- последующей подменой счетов СОФРа на сводные
    x_Rows := cft_utils.CFT_GetSofr306OnDate( p_ReqStart, p_ReqEnd );  

    -- проводки ЦФТ собираются в темперную таблицу uEntCft_tmp
    -- p_Mode = 1, это проводки по 306-ым счетам
    x_Rows := cft_utils.CFT_GetCftOnReqID( p_ReqID, 1 );  

    -- Удалим лишние ЦФТ-проводки
    DELETE FROM uEntCft_tmp cft 
      WHERE ((RSB_MASK.CompareStringWithMask( '306*', cft.t_acct_db ) > 0) 
              AND (RSB_MASK.CompareStringWithMask( x_Filter306, cft.t_acct_cr ) > 0 ));
    DELETE FROM uEntCft_tmp cft 
      WHERE ((RSB_MASK.CompareStringWithMask( x_Filter306, cft.t_acct_db ) > 0) 
              AND (RSB_MASK.CompareStringWithMask( '306*', cft.t_acct_cr ) > 0 ));
    COMMIT;

    -- Получение данных отчета-сверки по 306-ым
    x_Sql := getSqlCreateCompare306();
    it_log.log( p_msg => x_Prefix||'Получение данных отчета-сверки по 306-ым', p_msg_type => it_log.c_msg_type__debug, p_msg_clob => x_Sql );
    EXECUTE IMMEDIATE x_Sql;
    x_Result := SQL%ROWCOUNT;
    LogDone(x_Prefix, x_StartTime, x_Result);
    COMMIT;

    RETURN x_Result0;
  EXCEPTION
    WHEN others THEN
      it_error.put_error_in_stack;
      it_log.log(p_msg => 'Error'||SQLERRM, p_msg_type => it_log.c_msg_type__error);
      it_error.clear_error_stack;
      RAISE;
  END;

END cft_utils;
/
