create or replace package body rsb_dclportsofr2dwh is

 g_dt_begin               date;


 procedure initial_param(p_dt_begin  date default null)
 is

 begin

 execute immediate 'truncate table dclportsofr2dwh_dbt';
 execute immediate 'truncate table dclportsofr2dwh_tmp';

 if(p_dt_begin is null) then
   select trunc(sysdate-1)
    into g_dt_begin
    from dual;
 else
   g_dt_begin:=p_dt_begin;
 end if;

 end;


 procedure load_bufferTable_1(p_out_err_msg out        varchar2
                             ,p_out_result  out        number)
 is
 begin

     insert into dclportsofr2dwh_tmp (t_partyid
                                     ,t_contrnumber
                                     ,t_contrdatebegin
                                     ,t_clientname
                                     ,t_clientcode
                                     ,t_dsfcontrid
                                     ,t_fininstr
                                     ,t_fininstrtype
                                     ,t_fiid
                                     ,t_avrkind
                                     ,t_qty
                                     ,t_nkd
                                     ,t_price
                                     ,t_rateid
                                     ,t_ratecb
                                     ,t_fininstrccy
                                     ,t_facevalue
                                     ,t_open_balance
                                     ,t_open_balance_rub
                                     ,t_is_partial
                                     ,t_profitaccount
                                     ,t_iis
                                     ,t_dlcontrid)

      WITH dt as (select g_dt_begin as dt_begin from dual)
    ,sfcontr as (
      select dt_begin --Даты которые задаем в отчете
          ,dlcontr.t_dlcontrid t_dlcontrid --идентификатор  ДБО
          ,sfcontr.t_id t_sfcontrid --идентификатор договора
          ,sfcontr.t_partyid t_partyid --плательщик
          ,sfcontr.t_number t_contrnumber --номер договора
          ,sfcontr.t_datebegin t_contrdatebegin --дата начала действия договора
          ,nvl(party.t_name,chr(1)) t_clientname  --ФИО клиента
          ,nvl((select t_code
                  from ddlobjcode_dbt dlobj --код объекта
                 where dlobj.t_objecttype = 207  --идентификатор типа объекта
                   and dlobj.t_codekind = 1
                   and dlobj.t_objectid =  dlcontr.t_dlcontrid --связка с площадкой ДБО
                   and (dlobj.t_bankclosedate >=  dt_begin or dlobj.T_BANKCLOSEDATE = to_date('01010001', 'ddmmyyyy'))
                   ),chr(1)) t_clientcode --код объекта
          --Признак выплаты купона на текущий/брокерский счет
          ,atcor.t_general  t_profitaccount
          ,case
             when dlcontr.t_iis = chr(88)
               then 1
               else 0
           end  t_iis
         from dsfcontr_dbt sfcontr --   Договор обслуживания
         join (select dt_begin from dt) on 1 = 1
              and sfcontr.t_partyid != nvl(RsbSessionData.OurBank,1)   -- плательщик не равен 1
         join ddlcontr_dbt dlcontr on dlcontr.t_sfcontrid = sfcontr.t_id  --Договор брокерского обслуживания
         left join dparty_dbt party on party.t_partyid = sfcontr.t_partyid   --Субьект экономики
         left join dobjatcor_dbt atcor  on  atcor.t_object = LPAD (sfcontr.t_id, 34, 0)
                                        and atcor.t_groupid=123
                                        and atcor.t_objecttype=207
                                        and atcor.t_validfromdate <=  dt_begin
                                        and atcor.t_validtodate >=  dt_begin
        where (sfcontr.t_dateclose = to_date('01.01.0001','dd.mm.yyyy') or  sfcontr.t_dateclose >= dt_begin)
          and party.t_legalform = 2
          and sfcontr.t_servkind = 0
          and sfcontr.t_servkindsub = 0
   )
   --берем лимиты с приоритетом от t365 до t0
   ,limsec AS (
          SELECT F.t_limit_kind
                ,F.t_client
                ,F.t_client_code
                ,F.t_seccode
                ,F.t_security
                ,F.t_open_balance
          FROM
          (SELECT row_number() OVER (PARTITION BY limsec.t_client,limsec.t_client_code, limsec.t_seccode,limsec.t_security ORDER BY limsec.t_limit_kind DESC) row_order
                ,limsec.t_limit_kind
                ,limsec.t_client
                ,limsec.t_client_code
                ,limsec.t_seccode
                ,limsec.t_security
                ,limsec.t_open_balance
          FROM DDL_LIMITSECURITES_DBT limsec 
          WHERE limsec.t_limit_kind in (0,1,2,365)
            AND limsec.t_open_balance <> 0
            AND limsec.t_date = (select max(S.t_date) from DDL_LIMITSECURITES_DBT S)) F
          WHERE F.row_order = 1)
   ,q AS(
       select distinct
              FIN.T_FIID
             ,FIN.t_Name
             --код интрумента
             ,nvl(avo.t_isin,avo.t_lsin) as t_isin
             ,null t_Contract
             --Валюта инструмента
             ,(SELECT finface.t_ccy
                 FROM dfininstr_dbt finface
                WHERE finface.t_fiid = fin.t_facevaluefi) face_fi
             --код базового инструмента
             ,fin.t_facevaluefi
             --код типа ц.б
             ,RSB_FIInstr.FI_AvrKindsGetRoot(fin.t_FI_Kind, fin.t_AvoirKind) t_AvrKind
             --ID клиент
             ,t.t_Party
              ,(select avrkind.t_name
                 from davrkinds_dbt avrkind
                where avrkind.t_root = RSB_FIInstr.FI_AvrKindsGetRoot(fin.t_FI_Kind, fin.t_AvoirKind)
                  and avrkind.t_avoirkind = fin.t_avoirkind)  t_fininstrtype_
             --кол-во ценных бумаг теперь берем только с таблицы лимитов
             ,null as t_Amount
             --номинальная стоимость в единицах базового фи, за минусом всех амортизационных выплат
             ,RSI_RSB_FIInstr.FI_GetNominalOnDate(pFIID =>  FIN.T_FIID
                                                 ,pDate => g_dt_begin) t_facevalue
             --НКД на одну бумагу
             ,RSI_RSB_FIInstr.CalcNKD_Ex_NoRound(FIID => FIN.T_FIID
                                                ,CalcDate => g_dt_begin
                                                ,Amount => 1
                                                ,LastDate =>  1
                                                ,CorrectDate => 0) t_NKD
              --получить активную ID котировку
             ,RSB_BASESUM.GetActiveRateId(FIN.T_FIID, g_dt_begin) t_RateID
             --является ли облигация амортизируемая
             ,case when RSB_FIInstr.FI_AvrKindsGetRoot(fin.t_FI_Kind, fin.t_AvoirKind) = RSI_RSB_FIInstr.AVOIRKIND_BOND and
                          (select count(1) from dual
                           where exists (select null
                            from dfiwarnts_dbt fiw
                           where fiw.t_fiid = fin.T_FIID
                             and fiw.t_ispartial = chr(88))) > 0
                   then 1
              end is_partial
             ,dl.t_dlcontrid
       FROM      
         (SELECT PMWRTCL.t_Fiid,
                 PMWRTCL.t_Contract,
                 PMWRTCL.t_Party
          FROM DPMWRTCL_DBT PMWRTCL
         WHERE PMWRTCL.T_BEGDATE <= g_dt_begin
           AND PMWRTCL.T_ENDDATE >= g_dt_begin
           AND PMWRTCL.T_AMOUNT <> 0
        UNION          
         SELECT tk.t_pfi as t_Fiid,
                tk.t_clientcontrid as t_Contract,
                tk.t_clientid  as t_Party        
         FROM ddl_tick_dbt tk
         WHERE tk.t_dealdate = g_dt_begin) T
        JOIN DDLCONTRMP_DBT DL on DL.t_sfcontrid = T.t_Contract                  
        JOIN DFININSTR_DBT FIN on FIN.T_FIID = T.t_Fiid
        JOIN DAVOIRISS_DBT AVO on AVO.T_FIID = FIN.T_FIID
         WHERE FIN.t_Fi_Kind = RSI_RSB_FIInstr.FIKIND_AVOIRISS
           AND RSB_FIInstr.FI_AvrKindsGetRoot(fin.t_FI_Kind, fin.t_AvoirKind) in (RSI_RSB_FIInstr.AVOIRKIND_DEPOSITORY_RECEIPT,
                                                                                  RSI_RSB_FIInstr.AVOIRKIND_INVESTMENT_SHARE,
                                                                                  RSI_RSB_FIInstr.AVOIRKIND_BOND,
                                                                                  RSI_RSB_FIInstr.AVOIRKIND_SHARE)
           AND T.t_Party in (select party.t_partyid  from dparty_dbt party where party.t_legalform = 2)),
     qq as (
         select
            --код инструмента
            q.T_FIID,
            --наименование
            q.t_Name,
            --клиент
            q.t_Party,
            --тип инструмента
             t_fininstrtype_   t_fininstrtype,
             --гос. номер
             q.t_isin,
             q.t_Contract,
             q.t_AvrKind,
             --кол-во ЦБ
             q.t_Amount,
             q.t_facevalue,
             q.t_NKD,
             q.t_RateID,
             q.face_fi,
             --валюа цб
            NVL((SELECT finface.t_ccy
                 FROM dfininstr_dbt finface
                WHERE finface.t_fiid = NVL(ratedef.t_FIID, -1)), q.face_fi) t_ccy,
             --интрумент для котировки
            NVL(ratedef.t_FIID, -1) t_RateFIID,
            --текущий курс котировки
            Rsb_SPRepFun.GetCourse(NVL(ratedef.t_RateID, 0), g_dt_begin) t_Rate,
            CASE
               WHEN NVL(ratedef.t_SinceDate, to_date('31.12.9999','dd.mm.yyyy')) <= g_dt_begin
                  THEN ratedef.t_SinceDate
                  ELSE NVL((SELECT MAX(hist.t_SinceDate)
                            FROM dratehist_dbt hist
                           WHERE hist.t_RateID = ratedef.t_RateID
                             AND hist.t_SinceDate <= g_dt_begin),
                         to_date('01.01.0001','dd.mm.yyyy'))
            END t_RateDate,
            --курс для валюты котировки
            NVL(RSI_RSB_FIInstr.ConvSum(1, NVL(ratedef.t_FIID, -1),RSI_RSB_FIInstr.NATCUR, g_dt_begin), 0) t_RateCB,
            --текущий курс для валюты инструмента
            NVL(RSI_RSB_FIInstr.ConvSum(1, NVL(q.t_facevaluefi, -1),RSI_RSB_FIInstr.NATCUR, g_dt_begin), 0) t_RateCB_ccy,
            --является ли облигация амортизируемой
            q.is_partial,
            q.t_dlcontrid
       from q left join dratedef_dbt ratedef on q.t_RateID = ratedef.t_rateid)

      select qq.t_Party
            ,sfcontr.t_contrnumber
            ,sfcontr.t_contrdatebegin
            ,sfcontr.t_clientname
            ,sfcontr.t_clientcode
            ,sfcontr.t_sfcontrid
            ,qq.t_isin
            ,qq.t_fininstrtype
            ,qq.T_FIID
            ,qq.t_AvrKind
            ,limsec.t_open_balance
            ,qq.t_NKD
            ,qq.t_Rate
            ,qq.t_RateID
            ,qq.t_RateCB
            ,qq.t_ccy
            ,qq.t_facevalue
            --стоимость фин инструмента с округлением
            ,CASE WHEN t_RateID = -1 THEN 0
                  WHEN t_AvrKind = RSI_RSB_FIInstr.AVOIRKIND_BOND 
                    THEN limsec.t_open_balance * (t_Rate + t_NKD)
                  ELSE limsec.t_open_balance * t_Rate
             END as t_Cost
            --стоимость фин инструмента с округлением в рублях
            ,CASE WHEN t_RateID = -1 THEN 0
                  WHEN t_AvrKind = RSI_RSB_FIInstr.AVOIRKIND_BOND THEN limsec.t_open_balance * (t_Rate + t_NKD)
                  ELSE limsec.t_open_balance * t_Rate
             END * t_RateCB as t_CostRub
             ,qq.is_partial
             ,sfcontr.t_profitaccount
             ,sfcontr.t_iis
             ,sfcontr.t_dlcontrid
      from qq left join sfcontr on qq.t_Party = sfcontr.t_partyid 
                               and qq.t_dlcontrid = sfcontr.t_dlcontrid
              inner join limsec on limsec.t_client = qq.t_Party
                               and (limsec.t_security = qq.t_fiid or limsec.t_seccode = qq.t_isin)
                               and limsec.t_client_code = sfcontr.t_clientcode          
          where sfcontr.t_contrnumber is not null;


 p_out_result:= 0;
 p_out_err_msg:= '';


 exception
  when others
    then
       p_out_result:= 1;
       p_out_err_msg:= get_text_error(SQLCODE,SQLERRM  || chr (10) || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE());
       log_register_event(p_status => p_out_result
                         ,p_message => p_out_err_msg);
 end;

 procedure load_bufferTable_2(p_out_err_msg out        varchar2
                             ,p_out_result  out        number)
 is
 begin

     insert into dclportsofr2dwh_tmp (t_partyid
                                     ,t_contrnumber
                                     ,t_contrdatebegin
                                     ,t_clientname
                                     ,t_clientcode
                                     ,t_dsfcontrid
                                     ,t_fininstr
                                     ,t_fininstrtype
                                     ,t_fiid
                                     ,t_avrkind
                                     ,t_qty
                                     ,t_nkd
                                     ,t_price
                                     ,t_rateid
                                     ,t_ratecb
                                     ,t_fininstrccy
                                     ,t_facevalue
                                     ,t_open_balance
                                     ,t_open_balance_rub
                                     ,t_profitaccount
                                     ,t_iis
                                     ,t_dlcontrid)

      WITH dt as (select g_dt_begin as dt_begin from dual)  
    ,sfcontr as (
      select dt_begin --Даты которые задаем в отчете
          ,dlcontr.t_dlcontrid t_dlcontrid --идентификатор  ДБО
          ,sfcontr.t_id t_sfcontrid --идентификатор договора 
          ,sfcontr.t_partyid t_partyid --плательщик 
          ,sfcontr.t_number t_contrnumber --номер договора 
          ,sfcontr.t_datebegin t_contrdatebegin --дата начала 
          ,nvl(party.t_name,chr(1)) t_clientname  --полное наименование субьекта экономики
          ,nvl((select t_code 
                  from ddlobjcode_dbt dlobj--код объекта
                 where dlobj.t_objecttype = 207  --идентификатор типа объекта 
                   and dlobj.t_codekind = 1 
                   and dlobj.t_objectid =  dlcontr.t_dlcontrid --связка с площадкой ДБО
                   and (dlobj.t_bankclosedate >=  dt_begin or dlobj.T_BANKCLOSEDATE = to_date('01010001', 'ddmmyyyy'))
                   ),chr(1)) t_clientcode --код объекта 
         --Признак выплаты купона на текущий/брокерский счет
          ,atcor.t_general  t_profitaccount  
          ,case 
             when dlcontr.t_iis = chr(88) 
               then 1
               else 0
           end  t_iis                                                        
         from dsfcontr_dbt sfcontr --   Договор обслуживания 
         join (select dt_begin from dt) on 1 = 1 
              and sfcontr.t_partyid != nvl(RsbSessionData.OurBank,1)  -- плательщик не равен 1         
         join ddlcontr_dbt dlcontr on dlcontr.t_sfcontrid = sfcontr.t_id  --Договор брокерского обслуживания          
         left join dparty_dbt party on party.t_partyid = sfcontr.t_partyid   --Субьект экономики  
         left join dobjatcor_dbt atcor  on  atcor.t_object = LPAD (sfcontr.t_id, 34, 0)  
                                        and atcor.t_groupid=123 
                                        and atcor.t_objecttype=207 
                                        and atcor.t_validfromdate >= dt_begin
                                        and atcor.t_validtodate <= dt_begin                 
        where (sfcontr.t_dateclose = to_date('01.01.0001','dd.mm.yyyy') or  sfcontr.t_dateclose >= dt_begin)  
          and party.t_legalform = 2
          and sfcontr.t_servkind = 0
          and sfcontr.t_servkindsub = 0
          )
    ,limcash AS (
        SELECT F.t_limit_kind
              ,F.t_client
              ,F.t_client_code
              ,F.t_internalaccount
              ,F.t_open_balance
         FROM (
        SELECT row_number() OVER (PARTITION BY limcash.t_client,limcash.t_client_code, limcash.t_internalaccount ORDER BY limcash.t_limit_kind DESC) row_order
              ,limcash.t_limit_kind
              ,limcash.t_client
              ,limcash.t_client_code
              ,limcash.t_internalaccount
              ,limcash.t_open_balance
        FROM DDL_LIMITCASHSTOCK_DBT limcash   
       WHERE limcash.t_limit_kind in (0,1,2,365)
         AND limcash.t_open_balance <> 0
         AND limcash.t_date = (select max(C.t_date) from DDL_LIMITCASHSTOCK_DBT C)) F
       WHERE row_order = 1)       
    ,q as 
    (SELECT   'Валюта' t_fininstrtype, 
               t_ccy,
               t_AccountID,
               t_Code_Currency t_fiid, 
               t_Rate t_RateCB, 
               t_Rest t_open_balance,
               t_PartyID,
               iis,
               t_dlcontrid      
          FROM (SELECT t_AccountID,
                       t_Code_Currency,
                       RSB_Account.restac(t_Account, t_Code_Currency, g_dt_begin, t_Chapter, null) t_Rest,
                       NVL(RSI_RSB_FIInstr.ConvSum(1, t_Code_Currency, RSI_RSB_FIInstr.NATCUR, g_dt_begin), 0) t_Rate,
                       t_ccy,
                       t_PartyID,
                       iis,
                       t_dlcontrid
                  FROM (SELECT DISTINCT acc.t_AccountID, acc.t_Account, acc.t_Chapter, acc.t_Code_Currency, sf.t_PartyID, fin.t_ccy,
                           (select count(*)
                                       from ddlcontr_dbt dlcontr 
                                      where dlcontr.t_dlcontrid = mp.t_dlcontrid
                                       and dlcontr.t_iis = chr(88)) iis
                             ,mp.t_dlcontrid
                          FROM ddlcontrmp_dbt mp, dsfcontr_dbt sf, dmccateg_dbt cat, dmcaccdoc_dbt mc, daccount_dbt acc,DFININSTR_DBT FIN 
                         WHERE mc.t_CatID = cat.t_ID
                           AND mc.t_Owner = sf.t_PartyID
                           AND mc.t_ClientContrID = sf.t_ID
                           AND acc.t_Account = mc.t_Account
                           AND acc.t_Chapter = mc.t_Chapter
                           AND acc.t_Code_Currency = mc.t_Currency
                           AND acc.t_Open_Date <= g_dt_begin
                           AND (acc.t_Close_Date = TO_DATE('01.01.0001','DD.MM.YYYY') OR acc.t_Close_Date >= g_dt_begin)
                           AND cat.t_LevelType = 1
                           AND FIN.T_FIID = acc.t_code_currency 
                           AND cat.t_Code in ('ДС клиента, ц/б')
                           AND sf.t_ID = mp.t_SfContrID
                           )))        
      select DISTINCT
             sfcontr.t_partyid
            ,sfcontr.t_contrnumber
            ,sfcontr.t_contrdatebegin
            ,sfcontr.t_clientname
            ,sfcontr.t_clientcode
            ,sfcontr.t_sfcontrid
            ,q.t_ccy
            ,q.t_fininstrtype
            ,q.T_FIID
            ,0
            ,SUM(nvl(limcash.t_open_balance,q.t_open_balance)) OVER (PARTITION BY sfcontr.t_partyid,sfcontr.t_dlcontrid,q.T_FIID) t_qty --Сюда можно также класть t_open_balance, тк по ТЗ это одно и тоже
            ,null --нкд 
            ,null --null ,тк рыночная цена котировка в валюте наверно не должна указываться
            ,0 --t_rateid
            ,q.t_RateCB
            ,q.t_ccy 
            ,null  --t_facevalue, номинала нет
            ,SUM(nvl(limcash.t_open_balance,q.t_open_balance)) OVER (PARTITION BY sfcontr.t_partyid,sfcontr.t_dlcontrid,q.T_FIID) t_open_balance
            ,SUM(nvl(limcash.t_open_balance,q.t_open_balance) * t_RateCB) OVER (PARTITION BY sfcontr.t_partyid,sfcontr.t_dlcontrid,q.T_FIID) t_open_balance_rub
            ,sfcontr.t_profitaccount
            ,sfcontr.t_iis
            ,sfcontr.t_dlcontrid
         from q inner join sfcontr on q.t_PartyID = sfcontr.t_partyid AND q.t_dlcontrid = sfcontr.t_dlcontrid
                 left join limcash on limcash.t_client = sfcontr.t_partyid
                                  AND limcash.t_internalaccount = q.t_AccountID
                                  AND limcash.t_client_code = sfcontr.t_clientcode
        where sfcontr.t_contrnumber is not null;
                                  

 p_out_result:= 0;
 p_out_err_msg:= '';


 exception
  when others
    then
       p_out_result:= 1;
       p_out_err_msg:= get_text_error(SQLCODE,SQLERRM  || chr (10) || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE());
       log_register_event(p_status => p_out_result
                         ,p_message => p_out_err_msg);
 end;


  procedure load_bufferTable_3(p_out_err_msg out        varchar2
                              ,p_out_result  out        number)
 is
 begin

 /*
15 t_inputcash  NUMBER(32,12)  Input_ДС в руб. экв.
Стоимость зачисленных денежных средств за отчетную дату в рублевом эквиваленте.
Стоимость зачисления = кол-во ДС * курс ЦБ РФ на дату отчета

dnptxop_dbt.t_outsum,   где  Dockind=4607 (зачисление и списание)
и подвид t_subkind_operation = 10 зачисление / 20 списание  для соответствующих субдоговоров dnptxop_dbt.t_contract либо клиентов t_client
*/

   
  MERGE INTO dclportsofr2dwh_tmp D
     USING (SELECT NVL(SUM(RSI_RSB_FIInstr.ConvSum(nptxop.t_outsum,nptxop.t_currency, RSI_RSB_FIInstr.NATCUR,nptxop.T_OPERDATE,1)),0) t_inputcash
                   ,clp.t_partyid
                   ,clp.t_fiid 
                   ,clp.t_dlcontrid
             FROM dnptxop_dbt nptxop 
                 JOIN ddlcontrmp_dbt DL ON dl.t_sfcontrid = nptxop.t_Contract
                 JOIN dclportsofr2dwh_tmp clp ON nptxop.t_client = clp.t_partyid 
                                              AND nptxop.t_currency = clp.t_fiid 
                                              AND dl.t_dlcontrid = clp.t_dlcontrid
                                              AND clp.t_avrkind = 0
            WHERE nptxop.t_dockind = 4607
              AND nptxop.t_subkind_operation in (10 /*Зачисление*/)
              AND nptxop.t_status = 2
              AND nptxop.t_operdate = g_dt_begin
             GROUP BY  clp.t_partyid
                      ,clp.t_fiid 
                      ,clp.t_dlcontrid) Q
      ON (D.t_partyid   = Q.t_partyid   AND
          D.t_fiid      = Q.t_fiid      AND 
          D.t_dlcontrid = Q.t_dlcontrid AND
          D.t_avrkind   = 0)
    WHEN 
      MATCHED THEN 
      UPDATE SET D.t_inputcash = Q.t_inputcash;  

 p_out_result:= 0;
 p_out_err_msg:= '';


 exception
  when others
    then
       p_out_result:= 1;
       p_out_err_msg:= get_text_error(SQLCODE,SQLERRM || chr (10) || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE());
       log_register_event(p_status => p_out_result
                         ,p_message => p_out_err_msg);
 end;



  procedure load_bufferTable_4(p_out_err_msg out        varchar2
                              ,p_out_result  out        number)
 is
 begin

 /*
17  t_otputcash  NUMBER(32,12)  Output_ДС в руб. экв.
Стоимость списанных/выведенных денежных средств за отчетную дату в рублевом эквиваленте.
Стоимость зачисления = кол-во ДС * курс ЦБ РФ на дату отчета

dnptxop_dbt.t_outsum,   где  Dockind=4607 (зачисление и списание)  и подвид t_subkind_operation = 10 зачисление / 20 списание
для соответствующих субдоговоров dnptxop_dbt.t_contract либо клиентов t_client
*/

     MERGE INTO dclportsofr2dwh_tmp D
     USING (SELECT NVL(SUM(RSI_RSB_FIInstr.ConvSum(nptxop.t_outsum,nptxop.t_currency,RSI_RSB_FIInstr.NATCUR,nptxop.T_OPERDATE,1)),0) t_outputcash
                   ,clp.t_partyid
                   ,clp.t_fiid 
                   ,clp.t_dlcontrid
             FROM dnptxop_dbt nptxop 
                 JOIN ddlcontrmp_dbt DL ON dl.t_sfcontrid = nptxop.t_Contract
                 JOIN dclportsofr2dwh_tmp clp ON nptxop.t_client = clp.t_partyid 
                                              AND nptxop.t_currency = clp.t_fiid 
                                              AND dl.t_dlcontrid = clp.t_dlcontrid
                                              AND clp.t_avrkind = 0
            WHERE nptxop.t_dockind = 4607
              AND nptxop.t_subkind_operation in (20 /*Списание*/)
              AND nptxop.t_status = 2
              AND nptxop.t_operdate = g_dt_begin
             GROUP BY  clp.t_partyid
                      ,clp.t_fiid 
                      ,clp.t_dlcontrid) Q
      ON (D.t_partyid   = Q.t_partyid AND
          D.t_fiid      = Q.t_fiid AND 
          D.t_dlcontrid = Q.t_dlcontrid AND
          D.t_avrkind   = 0)
    WHEN 
      MATCHED THEN 
      UPDATE SET D.t_outputcash = Q.t_outputcash;
     

 p_out_result:= 0;
 p_out_err_msg:= '';


 exception
  when others
    then
       p_out_result:= 1;
       p_out_err_msg:= get_text_error(SQLCODE,SQLERRM  || chr (10) || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE());
       log_register_event(p_status => p_out_result
                         ,p_message => p_out_err_msg);
 end;



  procedure load_bufferTable_5(p_out_err_msg out        varchar2
                              ,p_out_result  out        number)
 is
 begin

 /*
16 t_inputsec  NUMBER(32,12)  Input_ЦБ в руб. экв.

Стоимость зачисленных ценных бумаг за отчетную дату в рублевом эквиваленте.
Стоимость зачисления = кол-во ЦБ * на котировку рыночная цена номер 3 на дату отчета * курс ЦБ РФ на дату отчета

Биржевые котировки Рыночная цена номер 3 или Market price 3  хранятся в таблице dratedef_dbt с  в записях с dratedef_dbt.t_type=1,
т.е. справочнике СОФРа Виды курсов  курсов финансовых инструментов   - Рыночная цена с номером 1

кол-во ЦБ из поля  ddl_leg_dbt.principal  для сделок ddl_tick_dbt.t_dealtype операций с номером  2011   (2011 для зачислений)
Связка таблиц ddl_tick_dbt (сделки с цб/векселями/мбк) и  ddl_leg_dbt (условия сделки)    через ddl_tick_dbt.t_dealid=ddl_leg_dbt.t_dealid
*/

   MERGE INTO dclportsofr2dwh_tmp D
   USING (SELECT  SUM(leg.T_PRINCIPAL * clp.t_price * clp.t_ratecb) t_inputsec
                 ,clp.t_partyid
                 ,clp.t_fiid 
                 ,clp.t_dlcontrid
           from ddl_tick_dbt TICK
            JOIN DDL_LEG_DBT LEG         ON LEG.T_DEALID = TICK.T_DEALID
            JOIN ddlcontrmp_dbt DL       ON dl.t_sfcontrid = tick.t_clientcontrid
            JOIN ddlcontr_dbt dlcontr    ON dlcontr.t_dlcontrid = DL.t_dlcontrid
            JOIN dclportsofr2dwh_tmp clp ON TICK.T_CLIENTID = clp.t_partyid 
                                         AND tick.t_pfi = clp.t_fiid 
                                         AND dl.t_dlcontrid = clp.t_dlcontrid
          WHERE TICK.t_dealtype = 2011
            AND TICK.T_DEALDATE = g_dt_begin
            AND clp.t_avrkind <> 0
           group by  clp.t_partyid
                    ,clp.t_fiid 
                    ,clp.t_dlcontrid) Q
    ON (D.t_partyid   = Q.t_partyid AND
        D.t_fiid      = Q.t_fiid AND 
        D.t_dlcontrid = Q.t_dlcontrid AND
        D.t_avrkind   <> 0)
  WHEN 
    MATCHED THEN 
    UPDATE SET D.t_inputsec = Q.t_inputsec;   

 p_out_result:= 0;
 p_out_err_msg:= '';


 exception
  when others
    then
       p_out_result:= 1;
       p_out_err_msg:= get_text_error(SQLCODE,SQLERRM  || chr (10) || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE());
       log_register_event(p_status => p_out_result
                         ,p_message => p_out_err_msg);
 end;



  procedure load_bufferTable_6(p_out_err_msg out        varchar2
                              ,p_out_result  out        number)
 is
 begin

 /*
18 t_outputsec  NUMBER(32,12)  Output_ЦБ в руб. экв.
Стоимость списанных/выведенных ценных бумаг за отчетную дату в рублевом эквиваленте.
Стоимость зачисления = кол-во ЦБ * на котировку "рыночная цена номер 3" на дату отчета * курс ЦБ РФ на дату отчета

кол-во ЦБ ddl_leg_dbt.principal  для сделок ddl_tick_dbt.t_dealtype операций с номером  2010 (2010 для списаний)
Связка таблиц ddl_tick_dbt (сделки с цб/векселями/мбк) и  ddl_leg_dbt (условия сделки)    через ddl_tick_dbt.t_dealid=ddl_leg_dbt.t_dealid
*/
  
     MERGE INTO dclportsofr2dwh_tmp D
   USING (SELECT  SUM(leg.T_PRINCIPAL * clp.t_price * clp.t_ratecb) t_outputsec
                 ,clp.t_partyid
                 ,clp.t_fiid 
                 ,clp.t_dlcontrid
           from ddl_tick_dbt TICK
            JOIN DDL_LEG_DBT LEG         ON LEG.T_DEALID = TICK.T_DEALID
            JOIN ddlcontrmp_dbt DL       ON dl.t_sfcontrid = tick.t_clientcontrid
            JOIN ddlcontr_dbt dlcontr    ON dlcontr.t_dlcontrid = DL.t_dlcontrid
            JOIN dclportsofr2dwh_tmp clp ON TICK.T_CLIENTID = clp.t_partyid 
                                         AND tick.t_pfi = clp.t_fiid 
                                         AND dl.t_dlcontrid = clp.t_dlcontrid
          WHERE TICK.t_dealtype = 2010
            AND TICK.T_DEALDATE = g_dt_begin
            AND clp.t_avrkind <> 0
           group by  clp.t_partyid
                    ,clp.t_fiid 
                    ,clp.t_dlcontrid) Q
    ON (D.t_partyid   = Q.t_partyid AND
        D.t_fiid      = Q.t_fiid AND 
        D.t_dlcontrid = Q.t_dlcontrid AND
        D.t_avrkind   <> 0)
  WHEN 
    MATCHED THEN 
      UPDATE SET D.t_outputsec = Q.t_outputsec; 

 p_out_result:= 0;
 p_out_err_msg:= '';


 exception
  when others
    then
       p_out_result:= 1;
       p_out_err_msg:= get_text_error(SQLCODE,SQLERRM  || chr (10) || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE());
       log_register_event(p_status => p_out_result
                         ,p_message => p_out_err_msg);
 end;



 procedure load_bufferTable_7(p_out_err_msg out        varchar2
                              ,p_out_result  out        number)
 is
 begin

 /*
19  t_redemption  NUMBER(32,12) Погашение (вкл. Оферту) в руб. экв.
Сумма погашения номинала облигаций (вкл. оферту) в рублевом эквиваленте на отчетную дату по курсу ЦБ РФ на дату отчета.
Признак фактического погашения davoiriss_dbt.t_spisclosed=X в дату dfininstr_dbt.t_drwingdate
*/
 
 MERGE INTO dclportsofr2dwh_tmp D
  USING(
  --выплата на дату, умноженный на кол-во и курс
   SELECT fin.t_facevalue * clp.t_qty * clp.t_ratecb as t_redemption
         ,clp.t_partyid
         ,clp.t_dlcontrid
         ,clp.t_fiid
      FROM  dfininstr_dbt fin 
       JOIN dclportsofr2dwh_tmp clp ON fin.t_fiid = clp.t_fiid   
       JOIN davoiriss_dbt avo ON AVO.t_fiid = clp.t_fiid                                           
      WHERE fin.t_fi_kind = RSI_RSB_FIInstr.FIKIND_AVOIRISS
       AND clp.t_is_partial is null
       AND avo.t_spisclosed = chr(88)
       AND fin.t_drawingdate = g_dt_begin) Q
    ON (D.t_partyid   = Q.t_partyid and
        D.t_dlcontrid = Q.t_dlcontrid and
        D.t_fiid      = Q.t_fiid and 
        D.t_avrkind   = RSI_RSB_FIInstr.AVOIRKIND_BOND)
  WHEN 
    MATCHED THEN 
      UPDATE SET D.t_redemption = Q.t_redemption;   

 /*
20  t_amortization  NUMBER(32,12) Амортизация в руб. экв.
Сумма амортизация за дату отчета по амортизируемым облигациям в рублевом эквиваленте по курсу ЦБ РФ на дату отчета.
Амортизация из поля dfiwarnts_dbt.t_incomevolume , где dfiwarnts_dbt.t_ispartial=X

Сумма амортизации=qty*(Начальный номинал -Текущий номинал)
*/

  
  MERGE INTO dclportsofr2dwh_tmp D
  USING(
  --выплата на дату, умноженный на кол-во и курс
   SELECT SUM(
          CASE 
             WHEN fw.t_RelativeIncome = CHR(0) 
              THEN fw.t_IncomeVolume
              ELSE ROUND( fin.t_facevalue * fw.t_IncomeRate / GREATEST(1, fw.t_IncomeScale) / 100, fw.t_IncomePoint)
          END * clp.t_qty * clp.t_ratecb) as t_amortization
         ,clp.t_partyid
         ,clp.t_dlcontrid
         ,clp.t_fiid
      FROM DFIWARNTS_DBT fw 
       JOIN dfininstr_dbt fin on fw.t_fiid = fin.t_fiid
       JOIN dclportsofr2dwh_tmp clp ON fin.t_fiid = clp.t_fiid                                                
      WHERE fin.t_fi_kind = RSI_RSB_FIInstr.FIKIND_AVOIRISS
      AND clp.t_is_partial = 1
       AND fw.t_IsPartial = CHR(88)
       AND fw.t_DrawingDate = g_dt_begin
      GROUP BY clp.t_partyid
              ,clp.t_dlcontrid
              ,clp.t_fiid) Q
    ON (D.t_partyid   = Q.t_partyid and
        D.t_dlcontrid = Q.t_dlcontrid and 
        D.t_fiid      = Q.t_fiid and 
        D.t_is_partial = 1)
  WHEN 
    MATCHED THEN 
      UPDATE SET D.t_amortization = Q.t_amortization;

 /* 21 t_div  NUMBER(32,12)  Дивиденды в руб. экв.
 Сумма дивидендов по акциям за дату отчета в рублевом эквиваленте по курсу ЦБ РФ на дату отчета. */

   MERGE INTO dclportsofr2dwh_tmp D
   USING(
       --в один день может быть несколько записей по бумаге
       --берем сумму записей
    SELECT SUM(cdr.t_clientsum * NVL(RSI_RSB_FIInstr.ConvSum(1, nvl(fin.t_fiid,-1),RSI_RSB_FIInstr.NATCUR, g_dt_begin), 0)) t_div
          ,clp.t_contrnumber
          ,clp.t_fininstr
      FROM dcdrecords_dbt cdr 
         LEFT JOIN dfininstr_dbt fin ON cdr.t_clientcurrency = fin.t_iso_number 
                                    AND fin.t_fi_kind = 1
              JOIN dclportsofr2dwh_tmp clp ON cdr.t_agreementnumber = clp.t_contrnumber
                                            --по коду исин сравниваем  
                                           AND cdr.t_isinregistrationnumber = clp.t_fininstr                       
          WHERE cdr.t_corporateactiontype = 'DVCA'
            --статус актив
            AND cdr.t_operationstatus = 'активна'
            --на дату отчета
            AND cdr.t_paymentdate = g_dt_begin
            --только для акций
            AND clp.t_avrkind = RSI_RSB_FIInstr.AVOIRKIND_SHARE
           GROUP BY clp.t_contrnumber
                   ,clp.t_fininstr) Q
    ON (D.t_fininstr    = Q.t_fininstr and
        D.t_contrnumber = Q.t_contrnumber and 
        D.t_avrkind   = RSI_RSB_FIInstr.AVOIRKIND_SHARE)
  WHEN 
    MATCHED THEN 
      UPDATE SET D.t_div = Q.t_div;


 /*
23  t_coupon  NUMBER(32,12) Полученный купон в руб. экв.
Сумма купонов по облигациям за дату отчета в рублевом эквиваленте по курсу ЦБ РФ на дату отчета
*/


  MERGE INTO dclportsofr2dwh_tmp D
   USING (SELECT RSI_RSB_FIINSTR.CalcNKD_Ex(FIID     => CL.t_fiid
                                           ,CalcDate => g_dt_begin
                                           ,Amount   => CL.t_qty
                                           ,LastDate => 1) * CL.t_ratecb as t_coupon
                                           
               ,CL.t_partyid
               ,CL.t_fiid
               ,CL.t_dlcontrid
  FROM dclportsofr2dwh_tmp CL LEFT JOIN dfiwarnts_dbt FIW ON CL.t_fiid = FIW.t_fiid                            
 WHERE CL.t_avrkind = RSI_RSB_FIInstr.AVOIRKIND_BOND
  AND FIW.t_drawingdate =  g_dt_begin
  AND FIW.t_ispartial = chr(0)) Q
    ON (D.t_partyid   = Q.t_partyid AND
        D.t_fiid      = Q.t_fiid AND 
        D.t_dlcontrid = Q.t_dlcontrid AND
        D.t_avrkind   = RSI_RSB_FIInstr.AVOIRKIND_BOND)
  WHEN 
    MATCHED THEN 
      UPDATE SET D.t_coupon = Q.t_coupon; 


 p_out_result:= 0;
 p_out_err_msg:= '';


 exception
  when others
    then
       p_out_result:= 1;
       p_out_err_msg:= get_text_error(SQLCODE,SQLERRM  || chr (10) || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE());
       log_register_event(p_status => p_out_result
                         ,p_message => p_out_err_msg);
 end;



 procedure load_bufferTable_8(p_out_err_msg out        varchar2
                              ,p_out_result  out        number)
 is
 begin


 insert into /*+ append*/  dclportsofr2dwh_dbt
                          (t_reportdate,
                           t_contractnumber,
                           t_contrdatebegin,
                           t_clientname,
                           t_clientcode,
                           t_fininstr,
                           t_fininstrtype,
                           t_qty,
                           t_nkd,
                           t_price,
                           t_fininstrccy,
                           t_facevalue,
                           t_open_balance,
                           t_open_balance_rub,
                           t_inputcash,
                           t_inputsec,
                           t_otputcash,
                           t_outputsec,
                           t_redemption,
                           t_amortization,
                           t_div,
                           t_profitaccount,
                           t_coupon,
                           t_uploadtime,
                           t_partyid)

  select g_dt_begin
        ,t.t_contrnumber
        ,t.t_contrdatebegin
        ,t.t_clientname
        ,t.t_clientcode
        ,t.t_fininstr
        ,t.t_fininstrtype
        ,t.t_qty
        ,t.t_nkd
        ,t.t_price
        ,t.t_fininstrccy
        ,t.t_facevalue
        ,t.t_open_balance
        ,t.t_open_balance_rub
        ,t.t_inputcash
        ,t.t_inputsec
        ,t.t_outputcash
        ,t.t_outputsec
        ,t.t_redemption
        ,t.t_amortization
        ,t.t_div
        ,t.t_profitaccount
        ,t.t_coupon
        ,systimestamp
        ,t.t_partyid
  from dclportsofr2dwh_tmp t;

  commit;

 p_out_result:= 0;
 p_out_err_msg:= '';

  rsb_dclportsofr2dwh.log_register_event(p_status => p_out_result
                                    ,p_message => p_out_err_msg);

 exception
  when others
    then
       Rollback;
       p_out_result:= 1;
       p_out_err_msg:= get_text_error(SQLCODE,SQLERRM  || chr (10) || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE());
       log_register_event(p_status => p_out_result
                         ,p_message => p_out_err_msg);
 end;

 function get_text_error(p_sql_code      number
                        ,p_error_message varchar2) return varchar2

  is
   v_text_message varchar2(4000);
  begin

    v_text_message:= 'Error code '||p_sql_code||'. '|| substr(p_error_message,0, 300);
    return v_text_message;

  end;

  procedure log_register_event(p_status number
                              ,p_message varchar2 default null) --0 - Успешно, 1 - Ошибка

  is
     v_ERRORTEXT varchar2(1000);
     v_ID itt_q_message_log.msgid%type;
  begin

  if(p_status = 0) then

    IT_EVENT.RegisterEvent(NULL
                         ,'SOFR'
                         , 'clportsofr2dwh'
                         ,'Процедура clportsofr2dwh.mac: успешное заполнение буферной таблицы dclportsofr2dwh_dbt аналитикой по портфелю клиента'
                         ,'<XML LevelInfo = "1"/>'
                         ,v_ERRORTEXT
                         ,v_ID);

  elsif(p_status = 1) then

    IT_EVENT.RegisterEvent(NULL
                         ,'SOFR'
                         , 'clportsofr2dwh'
                         , 'Процедура clportsofr2dwh.mac:  ошибка заполнения буферной таблицы dclportsofr2dwh_dbt аналитикой по портфелю клиента. ' || chr(10)
                            || p_message
                         ,'<XML LevelInfo = "8"/>'
                         ,v_ERRORTEXT
                         ,v_ID);
   end if;

  end;


end rsb_dclportsofr2dwh;
