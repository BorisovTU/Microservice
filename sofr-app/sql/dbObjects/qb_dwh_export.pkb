create or replace package body qb_dwh_export is

  cursor cur_All_Export_Deal (in_DocKind number, in_DocID number) is
                     -- МБК
                     select tck.t_dealid dealID,         -- ИД сделки
                            tck.t_bofficekind DocKind,   -- Вид Сделки
                            tck.t_dealcode Deal_Code,    -- Код Сделки
                            tck.t_dealid || '#' || 'TCK' DWH_Deal_Code, -- Код сделки DWH
                            tck.t_department Department, -- Филиал
                            tck.t_flag1 isProl           -- признак пролонгации
                       from ddl_tick_dbt tck
                      where tck.t_dealstatus != 0
                            and tck.t_bofficekind in( 102, 208 )
                      --and tck.t_dealid = 121938

                        and (tck.t_bofficekind = in_DocKind or in_DocKind = 0 )
                        and (tck.t_dealid = in_DocID or in_DocID = 0)

                     -- Ген. соглашения
                     union all
                     select genagr.t_genagrid dealID,
                            genagr.t_dockind DocKind,
                            genagr.t_code Deal_Code,
                            genagr.t_genagrid || '#' || 'GEN' DWH_Deal_Code,
                            genagr.t_department Department,
                            chr(0) isprol
                       from ddl_genagr_dbt genagr
                      where genagr.t_dockind = 4611
                        and (genagr.t_dockind = in_DocKind or in_DocKind = 0 )
                        and (genagr.t_genagrid = in_DocID or in_DocID = 0)
                     -- Обеспечения
                     union all
                     select dl_order.t_contractid dealID,
                            dl_order.t_dockind DocKind,
                            dl_order.t_ordernumber Deal_Code,
                            dl_order.t_contractid || '#' || 'PRV' DWH_Deal_Code,
                            dl_order.t_department Department,
                            chr(0) isprol
                       from ddl_order_dbt dl_order
                      where dl_order.t_contractstatus != 0
                        and dl_order.t_dockind = 126
                        and (dl_order.t_dockind = in_DocKind or in_DocKind = 0 )
                        and (dl_order.t_contractid = in_DocID or in_DocID = 0)
                     -- Драг. метал
                     union all
                     select t.t_id DealId,
                            t.t_Dockind Dockind,
                            t.t_code Deal_Code,
                            t.t_id || '#DVN' DWH_Deal_Code,
                            t.t_department Department,
                            chr(0) isprol
                       from ddvndeal_dbt t
                      inner join ddvnfi_dbt nFI on nFI.t_Type = 0
                                               and nFI.t_DealID = t.t_ID
                      inner join dfininstr_dbt fi on fi.t_fi_kind = 6
                                                 and fi.t_fiid = nFI.t_Fiid
                      where t.t_state != 0
                        and (t.t_Dockind = in_DocKind or in_DocKind = 0 )
                        and (t.t_id = in_DocID or in_DocID = 0)
                     -- CSA
                     union all
                     select csa.t_csaid dealID,
                            4626 DocKind,
                            csa.t_number Deal_Code,
                            csa.t_number DWH_Deal_Code,
                            1 Department,
                            chr(0) isprol
                      from ddvcsa_dbt csa
                           inner join (select p.t_csaid, p.t_sumpaycur, p.t_direction,  sum(p.t_sumpay) t_sumpay
                                         from ddvcsapm_dbt p
                                        where p.t_direction = 0
                                        group by p.t_csaid, p.t_sumpaycur, p.t_direction) csap on csap.t_csaid = csa.t_csaid
                     where exists (select 1
                                     from doproper_dbt o
                                          inner join doprdocs_dbt d on d.t_id_operation = o.t_id_operation
                                          --inner join dacctrn_dbt t on t.t_acctrnid = d.t_acctrnid and t.t_date_carry<=p_date
                                    where o.t_dockind = 4626 and o.t_documentid = lpad(csa.t_csaid,34,'0') )
                        and (4626 = in_DocKind or in_DocKind = 0 )
                        and (csa.t_csaid = in_DocID or in_DocID = 0);

  ------------------------------------------------------
  --Информация по сделке Кредитная линия/Транш/обычная сделка МБК
  ------------------------------------------------------
  cursor cur_deal(p_id number) is
    select t.t_dealid dealID,
           t.t_bofficekind DocKind,
           t.t_dealid || '#' || 'TCK' DwhDealID,
           t.t_dealdate dealdate,
           case
             when t.t_partyID > 0 then
              t.t_partyID
             else
              null
           end PartyId, -- контрагент
           t.t_department department,
           op_kind.t_kind_operation kind_operation,
           case
             when op_kind.t_kind_operation = 12310 then
              85 -- Привлечение ТФ
             when op_kind.t_kind_operation = 12335 then
              86 -- Размещение ТФ
             when t.t_bofficekind = 102 and
                  instr(op_kind.t_systypes, 'B') > 1 then
              1 --decode(t.t_typedoc, 'L',2, 'D', 1)
             when t.t_bofficekind = 102 and
                  instr(op_kind.t_systypes, 'S') > 1 then
              2
             when t.t_bofficekind = 208 and
                  instr(op_kind.t_systypes, 'B') > 1 then
              11 --decode(t.t_typedoc, 'L',2, 'D', 1)
             when t.t_bofficekind = 208 and
                  instr(op_kind.t_systypes, 'S') > 1 then
              22
           --when t.t_bofficekind = 208 then 2
           end mb_kind,
           case
             when t.t_dealcode = 'МБК-DEVEL-BANK-OF-BELAR2809201' then
              t.t_dealcode || '7'
             when t.t_dealcode = 'МБК-DEVEL-BANK-OF-BELARUS25122' then
              t.t_dealcode || '017'
             when t.t_dealcode = 'МБК-DEVEL-BANK-OF-BELARUS_2310' then
              t.t_dealcode || '2017'
             else
              case
             when t.t_dealtype = 12300 and t.t_partyid = 343 then
              t.t_dealcodets
             else
              t.t_dealcode
           end end deal_code,
           '0' Is_Interior,
           l.t_start deal_Start,
           l.t_maturity deal_End,
           t.t_comment note,
           l.t_pfi fiid,
           l.t_principal Deal_Sum,
           decode(l.t_pfi,
                  0,
                  l.t_principal,
                  rsb_fiinstr.ConvSum(l.t_principal,
                                      l.t_pfi,
                                      0,
                                      t.t_dealdate)) deal_sum_nat,
           t.t_flag1 isProl,
           case
             when t.t_brokerid > 0 then
              t.t_brokerid
             else
              null
           end brokerid, -- брокер
           case
             when t.t_clientid > 0 then
              t.t_clientid
             else
              null
           end clientid, -- Клиент
           case
             when t.t_traderid > 0 then
              t.t_partyID
             else
              null
           end traderid, -- Трейдер
           case
             when t.t_depositid > 0 then
              t.t_depositid
             else
              null
           end depositid, -- Депозитарий
           case
             when t.t_marketid > 0 then
              t.t_marketid
             else
              null
           end marketid, -- Депозитарий
           t.t_closedate closedate,
           case
             when t.t_parentid > 0 then
              t.t_parentid
             else
              null
           end parentid, -- ID Кредитной линии по которой выдан транш
           case
             when t.t_genagrid > 0 then
              t.t_genagrid
             else
              null
           end genagrid, -- ID Генерального соглашения по которой выдан транш
           t.t_debtlimit DEBTLIMIT,
           t.t_issuancelimit PAYMENTLIMIT,
           t.t_limitcur,
           case
             when t.t_debtlimit > 0 and t.t_issuancelimit > 0 then
              '3' -- 3 ? Комбинированная (лимиты по выдаче и по задолженности)
             when t.t_debtlimit > 0 and t.t_issuancelimit = 0 then
              '2' --2 ? С лимитом по задолженности
             when t.t_debtlimit = 0 and t.t_issuancelimit > 0 then
              '1' -- 1 ? С лимитом по выдаче
             else
              '0' --0 ? Не определено
           end TYPE_BY_LIMIT, --Тип кредитной линии по лимиту
           case
             when t.t_tax_amount > 0 then
              chr(88)
             else
              chr(0)
           end tax, --Признак взымания налога
           t.t_tax_amount tax_Rate, -- Сумма налога
           case
             when t.t_credit_tax_amount > 0 then
              chr(88)
             else
              chr(0)
           end credit_tax, -- Признак Комиссия за выдачу
           t.t_credit_tax_cur credit_tax_cur, -- Валюта(1-нациолнальная 2-валюта сделки) Комиссия за выдачу
           t.t_credit_tax_amount credit_tax_rate, -- Сумма Комиссия за выдачу
           t.t_credit_tax_term,
           case
             when l.t_basis = 1 then
              0 --360/30
             when l.t_basis = 2 then
              2 --360/Act
             when l.t_basis = 4 then
              1 --Act/Act
             when l.t_basis = 8 then
              40 --365/Act
             when l.t_basis = 1001 then
              null --360/31
           end Rate_Base --select * from dnamealg_dbt v where v.t_itypealg = 2317
      from ddl_tick_dbt t
     inner join ddl_leg_dbt l on l.t_dealid = t.t_dealid and l.t_legID = 1
     inner join doprkoper_dbt op_kind on op_kind.t_kind_operation =
                                         t.t_dealtype
     where t.t_dealid = p_id;

  ------------------------------------------------------
  -- Курсор по вновь заключенным сделкам(в том числе пролонгированным)
  -- Выгружаем только сделки, по которым исполнен платеж (если исходящий), либо если сквитован входящий.
  -- Грубо говоря, пока нет проводки в балансе, ассоциированной со сделкой, эта сделка не представляет никакого интереса для ЦХД.
  ------------------------------------------------------

  cursor cur_DealsForExport(p_department number, p_date date) is
  --сделки только с завершенным движением по балансу
    select t.t_dealid dealID
      from ddl_tick_dbt t
      inner join ddl_leg_dbt l on l.t_legid = 1 and l.t_dealid = t.t_dealid
     where t.t_bofficekind in (102, 208)
       and t.t_department = p_department
       and ((exists
            (select 1
                from dpmpaym_dbt p
               where p.t_paymstatus = 32000
                 and p.t_dockind in (102, 208)
                 and p.t_documentid = t.t_dealid) and t.t_bofficekind = 102) or
           (t.t_dealstatus != 0 and t.t_bofficekind = 102) or
           (t.t_bofficekind = 208 and t.t_dealstatus != 0))
       and (t.t_dealdate >= cDateMBK
            or cDateMBK <= decode(nvl(qb_dwh_utils.Get_Note_Dat(103, t.t_dealid, 102), t.t_closedate),
                                  to_date('01.01.0001','dd.mm.yyyy'),
                                  to_date('01.01.4001','dd.mm.yyyy'),
                                  nvl(qb_dwh_utils.Get_Note_Dat(103, t.t_dealid, 102), t.t_closedate)
                                  ))
      --           and t.t_dealid = 413
    --and t.t_dealdate = p_date -- УБРАТЬ КОГДА ОТТЕСТИРУЮ
    --and t.t_flag1 != chr(88) -- кроме пролонгаций
    ;

  ------------------------------------------------------
  -- Курсор по Генеральным соглашения МБК (t_dockind = 4611 Генеральное соглашение МБК)
  ------------------------------------------------------
  cursor cur_GenAgr(In_DocKind Number, In_DealId number) is
    select genagr.t_genagrid dealID,
           genagr.t_genagrid || '#' || 'GEN' DwhDealID,
           genagr.t_dockind DocKind,
           decode(genagr.t_date_genagr,
                  to_date('02.01.1900', 'dd.mm.yyyy'),
                  to_date('01.01.1980', 'dd.mm.yyyy'),
                  genagr.t_date_genagr) dealdate,
           case
             when genagr.t_partyID > 0 then
              genagr.t_partyID
             else
              null
           end PartyId, -- контрагент
           genagr.t_department department,
           30 mb_kind,
           genagr.t_code deal_code,
           '0' Is_Interior,
           decode(genagr.t_start,
                  to_date('02.01.1900', 'dd.mm.yyyy'),
                  to_date('01.01.1980', 'dd.mm.yyyy'),
                  genagr.t_start) deal_Start,
           decode(genagr.t_date_end,
                  to_date('01.01.0001', 'dd.mm.yyyy'),
                  to_date('01.01.3001', 'dd.mm.yyyy'),
                  genagr.t_date_end) deal_End,
           genagr.t_comment note,
           genagr.t_tax tax, -- Признак взымания налога
           genagr.t_tax_amount tax_Rate, -- Сумма налога
           genagr.t_credit_tax credit_tax, -- Признак Комиссия за выдачу
           genagr.t_credit_tax_cur credit_tax_cur, -- Валюта(1-нациолнальная 2-валюта сделки) Комиссия за выдачу
           genagr.t_credit_tax_amount credit_tax_rate, -- Сумма Комиссия за выдачу
           genagr.t_credit_tax_term, -- Срок от даты сделки Комиссия за выдачу
           genagr.t_control_tax control_tax, -- Признак Комиссия за управление
           genagr.t_control_tax_cur control_tax_cur, -- Валюта(1-нациолнальная 2-валюта сделки) Комиссия за управление
           genagr.t_control_tax_amount control_tax_rate, -- Комиссия за управление
           genagr.t_control_tax_term -- Срок от даты сделки Комиссия за управление
      from ddl_genagr_dbt genagr
     where genagr.t_dockind = In_DocKind
       and genagr.t_genagrid = In_DealId;

  ------------------------------------------------------
  -- Курсор по Генеральным соглашения МБК (t_dockind = 4611 Генеральное соглашение МБК)
  ------------------------------------------------------
  cursor cur_GenAgr_For_Export(p_department number default 0) is
    select genagr.t_genagrid dealID, genagr.t_dockind DocKind
      from ddl_genagr_dbt genagr
     where ((genagr.t_department = p_department) or (p_department = 0))
       and genagr.t_dockind = 4611
       and (genagr.t_start >= cDateMBK
            or cDateMBK <= decode(genagr.t_date_end,
                                  to_date('01.01.0001','dd.mm.yyyy'),
                                  to_date('01.01.4001','dd.mm.yyyy'),
                                  genagr.t_date_end
                                  ));

  ------------------------------------------------------
  -- Курсор по обеспечениям
  ------------------------------------------------------

  cursor cur_Ens(In_DocKind Number, In_DealId number) is
    select dl_order.t_contractid dealID,
           dl_order.t_dockind DocKind,
           dl_order.t_contractid || '#' || 'PRV' DwhDealID,
           dl_order.t_createdate dealdate,
           case
             when dl_order.t_contractor > 0 then
              dl_order.t_contractor
             else
              null
           end PartyId, -- контрагент
           dl_order.t_department department,
           23 mb_kind,
           dl_order.t_ordernumber deal_code,
           '0' Is_Interior,
           dl_order.t_signdate deal_Start,
           dl_order.t_expiry deal_End,
           null note, -- не предусмотрено
           dl_order.t_fiid fiid,
           dl_order.t_cost Deal_Sum,
           decode(dl_order.t_fiid,
                  0,
                  dl_order.t_cost,
                  rsb_fiinstr.ConvSum(dl_order.t_cost,
                                      dl_order.t_fiid,
                                      0,
                                      dl_order.t_createdate)) deal_sum_nat,
           null isProl, -- не предусмотрено
           dl_order.t_closingdate closedate,
           dl_order.t_systypes,
           '100' PROVISIONDEAL_TYPE_CODE,
           (select Max(s.t_qualitycategory)
              from ddl_secur_dbt s
             where s.t_contractkind = dl_order.t_dockind
               and s.t_contractid = dl_order.t_contractid) quality -- берем по самой худьшей
      from ddl_order_dbt dl_order
     where dl_order.t_dockind = In_DocKind
       and dl_order.t_contractid = In_DealId;

  ------------------------------------------------------
  -- Курсор по Договорам обеспечения подлежащим выгрузке
  ------------------------------------------------------

  cursor cur_Ens_For_Export(p_department number default 0) is
    select dl_order.t_contractid dealID,
           dl_order.t_dockind DocKind,
           dl_order.t_contractid || '#' || 'PRV' DwhDealID
      from ddl_order_dbt dl_order
     where ((dl_order.t_department = p_department) or (p_department = 0))
       and dl_order.t_contractstatus != 0
       and dl_order.t_dockind = 126
       and (dl_order.t_signdate >= cDateMBK
            or cDateMBK <= decode(dl_order.t_expiry,
                                  to_date('01.01.0001','dd.mm.yyyy'),
                                  to_date('01.01.4001','dd.mm.yyyy'),
                                  dl_order.t_expiry
                                  ));

  ------------------------------------------------------
  -- Курсор по сделкам с драгоценными металами
  ------------------------------------------------------

  cursor cur_Precious_Metals(In_DocKind Number, In_DealId number) is
    select t.t_id DealId,
           t.t_Dockind Dockind,
           t.t_date BeginDate,
           (select max(nFI0.t_Paydate)
              from ddvnfi_dbt nFI0
             where nFI0.t_DealID = t.t_ID) EndDate,
           t.t_code DocNum,
           nvl(Client.t_Partyid, Contractor.t_Partyid) Subject_Code,
           t.t_id || '#DVN' Code,
           t.t_comment note,
           27 DealType,
           t.t_department Department_Code,
           '0' Is_Interior
      from ddvndeal_dbt t
     inner join ddvnfi_dbt nFI on nFI.t_Type = 0
                              and nFI.t_DealID = t.t_ID
     inner join dfininstr_dbt fi on fi.t_fi_kind = 6
                                and fi.t_fiid = nFI.t_Fiid
      left outer join ddvnfi_dbt nFI_2 on nFI_2.t_Type = 2
                                      and nFI_2.t_DealID = t.t_ID
      left outer join dparty_dbt Agent on Agent.t_PartyID = t.t_Agent
      left outer join dsfcontr_dbt AgentContr on AgentContr.t_ID =
                                                 t.t_AgentContr
      left outer join dparty_dbt Client on Client.t_PartyID = t.t_Client
      left outer join dsfcontr_dbt ClientContr on ClientContr.t_ID =
                                                  t.t_ClientContr
      left outer join dparty_dbt Contractor on Contractor.t_PartyID =
                                               t.t_Contractor
      left outer join ddp_dep_dbt dp_dep on dp_dep.t_code = t.t_Department
      left outer join ddl_genagr_dbt GenAgr on GenAgr.t_GenAgrID =
                                               t.t_GenAgrID
      left outer join dnamealg_dbt DVKindNA on DVKindNA.t_iTypeAlg = 7010
                                           and DVKindNA.t_iNumberAlg =
                                               t.t_DVKind
      left outer join dnamealg_dbt MarketKindNA on MarketKindNA.t_iTypeAlg = 7039
                                               and MarketKindNA.t_iNumberAlg =
                                                   t.t_MarketKind
      left outer join dnamealg_dbt PeriodClsNA on PeriodClsNA.t_iTypeAlg = 7038
                                              and PeriodClsNA.t_iNumberAlg =
                                                  t.t_PeriodCls
      left outer join dnamealg_dbt StateNA on StateNA.t_iTypeAlg = 7009
                                          and StateNA.t_iNumberAlg =
                                              t.t_State
      left outer join dnamealg_dbt TypeNA on TypeNA.t_iTypeAlg = 7004
                                         and TypeNA.t_iNumberAlg = t.t_Type
     WHERE t.t_state != 0
       and t.t_dockind = In_DocKind
       and t.t_id = in_DealId;

  ------------------------------------------------------
  -- Курсор по сделкам с драгоценными металами для выгрузки
  ------------------------------------------------------

  cursor cur_Precious_Metals_For_Export(p_department number default 0) is
    select t.t_id DealId, t.t_Dockind Dockind
      from ddvndeal_dbt t
     inner join ddvnfi_dbt nFI on nFI.t_Type = 0
                              and nFI.t_DealID = t.t_ID
     inner join dfininstr_dbt fi on fi.t_fi_kind = 6
                                and fi.t_fiid = nFI.t_Fiid
     WHERE ((t.t_department = p_department) or (p_department = 0))
       and t.t_state != 0
       and (t.t_date >= cDatePrecious_Metals
            or cDatePrecious_Metals <= (select max(nFI0.t_Paydate)
                                          from ddvnfi_dbt nFI0
                                         where nFI0.t_DealID = t.t_ID));


  ------------------------------------------------------
  -- Курсор по CSA
  ------------------------------------------------------

  cursor cur_CSA(In_DealId number, in_date date) is
  select csa.t_csaid dealID,
         4626 DocKind,
         csa.t_csaid || '#' || 'CSA'  DwhDealID,
         csa.t_begdate dealdate,
         csa.t_partyid PartyId, -- контрагент
         1 department,
         87 mb_kind,
         csa.t_code deal_code,
         '0' Is_Interior,
         csa.t_begdate deal_Start,
         decode(csa.t_enddate,
                to_date('01.01.0001', 'dd.mm.yyyy'),
                to_date('01.01.3001','dd.mm.yyyy'),
                csa.t_enddate) deal_End,
         csa.t_comment note,
         csap.t_sumpaycur fiid,
         csap.csa_rest Deal_Sum,
             decode(csap.t_sumpaycur,
                    0,
                    csap.csa_rest,
                    rsb_fiinstr.ConvSum(csap.csa_rest,
                                        csap.t_sumpaycur,
                                        0,
                                        csa.t_begdate)) deal_sum_nat
    from ddvcsa_dbt csa
         inner join (select p.t_csaid,
                                    p.t_sumpaycur,
                                    sum(decode(p.t_direction, 1, -p.t_sumpay, p.t_sumpay)) csa_rest
                               from ddvcsapm_dbt p
                              where p.t_date <= in_date--  trunc(sysdate)
                              group by p.t_csaid, p.t_sumpaycur)  csap on csap.t_csaid = csa.t_csaid
   where exists (
                  select *
                    from doproper_dbt o
                         inner join doprdocs_dbt d on d.t_id_operation = o.t_id_operation
                         inner join dacctrn_dbt t on t.t_acctrnid = d.t_acctrnid
                         inner join daccount_dbt acc on acc.t_client = csa.t_partyid and acc.t_account = t.t_account_payer
                   where o.t_dockind = 4626 and o.t_documentid = lpad(csa.t_csaid,34,'0') )
       and csa.t_csaid = In_DealId;

  ------------------------------------------------------
  -- Курсор по Договорам обеспечения подлежащим выгрузке
  ------------------------------------------------------

  cursor cur_CSA_For_Export(p_department number, p_date date) is
  select csa.t_csaid dealID, -- Кредит
         4626 DocKind,
         csa.t_csaid || '#' || 'CSA' DwhDealID
    from ddvcsa_dbt csa
         inner join (select p.t_csaid, p.t_sumpaycur, p.t_direction,  sum(p.t_sumpay) t_sumpay
                       from ddvcsapm_dbt p
                      where p.t_direction = 0
                      group by p.t_csaid, p.t_sumpaycur, p.t_direction) csap on csap.t_csaid = csa.t_csaid
   where exists (
                  select *
                    from doproper_dbt o
                         inner join doprdocs_dbt d on d.t_id_operation = o.t_id_operation
                         inner join dacctrn_dbt t on t.t_acctrnid = d.t_acctrnid and t.t_date_carry<=p_date
                         --inner join daccount_dbt acc on acc.t_client = csa.t_partyid and acc.t_account = t.t_account_payer
                   where o.t_dockind = 4626 and o.t_documentid = lpad(csa.t_csaid,34,'0') )
         and 1 = p_department
       and (csa.t_begdate >= cDateCSA
            or cDateMBK <= decode(csa.t_enddate,
                                  to_date('01.01.0001', 'dd.mm.yyyy'),
                                  to_date('01.01.3001','dd.mm.yyyy'),
                                  csa.t_enddate));

  ------------------------------------------------------
  -- Курсор по хеджированию
  ------------------------------------------------------
  cursor cur_HEDG_For_Export(p_department number, p_date date) is
 
    select  val.t_id HEDG_id from DDLHDGRFAIRVAL_DBT val 
      inner join dfininstr_dbt fin on fin.t_fiid = val.t_curfiid
      inner join ddlhdgrelation_dbt rel on rel.t_id = val.t_relationid 
      where val.t_date <= p_date;
    

  function Get_Carry (in_DocKind number default 0, in_DocID number default 0) return tab_Carry pipelined is
  rCarry rec_Carry;
  p_DWHMigrationDate date;
  begin
    p_DWHMigrationDate := qb_dwh_utils.GetDWHMigrationDate;
    -- Курсор по сделкам
    for rec_deal in cur_All_Export_Deal (in_DocKind, in_DocID) loop

      for rec_trn in (select o.t_kind_operation Kind_Operation,        -- Вид операции
                             o.t_id_operation OperationID,             -- ИД операции
                             stk.t_name Stepname,                     -- Шаг
                             t.t_acctrnid AcctrnID,                    -- ИД проводки
                             t.t_date_carry trn_Date,                  -- Дата проводки
                             t.t_number_pack trn_Pack,                 -- Пачка в проводке
                             nvl(g.t_extendedground, chr(1)) trn_DopInfo,           -- Доп информация в проводке
                             t.t_account_payer trn_PayerAccount,       -- Дебет
                             t.t_account_receiver trn_ReceiverAccount, -- Кредит
                             t.t_ground trn_Description,               -- Основание
                             t.t_fiid_payer trn_Currency_Code,         -- Валюта
                             t.t_sum_payer trn_Amount,                 -- Сумма
                             t.t_userfield4 trn_U4,                    -- UserField4 в проводке
                             pi.t_paymentid Dealpayment_Id,          -- ИД платежа по сделке
                             pi.t_amount Dealpayment_Amount,     -- Сумма платежа в платеже по сделке
                             pi.t_userfield4 Dealpayment_U4,          -- UserField4 в платеже по сделке
                             p.t_paymentid payment_Id,                 -- ИД платежа в рамках которого проводка
                             p.t_dockind payment_dockind,             -- Dockind платежа в рамках которого проводка
                             p.t_amount payment_Amount,           -- Сумма платежа в рамках которого проводка
                             nvl(p.t_userfield4, chr(1)) payment_U4,               -- UserField4 платежа в рамках которого проводка
                             null p322_ID,                                    -- ИД связанного (входящего) платежа
                             null p322_Dockind,                           -- Dockind связанного (входящего) платежа
                             null p322_NumDoc,                         -- Номер связанного(входящего) платежа
                             null p322_Pack,                               -- Пачка связанного(входящего) платежа
                             null p322_PayerAccount,                  -- дебет связанного(входящего) платежа
                             null p322_ReceiverAccount,              -- кредит связанного(входящего) платежа
                             null p322_Description,                     -- Основание связанного(входящего) платежа
                             null p322_U4                                  -- UserField4 в связанного(входящего) платеже
                        from doproper_dbt o
                             inner join doprdocs_dbt d on o.t_id_operation = d.t_id_operation
                             inner join doprstep_dbt st on st.t_id_operation = o.t_id_operation and st.t_id_step = d.t_id_step
                             inner join doprostep_dbt stk on stk.t_name != 'Перенос средств' and stk.t_blockID  = st.t_blockID and stk.t_number_step =  st.t_number_step
                             inner join dacctrn_dbt t  on ((t.t_date_carry >= p_DWHMigrationDate and exists
                                                                (select 1
                                                                   from ddl_tick_dbt tick
                                                                  where tick.t_bofficekind = o.t_dockind) -- Сделка МБК
                                                               ) or (t.t_date_carry >= cDatePrecious_Metals and
                                                               exists (select 1
                                                                        from ddvndeal_dbt dvndeal
                                                                             inner join ddvnfi_dbt nFI on nFI.t_Type = 0 and nFI.t_DealID = dvndeal.t_ID
                                                                             inner join dfininstr_dbt fi on fi.t_fi_kind = 6 and fi.t_fiid = nFI.t_Fiid
                                                                       where dvndeal.t_dockind = o.t_dockind) -- Сделка драг металы
                                                               ) or (o.t_dockind = 4626 and t.t_date_carry >= cDateCSA) -- CSA
                                                               )
                                                            and t.t_number_pack != 200 and t.t_acctrnid = d.t_acctrnid
                               left outer join dextgnd_dbt g on g.t_acctrnid = t.t_acctrnid
                               left outer join dpmdocs_dbt pmdoc on pmdoc.t_acctrnid = t.t_acctrnid
                               left outer join dpmpaym_dbt p on p.t_paymentid = pmdoc.t_paymentid
                               left outer join dpmpaym_dbt pi on pi.t_dockind  = rec_deal.dockind and pi.t_documentid = lpad(rec_deal.dealID, 34, '0')
                                                                                 and (pi.t_paymentid = p.t_paymentid or pi.t_paymentid in (select t_initialpayment from dpmlink_dbt where t_purposepayment = p.t_paymentid))

                        where o.t_dockind = rec_deal.DocKind and o.t_documentid = lpad(rec_deal.dealID, 34, '0')
                           and d.t_dockind = 1
                        ) loop
        rCarry.DockKind             := rec_deal.dockind;
        rCarry.DealId               := rec_deal.dealid;
        rCarry.Deal_Code            := rec_deal.deal_code;
        rCarry.Kind_Operation       := rec_trn.kind_operation;
        rCarry.OperationID          := rec_trn.operationid;
        rCarry.Stepname           := rec_trn.Stepname;
        rCarry.AcctrnID             := rec_trn.acctrnid;
        rCarry.ResultBisquitID     := case when length(rec_trn.trn_u4)>2 then rec_trn.trn_u4
                                                        when length(rec_trn.payment_U4)>2 then rec_trn.payment_U4
                                                        when length(rec_trn.trn_dopinfo)>2 then rec_trn.trn_dopinfo
                                                        else null
                                                 end;
        if rCarry.ResultBisquitID is null then
          begin
          select decode(p.t_userfield4, chr(1), null,p.t_userfield4)
            into rCarry.ResultBisquitID
            from dpmlink_dbt l
           inner join dpmpaym_dbt p on p.t_paymentid = l.t_initialpayment
            where l.t_purposepayment = rec_trn.payment_Id;
            exception
              when others then
                null;
            end;
        end if;
        rCarry.trn_Date             := rec_trn.trn_date;
        rCarry.trn_Pack             := rec_trn.trn_pack;
        rCarry.trn_DopInfo          := rec_trn.trn_dopinfo;
        rCarry.trn_PayerAccount     := rec_trn.trn_payeraccount;
        rCarry.trn_ReceiverAccount  := rec_trn.trn_receiveraccount;
        rCarry.trn_Description      := rec_trn.trn_description;
        rCarry.trn_Currency_Code    := rec_trn.trn_currency_code;
        rCarry.trn_Amount           := rec_trn.trn_amount;
        rCarry.trn_U4               := rec_trn.trn_u4;
        rCarry.Dealpayment_Id    := rec_trn.Dealpayment_Id;
        rCarry.Dealpayment_Amount:= rec_trn.Dealpayment_Amount;
        rCarry.Dealpayment_U4    := rec_trn.Dealpayment_U4;
        rCarry.payment_Id        := rec_trn.payment_Id;
        rCarry.payment_dockind   := rec_trn.payment_dockind;
        rCarry.payment_Amount    := rec_trn.payment_Amount;
        rCarry.payment_U4        := rec_trn.payment_U4;
        rCarry.p322_ID              := rec_trn.p322_id;
        rCarry.p322_dockind           := rec_trn.p322_dockind;
        rCarry.p322_NumDoc          := rec_trn.p322_numdoc;
        rCarry.p322_Pack            := rec_trn.p322_pack;
        rCarry.p322_PayerAccount    := rec_trn.p322_payeraccount;
        rCarry.p322_ReceiverAccount := rec_trn.p322_receiveraccount;
        rCarry.p322_Description     := rec_trn.p322_description;
        rCarry.p322_U4              := rec_trn.p322_u4;
        pipe row (rCarry);
      end loop;
      --Дробление платежей

      for rec_trn in (select null Kind_Operation,        -- Вид операции
                             null OperationID,             -- ИД операции
                             t.t_acctrnid AcctrnID,                    -- ИД проводки
                             t.t_date_carry trn_Date,                  -- Дата проводки
                             t.t_number_pack trn_Pack,                 -- Пачка в проводке
                             nvl(g.t_extendedground, chr(1)) trn_DopInfo,           -- Доп информация в проводке
                             t.t_account_payer trn_PayerAccount,       -- Дебет
                             t.t_account_receiver trn_ReceiverAccount, -- Кредит
                             t.t_ground trn_Description,               -- Основание
                             t.t_fiid_payer trn_Currency_Code,         -- Валюта
                             t.t_sum_payer trn_Amount,                 -- Сумма
                             t.t_userfield4 trn_U4,                    -- UserField4 в проводке
                             pi.t_paymentid Dealpayment_Id,          -- ИД платежа по сделке
                             pi.t_amount Dealpayment_Amount,     -- Сумма платежа в платеже по сделке
                             pi.t_userfield4 Dealpayment_U4,          -- UserField4 в платеже по сделке
                             p.t_paymentid payment_Id,                 -- ИД платежа в рамках которого проводка
                             p.t_dockind payment_dockind,             -- Dockind платежа в рамках которого проводка
                             p.t_amount payment_Amount,           -- Сумма платежа в рамках которого проводка
                             nvl(p.t_userfield4, chr(1)) payment_U4,               -- UserField4 платежа в рамках которого проводка
                             p.t_paymentid p322_ID,                    -- ИД связанного (входящего) платежа
                             p.t_dockind p322_dockind,                 -- Dockind связанного (входящего) платежа
                             pp.t_number p322_NumDoc,                  -- Номер связанного(входящего) платежа
                             p.t_numberpack p322_Pack,                 -- Пачка связанного(входящего) платежа
                             p.t_payeraccount p322_PayerAccount,       -- дебет связанного(входящего) платежа
                             p.t_receiveraccount p322_ReceiverAccount, -- кредит связанного(входящего) платежа
                             pp.t_ground p322_Description,         -- Основание связанного(входящего) платежа
                             p.t_userfield4 p322_U4                    -- UserField4 в связанного(входящего) платеже

                        from dpmlink_dbt l
                             inner join dpmpaym_dbt p on p.t_paymentid = l.t_purposepayment
                             inner join dpmrmprop_dbt pp on pp.t_paymentid = l.t_purposepayment
                             inner join dpmpaym_dbt pi on pi.t_paymentid = l.t_initialpayment
                             left outer join dpmdocs_dbt d on d.t_paymentid = l.t_purposepayment
                             left outer join dacctrn_dbt t on ((t.t_date_carry >= p_DWHMigrationDate and exists
                                                                (select 1
                                                                   from ddl_tick_dbt tick
                                                                  where tick.t_bofficekind = pi.t_dockind) -- Сделка МБК
                                                               ) or (t.t_date_carry >= cDatePrecious_Metals and
                                                               exists (select 1
                                                                        from ddvndeal_dbt dvndeal
                                                                             inner join ddvnfi_dbt nFI on nFI.t_Type = 0 and nFI.t_DealID = dvndeal.t_ID
                                                                             inner join dfininstr_dbt fi on fi.t_fi_kind = 6 and fi.t_fiid = nFI.t_Fiid
                                                                       where dvndeal.t_dockind = pi.t_dockind) -- Сделка драг металы
                                                               ) or (pi.t_dockind = 4626 and t.t_date_carry >= cDateCSA) -- CSA
                                                               )
                                                            and t.t_acctrnid = d.t_acctrnid
                               left outer join dextgnd_dbt g on g.t_acctrnid = t.t_acctrnid
                       where l.t_initialpayment != l.t_purposepayment
                             and pi.t_numberpack = 175
                             and pi.t_dockind  = rec_deal.DocKind
                             and pi.t_documentid = lpad(rec_deal.dealID, 34, '0')
                        ) loop
        rCarry.DockKind             := rec_deal.dockind;
        rCarry.DealId               := rec_deal.dealid;
        rCarry.Deal_Code            := rec_deal.deal_code;
        rCarry.Kind_Operation       := rec_trn.kind_operation;
        rCarry.OperationID          := rec_trn.operationid;
        rCarry.Stepname           := null;
        rCarry.AcctrnID             := rec_trn.acctrnid;
        rCarry.ResultBisquitID     := case when length(rec_trn.trn_u4)>2 then rec_trn.trn_u4
                                                        when length(rec_trn.payment_U4)>2 then rec_trn.payment_U4
                                                        when length(rec_trn.trn_dopinfo)>2 then rec_trn.trn_dopinfo
                                                        else null
                                                 end;
        rCarry.trn_Date             := rec_trn.trn_date;
        rCarry.trn_Pack             := rec_trn.trn_pack;
        rCarry.trn_DopInfo          := rec_trn.trn_dopinfo;
        rCarry.trn_PayerAccount     := rec_trn.trn_payeraccount;
        rCarry.trn_ReceiverAccount  := rec_trn.trn_receiveraccount;
        rCarry.trn_Description      := rec_trn.trn_description;
        rCarry.trn_Currency_Code    := rec_trn.trn_currency_code;
        rCarry.trn_Amount           := rec_trn.trn_amount;
        rCarry.trn_U4               := rec_trn.trn_u4;
        rCarry.Dealpayment_Id    := rec_trn.Dealpayment_Id;
        rCarry.Dealpayment_Amount:= rec_trn.Dealpayment_Amount;
        rCarry.Dealpayment_U4    := rec_trn.Dealpayment_U4;
        rCarry.payment_Id        := rec_trn.payment_Id;
        rCarry.payment_dockind   := rec_trn.payment_dockind;
        rCarry.payment_Amount    := rec_trn.payment_Amount;
        rCarry.payment_U4        := rec_trn.payment_U4;
        rCarry.p322_ID              := rec_trn.p322_id;
        rCarry.p322_Dockind              := rec_trn.p322_Dockind;
        rCarry.p322_NumDoc          := rec_trn.p322_numdoc;
        rCarry.p322_Pack            := rec_trn.p322_pack;
        rCarry.p322_PayerAccount    := rec_trn.p322_payeraccount;
        rCarry.p322_ReceiverAccount := rec_trn.p322_receiveraccount;
        rCarry.p322_Description     := rec_trn.p322_description;
        rCarry.p322_U4              := rec_trn.p322_u4;
        pipe row (rCarry);
      end loop;
    end loop;
  end;
  function Get_DWH_Carry (in_DocKind number default 0, in_DocID number default 0) return tab_Carry pipelined is
  rCarry rec_Carry;
  dwhCarryCode varchar2(500);
  pDWH_Deal varchar2(500);
  str_trn   varchar2(500);
  str_tmp   varchar2(500);
  str_id    varchar2(500);
  str_sum   varchar2(500);
  pParentDealId number;
  Is_not_Roll   number;
  cExtendedGround number;
  v_selectDeal varchar2(32600):= q'[
  select tck.t_dealid dealID,         -- ИД сделки
         tck.t_bofficekind DocKind,   -- Вид Сделки
         tck.t_dealcode Deal_Code,    -- Код Сделки
         tck.t_dealid || '#' || 'TCK' DWH_Deal_Code, -- Код сделки DWH
         tck.t_department Department, -- Филиал
         tck.t_flag1 isProl           -- признак пролонгации
    from ddl_tick_dbt tck
   where tck.t_dealstatus != 0
         and tck.t_bofficekind in( 102, 208 )
     ]'||case when in_DocKind != 0 then ' and tck.t_bofficekind = :in_DocKind ' end ||'
     ' ||case when in_DocID != 0 then ' and tck.t_dealid = :in_DocID ' end ||q'[
  -- Ген. соглашения
  union all
  select genagr.t_genagrid dealID,
         genagr.t_dockind DocKind,
         genagr.t_code Deal_Code,
         genagr.t_genagrid || '#' || 'GEN' DWH_Deal_Code,
         genagr.t_department Department,
         chr(0) isprol
    from ddl_genagr_dbt genagr
   where genagr.t_dockind = 4611
     ]'||case when in_DocKind != 0 then ' and genagr.t_dockind = :in_DocKind ' end ||'
     ' ||case when in_DocID != 0 then ' and genagr.t_genagrid = :in_DocID ' end ||q'[
  -- Обеспечения
  union all
  select dl_order.t_contractid dealID,
         dl_order.t_dockind DocKind,
         dl_order.t_ordernumber Deal_Code,
         dl_order.t_contractid || '#' || 'PRV' DWH_Deal_Code,
         dl_order.t_department Department,
         chr(0) isprol
    from ddl_order_dbt dl_order
   where dl_order.t_contractstatus != 0
     and dl_order.t_dockind = 126
     ]'||case when in_DocKind != 0 then ' and dl_order.t_dockind = :in_DocKind ' end ||'
     ' ||case when in_DocID != 0 then ' and dl_order.t_contractid = :in_DocID ' end ||q'[
  -- Драг. метал
  union all
  select t.t_id DealId,
         t.t_Dockind Dockind,
         t.t_code Deal_Code,
         t.t_id || '#DVN' DWH_Deal_Code,
         t.t_department Department,
         chr(0) isprol
    from ddvndeal_dbt t
   inner join ddvnfi_dbt nFI on nFI.t_Type = 0
                            and nFI.t_DealID = t.t_ID
   inner join dfininstr_dbt fi on fi.t_fi_kind = 6
                              and fi.t_fiid = nFI.t_Fiid
   where t.t_state != 0
     ]'||case when in_DocKind != 0 then ' and t.t_Dockind = :in_DocKind ' end ||'
     ' ||case when in_DocID != 0 then ' and t.t_id = :in_DocID ' end ||q'[
  -- CSA
  union all
  select csa.t_csaid dealID,
         4626 DocKind,
         csa.t_number Deal_Code,
         csa.t_number DWH_Deal_Code,
         1 Department,
         chr(0) isprol
   from ddvcsa_dbt csa
        inner join (select p.t_csaid, p.t_sumpaycur, p.t_direction,  sum(p.t_sumpay) t_sumpay
                      from ddvcsapm_dbt p
                     where p.t_direction = 0
                     group by p.t_csaid, p.t_sumpaycur, p.t_direction) csap on csap.t_csaid = csa.t_csaid
  where exists (select 1
                  from doproper_dbt o
                       inner join doprdocs_dbt d on d.t_id_operation = o.t_id_operation
                       --inner join dacctrn_dbt t on t.t_acctrnid = d.t_acctrnid and t.t_date_carry<=p_date
                 where o.t_dockind = 4626 and o.t_documentid = lpad(csa.t_csaid,34,'0') )
     ]'||case when in_DocKind != 0 then ' and 4626 = :in_DocKind ' end ||'
     ' ||case when in_DocID != 0 then ' and csa.t_csaid = :in_DocID ' end ;
  cur_deal sys_refcursor ;
  cursor cur_rec_real IS select tck.t_dealid dealID,         -- ИД сделки
         tck.t_bofficekind DocKind,   -- Вид Сделки
         tck.t_dealcode Deal_Code,    -- Код Сделки
         tck.t_dealid || '#' || 'TCK' DWH_Deal_Code, -- Код сделки DWH
         tck.t_department Department, -- Филиал
         tck.t_flag1 isProl           -- признак пролонгации
    from ddl_tick_dbt tck  where 1=0 ;
  rec_deal cur_rec_real%rowtype ;
  begin
    if in_DocKind != 0 and in_DocID != 0 then 
       open cur_deal  for v_selectDeal using in_DocKind,in_DocID,in_DocKind,in_DocID,in_DocKind,in_DocID,in_DocKind,in_DocID,in_DocKind,in_DocID ;
    elsif  in_DocKind != 0 and in_DocID = 0 then
       open cur_deal  for v_selectDeal using in_DocKind,in_DocKind,in_DocKind,in_DocKind,in_DocKind ;
    elsif in_DocKind = 0 and in_DocID != 0 then
       open cur_deal  for v_selectDeal using in_DocID,in_DocID,in_DocID,in_DocID,in_DocID ;
    else
       open cur_deal  for v_selectDeal  ;
    end if;
   -- Курсор по сделкам
    /*for rec_deal in (-- МБК
                     select tck.t_dealid dealID,         -- ИД сделки
                            tck.t_bofficekind DocKind,   -- Вид Сделки
                            tck.t_dealcode Deal_Code,    -- Код Сделки
                            tck.t_dealid || '#' || 'TCK' DWH_Deal_Code, -- Код сделки DWH
                            tck.t_department Department, -- Филиал
                            tck.t_flag1 isProl           -- признак пролонгации
                       from ddl_tick_dbt tck
                      where tck.t_dealstatus != 0
                            and tck.t_bofficekind in( 102, 208 )
                      --and tck.t_dealid = 121938

                        and (tck.t_bofficekind = in_DocKind or in_DocKind = 0 )
                        and (tck.t_dealid = in_DocID or in_DocID = 0)

                     -- Ген. соглашения
                     union all
                     select genagr.t_genagrid dealID,
                            genagr.t_dockind DocKind,
                            genagr.t_code Deal_Code,
                            genagr.t_genagrid || '#' || 'GEN' DWH_Deal_Code,
                            genagr.t_department Department,
                            chr(0) isprol
                       from ddl_genagr_dbt genagr
                      where genagr.t_dockind = 4611
                        and (genagr.t_dockind = in_DocKind or in_DocKind = 0 )
                        and (genagr.t_genagrid = in_DocID or in_DocID = 0)
                     -- Обеспечения
                     union all
                     select dl_order.t_contractid dealID,
                            dl_order.t_dockind DocKind,
                            dl_order.t_ordernumber Deal_Code,
                            dl_order.t_contractid || '#' || 'PRV' DWH_Deal_Code,
                            dl_order.t_department Department,
                            chr(0) isprol
                       from ddl_order_dbt dl_order
                      where dl_order.t_contractstatus != 0
                        and dl_order.t_dockind = 126
                        and (dl_order.t_dockind = in_DocKind or in_DocKind = 0 )
                        and (dl_order.t_contractid = in_DocID or in_DocID = 0)
                     -- Драг. метал
                     union all
                     select t.t_id DealId,
                            t.t_Dockind Dockind,
                            t.t_code Deal_Code,
                            t.t_id || '#DVN' DWH_Deal_Code,
                            t.t_department Department,
                            chr(0) isprol
                       from ddvndeal_dbt t
                      inner join ddvnfi_dbt nFI on nFI.t_Type = 0
                                               and nFI.t_DealID = t.t_ID
                      inner join dfininstr_dbt fi on fi.t_fi_kind = 6
                                                 and fi.t_fiid = nFI.t_Fiid
                      where t.t_state != 0
                        and (t.t_Dockind = in_DocKind or in_DocKind = 0 )
                        and (t.t_id = in_DocID or in_DocID = 0)
                     -- CSA
                     union all
                     select csa.t_csaid dealID,
                            4626 DocKind,
                            csa.t_number Deal_Code,
                            csa.t_number DWH_Deal_Code,
                            1 Department,
                            chr(0) isprol
                      from ddvcsa_dbt csa
                           inner join (select p.t_csaid, p.t_sumpaycur, p.t_direction,  sum(p.t_sumpay) t_sumpay
                                         from ddvcsapm_dbt p
                                        where p.t_direction = 0
                                        group by p.t_csaid, p.t_sumpaycur, p.t_direction) csap on csap.t_csaid = csa.t_csaid
                     where exists (select 1
                                     from doproper_dbt o
                                          inner join doprdocs_dbt d on d.t_id_operation = o.t_id_operation
                                          --inner join dacctrn_dbt t on t.t_acctrnid = d.t_acctrnid and t.t_date_carry<=p_date
                                    where o.t_dockind = 4626 and o.t_documentid = lpad(csa.t_csaid,34,'0') )
                        and (4626 = in_DocKind or in_DocKind = 0 )
                        and (csa.t_csaid = in_DocID or in_DocID = 0)
                      ) */
   loop
     fetch cur_deal into rec_deal ;
     EXIT WHEN cur_deal%NOTFOUND;
       --if rec_deal.dealID = 121938 then Raise_application_error(-20000, '121938#TCK'); end if;
      -- Определим код сделки к которой привязывать движения для DWH
      if instr(rec_deal.DWH_Deal_Code, 'TCK') > 0 then --МБК
        if rec_deal.isprol = chr(88) then
          --Найдем ИД текущей пролонгации
          /*pParentDealId := GetFirstDealId(rec_deal.dealID, 1);
          -- Проверим является это обычной пролонгацией или ROLL
          select count(1)
            into Is_not_Roll
            from ddl_tick_dbt t
           inner join doproper_dbt o on o.t_documentid =
                                        lpad(t.t_dealid, 34, '0')
           inner join doprstep_dbt s on s.t_blockid in (1000009, 1000021)
                                    and s.t_id_operation = o.t_id_operation
           where t.t_dealid = pParentDealId;
          */
          -- Для пролонгаций типа Roll привязка проводок делается к первой сделке
          --if Is_not_Roll > 0 then
            pDWH_Deal := qb_dwh_utils.GetComponentCode('FCT_DEAL',
                                                       qb_dwh_utils.System_BISQUIT,
                                                       rec_deal.Department,
                                                       rec_deal.DWH_Deal_Code);
          /*else
            pParentDealId := GetFirstDealId(rec_deal.DealId,0);
            pDWH_Deal := qb_dwh_utils.GetComponentCode('FCT_DEAL',
                                                       qb_dwh_utils.System_BISQUIT,
                                                       rec_deal.Department,
                                                       pParentDealId || '#' || 'TCK');

          end if;*/
        else
            pDWH_Deal := qb_dwh_utils.GetComponentCode('FCT_DEAL',
                                                       qb_dwh_utils.System_BISQUIT,
                                                       rec_deal.Department,
                                                       rec_deal.DWH_Deal_Code);
        end if;
      else
        pDWH_Deal := qb_dwh_utils.GetComponentCode('FCT_DEAL',
                                                   qb_dwh_utils.System_BISQUIT,
                                                   rec_deal.Department,
                                                   rec_deal.DWH_Deal_Code);
      end if;
      for rec_trn in (select o.t_kind_operation Kind_Operation,        -- Вид операции
                             o.t_id_operation OperationID,             -- ИД операции
                             stk.t_name Stepname,                      -- Шаг
                             t.t_acctrnid AcctrnID,                    -- ИД проводки
                             t.t_date_carry trn_Date,                  -- Дата проводки
                             t.t_number_pack trn_Pack,                 -- Пачка в проводке
                             nvl(g.t_extendedground, chr(1)) trn_DopInfo, -- Доп информация в проводке
                             t.t_account_payer trn_PayerAccount,       -- Дебет
                             t.t_account_receiver trn_ReceiverAccount, -- Кредит
                             t.t_ground trn_Description,               -- Основание
                             t.t_fiid_payer trn_Currency_Code,         -- Валюта
                             t.t_sum_payer trn_Amount,                 -- Сумма
                             t.t_userfield4 trn_U4,                    -- UserField4 в проводке
                             pi.t_paymentid Dealpayment_Id,            -- ИД платежа по сделке
                             pi.t_amount Dealpayment_Amount,           -- Сумма платежа в платеже по сделке
                             pi.t_userfield4 Dealpayment_U4,           -- UserField4 в платеже по сделке
                             p.t_paymentid payment_Id,                 -- ИД платежа в рамках которого проводка
                             p.t_dockind payment_dockind,              -- Dockind платежа в рамках которого проводка
                             p.t_amount payment_Amount,                -- Сумма платежа в рамках которого проводка
                             nvl(p.t_userfield4, chr(1)) payment_U4,   -- UserField4 платежа в рамках которого проводка
                             p322.t_paymentid p322_ID,                    -- ИД связанного (входящего) платежа
                             p322.t_dockind p322_dockind,                 -- Dockind связанного (входящего) платежа
                             pp322.t_number p322_NumDoc,                  -- Номер связанного(входящего) платежа
                             p322.t_numberpack p322_Pack,                 -- Пачка связанного(входящего) платежа
                             p322.t_payeraccount p322_PayerAccount,       -- дебет связанного(входящего) платежа
                             p322.t_receiveraccount p322_ReceiverAccount, -- кредит связанного(входящего) платежа
                             pp322.t_ground p322_Description,             -- Основание связанного(входящего) платежа
                             p322.t_userfield4 p322_U4,                   -- UserField4 в связанного(входящего) платеже
                             case when nvl(instr(nvl(decode(t.t_userfield4, chr(1), null, t.t_userfield4),
                                                     decode(p.t_userfield4, chr(1), null, p.t_userfield4)
                                                    ),
                                                 ','),
                                           0) != 0
                                       then 1
                                       else 0
                             end isHalfCarry -- Признак полупроводки
                        from doproper_dbt o
                             inner join doprdocs_dbt d on o.t_id_operation = d.t_id_operation
                             inner join doprstep_dbt st on st.t_id_operation = o.t_id_operation and st.t_id_step = d.t_id_step
                             inner join doprostep_dbt stk on stk.t_name != 'Перенос средств' and stk.t_blockID  = st.t_blockID and stk.t_number_step =  st.t_number_step
                             inner join dacctrn_dbt t  on ((t.t_date_carry >= cDateMBK and exists
                                                                (select 1
                                                                   from ddl_tick_dbt tick
                                                                  where tick.t_bofficekind = o.t_dockind) -- Сделка МБК
                                                               ) or (t.t_date_carry >= cDatePrecious_Metals and
                                                               exists (select 1
                                                                        from ddvndeal_dbt dvndeal
                                                                             inner join ddvnfi_dbt nFI on nFI.t_Type = 0 and nFI.t_DealID = dvndeal.t_ID
                                                                             inner join dfininstr_dbt fi on fi.t_fi_kind = 6 and fi.t_fiid = nFI.t_Fiid
                                                                       where dvndeal.t_dockind = o.t_dockind) -- Сделка драг металы
                                                               ) or (o.t_dockind = 4626 and t.t_date_carry >= cDateCSA) -- CSA
                                                               )
                                                            and t.t_number_pack != 200 and t.t_acctrnid = d.t_acctrnid
                               left outer join dextgnd_dbt g on g.t_acctrnid = t.t_acctrnid
                               left outer join dpmdocs_dbt pmdoc on  pmdoc.t_acctrnid = t.t_acctrnid
                               left outer join dpmpaym_dbt p on (p.t_dockind  = o.t_dockind or
                                                                 (p.t_dockind  = 4627 and o.t_dockind = 4626)
                                                                ) and p.t_paymentid = pmdoc.t_paymentid
                               left outer join dpmpaym_dbt pi on pi.t_dockind  = o.t_dockind and pi.t_documentid = o.t_documentid
                                                                                 and (pi.t_paymentid = p.t_paymentid or pi.t_paymentid in (select t_initialpayment from dpmlink_dbt where t_purposepayment = p.t_paymentid))

                               left outer join dpmpaym_dbt p322 on p322.t_dockind  = 322 and p322.t_paymentid = pmdoc.t_paymentid
                               left outer join dpmrmprop_dbt pp322 on pp322.t_paymentid = p322.t_paymentid
                        where o.t_dockind = rec_deal.DocKind and o.t_documentid = lpad(rec_deal.dealID, 34, '0')
                          --and d.t_dockind = 1
                        ) loop
       -- Занулим u4 если нет # в коде, так как есть случаи когда используется не по назначению
       if instr(rec_trn.trn_u4,'#') = 0 and rec_trn.trn_u4 is not null then
         rec_trn.trn_u4 := null;
       end if;
       if instr(rec_trn.payment_u4,'#') = 0 and rec_trn.payment_u4 is not null then
         rec_trn.payment_u4 := null;
       end if;
       if instr(rec_trn.p322_U4,'#') = 0 and rec_trn.p322_U4 is not null then
         rec_trn.p322_U4 := null;
       end if;
        -- Код сделки DWH CSA определяется на основании счета участвующего в движении
       if rec_deal.DocKind = 4626 then -- CSA
         select max(case when upper(c.t_code) in ('-МС', '-%МС')
                       then csa.t_csaid || '#CSA#' || 'П' || '#' || fi.t_ccy
                     when upper(c.t_code) in ('+МС', '+%МС')
                       then csa.t_csaid || '#CSA#' || 'А' || '#' || fi.t_ccy
                else csa.t_csaid || '#CSA'
                end) dopDealId
           into pDWH_Deal
           from ddvcsa_dbt csa
                inner join dmcaccdoc_dbt d on d.t_dockind = 4626
                                              and d.t_docid = csa.t_csaid
                                              and d.t_activatedate != d.t_disablingdate
                                              and d.t_account in (rec_trn.trn_payeraccount, rec_trn.trn_receiveraccount)
                inner join dmccateg_dbt c on c.t_id = d.t_catid
                inner join daccount_dbt acc on acc.t_client = csa.t_partyid and acc.t_account = d.t_account
                inner join dfininstr_dbt fi on fi.t_fiid = acc.t_code_currency
          where csa.t_csaid = rec_deal.dealid;
       end if;
       -- Пачка 175 в проводке ищем доп данные по проводке
        if (rec_trn.trn_u4 is null and rec_trn.payment_u4 is null and rec_trn.p322_U4 is null)
         and nvl(rec_trn.trn_Pack, 0) in (60, 175) then
        select count(g.t_extendedground)
          into cExtendedGround
          from dextgnd_dbt g
         where g.t_acctrnid = rec_trn.acctrnid;
        else
          cExtendedGround:=0;
        end if;

        if cExtendedGround > 0 then
          for rectmp in (select g.t_extendedground --|| ';' точка запятой в примере была
                           from dextgnd_dbt g
                          where g.t_acctrnid = rec_trn.acctrnid) loop
            --Формат = id\sum;id\sum;

            str_trn := rectmp.t_extendedground;
            While (instr(str_trn, ';') > 0) loop
              begin
              str_tmp      := substr(str_trn, 1, instr(str_trn, ';') - 1);
              str_id       := substr(str_trn, 1, instr(str_trn, '\') - 1);
              str_sum      := substr(str_trn,
                                     instr(str_trn, '\') + 1,
                                     instr( substr(str_trn, instr(str_trn, '\') + 1), ';') - 1);
              if (rec_trn.trn_date < dateMigrCFT) then
                dwhCarryCode := qb_dwh_utils.GetComponentCode('FCT_CARRY_SEPARATE',
                                                              qb_dwh_utils.System_BISQUIT,
                                                              rec_deal.Department,
                                                              str_id);
              else
                dwhCarryCode := qb_dwh_utils.GetComponentCode('FCT_CARRY_SEPARATE',
                                                              qb_dwh_utils.System_IBSO,
                                                              rec_deal.Department,
                                                              str_id);
              end if;
                                                            

              rCarry.DockKind             := rec_deal.dockind;
              rCarry.DealId               := rec_deal.dealid;
              rCarry.Deal_Code            := rec_deal.deal_code;
              rCarry.DWH_Deal_Code        := pDWH_Deal ;
              rCarry.Kind_Operation       := rec_trn.kind_operation;
              rCarry.OperationID          := rec_trn.operationid;
              rCarry.Stepname             := rec_trn.Stepname;
              rCarry.AcctrnID             := rec_trn.acctrnid;
              rCarry.ResultBisquitID      := dwhCarryCode;
              rCarry.trn_Date             := rec_trn.trn_date;
              rCarry.trn_Pack             := rec_trn.trn_pack;
              rCarry.trn_DopInfo          := rec_trn.trn_dopinfo;
              rCarry.trn_PayerAccount     := rec_trn.trn_payeraccount;
              rCarry.trn_ReceiverAccount  := rec_trn.trn_receiveraccount;
              rCarry.trn_Description      := rec_trn.trn_description;
              rCarry.trn_Currency_Code    := rec_trn.trn_currency_code;
              rCarry.trn_Amount           := str_sum;
              rCarry.trn_U4               := rec_trn.trn_u4;
              rCarry.Dealpayment_Id       := rec_trn.Dealpayment_Id;
              rCarry.Dealpayment_Amount   := rec_trn.Dealpayment_Amount;
              rCarry.Dealpayment_U4       := rec_trn.Dealpayment_U4;
              rCarry.payment_Id           := rec_trn.payment_Id;
              rCarry.payment_dockind      := rec_trn.payment_dockind;
              rCarry.payment_Amount       := rec_trn.payment_Amount;
              --rCarry.payment              := rec_trn.p322_pack;
              rCarry.payment_U4           := rec_trn.payment_U4;
              rCarry.p322_ID              := rec_trn.p322_id;
              rCarry.p322_dockind         := rec_trn.p322_dockind;
              rCarry.p322_NumDoc          := rec_trn.p322_numdoc;
              rCarry.p322_Pack            := rec_trn.p322_pack;
              rCarry.p322_PayerAccount    := rec_trn.p322_payeraccount;
              rCarry.p322_ReceiverAccount := rec_trn.p322_receiveraccount;
              rCarry.p322_Description     := rec_trn.p322_description;
              rCarry.p322_U4              := rec_trn.p322_u4;
              rCarry.trn_IsHalfCarry      := rec_trn.IsHalfCarry;
              pipe row (rCarry);
              str_trn := substr(str_trn, instr(str_trn, ';') + 1);

              exception
                when others then
                  Raise_application_error(-20000, 'str_tmp=' || str_tmp ||
                                                  ' str_id=' || str_id ||
                                                  ' str_sum= ' || str_sum ||
                                                  ' acctrn=' || rec_trn.acctrnid);
              end;
            end loop;

          end loop;
        else
          if (rec_trn.trn_date < dateMigrCFT) then
            dwhCarryCode := qb_dwh_utils.GetComponentCode('FCT_CARRY',
                                                          qb_dwh_utils.System_BISQUIT,
                                                          rec_deal.Department,
                                                          rec_trn.AcctrnID,
                                                          rec_deal.dockind);
          else
            dwhCarryCode := qb_dwh_utils.GetComponentCode('FCT_CARRY',
                                                          qb_dwh_utils.System_IBSO,
                                                          rec_deal.Department,
                                                          rec_trn.AcctrnID,
                                                          rec_deal.dockind);
          end if;
                                                        
          rCarry.DockKind             := rec_deal.dockind;
          rCarry.DealId               := rec_deal.dealid;
          rCarry.Deal_Code            := rec_deal.deal_code;
          rCarry.DWH_Deal_Code        := pDWH_Deal;
          rCarry.Kind_Operation       := rec_trn.kind_operation;
          rCarry.OperationID          := rec_trn.operationid;
          rCarry.Stepname             := rec_trn.Stepname;
          rCarry.AcctrnID             := rec_trn.acctrnid;
          rCarry.ResultBisquitID      := dwhCarryCode;
          rCarry.trn_Date             := rec_trn.trn_date;
          rCarry.trn_Pack             := rec_trn.trn_pack;
          rCarry.trn_DopInfo          := rec_trn.trn_dopinfo;
          rCarry.trn_PayerAccount     := rec_trn.trn_payeraccount;
          rCarry.trn_ReceiverAccount  := rec_trn.trn_receiveraccount;
          rCarry.trn_Description      := rec_trn.trn_description;
          rCarry.trn_Currency_Code    := rec_trn.trn_currency_code;
          rCarry.trn_Amount           := rec_trn.trn_amount;
          rCarry.trn_U4               := rec_trn.trn_u4;
          rCarry.Dealpayment_Id       := rec_trn.Dealpayment_Id;
          rCarry.Dealpayment_Amount   := rec_trn.Dealpayment_Amount;
          rCarry.Dealpayment_U4       := rec_trn.Dealpayment_U4;
          rCarry.payment_Id           := rec_trn.payment_Id;
          rCarry.payment_dockind      := rec_trn.payment_dockind;
          rCarry.payment_Amount       := rec_trn.payment_Amount;
          rCarry.payment_U4           := rec_trn.payment_U4;
          rCarry.p322_ID              := rec_trn.p322_id;
          rCarry.p322_dockind         := rec_trn.p322_dockind;
          rCarry.p322_NumDoc          := rec_trn.p322_numdoc;
          rCarry.p322_Pack            := rec_trn.p322_pack;
          rCarry.p322_PayerAccount    := rec_trn.p322_payeraccount;
          rCarry.p322_ReceiverAccount := rec_trn.p322_receiveraccount;
          rCarry.p322_Description     := rec_trn.p322_description;
          rCarry.p322_U4              := rec_trn.p322_u4;
          rCarry.trn_IsHalfCarry      := rec_trn.IsHalfCarry;
          pipe row (rCarry);
        end if;
      end loop;

      --Дробление платежей
      for rec_trn in (select null Kind_Operation,                      -- Вид операции
                             null OperationID,                         -- ИД операции
                             t.t_acctrnid AcctrnID,                    -- ИД проводки
                             t.t_date_carry trn_Date,                  -- Дата проводки
                             t.t_number_pack trn_Pack,                 -- Пачка в проводке
                             nvl(g.t_extendedground, chr(1)) trn_DopInfo,   -- Доп информация в проводке
                             t.t_account_payer trn_PayerAccount,       -- Дебет
                             t.t_account_receiver trn_ReceiverAccount, -- Кредит
                             t.t_ground trn_Description,               -- Основание
                             t.t_fiid_payer trn_Currency_Code,         -- Валюта
                             t.t_sum_payer trn_Amount,                 -- Сумма
                             t.t_userfield4 trn_U4,                    -- UserField4 в проводке
                             pi.t_paymentid Dealpayment_Id,            -- ИД платежа по сделке
                             pi.t_amount Dealpayment_Amount,           -- Сумма платежа в платеже по сделке
                             pi.t_userfield4 Dealpayment_U4,           -- UserField4 в платеже по сделке
                             p.t_paymentid payment_Id,                 -- ИД платежа в рамках которого проводка
                             p.t_dockind payment_dockind,              -- Dockind платежа в рамках которого проводка
                             p.t_amount payment_Amount,                -- Сумма платежа в рамках которого проводка
                             nvl(p.t_userfield4, chr(1)) payment_U4,   -- UserField4 платежа в рамках которого проводка
                             p.t_paymentid p322_ID,                    -- ИД связанного (входящего) платежа
                             p.t_dockind p322_dockind,                 -- Dockind связанного (входящего) платежа
                             pp.t_number p322_NumDoc,                  -- Номер связанного(входящего) платежа
                             p.t_numberpack p322_Pack,                 -- Пачка связанного(входящего) платежа
                             p.t_payeraccount p322_PayerAccount,       -- дебет связанного(входящего) платежа
                             p.t_receiveraccount p322_ReceiverAccount, -- кредит связанного(входящего) платежа
                             pp.t_ground p322_Description,             -- Основание связанного(входящего) платежа
                             p.t_userfield4 p322_U4,                   -- UserField4 в связанного(входящего) платеже
                             0 IsHalfCarry
                        from dpmlink_dbt l
                             inner join dpmpaym_dbt p on p.t_paymentid = l.t_purposepayment
                             inner join dpmrmprop_dbt pp on pp.t_paymentid = l.t_purposepayment
                             inner join dpmpaym_dbt pi on pi.t_paymentid = l.t_initialpayment
                             left outer join dpmdocs_dbt d on d.t_paymentid = l.t_purposepayment
                             left outer join dacctrn_dbt t on ((t.t_date_carry >= cDateMBK and exists
                                                                (select 1
                                                                   from ddl_tick_dbt tick
                                                                  where tick.t_bofficekind = pi.t_dockind) -- Сделка МБК
                                                               ) or (t.t_date_carry >= cDatePrecious_Metals and
                                                               exists (select 1
                                                                        from ddvndeal_dbt dvndeal
                                                                             inner join ddvnfi_dbt nFI on nFI.t_Type = 0 and nFI.t_DealID = dvndeal.t_ID
                                                                             inner join dfininstr_dbt fi on fi.t_fi_kind = 6 and fi.t_fiid = nFI.t_Fiid
                                                                       where dvndeal.t_dockind = pi.t_dockind) -- Сделка драг металы
                                                               ) or (pi.t_dockind = 4626 and t.t_date_carry >= cDateCSA) -- CSA
                                                               )
                                                            and t.t_acctrnid = d.t_acctrnid
                               left outer join dextgnd_dbt g on g.t_acctrnid = t.t_acctrnid
                       where l.t_initialpayment != l.t_purposepayment
                             and pi.t_numberpack = 175
                             and pi.t_dockind  = rec_deal.DocKind
                             and pi.t_documentid = lpad(rec_deal.dealID, 34, '0')
                        ) loop

       if instr(rec_trn.trn_u4,'#') = 0 and rec_trn.trn_u4 is not null then
         rec_trn.trn_u4 := null;
       end if;
       if instr(rec_trn.payment_u4,'#') = 0 and rec_trn.payment_u4 is not null then
         rec_trn.payment_u4 := null;
       end if;
        if (rec_trn.trn_date < dateMigrCFT) then
          dwhCarryCode := qb_dwh_utils.GetComponentCode('FCT_CARRY',
                                                          qb_dwh_utils.System_BISQUIT,
                                                          rec_deal.Department,
                                                          rec_trn.AcctrnID,
                                                          rec_deal.dockind);
        else
          dwhCarryCode := qb_dwh_utils.GetComponentCode('FCT_CARRY',
                                                          qb_dwh_utils.System_IBSO,
                                                          rec_deal.Department,
                                                          rec_trn.AcctrnID,
                                                          rec_deal.dockind);
        end if;                                                        
        rCarry.DockKind             := rec_deal.dockind;
        rCarry.DealId               := rec_deal.dealid;
        rCarry.Deal_Code            := rec_deal.deal_code;
        -- Код сделки DWH
        rCarry.DWH_Deal_Code        := pDWH_Deal;
        rCarry.Kind_Operation       := rec_trn.kind_operation;
        rCarry.OperationID          := rec_trn.operationid;
        rCarry.Stepname             := null;
        rCarry.AcctrnID             := rec_trn.acctrnid;
        rCarry.ResultBisquitID      := dwhCarryCode;
        rCarry.trn_Date             := rec_trn.trn_date;
        rCarry.trn_Pack             := rec_trn.trn_pack;
        rCarry.trn_DopInfo          := rec_trn.trn_dopinfo;
        rCarry.trn_PayerAccount     := rec_trn.trn_payeraccount;
        rCarry.trn_ReceiverAccount  := rec_trn.trn_receiveraccount;
        rCarry.trn_Description      := rec_trn.trn_description;
        rCarry.trn_Currency_Code    := rec_trn.trn_currency_code;
        rCarry.trn_Amount           := rec_trn.trn_amount;
        rCarry.trn_U4               := rec_trn.trn_u4;
        rCarry.Dealpayment_Id       := rec_trn.Dealpayment_Id;
        rCarry.Dealpayment_Amount   := rec_trn.Dealpayment_Amount;
        rCarry.Dealpayment_U4       := rec_trn.Dealpayment_U4;
        rCarry.payment_Id           := rec_trn.payment_Id;
        rCarry.payment_dockind      := rec_trn.payment_dockind;
        rCarry.payment_Amount       := rec_trn.payment_Amount;
        rCarry.payment_U4           := rec_trn.payment_U4;
        rCarry.p322_ID              := rec_trn.p322_id;
        rCarry.p322_Dockind         := rec_trn.p322_Dockind;
        rCarry.p322_NumDoc          := rec_trn.p322_numdoc;
        rCarry.p322_Pack            := rec_trn.p322_pack;
        rCarry.p322_PayerAccount    := rec_trn.p322_payeraccount;
        rCarry.p322_ReceiverAccount := rec_trn.p322_receiveraccount;
        rCarry.p322_Description     := rec_trn.p322_description;
        rCarry.p322_U4              := rec_trn.p322_u4;
        rCarry.trn_IsHalfCarry      := rec_trn.IsHalfCarry;
        pipe row (rCarry);
      end loop;
    end loop;
    close cur_deal;
  end;
  ------------------------------------------------------
  -- Получит значение для смешения даты пдалтежа в не рабочии дни
  ------------------------------------------------------
  function GetWorkDayDiff(in_DealId in number, in_Purpose number)
    return varchar2 is
    out_Result Varchar2(1) := '0';
  begin
    for i in (select p.t_valuedate ValueDate,
                     to_date(p.t_userfield3, 'dd.mm.yyyy') FactDate
                from dpmpaym_dbt p
               where p.t_purpose = in_Purpose
                 and length(p.t_userfield3) > 1
                 and p.t_documentid = in_DealId) loop
      if i.valuedate != i.Factdate then
        out_Result := '1';
        return out_Result;
      end if;
    end loop;
    return out_Result;
  exception
    when others then
      return out_Result;
  end;
  ------------------------------------------------------
  -- На основании события инициируем общие параметры выгрузки
  ------------------------------------------------------
  procedure InitExportData(in_EventID       in number,
                           out_dwhRecStatus out varchar2,
                           out_dwhDT        out varchar2,
                           out_dwhSysMoment out varchar2,
                           out_dwhEXT_FILE  out varchar2,
                           in_version       in number := 1) is
    pDepartment number;
    p_Date      varchar2(30);
  begin
    -- Получим общие параметры выгрузки
    pDepartment      := to_number(qb_bp_utils.GetAttrValue(in_EventID,
                                                           cAttrDepartment));
    out_dwhRecStatus := qb_bp_utils.GetAttrValue(in_EventID,
                                                 cAttrRec_Status);
    p_Date           := qb_bp_utils.GetAttrValue(in_EventID, cAttrDT);

    if  (in_version = 1) then
    out_dwhDT := qb_dwh_utils.DateToChar(To_date(p_Date, 'dd.mm.yyyy'));
    elsif (in_version = 2) then
      out_dwhDT := To_Char(sysdate,'dd.mm.yyyy HH24:MI:SS');
    end if;

    select qb_dwh_utils.DateTimeToChar(e.t_timestamp)
      into out_dwhSysMoment
      from dqb_bp_event_dbt e
     where e.t_id = in_EventID;

    out_dwhEXT_FILE := qb_dwh_utils.GetComponentCode('EXT_FILE',
                                                     qb_dwh_utils.System_RS,
                                                     pDepartment,
                                                     qb_dwh_utils.GetEXT_FILE_ID(in_EventID,
                                                                                 out_dwhDT,
                                                                                 out_dwhRecStatus));

  end;

  -----------------------------------------------------
  --ВПроцедура первичной выгрузки связей после миграции в ASS_DEAL_MIGRATION@LDR_INFA()
  -----------------------------------------------------
  procedure add_ASS_DEAL_MIGRATION(in_date in date) is
    CntDeal      number := 0;
    EventID      number := 0;
    dwhRecStatus varchar2(1);
    dwhDT        varchar2(10);
    dwhSysMoment varchar2(19);
    dwhEXT_FILE  varchar2(300);
  begin
    -- Установим событие начало выгрузки
    qb_bp_utils.startevent(cEvent_EXPORT_MBK_MIGRATION, null, EventID);
    qb_bp_utils.SetAttrValue(EventID,
                             cAttrRec_Status,
                             qb_dwh_utils.REC_ADD);
    qb_bp_utils.SetAttrValue(EventID, cAttrDepartment, 1);
    qb_bp_utils.SetAttrValue(EventID, cAttrDT, trunc(sysdate));
    dwhRecStatus := qb_dwh_utils.REC_ADD;
    InitExportData(EventID, dwhRecStatus, dwhDT, dwhSysMoment, dwhEXT_FILE);
    -- Курсору по активным сделкам на дату
    for rec in (select d.t_dealid dealid,
                       d.t_bofficekind dealKind,
                       qb_dwh_utils.GetComponentCode('FCT_DEAL',
                                                     qb_dwh_utils.System_RS,
                                                     d.t_department,
                                                     d.t_dealid) || '#' ||
                       'TCK' DEAL_CUR_CODE,
                       d.t_dealCode DEAL_PREV_CODE,
                       qb_dwh_utils.GetCODE_DEPARTMENT(d.t_department) CUR_DEPARTMENT,
                       qb_dwh_utils.GetCODE_DEPARTMENT(d.t_department) PREV_DEPARTMENT,
                       '4' MIGRATION_TYPE,
                       cDateMBK DT
                  from ddl_tick_dbt d
                 where d.t_bofficekind in (102, -- Межбанковский кредит
                                           208 -- Межбанковская КЛ
                                          )
                   and cMBK = 1
                   and d.t_dealdate < cDateMBK
                union all
                select genagr.t_genagrid dealID,
                       genagr.t_dockind dealKind,
                       qb_dwh_utils.GetComponentCode('FCT_DEAL',
                                                     qb_dwh_utils.System_RS,
                                                     genagr.t_department,
                                                     genagr.t_genagrid) || '#' ||
                       'GEN' DEAL_CUR_CODE,
                       genagr.t_code,
                       qb_dwh_utils.GetCODE_DEPARTMENT(genagr.t_department) CUR_DEPARTMENT,
                       qb_dwh_utils.GetCODE_DEPARTMENT(genagr.t_department) PREV_DEPARTMENT,
                       '4' MIGRATION_TYPE,
                       cDateMBK DT
                  from ddl_genagr_dbt genagr
                 where genagr.t_dockind = 4611 -- Генеральное соглашение МБК
                   and cMBK = 1
                   and genagr.t_date_genagr < cDateMBK
               union all
                select t.t_id DealId, t.t_Dockind Dockind, -- Сделки драг металы
                       qb_dwh_utils.GetComponentCode('FCT_DEAL',
                                                     qb_dwh_utils.System_RS,
                                                     t.t_department,
                                                     t.t_id) || '#' ||
                       'DVN' DEAL_CUR_CODE,
                       t.t_code DEAL_PREV_CODE,
                       qb_dwh_utils.GetCODE_DEPARTMENT(t.t_department) CUR_DEPARTMENT,
                       qb_dwh_utils.GetCODE_DEPARTMENT(t.t_department) PREV_DEPARTMENT,
                       '4' MIGRATION_TYPE,
                       cDatePrecious_Metals DT
                  from ddvndeal_dbt t
                 inner join ddvnfi_dbt nFI on nFI.t_Type = 0
                                          and nFI.t_DealID = t.t_ID
                 inner join dfininstr_dbt fi on fi.t_fi_kind = 6
                                            and fi.t_fiid = nFI.t_Fiid
                 WHERE t.t_state != 0
                       and cPrecious_Metals = 1
                       and t.t_date < cDatePrecious_Metals
                union all
                select csa.t_csaid dealID, -- Сделки CSA
                       4626 DocKind,
                       qb_dwh_utils.GetComponentCode('FCT_DEAL',
                                                     qb_dwh_utils.System_RS,
                                                     1,
                                                     csa.t_csaid) || '#' ||
                       'CSA' DEAL_CUR_CODE,
                       csa.t_code DEAL_PREV_CODE,
                       qb_dwh_utils.GetCODE_DEPARTMENT(1) CUR_DEPARTMENT,
                       qb_dwh_utils.GetCODE_DEPARTMENT(1) PREV_DEPARTMENT,
                       '4' MIGRATION_TYPE,
                       cDateCSA DT
                  from ddvcsa_dbt csa
                       inner join (select p.t_csaid, p.t_sumpaycur, p.t_direction,  sum(p.t_sumpay) t_sumpay
                                     from ddvcsapm_dbt p
                                    where p.t_direction = 0
                                    group by p.t_csaid, p.t_sumpaycur, p.t_direction) csap on csap.t_csaid = csa.t_csaid
                 where /*exists (
                                select *
                                  from doproper_dbt o
                                       inner join doprdocs_dbt d on d.t_id_operation = o.t_id_operation
                                       inner join dacctrn_dbt t on t.t_acctrnid = d.t_acctrnid and t.t_date_carry<=csa.t_begdate
                                 where o.t_dockind = 4626 and o.t_documentid = lpad(csa.t_csaid,34,'0') )
                       and*/ cCSA = 1
                       and csa.t_begdate < cDateCSA
                   ) loop
      CntDeal := CntDeal + 1;
      -- Запишем сделку по которой начата операция выгрузки
      qb_bp_utils.SetAttrValue(EventID, cDealID, rec.dealid, CntDeal);
      begin
        qb_dwh_utils.ins_ASS_DEAL_MIGRATION(rec.DEAL_CUR_CODE,
                                            rec.DEAL_PREV_CODE,
                                            rec.CUR_DEPARTMENT,
                                            rec.PREV_DEPARTMENT,
                                            rec.MIGRATION_TYPE,
                                            dwhRecStatus,
                                            qb_dwh_utils.DateToChar(rec.DT) /*dwhDT*/,
                                            dwhSysMoment,
                                            dwhEXT_FILE);

      exception
        when others then
          -- пока не останавливаем обработку что бы максимально отследить ошибки, дальше по требованиям заказчика решать будем
          qb_bp_utils.SetError(EventID,
                               SQLCODE,
                               SQLERRM,
                               2,
                               cDealID,
                               rec.dealid);
      end;
    end loop;
    --Завершим выгрузку новых сделок
    qb_bp_utils.EndEvent(EventID, null);
  end;
  ------------------------------------------------------
  -- Процедура добавлениее риска по сделке на основании сделки и даты выгрузки
  ------------------------------------------------------
  procedure Add_FCT_DEALRISK(in_dealId       in number,
                             in_Department   in number,
                             in_Date         in date,
                             in_dwhDeal      in Varchar2,
                             in_dwhRecStatus in Varchar2,
                             in_dwhDT        in Varchar2,
                             in_dwhSysMoment in Varchar2,
                             in_dwhEXT_FILE  in Varchar2) is
    p_CNT         number := 0;
    p_DWH_OVER_DT varchar2(250);
  begin
    for i in (SELECT t.t_id,
                     t.t_dealid,
                     t.t_lnkdate lnkdate,
                     t.t_qualitycategory,
                     t.t_reservepercent,
                     t.t_insmethod,
                     t.t_riseqc,
                     d.t_department,
                     (select min(t1.t_lnkdate)
                        from dmm_qcateg_dbt t1
                       where t1.t_lnkdate > t.t_lnkdate
                         and t1.t_DealID = t.t_DealID) over_date,
                     case
                       when l.t_basis = 1 then
                        0 --360/30
                       when l.t_basis = 2 then
                        2 --360/Act
                       when l.t_basis = 4 then
                        1 --Act/Act
                       when l.t_basis = 8 then
                        40 --365/Act
                       when l.t_basis = 1001 then
                        null --360/31
                     end Rate_Base --select * from dnamealg_dbt v where v.t_itypealg = 2317

                FROM dmm_qcateg_dbt t
               inner join ddl_tick_dbt d on d.t_department = in_Department
                                        and d.t_dealid = t.t_dealid -- выступает как проверка порции по филиалу
               inner join ddl_leg_dbt l on l.t_dealid = d.t_dealid and l.t_legID = 1
               WHERE --(--(in_dealId is null and t.t_lnkdate = in_Date) -- либо все изменения за дату
              --or
              --(t.t_lnkdate = (select max(t.t_lnkdate)
              --                  from dmm_qcateg_dbt t
              --                 where t.t_lnkdate <= in_Date
              --                       and t.t_DealID = in_dealId)
              --and
               t.t_DealID = in_dealId
              --)
              --)
              ) loop
      p_CNT := p_CNT + 1;
      qb_dwh_utils.ins_FCT_DEALRISK(in_dwhDeal,
                                    '254i',
                                    qb_dwh_utils.GetComponentCode('DET_RISK',
                                                                  qb_dwh_utils.System_BISQUIT,
                                                                  i.t_department,
                                                                  i.t_qualitycategory),
                                    qb_dwh_utils.NumberToChar(i.t_reservepercent,
                                                              5,
                                                              1),
                                    i.t_insmethod,
                                    in_dwhRecStatus,
                                    qb_dwh_utils.DateToChar(i.lnkdate), --in_dwhDT,
                                    in_dwhSysMoment,
                                    in_dwhEXT_FILE);
      if i.over_date is not null then
        p_DWH_OVER_DT := qb_dwh_utils.DateToChar(i.over_date);
      end if;

      qb_dwh_utils.ins_FCT_PROCRATE_DEAL(in_dwhDeal,
                                         'PercentRezerv',
                                         null, -- Всегда пусто, согласно коментариев Рогалева
                                         qb_dwh_utils.GetComponentCode('DET_KINDPROCRATE',
                                                                       qb_dwh_utils.System_BISQUIT,
                                                                       i.t_department,
                                                                       i.Rate_Base), -- Беру базу согласно условий сделки
                                         qb_dwh_utils.NumberToChar(i.t_reservepercent,
                                                                   5),
                                         null,
                                         p_DWH_OVER_DT,
                                         qb_dwh_utils.DateToChar(qb_dwh_utils.DT_BEGIN), -- Константа согласно документации
                                         in_dwhRecStatus,
                                         qb_dwh_utils.DateToChar(i.lnkdate),
                                         in_dwhSysMoment,
                                         in_dwhExt_File);
    end loop;
    -- если записей не было добавим насильно
    if p_CNT = 0 then
      for i in (select t.t_dealdate,
                       t.t_department,
                       case
                         when l.t_basis = 1 then
                          0 --360/30
                         when l.t_basis = 2 then
                          2 --360/Act
                         when l.t_basis = 4 then
                          1 --Act/Act
                         when l.t_basis = 8 then
                          40 --365/Act
                         when l.t_basis = 1001 then
                          null --360/31
                       end Rate_Base
                  from ddl_tick_dbt t
                 inner join ddl_leg_dbt l on l.t_dealid = t.t_dealid and l.t_legID = 1
                 where t.t_dealid = in_dealId) loop
        qb_dwh_utils.ins_FCT_DEALRISK(in_dwhDeal,
                                      '254i',
                                      qb_dwh_utils.GetComponentCode('DET_RISK',
                                                                    qb_dwh_utils.System_BISQUIT,
                                                                    i.t_department,
                                                                    1),
                                      qb_dwh_utils.NumberToChar(0, 5, 1),
                                      '',
                                      in_dwhRecStatus,
                                      qb_dwh_utils.DateToChar(i.t_dealdate), --in_dwhDT,
                                      in_dwhSysMoment,
                                      in_dwhEXT_FILE);
        qb_dwh_utils.ins_FCT_PROCRATE_DEAL(in_dwhDeal,
                                           'PercentRezerv',
                                           null, -- Всегда пусто, согласно коментариев Рогалева
                                           qb_dwh_utils.GetComponentCode('DET_KINDPROCRATE',
                                                                         qb_dwh_utils.System_BISQUIT,
                                                                         i.t_department,
                                                                         i.Rate_Base), -- Беру базу согласно условий сделки
                                           qb_dwh_utils.NumberToChar(0, 5),
                                           null,
                                           null,
                                           qb_dwh_utils.DateToChar(qb_dwh_utils.DT_BEGIN), -- Константа согласно документации
                                           in_dwhRecStatus,
                                           qb_dwh_utils.DateToChar(i.t_dealdate),
                                           in_dwhSysMoment,
                                           in_dwhExt_File);
      end loop;
    end if;
  end;
  ------------------------------------------------------
  -- Рекурсивная функция возвращает DealID первичной сделки для пролонгаций, если это не пролонгация то входящий DealId
  ------------------------------------------------------
  function GetFirstDealId(in_dealId            in number,
                          CurrentProlongedDeal number default 0 -- 0-вернет самую верхнюю сделку, 1 - Вернуть текшую пролангированную сделку
                          ) return number is
    out_result number;
    tmp_dealId number;

  begin
    --return in_dealId;
    begin
      select t.t_dealid
        into tmp_dealId
        from doprdocs_dbt d
       inner join doproper_dbt o on o.t_id_operation = d.t_id_operation
       inner join ddl_tick_dbt t on t.t_bofficekind in (102, 208)
                                and t.t_dealid = to_number(o.t_documentid)
       where rownum = 1
         and d.t_dockind in (102, 208)
         and d.t_documentid = lpad(in_dealId, 34, '0');
      if CurrentProlongedDeal = 0 and (tmp_dealId != in_dealId ) then
        out_result := GetFirstDealId(tmp_dealId);
        return out_result;
      else
        return tmp_dealId;
      end if;
    exception
      when no_data_found then
        -- Выходим из процедуры найдя последний первичный документ пролонгаций
        out_result := in_dealId;
        return out_result;
    end;
  end;

  procedure export_carry_by_mcCat(in_DockKind     number,
                                  in_DocID        number,
                                  in_dwhDeal      in Varchar2,
                                  in_dwhRecStatus in Varchar2,
                                  in_dwhDT        in Varchar2,
                                  in_dwhSysMoment in Varchar2,
                                  in_dwhEXT_FILE  in Varchar2) is
    dwhCarryCode       varchar2(250);
    deal_Code_Currency number;
    nat_Code_Currency  number := 0;
    RUR_Code_Currency  number := 0;
    atm_Code_Currency  number := 0;
    p_DWHMigrationDate date;
    vInfo              varchar2(100);

    str_trn varchar2(4000);
    str_tmp varchar2(4000);
    str_id  varchar2(4000);
    str_sum varchar2(4000);
  begin
    p_DWHMigrationDate := qb_dwh_utils.GetDWHMigrationDate;
    for recTMPAcc in (select tmp.DocKind,
                          tmp.DocId DealID,
                          tmp.start_date,
                          tmp.end_date,
                          greatest(tmp.start_date, p_DWHMigrationDate) rest_date,
                          least(tmp.end_date,trunc(sysdate)) - greatest(p_DWHMigrationDate, tmp.start_date + 1) tmp_connect,
                          tmp.t_account,
                          tmp.t_code_currency,
                          tmp.department,
                          tmp.t_chapter,
                          tmp.t_catid,
                          tmp.t_templnum,
                          tmp.uf4
                     from (select distinct t.t_bofficekind DocKind,
                                  t.t_dealid DocId,
                                  decode(l.t_start,
                                         to_date('01.01.0001', 'dd.mm.yyyy'),
                                         t.t_dealdate,
                                         l.t_start) start_date,
                                  l.t_maturity end_date,
                                  mc.t_account,
                                  acc.t_code_currency,
                                  acc.t_chapter,
                                  acc.t_department department,
                                  mc.t_catid,
                                  mc.t_templnum,
                                  --acc.t_userfield4 uf4
                                  case
                                    when (acc.t_account in null) then
                                      mc.t_account
                                    when (acc.t_userfield4 is null) or
                                        (acc.t_userfield4 = chr(0)) or
                                        (acc.t_userfield4 = chr(1)) or
                                        (acc.t_userfield4 like '0x%') then
                                      acc.t_account
                                    else
                                      acc.t_userfield4
                                  end uf4
                             from ddl_tick_dbt t
                            inner join ddl_leg_dbt l on l.t_dealid = t.t_dealid and l.t_legID = 1
                            inner join dmcaccdoc_dbt mc on 0 !=
                                        (select count(1)
                                           from dmcaccdoc_dbt md0
                                          where md0.t_dockind = mc.t_dockind
                                            and qb_dwh_export.GetFirstDealId(md0.t_docid, 0) != qb_dwh_export.GetFirstDealId(mc.t_docid, 0) --
                                            and md0.t_docid != mc.t_docid
                                            and md0.t_catid != 500
                                            and md0.t_activatedate != md0.t_disablingdate
                                            and md0.t_account = mc.t_account) -- Больше 1 привязки
                                    and mc.t_catid != 500
                                    and mc.t_dockind = t.t_bofficekind
                                    and mc.t_docid = t.t_dealid
         left join daccount_dbt acc on acc.t_account = mc.t_account
                            where ((t.t_bofficekind = in_DockKind) or (in_DockKind is null))
                              and ((t.t_dealid = in_DocID) or (in_DocID is null))
                          ) tmp
                          ) loop

      -- определим валюту сделки
      if in_DockKind in (102, 208) then
        select t.t_pfi
          into deal_Code_Currency
          from ddl_tick_dbt t
         where t.t_dealid = recTMPAcc.Dealid;
      elsif in_DockKind = 4611 then
        deal_Code_Currency := null;
      end if;

    for recAcc in (with dt as (select recTMPAcc.DocKind,
                                              recTMPAcc.DealId,
                                              recTMPAcc.start_date,
                                              recTMPAcc.end_date,
                                              recTMPAcc.rest_date + level - 1 rest_date,
                                              recTMPAcc.t_account,
                                              recTMPAcc.t_code_currency,
                                              recTMPAcc.department,
                                              recTMPAcc.t_chapter,
                                              recTMPAcc.t_catid,
                                              recTMPAcc.t_templnum,
                                              recTMPAcc.uf4
                                         from dual
                                       connect by level <= recTMPAcc.Tmp_Connect)
                            select dt.DocKind,
                                   dt.dealID dealID,
                                   dt.start_date,
                                   dt.end_date,
                                   dt.rest_date,
                                   dt.t_account,
                                   dt.t_code_currency,
                                   dt.department,
                                   dt.uf4,
                                   rsb_account.restall(dt.t_account, dt.t_chapter, dt.t_code_currency, dt.rest_date - 1) rest_in,
                                   decode(dt.t_code_currency,
                                          0,
                                          rsb_account.debeta(dt.t_account,
                                                             dt.t_chapter,
                                                             dt.rest_date,
                                                             dt.rest_date),
                                          rsb_account.debetac(dt.t_account,
                                                              dt.t_chapter,
                                                              dt.t_code_currency,
                                                              dt.rest_date,
                                                              dt.rest_date)) dbt_sum,
                                   decode(dt.t_code_currency,
                                          0,
                                          rsb_account.kredita(dt.t_account,
                                                              dt.t_chapter,
                                                              dt.rest_date,
                                                              dt.rest_date),
                                          rsb_account.kreditac(dt.t_account,
                                                               dt.t_chapter,
                                                               dt.t_code_currency,
                                                               dt.rest_date,
                                                               dt.rest_date)) crd_sum,
                                   rsb_account.restall(dt.t_account,
                                                       dt.t_chapter,
                                                       dt.t_code_currency,
                                                       dt.rest_date) rest_out

                              from dt
                             --where acc.t_account = recTMPAcc.t_account
                             --dt.rest_date between p_DWHMigrationDate and least(sysdate, dt.end_date)
                             --order by t_catid, t_templnum, rest_date

                   ) loop

      qb_dwh_utils.ins_FCT_DEAL_RST(in_dwhDeal,
                                    qb_dwh_utils.GetComponentCode('DET_ACCOUNT',
                                                                  qb_dwh_utils.System_IBSO,
                                                                  recAcc.Department,
                                                                  recAcc.uf4),
                                    qb_dwh_utils.numberToChar(recAcc.Rest_In, 3, 1),
                                    qb_dwh_utils.numberToChar(rsb_fiinstr.ConvSum(recAcc.Rest_In,
                                                                                  recAcc.t_Code_Currency,
                                                                                  deal_Code_Currency,
                                                                                  recAcc.Rest_Date), 3, 1),
                                    qb_dwh_utils.numberToChar(rsb_fiinstr.ConvSum(recAcc.Rest_In,
                                                                                  recAcc.t_Code_Currency,
                                                                                  nat_Code_Currency,
                                                                                  recAcc.Rest_Date), 3, 1),
                                    qb_dwh_utils.numberToChar(rsb_fiinstr.ConvSum(recAcc.Rest_In,
                                                                                  recAcc.t_Code_Currency,
                                                                                  rur_Code_Currency,
                                                                                  recAcc.Rest_Date), 3, 1),
                                    qb_dwh_utils.numberToChar(rsb_fiinstr.ConvSum(recAcc.Rest_In,
                                                                                  recAcc.t_Code_Currency,
                                                                                  atm_Code_Currency,
                                                                                  recAcc.Rest_Date), 3, 1),

                                    qb_dwh_utils.numberToChar(recAcc.Dbt_Sum, 3, 1),
                                    qb_dwh_utils.numberToChar(rsb_fiinstr.ConvSum(recAcc.Dbt_Sum,
                                                                                  recAcc.t_Code_Currency,
                                                                                  deal_Code_Currency,
                                                                                  recAcc.Rest_Date), 3, 1),
                                    qb_dwh_utils.numberToChar(rsb_fiinstr.ConvSum(recAcc.Dbt_Sum,
                                                                                  recAcc.t_Code_Currency,
                                                                                  nat_Code_Currency,
                                                                                  recAcc.Rest_Date), 3, 1),
                                    qb_dwh_utils.numberToChar(rsb_fiinstr.ConvSum(recAcc.Dbt_Sum,
                                                                                  recAcc.t_Code_Currency,
                                                                                  rur_Code_Currency,
                                                                                  recAcc.Rest_Date), 3, 1),
                                    qb_dwh_utils.numberToChar(rsb_fiinstr.ConvSum(recAcc.Dbt_Sum,
                                                                                  recAcc.t_Code_Currency,
                                                                                  atm_Code_Currency,
                                                                                  recAcc.Rest_Date), 3, 1),

                                    qb_dwh_utils.numberToChar(recAcc.Crd_Sum, 3, 1),
                                    qb_dwh_utils.numberToChar(rsb_fiinstr.ConvSum(recAcc.Crd_Sum,
                                                                                  recAcc.t_Code_Currency,
                                                                                  deal_Code_Currency,
                                                                                  recAcc.Rest_Date), 3, 1),
                                    qb_dwh_utils.numberToChar(rsb_fiinstr.ConvSum(recAcc.Crd_Sum,
                                                                                  recAcc.t_Code_Currency,
                                                                                  nat_Code_Currency,
                                                                                  recAcc.Rest_Date), 3, 1),
                                    qb_dwh_utils.numberToChar(rsb_fiinstr.ConvSum(recAcc.Crd_Sum,
                                                                                  recAcc.t_Code_Currency,
                                                                                  rur_Code_Currency,
                                                                                  recAcc.Rest_Date), 3, 1),
                                    qb_dwh_utils.numberToChar(rsb_fiinstr.ConvSum(recAcc.Crd_Sum,
                                                                                  recAcc.t_Code_Currency,
                                                                                  atm_Code_Currency,
                                                                                  recAcc.Rest_Date), 3, 1),

                                    qb_dwh_utils.numberToChar(recAcc.Rest_out, 3, 1),
                                    qb_dwh_utils.numberToChar(rsb_fiinstr.ConvSum(recAcc.Rest_out,
                                                                                  recAcc.t_Code_Currency,
                                                                                  deal_Code_Currency,
                                                                                  recAcc.Rest_Date), 3, 1),
                                    qb_dwh_utils.numberToChar(rsb_fiinstr.ConvSum(recAcc.Rest_out,
                                                                                  recAcc.t_Code_Currency,
                                                                                  nat_Code_Currency,
                                                                                  recAcc.Rest_Date), 3, 1),
                                    qb_dwh_utils.numberToChar(rsb_fiinstr.ConvSum(recAcc.Rest_out,
                                                                                  recAcc.t_Code_Currency,
                                                                                  rur_Code_Currency,
                                                                                  recAcc.Rest_Date), 3, 1),
                                    qb_dwh_utils.numberToChar(rsb_fiinstr.ConvSum(recAcc.Rest_out,
                                                                                  recAcc.t_Code_Currency,
                                                                                  atm_Code_Currency,
                                                                                  recAcc.Rest_Date), 3, 1),
                                    in_dwhRecStatus,
                                    qb_dwh_utils.DateToChar(recAcc.Rest_Date), --in_dwhDT,
                                    in_dwhSysMoment,
                                    in_dwhEXT_FILE);
      for rectrn in (select t.t_acctrnid acctrnid,
                            t.t_acctrnid carry_code,
                            null info,
                            t.t_date_carry,
                            t.t_fiid_payer,
                            t.t_fiid_receiver,
                            t.t_account_payer account_payer,
                            --acc_p.t_userfield4 uf4_p,
                            case
                              when (acc_p.t_account in null) then
                                t.t_account_payer
                              when (acc_p.t_userfield4 is null) or
                                  (acc_p.t_userfield4 = chr(0)) or
                                  (acc_p.t_userfield4 = chr(1)) or
                                  (acc_p.t_userfield4 like '0x%') then
                                acc_p.t_account
                              else
                                acc_p.t_userfield4
                            end uf4_p,
                            t.t_account_receiver account_receiver,
                            --acc_r.t_userfield4 uf4_r,
                            case
                              when (acc_r.t_account in null) then
                                t.t_account_receiver
                              when (acc_r.t_userfield4 is null) or
                                  (acc_r.t_userfield4 = chr(0)) or
                                  (acc_r.t_userfield4 = chr(1)) or
                                  (acc_r.t_userfield4 like '0x%') then
                                acc_r.t_account
                              else
                                acc_r.t_userfield4
                            end uf4_r,
                            t.t_sum_payer sum_payer,
                            t.t_sum_receiver sum_receiver,
                            case when t.t_fiid_payer != t.t_fiid_receiver
                              then decode(t.t_fiid_payer, 0, t.t_sum_receiver, t.t_sum_payer)
                              else decode(acc.t_account, t.t_account_payer, t.t_sum_payer,t_sum_receiver)
                            end sum_Account,
                            t.t_sum_natcur sum_natcur,
                            t.t_numb_document DocNum,
                            t.t_Ground Ground,
                            t.t_department department,
                            decode(acc.t_account,
                                   t.t_account_payer,
                                   t.t_sum_payer,
                                   0) as deb_sum,
                            decode(acc.t_account,
                                   t.t_account_receiver,
                                   t.t_sum_receiver,
                                   0) as crd_sum,
                            t.t_number_pack trn_pack,
                            p.t_numberpack paym_pack,
                            p.t_paymentid
                       from daccount_dbt acc
                      inner join dacctrn_dbt t on t.t_state = 1
                                              and /*instr(t.t_userfield4,',') = 0 and */
                                                  t.t_date_carry = recAcc.Rest_Date
                                              and acc.t_account in (t.t_account_payer, t.t_account_receiver)
                                              and instr(t.t_userfield4,'АННУЛ') = 0
                      inner join doprdocs_dbt d on d.t_acctrnid = t.t_acctrnid
                      inner join doproper_dbt o on o.t_dockind = in_DockKind
                                               and o.t_documentid = lpad(recAcc.Dealid, 34, '0')
                                               and d.t_id_operation = o.t_id_operation
                     --inner join dpmpaym_dbt p  on p.t_dockind = o.t_dockind and p.t_documentid = o.t_documentid
                     --inner join doprdocs_dbt d0 on d0.t_dockind =recAcc.Dockind and d0.t_documentid = lpad(p.t_paymentid,34,'0')
                     --inner join doproper_dbt o0 on o0.t_dockind = d0.t_dockind and o0.t_documentid = recAcc.Dealid -- только проводки по текущей сделке
                     --                              and o0.t_id_operation  = d0.t_id_operation
                      left join daccount_dbt acc_p on t.t_accountid_payer = acc_p.t_accountid
                      left join daccount_dbt acc_r on t.t_accountid_receiver = acc_r.t_accountid
                       left outer join dpmdocs_dbt pmdoc on pmdoc.t_acctrnid = t.t_acctrnid
                       left outer join dpmpaym_dbt p on p.t_paymentid = pmdoc.t_paymentid
                      where /*not exists
                      (select 1
                               from ldr_infa.fct_deal_carry lc
                              where lc.code = t.t_acctrnid)
                        and*/ acc.t_account = recacc.t_account
                        and t.t_number_pack != 200
                        and ((t.t_date_carry >= p_DWHMigrationDate and
                            exists
                             (select 1
                                 from ddl_tick_dbt tick
                                where tick.t_bofficekind = o.t_dockind) -- Сделка МБК
                            ) or (t.t_date_carry >= cDatePrecious_Metals and
                            exists
                             (select 1
                                      from ddvndeal_dbt dvndeal
                                     inner join ddvnfi_dbt nFI on nFI.t_Type = 0
                                                              and nFI.t_DealID =
                                                                  dvndeal.t_ID
                                     inner join dfininstr_dbt fi on fi.t_fi_kind = 6
                                                                and fi.t_fiid =
                                                                    nFI.t_Fiid
                                     WHERE dvndeal.t_dockind = o.t_dockind) -- Сделка драг металы
                            ) or (o.t_dockind = 4626 and t.t_date_carry >= cDateCSA) -- CSA
                            )
                            ) loop
        if nvl(rectrn.trn_pack, 0) = 175 then
          for rectmp in (select g.t_extendedground
                           from dextgnd_dbt g
                          where g.t_acctrnid = rectrn.acctrnid) loop
            --id\sum;
            str_trn := rectmp.t_extendedground;
            While (instr(str_trn, ';') > 0) loop
              str_tmp := substr(str_trn, 1, instr(str_trn, ';') - 1);
              str_id  := substr(str_trn, 1, instr(str_trn, '\') - 1);
              str_sum := substr(str_trn, instr(str_trn, '\') + 1);

              dwhCarryCode := qb_dwh_utils.NumberToChar(str_id, 0);
              if (rectrn.t_date_carry < dateMigrCFT) then
                vInfo        := qb_dwh_utils.GetComponentCode('FCT_CARRY_SEPARATE',
                                                              qb_dwh_utils.System_BISQUIT,
                                                              rectrn.Department,
                                                              str_id);
              else
                vInfo        := qb_dwh_utils.GetComponentCode('FCT_CARRY_SEPARATE',
                                                              qb_dwh_utils.System_IBSO,
                                                              rectrn.Department,
                                                              str_id);
              end if;                                                            
              qb_dwh_utils.ins_FCT_DEAL_CARRY(qb_dwh_utils.GetComponentCode('DET_ACCOUNT',
                                                                            qb_dwh_utils.System_IBSO,
                                                                            rectrn.Department,
                                                                            rectrn.uf4_p),
                                              qb_dwh_utils.GetComponentCode('DET_ACCOUNT',
                                                                            qb_dwh_utils.System_IBSO,
                                                                            rectrn.Department,
                                                                            rectrn.uf4_r),
                                              dwhCarryCode,
                                              rectrn.DocNum,
                                              rectrn.Ground,
                                              qb_dwh_utils.NumberToChar(str_sum,3),
                                              qb_dwh_utils.NumberToChar(str_sum,3),
                                              vInfo, --rectrn.info,
                                              in_dwhRecStatus,
                                              qb_dwh_utils.DateToChar(rectrn.t_date_carry), --in_dwhDT,
                                              in_dwhSysMoment,
                                              in_dwhEXT_FILE);
              qb_dwh_utils.ins_FCT_DM_CARRY_ASS(dwhCarryCode,
                                                dwhCarryCode,
                                                in_dwhDeal,
                                                in_dwhRecStatus,
                                                qb_dwh_utils.DateToChar(rectrn.t_date_carry), --in_dwhDT,
                                                in_dwhSysMoment,
                                                in_dwhEXT_FILE);

              str_trn := substr(str_trn, instr(str_trn, ';') + 1);
            end loop;
          end loop;

        else
          -- Для всех проводок
          dwhCarryCode := qb_dwh_utils.NumberToChar(rectrn.carry_code, 0);
          if (rectrn.t_date_carry < dateMigrCFT) then
            vInfo        := qb_dwh_utils.GetComponentCode('FCT_CARRY',
                                                          qb_dwh_utils.System_BISQUIT,
                                                          rectrn.Department,
                                                          rectrn.AccTrnID);
          else
            vInfo        := qb_dwh_utils.GetComponentCode('FCT_CARRY',
                                                          qb_dwh_utils.System_IBSO,
                                                          rectrn.Department,
                                                          rectrn.AccTrnID);
          end if;
            qb_dwh_utils.ins_FCT_DEAL_CARRY(qb_dwh_utils.GetComponentCode('DET_ACCOUNT',
                                                                          qb_dwh_utils.System_IBSO,
                                                                          rectrn.Department,
                                                                          rectrn.uf4_p),
                                            qb_dwh_utils.GetComponentCode('DET_ACCOUNT',
                                                                          qb_dwh_utils.System_IBSO,
                                                                          rectrn.Department,
                                                                          rectrn.uf4_r),
                                          dwhCarryCode,
                                          rectrn.DocNum,
                                          rectrn.Ground,
                                          qb_dwh_utils.NumberToChar(rectrn.sum_Account,3),
                                          qb_dwh_utils.NumberToChar(rectrn.Sum_NatCur, 3),
                                          vInfo,
                                          in_dwhRecStatus,
                                          qb_dwh_utils.DateToChar(rectrn.t_date_carry), --in_dwhDT,
                                          in_dwhSysMoment,
                                          in_dwhEXT_FILE);
          qb_dwh_utils.ins_FCT_DM_CARRY_ASS(dwhCarryCode,
                                            dwhCarryCode,
                                            in_dwhDeal,
                                            in_dwhRecStatus,
                                            qb_dwh_utils.DateToChar(rectrn.t_date_carry), --in_dwhDT,
                                            in_dwhSysMoment,
                                            in_dwhEXT_FILE);
        end if;
      end loop;
      --Раскроем дробления проводок
      for rectrn in (select t.t_acctrnid acctrnid,
                            t.t_acctrnid carry_code, -- что выгружать по покрытию? или если проводка не из модуля
                            null info,
                            t.t_date_carry,
                            t.t_fiid_payer,
                            t.t_fiid_receiver,
                            t.t_account_payer account_payer,
                            --acc_p.t_userfield4 uf4_p,
                            case
                              when (acc_p.t_account in null) then
                                t.t_account_payer
                              when (acc_p.t_userfield4 is null) or
                                  (acc_p.t_userfield4 = chr(0)) or
                                  (acc_p.t_userfield4 = chr(1)) or
                                  (acc_p.t_userfield4 like '0x%') then
                                acc_p.t_account
                              else
                                acc_p.t_userfield4
                            end uf4_p,
                            t.t_account_receiver account_receiver,
                            --acc_r.t_userfield4 uf4_r,
                            case
                              when (acc_r.t_account in null) then
                                t.t_account_receiver
                              when (acc_r.t_userfield4 is null) or
                                  (acc_r.t_userfield4 = chr(0)) or
                                  (acc_r.t_userfield4 = chr(1)) or
                                  (acc_r.t_userfield4 like '0x%') then
                                acc_r.t_account
                              else
                                acc_r.t_userfield4
                            end uf4_r,
                            t.t_sum_payer sum_payer,
                            t.t_sum_receiver sum_receiver,
                            decode(acc.t_account,
                                   t.t_account_payer,
                                   t.t_sum_payer,
                                   t_sum_receiver) sum_Account,
                            t.t_sum_natcur sum_natcur,
                            t.t_numb_document DocNum,
                            t.t_Ground Ground,
                            t.t_department department,
                            decode(acc.t_account,
                                   t.t_account_payer,
                                   t.t_sum_payer,
                                   0) as deb_sum,
                            decode(acc.t_account,
                                   t.t_account_receiver,
                                   t.t_sum_receiver,
                                   0) as crd_sum,
                            t.t_number_pack trn_pack,
                            p.t_numberpack paym_pack,
                            p.t_paymentid
                       from daccount_dbt acc
                      inner join dpmpaym_dbt pi on pi.t_valuedate = recAcc.Rest_Date
                                              and pi.t_numberpack = 175
                                              and acc.t_account in
                                                  (pi.t_payeraccount,
                                                   pi.t_receiveraccount)
                      inner join dpmlink_dbt l on l.t_purposepayment != l.t_initialpayment and l.t_initialpayment = pi.t_paymentid
                      inner join dpmdocs_dbt pmdoc on pmdoc.t_paymentid = l.t_purposepayment
                      inner join dpmpaym_dbt p on p.t_paymentid = pmdoc.t_paymentid
                      inner join dacctrn_dbt t on t.t_acctrnid = pmdoc.t_acctrnid
                      left join daccount_dbt acc_p on t.t_accountid_payer = acc_p.t_accountid
                      left join daccount_dbt acc_r on t.t_accountid_receiver = acc_r.t_accountid
                      where not exists (select 1
                                          from ldr_infa.fct_deal_carry lc
                                         where lc.code = t.t_acctrnid)
                        and acc.t_account = recacc.t_account
                        and ((pi.t_valuedate >= p_DWHMigrationDate and
                            exists
                             (select 1
                                 from ddl_tick_dbt tick
                                where tick.t_bofficekind = pi.t_dockind) -- Сделка МБК
                            ) or (pi.t_valuedate >= cDatePrecious_Metals and
                            exists
                             (select 1
                                      from ddvndeal_dbt dvndeal
                                     inner join ddvnfi_dbt nFI on nFI.t_Type = 0 and nFI.t_DealID = dvndeal.t_ID
                                     inner join dfininstr_dbt fi on fi.t_fi_kind = 6 and fi.t_fiid = nFI.t_Fiid
                                     where dvndeal.t_dockind = pi.t_dockind) -- Сделка драг металы
                            ))) loop
          dwhCarryCode := qb_dwh_utils.NumberToChar(rectrn.carry_code, 0);
          if (rectrn.t_date_carry < dateMigrCFT) then
            vInfo        := qb_dwh_utils.GetComponentCode('FCT_CARRY',
                                                          qb_dwh_utils.System_BISQUIT,
                                                          rectrn.Department,
                                                          rectrn.AccTrnID);
          else
            vInfo        := qb_dwh_utils.GetComponentCode('FCT_CARRY',
                                                          qb_dwh_utils.System_IBSO,
                                                          rectrn.Department,
                                                          rectrn.AccTrnID);
          end if;
          qb_dwh_utils.ins_FCT_DEAL_CARRY(qb_dwh_utils.GetComponentCode('DET_ACCOUNT',
                                                                        qb_dwh_utils.System_IBSO,
                                                                        rectrn.Department,
                                                                        rectrn.uf4_p),
                                          qb_dwh_utils.GetComponentCode('DET_ACCOUNT',
                                                                        qb_dwh_utils.System_IBSO,
                                                                        rectrn.Department,
                                                                        rectrn.uf4_r),
                                          dwhCarryCode,
                                          rectrn.DocNum,
                                          rectrn.Ground,
                                          qb_dwh_utils.NumberToChar(rectrn.sum_Account,
                                                                    3),
                                          qb_dwh_utils.NumberToChar(rectrn.Sum_NatCur,
                                                                    3),
                                          vInfo,
                                          in_dwhRecStatus,
                                          qb_dwh_utils.DateToChar(rectrn.t_date_carry), --in_dwhDT,
                                          in_dwhSysMoment,
                                          in_dwhEXT_FILE);
          qb_dwh_utils.ins_FCT_DM_CARRY_ASS(dwhCarryCode,
                                            dwhCarryCode,
                                            in_dwhDeal,
                                            in_dwhRecStatus,
                                            qb_dwh_utils.DateToChar(rectrn.t_date_carry), --in_dwhDT,
                                            in_dwhSysMoment,
                                            in_dwhEXT_FILE);
      end loop;
    end loop;
    end loop;
  end;

  procedure Add_Ass_CarryDeal(in_DockKind     number,
                              in_DocID        number,
                              in_dwhDeal      in Varchar2,
                              in_dwhRecStatus in Varchar2,
                              in_dwhDT        in Varchar2,
                              in_dwhSysMoment in Varchar2,
                              in_dwhEXT_FILE  in Varchar2) is
    dwhCarryCode       varchar2(250);
    deal_Code_Currency number;
    nat_Code_Currency  number := 0;
    RUR_Code_Currency  number := 0;
    atm_Code_Currency  number := 0;
    p_DWHMigrationDate date;
    str_trn            varchar2(4000);
    str_tmp            varchar2(4000);
    str_id             varchar2(4000);
    str_sum            varchar2(4000);

  begin
    p_DWHMigrationDate := nvl(qb_dwh_utils.GetDWHMigrationDate, to_date('01.01.1900', 'dd.mm.yyyy'));
    for rectrn in (select t.t_acctrnid acctrnid,
                          t.t_numb_document,
                          t.t_date_carry,
                          t.t_sum_payer sum_payer,
                          t.t_sum_receiver sum_receiver,
                          nvl(decode(t.t_userfield4, chr(1), null, t.t_userfield4),
                              decode(p.t_userfield4, chr(1), null, p.t_userfield4)
                             ) t_userfield4,
                          t.t_department department,
                          t.t_number_pack trn_pack,
                          p.t_numberpack paym_pack,
                          p.t_paymentid
                     from doproper_dbt o
                    inner join doprdocs_dbt d on o.t_id_operation = d.t_id_operation
                    inner join doprstep_dbt st on st.t_id_operation = o.t_id_operation and st.t_id_step = d.t_id_step
                    inner join  doprostep_dbt stk on stk.t_name != 'Перенос средств' and stk.t_blockID  = st.t_blockID and stk.t_number_step =  st.t_number_step
                    inner join dacctrn_dbt t on t.t_acctrnid = d.t_acctrnid
                     left outer join dpmdocs_dbt pmdoc on pmdoc.t_acctrnid = t.t_acctrnid
                     left outer join dpmpaym_dbt p on p.t_paymentid = pmdoc.t_paymentid
                    where nvl(instr(nvl(decode(t.t_userfield4, chr(1), null, t.t_userfield4),
                                        decode(p.t_userfield4, chr(1), null, p.t_userfield4)
                                        ),
                              ','),
                              0) = 0
                      and nvl(instr(nvl(decode(t.t_userfield4, chr(1), null, t.t_userfield4),
                                        decode(p.t_userfield4, chr(1), null, p.t_userfield4)
                                        ),
                              'АННУЛ'),
                              0) = 0
                      and ((t.t_date_carry >= p_DWHMigrationDate and exists
                            (select 1
                               from ddl_tick_dbt tick
                              where tick.t_bofficekind = o.t_dockind) -- Сделка МБК
                           ) or (t.t_date_carry >= cDatePrecious_Metals and
                           exists (select 1
                                    from ddvndeal_dbt dvndeal
                                         inner join ddvnfi_dbt nFI on nFI.t_Type = 0 and nFI.t_DealID = dvndeal.t_ID
                                         inner join dfininstr_dbt fi on fi.t_fi_kind = 6 and fi.t_fiid = nFI.t_Fiid
                                   where dvndeal.t_dockind = o.t_dockind) -- Сделка драг металы
                           ) or (o.t_dockind = 4626 and t.t_date_carry >= cDateCSA) -- CSA
                           )
                      and t.t_number_pack != 200
                      and o.t_documentid = lpad(in_DocID, 34, '0')
                      and o.t_dockind = in_DockKind) loop
      if nvl(rectrn.trn_pack, 0) = 175 then

        for rectmp in (select g.t_extendedground --|| ';'
                         from dextgnd_dbt g
                        where g.t_acctrnid = rectrn.acctrnid) loop
          --id\sum;

          str_trn := rectmp.t_extendedground;
          While (instr(str_trn, ';') > 0) loop
            str_tmp      := substr(str_trn, 1, instr(str_trn, ';') - 1);
            str_id       := substr(str_trn, 1, instr(str_trn, '\') - 1);
            str_sum      := substr(str_trn, instr(str_trn, '\') + 1);
            if (rectrn.t_date_carry < dateMigrCFT) then
              dwhCarryCode := qb_dwh_utils.GetComponentCode('FCT_CARRY_SEPARATE',
                                                            qb_dwh_utils.System_BISQUIT,
                                                            rectrn.Department,
                                                            str_id);
            else
              dwhCarryCode := qb_dwh_utils.GetComponentCode('FCT_CARRY_SEPARATE',
                                                            qb_dwh_utils.System_IBSO,
                                                            rectrn.Department,
                                                            str_id);
            end if;

            qb_dwh_utils.ins_ASS_CARRYDEAL(dwhCarryCode,
                                           in_dwhDeal,
                                           in_dwhRecStatus,
                                           qb_dwh_utils.DateToChar(rectrn.t_date_carry),
                                           in_dwhSysMoment,
                                           in_dwhEXT_FILE);

            str_trn := substr(str_trn, instr(str_trn, ';') + 1);
          end loop;

        end loop;/*
        elsif nvl(rectrn.paym_pack,0) = 175 then
           for rectmp in (select * from dpmlink_dbt l where l.t_initialpayment = rectrn.t_paymentid) loop

            dwhCarryCode  := qb_dwh_utils.GetComponentCode ('FCT_CARRY',qb_dwh_utils.System_BISQUIT, rectrn.Department, rectmp.t_purposepayment);

        qb_dwh_utils.ins_ASS_CARRYDEAL(dwhCarryCode,in_dwhDeal,
                                       in_dwhRecStatus,
                                       qb_dwh_utils.DateToChar(rectrn.t_date_carry),
                                       in_dwhSysMoment, in_dwhEXT_FILE);
           end loop;*/

      else
        if (rectrn.t_date_carry < dateMigrCFT) then
          dwhCarryCode := qb_dwh_utils.GetComponentCode('FCT_CARRY',
                                                        qb_dwh_utils.System_BISQUIT,
                                                        rectrn.Department,
                                                        rectrn.AccTrnID);
        else
          dwhCarryCode := qb_dwh_utils.GetComponentCode('FCT_CARRY',
                                                        qb_dwh_utils.System_IBSO,
                                                        rectrn.Department,
                                                        rectrn.AccTrnID);
        end if;
        qb_dwh_utils.ins_ASS_CARRYDEAL(dwhCarryCode,
                                       in_dwhDeal,
                                       in_dwhRecStatus,
                                       qb_dwh_utils.DateToChar(rectrn.t_date_carry),
                                       in_dwhSysMoment,
                                       in_dwhEXT_FILE);
      end if;
    end loop;
    -- Раскроем драбленные платежи
    for rectrn in (select t.t_acctrnid acctrnid,
                          t.t_numb_document,
                          t.t_date_carry,
                          t.t_sum_payer sum_payer,
                          t.t_sum_receiver sum_receiver,
                          nvl(decode(t.t_userfield4, chr(1), null, t.t_userfield4),
                              decode(p.t_userfield4, chr(1), null, p.t_userfield4)
                             ) t_userfield4,
                          t.t_department department,
                          t.t_number_pack trn_pack
                     from dpmlink_dbt l
                    inner join dpmpaym_dbt p on p.t_paymentid = l.t_purposepayment
                    inner join dpmpaym_dbt pi on pi.t_paymentid = l.t_initialpayment
                     left outer join dpmdocs_dbt d on d.t_paymentid = l.t_purposepayment
                     left outer join dacctrn_dbt t on t.t_acctrnid = d.t_acctrnid
                    where l.t_initialpayment != l.t_purposepayment
                      and nvl(instr(nvl(decode(t.t_userfield4, chr(1), null, t.t_userfield4),
                                        decode(p.t_userfield4, chr(1), null, p.t_userfield4)
                                        ),
                                    ','),
                              0) = 0
                      and nvl(instr(nvl(decode(t.t_userfield4, chr(1), null, t.t_userfield4),
                                        decode(p.t_userfield4, chr(1), null, p.t_userfield4)
                                        ),
                                    'АННУЛ'),
                              0) = 0
                      and ((t.t_date_carry >= p_DWHMigrationDate and exists
                           (select 1
                               from ddl_tick_dbt tick
                              where tick.t_bofficekind = pi.t_dockind) -- Сделка МБК
                          ) or (t.t_date_carry >= cDatePrecious_Metals and
                          exists
                           (select 1
                                    from ddvndeal_dbt dvndeal
                                   inner join ddvnfi_dbt nFI on nFI.t_Type = 0
                                                            and nFI.t_DealID =
                                                                dvndeal.t_ID
                                   inner join dfininstr_dbt fi on fi.t_fi_kind = 6
                                                              and fi.t_fiid =
                                                                  nFI.t_Fiid
                                   WHERE dvndeal.t_dockind = pi.t_dockind) -- Сделка драг металы
                          ) or (p.t_dockind = 4626 and t.t_date_carry >= cDateCSA) -- CSA
                          )
                      and t.t_number_pack != 200
                      and pi.t_documentid = lpad(in_DocID, 34, '0')
                      and pi.t_numberpack = 175
                      and pi.t_dockind = in_DockKind) loop
      if (rectrn.t_date_carry < dateMigrCFT) then
        dwhCarryCode := qb_dwh_utils.GetComponentCode('FCT_CARRY',
                                                      qb_dwh_utils.System_BISQUIT,
                                                      rectrn.Department,
                                                      rectrn.AccTrnID);
      else
        dwhCarryCode := qb_dwh_utils.GetComponentCode('FCT_CARRY',
                                                      qb_dwh_utils.System_IBSO,
                                                      rectrn.Department,
                                                      rectrn.AccTrnID);
      end if;
      qb_dwh_utils.ins_ASS_CARRYDEAL(dwhCarryCode,
                                     in_dwhDeal,
                                     in_dwhRecStatus,
                                     qb_dwh_utils.DateToChar(rectrn.t_date_carry),
                                     in_dwhSysMoment,
                                     in_dwhEXT_FILE);
    end loop;
  end;

  procedure Add_Ass_HalfCarryDeal(in_DockKind     number,
                                  in_DocID        number,
                                  in_dwhDeal      in Varchar2,
                                  in_dwhRecStatus in Varchar2,
                                  in_dwhDT        in Varchar2,
                                  in_dwhSysMoment in Varchar2,
                                  in_dwhEXT_FILE  in Varchar2) is
    dwhCarryCode       varchar2(250);
    deal_Code_Currency number;
    nat_Code_Currency  number := 0;
    RUR_Code_Currency  number := 0;
    atm_Code_Currency  number := 0;
    p_DWHMigrationDate date;
    str_Half           varchar2(4000);

  begin
    p_DWHMigrationDate := nvl(qb_dwh_utils.GetDWHMigrationDate,
                              to_date('01.01.1900', 'dd.mm.yyyy'));
    for rectrn in (select t.t_acctrnid acctrnid,
                          t.t_numb_document,
                          t.t_date_carry,
                          t.t_sum_payer sum_payer,
                          t.t_sum_receiver sum_receiver,
                          nvl(decode(t.t_userfield4, chr(1), null, t.t_userfield4),
                              decode(p.t_userfield4, chr(1), null, p.t_userfield4)
                             ) t_userfield4,
                          t.t_department department
                     from doproper_dbt o
                    inner join doprdocs_dbt d on o.t_id_operation = d.t_id_operation
                    inner join doprstep_dbt st on st.t_id_operation = o.t_id_operation and st.t_id_step = d.t_id_step
                    inner join  doprostep_dbt stk on stk.t_name != 'Перенос средств' and stk.t_blockID  = st.t_blockID and stk.t_number_step =  st.t_number_step
                    inner join dacctrn_dbt t on t.t_acctrnid = d.t_acctrnid --and t.t_date_carry >= p_DWHMigrationDate
                     left outer join dpmdocs_dbt pmdoc on pmdoc.t_acctrnid = t.t_acctrnid
                     left outer join dpmpaym_dbt p on p.t_paymentid = pmdoc.t_paymentid
                    where nvl(instr(nvl(decode(t.t_userfield4, chr(1), null, t.t_userfield4),
                                        decode(p.t_userfield4, chr(1), null, p.t_userfield4)
                                        ),
                                    ','),
                              0) > 0
                      and nvl(instr(nvl(decode(t.t_userfield4, chr(1), null, t.t_userfield4),
                                        decode(p.t_userfield4, chr(1), null, p.t_userfield4)
                                        ),
                                    'АННУЛ'),
                              0) = 0
                      and ((t.t_date_carry >= p_DWHMigrationDate and exists
                            (select 1
                               from ddl_tick_dbt tick
                              where tick.t_bofficekind = o.t_dockind) -- Сделка МБК
                           ) or (t.t_date_carry >= cDatePrecious_Metals and
                           exists (select 1
                                    from ddvndeal_dbt dvndeal
                                         inner join ddvnfi_dbt nFI on nFI.t_Type = 0 and nFI.t_DealID = dvndeal.t_ID
                                         inner join dfininstr_dbt fi on fi.t_fi_kind = 6 and fi.t_fiid = nFI.t_Fiid
                                   where dvndeal.t_dockind = o.t_dockind) -- Сделка драг металы
                           ) or (o.t_dockind = 4626) -- CSA
                           )
                      and t.t_number_pack != 200
                      and o.t_documentid = lpad(in_DocID, 34, '0')
                      and o.t_dockind = in_DockKind) loop

      str_Half := rectrn.t_userfield4 || ',';
      While (instr(str_Half, ',') > 0) loop
        if (rectrn.t_date_carry < dateMigrCFT) then
          dwhCarryCode := qb_dwh_utils.GetComponentCode('FCT_HALFCARRY',
                                                        qb_dwh_utils.System_BISQUIT,
                                                        rectrn.Department,
                                                        substr(str_Half,
                                                               1,
                                                               instr(str_Half,
                                                                     ',') - 1));
        else
          dwhCarryCode := qb_dwh_utils.GetComponentCode('FCT_HALFCARRY',
                                                        qb_dwh_utils.System_IBSO,
                                                        rectrn.Department,
                                                        substr(str_Half,
                                                               1,
                                                               instr(str_Half,
                                                                     ',') - 1));
        end if;                                                           

        qb_dwh_utils.ins_ASS_HalfCARRYDEAL(dwhCarryCode,
                                           in_dwhDeal,
                                           in_dwhRecStatus,
                                           qb_dwh_utils.DateToChar(rectrn.t_date_carry),
                                           in_dwhSysMoment,
                                           in_dwhEXT_FILE);
        str_Half := substr(str_Half, instr(str_Half, ',') + 1);
      end loop;
    end loop;
  end;

  procedure Add_DWH_Carry(in_DockKind     number,
                          in_DocID        number,
                          in_dwhRecStatus in Varchar2,
                          in_dwhDT        in Varchar2,
                          in_dwhSysMoment in Varchar2,
                          in_dwhEXT_FILE  in Varchar2) is
    dwhCarryCode       varchar2(250);
    deal_Code_Currency number;
    nat_Code_Currency  number := 0;
    RUR_Code_Currency  number := 0;
    atm_Code_Currency  number := 0;
    p_DWHMigrationDate date;
    str_trn            varchar2(4000);
    str_tmp            varchar2(4000);
    str_id             varchar2(4000);
    str_sum            varchar2(4000);
  begin
    for rec in (select * from table(qb_dwh_export.Get_DWH_Carry(in_DockKind, in_DocID))) loop
      if rec.trn_IsHalfCarry = 0 then
        qb_dwh_utils.ins_ASS_CARRYDEAL(rec.ResultBisquitID,
                                       rec.DWH_Deal_Code,
                                       in_dwhRecStatus,
                                       qb_dwh_utils.DateToChar(rec.trn_Date),
                                       in_dwhSysMoment,
                                       in_dwhEXT_FILE);
      else
        qb_dwh_utils.ins_ASS_HalfCARRYDEAL(rec.ResultBisquitID,
                                           rec.DWH_Deal_Code,
                                           in_dwhRecStatus,
                                           qb_dwh_utils.DateToChar(rec.trn_Date),
                                           in_dwhSysMoment,
                                           in_dwhEXT_FILE);
      end if;
    end loop;
  end;
  ------------------------------------------------------
  --Выгрузка Генеральных соглашений МБК в DWH
  ------------------------------------------------------
  procedure export_GenAgr(in_DocKind      in number,
                          in_DealId       in number,
                          in_Date         in Date,
                          in_dwhRecStatus in Varchar2,
                          in_dwhDT        in Varchar2,
                          in_dwhSysMoment in Varchar2,
                          in_dwhEXT_FILE  in Varchar2) is

    dwhDeal       varchar2(100);
    dwhSubject    varchar2(250);
    dwhDepartment Varchar2(30);
    dwhDealDate   varchar2(100);
  begin
    for rec in cur_GenAgr(in_DocKind, in_DealId) loop
      -- Сгенерируем код компаненты (Код сделки)
      dwhDeal := qb_dwh_utils.GetComponentCode('FCT_DEAL',
                                               qb_dwh_utils.System_RS,
                                               rec.Department,
                                               rec.dwhdealid);
      --dwhSubject    := qb_dwh_utils.GetCODE_SUBJECT (rec.PartyId);
      dwhSubject    := qb_dwh_utils.GetComponentCode('DET_SUBJECT',
                                                     qb_dwh_utils.System_IBSO,
                                                     rec.Department,
                                                     rec.PartyId);
      dwhDepartment := qb_dwh_utils.GetCODE_DEPARTMENT(rec.Department);
      dwhDealDate   := qb_dwh_utils.DateToChar(qb_dwh_utils.NvlBegDate(rec.dealdate));
      -- FCT_DEAL
      qb_dwh_utils.ins_FCT_DEAL(dwhDeal,
                                dwhSubject,
                                dwhDepartment,
                                rec.mb_kind,
                                rec.Deal_Code,
                                rec.Is_Interior,
                                qb_dwh_utils.DateToChar(rec.deal_Start),
                                qb_dwh_utils.DateToChar(rec.deal_End),
                                rec.note,
                                in_dwhRecStatus,
                                dwhDealDate, --in_dwhDT,
                                in_dwhSysMoment,
                                in_dwhEXT_FILE);

      -----------------------------------------------
      -- Связи субъектов
      -----------------------------------------------
      -- Контрагент
      qb_dwh_utils.ins_FCT_SUBJECT_ROLEDEAL(dwhDeal,
                                            qb_dwh_utils.GetComponentCode('DET_SUBJECT',
                                                                          qb_dwh_utils.System_IBSO,
                                                                          rec.Department,
                                                                          rec.partyid),
                                            qb_dwh_utils.GetComponentCode('DET_SUBJECT_ROLEDEAL',
                                                                          qb_dwh_utils.System_IBSO,
                                                                          rec.Department,
                                                                          'КОНТРАГЕНТ'),
                                            null, --in_Is_Agreement, -- "Признак согласия на выгрузку в БКИ" - не требуется, атрибут физ лица
                                            null, --in_DT_Agreement, -- "Признак согласия на выгрузку в БКИ" - не требуется, атрибут физ лица
                                            qb_dwh_utils.DateToChar(rec.deal_start), -- Открытый вопрос 2018-10-28
                                            qb_dwh_utils.DateToChar(rec.deal_end), -- Открытый вопрос 2018-10-28
                                            in_dwhRecStatus,
                                            dwhDealDate,
                                            in_dwhSysMoment,
                                            in_dwhExt_File);

      -----------------------------------------------
      -- Процентные ставки
      -----------------------------------------------
      -- Налог
      /*if rec.tax = chr(88) then
      qb_dwh_utils.ins_FCT_PROCRATE_DEAL(dwhDeal,
                                         'НАЛОГ', -- Необходимо соответствие для налога
                                         null, -- Всегда пусто, согласно коментариев Рогалева
                                         NULL, -- Необходимо соответствие базы для налога
                                         qb_dwh_utils.NumberToChar(rec.tax_rate,5),
                                         null, -- Сумма Отсутствует для Генерального соглашения
                                         null, -- Дата пересмотра ставки Отсутствует для Генерального соглашения
                                         qb_dwh_utils.DateToChar(qb_dwh_utils.DT_BEGIN), -- Константа согласно документации
                                         in_dwhRecStatus, dwhDealDate, in_dwhSysMoment, in_dwhExt_File
                                        );
      end if;*/
      -- Комиссия за выдачу
      if rec.credit_tax = chr(88) then
        qb_dwh_utils.ins_FCT_PROCRATE_DEAL(dwhDeal,
                                           'ВЫД',
                                           null, -- Всегда пусто, согласно коментариев Рогалева
                                           NULL, -- Необходимо соответствие базы для налога
                                           qb_dwh_utils.NumberToChar(rec.credit_tax_rate,
                                                                     5),
                                           null, -- Сумма Отсутствует для Генерального соглашения
                                           null, -- Дата пересмотра ставки Отсутствует для Генерального соглашения
                                           qb_dwh_utils.DateToChar(qb_dwh_utils.DT_BEGIN), -- Константа согласно документации
                                           in_dwhRecStatus,
                                           dwhDealDate,
                                           in_dwhSysMoment,
                                           in_dwhExt_File);
      end if;
      if rec.control_tax = chr(88) then
        -- Комиссия за управление
        qb_dwh_utils.ins_FCT_PROCRATE_DEAL(dwhDeal,
                                           'КРКОМ',
                                           null, -- Всегда пусто, согласно коментариев Рогалева
                                           NULL, -- Необходимо соответствие базы для налога
                                           qb_dwh_utils.NumberToChar(rec.control_tax_rate,
                                                                     5),
                                           null, -- Сумма Отсутствует для Генерального соглашения
                                           null, -- Дата пересмотра ставки Отсутствует для Генерального соглашения
                                           qb_dwh_utils.DateToChar(qb_dwh_utils.DT_BEGIN), -- Константа согласно документации
                                           in_dwhRecStatus,
                                           dwhDealDate,
                                           in_dwhSysMoment,
                                           in_dwhExt_File);
      end if;
      -----------------------------------------------
      -- Счета по сделке
      -----------------------------------------------

      qb_dwh_utils.add_ASS_ACCOUNTDEAL(rec.docKind,
                                                rec.dealid,
                                                in_Date,
                                                dwhDeal,
                                                in_dwhRecStatus,
                                                in_dwhDT,
                                                in_dwhSysMoment,
                                                in_dwhExt_File);
      -----------------------------------------------
      -- Проводки по сделке
      -----------------------------------------------
      /*export_carry_by_mcCat(rec.docKind,
                            rec.dealid,
                            dwhDeal,
                            in_dwhRecStatus,
                            in_dwhDT,
                            in_dwhSysMoment,
                            in_dwhExt_File);*/
      ----------------------------------------------
      -- Добавим зписи по связям проводок со сделкой
      -----------------------------------------------
      Add_DWH_Carry(rec.DocKind,
                        rec.dealid,
                        in_dwhRecStatus,
                        in_dwhDT,
                        in_dwhSysMoment,
                        in_dwhExt_File);
      ------------------------------------------------------
      -- Добавляем данные в ASS_DEAL_CAT_VAL@LDR_INFA (Связь сделки со значением ограниченного доп.атрибута)
      -- на основании категорий учета
      ------------------------------------------------------
      qb_dwh_utils.add_ASS_DEAL_CAT_VAL(rec.dockind,
                                        rec.dealid,
                                        in_Date,
                                        dwhDeal,
                                        in_dwhRecStatus,
                                        in_dwhDT,
                                        in_dwhSysMoment,
                                        in_dwhExt_File);

      ------------------------------------------------------
      --Добавление данных в FCT_DEAL_INDICATOR@LDR_INFA (Значение свободного доп.атрибута сделки)
      -- На основании Примечаний по объекту
      ------------------------------------------------------
      qb_dwh_utils.add_FCT_DEAL_INDICATOR(rec.dockind,
                                          rec.dealid,
                                          in_Date,
                                          dwhDeal,
                                          in_dwhRecStatus,
                                          in_dwhDT,
                                          in_dwhSysMoment,
                                          in_dwhExt_File);
    end loop;
  end;

  ------------------------------------------------------
  -- Инициация Выгрузки Генеральных соглашений
  ------------------------------------------------------
  procedure export_GenAgr_Status_Add(in_UploadID in number,
                                     in_department in number,
                                     in_date       in date) is
    CntDeal      number := 0;
    EventID      number := 0;
    dwhRecStatus varchar2(1);
    dwhDT        varchar2(10);
    dwhSysMoment varchar2(19);
    dwhEXT_FILE  varchar2(300);
  begin
    -- Установим начало выгрузки новых сделок
    qb_bp_utils.startevent(cEvent_EXPORT_MBK_GENAGR, in_UploadID, EventID);

    qb_bp_utils.SetAttrValue(EventID,
                             cAttrRec_Status,
                             qb_dwh_utils.REC_ADD);
    qb_bp_utils.SetAttrValue(EventID, cAttrDepartment, in_department);
    qb_bp_utils.SetAttrValue(EventID, cAttrDT, in_date);

    InitExportData(EventID, dwhRecStatus, dwhDT, dwhSysMoment, dwhEXT_FILE);
    -- Пробежимся по курсору со сделками
    for rec in cur_GenAgr_For_Export(in_department) loop
      CntDeal := CntDeal + 1;
      -- Запишем сделку по которой начата операция выгрузки
      qb_bp_utils.SetAttrValue(EventID, cDealID, rec.dealid, CntDeal);
      begin
        export_GenAgr(rec.dockind,
                      rec.dealid,
                      in_date,
                      dwhRecStatus,
                      dwhDT,
                      dwhSysMoment,
                      dwhEXT_FILE);
      exception
        when others then
          -- пока не останавливаем обработку что бы максимально отследить ошибки, дальше по требованиям заказчика решать будем
          qb_bp_utils.SetError(EventID,
                               SQLCODE,
                               SQLERRM,
                               2,
                               cDealID,
                               rec.dealid);
      end;
    end loop;
    --Завершим выгрузку новых сделок
    qb_bp_utils.EndEvent(EventID, null);
    commit;
  end;
  ------------------------------------------------------
  --Выгрузка Договоров обеспечения в DWH
  ------------------------------------------------------
  procedure export_Ens_Contract(in_DocKind      in number,
                                in_DealId       in number,
                                in_Date         in date,
                                in_dwhRecStatus in Varchar2,
                                in_dwhDT        in Varchar2,
                                in_dwhSysMoment in Varchar2,
                                in_dwhEXT_FILE  in Varchar2) is

    dwhDeal       varchar2(100);
    dwhSubject    varchar2(250);
    dwhDepartment Varchar2(30);
    dwhDealDate   Varchar2(100);
  begin
    for rec in cur_Ens(in_DocKind, in_DealId) loop
      -- Сгенерируем код компаненты (Код сделки)
      dwhDeal := qb_dwh_utils.GetComponentCode('FCT_DEAL',
                                               qb_dwh_utils.System_RS,
                                               rec.Department,
                                               rec.Dwhdealid);
      --dwhSubject    := qb_dwh_utils.GetCODE_SUBJECT (rec.PartyId);
      dwhSubject    := qb_dwh_utils.GetComponentCode('DET_SUBJECT',
                                                     qb_dwh_utils.System_IBSO,
                                                     rec.Department,
                                                     rec.PartyId);
      dwhDepartment := qb_dwh_utils.GetCODE_DEPARTMENT(rec.Department);
      dwhDealDate   := qb_dwh_utils.DateToChar(qb_dwh_utils.NvlBegDate(rec.dealdate));
      -- FCT_DEAL
      qb_dwh_utils.ins_FCT_DEAL(dwhDeal,
                                dwhSubject,
                                dwhDepartment,
                                rec.mb_kind,
                                rec.Deal_Code,
                                rec.Is_Interior,
                                qb_dwh_utils.DateToChar(rec.deal_Start),
                                qb_dwh_utils.DateToChar(rec.deal_End),
                                rec.note,
                                in_dwhRecStatus,
                                dwhDealDate,
                                in_dwhSysMoment,
                                in_dwhEXT_FILE);

      -----------------------------------------------
      -- Связи субъектов
      -----------------------------------------------
      -- Контрагент
      qb_dwh_utils.ins_FCT_SUBJECT_ROLEDEAL(dwhDeal,
                                            qb_dwh_utils.GetComponentCode('DET_SUBJECT',
                                                                          qb_dwh_utils.System_IBSO,
                                                                          rec.Department,
                                                                          rec.partyid),
                                            qb_dwh_utils.GetComponentCode('DET_SUBJECT_ROLEDEAL',
                                                                          qb_dwh_utils.System_IBSO,
                                                                          rec.Department,
                                                                          'ЗАЛОГОДАТЕЛЬ'),
                                            null, --in_Is_Agreement, -- "Признак согласия на выгрузку в БКИ" - не требуется, атрибут физ лица
                                            null, --in_DT_Agreement, -- "Признак согласия на выгрузку в БКИ" - не требуется, атрибут физ лица
                                            qb_dwh_utils.DateToChar(rec.deal_start), -- Открытый вопрос 2018-10-28
                                            qb_dwh_utils.DateToChar(rec.deal_end), -- Открытый вопрос 2018-10-28
                                            in_dwhRecStatus,
                                            qb_dwh_utils.DateToChar(rec.deal_start),
                                            in_dwhSysMoment,
                                            in_dwhExt_File);
      -----------------------------------------------
      -- Счета по сделке
      -----------------------------------------------
      qb_dwh_utils.add_ASS_ACCOUNTDEAL (rec.dockind,
                                        rec.dealid,
                                        in_Date,
                                        dwhDeal,
                                        in_dwhRecStatus,
                                        in_dwhDT,
                                        in_dwhSysMoment,
                                        in_dwhExt_File);
      -----------------------------------------------
      -- Проводки по сделке
      -----------------------------------------------
      /*export_carry_by_mcCat(rec.docKind,
                            rec.dealid,
                            dwhDeal,
                            in_dwhRecStatus,
                            in_dwhDT,
                            in_dwhSysMoment,
                            in_dwhExt_File);*/

      ----------------------------------------------
      -- Добавим зписи по связям проводок со сделкой
      -----------------------------------------------
      Add_DWH_Carry(rec.DocKind,
                        rec.dealid,
                        in_dwhRecStatus,
                        in_dwhDT,
                        in_dwhSysMoment,
                        in_dwhExt_File);

      ------------------------------------------------------
      -- Добавляем данные в ASS_DEAL_CAT_VAL@LDR_INFA (Связь сделки со значением ограниченного доп.атрибута)
      -- на основании категорий учета
      ------------------------------------------------------
      qb_dwh_utils.add_ASS_DEAL_CAT_VAL(rec.dockind,
                                        rec.dealid,
                                        in_Date,
                                        dwhDeal,
                                        in_dwhRecStatus,
                                        in_dwhDT,
                                        in_dwhSysMoment,
                                        in_dwhExt_File);

      ------------------------------------------------------
      --Добавление данных в FCT_DEAL_INDICATOR@LDR_INFA (Значение свободного доп.атрибута сделки)
      -- На основании Примечаний по объекту
      ------------------------------------------------------
      qb_dwh_utils.add_FCT_DEAL_INDICATOR(rec.dockind,
                                          rec.dealid,
                                          in_Date,
                                          dwhDeal,
                                          in_dwhRecStatus,
                                          in_dwhDT,
                                          in_dwhSysMoment,
                                          in_dwhExt_File);

      ------------------------------------------------------
      -- FCT_PROVISIONDEAL (Специфические условия по сделке обеспечения)
      ------------------------------------------------------
      qb_dwh_utils.ins_FCT_PROVISIONDEAL(dwhDeal,
                                         qb_dwh_utils.GetFINSTR_CODE(rec.fiid),
                                         qb_dwh_utils.GetComponentCode('DET_PROVISIONDEAL_TYPE',
                                                                       qb_dwh_utils.System_BISQUIT,
                                                                       rec.Department,
                                                                       rec.provisiondeal_type_code),
                                         qb_dwh_utils.NumberToChar(rec.quality,
                                                                   0),
                                         qb_dwh_utils.NumberToChar(rec.deal_sum,
                                                                   3),
                                         in_dwhRecStatus,
                                         in_dwhSysMoment,
                                         in_dwhExt_File);

      for rec_Ens_Deals in (select --s.t_numsecinorder PROVISIONDEAL_CODE,
                             tt.t_dealId || '#' || 'TCK' CREDITDEAL_CODE,
                             t.t_guarantee PROVISION_SUM,
                             /*s.t_quantity*/
                             null            AMOUNT, --отсутствует разбивка по количеству в рамках сделки
                             c.t_department  department,
                             tt.t_department deal_dep
                              from ddl_order_dbt c
                            --inner join ddl_secur_dbt s on s.t_contractkind = c.t_dockind and s.t_contractid = c.t_contractid
                             inner join dmm_grnt_dbt t on t.t_dealid > 0
                                                      and t.t_contractid =
                                                          c.t_contractid
                             inner join ddl_tick_dbt tt on tt.t_dealid =
                                                           t.t_dealid
                             inner join doprkoper_dbt op_kind on op_kind.t_kind_operation =
                                                                 tt.t_dealtype
                             where c.t_dockind = rec.dockind
                               and c.t_contractid = rec.dealid) loop

        ------------------------------------------------------
        -- ASS_FCT_DEAL
        ------------------------------------------------------
        qb_dwh_utils.ins_ASS_FCT_DEAL(qb_dwh_utils.GetComponentCode('FCT_DEAL',
                                                                    qb_dwh_utils.System_BISQUIT,
                                                                    rec_Ens_Deals.deal_dep,
                                                                    rec_Ens_Deals.CREDITDEAL_CODE),
                                      dwhdeal,
                                      'Provision',
                                      in_dwhRecStatus,
                                      dwhDealDate,--in_dwhDT,
                                      in_dwhSysMoment,
                                      in_dwhEXT_FILE);
        ------------------------------------------------------
        ----FCT_PROVISIONDEAL_CRED_OBJ Связь объекта обеспечения со сделкой и с кредитной сделкой
        ------------------------------------------------------
        qb_dwh_utils.ins_FCT_PROVISIONDEAL_CRED_OBJ(dwhdeal,
                                                    qb_dwh_utils.GetComponentCode('FCT_DEAL',
                                                                                  qb_dwh_utils.System_BISQUIT,
                                                                                  rec_Ens_Deals.deal_dep,
                                                                                  rec_Ens_Deals.CREDITDEAL_CODE),
                                                    rec_Ens_Deals.Provision_Sum,
                                                    rec_Ens_Deals.Amount,
                                                    in_dwhRecStatus,
                                                    in_dwhDT,
                                                    in_dwhSysMoment,
                                                    in_dwhExt_File);

      end loop;

    ------------------------------------------------------
    --FCT_PROVISION_OBJECT (Объект залога)
    ------------------------------------------------------
    -- 26.01.2018 Закоментировал на основании требований Рогалева
    /* for rec_Ens_Obj in (Select s.t_numsecinorder EnsObj_Code,
                                     'STOCKS' TYPEPROVISION_OBJECT_CODE,
                                     null Note, -- не предусмотрено
                                     s.t_valuationdate,
                                     s.t_fiid finStrCode,
                                     s.t_quantity ammount,
                                     s.t_cost balance_value,
                                     null market_value
                                from ddl_secur_dbt s
                               where s.t_contractkind = rec.dockind
                                     and s.t_contractid = rec.dealid
                              ) loop
             qb_dwh_utils.ins_FCT_PROVISION_OBJECT(qb_dwh_utils.GetComponentCode ('FCT_PROVISION_OBJECT',
                                                                                  qb_dwh_utils.System_BISQUIT,
                                                                                  rec.department,
                                                                                  rec_Ens_Obj.Ensobj_Code),
                                                   rec_Ens_Obj.Typeprovision_Object_Code,
                                                   qb_dwh_utils.GetFINSTR_CODE(rec_Ens_Obj.finStrCode),
                                                   qb_dwh_utils.NumberToChar(rec_Ens_Obj.Ammount,3),
                                                   qb_dwh_utils.NumberToChar(rec_Ens_Obj.balance_value,3),
                                                   qb_dwh_utils.NumberToChar(rec_Ens_Obj.market_value,3),
                                                   rec_Ens_Obj.Note,
                                                   in_dwhRecStatus,
                                                   qb_dwh_utils.DateToChar(rec_Ens_Obj.t_Valuationdate),
                                                   in_dwhSysMoment, in_dwhExt_File);
          end loop;*/
    end loop;
  end;

  ------------------------------------------------------
  -- Выгрузка вновь заключенных и пролонгированных сделок
  ------------------------------------------------------
  procedure export_Ens_Contract_Status_Add(in_UploadID in number,
                                           in_department in number,
                                           in_date       in date) is
    CntDeal      number := 0;
    EventID      number := 0;
    dwhRecStatus varchar2(1);
    dwhDT        varchar2(10);
    dwhSysMoment varchar2(19);
    dwhEXT_FILE  varchar2(300);
  begin
    -- Установим начало выгрузки новых сделок
    qb_bp_utils.startevent(cEvent_EXPORT_MBK_ENS_CONTRACT, in_UploadID, EventID);

    qb_bp_utils.SetAttrValue(EventID,
                             cAttrRec_Status,
                             qb_dwh_utils.REC_ADD);
    qb_bp_utils.SetAttrValue(EventID, cAttrDepartment, in_department);
    qb_bp_utils.SetAttrValue(EventID, cAttrDT, in_date);

    InitExportData(EventID, dwhRecStatus, dwhDT, dwhSysMoment, dwhEXT_FILE);
    -- Пробежимся по курсору со сделками
    for rec in cur_Ens_For_Export(in_department) loop
      CntDeal := CntDeal + 1;
      -- Запишем сделку по которой начата операция выгрузки
      qb_bp_utils.SetAttrValue(EventID, cDealID, rec.dealid, CntDeal);
      begin
        export_Ens_Contract(rec.dockind,
                            rec.dealid,
                            in_date,
                            dwhRecStatus,
                            dwhDT,
                            dwhSysMoment,
                            dwhEXT_FILE);
      exception
        when others then
          -- пока не останавливаем обработку что бы максимально отследить ошибки, дальше по требованиям заказчика решать будем
          qb_bp_utils.SetError(EventID,
                               SQLCODE,
                               SQLERRM,
                               2,
                               cDealID,
                               rec.dealid);
      end;
    end loop;
    --Завершим выгрузку новых сделок
    qb_bp_utils.EndEvent(EventID, null);
    commit;
  end;

  -----------------------------------------------
  -- Добавим информацию по графикам
  -----------------------------------------------
  procedure add_GraffData(in_DealId              in number,
                          in_dwhDeal             in varchar2,
                          in_dwhProlongationDeal in varchar2,
                          in_dwhRecStatus        in Varchar2,
                          in_dwhDT               in Varchar2,
                          in_dwhSysMoment        in Varchar2,
                          in_dwhEXT_FILE         in Varchar2) is
    vPrev_DT_Open date;
    vLastCalcDate date;
  begin
    for Rec_Attr in (
                     -- ОД
                      select t_Bofficekind DocKind,
                             t_dealid DealID, --DEAL_CODE
                             t_department Department,
                             10 Purpose,
                             2 TypeRepay_Code,
                             '-1' External_TypeRepay_Code,
                             case
                               when man_graph = 1 then
                                4 --сформированный явным образом
                               when t_graphdealid is null or t_princinendterm = chr(88) then
                                2 --в конце срока
                               else 3 --периодический
                             end TYPESCH,
                             case
                               when man_graph = 1 then
                                'П'
                               when t_graphdealid is null or t_princinendterm = chr(88) then
                                'КС'
                               when t_princ_periodunit = 2 and t_princ_period = 1 then
                                'М'
                               when t_princ_periodunit = 2 and t_princ_period = 3 then
                                'К' 
                               when t_princ_periodunit = 2 and t_princ_period = 6 then
                                'ПГ' 
                               else
                                null
                             end PERIODICITY, -- Код длительности цикла погашения ?????
                             case
                               when man_graph = 0 and t_princinendterm = chr(0) and t_princ_periodunit = 2 then
                                t_princ_period
                               else
                                null
                             end COUNT_PERIOD, -- Длительность цикла погашения.
                             case
                               when man_graph = 0 and t_princinendterm = chr(0) and t_princ_periodunit = 2 then
                                t_princ_period
                               else
                                null
                             end MONTH_PAY,
                             case
                               when man_graph = 0 and t_princinendterm = chr(0) and t_princ_periodunit = 2 then
                                t_princ_month
                               else
                                null
                             end DAY_PAY,
                             case
                               when man_graph = 0 and t_princinendterm = chr(0) and t_princ_periodunit = 2 then
                                case 
                                  when t_adjdatetype != 0 then
                                    '1'
                                  else
                                    '0' 
                                  end
                               else
                                null
                             end IS_WORKDAY,
                             case
                               when t_diff > 0 then
                                2
                               else
                                null
                             end GRACE_PERIODICITY, -- Уточнить
                             case
                               when t_diff > 0 then
                                t_diff
                               else
                                null
                             end GRACE_COUNT_PERIOD,
                             case
                               when man_graph = 0 and (t_graphdealid is null or t_princinendterm = chr(88)) then
                                t_principal
                               else
                                null
                             end SUM_REPAY,
                             t_start DT_OPEN_PER,
                             t_maturity DT_CLOSE_PER,
                             t_dealdate dt --case when g.t_dealid is null then t.t_dealdate end DT
                             from (
                       select t.t_dealid, --DEAL_CODE
                              t.t_department Department,
                              t.t_bofficekind,
                              t.t_dealdate,
                              t.t_department,
                              l.t_principal,
                              l.t_start,
                              l.t_maturity,
                              l.t_diff,
                              g.t_princinendterm,
                              g.t_dealid t_graphdealid,
                              g.t_princ_periodunit,
                              g.t_princ_period,
                              g.t_princ_month,
                              t.t_adjdatetype,
                              nvl((select nvl(1, 0) from dpmpaym_dbt where t_dockind = 102 and t_documentid = t.t_dealid and t_purpose = 10 and chr(t_subkind) = 'M' and rownum = 1), 0) as man_graph  
                              from ddl_tick_dbt t 
                       inner join ddl_leg_dbt l on l.t_dealid = t.t_dealid and l.t_legID = 1
                       left outer join dmmgraphconf_dbt g on g.t_dealid = t.t_dealid -- Отсутствует история
                      where t.t_bofficekind = 102
                        and t.t_dealid = in_DealId)
                     -- %%
                     Union all
                     select t_Bofficekind DocKind,
                             t_dealid DealID, --DEAL_CODE
                             t_department Department,
                             11 Purpose,
                             1 TypeRepay_Code,
                             '-1' External_TypeRepay_Code,
                             case
                               when man_graph = 1 then
                                4 --сформированный явным образом
                               when t_graphdealid is null or t_percinendterm = chr(88) then
                                2 --в конце срока
                               else 3 --периодический
                             end TYPESCH,
                             case
                               when man_graph = 1 then
                                'П'
                               when t_graphdealid is null or t_percinendterm = chr(88) then
                                'КС'
                               when t_perc_periodunit = 2 and t_perc_period = 1 then
                                'М'
                               when t_perc_periodunit = 2 and t_perc_period = 3 then
                                'К' 
                               when t_perc_periodunit = 2 and t_perc_period = 6 then
                                'ПГ' 
                               else
                                null
                             end PERIODICITY, -- Код длительности цикла погашения ?????
                             case
                               when man_graph = 0 and t_percinendterm = chr(0) and t_perc_periodunit = 2 then
                                t_perc_period
                               else
                                null
                             end COUNT_PERIOD, -- Длительность цикла погашения.
                             case
                               when man_graph = 0 and t_percinendterm = chr(0) and t_perc_periodunit = 2 then
                                t_perc_period
                               else
                                null
                             end MONTH_PAY,
                             case
                               when man_graph = 0 and t_percinendterm = chr(0) and t_perc_periodunit = 2 then
                                t_perc_month
                               else
                                null
                             end DAY_PAY,
                             case
                               when man_graph = 0 and t_percinendterm = chr(0) and t_perc_periodunit = 2 then
                                case 
                                  when t_adjdatetype != 0 then
                                    '1'
                                  else
                                    '0' 
                                  end
                               else
                                null
                             end IS_WORKDAY,
                             null GRACE_PERIODICITY, -- Уточнить наличие отстрочки
                             null GRACE_COUNT_PERIOD,
                             case
                               when man_graph = 0 and (t_graphdealid is null or t_percinendterm = chr(88)) then
                                t_cost
                               else
                                null
                             end SUM_REPAY,
                             t_start DT_OPEN_PER,
                             t_maturity DT_CLOSE_PER,
                             t_dealdate dt --case when g.t_dealid is null then t.t_dealdate end DT
                             from (
                       select t.t_dealid, --DEAL_CODE
                              t.t_department Department,
                              t.t_bofficekind,
                              t.t_dealdate,
                              t.t_department,
                              l.t_cost,
                              l.t_start,
                              l.t_maturity,
                              l.t_diff,
                              g.t_percinendterm,
                              g.t_dealid t_graphdealid,
                              g.t_perc_periodunit,
                              g.t_perc_period,
                              g.t_perc_month,
                              t.t_adjdatetype,
                              nvl((select nvl(1, 0) from dpmpaym_dbt where t_dockind = 102 and t_documentid = t.t_dealid and t_purpose = 11 and chr(t_subkind) = 'M' and rownum = 1), 0) as man_graph  
                              from ddl_tick_dbt t 
                       inner join ddl_leg_dbt l on l.t_dealid = t.t_dealid and l.t_legID = 1
                       left outer join dmmgraphconf_dbt g on g.t_dealid = t.t_dealid -- Отсутствует история
                      where t.t_bofficekind = 102
                        and t.t_dealid = in_DealId)) loop
      -- Последняя дата расчета
      select nvl(max(h.t_dater), max(l.t_start))
        into vLastCalcDate
        from ddl_tick_dbt t
       inner join ddl_leg_dbt l on l.t_dealid = t.t_dealid and l.t_legID = 1
        left outer join dpmpaym_dbt p on p.t_purpose = Rec_Attr.Purpose
                                     and p.t_documentid = t.t_dealid
                                     and p.t_dockind = t.t_bofficekind
        left outer join dpmrmprop_dbt pp on pp.t_paymentid = p.t_paymentid
        left outer join dstd_histpaym_dbt h on h.t_paymentid = p.t_paymentid
       where t.t_bofficekind = rec_attr.dockind
         and t.t_dealid = Rec_Attr.Dealid;

          select nvl(max(h.t_dater), max(l.t_start))
            into vPrev_DT_Open
            from ddl_tick_dbt t
           inner join ddl_leg_dbt l on l.t_dealid = t.t_dealid and l.t_legID = 1
            left outer join dpmpaym_dbt p on p.t_purpose = Rec_Attr.Purpose
                                         and p.t_documentid = t.t_dealid
                                         and p.t_dockind = t.t_bofficekind
            left outer join dpmrmprop_dbt pp on pp.t_paymentid = p.t_paymentid
            left outer join dstd_histpaym_dbt h on h.t_dater < vLastCalcDate
                                               and h.t_paymentid = p.t_paymentid
           where t.t_bofficekind = rec_attr.dockind
             and t.t_dealid = Rec_Attr.Dealid;
      -- если не было пересчетов то дату vPrev_DT_Open занулим
      if vLastCalcDate = vPrev_DT_Open then
        vPrev_DT_Open := null;
      end if;
      --FCT_ATTR_SCHEDULE
      qb_dwh_utils.ins_FCT_ATTR_SCHEDULE(nvl(in_dwhProlongationDeal,
                                             in_dwhDeal),
                                         Rec_Attr.Typerepay_Code,
                                         Rec_Attr.External_Typerepay_Code,
                                         Rec_Attr.Typesch,
                                         Rec_Attr.Periodicity,
                                         Rec_Attr.Count_Period,
                                         Rec_Attr.Month_Pay,
                                         Rec_Attr.Day_Pay,
                                         Rec_Attr.Is_Workday,
                                         Rec_Attr.Grace_Periodicity,
                                         Rec_Attr.Grace_Count_Period,
                                         qb_dwh_utils.NumberToChar(Rec_Attr.Sum_Repay, 3),
                                         qb_dwh_utils.DateToChar(Rec_Attr.Dt_Open_Per),
                                         qb_dwh_utils.DateToChar(Rec_Attr.Dt_Close_Per),
                                         in_dwhRecStatus,
                                         qb_dwh_utils.DateToChar(Rec_Attr.Dt_Open_Per), --in_dwhDT,
                                         in_dwhSysMoment,
                                         in_dwhExt_File);
      --FCT_REPAYSCHEDULE_DM  РАСЧЕТНАЯ ДАТА Строка планового графика ? таблица с неактуальными данными
      for rec_Sedule_dm in (select p.t_documentid DEAL_CODE,
                                   p.t_Paymentid  CODE,
                                   p.t_valuedate DT_OPEN,
                                   case
                                     when p.t_paymstatus = 3200 then 3 -- 3 ? фактичесакий,
                                     else 1 -- 1 ? планируемый,
                                   end TYPESCHEDULE,
                                   case
                                     when p.t_purpose = 10 then 2
                                     when p.t_purpose = 11 then 1
                                   end TYPEREPAY_CODE,
                                   case
                                     when t.t_bofficekind = 102 and
                                          instr(op_kind.t_systypes, 'B') > 1 then
                                      2 --привлечение/Депозит
                                     when t.t_bofficekind = 102 and
                                          instr(op_kind.t_systypes, 'S') > 1 then
                                      1 -- размещение/кредит
                                     when t.t_bofficekind = 208 then
                                      1
                                   end MOVINGDIRECTION,
                                   p.t_fiid FINSTR_CODE,
                                   decode(p.t_fiid,
                                          0,
                                          null,
                                          rsb_fiinstr.ConvSum(p.t_amount,
                                                              p.t_fiid,
                                                              0,
                                                              p.t_valuedate)) EVENTSUM,
                                   null FINSTRAMOUNT,
                                   p.t_amount DEALSUM
                              from ddl_tick_dbt t
                             inner join ddl_leg_dbt l on l.t_dealid = t.t_dealid and l.t_legID = 1
                             inner join doprkoper_dbt op_kind on op_kind.t_kind_operation = t.t_dealtype
                             inner join dpmpaym_dbt p on p.t_purpose = Rec_Attr.Purpose
                                                     and p.t_documentid = t.t_dealid
                                                     and p.t_dockind = t.t_bofficekind
                             inner join dpmrmprop_dbt pp on pp.t_paymentid = p.t_paymentid
                             where t.t_bofficekind = rec_attr.dockind
                               and t.t_dealid = Rec_Attr.Dealid) loop

        qb_dwh_utils.ins_FCT_REPAYSCHEDULE_DM(in_dwhDeal,
                                              rec_Sedule_dm.Code || '#' ||
                                              rec_Sedule_dm.Typeschedule,
                                              qb_dwh_utils.DateToChar(rec_Sedule_dm.Dt_Open),
                                              rec_Sedule_dm.Typeschedule,
                                              rec_Sedule_dm.Typerepay_Code,
                                              rec_Sedule_dm.Movingdirection,
                                              qb_dwh_utils.GetFINSTR_CODE(rec_Sedule_dm.Finstr_Code),
                                              qb_dwh_utils.NumberToChar(rec_Sedule_dm.Eventsum,3),
                                              qb_dwh_utils.NumberToChar(rec_Sedule_dm.Finstramount, 3),
                                              qb_dwh_utils.NumberToChar(rec_Sedule_dm.Dealsum, 3),
                                              in_dwhRecStatus,
                                              --in_dwhDT,
                                              in_dwhSysMoment,
                                              in_dwhExt_File);
        -- запишем первую историю в график
        qb_dwh_utils.ins_FCT_REPAYSCHEDULE_H(in_dwhDeal,
                                             rec_Sedule_dm.TypeRepay_Code,
                                             qb_dwh_utils.GetFINSTR_CODE(rec_Sedule_dm.FinStr_Code),
                                             rec_Sedule_dm.Code || '#' ||
                                             rec_Sedule_dm.Typeschedule,
                                             qb_dwh_utils.DateToChar(rec_Sedule_dm.DT_OPEN),
                                             rec_Sedule_dm.TypeSchedule,
                                             rec_Sedule_dm.MovingDirection,
                                             qb_dwh_utils.NumberToChar(rec_Sedule_dm.EventSum, 3),
                                             qb_dwh_utils.NumberToChar(rec_Sedule_dm.FinStrAmount, 3),
                                             qb_dwh_utils.NumberToChar(rec_Sedule_dm.DealSum, 3),
                                             --null,
                                             qb_dwh_utils.DateToChar(vPrev_DT_Open),
                                             in_dwhRecStatus,
                                             qb_dwh_utils.DateToChar(vLastCalcDate), --in_dwhDT,
                                             in_dwhSysMoment,
                                             in_dwhExt_File);
      end loop;

      --FCT_REPAYSCHEDULE_DM  ДАТА Валютирования Строка планового графика ? таблица с неактуальными данными
      for rec_Sedule_dm in (select p.t_documentid DEAL_CODE,
                                   p.t_Paymentid CODE,
                                   decode(GetWorkDayDiff(t.t_dealid,
                                                          p.t_purpose),
                                           0,
                                           p.t_valuedate,
                                           rsi_rsbcalendar.GetDateAfterWorkDay(p.t_valuedate,
                                                                               0))
                                   DT_OPEN,
                                   4 TYPESCHEDULE,
                                   case
                                     when p.t_purpose = 10 then
                                      2
                                     when p.t_purpose = 11 then
                                      1
                                   end TYPEREPAY_CODE,
                                   case
                                     when t.t_bofficekind = 102 and
                                          instr(op_kind.t_systypes, 'B') > 1 then
                                      2 --привлечение/Депозит
                                     when t.t_bofficekind = 102 and
                                          instr(op_kind.t_systypes, 'S') > 1 then
                                      1 -- размещение/кредит
                                     when t.t_bofficekind = 208 then
                                      1
                                   end MOVINGDIRECTION,
                                   p.t_fiid FINSTR_CODE,
                                   decode(p.t_fiid,
                                          0,
                                          null,
                                          rsb_fiinstr.ConvSum(p.t_amount,
                                                              p.t_fiid,
                                                              0,
                                                              p.t_valuedate)) EVENTSUM,
                                   null FINSTRAMOUNT,
                                   p.t_amount DEALSUM
                              from ddl_tick_dbt t
                             inner join ddl_leg_dbt l on l.t_dealid = t.t_dealid and l.t_legID = 1
                             inner join doprkoper_dbt op_kind on op_kind.t_kind_operation = t.t_dealtype
                             inner join dpmpaym_dbt p on p.t_purpose = Rec_Attr.Purpose
                                                     and p.t_documentid = t.t_dealid
                                                     and p.t_dockind = t.t_bofficekind
                             inner join dpmrmprop_dbt pp on pp.t_paymentid = p.t_paymentid
                             where t.t_bofficekind = rec_attr.dockind
                               and t.t_dealid = Rec_Attr.Dealid) loop

        qb_dwh_utils.ins_FCT_REPAYSCHEDULE_DM(in_dwhDeal,
                                              rec_Sedule_dm.Code || '#' ||
                                              rec_Sedule_dm.Typeschedule,
                                              qb_dwh_utils.DateToChar(rec_Sedule_dm.Dt_Open),
                                              rec_Sedule_dm.Typeschedule,
                                              rec_Sedule_dm.Typerepay_Code,
                                              rec_Sedule_dm.Movingdirection,
                                              qb_dwh_utils.GetFINSTR_CODE(rec_Sedule_dm.Finstr_Code),
                                              qb_dwh_utils.NumberToChar(rec_Sedule_dm.Eventsum, 3),
                                              qb_dwh_utils.NumberToChar(rec_Sedule_dm.Finstramount, 3),
                                              qb_dwh_utils.NumberToChar(rec_Sedule_dm.Dealsum, 3),
                                              in_dwhRecStatus,
                                              --in_dwhDT,
                                              in_dwhSysMoment,
                                              in_dwhExt_File);

        -- запишем первую историю в график
        qb_dwh_utils.ins_FCT_REPAYSCHEDULE_H(in_dwhDeal,
                                             rec_Sedule_dm.TypeRepay_Code,
                                             qb_dwh_utils.GetFINSTR_CODE(rec_Sedule_dm.FinStr_Code),
                                             rec_Sedule_dm.Code || '#' ||
                                             rec_Sedule_dm.Typeschedule,
                                             qb_dwh_utils.DateToChar(rec_Sedule_dm.DT_OPEN),
                                             rec_Sedule_dm.TypeSchedule,
                                             rec_Sedule_dm.MovingDirection,
                                             qb_dwh_utils.NumberToChar(rec_Sedule_dm.EventSum, 3),
                                             qb_dwh_utils.NumberToChar(rec_Sedule_dm.FinStrAmount, 3),
                                             qb_dwh_utils.NumberToChar(rec_Sedule_dm.DealSum, 3),
                                             qb_dwh_utils.DateToChar(vPrev_DT_Open),--null
                                             in_dwhRecStatus,
                                             qb_dwh_utils.DateToChar(vLastCalcDate), --in_dwhDT,
                                             in_dwhSysMoment,
                                             in_dwhExt_File);
      end loop;
      -- Запишем историю по графику
      -- Соберем курсор для дат изменения
      for rec_date_h in (select *
                           from (select distinct t.t_dealid, h.t_dater DT
                                   from ddl_tick_dbt t
                                  inner join ddl_leg_dbt l on l.t_dealid = t.t_dealid and l.t_legID = 1
                                  inner join dpmpaym_dbt p on p.t_purpose = Rec_Attr.Purpose
                                                          and p.t_documentid = t.t_dealid
                                                          and p.t_dockind = t.t_bofficekind
                                  inner join dpmrmprop_dbt pp on pp.t_paymentid = p.t_paymentid
                                  inner join dstd_histpaym_dbt h on h.t_dater != l.t_start
                                                                and h.t_paymentid = p.t_paymentid
                                  where t.t_bofficekind = rec_attr.dockind
                                    and t.t_dealid = Rec_Attr.Dealid) tt
                          order by tt.dt desc) loop
        -- теперь уже соберем график с учетом изменений.
        for rec_h in (select p.t_documentid DEAL_CODE,
                             p.t_Paymentid  CODE,
                             nvl(h.t_valuedate, p.t_valuedate) PAY_Date,
                             case
                               when p.t_paymstatus = 3200 then 3 -- 3 ? фактичесакий,
                               else 1 -- 1 ? планируемый,
                             end TYPESCHEDULE,
                             case
                               when p.t_purpose = 10 then 2
                               when p.t_purpose = 11 then 1
                             end TYPEREPAY_CODE,
                             case
                               when t.t_bofficekind = 102 and
                                    instr(op_kind.t_systypes, 'B') > 1 then
                                2 --привлечение/Депозит
                               when t.t_bofficekind = 102 and
                                    instr(op_kind.t_systypes, 'S') > 1 then
                                1 -- размещение/кредит
                               when t.t_bofficekind = 208 then
                                1
                             end MOVINGDIRECTION,
                             p.t_fiid FINSTR_CODE,
                             decode(p.t_fiid,
                                    0,
                                    null,
                                    rsb_fiinstr.ConvSum(nvl(h.t_amount, p.t_amount),
                                                        p.t_fiid,
                                                        0,
                                                        p.t_valuedate)) EVENTSUM,
                             null FINSTRAMOUNT,
                             nvl(h.t_amount, p.t_amount) DEALSUM,
                             l.t_start
                        from ddl_tick_dbt t
                       inner join ddl_leg_dbt l on l.t_dealid = t.t_dealid and l.t_legID = 1
                       inner join doprkoper_dbt op_kind on op_kind.t_kind_operation = t.t_dealtype
                       inner join dpmpaym_dbt p on p.t_purpose = Rec_Attr.Purpose
                                               and p.t_documentid = t.t_dealid
                                               and p.t_dockind = t.t_bofficekind
                       inner join dpmrmprop_dbt pp on pp.t_paymentid = p.t_paymentid
                        left outer join (select *
                                          from dstd_histpaym_dbt h0
                                         where h0.rowid =
                                               (select min(h1.rowid) h_row
                                                  from dstd_histpaym_dbt h1
                                                 where h1.t_paymentid = h0.t_paymentid
                                                   and h1.t_dater = h0.t_dater)
                                        ) h on h.t_dater = rec_date_h.dt
                                               and h.t_paymentid = p.t_paymentid
                       where t.t_bofficekind = rec_attr.dockind
                         and t.t_dealid = Rec_Attr.Dealid) loop

      -- Последняя дата расчета
      select nvl(max(h.t_dater), max(l.t_start))
        into vLastCalcDate
        from ddl_tick_dbt t
       inner join ddl_leg_dbt l on l.t_dealid = t.t_dealid and l.t_legID = 1
        left outer join dpmpaym_dbt p on p.t_purpose = Rec_Attr.Purpose
                                     and p.t_documentid = t.t_dealid
                                     and p.t_dockind = t.t_bofficekind
        left outer join dpmrmprop_dbt pp on pp.t_paymentid = p.t_paymentid
        left outer join dstd_histpaym_dbt h on h.t_dater < rec_date_h.dt
                                               and h.t_paymentid = p.t_paymentid
       where t.t_bofficekind = rec_attr.dockind
         and t.t_dealid = Rec_Attr.Dealid;

          select nvl(max(h.t_dater), max(l.t_start))
            into vPrev_DT_Open
            from ddl_tick_dbt t
           inner join ddl_leg_dbt l on l.t_dealid = t.t_dealid and l.t_legID = 1
            left outer join dpmpaym_dbt p on p.t_purpose = Rec_Attr.Purpose
                                         and p.t_documentid = t.t_dealid
                                         and p.t_dockind = t.t_bofficekind
            left outer join dpmrmprop_dbt pp on pp.t_paymentid = p.t_paymentid
            left outer join dstd_histpaym_dbt h on h.t_dater < vLastCalcDate
                                               and h.t_paymentid = p.t_paymentid
           where t.t_bofficekind = rec_attr.dockind
             and t.t_dealid = Rec_Attr.Dealid;
      -- если не было пересчетов то дату vPrev_DT_Open занулим
      if vLastCalcDate = vPrev_DT_Open then
        vPrev_DT_Open := null;
      end if;
          qb_dwh_utils.ins_FCT_REPAYSCHEDULE_H(in_dwhDeal,
                                               rec_h.TypeRepay_Code,
                                               qb_dwh_utils.GetFINSTR_CODE(rec_h.FinStr_Code),
                                               rec_h.Code || '#' ||
                                               rec_h.Typeschedule,
                                               qb_dwh_utils.DateToChar(rec_h.Pay_Date),
                                               rec_h.TypeSchedule,
                                               rec_h.MovingDirection,
                                               qb_dwh_utils.NumberToChar(rec_h.EventSum, 3),
                                               qb_dwh_utils.NumberToChar(rec_h.FinStrAmount, 3),
                                               qb_dwh_utils.NumberToChar(rec_h.DealSum, 3),
                                               qb_dwh_utils.DateToChar( vPrev_DT_Open ),
                                               in_dwhRecStatus,
                                               qb_dwh_utils.DateToChar( vLastCalcDate ), --in_dwhDT,
                                               in_dwhSysMoment,
                                               in_dwhExt_File);
        end loop;
        null;
      end loop;
    end loop;
  end;

  ------------------------------------------------------
  --Выгрузка сделки МБК в DWH
  ------------------------------------------------------
  procedure export_deal(in_DealId       in number,
                        in_date         in date,
                        in_dwhRecStatus in Varchar2,
                        in_dwhDT        in Varchar2,
                        in_dwhSysMoment in Varchar2,
                        in_dwhEXT_FILE  in Varchar2) is
    dwhDeal             varchar2(100);
    dwhSubject          varchar2(250);
    dwhDepartment       Varchar2(30);
    dwhParentDeal       varchar2(100);
    dwhOrignParentDeal  varchar2(100);
    dwhProlongationDeal varchar2(100);

    --Констатнты из документации
    dwhDEALTYPEMBK varchar2(1) := '1'; -- Обычный МБК
    dwhTYPESYND    varchar2(1) := '1'; -- Не синдицированный кредит
    --
    vParentDealId    number; -- ИД первичной сделки для пролонгации
    dwhDealDate      varchar2(100);
    v_ED_Date        Date;
    Is_not_Roll      number;
    vMovingDirection Varchar2(1);
    dwhProlongationDate varchar2(10);
  begin
    -- Откроем курсор по сделке
    for rec in cur_deal(in_DealId) loop
      -- Сгенерируем код компаненты (Код сделки)
      dwhDeal    := qb_dwh_utils.GetComponentCode('FCT_DEAL',
                                                  qb_dwh_utils.System_BISQUIT,
                                                  rec.Department,
                                                  rec.DWHdealID);
      dwhSubject := qb_dwh_utils.GetComponentCode('DET_SUBJECT',
                                                  qb_dwh_utils.System_IBSO,
                                                  rec.Department,
                                                  rec.PartyId);
      --dwhSubject    := qb_dwh_utils.GetCODE_SUBJECT (rec.PartyId);
      dwhDepartment := qb_dwh_utils.GetCODE_DEPARTMENT(rec.Department);
      dwhDealDate   := qb_dwh_utils.DateToChar(qb_dwh_utils.NvlBegDate(rec.dealdate));

      if rec.isprol = chr(88) then
        --Найдем ИД текущей пролонгации
        /*
        vParentDealId := GetFirstDealId(rec.dealID, 1);

        select count(1)
          into Is_not_Roll
          from ddl_tick_dbt t
         inner join doproper_dbt o on o.t_documentid =
                                      lpad(t.t_dealid, 34, '0')
         inner join doprstep_dbt s on s.t_blockid in (1000009, 1000021)
                                  and s.t_id_operation = o.t_id_operation
         where t.t_dealid = vParentDealId;
        */
        Is_not_Roll := 0; /*Пока нет критерия определения признака, приступать после исправления по заявке RSHBSOFR-1755*/

        if Is_not_Roll > 0 then
          dwhDeal             := dwhDeal || '#PROLONG';
        else
          --vParentDealId :=GetFirstDealId(rec.dealID,1);
          dwhOrignParentDeal := qb_dwh_utils.GetComponentCode('FCT_DEAL',
                                                               qb_dwh_utils.System_BISQUIT,
                                                               rec.Department,
                                                               GetFirstDealId(rec.dealID,0) || '#' ||
                                                               'TCK');
        end if;
      end if;
      -- FCT_DEAL
      qb_dwh_utils.ins_FCT_DEAL(dwhDeal,
                                dwhSubject,
                                dwhDepartment,
                                rec.mb_kind,
                                rec.Deal_Code,
                                rec.Is_Interior,
                                qb_dwh_utils.DateToChar(rec.deal_Start),
                                qb_dwh_utils.DateToChar(rec.deal_End),
                                rec.note,
                                in_dwhRecStatus,
                                dwhDealDate, --in_dwhDT,
                                in_dwhSysMoment,
                                in_dwhEXT_FILE);

      if (rec.mb_kind = 1 and rec.dockind = 102) /*or (rec.mb_kind = 85 and rec.kind_operation = 12310)*/
       then
        -- Межбанковский депозит
        -- FCT_MBKDEPDEAL
        qb_dwh_utils.ins_FCT_MBKDEPDEAL(dwhDeal,
                                        qb_dwh_utils.GetFINSTR_CODE(rec.fiid),
                                        qb_dwh_utils.NumberToChar(rec.deal_sum,
                                                                  3),
                                        dwhDEALTYPEMBK,
                                        in_dwhRecStatus,
                                        in_dwhSysMoment,
                                        in_dwhEXT_FILE);
      elsif (rec.mb_kind = 2 and rec.dockind = 102) /* or (rec.mb_kind = 85 and rec.kind_operation = 12335)*/
       then
        -- Межбанковский кредит
        -- FCT_MBKCREDEAL
        qb_dwh_utils.ins_FCT_MBKCREDEAL(dwhDeal,
                                        qb_dwh_utils.GetFINSTR_CODE(rec.fiid),
                                        qb_dwh_utils.NumberToChar(rec.deal_sum,
                                                                  3),
                                        dwhDEALTYPEMBK,
                                        dwhTYPESYND,
                                        in_dwhRecStatus,
                                        in_dwhSysMoment,
                                        in_dwhEXT_FILE);
      elsif rec.dockind = 208 then
        -- Кредитная линия
        qb_dwh_utils.ins_FCT_CREDITLINEDEAL(dwhDeal,
                                            qb_dwh_utils.GetFINSTR_CODE(rec.t_limitcur),
                                            rec.type_by_limit,
                                            '1',
                                            qb_dwh_utils.NumberToChar(rec.paymentlimit,
                                                                      3),
                                            qb_dwh_utils.NumberToChar(rec.debtlimit,
                                                                      3),
                                            in_dwhRecStatus,
                                            dwhDealDate,
                                            in_dwhSysMoment,
                                            in_dwhEXT_FILE);
      elsif rec.mb_kind in (85, 86) then
        --Запись данных в FCT_LC  (Аккредитивы)
        if rec.kind_operation = 12335 then
          vMovingDirection := 0; --Выданный
        elsif rec.kind_operation = 12310 then
          vMovingDirection := 1; -- полученный
        end if;
        qb_dwh_utils.ins_FCT_LC(dwhDeal, --Code,
                                vMovingDirection, --in_MovingDirection,
                                '2', --in_TypeLC,
                                --null,--in_SubTypeLC,
                                --null,--in_TypeExecute,
                                qb_dwh_utils.NumberToChar(rec.deal_sum, 3), --in_AmountLC,
                                null, --in_NumberLC,
                                '-1', --in_Beneficiary_Code_Subject,
                                '-1', --in_Principal_Code_Subject,
                                '-1', --in_Bank_Issuer_Code_Subject,
                                '-1', --in_Bank_Executor_Code_Subject,
                                qb_dwh_utils.GetFINSTR_CODE(rec.fiid), --in_FinStr_Code,
                                in_dwhRecStatus,
                                dwhDealDate,
                                in_dwhSysMoment,
                                in_dwhEXT_FILE);
      end if;
      --Если сделка пролонгация для другой сделки то запишем об этом информацию
      if rec.isprol = chr(88) then
        --Найдем ИД текущей пролонгации
        vParentDealId := GetFirstDealId(rec.dealID, 1);

        -- сгенерируем код на основании первичной сделки для пролонгации
        select qb_dwh_utils.GetComponentCode('FCT_DEAL',
                                             qb_dwh_utils.System_BISQUIT,
                                             t.t_Department,
                                             t.t_dealid || '#' || 'TCK')
          into dwhParentDeal
          from ddl_tick_dbt t
         where t.t_dealid = vParentDealId;

        if Is_not_Roll > 0 then
          -- ASS_FCT_DEAL 19.01.2019 Сюда не ролл пролонгации
          qb_dwh_utils.ins_ASS_FCT_DEAL(dwhParentDeal,
                                        dwhDeal,
                                        'Prolongation',
                                        in_dwhRecStatus,
                                        dwhDealDate, --in_dwhDT,
                                        in_dwhSysMoment,
                                        in_dwhEXT_FILE);
        else
          -- для ролл пролонгаций
          --Найдем ИД первичной сделки
          /*vParentDealId := GetFirstDealId(rec.dealID, 0);
          -- сгенерируем код на основании первичной сделки для пролонгации
          select qb_dwh_utils.GetComponentCode('FCT_DEAL',
                                               qb_dwh_utils.System_BISQUIT,
                                               t.t_Department,
                                               t.t_dealid || '#' || 'TCK')
            into dwhOrignParentDeal
            from ddl_tick_dbt t
           where t.t_dealid = vParentDealId;*/
          --FCT_PROLONGATION
          dwhProlongationDate := qb_dwh_utils.DateToChar(rec.deal_start);  -- Вместо dwhDealDate, iSupport : #505368
          qb_dwh_utils.ins_FCT_PROLONGATION(dwhParentDeal,
                                            dwhDeal,
                                            qb_dwh_utils.NumberToChar(rec.deal_sum,
                                                                      3),
                                            qb_dwh_utils.NumberToChar(rec.deal_sum_nat,
                                                                      3),
                                            dwhParentDeal,--dwhOrignParentDeal,
                                            dwhDeal, --null,
                                            in_dwhRecStatus,
                                            dwhProlongationDate, --in_dwhDT,
                                            in_dwhSysMoment,
                                            in_dwhEXT_FILE);
        end if;
      end if;

      if rec.parentid is not null then
        -- транш
        -- ASS_FCT_DEAL
        qb_dwh_utils.ins_ASS_FCT_DEAL(qb_dwh_utils.GetComponentCode('FCT_DEAL',
                                                                    qb_dwh_utils.System_BISQUIT,
                                                                    rec.Department,
                                                                    rec.parentid || '#' ||
                                                                    'TCK'),
                                      dwhDeal,
                                      'CreditLine',
                                      in_dwhRecStatus,
                                      dwhDealDate,
                                      in_dwhSysMoment,
                                      in_dwhEXT_FILE);
      end if;

      if rec.genagrid is not null then
        -- Генеральное соглашение
        -- ASS_FCT_DEAL
        qb_dwh_utils.ins_ASS_FCT_DEAL(qb_dwh_utils.GetComponentCode('FCT_DEAL',
                                                                    qb_dwh_utils.System_BISQUIT,
                                                                    rec.Department,
                                                                    rec.genagrid || '#' ||
                                                                    'GEN'),
                                      dwhDeal,
                                      'Agreement',
                                      in_dwhRecStatus,
                                      dwhDealDate,
                                      in_dwhSysMoment,
                                      in_dwhEXT_FILE);
      end if;
      -----------------------------------------------
      -- Выгрузим связи субъектов по сделке
      -----------------------------------------------

      if rec.partyid Is not null and dwhOrignParentDeal is null then
        -- Контрагент
        qb_dwh_utils.ins_FCT_SUBJECT_ROLEDEAL(nvl(dwhOrignParentDeal,
                                                  dwhDeal),
                                              qb_dwh_utils.GetComponentCode('DET_SUBJECT',
                                                                            qb_dwh_utils.System_BISQUIT,
                                                                            rec.Department,
                                                                            rec.partyid),
                                              qb_dwh_utils.GetComponentCode('DET_SUBJECT_ROLEDEAL',
                                                                            qb_dwh_utils.System_IBSO,
                                                                            rec.Department,
                                                                            'КОНТРАГЕНТ'),
                                              null, --in_Is_Agreement, -- "Признак согласия на выгрузку в БКИ" - не требуется, атрибут физ лица
                                              null, --in_DT_Agreement, -- "Признак согласия на выгрузку в БКИ" - не требуется, атрибут физ лица
                                              qb_dwh_utils.DateToChar(rec.dealdate),
                                              qb_dwh_utils.DateToChar(qb_dwh_utils.DT_END),
                                              in_dwhRecStatus,
                                              dwhDealDate,
                                              in_dwhSysMoment,
                                              in_dwhExt_File);
      end if;

      if rec.brokerid Is not null then
        -- брокер
        qb_dwh_utils.ins_FCT_SUBJECT_ROLEDEAL(nvl(dwhOrignParentDeal,
                                                  dwhDeal),
                                              qb_dwh_utils.GetComponentCode('DET_SUBJECT',
                                                                            qb_dwh_utils.System_IBSO,
                                                                            rec.Department,
                                                                            rec.brokerid),
                                              qb_dwh_utils.GetComponentCode('DET_SUBJECT_ROLEDEAL',
                                                                            qb_dwh_utils.System_IBSO,
                                                                            rec.Department,
                                                                            'БРОКЕР'),
                                              null, --in_Is_Agreement, -- "Признак согласия на выгрузку в БКИ" - не требуется, атрибут физ лица
                                              null, --in_DT_Agreement, -- "Признак согласия на выгрузку в БКИ" - не требуется, атрибут физ лица
                                              qb_dwh_utils.DateToChar(rec.dealdate),
                                              qb_dwh_utils.DateToChar(qb_dwh_utils.DT_END),
                                              in_dwhRecStatus,
                                              dwhDealDate,
                                              in_dwhSysMoment,
                                              in_dwhExt_File);
      end if;
      if rec.clientid Is not null then
        -- Клиент
        qb_dwh_utils.ins_FCT_SUBJECT_ROLEDEAL(nvl(dwhOrignParentDeal,
                                                  dwhDeal),
                                              qb_dwh_utils.GetComponentCode('DET_SUBJECT',
                                                                            qb_dwh_utils.System_IBSO,
                                                                            rec.Department,
                                                                            rec.clientid),
                                              qb_dwh_utils.GetComponentCode('DET_SUBJECT_ROLEDEAL',
                                                                            qb_dwh_utils.System_IBSO,
                                                                            rec.Department,
                                                                            'КЛИЕНТ'),
                                              null, --in_Is_Agreement, -- "Признак согласия на выгрузку в БКИ" - не требуется, атрибут физ лица
                                              null, --in_DT_Agreement, -- "Признак согласия на выгрузку в БКИ" - не требуется, атрибут физ лица
                                              qb_dwh_utils.DateToChar(rec.dealdate),
                                              qb_dwh_utils.DateToChar(qb_dwh_utils.DT_END),
                                              in_dwhRecStatus,
                                              dwhDealDate,
                                              in_dwhSysMoment,
                                              in_dwhExt_File);
      end if;
      if rec.traderid Is not null then
        -- Трейдер
        qb_dwh_utils.ins_FCT_SUBJECT_ROLEDEAL(nvl(dwhOrignParentDeal,
                                                  dwhDeal),
                                              qb_dwh_utils.GetComponentCode('DET_SUBJECT',
                                                                            qb_dwh_utils.System_IBSO,
                                                                            rec.Department,
                                                                            rec.traderid),
                                              qb_dwh_utils.GetComponentCode('DET_SUBJECT_ROLEDEAL',
                                                                            qb_dwh_utils.System_IBSO,
                                                                            rec.Department,
                                                                            'ТРАЙДЕР'),
                                              null, --in_Is_Agreement, -- "Признак согласия на выгрузку в БКИ" - не требуется, атрибут физ лица
                                              null, --in_DT_Agreement, -- "Признак согласия на выгрузку в БКИ" - не требуется, атрибут физ лица
                                              qb_dwh_utils.DateToChar(rec.dealdate),
                                              qb_dwh_utils.DateToChar(qb_dwh_utils.DT_END),
                                              in_dwhRecStatus,
                                              dwhDealDate,
                                              in_dwhSysMoment,
                                              in_dwhExt_File);
      end if;
      if rec.depositid Is not null then
        -- Депозитарий
        qb_dwh_utils.ins_FCT_SUBJECT_ROLEDEAL(nvl(dwhOrignParentDeal,
                                                  dwhDeal),
                                              qb_dwh_utils.GetComponentCode('DET_SUBJECT',
                                                                            qb_dwh_utils.System_IBSO,
                                                                            rec.Department,
                                                                            rec.depositid),
                                              qb_dwh_utils.GetComponentCode('DET_SUBJECT_ROLEDEAL',
                                                                            qb_dwh_utils.System_IBSO,
                                                                            rec.Department,
                                                                            'ДЕПОЗИТАРИЙ'),
                                              null, --in_Is_Agreement, -- "Признак согласия на выгрузку в БКИ" - не требуется, атрибут физ лица
                                              null, --in_DT_Agreement, -- "Признак согласия на выгрузку в БКИ" - не требуется, атрибут физ лица
                                              qb_dwh_utils.DateToChar(rec.dealdate),
                                              qb_dwh_utils.DateToChar(qb_dwh_utils.DT_END),
                                              in_dwhRecStatus,
                                              dwhDealDate,
                                              in_dwhSysMoment,
                                              in_dwhExt_File);
      end if;
      if rec.marketid Is not null then
        -- Торговая площадка
        qb_dwh_utils.ins_FCT_SUBJECT_ROLEDEAL(nvl(dwhOrignParentDeal,
                                                  dwhDeal),
                                              qb_dwh_utils.GetComponentCode('DET_SUBJECT',
                                                                            qb_dwh_utils.System_IBSO,
                                                                            rec.Department,
                                                                            rec.marketid),
                                              qb_dwh_utils.GetComponentCode('DET_SUBJECT_ROLEDEAL',
                                                                            qb_dwh_utils.System_IBSO,
                                                                            rec.Department,
                                                                            'ТОРГОВАЯ ПЛОЩАДКА'),
                                              null, --in_Is_Agreement, -- "Признак согласия на выгрузку в БКИ" - не требуется, атрибут физ лица
                                              null, --in_DT_Agreement, -- "Признак согласия на выгрузку в БКИ" - не требуется, атрибут физ лица
                                              qb_dwh_utils.DateToChar(rec.dealdate),
                                              qb_dwh_utils.DateToChar(qb_dwh_utils.DT_END),
                                              in_dwhRecStatus,
                                              dwhDealDate,
                                              in_dwhSysMoment,
                                              in_dwhExt_File);
      end if;

      -----------------------------------------------
      -- Процентные ставки
      -----------------------------------------------
      -- Налог
      if rec.tax = chr(88) then
        qb_dwh_utils.ins_FCT_PROCRATE_DEAL(nvl(dwhOrignParentDeal,
                                               dwhDeal),
                                           'НАЛОГ', -- Необходимо соответствие для налога
                                           null, -- Всегда пусто, согласно коментариев Рогалева
                                           qb_dwh_utils.GetComponentCode('DET_KINDPROCRATE',
                                                                         qb_dwh_utils.System_BISQUIT,
                                                                         rec.Department,
                                                                         rec.Rate_Base), -- Беру базу согласно условий сделки
                                           qb_dwh_utils.NumberToChar(rec.tax_rate,
                                                                     5),
                                           null, -- Сумма Отсутствует для Генерального соглашения
                                           null, -- Дата пересмотра ставки Отсутствует для Генерального соглашения
                                           qb_dwh_utils.DateToChar(qb_dwh_utils.DT_BEGIN), -- Константа согласно документации
                                           in_dwhRecStatus,
                                           dwhDealDate,
                                           in_dwhSysMoment,
                                           in_dwhExt_File);
      end if;
      -- Комиссия за выдачу
      if rec.credit_tax = chr(88) then
        qb_dwh_utils.ins_FCT_PROCRATE_DEAL(nvl(dwhOrignParentDeal,
                                               dwhDeal),
                                           'ВЫД',
                                           null, -- Всегда пусто, согласно коментариев Рогалева
                                           qb_dwh_utils.GetComponentCode('DET_KINDPROCRATE',
                                                                         qb_dwh_utils.System_BISQUIT,
                                                                         rec.Department,
                                                                         rec.Rate_Base), -- Необходимо соответствие базы для налога
                                           qb_dwh_utils.NumberToChar(rec.credit_tax_rate,
                                                                     5),
                                           null, -- Сумма Отсутствует для Генерального соглашения
                                           null, -- Дата пересмотра ставки Отсутствует для Генерального соглашения
                                           qb_dwh_utils.DateToChar(qb_dwh_utils.DT_BEGIN), -- Константа согласно документации
                                           in_dwhRecStatus,
                                           dwhDealDate,
                                           in_dwhSysMoment,
                                           in_dwhExt_File);
      end if;

      -- Найдем дату досрочного расторжения
      v_ED_Date := qb_dwh_utils.Get_Note_Dat(103, rec.dealid, 102);
      if v_ED_Date = to_date('01.01.0001', 'dd.mm.yyyy') then
        v_ED_Date := null;
      end if;
      -----------------------------------------------
      -- Запишим ставки по сделке
      -----------------------------------------------
      for rate_rec in (select r.t_dizm first_dt,
                              LAST_VALUE(r.t_dizm) OVER(ORDER BY r.t_dizm RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) - 1 end_dt,
                              r.t_kor_pro rate
                         from ddl_tick_dbt t
                        inner join DSTD_PLS_DBT r on r.t_id = t.t_dealid
                        where t.t_dealid = rec.dealid) loop
        qb_dwh_utils.ins_FCT_PROCRATE_DEAL(/*nvl(dwhOrignParentDeal,
                                              */ dwhDeal/*)*/,
                                           'Juridic',
                                           null, -- Всегда пусто, согласно коментариев Рогалева
                                           qb_dwh_utils.GetComponentCode('DET_KINDPROCRATE',
                                                                         qb_dwh_utils.System_BISQUIT,
                                                                         rec.Department,
                                                                         rec.Rate_Base), -- Необходимо соответствие базы для налога
                                           qb_dwh_utils.NumberToChar(rate_rec.rate,
                                                                     5),
                                           null, -- Сумма
                                           qb_dwh_utils.DateToChar(rate_rec.end_dt), -- Дата пересмотра ставки
                                           qb_dwh_utils.DateToChar(qb_dwh_utils.DT_BEGIN), -- Константа согласно документации
                                           in_dwhRecStatus,
                                           qb_dwh_utils.DateToChar(rate_rec.first_dt),
                                           in_dwhSysMoment,
                                           in_dwhExt_File);
        -- Если есть досрочное расторжение по сделке запишем отдельно ставку
        if v_ED_Date is not null and v_ED_Date = rate_rec.first_dt then
          qb_dwh_utils.ins_FCT_PROCRATE_DEAL(/*nvl(dwhOrignParentDeal,
                                                 */dwhDeal/*)*/,
                                             'ДепДоср',
                                             null, -- Всегда пусто, согласно коментариев Рогалева
                                             qb_dwh_utils.GetComponentCode('DET_KINDPROCRATE',
                                                                           qb_dwh_utils.System_BISQUIT,
                                                                           rec.Department,
                                                                           rec.Rate_Base), -- Необходимо соответствие базы для налога
                                             qb_dwh_utils.NumberToChar(rate_rec.rate,
                                                                       5),
                                             null, -- Сумма
                                             qb_dwh_utils.DateToChar(rate_rec.end_dt), -- Дата пересмотра ставки
                                             qb_dwh_utils.DateToChar(qb_dwh_utils.DT_BEGIN), -- Константа согласно документации
                                             in_dwhRecStatus,
                                             qb_dwh_utils.DateToChar(rate_rec.first_dt),
                                             in_dwhSysMoment,
                                             in_dwhExt_File);
        end if;
      end loop;
      -----------------------------------------------
      -- Счета по сделке
      -----------------------------------------------
      qb_dwh_utils.add_ASS_ACCOUNTDEAL(rec.dockind,
                                       rec.dealid,
                                       in_date,
                                       dwhDeal,
                                       in_dwhRecStatus,
                                       in_dwhDT,
                                       in_dwhSysMoment,
                                       in_dwhExt_File);
      -----------------------------------------------
      -- Проводки по сделке
      -----------------------------------------------
      /*export_carry_by_mcCat(rec.DocKind,
                            rec.dealid,
                            nvl(dwhOrignParentDeal, dwhDeal),
                            in_dwhRecStatus,
                            in_dwhDT,
                            in_dwhSysMoment,
                            in_dwhExt_File);*/

      ----------------------------------------------
      -- Добавим зписи по связям проводок со сделкой
      -----------------------------------------------
      Add_DWH_Carry(rec.DocKind,
                    rec.dealid,
                    in_dwhRecStatus,
                    in_dwhDT,
                    in_dwhSysMoment,
                    in_dwhExt_File);

      -----------------------------------------------
      -- Установим актупльные данные по риску на момент выгрузки
      -----------------------------------------------
      Add_FCT_DEALRISK(rec.dealID,
                       rec.Department,
                       in_date,
                       /*nvl(dwhOrignParentDeal,*/ dwhDeal/*)*/,
                       in_dwhRecStatus,
                       in_dwhDT,
                       in_dwhSysMoment,
                       in_dwhExt_File);

      ------------------------------------------------------
      -- Добавляем данные в ASS_DEAL_CAT_VAL@LDR_INFA (Связь сделки со значением ограниченного доп.атрибута)
      -- на основании категорий учета
      ------------------------------------------------------
      qb_dwh_utils.add_ASS_DEAL_CAT_VAL(rec.dockind,
                                        rec.dealid,
                                        in_Date,
                                        /*nvl(dwhOrignParentDeal,*/ dwhDeal/*)*/,
                                        in_dwhRecStatus,
                                        in_dwhDT,
                                        in_dwhSysMoment,
                                        in_dwhExt_File);

      ------------------------------------------------------
      --Добавление данных в FCT_DEAL_INDICATOR@LDR_INFA (Значение свободного доп.атрибута сделки)
      -- На основании Примечаний по объекту
      ------------------------------------------------------
      qb_dwh_utils.add_FCT_DEAL_INDICATOR(rec.dockind,
                                          rec.dealid,
                                          in_Date,
                                          /*nvl(dwhOrignParentDeal,*/ dwhDeal/*)*/,
                                          in_dwhRecStatus,
                                          in_dwhDT,
                                          in_dwhSysMoment,
                                          in_dwhExt_File);
      ------------------------------------------------------
      --Добавим графики
      ------------------------------------------------------
      add_GraffData(rec.dealid,
                    dwhDeal,
                    null,--dwhOrignParentDeal,
                    in_dwhRecStatus,
                    in_dwhDT,
                    in_dwhSysMoment,
                    in_dwhExt_File);
    commit;
    end loop;
  end;

  ------------------------------------------------------
  -- Выгрузка вновь заключенных и пролонгированных сделок
  ------------------------------------------------------
  procedure export_Deals_Status_Add(in_UploadID in number,
                                    in_department in number,
                                    in_date       in date) is
    CntDeal      number := 0;
    EventID      number := 0;
    dwhRecStatus varchar2(1);
    dwhDT        varchar2(10);
    dwhSysMoment varchar2(19);
    dwhEXT_FILE  varchar2(300);
  begin
    -- Установим начало выгрузки новых сделок
    qb_bp_utils.startevent(cEvent_EXPORT_MBK_DEALS, in_UploadID, EventID);

    qb_bp_utils.SetAttrValue(EventID,
                             cAttrRec_Status,
                             qb_dwh_utils.REC_ADD);
    qb_bp_utils.SetAttrValue(EventID, cAttrDepartment, in_department);
    qb_bp_utils.SetAttrValue(EventID, cAttrDT, in_date);

    InitExportData(EventID, dwhRecStatus, dwhDT, dwhSysMoment, dwhEXT_FILE);
    -- Пробежимся по курсору со сделками
    for rec in cur_DealsForExport(in_department, in_date) loop
      CntDeal := CntDeal + 1;
      -- Запишем сделку по которой начата операция выгрузки
      qb_bp_utils.SetAttrValue(EventID, cDealID, rec.dealid, CntDeal);
      begin
        export_deal(rec.dealid,
                    in_date,
                    dwhRecStatus,
                    dwhDT,
                    dwhSysMoment,
                    dwhEXT_FILE);
      exception
        when others then
          -- пока не останавливаем обработку что бы максимально отследить ошибки, дальше по требованиям заказчика решать будем
          qb_bp_utils.SetError(EventID,
                               SQLCODE,
                               SQLERRM,
                               2,
                               cDealID,
                               rec.dealid);
      end;
      commit;
    end loop;
    --Завершим выгрузку новых сделок
    qb_bp_utils.EndEvent(EventID, null);
  end;

  ------------------------------------------------------
  -- Выгрузка вновь заключенных и пролонгированных сделок
  ------------------------------------------------------
  procedure export_Dict_Status_Add(in_UploadID in number,
                                   in_department in number,
                                   in_date       in date) is
    EventID      number := 0;
    dwhRecStatus varchar2(1);
    dwhDT        varchar2(10);
    dwhSysMoment varchar2(19);
    dwhEXT_FILE  varchar2(300);
  begin
    -- Установим начало выгрузки новых сделок
    qb_bp_utils.startevent(cEvent_EXPORT_MBK_Dict, in_UploadID, EventID);

    qb_bp_utils.SetAttrValue(EventID,
                             cAttrRec_Status,
                             qb_dwh_utils.REC_ADD);
    qb_bp_utils.SetAttrValue(EventID, cAttrDepartment, in_department);
    qb_bp_utils.SetAttrValue(EventID, cAttrDT, in_date);

    InitExportData(EventID, dwhRecStatus, dwhDT, dwhSysMoment, dwhEXT_FILE);
    -- Выгрузка DET_DEAL_CAT
    begin
      qb_dwh_utils.add_DET_DEAL_CAT(103,
                                    dwhRecStatus,
                                    dwhDT,
                                    dwhSysMoment,
                                    dwhEXT_FILE);

      qb_dwh_utils.add_DET_DEAL_CAT(126,
                                    dwhRecStatus,
                                    dwhDT,
                                    dwhSysMoment,
                                    dwhEXT_FILE);

      qb_dwh_utils.add_DET_DEAL_CAT(4626,
                                    dwhRecStatus,
                                    dwhDT,
                                    dwhSysMoment,
                                    dwhEXT_FILE);
    exception
      when others then
        qb_bp_utils.SetError(EventID,
                             SQLCODE,
                             SQLERRM,
                             2,
                             cDict,
                             'DET_DEAL_CAT');
    end;
    -- Выгрузка DET_DEAL_TYPEATTR
    begin
      qb_dwh_utils.add_DET_DEAL_TYPEATTR(103,
                                         dwhRecStatus,
                                         dwhDT,
                                         dwhSysMoment,
                                         dwhEXT_FILE);

    exception
      when others then
        qb_bp_utils.SetError(EventID,
                             SQLCODE,
                             SQLERRM,
                             2,
                             cDict,
                             'DET_DEAL_TYPEATTR');
    end;
    -- Выгрузка DET_DEPARTMENT
    begin
      qb_dwh_utils.add_DET_DEPARTMENT(dwhRecStatus,
                                      dwhDT,
                                      dwhSysMoment,
                                      dwhEXT_FILE);
    exception
      when others then
        qb_bp_utils.SetError(EventID,
                             SQLCODE,
                             SQLERRM,
                             2,
                             cDict,
                             'DET_DEPARTMENT');
    end;
    -- Выгрузка DET_MEASUREMENT_UNIT
    begin
      qb_dwh_utils.add_DET_MEASUREMENT_UNIT(dwhRecStatus,
                                            dwhDT,
                                            dwhSysMoment,
                                            dwhEXT_FILE);
    exception
      when others then
        qb_bp_utils.SetError(EventID,
                             SQLCODE,
                             SQLERRM,
                             2,
                             cDict,
                             'DET_MEASUREMENT_UNIT');
    end;
    -- Выгрузка DET_RISK
    begin
      qb_dwh_utils.add_DET_RISK(dwhRecStatus,
                                dwhDT,
                                dwhSysMoment,
                                dwhEXT_FILE);
    exception
      when others then
        qb_bp_utils.SetError(EventID,
                             SQLCODE,
                             SQLERRM,
                             2,
                             cDict,
                             'DET_RISK');
    end;
    -- Выгрузка DET_PROCBASE
    begin
      qb_dwh_utils.add_DET_PROCBASE(dwhRecStatus,
                                    dwhDT,
                                    dwhSysMoment,
                                    dwhEXT_FILE);
    exception
      when others then
        qb_bp_utils.SetError(EventID,
                             SQLCODE,
                             SQLERRM,
                             2,
                             cDict,
                             'DET_PROCBASE');
    end;
    -- Выгрузка DET_PROVISIONDEAL_TYPE
    begin
      qb_dwh_utils.add_DET_PROVISIONDEAL_TYPE(dwhRecStatus,
                                              dwhDT,
                                              dwhSysMoment,
                                              dwhEXT_FILE);
    exception
      when others then
        qb_bp_utils.SetError(EventID,
                             SQLCODE,
                             SQLERRM,
                             2,
                             cDict,
                             'DET_PROVISIONDEAL_TYPE');
    end;

    -- Выгрузка DET_SUBJECT_ROLEDEAL
    begin
      qb_dwh_utils.add_DET_SUBJECT_ROLEDEAL(dwhRecStatus,
                                            dwhDT,
                                            dwhSysMoment,
                                            dwhEXT_FILE);
    exception
      when others then
        qb_bp_utils.SetError(EventID,
                             SQLCODE,
                             SQLERRM,
                             2,
                             cDict,
                             'DET_SUBJECT_ROLEDEAL');
    end;

    -- Выгрузка DET_SYSTEM
    begin
      qb_dwh_utils.add_DET_SYSTEM(dwhRecStatus,
                                  dwhDT,
                                  dwhSysMoment,
                                  dwhEXT_FILE);
    exception
      when others then
        qb_bp_utils.SetError(EventID,
                             SQLCODE,
                             SQLERRM,
                             2,
                             cDict,
                             'DET_SYSTEM');
    end;
    --add_DET_ROLEACCOUNT_DEAL
    begin
      qb_dwh_utils.add_DET_ROLEACCOUNT_DEAL(dwhRecStatus,
                                            dwhDT,
                                            dwhSysMoment,
                                            dwhEXT_FILE);
    exception
      when others then
        qb_bp_utils.SetError(EventID,
                             SQLCODE,
                             SQLERRM,
                             2,
                             cDict,
                             'DET_ROLEACCOUNT_DEAL');
    end;
    --Завершим выгрузку новых сделок
    qb_bp_utils.EndEvent(EventID, null);
    commit;
  end;

  /** <font color=teal><b>Процедура добавления связей ASS_ACCOUNTDEAL (Связь сделки со счетом) на основании привязанных к документу Категорий Учета </b></font>
  *   @param in_Kind_DocID  Входящий параметр. Вид документа к которому привязаны категории учета
  *   @param in_DocID       Входящий параметр. ID документа к которому привязаны категории учета
  *   @param in_Date        Дата последнего события с привязкой категории учета
  *   @param in_dwhDeal     Код сдежки используемый при генерации Кода сделки для DWH( IDсделки # Тип сделки)
  *   @param in_Rec_Status  Тип учетного события REC_ADD/REC_CLOSED/REC_DELETE
  *   @param in_DT          Дата учетного события
  *   @param in_SysMoment   Момент (дата и время с точностью до секунды), когда началось формирование порции.
  *   @param in_Ext_File    Идентификатор порции данных, в рамках которой была выгружена рассматриваемая строка
  */
  procedure add_ASS_ACCOUNTDEAL_By_PM (in_Kind_DocID in number,
                                          in_DocID      in number,
                                          in_Date       in date,
                                          in_dwhDeal    in varchar2,
                                          in_Rec_Status in varchar2,
                                          in_DT         in varchar2,
                                          in_SysMoment  in varchar2,
                                          in_Ext_File   in varchar2
                                         ) is
    begin
      -- Пройдемся по всем изменениям в привязках за день
      for i in (with deal as
                     (select /*+ materialize*/ DVN.T_DATE
                            ,DVN.T_ID
                            ,T.T_ACCOUNT_PAYER
                            ,T.T_ACCOUNT_RECEIVER
                      from ddvndeal_dbt dvn
                           inner join ddvnfi_dbt nFI on nFI.t_Type = 0 and nFI.t_DealID = dvn.t_ID
                           inner join doproper_dbt o on o.t_documentid = lpad(dvn.t_id, 34, '0')
                                                        and o.t_dockind = dvn.t_dockind
                           inner join doprdocs_dbt d on o.t_id_operation = d.t_id_operation
                           inner join dacctrn_dbt t on t.t_acctrnid = d.t_acctrnid
                       where dvn.t_dockind = in_Kind_DocID
                       and dvn.t_id = in_DocID
                      ),
                  deal_acc as
                     (select  t.T_DATE
                            ,t.T_ID
                            ,T.T_ACCOUNT_PAYER as account
                        from deal t
                        union 
                        select t.T_DATE
                            ,t.T_ID
                            ,T.T_ACCOUNT_RECEIVER
                        from deal t
                      )
                  select /*+ ordered use_nl(dc)*/ distinct c.t_id,
                       c.t_number,
                       c.t_code,
                       --acc.t_userfield4 t_account,--dc.t_account,
                       case
                          when (acc.t_userfield4 is null) or
                              (acc.t_userfield4 = chr(0)) or
                              (acc.t_userfield4 = chr(1)) or
                              (acc.t_userfield4 like '0x%') then
                            acc.t_account
                          else
                            acc.t_userfield4
                       end t_account,
                       greatest (dc.t_activatedate,t.t_date) t_activatedate,
                       case when (dc.t_disablingdate = to_date('01.01.0001','dd.mm.yyyy')
                                  and trunc (sysdate) >= (select max(nFI0.t_Paydate)
                                                            from ddvnfi_dbt nFI0
                                                           where nFI0.t_DealID = t.t_ID))
                                 or
                                 (dc.t_disablingdate != to_date('01.01.0001','dd.mm.yyyy')
                                  and dc.t_disablingdate >= (select max(nFI0.t_Paydate)
                                                               from ddvnfi_dbt nFI0
                                                              where nFI0.t_DealID = t.t_ID)
                                 )
                            then (select max(nFI0.t_Paydate)
                                    from ddvnfi_dbt nFI0
                                   where nFI0.t_DealID = t.t_ID)
                            when
                                 (dc.t_disablingdate != to_date('01.01.0001','dd.mm.yyyy')
                                  and dc.t_disablingdate <= (select max(nFI0.t_Paydate)
                                                               from ddvnfi_dbt nFI0
                                                              where nFI0.t_DealID = t.t_ID)
                                 )
                            then dc.t_disablingdate
                            else to_date('01.01.0001','dd.mm.yyyy')
                       end t_disablingdate,
                       greatest (dc.t_actiondate,t.t_date) t_actiondate,
                       dc.t_Departmentid Department
                  from deal_acc t
                  /*from ddvndeal_dbt dvn
                       inner join ddvnfi_dbt nFI on nFI.t_Type = 0 and nFI.t_DealID = dvn.t_ID
                       inner join doproper_dbt o on o.t_documentid = lpad(dvn.t_id, 34, '0')
                                                    and o.t_dockind = dvn.t_dockind
                       inner join doprdocs_dbt d on o.t_id_operation = d.t_id_operation
                       inner join dacctrn_dbt t on t.t_acctrnid = d.t_acctrnid */
                  inner join dmcaccdoc_dbt dc on dc.t_account  = t.account --in (t.t_account_payer, t.t_account_receiver)
                                                      and dc.t_dockind = 0
                                                      --and dc.t_contractor = dvn.t_contractor
                  inner join dmccateg_dbt c on c.t_id = dc.t_catid
                  inner join daccount_dbt acc on acc.t_account = dc.t_account
                  /*where dvn.t_dockind = in_Kind_DocID
                       and dvn.t_id = in_DocID */ ) loop
          -- Выгрузка новых связей
          qb_dwh_utils.ins_ASS_ACCOUNTDEAL (qb_dwh_utils.GetComponentCode ('DET_ACCOUNT',qb_dwh_utils.System_IBSO, i.Department, i.t_account),
                               in_dwhDeal,
                               qb_dwh_utils.GetRoleAccount_Deal_Code(i.t_id),
                               qb_dwh_utils.REC_ADD,
                               qb_dwh_utils.DateToChar(i.t_activatedate),
                               in_SysMoment, in_Ext_File,
                               -- KS 23.02.2022 Закрытие действующих связей должно осуществляться через поле ASS_ACCOUNTDEAL.DT_END
                               case i.t_disablingdate
                                 when to_date('01.01.0001','dd.mm.yyyy') then qb_dwh_utils.DateToChar(qb_dwh_utils.DT_END)
                                 else qb_dwh_utils.DateToChar(i.t_disablingdate-1)
                               end
                              );
        /*
        if i.t_disablingdate != to_date('01.01.0001','dd.mm.yyyy')  then
          -- Закрытие действующих связий
          qb_dwh_utils.ins_ASS_ACCOUNTDEAL (qb_dwh_utils.GetComponentCode ('DET_ACCOUNT',qb_dwh_utils.System_IBSO, i.Department, i.t_account),
                               in_dwhDeal,
                               qb_dwh_utils.GetRoleAccount_Deal_Code(i.t_id),
                               --in_Rec_Status,
                               qb_dwh_utils.REC_CLOSED,
                               qb_dwh_utils.DateToChar(i.t_disablingdate),
                               in_SysMoment, in_Ext_File
                              );
        end if;
        */

      end loop;
    end;
  ------------------------------------------------------
  --Выгрузка драгоценных металов в DWH
  ------------------------------------------------------
  procedure export_Precious_Metals(in_DocKind      in number,
                                   in_DealId       in number,
                                   in_date         in date,
                                   in_dwhRecStatus in Varchar2,
                                   in_dwhDT        in Varchar2,
                                   in_dwhSysMoment in Varchar2,
                                   in_dwhEXT_FILE  in Varchar2) is
    dwhDeal       varchar2(100);
    dwhSubject    varchar2(250);
    dwhDepartment Varchar2(30);
    dwhDealDate   varchar2(100);
  begin
    -- Откроем курсор по сделке
    for rec in cur_Precious_Metals(in_DocKind, in_DealId) loop
      -- Сгенерируем код компаненты (Код сделки)
      dwhDeal       := qb_dwh_utils.GetComponentCode('FCT_DEAL',
                                                     qb_dwh_utils.System_BISQUIT,
                                                     rec.Department_Code,
                                                     rec.code);
      dwhSubject    := qb_dwh_utils.GetComponentCode('DET_SUBJECT',
                                                     qb_dwh_utils.System_IBSO,
                                                     rec.Department_Code,
                                                     rec.subject_code);
      dwhDepartment := qb_dwh_utils.GetCODE_DEPARTMENT(rec.Department_Code);
      dwhDealDate   := qb_dwh_utils.DateToChar(qb_dwh_utils.NvlBegDate(rec.begindate));

      -- FCT_DEAL
      qb_dwh_utils.ins_FCT_DEAL(dwhDeal,
                                dwhSubject,
                                dwhDepartment,
                                rec.dealtype,
                                rec.docnum,
                                rec.Is_Interior,
                                qb_dwh_utils.DateToChar(rec.begindate),
                                qb_dwh_utils.DateToChar(rec.enddate),
                                rec.note,
                                in_dwhRecStatus,
                                dwhDealDate, --in_dwhDT,
                                in_dwhSysMoment,
                                in_dwhEXT_FILE);

      -----------------------------------------------
      -- Счета по сделке
      -----------------------------------------------
      qb_dwh_utils.add_ASS_ACCOUNTDEAL(rec.dockind,
                                       rec.dealid,
                                       in_date,
                                       dwhDeal,
                                       in_dwhRecStatus,
                                       in_dwhDT,
                                       in_dwhSysMoment,
                                       in_dwhExt_File);

      /* -- Дублируется в qb_dwh_utils.add_ASS_ACCOUNTDEAL
      add_ASS_ACCOUNTDEAL_By_PM(rec.dockind,
                                rec.dealid,
                                in_date,
                                dwhDeal,
                                in_dwhRecStatus,
                                in_dwhDT,
                                in_dwhSysMoment,
                                in_dwhExt_File); */

      ----------------------------------------------
      -- Добавим зписи по связям проводок со сделкой
      -----------------------------------------------
      Add_dwh_Carry(rec.DocKind,
                    rec.dealid,
                    in_dwhRecStatus,
                    in_dwhDT,
                    in_dwhSysMoment,
                    in_dwhExt_File);

    end loop;
  end;
  ------------------------------------------------------
  -- Выгрузка драг. металов
  ------------------------------------------------------
  procedure export_Precious_Metals_Status_Add(in_UploadID in number,
                                              in_department in number,
                                              in_date       in date) is
    CntDeal      number := 0;
    EventID      number := 0;
    dwhRecStatus varchar2(1);
    dwhDT        varchar2(10);
    dwhSysMoment varchar2(19);
    dwhEXT_FILE  varchar2(300);
    calc_id     itt_parallel_exec.calc_id%type;
    --select t.t_id DealId, t.t_Dockind Dockind
     TYPE Deal_t IS TABLE OF ddvndeal_dbt.t_id%TYPE;
     TYPE Dockind_t IS TABLE OF ddvndeal_dbt.t_Dockind%TYPE; 
     TYPE CntDeal_t is table of number INDEX BY BINARY_INTEGER ;
     c_Deal Deal_t; 
     c_Dockind Dockind_t;
     c_CntDeal CntDeal_t ;
 
  begin
    -- Установим начало выгрузки новых сделок
    qb_bp_utils.startevent(cEvent_Precious_Metals, in_UploadID, EventID);

    qb_bp_utils.SetAttrValue(EventID,
                             cAttrRec_Status,
                             qb_dwh_utils.REC_ADD);
    qb_bp_utils.SetAttrValue(EventID, cAttrDepartment, in_department);
    qb_bp_utils.SetAttrValue(EventID, cAttrDT, in_date);

    InitExportData(EventID, dwhRecStatus, dwhDT, dwhSysMoment, dwhEXT_FILE);
    calc_id:=it_parallel_exec.init_calc();
    -- Пробежимся по курсору со сделками
    --select t.t_id DealId, t.t_Dockind Dockind
    OPEN cur_Precious_Metals_For_Export(in_department);
    FETCH cur_Precious_Metals_For_Export BULK COLLECT INTO c_Deal, c_Dockind;
    FOR n IN c_Deal.FIRST .. c_Deal.LAST
    loop
       c_CntDeal(n):= n;
    end loop; 

    CLOSE cur_Precious_Metals_For_Export ;
    FORALL n IN c_Deal.FIRST .. c_Deal.LAST 
      INSERT INTO itt_parallel_exec(calc_id, num01, num02,num03) 
           VALUES (calc_id,c_CntDeal(n), c_Dockind(n), c_Deal(n));
    commit;
    it_parallel_exec.run_task_chunks_by_sql(p_parallel_level => 12,
      p_chunk_sql => 'SELECT row_id, num01 FROM itt_parallel_exec partition (p'||calc_id||') 
         where calc_id = '||calc_id,
      p_sql_stmt=> 
    'declare v_row_id number := :start_id ; CntDeal integer := :end_id ;
        rec_Dockind ddvndeal_dbt.t_Dockind%TYPE; rec_dealid ddvndeal_dbt.t_id%TYPE;   
     begin
        select  num02,num03 into rec_Dockind , rec_dealid from itt_parallel_exec partition (p'||calc_id||') 
        where  row_id = v_row_id and calc_id = '||calc_id||';
        qb_bp_utils.SetAttrValue('||EventID||', '||cDealID||', rec_dealid, CntDeal);
        begin
          qb_dwh_export.export_Precious_Metals(rec_Dockind,
                                 rec_dealid,
                                 to_date('''||to_char(in_date,'yyyymmdd')||''',''yyyymmdd''),
                                 '''||dwhRecStatus||''',
                                 '''||dwhDT||''',
                                 '''||dwhSysMoment||''',
                                 '''||dwhEXT_FILE||''');
        exception
          when others then
            qb_bp_utils.SetError('||EventID||',
                                 SQLCODE,
                                 SQLERRM,
                                 2,
                                 '||cDealID||',
                                 rec_dealid);
        end;
        commit;
       exception
         when others then
         it_error.put_error_in_stack;
         it_log.log(p_msg => ''qb_dwh_export.export_Precious_Metals_Status_Add ERROR'',p_msg_type => it_log.C_MSG_TYPE__ERROR);
       end;');
     it_parallel_exec.clear_calc(p_id => calc_id);
   /* for rec in cur_Precious_Metals_For_Export(in_department) loop
      CntDeal := CntDeal + 1;
      -- Запишем сделку по которой начата операция выгрузки
      qb_bp_utils.SetAttrValue(EventID, cDealID, rec.dealid, CntDeal);
      begin
        export_Precious_Metals(rec.Dockind,
                               rec.dealid,
                               in_date,
                               dwhRecStatus,
                               dwhDT,
                               dwhSysMoment,
                               dwhEXT_FILE);
      exception
        when others then
          -- пока не останавливаем обработку что бы максимально отследить ошибки, дальше по требованиям заказчика решать будем
          qb_bp_utils.SetError(EventID,
                               SQLCODE,
                               SQLERRM,
                               2,
                               cDealID,
                               rec.dealid);
      end;
      commit;
    end loop;*/
    --Завершим выгрузку новых сделок
    qb_bp_utils.EndEvent(EventID, null);
    commit;
  end;

  -----------------------------------------------
  -- графики CSA
  -----------------------------------------------
  procedure add_CSA_GraffData(in_DealId              in number,
                          in_dwhDeal             in varchar2,
                          in_dwhProlongationDeal in varchar2,
                          in_dwhRecStatus        in Varchar2,
                          in_dwhDT               in Varchar2,
                          in_dwhSysMoment        in Varchar2,
                          in_dwhEXT_FILE         in Varchar2) is
  begin
      -----------------------------------------------
      -- Процентные ставки
      -----------------------------------------------
      for rate_rec in (select tmp.t_dmain first_dt,
                              LAST_VALUE(tmp.t_dmain) OVER(ORDER BY tmp.t_dmain RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) - 1 end_dt,
                              tmp.t_rate rate,
                              tmp.t_sumaccr,
                              tmp.t_base
                       from (
                       select r.t_dmain,
                              r.t_rate,
                              r.t_sumaccr,
                              r.t_csaid,
                              (select max(case when i.t_days = 0 and i.t_dyear = 0 then 1 -- факт/факт
                                               when i.t_days = 30 and i.t_dyear = 360 then 0 -- 30/360
                                               when i.t_days = 31 and i.t_dyear = 360 then 2 -- факт/360
                                               when i.t_days = 31 and i.t_dyear = 365 then 40 -- факт/365
                                          end)
                                 from ddvcsaind_dbt i
                                      inner join dfininstr_dbt fi on fi.t_fiid = i.t_fiid and fi.t_avoirkind = 2 -- размещение
                                where r.t_dmain between i.t_begdate and decode (i.t_enddate,
                                                                                to_date('01.01.0001','dd.mm.yyyy'),
                                                                                to_date('01.01.3001','dd.mm.yyyy'),
                                                                                i.t_enddate)
                                      and i.t_csaid = r.t_csaid) t_base

                         from ddvcsash_dbt r

                        ) tmp
                        where tmp.t_csaid = in_DealId
                              --and tmp.t_dmain  <= in_date
                              and tmp.t_rate != 0 -- потом проверить надо ли ноль
                      ) loop
        qb_dwh_utils.ins_FCT_PROCRATE_DEAL(in_dwhDeal,
                                           'Juridic',
                                           null, -- Всегда пусто, согласно коментариев Рогалева
                                           1, -- Необходимо соответствие базы для налога
                                           qb_dwh_utils.NumberToChar(rate_rec.rate,5),
                                           null, -- Сумма
                                           --qb_dwh_utils.DateToChar(rate_rec.end_dt),
                                           qb_dwh_utils.DateToChar(case when rate_rec.first_dt > rate_rec.end_dt then null else rate_rec.end_dt end), -- Дата пересмотра ставки
                                           qb_dwh_utils.DateToChar(qb_dwh_utils.DT_BEGIN), -- Константа согласно документации
                                           in_dwhRecStatus,
                                           qb_dwh_utils.DateToChar(rate_rec.first_dt),
                                           in_dwhSysMoment,
                                           in_dwhExt_File);
      end loop;
   -- Добавим графики

    for Rec_Attr in (
                     -- %%
                     select 4626 DocKind,
                            csa.t_csaid DealID, --DEAL_CODE
                            csa.t_department Department,
                            11 Purpose,
                            1 TypeRepay_Code,
                            '-1' External_TypeRepay_Code,
                            4 TYPESCH,
                            'П' PERIODICITY, -- Код длительности цикла погашения
                            null COUNT_PERIOD, -- Длительность цикла погашения.
                            null MONTH_PAY,
                            null DAY_PAY,
                            null IS_WORKDAY,
                            null GRACE_PERIODICITY,
                            null GRACE_COUNT_PERIOD,
                            null SUM_REPAY,
                            csa.t_begdate DT_OPEN_PER,
                            csa.t_enddate DT_CLOSE_PER,
                            csa.t_begdate dt
                       from ddvcsa_dbt csa
                      where exists (select 1 from ddvcsash_dbt sh where sh.t_rate > 0 and sh.t_csaid = csa.t_csaid)
                            and csa.t_csaid = in_DealId) loop
      --FCT_ATTR_SCHEDULE
      qb_dwh_utils.ins_FCT_ATTR_SCHEDULE(in_dwhDeal,
                                         Rec_Attr.Typerepay_Code,
                                         Rec_Attr.External_Typerepay_Code,
                                         Rec_Attr.Typesch,
                                         Rec_Attr.Periodicity,
                                         Rec_Attr.Count_Period,
                                         Rec_Attr.Month_Pay,
                                         Rec_Attr.Day_Pay,
                                         Rec_Attr.Is_Workday,
                                         Rec_Attr.Grace_Periodicity,
                                         Rec_Attr.Grace_Count_Period,
                                         qb_dwh_utils.NumberToChar(Rec_Attr.Sum_Repay, 3),
                                         qb_dwh_utils.DateToChar(Rec_Attr.Dt_Open_Per),
                                         qb_dwh_utils.DateToChar(Rec_Attr.Dt_Close_Per),
                                         in_dwhRecStatus,
                                         qb_dwh_utils.DateToChar(Rec_Attr.Dt_Open_Per), --in_dwhDT,
                                         in_dwhSysMoment,
                                         in_dwhExt_File);
      --FCT_REPAYSCHEDULE_DM  РАСЧЕТНАЯ ДАТА Строка планового графика таблица с неактуальными данными
      for rec_Sedule_dm in (select csa.t_csaid DEAL_CODE,
                                   sh.t_shid CODE,
                                   sh.t_dmain DT_OPEN,
                                   3 TYPESCHEDULE,
                                   1  TYPEREPAY_CODE,
                                   2 MOVINGDIRECTION, --привлечение/Депозит
                                   sh.t_cur FINSTR_CODE,
                                   abs(decode(sh.t_cur,
                                          0,
                                          null,
                                          rsb_fiinstr.ConvSum(sh.t_sumproccalc,
                                                              sh.t_cur,
                                                              0,
                                                              sh.t_dmain))) EVENTSUM,
                                   null FINSTRAMOUNT,
                                   abs(sh.t_sumproccalc) DEALSUM
                              from ddvcsa_dbt csa
                                   inner join ddvcsash_dbt sh on sh.t_rate >0 and sh.t_csaid = csa.t_csaid
                             where csa.t_csaid = Rec_Attr.Dealid) loop

        qb_dwh_utils.ins_FCT_REPAYSCHEDULE_DM(in_dwhDeal,

                                              rec_Sedule_dm.Code || '#' ||
                                              rec_Sedule_dm.Typeschedule,
                                              qb_dwh_utils.DateToChar(rec_Sedule_dm.Dt_Open),
                                              rec_Sedule_dm.Typeschedule,
                                              rec_Sedule_dm.Typerepay_Code,
                                              rec_Sedule_dm.Movingdirection,
                                              qb_dwh_utils.GetFINSTR_CODE(rec_Sedule_dm.Finstr_Code),
                                              qb_dwh_utils.NumberToChar(rec_Sedule_dm.Eventsum,
                                                                        3),
                                              qb_dwh_utils.NumberToChar(rec_Sedule_dm.Finstramount,
                                                                        3),
                                              qb_dwh_utils.NumberToChar(rec_Sedule_dm.Dealsum,
                                                                        3),
                                              in_dwhRecStatus,
                                              --in_dwhDT,
                                              in_dwhSysMoment,
                                              in_dwhExt_File);
        -- запишем первую историю в график
        qb_dwh_utils.ins_FCT_REPAYSCHEDULE_H(in_dwhDeal,
                                             rec_Sedule_dm.TypeRepay_Code,
                                             qb_dwh_utils.GetFINSTR_CODE(rec_Sedule_dm.FinStr_Code),
                                             rec_Sedule_dm.Code || '#' ||
                                             rec_Sedule_dm.Typeschedule,
                                             qb_dwh_utils.DateToChar(rec_Sedule_dm.DT_OPEN),
                                             rec_Sedule_dm.TypeSchedule,
                                             rec_Sedule_dm.MovingDirection,
                                             qb_dwh_utils.NumberToChar(rec_Sedule_dm.EventSum,
                                                                       3),
                                             qb_dwh_utils.NumberToChar(rec_Sedule_dm.FinStrAmount,
                                                                       3),
                                             qb_dwh_utils.NumberToChar(rec_Sedule_dm.DealSum,
                                                                       3),
                                             null, --qb_dwh_utils.DateToChar(vPrev_DT_Open),
                                             in_dwhRecStatus,
                                             qb_dwh_utils.DateToChar(rec_Sedule_dm.Dt_Open/*vLastCalcDate*/), --in_dwhDT,
                                             in_dwhSysMoment,
                                             in_dwhExt_File);
        end loop;
      end loop;
  end;
  ------------------------------------------------------
  --Выгрузка сделки МБК в DWH
  ------------------------------------------------------
  procedure export_CSA(in_DealId       in number,
                        in_date         in date,
                        in_dwhRecStatus in Varchar2,
                        in_dwhDT        in Varchar2,
                        in_dwhSysMoment in Varchar2,
                        in_dwhEXT_FILE  in Varchar2) is
    dwhCSADeal       varchar2(100);
    dwhDopDeal       varchar2(100);
    dwhSubject       varchar2(250);
    dwhDepartment    Varchar2(30);
    dwhDealDate      varchar2(100);
    --Констатнты из документации
    dwhDEALTYPEMBK varchar2(1) := '1'; -- Обычный МБК
    dwhTYPESYND    varchar2(1) := '1'; -- Не синдицированный кредит
    --
    vParentDealId    number; -- ИД первичной сделки для пролонгации
  begin
    -- Откроем курсор по сделке
    for rec in cur_csa(in_DealId, in_date) loop
      -- Сгенерируем код компаненты (Код сделки)
      dwhCSADeal := qb_dwh_utils.GetComponentCode('FCT_DEAL',
                                                  qb_dwh_utils.System_BISQUIT,
                                                  rec.Department,
                                                  rec.DWHdealID);
      dwhSubject := qb_dwh_utils.GetComponentCode('DET_SUBJECT',
                                                  qb_dwh_utils.System_IBSO,
                                                  rec.Department,
                                                  rec.PartyId);
      dwhDepartment := qb_dwh_utils.GetCODE_DEPARTMENT(rec.Department);
      dwhDealDate   := qb_dwh_utils.DateToChar(qb_dwh_utils.NvlBegDate(rec.dealdate));
      -- FCT_DEAL
      qb_dwh_utils.ins_FCT_DEAL(dwhCSADeal,
                                dwhSubject,
                                dwhDepartment,
                                rec.mb_kind,
                                rec.Deal_Code,
                                rec.Is_Interior,
                                qb_dwh_utils.DateToChar(rec.deal_Start),
                                qb_dwh_utils.DateToChar(rec.deal_End),
                                rec.note,
                                in_dwhRecStatus,
                                dwhDealDate,
                                in_dwhSysMoment,
                                in_dwhEXT_FILE);

      if rec.partyid Is not null then
        -- Контрагент
        qb_dwh_utils.ins_FCT_SUBJECT_ROLEDEAL(dwhCSADeal,
                                              qb_dwh_utils.GetComponentCode('DET_SUBJECT',
                                                                            qb_dwh_utils.System_IBSO,
                                                                            rec.Department,
                                                                            rec.partyid),
                                              qb_dwh_utils.GetComponentCode('DET_SUBJECT_ROLEDEAL',
                                                                            qb_dwh_utils.System_IBSO,
                                                                            rec.Department,
                                                                            'КОНТРАГЕНТ'
                                                                            --'КОТНРАГЕНТ ПО ДОГОВОРУ CSA'
                                                                            ),
                                              null, --in_Is_Agreement, -- "Признак согласия на выгрузку в БКИ" - не требуется, атрибут физ лица
                                              null, --in_DT_Agreement, -- "Признак согласия на выгрузку в БКИ" - не требуется, атрибут физ лица
                                              qb_dwh_utils.DateToChar(rec.dealdate),
                                              qb_dwh_utils.DateToChar(qb_dwh_utils.DT_END),
                                              in_dwhRecStatus,
                                              dwhDealDate,
                                              in_dwhSysMoment,
                                              in_dwhExt_File);
      end if;
      -- Запишем доп сделки
      for recDopDeal in (select c.t_id,
                       c.t_number,
                       c.t_code,
                       d.t_account,
                       d.t_activatedate,
                       d.t_disablingdate,
                       d.t_actiondate,
                       d.t_Departmentid Department,
                       decode(c.t_code, '-МС', '1','2') mb_kind,
                       csa.t_csaid || '#CSA#' || decode(c.t_code, '-МС', 'П','А') || '#' || fi.t_ccy dopDealId,
                       csa.t_code || '#' || decode(c.t_code, '-МС', 'П','А') || '#' || fi.t_ccy Deal_Code,
                       fi.t_fiid fiid
                  from ddvcsa_dbt csa
                       inner join dmcaccdoc_dbt d on d.t_dockind = 4626 and d.t_docid = csa.t_csaid and d.t_activatedate != d.t_disablingdate and d.t_account is not null
                       inner join dmccateg_dbt c on c.t_code in ('+МС', '-МС') and c.t_id = d.t_catid
                       inner join daccount_dbt acc on acc.t_client = csa.t_partyid and acc.t_account = d.t_account
                       inner join dfininstr_dbt fi on fi.t_fiid = acc.t_code_currency

                 where csa.t_csaid = rec.DealId) loop
      dwhDopDeal := qb_dwh_utils.GetComponentCode('FCT_DEAL',
                                                  qb_dwh_utils.System_BISQUIT,
                                                  rec.Department,
                                                  recDopDeal.dopDealId);
      -- FCT_DEAL
      qb_dwh_utils.ins_FCT_DEAL(dwhDopDeal,
                                dwhSubject,
                                dwhDepartment,
                                recDopDeal.mb_kind,
                                recDopDeal.Deal_Code, -- Подумать
                                rec.Is_Interior,
                                qb_dwh_utils.DateToChar(rec.deal_Start),
                                qb_dwh_utils.DateToChar(rec.deal_End),
                                rec.note,
                                in_dwhRecStatus,
                                dwhDealDate,
                                in_dwhSysMoment,
                                in_dwhEXT_FILE);

        -- Генеральное соглашение
        -- ASS_FCT_DEAL
        qb_dwh_utils.ins_ASS_FCT_DEAL(dwhCSADeal,
                                      dwhDopDeal,
                                      'Agreement',
                                      in_dwhRecStatus,
                                      dwhDealDate,
                                      in_dwhSysMoment,
                                      in_dwhEXT_FILE);

          if (recDopDeal.mb_kind = 1) then
            -- Межбанковский депозит
            -- FCT_MBKDEPDEAL
            qb_dwh_utils.ins_FCT_MBKDEPDEAL(dwhDopDeal,
                                            qb_dwh_utils.GetFINSTR_CODE(recDopDeal.fiid),
                                            qb_dwh_utils.NumberToChar(/*rec.deal_sum*/0, 3),
                                            dwhDEALTYPEMBK,
                                            in_dwhRecStatus,
                                            in_dwhSysMoment,
                                            in_dwhEXT_FILE);
                                            -----------------------------------------------
          -- Счета по сделке
          -----------------------------------------------
          for rec_acc in (select c.t_id,
                           c.t_number,
                           upper(c.t_code) t_code,
                           --acc.t_userfield4 t_account, --d.t_account,
                           case
                              when (acc.t_userfield4 is null) or
                                  (acc.t_userfield4 = chr(0)) or
                                  (acc.t_userfield4 = chr(1)) or
                                  (acc.t_userfield4 like '0x%') then
                                acc.t_account
                              else
                                acc.t_userfield4
                           end t_account,
                           d.t_activatedate,
                           d.t_disablingdate,
                           d.t_actiondate,
                           d.t_Departmentid Department,
                           upper(c.t_name) cat_name
                      from ddvcsa_dbt csa
                           inner join dmcaccdoc_dbt d on d.t_dockind = rec.dockind and d.t_docid = csa.t_csaid and d.t_activatedate != d.t_disablingdate and d.t_account is not null
                           inner join dmccateg_dbt c on c.t_code in ('-МС', '-%МС','+Треб. по%, отриц. ставка') and c.t_id = d.t_catid
                           inner join daccount_dbt acc on acc.t_client = csa.t_partyid and acc.t_account = d.t_account
                     where  csa.t_csaid = rec.DealId)  loop
              /*qb_dwh_utils.ins_ASS_ACCOUNTDEAL (qb_dwh_utils.GetComponentCode ('DET_ACCOUNT',qb_dwh_utils.System_BISQUIT, rec_acc.Department, rec_acc.t_account),
                                   dwhDopDeal,
                                   rec_acc.t_code,--'ССУДНЫЙ СЧЕТ',
                                   qb_dwh_utils.REC_ADD,
                                   qb_dwh_utils.DateToChar(rec.dealdate),
                                   in_dwhSysMoment, in_dwhExt_File
                                  );*/

          -- Выгрузка новых связей
          qb_dwh_utils.ins_ASS_ACCOUNTDEAL (qb_dwh_utils.GetComponentCode ('DET_ACCOUNT',qb_dwh_utils.System_IBSO, rec_acc.Department, rec_acc.t_account),
                               dwhDopDeal,
                               qb_dwh_utils.GetRoleAccount_Deal_Code(rec_acc.t_id),
                               qb_dwh_utils.REC_ADD,
                               qb_dwh_utils.DateToChar(rec_acc.t_activatedate),
                               in_dwhSysMoment, in_dwhEXT_FILE,
                               -- KS 23.02.2022 Закрытие действующих связей должно осуществляться через поле ASS_ACCOUNTDEAL.DT_END
                               case rec_acc.t_disablingdate
                                 when to_date('01.01.0001','dd.mm.yyyy') then qb_dwh_utils.DateToChar(qb_dwh_utils.DT_END)
                                 else qb_dwh_utils.DateToChar(rec_acc.t_disablingdate-1)
                               end
                              );
                              
        /*
        if rec_acc.t_disablingdate != to_date('01.01.0001','dd.mm.yyyy')  then
          -- Закрытие действующих связий
          qb_dwh_utils.ins_ASS_ACCOUNTDEAL (qb_dwh_utils.GetComponentCode ('DET_ACCOUNT',qb_dwh_utils.System_IBSO, rec_acc.Department, rec_acc.t_account),
                               dwhDopDeal,
                               qb_dwh_utils.GetRoleAccount_Deal_Code(rec_acc.t_id),
                               qb_dwh_utils.REC_CLOSED,
                               qb_dwh_utils.DateToChar(rec_acc.t_disablingdate),
                               in_dwhSysMoment, in_dwhEXT_FILE
                              );
        end if;
        */

          end loop;
          elsif (recDopDeal.mb_kind = 2) then
            -- Кредитный маринальный платеж
            -- FCT_MBKCREDEAL
            qb_dwh_utils.ins_FCT_MBKCREDEAL(dwhDopDeal,
                                            qb_dwh_utils.GetFINSTR_CODE(recDopDeal.fiid),
                                            qb_dwh_utils.NumberToChar(/*rec.deal_sum*/0, 3),
                                            dwhDEALTYPEMBK,
                                            dwhTYPESYND,
                                            in_dwhRecStatus,
                                            in_dwhSysMoment,
                                            in_dwhEXT_FILE);
          -----------------------------------------------
          -- Счета по сделке
          -----------------------------------------------
          for rec_acc in (select c.t_id,
                           c.t_number,
                           upper(c.t_code) t_code,
                           --acc.t_userfield4 t_account, --d.t_account,
                           case
                              when (acc.t_userfield4 is null) or
                                  (acc.t_userfield4 = chr(0)) or
                                  (acc.t_userfield4 = chr(1)) or
                                  (acc.t_userfield4 like '0x%') then
                                acc.t_account
                              else
                                acc.t_userfield4
                           end t_account,
                           d.t_activatedate,
                           d.t_disablingdate,
                           d.t_actiondate,
                           d.t_Departmentid Department,
                           upper(c.t_name) cat_name
                      from ddvcsa_dbt csa
                           inner join dmcaccdoc_dbt d on d.t_dockind = rec.dockind and d.t_docid = csa.t_csaid and d.t_activatedate != d.t_disablingdate and d.t_account is not null
                           inner join dmccateg_dbt c on c.t_code in ('+МС', '+%МС') and c.t_id = d.t_catid
                           inner join daccount_dbt acc on acc.t_client = csa.t_partyid and acc.t_account = d.t_account
                     where d.t_dockind = rec.dockind and csa.t_csaid = rec.DealId)  loop
              /*qb_dwh_utils.ins_ASS_ACCOUNTDEAL (qb_dwh_utils.GetComponentCode ('DET_ACCOUNT',qb_dwh_utils.System_BISQUIT, rec_acc.Department, rec_acc.t_account),
                                   dwhDopDeal,
                                   rec_acc.t_code,--'ССУДНЫЙ СЧЕТ',
                                   qb_dwh_utils.REC_ADD,
                                   qb_dwh_utils.DateToChar(rec.dealdate),
                                   in_dwhSysMoment, in_dwhExt_File
                                  );*/

          -- Выгрузка новых связей
          qb_dwh_utils.ins_ASS_ACCOUNTDEAL (qb_dwh_utils.GetComponentCode ('DET_ACCOUNT',qb_dwh_utils.System_IBSO, rec_acc.Department, rec_acc.t_account),
                               dwhDopDeal,
                               qb_dwh_utils.GetRoleAccount_Deal_Code(rec_acc.t_id),
                               qb_dwh_utils.REC_ADD,
                               qb_dwh_utils.DateToChar(rec_acc.t_activatedate),
                               in_dwhSysMoment, in_dwhExt_File,
                               -- KS 23.02.2022 Закрытие действующих связей должно осуществляться через поле ASS_ACCOUNTDEAL.DT_END
                               case rec_acc.t_disablingdate
                                 when to_date('01.01.0001','dd.mm.yyyy') then qb_dwh_utils.DateToChar(qb_dwh_utils.DT_END)
                                 else qb_dwh_utils.DateToChar(rec_acc.t_disablingdate-1)
                               end
                              );
        /*
        if rec_acc.t_disablingdate != to_date('01.01.0001','dd.mm.yyyy')  then
          -- Закрытие действующих связий
          qb_dwh_utils.ins_ASS_ACCOUNTDEAL (qb_dwh_utils.GetComponentCode ('DET_ACCOUNT',qb_dwh_utils.System_IBSO, rec_acc.Department, rec_acc.t_account),
                               dwhDopDeal,
                               qb_dwh_utils.GetRoleAccount_Deal_Code(rec_acc.t_id),
                               qb_dwh_utils.REC_CLOSED,
                               qb_dwh_utils.DateToChar(rec_acc.t_disablingdate),
                               in_dwhSysMoment, in_dwhExt_File
                              );
       end if;
       */

          end loop;
          end if;

          if rec.partyid Is not null then
            -- Контрагент
            qb_dwh_utils.ins_FCT_SUBJECT_ROLEDEAL(dwhDopDeal,
                                                  qb_dwh_utils.GetComponentCode('DET_SUBJECT',
                                                                                qb_dwh_utils.System_IBSO,
                                                                                rec.Department,
                                                                                rec.partyid),
                                                  qb_dwh_utils.GetComponentCode('DET_SUBJECT_ROLEDEAL',
                                                                                qb_dwh_utils.System_IBSO,
                                                                                rec.Department,
                                                                                'КОНТРАГЕНТ'
                                                                                ),
                                                  null, --in_Is_Agreement, -- "Признак согласия на выгрузку в БКИ" - не требуется, атрибут физ лица
                                                  null, --in_DT_Agreement, -- "Признак согласия на выгрузку в БКИ" - не требуется, атрибут физ лица
                                                  qb_dwh_utils.DateToChar(rec.dealdate),
                                                  qb_dwh_utils.DateToChar(qb_dwh_utils.DT_END),
                                                  in_dwhRecStatus,
                                                  dwhDealDate,
                                                  in_dwhSysMoment,
                                                  in_dwhExt_File);
          end if;

          ------------------------------------------------------
          -- Добавляем данные в ASS_DEAL_CAT_VAL@LDR_INFA (Связь сделки со значением ограниченного доп.атрибута)
          -- на основании категорий учета
          ------------------------------------------------------
          qb_dwh_utils.add_ASS_DEAL_CAT_VAL(rec.dockind,
                                            rec.dealid,
                                            in_Date,
                                            dwhDopDeal,
                                            in_dwhRecStatus,
                                            in_dwhDT,
                                            in_dwhSysMoment,
                                            in_dwhExt_File);

          ------------------------------------------------------
          --Добавление данных в FCT_DEAL_INDICATOR@LDR_INFA (Значение свободного доп.атрибута сделки)
          -- На основании Примечаний по объекту
          ------------------------------------------------------
          qb_dwh_utils.add_FCT_DEAL_INDICATOR(rec.dockind,
                                              rec.dealid,
                                              in_Date,
                                              dwhDopDeal,
                                              in_dwhRecStatus,
                                              in_dwhDT,
                                              in_dwhSysMoment,
                                              in_dwhExt_File,
                                              rec.mb_kind);
           add_CSA_GraffData(rec.dealid,
                             dwhDopDeal,
                             null,
                             in_dwhRecStatus,
                             in_dwhDT,
                             in_dwhSysMoment,
                             in_dwhExt_File);
      end loop; -- Закроем курсор по доп сделкам

      ------------------------------------------------------
      -- Добавляем данные в ASS_DEAL_CAT_VAL@LDR_INFA (Связь сделки со значением ограниченного доп.атрибута)
      -- на основании категорий учета
      ------------------------------------------------------
      qb_dwh_utils.add_ASS_DEAL_CAT_VAL(rec.dockind,
                                        rec.dealid,
                                        in_Date,
                                        dwhCSADeal,
                                        in_dwhRecStatus,
                                        in_dwhDT,
                                        in_dwhSysMoment,
                                        in_dwhExt_File);

      ------------------------------------------------------
      --Добавление данных в FCT_DEAL_INDICATOR@LDR_INFA (Значение свободного доп.атрибута сделки)
      -- На основании Примечаний по объекту
      ------------------------------------------------------
      qb_dwh_utils.add_FCT_DEAL_INDICATOR(rec.dockind,
                                          rec.dealid,
                                          in_Date,
                                          dwhCSADeal,
                                          in_dwhRecStatus,
                                          in_dwhDT,
                                          in_dwhSysMoment,
                                          in_dwhExt_File);

      add_CSA_GraffData(rec.dealid,
                        dwhCSADeal,
                        null,
                        in_dwhRecStatus,
                        in_dwhDT,
                        in_dwhSysMoment,
                        in_dwhExt_File);
      -----------------------------------------------
      -- Счета по сделке
      -----------------------------------------------
      for rec_acc in (select c.t_id,
                       c.t_number,
                       upper(c.t_code) t_code,
                       --acc.t_userfield4 t_account, --d.t_account,
                       case
                          when (acc.t_userfield4 is null) or
                              (acc.t_userfield4 = chr(0)) or
                              (acc.t_userfield4 = chr(1)) or
                              (acc.t_userfield4 like '0x%') then
                            acc.t_account
                          else
                            acc.t_userfield4
                       end t_account,
                       d.t_activatedate,
                       d.t_disablingdate,
                       d.t_actiondate,
                       d.t_Departmentid Department,
                       upper(c.t_name) cat_name
                  from ddvcsa_dbt csa
                       inner join dmcaccdoc_dbt d on d.t_activatedate != d.t_disablingdate and d.t_account is not null
                                                     and d.t_docid = csa.t_csaid
                       inner join dmccateg_dbt c on c.t_code not in ('+МС', '+%МС','-МС', '-%МС','+Треб. по%, отриц. ставка')
                                                             --in ('+Расчеты', '-Расчеты')
                                                             and c.t_id = d.t_catid
                       inner join daccount_dbt acc on d.t_chapter = acc.t_chapter and d.t_account = acc.t_account and d.t_currency = acc.t_code_currency
                       --inner join daccount_dbt acc on acc.t_client = csa.t_partyid and acc.t_account = d.t_account
                 where d.t_dockind = rec.dockind and csa.t_csaid = rec.DealId)  loop
          /*qb_dwh_utils.ins_ASS_ACCOUNTDEAL (qb_dwh_utils.GetComponentCode ('DET_ACCOUNT',qb_dwh_utils.System_BISQUIT, rec_acc.Department, rec_acc.t_account),
                               dwhCSADeal,
                               rec_acc.t_code,--'ССУДНЫЙ СЧЕТ',
                               qb_dwh_utils.REC_ADD,
                               qb_dwh_utils.DateToChar(rec.dealdate),
                               in_dwhSysMoment, in_dwhExt_File
                              );*/

          -- Выгрузка новых связей
          qb_dwh_utils.ins_ASS_ACCOUNTDEAL (qb_dwh_utils.GetComponentCode ('DET_ACCOUNT',qb_dwh_utils.System_IBSO, rec_acc.Department, rec_acc.t_account),
                               dwhCSADeal,
                               qb_dwh_utils.GetRoleAccount_Deal_Code(rec_acc.t_id),
                               qb_dwh_utils.REC_ADD,
                               qb_dwh_utils.DateToChar(rec_acc.t_activatedate),
                               in_dwhSysMoment, in_dwhExt_File,
                               -- KS 23.02.2022 Закрытие действующих связей должно осуществляться через поле ASS_ACCOUNTDEAL.DT_END
                               case rec_acc.t_disablingdate
                                 when to_date('01.01.0001','dd.mm.yyyy') then qb_dwh_utils.DateToChar(qb_dwh_utils.DT_END)
                                 else qb_dwh_utils.DateToChar(rec_acc.t_disablingdate-1)
                               end
                              );
        /*
        if rec_acc.t_disablingdate != to_date('01.01.0001','dd.mm.yyyy')  then
          -- Закрытие действующих связий
          qb_dwh_utils.ins_ASS_ACCOUNTDEAL (qb_dwh_utils.GetComponentCode ('DET_ACCOUNT',qb_dwh_utils.System_IBSO, rec_acc.Department, rec_acc.t_account),
                               dwhCSADeal,
                               qb_dwh_utils.GetRoleAccount_Deal_Code(rec_acc.t_id),
                               qb_dwh_utils.REC_CLOSED,
                               qb_dwh_utils.DateToChar(rec_acc.t_disablingdate),
                               in_dwhSysMoment, in_dwhExt_File
                              );
        end if;
        */

      end loop;
      -----------------------------------------------
      -- Проводки по сделке
      -----------------------------------------------
      /*export_carry_by_mcCat(rec.DocKind,
                            rec.dealid,
                            dwhDeal,
                            in_dwhRecStatus,
                            in_dwhDT,
                            in_dwhSysMoment,
                            in_dwhExt_File);*/

      ----------------------------------------------
      -- Добавим зписи по связям проводок со сделкой
      -----------------------------------------------

      Add_DWH_Carry(rec.DocKind,
                    rec.dealid,
                    in_dwhRecStatus,
                    in_dwhDT,
                    in_dwhSysMoment,
                    in_dwhExt_File);


    end loop;
  end;

  ------------------------------------------------------
  -- Выгрузка вновь заключенных и пролонгированных сделок
  ------------------------------------------------------
  procedure export_CSA_Status_Add(in_UploadID in number,
                                  in_department in number,
                                  in_date       in date) is
    CntDeal      number := 0;
    EventID      number := 0;
    dwhRecStatus varchar2(1);
    dwhDT        varchar2(10);
    dwhSysMoment varchar2(19);
    dwhEXT_FILE  varchar2(300);
  begin
    -- Установим начало выгрузки новых сделок
    qb_bp_utils.startevent(cEvent_EXPORT_CSA, in_UploadID, EventID);

    qb_bp_utils.SetAttrValue(EventID, cAttrRec_Status, qb_dwh_utils.REC_ADD);
    qb_bp_utils.SetAttrValue(EventID, cAttrDepartment, in_department);
    qb_bp_utils.SetAttrValue(EventID, cAttrDT, in_date);

    InitExportData(EventID, dwhRecStatus, dwhDT, dwhSysMoment, dwhEXT_FILE);
    -- Пробежимся по курсору со сделками
    for rec in cur_CSA_For_Export(in_department, in_date) loop
      CntDeal := CntDeal + 1;
      -- Запишем сделку по которой начата операция выгрузки
      qb_bp_utils.SetAttrValue(EventID, cDealID, rec.dealid, CntDeal);
      begin
        export_CSA(rec.dealid, in_date, dwhRecStatus, dwhDT, dwhSysMoment, dwhEXT_FILE);
      exception
        when others then
          -- пока не останавливаем обработку что бы максимально отследить ошибки, дальше по требованиям заказчика решать будем
          qb_bp_utils.SetError(EventID, SQLCODE, SQLERRM, 2, cDealID, rec.dealid);
      end;
      commit;
    end loop;
    --Завершим выгрузку новых сделок
    qb_bp_utils.EndEvent(EventID, null);
    commit;
  end;

  ------------------------------------------------------
  -- Выгрузка хеджирования
  ------------------------------------------------------
  procedure export_HEDG(in_HEDG_id      in number,
                        in_date         in date,
                        in_dwhRecStatus in Varchar2,
                        in_dwhDT        in Varchar2,
                        in_dwhSysMoment in Varchar2,
                        in_dwhEXT_FILE  in Varchar2) is

  begin
    
    for hedg_rec in (
        select to_char(val.t_date,'dd-mm-yyyy') DT, 
              '9999'||'#'||'SOFRXXX'||'#'||to_char(rel.t_objid)||to_char(rel.t_instrid)||TO_char(val.t_date,'ddmmyyyy') CODE, 
              (case 
                     when rel.t_objdockind in (102, 103) then '0000'||'#'||'SOFRXXX'||'#'||to_char(rel.t_objid)||'#'||'TCK'
              end) DEAL_CODE,
              (case when rel.t_objdockind = 24 then (select dep.t_name from ddp_dep_dbt dep , dvsbanner_dbt bnr  where bnr.t_bcid = rel.t_objid and dep.t_code = bnr.t_department)||'#'||'SOFRXXX'||'#'||to_char(rel.t_objid)||'#'||'BNR'
                     when rel.t_objdockind = 12 then '0000'||'#'||'SOFRXXX'||'#'||to_char(rel.t_objid)||'#'||'FIN'
              end) FINSTR_CODE,
              rel.t_ext_dealid ASUDR_DEAL_CODE, 
              rel.t_ext_portf PORTFOLIO_CODE, 
              rel.t_ext_subportf SUB_PORTF_CODE,
              fin.t_ccy CURRENCY_CURR_CODE_TXT, 
              val.t_actualfv COST_ON_DATE,  
              val.t_prevfv PREV_COST, 
              val.t_dfv CHG_AMOUNT, 
              (case when rel.t_objdockind = 24 then 'Вексель' 
                     when rel.t_objdockind = 12 then 'Облигация'
                     when rel.t_objdockind in (102, 103) then 'МБК'
              end) DEAL_KIND_CODE ,
              rel.t_ext_hedgeid HEDGE_REL_CODE, 
              to_char(rel.t_begindate,'dd-mm-yyyy') HEDG_BEGIN_DT, 
              to_char(rel.t_enddate,'dd-mm-yyyy') HEDG_END_DT,
              rel.t_ext_instrid HEDG_TOOL_CODE, 
              '0000'||'#'||'SOFRXXX'||'#'||to_char(rel.t_instrid)||'#DVN'||'#'||'93' TOOL_CODE_SOFR,
              (case when rel.t_objdockind = 24 and rel.t_objtype = 4 then (select distinct decode(acc.t_userfield4, chr(1), chr(1), '0000'||'#'||'IBSOXXX'||'#'||acc.t_userfield4) from dmcaccdoc_dbt doc, daccount_dbt acc where doc.t_catid = 1023 and doc.t_fiid = rel.t_objid and doc.t_docid = rel.t_id and acc.t_account = doc.t_account and acc.t_chapter = 1 and acc.t_code_currency = doc.t_currency) 
                       when rel.t_objdockind = 24 and rel.t_objtype = 3 then (select distinct decode(acc.t_userfield4, chr(1), chr(1), '0000'||'#'||'IBSOXXX'||'#'||acc.t_userfield4) from dmcaccdoc_dbt doc, daccount_dbt acc where doc.t_catid = 1025 and doc.t_fiid = rel.t_objid and doc.t_docid = rel.t_id and acc.t_account = doc.t_account and acc.t_chapter = 1 and acc.t_code_currency = doc.t_currency)
                       when rel.t_objdockind in(102, 103) and t_objtype = 6 then  ( select distinct decode(acc.t_userfield4, chr(1), chr(1), '0000'||'#'||'IBSOXXX'||'#'||acc.t_userfield4) from dmcaccdoc_dbt doc, daccount_dbt acc where doc.t_catid = 1016 and doc.t_dockind = 102 and doc.t_docid = rel.t_objid and acc.t_account = doc.t_account and acc.t_chapter = 1 and acc.t_code_currency = doc.t_currency and doc.t_firole = 10000+ rel.t_id)   
                       when rel.t_objdockind in(102, 103) and t_objtype = 5 then  (select distinct decode(acc.t_userfield4, chr(1), chr(1), '0000'||'#'||'IBSOXXX'||'#'||acc.t_userfield4) from dmcaccdoc_dbt  doc, daccount_dbt acc where doc.t_catid = 1014 and doc.t_dockind = 102 and doc.t_docid = rel.t_objid and acc.t_account = doc.t_account and acc.t_chapter = 1 and acc.t_code_currency = doc.t_currency and doc.t_firole = 10000+ rel.t_id)
                       when rel.t_objdockind = 12 and t_objtype = 1 then (select distinct decode(acc.t_userfield4, chr(1), chr(1), '0000'||'#'||'IBSOXXX'||'#'||acc.t_userfield4) from dmcaccdoc_dbt doc, daccount_dbt acc  where doc.t_catid = 1019 and doc.t_fiid = rel.t_objid and doc.t_docid = rel.t_id  and acc.t_account = doc.t_account and acc.t_chapter = 1 and acc.t_code_currency = doc.t_currency)
                       when rel.t_objdockind = 12 and t_objtype = 2 then (select distinct decode(acc.t_userfield4, chr(1), chr(1), '0000'||'#'||'IBSOXXX'||'#'||acc.t_userfield4) from dmcaccdoc_dbt doc, daccount_dbt acc  where doc.t_catid = 1021 and doc.t_fiid = rel.t_objid and doc.t_docid = rel.t_id  and acc.t_account = doc.t_account and acc.t_chapter = 1 and acc.t_code_currency = doc.t_currency)
                       end ) INC_ACC_CODE,
              (case when rel.t_objdockind = 24 and rel.t_objtype = 4 then (select distinct decode(acc.t_userfield4, chr(1), chr(1), '0000'||'#'||'IBSOXXX'||'#'||acc.t_userfield4) from dmcaccdoc_dbt doc, daccount_dbt acc  where doc.t_catid = 1022 and doc.t_fiid = rel.t_objid and doc.t_docid = rel.t_id  and acc.t_account = doc.t_account and acc.t_chapter = 1 and acc.t_code_currency = doc.t_currency) 
                       when rel.t_objdockind = 24 and rel.t_objtype = 3 then (select distinct decode(acc.t_userfield4, chr(1), chr(1), '0000'||'#'||'IBSOXXX'||'#'||acc.t_userfield4) from dmcaccdoc_dbt doc, daccount_dbt acc  where doc.t_catid = 1024 and doc.t_fiid = rel.t_objid and doc.t_docid = rel.t_id  and acc.t_account = doc.t_account and acc.t_chapter = 1 and acc.t_code_currency = doc.t_currency)
                       when rel.t_objdockind in(102, 103) and t_objtype = 6 then  (select distinct decode(acc.t_userfield4, chr(1), chr(1), '0000'||'#'||'IBSOXXX'||'#'||acc.t_userfield4) from dmcaccdoc_dbt doc, daccount_dbt acc  where doc.t_catid = 1017 and doc.t_dockind = 102 and doc.t_docid = rel.t_objid  and acc.t_account = doc.t_account and acc.t_chapter = 1 and acc.t_code_currency = doc.t_currency and doc.t_firole = 10000+ rel.t_id) 
                       when rel.t_objdockind in(102, 103) and t_objtype = 5 then  (select distinct decode(acc.t_userfield4, chr(1), chr(1), '0000'||'#'||'IBSOXXX'||'#'||acc.t_userfield4) from dmcaccdoc_dbt doc, daccount_dbt acc  where doc.t_catid = 1015 and doc.t_dockind = 102 and doc.t_docid = rel.t_objid  and acc.t_account = doc.t_account and acc.t_chapter = 1 and acc.t_code_currency = doc.t_currency and doc.t_firole = 10000+ rel.t_id)
                       when rel.t_objdockind = 12 and t_objtype = 1 then (select distinct decode(acc.t_userfield4, chr(1), chr(1), '0000'||'#'||'IBSOXXX'||'#'||acc.t_userfield4) from dmcaccdoc_dbt doc, daccount_dbt acc  where doc.t_catid = 1018 and doc.t_fiid = rel.t_objid and doc.t_docid = rel.t_id  and acc.t_account = doc.t_account and acc.t_chapter = 1 and acc.t_code_currency = doc.t_currency)
                       when rel.t_objdockind = 12 and t_objtype = 2 then (select distinct decode(acc.t_userfield4, chr(1), chr(1), '0000'||'#'||'IBSOXXX'||'#'||acc.t_userfield4) from dmcaccdoc_dbt doc, daccount_dbt acc  where doc.t_catid = 1020 and doc.t_fiid = rel.t_objid and doc.t_docid = rel.t_id  and acc.t_account = doc.t_account and acc.t_chapter = 1 and acc.t_code_currency = doc.t_currency)
              end ) DEC_ACC_CODE,
              (case when rel.t_objdockind = 24 and rel.t_objtype = 4 then (select distinct t_account from dmcaccdoc_dbt where t_catid = 1023 and t_fiid = rel.t_objid and t_docid = rel.t_id) 
                       when rel.t_objdockind = 24 and rel.t_objtype = 3 then (select distinct t_account from dmcaccdoc_dbt where t_catid = 1025 and t_fiid = rel.t_objid and t_docid = rel.t_id)
                       when rel.t_objdockind in(102, 103) and t_objtype = 6 then  (select distinct t_account from dmcaccdoc_dbt where t_catid = 1016 and t_dockind = 102 and t_docid = rel.t_objid and t_firole = 10000+ rel.t_id)   
                       when rel.t_objdockind in(102, 103) and t_objtype = 5 then  (select distinct t_account from dmcaccdoc_dbt where t_catid = 1014 and t_dockind = 102 and t_docid = rel.t_objid and t_firole = 10000+ rel.t_id)
                       when rel.t_objdockind = 12 and t_objtype = 1 then (select distinct t_account from dmcaccdoc_dbt where t_catid = 1019 and t_fiid = rel.t_objid and t_docid = rel.t_id)
                       when rel.t_objdockind = 12 and t_objtype = 2 then (select distinct t_account from dmcaccdoc_dbt where t_catid = 1021 and t_fiid = rel.t_objid and t_docid = rel.t_id)
                       end ) INC_ACC_NUM,
              (case when rel.t_objdockind = 24 and rel.t_objtype = 4 then (select distinct t_account from dmcaccdoc_dbt where t_catid = 1022 and t_fiid = rel.t_objid and t_docid = rel.t_id) 
                       when rel.t_objdockind = 24 and rel.t_objtype = 3 then (select distinct t_account from dmcaccdoc_dbt where t_catid = 1024 and t_fiid = rel.t_objid and t_docid = rel.t_id)
                       when rel.t_objdockind in(102, 103) and t_objtype = 6 then  (select distinct t_account from dmcaccdoc_dbt where t_catid = 1017 and t_dockind = 102 and t_docid = rel.t_objid and t_firole = 10000+ rel.t_id) 
                       when rel.t_objdockind in(102, 103) and t_objtype = 5 then  (select distinct t_account from dmcaccdoc_dbt where t_catid = 1015 and t_dockind = 102 and t_docid = rel.t_objid and t_firole = 10000+ rel.t_id)
                       when rel.t_objdockind = 12 and t_objtype = 1 then (select distinct t_account from dmcaccdoc_dbt where t_catid = 1018 and t_fiid = rel.t_objid and t_docid = rel.t_id)
                       when rel.t_objdockind = 12 and t_objtype = 2 then (select distinct t_account from dmcaccdoc_dbt where t_catid = 1020 and t_fiid = rel.t_objid and t_docid = rel.t_id)
              end ) DEC_ACC_NUM,
              in_dwhRecStatus REC_STATUS,
              in_dwhSysMoment SYSMOMENT,
              in_dwhEXT_FILE  EXT_FILE
 
        from DDLHDGRFAIRVAL_DBT val
          inner join dfininstr_dbt fin on fin.t_fiid = val.t_curfiid
          inner join ddlhdgrelation_dbt rel on rel.t_id = val.t_relationid 
          where val.t_id = in_HEDG_id and val.t_date <= in_date
        )  

    loop
        qb_dwh_utils.ins_FCT_HEDG_CHG (hedg_rec.dt, 
                                       hedg_rec.code, 
                                       hedg_rec.deal_code, 
                                       hedg_rec.finstr_code, 
                                       hedg_rec.asudr_deal_code, 
                                       hedg_rec.portfolio_code, 
                                       hedg_rec.sub_portf_code, 
                                       hedg_rec.currency_curr_code_txt,                   
                                       hedg_rec.cost_on_date, 
                                       hedg_rec.prev_cost, 
                                       hedg_rec.chg_amount, 
                                       hedg_rec.deal_kind_code, 
                                       hedg_rec.hedge_rel_code, 
                                       hedg_rec.hedg_begin_dt, 
                                       hedg_rec.hedg_end_dt, 
                                       hedg_rec.hedg_tool_code,
                                       hedg_rec.tool_code_sofr, 
                                       hedg_rec.inc_acc_code, 
                                       hedg_rec.dec_acc_code, 
                                       hedg_rec.inc_acc_num, 
                                       hedg_rec.dec_acc_num, 
                                       hedg_rec.rec_status, 
                                       hedg_rec.sysmoment, 
                                       hedg_rec.ext_file);
    end loop;

  end;

  procedure export_HEDG_Status_Add(in_UploadID in number,
                                  in_department in number,
                                  in_date       in date) is
    CntDeal      number := 0;
    EventID      number := 0;
    dwhRecStatus varchar2(1);
    dwhDT        varchar2(10);
    dwhSysMoment varchar2(19);
    dwhEXT_FILE  varchar2(300);
  begin
    -- Установим начало выгрузки хеджирования
    qb_bp_utils.startevent(cEvent_EXPORT_HEDG, in_UploadID, EventID);

    qb_bp_utils.SetAttrValue(EventID, cAttrRec_Status, qb_dwh_utils.REC_ADD);
    qb_bp_utils.SetAttrValue(EventID, cAttrDepartment, in_department);
    qb_bp_utils.SetAttrValue(EventID, cAttrDT, in_date);

    InitExportData(EventID, dwhRecStatus, dwhDT, dwhSysMoment, dwhEXT_FILE);
    -- Пробежимся по курсору
    for rec in cur_HEDG_For_Export(in_department, in_date) loop
      CntDeal := CntDeal + 1;
      -- Запишем сделку по которой начата операция выгрузки
      qb_bp_utils.SetAttrValue(EventID, cDealID, rec.HEDG_id, CntDeal);
      begin
      
        export_HEDG(rec.HEDG_id, in_date, dwhRecStatus, dwhDT, dwhSysMoment, dwhEXT_FILE);
      exception
        when others then
          -- пока не останавливаем обработку что бы максимально отследить ошибки, дальше по требованиям заказчика решать будем
          qb_bp_utils.SetError(EventID, SQLCODE, SQLERRM, 2, cDealID, rec.HEDG_id);
      end;
      commit;
    end loop;
    --Завершим выгрузку новых сделок
    qb_bp_utils.EndEvent(EventID, null);
    commit;
  end;

  ------------------------------------------------------
  -- Выгрузим все за дату
  ------------------------------------------------------
  procedure export_all(in_Date   date,
                       in_id     in number default 0,
                       in_id_pre in number default 0) is
  vLdrClear varchar2(400);
  vDateLastOD date;
  vDateBeg    date;
  begin
  vLdrClear := nvl(RSB_Common.GetRegStrValue('РСХБ\ИНТЕГРАЦИЯ\ЦХД\TRUNCATE_LDRINFA'),'YES');
  if (vLdrClear = 'YES') then
     qb_dwh_utils.clearAll(1);
  end if;
    for i in (select d.t_code from ddp_dep_dbt d where d.t_code = 1
             /*select distinct d.t_code
                from ddp_dep_dbt d
               where exists (select 1
                        from ddl_tick_dbt t
                       where t.t_bofficekind in (102, 208)
                         and t.t_department = d.t_code) -- Есть КЛ/Транш/Сделка
                  or exists (select 1
                        from ddl_genagr_dbt g
                       where g.t_department = d.t_code) -- Есть ГС
                  or exists (select 1
                        from ddl_order_dbt o
                       where o.t_department = d.t_code) -- Есть ДЗ
              */) loop

      vDateLastOD := qb_dwh_utils.GetLastClosedOD(i.t_code);
      vDateBeg   := sysdate;
      -- Выгрузка Ген соглашений
      export_GenAgr_Status_Add(nvl(in_id,0), i.t_code, in_date);
      -- Выгрузка кредитных линий/траншей/обычных сделок МБК
      export_Deals_Status_Add(nvl(in_id,0), i.t_code, in_date);
      -- Выгрузка обеспечений
      export_Ens_Contract_Status_Add(nvl(in_id,0), i.t_code, in_date);
      -- Выгрузка сделок по драгоценным металам
      if cPrecious_Metals = 1 then
        export_Precious_Metals_Status_Add(nvl(in_id,0), i.t_code, in_date);
      end if;
      if cCSA = 1 then
        export_CSA_Status_Add(nvl(in_id,0), i.t_code, in_date);
      end if;
      if cHEDG = 1 then
        export_HEDG_Status_Add(nvl(in_id,0), i.t_code, in_date);
      end if;
      qb_dwh_utils.add_export_log ( nvl(in_id,0),
                                    nvl(in_id_pre,0),
                                    qb_dwh_utils.GetCODE_DEPARTMENT(i.t_code),
                                    vDateLastOD,
                                    vDateBeg,
                                    sysdate ) ;
    end loop;
    -- Выгрузим справочники
    export_Dict_Status_Add(in_id, 1, in_Date);

    commit;
  end;

end qb_dwh_export;
/
