create or replace package body USR_RPRT_OD3 as

  --p_leg - Ид записи в ddl_leg_dbt
  function f_get_dealrepo_account(p_dealid      in integer
                                 ,p_leg         in integer
                                 ,p_dealtype    in integer
                                 ,p_bofficekind in integer
                                 ,p_date        in date default sysdate) return varchar2 is
    v_account varchar2(20 char) := chr(1);
  begin
    --обратное РЕПО
    if RSB_SECUR.IsBuy(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(p_dealtype, p_bofficekind))) = 1
    then
      begin
        select t_account
          into v_account
          from dmcaccdoc_dbt
         where t_dockind = REPOPART_DOCKIND
           and t_docid = p_leg
           and t_catnum = 234 /*Ц/б, ПВО*/
           and t_firole = 3;
      exception
        when others then
          return chr(1);
      end;
    end if;
    --прямое РЕПО на корзину. Старая КУ
    if RSB_SECUR.IsBasket(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(p_dealtype, p_bofficekind))) = 1
    then
      begin
        select t_account
          into v_account
          from dmcaccdoc_dbt
         where t_dockind = 4620
           and t_catnum = 1237 /*Ц/б, Корзина БПП*/
           and t_docid = (select nvl(min(t_id), -1) from ddl_tick_ens_dbt where t_dealid = p_dealid)
           and rsb_account.restac(t_account, t_currency, p_date, t_chapter, null) != 0;
      exception
        when no_data_found then
          v_account := chr(1);
        when others then
          return chr(1);
      end;
    end if;
    --прямое РЕПО на корзину.
    if v_account = chr(1)
       and RSB_SECUR.IsBasket(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(p_dealtype, p_bofficekind))) = 1
    then
      begin
        select t_account
          into v_account
          from dmcaccdoc_dbt mcacc
         where t_dockind = 4620
           and t_catnum = 231 /*Наш портфель*/
           and t_docid = (select nvl(min(t_id), -1) from ddl_tick_ens_dbt where t_dealid = p_dealid)
           and 1 = (select t_value4
                      from dmctempl_dbt
                     where t_catid = mcacc.t_catid
                       and t_number = mcacc.t_templnum)
           and rsb_account.restac(mcacc.t_account, mcacc.t_currency, p_date, mcacc.t_chapter, null) != 0;
      exception
        when no_data_found then
          v_account := chr(1);
        when others then
          return chr(1);
      end;
    end if;
    --РЕПО в РЕПО
    if v_account = chr(1)
    then
      begin
        select t_account
          into v_account
          from dmcaccdoc_dbt
         where t_dockind = REPOPART_DOCKIND
           and t_docid = p_leg
           and t_catnum = 1246 /*Ц/б, ПВО_БПП*/
           and t_firole = 36;
      exception
        when no_data_found then
          v_account := chr(1);
        when others then
          return chr(1);
      end;
    end if;
    --Прямое РЕПО. Старая КУ
    if v_account = chr(1)
    then
      begin
        select t_account
          into v_account
          from dmcaccdoc_dbt
         where t_dockind = REPOPART_DOCKIND
           and t_docid = p_leg
           and t_catnum = 233 /*Ц/б, БПП*/
           and p_date between t_activatedate and t_disablingdate - 1;
      exception
        when others then
          v_account := chr(1);
      end;
    end if;
    --Прямое РЕПО.
    if v_account = chr(1)
    then
      begin
        select t_account
          into v_account
          from dmcaccdoc_dbt mcacc
         where t_dockind = REPOPART_DOCKIND
           and t_docid = p_leg
           and t_catnum = 231 /*Наш портфель*/
           and 1 = (select t_value4
                      from dmctempl_dbt
                     where t_catid = mcacc.t_catid
                       and t_number = mcacc.t_templnum)
           and rsb_account.restac(mcacc.t_account, mcacc.t_currency, p_date, mcacc.t_chapter, null) != 0;
      exception
        when others then
          v_account := chr(1);
      end;
    end if;
    /* Это ещё нужно?
    IF v_account = chr(1) THEN
      begin
        select t_account into v_account from dmcaccdoc_dbt where t_id = (select max(t_id) from dmcaccdoc_dbt where t_dockind = REPOPART_DOCKIND and t_docid = p_leg and t_catnum = 233);
      exception
        when others then
          v_account := chr(1);
      end;
    END IF;
    */
    return v_account;
  end f_get_dealrepo_account;

  --Корректировка до суммы ЭПС. По лоту сделки РЕПО
  function f_get_corrinttoeir(p_dealid in integer
                             ,p_fiid   in integer
                             ,p_date   in date default sysdate) return number is
    v_corrintoeir number := 0;
    v_count       integer := 0;
  begin
    begin
      select count(1)
        into v_count
        from v_scwrthistex
       where t_state in (1, 3)
         and t_fiid = p_fiid
         and t_dealid = p_dealid
         and t_corrinttoeir <> 0;
    exception
      when others then
        v_count := 0;
    end;
    if v_count > 0
    then
      begin
        select sum(t_correstreserve)
          into v_corrintoeir
          from v_scwrthistex v
         where t_state in (1, 3)
           and t_amount > 0
           and t_fiid = p_fiid
           and v.t_dealid = p_dealid
           and t_instance = (select max(t_instance)
                               from v_scwrthistex
                              where v.t_sumid = t_sumid
                                and v.t_changedate = t_changedate)
           and v.t_changedate = (select max(t.t_changedate)
                                   from v_scwrthistex t
                                  where v.t_sumid = t_sumid
                                    and t.t_changedate <= p_date);
      exception
        when others then
          v_corrintoeir := 0;
      end;
    end if;
    return v_corrintoeir;
  end f_get_corrinttoeir;

  --Корректировка до суммы оценочного резерва. По лоту сделки РЕПО
  function f_get_correstreserve(p_dealid in integer
                               ,p_fiid   in integer
                               ,p_date   in date default sysdate) return number is
    v_correstreserve number := 0;
    v_count          integer := 0;
  begin
    begin
      select count(1)
        into v_count
        from v_scwrthistex
       where t_state in (1, 3)
         and t_fiid = p_fiid
         and t_dealid = p_dealid
         and t_corrinttoeir <> 0;
    exception
      when others then
        v_count := 0;
    end;
    if v_count > 0
    then
      begin
        select sum(t_correstreserve)
          into v_correstreserve
          from v_scwrthistex v
         where t_state in (1, 3)
           and t_amount > 0
           and t_fiid = p_fiid
           and v.t_dealid = p_dealid
           and t_instance = (select max(t_instance)
                               from v_scwrthistex
                              where v.t_sumid = t_sumid
                                and v.t_changedate = t_changedate)
           and v.t_changedate = (select max(t.t_changedate)
                                   from v_scwrthistex t
                                  where v.t_sumid = t_sumid
                                    and t.t_changedate <= p_date);
      exception
        when others then
          v_correstreserve := 0;
      end;
    end if;
    return v_correstreserve;
  end f_get_correstreserve;

  --Сумма оценочного резерва. По лоту сделки РЕПО
  function f_get_estreserve(p_dealid in integer
                           ,p_fiid   in integer
                           ,p_date   in date default sysdate) return number is
    v_estreserve number := 0;
    v_count      integer := 0;
  begin
    begin
      select count(1)
        into v_count
        from v_scwrthistex
       where t_state in (1, 3)
         and t_fiid = p_fiid
         and t_dealid = p_dealid
         and t_corrinttoeir <> 0;
    exception
      when others then
        v_count := 0;
    end;
    if v_count > 0
    then
      begin
        select sum(t_estreserve)
          into v_estreserve
          from v_scwrthistex v
         where t_state in (1, 3)
           and t_amount > 0
           and t_fiid = p_fiid
           and v.t_dealid = p_dealid
           and t_instance = (select max(t_instance)
                               from v_scwrthistex
                              where v.t_sumid = t_sumid
                                and v.t_changedate = t_changedate)
           and v.t_changedate = (select max(t.t_changedate)
                                   from v_scwrthistex t
                                  where v.t_sumid = t_sumid
                                    and t.t_changedate <= p_date);
      exception
        when others then
          v_estreserve := 0;
      end;
    end if;
    return v_estreserve;
  end f_get_estreserve;

  -- Формирование отчета в CSV 
  function make_report(p_dt     date
                      ,o_errmsg out varchar2) return number as
    pragma autonomous_transaction;
    v_sessionid  integer := sys_context('USERENV', 'SESSIONID');
    v_isBond     boolean;
    v_report     clob;
    v_nline      integer := 0;
    v_part       integer := 1;
    v_MarketRate number;
    v_AvgRate    number;
    v_RateType   number;
    v_Rate       number;
    v_Scale      number;
    v_Point      number;
    v_IsInverse  char(1);
    v_id_file    number;
  begin
    delete from itt_file f
     where f.file_code = it_file.C_FILE_CODE_REP_OD3
       and (f.create_sysdate < sysdate - 1 or f.sessionid = v_sessionid);
    commit;
    dbms_lob.createtemporary(lob_loc => v_report, cache => true);
    for cur in (select (select t_name from dparty_dbt where t_partyid = (select t_partyid from ddp_dep_dbt where t_code = tick.t_department)) department
                      ,(select t_name from dparty_dbt where t_partyid = tick.t_partyid) contractor
                      ,case
                         when RSB_SECUR.IsBuy(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(tick.t_DealType, tick.t_BofficeKind))) = 1 then
                          'покупка с обязательством обратной продажи'
                         else
                          'продажа с обязательством обратного выкупа'
                       end operkind
                      ,(select t_ccy from dfininstr_dbt where t_fiid = leg1.t_cfi) currency
                      ,(select t_name
                          from davrkinds_dbt
                         where t_avoirkind = rsb_secur.SecurKind(fininstr.t_avoirkind)
                           and t_fi_kind = 2) pfikind
                      ,nvl((select t_name from dparty_dbt where t_partyid = fininstr.t_issuer), ' ') issuername
                      ,leg1.t_principal principal
                      ,fininstr.t_facevalue facevalue
                      , --Перевести в валюту сделки?
                       leg1.t_maturity leg1_date
                      ,leg2.t_maturity leg2_date
                      ,leg2.t_maturity - leg1.t_maturity duration
                      ,case
                         when leg1.t_relativeprice = 'X' then
                          round(leg1.t_cost / leg1.t_principal, 4)
                         else
                          leg1.t_price
                       end leg1_price
                      ,leg1.t_nkd leg1_nkd
                      ,leg2.t_nkd - leg1.t_nkd indeal_nkd
                      ,case
                         when leg2.t_relativeprice = 'X' then
                          round(leg2.t_cost / leg2.t_principal, 4)
                         else
                          leg2.t_price
                       end leg2_price
                      ,leg2.t_nkd leg2_nkd
                      ,avoiriss.t_isin isin
                      ,avoiriss.t_lsin regcode
                      ,fininstr.t_avoirkind
                      ,tick.t_dealid dealid
                      ,tick.t_closedate closedate
                      ,fininstr.t_fiid fiid
                      ,leg1.t_cfi cfi
                      ,USR_RPRT_OD3.f_get_dealrepo_account(tick.t_dealid, leg1.t_id, tick.t_dealtype, tick.t_bofficekind, p_dt) deal_account
                      ,USR_RPRT_OD3.f_get_corrinttoeir(tick.t_dealid, fininstr.t_fiid, p_dt) t_corrinttoeir
                      ,USR_RPRT_OD3.f_get_correstreserve(tick.t_dealid, fininstr.t_fiid, p_dt) t_correstreserve
                      ,USR_RPRT_OD3.f_get_estreserve(tick.t_dealid, fininstr.t_fiid, p_dt) t_estreserve
                  from ddl_tick_dbt  tick
                      ,ddl_leg_dbt   leg1
                      ,ddl_leg_dbt   leg2
                      ,dfininstr_dbt fininstr
                      ,davoiriss_dbt avoiriss
                 where t_bofficekind = 101
                   and rsb_secur.DealIsRepo(tick.t_dealid) = 1
                   and t_dealdate <= p_dt --открыты ранее отчетной даты
                   and (trunc(t_dealdate, 'yyyy') = trunc(p_dt, 'yyyy') or --открытые в дату отчётного года
                       t_closedate = to_date('01.01.0001', 'dd.mm.yyyy') or --ещё не закрытые
                       t_closedate > p_dt or --открытые за отчетную дату
                       (trunc(t_dealdate, 'yyyy') < trunc(p_dt, 'yyyy') and
                       (t_closedate = to_date('01.01.0001', 'dd.mm.yyyy') or trunc(t_closedate, 'yyyy') = trunc(p_dt, 'yyyy')))) -- открытые на 01 января отчетного года
                   and tick.t_dealid = leg1.t_dealid
                   and leg1.t_legid = 0
                   and leg1.t_legkind = 0
                   and tick.t_dealid = leg2.t_dealid
                   and leg2.t_legid = 0
                   and leg2.t_legkind = 2
                   and tick.t_pfi = fininstr.t_fiid
                   and tick.t_pfi = avoiriss.t_fiid
                   and tick.t_dealstatus <> 0
                   and tick.t_clientid = -1
                 order by leg1.t_maturity)
    loop
      v_nline      := v_nline + 1;
      v_isBond     := RSI_RSB_FIInstr.FI_IsAvrKindBond(AvoirKind => cur.t_avoirkind);
      v_RateType   := 1;
      v_MarketRate := RSI_RSB_FIInstr.ConvSum2(SumB => 1
                                              ,pFromFI => cur.fiid
                                              ,pToFI => cur.cfi
                                              ,pbdate => p_dt
                                               --, pround     => 
                                              ,pRateType => v_RateType
                                              ,pRate => v_Rate
                                              ,pScale => v_Scale
                                              ,pPoint => v_Point
                                              ,pIsInverse => v_IsInverse);
      v_RateType   := 4;
      v_AvgRate    := RSI_RSB_FIInstr.ConvSum2(SumB => 1
                                              ,pFromFI => cur.fiid
                                              ,pToFI => cur.cfi
                                              ,pbdate => p_dt
                                               --, pround     => 
                                              ,pRateType => v_RateType
                                              ,pRate => v_Rate
                                              ,pScale => v_Scale
                                              ,pPoint => v_Point
                                              ,pIsInverse => v_IsInverse);
      v_report := v_report || it_rsl_string.GetCell(cur.department) || --  Филиал
                  it_rsl_string.GetCell(SubStr(cur.deal_account, 1, 5)) || -- Номер счета второго порядка
                  it_rsl_string.GetCell(cur.deal_account) || -- Номер лицевого счета (20 знаков)
                  it_rsl_string.GetCell(cur.contractor) || -- Контрагент
                  it_rsl_string.GetCell(cur.operkind) || -- Вид операции (продажа с обязательством обратного выкупа; покупка с обязательством обратной продажи)
                  it_rsl_string.GetCell(cur.currency) || -- Валюта
                  it_rsl_string.GetCell(cur.pfikind) || -- Базовый актив (вид ценной бумаги)
                  it_rsl_string.GetCell(cur.issuername) || -- Эмитент
                  it_rsl_string.GetCell(cur.principal) || -- Количество ценных бумаг
                  it_rsl_string.GetCell(cur.facevalue) || -- Номинал ценной бумаги
                  it_rsl_string.GetCell(cur.leg1_date) || -- Дата покупки (продажи)
                  it_rsl_string.GetCell(cur.leg2_date) || -- Дата совершения обратной сделки
                  it_rsl_string.GetCell(cur.duration) || -- Кол-во дней
                  it_rsl_string.GetCell(cur.leg1_price) || -- Цена покупки (продажи) одной бумаги
                  it_rsl_string.GetCell(cur.leg1_nkd) || -- Купон
                  it_rsl_string.GetCell(cur.indeal_nkd) || -- Купон, уплаченный / полученный внутри периода сделки РЕПО
                  it_rsl_string.GetCell(cur.leg2_price) || -- Цена обратной сделки по одной ценной бумаге
                  it_rsl_string.GetCell(cur.leg2_nkd) || -- Купон
                  it_rsl_string.GetCell('') || -- Сумма авансового платежа
                  it_rsl_string.GetCell(v_MarketRate) || -- Рыночная котировка по состоянию на отчетную дату; если сделки заключаются на внебиржевом рынке, то цена последней котировки на покупку
                  it_rsl_string.GetCell(v_AvgRate) || -- Средневзвешенная котировка по состоянию на отчетную дату.
                  it_rsl_string.GetCell('') || -- На основе иных методик оценки (если рыночная оценка отсутствует)
                  it_rsl_string.GetCell('') || -- Название рейтингового агенства
                  it_rsl_string.GetCell('') || -- Для сделок прямого РЕПО, открытых по состоянию на отчетную дату: Рейтинг ценной бумаги (по данным рейтинговых агентств, с указанием наименования агентства)
                  it_rsl_string.GetCell('') || -- Особые условия залога
                  it_rsl_string.GetCell(cur.isin) || -- Код ISIN
                  it_rsl_string.GetCell(cur.regcode) || -- Гос.Рег.
                  it_rsl_string.GetCell(case
                                          when v_IsBond then
                                           cur.t_corrinttoeir
                                          else
                                           0
                                        end) || -- Корректировки, увеличивающие и уменьшающие стоимость долговых ценных
                  it_rsl_string.GetCell(cur.t_correstreserve) || -- Корректировка резерва на возможные потери
                  it_rsl_string.GetCell(cur.t_estreserve) || -- Сумма оценочного резерва под ОКУ
                  it_rsl_string.GetCell(case
                                          when not v_IsBond then
                                           cur.t_corrinttoeir
                                          else
                                           0
                                        end) || -- Корректировки, увеличивающие и уменьшающие стоимость долевых ценных бумаг
                  it_rsl_string.GetCell('', true); -- Сумма предварительных существенных затрат
      if v_nline >= 1000000
      then
        it_rsl_string.CSVTemplate(v_report);
        v_id_file := it_file.insert_file(p_file_name => 'OLD_ОД_3_' || to_char(p_dt, 'yyyymmdd') || '_p' || v_part || '.csv'
                                        ,p_file_clob => v_report
                                        ,p_from_system => it_file.C_SOFR_DB
                                        ,p_from_module => $$plsql_unit
                                        ,p_to_system => it_file.C_SOFR_DB
                                        ,p_to_module => null
                                        ,p_create_user => user
                                        ,p_file_code => it_file.C_FILE_CODE_REP_OD3
                                        ,p_part_no => v_part
                                        ,p_sessionid => v_sessionid);
        v_part    := v_part + 1;
        v_report  := null;
        v_nline   := 0;
      end if;
    end loop;
    if v_nline > 0
    then
      it_rsl_string.CSVTemplate(v_report);
      v_id_file := it_file.insert_file(p_file_name => 'OLD_ОД_3_' || to_char(p_dt, 'yyyymmdd') || '_p' || v_part || '.csv'
                                      ,p_file_clob => v_report
                                      ,p_from_system => it_file.C_SOFR_DB
                                      ,p_from_module => $$plsql_unit
                                      ,p_to_system => it_file.C_SOFR_DB
                                      ,p_to_module => null
                                      ,p_create_user => user
                                      ,p_file_code => it_file.C_FILE_CODE_REP_OD3
                                      ,p_part_no => v_part
                                      ,p_sessionid => v_sessionid);
    end if;
    commit;
    return v_sessionid;
  exception
    when others then
      rollback;
      o_errmsg := 'Ошибка формирования отчета :' || it_q_message.get_errtxt(sqlerrm);
      it_error.put_error_in_stack;
      it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
      return 0;
  end;

/* while (sql.MoveNext() )
    account = sql.value("deal_account");
    ConvSum (MarketRate, $1, ReportDate, sql.value("fiid"), sql.value("cfi"), 1);
    ConvSum (AvgRate, $1, ReportDate, sql.value("fiid"), sql.value("cfi"), 4);

    IsBond = FI_IsBond(sql.value("fiid"));

    rn = rn + 1;
    Rep.SetExecute("iBook.Sheets("+sheet+").Rows("+rn+").WrapText = true;");

    Rep.AddPrintCell(sql.value("department"),       0, 0, format_line); // Филиал
    Rep.AddPrintCell(SubStr(account, 1, 5),         0, 0, format_line); // Номер счета второго порядка
    Rep.AddPrintCell(account,                       0, 0, format_line); // Номер лицевого счета (20 знаков)
    Rep.AddPrintCell(sql.value("contractor"),       0, 0, format_line); // Контрагент
    Rep.AddPrintCell(sql.value("operkind"),         0, 0, format_line); // Вид операции (продажа с обязательством обратного выкупа; покупка с обязательством обратной продажи)
    Rep.AddPrintCell(sql.value("currency"),         0, 0, format_line); // Валюта
    Rep.AddPrintCell(sql.value("pfikind"),          0, 0, format_line); // Базовый актив (вид ценной бумаги)
    Rep.AddPrintCell(sql.value("issuername"),       0, 0, format_line); // Эмитент
    Rep.AddPrintCell(sql.value("principal"),        0, 0, format_line); // Количество ценных бумаг
    Rep.AddPrintCell(sql.value("facevalue"),        0, 0, format_line); // Номинал ценной бумаги
    Rep.AddPrintCell(Date(sql.value("leg1_date")),  0, 0, format_line); // Дата покупки (продажи)
    Rep.AddPrintCell(Date(sql.value("leg2_date")),  0, 0, format_line); // Дата совершения обратной сделки
    Rep.AddPrintCell(int(sql.value("duration")),    0, 0, format_line); // Кол-во дней
    Rep.AddPrintCell(sql.value("leg1_price"),       0, 0, format_line); // Цена покупки (продажи) одной бумаги
    Rep.AddPrintCell(sql.value("leg1_nkd"),         0, 0, format_line); // Купон
    Rep.AddPrintCell(sql.value("indeal_nkd"),       0, 0, format_line); // Купон, уплаченный / полученный внутри периода сделки РЕПО
    Rep.AddPrintCell(sql.value("leg2_price"),       0, 0, format_line); // Цена обратной сделки по одной ценной бумаге
    Rep.AddPrintCell(sql.value("leg2_nkd"),         0, 0, format_line); // Купон
    Rep.AddPrintCell("",                            0, 0, format_line); // Сумма авансового платежа
    Rep.AddPrintCell(MarketRate,                    0, 0, format_line); // Рыночная котировка по состоянию на отчетную дату; если сделки заключаются на внебиржевом рынке, то цена последней котировки на покупку
    Rep.AddPrintCell(AvgRate,                       0, 0, format_line); // Средневзвешенная котировка по состоянию на отчетную дату.
    Rep.AddPrintCell("",                            0, 0, format_line); // На основе иных методик оценки (если рыночная оценка отсутствует)
    Rep.AddPrintCell("",                            0, 0, format_line); // Название рейтингового агенства
    Rep.AddPrintCell("",                            0, 0, format_line); // Для сделок прямого РЕПО, открытых по состоянию на отчетную дату: Рейтинг ценной бумаги (по данным рейтинговых агентств, с указанием наименования агентства)
    Rep.AddPrintCell("",                            0, 0, format_line); // Особые условия залога
    Rep.AddPrintCell(sql.value("isin"),             0, 0, format_line); // Код ISIN
    Rep.AddPrintCell(sql.value("regcode"),          0, 0, format_line); // Гос.Рег.

    if (IsBond)
      Rep.AddPrintCell(sql.value("t_corrinttoeir"), 0, 0, format_line); // Корректировки, увеличивающие и уменьшающие стоимость долговых ценных
    else
      Rep.AddPrintCell("",                          0, 0, format_line); // Корректировки, увеличивающие и уменьшающие стоимость долговых ценных
    end;
    Rep.AddPrintCell(sql.value("t_correstreserve"), 0, 0, format_line); // Корректировка резерва на возможные потери
    Rep.AddPrintCell(sql.value("t_estreserve"),     0, 0, format_line); // Сумма оценочного резерва под ОКУ

    if (not IsBond)
      Rep.AddPrintCell(sql.value("t_corrinttoeir"), 0, 0, format_line); // Корректировки, увеличивающие и уменьшающие стоимость долевых ценных бумаг
     else
      Rep.AddPrintCell("",                          0, 0, format_line); // Корректировки, увеличивающие и уменьшающие стоимость долевых ценных бумаг
    end;
    Rep.AddPrintCell("",                            0, 0, format_line); // Сумма предварительных существенных затрат

    Rep.AddStr();
    UseProgress(count = count + 1);*/
end USR_RPRT_OD3;
/
