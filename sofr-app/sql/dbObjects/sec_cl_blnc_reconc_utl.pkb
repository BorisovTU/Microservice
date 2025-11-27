create or replace package body sec_cl_blnc_reconc_utl as
  --Encoding: Win 866

  /**
   @file      sec_cl_blnc_reconc_utl
   @brief       Утилиты для отчета-сверки с Диасофт

   # changeLog
   |date       |author         |tasks                                                     |note
   |-----------|---------------|----------------------------------------------------------|-------------------------------------------------------------
   |2025.07.16 |Велигжанин А.В.|DEF-93865                                                 | cl_GatherDataInnerAccounting(), остаток на дату действия счета
   |2024.07.27 |Дылгеров Ц.В.  |DEF-63390                                                 | Перенос из sec_acts.mac заполнения dSp_Acts_Tmp
   |2024.02.29 |Велигжанин А.В.|DEF-60878                                                 | Доработка fill_buf_by_depo_data()
   |           |               |                                                          | Счет для сверки нужно мапить через {t_fiid, t_partyid, t_number}
   |           |               |                                                          | Бывают случаи, partyid=151208, за 29.12.2023,
   |           |               |                                                          | Когда данные из Депозитария маппатся на правильный счет,
   |           |               |                                                          | А в лотах счет -- неправильный
   |           |               |                                                          | Тогда через ID счета -- данные не сопоставятся.
   |           |               |                                                          | А через t_number -- сопоставятся.
   |2024.02.21 |Симанов А.     |DEF-59870                                                 | Создание

  */

  /*
    @brief time_since_varchar - время, прошедшее с момента события p_time
    @param[in] p_time - время события
  */
  function time_since_varchar(p_time pls_integer) return varchar2 is
  begin
    return to_char((dbms_utility.get_time - p_time) / 100,
                   'fm9999999990D00');
  end time_since_varchar;

  function Check_DepoAcc_TradePlace(p_depoacc_firstletter  varchar2
                                   ,p_depoacc_middlenumber varchar2
                                   ,p_marketid             number
                                   ,p_servkindsub          number) return number deterministic as
    v_market U_DEPOACC_TRADEPLACE.T_MARKET%type;
  begin
    select max(tp.t_market)
      into v_market
      from U_DEPOACC_TRADEPLACE tp
     where tp.t_depoacc_firstletter = p_depoacc_firstletter
       and tp.t_depoacc_middlenumber = p_depoacc_middlenumber ;
    if v_market = 'Исключение' then
       return 0;
    elsif v_market = 'ПАО СПБ Банк' and p_marketid = 151337 and p_servkindsub = 8 then 
       return 1;
    elsif v_market = 'НКО АО НРД' and p_marketid = 2 and p_servkindsub = 8 then
       return 1;
    elsif v_market is null  and p_marketid = -1 and p_servkindsub = 9 then
       return 1;
    else
       return 0;
    end if;  
  end;
  /*
    @brief fill_buf_by_depo_data - заполнение мапинговой таблицы (поправьте, если не так)
    @param[in] p_date - время заполнения
    @param[in] p_partyid - код субьекта клиента
    @param[in] p_fiid - код ЦБ инструмента
    @param[in] p_issuer -  код эмитента
    @param[in] p_avoirkind - код подвила инструмента
  */
  procedure fill_buf_by_depo_data(p_date       date,
                                  p_partyid    dparty_dbt.t_partyid%type,
                                  p_fiid       dpmwrtcl_dbt.t_fiid%type,
                                  p_issuer     dfininstr_dbt.t_issuer%type,
                                  p_avoirkind  davrkinds_dbt.t_avoirkind%type,
                                  p_contractid dsfcontr_dbt.t_id%type) is
    l_start_time pls_integer;
    n            NUMBER := 0;
    tmp_autoinc  NUMBER;
  begin
    l_start_time := dbms_utility.get_time;

    it_log.log(p_msg => 'Start: ', p_msg_type => it_log.c_msg_type__debug);

    FOR c IN (with rest_data as (
                 select --+ materialize full(rest)
                        rest.value,
                        case
                          when acc.t_contractnumber = '35/37-1193858-ИИС' then
                           '35/37-1193858-ИИС-1'
                          else
                           acc.t_contractnumber
                        end contractnumber, --wtf?
                        acc.t_sectioncode sectioncode,
                        (select tp.t_depoacc_firstletter
                           from U_DEPOACC_TRADEPLACE tp
                         where acc.t_sectioncode like tp.t_depoacc_firstletter || '%'
                           and tp.t_depoacc_middlenumber = acc.depoacc_middlenumber
                         order by length(tp.t_depoacc_firstletter) desc fetch first rows only ) depoacc_firstletter,
                        acc.depoacc_middlenumber ,
                        upper(case
                                when i.t_isin = 'RU000A01002C2' then
                                 'RU000A1002C2'
                                else
                                 i.t_isin
                              end) isin --wtf2?
                   from ddiasrestdepo_dbt rest
                   join (select substr(ad.t_sectioncode,
                               instr(ad.t_sectioncode, '-', 1, 2) + 1,
                               instr(ad.t_sectioncode, '-', 1, 3) - instr(ad.t_sectioncode, '-', 1, 2) - 1) depoacc_middlenumber, ad.* 
                           from ddiasaccdepo_dbt ad )   acc on rest.accdepoid = acc.t_sofraccid
                   join ddiasisin_dbt     i   on i.t_id = rest.isin
                   left join dsfcontr_dbt sfc on sfc.t_number = acc.t_contractnumber
                                             and sfc.t_servkind = 0
                   left join davoiriss_dbt av on av.t_isin = i.t_isin
                   left join dfininstr_dbt f  on f.t_fiid = av.t_fiid
                  where rest.reportdate = p_date
                    and (instr(acc.t_sectioncode, '-2727-') = 0 or
                        value != 0)
                    and (p_partyid = -1 or sfc.t_partyid = p_partyid)
                    and (p_fiid = -1 or av.t_fiid = p_fiid)
                    and (p_issuer = -1 or rsi_rsb_fiinstr.fi_getissuerondate(pfiid => av.t_fiid,
                                                                             pdate => p_date) = p_issuer)
                    and (p_avoirkind = -1 or
                        rsb_fiinstr.fi_avrkindseq(fi_kind        => f.t_fi_kind,
                                                   avoirkind      => p_avoirkind,
                                                   checkavoirkind => f.t_avoirkind) = 1))
                select av.t_fiid,
                       sum(rest.value) value,
                       sfc_m.t_partyid,
                       sfc_v.t_number as t_number,
                       max(sfc_v.t_id) AS sfc_do_id,
                       listagg(rest.sectioncode, ',') within group (order by rest.sectioncode) sectioncode
                  from rest_data rest
                  join dsfcontr_dbt sfc_m on sfc_m.t_number = rest.contractnumber
                                         and sfc_m.t_servkindsub = 0
                  join ddlcontr_dbt dlc_m on dlc_m.t_sfcontrid = sfc_m.t_id
                  join ddlcontrmp_dbt mp  on mp.t_dlcontrid = dlc_m.t_dlcontrid
                  join dsfcontr_dbt sfc_v on sfc_v.t_id = mp.t_sfcontrid
                                         and sfc_v.t_servkind = 1
                                         and Check_DepoAcc_TradePlace(p_depoacc_firstletter  => rest.depoacc_firstletter,
                                                                      p_depoacc_middlenumber => rest.depoacc_middlenumber,
                                                                      p_marketid             => mp.t_marketid,
                                                                      p_servkindsub          => sfc_v.t_servkindsub) = 1
                  left join davoiriss_dbt av on av.t_isin = rest.isin
                 where p_date between mp.t_mpregdate and case -- DEF-60878, во избежание дублей из площадок ДБО нужно выбирать только действующие на дату отчета
                         when mp.t_mpclosedate = to_date('1-1-1', 'dd-mm-yyyy') then
                          to_date('31-12-2099', 'dd-mm-yyyy')
                         else
                          mp.t_mpclosedate
                       end
                   and p_date between sfc_v.t_datebegin and case -- DEF-60878, во избежание дублей из договоров нужно выбирать только действующие на дату отчета
                         when sfc_v.t_dateclose = to_date('1-1-1', 'dd-mm-yyyy') then
                          to_date('31-12-2099', 'dd-mm-yyyy')
                         else
                          sfc_v.t_dateclose
                       end
                   and (p_contractid = -1 or sfc_v.t_id = p_contractid)
                 group by av.t_fiid, sfc_m.t_partyid, sfc_v.t_number
              ) LOOP
      IF MOD(n, 1000) = 0 THEN
        COMMIT;
      END IF;

      tmp_autoinc := 0;
      n           := n + 1;

      -- поиск сверочной записи
      BEGIN
        select r.t_autoinc
          INTO tmp_autoinc
          FROM dsp_acts_tmp r
          JOIN dsfcontr_dbt sf
            ON (sf.t_id = r.t_contrid)
         WHERE r.t_fiid = c.t_fiid
           and r.t_clientid = c.t_partyid
           and sf.t_number = c.t_number
           and rownum = 1;
      EXCEPTION
        WHEN others THEN
          tmp_autoinc := 0;
      END;

      IF (tmp_autoinc = 0) THEN
        -- нет, вставляем
        BEGIN
          INSERT INTO dsp_acts_tmp r
            (r.t_ClientID,
             r.t_FIID,
             r.t_RestDep,
             r.t_Rest,
             r.t_RestCB,
             r.t_ContrID,
             r.t_Account,
             r.t_sectioncode)
          VALUES
            (c.t_partyid, c.t_FIID, c.value, 0, 0, c.sfc_do_id, null, c.sectioncode);
        EXCEPTION
          WHEN others THEN
            null;
        END;
      ELSE
        -- eсть, изменяем
        UPDATE dsp_acts_tmp r
           SET r.t_Restdep = r.t_Restdep + c.value
              ,r.t_sectioncode = c.sectioncode
         WHERE r.t_autoinc = tmp_autoinc;
      END IF;

    END LOOP;
    COMMIT;

    it_log.log(p_msg      => 'End. rows: ' || n || '; time: ' ||
                             time_since_varchar(p_time => l_start_time),
               p_msg_type => it_log.c_msg_type__debug);

  exception
    when others then
      it_error.put_error_in_stack;
      it_log.log(p_msg => 'Error', p_msg_type => it_log.c_msg_type__error);
      it_error.clear_error_stack;
      raise;
  end fill_buf_by_depo_data;

  /*
    @brief cl_GatherLots - сборка лотов во временную таблицу - вызывается в момент постороения отчета
    @param[in] p_AvrKind       код подвида инструмента
    @param[in] p_Department    департамент
    @param[in] p_ClientCode    код клиента
    @param[in] p_ReportDateBeg начало отчета
    @param[in] p_ReportDateEnd конец отчета
    @param[in] p_Contract      код договора
    @param[in] p_FIID          код финансового инструмента

  */
  procedure cl_GatherLots(p_AvrKind       integer,
                          p_Department    integer,
                          p_ClientCode    integer,
                          p_ReportDateBeg date,
                          p_ReportDateEnd date,
                          p_Contract      integer,
                          p_FIID          integer) IS
    l_start_time pls_integer;
    n           NUMBER := 0;
  BEGIN
    l_start_time := dbms_utility.get_time;

    FOR c IN (WITH a as
                 (select NVL(CASE
                              WHEN fin.t_avoirkind != 16 THEN
                               c.t_amount
                              ELSE
                               c.t_amount
                            END,
                            0) am,
                        c.t_fiid,
                        c.t_party,
                        c.t_contract
                   FROM dpmwrtcl_dbt c
                  INNER JOIN dfininstr_dbt fin
                     ON fin.t_fiid = c.t_fiid
                    AND rsb_fiinstr.fi_avrkindseq(fin.t_fi_kind,
                                                  CASE
                                                    WHEN p_AvrKind = -1 THEN
                                                     fin.t_avoirkind
                                                    ELSE
                                                     p_AvrKind
                                                  END,
                                                  fin.t_avoirkind) = 1
                  WHERE c.t_department = p_Department
                    AND c.t_party = CASE
                          WHEN p_ClientCode = -1 THEN
                           c.t_party
                          ELSE
                           p_ClientCode
                        END
                    AND c.t_begdate <= p_ReportDateBeg
                    AND (c.t_enddate = TO_DATE('31-12-9999', 'DD-MM-YYYY') or
                        c.t_enddate >= p_ReportDateEnd)
                    AND c.t_contract = CASE
                          WHEN p_Contract = -1 THEN
                           c.t_contract
                          ELSE
                           p_Contract
                        END
                    AND C.T_FIID = CASE
                          WHEN p_FIID = -1 then
                           C.T_FIID
                          ELSE
                           p_FIID
                        END)
                -- объединение двух ЦБ c fiid 3813 и 1265  DEF-63390
                select *
                  from a
                 where a.t_fiid not in (3813, 1265)
                union all
                select sum(am) am, 3813 t_fiid, t_party, t_contract
                  from a
                 where a.t_fiid in (3813, 1265)
                 group by t_contract, t_party) LOOP

      IF MOD(n, 1000) = 0 THEN
        COMMIT;
      END IF;

      n           := n + 1;
      UPDATE dsp_acts_tmp Sp_ActsTmp
         SET Sp_ActsTmp.t_restCB = Sp_ActsTmp.t_restCB + c.am
       WHERE Sp_ActsTmp.t_clientid = c.t_party
         AND Sp_ActsTmp.t_fiid = c.t_fiid
         AND Sp_ActsTmp.t_contrid = c.t_contract;

      IF SQL%ROWCOUNT = 0 THEN
        INSERT INTO dsp_acts_tmp Sp_ActsTmp
          (Sp_ActsTmp.t_ClientID,
           Sp_ActsTmp.t_FIID,
           Sp_ActsTmp.t_RestDep,
           Sp_ActsTmp.t_Rest,
           Sp_ActsTmp.t_RestCB,
           Sp_ActsTmp.t_ContrID,
           Sp_ActsTmp.t_Account)
        VALUES
          (c.t_party, c.t_FIID, 0, 0, c.am, c.t_contract, '');
      END IF;

    END LOOP;
    COMMIT;

    it_log.log(p_msg      => 'End. rows: ' || n || '; time: ' || time_since_varchar(p_time => l_start_time),
               p_msg_type => it_log.c_msg_type__debug);
  END;

  /*
    @brief cl_GatherDataInnerAccounting - сборка счетов ВУ во временную таблицу - вызывается в момент построения отчета
    @param[in] pClientCode код клиента
    @param[in] pIsPeriod  тип периода отчета
    @param[in] pReportDateEnd конец отчета
    @param[in] pReportDateBeg начало отчета
    @param[in] pAvrCode код валюты счета
    @param[in] pIssuerCode код эмитента
    @param[in] pOurBank код нашего банка
    @param[in] pAvrKind код подвида финансового инструмента
  */
  procedure cl_GatherDataInnerAccounting(pClientCode    integer,
                                         pIsPeriod      integer,
                                         pReportDateEnd date,
                                         pReportDateBeg date,
                                         pAvrCode       Number,
                                         pIssuerCode    varchar2,
                                         pOurBank       integer,
                                         pAvrKind       integer,
                                         p_contractid   integer) IS
    l_cat_id number(10) := 368;
    l_start_time pls_integer;

    cursor cur_inner_accounting_data is
         WITH contracts AS
                (select /*+ full(sf) */ sf.t_partyid partyid,
                        sf.t_id sfcontrid
                   FROM dsfcontr_dbt   sf
                  WHERE sf.t_servkind = 1
                    AND (pClientCode = -1 OR sf.t_partyid = pClientCode)
                    and (p_contractid = -1 or sf.t_id = p_contractid)
                    AND sf.t_partyid != pOurBank
                    AND (sf.t_dateclose = TO_DATE('01.01.0001', 'DD.MM.YYYY')
                         or sf.t_dateclose >= (case
                                                when pIsPeriod = 1 then
                                                 pReportDateEnd
                                                else
                                                 pReportDateBeg
                                              end))
                ),
                a as
                (select /*+ full(f) index(accd DMCACCDOC_IDX_CHAP_CMN_CAT) */ c.partyid,
                        c.sfcontrid,
                        accd.t_Account,
                        accd.t_Currency
                        -- DEF-93865 если счет недействующий на дату, показываем остаток 0
                        , case when accd.t_disablingdate > to_date('01010001', 'ddmmyyyy') and accd.t_disablingdate < pReportDateBeg then 0
                               when accd.t_activatedate > pReportDateBeg then 0
                               else 
                                    rsb_account.restac(accd.t_Account, accd.t_Currency,
                                            (case
                                              when pIsPeriod = 1 then
                                               pReportDateEnd - 1
                                              else
                                               pReportDateBeg - 1
                                            end),
                                            accd.t_Chapter,
                                            NULL) end AS rest_ac
                   FROM contracts c,
                        dmcaccdoc_dbt accd,
                        dfininstr_dbt f
                  WHERE accd.t_Chapter = 22
                    AND accd.t_CatID = l_cat_id
                    AND accd.t_iscommon = CHR(88)
                    AND accd.t_ClientContrID = c.sfcontrid
                    AND accd.t_owner = c.partyid
                    AND accd.t_Currency = (case
                          when (pAvrCode = -1) then
                           accd.t_Currency
                          else
                           pAvrCode
                        end)
                    AND f.t_fiid = accd.T_CURRENCY
                    AND f.t_fi_kind = 2
                    AND (pIssuerCode <= 0 OR RSI_RSB_FIInstr.FI_GetIssuerOnDate(accd.t_Currency,
                                                                                case
                                                                                  when pIsPeriod = 1 then
                                                                                   pReportDateEnd
                                                                                  else
                                                                                   pReportDateBeg
                                                                                end) = pIssuerCode)
                    AND RSB_FIInstr.FI_AvrKindsEQ(f.t_FI_Kind,
                                                  case
                                                    when pAvrKind = -1 then
                                                     f.t_AvoirKind
                                                    else
                                                     pAvrKind
                                                  end,
                                                  f.t_AvoirKind) = 1)
                -- объединение двух ЦБ c fiid 3813 и 1265  DEF-63390
                select a.partyid,
                       a.t_currency,
                       a.rest_ac,
                       a.sfcontrid,
                       a.t_Account
                  from a
                 where a.t_currency not in (3813, 1265)
                union all
                select partyid,
                       3813,
                       sum(rest_ac) rest_ac,
                       sfcontrid,
                       max(t_Account)
                  from a
                 where a.t_currency in (3813, 1265)
                 group by sfcontrid, partyid;

    type cur_data_type is table of cur_inner_accounting_data%rowtype;

    l_cur_data cur_data_type;
  BEGIN
    l_start_time := dbms_utility.get_time;

    open cur_inner_accounting_data;
    loop
      fetch cur_inner_accounting_data bulk collect into l_cur_data limit 100000;

      forall i in l_cur_data.first .. l_cur_data.last
      insert /*+ append*/ into dSp_Acts_Tmp (t_ClientID,
                                t_FIID,
                                t_RestDep,
                                t_Rest,
                                t_RestCB,
                                t_ContrID,
                                t_Account)
      values (l_cur_data(i).partyid,
              l_cur_data(i).t_Currency,
              0,
              l_cur_data(i).rest_ac,
              0,
              l_cur_data(i).sfcontrid,
              l_cur_data(i).t_Account
             );
      commit;

      exit when cur_inner_accounting_data%notfound;
    end loop;

    it_log.log(p_msg      => 'End. time: ' || time_since_varchar(p_time => l_start_time),
               p_msg_type => it_log.c_msg_type__debug);
  END cl_GatherDataInnerAccounting;

  function get_payments_scheme_synonym
    return varchar2 is
  begin
    return it_rs_interface.get_parm_varchar_path(p_parm_path => 'COMMON\PAYMENTS\PAYMENTS_SCHEME_GATE');
  end get_payments_scheme_synonym;
  
  procedure clear_payments_buffer is
  begin
    execute immediate 'truncate table spec_depo_data_tmp';
  end clear_payments_buffer;

  procedure prepare_spec_depo_data (
    p_date date
  ) is
    l_payments_scheme_synonym varchar2(50);
  begin
    l_payments_scheme_synonym := get_payments_scheme_synonym;

    execute immediate
      'insert into spec_depo_data_tmp (
         trdaccid,
         isin,
         closing_balance
       )
        select t.trdaccid,
               t.isin,
               t.closingbalance
          from ' || l_payments_scheme_synonym || '.mb_eqm99 t
          join ' || l_payments_scheme_synonym || '.processing_actual p on p.id_mb_requisites = t.id_mb_requisites
         where substr(t.trdaccid, 1, 1) = ''Y''
           and p.file_type = ''EQM99''
           and p.trade_date = (select max(p2.trade_date)
                                 from ' || l_payments_scheme_synonym || '.processing_actual p2
                                where p2.file_type = ''EQM99''
                                  and p2.trade_date <= :tradeDate)
      '
    using in p_date;
  
  exception
    when others then
      it_log.log_error(p_object => 'sec_cl_blnc_reconc_utl.prepare_spec_depo_data',
                       p_msg    => sqlerrm);

  end prepare_spec_depo_data;
  
  function get_spec_depo_rest (
    p_client_account spec_depo_data_tmp.trdaccid%type,
    p_isin           spec_depo_data_tmp.isin%type
  ) return spec_depo_data_tmp.closing_balance%type is
    l_balance spec_depo_data_tmp.closing_balance%type;
  begin
    select t.closing_balance
      into l_balance
      from spec_depo_data_tmp t 
     where t.trdaccid = p_client_account
       and t.isin = p_isin;
    
    return l_balance;
  exception
    when others then
      return null;
  end get_spec_depo_rest;

  procedure update_buf_depo_rest (
    p_clientid  dsp_acts_tmp.t_clientid%type,
    p_contrid   dsp_acts_tmp.t_contrid%type,
    p_fiid      dsp_acts_tmp.t_fiid%type,
    p_rest      dsp_acts_tmp.t_restdep%type
  ) is
  begin
    update dsp_acts_tmp t
       set t.t_restdep = p_rest
     where t.t_clientid = p_clientid
       and t.t_contrid = p_contrid
       and t.t_fiid = p_fiid;
  end update_buf_depo_rest;
  
  /*  Только обновляет уже существующие данные в буфере, новых не добавляет
  */
  procedure extend_buf_by_spec_depo_data (
    p_date date
  ) is
    l_depo_rest          dsp_acts_tmp.t_restdep%type;
    l_start_time         pls_integer;
    l_cnt_processed_rows integer := 0;
  begin
    l_start_time := dbms_utility.get_time;
    clear_payments_buffer;
    prepare_spec_depo_data(p_date => p_date);
    
    for rec in (with clients as (
                    select t.t_clientid,
                           t.t_fiid,
                           t.t_contrid,
                           sfcontr_read.get_moex_stock_client_account(p_sfcontr_id => t.t_contrid) moex_stock_client_account
                      from dsp_acts_tmp t
                    )
                   select c.*,
                          av.t_isin
                     from clients c
                     join davoiriss_dbt av on c.t_fiid = av.t_fiid
                    where substr(c.moex_stock_client_account, 0, 1) = 'Y'
    ) loop
      l_depo_rest := get_spec_depo_rest(p_client_account => rec.moex_stock_client_account,
                                        p_isin           => rec.t_isin);

      if l_depo_rest is not null then
        update_buf_depo_rest(p_clientid => rec.t_clientid,
                             p_contrid  => rec.t_contrid,
                             p_fiid     => rec.t_fiid,
                             p_rest     => l_depo_rest);
      end if;
      l_cnt_processed_rows := l_cnt_processed_rows + 1;
    end loop;

    it_log.log(p_msg      => 'End. rows: ' || l_cnt_processed_rows || '; time: ' || time_since_varchar(p_time => l_start_time),
               p_msg_type => it_log.c_msg_type__debug);
  
  exception
    when others then
      it_log.log_error(p_object => 'sec_cl_blnc_reconc_utl.extend_buf_by_spec_depo_data',
                       p_msg    => sqlerrm);
      raise_application_error(-20000, 'Error on calc spec depo data', true);
  end extend_buf_by_spec_depo_data;
  
end sec_cl_blnc_reconc_utl;
/
