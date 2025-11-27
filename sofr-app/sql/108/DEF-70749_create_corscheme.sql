declare
  l_currency_list t_string_list := t_string_list('959', '961');
  
  l_log_object    varchar2(40) := 'new_cur';
  l_account_tmpl  varchar2(20);
  g_account       varchar2(20);
  l_scheme_number number(5) := 9990;
  
  function create_account (
    p_account       varchar2,
    p_code_currency number,
    p_kind_account  varchar2,
    p_name          varchar2,
    p_client        number,
    p_type_account  varchar2,
    p_usertype_account varchar2
  ) return number is
    l_accountid number(10);
  begin
    insert into daccount_dbt (t_accountid,
                              t_open_close,
                              t_code_currency,
                              t_account,
                              t_chapter,
                              t_department,
                              t_branch,
                              t_client,
                              t_oper,
                              t_balance,
                              t_sort,
                              t_open_date,
                              t_close_date,
                              t_index2,
                              t_index3,
                              t_kind_account,
                              t_type_account,
                              t_etype_account,
                              t_usertypeaccount,
                              t_final_date,
                              t_datenochange,
                              t_symbol,
                              t_nameaccount,
                              t_change_date,
                              t_change_dateprev,
                              t_pairaccount,
                              t_userfield1,
                              t_userfield2,
                              t_userfield3,
                              t_userfield4,
                              t_operationdate,
                              t_daystoend,
                              t_orscheme,
                              t_contractrko,
                              t_officeid,
                              t_depoacc,
                              t_deporoot,
                              t_havesubaccounts,
                              t_controloper,
                              t_currencyeq,
                              t_currencyeq_ratedate,
                              t_currencyeq_ratetype,
                              t_currencyeq_rateextra,
                              t_opucode,
                              t_beneficiaryid,
                              t_version,
                              t_ofrrecid,
                              t_contragent,
                              t_createsysdate,
                              t_createsystime)
    values (0,
            chr(0),
            p_code_currency,
            p_account,
            1,
            1,
            1,
            p_client,
            1,
            substr(p_account, 1, 5),
            substr(p_account, 1, 8) || substr(p_account, 10),
            trunc(sysdate),
            to_date('01.01.0001', 'dd.mm.yyyy'),
            chr(0),
            chr(0),
            p_kind_account,
            p_type_account,
            chr(1),
            p_usertype_account,
            to_date('01.01.0001', 'dd.mm.yyyy'),
            to_date('01.01.0001', 'dd.mm.yyyy'),
            chr(1),
            p_name,
            to_date('01.01.0001', 'dd.mm.yyyy'),
            to_date('01.01.0001', 'dd.mm.yyyy'),
            chr(0),
            chr(1),
            chr(1),
            chr(1),
            chr(1),
            to_date('01.01.0001', 'dd.mm.yyyy'),
            0,
            0,
            0,
            0,
            0,
            0,
            chr(0),
            1,
            -1,
            0,
            0,
            0,
            chr(1),
            0,
            0,
            0,
            0,
            sysdate,
            sysdate)
    returning t_accountid into l_accountid;

    return l_accountid;
  end create_account;
  
  procedure create_accblnc (
    p_accountid     number,
    p_account       varchar2,
    p_code_currency number
  ) is
  begin
    insert into daccblnc_dbt(t_accountid,
                             t_account,
                             t_code_currency,
                             t_chapter,
                             t_balance0,
                             t_balance1,
                             t_balance2,
                             t_balance3,
                             t_balance4,
                             t_balance5,
                             t_balance6,
                             t_balance7,
                             t_balance8,
                             t_balance9,
                             t_balance10,
                             t_balance11)
    values (p_accountid,
            p_account,
            p_code_currency,
            1,
            substr(p_account, 1, 5),
            chr(1),
            chr(1),
            chr(1),
            chr(1),
            chr(1),
            chr(1),
            chr(1),
            chr(1),
            chr(1),
            chr(1),
            chr(1));
  end create_accblnc;
  
  function open_account (
    p_account       varchar2,
    p_code_currency number,
    p_cur_name      varchar2
  ) return varchar2 is
    l_account       varchar2(20);
    l_kind_account  varchar2(1);
    l_name          varchar2(200);
    l_accountid     number(10);
    l_client        number(10);
    l_type_account  varchar2(5);
    l_usertype_account varchar2(5);
  begin
    l_account          := rsi_rsb_account.GetAccountKeyByDprtCode(p_account => p_account, p_code => 1);
    l_client           := 1; --30002;
    l_type_account     := 'КФ';
    l_usertype_account := '-';
    l_kind_account     := 'А';
    l_name             := 'Технический счет для корсхемы ' || p_cur_name;

    l_accountid := create_account(p_account          => l_account,
                                  p_code_currency    => p_code_currency,
                                  p_kind_account     => l_kind_account,
                                  p_name             => l_name,
                                  p_client           => l_client,
                                  p_type_account     => l_type_account,
                                  p_usertype_account => l_usertype_account);

    create_accblnc(p_accountid     => l_accountid,
                   p_account       => l_account,
                   p_code_currency => p_code_currency);

    it_log.log_handle(p_object   => l_log_object,
                      p_msg      => 'account created: ' || l_account || '. currency: ' || p_cur_name);
    return l_account;
  end open_account;
  
  procedure create_corschem (
    p_number   number,
    p_currency number,
    p_cur_name varchar2,
    p_account  varchar2
  ) is
  begin
    insert into dcorschem_dbt (t_number,
                               t_fiid,
                               t_fi_kind,
                               t_corrid,
                               t_tpid,
                               t_name,
                               t_account,
                               t_coraccount,
                               t_corpartition,
                               t_isnostro,
                               t_ownership,
                               t_isbase,
                               t_ispair,
                               t_isinternal,
                               t_dognumb,
                               t_dogopendate,
                               t_dogclosedate,
                               t_daystoexpire,
                               t_iskvitoutpaym,
                               t_iskvitinpaym,
                               t_userfield1,
                               t_userfield2,
                               t_userfield3,
                               t_userfield4,
                               t_state,
                               t_bankdate,
                               t_sysdate,
                               t_systime,
                               t_userid,
                               t_ourcutofftime,
                               t_corrcutofftime,
                               t_clientcutofftime,
                               t_createconfirmation,
                               t_department,
                               t_minlimit,
                               t_maxlimit,
                               t_inpmdate,
                               t_inpmdateoffset)
    values (p_number,
            p_currency,
            1,
            30002,
            1,
            'Тех. схема для ' || p_cur_name,
            p_account,
            '111',
            chr(1),
            'X',
            0,
            chr(0),
            chr(0),
            chr(0),
            chr(1),
            trunc(sysdate),
            to_date('01.01.0001', 'dd.mm.yyyy'),
            0,
            chr(0),
            chr(0),
            chr(1),
            chr(1),
            chr(1),
            chr(1),
            0,
            to_date('01.01.0001', 'dd.mm.yyyy'),
            sysdate,
            sysdate,
            1,
            to_date('01.01.0001', 'dd.mm.yyyy'),
            to_date('01.01.0001', 'dd.mm.yyyy'),
            to_date('01.01.0001', 'dd.mm.yyyy'),
            chr(0),
            1,
            0,
            0,
            0,
            0);
  end create_corschem;
  
  procedure create_bnkschem (
    p_number   number,
    p_currency number
  ) is
  begin
    insert into dbnkschem_dbt (t_autoinc,
                               t_bankid,
                               t_fiid,
                               t_fi_kind,
                               t_schem,
                               t_isseprate,
                               t_ownschem,
                               t_depschem,
                               t_trustschem,
                               t_isnostro,
                               t_isbase,
                               t_defaultdebout,
                               t_defaultkredout,
                               t_defaultdebin,
                               t_defaultkredin,
                               t_state,
                               t_bankdate,
                               t_sysdate,
                               t_systime,
                               t_userid,
                               t_insideofficepartyid,
                               t_packsnumbers,
                               t_sort,
                               t_beginperiod,
                               t_endperiod,
                               t_bankcode,
                               t_codekind,
                               t_isuseforchildnode,
                               t_department,
                               t_allowtaxpayms)
    values (0,
            30002,
            p_currency,
            1,
            p_number,
            chr(0),
            0,
            0,
            0,
            chr(0),
            chr(0),
            'X',
            'X',
            'X',
            'X',
            0,
            to_date('01.01.0001', 'dd.mm.yyyy'),
            to_date('01.01.0001', 'dd.mm.yyyy'),
            to_date('01.01.0001', 'dd.mm.yyyy'),
            1,
            -1,
            chr(1),
            99,
            to_date('01.01.0001', 'dd.mm.yyyy'),
            to_date('01.01.0001', 'dd.mm.yyyy'),
            'CHASUS33',
            1,
            chr(0),
            1,
            chr(0));
  end create_bnkschem;

  procedure create_scheme (
    p_account       varchar2,
    p_currency      number,
    p_cur_name      varchar2,
    p_scheme_number number
  ) is
  begin
    create_corschem(p_number   => p_scheme_number,
                    p_currency => p_currency,
                    p_cur_name => p_cur_name,
                    p_account  => p_account);

    create_bnkschem(p_number   => p_scheme_number,
                    p_currency => p_currency);

    it_log.log_handle(p_object   => l_log_object,
                      p_msg      => 'scheme created. number: ' || p_scheme_number || '. account: ' || p_account || '. currency: ' || p_cur_name);
  end create_scheme;
begin
  
  for rec in (select t.column_value
                    ,f.t_fiid
                    ,f.t_codeinaccount
                    ,f.t_name
                from table(l_currency_list) t
                join dfininstr_dbt f on f.t_fi_code = t.column_value
                                    and f.t_fi_kind = 1
              )
  loop
    l_account_tmpl := '30114' || rec.t_codeinaccount || 'K99999999999';
    g_account := open_account(p_account       => l_account_tmpl,
                              p_code_currency => rec.t_fiid,
                              p_cur_name      => rec.t_name);

    create_scheme(p_account       => g_account,
                  p_currency      => rec.t_fiid,
                  p_cur_name      => rec.t_name,
                  p_scheme_number => l_scheme_number);
    l_scheme_number := l_scheme_number + 1;
  end loop;
  
  commit;
end;
/