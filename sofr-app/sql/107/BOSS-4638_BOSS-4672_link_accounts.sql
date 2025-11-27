declare
  l_account_templates t_string_list := t_string_list('47404051К99001001347',
               '47403051К99001001347',
               '47404051К99002202619',
               '47403051К99002202619',
               '47404051К99003001347',
               '47403051К99003001347',
               '47404051К99003202619',
               '47403051К99003202619',
               '30601051К99992222222',
               '30606051К99992222222',
               '47423051К99992222222',
               '47404860К99001001347',
               '47403860К99001001347',
               '47404860К99002202619',
               '47403860К99002202619',
               '47404860К99003001347',
               '47403860К99003001347',
               '47404860К99003202619',
               '47403860К99003202619',
               '30601860К99992222222',
               '30606860К99992222222',
               '47423860К99992222222',
               '47404972К99001001347',
               '47403972К99001001347',
               '47404972К99002202619',
               '47403972К99002202619',
               '47404972К99003001347',
               '47403972К99003001347',
               '47404972К99003202619',
               '47403972К99003202619',
               '30601972К99992222222',
               '30606972К99992222222',
               '47423972К99992222222',
               '47404417К99001001347',
               '47403417К99001001347',
               '47404417К99002202619',
               '47403417К99002202619',
               '47404417К99003001347',
               '47403417К99003001347',
               '47404417К99003202619',
               '47403417К99003202619',
               '30601417К99992222222',
               '30606417К99992222222',
               '47423417К99992222222');

  function get_cat_row(p_cat_code dmccateg_dbt.t_code%type)
    return dmccateg_dbt%rowtype is
    l_cat_row dmccateg_dbt%rowtype;
  begin
    select *
      into l_cat_row
      from dmccateg_dbt
     where t_code = p_cat_code
       and t_leveltype = 1;
    return l_cat_row;
  end get_cat_row;

  procedure save_to_brok_acc (
    p_account     dbrokacc_dbt.t_account%type,
    p_currency_id dbrokacc_dbt.t_currency%type
  ) is
  begin
    merge into dbrokacc_dbt a
    using dual
    on (a.t_account = p_account)
    when not matched then
      insert (t_servkind,
              t_servkindsub,
              t_currency,
              t_account)
      values (0, 0, p_currency_id, p_account);

    if sql%rowcount > 0 then
      it_log.log_handle(p_object => 'new_client_account.save_to_brok_acc',
                        p_msg    => p_account);
    end if;
  end save_to_brok_acc;
  
  procedure link_acc_to_cat(
    p_account             daccount_dbt.t_account%type,
    p_cat_code            dmccateg_dbt.t_code%type,
    p_marketplaceofficeid dmcaccdoc_dbt.t_marketplaceofficeid%type,
    p_templnum            dmcaccdoc_dbt.t_templnum%type
  ) is
    l_cat_row dmccateg_dbt%rowtype;
  begin
    l_cat_row := get_cat_row(p_cat_code => p_cat_code);

    merge into dmcaccdoc_dbt m
    using (select a.t_chapter as t_chapter,
                 a.t_account as t_account,
                 a.t_code_currency as t_currency,
                 a.t_kind_account as t_kind_account
            from daccount_dbt a
           where a.t_account = p_account) a
    on (m.t_account = a.t_account and
        m.t_iscommon = 'X' and
        m.t_catid = l_cat_row.t_id)
    when not matched then
      insert (t_id,
             t_iscommon,
             t_dockind,
             t_docid,
             t_catid,
             t_catnum,
             t_chapter,
             t_account,
             t_currency,
             t_templnum,
             t_groupnum,
             t_periodid,
             t_activatedate,
             t_disablingdate,
             t_isusable,
             t_fiid,
             t_owner,
             t_place,
             t_issuer,
             t_kind_account,
             t_centr,
             t_centroffice,
             t_actiondate,
             t_fiid2,
             t_clientcontrid,
             t_bankcontrid,
             t_marketplaceid,
             t_marketplaceofficeid,
             t_firole,
             t_indexdate,
             t_departmentid,
             t_contractor,
             t_branch,
             t_corrdepartmentid,
             t_currencyeq,
             t_currencyeq_ratetype,
             t_currencyeq_ratedate,
             t_currencyeq_rateextra,
             t_mcbranch)
  values (dmcaccdoc_dbt_seq.nextval, --t_id
         'X', --t_iscommon
         0, --t_dockind
         0, --t_docid
         l_cat_row.t_id, --t_catid
         l_cat_row.t_number, --t_catnum
         a.t_chapter, --t_chapter
         a.t_account, --t_account
         a.t_currency, --t_currency
         p_templnum, --t_templnum
         0, --t_groupnum
         0, --t_periodid
         trunc(sysdate), --t_activatedate
         to_date('01.01.0001', 'dd.mm.yyyy'), --t_disablingdate
         'X', --t_isusable
         -1, --t_fiid
         -1, --t_owner
         -1, --t_place
         -1, --t_issuer
         a.t_kind_account, --t_kind_account
         -1, --t_centr
         -1, --t_centroffice
         trunc(sysdate), --t_actiondate
         -1, --t_fiid2
         -1, --t_clientcontrid
         -1, --t_bankcontrid
         4, --t_marketplaceid
         p_marketplaceofficeid, --t_marketplaceofficeid,
         0, --t_firole
         -1, --t_indexdate
         1, --t_departmentid
         -1, --t_contractor
         1, --t_branch
         0, --t_corrdepartmentid
         -1, --t_currencyeq
         0, --t_currencyeq_ratetype
         0, --t_currencyeq_ratedate
         0, --t_currencyeq_rateextra
         1 --t_mcbranch
         );

    if sql%rowcount > 0 then
      it_log.log_handle(p_object => 'new_client_account.link_acc_to_cat',
                        p_msg    => 'account: ' || p_account || '. categ: ' || p_cat_code);
    end if;
  end link_acc_to_cat;
  
  function get_category_by_account (
    p_account varchar2
  ) return varchar2 is
    l_prefix     varchar2(1);
    l_categ_name varchar2(100);
  begin
    if substr(p_account, 1, 5) = '47403' then
      l_prefix := '-';
    elsif substr(p_account, 1, 5) = '47404' then
      l_prefix := '+';
    end if;
    
    if substr(p_account, 17) = '1347' then
      l_categ_name := 'Обеспечение';
    elsif substr(p_account, 17) = '2619' then
      l_categ_name := 'Биржа';
    end if;
    
    return l_prefix || l_categ_name;
  end get_category_by_account;
  
  function get_templnum_by_categ (
    p_categ_name varchar2
  ) return number is
    l_templnum number(1);
  begin
    if p_categ_name = '-Биржа' or p_categ_name = '+Биржа' then
      l_templnum := 3;
    elsif p_categ_name = '-Обеспечение' then
      l_templnum := 2;
    elsif p_categ_name = '+Обеспечение' then
      l_templnum := 4;
    end if;
    
    return l_templnum;
  end get_templnum_by_categ;

  procedure process_account (
    p_account     varchar2,
    p_currency_id number
  ) is
    l_categ_name          varchar2(100);
    l_marketplaceofficeid number(9);
    l_templnum            number(1);
  begin
    if substr(p_account, 1, 5) in ('47423', '30606', '30601') then
      save_to_brok_acc(p_account     => p_account,
                       p_currency_id => p_currency_id);
      return;
    end if;
    
    l_categ_name          := get_category_by_account(p_account => p_account);
    l_marketplaceofficeid := case when substr(p_account, 14, 1) = 3 then 24 else 19 end;
    l_templnum            := get_templnum_by_categ(p_categ_name => l_categ_name);

    link_acc_to_cat(p_account             => p_account,
                    p_cat_code            => l_categ_name,
                    p_marketplaceofficeid => l_marketplaceofficeid,
                    p_templnum            => l_templnum);
  end process_account;
begin

  for acc in (select a.*
                from table(l_account_templates) t
                join daccount_dbt a on a.t_account like substr(t.column_value, 1, 8) || '%' || substr(t.column_value, 10))
  loop
    process_account(p_account     => acc.t_account,
                    p_currency_id => acc.t_code_currency);

    it_log.log_handle(p_object => 'new_client_account.main',
                      p_msg    => 'processed: ' || acc.t_account);
  end loop;

  commit;
end;
/