declare

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

  procedure link_acc_to_cat(
    p_account  daccount_dbt.t_account%type,
    p_cat_code dmccateg_dbt.t_code%type
  ) is
    l_marketplaceofficeid number(9);
    l_cat_row dmccateg_dbt%rowtype;
  begin
    l_marketplaceofficeid := case when substr(p_account, 14, 1) = 3 then 24 else 19 end;
    l_cat_row := get_cat_row(p_cat_code => p_cat_code);

    merge into dmcaccdoc_dbt m
    using (select a.t_chapter as t_chapter,
                 a.t_account as t_account,
                 a.t_code_currency as t_currency,
                 a.t_kind_account as t_kind_account
            from daccount_dbt a
           where a.t_account = p_account) a
    on (m.t_account = a.t_account and
        m.t_iscommon = 'X')
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
         3, --t_templnum
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
         l_marketplaceofficeid, --t_marketplaceofficeid,
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
  end link_acc_to_cat;
  
  procedure save_to_brok_acc (
    p_account  dbrokacc_dbt.t_account%type,
    p_currency dbrokacc_dbt.t_currency%type
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
      values (0, 0, p_currency, p_account);
  end save_to_brok_acc;

begin
  link_acc_to_cat(p_account => '47403344799003202619', p_cat_code => '-Биржа');
  link_acc_to_cat(p_account => '47403344499002202619', p_cat_code => '-Биржа');
  link_acc_to_cat(p_account => '47403398499003202619', p_cat_code => '-Биржа');
  link_acc_to_cat(p_account => '47403398199002202619', p_cat_code => '-Биржа');
  link_acc_to_cat(p_account => '47403784799003202619', p_cat_code => '-Биржа');
  link_acc_to_cat(p_account => '47403784499002202619', p_cat_code => '-Биржа');
  link_acc_to_cat(p_account => '47403933799003202619', p_cat_code => '-Биржа');
  link_acc_to_cat(p_account => '47403933499002202619', p_cat_code => '-Биржа');
  link_acc_to_cat(p_account => '47403949699003202619', p_cat_code => '-Биржа');
  link_acc_to_cat(p_account => '47403949399002202619', p_cat_code => '-Биржа');
  
  link_acc_to_cat(p_account => '47404344099003202619', p_cat_code => '+Биржа');
  link_acc_to_cat(p_account => '47404344799002202619', p_cat_code => '+Биржа');
  link_acc_to_cat(p_account => '47404398799003202619', p_cat_code => '+Биржа');
  link_acc_to_cat(p_account => '47404398499002202619', p_cat_code => '+Биржа');
  link_acc_to_cat(p_account => '47404784099003202619', p_cat_code => '+Биржа');
  link_acc_to_cat(p_account => '47404784799002202619', p_cat_code => '+Биржа');
  link_acc_to_cat(p_account => '47404933099003202619', p_cat_code => '+Биржа');
  link_acc_to_cat(p_account => '47404933799002202619', p_cat_code => '+Биржа');
  link_acc_to_cat(p_account => '47404949999003202619', p_cat_code => '+Биржа');
  link_acc_to_cat(p_account => '47404949699002202619', p_cat_code => '+Биржа');
  
  link_acc_to_cat(p_account => '47403344699003001347', p_cat_code => '-Обеспечение');
  link_acc_to_cat(p_account => '47403344099001001347', p_cat_code => '-Обеспечение');
  link_acc_to_cat(p_account => '47403398399003001347', p_cat_code => '-Обеспечение');
  link_acc_to_cat(p_account => '47403398799001001347', p_cat_code => '-Обеспечение');
  link_acc_to_cat(p_account => '47403784699003001347', p_cat_code => '-Обеспечение');
  link_acc_to_cat(p_account => '47403784099001001347', p_cat_code => '-Обеспечение');
  link_acc_to_cat(p_account => '47403933699003001347', p_cat_code => '-Обеспечение');
  link_acc_to_cat(p_account => '47403933099001001347', p_cat_code => '-Обеспечение');
  link_acc_to_cat(p_account => '47403949599003001347', p_cat_code => '-Обеспечение');
  link_acc_to_cat(p_account => '47403949999001001347', p_cat_code => '-Обеспечение');
  
  link_acc_to_cat(p_account => '47404344999003001347', p_cat_code => '+Обеспечение');
  link_acc_to_cat(p_account => '47404344399001001347', p_cat_code => '+Обеспечение');
  link_acc_to_cat(p_account => '47404398699003001347', p_cat_code => '+Обеспечение');
  link_acc_to_cat(p_account => '47404398099001001347', p_cat_code => '+Обеспечение');
  link_acc_to_cat(p_account => '47404784999003001347', p_cat_code => '+Обеспечение');
  link_acc_to_cat(p_account => '47404784399001001347', p_cat_code => '+Обеспечение');
  link_acc_to_cat(p_account => '47404933999003001347', p_cat_code => '+Обеспечение');
  link_acc_to_cat(p_account => '47404933399001001347', p_cat_code => '+Обеспечение');
  link_acc_to_cat(p_account => '47404949899003001347', p_cat_code => '+Обеспечение');
  link_acc_to_cat(p_account => '47404949299001001347', p_cat_code => '+Обеспечение');
  
  --HKD
  save_to_brok_acc(p_account => '47423344299992222222', p_currency => 13);
  save_to_brok_acc(p_account => '30601344099992222222', p_currency => 13);
  save_to_brok_acc(p_account => '30606344599992222222', p_currency => 13);
  
  --KZT
  save_to_brok_acc(p_account => '47423398999992222222', p_currency => 15);
  save_to_brok_acc(p_account => '30601398799992222222', p_currency => 15);
  save_to_brok_acc(p_account => '30606398299992222222', p_currency => 15);
  
  --AED
  save_to_brok_acc(p_account => '47423784299992222222', p_currency => 39009);
  save_to_brok_acc(p_account => '30601784099992222222', p_currency => 39009);
  save_to_brok_acc(p_account => '30606784599992222222', p_currency => 39009);
  
  --BYN
  save_to_brok_acc(p_account => '47423933299992222222', p_currency => 21);
  save_to_brok_acc(p_account => '30601933099992222222', p_currency => 21);
  save_to_brok_acc(p_account => '30606933599992222222', p_currency => 21);
  
  --TRY
  save_to_brok_acc(p_account => '47423949199992222222', p_currency => 22);
  save_to_brok_acc(p_account => '30601949999992222222', p_currency => 22);
  save_to_brok_acc(p_account => '30606949499992222222', p_currency => 22);
  
  commit;
end;
/
