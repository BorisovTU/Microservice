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
    p_cat_code dmccateg_dbt.t_code%type,
    p_templnum dmcaccdoc_dbt.t_templnum%type
  ) is
    l_marketplaceofficeid number(9);
    l_cat_row dmccateg_dbt%rowtype;
  begin
    l_marketplaceofficeid := case when substr(p_account, 14, 1) = '3' then 24 else 19 end;
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
  link_acc_to_cat(p_account => '47403A98499002202619', p_cat_code => '-¨à¦ ', p_templnum => 3);
  link_acc_to_cat(p_account => '47403A98799003202619', p_cat_code => '-¨à¦ ', p_templnum => 3);
  link_acc_to_cat(p_account => '47403A99799002202619', p_cat_code => '-¨à¦ ', p_templnum => 3);
  link_acc_to_cat(p_account => '47403A99099003202619', p_cat_code => '-¨à¦ ', p_templnum => 3);

  link_acc_to_cat(p_account => '47404A98799002202619', p_cat_code => '+¨à¦ ', p_templnum => 3);
  link_acc_to_cat(p_account => '47404A98099003202619', p_cat_code => '+¨à¦ ', p_templnum => 3);  
  link_acc_to_cat(p_account => '47404A99099002202619', p_cat_code => '+¨à¦ ', p_templnum => 3);
  link_acc_to_cat(p_account => '47404A99399003202619', p_cat_code => '+¨à¦ ', p_templnum => 3);

  link_acc_to_cat(p_account => '30413A98999001001347', p_cat_code => '’®à£®¢ë© áç¥â', p_templnum => 2);
  link_acc_to_cat(p_account => '30413A98599003001347', p_cat_code => '’®à£®¢ë© áç¥â', p_templnum => 2);  
  link_acc_to_cat(p_account => '30413A99299001001347', p_cat_code => '’®à£®¢ë© áç¥â', p_templnum => 2);
  link_acc_to_cat(p_account => '30413A99899003001347', p_cat_code => '’®à£®¢ë© áç¥â', p_templnum => 2);
  
  --GLD
  save_to_brok_acc(p_account => '47423A98299992222222', p_currency => 2580);
  save_to_brok_acc(p_account => '30601A98099992222222', p_currency => 2580);
  save_to_brok_acc(p_account => '30606A98599992222222', p_currency => 2580);
  
  --SLV
  save_to_brok_acc(p_account => '47423A99599992222222', p_currency => 2581);
  save_to_brok_acc(p_account => '30601A99399992222222', p_currency => 2581);
  save_to_brok_acc(p_account => '30606A99899992222222', p_currency => 2581);  
end;
/
