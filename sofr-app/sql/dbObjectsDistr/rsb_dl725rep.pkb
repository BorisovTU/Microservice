CREATE OR REPLACE PACKAGE BODY RSB_DL725REP
IS
  -- Очистка промежуточных таблиц отчёта
  PROCEDURE ClearTables(pSessionId IN NUMBER, pPart IN NUMBER)
  AS
  BEGIN
    it_log.log(p_msg  => 'pSessionID='||pSessionId||' '||
               'Удаление старых записей из промежуточных таблиц раздела '||pPart,
               p_msg_type => it_log.C_MSG_TYPE__MSG);

    IF (pPart = 1) THEN
      delete from dclientportfolio_dbt;
      delete from dfipositionsbytype_dbt;
      delete from dpart1form725_dbt;
    END IF;

    commit;
    
    it_log.log(p_msg  => 'pSessionID='||pSessionId||' '||
               'Завершено удаление старых записей из промежуточных таблиц раздела '||pPart,
               p_msg_type => it_log.C_MSG_TYPE__MSG);

  END;

  -- Заполнение промежуточных таблиц раздела 1
  PROCEDURE FillTables_Part1(pSessionId IN NUMBER, pBegDate IN DATE, pEndDate IN DATE)
  AS
  BEGIN
    it_log.log(p_msg  => 'pSessionID='||pSessionId||' '||
               'Заполнение промежуточных таблиц раздела 1',
               p_msg_type => it_log.C_MSG_TYPE__MSG);

    --Формирование базиса в таблице расшифровки стоимости портфелей клиентов 
    insert into dclientportfolio_dbt (t_sessionid, t_clientcode, t_firmid, t_partyid, t_clientname, t_contractid, t_clienttype, t_isresident, t_isqualinvestor, t_rcvstatus, t_initmargin, t_rcv1, t_rcv2, t_risklevel, t_clientcounter)
    select pSessionId, rcv.t_ekk, rcv.t_firmid,
           Rsb_Secur.SC_GetObjCodeOnDate(3, 1, sfc.t_partyid,  to_date('18.08.2025','dd.mm.yyyy')) t_code, 
           case
            when prt.t_legalform = 2 then
             prs.t_name1 || ' ' || prs.t_name2 || ' ' || prs.t_name3         
            else
             prt.t_shortname
           end t_clientname,
           sfc.t_number, 
           case
            when prt.t_legalform = 2 then
             2
            else
             1
           end t_clienttype,
           case
            when prt.t_notresident = chr(88) then
             2
            else
             1
           end t_isresident,
           case
            when exists (select 1 from dscqinv_dbt where t_partyid = sfc.t_partyid and t_state = 1) then
             1
            else
             2
           end t_isqualinvestor,
           case
            when rcv.t_rcv1 >= 0 then
             1
            when rcv.t_rcv1 < 0 and rcv.t_rcv2 >= 0 then
             2
            when rcv.t_rcv2 < 0 and t_initmargin = 0 then
             3
            else
             4
           end t_rcvstatus,
           rcv.t_initmargin, rcv.t_rcv1, rcv.t_rcv2, rcv.t_risklevel, 0 as t_clientcounter        
      from drcv_dbt rcv, dparty_dbt prt, ddlcontr_dbt dlc, dsfcontr_dbt sfc, dpersn_dbt prs 
     where trunc(rcv.t_timestamp) = pEndDate and
       ( (rcv.t_futposition_type IN ('Ф','О','ФО')) or exists (select 1 from dmoneylimitsjournal_dbt lim where lim.t_clientcode = rcv.t_ekk and lim.t_firmid = rcv.t_firmid and lim.t_calcdatetime = rcv.t_timestamp) )
       and dlc.t_dlcontrid = (select t_dlcontrid from ddlcontrmp_dbt where t_mpcode = rcv.t_ekk and rownum = 1)
       and sfc.t_id = dlc.t_sfcontrid
       and prt.t_partyid = sfc.t_partyid
       and prs.t_personid = sfc.t_partyid
       and rcv.t_timestamp = (select max(t_timestamp) from drcv_dbt where t_ekk = rcv.t_ekk and t_firmid = rcv.t_firmid and trunc(t_timestamp) = trunc(rcv.t_timestamp));

    --Заполняем информацию по позициям портфелей
    FOR portfolio IN (SELECT * FROM dclientportfolio_dbt)
     LOOP
      insert into dfipositionsbytype_dbt (t_sessionid, t_clientcode, t_firmid, t_fikind, t_ficode, t_nonqualassec, t_settledate, t_currentbal, t_currentlimit, t_settleprice, t_currcode, t_discountccp, t_isnonliquidasset)
           select pSessionId, dm.t_clientcode, dm.t_firmid,
                  case
                   when exists (select 1 from dfininstr_dbt where t_fi_code = dm.t_currcode and t_fi_kind = 6) then
                    5
                   else
                    1
                  end t_fikind,
                  case
                   when dm.t_currcode = 'SUR' then
                    'RUB'
                   else
                    dm.t_currcode
                  end t_ficode,             
                  chr(1) as t_nonqualassec, dm.t_calcdatetime, dm.t_currentbal, dm.t_currentlimit, dm.t_crossrate,
                  case
                   when dm.t_currcode = 'SUR' then
                    'RUB'
                   else
                    dm.t_currcode
                  end t_currcode,
                  nvl((select t_discount_ccp from dccppricerange_dbt where t_seccode = dm.t_currcode and trunc(t_calcdatetime) = trunc(dm.t_calcdatetime)),1) as t_discountccp,
                  case
                   when dm.t_longdiscount = 1 and dm.t_shortdiscount is null then
                    'X'
                   else
                    chr(1)
                  end t_isnonliquidasset 
             from dmoneylimitsjournal_dbt dm where dm.t_clientcode = portfolio.t_clientcode and dm.t_currcode is not null and dm.t_firmid = portfolio.t_firmid and trunc(dm.t_calcdatetime) = pEndDate  
              and dm.t_calcdatetime = (select max(t_calcdatetime) from dmoneylimitsjournal_dbt where t_clientcode = dm.t_clientcode and t_firmid = dm.t_firmid and trunc(t_calcdatetime) = trunc(dm.t_calcdatetime))
            union all
           select pSessionId, dd.t_clientcode, dd.t_firmid, 2 as t_fikind, dd.t_seccode,
                  case
                   when RSB_SECUR.GetMainObjAttr(12, LPAD(nvl((select t_objectid from dobjcode_dbt where t_code = dd.t_seccode and t_codekind = 11 and t_objecttype = 9),0), 10, '0'), 28, pEndDate ) = 2 then
                    'X'
                   else
                    chr(1)
                  end t_nonqualassec, dd.t_calcdatetime, dd.t_currentbal, dd.t_currentlimit, dd.t_price,
                  case
                   when dd.t_pricecurrcode = 'SUR' then
                    'RUB'
                   else
                    dd.t_pricecurrcode
                  end t_currcode,
                  nvl((select t_discount_ccp from dccppricerange_dbt where t_seccode = dd.t_seccode and trunc(t_calcdatetime) = trunc(dd.t_calcdatetime)),1) as t_discountccp,
                  case
                   when dd.t_longdiscount = 1 and dd.t_shortdiscount is null then
                    'X'
                   else
                    chr(1)
                  end t_isnonliquidasset
             from ddepolimitsjournal_dbt dd where dd.t_clientcode = portfolio.t_clientcode and dd.t_firmid = portfolio.t_firmid and trunc(dd.t_calcdatetime) = pEndDate 
              and dd.t_calcdatetime = (select max(t_calcdatetime) from ddepolimitsjournal_dbt where t_clientcode = dd.t_clientcode and t_firmid = dd.t_firmid and trunc(t_calcdatetime) = trunc(dd.t_calcdatetime))
            union all
           select pSessionId, df.t_clientcode, df.t_firmid, 3 as t_fikind, df.t_seccode, chr(1) as t_nonqualassec, df.t_calcdatetime, df.t_currentbal, df.t_currentlimit, df.t_settleprice,
                  case
                   when df.t_settlepricecurrcode = 'SUR' then
                    'RUB'
                   else
                    df.t_settlepricecurrcode
                  end t_currcode,
                  nvl((select t_discount_ccp from dccppricerange_dbt where t_seccode = df.t_seccode and trunc(t_calcdatetime) = trunc(df.t_calcdatetime)),1) as t_discountccp,
                  case
                   when (df.t_longdiscount = 1 and df.t_shortdiscount is null) or (df.t_longdiscount = 1 and df.t_shortdiscount = 1) then
                    'X'
                   else
                    chr(1)
                  end t_isnonliquidasset 
             from dfutoptlimitsjournal_dbt df where df.t_clientcode = portfolio.t_clientcode and df.t_firmid = portfolio.t_firmid and trunc(df.t_calcdatetime) = pEndDate 
              and df.t_calcdatetime = (select max(t_calcdatetime) from dfutoptlimitsjournal_dbt where t_clientcode = df.t_clientcode and t_firmid = df.t_firmid and trunc(t_calcdatetime) = trunc(df.t_calcdatetime));            
    END LOOP;

    --Обновляем информацию по позициям в портфелях
    update dclientportfolio_dbt portfolio
       set portfolio.t_rubposition = (select nvl(sum(pos.t_currentlimit),0) from dfipositionsbytype_dbt pos where pos.t_clientcode = portfolio.t_clientcode and pos.t_firmid = portfolio.t_firmid and trunc(pos.t_settledate) = pEndDate and pos.t_ficode = 'RUB' ),
           portfolio.t_curposition = (select nvl(sum(pos.t_currentlimit*RSI_RSB_FIINSTR.CalcSumCross (1.0, (select t_fiid from dfininstr_dbt where t_ccy = pos.t_currcode), 0, pEndDate, 0)),0) from dfipositionsbytype_dbt pos where pos.t_clientcode = portfolio.t_clientcode and pos.t_firmid = portfolio.t_firmid and trunc(pos.t_settledate) = pEndDate and pos.t_fikind = 1 and pos.t_ficode != 'RUB' ),
           portfolio.t_secposition = (select nvl(sum(pos.t_currentlimit*RSI_RSB_FIINSTR.CalcSumCross (1.0, (select t_fiid from dfininstr_dbt where t_ccy = pos.t_currcode), 0, pEndDate, 0)),0) from dfipositionsbytype_dbt pos where pos.t_clientcode = portfolio.t_clientcode and pos.t_firmid = portfolio.t_firmid and trunc(pos.t_settledate) = pEndDate and pos.t_fikind = 2 and pos.t_nonqualassec != 'X' ),
           portfolio.t_futuresposition = (select nvl(sum(pos.t_currentlimit*RSI_RSB_FIINSTR.CalcSumCross (1.0, (select t_fiid from dfininstr_dbt where t_ccy = pos.t_currcode), 0, pEndDate, 0)),0) from dfipositionsbytype_dbt pos where pos.t_clientcode = portfolio.t_clientcode and pos.t_firmid = portfolio.t_firmid and trunc(pos.t_settledate) = pEndDate and pos.t_fikind = 3 ),
           portfolio.t_optionposition = (select nvl(sum(pos.t_currentlimit*RSI_RSB_FIINSTR.CalcSumCross (1.0, (select t_fiid from dfininstr_dbt where t_ccy = pos.t_currcode), 0, pEndDate, 0)),0) from dfipositionsbytype_dbt pos where pos.t_clientcode = portfolio.t_clientcode and pos.t_firmid = portfolio.t_firmid and trunc(pos.t_settledate) = pEndDate and pos.t_fikind = 4 ),
           portfolio.t_precmetposition = (select nvl(sum(pos.t_currentlimit*RSI_RSB_FIINSTR.CalcSumCross (1.0, (select t_fiid from dfininstr_dbt where t_ccy = pos.t_currcode), 0, pEndDate, 0)),0) from dfipositionsbytype_dbt pos where pos.t_clientcode = portfolio.t_clientcode and pos.t_firmid = portfolio.t_firmid and trunc(pos.t_settledate) = pEndDate and pos.t_fikind = 5 ),
           portfolio.t_nonqualassecposition = (select nvl(sum(pos.t_currentlimit*RSI_RSB_FIINSTR.CalcSumCross (1.0, (select t_fiid from dfininstr_dbt where t_ccy = pos.t_currcode), 0, pEndDate, 0)),0) from dfipositionsbytype_dbt pos where pos.t_clientcode = portfolio.t_clientcode and pos.t_firmid = portfolio.t_firmid and trunc(pos.t_settledate) = pEndDate and pos.t_fikind = 2 and pos.t_nonqualassec = 'X' ),
           portfolio.t_curinitmargin = (select nvl(sum(pos.t_currentbal*pos.t_discountccp*RSI_RSB_FIINSTR.CalcSumCross (1.0, (select t_fiid from dfininstr_dbt where t_ccy = pos.t_currcode), 0, pEndDate, 0)),0) from dfipositionsbytype_dbt pos where pos.t_clientcode = portfolio.t_clientcode and pos.t_firmid = portfolio.t_firmid and trunc(pos.t_settledate) = pEndDate and pos.t_fikind = 1 and pos.t_ficode != 'RUB' ),
           portfolio.t_secinitmargin = (select nvl(sum(pos.t_currentbal*pos.t_discountccp*RSI_RSB_FIINSTR.CalcSumCross (1.0, (select t_fiid from dfininstr_dbt where t_ccy = pos.t_currcode), 0, pEndDate, 0)),0) from dfipositionsbytype_dbt pos where pos.t_clientcode = portfolio.t_clientcode and pos.t_firmid = portfolio.t_firmid and trunc(pos.t_settledate) = pEndDate and pos.t_fikind = 2 and pos.t_nonqualassec != 'X' ),
           portfolio.t_futuresinitmargin = (select nvl(sum(pos.t_currentbal*pos.t_discountccp*RSI_RSB_FIINSTR.CalcSumCross (1.0, (select t_fiid from dfininstr_dbt where t_ccy = pos.t_currcode), 0, pEndDate, 0)),0) from dfipositionsbytype_dbt pos where pos.t_clientcode = portfolio.t_clientcode and pos.t_firmid = portfolio.t_firmid and trunc(pos.t_settledate) = pEndDate and pos.t_fikind = 3 ),
           portfolio.t_optioninitmargin = (select nvl(sum(pos.t_currentbal*pos.t_discountccp*RSI_RSB_FIINSTR.CalcSumCross (1.0, (select t_fiid from dfininstr_dbt where t_ccy = pos.t_currcode), 0, pEndDate, 0)),0) from dfipositionsbytype_dbt pos where pos.t_clientcode = portfolio.t_clientcode and pos.t_firmid = portfolio.t_firmid and trunc(pos.t_settledate) = pEndDate and pos.t_fikind = 4 ),
           portfolio.t_precmetinitmargin = (select nvl(sum(pos.t_currentbal*pos.t_discountccp*RSI_RSB_FIINSTR.CalcSumCross (1.0, (select t_fiid from dfininstr_dbt where t_ccy = pos.t_currcode), 0, pEndDate, 0)),0) from dfipositionsbytype_dbt pos where pos.t_clientcode = portfolio.t_clientcode and pos.t_firmid = portfolio.t_firmid and trunc(pos.t_settledate) = pEndDate and pos.t_fikind = 5 ),
           portfolio.t_nonqualassecinitmargin = (select nvl(sum(pos.t_currentbal*pos.t_discountccp*RSI_RSB_FIINSTR.CalcSumCross (1.0, (select t_fiid from dfininstr_dbt where t_ccy = pos.t_currcode), 0, pEndDate, 0)),0) from dfipositionsbytype_dbt pos where pos.t_clientcode = portfolio.t_clientcode and pos.t_firmid = portfolio.t_firmid and trunc(pos.t_settledate) = pEndDate and pos.t_fikind = 2 and pos.t_nonqualassec = 'X' )
     where portfolio.t_sessionid = pSessionId;

    --Вычисляем общую сумму портфелей и определяем когорту
    update dclientportfolio_dbt portfolio
       set portfolio.t_portfoliovalue = portfolio.t_rubposition + portfolio.t_curposition + portfolio.t_secposition + portfolio.t_futuresposition + portfolio.t_optionposition + portfolio.t_precmetposition + portfolio.t_nonqualassecposition,
           portfolio.t_cohort = case
                                 when portfolio.t_rubposition + portfolio.t_curposition + portfolio.t_secposition + portfolio.t_futuresposition + portfolio.t_optionposition + portfolio.t_precmetposition + portfolio.t_nonqualassecposition < -10000000 then
                                  1
                                 when portfolio.t_rubposition + portfolio.t_curposition + portfolio.t_secposition + portfolio.t_futuresposition + portfolio.t_optionposition + portfolio.t_precmetposition + portfolio.t_nonqualassecposition < -1000000 then
                                  2
                                 when portfolio.t_rubposition + portfolio.t_curposition + portfolio.t_secposition + portfolio.t_futuresposition + portfolio.t_optionposition + portfolio.t_precmetposition + portfolio.t_nonqualassecposition < -100000 then
                                  3
                                 when portfolio.t_rubposition + portfolio.t_curposition + portfolio.t_secposition + portfolio.t_futuresposition + portfolio.t_optionposition + portfolio.t_precmetposition + portfolio.t_nonqualassecposition < -10000 then
                                  4
                                 when portfolio.t_rubposition + portfolio.t_curposition + portfolio.t_secposition + portfolio.t_futuresposition + portfolio.t_optionposition + portfolio.t_precmetposition + portfolio.t_nonqualassecposition < 0 then
                                  5
                                 when portfolio.t_rubposition + portfolio.t_curposition + portfolio.t_secposition + portfolio.t_futuresposition + portfolio.t_optionposition + portfolio.t_precmetposition + portfolio.t_nonqualassecposition = 0 then
                                  6
                                 when portfolio.t_rubposition + portfolio.t_curposition + portfolio.t_secposition + portfolio.t_futuresposition + portfolio.t_optionposition + portfolio.t_precmetposition + portfolio.t_nonqualassecposition < 10000 then
                                  7
                                 when portfolio.t_rubposition + portfolio.t_curposition + portfolio.t_secposition + portfolio.t_futuresposition + portfolio.t_optionposition + portfolio.t_precmetposition + portfolio.t_nonqualassecposition < 100000 then
                                  8
                                 when portfolio.t_rubposition + portfolio.t_curposition + portfolio.t_secposition + portfolio.t_futuresposition + portfolio.t_optionposition + portfolio.t_precmetposition + portfolio.t_nonqualassecposition < 1000000 then
                                  9
                                 when portfolio.t_rubposition + portfolio.t_curposition + portfolio.t_secposition + portfolio.t_futuresposition + portfolio.t_optionposition + portfolio.t_precmetposition + portfolio.t_nonqualassecposition < 10000000 then
                                  10
                                 when portfolio.t_rubposition + portfolio.t_curposition + portfolio.t_secposition + portfolio.t_futuresposition + portfolio.t_optionposition + portfolio.t_precmetposition + portfolio.t_nonqualassecposition < 100000000 then
                                  11
                                 else
                                  12
                                 end
     where portfolio.t_sessionid = pSessionId;

    --Заполняем clientcounter
    update dclientportfolio_dbt portfolio
       set portfolio.t_clientcounter = 1
     where (portfolio.t_clientcode, portfolio.t_firmid) IN (select p.t_clientcode, p.t_firmid from dclientportfolio_dbt p where p.t_clientcode = portfolio.t_clientcode and p.t_portfoliovalue IN (select max(t_portfoliovalue) from dclientportfolio_dbt where p.t_clientcode = portfolio.t_clientcode group by t_clientcode) and rownum = 1); 

    --Заполняем таблицу отчёта
    insert into dpart1form725_dbt (t_sessionid, t_a12, t_a13, t_a14, t_a15, t_a16, t_a17, t_a18, t_a19, t_a110, t_a111, t_a112, t_a113, t_a114, t_a115, t_a116, t_a117, t_a118, t_a119, t_a120, t_a121, t_a122, t_a123, t_a124, t_a125, t_a126, t_a127, t_a128, t_a129, t_a130, t_a131, t_a132, t_a133, t_a134, t_a135)
    select t_sessionid, t_clienttype, t_isresident, t_risklevel, t_isqualinvestor, t_rcvstatus, 1, t_cohort, count(distinct t_clientcode), count(distinct t_contractid), round(sum(t_rcv1),0), round(sum(t_rcv2),0), round(sum(t_portfoliovalue),0),
           round(sum(case when t_rubposition > 0 then t_rubposition else 0 end), 0), round(sum(case when t_curposition > 0 then t_curposition else 0 end), 0), round(sum(case when t_secposition > 0 then t_secposition else 0 end), 0), round(sum(case when t_futuresposition > 0 then t_futuresposition else 0 end), 0),
           round(sum(case when t_optionposition > 0 then t_optionposition else 0 end), 0), round(sum(case when t_precmetposition > 0 then t_precmetposition else 0 end + case when t_nonqualassecposition > 0 then t_nonqualassecposition else 0 end), 0),
           round(sum(case when t_rubposition < 0 then t_rubposition else 0 end), 0), round(sum(case when t_curposition < 0 then t_curposition else 0 end), 0), round(sum(case when t_secposition < 0 then t_secposition else 0 end), 0), round(sum(case when t_futuresposition < 0 then t_futuresposition else 0 end), 0),
           round(sum(case when t_optionposition < 0 then t_optionposition else 0 end), 0), round(sum(case when t_precmetposition < 0 then t_precmetposition else 0 end + case when t_nonqualassecposition < 0 then t_nonqualassecposition else 0 end), 0), 0, 0,
           round(sum(t_curinitmargin+t_secinitmargin+t_futuresinitmargin+t_optioninitmargin+t_precmetinitmargin+t_nonqualassecinitmargin),0), round(sum(t_curinitmargin),0), round(sum(t_secinitmargin),0), round(sum(t_futuresinitmargin),0), round(sum(t_optioninitmargin),0), round(sum(t_precmetinitmargin+t_nonqualassecinitmargin),0), 0, 0
      from dclientportfolio_dbt where t_sessionid = pSessionId group by t_clienttype, t_isresident, t_risklevel, t_isqualinvestor, t_rcvstatus, t_cohort, t_sessionid;
   
    it_log.log(p_msg  => 'pSessionID='||pSessionId||' '||
               'Завершено заполнение промежуточных таблиц раздела 1',
               p_msg_type => it_log.C_MSG_TYPE__MSG);

  END;

END RSB_DL725REP;
/
