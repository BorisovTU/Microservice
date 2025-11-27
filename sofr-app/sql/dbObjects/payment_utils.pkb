create or replace package body payment_utils as

  g_empty_date constant date := to_date('01010001','ddmmyyyy');

  function get_department_param(
    p_dep_code     ddp_dep_dbt.t_code%type,
    o_bankname out dparty_dbt.t_name%type,
    o_coracc   out dbankdprt_dbt.t_coracc%type
  ) return ddp_dep_dbt.t_partyid%type
  is 
    l_bankid ddp_dep_dbt.t_partyid%type;
  begin
    select p.t_partyid, p.t_name, dprt.t_coracc 
      into l_bankid, o_bankname, o_coracc
      from ddp_dep_dbt dep,
           dbankdprt_dbt dprt, 
           dparty_dbt p
     where dprt.t_partyid = dep.t_partyid
       and dep.t_partyid = p.t_partyid 
       and dep.t_code = p_dep_code;
    return l_bankid;
  exception
    when no_data_found then 
      o_bankname := chr(1);
      o_coracc := chr(1);
      return 0;
  end;

  function get_schem_param( 
    p_bank_id       in dbnkschem_dbt.t_bankid%type,
    p_code_currency in dbnkschem_dbt.t_fiid%type,
    o_schem_corrac  out  dcorschem_dbt.t_account%type
  ) return dcorschem_dbt.t_number%type
  is
    l_schem_number dcorschem_dbt.t_number%type;
  begin
      select s.t_number, s.t_account
        into l_schem_number, o_schem_corrac
      from 
          dbnkschem_dbt bnk,
          dcorschem_dbt s
      where bnk.t_bankid = p_bank_id 
        and bnk.t_fiid = p_code_currency
        and bnk.t_defaultdebout = 'X'
        and s.t_number = bnk.t_schem
        and s.t_fiid = bnk.t_fiid
        and s.t_fi_kind = bnk.t_fi_kind
        and s.t_corrid = bnk.t_bankid;
    return l_schem_number;
    
  exception
    when no_data_found then 
      l_schem_number := -1;
      o_schem_corrac := chr(1);
      return l_schem_number;
  end;

  function save_pmpaym (
    p_documentid      dpmpaym_dbt.t_documentid%type,
    p_dockind         dpmpaym_dbt.t_dockind%type,
    p_purpose         dpmpaym_dbt.t_purpose%type,
    p_subpurpose      dpmpaym_dbt.t_subpurpose%type,
    p_currency        dpmpaym_dbt.t_fiid%type,
    p_payer           dpmpaym_dbt.t_payer%type,
    p_payeraccount    dpmpaym_dbt.t_payeraccount%type,
    p_payerbankid     dpmpaym_dbt.t_payerbankid%type,
    p_receiver        dpmpaym_dbt.t_receiver%type,
    p_receiveraccount dpmpaym_dbt.t_receiveraccount%type,
    p_receiverbankid  dpmpaym_dbt.t_receiverbankid%type,
    p_amount          dpmpaym_dbt.t_amount%type,
    p_valuedate       dpmpaym_dbt.t_valuedate%type,
    p_numberpack      dpmpaym_dbt.t_numberpack%type,
    p_futurepayeraccount    dpmpaym_dbt.t_futurepayeraccount%type,
    p_futurereceiveraccount dpmpaym_dbt.t_futurereceiveraccount%type
  ) return dpmpaym_dbt.t_paymentid%type is
    l_pmpaym_row  dpmpaym_dbt%rowtype;
    l_id          dpmpaym_dbt.t_paymentid%type;
  begin
    l_pmpaym_row.t_paymentid               := 0;
    l_pmpaym_row.t_dockind                 := p_dockind;
    l_pmpaym_row.t_documentid              := p_documentid;
    l_pmpaym_row.t_purpose                 := p_purpose;
    l_pmpaym_row.t_subpurpose              := p_subpurpose;
    l_pmpaym_row.t_fiid                    := p_currency;
    l_pmpaym_row.t_amount                  := p_amount;
    l_pmpaym_row.t_payfiid                 := p_currency;
    l_pmpaym_row.t_payer                   := p_payer;
    l_pmpaym_row.t_payerbankid             := p_payerbankid;
    l_pmpaym_row.t_payermesbankid          := 0;
    l_pmpaym_row.t_payeraccount            := p_payeraccount;
    l_pmpaym_row.t_receiver                := p_receiver;
    l_pmpaym_row.t_receiverbankid          := p_receiverbankid;
    l_pmpaym_row.t_receivermesbankid       := 1;
    l_pmpaym_row.t_receiveraccount         := p_receiveraccount;
    l_pmpaym_row.t_valuedate               := p_valuedate;
    l_pmpaym_row.t_paymstatus              := 0;
    l_pmpaym_row.t_deliverykind            := 0;
    l_pmpaym_row.t_netting                 := chr(0);
    l_pmpaym_row.t_department              := 1;
    l_pmpaym_row.t_prockind                := 0;
    l_pmpaym_row.t_planpaymid              := 0;
    l_pmpaym_row.t_factpaymid              := 0;
    l_pmpaym_row.t_numberpack              := p_numberpack;
    l_pmpaym_row.t_subsplittedpayment      := 0;
    l_pmpaym_row.t_createdinss             := 0;
    l_pmpaym_row.t_lastsplitsession        := 0;
    l_pmpaym_row.t_tobackoffice            := chr(0);
    l_pmpaym_row.t_legid                   := 0;
    l_pmpaym_row.t_indoorstorage           := 0;
    l_pmpaym_row.t_amountnds               := 0;
    l_pmpaym_row.t_accountnds              := chr(1);
    l_pmpaym_row.t_isplanpaym              := 'X';
    l_pmpaym_row.t_isfactpaym              := 'X';
    l_pmpaym_row.t_reserv1                 := 0;
    l_pmpaym_row.t_recallamount            := 0;
    l_pmpaym_row.t_futurepayeraccount      := p_futurepayeraccount;
    l_pmpaym_row.t_futurereceiveraccount   := p_futurereceiveraccount;
    l_pmpaym_row.t_payamount               := p_amount;
    l_pmpaym_row.t_ratetype                := 0; 
    l_pmpaym_row.t_isinverse               := chr(0);
    l_pmpaym_row.t_scale                   := 1;
    l_pmpaym_row.t_point                   := 4;
    l_pmpaym_row.t_isfixamount             := chr(0);
    l_pmpaym_row.t_feetype                 := 0;
    l_pmpaym_row.t_defcomid                := 0;
    l_pmpaym_row.t_payercodekind           := 1;
    l_pmpaym_row.t_payercode               := nvl(party_read.get_party_code(p_party_id => p_payer, p_code_kind => 1), chr(1));
    l_pmpaym_row.t_receivercodekind        := 1;
    l_pmpaym_row.t_receivercode            := nvl(party_read.get_party_code(p_party_id => p_receiver, p_code_kind => 1), chr(1));
    l_pmpaym_row.t_rate                    := 10000;
    l_pmpaym_row.t_fiid_futurepayacc       := p_currency;
    l_pmpaym_row.t_fiid_futurerecacc       := p_currency;
    l_pmpaym_row.t_baseamount              := p_amount;
    l_pmpaym_row.t_basefiid                := p_currency;
    l_pmpaym_row.t_ratedate                := g_empty_date;
    l_pmpaym_row.t_baseratetype            := 0;
    l_pmpaym_row.t_baserate                := 10000;
    l_pmpaym_row.t_basepoint               := 4;
    l_pmpaym_row.t_basescale               := 1;
    l_pmpaym_row.t_isbaseinverse           := chr(0);
    l_pmpaym_row.t_baseratedate            := g_empty_date;
    l_pmpaym_row.t_futurepayeramount       := p_amount;
    l_pmpaym_row.t_futurereceiveramount    := p_amount;
    l_pmpaym_row.t_subkind                 := 0;
    l_pmpaym_row.t_payerdpnode             := 0;
    l_pmpaym_row.t_receiverdpnode          := 0;
    l_pmpaym_row.t_payerdpblock            := -1;
    l_pmpaym_row.t_receiverdpblock         := -1;
    l_pmpaym_row.t_i2placedate             := g_empty_date;
    l_pmpaym_row.t_payerbankmarkdate       := g_empty_date;
    l_pmpaym_row.t_receiverbankmarkdate    := g_empty_date;
    l_pmpaym_row.t_payerbankenterdate      := g_empty_date;
    l_pmpaym_row.t_partpaymnumber          := 0;
    l_pmpaym_row.t_partpaymshifrmain       := chr(1);
    l_pmpaym_row.t_partpaymnummain         := chr(1);
    l_pmpaym_row.t_partpaymdatemain        := g_empty_date;
    l_pmpaym_row.t_partpaymrestamountmain  := 0;
    l_pmpaym_row.t_kindoperation           := 0;
    l_pmpaym_row.t_claimid                 := 0;
    l_pmpaym_row.t_oper                    := RsbSessionData.Oper;
    l_pmpaym_row.t_opernode                := 1;
    l_pmpaym_row.t_origin                  := 3;
    l_pmpaym_row.t_startdepartment         := 1;
    l_pmpaym_row.t_enddepartment           := 1;
    l_pmpaym_row.t_dbflag                  := chr(0);
    l_pmpaym_row.t_ispurpose               := chr(0);
    l_pmpaym_row.t_notforbackoffice        := chr(0);
    l_pmpaym_row.t_placetoindex            := chr(0);
    l_pmpaym_row.t_primdockind             := 450;
    l_pmpaym_row.t_converted               := chr(0);
    l_pmpaym_row.t_payerdppartition        := chr(1);
    l_pmpaym_row.t_receiverdppartition     := chr(1);
    l_pmpaym_row.t_linkamountkind          := 0;
    l_pmpaym_row.t_comissfiid              := -1;
    l_pmpaym_row.t_comissaccount           := chr(1);
    l_pmpaym_row.t_contrnversion           := 0;
    l_pmpaym_row.t_boprocesskind           := 0;
    l_pmpaym_row.t_futuredratetype         := 0;
    l_pmpaym_row.t_futuredrate             := 0;
    l_pmpaym_row.t_futuredratepoint        := 0;
    l_pmpaym_row.t_futuredratescale        := 0;
    l_pmpaym_row.t_futuredrateisinverse    := chr(0);
    l_pmpaym_row.t_futuredratedate         := g_empty_date;
    l_pmpaym_row.t_futureratedepartment    := 0;
    l_pmpaym_row.t_chapter                 := 1;
    l_pmpaym_row.t_futurebaseamount        := p_amount;
    l_pmpaym_row.t_orderfiid               := 0;
    l_pmpaym_row.t_orderamount             := p_amount;
    l_pmpaym_row.t_futurecratetype         := 0;
    l_pmpaym_row.t_futurecrate             := 0;
    l_pmpaym_row.t_futurecratedate         := g_empty_date;
    l_pmpaym_row.t_futurecratescale        := 0;
    l_pmpaym_row.t_futurecratepoint        := 0;
    l_pmpaym_row.t_futurecrateisinverse    := chr(0);
    l_pmpaym_row.t_closedate               := g_empty_date;
    l_pmpaym_row.t_checkterror             := 0;
    l_pmpaym_row.t_primdocorigin           := 1;
    l_pmpaym_row.t_typedocument            := chr(1);
    l_pmpaym_row.t_usertypedocument        := chr(1);
    l_pmpaym_row.t_userfield1              := chr(1);
    l_pmpaym_row.t_userfield2              := chr(1);
    l_pmpaym_row.t_userfield3              := chr(1);
    l_pmpaym_row.t_userfield4              := chr(1);
    l_pmpaym_row.t_mcmethodid              := 0;
    l_pmpaym_row.t_paytype                 := 0;
    l_pmpaym_row.t_creationdate            := trunc(sysdate);
    l_pmpaym_row.t_creationtime            := to_date('01010001 '||to_char(sysdate, 'hh24:mi:ss'),'ddmmyyyy hh24:mi:ss');
    l_pmpaym_row.t_minimizationturn        := chr(0);
    l_pmpaym_row.t_contentoperation        := chr(1);
    
    insert into dpmpaym_dbt values l_pmpaym_row
    returning t_paymentid into l_id;
    
    return l_id;
  end save_pmpaym;


  procedure save_pmprop (
    p_paymentid   dpmprop_dbt.t_paymentid%type,
    p_bank_id     number,
    p_debetcredit dpmprop_dbt.t_debetcredit%type,
    p_corracc     dpmprop_dbt.t_ourcorracc%type,
    p_schem_number  dpmprop_dbt.t_corschem%type
  ) is
    l_pmprop_row  dpmprop_dbt%rowtype;
    
    l_corschem    dpmprop_dbt.t_corschem%type;
    l_issender    dpmprop_dbt.t_issender%type;
    l_group       dpmprop_dbt.t_group%type;
    l_ourcorracc  dpmprop_dbt.t_ourcorracc%type;
    l_inourbalance  dpmprop_dbt.t_inourbalance%type;
    l_corrcodekind  dpmprop_dbt.t_corrcodekind%type;
    l_corrcodename  dpmprop_dbt.t_corrcodename%type;
  begin
    
    if p_debetcredit = 0 then 
      l_corschem      := -1;
      l_issender      := 'X';
      l_corrcodekind  := 0;
      l_corrcodename  := chr(1);
      l_ourcorracc    := p_corracc;
      l_inourbalance  := 'X';
      l_group         := 0;
    else 
      l_corschem      := p_schem_number;
      l_issender      := chr(0);
      l_corrcodekind  := 3;
      l_corrcodename  := 'Åàä';
      l_ourcorracc    := chr(1);
      l_inourbalance  := chr(0);
      l_group         := 0;
    end if;
    
    l_pmprop_row.t_paymentid          := p_paymentid;
    l_pmprop_row.t_debetcredit        := p_debetcredit;
    l_pmprop_row.t_codekind           := 3;
    l_pmprop_row.t_codename           := 'Åàä';
    l_pmprop_row.t_bankcode           := party_read.get_party_code(p_party_id => p_bank_id, p_code_kind => l_pmprop_row.t_codekind);
    l_pmprop_row.t_payfiid            := 0;
    l_pmprop_row.t_corschem           := l_corschem;
    l_pmprop_row.t_issender           := l_issender;
    l_pmprop_row.t_propstatus         := 0;
    l_pmprop_row.t_tpid               := 0;
    l_pmprop_row.t_transferdate       := g_empty_date;
    l_pmprop_row.t_corracc            := chr(1);
    l_pmprop_row.t_corrcodekind       := l_corrcodekind;
    l_pmprop_row.t_corrcodename       := l_corrcodename;
    l_pmprop_row.t_corrcode           := chr(1);
    l_pmprop_row.t_sortkey            := 0;
    l_pmprop_row.t_corrpostype        := 0;
    l_pmprop_row.t_instructionabonent := 0;
    l_pmprop_row.t_settlementsystemcode := chr(1);
    l_pmprop_row.t_corrid             := -1;
    l_pmprop_row.t_ourcorrid          := -1;
    l_pmprop_row.t_ourcorrcodekind    := 0;
    l_pmprop_row.t_ourcorrcode        := chr(1);
    l_pmprop_row.t_ourcorracc         := l_ourcorracc;
    l_pmprop_row.t_inourbalance       := l_inourbalance;
    l_pmprop_row.t_group              := l_group;
    l_pmprop_row.t_contrnversion      := 0;
    l_pmprop_row.t_tpschemid          := 0;
    l_pmprop_row.t_rlsformid          := 0;
    l_pmprop_row.t_subkindmessage     := 0;
    l_pmprop_row.t_reserve            := chr(1);
    l_pmprop_row.t_spi_ident          := chr(1);
    
    insert into dpmprop_dbt values l_pmprop_row;
    
  end save_pmprop;

  procedure save_pmrmprop (
    p_paymentid         dpmrmprop_dbt.t_paymentid%type,
    p_number            dpmrmprop_dbt.t_number%type,
    p_date              dpmrmprop_dbt.t_date%type,
    p_payername         dpmrmprop_dbt.t_payername%type,
    p_payerbankname     dpmrmprop_dbt.t_payerbankname%type,
    p_payercorracc      dpmrmprop_dbt.t_payercorraccnostro%type,
    p_payerinn          dpmrmprop_dbt.t_payerinn%type,
    p_receivername      dpmrmprop_dbt.t_receiverinn%type,
    p_receiverbankname  dpmrmprop_dbt.t_receiverbankname%type,
    p_receiverinn       dpmrmprop_dbt.t_payerinn%type,
    p_receivercorracc   dpmrmprop_dbt.t_receivercorraccnostro%type,
    p_paydate           dpmrmprop_dbt.t_paydate%type,
    p_ground            dpmrmprop_dbt.t_ground%type
  ) is
    l_pmrmprop_row  dpmrmprop_dbt%rowtype;
  begin
    l_pmrmprop_row.t_paymentid              := p_paymentid;
    l_pmrmprop_row.t_number                 := p_number;
    l_pmrmprop_row.t_reference              := chr(1);
    l_pmrmprop_row.t_date                   := p_date;
    l_pmrmprop_row.t_paymentkind            := 'ù';
    l_pmrmprop_row.t_payercorraccnostro     := p_payercorracc;
    l_pmrmprop_row.t_payerbankname          := p_payerbankname;
    l_pmrmprop_row.t_payername              := p_payername;
    l_pmrmprop_row.t_payerinn               := p_payerinn;
    l_pmrmprop_row.t_receivercorraccnostro  := p_receivercorracc;
    l_pmrmprop_row.t_receiverbankname       := p_receiverbankname;
    l_pmrmprop_row.t_receivername           := p_receivername;
    l_pmrmprop_row.t_receiverinn            := p_receiverinn;
    l_pmrmprop_row.t_shifroper              := chr(1);
    l_pmrmprop_row.t_priority               := 5;
    l_pmrmprop_row.t_paydate                := p_paydate;
    l_pmrmprop_row.t_ground                 := p_ground;
    l_pmrmprop_row.t_clientdate             := p_date;
    l_pmrmprop_row.t_processkind            := chr(1);
    l_pmrmprop_row.t_messagetype            := chr(1);
    l_pmrmprop_row.t_partyinfo              := chr(1);
    l_pmrmprop_row.t_payercorrbankname      := chr(1);
    l_pmrmprop_row.t_receivercorrbankname   := chr(1);
    l_pmrmprop_row.t_isshortformat          := chr(0);
    l_pmrmprop_row.t_kindpaycurrency        := 0;
    l_pmrmprop_row.t_ourpayercorrname       := chr(1);
    l_pmrmprop_row.t_ourreceivercorrname    := chr(1);
    l_pmrmprop_row.t_payerchargeoffdate     := g_empty_date;
    l_pmrmprop_row.t_taxauthorstate         := chr(1);
    l_pmrmprop_row.t_bttticode              := chr(1);
    l_pmrmprop_row.t_okatocode              := chr(1);
    l_pmrmprop_row.t_taxpmground            := chr(1);
    l_pmrmprop_row.t_taxpmperiod            := chr(1);
    l_pmrmprop_row.t_taxpmnumber            := chr(1);
    l_pmrmprop_row.t_taxpmdate              := chr(1);
    l_pmrmprop_row.t_taxpmtype              := chr(1);
    l_pmrmprop_row.t_symbnotbaldebet        := chr(1);
    l_pmrmprop_row.t_instancy               := 0;
    l_pmrmprop_row.t_docdispatchdate        := g_empty_date;
    l_pmrmprop_row.t_cashsymboldebet        := chr(1);
    l_pmrmprop_row.t_cashsymbolcredit       := chr(1);
    l_pmrmprop_row.t_symbnotbalcredit       := chr(1);
    l_pmrmprop_row.t_isoptimbytime          := chr(0);
    l_pmrmprop_row.t_comisscharges          := 0;
    l_pmrmprop_row.t_instructioncode        := chr(1);
    l_pmrmprop_row.t_additionalinfo         := chr(1);
    l_pmrmprop_row.t_contrnversion          := 0;
    l_pmrmprop_row.t_neednotify             := chr(0);
    l_pmrmprop_row.t_kindoper               := chr(1);
    l_pmrmprop_row.t_receiverchargeoffdate  := g_empty_date;
    l_pmrmprop_row.t_uin                    := chr(1);
    l_pmrmprop_row.t_legality               := 0;
    l_pmrmprop_row.t_legalityreason         := chr(1);
    l_pmrmprop_row.t_legalityedit           := 0;
    l_pmrmprop_row.t_paymentbyotherperson   := 0;
    l_pmrmprop_row.t_precedence             := chr(1);
    l_pmrmprop_row.t_settlementtime         := g_empty_date;
    
    insert into dpmrmprop_dbt values l_pmrmprop_row;
    
  end save_pmrmprop;  

end payment_utils;
/
