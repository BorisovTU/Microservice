CREATE OR REPLACE PROCEDURE RSHB_F303_2CHD_Generate(
   startdate        IN DATE,
   enddate          IN DATE)
iS
   CURSOR deals (
         sdate        IN DATE,
         edate        IN DATE)
      IS
         select leg.t_id as dealid,
secpart.t_id as dealidSsylka,
CASE WHEN INSTR ( TypeOperation.t_systypes, 'X') <> 0
                THEN 1
                ELSE 0
             END as instrumenttype,
TP.T_NAME as TP,
CASE WHEN leg.t_legkind = 0 THEN tick.t_dealcode || '-1'
ELSE tick.t_dealcode || '-2'
END as Dealnum,
case when leg.t_legkind = 0 
then  CASE WHEN INSTR ( TypeOperation.t_systypes,
                                'B') <> 0
                THEN
                   1
                else
                   0
                end
else CASE WHEN INSTR ( TypeOperation.t_systypes,
                                'B') = 0
                THEN
                   1
                else
                   0
                end
end as Dealtype,
case when leg.t_legkind = 0 then  34
else  35 end as contractflag,
case when INSTR(oper.t_name,'œˇÏÓÂ –≈œŒ') <> 0
then 'œˇÏÓÂ –≈œŒ'
else  'Œ·‡ÚÌÓÂ –≈œŒ' end as DealTypeBrief,
TICK.T_DEALCODE as tradingsysnum,
TICK.T_DEALDATE as dealdate,
RQ.T_plandate as valdatePlan,
rq.t_factdate as valdateFact,
case when leg.t_cfi = 7 then 1
when leg.t_cfi = 0 then 2
else 0 end as Fundid,
case when leg.t_cfi = 7 then 'USD'
when leg.t_cfi = 0 then 'RUB'
else 'EUR' end as FundBrief,
case when leg.t_cfi = 7 then 1
when leg.t_cfi = 0 then 2
else 0 end  as FundRaschid,
case when leg.t_cfi = 7 then 'USD'
when leg.t_cfi = 0 then 'RUB'
else 'EUR' end as FundRaschBrief,
'–≈œŒ' as vidSD,
null as ViD2332,
'45286590000' as Œ ¿“Œ,
COR.T_ATTRID as KatKachestva,
code.t_code as IDKontrBis,
case when isrkc = 'X' then 'RKC'
when isbank = 'X' then 'BANK'
else 'URLICO' end as typeKontr,
party.t_shortname as ContrName,
leg.T_PFI as securityid,
avo.t_isin as ISIN,
LEG.T_INCOMERATE as prcRepo,
'fix' as TypePrcRepo,
case when rqP.t_factdate = to_date('01/01/0001','dd/mm/yyyy')
then RSI_RSB_FIInstr.ConvSumType(rqP.t_amount,leg.t_pfi,0,12,rqP.t_plandate,0)
else RSI_RSB_FIInstr.ConvSumType(rqP.t_amount,leg.t_pfi,0,12,rqP.t_factdate,0)end as TSS,
LEG.T_TOTALCOST as dealqty
from ddl_leg_dbt leg
left join ddl_tick_dbt tick on leg.t_dealid = tick.t_dealid
left join doprkoper_dbt oper on tick.t_dealtype = oper.T_KIND_OPERATION
left join ddlrq_dbt rq on RQ.T_DOCID = tick.t_dealid and RQ.T_DOCKIND = 101 and rq.t_type = 2 and ((leg.t_legkind = 0 and RQ.T_DEALPART = 1) or (leg.t_legkind = 2 and RQ.T_DEALPART = 2))
left join ddlrq_dbt rqP on RQP.T_DOCID = tick.t_dealid and RQP.T_DOCKIND = 101 and rqP.t_type = 8 and ((leg.t_legkind = 0 and RQP.T_DEALPART = 1) or (leg.t_legkind = 2 and RQP.T_DEALPART = 2))
left join dparty_dbt party on TICK.T_PARTYID = party.t_partyid
left join davoiriss_dbt avo on LEG.T_PFI = avo.t_fiid 
left join dsfcontrplan_dbt contr on tick.T_CLIENTCONTRID = contr.T_ID
left join dsfplan_dbt tp on CONTR.T_SFPLANID = TP.T_SFPLANID
left join dobjatcor_dbt cor on to_number(COR.T_OBJECT) = party.t_partyid and cor.t_objecttype = 3 and cor.t_groupid = 13
left join dobjcode_dbt code on party.t_partyid = CODE.T_OBJECTID and code.t_codekind = 101
left join (select party.t_partyid, party.t_legalform,
    case when isb.num = 1 then 'X' end as isbank,
    case when isdu.num = 1 then 'X' end as isrkc
    from dparty_dbt party
    left join(select PTOWN.T_PARTYID, count(*) as num from dpartyown_dbt ptown where PTOWN.T_PARTYKIND = 2 group by PTOWN.T_PARTYID) isb on  isb.T_PARTYID = party.t_partyid
    left join(select PTOWN.T_PARTYID, count(*) as num from dpartyown_dbt ptown where PTOWN.T_PARTYKIND = 41 group by PTOWN.T_PARTYID) isdu on  isdu.T_PARTYID = party.t_partyid) kind on party.t_partyid = kind.t_partyid
    left join (SELECT T_ID, t_dealid FROM ddl_leg_dbt ) secpart on  secpart.t_dealid IN (SELECT t_dealid FROM ddl_leg_dbt WHERE t_id = leg.t_id) and LEG.T_ID <> secpart.T_ID
    left join (SELECT t_systypes,t_kind_operation from doprkoper_dbt) TypeOperation on TypeOperation.t_kind_operation = tick.t_dealtype
where INSTR ( (SELECT t_systypes FROM doprkoper_dbt WHERE t_kind_operation = tick.t_dealtype),'t') <> 0 and TICK.T_DEALDATE between sdate and edate;


cursor Docs (id_deal in tRSHB_F303_2CHD_P.DEALID%type)
 is 
  select leg.t_id as dealid,
ACCTRN.T_DATE_CARRY as operdate,
ACCTRN.T_NUMBER_PACK as Batch,
ACCTRN.T_ACCOUNTID_PAYER as DebResourceid,
ACCTRN.T_ACCOUNT_payer as DebAcc,
ACCTRN.T_FIID_PAYER as Debfundid,
fi1.t_ccy as DebfundBrief,
case when acctrn.t_scale <> 0 and acctrn.t_scale is not null
then acctrn.t_rate/ power(10, acctrn.t_point)/acctrn.t_scale 
else 0 end as DebCourse,
acctrn.t_sum_payer as DebQty,
acctrn.t_sum_natcur as DebQtyRub,
acctrn.T_ACCOUNTID_RECEIVER as CreResourceid,
acctrn.T_ACCOUNT_RECEIVER as CreAcc,
acctrn.t_fiid_receiver as Crefundid,
fi2.t_ccy as CrefundBrief,
case when acctrn.t_scale <> 0 and acctrn.t_scale is not null
then acctrn.t_rate/ power(10, acctrn.t_point)/acctrn.t_scale 
else 0 end as CreCourse,
acctrn.t_sum_receiver as CreQty,
acctrn.t_sum_natcur as CreQtyRub,
ACCTRN.T_GROUND as komment
from ddl_leg_dbt leg
left join ddl_tick_dbt tick on LEG.T_DEALID = tick.t_dealid
left join ddlgrdeal_dbt grdeal on TICK.T_DEALID = GRDEAL.t_docid and ((leg.t_legkind = 0 and grdeal.t_templnum in (13,15,17,19,20,37,39,46)) or (leg.t_legkind = 2 and grdeal.t_templnum in (28,31,34,35,36,38,40))) 
left join ddlgrdoc_dbt grdoc on GRDEAL.T_ID = grdoc.t_grdealid and GRDOC.T_DOCKIND = 1
left join DACCTRN_DBT acctrn on GRDOC.T_DOCID = ACCTRN.T_ACCTRNID and ACCTRN.t_chapter not in (21,22)
left join dfininstr_dbt fi1 on ACCTRN.T_FIID_PAYER = fi1.t_fiid
left join dfininstr_dbt fi2 on ACCTRN.t_fiid_receiver = fi2.t_fiid
where INSTR ( (SELECT t_systypes FROM doprkoper_dbt WHERE t_kind_operation = tick.t_dealtype),'t') <> 0 and ACCTRN.t_acctrnid is not null and leg.t_id = id_deal;

cursor Accounts (id_deal in tRSHB_F303_2CHD_A.DEALID%type)
is 
select leg.t_id as dealid,
ac.t_accountid as resourceid,
cat.t_code as AccType,
ac.t_account as AccBrief,
ac.t_nameaccount as AccName
from ddl_leg_dbt leg
left join dmcaccdoc_dbt accdoc on accdoc.t_docid = LEG.T_ID and ACCDOC.T_DOCKIND = 176 and accdoc.t_chapter not in (21,22) 
left join daccount_dbt ac on accdoc.t_account like ac.t_account
left join DMCCATEG_DBT cat on accdoc.t_catid = CAT.T_ID and accdoc.t_catnum = cat.t_number 
where ac.t_accountid is not null and  leg.t_id = id_deal;

deals_rec deals%rowtype;
Docs_rec Docs%rowtype;
accounts_rec Accounts%rowtype;

   begin
   execute immediate 'truncate  table tRSHB_F303_2CHD_D';
   execute immediate 'truncate  table tRSHB_F303_2CHD_P';
   execute immediate 'truncate  table tRSHB_F303_2CHD_A';
   
   open deals(startdate, enddate);
   loop
   fetch deals into deals_rec;
   EXIT WHEN deals%notfound;
   insert into tRSHB_F303_2CHD_D values deals_rec;
     
     open Docs(deals_rec.dealid);
     loop
     fetch Docs into Docs_rec;
      EXIT WHEN Docs%notfound;
     insert into tRSHB_F303_2CHD_P values Docs_rec;
     end loop;
     close Docs;
     
     open Accounts(deals_rec.dealid);
     loop
     fetch accounts into accounts_rec;
     exit when accounts%notfound;
     insert into tRSHB_F303_2CHD_A values accounts_rec;
     end loop;
     close accounts;
  
   end loop; 
   close deals;
   commit;
   end;