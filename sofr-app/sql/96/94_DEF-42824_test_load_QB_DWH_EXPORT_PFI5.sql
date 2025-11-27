--первоначальная загрузка для пакета QB_DWH_EXPORT_PFI
create or replace package test_load_QB_DWH_EXPORT_PFI is

  procedure load_fct_account_sofr;

end test_load_QB_DWH_EXPORT_PFI;
/
create or replace package body test_load_QB_DWH_EXPORT_PFI is

  dwhEXT_FILE  varchar2(300);

  procedure export_pfi(in_date in date) is
    --prev_in_date date := rsi_rsbcalendar.getdateafterworkday(in_date, -1); DEF-42824
  begin
    /*if (To_Char(in_date, 'D') = 1) then DEF-42824
      return; -- не брать воскресение
    end if;*/
    dbms_output.put_line(in_date);

    insert into LDR_INFA_PFI.FCT_ACCOUNT_SOFR
      (account_sofr_code,
       rest,
       rest_nat,
       debet,
       debet_nat,
       credit,
       credit_nat,
       dt,
       sysmoment,
       rec_status,
       ext_file)
      select '0000#SOFRXXX#' || a.t_account as ACCOUNT_SOFR_CODE,
             to_char(rsi_rsb_account.restall(a.t_account,
                                             1,
                                             a.t_code_currency,
                                             in_date,
                                             a.t_code_currency)) as REST,
             to_char(rsi_rsb_account.restall(a.t_account,
                                             1,
                                             a.t_code_currency,
                                             in_date,
                                             0)) as REST_NAT,
             to_char(rsi_rsb_account.debetac(a.t_account,
                                             1,
                                             a.t_code_currency,
                                             in_date,
                                             in_date,
                                             a.t_code_currency)) as DEBET,
             to_char(rsi_rsb_account.debetac(a.t_account,
                                             1,
                                             a.t_code_currency,
                                             in_date,
                                             in_date,
                                             0)) as DEBET_NAT,
             to_char(rsi_rsb_account.kreditac(a.t_account,
                                              1,
                                              a.t_code_currency,
                                              in_date,
                                              in_date,
                                              a.t_code_currency)) as CREDIT,
             to_char(rsi_rsb_account.kreditac(a.t_account,
                                              1,
                                              a.t_code_currency,
                                              in_date,
                                              in_date,
                                              0)) as CREDIT_NAT,
             qb_dwh_utils.DateToChar(in_date) as DT,
             qb_dwh_utils.DateTimeToChar(sysdate) as SYSMOMENT,
             '0' as REC_STATUS,
             dwhEXT_FILE as EXT_FILE
        from Daccount_dbt a
       inner join dparty_dbt p
          on a.t_client = p.t_partyid
       inner join dfininstr_dbt f
          on f.t_fiid = a.t_code_currency
         and f.t_fi_kind = 1
       where (a.t_balance = '30601' or a.t_balance = '30606');
    commit;

    delete from LDR_INFA_PFI.FCT_ACCOUNT_SOFR f
     where f.debet = 0
       and f.debet_nat = 0
       and f.credit = 0
       and f.credit_nat = 0;
    commit;


  exception
    when others then
      null;
  end;

  procedure load_fct_account_sofr is
    dt        date := to_date('24012023', 'ddmmyyyy');  --стартовое значение
    sysdt     date := sysdate;
    pDepartment number;
    out_dwhDT varchar2(20) :=To_Char(sysdate,'dd.mm.yyyy HH24:MI:SS');
    out_dwhRecStatus varchar2(20);

  begin
    execute immediate 'truncate table LDR_INFA_PFI.FCT_ACCOUNT_SOFR';
    pDepartment      := 1;
    out_dwhRecStatus := '99999'; --случайное значение
    dwhEXT_FILE := qb_dwh_utils.GetComponentCode('EXT_FILE',
                                                     qb_dwh_utils.System_RS,
                                                     pDepartment,
                                                     qb_dwh_utils.GetEXT_FILE_ID(99999, --in_EventID, случайное
                                                                                 out_dwhDT,
                                                                                 out_dwhRecStatus));

    while (trunc(dt) < trunc(sysdt) 
      --to_date('26012023','ddmmyyyy')
      ) loop
      export_PFI(dt);
      dt := dt + 1;
    end loop;

    --возможна ли оптимизация если delete вынесен за цикл здесь? или оставить delete внутри  цикла внутри export_PFI?
    /*delete from LDR_INFA_PFI.FCT_ACCOUNT_SOFR f
     where f.debet = 0
       and f.debet_nat = 0
       and f.credit = 0
       and f.credit_nat = 0;
    commit;*/


  end;

end test_load_QB_DWH_EXPORT_PFI;
/
