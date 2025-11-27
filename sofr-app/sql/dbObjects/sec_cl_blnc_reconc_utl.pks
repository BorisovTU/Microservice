create or replace package sec_cl_blnc_reconc_utl as
  --reconciliation of securities balances. Clients

  procedure fill_buf_by_depo_data(p_date       date,
                                  p_partyid    dparty_dbt.t_partyid%type,
                                  p_fiid       dpmwrtcl_dbt.t_fiid%type,
                                  p_issuer     dfininstr_dbt.t_issuer%type,
                                  p_avoirkind  davrkinds_dbt.t_avoirkind%type,
                                  p_contractid dsfcontr_dbt.t_id%type);

  procedure cl_GatherLots(p_AvrKind       integer,
                          p_Department    integer,
                          p_ClientCode    integer,
                          p_ReportDateBeg date,
                          p_ReportDateEnd date,
                          p_Contract      integer,
                          p_FIID          integer);

  procedure cl_GatherDataInnerAccounting(pClientCode    integer,
                                         pIsPeriod      integer,
                                         pReportDateEnd date,
                                         pReportDateBeg date,
                                         pAvrCode       Number,
                                         pIssuerCode    varchar2,
                                         pOurBank       integer,
                                         pAvrKind       integer,
                                         p_contractid   integer);


  procedure extend_buf_by_spec_depo_data (
    p_date date
  );
  function Check_DepoAcc_TradePlace(p_depoacc_firstletter varchar2,p_depoacc_middlenumber varchar2
                                    ,p_marketid number,p_servkindsub number) return number deterministic ;
end sec_cl_blnc_reconc_utl;
/
