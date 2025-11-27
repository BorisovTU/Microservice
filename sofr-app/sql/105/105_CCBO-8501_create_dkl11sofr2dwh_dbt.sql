begin
  execute immediate q'[
     create table dkl11sofr2dwh_dbt (
     t_recid NUMBER(10),
     t_contrnumber  VARCHAR2(20),
     t_contrdatebegin DATE, 
     t_planname VARCHAR2(50),
     t_clientname VARCHAR2(320), 
     t_borndate DATE, 
     t_clientcode VARCHAR2(12),  
     t_ds_out  NUMBER, 
     t_p_out NUMBER,   
     t_clienttype  VARCHAR2(2), 
     t_depname  VARCHAR2(320),  
     t_depcode  VARCHAR2(4),
     t_finname  VARCHAR2(4000),
     t_marketname VARCHAR2(11),  
     t_ds_plus  NUMBER, 
     t_p_plus NUMBER, 
     t_ds_minus NUMBER,
     t_p_minus  NUMBER, 
     t_netto  NUMBER, 
     t_assetsbegin  NUMBER,
     t_assetsend  NUMBER,
     t_restperc NUMBER,
     t_turnsum NUMBER,
     t_turnsumrepo  NUMBER, 
     t_comsum NUMBER,
     t_comsumbank NUMBER, 
     t_profit NUMBER,
     t_comsumrepo NUMBER, 
     t_comsumbankrepo NUMBER, 
     t_profitrepo NUMBER, 
     t_specreposwap  NUMBER,
     t_totalprofit  NUMBER,
     t_enddatereport DATE, 
     t_uploadtime  TIMESTAMP, 
     t_partyid NUMBER(10)  
  )]';  
   
end;
/

