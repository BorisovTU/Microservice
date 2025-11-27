
-- Create table
begin
  execute immediate q'[
create global temporary table DCLPORTSOFR2DWH_TMP
(
  t_partyid          NUMBER(10),
  t_reportdate       DATE,
  t_contrnumber      VARCHAR2(20),
  t_contrdatebegin   DATE,
  t_clientname       VARCHAR2(320),
  t_clientcode       VARCHAR2(12),
  t_dsfcontrid       NUMBER(10),
  t_fininstr         VARCHAR2(50),
  t_fininstrtype     VARCHAR2(50),
  t_fiid             NUMBER(10),
  t_avrkind          NUMBER(10),
  t_qty              NUMBER(32,12),
  t_nkd              NUMBER(32,12),
  t_price            NUMBER(32,12),
  t_rateid           NUMBER(10),
  t_ratecb           NUMBER(32,12),
  t_fininstrccy      VARCHAR2(3),
  t_facevalue        NUMBER(32,12),
  t_open_balance     NUMBER(32,12),
  t_open_balance_rub NUMBER(32,12),
  t_principal_plus   NUMBER(10),
  t_principal_minus  NUMBER(10),
  t_inputcash        NUMBER(32,12),
  t_inputsec         NUMBER(32,12),
  t_outputcash        NUMBER(32,12),
  t_outputsec        NUMBER(32,12),
  t_redemption       NUMBER(32,12),
  t_is_partial       NUMBER(10),  
  t_amortization     NUMBER(32,12),
  t_div              NUMBER(32,12),
  t_profitaccount    CHAR(1),
  t_coupon           NUMBER(32,12),
  t_iis              NUMBER
)
on commit preserve rows]';   
end;
/