-- Create table
create table itt_dratedef_dbt_rst
(
  t_isin            VARCHAR2(50),
  t_facevalue       VARCHAR2(50),
  t_ticker          VARCHAR2(250),
  t_security_name   VARCHAR2(250),
  px_last           VARCHAR2(250),
  crncy             VARCHAR2(250),
  mifid_asset_class VARCHAR2(250),
  par_amt           VARCHAR2(250),
  correct_amt       VARCHAR2(250),
  amt               VARCHAR2(250),
  t_sincedate       DATE
);