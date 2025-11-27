-- Create table
create table itt_dratedef_dbt
(
  t_rateid        NUMBER(10),
  t_fiid          NUMBER(10),
  t_otherfi       NUMBER(10),
  t_name          VARCHAR2(60),
  t_definition    VARCHAR2(164),
  t_type          NUMBER(5),
  t_isdominant    CHAR(1),
  t_isrelative    CHAR(1),
  t_informator    NUMBER(10),
  t_market_place  NUMBER(10),
  t_isinverse     CHAR(1),
  t_rate          FLOAT(53),
  t_scale         NUMBER(10),
  t_point         NUMBER(5),
  t_inputdate     DATE,
  t_inputtime     DATE,
  t_oper          NUMBER(5),
  t_sincedate     DATE,
  t_section       NUMBER(10),
  t_version       NUMBER(10),
  t_ismanualinput CHAR(1)
);


