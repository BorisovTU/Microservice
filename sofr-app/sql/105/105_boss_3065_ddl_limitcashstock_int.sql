declare
  n      number;
  t_name varchar2(100) := 'DDL_LIMITCASHSTOCK_INT';
  c_name varchar2(100);
begin
  c_name := 't_calc_sid';
  select count(*)
    into n
    from user_tab_columns c
   where c.TABLE_NAME = upper(t_name)
     and c.COLUMN_NAME = upper(c_name);
  if n = 0
  then
    execute immediate ' create table DDL_LIMITCASHSTOCK_INT
(
  t_id              NUMBER(10) default 0,
  t_date            DATE default TO_DATE(''01.01.0001'',''dd.mm.yyyy''),
  t_time            DATE default TO_DATE(''01.01.0001'',''dd.mm.yyyy''),
  t_market          VARCHAR2(35) default chr(1),
  t_client          NUMBER(10) default 0,
  t_internalaccount NUMBER(10) default 0,
  t_firm_id         VARCHAR2(12) default CHR(1),
  t_tag             VARCHAR2(5) default CHR(1),
  t_currid          NUMBER(10) default -1,
  t_curr_code       VARCHAR2(3) default CHR(1),
  t_client_code     VARCHAR2(35) default CHR(1),
  t_open_balance    NUMBER(32,12) default 0,
  t_open_limit      NUMBER(32,12) default 0,
  t_current_limit   NUMBER(32,12) default 0,
  t_leverage        NUMBER(32,12) default 0,
  t_limit_kind      NUMBER(5) default 0,
  t_money306        NUMBER(32,12) default 0,
  t_due474          NUMBER(32,12) default 0,
  t_plan_plus_deal  NUMBER(32,12) default 0,
  t_plan_minus_deal NUMBER(32,12) default 0,
  t_comprevious     NUMBER(32,12) default 0,
  t_isblocked       CHAR(1) default CHR(0),
  t_market_kind     VARCHAR2(10) default chr(1),
  t_contrid         NUMBER(10) default 0,
  t_servsubkind     NUMBER(5) default 0,
  t_enddate         DATE default TO_DATE(''01.01.0001'',''dd.mm.yyyy''),
  t_comprevious_1   NUMBER(32,12) default 0,
  t_sp              NUMBER(32,12) default 0,
  t_zch             NUMBER(32,12) default 0,
  t_calc_sid        varchar2(128) default ''X'' not null
) partition by list (t_calc_sid) (partition p99999999x values (''X''))';


    execute immediate 'comment on column ddl_limitcashstock_int.t_calc_sid  is ''СИД расчета = rshb_rsi_sclimit.GC_CALC_SID_DEFAULT''';

   execute immediate 'create index DDL_LIMITCASHSTOCK_INT_IDX1 on DDL_LIMITCASHSTOCK_INT (t_market_kind, t_client_code)  local';

  end if;
end;
/