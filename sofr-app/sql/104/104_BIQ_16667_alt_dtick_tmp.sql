declare
  n      number;
  t_name varchar2(100) := 'DTICK_TMP';
  c_name varchar2(100) ;
begin
  c_name := 't_dlcontrid';
  select count(*) into n from user_tab_columns c where c.TABLE_NAME = upper(t_name) and c.COLUMN_NAME = upper(c_name);
  if n = 0 then
    execute immediate 'alter table DTICK_TMP add t_dlcontrid NUMBER(10)';
    execute immediate 'alter table DTICK_TMP add t_calc_sid varchar2(128) default ''X'' not null';
    execute immediate 'comment on column DTICK_TMP.t_calc_sid  is ''СИД расчета = rshb_rsi_sclimit.GC_CALC_SID_DEFAULT''';
    execute immediate 'alter table DTICK_TMP modify partition by list (t_calc_sid) (partition p99999999x values (''X''))';
    execute immediate 'drop index DTICK_TMP_IDX0';
    execute immediate 'create unique index DTICK_TMP_IDX0 on dtick_tmp (T_CALC_SID,T_DEALID) local' ;
    execute immediate 'drop index DTICK_TMP_IDX1';
    execute immediate 'create index DTICK_TMP_IDX1 on dtick_tmp (t_marketid,T_CLIENTCONTRID, T_CLIENTID) local' ;

    execute immediate 'create index DTICK_TMP_IDX2 on DTICK_TMP (t_marketid,t_dlcontrid)  local';

  end if;
end;
/