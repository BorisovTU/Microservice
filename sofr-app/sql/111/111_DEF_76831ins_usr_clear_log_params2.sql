begin
  delete from USR_CLEAR_LOG_PARAMS t where t.t_table_name in ('DDL_LIMIT_ARCH_DBT');
  insert into USR_CLEAR_LOG_PARAMS
    (T_TABLE_NAME
    ,T_QUERY)
  values
    ('DDL_LIMIT_ARCH_DBT'
    , q'[declare
  dt_start date := sysdate;
  type tt_arch_table is table of varchar2(128);
  vt_arch_table tt_arch_table := tt_arch_table('DDL_LIMITCASHSTOCKARCH_DBT', 'DDL_LIMITSECURITESARCH_DBT', 'DDL_limitfuturmarkarch_DBT');
  vt_arch_hint  tt_arch_table := tt_arch_table('/*+ index(l DDL_LIMITCASHSTOCKARCH_DBT_IDX3) */', '/*+ index(l DDL_LIMSECURITESARCH_DBT_IDX1) */', '');
  v_date_last   date;
  v_date_end    date;
  v_reccount    integer := 100000;
  v_reccountall integer := 0;
begin
  for n in vt_arch_table.first .. vt_arch_table.last
  loop
    execute immediate 'SELECT ' || vt_arch_hint(n) ||' min(T_DATE), max(T_DATE) FROM ' || vt_arch_table(n) || ' l  WHERE  l.T_DATE < ADD_MONTHS(trunc(sysdate),-12)'
      into v_date_last, v_date_end;
    while nvl(v_date_last, sysdate + 1) <= nvl(v_date_end, sysdate)
    loop
      v_reccount := 100000;
      v_reccountall := 0;
      while v_reccount = 100000
      loop
        execute immediate 'delete ' || vt_arch_hint(n) ||' FROM ' || vt_arch_table(n) || ' l WHERE l.T_DATE >= :dt and l.T_DATE < :dt+1 and rownum <= 100000 '
          using v_date_last,v_date_last;
        v_reccount := sql%rowcount;
        commit;
        v_reccountall := v_reccountall + v_reccount;
      end loop;
      if v_reccountall > 0
      then
        it_log.log_handle(p_object => 'USR_CLEAR_LOGS.clear_limitarch'
                         ,p_msg => 'CLEAR_LOGS.Очистка архива ' || vt_arch_table(n) || ' за ' || to_char(v_date_last, 'dd.mm.yyyy') || '(' || v_reccountall || 'зап.)');
      end if;
      if sysdate > dt_start + numtodsinterval(4, 'HOUR')
      then
        it_log.log_handle(p_object => 'USR_CLEAR_LOGS.clear_limitarch', p_msg => 'CLEAR_LOGS.Прерывание работы по времени выполнения ( > 4ч.)');
        exit;
      end if;
      v_date_last := v_date_last + 1;
    end loop;
  end loop;
end; ]');
end;
/
