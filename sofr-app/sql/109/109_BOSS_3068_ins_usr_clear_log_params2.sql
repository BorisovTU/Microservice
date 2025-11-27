begin
  delete from USR_CLEAR_LOG_PARAMS t where t.t_table_name in ('ITT_LOG', 'DCALCLIMITLOG_DBT', 'DDL_LIMIT_RCH_DBT', 'DDL_LIMIT_ARCH_DBT');
  insert into USR_CLEAR_LOG_PARAMS
    (T_TABLE_NAME
    ,T_QUERY)
  values
    ('ITT_LOG'
    , 'DECLARE 
  max_id INTEGER := 0; 
  min_id INTEGER := 0; 
BEGIN 
  SELECT max(id_log), min(id_log) INTO max_id, min_id FROM itt_log WHERE CREATE_SYSDATE < ADD_MONTHS(trunc(sysdate),-6); 
  WHILE max_id >= min_id 
  LOOP 
    DELETE FROM itt_log l WHERE l.id_log BETWEEN (max_id - 100000) AND max_id; 
    COMMIT; 
    max_id := max_id - 100000; 
  END LOOP; 
END;');
  insert into USR_CLEAR_LOG_PARAMS
    (T_TABLE_NAME
    ,T_QUERY)
  values
    ('DCALCLIMITLOG_DBT'
    , 'DECLARE 
  max_dtend date ; 
  min_dtend date ; 
BEGIN 
  SELECT max(t_end),min(t_end) INTO max_dtend, min_dtend FROM dcalclimitlog_dbt WHERE t_end < ADD_MONTHS(trunc(sysdate),-6) ; 
  WHILE max_dtend >= min_dtend 
  LOOP 
    DELETE FROM dcalclimitlog_dbt l WHERE  t_end  BETWEEN (max_dtend - NUMTODSINTERVAL(5, ''DAY'')) AND max_dtend; 
    COMMIT; 
    max_dtend := max_dtend - NUMTODSINTERVAL(5, ''DAY''); 
  END LOOP; 
END;');
  insert into USR_CLEAR_LOG_PARAMS
    (T_TABLE_NAME
    ,T_QUERY)
  values
    ('DDL_LIMIT_ARCH_DBT'
    , 'DECLARE 
  dt_start date := sysdate; 
  type tt_arch_table is table of varchar2(128) ;
  vt_arch_table tt_arch_table :=tt_arch_table(''DDL_LIMITCASHSTOCKARCH_DBT'', ''DDL_LIMITSECURITESARCH_DBT'', ''DDL_limitfuturmarkarch_DBT'');
  v_date_last date ;
  v_date_end  date ;
BEGIN 
  for n in vt_arch_table.first .. vt_arch_table.last 
  loop
    execute immediate ''SELECT min(T_DATE), max(T_DATE) FROM ''||vt_arch_table(n) ||'' WHERE  T_DATE < ADD_MONTHS(trunc(sysdate),-12)''
    into v_date_last, v_date_end ; 
    while nvl(v_date_last,sysdate+1)  <= nvl(v_date_end,sysdate)
    loop
       execute immediate ''delete  FROM ''||vt_arch_table(n)||'' WHERE  T_DATE = :dt'' using v_date_last ;
       if sql%rowcount > 0 then
         it_log.log_handle(p_object => ''USR_CLEAR_LOGS.clear_limitarch'',p_msg => ''CLEAR_LOGS.Очистка архива ''||vt_arch_table(n)||'' за ''||to_char(v_date_last,''dd.mm.yyyy'')||''(''||sql%rowcount||''зап.)'');
         commit;
       end if;
       if sysdate > dt_start + numtodsinterval(2,''HOUR'') then
         it_log.log_handle(p_object => ''USR_CLEAR_LOGS.clear_limitarch'',p_msg => ''CLEAR_LOGS.Прерывание работы по времени выполнения ( > 2ч.)'');
         exit;
       end if;
      v_date_last := v_date_last + 1;
    end loop;
  end loop ;
END;');
  commit;
end;
/
