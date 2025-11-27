create or replace procedure USR_CLEAR_LOGS is
  V_QUERY clob;
  v_pref varchar2(100):='CLEAR_LOGS: ';
begin
  it_log.log(p_msg => v_pref||'‘’€’');
  for TABLES in (select * from USR_CLEAR_LOG_PARAMS)
  loop
    it_log.log(p_msg => v_pref||' η «® ®η¨αβª¨ ' || TABLES.T_TABLE_NAME);
    V_QUERY := TABLES.T_QUERY;
    begin
      execute immediate V_QUERY;
    exception
      when others then
        it_log.log(p_msg => v_pref||'Error#' || sqlcode || ':' || sqlerrm, p_msg_type => it_log.C_MSG_TYPE__ERROR, p_msg_clob => V_QUERY);
    end;
    update USR_CLEAR_LOG_PARAMS set T_LAST_CLEARING = sysdate where T_ID = TABLES.T_ID;
    commit;
    it_log.log(p_msg => v_pref||'ª®­η ­¨¥ ®η¨αβª¨ ' || TABLES.T_TABLE_NAME);
  end loop;
  it_log.log(p_msg => v_pref||'”');
exception
  when others then
    it_log.log(p_msg => v_pref||'Error#' || sqlcode || ':' || sqlerrm, p_msg_type => it_log.C_MSG_TYPE__ERROR);
end;
/
