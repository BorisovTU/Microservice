-- Старт QManager
declare
  v_result number;
  o_result varchar2(32000);
begin
  begin
    execute immediate 'begin upgrader.set_alter_system_last; end;' ;
  exception
    when others then
      it_log.log(p_msg => 'UPGRADER.SET_ALTER_SYSTEM_LAST: ERROR', p_msg_clob => sqlerrm);
  end;
  begin
    execute immediate 'begin :res := it_q_manager.startmanager(o_info => :res_txt); end ;'
      using out v_result, out o_result;
    it_log.log(p_msg => 'START IT_Q_MANAGER: ' || case
                          when v_result = 1 then
                           'OK'
                          else
                           'ERROR'
                        end
              ,p_msg_clob => o_result);
  exception
    when others then
      it_log.log(p_msg => 'START IT_Q_MANAGER: ERROR', p_msg_clob => sqlerrm);
  end;
end;
