declare

  procedure create_sequence (
     p_name   varchar2
    ,p_start  number default 1
    ,p_force  number default 1
  ) is
    l_log_object varchar2(100) := 'create_sequence.' || p_name;
    l_cnt        number(9);
    l_name       varchar2(100) := upper(p_name);
  begin
    SELECT count(*) INTO l_cnt FROM user_objects r WHERE r.OBJECT_NAME = l_name and r.OBJECT_TYPE = 'SEQUENCE';
    
    if (l_cnt = 1) then
      it_log.log(p_msg => l_log_object || '. Already exists');
      
      if p_force = 1 then
        execute immediate 'drop sequence ' || l_name;
        it_log.log(p_msg => l_log_object || '. dropped');
      else
        return;
      end if;
    end if;

    execute immediate 'create sequence ' || l_name
       ||' start with '||to_char(p_start)
       ||' maxvalue 999999999999999999999999999 MINVALUE 1 NOCYCLE NOCACHE NOORDER';
       
    it_log.log(p_msg => l_log_object || '. created');
  exception
   when others then
     it_log.log(p_msg => l_log_object || '. ' || sqlerrm);
     raise;
  end create_sequence;
begin
  create_sequence(p_name => 'dl_tick_internal_code_sq', p_force => 0); 
end;
/
