declare
n number;
t_name varchar2(100):='itt_rcb_portf_7ep_mgc';
begin
  select count(*) into n from user_tables  t where t.TABLE_NAME =  upper(t_name) ;
  if n > 0 then
   execute immediate 'drop table '||t_name;
  end if;
exception
  when no_data_found then
    null;
end;
/
