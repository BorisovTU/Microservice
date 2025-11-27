create or replace package body lock_utils is
  
  gc_userenv_current_schema constant varchar(100) := sys_context('userenv', 'current_schema');



  function Getlockname( p_lockname varchar2) return varchar2
    as
  begin
      return gc_userenv_current_schema||'*'||p_lockname ; -- Для исключения блокировок между инстансами '#' - используется в QManager
  end;

  function set_lock
  (
    p_lockname          varchar2
   ,p_release_on_commit boolean := false
   ,p_exclusive         boolean := false
   ,p_timeout           integer := 0
  ) return boolean is
    l_lockhandle varchar2(128);
    l_result     number;
  begin
    dbms_lock.allocate_unique_autonomous(lockname   => Getlockname(p_lockname)
                                        ,lockhandle => l_lockhandle);
    l_result := dbms_lock.request(lockhandle        => l_lockhandle
                                 ,lockmode          => case
                                                         when not (p_exclusive) then
                                                          dbms_lock.s_mode
                                                         else
                                                          dbms_lock.x_mode
                                                       end
                                 ,timeout           => p_timeout
                                 ,release_on_commit => p_release_on_commit);
  
    return l_result in(0
                      ,4);
  end set_lock;

  procedure set_lock_must
  (
    p_lockname          varchar2
   ,p_release_on_commit boolean := false
   ,p_exclusive         boolean := false
   ,p_timeout           integer := 0
  ) is
  begin
    if not set_lock(p_lockname          => p_lockname
                   ,p_release_on_commit => p_release_on_commit
                   ,p_exclusive         => p_exclusive
                   ,p_timeout           => p_timeout) then
      raise_application_error(-20000
                             ,'Could not lock ' || p_lockname || ' in ' || case when
                              p_exclusive then 'exclusive' else 'shared'
                              end || ' mode.');
    end if;
  end set_lock_must;

  procedure release_lock(p_lockname varchar2) is
    l_lockhandle varchar2(128);
    l_result     number;
  begin
    dbms_lock.allocate_unique_autonomous(lockname   => Getlockname(p_lockname)
                                        ,lockhandle => l_lockhandle);
    l_result := dbms_lock.release(lockhandle => l_lockhandle);
  end release_lock;
end lock_utils;
/
