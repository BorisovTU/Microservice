create or replace package lock_utils is

  function set_lock
  (
    p_lockname          varchar2
   ,p_release_on_commit boolean := false
   ,p_exclusive         boolean := false
   ,p_timeout           integer := 0
  ) return boolean;

  procedure set_lock_must
  (
    p_lockname          varchar2
   ,p_release_on_commit boolean := false
   ,p_exclusive         boolean := false
   ,p_timeout           integer := 0
  );

  procedure release_lock(p_lockname varchar2);
end lock_utils;
/
