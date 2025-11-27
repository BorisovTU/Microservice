declare
  n integer;
begin
  select count(1)
    into n
    from user_triggers t
   where t.TRIGGER_NAME = 'DDL_LIMITSECURARCH_DBT_T0_AINC';
  if n > 0 then
    execute immediate 'drop trigger DDL_LIMITSECURARCH_DBT_T0_AINC';
  end if;
  select count(1)
    into n
    from user_triggers t
   where t.TRIGGER_NAME = 'DDL_LIMITCASHARCH_DBT_T0_AINC';
  if n > 0 then
    execute immediate 'drop trigger DDL_LIMITCASHARCH_DBT_T0_AINC';
  end if;
  select count(1)
    into n
    from user_triggers t
   where t.TRIGGER_NAME = 'DDL_LIMITFUTURMARKARCH_DBT_T0_AINC';
  if n > 0 then
    execute immediate 'drop trigger DDL_LIMITFUTURMARKARCH_DBT_T0_AINC';
  end if;
  select count(1)
    into n
    from user_sequences t
   where t.SEQUENCE_NAME = 'DDL_LIMITSECURITESARCH_DBT_SEQ';
  if n > 0 then
    execute immediate 'drop sequence DDL_LIMITSECURITESARCH_DBT_SEQ';
  end if;
  select count(1)
    into n
    from user_sequences t
   where t.SEQUENCE_NAME = 'DDL_LIMITCASHSTOCKARCH_DBT_SEQ';
    if n > 0 then
    execute immediate 'drop sequence DDL_LIMITCASHSTOCKARCH_DBT_SEQ';
  end if;
  select count(1)
    into n
    from user_sequences t
   where t.SEQUENCE_NAME = 'DDL_LIMITFUTURMARKARCH_DBT_SEQ';
    if n > 0 then
    execute immediate 'drop sequence DDL_LIMITFUTURMARKARCH_DBT_SEQ';
  end if;
end;
