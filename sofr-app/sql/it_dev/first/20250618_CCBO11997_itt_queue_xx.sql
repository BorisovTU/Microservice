declare
  n      number;
  q      number;
  t_name varchar2(100);
  i_name varchar2(100);
begin
  for q in 1 .. 2
  loop
    if q = 1
    then
      t_name := 'ITT_QUEUE_IN_XX';
      i_name := 'ITI_QUEUE_IN_XX_PK';
    else
      t_name := 'ITT_QUEUE_OUT_XX';
      i_name := 'ITI_QUEUE_OUT_XX_PK';
    end if;
    select count(*) into n from user_tables t where t.table_name = upper(t_name);
    if n > 0
    then
      execute immediate 'drop table ' || t_name;
    end if;
    execute immediate 'create table ' || t_name || '(qmsgid raw(16) default sys_guid() not null
                          , qenqdt timestamp default systimestamp not null
                          , log_id number not null
                          , state number default 0 not null
                          , statedt timestamp default systimestamp not null
                          , delay timestamp default systimestamp not null
                          , expiration number default 0 not null
                          , correlation varchar2(128) not null 
                          , msgid VARCHAR2(128) not null
                          , corrmsgid VARCHAR2(128)
                          , message_type CHAR(1) not null
                          , delivery_type CHAR(1) not null
                          , priority CHAR(1) not null
                          , servicename   VARCHAR2(128)
                          , servicegroup  VARCHAR2(128)
                          , sender VARCHAR2(128)
                          , senderuser VARCHAR2(128)
                          , receiver VARCHAR2(128)
                          , txtmessbody VARCHAR2(128)
                           )';
    execute immediate 'comment on table ' || t_name || ' is ''Табличная очередь ''';
    execute immediate 'create unique index '||i_name||' on '||t_name||'(log_id) reverse tablespace INDX';

  end loop;
end;
/
