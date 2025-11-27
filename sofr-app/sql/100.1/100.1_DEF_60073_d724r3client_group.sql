declare
  n      number;
  t_name varchar2(100) := 'D724R3CLIENT_GROUP';
begin
  select count(*) into n from user_tables t where t.table_name = upper(t_name);
  if n = 0
  then
    execute immediate ' create table D724R3CLIENT_GROUP
(
  t_sessionid         NUMBER ,
  t_client_groupid    VARCHAR2(100),
  t_type_fl           NUMBER(5),
  t_ki                NUMBER(5),
  t_activeclient      NUMBER(5),
  t_code_oksm         VARCHAR2(3),
  t_code_okato        VARCHAR2(5),
  countclient         NUMBER,
  countclientiis      NUMBER,
  totalcostfi         NUMBER,
  totalrestacc        NUMBER,
  totalcostfiiis      NUMBER,
  totalrestacciis     NUMBER,
  countclientbegcontr NUMBER,
  countclientendcontr NUMBER,
  totalcostpfi        NUMBER,
  totalsumtr          NUMBER,
  totalsumob          NUMBER,
  totalcostpfiiis     NUMBER,
  totalsumtriis       NUMBER,
  totalsumobiis       NUMBER
)';
   execute immediate 'create index D724R3CLIENT_GROUP_IDX1 on D724R3CLIENT_GROUP (t_sessionid, t_client_groupid)  tablespace INDX ';
 end if;
end;
