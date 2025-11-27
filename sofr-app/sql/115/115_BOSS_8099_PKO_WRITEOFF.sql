declare
  cnt number;
begin
  update PKO_WriteOff w
     set w.islimitcorrected = nvl(w.islimitcorrected, chr(0))
        ,w.iscanceled       = nvl(w.iscanceled, chr(0))
        ,w.iscompleted      = nvl(w.iscompleted, chr(0));
  begin
    execute immediate 'alter table PKO_WRITEOFF modify islimitcorrected default chr(0) not null';
    execute immediate 'alter table PKO_WRITEOFF modify iscanceled default chr(0) not null';
    execute immediate 'alter table PKO_WRITEOFF modify iscompleted default chr(0) not null';
  exception
    when others then
      null;
  end;
  select count(*) into cnt from user_indexes i where i.INDEX_NAME = 'PKOWRITEOFF_IDX0';
  if cnt = 0
  then
    execute immediate 'create unique index PKOWRITEOFF_IDX0 on PKO_WRITEOFF (id)   tablespace indx';
  end if;
  select count(*) into cnt from user_indexes i where i.INDEX_NAME = 'PKOWRITEOFF_IDX1';
  if cnt = 0
  then
    execute immediate 'create index PKOWRITEOFF_IDX1 on PKO_WRITEOFF (startwriteoffdate)   tablespace indx';
  end if;
  select count(*) into cnt from user_indexes i where i.INDEX_NAME = 'PKOWRITEOFF_IDX2';
  if cnt = 0
  then
    execute immediate 'create index PKOWRITEOFF_IDX2 on PKO_WRITEOFF (expirationdate)   tablespace indx';
  end if;
  select count(*) into cnt from user_indexes i where i.INDEX_NAME = 'PKOWRITEOFF_IDX3';
  if cnt = 0
  then
    execute immediate 'create index PKOWRITEOFF_IDX3 on PKO_WRITEOFF (clientid, securityid)  tablespace indx ';
  end if;
end;
