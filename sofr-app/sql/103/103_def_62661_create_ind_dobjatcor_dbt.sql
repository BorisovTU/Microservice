declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME= 'DOBJATCOR_DBT_USR1' ;
  if cnt =0 then
     execute immediate 'create index DOBJATCOR_DBT_USR1 on dobjatcor_dbt (t_objecttype, t_groupid, t_validfromdate, t_validtodate) tablespace INDX';
  end if;
end;
/
