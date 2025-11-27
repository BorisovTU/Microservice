declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME= 'DMCACCDOC_IDX_CHAP_CMN_CAT' ;
  if cnt =1 then
     execute immediate 'drop index DMCACCDOC_IDX_CHAP_CMN_CAT';
  end if;
  execute immediate 'CREATE INDEX DMCACCDOC_IDX_CHAP_CMN_CAT ON DMCACCDOC_DBT (T_CHAPTER, T_ISCOMMON, T_CATID) COMPRESS 2';
end;
/
