declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME= 'DNPTXLOT_DBT_IDX5' ;
  if cnt =1 then
     execute immediate 'drop index DNPTXLOT_DBT_IDX5';
  end if;
  execute immediate 'create index DNPTXLOT_DBT_IDX5 on dnptxlot_dbt (t_buydate) tablespace INDX';

  select count(*) into cnt from user_indexes i where i.INDEX_NAME= 'DNPTXLOT_DBT_IDX6' ;
  if cnt =1 then
     execute immediate 'drop index DNPTXLOT_DBT_IDX6';
  end if;
  execute immediate 'create index DNPTXLOT_DBT_IDX6 on DNPTXLOT_DBT (t_saledate) tablespace INDX';

  select count(*) into cnt from user_indexes i where i.INDEX_NAME= 'DNPTXLOT_DBT_USR1' ;
  if cnt =0 then
     execute immediate 'create index DNPTXLOT_DBT_usr1 on DNPTXLOT_DBT (t_client, t_buydate) tablespace INDX';
  end if;

  select count(*) into cnt from user_indexes i where i.INDEX_NAME= 'DNPTXLOT_DBT_USR2' ;
  if cnt =0 then
     execute immediate 'create index DNPTXLOT_DBT_usr2 on DNPTXLOT_DBT (t_client, t_saledate) tablespace INDX';
  end if;

  select count(*) into cnt from user_indexes i where i.INDEX_NAME= 'DDL_LIMITADJUST_DBT_IDX2' ;
  if cnt =0 then
     execute immediate 'create index DDL_LIMITADJUST_DBT_IDX2 on DDL_LIMITADJUST_DBT (t_id_oper) tablespace INDX';
  end if;

end;
/
