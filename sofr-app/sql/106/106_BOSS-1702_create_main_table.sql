declare
    vcnt number;
    vsql varchar2(2000);
begin

   select count(*) into vcnt from user_tables where table_name = upper('UNPTXOP_PAYMENT_LINK_DBT');
   if vcnt = 0 then
      execute immediate  'create table UNPTXOP_PAYMENT_LINK_DBT (
                                       T_NPTXOPID     NUMBER(10),
                                       T_PAYMENTID   NUMBER(10),
                                       T_ACCTRNID     NUMBER(10),
                                       T_EVENTID       NUMBER(10),
                                       T_RESPONSEID NUMBER(10),
                                       T_ERRORCODE  NUMBER(10),
                                       T_ERRORTEXT  VARCHAR2(2000)
                                     )';
      execute immediate 'comment on table UNPTXOP_PAYMENT_LINK_DBT is ''Связь операции зачисления и платежа подкрепления''';
      execute immediate 'COMMENT ON COLUMN UNPTXOP_PAYMENT_LINK_DBT.T_NPTXOPID IS ''ID операции зачисления''';
      execute immediate 'COMMENT ON COLUMN UNPTXOP_PAYMENT_LINK_DBT.T_PAYMENTID IS ''ID платежа подкрепления''';
      execute immediate 'COMMENT ON COLUMN UNPTXOP_PAYMENT_LINK_DBT.T_ACCTRNID IS ''ID проводки платежа''';
      execute immediate 'COMMENT ON COLUMN UNPTXOP_PAYMENT_LINK_DBT.T_EVENTID IS ''utableprocessevent_dbt.t_reсid выгрузки платежа''';
      execute immediate 'COMMENT ON COLUMN UNPTXOP_PAYMENT_LINK_DBT.T_RESPONSEID IS ''usr_exchng_log_dbt.t_id ответа на выгрузку''';
      execute immediate 'COMMENT ON COLUMN UNPTXOP_PAYMENT_LINK_DBT.T_ERRORCODE IS ''Код ошибки''';
      execute immediate 'COMMENT ON COLUMN UNPTXOP_PAYMENT_LINK_DBT.T_ERRORTEXT IS ''Текст ошибки''';

      execute immediate 'CREATE UNIQUE INDEX UNPTXOP_PAYMENT_LINK_IDX0 ON UNPTXOP_PAYMENT_LINK_DBT (T_NPTXOPID)';
      execute immediate 'CREATE INDEX UNPTXOP_PAYMENT_LINK_IDX1 ON UNPTXOP_PAYMENT_LINK_DBT (T_PAYMENTID)';
      execute immediate 'CREATE INDEX UNPTXOP_PAYMENT_LINK_IDX2 ON UNPTXOP_PAYMENT_LINK_DBT (T_EVENTID)';
      execute immediate 'CREATE INDEX UNPTXOP_PAYMENT_LINK_IDX3 ON UNPTXOP_PAYMENT_LINK_DBT (t_responseid)';
      
   end if;
   
end;    
