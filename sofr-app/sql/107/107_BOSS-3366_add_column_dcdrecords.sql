DECLARE
   e_exist_field EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_exist_field, -1430);
BEGIN
   begin
        EXECUTE IMMEDIATE 'alter table dcdrecords_dbt add T_PAYRECEIVEDDATE date';
   EXCEPTION WHEN e_exist_field THEN NULL;
   end;

   begin
        EXECUTE IMMEDIATE 'alter table dcdrecords_dbt add T_FIXINGDATE date';
   EXCEPTION WHEN e_exist_field THEN NULL;
   end;
   
   begin
        EXECUTE IMMEDIATE 'alter table dcdrecords_dbt add T_QUANTITY number(32,12)';
   EXCEPTION WHEN e_exist_field THEN NULL;
   end;

   EXECUTE IMMEDIATE 'COMMENT ON COLUMN dcdrecords_dbt.T_PAYRECEIVEDDATE IS ''Дата поступления на корр.счет платежа от эмитента''';
   EXECUTE IMMEDIATE 'COMMENT ON COLUMN dcdrecords_dbt.T_FIXINGDATE IS '' Дата фиксации списка владельцев''';
   EXECUTE IMMEDIATE 'COMMENT ON COLUMN dcdrecords_dbt.T_QUANTITY IS ''Количество ц/б''';

end;