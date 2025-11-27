-- добавление полей к таблицам
BEGIN
  EXECUTE IMMEDIATE '
  ALTER TABLE davoiriss_dbt 
          ADD t_CouponRefuseRight CHAR(1) DEFAULT CHR(0)';

  EXECUTE IMMEDIATE '
  COMMENT ON COLUMN davoiriss_dbt.t_CouponRefuseRight IS ''Право отказа от выплаты купона''';
EXCEPTION 
  WHEN OTHERS THEN NULL;
END;
/

BEGIN
  EXECUTE IMMEDIATE '
  ALTER TABLE dfiwarnts_dbt 
          ADD t_PaymentRefuse CHAR(1) DEFAULT CHR(0)';

  EXECUTE IMMEDIATE '
  COMMENT ON COLUMN dfiwarnts_dbt.t_PaymentRefuse IS ''Отказ от выплаты''';
EXCEPTION 
  WHEN OTHERS THEN NULL;
END;
/

BEGIN
  EXECUTE IMMEDIATE 'COMMIT';
EXCEPTION 
  WHEN OTHERS THEN NULL;
END;
/