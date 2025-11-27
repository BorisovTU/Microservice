-- добавление полей к таблицам
BEGIN
  EXECUTE IMMEDIATE 'ALTER TABLE ddlcontrmp_dbt ADD t_rcodetks varchar2(64) DEFAULT CHR(1)';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN ddlcontrmp_dbt.t_rcodetks IS ''Расчетный код ТКС в НКЦ''';
EXCEPTION 
  WHEN OTHERS THEN NULL;
END;
