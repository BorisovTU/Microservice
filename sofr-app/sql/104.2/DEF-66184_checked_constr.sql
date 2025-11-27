DECLARE
BEGIN
   UPDATE dnptxsnobvertb_dbt
      SET T_CHECKED = CHR (0)
    WHERE T_CHECKED IS NULL;

   execute immediate 'ALTER TABLE dnptxsnobvertb_dbt MODIFY T_CHECKED NOT NULL';
END;
/