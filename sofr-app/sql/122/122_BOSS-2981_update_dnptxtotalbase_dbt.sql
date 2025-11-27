BEGIN
  UPDATE dnptxtotalbase_dbt total
     SET total.t_dlcontrid = (select op.t_contract from dnptxop_dbt op where op.t_id = total.t_docid and op.t_dockind = total.t_dockind)
   WHERE total.t_taxbasekind = 5 and total.t_taxperiod = 2025;
END;
/