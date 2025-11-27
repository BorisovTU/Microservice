BEGIN
  UPDATE dnptxtotalbase_dbt total
     SET total.t_dlcontrid = (select mp.t_DlContrID from ddlcontrmp_dbt mp where mp.t_SfContrID = (select op.t_contract from dnptxop_dbt op where op.t_id = total.t_docid and op.t_dockind = total.t_dockind))
   WHERE total.t_taxbasekind = 5 and total.t_taxperiod = 2025 and total.t_dockind = 4607;
END;
/