DECLARE
BEGIN
   FOR i
      IN (  SELECT lnk.T_BCCFI,
                   lnk.t_contractid,
                   SUM (lnk.T_TAXBASEAMOUNT) AS t_taxbase,
                   txobj.T_OBJID,
                   RSI_RSB_FIInstr.ConvSum (SUM (lnk.T_TAXBASEAMOUNT),
                                            lnk.T_BCCFI,
                                            0,
                                            txobj.t_date,
                                            1)
                      t_convsum
              FROM DVSORDLNK_DBT lnk, dnptxobj_dbt txobj
             WHERE     lnk.T_LINKKIND = 1
                   AND lnk.T_BCCFI != 0
                   AND txobj.T_ANALITICKIND1 IN (1115, 1130, 1120)
                   AND txobj.T_ANALITIC1 = lnk.t_contractid
                   AND txobj.t_kind IN (1142, 1141, 1140)
                   AND LNK.T_INTERESTCHARGEDATE >=
                          TO_DATE ('01.01.2023', 'DD.MM.YYYY')
                   AND lnk.T_BCCFI <> TXOBJ.T_CUR
                   AND TXOBJ.t_cur = 0
                   AND lnk.T_TAXBASEAMOUNT = txobj.T_SUM
                   AND txobj.T_DIRECTION = 1
          GROUP BY lnk.t_contractid,
                   lnk.T_BCCFI,
                   txobj.T_OBJID,
                   txobj.t_date)
   LOOP
      UPDATE dnptxobj_dbt
         SET t_sum = i.t_convsum, t_sum0 = i.t_convsum
       WHERE t_objid = i.t_objid;
   END LOOP;
END;
/
