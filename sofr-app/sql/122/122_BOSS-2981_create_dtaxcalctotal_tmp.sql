CREATE GLOBAL TEMPORARY TABLE dtaxcalctotal_tmp
(
   t_taxholdbrokdepo   number (32, 12),
   t_taxholdbroksofr   number (32, 12),
   t_taxholdbrok       number (32, 12),
   t_taxholdmaterial   number (32, 12),
   t_taxholdbill       number (32, 12),
   t_taxholdonb        number (32, 12),
   t_codekbk           varchar (20),
   t_rate              number(10)	
)
ON COMMIT PRESERVE ROWS
NOCACHE
/