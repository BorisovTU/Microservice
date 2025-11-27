CREATE GLOBAL TEMPORARY TABLE dtaxecalcfunction_tmp
(
    t_sys number(5),
    t_taxbasekind number(10),
    t_specialtag varchar(16),
    t_taxbase  number(32,12),
    t_rate  number(5),
    t_codekbk  varchar(20),
    t_taxcalc number(32,12),
    t_taxhold number(32,12)	
)
ON COMMIT PRESERVE ROWS
NOCACHE
/