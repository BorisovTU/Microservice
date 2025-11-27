BEGIN
DELETE FROM DREGVAL_DBT
      WHERE t_keyid IN
               (SELECT prm.T_KEYID
                  FROM (SELECT 'ACCOPFL' t_fltName FROM DUAL
                        UNION ALL
                        SELECT 'CLNLOTFL' t_fltName FROM DUAL
                        UNION ALL
                        SELECT 'FSFOPER' t_fltName FROM DUAL
                        UNION ALL
                        SELECT 'INACOPFL' t_fltName FROM DUAL
                        UNION ALL
                        SELECT 'IOPOOLFL' t_fltName FROM DUAL
                        UNION ALL
                        SELECT 'PSVOFCOM' t_fltName FROM DUAL
                        UNION ALL
                        SELECT 'TICKFLT' t_fltName FROM DUAL
                        UNION ALL
                        SELECT 'SFDEFFLT' t_fltName FROM DUAL
                        UNION ALL
                        SELECT 'SFINVFLT' t_fltName FROM DUAL
                        UNION ALL
                        SELECT 'SVACCFLT' t_fltName FROM DUAL
                        UNION ALL
                        SELECT 'CLMFLTR' t_fltName FROM DUAL
                        UNION ALL
                        SELECT 'EXCODEFL' t_fltName FROM DUAL
                        UNION ALL
                        SELECT 'FLTRREQ' t_fltName FROM DUAL
                        UNION ALL
                        SELECT 'MMCRDLNF' t_fltName FROM DUAL
                        UNION ALL
                        SELECT 'MMORDF' t_fltName FROM DUAL
                        UNION ALL
                        SELECT 'NTGDLF' t_fltName FROM DUAL
                        UNION ALL
                        SELECT 'NTGRQF' t_fltName FROM DUAL
                        UNION ALL
                        SELECT 'SETTFLT' t_fltName FROM DUAL
                        UNION ALL
                        SELECT 'DVDLFLTR' t_fltName FROM DUAL
                        UNION ALL
                        SELECT 'DVOPFLTR' t_fltName FROM DUAL
                        UNION ALL
                        SELECT 'FFXDEAL' t_fltName FROM DUAL
                        UNION ALL
                        SELECT 'FLTR_POS' t_fltName FROM DUAL
                        UNION ALL
                        SELECT 'FOUTDEAL' t_fltName FROM DUAL
                        UNION ALL
                        SELECT 'NPTXDFLT' t_fltName FROM DUAL
                        UNION ALL
                        SELECT 'NUTXTFLT' t_fltName FROM DUAL
                        UNION ALL
                        SELECT 'NUTXLFLT' t_fltName FROM DUAL
                        UNION ALL
                        SELECT 'NUTXDFLT' t_fltName FROM DUAL
                        UNION ALL
                        SELECT 'NPTXTFLT' t_fltName FROM DUAL
                        UNION ALL
                        SELECT 'NPTXLFLT' t_fltName FROM DUAL) allFlt,
                       DREGPARM_DBT prm
                 WHERE T_NAME LIKE '%\_\_' || allFlt.t_fltName ESCAPE '\');
END;
/