/*Обновление DDLSUM_DBT*/
DECLARE
BEGIN

UPDATE ddlsum_dbt l
   SET l.T_NOTCOUNTEDONIIS = 'X'
 WHERE l.t_DocKind = 127
   AND l.t_Kind = 1230
   AND l.T_NOTCOUNTEDONIIS = CHR(0)
   AND EXISTS(SELECT 1
                FROM dnptxlot_dbt LB2
               WHERE LB2.T_DOCKIND = l.t_DocKind
                 AND LB2.T_DOCID = l.t_docid
                 AND lb2.t_kind = 1
                 AND EXISTS(SELECT 1         
                              FROM ddlcontrmp_dbt mp, ddlcontr_dbt dl       
                             WHERE mp.t_SfContrID = LB2.T_CONTRACT         
                               AND dl.t_DlContrID = mp.t_DlContrID         
                               AND dl.t_IIS = CHR(0))
                 AND NOT EXISTS
                                 (SELECT 1
                                    FROM dnptxlnk_dbt lnk
                                   WHERE LNK.T_BUYID = lb2.t_id)
                 AND COALESCE (
                             (SELECT T_ATTRID
                                FROM DOBJATCOR_DBT
                               WHERE T_OBJECTTYPE = 101 AND T_GROUPID = 111
                                     AND T_OBJECT =
                                            LPAD (LB2.T_DOCID,
                                                  34,
                                                  '0')),
                             0) != 2
             );

UPDATE ddlsum_dbt l
   SET l.T_NOTCOUNTEDONIIS = 'X'
 WHERE l.t_DocKind = 127
   AND l.t_Kind = 1230
   AND l.T_NOTCOUNTEDONIIS = CHR(0)
   AND EXISTS(SELECT 1
                FROM dnptxlot_dbt LB2
               WHERE LB2.T_DOCKIND = l.t_DocKind
                 AND LB2.T_DOCID = l.t_docid
                 AND lb2.t_kind = 1
                 AND EXISTS(SELECT 1         
                              FROM ddlcontrmp_dbt mp, ddlcontr_dbt dl       
                             WHERE mp.t_SfContrID = LB2.T_CONTRACT         
                               AND dl.t_DlContrID = mp.t_DlContrID         
                               AND dl.t_IIS = CHR(0))
                 AND EXISTS
                             (SELECT 1
                                FROM dnptxlnk_dbt lnk
                               WHERE LNK.T_BUYID = lb2.t_id
                                 AND LNK.T_DATE >= TO_DATE('01.01.2025', 'DD.MM.YYYY') 
                             )
                 AND COALESCE (
                             (SELECT T_ATTRID
                                FROM DOBJATCOR_DBT
                               WHERE T_OBJECTTYPE = 101 AND T_GROUPID = 111
                                     AND T_OBJECT =
                                            LPAD (LB2.T_DOCID,
                                                  34,
                                                  '0')),
                             0) != 2
             );

 
END;
/