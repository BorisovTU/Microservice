/*Обновление DNPTXLOT_DBT*/
DECLARE
BEGIN

UPDATE DNPTXLOT_DBT l   
   SET l.T_NOTCOUNTEDONIIS = 'X' 
 WHERE l.T_DOCKIND = 127   
   AND l.T_NOTCOUNTEDONIIS = CHR(0)   
   AND l.t_kind = 1
   AND EXISTS(SELECT 1                                                                     
                FROM ddlcontrmp_dbt mp, ddlcontr_dbt dl                                                                  
               WHERE mp.t_SfContrID = l.T_CONTRACT                                                                      
                 AND dl.t_DlContrID = mp.t_DlContrID                                                                      
                 AND dl.t_IIS = CHR(0))   
   AND NOT EXISTS (SELECT 1         
                     FROM dnptxlnk_dbt lnk         
                    WHERE LNK.T_BUYID = l.t_id     )    
   AND EXISTS(SELECT 1 
                FROM ddlsum_dbt                                                                             
               WHERE t_dockind = l.T_DOCKIND                                                                               
                 AND t_docid = l.T_DOCID                                                                               
                 AND t_kind = 1230                                                                               
                 and T_NOTCOUNTEDONIIS = 'X'                                                                               
              );
              
UPDATE DNPTXLOT_DBT l   
   SET l.T_NOTCOUNTEDONIIS = 'X'  
 WHERE l.T_DOCKIND = 138   
   AND l.T_NOTCOUNTEDONIIS = CHR(0)   
   AND l.t_kind = 1
   AND EXISTS(SELECT 1                                                                     
                FROM ddlcontrmp_dbt mp, ddlcontr_dbt dl                                                                  
               WHERE mp.t_SfContrID = l.T_CONTRACT                                                                      
                 AND dl.t_DlContrID = mp.t_DlContrID                                                                      
                 AND dl.t_IIS = CHR(0))   
   AND NOT EXISTS (SELECT 1         
                     FROM dnptxlnk_dbt lnk         
                    WHERE LNK.T_BUYID = l.t_id);     
                    
UPDATE DNPTXLOT_DBT l   
   SET l.T_NOTCOUNTEDONIIS = 'X' 
 WHERE l.T_DOCKIND = 101   
   AND l.T_NOTCOUNTEDONIIS = CHR(0)   
   AND l.t_kind = 1
   AND EXISTS(SELECT 1                                                                     
                FROM ddlcontrmp_dbt mp, ddlcontr_dbt dl                                                                  
               WHERE mp.t_SfContrID = l.T_CONTRACT                                                                      
                 AND dl.t_DlContrID = mp.t_DlContrID                                                                      
                 AND dl.t_IIS = CHR(0))   
   AND NOT EXISTS (SELECT 1         
                     FROM dnptxlnk_dbt lnk         
                    WHERE LNK.T_BUYID = l.t_id);

UPDATE DNPTXLOT_DBT l   
   SET l.T_NOTCOUNTEDONIIS = 'X' 
 WHERE l.T_DOCKIND = 127   
   AND l.T_NOTCOUNTEDONIIS = CHR(0)   
   AND l.t_kind = 1
   AND EXISTS(SELECT 1                                                                     
                FROM ddlcontrmp_dbt mp, ddlcontr_dbt dl                                                                  
               WHERE mp.t_SfContrID = l.T_CONTRACT                                                                      
                 AND dl.t_DlContrID = mp.t_DlContrID                                                                      
                 AND dl.t_IIS = CHR(0))   
   AND EXISTS (SELECT 1         
                 FROM dnptxlnk_dbt lnk         
                WHERE LNK.T_BUYID = l.t_id     
                  AND LNK.T_DATE >= TO_DATE('01.01.2025', 'DD.MM.YYYY'))    
   AND EXISTS(SELECT 1 
                FROM ddlsum_dbt                                                                             
               WHERE t_dockind = l.T_DOCKIND                                                                               
                 AND t_docid = l.T_DOCID                                                                               
                 AND t_kind = 1230                                                                               
                 and T_NOTCOUNTEDONIIS = 'X'                                                                               
              );
              
UPDATE DNPTXLOT_DBT l   
   SET l.T_NOTCOUNTEDONIIS = 'X'  
 WHERE l.T_DOCKIND = 138   
   AND l.T_NOTCOUNTEDONIIS = CHR(0)   
   AND l.t_kind = 1
   AND EXISTS(SELECT 1                                                                     
                FROM ddlcontrmp_dbt mp, ddlcontr_dbt dl                                                                  
               WHERE mp.t_SfContrID = l.T_CONTRACT                                                                      
                 AND dl.t_DlContrID = mp.t_DlContrID                                                                      
                 AND dl.t_IIS = CHR(0))   
   AND EXISTS (SELECT 1         
                 FROM dnptxlnk_dbt lnk         
                WHERE LNK.T_BUYID = l.t_id
                  AND LNK.T_DATE >= TO_DATE('01.01.2025', 'DD.MM.YYYY'));     
                    
UPDATE DNPTXLOT_DBT l   
   SET l.T_NOTCOUNTEDONIIS = 'X' 
 WHERE l.T_DOCKIND = 101   
   AND l.T_NOTCOUNTEDONIIS = CHR(0)   
   AND l.t_kind = 1
   AND EXISTS(SELECT 1                                                                     
                FROM ddlcontrmp_dbt mp, ddlcontr_dbt dl                                                                  
               WHERE mp.t_SfContrID = l.T_CONTRACT                                                                      
                 AND dl.t_DlContrID = mp.t_DlContrID                                                                      
                 AND dl.t_IIS = CHR(0))   
   AND EXISTS (SELECT 1         
                 FROM dnptxlnk_dbt lnk         
                WHERE LNK.T_BUYID = l.t_id
                  AND LNK.T_DATE >= TO_DATE('01.01.2025', 'DD.MM.YYYY'));


END;
/
