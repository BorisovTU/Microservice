begin
  MERGE INTO ddlsum_dbt l
  USING (
      SELECT 
          q1.T_DLSUMID,
          MAX(CASE
              WHEN q1.t_isiis = 0 AND q1.t_isouter != 2 THEN CHR(88)
              ELSE CHR(0)
          END) AS t_new_flag
      FROM (
          SELECT DISTINCT
              DLS.T_DLSUMID,
              RSI_NPTO.CheckContrIIS(LB.T_CONTRACT) AS t_isiis,
              NVL(
                  (SELECT T_ATTRID
                   FROM DOBJATCOR_DBT
                   WHERE T_OBJECTTYPE = 101
                     AND T_GROUPID = 111
                     AND T_OBJECT = LPAD(LB.T_DOCID, 34, '0')),
                  0
              ) AS t_isouter
          FROM 
              dnptxlnk_dbt S,
              dnptxlot_dbt LB,
              dnptxlot_dbt LS,
              ddlsum_dbt dls
          WHERE 
              S.t_BuyID = LB.t_ID
              AND S.t_SaleID = LS.t_ID
              AND LB.T_DOCKIND = 127
              AND LS.T_SALEDATE >= TO_DATE('01.01.2025', 'DD.MM.YYYY')
              AND LB.T_CONTRACT > 0
              AND dls.t_dockind = lb.T_DOCKIND
              AND dls.t_docid = LB.T_DOCID
              AND dls.t_kind = 1230
          
          UNION ALL
          
          SELECT 
              dls2.t_dlsumid,
              RSI_NPTO.CheckContrIIS(LB2.T_CONTRACT) AS t_isiis,
              NVL(
                  (SELECT T_ATTRID
                   FROM DOBJATCOR_DBT
                   WHERE T_OBJECTTYPE = 101
                     AND T_GROUPID = 111
                     AND T_OBJECT = LPAD(LB2.T_DOCID, 34, '0')),
                  0
              ) AS t_isouter
          FROM 
              dnptxlot_dbt LB2, 
              ddlsum_dbt dls2
          WHERE 
              lb2.t_kind = 2
              AND LB2.T_DOCKIND = 127
              AND dls2.t_dockind = lb2.T_DOCKIND
              AND dls2.t_docid = LB2.T_DOCID
              AND dls2.t_kind = 1230
              AND NOT EXISTS (
                  SELECT 1
                  FROM dnptxlnk_dbt lnk
                  WHERE LNK.T_BUYID = lb2.t_id
              )
      ) q1
      GROUP BY t_dlsumid
  ) src
  ON (l.t_dlsumid = src.t_dlsumid)
  WHEN MATCHED THEN
      UPDATE SET l.T_NOTCOUNTEDONIIS = src.t_new_flag;
end;
/

begin
  MERGE INTO DNPTXLOT_DBT l
  USING (
      SELECT 
          q1.T_ID,
          MAX(CASE
              WHEN q1.T_DOCKIND = 127 THEN q1.t_formdata
              ELSE DECODE(q1.t_isiis, 0, CHR(88), CHR(0))
          END) AS t_new_flag
      FROM (
          SELECT DISTINCT
              LB.T_ID,
              RSI_NPTO.CheckContrIIS(LB.T_CONTRACT) AS t_isiis,
              LB.T_DOCKIND,
              NVL(
                  CASE
                      WHEN LB.T_DOCKIND = 127 THEN
                          (SELECT T_NOTCOUNTEDONIIS
                           FROM ddlsum_dbt
                           WHERE t_dockind = 127
                             AND t_docid = LB.T_DOCID
                             AND t_kind = 1230
                             AND ROWNUM = 1)
                      ELSE
                          CHR(0)
                  END,
                  CHR(0)
              ) AS t_formdata
          FROM 
              dnptxlnk_dbt S, 
              dnptxlot_dbt LB, 
              dnptxlot_dbt LS
          WHERE 
              S.t_BuyID = LB.t_ID
              AND S.t_SaleID = LS.t_ID
              AND LB.T_DOCKIND IN (138, 127, 101)
              AND LS.T_SALEDATE >= TO_DATE('01.01.2025', 'DD.MM.YYYY')
              AND LB.T_CONTRACT > 0
          
          UNION ALL
          
          SELECT 
              lb2.t_id,
              RSI_NPTO.CheckContrIIS(LB2.T_CONTRACT) AS t_isiis,
              LB2.T_DOCKIND,
              NVL(
                  CASE
                      WHEN LB2.T_DOCKIND = 127 THEN
                          (SELECT T_NOTCOUNTEDONIIS
                           FROM ddlsum_dbt
                           WHERE t_dockind = 127
                             AND t_docid = LB2.T_DOCID
                             AND t_kind = 1230
                             AND ROWNUM = 1)
                      ELSE
                          CHR(0)
                  END,
                  CHR(0)
              ) AS t_formdata
          FROM 
              dnptxlot_dbt LB2
          WHERE 
              lb2.t_kind = 2
              AND LB2.T_DOCKIND IN (138, 127, 101)
              AND NOT EXISTS (
                  SELECT 1
                  FROM dnptxlnk_dbt lnk
                  WHERE LNK.T_BUYID = lb2.t_id
              )
      ) q1
      GROUP BY T_ID
  ) src
  ON (l.T_ID = src.T_ID)
  WHEN MATCHED THEN
      UPDATE SET l.T_NOTCOUNTEDONIIS = src.t_new_flag;
end;
/
